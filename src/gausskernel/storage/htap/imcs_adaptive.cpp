/* CPU-only, fixed-partition research Adapt. No PMU, bandwidth model, or
 * tuple-level timer. Only admission uses per-batch shared atomics. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/htap/imcs_adaptive.h"
#include "access/htap/imcs_hash_table.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/imcs_partition_wise.h"
#include "access/htap/imcs_numa_policy.h"
#include "access/htap/imcs_numa_cpu.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vec_cache_coord.h"
#include "access/htap/imcucache_mgr.h"
#include "executor/instrument.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include <sys/mman.h>
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <vector>
#include <map>
#include <algorithm>
#include <cmath>
#include <new>

namespace {
constexpr int NODES = imcsnuma::MAX_NODES;
constexpr int MAX_WORKERS = 4096;
struct Config { cpu_set_t scope; int period; };
struct Payload {
    uint64 cookie, visit, epoch, generation, remaining, completed;
    Oid relid, group;
    int ordinal, node;
    double cycles;
    bool instrumented;
};
struct alignas(128) Worker {
    uint64 sequence;
    Payload p;
    clockid_t clock;
    bool online;
};
static Worker workers[MAX_WORKERS];
static pthread_mutex_t registryMutex = PTHREAD_MUTEX_INITIALIZER;
static Config config;
static uint64 configVersion = 0, epoch = 0, nextCookie = 0;
static uint32 activeWorkers = 0;
static uint64 moveCount = 0, moveTimeUs = 0, failedMoves = 0;
// Process-wide cumulative diagnostics; compare before/after snapshots per query.
static uint64 sampleTicks = 0, loadRejected = 0, stabilityRejected = 0;
static uint64 noCandidate = 0, costRejected = 0;
static uint64 cpuSampleUnavailable = 0, imbalanceSamples = 0, stableImbalanceSamples = 0;
static uint64 sourceLow = 0, targetHigh = 0, gapSmall = 0, topologyMissing = 0;
static uint64 layoutRejected = 0, invalidDemand = 0, targetOverload = 0;
static uint64 gainSmall = 0, remainingTooShort = 0, benefitTooSmall = 0;
static uint64 hotUtilSum = 0, coldUtilSum = 0, utilizationSamples = 0;
struct Local {
    Worker* slot;
    const void* owner;
    Payload p;
    bool ambiguous;
};
static THR_LOCAL Local local;
static THR_LOCAL bool exitHook = false;

static uint64 NowUs()
{
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}
static uint64 Unit(const Payload& p) { return ((uint64)p.group << 32) | (uint32)(p.ordinal + 1); }
static void Publish()
{
    if (!local.slot) return;
    local.p.epoch = __atomic_load_n(&epoch, __ATOMIC_ACQUIRE);
    Worker* w = local.slot;
    uint64 seq = __atomic_load_n(&w->sequence, __ATOMIC_RELAXED);
    __atomic_store_n(&w->sequence, seq + 1, __ATOMIC_SEQ_CST);
    // On ARM, publish the odd sequence before any payload store becomes visible.
    __atomic_thread_fence(__ATOMIC_RELEASE);
#define PUT(f) __atomic_store(&w->p.f, &local.p.f, __ATOMIC_RELAXED)
    PUT(cookie); PUT(visit); PUT(epoch); PUT(generation); PUT(remaining); PUT(completed);
    PUT(relid); PUT(group); PUT(ordinal); PUT(node); PUT(cycles); PUT(instrumented);
#undef PUT
    __atomic_store_n(&w->sequence, seq + 2, __ATOMIC_RELEASE);
}
static bool ReadWorkerSnapshot(const Worker& w, Payload* p)
{
    if (!__atomic_load_n(&w.online, __ATOMIC_ACQUIRE)) return false;
    uint64 seq = __atomic_load_n(&w.sequence, __ATOMIC_ACQUIRE);
    if (seq & 1) return false;
#define GET(f) __atomic_load(&w.p.f, &p->f, __ATOMIC_RELAXED)
    GET(cookie); GET(visit); GET(epoch); GET(generation); GET(remaining); GET(completed);
    GET(relid); GET(group); GET(ordinal); GET(node); GET(cycles); GET(instrumented);
#undef GET
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    return seq == __atomic_load_n(&w.sequence, __ATOMIC_ACQUIRE) &&
        __atomic_load_n(&w.online, __ATOMIC_ACQUIRE);
}
static bool StillActive(uint64 unit)
{
    uint64 current = __atomic_load_n(&epoch, __ATOMIC_ACQUIRE);
    for (const auto& w : workers) {
        Payload p;
        if (ReadWorkerSnapshot(w, &p) && Unit(p) == unit && p.instrumented &&
            p.epoch + 1 >= current && p.remaining >= imcsnuma::MIN_REMAINING_CUS)
            return true;
    }
    return false;
}
static Config ReadConfig()
{
    Config result = {};
    result.period = u_sess->attr.attr_sql.imcs_adapt_period_ms;
    const char* p = u_sess->attr.attr_sql.imcs_partition_scan_cpus;
    while (*p) {
        char* end;
        unsigned long cpu = strtoul(p, &end, 10);
        if (end == p || cpu >= CPU_SETSIZE || CPU_ISSET(cpu, &result.scope) || (*end && *end != ','))
            ereport(ERROR, (errmsg("Adapt requires distinct explicit CPU IDs")));
        CPU_SET(cpu, &result.scope);
        p = *end ? end + 1 : end;
        if (*end && !*p) ereport(ERROR, (errmsg("Trailing comma in Adapt CPU pool")));
    }
    if (CPU_COUNT(&result.scope) < 2) ereport(ERROR, (errmsg("Adapt requires a CPU resource pool")));
    return result;
}
static void WorkerExit(int code, Datum arg) { ImcsAdaptWorkerLeave(); }
struct PinnedBuffer { int slot; CU* cu; };
struct MoveResources {
    bool attempted = false;
    std::vector<PinnedBuffer> buffers;
    std::vector<RowGroup*> groups;
};

static void ReleaseMoveResources(MoveResources* resources, const std::vector<IMCSDesc*>& descs)
{
    for (const auto& buffer : resources->buffers)
        IMCU_CACHE->UnpinDataBlockForPlacement(buffer.slot);
    for (auto i = resources->groups.rbegin(); i != resources->groups.rend(); ++i)
        (*i)->UnlockRowGroup();
    for (auto* desc : descs)
        __atomic_store_n(&desc->adaptMoving, 0, __ATOMIC_SEQ_CST);
    for (auto i = descs.rbegin(); i != descs.rend(); ++i)
        LWLockRelease((*i)->imcsDescLock);
}

/* Query every page, including status errors. An aggregate return code of zero
 * alone is not evidence that every page is at the requested node. */
static bool VerifyPages(CU* cu, int node)
{
    void* pages[256];
    int status[256];
    long size = sysconf(_SC_PAGESIZE);
    if (size <= 0)
        return false;
    size_t length = cu->m_partitionWiseBufferSize;
    for (size_t base = 0; base < length;) {
        unsigned count = 0;
        for (; count < lengthof(pages) && base < length; ++count, base += size)
            pages[count] = (char*)cu->m_partitionWiseBuffer + base;
        if (move_pages(0, count, pages, NULL, status, 0) != 0)
            return false;
        for (unsigned i = 0; i < count; ++i)
            if (status[i] != node)
                return false;
    }
    return true;
}

static bool MoveBuffer(CU* cu, int node)
{
    unsigned long mask = 1UL << node;
    return mbind(cu->m_partitionWiseBuffer, cu->m_partitionWiseBufferSize,
        MPOL_BIND, &mask, NODES, MPOL_MF_MOVE | MPOL_MF_STRICT) == 0 && VerifyPages(cu, node);
}

/* Called with the global descriptor hash lock shared: DROP cannot invalidate
 * descriptors. Descriptor locks prevent row-group insertion, and row-group
 * locks prevent vacuum from replacing CUs while the group is being migrated.
 * Admitted NPS/Adapt batches drain before Move; other stable-pointer readers
 * remain safe because mbind does not change virtual addresses.
 */
static bool ExecuteMove(const imcsnuma::PlacementAction& action, uint64 now,
                        std::map<uint64, std::vector<IMCSDesc*> >& members, uint64 expectedPages)
{
    uint64 started = NowUs();
    auto found = members.find(action.unit);
    if (found == members.end() || action.target < 0 || action.target >= NODES)
        return false;
    auto& descs = found->second;
    bool ready = true;
    uint64 generation = 0;
    for (auto* desc : descs) {
        generation = Max(generation, pg_atomic_read_u64(&desc->numaGeneration));
        if (ImcsCurrentDataNode(desc) != action.source)
            ready = false;
    }
    if (!ready || generation != action.generation)
        return false;
    long long freeBytes = 0;
    (void)numa_node_size64(action.target, &freeBytes);
    uint64 required = 0;
    for (auto* desc : descs)
        required += pg_atomic_read_u64(&desc->numaMappedBytes);
    long pageSize = sysconf(_SC_PAGESIZE);
    if (pageSize <= 0 || required / pageSize != expectedPages ||
        freeBytes <= 0 || required > (uint64)freeBytes / 2)
        return false;

    /* Reserve all storage before acquiring pthread row-group locks. Explicit
     * cleanup covers PostgreSQL errors; kernel failures use the rollback path. */
    MoveResources* resources = new MoveResources();
    auto& buffers = resources->buffers;
    auto& groups = resources->groups;
    size_t acquired = 0;
    for (; acquired < descs.size(); ++acquired) {
        if (!LWLockConditionalAcquire(descs[acquired]->imcsDescLock, LW_SHARED)) {
            while (acquired > 0)
                LWLockRelease(descs[--acquired]->imcsDescLock);
            delete resources;
            return false;
        }
    }
    size_t rowCount = 0, cuCount = 0;
    for (auto* desc : descs) {
        rowCount += desc->maxRowGroupCapacity;
        cuCount += (size_t)desc->maxRowGroupCapacity * (desc->imcsNatts + 1);
    }
    try {
        groups.reserve(rowCount);
        buffers.reserve(cuCount);
    } catch (...) {
        for (auto i = descs.rbegin(); i != descs.rend(); ++i)
            LWLockRelease((*i)->imcsDescLock);
        delete resources;
        throw;
    }
    /* Close admission for all members, then let active batches finish. This
     * keeps each resource sample on one placement generation; it does not
     * change lane ownership. A bounded wait avoids stalling on blocked scans. */
    for (auto* desc : descs)
        __atomic_store_n(&desc->adaptMoving, 1, __ATOMIC_SEQ_CST);
    uint64 deadline = NowUs() + 1000000;
    for (;;) {
        bool active = false;
        for (auto* desc : descs)
            active |= __atomic_load_n(&desc->adaptActiveScans, __ATOMIC_SEQ_CST) != 0;
        if (!active)
            break;
        if (NowUs() >= deadline) {
            ReleaseMoveResources(resources, descs);
            delete resources;
            return false;
        }
        pg_usleep(1000);
    }
    /* Placement pins have explicit ownership and do not allocate or use a
     * ResourceOwner while pthread locks are held. */
    HOLD_INTERRUPTS();
    bool success = false;
    bool rollback = true;
    PG_TRY();
    {
        for (auto* desc : descs) {
            for (uint32 r = 0; r < desc->maxRowGroupCapacity && ready; ++r) {
                RowGroup* row = desc->rowGroups[r];
                if (row == NULL)
                    continue;
                if (!row->isLocalLatch || pthread_rwlock_tryrdlock(&row->m_mutex) != 0) {
                    ready = false;
                    break;
                }
                groups.push_back(row);
                for (int col = 0; col <= desc->imcsNatts; ++col) {
                    CUDesc* cud = row->m_cuDescs[col];
                    if (cud == NULL || cud->IsNullCU())
                        continue;
                    DataSlotTag tag = IMCU_CACHE->InitCUSlotTag(&desc->numaRelNode, col, cud->cu_id, cud->cu_pointer);
                    int slot = IMCU_CACHE->PinReadyDataBlockForPlacement(&tag);
                    if (!IsValidCacheSlotID(slot)) { ready = false; break; }
                    CU* cu = IMCU_CACHE->GetCUBuf(slot);
                    buffers.push_back({slot, cu});
                    /* No IO/reload/allocator in the executor: only already resident,
                     * fully initialized dedicated mmap buffers are eligible. */
                    if (!IMCU_CACHE->TryLockReadyDataBlock(slot)) {
                        ready = false;
                        break;
                    }
                    ready = cu->m_partitionWiseBuffer != NULL && VerifyPages(cu, action.source);
                    IMCU_CACHE->ReleaseCompressLock(slot);
                    if (!ready)
                        break;
                }
            }
        }
        uint64 actualBytes = 0;
        for (const auto& buffer : buffers)
            actualBytes += buffer.cu->m_partitionWiseBufferSize;
        (void)numa_node_size64(action.target, &freeBytes);
        ready = ready && actualBytes == required && freeBytes > 0 && actualBytes <= (uint64)freeBytes / 2;
        ready = ready && StillActive(action.unit);
        size_t moved = 0;
        bool attempted = ready && !buffers.empty();
        resources->attempted = attempted;
        if (attempted) {
            for (; moved < buffers.size(); ++moved)
                if (!MoveBuffer(buffers[moved].cu, action.target))
                    break;
        }
        success = attempted && moved == buffers.size();
        if (attempted && !success) {
            // Include the failed buffer: the kernel may have moved some of its pages.
            for (size_t i = 0; i <= moved && i < buffers.size(); ++i)
                rollback = MoveBuffer(buffers[i].cu, action.source) && rollback;
        }
        if (success) {
            for (auto* desc : descs) {
                __atomic_store_n(&desc->currentNumaNode, (uint32)action.target, __ATOMIC_RELEASE);
                pg_atomic_fetch_add_u64(&desc->numaGeneration, 1);
            }
        } else if (!rollback) {
            // Keep old routing authoritative, but stop automatic retries on this unit.
            for (auto* desc : descs)
                __atomic_store_n(&desc->adaptDisabled, 1, __ATOMIC_RELEASE);
        }
    }
    PG_CATCH();
    {
        ReleaseMoveResources(resources, descs);
        RESUME_INTERRUPTS();
        delete resources;
        PG_RE_THROW();
    }
    PG_END_TRY();
    size_t count = buffers.size();
    bool attempted = resources->attempted;
    uint64 elapsed = NowUs() - started;
    if (attempted && success) {
        __atomic_add_fetch(&moveTimeUs, elapsed, __ATOMIC_RELEASE);
        __atomic_add_fetch(&moveCount, 1, __ATOMIC_RELEASE);
    } else if (attempted) __atomic_add_fetch(&failedMoves, 1, __ATOMIC_RELEASE);
    ReleaseMoveResources(resources, descs);
    RESUME_INTERRUPTS();
    delete resources;
    if (attempted) ereport(LOG, (errmsg("IMCS Adapt move unit=%llu source=%d target=%d buffers=%zu pages=%llu elapsed_us=%llu result=%s rollback=%s",
        (unsigned long long)action.unit, action.source, action.target, count, (unsigned long long)expectedPages,
        (unsigned long long)elapsed, success ? "complete" : "failed", rollback ? "ok" : "failed")));
    return success;
}
} // namespace

void ImcsAdaptScanInit(const void* owner, IMCSDesc* desc)
{
    if (!u_sess->attr.attr_sql.enable_imcs_adapt) return;
    if (!u_sess->attr.attr_sql.enable_imcs_nps)
        ereport(ERROR, (errmsg("CPU Adapt requires enable_imcs_nps")));
    // Affinity-only control: FollowNode still uses the destination-node CPU
    // mask, but there are no registered workers, samples or Move decisions.
    if (!u_sess->attr.attr_sql.enable_imcs_adapt_migration) return;
    if (!desc || !OidIsValid(desc->numaGroup) || u_sess->stream_cxt.producer_dop <= 1) return;
    if (!local.slot) {
        Config requested = ReadConfig();
        clockid_t clock;
        if (pthread_getcpuclockid(pthread_self(), &clock) != 0)
            ereport(ERROR, (errmsg("Cannot register Adapt worker CPU clock")));
        pthread_mutex_lock(&registryMutex);
        bool conflict = activeWorkers && (!CPU_EQUAL(&config.scope, &requested.scope) || config.period != requested.period);
        if (!conflict && (!CPU_EQUAL(&config.scope, &requested.scope) || config.period != requested.period)) {
            config = requested;
            ++configVersion;
        }
        Worker* slot = NULL;
        if (!conflict) {
            for (auto& w : workers) if (!w.online) { slot = &w; break; }
        }
        if (slot) {
            local = Local();
            local.slot = slot;
            local.owner = owner;
            local.p.cookie = ++nextCookie;
            slot->clock = clock;
            // Sequence stays monotonic across slot reuse (avoid an ABA snapshot).
            Publish();
            __atomic_store_n(&slot->online, true, __ATOMIC_RELEASE);
            pg_atomic_fetch_add_u32(&activeWorkers, 1);
        }
        pthread_mutex_unlock(&registryMutex);
        if (conflict) ereport(ERROR, (errmsg("Concurrent Adapt scans require the same CPU pool and period")));
        if (!slot) ereport(ERROR, (errmsg("Adapt worker registry is full")));
        if (!exitHook) { on_proc_exit(WorkerExit, 0); exitHook = true; }
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
    }
    if (local.owner != owner) local.ambiguous = true;
    ++local.p.visit;
    local.p.relid = desc->relOid;
    local.p.group = desc->numaGroup;
    local.p.ordinal = desc->numaOrdinal;
    local.p.node = ImcsCurrentDataNode(desc);
    local.p.generation = pg_atomic_read_u64(&desc->numaGeneration);
    local.p.completed = local.p.remaining = 0;
    local.p.cycles = 0;
    local.p.instrumented = false;
    Publish();
}
void ImcsAdaptProgress(const void* owner, uint32 cuid, uint32 lastCuid)
{
    if (!local.slot || local.owner != owner || local.ambiguous) return;
    local.p.completed = cuid;
    local.p.remaining = lastCuid > cuid ? lastCuid - cuid : 0;
}
void ImcsAdaptBegin(IMCSDesc* desc, Instrumentation* instr, volatile ImcsAdaptSample* sample)
{
    sample->desc = desc;
    sample->entered = false;
    sample->cycles = local.slot && instr ? instr->cpuusage.m_cycles : 0;
    if (!desc || !OidIsValid(desc->numaGroup) || u_sess->stream_cxt.producer_dop <= 1) return;
    if (u_sess->attr.attr_sql.enable_imcs_adapt && __atomic_load_n(&desc->adaptDisabled, __ATOMIC_ACQUIRE))
        ereport(ERROR, (errmsg("Adapt group disabled after incomplete rollback; repopulate before testing")));
    // Consistency handshake, not statistical aggregation. A pending Move closes admission.
    for (;;) {
        while (__atomic_load_n(&desc->adaptMoving, __ATOMIC_SEQ_CST)) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(1000);
        }
        pg_atomic_fetch_add_u32(&desc->adaptActiveScans, 1);
        if (!__atomic_load_n(&desc->adaptMoving, __ATOMIC_SEQ_CST)) break;
        pg_atomic_fetch_sub_u32(&desc->adaptActiveScans, 1);
    }
    sample->entered = true;
    // Cached node affinity: syscall only initially or after an actual placement change.
    ImcsPartitionWiseFollowNode(ImcsCurrentDataNode(desc));
    if (local.slot && !local.ambiguous && local.p.relid == desc->relOid) {
        uint64 generation = pg_atomic_read_u64(&desc->numaGeneration);
        if (local.p.generation != generation) {
            ++local.p.visit;
            local.p.generation = generation;
            local.p.cycles = 0;
            local.p.node = ImcsCurrentDataNode(desc);
        }
    }
}
void ImcsAdaptEnd(volatile ImcsAdaptSample* sample, Instrumentation* instr, bool done, bool failed)
{
    if (sample->entered && local.slot && local.p.relid == sample->desc->relOid) {
        // Reuse the executor's existing scan counter. It is an activity indicator
        // (ARM cntvct), never converted to true thread CPU seconds.
        if (!failed && instr && instr->cpuusage.m_cycles > sample->cycles) {
            local.p.cycles += instr->cpuusage.m_cycles - sample->cycles;
            local.p.instrumented = !local.ambiguous;
        }
        if (done || failed) local.p.remaining = 0;
        if (done || failed || local.p.epoch != __atomic_load_n(&epoch, __ATOMIC_ACQUIRE)) Publish();
    }
    if (sample->entered) {
        sample->entered = false;
        pg_atomic_fetch_sub_u32(&sample->desc->adaptActiveScans, 1);
    }
}
void ImcsAdaptWorkerLeave()
{
    if (!local.slot) return;
    pthread_mutex_lock(&registryMutex);
    __atomic_store_n(&local.slot->online, false, __ATOMIC_RELEASE);
    pg_atomic_fetch_sub_u32(&activeWorkers, 1);
    pthread_mutex_unlock(&registryMutex);
    local = Local();
    SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
}

namespace {
struct Observation { Payload p; uint64 cpuNs, at, wall; bool valid; };
struct Candidate { uint64 unit; Oid group; int ordinal; double cores, remainingUs; };
struct Monitor {
    imcsnuma::CpuSampler cpu;
    uint64 version = 0, last = 0;
    int hot = -1, cold = -1;
    unsigned streak = 0;
    Observation previous[MAX_WORKERS] = {};
    struct Recent { uint64 unit, at; } recent[64] = {};
    unsigned nextRecent = 0;
    double cost[NODES][NODES] = {};
    std::vector<Candidate> candidates;
    Monitor() { candidates.reserve(MAX_WORKERS); }
};
static THR_LOCAL Monitor* monitor = NULL;
static void MonitorExit(int code, Datum arg) { delete monitor; monitor = NULL; }
}
long ImcsAdaptWaitMs()
{
    if (!__atomic_load_n(&activeWorkers, __ATOMIC_ACQUIRE)) return 60000;
    pthread_mutex_lock(&registryMutex);
    int period = config.period;
    pthread_mutex_unlock(&registryMutex);
    if (!monitor || !monitor->last) return period;
    uint64 elapsed = NowUs() - monitor->last;
    return elapsed >= (uint64)period * 1000 ? 1 : period - elapsed / 1000;
}
void ImcsAdaptTick()
{
    // With Adapt off there is no /proc I/O, worker walk, statistics or decisions.
    if (!__atomic_load_n(&activeWorkers, __ATOMIC_ACQUIRE)) {
        if (monitor) { monitor->last = 0; monitor->streak = 0; }
        return;
    }
    Config cfg;
    uint64 version;
    pthread_mutex_lock(&registryMutex);
    cfg = config;
    version = configVersion;
    pthread_mutex_unlock(&registryMutex);
    uint64 now = NowUs();
    if (monitor && monitor->last && now - monitor->last < (uint64)cfg.period * 1000) return;
    try {
        if (!monitor) { monitor = new Monitor(); on_proc_exit(MonitorExit, 0); }
        Monitor& m = *monitor;
        if (m.version != version || !m.last) {
            if (m.version != version) {
                m.cpu.configure(cfg.scope);
                ereport(LOG, (errmsg("IMCS Adapt initialized: cpu-only period_ms=%d pool_cpus=%d", cfg.period, CPU_COUNT(&cfg.scope))));
            } else m.cpu.reset();
            m.version = version;
            m.streak = 0;
            for (auto& old : m.previous) old.valid = false;
        }
        m.last = now;
        uint64 current = __atomic_add_fetch(&epoch, 1, __ATOMIC_ACQ_REL);
        __atomic_fetch_add(&sampleTicks, 1, __ATOMIC_RELAXED);
        if (!m.cpu.sample()) {
            __atomic_fetch_add(&cpuSampleUnavailable, 1, __ATOMIC_RELAXED);
            m.streak = 0; return;
        }
        int hot = -1, cold = -1;
        for (int node = 0; node < NODES; ++node) if (m.cpu.count[node]) {
            if (hot < 0 || m.cpu.utilization[node] > m.cpu.utilization[hot]) hot = node;
            if (cold < 0 || m.cpu.utilization[node] < m.cpu.utilization[cold]) cold = node;
        }
        // CPU utilization refers to configured cores per NUMA node, not sockets.
        // These independent rejection counters may overlap within a sample.
        if (hot < 0 || cold == hot) {
            __atomic_fetch_add(&topologyMissing, 1, __ATOMIC_RELAXED);
        } else {
            __atomic_fetch_add(&utilizationSamples, 1, __ATOMIC_RELAXED);
            __atomic_fetch_add(&hotUtilSum, (uint64)(m.cpu.utilization[hot] * 1000000), __ATOMIC_RELAXED);
            __atomic_fetch_add(&coldUtilSum, (uint64)(m.cpu.utilization[cold] * 1000000), __ATOMIC_RELAXED);
            if (m.cpu.utilization[hot] <= imcsnuma::SOURCE_MIN)
                __atomic_fetch_add(&sourceLow, 1, __ATOMIC_RELAXED);
            if (m.cpu.utilization[cold] >= imcsnuma::TARGET_MAX)
                __atomic_fetch_add(&targetHigh, 1, __ATOMIC_RELAXED);
            if (m.cpu.utilization[hot] - m.cpu.utilization[cold] <= imcsnuma::CPU_GAP)
                __atomic_fetch_add(&gapSmall, 1, __ATOMIC_RELAXED);
        }
        bool balanced = hot < 0 || cold == hot || m.cpu.utilization[hot] <= imcsnuma::SOURCE_MIN ||
            m.cpu.utilization[cold] >= imcsnuma::TARGET_MAX ||
            m.cpu.utilization[hot] - m.cpu.utilization[cold] <= imcsnuma::CPU_GAP;
        if (balanced) {
            m.streak = 0;
            __atomic_fetch_add(&loadRejected, 1, __ATOMIC_RELAXED);
        } else {
            __atomic_fetch_add(&imbalanceSamples, 1, __ATOMIC_RELAXED);
            m.streak = m.hot == hot && m.cold == cold ? m.streak + 1 : 1;
            m.hot = hot; m.cold = cold;
        }
        bool eligible = !balanced && m.streak >= imcsnuma::STABLE_PERIODS;
        if (!balanced && !eligible) __atomic_fetch_add(&stabilityRejected, 1, __ATOMIC_RELAXED);
        m.candidates.clear();
        pthread_mutex_lock(&registryMutex);
        for (int i = 0; i < MAX_WORKERS; ++i) {
            Payload p;
            timespec ts;
            Observation& old = m.previous[i];
            if (!ReadWorkerSnapshot(workers[i], &p) || clock_gettime(workers[i].clock, &ts) != 0) { old.valid = false; continue; }
            uint64 cpu = (uint64)ts.tv_sec * 1000000000 + ts.tv_nsec;
            bool known = old.valid && old.at + 1 == current && p.cookie == old.p.cookie &&
                p.visit == old.p.visit && p.generation == old.p.generation && p.node == hot &&
                p.epoch + 1 >= current && p.epoch > old.p.epoch && p.instrumented &&
                p.remaining >= imcsnuma::MIN_REMAINING_CUS && p.completed > old.p.completed &&
                p.cycles > old.p.cycles && cpu > old.cpuNs;
            if (eligible && known) {
                double cpuUs = (cpu - old.cpuNs) / 1000.0;
                uint64 id = Unit(p);
                auto found = std::find_if(m.candidates.begin(), m.candidates.end(),
                    [id](const Candidate& c) { return c.unit == id; });
                if (found == m.candidates.end()) {
                    m.candidates.push_back({id, p.group, p.ordinal, 0, 0});
                    found = m.candidates.end() - 1;
                }
                found->cores += cpuUs / (now - old.wall);
                found->remainingUs += cpuUs * p.remaining / (p.completed - old.p.completed);
            }
            old.p = p; old.cpuNs = cpu; old.at = current; old.wall = now; old.valid = true;
        }
        pthread_mutex_unlock(&registryMutex);
        // Keep snapshots fresh even during balanced/stabilizing periods. Never
        // inspect descriptors/pages or consider a move until the load gate passes.
        if (!eligible) return;
        __atomic_fetch_add(&stableImbalanceSamples, 1, __ATOMIC_RELAXED);
        Candidate* best = NULL;
        for (auto& candidate : m.candidates) {
            bool cooling = false;
            for (const auto& recent : m.recent)
                if (recent.unit == candidate.unit && current - recent.at < imcsnuma::COOLDOWN_PERIODS) cooling = true;
            if (!cooling && (!best || candidate.remainingUs > best->remainingUs)) best = &candidate;
        }
        if (!best || !StillActive(best->unit)) {
            __atomic_fetch_add(&noCandidate, 1, __ATOMIC_RELAXED);
            return;
        }
        // Only now acquire descriptor lifetime protection and find the selected
        // group's members. Never inspect pages to search for a candidate.
        IMCSHashTable* table = IMCS_HASH_TABLE;
        LWLockAcquire(table->m_imcs_lock, LW_SHARED);
        PG_TRY();
        {
            try {
            std::map<uint64, std::vector<IMCSDesc*> > members;
            uint64 generation = 0, pages = 0;
            int expected = 0;
            bool valid = true;
            HASH_SEQ_STATUS seq;
            hash_seq_init(&seq, table->m_imcs_hash);
            IMCSDesc* desc;
            while ((desc = (IMCSDesc*)hash_seq_search(&seq)) != NULL) {
                if (desc->numaGroup != best->group || desc->numaOrdinal != best->ordinal) continue;
                members[best->unit].push_back(desc);
                expected = desc->numaGroupMembers;
                generation = Max(generation, pg_atomic_read_u64(&desc->numaGeneration));
                pages += pg_atomic_read_u64(&desc->numaMappedBytes) / sysconf(_SC_PAGESIZE);
                valid &= ImcsCurrentDataNode(desc) == hot && desc->imcsStatus == IMCS_POPULATE_COMPLETE &&
                    pg_atomic_read_u64(&desc->cuNumsInDisk) == 0 &&
                    !__atomic_load_n(&desc->adaptDisabled, __ATOMIC_ACQUIRE);
            }
            double cost = m.cost[hot][cold] > 0 ? m.cost[hot][cold] : imcsnuma::INITIAL_MOVE_US_PER_PAGE;
            if (valid && (int)members[best->unit].size() == expected && expected > 0 &&
                imcsnuma::worthMoving(m.cpu.utilization[hot], m.cpu.utilization[cold], m.cpu.count[hot],
                    m.cpu.count[cold], best->cores, best->remainingUs, pages, cost)) {
                imcsnuma::PlacementAction action;
                action.unit = best->unit; action.generation = generation; action.source = hot; action.target = cold;
                uint64 before = __atomic_load_n(&moveTimeUs, __ATOMIC_ACQUIRE);
                bool success = ExecuteMove(action, now, members, pages);
                m.recent[m.nextRecent++ % lengthof(m.recent)] = {best->unit, current};
                if (success && pages) {
                    double observed = double(__atomic_load_n(&moveTimeUs, __ATOMIC_ACQUIRE) - before) / pages;
                    m.cost[hot][cold] = 0.8 * cost + 0.2 * observed;
                }
            } else {
                __atomic_fetch_add(&costRejected, 1, __ATOMIC_RELAXED);
                if (!valid || (int)members[best->unit].size() != expected || expected <= 0) {
                    __atomic_fetch_add(&layoutRejected, 1, __ATOMIC_RELAXED);
                } else if (!std::isfinite(best->cores) || best->cores <= 0 ||
                    !std::isfinite(best->remainingUs) || best->remainingUs <= 0 || !pages ||
                    !std::isfinite(cost) || cost <= 0 || m.cpu.count[hot] <= 0 || m.cpu.count[cold] <= 0) {
                    __atomic_fetch_add(&invalidDemand, 1, __ATOMIC_RELAXED);
                } else {
                    // Explain the unchanged worthMoving decision. Reasons overlap.
                    double afterSource = std::max(0.0, m.cpu.utilization[hot] - best->cores / m.cpu.count[hot]);
                    double afterTarget = m.cpu.utilization[cold] + best->cores / m.cpu.count[cold];
                    double gain = (m.cpu.utilization[hot] - m.cpu.utilization[cold]) - std::abs(afterSource - afterTarget);
                    double costUs = double(pages) * cost;
                    if (afterTarget >= imcsnuma::TARGET_MAX) __atomic_fetch_add(&targetOverload, 1, __ATOMIC_RELAXED);
                    if (gain < 0.02) __atomic_fetch_add(&gainSmall, 1, __ATOMIC_RELAXED);
                    if (best->remainingUs <= 3 * costUs) __atomic_fetch_add(&remainingTooShort, 1, __ATOMIC_RELAXED);
                    if (gain * best->remainingUs <= 2 * costUs) __atomic_fetch_add(&benefitTooSmall, 1, __ATOMIC_RELAXED);
                }
            }
            } catch (const std::exception& error) {
                ereport(WARNING, (errmsg("IMCS Adapt candidate skipped: %s", error.what())));
            }
            LWLockRelease(table->m_imcs_lock);
        }
        PG_CATCH();
        {
            LWLockRelease(table->m_imcs_lock);
            PG_RE_THROW();
        }
        PG_END_TRY();
    } catch (const std::exception& error) {
        ereport(WARNING, (errmsg("IMCS CPU Adapt skipped: %s", error.what())));
    }
}

char* ImcsAdaptStatusJson(Oid root)
{
    uint64 count = 0, mapped = 0;
    bool initial = true, complete = true, disabled = false, native = true;
    if (OidIsValid(root) && HAVE_HTAP_TABLES) {
        HTAB* expected = NULL;
        if (u_sess->attr.attr_sql.enable_imcs_nps) {
            Relation rel = heap_open(root, AccessShareLock);
            expected = ImcsNpsBuildDataNodes(rel); // read-only physical key/bucket validation
            heap_close(rel, NoLock);
        }
        IMCSHashTable* table = IMCS_HASH_TABLE;
        LWLockAcquire(table->m_imcs_lock, LW_SHARED);
        PG_TRY();
        {
            HASH_SEQ_STATUS seq;
            hash_seq_init(&seq, table->m_imcs_hash);
            IMCSDesc* desc;
            while ((desc = (IMCSDesc*)hash_seq_search(&seq)) != NULL) {
                if ((desc->relOid != root && desc->parentOid != root) || (desc->imcsNatts <= 0 || desc->rowGroups == NULL)) continue;
                ++count;
                if (ImcsCurrentDataNode(desc) >= 0 && (!desc->isPartition || OidIsValid(desc->numaGroup))) ++mapped;
                int currentNode = ImcsCurrentDataNode(desc);
                initial &= currentNode == desc->dataNumaNode;
                native &= currentNode == -1 && desc->dataNumaNode == -1 && !OidIsValid(desc->numaGroup);
                if (expected) initial &= ImcsNpsIdentityMatches(expected, desc);
                complete &= desc->imcsStatus == IMCS_POPULATE_COMPLETE && !pg_atomic_read_u64(&desc->cuNumsInDisk);
                disabled |= __atomic_load_n(&desc->adaptDisabled, __ATOMIC_ACQUIRE) != 0;
            }
        }
        PG_CATCH();
        {
            LWLockRelease(table->m_imcs_lock);
            if (expected) hash_destroy(expected);
            PG_RE_THROW();
        }
        PG_END_TRY();
        LWLockRelease(table->m_imcs_lock);
        if (expected) hash_destroy(expected);
    }
    uint64 cacheStarted, cacheWritten, cacheStartFailed, cachePerfUnavailable, cacheWriteFailed, cacheDiscarded;
    VecCacheTraceReadCounters(&cacheStarted, &cacheWritten, &cacheStartFailed, &cachePerfUnavailable, &cacheWriteFailed, &cacheDiscarded);
    uint64 rhjBuilds, rhjCacheBuilds, rhjChipletBuilds, rhjParts, rhjMisses;
    RhjReadCounters(&rhjBuilds, &rhjCacheBuilds, &rhjChipletBuilds, &rhjParts, &rhjMisses);
    uint64 claims, reassigned;
    ImcsLocalTaskCounters(&claims, &reassigned);
    uint64 poolEntries;
    uint32 poolCpus;
    ImcsPartitionWisePoolCounters(&poolEntries, &poolCpus);
    char* detail = psprintf("\"cpu_sample_unavailable\":%llu,\"imbalance_samples\":%llu,\"stable_imbalance_samples\":%llu,\"source_low\":%llu,\"target_high\":%llu,\"gap_small\":%llu,\"topology_missing\":%llu,\"layout_rejected\":%llu,\"invalid_demand\":%llu,\"target_overload\":%llu,\"gain_small\":%llu,\"remaining_too_short\":%llu,\"benefit_too_small\":%llu,\"hot_util_ppm_sum\":%llu,\"cold_util_ppm_sum\":%llu,\"utilization_samples\":%llu",
        (unsigned long long)__atomic_load_n(&cpuSampleUnavailable, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&imbalanceSamples, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&stableImbalanceSamples, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&sourceLow, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&targetHigh, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&gapSmall, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&topologyMissing, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&layoutRejected, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&invalidDemand, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&targetOverload, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&gainSmall, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&remainingTooShort, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&benefitTooSmall, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&hotUtilSum, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&coldUtilSum, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&utilizationSamples, __ATOMIC_RELAXED));
    char* coord = VecCacheCoordStatsJson();
    char* result = psprintf("{\"experiment_policy_version\":3,\"launcher_membind_mask\":%llu,\"baseline_pool_entries\":%llu,\"baseline_pool_cpus\":%u,\"entries\":%llu,\"mapped\":%llu,\"native\":%s,\"initial\":%s,\"complete\":%s,\"disabled\":%s,\"moves\":%llu,\"move_us\":%llu,\"failures\":%llu,\"active_workers\":%u,\"adapt_sampling_version\":4,\"local_task_claims\":%llu,\"local_task_reassigned\":%llu,\"sample_ticks\":%llu,\"load_rejected\":%llu,\"stability_rejected\":%llu,\"no_candidate\":%llu,\"cost_or_layout_rejected\":%llu,\"rhj_builds\":%llu,\"rhj_cache_builds\":%llu,\"rhj_chiplet_builds\":%llu,\"rhj_partitions\":%llu,\"rhj_target_misses\":%llu,\"cache_trace_started\":%llu,\"cache_trace_written\":%llu,\"cache_trace_start_failed\":%llu,\"cache_trace_perf_unavailable\":%llu,\"cache_trace_write_failed\":%llu,\"cache_trace_discarded\":%llu,%s,%s}",
        (unsigned long long)imcsnuma::LauncherBindNodes(), (unsigned long long)poolEntries, poolCpus, (unsigned long long)count, (unsigned long long)mapped, native ? "true" : "false", initial ? "true" : "false", complete ? "true" : "false",
        disabled ? "true" : "false", (unsigned long long)__atomic_load_n(&moveCount, __ATOMIC_ACQUIRE),
        (unsigned long long)__atomic_load_n(&moveTimeUs, __ATOMIC_ACQUIRE),
        (unsigned long long)__atomic_load_n(&failedMoves, __ATOMIC_ACQUIRE), __atomic_load_n(&activeWorkers, __ATOMIC_ACQUIRE),
        (unsigned long long)claims, (unsigned long long)reassigned,
        (unsigned long long)__atomic_load_n(&sampleTicks, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&loadRejected, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&stabilityRejected, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&noCandidate, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&costRejected, __ATOMIC_RELAXED),
        (unsigned long long)rhjBuilds, (unsigned long long)rhjCacheBuilds, (unsigned long long)rhjChipletBuilds,
        (unsigned long long)rhjParts, (unsigned long long)rhjMisses,
        (unsigned long long)cacheStarted, (unsigned long long)cacheWritten, (unsigned long long)cacheStartFailed,
        (unsigned long long)cachePerfUnavailable, (unsigned long long)cacheWriteFailed, (unsigned long long)cacheDiscarded, detail, coord);
    pfree(detail);
    pfree(coord);
    return result;
}
