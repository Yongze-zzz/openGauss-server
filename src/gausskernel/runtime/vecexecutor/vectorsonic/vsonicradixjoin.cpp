/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * -----------------------------------------------------------------------------
 * vsonicradixjoin.cpp
 *              Routines to handle vector sonic hashjoin nodes.
 *              Sonic Hash Join nodes are based on the column-based hash table.
 *
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicradixjoin.cpp
 *
 * -----------------------------------------------------------------------------
 */
#include "vectorsonic/vsonichash.h"
#include "vecexecutor/operator_data_access.h"
#include "vectorsonic/vsonicradixjoin.h"
#include "vecexecutor/vec_cache_coord.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/exec/execStream.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include <sched.h>
#include <pthread.h>
#include <time.h>
#include <type_traits>
#include "utils/memprot.h"
#include "utils/selfuncs.h"
#include <cmath>

#define leftrot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define PROFILE_PART(x, sz)                                                                                \
    if (anls_opt_is_on(ANLS_HASH_CONFLICT)) {                                                              \
        profile(true, x, sz);                                                                              \
    } else if (u_sess->attr.attr_resource.resource_track_level >= RESOURCE_TRACK_QUERY &&                  \
               u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource) { \
        profile(false, x, sz);                                                                             \
    }
#define INSTR (m_runtime->js.ps.instrument)

static const uint32 RHJ_PROBE_TILE_BATCHES = 32;
static const uint32 RHJ_PROBE_TILE_ROWS = RHJ_PROBE_TILE_BATCHES * BatchMaxSize;
static const uint64 RHJ_PROBE_TILE_BYTES = 4 * 1024 * 1024; // stop at a vector boundary

static uint64 rhjBuilds = 0, rhjCacheBuilds = 0, rhjChipletBuilds = 0, rhjPartitions = 0, rhjTargetMisses = 0;
void RhjReadCounters(uint64* builds, uint64* cacheBuilds, uint64* chipletBuilds, uint64* partitions, uint64* targetMisses)
{
    *builds = __atomic_load_n(&rhjBuilds, __ATOMIC_RELAXED);
    *cacheBuilds = __atomic_load_n(&rhjCacheBuilds, __ATOMIC_RELAXED);
    *chipletBuilds = __atomic_load_n(&rhjChipletBuilds, __ATOMIC_RELAXED);
    *partitions = __atomic_load_n(&rhjPartitions, __ATOMIC_RELAXED);
    *targetMisses = __atomic_load_n(&rhjTargetMisses, __ATOMIC_RELAXED);
}

namespace {
struct alignas(128) RhjWorkerSample { uint64 tid, at; int domain, cpu; };
static RhjWorkerSample rhjWorkers[2048];
static uint64 rhjCacheBytes[CPU_SETSIZE];
static int rhjDomains[CPU_SETSIZE];
static unsigned rhjLanes[CPU_SETSIZE];
static pthread_mutex_t rhjTopologyMutex = PTHREAD_MUTEX_INITIALIZER;
static uint64 rhjTrackingDemand = 0;
static uint64 rhjSnapshotAt = 0;
static int rhjSnapshotCounts[CPU_SETSIZE];
static pthread_mutex_t rhjSnapshotMutex = PTHREAD_MUTEX_INITIALIZER;
static THR_LOCAL int rhjSlot = -1;
static THR_LOCAL uint64 rhjPublished = 0;
static THR_LOCAL bool rhjRequestedTracking = false;
static THR_LOCAL int rhjJoinDepth = 0;
static uint64 RhjNow()
{
    timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
static unsigned RhjCountLlcLanes(const char* list)
{
    unsigned lanes = 0;
    long previous = -1;
    char* end = NULL;
    do {
        long first = strtol(list, &end, 10);
        if (end == list)
            return 0;
        long last = first;
        if (*end == '-') {
            list = end + 1;
            last = strtol(list, &end, 10);
            if (end == list)
                return 0;
        }
        if (first <= previous || last < first || last >= CPU_SETSIZE)
            return 0;
        lanes += last - first + 1;
        previous = last;
        list = end + 1;
    } while (*end == ',');
    return *end == '\n' || *end == '\0' ? lanes : 0;
}

static uint64 RhjCache(int cpu, int* domain)
{
    if (cpu < 0 || cpu >= CPU_SETSIZE) return 0;
    uint64 bytes = __atomic_load_n(&rhjCacheBytes[cpu], __ATOMIC_ACQUIRE);
    if (!bytes) {
        pthread_mutex_lock(&rhjTopologyMutex);
        char path[160]; unsigned long long size = 0; char unit = 0; int first = -1;
        snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%d/cache/index3/size", cpu);
        FILE* file = fopen(path, "r");
        if (file) { if (fscanf(file, "%llu%c", &size, &unit) != 2) size = 0; fclose(file); }
        snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%d/cache/index3/shared_cpu_list", cpu);
        file = fopen(path, "r");
        unsigned lanes = 0;
        if (file) {
            char list[CPU_SETSIZE * 5 + 1];
            if (fgets(list, sizeof(list), file) != NULL && sscanf(list, "%d", &first) == 1)
                lanes = RhjCountLlcLanes(list);
            fclose(file);
        }
        bytes = size * (unit == 'K' ? 1024ULL : unit == 'M' ? 1048576ULL : 1ULL);
        if (first < 0 || first >= CPU_SETSIZE) bytes = 0;
        if (bytes) {
            __atomic_store_n(&rhjDomains[cpu], first, __ATOMIC_RELAXED);
            __atomic_store_n(&rhjLanes[cpu], lanes, __ATOMIC_RELAXED);
            __atomic_store_n(&rhjCacheBytes[cpu], bytes, __ATOMIC_RELEASE);
        }
        pthread_mutex_unlock(&rhjTopologyMutex);
    }
    if (bytes) *domain = __atomic_load_n(&rhjDomains[cpu], __ATOMIC_RELAXED);
    return bytes;
}
}

int RhjLlcDomain(int cpu, uint64* cacheBytes, unsigned* lanes)
{
    int domain = -1;
    uint64 bytes = RhjCache(cpu, &domain);
    if (cacheBytes != NULL)
        *cacheBytes = bytes;
    if (lanes != NULL)
        *lanes = bytes != 0 ? __atomic_load_n(&rhjLanes[cpu], __ATOMIC_RELAXED) : 0;
    return bytes != 0 ? domain : -1;
}

static void RhjTrackPublish()
{
    if (!rhjJoinDepth || !__atomic_load_n(&rhjTrackingDemand, __ATOMIC_ACQUIRE)) return;
    uint64 now = RhjNow();
    if (rhjSlot >= 0 && now - rhjPublished < 2000000ULL) return;
    uint64 tid = (uint64)t_thrd.proc_cxt.MyProcPid;
    if (rhjSlot < 0) {
        for (int i = 0; i < 2048; ++i) {
            uint64 found = __atomic_load_n(&rhjWorkers[i].tid, __ATOMIC_ACQUIRE);
            if (found == tid || (found == 0 && __atomic_compare_exchange_n(&rhjWorkers[i].tid, &found, tid, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))) {
                rhjSlot = i; break;
            }
        }
    }
    if (rhjSlot < 0) return;
    int domain = -1;
    int cpu = sched_getcpu();
    if (!RhjCache(cpu, &domain)) return;
    __atomic_store_n(&rhjWorkers[rhjSlot].domain, domain, __ATOMIC_RELAXED);
    __atomic_store_n(&rhjWorkers[rhjSlot].cpu, cpu, __ATOMIC_RELAXED);
    __atomic_store_n(&rhjWorkers[rhjSlot].at, now, __ATOMIC_RELEASE);
    rhjPublished = now;
}

void RhjTrackEnter(PlanState* node)
{
    if (IsA(node, VecHashJoinState)) {
        ++rhjJoinDepth;
        if (u_sess->attr.attr_sql.radix_chiplet_aware && !rhjRequestedTracking) {
            __atomic_fetch_add(&rhjTrackingDemand, 1, __ATOMIC_ACQ_REL);
            rhjRequestedTracking = true;
        }
    }
    RhjTrackPublish();
}

void RhjTrackLeave(PlanState* node)
{
    if (IsA(node, VecHashJoinState) && rhjJoinDepth > 0) --rhjJoinDepth;
    if (!rhjJoinDepth && rhjSlot >= 0) {
        __atomic_store_n(&rhjWorkers[rhjSlot].at, 0, __ATOMIC_RELEASE);
        rhjPublished = 0;
    }
}

void RhjTrackReset()
{
    if (rhjSlot >= 0) {
        __atomic_store_n(&rhjWorkers[rhjSlot].at, 0, __ATOMIC_RELEASE);
        __atomic_store_n(&rhjWorkers[rhjSlot].tid, 0, __ATOMIC_RELEASE);
    }
    rhjSlot = -1; rhjPublished = 0; rhjJoinDepth = 0;
    if (rhjRequestedTracking) {
        __atomic_fetch_sub(&rhjTrackingDemand, 1, __ATOMIC_ACQ_REL);
        rhjRequestedTracking = false;
    }
}

static int RhjObservedWorkers(PlanState* node, uint64* cacheBytes)
{
    RhjTrackPublish();
    int domain = -1;
    *cacheBytes = RhjCache(sched_getcpu(), &domain);
    if (!*cacheBytes || rhjSlot < 0)
        ereport(ERROR, (errmsg("Cannot establish RHJ cache domain or worker observation")));
    uint64 now = RhjNow();
    if (now - __atomic_load_n(&rhjSnapshotAt, __ATOMIC_ACQUIRE) >= 10000000ULL &&
        pthread_mutex_trylock(&rhjSnapshotMutex) == 0) {
        if (now - rhjSnapshotAt >= 10000000ULL) {
            int counts[CPU_SETSIZE] = {};
            bool seenCpu[CPU_SETSIZE] = {};
            for (int i = 0; i < 2048; ++i) {
                uint64 at = __atomic_load_n(&rhjWorkers[i].at, __ATOMIC_ACQUIRE);
                int d = __atomic_load_n(&rhjWorkers[i].domain, __ATOMIC_RELAXED);
                int cpu = __atomic_load_n(&rhjWorkers[i].cpu, __ATOMIC_RELAXED);
                if (at && at <= now && now - at <= 50000000ULL && d >= 0 && d < CPU_SETSIZE &&
                    cpu >= 0 && cpu < CPU_SETSIZE && !seenCpu[cpu]) {
                    seenCpu[cpu] = true;
                    ++counts[d];
                }
            }
            for (int i = 0; i < CPU_SETSIZE; ++i) __atomic_store_n(&rhjSnapshotCounts[i], counts[i], __ATOMIC_RELAXED);
            __atomic_store_n(&rhjSnapshotAt, now, __ATOMIC_RELEASE);
        }
        pthread_mutex_unlock(&rhjSnapshotMutex);
    }
    return Max(1, __atomic_load_n(&rhjSnapshotCounts[domain], __ATOMIC_RELAXED));
}

/* A one-shot SonicHashSource used by the radix probe path. */
class SonicHashBatchSource : public SonicHashSource {
public:
    SonicHashBatchSource(VectorBatch* batch, bool& used) : m_batch(batch), m_used(used)
    {}

    ~SonicHashBatchSource()
    {}

    VectorBatch* getBatch()
    {
        if (m_used) {
            return NULL;
        }
        m_used = true;
        return m_batch;
    }

private:
    VectorBatch* m_batch;
    bool& m_used;
};

/* Preserve the direct Sonic probe path when radix build selected one table.
 * Keep child time separate without staging or reordering the input vector. */
class SonicHashTimedSource : public SonicHashSource {
public:
    SonicHashTimedSource(SonicHashSource* source, double& inputTime, uint64& batches, uint64& tiles,
        uint32& peakRows)
        : m_source(source), m_inputTime(inputTime), m_batches(batches), m_tiles(tiles), m_peakRows(peakRows)
    {}

    VectorBatch* getBatch()
    {
        instr_time start;
        (void)INSTR_TIME_SET_CURRENT(start);
        VectorBatch* batch = m_source->getBatch();
        m_inputTime += elapsed_time(&start);
        if (!BatchIsNull(batch)) {
            ++m_batches;
            ++m_tiles;
            m_peakRows = Max(m_peakRows, (uint32)batch->m_rows);
        }
        return batch;
    }

private:
    SonicHashSource* m_source;
    double& m_inputTime;
    uint64& m_batches;
    uint64& m_tiles;
    uint32& m_peakRows;
};

#ifdef ENABLE_HTAP
namespace {
const uint32 rhjOverlapSampleCapacity = 1024;
const uint64 rhjMatchDomainCooldownNs = UINT64CONST(250000);
const uint64 rhjMatchWorkerCooldownNs = UINT64CONST(2000000);
const uint64 rhjMatchSlowYieldNs = UINT64CONST(50000);
const uint64 rhjMatchSlowCooldownNs = UINT64CONST(10000000);

bool RhjPeerBothActive(const RhjProbeOverlapDomain& domain)
{
    return (uint32)__atomic_load_n(&domain.lookup, __ATOMIC_ACQUIRE) != 0 &&
        (uint32)__atomic_load_n(&domain.match, __ATOMIC_ACQUIRE) != 0;
}

}

class SonicRadixHashJoin::ProbeCacheSample {
public:
    ProbeCacheSample(ProbeCacheStats& stats, bool enabled, uint64 rows, const RhjProbeOverlapDomain* peer = NULL,
        ProbeOverlapTrace* trace = NULL, uint32 part = 0, bool split = false, bool match = false,
        bool timed = false, uint32 candidates = 0)
        : m_stats(stats), m_rows(rows), m_peer(peer), m_trace(trace), m_part(part), m_split(split), m_match(match),
          m_timed(timed), m_candidates(candidates)
    {
        unsigned sampleMask = m_timed || (m_peer != NULL && m_match &&
            u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind != (int)RhjOverlapPeerKind::RadixJoin) ? 7U : 31U;
        if (enabled && (++stats.calls & sampleMask) == ((unsigned)u_sess->stream_cxt.smp_id & sampleMask)) {
            if (m_peer != NULL) {
                m_radixPeer = u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind == (int)RhjOverlapPeerKind::RadixJoin;
                if (m_radixPeer) {
                    m_lookupStart = __atomic_load_n(&m_peer->lookup, __ATOMIC_ACQUIRE);
                    m_matchStart = __atomic_load_n(&m_peer->match, __ATOMIC_ACQUIRE);
                }
                m_heterogeneousStart = __atomic_load_n(&m_peer->heterogeneous, __ATOMIC_ACQUIRE);
            }
            if (!VecCacheTraceSnapshot(&m_before, m_timed ? &m_beforeCycles : NULL, m_timed ? &m_beforeStalls : NULL))
                return;
            m_start = RhjNow();
            m_cpuStart = cpuTime();
            m_active = true;
        }
    }

    void markHashEnd()
    {
        if (m_active && m_peer != NULL && m_split) {
            m_cpuSplit = cpuTime() - m_cpuStart;
            m_haveSplit = VecCacheTraceSnapshot(&m_middle);
        }
    }

    ~ProbeCacheSample()
    {
        if (!m_active)
            return;
        uint64 endNs = RhjNow();
        uint64 elapsed = endNs - m_start;
        uint64 cpuElapsed = cpuTime() - m_cpuStart;
        VecCacheTraceCounters after;
        uint64 cycles = 0;
        uint64 stalls = 0;
        if (!VecCacheTraceSnapshot(&after, m_timed ? &cycles : NULL, m_timed ? &stalls : NULL) ||
            (m_timed && (cycles <= m_beforeCycles || stalls < m_beforeStalls)) ||
            after.instructions <= m_before.instructions ||
            after.misses < m_before.misses || after.runningNs <= m_before.runningNs ||
            after.enabledNs <= m_before.enabledNs)
            return;
        uint64 running = after.runningNs - m_before.runningNs;
        uint64 enabled = after.enabledNs - m_before.enabledNs;
        if (running > enabled || (double)running < 0.95 * enabled)
            return;
        ++m_stats.samples;
        m_stats.rows += m_rows;
        m_stats.instructions += after.instructions - m_before.instructions;
        m_stats.misses += after.misses - m_before.misses;
        m_stats.runningNs += running;
        m_stats.wallNs += elapsed;
        m_stats.cpuNs += cpuElapsed;
        if (m_timed) {
            record(m_match ? 3 : 0, 0, after.misses - m_before.misses, cpuElapsed,
                endNs, cycles - m_beforeCycles, stalls - m_beforeStalls);
            return;
        }
        if (m_peer != NULL) {
            uint64 lookupEnd = m_radixPeer ? __atomic_load_n(&m_peer->lookup, __ATOMIC_ACQUIRE) : 0;
            uint64 matchEnd = m_radixPeer ? __atomic_load_n(&m_peer->match, __ATOMIC_ACQUIRE) : 0;
            uint64 heterogeneousEnd = __atomic_load_n(&m_peer->heterogeneous, __ATOMIC_ACQUIRE);
            if ((m_lookupStart >> 32) != (lookupEnd >> 32) || (m_matchStart >> 32) != (matchEnd >> 32)) {
                ++m_trace->transitions;
            } else {
                // A peer can copy a short batch entirely inside a match sample.
                bool peerChanged = (m_heterogeneousStart >> 32) != (heterogeneousEnd >> 32);
                m_trace->transitions += peerChanged;
                uint32 state = ((uint32)m_lookupStart != 0 ? 1U : 0U) | ((uint32)m_matchStart != 0 ? 2U : 0U) |
                    (peerChanged ? 8U : 0U) | ((uint32)m_heterogeneousStart != 0 ? 4U : 0U);
                if (m_split && !m_match) {
                    if (m_haveSplit && m_middle.misses >= m_before.misses && m_middle.misses <= after.misses &&
                        m_cpuSplit <= cpuElapsed && validSplit(m_before, m_middle) && validSplit(m_middle, after)) {
                        record(1, state, m_middle.misses - m_before.misses, m_cpuSplit);
                        record(2, state, after.misses - m_middle.misses, cpuElapsed - m_cpuSplit);
                    } else {
                        ++m_trace->invalidSplit;
                    }
                } else {
                    record(m_match ? 3 : 0, state, after.misses - m_before.misses, cpuElapsed);
                }
            }
        }
    }

private:
    ProbeCacheStats& m_stats;
    uint64 m_rows;
    const RhjProbeOverlapDomain* m_peer;
    ProbeOverlapTrace* m_trace;
    uint32 m_part;
    bool m_split;
    bool m_radixPeer = false;
    bool m_match;
    bool m_timed;
    uint32 m_candidates;
    uint64 m_beforeCycles = 0;
    uint64 m_beforeStalls = 0;
    bool m_haveSplit = false;
    uint64 m_cpuSplit = 0;
    uint64 m_lookupStart = 0;
    uint64 m_matchStart = 0;
    uint64 m_heterogeneousStart = 0;
    bool m_active = false;
    uint64 m_start;
    uint64 m_cpuStart;
    VecCacheTraceCounters m_before;
    VecCacheTraceCounters m_middle;
    static bool validSplit(const VecCacheTraceCounters& before, const VecCacheTraceCounters& after)
    {
        if (after.runningNs <= before.runningNs || after.enabledNs <= before.enabledNs ||
            after.instructions <= before.instructions)
            return false;
        uint64 running = after.runningNs - before.runningNs;
        uint64 enabled = after.enabledNs - before.enabledNs;
        return running <= enabled && (double)running >= 0.95 * enabled;
    }

    void record(uint32 stage, uint32 state, uint64 misses, uint64 cpuNs,
        uint64 endNs = 0, uint64 cycles = 0, uint64 stalls = 0)
    {
        if (m_trace->size == rhjOverlapSampleCapacity * (m_timed ? 2U : 1U)) {
            ++m_trace->dropped;
            return;
        }
        m_trace->records[m_trace->size++] = {m_start, misses, cpuNs, m_part, (uint32)m_rows,
            state, stage, endNs, cycles, stalls, m_candidates};
    }
    static uint64 cpuTime()
    {
        timespec ts = {};
        (void)clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        return (uint64)ts.tv_sec * UINT64CONST(1000000000) + ts.tv_nsec;
    }
};
#endif

/*
 * Hash table size: next size + bucket size
 * next size   : 4bytes * (nrows + 1)
 * ignore 1 in estimated result.
 * bucket size : 4bytes * bucket row number
 * Bucket row number is hashfindprime(nrows). But avoid
 * calculating the result every time, we give it an estimated
 * result 1.01 * nrows. It maybe not suitable in some situations.
 *
 * Estimated hash table size:
 * 4 bytes * (nrows + 1.01 nrows) = 8.04 nrows.
 */

#ifdef USE_PRIME
#define GETLOCID(val, mask) ((val) % (mask))
#else
#define GETLOCID(val, mask) ((val) & (mask))
#endif

SonicRadixHashJoin::SonicRadixHashJoin(int size, VecHashJoinState* node)
    : SonicHash(size),
      m_complicatekey(false),
      m_radixMode(false),
      m_radixOuterBatch(NULL),
      m_radixProbeBatch(NULL),
      m_radixRowIdx(NULL),
      m_radixPartStart(NULL),
      m_radixCounts(NULL),
      m_radixCursor(NULL),
      m_radixOuterHash(NULL),
      m_radixProbeCopyTime(0),
      m_radixProbePartitionTime(0),
      m_radixInputContext(NULL),
      m_radixInputAllocated(0),
      m_radixInputAvailable(0),
      m_radixPart(0),
      m_radixHasBatch(false),
      m_radixPartInProgress(false),
      m_radixIndexedProbe(false),
      m_radixProbeTileContext(NULL),
      m_radixProbeTileBatches(NULL),
      m_radixProbePendingBatch(NULL),
      m_radixProbePendingRow(0),
      m_radixProbeTileRows(0),
      m_radixPartOffset(0),
      m_radixProbeInputEnded(false),
      m_radixProbeTiles(0),
      m_radixProbeInputBatches(0),
      m_radixProbeTilePeakRows(0),
      m_radixProbeTilePeakBytes(0),
      m_runtime(node),
      m_outRawBatch(NULL),
      m_matchLocIndx(0),
      m_probeIdx(0),
      m_arrayExpandSize(0),
      m_partLoadedOffset(-1),
      m_maxPLevel(3),
      m_isValid(NULL)
{
    ScalarDesc unknown_desc;

    AddControlMemoryContext(m_runtime->js.ps.instrument, m_memControl.hashContext);

    m_build_time = 0.0;
    m_probe_time = 0.0;
    /* Freeze settings for this executor, including across rescans. */
    m_radixMode = true;
    m_radixTargetMB = u_sess->attr.attr_sql.radix_partition_size_mb;
    m_cacheAware = u_sess->attr.attr_sql.radix_cache_aware;
    m_chipletAware = u_sess->attr.attr_sql.radix_chiplet_aware;
    m_llcMB = u_sess->attr.attr_sql.radix_llc_size_mb;
    m_workersPerChiplet = 1; // measured at build checkpoints, never inferred from DOP.
    m_partition_time = 0;
    m_peakBytes = 0;
    m_radixSourceUsed = false;
    m_radixReported = false;
    m_radixSharedBuildContext = NULL;
    m_radixTriedSharedBuild = false;
    m_radixBuildInputTime = m_radixProbeInputTime = 0;
#ifdef ENABLE_HTAP
    if (u_sess->attr.attr_sql.enable_cache_trace) {
        const char* profileNode = gs_getenv_r("OG_RHJ_PROBE_PROFILE_NODE");
        char* end = NULL;
        long id = profileNode == NULL ? 0 : strtol(profileNode, &end, 10);
        m_profileProbeCache = id > 0 && *end == '\0' && id == node->js.ps.plan->plan_node_id;
    }
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group != NULL && group->m_rhjProbeOverlap != NULL) {
        const auto& settings = u_sess->attr.attr_sql;
        int plan = node->js.ps.plan->plan_node_id;
        m_profileProbePeer = plan == settings.imcs_rhj_overlap_peer_node &&
            settings.imcs_rhj_overlap_peer_kind == (int)RhjOverlapPeerKind::RadixJoin;
        m_probeOverlapVictim = plan == settings.imcs_rhj_overlap_victim_node;
        if (m_probeOverlapVictim && settings.enable_cache_trace) {
            m_streamTimeline = RhjStreamTimeline();
            const char* split = gs_getenv_r("OG_RHJ_SPLIT_LOOKUP");
            m_splitProbeOverlap = settings.imcs_rhj_overlap_peer_kind != (int)RhjOverlapPeerKind::RadixJoin ||
                (split != NULL && strcmp(split, "on") == 0);
            m_probeOverlap.records = (ProbeOverlapRecord*)palloc(rhjOverlapSampleCapacity * (m_streamTimeline ? 2 : 1) *
                sizeof(*m_probeOverlap.records));
            m_profileProbeCache = true;
        }
    }
#endif

    m_buildOp.tupleDesc = innerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    m_probeOp.tupleDesc = outerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;

    m_buildOp.cols = m_buildOp.tupleDesc->natts;
    m_probeOp.cols = m_probeOp.tupleDesc->natts;

    m_buildOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
    m_probeOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);

    m_eqfunctions = m_runtime->eqfunctions;

    /* init hash key index */
    m_buildOp.keyNum = list_length(m_runtime->hj_InnerHashKeys);
    m_probeOp.keyNum = m_buildOp.keyNum;
    m_buildOp.keyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.keyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_buildOp.oKeyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.oKeyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);

    m_integertype = (bool*)palloc(sizeof(bool) * m_buildOp.keyNum);
    for (int i = 0; i < m_buildOp.keyNum; i++) {
        m_integertype[i] = true;
    }
    setHashIndex(m_buildOp.keyIndx, m_buildOp.oKeyIndx, m_runtime->hj_InnerHashKeys);
    setHashIndex(m_probeOp.keyIndx, m_probeOp.oKeyIndx, m_runtime->hj_OuterHashKeys);

    errno_t rc = memset_s(m_memPartFlag, sizeof(m_memPartFlag), 0, SONIC_PART_MAX_NUM * sizeof(m_memPartFlag[0]));
    securec_check(rc, "\0", "\0");

    initMemoryControl();
    m_memControl.hashContext = AllocSetContextCreate(CurrentMemoryContext,
        "SonicRadixHashJoinContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        EnableBorrowWorkMemory() ? RACK_CONTEXT : STANDARD_CONTEXT,
        m_memControl.totalMem);

    /* init hash functions */
    m_buildOp.hashFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_buildOp.hashAtomFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashAtomFunc = NULL;

    if (m_complicatekey) {
        bindingFp<true>();
    } else {
        bindingFp<false>();
    }
    initHashFmgr();

    /* init one memory partition for memory hashjoin */
    m_partNum = 1;

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        m_innerPartitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*));
        m_innerPartitions[0] = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem, m_atomSize);
        initPartition<true>(m_innerPartitions[0]);
    }
    m_outerPartitions = NULL;
    calcDatumArrayExpandSize();

    m_hashOpPartition = New(CurrentMemoryContext) SonicHashOpSource(outerPlanState(m_runtime));

    if (m_complicatekey) {
        m_complicate_innerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
        m_complicate_outerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
        m_cjVector = New(CurrentMemoryContext) ScalarVector;
        m_cjVector->init(CurrentMemoryContext, unknown_desc);
    } else {
        replaceEqFunc();
    }

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t ret = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(ret, "\0", "\0");
    }

    m_diskPartNum = 0;
    m_strategy = MEMORY_HASH;
    elog(DEBUG1, "SonicRadixHashJoin initialized node=%d cache_aware=%d chiplet_aware=%d",
        m_runtime->js.ps.plan->plan_node_id, m_cacheAware, m_chipletAware);
}

/*
 * @Description: Binding some execution functions.
 */
template <bool complicateJoinKey>
void SonicRadixHashJoin::bindingFp()
{
    m_funBuild[0] = &SonicRadixHashJoin::saveToMemory<complicateJoinKey>;
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        m_funBuild[1] = &SonicRadixHashJoin::saveToDisk<complicateJoinKey, true>;
    } else {
        m_funBuild[1] = &SonicRadixHashJoin::saveToDisk<complicateJoinKey, false>;
    }
    m_probeFun[0] = &SonicRadixHashJoin::probeMemory;
    m_probeFun[1] = &SonicRadixHashJoin::probeGrace;
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        m_saveProbePartition = &SonicRadixHashJoin::saveProbePartition<complicateJoinKey, true>;
    } else {
        m_saveProbePartition = &SonicRadixHashJoin::saveProbePartition<complicateJoinKey, false>;
    }
    if (!complicateJoinKey) {
        initMatchFunc(m_buildOp.tupleDesc, m_buildOp.keyNum);
        initHashFunc(m_buildOp.tupleDesc, (void*)m_buildOp.hashFunc, m_buildOp.keyIndx, false);
        initHashFunc(m_buildOp.tupleDesc, (void*)m_buildOp.hashAtomFunc, m_buildOp.keyIndx, true);
        initHashFunc(m_probeOp.tupleDesc, (void*)m_probeOp.hashFunc, m_probeOp.keyIndx, false);
    }
}

/*
 * @Description: Compute hash key index and check whether the hashkey is simple type.
 * @out keyIndx - Record build side hash key attr number.
 * @out oKeyIndx - Record build side hash key origin attr number.
 * @in hashkeys - keyIndx, oKeyIndx should be allocated by caller.
 */
void SonicRadixHashJoin::setHashIndex(uint16* keyIndx, uint16* oKeyIndx, List* hashKeys)
{
    int i = 0;
    ListCell* lc = NULL;
    ExprState* expr_state = NULL;
    Var* variable = NULL;

    foreach (lc, hashKeys) {
        expr_state = (ExprState*)lfirst(lc);
        if (IsA(expr_state->expr, Var)) {
            variable = (Var*)expr_state->expr;
        } else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType* rel_type = (RelabelType*)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var*)rel_type->arg)->varattno > 0) {
                variable = (Var*)((RelabelType*)expr_state->expr)->arg;
            } else {
                m_complicatekey = true;
                break;
            }
        } else {
            m_complicatekey = true;
            break;
        }

        keyIndx[i] = variable->varattno - 1;
        oKeyIndx[i] = variable->varoattno - 1;
        m_integertype[i] = (unsigned int)(m_integertype[i]) & (unsigned int)integerType(variable->vartype);
        i++;
    }
}

/*
 * @Description: Initial memory control information.
 */
void SonicRadixHashJoin::initMemoryControl()
{
    VecHashJoin* node = (VecHashJoin*)m_runtime->js.ps.plan;

    m_memControl.totalMem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop) * 1024L;
    m_memControl.totalMem += GetAvailRackMemory(((Plan*)node)->dop) * 1024L;
    if (((Plan*)node)->operatorMaxMem > 0) {
        m_memControl.maxMem = SET_NODEMEM(((Plan*)node)->operatorMaxMem, ((Plan*)node)->dop) * 1024L;
    }

    elog(DEBUG2,
        "SonicRadixHashJoinTbl[%d]: operator memory uses %dKB",
        ((Plan*)node)->plan_node_id,
        (int)(m_memControl.totalMem / 1024L));
}

/*
 * @Description: Initial hash functions info from m_runtime.
 * 	Should be under hashjoin hashContext.
 */
void SonicRadixHashJoin::initHashFmgr()
{
    ListCell* lc = NULL;
    int i = 0;
    m_buildOp.hashFmgr = (FmgrInfo*)palloc(sizeof(FmgrInfo) * m_buildOp.keyNum);
    m_probeOp.hashFmgr = (FmgrInfo*)palloc(sizeof(FmgrInfo) * m_probeOp.keyNum);
    foreach (lc, m_runtime->hj_HashOperators) {
        Oid hashop = lfirst_oid(lc);
        Oid left_hashfn;
        Oid right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("could not find hash function for hash operator %u", hashop)));
        }
        fmgr_info(left_hashfn, &m_probeOp.hashFmgr[i]);
        fmgr_info(right_hashfn, &m_buildOp.hashFmgr[i]);
        i++;
    }

    if (u_sess->attr.attr_sql.enable_fast_numeric) {
        replace_numeric_hash_to_bi(i, m_probeOp.hashFmgr);
        replace_numeric_hash_to_bi(i, m_buildOp.hashFmgr);
    }
}

/*
 * @Description: initialize one partition,
 * 	for SonicHashMemPartition, it will init the m_data per col,
 * 	for SonicHashFilePartition, it will init the m_file per file.
 * @in partition - partition need to be initialized.
 */
template <bool isInner>
void SonicRadixHashJoin::initPartition(SonicHashPartition* partition)
{

    MemoryContext old_cxt = MemoryContextSwitchTo(partition->m_context);
    DatumDesc desc;
    FormData_pg_attribute* attrs = NULL;
    SonicHashInputOpAttr* attr_op = NULL;
    if (isInner) {
        attrs = m_buildOp.tupleDesc->attrs;
        attr_op = &m_buildOp;
    } else {
        attrs = m_probeOp.tupleDesc->attrs;
        attr_op = &m_probeOp;
    }

    /* Shared readers need stable Datums. Compressed columns can decode into
     * mutable per-array buffers, so store encoded values as owned Datums. */
    const bool sharedBuild = isInner && m_radixSharedBuildContext != NULL;
    for (int idx = 0; idx < attr_op->cols; idx++) {
        bool doNotCompress = sharedBuild ||
            (!m_complicatekey && isHashKey(attrs[idx].atttypid, idx, attr_op->keyIndx, attr_op->keyNum));
        getDataDesc(&desc, 0, &attrs[idx], doNotCompress);
        partition->init(idx, &desc);
    }

    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: try to load as many inner partitions as possible from files.
 * 	This function is called during GRACE_HASH,
 * 	It sorts the partitions' size,
 * 	and load as many partitions as possible from the disk,
 * 	to do join in memory.
 * 	So that, few partitions are remained to probe.
 * @in memorySize - The total size of partitions should be less than it.
 */
void SonicRadixHashJoin::loadInnerPartitions(uint64 memorySize)
{
    SonicHashMemPartition* mem_partition = NULL;
    sortPartitionSize(memorySize);

    /* Some partition can be loaded into memory, init a memory partition to load the data. */
    if (m_partLoadedOffset >= 0) {
        AutoContextSwitch memSwitch(m_memControl.hashContext);

        mem_partition = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem, m_atomSize);
        initPartition<true>(mem_partition);
    }

    Assert(m_partLoadedOffset >= -1);
    for (uint32 idx = 0; idx < (uint32)(m_partLoadedOffset + 1); ++idx) {
        loadMultiInnerPartition(m_partSizeOrderedIdx[idx], mem_partition);
    }

    /* finish loaded, now save to change tht pointer */
    if (mem_partition != NULL) {
        m_innerPartitions[m_partSizeOrderedIdx[0]] = mem_partition;
    }
}

/*
 * @Description: Load as many partition as possible.
 * @in partIdx - partition index.
 * @in memPartition - store data from file partition to it.
 */
void SonicRadixHashJoin::loadMultiInnerPartition(uint32 partIdx, SonicHashMemPartition* memPartition)
{
    Assert(m_memPartFlag[partIdx]);

    int64 nrows = 0;
    SonicHashFilePartition* file_partition = NULL;

    file_partition = (SonicHashFilePartition*)m_innerPartitions[partIdx];
    Assert(file_partition != NULL);
    Assert(file_partition->m_status == partitionStatusFile);
    nrows = file_partition->m_rows;

    /* If this partition has nothing, release the file handler buffer and handler and return. */
    if (nrows == 0) {
        Assert(file_partition->m_size == 0);
        file_partition->releaseFileHandlerBuffer();
        file_partition->closeFiles();
        file_partition->m_status = partitionStatusFinish;
        return;
    }

    file_partition->prepareFileHandlerBuffer();
    file_partition->rewindFiles();

    /* Load data from file and put them into SonicDatumArray. */
    memPartition->loadPartition((void*)file_partition);
    if (m_complicatekey) {
        /* Load hash value from file if m_complicatekey is true. */
        memPartition->loadHash((void*)file_partition);
    }
    /* Data of the file partition has been loaded, save to free the file partition resource */
    m_innerPartitions[partIdx]->freeResources();
    m_innerPartitions[partIdx] = NULL;
}

/*
 * @Description: load the partIdx-th partition.
 * @in partIdx - partition index.
 */
void SonicRadixHashJoin::loadInnerPartition(uint32 partIdx)
{
    int64 nrows = 0;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition* memPartition = NULL;

    file_partition = (SonicHashFilePartition*)m_innerPartitions[partIdx];
    Assert(file_partition != NULL);
    Assert(file_partition->m_status == partitionStatusFile);

    nrows = file_partition->m_rows;
    /* If this partition has nothing, release the file handler buffer and handler and return */
    if (nrows == 0) {
        Assert(file_partition->m_size == 0);
        file_partition->releaseFileHandlerBuffer();
        file_partition->closeFiles();
        file_partition->m_status = partitionStatusFinish;
        return;
    }

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        memPartition = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem, m_atomSize);
        initPartition<true>(memPartition);

        file_partition->prepareFileHandlerBuffer();
        file_partition->rewindFiles();
    }

    /* Load data from file and put them into SonicDatumArray. */
    ((SonicHashMemPartition*)memPartition)->loadPartition((void*)file_partition);

    /* Load hash value from file if m_complicatekey is true. */
    if (m_complicatekey) {
        ((SonicHashMemPartition*)memPartition)->loadHash((void*)file_partition);
    }

    /*
     * Release file partition resource and
     * make it pointer to memory partition constructed above.
     */
    m_innerPartitions[partIdx]->freeResources();
    m_innerPartitions[partIdx] = memPartition;
}

/*
 * @Description: calculate hashtable head size (m_bucket + m_next).
 * @in rows - rows in memory.
 */
uint64 SonicRadixHashJoin::get_hash_head_size(int64 rows)
{
    int64 hash_size = (int64)calcHashSize(rows);
    int64 sz_hash = Max(hash_size, rows + 1);
    int byte_size = 4;
    uint64 hash_head_size = 0;
    if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
        byte_size = 2;
    }

    bool use_hash_table = (sz_hash * byte_size >= (int64)MaxAllocSize);

    if (!use_hash_table) {
        hash_head_size = (hash_size + rows + 1) * byte_size;
    } else {
        uint64 numSegBucket = (hash_size - 1) / INIT_DATUM_ARRAY_SIZE + 1;
        uint64 numSegNext = rows / INIT_DATUM_ARRAY_SIZE + 1;

        hash_head_size = sizeof(atom*) * INIT_ARR_CONTAINER_SIZE * 2 +
                         INIT_DATUM_ARRAY_SIZE * byte_size * (numSegBucket + numSegNext);
    }

    return hash_head_size;
}

/*
 * @Description: Build side main function.
 */
void SonicRadixHashJoin::Build()
{
    if (m_radixMode) {
        if (!trySharedRadixBuild()) {
#ifdef ENABLE_HTAP
            PlanState* inner = innerPlanState(m_runtime);
            if (IsA(inner, VecStreamState)) {
                StreamSharedContext* context = ((StreamState*)inner)->sharedContext;
                if (context != NULL && context->radixBroadcastReceiverDomains != NULL)
                    ereport(ERROR, (errmsg("Preselected radix broadcast requires a shared hash build")));
            }
#endif
            BuildRadix();
        }
        return;
    }

    PlanState* inner_node = innerPlanState(m_runtime);
    VectorBatch* batch = NULL;
    instr_time start_time;

    for (;;) {
        batch = VectorEngine(inner_node);
        if (unlikely(BatchIsNull(batch))) {
            if (m_strategy == MEMORY_HASH) {
                (void)INSTR_TIME_SET_CURRENT(start_time);
                uint64 hash_head_size = get_hash_head_size(m_rows);
                judgeMemoryOverflow(hash_head_size);

                if (!hasEnoughMem()) {
                    /*
                     * If the total number of rows reaches SONIC_MAX_ROWS,
                     * spill the data into disk.
                     * Because the hash table can not handle this amount of data.
                     */
                    m_strategy = GRACE_HASH;
                    m_partNum = calcPartitionNum();

                    if (m_complicatekey) {
                        flushToDisk<true>();
                    } else {
                        flushToDisk<false>();
                    }
                    pgstat_increase_session_spill();
                }
                m_build_time += elapsed_time(&start_time);
            }
            break;
        }

        (void)INSTR_TIME_SET_CURRENT(start_time);

        RuntimeBinding(m_funBuild, m_strategy)(batch);

        m_rows += batch->m_rows;
        m_build_time += elapsed_time(&start_time);
    }

    if (m_strategy == GRACE_HASH) {
        /*
         * Done spilling build side,
         * thus record the partition debug info here before loading them.
         */
        HASH_BASED_DEBUG(recordPartitionInfo(true, -1, 0, m_partNum));

        /* release all the temp files in all the inner partitions */
        releaseAllFileHandlerBuffer(true);
    }

    /*
     * Done obtaining build side data,
     * record spill related information here cause them may be loaded into memory later.
     */
    reportSorthashinfo(reportTypeBuild, m_partNum);

    pushDownFilterIfNeed();

    /* prepareProbe, also record the build time and profile */
    prepareProbe();

    /*
     * Done building hash table for build side,
     * record memory and time related information here.
     */
    if (HAS_INSTR(&m_runtime->js, true)) {
        INSTR->sysBusy = m_memControl.sysBusy;
        INSTR->spreadNum = m_memControl.spreadNum;
        INSTR->sorthashinfo.hashbuild_time = m_build_time;
        INSTR->sorthashinfo.spaceUsed = m_memControl.allocatedMem - m_memControl.availMem;
    }
}

/*
 * @Description: Build side of the radix hash join mode.
 * 	The build side is always split into cache-sized radix partitions, even
 * 	when the whole build side fits in memory. Each partition gets its own
 * 	chained hash table, so build and probe of one partition touch a small
 * 	working set instead of one global hash table.
 */
namespace {
struct SharedRadixTable {
    uint32 partitions;
    int64 rows;
    SonicHashPartition** tables;
    uint64 recentProbePart;
    uint64* partitionBytes;
    uint32* activeProbes;
};

// The low 16 bits hold the partition. Keep the freshness interval short
// relative to the lifetime of a probe tile, and never wait for a hint.
const uint64 sharedProbeHintUs = 100;
#ifdef ENABLE_HTAP
struct alignas(128) RadixActiveDomain {
    uint32 distinct;
};
static RadixActiveDomain radixActiveDomains[CPU_SETSIZE];

bool trackSharedRadixProbe()
{
    const auto& settings = u_sess->attr.attr_sql;
    return settings.enable_radix_shared_probe_count || settings.enable_cache_trace;
}
#endif
}

bool SonicRadixHashJoin::trySharedRadixBuild()
{
#ifdef ENABLE_HTAP
    if (!u_sess->attr.attr_sql.radix_shared_build || m_radixTriedSharedBuild || IsThreadProcessStreamRecursive())
        return false;
    m_radixTriedSharedBuild = true;
    PlanState* inner = innerPlanState(m_runtime);
    Plan* plan = m_runtime->js.ps.plan;
    if (!IsA(inner, VecStreamState) || plan->dop <= 1 || plan->ispwj || plan->allParam != NULL ||
        inner->plan->allParam != NULL || plan->recursive_union_plan_nodeid > 0 ||
        ((VecHashJoin*)plan)->rebuildHashTable || m_complicatekey || m_buildOp.keyNum == 0)
        return false;
    if (PLAN_LOCAL_ROWS(inner->plan) / SET_DOP(inner->plan->dop) < 4 * BatchMaxSize)
        return false;
    StreamState* stream = (StreamState*)inner;
    StreamSharedContext* context = stream->sharedContext;
    if (context == NULL || context->radixBuildSlots == NULL || stream->consumer == NULL)
        return false;

    cpu_set_t affinity;
    int domain = -1;
    if (sched_getaffinity(0, sizeof(affinity), &affinity) != 0 ||
        RhjCache(sched_getcpu(), &domain) == 0 || domain < 0 || domain >= CPU_SETSIZE)
        return false;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &affinity))
            continue;
        int cpuDomain = -1;
        if (RhjCache(cpu, &cpuDomain) == 0 || cpuDomain != domain)
            return false;
    }

    StreamRadixBuildSlot* slot = &context->radixBuildSlots[domain];
    int receiver = 0;
    if (context->radixBroadcastReceiverDomains != NULL) {
        receiver = context->radixBroadcastReceiverDomains[u_sess->stream_cxt.smp_id];
        if (abs(receiver) != domain + 1)
            ereport(ERROR, (errmsg("Preselected radix broadcast receiver is in an unexpected LLC domain")));
    }
    uint32 unclaimed = RADIX_BUILD_UNCLAIMED;
    if (receiver >= 0 && __atomic_compare_exchange_n(&slot->state, &unclaimed, RADIX_BUILD_RUNNING, false,
            __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
        if (context->cacheProfileChannels != NULL)
            ereport(LOG, (errmsg("rhj_broadcast_consumer query_id=%lu stream_id=%u consumer=%d role=builder "
                "llc=%d time_ns=%lu", u_sess->debug_query_id, context->key_s.planNodeId,
                (int)u_sess->stream_cxt.smp_id, domain, RhjNow()), errhidestmt(true), ErrOutToClient(false)));
        PG_TRY();
        {
            m_radixSharedBuildContext = AllocSetContextCreate(context->localStreamMemoryCtx, "radixSharedBuild",
                ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE, SHARED_CONTEXT);
            BuildRadix();
            SharedRadixTable* table = (SharedRadixTable*)MemoryContextAlloc(m_radixSharedBuildContext, sizeof(*table));
            table->partitions = m_partNum;
            table->rows = m_rows;
            table->recentProbePart = 0;
            m_radixRecentPart = &table->recentProbePart;
            table->partitionBytes = NULL;
            if (m_partNum > 1 && (u_sess->attr.attr_sql.enable_radix_shared_probe_locality ||
                u_sess->attr.attr_sql.imcs_chiplet_probe_tile_policy != 0)) {
                table->partitionBytes = (uint64*)MemoryContextAllocZero(m_radixSharedBuildContext,
                    m_partNum * sizeof(*table->partitionBytes));
                for (uint32 p = 0; p < m_partNum; ++p) {
                    if (m_innerPartitions[p] == NULL || m_innerPartitions[p]->m_rows == 0)
                        continue;
                    uint64 allocated = 0, available = 0;
                    calcHashContextSize(m_innerPartitions[p]->m_context, &allocated, &available);
                    table->partitionBytes[p] = allocated - available;
                }
            }
            m_radixSharedPartitionBytes = table->partitionBytes;
            m_radixSharedDomain = domain;
            configureChipletProbeBudget();
            table->activeProbes = NULL;
            if (m_partNum > 1 && m_runtime->js.jointype == JOIN_INNER && u_sess->attr.attr_sql.enable_imcs_nps &&
                !u_sess->attr.attr_sql.enable_imcs_adapt && trackSharedRadixProbe()) {
                table->activeProbes = (uint32*)MemoryContextAllocZero(m_radixSharedBuildContext,
                    m_partNum * sizeof(*table->activeProbes));
            }
            m_radixActiveProbes = table->activeProbes;
            table->tables = (SonicHashPartition**)MemoryContextAlloc(m_radixSharedBuildContext,
                m_partNum * sizeof(*table->tables));
            for (uint32 p = 0; p < m_partNum; ++p)
                table->tables[p] = m_innerPartitions[p];
            __atomic_store_n(&slot->table, table, __ATOMIC_RELEASE);
        }
        PG_CATCH();
        {
            __atomic_store_n(&slot->state, RADIX_BUILD_FAILED, __ATOMIC_RELEASE);
            PG_RE_THROW();
        }
        PG_END_TRY();
        ereport(LOG, (errmsg("RHJ shared build node=%d llc_domain=%d rows=%lld parts=%u",
            plan->plan_node_id, domain, (long long)m_rows, m_partNum)));
        return true;
    }
    if (receiver > 0)
        ereport(ERROR, (errmsg("Preselected radix broadcast builder already claimed its LLC domain")));

    /* The elected builder retains its channel and receives every producer's
     * rows. Close this worker's redundant broadcast channels using the
     * normal local Stream early close path, which wakes quota waiters. */
    instr_time start;
    (void)INSTR_TIME_SET_CURRENT(start);
    if (receiver == 0)
        gs_memory_close_conn(context, stream->consumer->getConnNum(), u_sess->stream_cxt.smp_id);
    if (context->cacheProfileChannels != NULL)
        ereport(LOG, (errmsg("rhj_broadcast_consumer query_id=%lu stream_id=%u consumer=%d role=follower "
            "llc=%d time_ns=%lu", u_sess->debug_query_id, context->key_s.planNodeId,
            (int)u_sess->stream_cxt.smp_id, domain, RhjNow()), errhidestmt(true), ErrOutToClient(false)));
    SharedRadixTable* table;
    while ((table = (SharedRadixTable*)__atomic_load_n(&slot->table, __ATOMIC_ACQUIRE)) == NULL) {
        CHECK_FOR_INTERRUPTS();
        if (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) == RADIX_BUILD_FAILED)
            ereport(ERROR, (errmsg("Shared radix hash table build failed")));
#ifndef ENABLE_MULTIPLE_NODES
        if (u_sess->stream_cxt.global_obj != NULL &&
            u_sess->stream_cxt.global_obj->GetStreamQuitStatus() == STREAM_ERROR)
            ereport(ERROR, (errmsg("Stream failed while waiting for a shared radix build")));
#endif
        pg_usleep(1000);
    }
    m_radixBuildInputTime += elapsed_time(&start);
    m_innerPartitions[0]->freeResources();
    pfree(m_innerPartitions[0]);
    pfree(m_innerPartitions);
    m_innerPartitions = table->tables;
    m_radixRecentPart = &table->recentProbePart;
    m_radixSharedPartitionBytes = table->partitionBytes;
    m_radixSharedDomain = domain;
    m_radixActiveProbes = table->activeProbes;
    m_partNum = table->partitions;
    m_rows = table->rows;
    configureChipletProbeBudget();
    initRadixProbeBuffers();
    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, false>;
    m_probeIdx = 0;
    m_probeStatus = PROBE_FETCH;
    m_runtime->joinState = HASH_PROBE;
    ereport(LOG, (errmsg("RHJ shared reuse node=%d llc_domain=%d rows=%lld parts=%u input_channel_closed=1",
        plan->plan_node_id, domain, (long long)m_rows, m_partNum)));
    return true;
#else
    return false;
#endif
}

void SonicRadixHashJoin::initRadixProbeBuffers()
{
    if (m_partNum == 1)
        return;
    AutoContextSwitch memSwitch(m_memControl.hashContext);
    m_radixRowIdx = (uint32*)palloc(sizeof(uint32) * RHJ_PROBE_TILE_ROWS);
    m_radixPartStart = (uint32*)palloc(sizeof(uint32) * (m_partNum + 1));
    m_radixCounts = (uint32*)palloc(sizeof(uint32) * m_partNum);
    m_radixCursor = (uint32*)palloc(sizeof(uint32) * m_partNum);
    m_radixOuterHash = (uint32*)palloc(sizeof(uint32) * RHJ_PROBE_TILE_ROWS);
    m_radixProbeTileBatches = (VectorBatch**)palloc0(sizeof(VectorBatch*) * RHJ_PROBE_TILE_BATCHES);
    m_radixProbeBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
#ifdef ENABLE_HTAP
    if (m_profileProbeCache)
        m_partitionLookupCache = (ProbeCacheStats*)palloc0(m_partNum * sizeof(*m_partitionLookupCache));
#endif
}

void SonicRadixHashJoin::BuildRadix()
{
    PlanState* inner_node = innerPlanState(m_runtime);
    VectorBatch* batch = NULL;
    instr_time start_time;
    VecHashJoin* node = (VecHashJoin*)m_runtime->js.ps.plan;

    struct BufferedInput { MemoryContext context; VectorBatch* batch; uint64 allocated, available; };
    List* buffered = NIL;
    uint64 cacheBytes = (uint64)m_llcMB * 1024 * 1024;
    m_workersPerChiplet = 1;
    uint64 inputRows = 0;
    BufferedInput* pending = NULL;
    bool inputComplete = false;
    const uint64 pilotRowsLimit = 32 * BatchMaxSize;
    const uint64 pilotBytesLimit = 4 * 1024 * 1024;
    m_radixInputContext = AllocSetContextCreate(m_memControl.hashContext,
        "radixBufferedInputs", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
    m_radixInputAllocated = m_radixInputAvailable = 0;
    // Observe actual concurrent join workers during collection, before final
    // radix tables are chosen. Never wait on an all-worker startup barrier.
    for (;;) {
        instr_time inputStart;
        (void)INSTR_TIME_SET_CURRENT(inputStart);
        batch = VectorEngine(inner_node);
        m_radixBuildInputTime += elapsed_time(&inputStart);
        if (BatchIsNull(batch)) {
            inputComplete = true;
            break;
        }
        (void)INSTR_TIME_SET_CURRENT(start_time);
        CHECK_FOR_INTERRUPTS();
        if (m_chipletAware && m_radixSharedBuildContext == NULL)
            m_workersPerChiplet = Max(m_workersPerChiplet, RhjObservedWorkers(&m_runtime->js.ps, &cacheBytes));
        checkRadixMemory();
        /* Stream batches can contain only a few rows. Pack them into full
         * vectors instead of reserving BatchMaxSize slots for every incoming
         * message. Deep-copy varlena values before advancing the source. */
        int sourceRow = 0;
        while (sourceRow < batch->m_rows) {
            if (pending == NULL || pending->batch->m_rows == BatchMaxSize) {
                MemoryContext copyContext = AllocSetContextCreate(m_radixInputContext,
                    "radixBufferedInput", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
                AutoContextSwitch switchContext(m_memControl.hashContext);
                pending = (BufferedInput*)palloc(sizeof(BufferedInput));
                pending->context = copyContext;
                pending->allocated = pending->available = 0;
                pending->batch = New(copyContext) VectorBatch(copyContext, m_buildOp.tupleDesc);
                buffered = lappend(buffered, pending);
            }
            int copyEnd = Min(batch->m_rows, sourceRow + BatchMaxSize - pending->batch->m_rows);
            {
                AutoContextSwitch switchContext(pending->context);
                pending->batch->Copy<true, true>(batch, sourceRow, copyEnd);
            }
            uint64 allocated = 0, available = 0;
            calcHashContextSize(pending->context, &allocated, &available);
            m_radixInputAllocated = m_radixInputAllocated - pending->allocated + allocated;
            m_radixInputAvailable = m_radixInputAvailable - pending->available + available;
            pending->allocated = allocated;
            pending->available = available;
            sourceRow = copyEnd;
        }
        inputRows += batch->m_rows;
        checkRadixMemory();
        m_build_time += elapsed_time(&start_time);
        if (inputRows >= pilotRowsLimit || m_radixInputAllocated - m_radixInputAvailable >= pilotBytesLimit)
            break;
    }
    uint64 pilotRows = inputRows;
    if (m_radixSharedBuildContext != NULL) {
        int domain = -1;
        uint64 domainBytes = RhjCache(sched_getcpu(), &domain);
        if (domainBytes != 0)
            cacheBytes = domainBytes;
    } else if (m_chipletAware) {
        m_workersPerChiplet = Max(m_workersPerChiplet, RhjObservedWorkers(&m_runtime->js.ps, &cacheBytes));
    }
    checkRadixMemory();
    /* One independent full walk validates incremental charges before layout
     * selection; retain the strict worker budget without repeated full walks. */
    uint64 verifiedAllocated = 0, verifiedAvailable = 0;
    calcHashContextSize(m_memControl.hashContext, &verifiedAllocated, &verifiedAvailable, false);
    calcHashContextSize(m_radixSharedBuildContext, &verifiedAllocated, &verifiedAvailable, false);
    if (verifiedAllocated != m_memControl.allocatedMem || verifiedAvailable != m_memControl.availMem)
        ereport(ERROR, (errmsg("RHJ buffered memory accounting mismatch")));
    uint64 actualBytes = m_memControl.allocatedMem - m_memControl.availMem;
    int64 target_bytes = (int64)m_radixTargetMB * 1024 * 1024;
    if (m_radixSharedBuildContext != NULL)
        target_bytes = Min(target_bytes, (int64)(cacheBytes / 2));
    else if (m_chipletAware)
        target_bytes = Min(target_bytes, Max((int64)65536, (int64)(cacheBytes / m_workersPerChiplet / 2)));
    double plannedRows = PLAN_LOCAL_ROWS(innerPlan(node)) / SET_DOP(innerPlan(node)->dop);
    if (!std::isfinite(plannedRows) || plannedRows < (double)inputRows)
        plannedRows = (double)inputRows;
    uint64 estimatedRows = inputComplete ? inputRows : (uint64)std::ceil(Min(plannedRows,
        (double)SONIC_MAX_ROWS * SONIC_PART_MAX_NUM));
    double bytesPerRow = inputRows ? (double)actualBytes / inputRows + 12.0 : 0;
    double estimatedBytes = (m_cacheAware || m_chipletAware) ? bytesPerRow * estimatedRows :
        (double)estimatedRows * Max(1, innerPlan(node)->plan_width);
    if (m_radixSharedBuildContext != NULL)
        estimatedBytes = (double)estimatedRows * (Max(1, innerPlan(node)->plan_width) + 12);
    uint64 build_bytes = (uint64)Min(estimatedBytes, (double)PG_INT64_MAX);
    uint32 part_num = 1;
    while (part_num < SONIC_PART_MAX_NUM && (uint64)part_num * target_bytes < build_bytes) part_num <<= 1;
    uint32 requestedParts = part_num;
    /* Very fine cache partitions fragment a vector into tiny virtual calls.
     * Preserve roughly sixteen rows per destination in a full input batch.
     * The cache budget is a target; exceeding it remains explicitly counted. */
    uint32 vectorLimit = 1;
    if (m_chipletAware && m_radixSharedBuildContext == NULL) {
        /* Keep enough build rows in each partition to amortize a separate
         * hash table and the per-partition probe calls. The LLC budget can be
         * unattainable at high DOP; do not sacrifice vector throughput for it. */
        const uint64 minPartitionRows = 32ULL * BatchMaxSize;
        while (vectorLimit < SONIC_PART_MAX_NUM / 2 &&
            (uint64)(vectorLimit * 2) * minPartitionRows <= estimatedRows)
            vectorLimit <<= 1;
    } else {
        while (vectorLimit < SONIC_PART_MAX_NUM && vectorLimit * 16 < BatchMaxSize)
            vectorLimit <<= 1;
    }
    int64 chipletBudgetBytes = m_chipletAware ? target_bytes : 0;
    bool chipletBudgetFeasible = true;
    if (m_chipletAware && m_radixSharedBuildContext == NULL && estimatedBytes > (double)target_bytes * vectorLimit) {
        /* Even the finest useful partition cannot satisfy the observed LLC
         * share. Use ordinary radix sizing instead of paying for many tiny
         * partitions that still miss the cache target. */
        chipletBudgetFeasible = false;
        target_bytes = (int64)m_radixTargetMB * 1024 * 1024;
        estimatedBytes = (double)estimatedRows * Max(1, innerPlan(node)->plan_width);
        build_bytes = (uint64)Min(estimatedBytes, (double)PG_INT64_MAX);
        part_num = 1;
        while (part_num < SONIC_PART_MAX_NUM && (uint64)part_num * target_bytes < build_bytes)
            part_num <<= 1;
        requestedParts = part_num;
        vectorLimit = 1;
        while (vectorLimit < SONIC_PART_MAX_NUM && vectorLimit * 16 < BatchMaxSize)
            vectorLimit <<= 1;
    }
    part_num = Min(part_num, vectorLimit);
    m_rows = inputRows;
    m_innerPartitions[0]->freeResources();
    pfree(m_innerPartitions);
    m_partNum = Max((uint32)1, part_num);
    m_radixHasBatch = false;
    m_radixPart = 0;
    m_radixPartInProgress = false;

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        m_innerPartitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*) * m_partNum);
    }
    initRadixProbeBuffers();

    ListCell* bufferedCell = NULL;
    foreach (bufferedCell, buffered) {
        BufferedInput* copy = (BufferedInput*)lfirst(bufferedCell);
        batch = copy->batch;
        (void)INSTR_TIME_SET_CURRENT(start_time);
        CHECK_FOR_INTERRUPTS();
        saveToRadixMemory(batch);
        double part_time = elapsed_time(&start_time);
        m_partition_time += part_time;
        m_build_time += part_time;
        m_radixInputAllocated -= copy->allocated;
        m_radixInputAvailable -= copy->available;
        MemoryContextDelete(OperatorDataAccessReleaseContext(copy->context));
        pfree(copy);
        checkRadixMemory();
    }
    list_free(buffered);
    Assert(m_radixInputAllocated == 0 && m_radixInputAvailable == 0);
    MemoryContextDelete(OperatorDataAccessReleaseContext(m_radixInputContext));
    m_radixInputContext = NULL;
    /* The pilot established the layout. Stream remaining batches directly
     * into owned Sonic partitions, avoiding full-input vector materialization.
     * Planning errors can miss the cache target, never alter row semantics. */
    while (!inputComplete) {
        instr_time inputStart;
        (void)INSTR_TIME_SET_CURRENT(inputStart);
        batch = VectorEngine(inner_node);
        m_radixBuildInputTime += elapsed_time(&inputStart);
        if (BatchIsNull(batch)) break;
        (void)INSTR_TIME_SET_CURRENT(start_time);
        CHECK_FOR_INTERRUPTS();
        saveToRadixMemory(batch);
        inputRows += batch->m_rows;
        m_rows = inputRows;
        checkRadixMemory();
        m_build_time += elapsed_time(&start_time);
    }
    ereport(LOG, (errmsg("RHJ effective=radix node=%d rows=%llu parts=%u cache_aware=%d chiplet_aware=%d llc_bytes=%llu observed_join_cpus=%d target_bytes=%lld chiplet_budget_bytes=%lld chiplet_budget_feasible=%d observation_window_ms=50 pilot_rows=%llu estimated_rows=%llu requested_parts=%u vector_limit_parts=%u streaming_build=1",
        m_runtime->js.ps.plan->plan_node_id, (unsigned long long)inputRows, m_partNum, m_cacheAware, m_chipletAware,
        (unsigned long long)cacheBytes, m_workersPerChiplet, (long long)target_bytes,
        (long long)chipletBudgetBytes, chipletBudgetFeasible,
        (unsigned long long)pilotRows, (unsigned long long)estimatedRows, requestedParts, vectorLimit)));

    /* Build one small hash table per radix partition. */
    (void)INSTR_TIME_SET_CURRENT(start_time);
    for (uint32 p = 0; p < m_partNum; ++p) {
        SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[p];
        if (mem_partition == NULL || mem_partition->m_rows == 0) {
            continue;
        }

        if (mem_partition->m_rows > SONIC_MAX_ROWS)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("radix partition exceeds Sonic row address limit")));
        m_hashSize = (int64)calcHashSize(mem_partition->m_rows);
        m_bucketTypeSize = 4;
        checkRadixMemory();
        uint64 table_bytes = 4 * ((uint64)m_hashSize + mem_partition->m_rows + 1);
        if (table_bytes > m_memControl.totalMem ||
            m_memControl.allocatedMem > m_memControl.totalMem - table_bytes)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("radix partition hash table exceeds worker memory budget"),
                errhint("Disable enable_radix_hashjoin to use spilling.")));
        initHashTable(4, p);
        /* This path deliberately uses contiguous 32-bit buckets. */
        if (mem_partition->m_segHashTable)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("radix partition needs a segmented hash table"),
                errhint("Disable enable_radix_hashjoin or increase work memory.")));
        if (m_complicatekey) {
            buildHashTable<uint32, true, false>(p);
        } else {
            buildHashTable<uint32, false, false>(p);
        }
    }
    m_build_time += elapsed_time(&start_time);
    uint64 largestPartition = 0;
    for (uint32 p = 0; p < m_partNum; ++p) {
        if (!m_innerPartitions[p]) continue;
        uint64 allocated = 0, available = 0;
        calcHashContextSize(m_innerPartitions[p]->m_context, &allocated, &available);
        largestPartition = Max(largestPartition, allocated - available);
    }
    ereport(LOG, (errmsg("RHJ working_set node=%d largest_partition_bytes=%llu target_bytes=%lld target_met=%d peak_bytes=%llu",
        m_runtime->js.ps.plan->plan_node_id, (unsigned long long)largestPartition, (long long)target_bytes,
        largestPartition <= (uint64)target_bytes, (unsigned long long)m_peakBytes)));

    __atomic_fetch_add(&rhjBuilds, 1, __ATOMIC_RELAXED);
    if (m_cacheAware) __atomic_fetch_add(&rhjCacheBuilds, 1, __ATOMIC_RELAXED);
    if (m_chipletAware) __atomic_fetch_add(&rhjChipletBuilds, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&rhjPartitions, m_partNum, __ATOMIC_RELAXED);
    if (largestPartition > (uint64)target_bytes) __atomic_fetch_add(&rhjTargetMisses, 1, __ATOMIC_RELAXED);

    /* The legacy bloom builder sees partition zero only. Publishing that
     * filter here would discard matches in other partitions. Keep it disabled. */
    checkRadixMemory();
    m_runtime->joinState = HASH_PROBE;

    if (m_complicatekey) {
        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, true, false>;
    } else {
        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, false>;
    }
    m_probeIdx = 0;
    m_probeStatus = PROBE_FETCH;

    if (HAS_INSTR(&m_runtime->js, true)) {
        INSTR->sorthashinfo.hashbuild_time = m_build_time;
        INSTR->sorthashinfo.spaceUsed = m_memControl.allocatedMem - m_memControl.availMem;
        INSTR->sorthashinfo.hash_writefile = false;
        INSTR->sorthashinfo.hash_FileNum = 0;
        INSTR->sorthashinfo.hash_spillNum = 0;
        INSTR->sorthashinfo.hash_partNum = m_partNum;
    }
}

/*
 * @Description: Put one batch into the radix build partitions.
 * @in batch - build side batch.
 */
void SonicRadixHashJoin::saveToRadixMemory(VectorBatch* batch)
{
    int nrows = batch->m_rows;
    if (nrows <= 0) {
        return;
    }

    if (m_complicatekey) {
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
    } else {
        hashBatchArray(batch, (void*)m_buildOp.hashFunc, m_buildOp.hashFmgr, m_buildOp.keyIndx, m_hashVal);
    }

    calcPartIdx(m_hashVal, m_partNum, nrows);

    for (int i = 0; i < nrows; ++i) {
        uint32 part = m_partIdx[i];
        if (m_innerPartitions[part] != NULL) continue;
        checkRadixMemory();
        uint64 bootstrap = m_arrayExpandSize + 65536;
        if (m_memControl.allocatedMem > m_memControl.totalMem || bootstrap > m_memControl.totalMem - m_memControl.allocatedMem)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("RHJ partition array bootstrap exceeds worker budget"),
                errhint("Disable radix join for this query; no silent Sonic fallback is used.")));
        {
            bool shared = m_radixSharedBuildContext != NULL;
            AutoContextSwitch switchContext(shared ? m_radixSharedBuildContext : m_memControl.hashContext);
            m_innerPartitions[part] = New(CurrentMemoryContext) SonicHashMemPartition(
                "radixInnerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem,
                m_atomSize, shared);
            initPartition<true>(m_innerPartitions[part]);
        }
    }
    SonicHashMemPartition** mem_parts = (SonicHashMemPartition**)palloc(sizeof(SonicHashMemPartition*) * m_partNum);
    for (uint32 p = 0; p < m_partNum; ++p) {
        mem_parts[p] = (SonicHashMemPartition*)m_innerPartitions[p];
    }

    SonicHashMemPartition::putBatchByPartition(
        mem_parts, m_partNum, batch, m_partIdx, m_complicatekey ? m_hashVal : NULL);

    pfree(mem_parts);
}

#ifdef ENABLE_HTAP
void SonicRadixHashJoin::configureChipletProbeBudget()
{
    const auto& sql = u_sess->attr.attr_sql;
    if (sql.imcs_chiplet_probe_tile_policy == 0 || !sql.enable_imcs_nps || sql.enable_imcs_adapt ||
        sql.enable_imcs_stream_consumer_placement || m_radixSharedPartitionBytes == NULL ||
        m_runtime->js.jointype != JOIN_INNER || operator_data_access_active)
        return;

    uint64 llcBytes = 0;
    unsigned lanes = 0;
    if (RhjLlcDomain(sched_getcpu(), &llcBytes, &lanes) != m_radixSharedDomain || lanes == 0)
        return;
    uint64 largestPartition = 0;
    for (uint32 part = 0; part < m_partNum; ++part)
        largestPartition = Max(largestPartition, m_radixSharedPartitionBytes[part]);
    if (largestPartition == 0)
        return;
    /* Charge the active shared partition once. Leave a quarter of the LLC
     * estimate for Stream buffers, probe scratch and other operators. */
    uint64 usable = llcBytes - llcBytes / 4;
    m_chipletProbeBudget.availableBytes = usable - Min(usable, largestPartition);
    m_chipletProbeBudget.lanes = lanes;
}

void SonicRadixHashJoin::trimRadixProbeSlots(uint32 keep, uint64 budget)
{
    for (uint32 i = RHJ_PROBE_TILE_BATCHES; i > 1; --i) {
        RadixProbeSlot& slot = m_radixReusableSlots[i - 1];
        if (slot.context == NULL)
            continue;
        /* A tile stops after admitting the vector that crosses its byte
         * target. Let that same boundary vector survive the next refill. */
        if (i <= keep && m_radixReusableUsed - slot.used <= budget)
            break;
        m_radixReusableUsed -= slot.used;
        MemoryContextDelete(OperatorDataAccessReleaseContext(slot.context));
        slot = {};
        m_radixProbeTileBatches[i - 1] = NULL;
        ++m_radixProbeReleasedSlots;
    }
}
#endif

/* Buffer a bounded sequence of probe vectors. Packed source vectors own all
 * varlena values until every partition and duplicate output is drained. The
 * byte target is checked at vector boundaries; the worker budget stays hard. */
void SonicRadixHashJoin::loadRadixProbeTile()
{
    m_radixProbeBatch->Reset();
    m_radixProbeTileRows = 0;
    uint64 tileBytes = RHJ_PROBE_TILE_BYTES;
    uint32 tileRows = RHJ_PROBE_TILE_ROWS;
    uint64 stagedBytes = 0;
#ifdef ENABLE_HTAP
    const int policy = u_sess->attr.attr_sql.imcs_chiplet_probe_tile_policy;
    if (policy >= 4 && m_chipletProbeBudget.lanes != 0 && m_radixProbeTileContext == NULL && m_radixReusableSlots == NULL) {
        m_radixReusableSlots = (RadixProbeSlot*)MemoryContextAllocZero(m_memControl.hashContext,
            RHJ_PROBE_TILE_BATCHES * sizeof(RadixProbeSlot));
    }
    bool reuse = m_radixReusableSlots != NULL;
    uint32 fixedRows = tileRows;
    uint64 aggregateBytes = 0;
    if (m_chipletProbeBudget.lanes != 0) {
        const uint64 minTileBytes = 256 * 1024;
        uint64 available = m_chipletProbeBudget.availableBytes;
        const uint64 fixedBytes = Min(tileBytes, Max(minTileBytes, available / m_chipletProbeBudget.lanes));
        if (policy == 2 || policy == 3 || policy == 5)
            aggregateBytes = ChipletCacheRecentAggregateBytes(m_radixSharedDomain);
        uint64 radixBytes = (policy == 3 || policy == 5) ?
            ChipletCacheRecentRadixBytes(m_radixSharedDomain, m_runtime->js.ps.plan->plan_node_id) : 0;
        uint64 peerBytes = aggregateBytes + radixBytes;
        available -= Min(available, peerBytes);
        uint64 perLane = available / m_chipletProbeBudget.lanes;
        uint64 remainingBytes = Min(fixedBytes, Max(minTileBytes, perLane));
        uint64 columns = m_probeOp.tupleDesc->natts;
        uint64 planWidth = Max(0, outerPlan(m_runtime->js.ps.plan)->plan_width);
        uint64 batchBytes = BatchMaxSize *
            (Max(planWidth, columns * (sizeof(ScalarValue) + sizeof(uint8))) + 2 * sizeof(uint32));
        fixedRows = BatchMaxSize * Min((uint64)RHJ_PROBE_TILE_BATCHES, Max(UINT64CONST(1), fixedBytes / batchBytes));
        tileRows = BatchMaxSize * Min((uint64)RHJ_PROBE_TILE_BATCHES, Max(UINT64CONST(1), remainingBytes / batchBytes));
        /* Quantize the estimate in full vectors, so a small peer sample does
         * not change the fetch size just because allocator blocks are coarse. */
        m_chipletProbeBudget.aggregateSeen += aggregateBytes != 0;
        m_chipletProbeBudget.radixSeen += radixBytes != 0;
        m_chipletProbeBudget.floorHits += peerBytes != 0 && perLane < minTileBytes;
        m_chipletProbeBudget.roundedAway += remainingBytes < fixedBytes && tileRows == fixedRows;
        tileBytes = tileRows < fixedRows ? remainingBytes : fixedBytes;
        if (policy == 3 || policy == 5) {
            if (policy == 5) {
                /* Keep most of the staging space when a peer becomes active.
                 * This amortizes the partition walk and keeps hot slots resident. */
                uint32 minRows = fixedRows - (fixedRows / BatchMaxSize / 4) * BatchMaxSize;
                tileRows = Max(tileRows, minRows);
            }
            bool pressure = tileRows < fixedRows;
            uint32 previousRows = m_chipletProbeBudget.lastRows == 0 ? fixedRows : m_chipletProbeBudget.lastRows;
            /* A single hot sample leaves the current tile size in place.
             * After the peer cools down, restore one vector at a time. */
            if (pressure && !m_chipletProbeBudget.previousPressure)
                tileRows = Max(tileRows, previousRows);
            if (!pressure && previousRows < fixedRows)
                tileRows = Min(fixedRows, previousRows + BatchMaxSize);
            tileBytes = tileRows < fixedRows ? Min(fixedBytes, batchBytes * (tileRows / BatchMaxSize)) : fixedBytes;
            m_chipletProbeBudget.lastRows = tileRows;
            m_chipletProbeBudget.previousPressure = pressure;
        }
    }
#else
    bool reuse = false;
#endif
    errno_t rc;
    if (!reuse || m_radixProbeTileContext == NULL) {
        if (m_radixProbeTileContext != NULL)
            MemoryContextDelete(OperatorDataAccessReleaseContext(m_radixProbeTileContext));
        m_radixProbeTileContext = AllocSetContextCreate(m_memControl.hashContext,
            "radixProbeTile", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
        rc = memset_s(m_radixProbeTileBatches, sizeof(VectorBatch*) * RHJ_PROBE_TILE_BATCHES,
            0, sizeof(VectorBatch*) * RHJ_PROBE_TILE_BATCHES);
        securec_check(rc, "\0", "\0");
    }
#ifdef ENABLE_HTAP
    if (reuse)
        trimRadixProbeSlots(tileRows / BatchMaxSize, tileBytes);
#endif
    while (m_radixProbeTileRows < tileRows && !m_radixProbeInputEnded) {
        if (m_radixProbePendingBatch == NULL || m_radixProbePendingRow >= (uint32)m_radixProbePendingBatch->m_rows) {
            instr_time inputStart;
            (void)INSTR_TIME_SET_CURRENT(inputStart);
            m_radixProbePendingBatch = m_hashOpPartition->getBatch();
            m_radixProbeInputTime += elapsed_time(&inputStart);
            m_radixProbePendingRow = 0;
            if (BatchIsNull(m_radixProbePendingBatch)) {
                m_radixProbePendingBatch = NULL;
                m_radixProbeInputEnded = true;
                break;
            }
            ++m_radixProbeInputBatches;
        }
        VectorBatch* source = m_radixProbePendingBatch;
        if (m_complicatekey)
            CalcComplicateHashVal(source, m_runtime->hj_OuterHashKeys, false);
        else
            hashBatchArray(source, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
#ifdef ENABLE_HTAP
        bool allocationGrew = false;
#endif
        while (m_radixProbePendingRow < (uint32)source->m_rows && m_radixProbeTileRows < tileRows) {
            uint32 index = m_radixProbeTileRows / BatchMaxSize;
            MemoryContext batchContext = m_radixProbeTileContext;
#ifdef ENABLE_HTAP
            RadixProbeSlot* slot = reuse ? &m_radixReusableSlots[index] : NULL;
            bool firstInSlot = (m_radixProbeTileRows % BatchMaxSize) == 0;
            if (slot != NULL) {
                if (slot->context == NULL) {
                    slot->context = AllocSetContextCreate(m_radixProbeTileContext, "radixProbeVector",
                        ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
                    allocationGrew = true;
                } else if (firstInSlot) {
                    m_radixProbeTileBatches[index]->Reset(false);
                    ++m_radixProbeReusedSlots;
                }
                batchContext = slot->context;
                if (!firstInSlot)
                    stagedBytes -= slot->used;
            }
#endif
            if (m_radixProbeTileBatches[index] == NULL) {
                AutoContextSwitch switchContext(batchContext);
                m_radixProbeTileBatches[index] = New(batchContext) VectorBatch(batchContext, m_probeOp.tupleDesc);
            }
            VectorBatch* dest = m_radixProbeTileBatches[index];
            uint32 rows = Min((uint32)source->m_rows - m_radixProbePendingRow,
                (uint32)BatchMaxSize - (uint32)dest->m_rows);
            {
                AutoContextSwitch switchContext(batchContext);
                dest->Copy<true, true>(source, m_radixProbePendingRow, m_radixProbePendingRow + rows);
            }
#ifdef ENABLE_HTAP
            if (slot != NULL) {
                const auto* allocator = (const AllocSetContext*)batchContext;
                uint64 allocated = allocator->totalSpace;
                uint64 available = allocator->freeSpace;
                for (MemoryContext child = batchContext->firstchild; child != NULL; child = child->nextchild)
                    calcHashContextSize(child, &allocated, &available);
                m_radixReusableUsed -= slot->used;
                slot->used = allocated - Min(allocated, available);
                m_radixReusableUsed += slot->used;
                stagedBytes += slot->used;
                allocationGrew |= allocated > slot->allocated;
                slot->allocated = allocated;
            }
#endif
            rc = memcpy_s(m_radixOuterHash + m_radixProbeTileRows,
                sizeof(uint32) * (RHJ_PROBE_TILE_ROWS - m_radixProbeTileRows),
                m_hashVal + m_radixProbePendingRow, sizeof(uint32) * rows);
            securec_check(rc, "\0", "\0");
            m_radixProbeTileRows += rows;
            m_radixProbePendingRow += rows;
        }
        if (!reuse) {
            uint64 allocated = 0, available = 0;
            calcHashContextSize(m_radixProbeTileContext, &allocated, &available);
            stagedBytes = allocated;
#ifdef ENABLE_HTAP
            if (m_chipletProbeBudget.lanes != 0)
                stagedBytes -= Min(allocated, available);
#endif
            m_radixProbeTilePeakBytes = Max(m_radixProbeTilePeakBytes, allocated);
            checkRadixMemory();
        }
#ifdef ENABLE_HTAP
        else if (allocationGrew)
            checkRadixMemory();
#endif
        if (stagedBytes >= tileBytes)
            break;
    }
    if (m_radixProbeTileRows) {
#ifdef ENABLE_HTAP
        if (reuse) {
            uint64 allocated = 0, available = 0;
            calcHashContextSize(m_radixProbeTileContext, &allocated, &available);
            m_radixProbeTilePeakBytes = Max(m_radixProbeTilePeakBytes, allocated);
        }
#endif
        ++m_radixProbeTiles;
        m_radixProbeTilePeakRows = Max(m_radixProbeTilePeakRows, m_radixProbeTileRows);
#ifdef ENABLE_HTAP
        if (m_chipletProbeBudget.lanes != 0) {
            m_chipletProbeBudget.stagingBytes += stagedBytes;
            m_chipletProbeBudget.peerReduced += tileRows < fixedRows;
            if (stagedBytes >= tileBytes || m_radixProbeTileRows == tileRows) {
                ++m_chipletProbeBudget.budgetLimitedTiles;
                if (tileRows < fixedRows && aggregateBytes != 0)
                    ++m_chipletProbeBudget.aggregateLimitedTiles;
            }
        }
#endif
    }
}

#ifdef ENABLE_HTAP
VectorBatch* SonicRadixHashJoin::probeTrackedRadixChunk(SonicHashSource* source, uint32 part)
{
    uint32* references = &m_radixActiveProbes[part];
    uint32* distinct = &radixActiveDomains[m_radixSharedDomain].distinct;
    if (__atomic_fetch_add(references, 1, __ATOMIC_ACQ_REL) == 0) {
        // Count each shared table partition once in the LLC domain.
        __atomic_fetch_add(distinct, 1, __ATOMIC_RELAXED);
    }
    if (u_sess->attr.attr_sql.enable_cache_trace) {
        uint32 active = __atomic_load_n(distinct, __ATOMIC_RELAXED);
        ++m_radixActiveHistogram[Min(active, 8U)];
    }

    VectorBatch* result = NULL;
    PG_TRY();
    {
        result = (this->*m_probeTypeFun)(source);
    }
    PG_CATCH();
    {
        if (__atomic_fetch_sub(references, 1, __ATOMIC_ACQ_REL) == 1)
            __atomic_fetch_sub(distinct, 1, __ATOMIC_RELAXED);
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (__atomic_fetch_sub(references, 1, __ATOMIC_ACQ_REL) == 1)
        __atomic_fetch_sub(distinct, 1, __ATOMIC_RELAXED);
    return result;
}
#endif

/* Probe each partition across multiple source vectors, emitting ordinary
 * BatchMaxSize chunks. A chunk can return many duplicate-match output batches;
 * its sources must remain alive until probeMemoryTable reports exhaustion. */
VectorBatch* SonicRadixHashJoin::ProbeRadix()
{
    if (m_partNum == 1) {
        m_probeIdx = 0;
        SonicHashTimedSource source(m_hashOpPartition, m_radixProbeInputTime,
            m_radixProbeInputBatches, m_radixProbeTiles, m_radixProbeTilePeakRows);
        return (this->*m_probeTypeFun)(&source);
    }
#ifdef ENABLE_HTAP
    bool preferRecent = m_radixSharedPartitionBytes != NULL && m_runtime->js.jointype == JOIN_INNER &&
        u_sess->attr.attr_sql.enable_radix_shared_probe_locality && u_sess->attr.attr_sql.enable_imcs_nps &&
        !u_sess->attr.attr_sql.enable_imcs_adapt && !operator_data_access_active;
    bool trackActive = m_radixActiveProbes != NULL && !operator_data_access_active && trackSharedRadixProbe();
#endif
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        if (!m_radixHasBatch) {
            int nrows;
            if (m_partNum == 1) {
                instr_time inputStart;
                (void)INSTR_TIME_SET_CURRENT(inputStart);
                m_radixOuterBatch = m_hashOpPartition->getBatch();
                m_radixProbeInputTime += elapsed_time(&inputStart);
                if (BatchIsNull(m_radixOuterBatch)) return NULL;
                ++m_radixProbeInputBatches;
                ++m_radixProbeTiles;
                nrows = m_radixOuterBatch->m_rows;
                m_radixProbeTilePeakRows = Max(m_radixProbeTilePeakRows, (uint32)nrows);
                if (m_complicatekey)
                    CalcComplicateHashVal(m_radixOuterBatch, m_runtime->hj_OuterHashKeys, false);
                else
                    hashBatchArray(m_radixOuterBatch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
                errno_t rc = memcpy_s(m_radixOuterHash, sizeof(uint32) * RHJ_PROBE_TILE_ROWS,
                    m_hashVal, sizeof(uint32) * nrows);
                securec_check(rc, "\0", "\0");
            } else {
                loadRadixProbeTile();
                nrows = (int)m_radixProbeTileRows;
                if (nrows == 0) return NULL;
            }
            instr_time partitionStart;
            (void)INSTR_TIME_SET_CURRENT(partitionStart);
            errno_t rc = memset_s(m_radixCounts, sizeof(uint32) * m_partNum, 0, sizeof(uint32) * m_partNum);
            securec_check(rc, "\0", "\0");
            unsigned bits = 0;
            for (uint32 p = m_partNum; p > 1; p >>= 1) ++bits;
            auto partitionOf = [this, bits](uint32 hash) -> uint32 {
#ifdef USE_PRIME
                return hash % m_partNum;
#else
                return m_partNum == 1 ? 0 : hash >> (32 - bits);
#endif
            };
            for (int row = 0; row < nrows; ++row)
                ++m_radixCounts[partitionOf(m_radixOuterHash[row])];
            uint32 offset = 0;
            for (uint32 p = 0; p < m_partNum; ++p) {
                m_radixPartStart[p] = m_radixCursor[p] = offset;
                offset += m_radixCounts[p];
            }
            m_radixPartStart[m_partNum] = offset;
            for (int row = 0; row < nrows; ++row)
                m_radixRowIdx[m_radixCursor[partitionOf(m_radixOuterHash[row])]++] = row;
            double elapsed = elapsed_time(&partitionStart);
            m_partition_time += elapsed;
            m_radixProbePartitionTime += elapsed;
            m_radixPart = m_radixPartOffset = 0;
            m_radixPriorityPart = 0;
#ifdef ENABLE_HTAP
            if (preferRecent) {
                uint64 now = RhjNow() / 1000;
                uint64 hint = __atomic_load_n(m_radixRecentPart, __ATOMIC_RELAXED);
                uint32 candidate = hint & 0xffff;
                if (hint != 0 && now - (hint >> 16) < sharedProbeHintUs &&
                    candidate != 0 && candidate < m_partNum && m_radixCounts[candidate] != 0 &&
                    m_innerPartitions[candidate] != NULL && m_innerPartitions[candidate]->m_rows != 0) {
                    for (uint32 p = 0; p < candidate; ++p) {
                        if (m_radixCounts[p] != 0 && m_innerPartitions[p] != NULL &&
                            m_innerPartitions[p]->m_rows != 0) {
                            uint64 cacheBytes = 0;
                            unsigned lanes = 0;
                            int domain = RhjLlcDomain(sched_getcpu(), &cacheBytes, &lanes);
                            uint64 hotBytes = m_radixSharedPartitionBytes[candidate];
                            uint64 competingBytes = hotBytes + m_radixSharedPartitionBytes[p];
                            bool pressure = competingBytes > cacheBytes / 4 ||
                                (u_sess->attr.attr_sql.enable_imcs_cache_harmful &&
                                    VecCacheCoordHasHarmfulPeers(domain, lanes));
                            /* Leave room for probe vectors and other operators. */
                            if (domain == m_radixSharedDomain && hotBytes != 0 &&
                                hotBytes <= cacheBytes / 2 && pressure) {
                                m_radixPriorityPart = candidate;
                                ++m_radixPromotedTiles;
                            }
                            break;
                        }
                    }
                }
            }
#endif
            m_radixHasBatch = true;
            m_radixPartInProgress = false;
            m_radixIndexedProbe = false;
        }
        while (m_radixPart < m_partNum) {
            // Move just the ready local partition to the front. Keep the
            // original order of all other partitions and visit each once.
            uint32 part = m_radixPart;
            if (m_radixPriorityPart != 0 && m_radixPart <= m_radixPriorityPart)
                part = m_radixPart == 0 ? m_radixPriorityPart : m_radixPart - 1;
            SonicHashMemPartition* build = (SonicHashMemPartition*)m_innerPartitions[part];
            if (build == NULL || build->m_rows == 0) {
                ++m_radixPart;
                m_radixPartOffset = 0;
                continue;
            }
            if (!m_radixPartInProgress) {
                uint32 start = Max(m_radixPartStart[part], m_radixPartOffset);
                uint32 end = Min(m_radixPartStart[part + 1], start + BatchMaxSize);
                if (start == end) {
                    ++m_radixPart;
                    m_radixPartOffset = 0;
                    continue;
                }
#ifdef ENABLE_HTAP
                if (preferRecent && m_radixPartOffset == 0) {
                    uint64 now = RhjNow() / 1000;
                    uint64 previous = __atomic_load_n(m_radixRecentPart, __ATOMIC_RELAXED);
                    if (previous == 0 || (now >= (previous >> 16) && now - (previous >> 16) >= sharedProbeHintUs)) {
                        uint64 hint = (now << 16) | part;
                        (void)__atomic_compare_exchange_n(m_radixRecentPart, &previous, hint, false,
                            __ATOMIC_RELAXED, __ATOMIC_RELAXED);
                    }
                }
#endif
                instr_time copyStart;
                (void)INSTR_TIME_SET_CURRENT(copyStart);
                if (m_partNum == 1) {
                    errno_t rc = memcpy_s(m_hashVal, sizeof(uint32) * INIT_DATUM_ARRAY_SIZE,
                        m_radixOuterHash, sizeof(uint32) * m_radixOuterBatch->m_rows);
                    securec_check(rc, "\0", "\0");
                } else {
                    m_radixProbeBatch->Reset();
                    int rows = end - start;
                    m_radixIndexedProbe = !m_complicatekey;
#ifdef ENABLE_HTAP
                    m_radixIndexedProbe = m_radixIndexedProbe && !operator_data_access_active;
#endif
                    for (int row = 0; row < rows; ++row) {
                        uint32 sourceRow = m_radixRowIdx[start + row];
                        VectorBatch* src = m_radixProbeTileBatches[sourceRow / BatchMaxSize];
                        uint32 nth = sourceRow % BatchMaxSize;
                        if (m_radixIndexedProbe) {
                            for (int key = 0; key < m_probeOp.keyNum; ++key) {
                                int col = m_probeOp.keyIndx[key];
                                ScalarVector* from = &src->m_arr[col];
                                ScalarVector* to = &m_radixProbeBatch->m_arr[col];
                                to->m_desc = from->m_desc;
                                to->m_const = false;
                                to->m_flag[row] = from->m_flag[nth];
                                to->m_vals[row] = NOT_NULL(to->m_flag[row]) ? from->m_vals[nth] : 0;
                            }
                        } else if (m_complicatekey
#ifdef ENABLE_HTAP
                            || unlikely(operator_data_access_active)
#endif
                        ) {
                            m_radixProbeBatch->CopyNth(src, nth);
                        } else {
                            for (int col = 0; col < m_probeOp.cols; ++col) {
                                ScalarVector* from = &src->m_arr[col];
                                ScalarVector* to = &m_radixProbeBatch->m_arr[col];
                                to->m_desc = from->m_desc;
                                to->m_const = false;
                                to->m_flag[row] = from->m_flag[nth];
                                to->m_vals[row] = NOT_NULL(to->m_flag[row]) ? from->m_vals[nth] : 0;
                            }
                        }
                        m_hashVal[row] = m_radixOuterHash[sourceRow];
                    }
                    m_radixProbeBatch->m_rows = rows;
                    m_radixProbeBatch->FixRowCount();
                }
                m_radixProbeCopyTime += elapsed_time(&copyStart);
                m_radixPartOffset = end;
                m_radixSourceUsed = false;
                m_probeStatus = PROBE_FETCH;
                m_matchLocIndx = 0;
                m_radixPartInProgress = true;
            }
            m_probeIdx = part;
            SonicHashBatchSource source(m_partNum == 1 ? m_radixOuterBatch : m_radixProbeBatch, m_radixSourceUsed);
            VectorBatch* result;
#ifdef ENABLE_HTAP
            int policy = u_sess->attr.attr_sql.imcs_chiplet_probe_tile_policy;
            if (m_chipletProbeBudget.lanes != 0 && (policy == 3 || policy == 5) &&
                (m_chipletProbeBudget.probeChunks++ & 31) == 0) {
                ChipletCachePublishRadix(m_radixSharedDomain, m_runtime->js.ps.plan->plan_node_id, part,
                    m_radixSharedPartitionBytes[part]);
            }
            if (trackActive)
                result = probeTrackedRadixChunk(&source, part);
            else
#endif
                result = (this->*m_probeTypeFun)(&source);
            if (result != NULL) return result;
            m_radixPartInProgress = false;
        }
        m_radixHasBatch = false;
    }
}

/*
 * @Description: save the data to memory.
 * @in batch - Put the data in batch to momory.
 */
template <bool complicateJoinKey>
void SonicRadixHashJoin::saveToMemory(VectorBatch* batch)
{
    int rows = batch->m_rows;
    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[0];

    if (!hasEnoughMem() || (m_rows + rows) > SONIC_MAX_ROWS) {
        /*
         * If the total number of rows reaches SONIC_MAX_ROWS,
         * spill the data into disk.
         * Because the hash table can not handle this amount of data.
         */
        m_strategy = GRACE_HASH;
        m_partNum = calcPartitionNum();

        WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
        flushToDisk<complicateJoinKey>();

        RuntimeBinding(m_funBuild, m_strategy)(batch);
        pgstat_increase_session_spill();
        (void)pgstat_report_waitstatus(oldStatus);
        return;
    }

    if (complicateJoinKey) {
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
        memPartition->putHash(m_hashVal, rows);
    }

#ifdef ENABLE_HTAP
    OperatorDataAccessReadBatch(batch);
#endif
    memPartition->putBatch(batch);

    /*
     * Check the memory utilization to tell
     * whether to spill the next coming batch.
     */
    judgeMemoryOverflow(0);
}

/*
 * @Description: save the data to file.
 * @in batch - Put the data in batch to file.
 */
template <bool complicateJoinKey, bool optspill>
void SonicRadixHashJoin::saveToDisk(VectorBatch* batch)
{

    int idx = 0;
    int row_idx = 0;
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    int nrows = batch->m_rows;
    uint32* part_idx = NULL;

    /* First, calclute the hashVal of each row */
    if (complicateJoinKey) {
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
    } else {
        hashBatchArray(batch, (void*)m_buildOp.hashFunc, m_buildOp.hashFmgr, m_buildOp.keyIndx, m_hashVal);
    }

    /* Get partition number for hash value. */
    calcPartIdx(m_hashVal, m_partNum, nrows);

    /* Second, save the batch to disk */
    part_idx = m_partIdx;

    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
        for (idx = 0; idx < m_buildOp.cols; ++idx) {
            arr_val = batch->m_arr[idx].m_vals + row_idx;
            null_flag = batch->m_arr[idx].m_flag + row_idx;

            Assert(m_innerPartitions[*part_idx] != NULL);
            SonicHashFilePartition* innerPartition = (SonicHashFilePartition*)m_innerPartitions[*part_idx];
            innerPartition->putVal<optspill>(arr_val, null_flag, idx);
        }
    }

    part_idx = m_partIdx;
    for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
        m_innerPartitions[*part_idx]->m_rows += 1;
    }

    /* put hash value to disk if complicateJoinKey is true. */
    if (complicateJoinKey) {
        part_idx = m_partIdx;
        for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
            m_innerPartitions[*part_idx]->putHash(&m_hashVal[row_idx]);
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: Flush the data from memory structure to disk.
 */
template <bool complicateJoinKey>
void SonicRadixHashJoin::flushToDisk()
{
    SonicHashPartition** inner_partitions = NULL;

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        /* Init the hash level. */
        m_pLevel = (uint8*)palloc0(sizeof(uint8) * m_partNum);

        /* Init the mark m_isValid[] */
        m_isValid = (bool*)palloc0(sizeof(bool) * m_partNum);

        /* Before flush, rebuild inner partitions. */
        m_partSizeOrderedIdx = (uint32*)palloc0(sizeof(uint32) * m_partNum);

        inner_partitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*) * m_partNum);
        for (uint32 partIdx = 0; partIdx < m_partNum; ++partIdx) {
            inner_partitions[partIdx] = New(CurrentMemoryContext) SonicHashFilePartition(
                (char*)"innerPartitionContext", complicateJoinKey, m_buildOp.tupleDesc, m_memControl.totalMem);
            initPartition<true>(inner_partitions[partIdx]);
            m_pLevel[partIdx] = 1;
            m_isValid[partIdx] = true;
        }
    }

    if (m_runtime->js.ps.instrument) {
        int64 memory_size = 0;
        CalculateContextSize(CurrentMemoryContext, &memory_size);
        if (m_runtime->js.ps.instrument->memoryinfo.peakOpMemory < memory_size) {
            m_runtime->js.ps.instrument->memoryinfo.peakOpMemory = memory_size;
        }

        /*
         * Record the peak control memory right here,
         * since the m_memControl.hashContext is full now.
         */
        memory_size = 0;
        CalculateContextSize(m_memControl.hashContext, &memory_size);
        if (m_runtime->js.ps.instrument->memoryinfo.peakControlMemory < memory_size) {
            m_runtime->js.ps.instrument->memoryinfo.peakControlMemory = memory_size;
        }
    }

    /*
     * When m_innerPartitions[0] hasn't store any data,
     * it means the current memory cannot fill in any batch,
     * so we return and switch to saveToDisk.
     */
    if (m_innerPartitions[0]->m_rows == 0) {
        Assert(m_innerPartitions[0]->m_size == 0);
        m_innerPartitions[0]->freeResources();
        m_innerPartitions = inner_partitions;

        return;
    }

    SonicHashMemPartition* partition = (SonicHashMemPartition*)m_innerPartitions[0];

    /*
     * Start flush:
     * First we need to access the hash value:
     *	for complicate join, get hash value from m_hash,
     *	for non-complicate join, calculate and store the
     *	hash value in m_hashVal[INIT_DATUM_ARRAY_SIZE] per atom.
     * Second, calcuate the partition index for each row
     * for once and stored in array m_partIdx[].
     * Last, write each row into correspoinding partition,
     * and also write the hashVal if it is a complicate join case.
     */
    uint32* hash_val = NULL;
    int arr_num = partition->m_data[0]->m_arrIdx + 1;
    int nrows = 0;  /* total rows in current atom */
    int row_idx = 0; /* row_idx in current atom */
    int arr_idx = 0; /* current array index in m_arr */
    uint32* part_idx = NULL;

    for (arr_idx = 0; arr_idx < arr_num; ++arr_idx) {
        nrows = (arr_idx < arr_num - 1) ? partition->m_data[0]->m_atomSize : partition->m_data[0]->m_atomIdx;
        /* Calcualte hash values for the atom in a non complicate join case. */
        if (!complicateJoinKey) {
            hashAtomArray(partition->m_data,
                nrows,
                arr_idx,
                (void*)m_buildOp.hashAtomFunc,
                m_buildOp.hashFmgr,
                m_buildOp.keyIndx,
                m_hashVal);
            hash_val = m_hashVal;
        } else {
            hash_val = (uint32*)partition->m_hash->m_arr[arr_idx]->data;
        }

        calcPartIdx(hash_val, m_partNum, nrows);

        partition->flushPartition(arr_idx, m_partIdx, inner_partitions, nrows);

        if (complicateJoinKey) {
            part_idx = m_partIdx;
            row_idx = 0;
            if (unlikely(arr_idx == 0)) {
                /* Starts from the second position. */
                part_idx++;
                hash_val++;
                row_idx = 1;
            }

            for (; row_idx < nrows; row_idx++, part_idx++) {
                inner_partitions[*part_idx]->putHash(hash_val++);
            }
        }
    }

    /* Release the partition in memory. */
    partition->freeResources();
    partition = NULL;
    m_innerPartitions = inner_partitions;
}

/*
 * @Description: Prepare for probe process.
 *	Initial probe functions.
 *	build hashtable with build side data.
 *	Load data from file partition if necessary.
 */
void SonicRadixHashJoin::prepareProbe()
{
    int64 max_partition_rows = 0;
    uint64 sz_hash;
    SonicHashMemPartition* mem_partition = NULL;
    instr_time start_time;

    switch (m_strategy) {
        case MEMORY_HASH: {
            /* Do the join in memory */
            (void)INSTR_TIME_SET_CURRENT(start_time);

            m_probeStatus = PROBE_FETCH;

            Assert(m_innerPartitions[m_probeIdx]);
            Assert(m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);

            mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
            max_partition_rows = mem_partition->m_rows;

            /*
             * If sz_hash is less than or equal to 2 bytes,
             * Use 2 bytes for hash table size.
             * Otherwise use 4 bytes for hash table size.
             * Should consider both m_bucket and m_next size.
             */
            m_hashSize = (int64)calcHashSize(max_partition_rows);
            sz_hash = Max(m_hashSize, max_partition_rows + 1);

            WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
            if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
                m_bucketTypeSize = 2;
                initHashTable(2, m_probeIdx);
                if (m_complicatekey) {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, true, true>;
                        buildHashTable<uint16, true, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, true, false>;
                        buildHashTable<uint16, true, false>(m_probeIdx);
                    }
                } else {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, false, true>;
                        buildHashTable<uint16, false, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, false, false>;
                        buildHashTable<uint16, false, false>(m_probeIdx);
                    }
                }
            } else {
                m_bucketTypeSize = 4;
                initHashTable(4, m_probeIdx);
                if (m_complicatekey) {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, true, true>;
                        buildHashTable<uint32, true, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, true, false>;
                        buildHashTable<uint32, true, false>(m_probeIdx);
                    }
                } else {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, true>;
                        buildHashTable<uint32, false, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, false>;
                        buildHashTable<uint32, false, false>(m_probeIdx);
                    }
                }
            }

            (void)pgstat_report_waitstatus(oldStatus);
            m_build_time += elapsed_time(&start_time);
            PROFILE_PART(0, m_bucketTypeSize);
        } break;
        case GRACE_HASH: {
            (void)INSTR_TIME_SET_CURRENT(start_time);
            m_probeStatus = PROBE_PARTITION_MEM;
            m_probePartStatus = PROBE_FETCH;

            /*
             * Load as many inner partitions as possible,
             * so that during the probe side,
             * few outer partitions will be spilled to disk.
             * We also record the start and last partition index to support profile.
             */
            loadInnerPartitions(m_memControl.totalMem);
            initProbePartitions();

            if (m_partLoadedOffset >= 0) {
                /* All data are put in the partition, which is used to be the smallest partition */
                m_probeIdx = m_partSizeOrderedIdx[0];

                Assert(m_innerPartitions[m_probeIdx]);
                Assert(m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);

                mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
                max_partition_rows = mem_partition->m_rows;
                /*
                 * If sz_hash is less than or equal to 2 bytes,
                 * Use 2 bytes for hash table size.
                 * Otherwise use 4 bytes for hash table size.
                 * Should consider both m_bucket and m_next size.
                 */
                m_hashSize = calcHashSize(max_partition_rows);
                sz_hash = Max(m_hashSize, max_partition_rows + 1);
                if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
                    m_bucketTypeSize = 2;
                    initHashTable(2, m_probeIdx);
                    if (m_complicatekey) {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, true, true>;
                            buildHashTable<uint16, true, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, true, false>;
                            buildHashTable<uint16, true, false>(m_probeIdx);
                        }
                    } else {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, false, true>;
                            buildHashTable<uint16, false, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, false, false>;
                            buildHashTable<uint16, false, false>(m_probeIdx);
                        }
                    }
                } else {
                    m_bucketTypeSize = 4;
                    initHashTable(4, m_probeIdx);
                    if (m_complicatekey) {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint32, true, true>;
                            buildHashTable<uint32, true, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint32, true, false>;
                            buildHashTable<uint32, true, false>(m_probeIdx);
                        }
                    } else {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint32, false, true>;
                            buildHashTable<uint32, false, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint32, false, false>;
                            buildHashTable<uint32, false, false>(m_probeIdx);
                        }
                    }
                }

                m_build_time += elapsed_time(&start_time);
                if (m_partLoadedOffset >= 0) {
                    PROFILE_PART(m_probeIdx, m_bucketTypeSize);
                }
            } else {
                if (m_complicatekey)
                    m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, true, false>;
                else
                    m_probeTypeFun = &SonicRadixHashJoin::probePartition<uint16, false, false>;
            }

        } break;
        default:
            Assert(false);
            break;
    }
    m_runtime->joinState = HASH_PROBE;
}

/*
 * @Description: Initial hash table.
 * @in byteSize - single hash bucket size.
 * @in curPartIdx - Initial hash table in curPartIdx-th partition.
 */
void SonicRadixHashJoin::initHashTable(uint8 byteSize, uint32 curPartIdx)
{
    int64 nrows = 0;
    int64 sz_hash;
    SonicHashMemPartition* mem_partition = NULL;
    mem_partition = (SonicHashMemPartition*)m_innerPartitions[curPartIdx];

    /* This partition hasn't any data. So return directly. */
    if (mem_partition == NULL)
        return;

    AutoContextSwitch memSwitch(mem_partition->m_context);
    nrows = mem_partition->m_rows;

    /*
     * Here we should guarentee that the nrows should be less than SONIC_MAX_ROWS.
     * To avoid bad performance influence,
     * we check this condition in loadPartition() when we load all data from temp files
     * and in SaveToMemory() when put original data into memory.
     */
    Assert(nrows <= SONIC_MAX_ROWS);

    /*
     * We guarentee m_hashSize is less than uint32, because
     * nrows should be less than SONIC_MAX_ROWS.
     */
    mem_partition->m_hashSize = (uint32)m_hashSize;
    mem_partition->m_bucketTypeSize = byteSize;

    sz_hash = Max((int64)mem_partition->m_hashSize, nrows + 1);
    /* Check whether size of one palloc larger than the limit. */
    bool useSegHashTable = (sz_hash * byteSize >= (int64)MaxAllocSize);

    if (!useSegHashTable) {
        mem_partition->m_bucket = (char*)palloc0(byteSize * mem_partition->m_hashSize);
        /* One more cell is need for the zero in next array. */
        mem_partition->m_next = (char*)palloc0(byteSize * (nrows + 1));

        mem_partition->m_segHashTable = false;
    } else {
        uint64 numSegBucket;
        uint64 numSegNext;
        DatumDesc desc;

        if (byteSize == 2) {
            getDataDesc(&desc, 2, NULL, false);
        } else {
            getDataDesc(&desc, 4, NULL, false);
        }

        /* Init m_segBucket */
        mem_partition->m_segBucket =
            AllocateIntArray(mem_partition->m_context, mem_partition->m_context, INIT_DATUM_ARRAY_SIZE, false, &desc);
        mem_partition->m_segBucket->m_atomIdx = 0;

        numSegBucket = (mem_partition->m_hashSize - 1) / INIT_DATUM_ARRAY_SIZE + 1;
        for (uint64 i = 0; i < numSegBucket; i++) {
            mem_partition->m_segBucket->genNewArray(false);
        }

        /* Init m_segNext */
        mem_partition->m_segNext =
            AllocateIntArray(mem_partition->m_context, mem_partition->m_context, INIT_DATUM_ARRAY_SIZE, false, &desc);
        mem_partition->m_segNext->m_atomIdx = 0;

        numSegNext = nrows / INIT_DATUM_ARRAY_SIZE + 1;
        for (uint64 i = 0; i < numSegNext; i++) {
            mem_partition->m_segNext->genNewArray(false);
        }

        mem_partition->m_segHashTable = true;
    }
}

/*
 * @Description: build the curPartIdx-th partition's hash table.
 * @in curPartIdx - Partition index.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
void SonicRadixHashJoin::buildHashTable(uint32 curPartIdx)
{
    int arrSize;
    int arrNum;
    int i, j;
    uint32* hash_res = NULL;
    uint32* hash_val = NULL;
    uint32 tup_idx = 0;
    uint32 loc_id = 0;
    uint32 mask;

    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[curPartIdx];
    Assert(mem_partition != NULL);

    BucketType* hashBucket = ((BucketType*)mem_partition->m_bucket);
    BucketType* hashNext = ((BucketType*)mem_partition->m_next);

#ifdef ENABLE_HTAP
    OperatorDataAccessScope* dataAccess =
        unlikely(operator_data_access_active) ? OperatorDataAccessState() : NULL;
#endif

#ifdef USE_PRIME
    mem_partition->m_mask = mem_partition->m_hashSize;
#else
    mem_partition->m_mask = mem_partition->m_hashSize - 1;
#endif

    /* This partition hasn't any data. So return directly. */
    if (mem_partition->m_rows == 0) {
        return;
    }

    if (!complicateJoinKey) {
        hash_res = m_hashVal;
    }

    mask = mem_partition->m_mask;

    arrNum = mem_partition->m_data[0]->m_arrIdx + 1;

    for (i = 0; i < arrNum; i++) {
        arrSize = (i < arrNum - 1) ? m_atomSize : mem_partition->m_data[0]->m_atomIdx;

        if (complicateJoinKey) {
            hash_res = (uint32*)mem_partition->m_hash->m_arr[i]->data;
            mem_partition->m_hash->m_atomIdx = arrSize;
        } else {
            hashAtomArray(mem_partition->m_data,
                arrSize,
                i,
                (void*)m_buildOp.hashAtomFunc,
                m_buildOp.hashFmgr,
                m_buildOp.keyIndx,
                hash_res);
        }

        hash_val = hash_res;

        /* insert tuple index into hash table. */
        for (j = 0; j < arrSize; j++) {
            /* loc_id is bucket index. */
            loc_id = GETLOCID(*hash_val, mask);

            if (!isSegHashTable) {
#ifdef ENABLE_HTAP
                if (dataAccess != NULL) {
                    dataAccess->Observe(&hashBucket[loc_id], sizeof(BucketType), DA_STATE, DA_READ);
                    dataAccess->Observe(&hashNext[tup_idx], sizeof(BucketType), DA_STATE, DA_WRITE);
                    dataAccess->Observe(&hashBucket[loc_id], sizeof(BucketType), DA_STATE, DA_WRITE);
                }
#endif
                hashNext[tup_idx] = hashBucket[loc_id];
                hashBucket[loc_id] = tup_idx;
            } else {
                /* For now, only use uint32 */
                uint32 segBucketPos = (uint32)mem_partition->m_segBucket->getNthDatum(loc_id);
#ifdef ENABLE_HTAP
                if (dataAccess != NULL) {
                    dataAccess->ObserveSonic(mem_partition->m_segNext, tup_idx, DA_WRITE);
                    dataAccess->ObserveSonic(mem_partition->m_segBucket, loc_id, DA_WRITE);
                }
#endif
                mem_partition->m_segNext->setNthDatum(tup_idx, (ScalarValue*)&segBucketPos);
                mem_partition->m_segBucket->setNthDatum(loc_id, (ScalarValue*)&tup_idx);
            }

            tup_idx++;
            hash_val++;
        }
    }
}

/*
 * @Description: Probe side main function.
 * 	Call probeMemory or probeGrace by m_strategy.
 */
VectorBatch* SonicRadixHashJoin::Probe()
{
    if (m_radixMode) {
        instr_time start;
        (void)INSTR_TIME_SET_CURRENT(start);
        VectorBatch* result = ProbeRadix();
        double finalCallTime = elapsed_time(&start);
        if (result == NULL && !m_radixReported) {
#ifdef ENABLE_HTAP
            uint64 profileEnd = m_streamTimeline == NULL ? 0 : RhjNow();
#endif
            ereport(LOG, (errmsg("RHJ phases node=%d parts=%u build_self_ms=%.3f build_input_ms=%.3f probe_wall_ms=%.3f probe_input_ms=%.3f probe_partition_ms=%.3f probe_copy_ms=%.3f peak_bytes=%llu probe_input_batches=%llu probe_tiles=%llu probe_tile_peak_rows=%u probe_tile_peak_bytes=%llu shared_probe_promoted_tiles=%llu",
                m_runtime->js.ps.plan->plan_node_id, m_partNum, m_build_time * 1000.0,
                m_radixBuildInputTime * 1000.0, (m_probe_time + finalCallTime) * 1000.0,
                m_radixProbeInputTime * 1000.0, m_radixProbePartitionTime * 1000.0,
                m_radixProbeCopyTime * 1000.0, (unsigned long long)m_peakBytes,
                (unsigned long long)m_radixProbeInputBatches, (unsigned long long)m_radixProbeTiles,
                m_radixProbeTilePeakRows, (unsigned long long)m_radixProbeTilePeakBytes,
                (unsigned long long)m_radixPromotedTiles)));
            m_radixReported = true;
#ifdef ENABLE_HTAP
            if (m_chipletProbeBudget.lanes != 0) {
                ereport(LOG, (errmsg("chiplet_probe_tile node=%d llc=%d policy=%d tiles=%lu "
                    "limited=%lu aggregate_limited=%lu staged_bytes=%lu available_bytes=%lu lanes=%u "
                    "aggregate_seen=%lu radix_seen=%lu floor_hits=%lu rounded_away=%lu peer_reduced=%lu "
                    "reused_slots=%lu released_slots=%lu",
                    m_runtime->js.ps.plan->plan_node_id, m_radixSharedDomain,
                    u_sess->attr.attr_sql.imcs_chiplet_probe_tile_policy, m_radixProbeTiles,
                    m_chipletProbeBudget.budgetLimitedTiles, m_chipletProbeBudget.aggregateLimitedTiles,
                    m_chipletProbeBudget.stagingBytes, m_chipletProbeBudget.availableBytes,
                    m_chipletProbeBudget.lanes, m_chipletProbeBudget.aggregateSeen, m_chipletProbeBudget.radixSeen,
                    m_chipletProbeBudget.floorHits, m_chipletProbeBudget.roundedAway, m_chipletProbeBudget.peerReduced,
                    m_radixProbeReusedSlots, m_radixProbeReleasedSlots),
                    errhidestmt(true), ErrOutToClient(false)));
            }
            if (m_probeOverlapChecks != 0) {
                ereport(LOG, (errmsg("rhj_overlap_prefetch node=%d llc=%d checks=%lu busy=%lu prefetches=%lu",
                    m_runtime->js.ps.plan->plan_node_id, m_radixSharedDomain, m_probeOverlapChecks,
                    m_probeOverlapBusy, m_probeOverlapPrefetches), errhidestmt(true), ErrOutToClient(false)));
            }
            if (m_matchYield.checks != 0) {
                ereport(LOG, (errmsg("rhj_match_yield node=%d llc=%d checks=%lu busy=%lu yields=%lu wait_ns=%lu "
                    "slow=%lu still_busy=%lu",
                    m_runtime->js.ps.plan->plan_node_id, m_radixSharedDomain, m_matchYield.checks, m_matchYield.busy,
                    m_matchYield.yields, m_matchYield.waitNs, m_matchYield.slow, m_matchYield.stillBusy),
                    errhidestmt(true), ErrOutToClient(false)));
            }
            if (m_probeOverlap.records != NULL && m_radixSharedDomain >= 0 &&
                (m_streamTimeline == NULL || m_streamTimeline->includes(m_radixSharedDomain))) {
                int node = m_runtime->js.ps.plan->plan_node_id;
                int peer = u_sess->attr.attr_sql.imcs_rhj_overlap_peer_node;
                const auto& peerDomain = u_sess->stream_cxt.global_obj->m_rhjProbeOverlap[m_radixSharedDomain];
                uint64 peerEpoch = __atomic_load_n(&peerDomain.heterogeneous, __ATOMIC_ACQUIRE) >> 32;
                ereport(LOG, (errmsg("%s node=%d peer=%d llc=%d worker=%d samples=%u transitions=%u dropped=%u "
                    "invalid_split=%u peer_epoch=%lu done_ns=%lu batches=%lu",
                    m_streamTimeline == NULL ? "rhj_overlap_status" : "rhj_timed_probe_status", node, peer,
                    m_radixSharedDomain, (int)u_sess->stream_cxt.smp_id, m_probeOverlap.size, m_probeOverlap.transitions,
                    m_probeOverlap.dropped, m_probeOverlap.invalidSplit, peerEpoch, profileEnd, m_radixProbeInputBatches),
                    errhidestmt(true), ErrOutToClient(false)));
                if (m_probeOverlap.size > 0) {
                    StringInfoData data;
                    initStringInfo(&data);
                    for (uint32 start = 0; start < m_probeOverlap.size; start += 32) {
                        resetStringInfo(&data);
                        uint32 end = Min(start + 32, m_probeOverlap.size);
                        for (uint32 i = start; i < end; ++i) {
                            const auto& sample = m_probeOverlap.records[i];
                            if (m_streamTimeline != NULL) {
                                appendStringInfo(&data, "%s%lu,%lu,%u,%u,%u,%u,%lu,%lu,%lu,%lu", i == start ? "" : ";",
                                    sample.atNs, sample.endNs, sample.stage, sample.part, sample.rows, sample.candidates,
                                    sample.cycles, sample.stalls, sample.cpuNs, sample.misses);
                            } else {
                                appendStringInfo(&data, "%s%lu,%u,%u,%u,%lu,%lu", i == start ? "" : ";",
                                    sample.atNs, sample.part, sample.rows, sample.state, sample.misses, sample.cpuNs);
                                if (m_splitProbeOverlap)
                                    appendStringInfo(&data, ",%u", sample.stage);
                            }
                        }
                        ereport(LOG, (errmsg("%s node=%d peer=%d llc=%d worker=%d data=%s",
                            m_streamTimeline != NULL ? "rhj_timed_probe" :
                                (m_splitProbeOverlap ? "rhj_overlap_stage" : "rhj_overlap_lookup"),
                            node, peer, m_radixSharedDomain, (int)u_sess->stream_cxt.smp_id, data.data),
                            errhidestmt(true), ErrOutToClient(false)));
                    }
                    pfree(data.data);
                }
            }
            if (m_radixActiveProbes != NULL) {
                const uint64* h = m_radixActiveHistogram;
                ereport(LOG, (errmsg("rhj_probe_active node=%d llc=%d "
                    "hist=%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu",
                    m_runtime->js.ps.plan->plan_node_id, m_radixSharedDomain, h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8]),
                    errhidestmt(true), ErrOutToClient(false)));
            }
            if (m_profileProbeCache) {
                int domain = RhjLlcDomain(sched_getcpu(), NULL);
                if (m_partitionLookupCache != NULL) {
                    for (uint32 part = 0; part < m_partNum; ++part) {
                        const ProbeCacheStats& stats = m_partitionLookupCache[part];
                        m_lookupCache.calls += stats.calls;
                        m_lookupCache.samples += stats.samples;
                        m_lookupCache.rows += stats.rows;
                        m_lookupCache.instructions += stats.instructions;
                        m_lookupCache.misses += stats.misses;
                        m_lookupCache.runningNs += stats.runningNs;
                        m_lookupCache.wallNs += stats.wallNs;
                        m_lookupCache.cpuNs += stats.cpuNs;
                        if (stats.samples != 0 && m_radixSharedDomain >= 0) {
                            ereport(LOG, (errmsg("rhj_partition_lookup node=%d llc=%d part=%u samples=%lu rows=%lu "
                                "misses=%lu cpu_ns=%lu",
                                m_runtime->js.ps.plan->plan_node_id, m_radixSharedDomain, part, stats.samples,
                                stats.rows, stats.misses, stats.cpuNs), errhidestmt(true), ErrOutToClient(false)));
                        }
                    }
                }
                ereport(LOG, (errmsg("rhj_probe_cache node=%d worker=%d llc=%d lookup_calls=%lu lookup_samples=%lu "
                    "lookup_rows=%lu lookup_ins=%lu lookup_misses=%lu lookup_running_ns=%lu lookup_wall_ns=%lu "
                    "match_calls=%lu match_samples=%lu match_ins=%lu match_misses=%lu match_running_ns=%lu "
                    "match_wall_ns=%lu match_rows=%lu match_cpu_ns=%lu", m_runtime->js.ps.plan->plan_node_id,
                    (int)u_sess->stream_cxt.smp_id, domain,
                    m_lookupCache.calls, m_lookupCache.samples, m_lookupCache.rows, m_lookupCache.instructions,
                    m_lookupCache.misses, m_lookupCache.runningNs, m_lookupCache.wallNs, m_matchCache.calls,
                    m_matchCache.samples, m_matchCache.instructions, m_matchCache.misses, m_matchCache.runningNs,
                    m_matchCache.wallNs, m_matchCache.rows, m_matchCache.cpuNs), errhidestmt(true),
                    ErrOutToClient(false)));
            }
#endif
        }
        return result;
    }
    return RuntimeBinding(m_probeFun, m_strategy)();
}

/*
 * @Description: Probe function when the whole build side
 * 	data can be stored in memory.
 */
VectorBatch* SonicRadixHashJoin::probeMemory()
{
    return (this->*m_probeTypeFun)(m_hashOpPartition);
}

/*
 * @Description: Probe function when the whole build side data
 *	can be stored in memory or from single partition.
 * @in probeP - The data in probe side can be from SonicHashOpSource
 *	or single SonicHashFilePartition.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
inline VectorBatch* SonicRadixHashJoin::probeMemoryTable(SonicHashSource* probeP)
{
    VectorBatch* res_batch = NULL;
    int nrows;
    BucketType loc_id;
    uint16* loc1 = NULL;
    uint32* loc2 = NULL;
    uint32* loc3 = NULL;
    uint32 mask;
    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
    BucketType* hashBucket = (BucketType*)mem_partition->m_bucket;

#ifdef ENABLE_HTAP
    OperatorDataAccessScope* dataAccess =
        unlikely(operator_data_access_active) ? OperatorDataAccessState() : NULL;
    RhjProbeOverlapDomain* overlap = NULL;
    if (m_radixSharedDomain >= 0 && (m_profileProbePeer || m_probeOverlapVictim))
        overlap = &u_sess->stream_cxt.global_obj->m_rhjProbeOverlap[m_radixSharedDomain];
    bool timed = m_streamTimeline != NULL && m_streamTimeline->includes(m_radixSharedDomain);
#endif

    Assert(mem_partition->m_status == partitionStatusMemory);

    mask = mem_partition->m_mask;

    for (;;) {
        switch (m_probeStatus) {
            case PROBE_FETCH: {
                m_outRawBatch = probeP->getBatch();

                /* If no more data from probe side, goto state PROBE_FINAL. */
                if (unlikely(BatchIsNull(m_outRawBatch))) {
                    m_probeStatus = PROBE_FINAL;
                    break;
                }

                nrows = m_outRawBatch->m_rows;

#ifdef ENABLE_HTAP
                bool streamCoord = u_sess->attr.attr_sql.enable_imcs_cache_coord;
                bool streamPressure = streamCoord && u_sess->attr.attr_sql.enable_imcs_cache_coord_llc_pressure;
                bool probeCoord = u_sess->attr.attr_sql.enable_imcs_cache_probe_coord &&
                    u_sess->attr.attr_sql.enable_imcs_nps && !u_sess->attr.attr_sql.enable_imcs_adapt &&
                    m_radixMode && m_runtime->js.ps.plan->dop > 1 && dataAccess == NULL;
                bool harmfulCoord = m_radixMode && m_runtime->js.ps.plan->dop > 1 && dataAccess == NULL &&
                    VecCacheCoordHarmfulEnabled();
                if (unlikely(streamCoord || probeCoord || harmfulCoord)) {
                    int planId = m_runtime->js.ps.plan->plan_node_id;
                    uint64 queryId = u_sess->debug_query_id;
                    if (VecCacheCoordSampleProbe(queryId, planId)) {
                        if (harmfulCoord)
                            VecCacheCoordCheckHarmfulProbe();
                        if (streamCoord && !streamPressure)
                            VecCacheCoordObserveRadixProbe(queryId, planId);
                        if (probeCoord || streamPressure) {
                            uint64 allocated = 0;
                            uint64 available = 0;
                            calcHashContextSize(mem_partition->m_context, &allocated, &available);
                            uint64 bytes = allocated - available;
                            bool allowYield = probeCoord && bytes >= 512 * 1024;
                            if (streamPressure || allowYield) {
                                VecCacheCoordObserveProbeFootprint(queryId, planId, bytes,
                                    mem_partition->m_context->is_shared, allowYield);
                            }
                        }
                    }
                }
#endif
                /* Start after fetching input and any coordination yield. */
#ifdef ENABLE_HTAP
                ProbeCacheStats& lookup = m_partitionLookupCache == NULL ? m_lookupCache :
                    m_partitionLookupCache[m_probeIdx];
                bool radixPeer = u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind == (int)RhjOverlapPeerKind::RadixJoin;
                ProbeCacheSample sample(lookup, m_profileProbeCache, nrows,
                    radixPeer && m_probeOverlap.records != NULL ? overlap : NULL, &m_probeOverlap, m_probeIdx,
                    m_splitProbeOverlap, false, timed);
#endif
                auto lookupWork = [&] {
                    /* Radix already hashed and reordered ordinary keys. Expression
                     * keys retain their original evaluation path and context resets. */
                    if (!m_radixMode || m_partNum == 1 || complicateJoinKey) {
                        if (complicateJoinKey) {
                            CalcComplicateHashVal(m_outRawBatch, m_runtime->hj_OuterHashKeys, false);
                        } else {
                            hashBatchArray(m_outRawBatch, (void*)m_probeOp.hashFunc,
                                m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
                        }
                    }
#ifdef ENABLE_HTAP
                    sample.markHashEnd();
#endif

                    m_selectRows = 0;
                    loc3 = m_hashVal;
                    loc1 = m_selectIndx;
                    loc2 = m_loc;
                    auto scanBuckets = [&](auto prefetch) {
                        const int distance = 16;
                        if (prefetch) {
                            for (int i = 0; i < Min(nrows, distance); ++i)
                                __builtin_prefetch(&hashBucket[GETLOCID(m_hashVal[i], mask)], 0, 1);
                        }
                        /*
                         * Iterate probe data to find whether
                         * the hash value between build and probe is same.
                         */
                        for (int i = 0; i < nrows; i++, loc3++) {
                            if (prefetch && i + distance < nrows)
                                __builtin_prefetch(&hashBucket[GETLOCID(m_hashVal[i + distance], mask)], 0, 1);
                            if (isSegHashTable) {
                                loc_id = (BucketType)mem_partition->m_segBucket->getNthDatum(GETLOCID(*loc3, mask));
                            } else {
#ifdef ENABLE_HTAP
                                if (dataAccess != NULL)
                                    dataAccess->Observe(&hashBucket[GETLOCID(*loc3, mask)],
                                        sizeof(BucketType), DA_STATE, DA_READ);
#endif
                                loc_id = hashBucket[GETLOCID(*loc3, mask)];
                            }

                            /*
                             * If the hash value is same between build and probe,
                             * record both positions.
                             */
                            if (loc_id) {
                                *loc1++ = i;
                                *loc2++ = loc_id;
                                m_match[m_selectRows++] = true;
                            }
                        }
                    };
#ifdef ENABLE_HTAP
                    if (!isSegHashTable && dataAccess == NULL && nrows >= 64 && m_probeOverlapVictim &&
                        overlap != NULL) {
                        ++m_probeOverlapChecks;
                        bool peerBusy = RhjPeerBothActive(*overlap);
                        m_probeOverlapBusy += peerBusy;
                        if (peerBusy && u_sess->attr.attr_sql.enable_imcs_rhj_overlap_prefetch) {
                            ++m_probeOverlapPrefetches;
                            scanBuckets(std::true_type());
                            return;
                        }
                    }
#endif
                    scanBuckets(std::false_type());
                };
#ifdef ENABLE_HTAP
                if (m_profileProbePeer && overlap != NULL)
                    VecCacheCoordRunPeerStage(&overlap->lookup, lookupWork);
                else
#endif
                    lookupWork();
                m_probeStatus = PROBE_DATA;
            } break;

            case PROBE_DATA: {
#ifdef ENABLE_HTAP
                if (m_probeOverlapVictim && overlap != NULL && dataAccess == NULL && m_selectRows >= 64) {
                    ++m_matchYield.checks;
                    bool peerBusy = RhjPeerBothActive(*overlap);
                    m_matchYield.busy += peerBusy;
                    if (peerBusy && u_sess->attr.attr_sql.enable_imcs_rhj_overlap_match_yield) {
                        uint64 now = RhjNow();
                        uint64 last = __atomic_load_n(&overlap->lastMatchYield, __ATOMIC_RELAXED);
                        /* Limit attempts per LLC and per worker. Back off if
                         * the current worker takes a long time to resume. */
                        if (now >= m_nextMatchYield && now >= last && now - last >= rhjMatchDomainCooldownNs &&
                            __atomic_compare_exchange_n(&overlap->lastMatchYield, &last, now, false,
                                __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                            (void)sched_yield();
                            uint64 resumed = RhjNow();
                            uint64 elapsed = resumed - now;
                            bool slow = elapsed >= rhjMatchSlowYieldNs;
                            m_nextMatchYield = resumed + (slow ? rhjMatchSlowCooldownNs : rhjMatchWorkerCooldownNs);
                            ++m_matchYield.yields;
                            m_matchYield.waitNs += elapsed;
                            m_matchYield.slow += slow;
                            m_matchYield.stillBusy += RhjPeerBothActive(*overlap);
                            CHECK_FOR_INTERRUPTS();
                        }
                    }
                }
                ProbeCacheSample sample(m_matchCache, m_profileProbeCache,
                    m_splitProbeOverlap ? m_outRawBatch->m_rows : 0,
                    m_splitProbeOverlap && m_streamTimeline == NULL ? overlap : NULL,
                    &m_probeOverlap, m_probeIdx, m_splitProbeOverlap, true, timed, m_selectRows);
                /* goto join process. */
                int scheduleLlc = u_sess->attr.attr_sql.imcs_rhj_stream_schedule_llc;
                bool scheduleHere = scheduleLlc == m_radixSharedDomain ||
                    (scheduleLlc == -2 && u_sess->attr.attr_sql.imcs_rhj_stream_copy_slots > 0);
                bool publishVictim = m_probeOverlapVictim && m_selectRows > 0 && dataAccess == NULL &&
                    u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind == (int)RhjOverlapPeerKind::StreamCopy &&
                    scheduleHere;
                if (overlap != NULL && (m_profileProbePeer || publishVictim)) {
                    VecCacheCoordRunPeerStage(&overlap->match, [&] {
                        res_batch = innerJoin<BucketType, complicateJoinKey, isSegHashTable, false>(m_outRawBatch);
                    });
                } else
#endif
                    res_batch = innerJoin<BucketType, complicateJoinKey, isSegHashTable, false>(m_outRawBatch);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                }
            } break;
            case PROBE_FINAL:
                return NULL;
                /* keep compiler quiet */
                break;
            default:
                break;
        }
    }
    return NULL;
}

/*
 * @Description: probe function when there are more than one partition.
 */
VectorBatch* SonicRadixHashJoin::probeGrace()
{
    VectorBatch* res_batch = NULL;
    while (true) {
        switch (m_probeStatus) {
            case PROBE_PARTITION_MEM: {
                /*
                 * Call probePartition() do in-mem probe and join at first.
                 * After it, change state to PROBE_PREPARE_PAIR.
                 */
                res_batch = (this->*m_probeTypeFun)(NULL);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                }

                /* Change to single partition join */
                m_probeStatus = PROBE_PREPARE_PAIR;
            } break;
            case PROBE_PREPARE_PAIR: {
                /* Find one partition pair can join. */
                bool found = preparePartition();

                /* All finished. */
                if (!found) {
                    return NULL;
                }
                m_probeStatus = PROBE_FETCH;
            } break;
            case PROBE_FETCH:
            case PROBE_DATA:
            case PROBE_FINAL: {
                /* Do join between one partition pair. */
                res_batch = (this->*m_probeTypeFun)(m_outerPartitions[m_probeIdx]);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                } else {
                    Assert(m_innerPartitions[m_probeIdx] &&
                           m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);
                    Assert(m_outerPartitions[m_probeIdx] &&
                           m_outerPartitions[m_probeIdx]->m_status == partitionStatusFile);
                    /* Done join on this partition, should release context and close files. */
                    finishJoinPartition(m_probeIdx);

                    /* go to state PROBE_PREPARE_PAIR */
                    m_probeStatus = PROBE_PREPARE_PAIR;
                }
            } break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmodule(MOD_VEC_EXECUTOR),
                        (errmsg("Unrecognized vector sonic hashjoin run status."))));
                break;
        }
    }
}

/*
 * @Description: Fine one file partition pair can join.
 * @return - false when cannot get any pair because all partitions are finished searching.
 */
bool SonicRadixHashJoin::preparePartition()
{
    Assert(m_probeIdx < m_partNum);
    int64 build_side_row_num = 0;           /* number of rows of the found inner partition */
    int64 probe_side_row_num = 0;           /* number of rows of the found outer partition */
    uint32 idx = m_partLoadedOffset + 1; /* offset of the partition to load */

    /*
     * Search for a valid partition that can be fit in the memory.
     * If an inner partition is too large for the current memory,
     * repartition the inner partition and the coresponding outer partition,
     * then search for the next, until we found a vaild one.
     */
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
    for (; idx < m_partNum; ++idx) {
        m_probeIdx = m_partSizeOrderedIdx[idx];
        m_partLoadedOffset += 1;

        SonicHashPartition* buildP = m_innerPartitions[m_probeIdx];
        SonicHashPartition* probeP = m_outerPartitions[m_probeIdx];

        Assert(buildP != NULL);
        Assert(probeP != NULL);

        /* check if both partition stored in temp file */
        if (buildP->m_status != partitionStatusFile || probeP->m_status != partitionStatusFile) {
            finishJoinPartition(m_probeIdx);
            continue;
        }

        build_side_row_num = buildP->m_rows;
        probe_side_row_num = probeP->m_rows;
        bool join_this_partition = (build_side_row_num > 0) && (probe_side_row_num > 0);

        /*
         * If either inner or outer partition has no data,
         * so there is no need to probe the two partitions.
         */
        if (!join_this_partition) {
            /* free resources. */
            finishJoinPartition(m_probeIdx);
            continue;
        }

        /*
         * Check if we have enough memory for the partition:
         *
         * If not, Hash Join can not be done on this partition pair directly;
         * in this case, we repartition the partition into smaller partitions which will be
         * added to the current partitions set.
         *
         * Note: As for partitions which are proved to be invalid, we allow this repartition process.
         * These new partitions after repartition process will be marked as m_isValid=true, unless the
         * max rownum among these equals to rownum of the current partition.
         * In this case, if m_isValid is marked as false, this partition will not be repartitioned any more
         * and we print warning messages next time. Because it mostly caused by the duplicated hash table.
         */
        size_t size = size_t(((SonicHashFilePartition*)buildP)->m_varSize +
                             m_arrayExpandSize * ceil((double)build_side_row_num / (double)INIT_DATUM_ARRAY_SIZE));

        /* Add estimated hash table size */
        size += get_hash_head_size(build_side_row_num);

        if (m_isValid[m_probeIdx] && size > (m_memControl.totalMem)) {
            int64 old_part_row_num = buildP->m_rows;
            uint32 old_part_nums = m_partNum;

            int64 max_sub_part_row_num;
            uint32 i;

            /* Repartition process. */
            if (m_complicatekey) {
                rePartition<true>(m_probeIdx);
            } else {
                rePartition<false>(m_probeIdx);
            }

            /* If this partition is already partitioned 'm_maxPLevel' times, we would pay extra attention to it.*/
            if (m_pLevel[m_probeIdx] >= m_maxPLevel) {
                elog(LOG,
                    "[VecHashJoin] Warning: file [%u] has partitioned %d times, which exceeds 'm_maxPLevel' times. "
                    "Please pay attention to it.",
                    m_probeIdx,
                    m_pLevel[m_probeIdx]);
            }

            /* Find the max rowNum from new partitions repartitioned. */
            max_sub_part_row_num = m_innerPartitions[old_part_nums]->m_rows;
            for (i = old_part_nums + 1; i < m_partNum; i++)
                max_sub_part_row_num = Max(max_sub_part_row_num, m_innerPartitions[i]->m_rows);

            /* For a invalid repartition process, mark the new partitions as m_isValid = false.*/
            if (max_sub_part_row_num == old_part_row_num) {
                for (i = old_part_nums; i < m_partNum; i++)
                    m_isValid[i] = false;

                elog(LOG,
                    "[Vec SonicRadixHashJoin] Warning: in partition[%u], after %d times of repartition, data to be built "
                    "hash table may be "
                    "duplicated or too large, try ANALYZE first to get better plan.",
                    m_probeIdx,
                    m_pLevel[m_probeIdx]);
            }

            finishJoinPartition(m_probeIdx);
            continue;
        }

        uint64 szHash;
        SonicHashMemPartition* mem_partition = NULL;
        /* Load build side m_probeIdx-th partition. */
        loadInnerPartition(m_probeIdx);
        mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

        Assert(probeP->m_status == partitionStatusFile);
        /* Prepare probe side file buffer. */
        ((SonicHashFilePartition*)probeP)->prepareFileHandlerBuffer();
        /* Rewind probe side file partition. */
        ((SonicHashFilePartition*)probeP)->rewindFiles();

        /* Build hash table for this partition. */
        m_hashSize = (int64)calcHashSize(build_side_row_num);
        szHash = Max(m_hashSize, build_side_row_num + 1);
        if (((uint64)szHash & 0xffff) == (uint64)szHash) {
            m_bucketTypeSize = 2;
            initHashTable(2, m_probeIdx);
            if (m_complicatekey) {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, true, true>;
                    buildHashTable<uint16, true, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, true, false>;
                    buildHashTable<uint16, true, false>(m_probeIdx);
                }
            } else {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, false, true>;
                    buildHashTable<uint16, false, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint16, false, false>;
                    buildHashTable<uint16, false, false>(m_probeIdx);
                }
            }
        } else {
            m_bucketTypeSize = 4;
            initHashTable(4, m_probeIdx);
            if (m_complicatekey) {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, true, true>;
                    buildHashTable<uint32, true, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, true, false>;
                    buildHashTable<uint32, true, false>(m_probeIdx);
                }
            } else {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, true>;
                    buildHashTable<uint32, false, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicRadixHashJoin::probeMemoryTable<uint32, false, false>;
                    buildHashTable<uint32, false, false>(m_probeIdx);
                }
            }
        }
        PROFILE_PART(m_probeIdx, m_bucketTypeSize);
        (void)pgstat_report_waitstatus(old_status);
        return true;
    }
    (void)pgstat_report_waitstatus(old_status);
    return false;
}

/* A partitioned probe batch contains only join-key columns. Materialize
 * payloads for matched rows directly from the owned tile, avoiding a copy of
 * every probe row before the join. The tile remains alive until this chunk
 * and all duplicate-match output batches are drained. */
void SonicRadixHashJoin::copyRadixProbeMatches(VectorBatch* batch, uint16* matchRows, int rows)
{
    if (m_radixMode && m_partNum > 1 && m_radixIndexedProbe) {
        uint32 chunkStart = m_radixPartOffset - batch->m_rows;
        for (int col = 0; col < m_probeOp.cols; ++col) {
            ScalarValue* vals = m_probeOp.batch->m_arr[col].m_vals;
            uint8* flags = m_probeOp.batch->m_arr[col].m_flag;
            for (int row = 0; row < rows; ++row) {
                uint32 sourceRow = m_radixRowIdx[chunkStart + matchRows[row]];
                ScalarVector* source = &m_radixProbeTileBatches[sourceRow / BatchMaxSize]->m_arr[col];
                uint32 sourceIndex = sourceRow % BatchMaxSize;
                vals[row] = source->m_vals[sourceIndex];
                flags[row] = source->m_flag[sourceIndex];
            }
        }
        return;
    }
    for (int col = 0; col < m_probeOp.cols; ++col) {
        ScalarValue* vals = m_probeOp.batch->m_arr[col].m_vals;
        uint8* flags = m_probeOp.batch->m_arr[col].m_flag;
        ScalarValue* sourceVals = batch->m_arr[col].m_vals;
        uint8* sourceFlags = batch->m_arr[col].m_flag;
        for (int row = 0; row < rows; ++row) {
            vals[row] = sourceVals[matchRows[row]];
            flags[row] = sourceFlags[matchRows[row]];
        }
    }
}

/*
 * @Description: Inner join function.
 * @in batch - batch from probe side.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable, bool isPartStatus>
inline VectorBatch* SonicRadixHashJoin::innerJoin(VectorBatch* batch)
{

    int i = 0;
    BucketType loc_id;
    ScalarValue* vals = NULL;
    uint8* flags = NULL;
    uint16 left_rows = 0;
    uint16 rows = 0;
    uint32* loc1 = NULL;
    uint32* loc2 = NULL;
    uint16* loc3 = NULL;
    uint16* loc4 = NULL;
    bool* boolloc = NULL;

    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

    Assert(mem_partition && mem_partition->m_status == partitionStatusMemory);

    BucketType* hashNext = (BucketType*)mem_partition->m_next;

#ifdef ENABLE_HTAP
    OperatorDataAccessScope* dataAccess =
        unlikely(operator_data_access_active) ? OperatorDataAccessState() : NULL;
#endif

    while (m_selectRows) {
        /* match inner and outer keys. */
        if (complicateJoinKey) {
            matchComplicateKey<false>(batch, mem_partition);
        }
        else {
            for (i = 0; i < m_buildOp.keyNum; i++) {
                ScalarVector* val = &batch->m_arr[m_probeOp.keyIndx[i]];
                (this->*m_matchKey[i])(val, mem_partition->m_data[m_buildOp.keyIndx[i]], m_selectRows, i);
            }
        }

        /*
         * record matched inner and outer tuples.
         * Note: m_matchLocIndx maybe not zero.
         * Because some tuples may not be outputed in last cycle.
         */
        loc1 = &m_innerMatchLoc[m_matchLocIndx];
        loc3 = &m_outerMatchLoc[m_matchLocIndx];
        boolloc = m_match;
        loc2 = m_loc;
        loc4 = m_selectIndx;
        for (i = 0; i < m_selectRows; i++) {
            if (*boolloc++) {
                *loc1++ = *loc2;
                *loc3++ = *loc4;
                m_matchLocIndx++;
            }
            loc2++;
            loc4++;
        }

        /* Try to find the next inner tuple in the conflict array. */
        rows = 0;
        loc3 = m_selectIndx;
        loc4 = m_selectIndx;
        loc1 = m_loc;
        loc2 = m_loc;
        boolloc = m_match;
        for (i = 0; i < m_selectRows; i++) {
            if (isSegHashTable) {
                loc_id = (BucketType)mem_partition->m_segNext->getNthDatum(*loc2++);
            } else {
#ifdef ENABLE_HTAP
                if (dataAccess != NULL)
                    dataAccess->Observe(&hashNext[*loc2], sizeof(BucketType), DA_STATE, DA_READ);
#endif
                loc_id = hashNext[*loc2++];
            }

            if (loc_id) {
                *loc3++ = *loc4;
                *loc1++ = loc_id;
                rows++;
                *boolloc++ = true;
            }
            loc4++;
        }
        m_selectRows = rows;

        /* Assemble result batch when we get enough matched tuples. */
        if (m_matchLocIndx >= BatchMaxSize) {
            /* Construct build side VectorBatch with data in SonicDatumArray. */
            mem_partition->m_data[0]->getArrayAtomIdx(BatchMaxSize, m_innerMatchLoc, m_arrayIdx);
            for (i = 0; i < m_buildOp.cols; i++) {
                vals = m_buildOp.batch->m_arr[i].m_vals;
                flags = m_buildOp.batch->m_arr[i].m_flag;
                mem_partition->m_data[i]->getDatumFlagArray(BatchMaxSize, m_arrayIdx, vals, flags);
            }

            /* Construct probe side VectorBatch with data in batch passed in. */
            copyRadixProbeMatches(batch, m_outerMatchLoc, BatchMaxSize);

            left_rows = m_matchLocIndx - BatchMaxSize;

            /*
             * The matched tuples maybe larger than BatchMaxSize.
             * Move the rest un-output tuples to output in next cycle.
             */
            if (left_rows) {
                errno_t rc;

                rc = memmove_s(m_innerMatchLoc,
                    BatchMaxSize * sizeof(uint32),
                    &m_innerMatchLoc[BatchMaxSize],
                    left_rows * sizeof(uint32));
                securec_check(rc, "\0", "\0");

                rc = memmove_s(m_outerMatchLoc,
                    BatchMaxSize * sizeof(uint16),
                    &m_outerMatchLoc[BatchMaxSize],
                    left_rows * sizeof(uint16));
                securec_check(rc, "\0", "\0");
            }
            m_matchLocIndx = left_rows;
            m_buildOp.batch->FixRowCount(BatchMaxSize);
            m_probeOp.batch->FixRowCount(BatchMaxSize);
            return buildRes(m_buildOp.batch, m_probeOp.batch);
        }
    }

    /*
     * End join with current batch.
     * Try to fetch more data from probe side.
     */
    if (isPartStatus) {
        m_probePartStatus = PROBE_FETCH;
    } else {
        m_probeStatus = PROBE_FETCH;
    }

    /* Assemble result batch with the left matched data. */
    if (m_matchLocIndx > 0) {
        mem_partition->m_data[0]->getArrayAtomIdx(m_matchLocIndx, m_innerMatchLoc, m_arrayIdx);
        for (i = 0; i < m_buildOp.cols; i++) {
            vals = m_buildOp.batch->m_arr[i].m_vals;
            flags = m_buildOp.batch->m_arr[i].m_flag;
            mem_partition->m_data[i]->getDatumFlagArray(m_matchLocIndx, m_arrayIdx, vals, flags);
        }

        copyRadixProbeMatches(batch, m_outerMatchLoc, m_matchLocIndx);

        m_buildOp.batch->FixRowCount(m_matchLocIndx);
        m_probeOp.batch->FixRowCount(m_matchLocIndx);
        m_matchLocIndx = 0;
        return buildRes(m_buildOp.batch, m_probeOp.batch);
    } else
        return NULL;
}

/*
 * @Description: probe function for when multiple build partitions in memory.
 * @in probeP - Should be NULL for this function.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
inline VectorBatch* SonicRadixHashJoin::probePartition(SonicHashSource* probeP)
{
    PlanState* outer_node = outerPlanState(m_runtime);
    VectorBatch* res_batch = NULL;
    uint16 nrows;
    BucketType loc_id;

    uint16* loc1 = NULL;
    uint32* loc2 = NULL;
    uint32* loc3 = NULL;
    uint32* loc4 = NULL;
    uint32 part_idx;
    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
    PartitionStatus part_status;
    uint32 mask = 0;
    BucketType* hash_bucket = NULL;
    SonicDatumArray* seg_bucket = NULL;

#ifdef ENABLE_HTAP
    OperatorDataAccessScope* dataAccess =
        unlikely(operator_data_access_active) ? OperatorDataAccessState() : NULL;
#endif

    if (m_partLoadedOffset >= 0) {
        mask = mem_partition->m_mask;
        seg_bucket = mem_partition->m_segBucket;
        hash_bucket = (BucketType*)mem_partition->m_bucket;
    }

    for (;;) {
        /*
         * Use another status m_probePartStatus here.
         * Don't interfere with upper status in ProbeGrace().
         */
        switch (m_probePartStatus) {
            case PROBE_FETCH: {
                m_outRawBatch = VectorEngine(outer_node);
                /* If no more data from probe side, goto state PROBE_FINAL. */
                if (unlikely(BatchIsNull(m_outRawBatch))) {
                    m_probePartStatus = PROBE_FINAL;
                    break;
                }

                nrows = m_outRawBatch->m_rows;
                /* Calcute hash value of probe side data. */
                if (complicateJoinKey) {
                    /* Store hash value of complicate join key into m_hashVal[]. */
                    CalcComplicateHashVal(m_outRawBatch, m_runtime->hj_OuterHashKeys, false);
                } else {
                    hashBatchArray(
                        m_outRawBatch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
                }

                m_selectRows = 0;
                loc3 = m_hashVal;
                loc1 = m_selectIndx;
                loc2 = m_loc;
                loc4 = m_partLoc;
                /*
                 * Iterate probe data to find whether
                 * the hash value between build and probe is same.
                 */
                for (int i = 0; i < nrows; i++, loc3++) {
                    part_idx = *loc3 % m_partNum;

                    /* check the status of the inner partition */
                    if (!m_memPartFlag[part_idx]) {
                        part_status = m_innerPartitions[part_idx]->m_status;
                        if (part_status == partitionStatusFinish) {
                            /*
                             * The inner partition has no data and labled as finish
                             * so there is no need to probe the data.
                             */
                            continue;
                        }

                        /* Should be file partition and save the data into temp file. */
                        Assert(part_status == partitionStatusFile);
                        m_diskPartIdx[m_diskPartNum].partIdx = part_idx;
                        m_diskPartIdx[m_diskPartNum++].rowIdx = i;

                        /* The data in file partition are processed later. */
                        continue;
                    }

                    Assert(mem_partition && mem_partition->m_status == partitionStatusMemory);

                    if (isSegHashTable) {
                        loc_id = (BucketType)seg_bucket->getNthDatum(GETLOCID(*loc3, mask));
                    } else {
#ifdef ENABLE_HTAP
                        if (dataAccess != NULL)
                            dataAccess->Observe(&hash_bucket[GETLOCID(*loc3, mask)],
                                sizeof(BucketType), DA_STATE, DA_READ);
#endif
                        loc_id = hash_bucket[GETLOCID(*loc3, mask)];
                    }

                    /*
                     * If the hash value is same between build and probe,
                     * record both positions, and partition index of build side.
                     */
                    if (loc_id) {
                        *loc1++ = i;
                        *loc2++ = loc_id;
                        m_match[m_selectRows++] = true;
                    }
                }
                if ((m_partLoadedOffset >= 0) && (mem_partition->m_rows > 0)) {
                    m_probePartStatus = PROBE_DATA;
                } else {
                    m_probePartStatus = PROBE_FETCH;
                }
                /* Save data to file partition. */
                WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
                (this->*m_saveProbePartition)();
                (void)pgstat_report_waitstatus(oldStatus);
            } break;

            case PROBE_DATA: {
                /* Goto join process. */
                res_batch = innerJoin<BucketType, complicateJoinKey, isSegHashTable, true>(m_outRawBatch);
                if (!BatchIsNull(res_batch))
                    return res_batch;
            } break;
            case PROBE_FINAL:
                /* Free the probed partitions. */
                for (uint32 i = 0; i < m_partNum; i++) {
                    if (m_memPartFlag[i]) {
                        /*
                         * Free the inner and outer partition.
                         * If they are freed, make sure in the following procedure,
                         * those partition won't be called again.
                         */
                        finishJoinPartition(i);
                    }
                }

                /* done spilling probe side, record the probe info and the partition info */
                reportSorthashinfo(reportTypeProbe, m_partNum);
                HASH_BASED_DEBUG(recordPartitionInfo(false, -1, 0, m_partNum));
                /* release all the temp file buffer for all the unfinished outer partitions */
                releaseAllFileHandlerBuffer(false);
                return NULL;
            default:
                break;
        }
    }
    return NULL;
}

/*
 * @Description: repartition process on a specific partition,
 * 	when this function is called, both inner and outer should have data.
 * @in rePartIdx - partition index need to be repartition.
 */
template <bool complicateJoinKey>
void SonicRadixHashJoin::rePartition(uint32 rePartIdx)
{
    VectorBatch* batch = NULL;
    uint32 part_num = 0;      /* the number of partitions that we created due to repartition */
    uint32 total_part_num = 0; /* the total number of partitions after repartition */
    int nrows = 0;           /* batch rows */
    int row_idx = 0;
    uint32 idx = 0;
    uint32 rbit = 0; /* hash rot bits */
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    uint32* part_idx = NULL;
    bool is_inner = false;
    SonicHashFilePartition* cur_partition = NULL;
    SonicHashPartition** cur_partition_source = NULL;

    AutoContextSwitch memSwitch(m_memControl.hashContext);

    /* compute repartition numbers */
    part_num = getPower2NextNum(m_innerPartitions[rePartIdx]->m_size / m_memControl.totalMem);
    part_num = Max(2, part_num);
    part_num = Min(part_num, SONIC_PART_MAX_NUM);

    total_part_num = m_partNum + part_num;

    Assert(m_pLevel != NULL);
    m_pLevel = (uint8*)repalloc(m_pLevel, total_part_num * sizeof(uint8));

    m_isValid = (bool*)repalloc(m_isValid, total_part_num * sizeof(bool));

    m_partSizeOrderedIdx = (uint32*)repalloc(m_partSizeOrderedIdx, total_part_num * sizeof(uint32));

    m_innerPartitions = (SonicHashPartition**)repalloc(m_innerPartitions, total_part_num * sizeof(SonicHashPartition*));
    m_outerPartitions = (SonicHashPartition**)repalloc(m_outerPartitions, total_part_num * sizeof(SonicHashPartition*));

    for (idx = m_partNum; idx < total_part_num; ++idx) {
        m_innerPartitions[idx] = New(CurrentMemoryContext) SonicHashFilePartition(
            (char*)"innerPartitionContext", complicateJoinKey, m_buildOp.tupleDesc, m_memControl.totalMem);

        m_outerPartitions[idx] = New(CurrentMemoryContext) SonicHashFilePartition(
            (char*)"outerPartitionContext", complicateJoinKey, m_probeOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(m_innerPartitions[idx]);
        initPartition<false>(m_outerPartitions[idx]);

        /* increase the hash level for the new partitions */
        m_pLevel[idx] = m_pLevel[rePartIdx] + 1;

        /* initial the m_isValid for enlarged partitions */
        m_isValid[idx] = true;

        /* append the new partitions into size ordered partition indexes */
        m_partSizeOrderedIdx[idx] = idx;
    }

    /* rot bits: avoid using same bits of hashvalue everytime */
    rbit = m_pLevel[rePartIdx] * 10;

    /*
     * Repartition process:
     * First, repartition the inner partition,
     * then, deal the the outer partition.
     */
    cur_partition_source = m_innerPartitions;
    cur_partition = (SonicHashFilePartition*)m_innerPartitions[rePartIdx];
    cur_partition->prepareFileHandlerBuffer();
    cur_partition->rewindFiles();
    is_inner = true;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (;;) {
        batch = cur_partition->getBatch();
        if (unlikely(BatchIsNull(batch))) {
            if (is_inner) {
                /*
                 * Has finished dealing with the inner partition,
                 * so move to handle the outer partition.
                 */
                cur_partition_source = m_outerPartitions;
                cur_partition = (SonicHashFilePartition*)m_outerPartitions[rePartIdx];
                cur_partition->prepareFileHandlerBuffer();
                cur_partition->rewindFiles();
                is_inner = false;
                continue;
            }
            /* Has finished both inner and outer partitions. */
            break;
        }

        nrows = batch->m_rows;

        /* prepare hashVal */
        if (complicateJoinKey) {
            cur_partition->getHash(m_hashVal, nrows);
        } else {
            /* we do not have hashvalue yet, so compute it first */
            if (is_inner) {
                hashBatchArray(batch, (void*)m_buildOp.hashFunc, m_buildOp.hashFmgr, m_buildOp.keyIndx, m_hashVal);
            } else {
                hashBatchArray(batch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
            }
        }

        /* prepare part_idx for each row */
        calcRePartIdx(m_hashVal, part_num, rbit, nrows);

        /* save the batch to disk */
        part_idx = m_partIdx;
        if (u_sess->attr.attr_sql.enable_sonic_optspill) {
            for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                SonicHashFilePartition* filePatition = (SonicHashFilePartition*)cur_partition_source[*part_idx];
                for (uint16 i = 0; i < cur_partition->m_cols; ++i) {
                    arr_val = batch->m_arr[i].m_vals + row_idx;
                    null_flag = batch->m_arr[i].m_flag + row_idx;

                    filePatition->putVal<true>(arr_val, null_flag, i);
                }
            }
        } else {
            for (uint16 i = 0; i < cur_partition->m_cols; ++i) {
                row_idx = 0;
                part_idx = m_partIdx;
                arr_val = batch->m_arr[i].m_vals;
                null_flag = batch->m_arr[i].m_flag;
                for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                    ((SonicHashFilePartition*)cur_partition_source[*part_idx])->putVal<false>(arr_val, null_flag, i);
                    arr_val++;
                    null_flag++;
                }
            }
        }
        /* Fix the number of rows in each partition */
        part_idx = m_partIdx;
        for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
            cur_partition_source[*part_idx]->m_rows += 1;
        }

        if (complicateJoinKey) {
            part_idx = m_partIdx;
            for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                cur_partition_source[*part_idx]->putHash(&m_hashVal[row_idx]);
            }
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    /* after repartition, release the buffer in file handler */
    for (idx = m_partNum; idx < total_part_num; ++idx) {
        ((SonicHashFilePartition*)m_innerPartitions[idx])->releaseFileHandlerBuffer();
        ((SonicHashFilePartition*)m_outerPartitions[idx])->releaseFileHandlerBuffer();
    }

    HASH_BASED_DEBUG(recordPartitionInfo(true, m_probeIdx, m_partNum, total_part_num));
    HASH_BASED_DEBUG(recordPartitionInfo(false, m_probeIdx, m_partNum, total_part_num));

    if (m_pLevel[rePartIdx] >= WARNING_SPILL_TIME) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning =
            (unsigned int)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning) | (1 << WLM_WARN_SPILL_TIMES_LARGE);
    }

    /* add instrument */
    reportSorthashinfo(reportTypeRepartition, total_part_num, rePartIdx);

    /* after repartition, fix the m_partNum for now we have total_part_num of partitions */
    m_partNum = total_part_num;
}

/*
 * Description: Delete memory context.
 */
void SonicRadixHashJoin::freeMemoryContext()
{
    if (m_memControl.hashContext != NULL) {
        /* Delete child context for hashContext */
        MemoryContextDelete(OperatorDataAccessReleaseContext(m_memControl.hashContext));
        m_memControl.hashContext = NULL;
        m_innerPartitions = NULL;
        m_outerPartitions = NULL;
    }
    if (m_memControl.tmpContext != NULL) {
        /* Delete child context for tmpContext */
        MemoryContextDelete(OperatorDataAccessReleaseContext(m_memControl.tmpContext));
        m_memControl.tmpContext = NULL;
    }
}

/*
 * @Description: Release build and probe partition.
 * @in partIdx - Partition index.
 */
void SonicRadixHashJoin::finishJoinPartition(uint32 partIdx)
{
    if (m_innerPartitions && m_innerPartitions[partIdx] != NULL) {
        m_innerPartitions[partIdx]->freeResources();
        m_innerPartitions[partIdx] = NULL;
    }

    if (m_outerPartitions && m_outerPartitions[partIdx] != NULL) {
        m_outerPartitions[partIdx]->freeResources();
        m_outerPartitions[partIdx] = NULL;
    }
}

/*
 * @Description: Close certain build and probe file.
 * @in partIdx: The build and side partition index to be closed.
 */
void SonicRadixHashJoin::closePartFiles(uint32 partIdx)
{
    if (m_innerPartitions && m_innerPartitions[partIdx] && m_innerPartitions[partIdx]->m_status == partitionStatusFile)
        ((SonicHashFilePartition*)m_innerPartitions[partIdx])->closeFiles();

    if (m_outerPartitions && m_outerPartitions[partIdx] && m_outerPartitions[partIdx]->m_status == partitionStatusFile)
        ((SonicHashFilePartition*)m_outerPartitions[partIdx])->closeFiles();
}

/*
 * @Description: Close all build and probe files
 */
void SonicRadixHashJoin::closeAllFiles()
{
    for (uint32 i = 0; i < m_partNum; i++) {
        closePartFiles(i);
    }
}

/*
 * @Description: Build output with inBatch and outBatch.
 * @in inBatch - Build side batch.
 * @in outBatch - Probe side batch.
 */
VectorBatch* SonicRadixHashJoin::buildRes(VectorBatch* inBatch, VectorBatch* outBatch)
{
    ExprContext* econtext = NULL;
    VectorBatch* res_batch = NULL;
    ScalarVector* pvector = NULL;
    bool has_qual = false;

    DBG_ASSERT(inBatch->m_rows == outBatch->m_rows);

    ResetExprContext(m_runtime->js.ps.ps_ExprContext);

    /* Testing whether ps_ProjInfo is NULL */
    Assert(m_runtime->js.ps.ps_ProjInfo);
    econtext = m_runtime->js.ps.ps_ProjInfo->pi_exprContext;
    initEcontextBatch(NULL, outBatch, inBatch, NULL);

    if (m_runtime->js.joinqual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;

        if (m_runtime->jitted_joinqual) {
            pvector = m_runtime->jitted_joinqual(econtext);
        } else {
            pvector = ExecVecQual(m_runtime->js.joinqual, econtext, false);
        }

        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (m_runtime->js.ps.qual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;
        pvector = ExecVecQual((List*)m_runtime->js.ps.qual, econtext, false);

        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (has_qual) {
        outBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
        inBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
    }

    initEcontextBatch(NULL, outBatch, inBatch, NULL);
    res_batch = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);
    if (res_batch->m_rows != inBatch->m_rows) {
        res_batch->FixRowCount(inBatch->m_rows);
    }

    inBatch->Reset();
    outBatch->Reset();

    return res_batch;
}

/*
 * @Description: profile all hash tables in memory partition.
 * @in writeLog - If true, report log.
 * @in partIdx - index of the partition needed to profile.
 * @in bucketTypeSize - hash table size of one bucket.
 */
void SonicRadixHashJoin::profile(bool writeLog, uint32 partIdx, uint8 bucketTypeSize)
{
    Assert(0 <= partIdx && partIdx < m_partNum);
    Assert(m_innerPartitions[partIdx]);

    SonicHashMemPartition* mem_partition = NULL;
    char stats[MAX_LOG_LEN];

    if (m_innerPartitions[partIdx]->m_status == partitionStatusFinish) {
        return;
    }

    Assert(m_innerPartitions[partIdx]->m_status == partitionStatusMemory);
    mem_partition = (SonicHashMemPartition*)m_innerPartitions[partIdx];
    if (bucketTypeSize == 2)
        if (mem_partition->m_segHashTable)
            profileFunc<uint16, true>(stats, partIdx);
        else
            profileFunc<uint16, false>(stats, partIdx);
    else if (mem_partition->m_segHashTable)
        profileFunc<uint32, true>(stats, partIdx);
    else
        profileFunc<uint32, false>(stats, partIdx);
    if (writeLog) {
        ereport(LOG,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("[SonicRadixHashJoin(%d)(total parts:%u)(part idx:%u)] %s",
                    m_runtime->js.ps.plan->plan_node_id,
                    m_partNum,
                    partIdx,
                    stats)));
    }
}

/*
 * @Description: profile hash table in specified partition.
 * @out stats: string records hash table profiling results.
 * @in partIdx: partition index.
 */
template <typename BucketType, bool isSegHashTable>
void SonicRadixHashJoin::profileFunc(char* stats, uint32 partIdx)
{
    BucketType loc = 0;
    uint32 fill_cells = 0;
    uint32 total_num = 0;
    uint32 good_num = 0;
    uint32 small_num = 0;
    uint32 conflict_num = 0;
    uint32 chain_len = 0;
    uint32 max_chain_len = 0;

    Assert(m_innerPartitions[partIdx]);
    Assert(m_innerPartitions[partIdx]->m_status == partitionStatusMemory);

    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[partIdx];

    for (uint32 i = 0; i < memPartition->m_hashSize; i++) {
        if (isSegHashTable) {
            loc = (BucketType)memPartition->m_segBucket->getNthDatum(i);
        } else {
            loc = ((BucketType*)memPartition->m_bucket)[i];
        }

        if (loc != 0) {
            /* record each hash chain's length and accumulate hash element */
            chain_len = 0;
            fill_cells++;
            while (loc != 0) {
                chain_len++;
                if (isSegHashTable)
                    loc = (BucketType)memPartition->m_segNext->getNthDatum(loc);
                else
                    loc = ((BucketType*)memPartition->m_next)[loc];
            }

            /* record the number of hash chains with length equal to 2 */
            if (chain_len <= 2 && chain_len > 0) {
                good_num++;
            } else if (chain_len <= 5) {
                /* mark if the length of hash chain is greater than 3, we meet hash conflict */
                small_num++;
            } else {
                conflict_num++;
            }

            /* record the length of the max hash chain  */
            if (chain_len > max_chain_len) {
                max_chain_len = chain_len;
            }

            if (chain_len != 0) {
                total_num++;
            }
        }
    }

    /* organize the information */
    errno_t rc = sprintf_s(stats,
        MAX_LOG_LEN,
        "Hash Table Profiling: table size: %u,"
        " hash elements: %ld, table fill ratio %.2f, max hash chain len: %u,"
        " %u chains have length smaller than 2, %u chains have length smaller than 5,"
        " %u chains have conficts with length > 5.",
        memPartition->m_hashSize,
        memPartition->m_rows,
        (double)fill_cells / memPartition->m_hashSize,
        max_chain_len,
        good_num,
        small_num,
        conflict_num);
    securec_check_ss(rc, "\0", "\0");

    if (max_chain_len >= WARNING_HASH_CONFLICT_LEN || (total_num != 0 && conflict_num >= total_num / 2)) {
        pgstat_add_warning_hash_conflict();
        if (HAS_INSTR(&m_runtime->js, true)) {
            m_runtime->js.ps.instrument->warning |= (1 << WLM_WARN_HASH_CONFLICT);
        }
    }
}

/*
 * @Description: check if the sum of current allocated memory and hash table
 * memory will be allocated can be overflowed.
 * If system is too busy to allocate new memory, spill the data in memory.
 * If the sum memory is larger than the total memory, check if we can
 * spread total memory to the sum size.
 * If spread doesn't succeed, spill the data to the disk.
 */
void SonicRadixHashJoin::judgeMemoryOverflow(uint64 hash_head_size)
{
    uint64 avail_mem = 0;
    uint64 allocate_mem = 0;
    uint64 estimate_mem;
    uint64 hash_mem;

    /*
     * This is only for Build phase before spill.
     * If other phases will use this function, change this assert.
     */
    Assert(m_innerPartitions[0]);

    calcHashContextSize(m_memControl.hashContext, &allocate_mem, &avail_mem);

    /* Do not add hash_mem here before judging whether it can be allocated. */
    m_memControl.allocatedMem = allocate_mem;
    m_memControl.availMem = avail_mem;

    /* Estimated hash table size. */
    hash_mem = hash_head_size;

    estimate_mem = allocate_mem + hash_mem;

    m_memControl.sysBusy = gs_sysmemory_busy(estimate_mem * SET_DOP(m_runtime->js.ps.plan->dop), true);

    bool rackBusy = RackMemoryBusy(estimate_mem * SET_DOP(m_runtime->js.ps.plan->dop));
    int64 rackAvail = GetAvailRackMemory(SET_DOP(m_runtime->js.ps.plan->dop)) * 1024L;
    uint64 localTotalMemory = SET_NODEMEM(u_sess->attr.attr_memory.work_mem, m_runtime->js.ps.plan->dop) * 1024L;
    u_sess->local_memory_exhaust = estimate_mem > localTotalMemory;
    m_memControl.sysBusy = m_memControl.sysBusy || (u_sess->local_memory_exhaust && rackBusy);

    if (m_memControl.sysBusy) {
        /* If system is busy, spill data to disk. */
        m_memControl.spillToDisk = true;

        /* memory control */
        AllocSetContext* set = (AllocSetContext*)(m_innerPartitions[0]->m_context);
        set->maxSpaceSize = m_memControl.allocatedMem;

        /* The total memory can be used is reduced to allocatedMem. */
        m_memControl.totalMem = m_memControl.allocatedMem;

        return;
    }

    if (estimate_mem > m_memControl.totalMem) {
        if (m_memControl.maxMem > m_memControl.totalMem) {
            int64 spreadMem =
                Min(Min((uint64)dywlm_client_get_memory() * 1024L, estimate_mem), m_memControl.maxMem - estimate_mem);
            if (spreadMem > estimate_mem * MEM_AUTO_SPREAD_MIN_RATIO) {
                /* Update totalMem if auto spread can be done. */
                m_memControl.totalMem = estimate_mem;
                m_memControl.totalMem += spreadMem;

                /* Update used memory with memory will be allocated. */
                m_memControl.allocatedMem += hash_mem;

                /* memory control */
                AllocSetContext* set = (AllocSetContext*)(m_innerPartitions[0]->m_context);
                set->maxSpaceSize += spreadMem;

                m_memControl.spreadNum++;
                MEMCTL_LOG(DEBUG2,
                    "SonicRadixHashJoin(%d) auto mem spread %ldKB succeed, and work mem is %luKB.",
                    m_runtime->js.ps.plan->plan_node_id,
                    spreadMem / 1024L,
                    m_memControl.totalMem / 1024L);
                return;
            } else {
                /* failed to spread memory */
                MEMCTL_LOG(LOG,
                    "SonicRadixHashJoin(%d) auto mem spread %ldKB failed, and work mem is %luKB.",
                    m_runtime->js.ps.plan->plan_node_id,
                    spreadMem / 1024L,
                    m_memControl.totalMem / 1024L);
            }

            /*
             * If has enable memory spread, but still has to spill,
             * log a warning.
             */
            if (m_memControl.spreadNum > 0) {
                pgstat_add_warning_spill_on_memory_spread();
            }
        }
        m_memControl.spillToDisk = true;
    } else {
        /*
         * If estimate_mem is smaller than the limit,
         * create hash table and do in-memory join later on.
         */
        m_memControl.allocatedMem += hash_mem;
    }
}

/*
 * @Description: check if there is enough memory to fit in the coming batch.
 * @return: true if has enought memory, false if need to spill.
 */
bool SonicRadixHashJoin::hasEnoughMem()
{
    if (m_memControl.spillToDisk) {
        if (m_memControl.sysBusy) {
            m_memControl.totalMem = m_memControl.allocatedMem;
            /*
             * the "usedmem" logged here is the memory size that has been allocated,
             * that is, the memory size of the coming batch is excluded.
             * This is different from VecHashJoin,
             * which logs the allocated memory plus estimated memory size of the coming btach.
             */
            MEMCTL_LOG(LOG,
                "SonicRadixHashJoin(%d) early spilled, workmem: %luKB, usedmem: %luKB,"
                "current HashContext, totalSpace: %luKB, usedSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                m_memControl.totalMem / 1024L,
                m_memControl.allocatedMem / 1024L,
                m_memControl.allocatedMem / 1024L,
                (m_memControl.allocatedMem - m_memControl.availMem) / 1024L);
            pgstat_add_warning_early_spill();
        } else {
            ereport(LOG,
                (errmodule(MOD_VEC_EXECUTOR),
                    errmsg(
                        "Profiling Warning : SonicRadixHashJoin(%d) Disk Spilled.", m_runtime->js.ps.plan->plan_node_id)));
            MEMCTL_LOG(LOG,
                "SonicRadixHashJoin(%d) Disk Spilled: totalSpace: %luKB, freeSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                m_memControl.allocatedMem / 1024L,
                m_memControl.availMem / 1024L);
        }
        return false;
    }
    return true;
}

/*
 * @Description: Calculate memory context size
 * @in ctx: memory context to calculate
 * @in allocateSize: pointer to put total allocated size including child context
 * @in freeSize: pointer to put total free size including child context
 */
void SonicRadixHashJoin::calcHashContextSize(MemoryContext ctx, uint64* allocateSize, uint64* freeSize, bool allowCached)
{
    AllocSetContext* aset = (AllocSetContext*)ctx;
    MemoryContext child;

    if (NULL == ctx) {
        return;
    }

    /* calculate MemoryContext Stats */
    *allocateSize += (aset->totalSpace);
    *freeSize += (aset->freeSpace);

    /* Buffered vector sizes are charged after each bounded copy. Avoid
     * recursively revisiting all earlier vectors on every input batch: that
     * made the build-side memory guard quadratic in the number of batches. */
    if (allowCached && ctx == m_radixInputContext) {
        *allocateSize += m_radixInputAllocated;
        *freeSize += m_radixInputAvailable;
        return;
    }

    /* recursive MemoryContext's child */
    for (child = ctx->firstchild; child != NULL; child = child->nextchild) {
        calcHashContextSize(child, allocateSize, freeSize, allowCached);
    }
}

/*
 * @Description: Calculate partition number with planner parameters.
 */
uint32 SonicRadixHashJoin::calcPartitionNum()
{
    VecHashJoin* node = NULL;
    Plan* inner_plan = NULL;
    int64 nrows;
    int width;
    uint32 part_num;

    node = (VecHashJoin*)m_runtime->js.ps.plan;
    inner_plan = innerPlan(node);

    nrows = (int64)inner_plan->plan_rows;
    width = inner_plan->plan_width;
    elog(DEBUG2, "SonicRadixHashJoin: total size: %ldByte, operator size: %ldByte.", (nrows * width), m_memControl.totalMem);

    part_num = getPower2NextNum((nrows * width) / m_memControl.totalMem);
    part_num = Max(32, part_num);
    part_num = Min(part_num, SONIC_PART_MAX_NUM);

    return part_num;
}

/*
 * @Description: Sort partitions by size.
 * @in memorySize - memory limit.
 * @return - the start index, thus the smallest partition index.
 */
void SonicRadixHashJoin::sortPartitionSize(uint64 memorySize)
{
    for (uint32 cur_idx = 0; cur_idx < m_partNum; ++cur_idx) {
        m_partSizeOrderedIdx[cur_idx] = cur_idx;
    }
    quickSort(m_partSizeOrderedIdx, 0, m_partNum);

    SonicHashFilePartition* file_partition = NULL;
    int rows = 0;
    int tmp_rows = 0;
    int left_rows = 0;
    size_t size = 0;
    size_t tmp_size = 0;
    int32 part_idx = -1;
    m_partLoadedOffset = -1;
    uint64 hash_head_size = 0;

    size = m_arrayExpandSize;
    /* set rows to 1 because the head in the 1st atom is occupied by default */
    rows = 1;
    for (uint32 idx = 0; idx < m_partNum; ++idx) {
        part_idx = m_partSizeOrderedIdx[idx];
        Assert(part_idx >= 0);

        file_partition = (SonicHashFilePartition*)m_innerPartitions[part_idx];
        tmp_rows = file_partition->m_rows;
        tmp_size = file_partition->m_varSize;
        left_rows = INIT_DATUM_ARRAY_SIZE - (rows % INIT_DATUM_ARRAY_SIZE);
        if (tmp_rows > left_rows) {
            tmp_size += m_arrayExpandSize * ((tmp_rows - left_rows - 1) / INIT_DATUM_ARRAY_SIZE + 1);
        }

        /*
         * Add estimated hash table size.
         */
        hash_head_size = get_hash_head_size(tmp_rows);
        if ((size + tmp_size + hash_head_size) < memorySize) {
            m_memPartFlag[part_idx] = true;
            m_partLoadedOffset = idx;
            size += tmp_size + hash_head_size;
            rows += tmp_rows;
        } else {
            break;
        }
    }
}

/*
 * @Description: quicksort algorithm.
 * @in idx - Partition indexes array. The space should be allocated by caller.
 * @in start - The first position of idx to be sorted.
 * @in end - The last position of idx to be sorted + 1 .
 * Sort interval: [start, end)
 */
inline void SonicRadixHashJoin::quickSort(uint32* idx, const int start, const int end)
{
    uint32 tmp = 0;
    int i, j, m;
    size_t mid_size;

    if (end - start > 1) {
        i = start;
        j = end - 1;
        m = idx[(start + end) / 2];
        Assert(m_innerPartitions[m]);
        mid_size = m_innerPartitions[m]->m_size;

        while (i <= j) {
            Assert(m_innerPartitions[idx[i]] && m_innerPartitions[idx[j]]);
            while (m_innerPartitions[idx[i]]->m_size < mid_size)
                i++;
            while (mid_size < m_innerPartitions[idx[j]]->m_size)
                j--;
            if (i <= j) {
                tmp = idx[j];
                idx[j] = idx[i];
                idx[i] = tmp;
                j--;
                i++;
            }
        }
        quickSort(idx, start, j + 1);
        quickSort(idx, i, end);
    }
}

/*
 * @Description: Initial match functions by desc.
 * @in desc - tuple description.
 * @in keyNum - number of hash keys.
 */
void SonicRadixHashJoin::initMatchFunc(TupleDesc desc, uint16 keyNum)
{
    m_matchKey = (matchFun*)palloc0(sizeof(matchFun) * keyNum);

    for (int i = 0; i < keyNum; i++) {
        /* If the i-th hash key is simple type, specialize its match function. */
        if (m_integertype[i])
            DispatchKeyInnerFunction(i);
        else {
            if (m_runtime->js.nulleqqual != NIL) {
                /* Note: check int64 again! */
                m_matchKey[i] = &SonicHash::matchCheckColT<int64, int64, false, true>;
            } else {
                m_matchKey[i] = &SonicHash::matchCheckColT<int64, int64, false, false>;
            }
        }
    }
}

/*
 * @Description: Specify match function by build side hash key type.
 * @in KeyIdx - hash key index in m_buildOp.keyIndx.
 */
void SonicRadixHashJoin::DispatchKeyInnerFunction(int KeyIdx)
{
    int attrid = m_buildOp.keyIndx[KeyIdx];
    switch ((m_buildOp.tupleDesc)->attrs[attrid].atttypid) {
        case INT1OID:
            DispatchKeyOuterFunction<uint8>(KeyIdx);
            break;
        case INT2OID:
            DispatchKeyOuterFunction<int16>(KeyIdx);
            break;
        case INT4OID:
            DispatchKeyOuterFunction<int32>(KeyIdx);
            break;
        case INT8OID:
            DispatchKeyOuterFunction<int64>(KeyIdx);
            break;
        default:
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unrecognize data type %u when choosing match functions from inner hash keys.",
                        (m_buildOp.tupleDesc)->attrs[attrid].atttypid)));
            break;
    }
}

/*
 * @Description: Specify match function by probe side hash key type.
 * @in KeyIdx - hash key index in m_probeOp.keyIndx.
 */
template <typename innerType>
void SonicRadixHashJoin::DispatchKeyOuterFunction(int KeyIdx)
{
    int attr_id = m_probeOp.keyIndx[KeyIdx];
    switch ((m_probeOp.tupleDesc)->attrs[attr_id].atttypid) {
        case INT1OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, uint8, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, uint8, true, false>;
            break;
        case INT2OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int16, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int16, true, false>;
            break;
        case INT4OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int32, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int32, true, false>;
            break;
        case INT8OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int64, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int64, true, false>;
            break;
        default:
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    (errmsg("Unrecognize data type %u when choosing match functions from outer hash keys.",
                        (m_probeOp.tupleDesc)->attrs[attr_id].atttypid))));
            break;
    }
}

/*
 * @Description: Calculate hash value for complicate join key.
 * @in batch - batch from build or probe side.
 * @in hashKeys - hash keys need to be calculated.
 * @in inner - true when batch is from build side.
 */
void SonicRadixHashJoin::CalcComplicateHashVal(VectorBatch* batch, List* hashKeys, bool inner)
{
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    ;
    ListCell* hk = NULL;
    ScalarVector* results = NULL;
    int nrows = batch->m_rows;
    bool first_enter = true;
    bool* pSelection = NULL;
    FmgrInfo* hash_functions = NULL;
    Datum key;
    ScalarValue hash_v;

    if (nrows == 0)
        return;

    m_cjVector->m_rows = 0;
    if (inner) {
        initEcontextBatch(NULL, NULL, batch, NULL);
        hash_functions = m_buildOp.hashFmgr;
    } else {
        initEcontextBatch(NULL, batch, NULL, NULL);
        hash_functions = m_probeOp.hashFmgr;
    }
    pSelection = batch->m_sel;
    ResetExprContext(econtext);
    AutoContextSwitch memSwitch(econtext->ecxt_per_tuple_memory);

    econtext->align_rows = nrows;
    int j = 0;
    foreach (hk, hashKeys) {
        ExprState* clause = (ExprState*)lfirst(hk);

        results = VectorExprEngine(clause, econtext, pSelection, m_cjVector, NULL);

        if (first_enter) {
            for (int i = 0; i < nrows; i++) {
                key = results->m_vals[i];

                if (NOT_NULL(results->m_flag[i]))
                    m_hashVal[i] = FunctionCall1(&hash_functions[j], key);
                else
                    m_hashVal[i] = 0;
            }
            first_enter = false;
        } else {
            for (int i = 0; i < nrows; i++) {
                if (NOT_NULL(results->m_flag[i])) {
                    key = results->m_vals[i];
                    /* rotate hashkey left 1 bit at each rehash step */
                    hash_v = m_hashVal[i];
                    hash_v = (hash_v << 1) | ((hash_v & 0x80000000) ? 1 : 0);
                    hash_v ^= FunctionCall1(&hash_functions[j], key);
                    m_hashVal[i] = hash_v;
                }
            }
        }
        j++;
    }

    /*
     * Rehash the hash value for avoiding the key and
     * distribute key using the same hash function.
     */
    for (int i = 0; i < nrows; i++)
        m_hashVal[i] = hash_uint32(DatumGetUInt32(m_hashVal[i]));
}

/*
 * @Description: match function for complicate join key.
 * @in batch - probe side data.
 * @in inPartition - If matchPartComplicateKey is false,
 * 	inPartition is build side data. Otherwise
 * 	inPartition are not used. build side data is
 * 	from m_innerPartitions[].
 */
template <bool matchPartComplicateKey>
void SonicRadixHashJoin::matchComplicateKey(VectorBatch* batch, SonicHashMemPartition* inPartition)
{
    int i, j;
    int nrows = batch->m_rows;
    ScalarVector* scalar_vector = NULL;
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    SonicDatumArray* array = NULL;
    int resultRow = 0;
    uint32* loc1 = NULL;
    uint32* loc2 = NULL;
    SonicHashMemPartition* mem_partition = NULL;

    ResetExprContext(econtext);

    m_complicate_innerBatch->Reset();
    m_complicate_outerBatch->Reset();

    for (i = 0; i < m_complicate_innerBatch->m_cols; i++) {
        if (!matchPartComplicateKey) {
            Assert(inPartition);

            /* Get data from the specify partition. */
            array = inPartition->m_data[i];
            array->getArrayAtomIdx(m_selectRows, m_loc, m_arrayIdx);
            array->getDatumFlagArray(m_selectRows, m_arrayIdx, m_matchKeys, m_nullFlag);
            scalar_vector = &m_complicate_innerBatch->m_arr[i];
            for (j = 0; j < m_selectRows; j++) {
                scalar_vector->m_vals[j] = m_matchKeys[j];
                scalar_vector->m_flag[j] = m_nullFlag[j];
            }
        } else {
            scalar_vector = &m_complicate_innerBatch->m_arr[i];
            loc1 = m_partLoc;
            loc2 = m_loc;
            for (j = 0; j < m_selectRows; j++, loc1++, loc2++) {
                mem_partition = (SonicHashMemPartition*)m_innerPartitions[*loc1];

                /* After ProbeXXX functions, m_innerPartitions[*loc1] should stay in memory */
                Assert(mem_partition);

                array = mem_partition->m_data[i];
                array->getNthDatumFlag(*loc2, (ScalarValue*)&m_matchKeys[j], &m_nullFlag[j]);
                scalar_vector->m_vals[j] = m_matchKeys[j];
                scalar_vector->m_flag[j] = m_nullFlag[j];
            }
        }
    }

    for (i = 0; i < nrows; i++)
        m_match[i] = false;
    for (i = 0; i < m_selectRows; i++) {
        for (j = 0; j < m_complicate_outerBatch->m_cols; j++) {
            scalar_vector = &m_complicate_outerBatch->m_arr[j];
            scalar_vector->m_vals[resultRow] = batch->m_arr[j].m_vals[m_selectIndx[i]];
            scalar_vector->m_flag[resultRow] = batch->m_arr[j].m_flag[m_selectIndx[i]];
        }
        resultRow++;
        m_match[i] = true;
    }

    m_complicate_innerBatch->FixRowCount(m_selectRows);
    m_complicate_outerBatch->FixRowCount(m_selectRows);

    /*
     * If m_complicate_innerBatch and m_complicate_outerBatch are both NULL,
     * no need to calculate the expression clause, return directly here.
     */
    if (unlikely(m_selectRows == 0))
        return;

    initEcontextBatch(m_complicate_outerBatch, m_complicate_outerBatch, m_complicate_innerBatch, NULL);

    ExecVecQual(m_runtime->hashclauses, econtext, false);

    /* for noneq_join */
    bool need_furtherCheck = false;
    if (m_runtime->js.nulleqqual != NULL) {
        /* no need to calc */
        for (i = 0; i < m_selectRows; i++) {
            if (m_complicate_outerBatch->m_sel[i])
                m_nulleqmatch[i] = false;
            else {
                m_nulleqmatch[i] = true;
                need_furtherCheck = true;
            }
        }

        if (need_furtherCheck) {
            bool* tmpSel = &m_complicate_outerBatch->m_sel[0];
            m_complicate_outerBatch->m_sel = &m_nulleqmatch[0];
            (void)ExecVecQual(m_runtime->js.nulleqqual, econtext, false);

            /* restore the selection */
            m_complicate_outerBatch->m_sel = tmpSel;
            for (i = 0; i < m_selectRows; i++) {
                m_complicate_outerBatch->m_sel[i] = m_complicate_outerBatch->m_sel[i] || m_nulleqmatch[i];
            }
        }
    }

    j = 0;
    for (i = 0; i < m_selectRows; i++) {
        if (m_match[i]) {
            if (!m_complicate_outerBatch->m_sel[j])
                m_match[i] = false;
            j++;
        }
    }
}

/*
 * @Description: bloom filter function.
 */
void SonicRadixHashJoin::pushDownFilterIfNeed()
{
    filter::BloomFilter** bf_array = m_runtime->bf_runtime.bf_array;
    List* bf_var_list = m_runtime->bf_runtime.bf_var_list;
    int arr_num;
    int arr_size;
    uint8 null_flag;
    ScalarValue val;
    SonicHashMemPartition* mem_partition = NULL;

    if (u_sess->attr.attr_sql.enable_bloom_filter && MEMORY_HASH == m_strategy && !m_complicatekey &&
        m_rows <= DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5) {
        Assert(m_probeIdx == 0);
        Assert(m_innerPartitions[m_probeIdx] && m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);
        mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

        for (int i = 0; i < list_length(bf_var_list); i++) {
            Var* var = (Var*)list_nth(bf_var_list, i);
            int idx = -1;
            uint32 rowidx = 0;
            for (int j = 0; j < m_buildOp.keyNum; ++j) {
                if (var->varoattno - 1 == m_buildOp.oKeyIndx[j] && var->varattno - 1 == m_buildOp.keyIndx[j]) {
                    idx = m_buildOp.keyIndx[j];
                    break;
                }
            }
            if (idx < 0) {
                continue;
            }

            Oid dataType = var->vartype;
            if (!SATISFY_BLOOM_FILTER(dataType)) {
                continue;
            }

            filter::BloomFilter* filter = filter::createBloomFilter(dataType,
                var->vartypmod,
                var->varcollid,
                HASHJOIN_BLOOM_FILTER,
                DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5,
                true);

            /*
             * For compute pool, we have to forbidden codegen of bf, since we
             * can not push down codegen expr now.
             */
            if (m_runtime->js.ps.state && m_runtime->js.ps.state->es_plannedstmt &&
                !m_runtime->js.ps.state->es_plannedstmt->has_obsrel) {
                switch (dataType) {
                    case INT2OID:
                    case INT4OID:
                    case INT8OID:
                        filter->jitted_bf_addLong = m_runtime->jitted_hashjoin_bfaddLong;
                        filter->jitted_bf_incLong = m_runtime->jitted_hashjoin_bfincLong;
                        break;
                    default:
                        /* do nothing */
                        break;
                }
            }
            arr_num = mem_partition->m_data[idx]->m_arrIdx + 1;
            for (int j = 0; j < arr_num; j++) {
                arr_size =
                    (j < arr_num - 1) ? mem_partition->m_data[idx]->m_atomSize : mem_partition->m_data[idx]->m_atomIdx;
                for (int k = 0; k < arr_size; k++) {
                    mem_partition->m_data[idx]->getNthDatumFlag(rowidx++, &val, &null_flag);
                    if (NOT_NULL(null_flag)) {
                        filter->addDatum((Datum)val);
                    }
                }
            }
            int pos = list_nth_int(m_runtime->bf_runtime.bf_filter_index, i);
            bf_array[pos] = filter;
        }
    }
}

/*
 * @Description: release all the file buffer of the inner or outer partitions
 * @in isInner - true to release inner partitions, false to release outer partitions.
 */
void SonicRadixHashJoin::releaseAllFileHandlerBuffer(bool isInner)
{
    SonicHashPartition* partition = NULL;
    SonicHashPartition** partitions = isInner ? m_innerPartitions : m_outerPartitions;
    for (uint32 i = 0; i < m_partNum; ++i) {
        partition = partitions[i];
        if (partition != NULL && partition->m_status == partitionStatusFile) {
            ((SonicHashFilePartition*)partition)->releaseFileHandlerBuffer();
        }
    }
}

/*
 * @Description: Calculate partition index and record them in m_partIdx.
 * @in hashVal - hash value to be used. The space should be allocated by caller.
 * @in partNum - partition number.
 * @in nrows - hash value numbers.
 */
inline void SonicRadixHashJoin::calcPartIdx(uint32* hashVal, uint32 partNum, int nrows)
{
#ifndef USE_PRIME
    if (m_radixMode && partNum > 1) {
        unsigned bits = 0;
        for (uint32 n = partNum; n > 1; n >>= 1) ++bits;
        for (int i = 0; i < nrows; ++i)
            m_partIdx[i] = hashVal[i] >> (32 - bits);
        return;
    }
#endif
    for (int i = 0; i < nrows; ++i)
        m_partIdx[i] = hashVal[i] % partNum;
}

/*
 * @Description: Calculate partition index and record them in m_partIdx when do repartition.
 * @in hashVal - hash value to be used. The space should be allocated by caller.
 * @in partNum - partition number.
 * @in rbit - used to rotate hash value.
 * @in nrows - hash value numbers.
 */
inline void SonicRadixHashJoin::calcRePartIdx(uint32* hashVal, uint32 partNum, uint32 rbit, int nrows)
{
    for (int i = 0; i < nrows; ++i) {
        m_partIdx[i] = m_partNum + (leftrot(hashVal[i], rbit) & (partNum - 1));
    }
}

/*
 * @Description: Initial probe partitions.
 */
void SonicRadixHashJoin::initProbePartitions()
{
    AutoContextSwitch memSwitch(m_memControl.hashContext);
    SonicHashPartition** probeP = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*) * m_partNum);
    for (uint32 partIdx = 0; partIdx < m_partNum; partIdx++) {
        /* only need to init those outer file partitions that the coresponding inner partitions are in the disk */
        if (m_innerPartitions[partIdx] && m_innerPartitions[partIdx]->m_status == partitionStatusFile) {
            probeP[partIdx] = New(CurrentMemoryContext) SonicHashFilePartition(
                (char*)"outerPartitionContext", m_complicatekey, m_probeOp.tupleDesc, m_memControl.totalMem);
            initPartition<false>(probeP[partIdx]);
        }
    }
    m_outerPartitions = probeP;
}

/*
 * @Description: Calculate datum array expand size.
 * 	those size are not include the size of SONIC_VAR_TYPE data
 * 	or the size of SONIC_NUMERIC_COMPRESS_TYPE data.
 */
void SonicRadixHashJoin::calcDatumArrayExpandSize()
{
    /* element size per row */
    size_t array_element_size = 0;
    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[0];
    for (uint16 colIdx = 0; colIdx < m_buildOp.cols; ++colIdx) {
        if (memPartition->m_data[colIdx]->m_desc.dataType == SONIC_CHAR_DIC_TYPE) {
            /* for SonicCharDatumArray, better to include the typeSize */
            array_element_size += memPartition->m_data[colIdx]->m_desc.typeSize;
        } else if (memPartition->m_data[colIdx]->m_desc.dataType == SONIC_NUMERIC_COMPRESS_TYPE) {
            /*
             * for SonicNumericDatumArray,
             * only inlcude the space for SonicNumericDatumArray->m_curOffset here,
             * the space to store the numeric will be recorded
             * into SonicHashPartition->m_varSize during spilling stage
             */
            array_element_size += sizeof(uint32);
        } else {
            array_element_size += memPartition->m_data[colIdx]->m_atomTypeSize;
        }
    }

    /*
     * m_arrayExpandSize includes all columns and their flags,
     * for the complicate key case, we should also record the space used for hash value,
     * which is an IntDatumArray without flag.
     */
    if (m_complicatekey) {
        array_element_size += sizeof(uint32);
    }
    m_arrayExpandSize = array_element_size * m_atomSize + m_atomSize * m_buildOp.cols;
}

/*
 * @Description: Calculate hash size by row numbers.
 * @in nrows - row number.
 */
inline uint64 SonicRadixHashJoin::calcHashSize(int64 nrows)
{
#ifdef USE_PRIME
    return hashfindprime(nrows);
#else
    return Max(MIN_HASH_TABLE_SIZE, getPower2LessNum(Min(nrows, (int)(MAX_BUCKET_NUM))));
#endif
}

/*
 * @Description: Reset resources.
 */
void SonicRadixHashJoin::ResetNecessary()
{
#ifdef ENABLE_HTAP
    const uint64 probeBudget = m_chipletProbeBudget.availableBytes;
    const unsigned probeLanes = m_chipletProbeBudget.lanes;
    m_chipletProbeBudget = {};
    m_chipletProbeBudget.availableBytes = probeBudget;
    m_chipletProbeBudget.lanes = probeLanes;
    m_radixReusableUsed = m_radixProbeReusedSlots = m_radixProbeReleasedSlots = 0;
    m_lookupCache = {};
    m_matchCache = {};
    m_probeOverlap.size = m_probeOverlap.transitions = m_probeOverlap.dropped = 0;
    m_probeOverlap.invalidSplit = 0;
    m_probeOverlapChecks = m_probeOverlapBusy = m_probeOverlapPrefetches = 0;
    m_matchYield = {};
    m_nextMatchYield = 0;
    errno_t histogramReset = memset_s(m_radixActiveHistogram, sizeof(m_radixActiveHistogram), 0,
        sizeof(m_radixActiveHistogram));
    securec_check(histogramReset, "\0", "\0");
    if (m_partitionLookupCache != NULL) {
        size_t bytes = m_partNum * sizeof(*m_partitionLookupCache);
        errno_t lookupReset = memset_s(m_partitionLookupCache, bytes, 0, bytes);
        securec_check(lookupReset, "\0", "\0");
    }
#endif
    if (!m_runtime->js.ps.plan->ispwj && m_strategy == MEMORY_HASH && m_runtime->js.ps.righttree->chgParam == NULL &&
        !((VecHashJoin*)m_runtime->js.ps.plan)->rebuildHashTable) {
        /* Okay to reuse the hash table; needn't rescan inner, either. */
        m_runtime->joinState = HASH_PROBE;
        m_probeStatus = PROBE_FETCH;
        if (m_radixMode) {
            m_probe_time = m_radixProbeCopyTime = m_radixProbePartitionTime = m_radixProbeInputTime = 0;
            if (m_radixProbeTileContext != NULL)
                MemoryContextDelete(OperatorDataAccessReleaseContext(m_radixProbeTileContext));
            m_radixProbeTileContext = NULL;
#ifdef ENABLE_HTAP
            if (m_radixReusableSlots != NULL) {
                Size bytes = RHJ_PROBE_TILE_BATCHES * sizeof(RadixProbeSlot);
                errno_t reset = memset_s(m_radixReusableSlots, bytes, 0, bytes);
                securec_check(reset, "\0", "\0");
            }
#endif
            m_radixProbePendingBatch = NULL;
            m_radixProbePendingRow = m_radixProbeTileRows = m_radixPartOffset = 0;
            m_radixProbeInputEnded = false;
            m_radixProbeTiles = m_radixProbeInputBatches = m_radixProbeTilePeakBytes = 0;
            m_radixProbeTilePeakRows = 0;
            m_radixReported = false;
            m_radixSourceUsed = false;
            m_matchLocIndx = 0;
            m_radixHasBatch = false;
            m_radixPart = 0;
            m_radixPromotedTiles = 0;
            m_radixPartInProgress = false;
            m_radixIndexedProbe = false;
            if (m_radixProbeBatch != NULL) {
                m_radixProbeBatch->Reset();
            }
        }
        return;
    }

    if (m_strategy == GRACE_HASH)
        closeAllFiles();

    m_radixSharedBuildContext = NULL;
    m_radixRecentPart = NULL;
    m_radixSharedPartitionBytes = NULL;
    m_radixSharedDomain = -1;
#ifdef ENABLE_HTAP
    m_radixActiveProbes = NULL;
    m_partitionLookupCache = NULL;
    m_chipletProbeBudget = {};
    m_radixReusableSlots = NULL;
#endif
    m_radixPromotedTiles = 0;
    /* Reset hashContext */
    MemoryContextResetAndDeleteChildren(OperatorDataAccessReleaseContext(m_memControl.hashContext));

    {
        /* Recreate m_innerPartitions */
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        m_innerPartitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*));
        m_innerPartitions[0] = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem, m_atomSize);
        initPartition<true>(m_innerPartitions[0]);
    }
    m_outerPartitions = NULL;

    m_runtime->joinState = HASH_BUILD;
    m_strategy = MEMORY_HASH;
    resetMemoryControl();
    m_rows = 0;
    m_probeIdx = 0;
    m_partNum = 1;
    m_radixHasBatch = false;
    m_radixPart = 0;
    m_radixPartInProgress = false;
    m_radixIndexedProbe = false;
    m_radixProbeBatch = NULL;
    m_radixOuterBatch = NULL;
    m_radixRowIdx = NULL;
    m_radixPartStart = NULL;
    m_radixCounts = NULL;
    m_radixCursor = NULL;
    m_radixOuterHash = NULL;
    m_radixProbeCopyTime = m_radixProbePartitionTime = 0;
    m_radixBuildInputTime = m_radixProbeInputTime = 0;
    m_radixProbeTileContext = NULL;
    m_radixProbeTileBatches = NULL;
    m_radixProbePendingBatch = NULL;
    m_radixProbePendingRow = m_radixProbeTileRows = m_radixPartOffset = 0;
    m_radixProbeInputEnded = false;
    m_radixProbeTiles = m_radixProbeInputBatches = m_radixProbeTilePeakBytes = 0;
    m_radixProbeTilePeakRows = 0;
    m_radixInputContext = NULL;
    m_radixInputAllocated = m_radixInputAvailable = 0;
    m_radixSourceUsed = false;
    m_radixReported = false;
    m_build_time = m_probe_time = m_partition_time = 0;
    m_peakBytes = 0;
    errno_t rc = memset_s(m_memPartFlag, sizeof(m_memPartFlag), 0, SONIC_PART_MAX_NUM * sizeof(m_memPartFlag[0]));
    securec_check(rc, "\0", "\0");
    m_diskPartNum = 0;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned
     * by first VectorEngine.
     */
    if (m_runtime->js.ps.righttree->chgParam == NULL) {
        VecExecReScan(m_runtime->js.ps.righttree);
    }
}

/*
 * @Description: reset memory control information.
 */
void SonicRadixHashJoin::resetMemoryControl()
{
    /* do not reset the total memory, since it might be successfully spread in the last partition scan */
    m_memControl.sysBusy = false;
    m_memControl.spillToDisk = false;
    m_memControl.spillNum = 0;
    m_memControl.spreadNum = 0;
    m_memControl.availMem = 0;
    m_memControl.allocatedMem = 0;

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t rc = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * @Description: record partition information into log file
 * @in buildside - build side or not (probe side)
 * @in partIdx - index of the partition if any to be repartitioned
 * @in istart - start index of the partitions
 * @in iend - end index of the partitions
 * @return - void
 */
void SonicRadixHashJoin::recordPartitionInfo(bool buildside, int32 partIdx, uint32 istart, uint32 iend)
{

    const char* side = buildside ? "Build" : "Probe";
    SonicHashPartition** source = (buildside ? m_innerPartitions : m_outerPartitions);
    SonicHashFilePartition* partition = NULL;

    Assert(istart >= 0 && istart <= iend);
    if (partIdx >= 0) {
        Assert(source[partIdx]);
        Assert(source[partIdx]->m_status == partitionStatusFile);
        partition = (SonicHashFilePartition*)(source[partIdx]);

        elog(DEBUG2,
            "[SonicVecHashJoin_RePartition] [%s Side]: "
            "Partition: %d, Spill Time: %d, File Num: %d, Total File Size: %lu, Total Memory: %lu, "
            "RePartition Num %u.",
            side,
            partIdx,
            m_pLevel[partIdx],
            partition->m_fileNum,
            partition->m_size,
            m_memControl.totalMem,
            iend - istart);
    }

    for (uint32 i = istart; i < iend; i++) {
        if (!buildside && (partIdx == -1) && source[i] == NULL) {
            /*
             * This case happens when recording probe partitions for the first time,
             * in this case, some outer partitions are not initialized,
             * cause coresponding inner partitions are already loaded in the memory.
             */
            continue;
        }

        Assert(source[i]);
        Assert(source[i]->m_status == partitionStatusFile);

        partition = (SonicHashFilePartition*)(source[i]);
        elog(DEBUG2,
            "[SonicVecHashJoin] [%s Side]: Partition: %u, Spill Time: %d, File Num: %d, Total File Size: %lu, Total "
            "Memory: %lu",
            side,
            i,
            m_pLevel[i],
            partition->m_fileNum,
            partition->m_size,
            m_memControl.totalMem);
    }
}

/*
 * @Description: analyze partition info
 * @in buildside - true for build side, false for probe side (including reparition)
 * @in startPartIdx - the first partition index to analyze
 * @in endPartIdx - the last partition index to analyze
 * @in totalFileNum - pointer to store all the file number
 * @in totalFileSize - pointer to store all the file size
 * @in partSizeMin - pointer to store the min total file size of a partition among all partitions
 * @in partSizeMax - pointer to store the max total file size of a partition among all partitions
 * @in spillPartNum - the actual number of partitions that are spilt to disk
 */
void SonicRadixHashJoin::analyzePartition(bool buildside, uint32 startPartIdx, uint32 endPartIdx, int64* totalFileNum,
    long* totalFileSize, long* partSizeMin, long* partSizeMax, int* spillPartNum)
{
    SonicHashFilePartition* file_partition = NULL;
    uint16 file_num = 0;
    size_t part_size = 0;
    uint32 part_num = 0;
    SonicHashPartition** source_part = buildside ? m_innerPartitions : m_outerPartitions;

    for (uint32 part_idx = startPartIdx; part_idx < endPartIdx; ++part_idx) {
        file_partition = (SonicHashFilePartition*)source_part[part_idx];
        if (file_partition == NULL) {
            continue;
        }
        part_num++;

        if (u_sess->attr.attr_sql.enable_sonic_optspill) {
            uint16 actual_filenum = 0;
            for (int i = 0; i < file_partition->m_fileNum; i++) {
                if (file_partition->m_files[i] != NULL)
                    actual_filenum += 1;
            }
            file_num = actual_filenum;
        } else {
            file_num = file_partition->m_fileNum;
        }

        part_size = file_partition->m_size;

        (*totalFileNum) += (int64)file_num;
        (*totalFileSize) += part_size;
        (*partSizeMin) = Min((size_t)(*partSizeMin), part_size);
        (*partSizeMax) = Max((size_t)(*partSizeMax), part_size);
    }

    (*spillPartNum) += part_num;
}

/*
 * @Description: report instrument info
 * @in reportType - reportTypeBuild for build side,
 * 	reportTypeProbe for probe side,
 * 	reportTypeRepartition for repartition.
 * @in totalPartNum - the number of total partitions at the report time,
 * 	for reportTypeBuild and reportTypeProbe, it should be the m_partNum,
 * 	for repoartTypeRepartition, it should be m_partNum plus the number of repartitions.
 * @in rePartIdx - partition index that is repartitioned, only set when reportTypeRepartition.
 */
void SonicRadixHashJoin::reportSorthashinfo(ReportType reportType, uint32 totalPartNum, int32 rePartIdx)
{
    if (!(HAS_INSTR(&m_runtime->js, true)))
        return;

    switch (reportType) {
        case reportTypeBuild:
            /* report the inner partitions info from index 0 to m_partNum */
            if (m_strategy == MEMORY_HASH) {
                INSTR->sorthashinfo.hash_writefile = false;
            } else {
                Assert(m_strategy == GRACE_HASH);
                Assert(m_partNum > 0);
                INSTR->sorthashinfo.spill_innerSizePartMin = INT64_MAX;
                analyzePartition(true,
                    0,
                    totalPartNum,
                    &(INSTR->sorthashinfo.hash_innerFileNum),
                    &(INSTR->sorthashinfo.spill_innerSize),
                    &(INSTR->sorthashinfo.spill_innerSizePartMin),
                    &(INSTR->sorthashinfo.spill_innerSizePartMax),
                    &(INSTR->sorthashinfo.spill_innerPartNum));
                INSTR->sorthashinfo.hash_writefile = true;
                INSTR->sorthashinfo.hash_partNum = totalPartNum;
                INSTR->sorthashinfo.hash_FileNum = INSTR->sorthashinfo.hash_innerFileNum;
                INSTR->sorthashinfo.spill_size = INSTR->sorthashinfo.spill_innerSize;
            }
            break;

        case reportTypeProbe:
            /* report the info of those outer partitions that are not NULL */
            INSTR->sorthashinfo.spill_outerSizePartMin = INT64_MAX;
            analyzePartition(false,
                0,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_outerFileNum),
                &(INSTR->sorthashinfo.spill_outerSize),
                &(INSTR->sorthashinfo.spill_outerSizePartMin),
                &(INSTR->sorthashinfo.spill_outerSizePartMax),
                &(INSTR->sorthashinfo.spill_outerPartNum));
            if (INSTR->sorthashinfo.spill_outerPartNum == 0)
                INSTR->sorthashinfo.spill_outerSizePartMin = 0;
            INSTR->sorthashinfo.hash_FileNum += INSTR->sorthashinfo.hash_outerFileNum;
            INSTR->sorthashinfo.spill_size += INSTR->sorthashinfo.spill_outerSize;
            break;

        case reportTypeRepartition:
            /* report the repartitioned partition info and the new partitions' info */
            Assert(rePartIdx >= 0);
            Assert(rePartIdx < (int32)m_partNum);

            if (m_pLevel[rePartIdx] >= WARNING_SPILL_TIME) {
                INSTR->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
            }

            INSTR->sorthashinfo.hash_spillNum = Max(m_pLevel[rePartIdx], INSTR->sorthashinfo.hash_spillNum);
            analyzePartition(true,
                m_partNum,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_innerFileNum),
                &(INSTR->sorthashinfo.spill_innerSize),
                &(INSTR->sorthashinfo.spill_innerSizePartMin),
                &(INSTR->sorthashinfo.spill_innerSizePartMax),
                &(INSTR->sorthashinfo.spill_innerPartNum));

            analyzePartition(false,
                m_partNum,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_outerFileNum),
                &(INSTR->sorthashinfo.spill_outerSize),
                &(INSTR->sorthashinfo.spill_outerSizePartMin),
                &(INSTR->sorthashinfo.spill_outerSizePartMax),
                &(INSTR->sorthashinfo.spill_outerPartNum));

            INSTR->sorthashinfo.hash_partNum = totalPartNum;
            INSTR->sorthashinfo.hash_FileNum =
                INSTR->sorthashinfo.hash_innerFileNum + INSTR->sorthashinfo.hash_outerFileNum;
            INSTR->sorthashinfo.spill_size = INSTR->sorthashinfo.spill_innerSize + INSTR->sorthashinfo.spill_outerSize;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    (errmsg("SonicRadixHashJoin reportSorthashinfo: Unsupport report type %d", reportType))));
            break;
    }
}

/*
 * @Description: Save probe data to file partitions.
 */
template <bool complicateJoinKey, bool optspill>
void SonicRadixHashJoin::saveProbePartition()
{
    SonicHashFilePartition* file_partition = NULL;
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    uint32 part_idx;
    int row_idx;

    for (uint32 i = 0; i < m_diskPartNum; i++) {
        part_idx = m_diskPartIdx[i].partIdx;
        row_idx = m_diskPartIdx[i].rowIdx;
        file_partition = (SonicHashFilePartition*)m_outerPartitions[part_idx];
        Assert(file_partition && file_partition->m_status == partitionStatusFile);

        for (int j = 0; j < m_probeOp.cols; j++) {
            arr_val = &m_outRawBatch->m_arr[j].m_vals[row_idx];
            null_flag = &m_outRawBatch->m_arr[j].m_flag[row_idx];
            file_partition->putVal<optspill>(arr_val, null_flag, j);
        }
        /* Put hash value if complicateJoinKey is true. */
        if (complicateJoinKey)
            file_partition->putHash(&m_hashVal[row_idx]);

        file_partition->m_rows += 1;
    }

    m_diskPartNum = 0;
}

/* Charge the shared tables to the worker that builds them. Probe state and
 * buffers remain private to each worker. */
void SonicRadixHashJoin::checkRadixMemory()
{
    uint64 allocated = 0, available = 0;
    calcHashContextSize(m_memControl.hashContext, &allocated, &available);
    calcHashContextSize(m_radixSharedBuildContext, &allocated, &available);
    m_memControl.allocatedMem = allocated;
    m_memControl.availMem = available;
    m_peakBytes = Max(m_peakBytes, allocated - available);
    if (allocated > m_memControl.totalMem)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
            errmsg("radix hash join exceeded its worker memory budget"),
            errdetail("Plan node %d: allocated %llu bytes, used %llu bytes, budget %llu bytes.",
                m_runtime->js.ps.plan->plan_node_id, (unsigned long long)allocated,
                (unsigned long long)(allocated - available), (unsigned long long)m_memControl.totalMem),
            errhint("Disable enable_radix_hashjoin to use spilling, or increase work memory.")));
}
