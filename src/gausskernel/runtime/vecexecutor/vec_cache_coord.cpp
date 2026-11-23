/* Recent radix probe activity and counters for local Stream scheduling. */
#include "postgres.h"
#include "miscadmin.h"
#include "distributelayer/streamCore.h"
#include "vecexecutor/vec_cache_coord.h"
#include <sched.h>
#include <time.h>

namespace {
struct alignas(128) StreamBatchCounters {
    uint64 local;
    uint64 remote;
    uint64 preferred;
};
static StreamBatchCounters streamBatches[CPU_SETSIZE + 1];
static uint64 overlapChecks = 0;
static uint64 overlapHits = 0;
static uint64 pressureSamples = 0;
static uint64 probeYields = 0;
static uint64 probeYieldNs = 0;

struct alignas(128) HarmfulCacheDomain {
    uint32 scans;
    uint32 aggregates;
    uint64 lastDefer;
    uint64 checks;
    uint64 overlap;
    uint64 hits;
    uint64 defers;
    uint64 deferNs;
};
static HarmfulCacheDomain harmfulDomains[CPU_SETSIZE];
static THR_LOCAL bool harmfulPhaseActive = false;
const uint64 harmfulWorkerCooldownNs = UINT64CONST(2000000);
const uint64 harmfulDomainCooldownNs = UINT64CONST(200000);
const long harmfulDeferUs = 20;

const uint64 probeWindowNs = 20 * UINT64CONST(1000000);
const uint64 refreshIntervalNs = 2 * UINT64CONST(1000000);

struct ProbeSample {
    int planId;
    uint64 lastSeen;
};

struct alignas(128) DomainProbes {
    uint32 held;
    uint64 queryId;
    ProbeSample samples[8];
};
static DomainProbes domains[CPU_SETSIZE];

// Sampled footprint, based on distinct CPU lanes recently probing this table.
struct ProbeFootprint {
    int planId;
    bool shared;
    uint64 bytes;
    uint64 windowStart;
    uint64 lastSeen;
    cpu_set_t cpus;
};

struct alignas(128) DomainPressure {
    uint32 held;
    uint64 queryId;
    ProbeFootprint probes[8];
};
static DomainPressure pressure[CPU_SETSIZE];
const uint64 pressureWindowNs = UINT64CONST(1000000);
const uint64 yieldIntervalNs = 2 * pressureWindowNs;
const long probeYieldUs = 40;

uint64 MonotonicNs()
{
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * UINT64CONST(1000000000) + (uint64)ts.tv_nsec;
}

template <typename Domain>
bool TryLock(Domain& domain)
{
    uint32 expected = 0;
    return __atomic_compare_exchange_n(&domain.held, &expected, 1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

template <typename Domain>
void Unlock(Domain& domain)
{
    __atomic_store_n(&domain.held, 0, __ATOMIC_RELEASE);
}
}

#ifdef ENABLE_HTAP
/* Samples belong to a StreamNodeGroup and expire independently in each LLC.
 * Colliding keys can replace an estimate and never affect tuple ownership. */
static const unsigned CHIPLET_AGG_SAMPLE_SLOTS = 64;
static const uint32 CHIPLET_AGG_SAMPLE_MS = 4;
static const uint64 CHIPLET_RADIX_SAMPLE_UNIT = 4096;

struct ChipletCacheActivity {
    uint64 aggregate[CPU_SETSIZE][CHIPLET_AGG_SAMPLE_SLOTS];
    uint64 radix[CPU_SETSIZE][CHIPLET_AGG_SAMPLE_SLOTS];
};

ChipletCacheActivity* CreateChipletCacheActivity(MemoryContext context)
{
    return (ChipletCacheActivity*)MemoryContextAllocZero(context, sizeof(ChipletCacheActivity));
}

static ChipletCacheActivity* CurrentChipletCacheActivity()
{
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    return group == NULL ? NULL : group->m_chipletCacheActivity;
}

bool ChipletCacheObserveAggregates()
{
    return CurrentChipletCacheActivity() != NULL;
}

void ChipletCachePublishAggregate(int domain, int node, int lane, uint64 bytes)
{
    ChipletCacheActivity* activity = CurrentChipletCacheActivity();
    if (activity == NULL || domain < 0 || domain >= CPU_SETSIZE || node <= 0 || bytes == 0)
        return;
    /* A shared aggregate uses lane -1 to account for its LLC state once. */
    uint32 key = ((uint32)node << 11) ^ (uint32)(lane + 1);
    key ^= key >> 16;
    key *= 0x7feb352dU;
    key ^= key >> 15;
    unsigned slot = key % CHIPLET_AGG_SAMPLE_SLOTS;
    uint64 stamp = (uint32)(MonotonicNs() / UINT64CONST(1000000));
    uint64 sample = (stamp << 32) | Min(bytes, (uint64)UINT32_MAX);
    __atomic_store_n(&activity->aggregate[domain][slot], sample, __ATOMIC_RELAXED);
}

uint64 ChipletCacheRecentAggregateBytes(int domain)
{
    ChipletCacheActivity* activity = CurrentChipletCacheActivity();
    if (activity == NULL || domain < 0 || domain >= CPU_SETSIZE)
        return 0;
    uint32 now = (uint32)(MonotonicNs() / UINT64CONST(1000000));
    uint64 bytes = 0;
    for (const uint64& entry : activity->aggregate[domain]) {
        uint64 sample = __atomic_load_n(&entry, __ATOMIC_RELAXED);
        if (now - (uint32)(sample >> 32) < CHIPLET_AGG_SAMPLE_MS)
            bytes += (uint32)sample;
    }
    return bytes;
}

void ChipletCachePublishRadix(int domain, int node, uint32 part, uint64 bytes)
{
    ChipletCacheActivity* activity = CurrentChipletCacheActivity();
    if (activity == NULL || domain < 0 || domain >= CPU_SETSIZE || node <= 0 || node > UINT16_MAX || bytes == 0)
        return;
    /* Workers sharing a radix partition refresh the same cache estimate. */
    uint32 key = (uint32)node * 0x9e3779b9U ^ part * 0x85ebca6bU;
    key ^= key >> 16;
    uint64 units = Min(1 + (bytes - 1) / CHIPLET_RADIX_SAMPLE_UNIT, (uint64)UINT16_MAX);
    uint64 stamp = (uint32)(MonotonicNs() / UINT64CONST(1000000));
    uint64 sample = (stamp << 32) | ((uint64)node << 16) | units;
    __atomic_store_n(&activity->radix[domain][key % CHIPLET_AGG_SAMPLE_SLOTS], sample, __ATOMIC_RELAXED);
}

uint64 ChipletCacheRecentRadixBytes(int domain, int ownNode)
{
    ChipletCacheActivity* activity = CurrentChipletCacheActivity();
    if (activity == NULL || domain < 0 || domain >= CPU_SETSIZE)
        return 0;
    uint32 now = (uint32)(MonotonicNs() / UINT64CONST(1000000));
    uint64 bytes = 0;
    for (const uint64& entry : activity->radix[domain]) {
        uint64 sample = __atomic_load_n(&entry, __ATOMIC_RELAXED);
        if (now - (uint32)(sample >> 32) < CHIPLET_AGG_SAMPLE_MS && (int)(uint16)(sample >> 16) != ownNode)
            bytes += (uint16)sample * CHIPLET_RADIX_SAMPLE_UNIT;
    }
    return bytes;
}

uint64* VecCacheCoordOverlapPeerStage(int node, RhjOverlapPeerKind kind)
{
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group == NULL || group->m_rhjProbeOverlap == NULL || !u_sess->attr.attr_sql.enable_cache_trace ||
        u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind != (int)kind ||
        u_sess->attr.attr_sql.imcs_rhj_overlap_peer_node != node)
        return NULL;
    int llc = RhjLlcDomain(sched_getcpu());
    return llc >= 0 && llc < CPU_SETSIZE ? &group->m_rhjProbeOverlap[llc].heterogeneous : NULL;
}

/* The generation changes whenever the worker count crosses zero. */
void VecCacheCoordChangePeerStage(uint64* stage, bool entering)
{
    uint64 previous = __atomic_load_n(stage, __ATOMIC_RELAXED);
    uint64 next;
    do {
        next = entering ? previous + 1 : previous - 1;
        if ((uint32)previous == (entering ? 0U : 1U))
            next += UINT64CONST(1) << 32;
    } while (!__atomic_compare_exchange_n(stage, &previous, next, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
}
#endif

bool VecCacheCoordHarmfulEnabled()
{
    return u_sess->attr.attr_sql.enable_imcs_cache_harmful &&
        u_sess->attr.attr_sql.enable_imcs_nps && !u_sess->attr.attr_sql.enable_imcs_adapt;
}

int VecCacheCoordEnterHarmfulPhase(HarmfulCachePhase phase)
{
    if (harmfulPhaseActive)
        return -1; // Count a worker once, even if an expression invokes another executor.
    int id = RhjLlcDomain(sched_getcpu());
    if (id < 0 || id >= CPU_SETSIZE)
        return -1;
    HarmfulCacheDomain& domain = harmfulDomains[id];
    uint32* active = phase == HarmfulCachePhase::Scan ? &domain.scans : &domain.aggregates;
    __atomic_fetch_add(active, 1, __ATOMIC_RELAXED);
    harmfulPhaseActive = true;
    return id;
}

void VecCacheCoordLeaveHarmfulPhase(int id, HarmfulCachePhase phase)
{
    HarmfulCacheDomain& domain = harmfulDomains[id];
    uint32* active = phase == HarmfulCachePhase::Scan ? &domain.scans : &domain.aggregates;
    __atomic_fetch_sub(active, 1, __ATOMIC_RELAXED);
    harmfulPhaseActive = false;
}

static bool HarmfulPeerCounts(unsigned scans, unsigned aggregates, unsigned lanes)
{
    unsigned minimum = Max(1U, (lanes + 7) / 8);
    return scans >= minimum && aggregates >= minimum && scans + aggregates >= (lanes + 1) / 2;
}

bool VecCacheCoordHasHarmfulPeers(int id, unsigned lanes)
{
    if (id < 0 || id >= CPU_SETSIZE || lanes == 0)
        return false;
    const HarmfulCacheDomain& domain = harmfulDomains[id];
    return HarmfulPeerCounts(__atomic_load_n(&domain.scans, __ATOMIC_RELAXED),
        __atomic_load_n(&domain.aggregates, __ATOMIC_RELAXED), lanes);
}

void VecCacheCoordCheckHarmfulProbe()
{
    static THR_LOCAL uint64 lastDefer = 0;
    unsigned lanes = 0;
    int id = RhjLlcDomain(sched_getcpu(), NULL, &lanes);
    if (id < 0 || id >= CPU_SETSIZE || lanes == 0 || harmfulPhaseActive)
        return;

    HarmfulCacheDomain& domain = harmfulDomains[id];
    __atomic_fetch_add(&domain.checks, 1, __ATOMIC_RELAXED);
    unsigned scans = __atomic_load_n(&domain.scans, __ATOMIC_RELAXED);
    unsigned aggregates = __atomic_load_n(&domain.aggregates, __ATOMIC_RELAXED);
    if (scans != 0 && aggregates != 0)
        __atomic_fetch_add(&domain.overlap, 1, __ATOMIC_RELAXED);
    // Use chiplet capacity to scale the original Scan + Aggregate rule.
    if (!HarmfulPeerCounts(scans, aggregates, lanes))
        return;
    __atomic_fetch_add(&domain.hits, 1, __ATOMIC_RELAXED);
    if (!u_sess->attr.attr_sql.enable_imcs_cache_harmful_defer)
        return;

    uint64 now = MonotonicNs();
    uint64 lastDomainDefer = __atomic_load_n(&domain.lastDefer, __ATOMIC_RELAXED);
    if (now - lastDefer < harmfulWorkerCooldownNs || now < lastDomainDefer ||
        now - lastDomainDefer < harmfulDomainCooldownNs ||
        !__atomic_compare_exchange_n(&domain.lastDefer, &lastDomainDefer, now, false,
            __ATOMIC_RELAXED, __ATOMIC_RELAXED))
        return;
    lastDefer = now;
    /* We already fetched the input. A bounded pause preserves the worker's
     * affinity and the owner of the probe hash table. */
    pg_usleep(harmfulDeferUs);
    __atomic_fetch_add(&domain.defers, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&domain.deferNs, MonotonicNs() - now, __ATOMIC_RELAXED);
    CHECK_FOR_INTERRUPTS();
}

bool VecCacheCoordSampleProbe(uint64 queryId, int planId)
{
    static THR_LOCAL uint64 previousQuery = 0;
    static THR_LOCAL int previousPlan = 0;
    static THR_LOCAL unsigned batches = 0;
    if (queryId == 0 || planId <= 0)
        return false;
    if (previousQuery != queryId || previousPlan != planId) {
        previousQuery = queryId;
        previousPlan = planId;
        batches = 0;
    }
    return (batches++ % 32) == 0;
}

void VecCacheCoordObserveProbeFootprint(uint64 queryId, int planId, uint64 bytes, bool shared, bool allowYield)
{
    static THR_LOCAL uint64 lastYield = 0;
    if (bytes == 0)
        return;
    int cpu = sched_getcpu();
    uint64 cacheBytes = 0;
    int id = RhjLlcDomain(cpu, &cacheBytes);
    if (id < 0 || id >= CPU_SETSIZE || cacheBytes == 0)
        return;
    DomainPressure& domain = pressure[id];
    if (!TryLock(domain))
        return;
    uint64 now = MonotonicNs();
    if (domain.queryId != queryId) {
        domain.queryId = queryId;
        for (ProbeFootprint& probe : domain.probes)
            probe.lastSeen = 0;
    }
    ProbeFootprint* self = &domain.probes[0];
    for (ProbeFootprint& probe : domain.probes) {
        if (probe.lastSeen != 0 && probe.planId == planId) {
            self = &probe;
            break;
        }
        if (probe.lastSeen < self->lastSeen)
            self = &probe;
    }
    if (self->lastSeen == 0 || self->planId != planId || now - self->windowStart >= pressureWindowNs) {
        CPU_ZERO(&self->cpus);
        self->windowStart = now;
        self->bytes = 0;
    }
    self->planId = planId;
    self->shared = shared;
    self->bytes = Max(bytes, self->bytes);
    self->lastSeen = now;
    CPU_SET(cpu, &self->cpus);

    if (!allowYield) {
        Unlock(domain);
        return;
    }
    uint64 footprint = self->bytes * (self->shared ? 1 : CPU_COUNT(&self->cpus));
    uint64 totalBytes = footprint;
    bool smallerPeer = false;
    for (const ProbeFootprint& probe : domain.probes) {
        if (&probe == self || probe.lastSeen == 0 || now - probe.lastSeen > pressureWindowNs)
            continue;
        uint64 peerBytes = probe.bytes * (probe.shared ? 1 : CPU_COUNT(&probe.cpus));
        totalBytes += peerBytes;
        smallerPeer |= peerBytes < footprint;
    }
    Unlock(domain);

    __atomic_fetch_add(&pressureSamples, 1, __ATOMIC_RELAXED);
    if (smallerPeer && totalBytes > 3 * cacheBytes / 4 && now - lastYield >= yieldIntervalNs) {
        CHECK_FOR_INTERRUPTS();
        lastYield = now;
        /* At this point the child returned a batch and no Stream quota or
         * coordinator lock is held. Limit the requested delay per worker. */
        pg_usleep(probeYieldUs);
        __atomic_fetch_add(&probeYields, 1, __ATOMIC_RELAXED);
        __atomic_fetch_add(&probeYieldNs, MonotonicNs() - now, __ATOMIC_RELAXED);
        CHECK_FOR_INTERRUPTS();
    }
}

void VecCacheCoordObserveRadixProbe(uint64 queryId, int planId)
{
    int id = RhjLlcDomain(sched_getcpu());
    if (id < 0 || id >= CPU_SETSIZE)
        return;
    DomainProbes& domain = domains[id];
    if (!TryLock(domain))
        return;
    if (domain.queryId != queryId) {
        domain.queryId = queryId;
        for (ProbeSample& sample : domain.samples)
            sample.lastSeen = 0;
    }
    ProbeSample* slot = &domain.samples[0];
    for (ProbeSample& sample : domain.samples) {
        if (sample.lastSeen != 0 && sample.planId == planId) {
            slot = &sample;
            break;
        }
        if (sample.lastSeen < slot->lastSeen)
            slot = &sample;
    }
    slot->planId = planId;
    slot->lastSeen = MonotonicNs();
    Unlock(domain);
}

bool VecCacheCoordHasProbeOverlap(uint64 queryId, bool requirePressure)
{
    static THR_LOCAL uint64 cachedQuery = 0;
    static THR_LOCAL uint64 checkedAt = 0;
    static THR_LOCAL int cachedCpu = -1;
    static THR_LOCAL bool overlap = false;
    static THR_LOCAL bool cachedRequirePressure = false;
    if (queryId == 0)
        return false;
    uint64 now = MonotonicNs();
    int cpu = sched_getcpu();
    if (queryId != cachedQuery || cpu != cachedCpu || requirePressure != cachedRequirePressure ||
        now - checkedAt >= (requirePressure ? pressureWindowNs : refreshIntervalNs)) {
        cachedQuery = queryId;
        cachedRequirePressure = requirePressure;
        cachedCpu = cpu;
        checkedAt = now;
        overlap = false;
        uint64 cacheBytes = 0;
        int id = RhjLlcDomain(cpu, requirePressure ? &cacheBytes : NULL);
        if (id >= 0 && id < CPU_SETSIZE && requirePressure && cacheBytes != 0 && TryLock(pressure[id])) {
            DomainPressure& domain = pressure[id];
            unsigned active = 0;
            uint64 bytes = 0;
            if (domain.queryId == queryId) {
                for (const ProbeFootprint& probe : domain.probes) {
                    if (probe.lastSeen == 0 || probe.lastSeen > now || now - probe.lastSeen > pressureWindowNs)
                        continue;
                    ++active;
                    /* Workers probing the same shared table use one copy on this LLC. */
                    bytes += probe.bytes * (probe.shared ? 1 : CPU_COUNT(&probe.cpus));
                }
            }
            overlap = active >= 2 && bytes > 3 * cacheBytes / 4;
            Unlock(domain);
        } else if (id >= 0 && id < CPU_SETSIZE && !requirePressure && TryLock(domains[id])) {
            DomainProbes& domain = domains[id];
            unsigned active = 0;
            if (domain.queryId == queryId) {
                for (const ProbeSample& sample : domain.samples) {
                    if (sample.lastSeen != 0 && now >= sample.lastSeen && now - sample.lastSeen <= probeWindowNs)
                        ++active;
                }
            }
            overlap = active >= 2;
            Unlock(domain);
        }
        __atomic_fetch_add(&overlapChecks, 1, __ATOMIC_RELAXED);
        if (overlap)
            __atomic_fetch_add(&overlapHits, 1, __ATOMIC_RELAXED);
    }
    return overlap;
}

void VecCacheCoordRecordStreamBatch(bool local, bool preferred)
{
    int cpu = sched_getcpu();
    StreamBatchCounters& counters = streamBatches[cpu >= 0 && cpu < CPU_SETSIZE ? cpu : CPU_SETSIZE];
    __atomic_fetch_add(local ? &counters.local : &counters.remote, 1, __ATOMIC_RELAXED);
    if (preferred)
        __atomic_fetch_add(&counters.preferred, 1, __ATOMIC_RELAXED);
}

char* VecCacheCoordStatsJson(void)
{
    uint64 local = 0;
    uint64 remote = 0;
    uint64 preferred = 0;
    for (const StreamBatchCounters& counters : streamBatches) {
        local += __atomic_load_n(&counters.local, __ATOMIC_RELAXED);
        remote += __atomic_load_n(&counters.remote, __ATOMIC_RELAXED);
        preferred += __atomic_load_n(&counters.preferred, __ATOMIC_RELAXED);
    }
    uint64 harmfulChecks = 0, harmfulOverlap = 0, harmfulHits = 0, harmfulDefers = 0, harmfulWaitNs = 0;
    for (const HarmfulCacheDomain& domain : harmfulDomains) {
        harmfulChecks += __atomic_load_n(&domain.checks, __ATOMIC_RELAXED);
        harmfulOverlap += __atomic_load_n(&domain.overlap, __ATOMIC_RELAXED);
        harmfulHits += __atomic_load_n(&domain.hits, __ATOMIC_RELAXED);
        harmfulDefers += __atomic_load_n(&domain.defers, __ATOMIC_RELAXED);
        harmfulWaitNs += __atomic_load_n(&domain.deferNs, __ATOMIC_RELAXED);
    }
    return psprintf("\"cache_coord_stream_local_batches\":%llu,\"cache_coord_stream_remote_batches\":%llu,"
        "\"cache_coord_stream_preferred_batches\":%llu,"
        "\"cache_coord_overlap_checks\":%llu,\"cache_coord_overlap_hits\":%llu,"
        "\"cache_coord_probe_samples\":%llu,\"cache_coord_probe_yields\":%llu,\"cache_coord_probe_yield_us\":%llu,"
        "\"cache_harmful_checks\":%llu,\"cache_harmful_overlap\":%llu,\"cache_harmful_hits\":%llu,"
        "\"cache_harmful_defers\":%llu,"
        "\"cache_harmful_defer_us\":%llu",
        (unsigned long long)local, (unsigned long long)remote, (unsigned long long)preferred,
        (unsigned long long)__atomic_load_n(&overlapChecks, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&overlapHits, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&pressureSamples, __ATOMIC_RELAXED),
        (unsigned long long)__atomic_load_n(&probeYields, __ATOMIC_RELAXED),
        (unsigned long long)(__atomic_load_n(&probeYieldNs, __ATOMIC_RELAXED) / 1000),
        (unsigned long long)harmfulChecks, (unsigned long long)harmfulOverlap,
        (unsigned long long)harmfulHits, (unsigned long long)harmfulDefers,
        (unsigned long long)(harmfulWaitNs / 1000));
}
