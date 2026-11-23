#ifndef VEC_CACHE_COORD_H
#define VEC_CACHE_COORD_H

#include "postgres.h"
extern void VecCacheCoordRecordStreamBatch(bool local, bool preferred);
extern void VecCacheCoordObserveRadixProbe(uint64 queryId, int planId);
extern bool VecCacheCoordHasProbeOverlap(uint64 queryId, bool requirePressure);
extern int RhjLlcDomain(int cpu, uint64* cacheBytes = NULL, unsigned* lanes = NULL);
extern bool VecCacheCoordSampleProbe(uint64 queryId, int planId);
extern void VecCacheCoordObserveProbeFootprint(uint64 queryId, int planId, uint64 bytes, bool shared, bool allowYield);
extern char* VecCacheCoordStatsJson(void);

#ifdef ENABLE_HTAP
struct ChipletCacheActivity;
extern ChipletCacheActivity* CreateChipletCacheActivity(MemoryContext context);
extern bool ChipletCacheObserveAggregates();
extern void ChipletCachePublishAggregate(int domain, int node, int lane, uint64 bytes);
extern uint64 ChipletCacheRecentAggregateBytes(int domain);
extern void ChipletCachePublishRadix(int domain, int node, uint32 part, uint64 bytes);
extern uint64 ChipletCacheRecentRadixBytes(int domain, int ownNode);

enum class RhjOverlapPeerKind { RadixJoin, Aggregate, StreamCopy };
extern uint64* VecCacheCoordOverlapPeerStage(int node, RhjOverlapPeerKind kind);
extern void VecCacheCoordChangePeerStage(uint64* stage, bool entering);
struct RhjStreamTimelineConfig {
    int outputStream;
    int firstLlc;
    int secondLlc;
    bool includes(int llc) const { return llc == firstLlc || llc == secondLlc; }
};
extern const RhjStreamTimelineConfig* RhjStreamTimeline();

template <typename Work>
inline void VecCacheCoordRunPeerStage(uint64* stage, Work work)
{
    VecCacheCoordChangePeerStage(stage, true);
    PG_TRY();
    {
        work();
    }
    PG_CATCH();
    {
        VecCacheCoordChangePeerStage(stage, false);
        PG_RE_THROW();
    }
    PG_END_TRY();
    VecCacheCoordChangePeerStage(stage, false);
}
#endif

enum class HarmfulCachePhase { Scan, Aggregate };
extern bool VecCacheCoordHarmfulEnabled();
extern int VecCacheCoordEnterHarmfulPhase(HarmfulCachePhase phase);
extern void VecCacheCoordLeaveHarmfulPhase(int domain, HarmfulCachePhase phase);
extern void VecCacheCoordCheckHarmfulProbe();
extern bool VecCacheCoordHasHarmfulPeers(int domain, unsigned lanes);

template <typename Work>
inline void VecCacheCoordRunHarmfulPhase(HarmfulCachePhase phase, Work work)
{
    int domain = VecCacheCoordEnterHarmfulPhase(phase);
    if (domain < 0) {
        work();
        return;
    }
    PG_TRY();
    {
        work();
    }
    PG_CATCH();
    {
        VecCacheCoordLeaveHarmfulPhase(domain, phase);
        PG_RE_THROW();
    }
    PG_END_TRY();
    VecCacheCoordLeaveHarmfulPhase(domain, phase);
}

#endif
