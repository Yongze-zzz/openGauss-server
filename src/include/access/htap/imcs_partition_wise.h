#ifndef IMCS_PARTITION_WISE_H
#define IMCS_PARTITION_WISE_H

#include "postgres.h"
#include "utils/hsearch.h"

class CU;
struct IMCSDesc;
struct RelationData;

/* Configuration is separate from the query switch: layout survives SET off. */
extern HTAB* ImcsPartitionWiseParseDataNodes();
extern HTAB* ImcsNpsBuildDataNodes(RelationData* rel);
extern void ImcsNpsSetIdentity(HTAB* map, IMCSDesc* desc);
extern bool ImcsNpsIdentityMatches(HTAB* map, const IMCSDesc* desc);
extern int ImcsCurrentDataNode(IMCSDesc* desc);
extern void ImcsPartitionWiseFollowNode(int node);
extern void ImcsPartitionWiseFollowNodeLane(int node, int localOrdinal);
extern bool ImcsPartitionWisePlaceConsumer(int node, int localOrdinal);
extern int ImcsPartitionWiseDataNode(HTAB* map, Oid relid);
extern void ImcsPartitionWisePoolEnter();
extern void ImcsPartitionWisePoolCounters(uint64* entries, uint32* cpus);
extern void ImcsPartitionWiseWorkerEnter(int worker, int workers, int stage = 0);
extern void ImcsPartitionWiseGetWorkerCpus(int workers, int* workerCpus);
extern void ImcsPartitionWiseWorkerLeave();
extern int ImcsPartitionWiseWorkerNode(int worker, int workers);
extern int ImcsPartitionWiseBucketOwner(int ordinal, int workers);
extern void ImcsPartitionWiseHashInit(int workers);
extern bool ImcsPartitionWiseHashInitForKey(int workers, Oid relation, AttrNumber attribute);
extern int ImcsPartitionWiseStreamGroups(
    int workers, int* workerGroups, int* representatives, int* groupNodes);
extern int ImcsPartitionWiseHashWorker(uint64 hashValue, int workers);
extern int ImcsPartitionWisePathStride(int workers);
extern void ImcsLocalTaskRecord(bool reassigned);
extern void ImcsLocalTaskCounters(uint64* claims, uint64* reassigned);
extern int ImcsPartitionWiseWorker(int partition, int nodeOrdinal, int dataNode, int workers);
extern void ImcsPartitionWisePlaceCU(CU* cu, int dataNode);

#endif
