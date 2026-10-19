#ifndef IMCS_PARTITION_WISE_H
#define IMCS_PARTITION_WISE_H

#include "postgres.h"
#include "utils/hsearch.h"

class CU;

/* Configuration is separate from the query switch: layout survives SET off. */
extern HTAB* ImcsPartitionWiseParseDataNodes();
extern int ImcsPartitionWiseDataNode(HTAB* map, Oid relid);
extern void ImcsPartitionWiseWorkerEnter(int worker, int workers);
extern void ImcsPartitionWiseWorkerLeave();
extern int ImcsPartitionWiseWorker(int partition, int nodeOrdinal, int dataNode, int workers);
extern void ImcsPartitionWisePlaceCU(CU* cu, int dataNode);

#endif