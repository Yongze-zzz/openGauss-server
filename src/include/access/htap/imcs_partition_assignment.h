
#ifndef IMCS_PARTITION_ASSIGNMENT_H
#define IMCS_PARTITION_ASSIGNMENT_H

inline void ImcsPartitionInitWorkers(const int* nodes, int workers, int nodeCount,
    int* counts, int* offsets, int* workerIds)
{
    for (int node = 0; node < nodeCount; ++node)
        counts[node] = 0;
    for (int worker = 0; worker < workers; ++worker)
        ++counts[nodes[worker]];

    int offset = 0;
    for (int node = 0; node < nodeCount; ++node) {
        offsets[node] = offset;
        offset += counts[node];
    }
    /* worker round robin */
    for (int worker = 0; worker < workers; ++worker)
        workerIds[offsets[nodes[worker]]++] = worker;
    /* offsets */
    for (int node = 0; node < nodeCount; ++node)
        offsets[node] -= counts[node];
}

inline int ImcsPartitionWorker(int partition, int nodeOrdinal, int dataNode,
    const int* counts, const int* offsets, const int* workerIds, int workers)
{
    int localWorkers = counts[dataNode];
    /* No local CPU: deterministic remote fallback still scans each partition once. */
    if (localWorkers == 0)
        return partition % workers;
    return workerIds[offsets[dataNode] + nodeOrdinal % localWorkers];
}

#endif