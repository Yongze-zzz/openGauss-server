
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/htap/imcs_partition_wise.h"
#include "access/htap/imcs_partition_assignment.h"
#include "storage/cu.h"
#include <ctype.h>
#include <numa.h>
#include <numaif.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

static const int MAX_WISE_NODES = sizeof(unsigned long) * 8;
static THR_LOCAL int wiseWorkers = 0;
static THR_LOCAL int wiseWorkerNodes[CPU_SETSIZE];
static THR_LOCAL int wiseNodeCounts[MAX_WISE_NODES];
static THR_LOCAL int wiseNodeOffsets[MAX_WISE_NODES];
static THR_LOCAL int wiseNodeWorkers[CPU_SETSIZE];
static THR_LOCAL cpu_set_t savedWiseCpus;
static THR_LOCAL bool wiseCpuChanged = false;
static THR_LOCAL long wisePageSize = 0;

static const char* SkipWiseSpace(const char* text)
{
    while (isspace((unsigned char)*text))
          ++text;
    return text;
}

static unsigned long ReadWiseNumber(const char** text)
{
    const char* start = SkipWiseSpace(*text);
    char* end = NULL;
    errno = 0;
     unsigned long value = strtoul(start, &end, 10);
     if (!isdigit((unsigned char)*start) || end == start || errno != 0)
         ereport(ERROR, (errmsg("Invalid number in IMCS PartitionWise configuration")));
     *text = SkipWiseSpace(end);
     return value;
}


/* 每次解析 populate 配置、初始化扫描 worker 时，各读取一次允许节点集合。 */
static unsigned long GetWiseAllowedNodes()
{
    unsigned long allowed = 0;
    if (numa_available() < 0 ||
        get_mempolicy(NULL, &allowed, MAX_WISE_NODES, NULL, MPOL_F_MEMS_ALLOWED) != 0)
        ereport(ERROR, (errmsg("Cannot read NUMA nodes allowed for IMCS PartitionWise")));
    return allowed;
}

/* 循环里只检查范围和位图，不再查询系统。 */
static void CheckWiseNode(int node, unsigned long allowed)
{
    if (node < 0 || node >= MAX_WISE_NODES || !(allowed & (1UL << node)))
        ereport(ERROR, (errmsg("NUMA node %d is unavailable to IMCS PartitionWise", node)));
}

struct ImcsPartitionNumaEntry {
    Oid relid;
    int node;
};

HTAB* ImcsPartitionWiseParseDataNodes()
{
    const char* text = SkipWiseSpace(u_sess->attr.attr_sql.imcs_partition_numa_map);
    if (*text == '\0')
        return NULL;

    unsigned long allowed = GetWiseAllowedNodes();
    HASHCTL info;
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(ImcsPartitionNumaEntry);
    info.hcxt = CurrentMemoryContext;
    HTAB* map = hash_create("IMCS partition NUMA map", 32, &info,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    PG_TRY();
    {
        while (*text != '\0') {
            CHECK_FOR_INTERRUPTS();
            unsigned long oid = ReadWiseNumber(&text);
            if (oid == 0 || oid > UINT_MAX || *text++ != ':')
                ereport(ERROR, (errmsg("Use partition_oid:numa_node pairs in imcs_partition_numa_map")));
            unsigned long node = ReadWiseNumber(&text);
            if (node >= (unsigned long)MAX_WISE_NODES)
                ereport(ERROR, (errmsg("Invalid node in imcs_partition_numa_map")));
            CheckWiseNode((int)node, allowed);

            Oid relid = (Oid)oid;
            bool found = false;
            ImcsPartitionNumaEntry* entry =
                (ImcsPartitionNumaEntry*)hash_search(map, &relid, HASH_ENTER, &found);
            if (found)
                ereport(ERROR, (errmsg("Duplicate partition in imcs_partition_numa_map")));
            entry->node = (int)node;

            if (*text == '\0')
                break;
            if (*text++ != ',' || *SkipWiseSpace(text) == '\0')
                ereport(ERROR, (errmsg("Use comma-separated partition_oid:numa_node pairs")));
        }
    }
    PG_CATCH();
    {
        hash_destroy(map);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return map;
}

int ImcsPartitionWiseDataNode(HTAB* map, Oid relid)
{
    if (map == NULL)
        return -1; /* 空配置沿用原生分配。 */
    ImcsPartitionNumaEntry* entry =
        (ImcsPartitionNumaEntry*)hash_search(map, &relid, HASH_FIND, NULL);
    if (entry == NULL)
        ereport(ERROR, (errmsg("Partition %u is missing from imcs_partition_numa_map", relid)));
    return entry->node;
}

void ImcsPartitionWiseWorkerEnter(int worker, int workers)
{
    if (wiseWorkers != 0) {
        if (wiseWorkers != workers)
            ereport(ERROR, (errmsg("IMCS PartitionWise DOP changed inside a Stream task")));
        return; /* Multiple scans in one Stream share the same CPU mapping. */
    }
    if (workers <= 1 || workers > CPU_SETSIZE || worker < 0 || worker >= workers)
        ereport(ERROR, (errmsg("Invalid IMCS PartitionWise worker/DOP")));

    cpu_set_t allowed;
    if (sched_getaffinity(getpid(), sizeof(allowed), &allowed) != 0)
        ereport(ERROR, (errmsg("Cannot read server CPU affinity: %m")));
    int cpus[CPU_SETSIZE];
    int count = 0;
    cpu_set_t seen;
    CPU_ZERO(&seen);
    const char* text = SkipWiseSpace(u_sess->attr.attr_sql.imcs_partition_scan_cpus);
    while (*text != '\0') {
        unsigned long cpu = ReadWiseNumber(&text);
        if (cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &allowed) || CPU_ISSET(cpu, &seen))
            ereport(ERROR, (errmsg("IMCS scan CPUs must be distinct CPUs in the server affinity mask")));
        CPU_SET(cpu, &seen);
        cpus[count++] = (int)cpu;
        if (*text == '\0')
            break;
        if (*text++ != ',' || *SkipWiseSpace(text) == '\0')
            ereport(ERROR, (errmsg("Use a comma-separated CPU list in imcs_partition_scan_cpus")));
    }
    if (count < workers)
        ereport(ERROR, (errmsg("imcs_partition_scan_cpus needs at least %d CPUs", workers)));

    unsigned long allowedNodes = GetWiseAllowedNodes();
    for (int i = 0; i < workers; ++i) {
        /* Spread a smaller DOP across the configured CPU list deterministically. */
        int cpu = cpus[(int)((int64)i * count / workers)];
        wiseWorkerNodes[i] = numa_node_of_cpu(cpu);
        CheckWiseNode(wiseWorkerNodes[i], allowedNodes);
    }
    /*  */
    ImcsPartitionInitWorkers(wiseWorkerNodes, workers, MAX_WISE_NODES,
        wiseNodeCounts, wiseNodeOffsets, wiseNodeWorkers);
    int cpu = cpus[(int)((int64)worker * count / workers)];
    int rc = pthread_getaffinity_np(pthread_self(), sizeof(savedWiseCpus), &savedWiseCpus);
    if (rc != 0)
        ereport(ERROR, (errmsg("Cannot save IMCS scan CPU affinity (error %d)", rc)));
    cpu_set_t fixed;
    CPU_ZERO(&fixed);
    CPU_SET(cpu, &fixed);
    rc = pthread_setaffinity_np(pthread_self(), sizeof(fixed), &fixed);
    if (rc != 0)
        ereport(ERROR, (errmsg("Cannot bind IMCS worker %d to CPU %d (error %d)", worker, cpu, rc)));
    wiseCpuChanged = true; /* Error cleanup must restore even before initialization completes. */
    wiseWorkers = workers;
    ereport(DEBUG1, (errmsg("IMCS PartitionWise worker=%d dop=%d cpu=%d node=%d",
        worker, workers, cpu, wiseWorkerNodes[worker])));
}

void ImcsPartitionWiseWorkerLeave()
{
    if (wiseCpuChanged) {
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(savedWiseCpus), &savedWiseCpus);
        if (rc != 0)
            ereport(FATAL, (errmsg("Cannot restore IMCS scan CPU affinity (error %d)", rc)));
        wiseCpuChanged = false;
    }
    wiseWorkers = 0;
}

int ImcsPartitionWiseWorker(int partition, int nodeOrdinal, int dataNode, int workers)
{
    if (wiseWorkers != workers)
        ereport(ERROR, (errmsg("IMCS PartitionWise workers have not been initialized")));
    if (dataNode < 0 || dataNode >= MAX_WISE_NODES)
        ereport(ERROR, (errmsg("Invalid IMCS PartitionWise data node %d", dataNode)));
    return ImcsPartitionWorker(partition, nodeOrdinal, dataNode,
        wiseNodeCounts, wiseNodeOffsets, wiseNodeWorkers, workers);
}

void ImcsPartitionWisePlaceCU(CU* cu, int dataNode)
{
    if (dataNode < 0 || cu->m_partitionWiseBuffer != NULL || cu->m_srcBuf == NULL)
        return;
    /* The NUMA map already checks allowed nodes; guard the bit shift below. */
    if (dataNode >= MAX_WISE_NODES)
        ereport(ERROR, (errmsg("Invalid IMCS PartitionWise data node %d", dataNode)));
    if (wisePageSize == 0) {
        long pageSize = sysconf(_SC_PAGESIZE);
        if (pageSize <= 0)
            ereport(ERROR, (errmsg("Cannot read system page size")));
        wisePageSize = pageSize;
    }
    /* Keep CU's extra eight readable bytes and align the optional offset array. */
    Size offset = MAXALIGN((Size)cu->m_srcBufSize + 8);
    Size bytes = offset + cu->m_offsetSize;
    if (cu->m_offsetSize < 0 || bytes > INT_MAX - (Size)wisePageSize)
        ereport(ERROR, (errmsg("Invalid or oversized NUMA IMCS CU buffer")));
    Size page = (Size)wisePageSize;

    Size length = ((bytes + page - 1) / page) * page;
    void* buffer = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buffer == MAP_FAILED)
        ereport(ERROR, (errmsg("Cannot allocate NUMA IMCS CU buffer: %m")));
    unsigned long mask = 1UL << dataNode;
    if (mbind(buffer, length, MPOL_BIND, &mask, MAX_WISE_NODES, 0) != 0) {
        int savedErrno = errno;
        munmap(buffer, length);
        errno = savedErrno;
        ereport(ERROR, (errmsg("Cannot bind IMCS CU buffer to node %d: %m", dataNode)));
    }
    /* Disable THP so neighbouring VMAs cannot combine different partition layouts. */
    (void)madvise(buffer, length, MADV_NOHUGEPAGE);
    char* data = (char*)buffer;
    ptrdiff_t sourceOffset = cu->m_srcData - cu->m_srcBuf;
    bool hasNulls = cu->m_nulls != NULL;
    memcpy(data, cu->m_srcBuf, cu->m_srcBufSize);
    if (cu->m_offset != NULL)
        memcpy(data + offset, cu->m_offset, cu->m_offsetSize);
    CStoreMemAlloc::Pfree(cu->m_srcBuf, !cu->m_inCUCache);
    if (cu->m_offset != NULL)
        CStoreMemAlloc::Pfree(cu->m_offset, !cu->m_inCUCache);
    cu->m_srcBuf = data;
    cu->m_srcData = data + sourceOffset;
    cu->m_nulls = hasNulls ? (unsigned char*)data : NULL;
    cu->m_offset = cu->m_offsetSize > 0 ? (int32*)(data + offset) : NULL;
    cu->m_partitionWiseBuffer = buffer;
    cu->m_partitionWiseBufferSize = length;
}

void CU::FreePartitionWiseBuffer()
{
    if (m_partitionWiseBuffer == NULL)
        return;
    if (munmap(m_partitionWiseBuffer, m_partitionWiseBufferSize) != 0)
        ereport(FATAL, (errmsg("Cannot release NUMA IMCS CU buffer: %m")));
    m_partitionWiseBuffer = NULL;
    m_partitionWiseBufferSize = 0;
    m_srcBuf = NULL;
    m_srcBufSize = 0;
    m_srcData = NULL;
    m_nulls = NULL;
    m_offset = NULL;
    m_offsetSize = 0;
}
