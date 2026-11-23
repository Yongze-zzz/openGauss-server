
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/htap/imcs_partition_wise.h"
#include "access/htap/imcs_partition_assignment.h"
#include "storage/cu.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/partitionmap_gs.h"
#include "access/htap/imcs_adaptive.h"
#include "access/htap/imcs_numa_cpu.h"
#include <ctype.h>
#include <numa.h>
#include <numaif.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

static const int MAX_WISE_NODES = sizeof(unsigned long) * 8;
static THR_LOCAL int wiseWorkers = 0;
static THR_LOCAL int wiseTopologyWorkers = 0;
static THR_LOCAL int wiseNpsPartitions = 0;
static THR_LOCAL int wiseWorkerNodes[CPU_SETSIZE];
static THR_LOCAL int wiseWorkerCpus[CPU_SETSIZE];
static THR_LOCAL int wiseNodeCounts[MAX_WISE_NODES];
static THR_LOCAL int wiseNodeOffsets[MAX_WISE_NODES];
static THR_LOCAL int wiseNodeWorkers[CPU_SETSIZE];
static THR_LOCAL int wiseActiveNodeCount = 0;
static THR_LOCAL int wiseActiveNodes[MAX_WISE_NODES];
static THR_LOCAL cpu_set_t savedWiseCpus;
static THR_LOCAL bool wiseCpuChanged = false;
static THR_LOCAL bool wiseMemoryChanged = false;
static THR_LOCAL int wiseCurrentCpu = -1;
static THR_LOCAL int wiseCurrentNode = -1;
static THR_LOCAL bool wiseNodeAffinity = false;
static THR_LOCAL bool wiseConsumerPlaced = false;
static THR_LOCAL cpu_set_t wiseAllowedNodeCpus[MAX_WISE_NODES];
static THR_LOCAL int savedWiseMode;
static THR_LOCAL unsigned long savedWiseNodes;
static THR_LOCAL long wisePageSize = 0;
// One update per successful task entry, never per tuple/vector.
static uint64 baselinePoolEntries = 0;
static uint64 localTaskClaims = 0, localTaskReassigned = 0;
void ImcsLocalTaskRecord(bool reassigned)
{
    __atomic_fetch_add(&localTaskClaims, 1, __ATOMIC_RELAXED);
    if (reassigned) __atomic_fetch_add(&localTaskReassigned, 1, __ATOMIC_RELAXED);
}
void ImcsLocalTaskCounters(uint64* claims, uint64* reassigned)
{
    *claims = __atomic_load_n(&localTaskClaims, __ATOMIC_RELAXED);
    *reassigned = __atomic_load_n(&localTaskReassigned, __ATOMIC_RELAXED);
}
static uint32 baselinePoolCpus = 0;

void ImcsPartitionWisePoolCounters(uint64* entries, uint32* cpus)
{
    *entries = __atomic_load_n(&baselinePoolEntries, __ATOMIC_ACQUIRE);
    *cpus = __atomic_load_n(&baselinePoolCpus, __ATOMIC_RELAXED);
}

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
    Oid group;
    int ordinal;
    int partitions;
    int members;
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
            entry->group = InvalidOid;
            entry->ordinal = -1;
            entry->partitions = 0;
            entry->members = 0;

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

int ImcsCurrentDataNode(IMCSDesc* desc)
{
    uint32 node = __atomic_load_n(&desc->currentNumaNode, __ATOMIC_ACQUIRE);
    return node == UINT_MAX ? -1 : (int)node;
}

void ImcsNpsSetIdentity(HTAB* map, IMCSDesc* desc)
{
    if (map == NULL)
        return;
    ImcsPartitionNumaEntry* entry = (ImcsPartitionNumaEntry*)hash_search(map, &desc->relOid, HASH_FIND, NULL);
    if (entry != NULL) {
        desc->numaGroup = entry->group;
        desc->numaOrdinal = entry->ordinal;
        desc->numaPartitions = entry->partitions;
        desc->numaGroupMembers = entry->members;
    }
}

/* Read-only experiment preflight; does not alter the placement policy. */
bool ImcsNpsIdentityMatches(HTAB* map, const IMCSDesc* desc)
{
    ImcsPartitionNumaEntry* entry = (ImcsPartitionNumaEntry*)hash_search(map, &desc->relOid, HASH_FIND, NULL);
    return entry != NULL && desc->dataNumaNode == entry->node && desc->numaGroup == entry->group &&
        desc->numaOrdinal == entry->ordinal && desc->numaPartitions == entry->partitions &&
        desc->numaGroupMembers == entry->members;
}

/* NPS never silently repartitions an existing table. Validate the physical hash
 * keys and bucket boundaries, then co-place corresponding buckets at populate. */
HTAB* ImcsNpsBuildDataNodes(Relation root)
{
    if (ENABLE_DSS || !IS_SINGLE_NODE || g_instance.attr.attr_memory.enable_borrow_memory)
        ereport(ERROR, (errmsg("NPS requires local single-node IMCS without borrowed memory")));
    if (*u_sess->attr.attr_sql.imcs_partition_numa_map != '\0')
        ereport(ERROR, (errmsg("NPS generates its own layout; clear imcs_partition_numa_map")));
    const char* spec = u_sess->attr.attr_sql.imcs_nps_groups;
    if (*SkipWiseSpace(spec) == '\0')
        ereport(ERROR, (errmsg("NPS requires imcs_nps_groups with matching physical join-key hash partitions")));
    Oid groupId = RelationGetRelid(root);
    int groupMembers = 1;
    List* seen = NIL;
    char* copy = pstrdup(spec);
    char* groupSave = NULL;
    for (char* group = strtok_r(copy, ";", &groupSave); group != NULL; group = strtok_r(NULL, ";", &groupSave)) {
        char* memberSave = NULL;
        Oid minimum = InvalidOid;
        HashPartitionMap* reference = NULL;
        Relation referenceRel = NULL;
        Oid referenceCollation = InvalidOid;
        bool memberOfGroup = false;
        int members = 0;
        for (char* member = strtok_r(group, ",", &memberSave); member != NULL; member = strtok_r(NULL, ",", &memberSave)) {
            char* colon = strrchr(member, ':');
            if (colon == NULL)
                ereport(ERROR, (errmsg("Use schema.table:join_key members in imcs_nps_groups")));
            *colon++ = '\0';
            Oid oid = DatumGetObjectId(DirectFunctionCall1(regclassin, CStringGetDatum(member)));
            if (list_member_oid(seen, oid))
                ereport(ERROR, (errmsg("Relation %u appears twice in imcs_nps_groups", oid)));
            seen = lappend_oid(seen, oid);
            Relation rel = heap_open(oid, AccessShareLock);
            int attnum = get_attnum(oid, colon);
            if (!RelationIsPartitioned(rel) || !PartitionMapIsHash(rel->partMap) ||
                rel->partMap->partitionKey->dim1 != 1 || attnum <= 0 ||
                rel->partMap->partitionKey->values[0] != attnum)
                ereport(ERROR, (errmsg("NPS member %s must be physically HASH partitioned on %s", member, colon)));
            HashPartitionMap* map = (HashPartitionMap*)rel->partMap;
            Oid collation = rel->rd_att->attrs[attnum - 1].attcollation;
            if (reference != NULL) {
                if (reference->hashElementsNum != map->hashElementsNum ||
                    reference->base.partitionKeyDataType[0] != map->base.partitionKeyDataType[0] ||
                    referenceCollation != collation)
                    ereport(ERROR, (errmsg("NPS group hash partition counts, key types and collations must match")));
                for (int i = 0; i < map->hashElementsNum; ++i) {
                    if (!equal(reference->hashElements[i].boundary[0], map->hashElements[i].boundary[0]))
                        ereport(ERROR, (errmsg("NPS group hash bucket boundaries must match")));
                }
            } else {
                /* Keep the reference relation locked and referenced until validation ends. */
                reference = map;
                referenceRel = rel;
                referenceCollation = collation;
            }
            minimum = !OidIsValid(minimum) || oid < minimum ? oid : minimum;
            memberOfGroup |= oid == RelationGetRelid(root);
            ++members;
            /* Retain the first member's relcache reference through validation.
             * NoLock retains DDL protection through transaction end. */
            if (rel != referenceRel)
                heap_close(rel, NoLock);
        }
        if (referenceRel != NULL)
            heap_close(referenceRel, NoLock);
        if (members < 2)
            ereport(ERROR, (errmsg("Each NPS co-partition group needs at least two tables")));
        if (memberOfGroup) {
            groupId = minimum;
            groupMembers = members;
        }
    }
    if (seen == NIL)
        ereport(ERROR, (errmsg("imcs_nps_groups must contain at least one co-partitioned table group")));
    list_free(seen);
    pfree(copy);
    if (RelationIsPartitioned(root) && !PartitionMapIsHash(root->partMap))
        ereport(ERROR, (errmsg("NPS currently supports only one-level HASH partitioned IMCS tables")));
    /* Unlisted tables are independent groups. */

    int nodes[MAX_WISE_NODES];
    int count = 0;
    unsigned long used = 0;
    unsigned long allowed = GetWiseAllowedNodes();
    cpu_set_t server;
    if (sched_getaffinity(getpid(), sizeof(server), &server) != 0)
        ereport(ERROR, (errmsg("Cannot read NPS server CPU affinity: %m")));
    const char* cpus = SkipWiseSpace(u_sess->attr.attr_sql.imcs_partition_scan_cpus);
    while (*cpus != '\0') {
        unsigned long cpu = ReadWiseNumber(&cpus);
        if (cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &server))
            ereport(ERROR, (errmsg("NPS CPUs must be in the server CPU affinity")));
        int node = numa_node_of_cpu((int)cpu);
        CheckWiseNode(node, allowed);
        used |= 1UL << node;
        if (*cpus == '\0')
            break;
        if (*cpus++ != ',')
            ereport(ERROR, (errmsg("Use comma-separated NPS CPU IDs")));
    }
    for (int node = 0; node < MAX_WISE_NODES; ++node)
        if (used & (1UL << node))
            nodes[count++] = node;
    if (count < 2)
        ereport(ERROR, (errmsg("NPS requires CPUs from at least two NUMA nodes")));
    HASHCTL info;
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(ImcsPartitionNumaEntry);
    info.hcxt = CurrentMemoryContext;
    HTAB* result = hash_create("NPS generated layout", 32, &info, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    ImcsPartitionNumaEntry* rootEntry = (ImcsPartitionNumaEntry*)hash_search(result, &root->rd_id, HASH_ENTER, NULL);
    rootEntry->node = nodes[0];
    rootEntry->group = InvalidOid;
    rootEntry->ordinal = -1;
    rootEntry->partitions = 0;
    rootEntry->members = 0;
    if (RelationIsPartitioned(root)) {
        HashPartitionMap* map = (HashPartitionMap*)root->partMap;
        for (int i = 0; i < map->hashElementsNum; ++i) {
            Oid oid = map->hashElements[i].partitionOid;
            ImcsPartitionNumaEntry* entry = (ImcsPartitionNumaEntry*)hash_search(result, &oid, HASH_ENTER, NULL);
            entry->node = nodes[i % count];
            entry->group = groupId;
            entry->ordinal = i;
            entry->partitions = map->hashElementsNum;
            entry->members = groupMembers;
        }
    }
    return result;
}

/* Data-unaware partition scans need the same CPU resource pool as NPS.
 * The thread-pool group initially confines Streams to its own NUMA node.
 * Widen only this task's own affinity before ExecutorStart; keep its PGPROC,
 * thread-pool ownership, memory policy and partition ownership unchanged.
 * The existing normal/error cleanup restores the shared worker's old mask. */
void ImcsPartitionWisePoolEnter()
{
    if (imcsnuma::RestoreLauncherMemoryPolicy() != 0)
        ereport(ERROR, (errmsg("Cannot preserve baseline launcher memory binding: %m")));
    if (wiseCpuChanged || wiseWorkers != 0)
        ereport(ERROR, (errmsg("IMCS worker affinity was not restored before the next Stream task")));
    cpu_set_t server, pool, previous;
    if (sched_getaffinity(getpid(), sizeof(server), &server) != 0)
        ereport(ERROR, (errmsg("Cannot read IMCS server CPU pool: %m")));
    CPU_ZERO(&pool);
    const char* text = SkipWiseSpace(u_sess->attr.attr_sql.imcs_partition_scan_cpus);
    if (*text == '\0') {
        pool = server;
    } else {
        while (*text != '\0') {
            unsigned long cpu = ReadWiseNumber(&text);
            if (cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &server) || CPU_ISSET(cpu, &pool))
                ereport(ERROR, (errmsg("IMCS pool CPUs must be distinct CPUs in the server affinity mask")));
            CPU_SET(cpu, &pool);
            if (*text == '\0') break;
            if (*text++ != ',' || *SkipWiseSpace(text) == '\0')
                ereport(ERROR, (errmsg("Use comma-separated CPU IDs for the IMCS scan pool")));
        }
    }
    if (CPU_COUNT(&pool) == 0)
        ereport(ERROR, (errmsg("IMCS scan CPU pool is empty")));
    int rc = pthread_getaffinity_np(pthread_self(), sizeof(previous), &previous);
    if (rc != 0)
        ereport(ERROR, (errmsg("Cannot save IMCS pool worker affinity (error %d)", rc)));
    savedWiseCpus = previous;
    rc = pthread_setaffinity_np(pthread_self(), sizeof(pool), &pool);
    if (rc != 0)
        ereport(ERROR, (errmsg("Cannot use the IMCS scan CPU pool (error %d)", rc)));
    wiseCpuChanged = true;
    cpu_set_t actual;
    rc = pthread_getaffinity_np(pthread_self(), sizeof(actual), &actual);
    if (rc != 0 || !CPU_EQUAL(&actual, &pool))
        ereport(ERROR, (errmsg("IMCS pool worker affinity verification failed (error %d)", rc)));
    __atomic_store_n(&baselinePoolCpus, (uint32)CPU_COUNT(&actual), __ATOMIC_RELAXED);
    __atomic_add_fetch(&baselinePoolEntries, 1, __ATOMIC_RELEASE);
    ereport(DEBUG1, (errmsg("IMCS partition scan CPU pool: worker=%d pool_cpus=%d previous_cpus=%d",
        (int)u_sess->stream_cxt.smp_id, CPU_COUNT(&pool), CPU_COUNT(&previous))));
}

static int ReadWiseWorkerCpuList(int workers, int* cpus)
{
    cpu_set_t allowed;
    if (sched_getaffinity(getpid(), sizeof(allowed), &allowed) != 0)
        ereport(ERROR, (errmsg("Cannot read server CPU affinity: %m")));
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
    return count;
}

static void MapWiseWorkerCpus(int workers, const int* cpus, int count, int* workerCpus)
{
    for (int i = 0; i < workers; ++i)
        workerCpus[i] = cpus[(int)((int64)i * count / workers)];
}

void ImcsPartitionWiseGetWorkerCpus(int workers, int* workerCpus)
{
    if (workers <= 1 || workers > CPU_SETSIZE)
        ereport(ERROR, (errmsg("Invalid IMCS PartitionWise DOP")));
    int cpus[CPU_SETSIZE];
    int count = ReadWiseWorkerCpuList(workers, cpus);
    MapWiseWorkerCpus(workers, cpus, count, workerCpus);
}

static int ImcsExchangeStageCpu(int stage, int worker, int workers, const int* cpus, int count)
{
    const char* config = u_sess->attr.attr_sql.imcs_chiplet_stage_cpus;
    if (stage <= 0 || config == NULL || *config == '\0')
        return -1;

    const char* text = SkipWiseSpace(config);
    while (*text != '\0') {
        unsigned long node = ReadWiseNumber(&text);
        if (*text++ != ':')
            ereport(ERROR, (errmsg("Use plan_node:cpu,cpu entries in imcs_chiplet_stage_cpus")));
        if (node == (unsigned long)stage) {
            cpu_set_t allowed, seen;
            CPU_ZERO(&allowed);
            CPU_ZERO(&seen);
            for (int i = 0; i < count; ++i)
                CPU_SET(cpus[i], &allowed);
            int placement[CPU_SETSIZE];
            int width = 0;
            do {
                unsigned long cpu = ReadWiseNumber(&text);
                if (cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &allowed) || CPU_ISSET(cpu, &seen))
                    ereport(ERROR, (errmsg("Exchange stage CPUs must be distinct entries in imcs_partition_scan_cpus")));
                CPU_SET(cpu, &seen);
                placement[width++] = (int)cpu;
                if (*text == '\0' || *text == ';')
                    break;
                if (*text++ != ',')
                    ereport(ERROR, (errmsg("Use a comma-separated CPU list for an exchange stage")));
            } while (true);
            return placement[(int)((int64)worker * width / workers)];
        }
        text = strchr(text, ';');
        if (text == NULL)
            break;
        text = SkipWiseSpace(text + 1);
    }
    return -1;
}

void ImcsPartitionWiseWorkerEnter(int worker, int workers, int stage)
{
    if (wiseWorkers != 0) {
        if (wiseWorkers != workers)
            ereport(ERROR, (errmsg("IMCS PartitionWise DOP changed inside a Stream task")));
        return; /* Multiple scans in one Stream share the same CPU mapping. */
    }
    if (workers <= 1 || workers > CPU_SETSIZE || worker < 0 || worker >= workers)
        ereport(ERROR, (errmsg("Invalid IMCS PartitionWise worker/DOP")));
    int cpus[CPU_SETSIZE];
    int count = ReadWiseWorkerCpuList(workers, cpus);
    MapWiseWorkerCpus(workers, cpus, count, wiseWorkerCpus);

    unsigned long allowedNodes = GetWiseAllowedNodes();
    for (int node = 0; node < MAX_WISE_NODES; ++node)
        CPU_ZERO(&wiseAllowedNodeCpus[node]);
    for (int i = 0; i < count; ++i) {
        int node = numa_node_of_cpu(cpus[i]);
        CheckWiseNode(node, allowedNodes);
        CPU_SET(cpus[i], &wiseAllowedNodeCpus[node]);
    }
    for (int i = 0; i < workers; ++i) {
        /* Spread a smaller DOP across the configured CPU list deterministically. */
        int cpu = wiseWorkerCpus[i];
        wiseWorkerNodes[i] = numa_node_of_cpu(cpu);
        CheckWiseNode(wiseWorkerNodes[i], allowedNodes);
    }
    /*  */
    ImcsPartitionInitWorkers(wiseWorkerNodes, workers, MAX_WISE_NODES,
        wiseNodeCounts, wiseNodeOffsets, wiseNodeWorkers);
    wiseActiveNodeCount = 0;
    for (int node = 0; node < MAX_WISE_NODES; ++node) {
        if (wiseNodeCounts[node] > 0)
            wiseActiveNodes[wiseActiveNodeCount++] = node;
    }
    wiseTopologyWorkers = workers;
    int stageCpu = ImcsExchangeStageCpu(stage, worker, workers, cpus, count);
    int cpu = stageCpu < 0 ? wiseWorkerCpus[worker] : stageCpu;
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
    wiseConsumerPlaced = false;
    wiseCurrentCpu = cpu;
    wiseCurrentNode = stageCpu < 0 ? wiseWorkerNodes[worker] : numa_node_of_cpu(cpu);
    CheckWiseNode(wiseCurrentNode, allowedNodes);
    wiseNodeAffinity = false;
    if (u_sess->attr.attr_sql.enable_imcs_nps) {
        if (get_mempolicy(&savedWiseMode, &savedWiseNodes, MAX_WISE_NODES, NULL, 0) != 0)
            ereport(ERROR, (errmsg("Cannot save NPS memory policy: %m")));
        unsigned long mask = 1UL << wiseCurrentNode;
        if ((imcsnuma::LauncherBindNodes() ? imcsnuma::RestoreLauncherMemoryPolicy() :
            set_mempolicy(MPOL_PREFERRED, &mask, MAX_WISE_NODES)) != 0)
            ereport(ERROR, (errmsg("Cannot set NPS worker memory preference: %m")));
        wiseMemoryChanged = true;
    }
    ereport(DEBUG1, (errmsg("IMCS PartitionWise worker=%d dop=%d cpu=%d node=%d",
        worker, workers, cpu, wiseWorkerNodes[worker])));
    if (stageCpu >= 0) {
        ereport(LOG, (errmsg("chiplet_stage_placement node=%d lane=%d cpu=%d numa=%d", stage, worker, cpu,
            wiseCurrentNode), errhidestmt(true), ErrOutToClient(false)));
    }
}

static void ImcsPartitionWiseFollowNodeInternal(int node, int localOrdinal)
{
    if (wiseWorkers == 0 || node < 0 || node >= MAX_WISE_NODES || CPU_COUNT(&wiseAllowedNodeCpus[node]) == 0)
        ereport(ERROR, (errmsg("Cannot follow adaptive IMCS placement to NUMA node %d", node)));
    bool adaptive = u_sess->attr.attr_sql.enable_imcs_adapt;
    cpu_set_t fixed;
    CPU_ZERO(&fixed);
    int cpu = -1;
    if (adaptive) {
        if (wiseNodeAffinity && wiseCurrentNode == node)
            return;
        // Let the OS balance existing threads across configured CPUs inside
        // the destination node. No queue stealing or partition ownership change.
        fixed = wiseAllowedNodeCpus[node];
    } else {
        int lane = localOrdinal >= 0 ? localOrdinal : (int)u_sess->stream_cxt.smp_id;
        if (wiseNodeCounts[node] > 0) {
            int worker = wiseNodeWorkers[wiseNodeOffsets[node] + lane % wiseNodeCounts[node]];
            cpu = wiseWorkerCpus[worker];
        } else {
            for (int i = 0; i < CPU_SETSIZE; ++i)
                if (CPU_ISSET(i, &wiseAllowedNodeCpus[node])) { cpu = i; break; }
        }
        if (!wiseNodeAffinity && wiseCurrentCpu == cpu)
            return;
        CPU_SET(cpu, &fixed);
    }
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(fixed), &fixed);
    if (rc != 0)
        ereport(ERROR, (errmsg("Cannot follow IMCS NUMA placement (error %d)", rc)));
    wiseCurrentCpu = cpu;
    wiseCurrentNode = node;
    wiseNodeAffinity = adaptive;
    unsigned long mask = 1UL << node;
    if ((imcsnuma::LauncherBindNodes() ? imcsnuma::RestoreLauncherMemoryPolicy() :
        set_mempolicy(MPOL_PREFERRED, &mask, MAX_WISE_NODES)) != 0)
        ereport(ERROR, (errmsg("Cannot follow IMCS NUMA memory preference: %m")));
}

void ImcsPartitionWiseFollowNode(int node)
{
    ImcsPartitionWiseFollowNodeInternal(node, -1);
}

void ImcsPartitionWiseFollowNodeLane(int node, int localOrdinal)
{
    if (localOrdinal < 0)
        ereport(ERROR, (errmsg("Invalid NPS local consumer ordinal")));
    ImcsPartitionWiseFollowNodeInternal(node, localOrdinal);
}

bool ImcsPartitionWisePlaceConsumer(int node, int localOrdinal)
{
    if (wiseConsumerPlaced)
        return false;
    ImcsPartitionWiseFollowNodeLane(node, localOrdinal);
    wiseConsumerPlaced = true;
    return true;
}

void ImcsPartitionWiseWorkerLeave()
{
    ImcsAdaptWorkerLeave();
    if (wiseMemoryChanged) {
        if (set_mempolicy(savedWiseMode, &savedWiseNodes, MAX_WISE_NODES) != 0)
            ereport(FATAL, (errmsg("Cannot restore NPS worker memory policy: %m")));
        wiseMemoryChanged = false;
    }
    if (wiseCpuChanged) {
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(savedWiseCpus), &savedWiseCpus);
        if (rc != 0)
            ereport(FATAL, (errmsg("Cannot restore IMCS scan CPU affinity (error %d)", rc)));
        wiseCpuChanged = false;
    }
    wiseWorkers = 0;
    wiseCurrentCpu = -1;
    wiseCurrentNode = -1;
    wiseNodeAffinity = false;
    wiseConsumerPlaced = false;
}

int ImcsPartitionWiseWorkerNode(int worker, int workers)
{
    if (wiseWorkers != workers || worker < 0 || worker >= workers)
        ereport(ERROR, (errmsg("Invalid IMCS local task worker")));
    return wiseWorkerNodes[worker];
}

int ImcsPartitionWiseBucketOwner(int ordinal, int workers)
{
    if (wiseWorkers != workers || wiseActiveNodeCount == 0 || ordinal < 0)
        ereport(ERROR, (errmsg("Invalid IMCS hash bucket ownership")));
    int node = wiseActiveNodes[ordinal % wiseActiveNodeCount];
    int local = (ordinal / wiseActiveNodeCount) % wiseNodeCounts[node];
    return wiseNodeWorkers[wiseNodeOffsets[node] + local];
}

/* Prepare the Stream mapping independently of scan-worker entry. A join branch
 * without an IMCS scan still has to route a key to exactly the same consumer as
 * its sibling branch; falling back on only one side silently loses matches. */
void ImcsPartitionWiseHashInit(int workers)
{
    if (workers <= 1 || workers > CPU_SETSIZE)
        ereport(ERROR, (errmsg("Invalid NPS Stream DOP")));
    cpu_set_t allowed, seen;
    if (sched_getaffinity(getpid(), sizeof(allowed), &allowed) != 0)
        ereport(ERROR, (errmsg("Cannot read server CPU affinity: %m")));
    CPU_ZERO(&seen);
    int cpus[CPU_SETSIZE];
    int count = 0;
    const char* text = SkipWiseSpace(u_sess->attr.attr_sql.imcs_partition_scan_cpus);
    while (*text != '\0') {
        unsigned long cpu = ReadWiseNumber(&text);
        if (cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &allowed) || CPU_ISSET(cpu, &seen))
            ereport(ERROR, (errmsg("NPS Stream CPUs must be distinct CPUs in the server affinity mask")));
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
        int cpu = cpus[(int)((int64)i * count / workers)];
        wiseWorkerCpus[i] = cpu;
        wiseWorkerNodes[i] = numa_node_of_cpu(cpu);
        CheckWiseNode(wiseWorkerNodes[i], allowedNodes);
    }
    ImcsPartitionInitWorkers(wiseWorkerNodes, workers, MAX_WISE_NODES,
        wiseNodeCounts, wiseNodeOffsets, wiseNodeWorkers);
    wiseActiveNodeCount = 0;
    for (int node = 0; node < MAX_WISE_NODES; ++node)
        if (wiseNodeCounts[node] > 0)
            wiseActiveNodes[wiseActiveNodeCount++] = node;
    wiseTopologyWorkers = workers;

    /* NPS validates equal hash-partition counts inside each co-partition group.
     * Resolve one declared member so Stream routing follows the actual physical
     * partition function rather than assuming partition_count == chiplet_count. */
    char* spec = pstrdup(SkipWiseSpace(u_sess->attr.attr_sql.imcs_nps_groups));
    char* end = strpbrk(spec, ",;");
    if (end != NULL)
        *end = '\0';
    char* colon = strrchr(spec, ':');
    if (colon == NULL)
        ereport(ERROR, (errmsg("NPS hot-flow colocation requires imcs_nps_groups")));
    *colon = '\0';
    Oid oid = DatumGetObjectId(DirectFunctionCall1(regclassin, CStringGetDatum(spec)));
    Relation rel = heap_open(oid, AccessShareLock);
    if (!RelationIsPartitioned(rel) || !PartitionMapIsHash(rel->partMap))
        ereport(ERROR, (errmsg("NPS hot-flow member must be HASH partitioned")));
    wiseNpsPartitions = ((HashPartitionMap*)rel->partMap)->hashElementsNum;
    heap_close(rel, AccessShareLock);
    pfree(spec);
}

/* A Stream hash only has physical locality when its preserved base-column
 * lineage is an exact member of one of the declared NPS groups. Do not infer
 * locality from the presence of a single hash key alone. */
bool ImcsPartitionWiseHashInitForKey(int workers, Oid relation, AttrNumber attribute)
{
    ImcsPartitionWiseHashInit(workers);
    char* copy = pstrdup(SkipWiseSpace(u_sess->attr.attr_sql.imcs_nps_groups));
    char* groupSave = NULL;
    bool matched = false;
    for (char* group = strtok_r(copy, ";", &groupSave); group != NULL && !matched;
         group = strtok_r(NULL, ";", &groupSave)) {
        char* memberSave = NULL;
        for (char* member = strtok_r(group, ",", &memberSave); member != NULL;
             member = strtok_r(NULL, ",", &memberSave)) {
            char* colon = strrchr(member, ':');
            if (colon == NULL)
                continue;
            *colon++ = '\0';
            Oid oid = DatumGetObjectId(DirectFunctionCall1(regclassin, CStringGetDatum(SkipWiseSpace(member))));
            if (oid != relation || get_attnum(oid, colon) != attribute)
                continue;
            Relation rel = heap_open(oid, AccessShareLock);
            if (!RelationIsPartitioned(rel) || !PartitionMapIsHash(rel->partMap)) {
                heap_close(rel, AccessShareLock);
                break;
            }
            wiseNpsPartitions = ((HashPartitionMap*)rel->partMap)->hashElementsNum;
            heap_close(rel, AccessShareLock);
            matched = true;
            break;
        }
    }
    pfree(copy);
    return matched;
}

/* Build compact chiplet groups for local Stream consumers. The worker-to-node
 * mapping is the same deterministic mapping used by NPS scan lanes, so this
 * does not depend on which thread happens to initialize the Stream context. */
int ImcsPartitionWiseStreamGroups(
    int workers, int* workerGroups, int* representatives, int* groupNodes)
{
    if (workerGroups == NULL || representatives == NULL || groupNodes == NULL)
        ereport(ERROR, (errmsg("Invalid NPS Stream group output")));
    ImcsPartitionWiseHashInit(workers);
    int groups = 0;
    for (int worker = 0; worker < workers; ++worker) {
        int node = wiseWorkerNodes[worker];
        int group = 0;
        while (group < groups && groupNodes[group] != node)
            ++group;
        if (group == groups) {
            if (groups >= MAX_WISE_NODES)
                ereport(ERROR, (errmsg("Too many NPS Stream chiplet groups")));
            groupNodes[groups] = node;
            representatives[groups] = worker;
            ++groups;
        }
        workerGroups[worker] = group;
    }
    return groups;
}

/* Follow the exact physical HASH partition function. NPS assigns physical
 * partition ordinal i to active chiplet i modulo the chiplet count. Remaining
 * hash entropy spreads keys over workers local to that chiplet. */
int ImcsPartitionWiseHashWorker(uint64 hashValue, int workers)
{
    if (wiseTopologyWorkers != workers || wiseActiveNodeCount <= 0 || wiseNpsPartitions <= 0)
        return -1;
    uint32 hash = (uint32)hashValue;
    int partition = wiseNpsPartitions - 1 - (int)(hash % (uint32)wiseNpsPartitions);
    int nodeOrdinal = partition % wiseActiveNodeCount;
    int node = wiseActiveNodes[nodeOrdinal];
    int localWorkers = wiseNodeCounts[node];
    if (localWorkers <= 0)
        return -1;
    uint32 laneHash = hash / BUCKETDATALEN;
    int localOrdinal = (int)(laneHash % (uint32)localWorkers);
    return wiseNodeWorkers[wiseNodeOffsets[node] + localOrdinal];
}

static int WiseGcd(int left, int right)
{
    while (right != 0) {
        int remainder = left % right;
        left = right;
        right = remainder;
    }
    return left;
}

/* Jump by approximately one chiplet's worker span. Making the stride coprime
 * with DOP guarantees that every producer is visited exactly once per cycle. */
int ImcsPartitionWisePathStride(int workers)
{
    if (workers <= 1)
        return 1;
    if (wiseTopologyWorkers != workers)
        ImcsPartitionWiseHashInit(workers);
    if (wiseActiveNodeCount <= 1)
        return 1;
    int stride = workers / wiseActiveNodeCount;
    if (stride < 1)
        stride = 1;
    while (stride < workers && WiseGcd(stride, workers) != 1)
        ++stride;
    return stride < workers ? stride : 1;
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
    // Materialize the extra readable bytes too. With a page-aligned source,
    // the padding otherwise lives on an unfaulted page (move_pages: ENOENT).
    memset(data + cu->m_srcBufSize, 0, offset - (Size)cu->m_srcBufSize);
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
    cu->m_partitionWiseAccounted = false;
    if (cu->imcsDesc != NULL) {
        pg_atomic_fetch_add_u64(&cu->imcsDesc->numaMappedBytes, length);
        cu->m_partitionWiseAccounted = true;
    }
}

void CU::FreePartitionWiseBuffer()
{
    if (m_partitionWiseBuffer == NULL)
        return;
    if (munmap(m_partitionWiseBuffer, m_partitionWiseBufferSize) != 0)
        ereport(FATAL, (errmsg("Cannot release NUMA IMCS CU buffer: %m")));
    if (m_partitionWiseAccounted && imcsDesc != NULL)
        pg_atomic_fetch_sub_u64(&imcsDesc->numaMappedBytes, m_partitionWiseBufferSize);
    m_partitionWiseAccounted = false;
    m_partitionWiseBuffer = NULL;
    m_partitionWiseBufferSize = 0;
    m_srcBuf = NULL;
    m_srcBufSize = 0;
    m_srcData = NULL;
    m_nulls = NULL;
    m_offset = NULL;
    m_offsetSize = 0;
}
