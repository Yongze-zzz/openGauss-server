/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * -------------------------------------------------------------------------
 *
 * vecpartiterator.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecpartiterator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "vecexecutor/vecpartiterator.h"
#include "executor/node/nodePartIterator.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexecutor.h"
#include "distributelayer/streamProducer.h"
#include "access/htap/imcs_local_tasks.h"

#ifdef ENABLE_HTAP
    #include "access/htap/imcs_hash_table.h"
    #include "access/htap/imcs_partition_wise.h"
    #include "access/htap/imcstore_am.h"
    #include "vecexecutor/vecnodeimcstorescan.h"
    #include "storage/cu.h"
    #include "utils/rel_gs.h"
    #include "utils/partitionmap.h"
#endif

VecPartIteratorState* ExecInitVecPartIterator(VecPartIterator* node, EState* estate, int eflags)
{
    VecPartIteratorState* state = NULL;

    state = makeNode(VecPartIteratorState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;

    /* initiate sub node */
    state->ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);
    state->ps.qual = NULL;
    state->ps.righttree = NULL;
    state->ps.subPlan = NULL;
    state->currentItr = -1;
    state->subPartCurrentItr = -1;
    state->partitionWiseWorkers = NULL;
    state->ps.ps_vec_TupFromTlist = false;
    state->ps.ps_ProjInfo = NULL;
    state->ps.vectorized = true;
    state->ps.ps_ResultTupleSlot = state->ps.lefttree->ps_ResultTupleSlot;

    return state;
}

static int GetVecscanPartitionNum(const PartIteratorState* node)
{
    VecPartIterator* pi_node = (VecPartIterator*)node->ps.plan;
    PlanState* noden = (PlanState*)node->ps.lefttree;
    int partitionScan;
    switch (nodeTag(noden)) {
        case T_CStoreScanState:
#ifdef ENABLE_HTAP
        case T_IMCStoreScanState:
#endif
            partitionScan = ((CStoreScanState*)noden)->part_id;
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScanState:
            partitionScan = ((TsStoreScanState*)noden)->part_id;
            break;
#endif
        case T_CStoreIndexScanState:
            partitionScan = ((CStoreIndexScanState*)noden)->part_id;
            break;
        case T_CStoreIndexCtidScanState:
            partitionScan = ((CStoreIndexCtidScanState*)noden)->part_id;
            break;
        case T_CStoreIndexHeapScanState:
            partitionScan = ((CStoreIndexHeapScanState*)noden)->part_id;
            break;
        case T_RowToVecState: {
            ScanState* scanState = (ScanState*)noden->lefttree;
            switch (nodeTag(scanState)) {
                case T_SeqScanState:
                case T_IndexScanState:
                case T_IndexOnlyScanState:
                case T_AnnIndexScanState:
                case T_BitmapHeapScanState:
                    partitionScan =  scanState->part_id;
                    break;
                default:
                    partitionScan = pi_node->itrs;
                    break;
            }
            break;
        }
        default:
            partitionScan = pi_node->itrs;
            break;
    }
    return partitionScan;
}

// static CStoreScanState* GetPartitionLaneCStoreScan(VecPartIteratorState* node)
static CStoreScanState* GetPartitionWiseScan(VecPartIteratorState* node)
{
    // if (!u_sess->attr.attr_sql.enable_cstore_partition_lane_scan ||
    //     u_sess->stream_cxt.producer_dop <= 1 || !IsA(node->ps.lefttree, CStoreScanState)) {
    if (u_sess->stream_cxt.producer_dop <= 1) {
        return NULL;
    }
    bool enabled = IsA(node->ps.lefttree, CStoreScanState) &&
        u_sess->attr.attr_sql.enable_cstore_partition_lane_scan;
#ifdef ENABLE_HTAP
    bool imcsScan = IsA(node->ps.lefttree, IMCStoreScanState);
    bool wholePartition = ((VecPartIterator*)node->ps.plan)->wholePartitionPerWorker;
    /* NUMA scheduling adds locality to whole-partition scans; it does not enable them. */
    if (imcsScan && u_sess->attr.attr_sql.enable_numa_aware_partition_scan &&
        !u_sess->attr.attr_sql.enable_imcs_partition_scan && !u_sess->attr.attr_sql.enable_imcs_nps) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("NUMA-aware IMCS partition scans require enable_imcs_partition_scan"),
            errhint("Set enable_imcs_partition_scan = on before running the scan.")));
    }
    enabled = enabled || (imcsScan && (u_sess->attr.attr_sql.enable_imcs_partition_scan ||
        u_sess->attr.attr_sql.enable_imcs_nps || wholePartition));
#endif
    if (!enabled)
       return NULL;
    CStoreScanState* scan = (CStoreScanState*)node->ps.lefttree;
    if (!scan->isPartTbl || ((Scan*)scan->ps.plan)->partition_iterator_elimination) {
        return NULL;
    }
#ifdef ENABLE_HTAP
    /* First version supports local one-level partitions; never partially scan a subpartition tree. */
    if (imcsScan && (!IS_SINGLE_NODE || ENABLE_DSS || IS_SPQ_RUNNING ||
        ((SpqCStoreScan*)scan->ps.plan)->isShareScan || scan->subPartLengthList != NIL))
        ereport(ERROR, (errmsg("Static IMCS PartitionWise requires local, non-shared, one-level partitions")));
#endif
    return scan;
}

#ifdef ENABLE_HTAP
static bool UseImcsLocalTasks(VecPartIteratorState* node)
{
    if (!u_sess->attr.attr_sql.enable_imcs_local_task_queue) return false;
    if (!u_sess->attr.attr_sql.enable_imcs_nps ||
        !u_sess->attr.attr_sql.enable_imcs_adapt ||
        u_sess->attr.attr_sql.enable_imcs_adapt_migration)
        ereport(ERROR, (errmsg("IMCS local tasks require NPS, Adapt affinity and migration disabled")));
    // Never redistribute fused join inputs independently: their SMP-lane
    // ownership can be part of the local join's distribution contract.
    StreamProducer* producer = u_sess->stream_cxt.producer_obj;
    return producer != NULL && u_sess->stream_cxt.global_obj != NULL &&
        producer->allowsImcsLocalTasks() &&
        producer->getPlan() != NULL && producer->getPlan()->planTree == node->ps.plan &&
        node->ps.plan->extParam == NULL && node->ps.plan->recursive_union_plan_nodeid == 0;
}

static int ClaimImcsLocalPartition(VecPartIteratorState* node, CStoreScanState* scan,
                                   int partitions, int worker, int dop, bool backward)
{
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group->m_imcsLocalTasks == NULL)
        ereport(ERROR, (errmsg("Cannot allocate IMCS local task queues")));
    // Resolve all PostgreSQL metadata before taking the C++ queue mutex.
    int home = ImcsPartitionWiseWorkerNode(worker, dop);
    Oid* oids = (Oid*)palloc(sizeof(Oid) * partitions);
    int* nodes = (int*)palloc(sizeof(int) * partitions);
    int index = 0;
    ListCell* cell = NULL;
    foreach (cell, scan->partitions) {
        oids[index] = ((Partition)lfirst(cell))->pd_id;
        nodes[index] = ImcsPartitionWiseWorkerNode(node->partitionWiseWorkers[index], dop);
        ++index;
    }
    int claimed = -1;
    try {
        std::vector<unsigned int> ids(oids, oids + partitions);
        std::vector<int> homes(nodes, nodes + partitions);
        claimed = group->m_imcsLocalTasks->claim(node->ps.plan->plan_node_id,
            node->imcsLocalTaskGeneration, backward ? -1 : 1, home, ids, homes);
    } catch (const std::exception& error) {
        // claim() has released its mutex before PostgreSQL error handling.
        ereport(ERROR, (errmsg("IMCS local partition claim failed: %s", error.what())));
    }
    pfree(oids);
    pfree(nodes);
    if (claimed >= 0) ImcsLocalTaskRecord(node->partitionWiseWorkers[claimed] != worker);
    return claimed < 0 ? partitions : (backward ? partitions - claimed - 1 : claimed);
}

static void InitImcsNumaPartitionWorkers(VecPartIteratorState* node, CStoreScanState* scan, int partitions)
{
    int workers = (int)u_sess->stream_cxt.producer_dop;
    ImcsPartitionWiseWorkerEnter((int)u_sess->stream_cxt.smp_id, workers);
    if (list_length(scan->partitions) != partitions)
        ereport(ERROR, (errmsg("IMCS PartitionWise partition list does not match the pruning result")));
    node->partitionWiseWorkers = (int*)MemoryContextAlloc(node->ps.state->es_query_cxt, sizeof(int) * partitions);
    int nodeOrdinals[sizeof(unsigned long) * 8] = {0};
    bool chipletOwner = ((VecPartIterator*)node->ps.plan)->chipletJoinOwner;
    HashPartitionMap* buckets = NULL;
    if (chipletOwner) {
        Relation parent = scan->ss_partition_parent;
        if (parent == NULL || parent->partMap == NULL || !PartitionMapIsHash(parent->partMap))
            ereport(ERROR, (errmsg("Chiplet bucket ownership requires an IMCS hash-partitioned table")));
        buckets = (HashPartitionMap*)parent->partMap;
    }
    int index = 0;
    ListCell* cell = NULL;
    foreach (cell, scan->partitions) {
        CHECK_FOR_INTERRUPTS();
        Oid oid = ((Partition)lfirst(cell))->pd_id;
        IMCSDesc* desc = IMCS_HASH_TABLE->GetImcsDesc(oid);
        if (chipletOwner) {
            int ordinal = desc == NULL ? -1 : desc->numaOrdinal;
            if (ordinal < 0 || ordinal >= buckets->hashElementsNum || buckets->hashElements[ordinal].partitionOid != oid) {
                for (ordinal = 0; ordinal < buckets->hashElementsNum; ++ordinal) {
                    if (buckets->hashElements[ordinal].partitionOid == oid)
                        break;
                }
                if (ordinal == buckets->hashElementsNum)
                    ereport(ERROR, (errmsg("Unable to identify the IMCS hash bucket for partition %u", oid)));
            }
            node->partitionWiseWorkers[index++] = ImcsPartitionWiseBucketOwner(ordinal, workers);
            continue;
        }
        /* Keep ownership immutable across all workers and rescans. Adapt may
         * change the physical node while other workers initialize this array;
         * following that node at batch boundaries must never change ownership. */
        int dataNode = desc == NULL ? -1 : desc->dataNumaNode;
        if (u_sess->attr.attr_sql.enable_imcs_nps && desc != NULL && !OidIsValid(desc->numaGroup))
            ereport(ERROR, (errmsg("Partition %u was not populated using a validated NPS layout", oid)));
        if (dataNode < 0 || dataNode >= (int)lengthof(nodeOrdinals))
            ereport(ERROR, (errmsg("IMCS partition %u has no NUMA data layout", oid),
                errhint("Set imcs_partition_numa_map before repopulating this partition.")));
        /* Count within the data node, not across interleaved global partition indices. */
        int ordinal = nodeOrdinals[dataNode]++;
        node->partitionWiseWorkers[index] = ImcsPartitionWiseWorker(index, ordinal, dataNode, workers);
        ereport(DEBUG1, (errmsg("IMCS PartitionWise partition=%u node=%d worker=%d",
            oid, dataNode, node->partitionWiseWorkers[index])));
        ++index;
    }
}

static bool UseImcsCuBalance(VecPartIteratorState* node, CStoreScanState* scan)
{
    const auto& sql = u_sess->attr.attr_sql;
    Plan* plan = node->ps.plan;
    if (!sql.enable_imcs_nps_cu_balance || !sql.enable_imcs_nps || sql.enable_imcs_adapt ||
        sql.enable_imcs_local_task_queue || ENABLE_DMS || plan->ispwj || plan->extParam != NULL ||
        plan->recursive_union_plan_nodeid != 0 || scan->isSampleScan || scan->rangeScanInRedis.isRangeScanInRedis ||
        ((VecPartIterator*)plan)->direction != ForwardScanDirection)
        return false;

    StreamProducer* producer = u_sess->stream_cxt.producer_obj;
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (producer == NULL || group == NULL || group->m_imcsLocalTasks == NULL ||
        !producer->allowsImcsLocalTasks(true) || producer->getPlan() == NULL)
        return false;
    const Plan* root = producer->getPlan()->planTree;
    if (root == plan)
        return true;
    /* The broadcast build is identical for every local probe lane. The
     * producer's outbound redistribution (or gather) removes lane ownership. */
    if (root == NULL || !IsA(root, VecHashJoin) || root->ispwj || root->extParam != NULL ||
        outerPlan(root) != plan || innerPlan(root) == NULL || !IsA(innerPlan(root), VecStream) ||
        ((const VecHashJoin*)root)->join.jointype != JOIN_INNER)
        return false;
    const Stream* build = (const Stream*)innerPlan(root);
    return build->type == STREAM_BROADCAST && build->smpDesc.distriType == LOCAL_BROADCAST;
}

static void InitImcsCuRanges(VecPartIteratorState* node, CStoreScanState* scan, int partitions, int worker, int dop)
{
    Oid* oids = (Oid*)palloc(sizeof(Oid) * partitions);
    int* homes = (int*)palloc(sizeof(int) * partitions);
    uint32* limits = (uint32*)palloc(sizeof(uint32) * partitions);
    const int home = ImcsPartitionWiseWorkerNode(worker, dop);
    int nodeWorkers[sizeof(unsigned long) * 8] = {};
    int localIndex = 0;
    for (int lane = 0; lane < dop; ++lane) {
        int nodeId = ImcsPartitionWiseWorkerNode(lane, dop);
        ++nodeWorkers[nodeId];
        localIndex += lane < worker && nodeId == home;
    }
    int i = 0;
    bool allHomesActive = true;
    ListCell* cell = NULL;
    foreach (cell, scan->partitions) {
        Oid oid = ((Partition)lfirst(cell))->pd_id;
        IMCSDesc* desc = IMCS_HASH_TABLE->GetImcsDesc(oid);
        if (desc == NULL || desc->imcsDescLock == NULL) {
            allHomesActive = false;
            break;
        }
        oids[i] = oid;
        homes[i] = desc->dataNumaNode;
        allHomesActive = allHomesActive && nodeWorkers[homes[i]] > 0;
        LWLockAcquire(desc->imcsDescLock, LW_SHARED);
        limits[i] = desc->curMaxRowGroupId + 1;
        LWLockRelease(desc->imcsDescLock);
        ++i;
    }

    if (allHomesActive) {
        try {
            std::vector<unsigned int> ids(oids, oids + partitions);
            u_sess->stream_cxt.global_obj->m_imcsLocalTasks->freezeCuLimits(node->ps.plan->plan_node_id,
                node->imcsLocalTaskGeneration, ids, limits);
        } catch (const std::exception& error) {
            ereport(ERROR, (errmsg("Cannot determine IMCS CU ranges: %s", error.what())));
        }

        uint64 total = 0;
        for (i = 0; i < partitions; ++i) {
            if (homes[i] == home)
                total += limits[i];
        }
        uint64 begin = total * localIndex / nodeWorkers[home];
        uint64 end = total * (localIndex + 1) / nodeWorkers[home];
        uint64 offset = 0;
        auto* ranges = (ImcsPartitionCuRange*)MemoryContextAllocZero(node->ps.state->es_query_cxt,
            sizeof(ImcsPartitionCuRange) * partitions);
        for (i = 0; i < partitions && offset < end; ++i) {
            if (homes[i] != home)
                continue;
            uint64 next = offset + limits[i];
            if (begin < next)
                ranges[i] = {(uint32)(Max(begin, offset) - offset), (uint32)(Min(end, next) - offset)};
            offset = next;
        }
        node->partitionCuRanges = ranges;
        ereport(DEBUG1, (errmsg("IMCS CU balance node=%d worker=%d home=%d assigned_cus=%lu local_cus=%lu",
            node->ps.plan->plan_node_id, worker, home, end - begin, total)));
    }
    pfree(oids);
    pfree(homes);
    pfree(limits);
}
#endif

/* return: false means all patition finished */
static bool InitVecscanPartition(VecPartIteratorState* node, int partitionScan)
{
    int paramno = 0;
    unsigned int itr_idx = 0;
    VecPartIterator* pi_node = (VecPartIterator*)node->ps.plan;
    ParamExecData* param = NULL;
    List* subPartLengthList = NULL;
    PlanState* noden = NULL;
    // CStoreScanState* laneScan = GetPartitionLaneCStoreScan(node);
    CStoreScanState* wiseScan = GetPartitionWiseScan(node);

    /* check sub partitions */
    if (IsA(node->ps.lefttree, RowToVecState)) {
        RowToVecState* rowToVecNode = (RowToVecState*)node->ps.lefttree;
        noden = (PlanState*)rowToVecNode->ps.lefttree;
        if (IsA(noden, ScanState) || IsA(noden, SeqScanState) || IsA(noden, IndexOnlyScanState) ||
            IsA(noden, IndexScanState) || IsA(noden, BitmapHeapScanState) || IsA(noden, TidScanState)) {
            subPartLengthList = ((ScanState *)noden)->subPartLengthList;
        }
    }

#ifdef ENABLE_HTAP
    if (IsA(node->ps.lefttree, IMCStoreScanState)) {
        noden = (PlanState*)node->ps.lefttree;
        subPartLengthList = ((ScanState *)noden)->subPartLengthList;
    }
#endif

    // if (laneScan != NULL) {
    if (wiseScan != NULL) {
        int dop = (int)u_sess->stream_cxt.producer_dop;
        // int lane = (int)u_sess->stream_cxt.smp_id;
        // int nextItr = (node->currentItr < 0) ? lane : node->currentItr + dop;
        int worker = (int)u_sess->stream_cxt.smp_id;
        /* Plain partition scans reuse CStore's static round-robin iteration. */
        int nextItr = (node->currentItr < 0) ? worker : node->currentItr + dop;
#ifdef ENABLE_HTAP
        bool wholePartition = pi_node->wholePartitionPerWorker;
        /* With changing data placement, a final aggregate still needs
         * deterministic partition ownership across all its workers. */
        if (wiseScan->m_isImcstore && (pi_node->chipletJoinOwner ||
            ((u_sess->attr.attr_sql.enable_numa_aware_partition_scan || u_sess->attr.attr_sql.enable_imcs_nps) &&
                !(wholePartition && u_sess->attr.attr_sql.enable_imcs_adapt)))) {
            if (node->partitionWiseWorkers == NULL) {
                InitImcsNumaPartitionWorkers(node, wiseScan, partitionScan);
                if (!wholePartition && UseImcsCuBalance(node, wiseScan))
                    InitImcsCuRanges(node, wiseScan, partitionScan, worker, dop);
            }
            if (node->partitionCuRanges != NULL) {
                for (nextItr = node->currentItr + 1; nextItr < partitionScan; ++nextItr) {
                    const auto& range = node->partitionCuRanges[nextItr];
                    if (range.first < range.limit) {
                        ((IMCStore*)wiseScan->m_CStore)->SetPartitionCuRange(range.first, range.limit);
                        break;
                    }
                }
            } else if (!wholePartition && UseImcsLocalTasks(node)) {
                nextItr = ClaimImcsLocalPartition(node, wiseScan, partitionScan, worker, dop,
                    BackwardScanDirection == pi_node->direction);
            } else for (nextItr = node->currentItr + 1; nextItr < partitionScan; ++nextItr) {
                int index = BackwardScanDirection == pi_node->direction ? partitionScan - nextItr - 1 : nextItr;
                if (node->partitionWiseWorkers[index] == worker)
                    break;
            }
        }
#endif

        if (nextItr >= partitionScan) {
            return false;
        }

        node->currentItr = nextItr;
        // laneScan->partitionLaneScan = true;
        wiseScan->partitionWiseScan = true;
    } else if (node->currentItr + 1 >= partitionScan) {
        /* if there is no partition to scan, return false */
        if (subPartLengthList != NIL) {
            int subPartLength = (int)list_nth_int(subPartLengthList, node->currentItr);
            if (node->subPartCurrentItr + 1 >= subPartLength) {
                return false;
            }
        } else {
            return false;
        }
    }

    Assert(ForwardScanDirection == pi_node->direction || BackwardScanDirection == pi_node->direction);

    /* set iterator parameter */
    if (wiseScan == NULL) {
        SetPartitionIteratorParamter(node, subPartLengthList);
    }

    itr_idx = node->currentItr;
    if (BackwardScanDirection == pi_node->direction)
        itr_idx = partitionScan - itr_idx - 1;

    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)itr_idx;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    /* reset the plan node so that next partition can be scanned */
    VecExecReScan(node->ps.lefttree);

    return true;
}

VectorBatch* ExecVecPartIterator(VecPartIteratorState* node)
{
    VectorBatch* result = NULL;
    EState* state = node->ps.lefttree->state;
    bool orig_early_free = state->es_skip_early_free;

    int partitionScan = GetVecscanPartitionNum(node);
    if (partitionScan == 0) {
        return NULL;
    }

    /* init first scanned partition */
    // if (-1 == node->currentItr)
    //     InitVecscanPartition(node, partitionScan);
    if (-1 == node->currentItr && !InitVecscanPartition(node, partitionScan))
        return NULL; /* An idle worker must not read the initially opened partition. */
    /* For partition wise join, can not early free left tree's caching memory */
    state->es_skip_early_free = true;
    result = VectorEngine(node->ps.lefttree);
    state->es_skip_early_free = orig_early_free;

    if (!BatchIsNull(result))
        return result;

    for (;;) {
        if (!InitVecscanPartition(node, partitionScan)) {
            return NULL;
        }

        /* For partition wise join, can not early free left tree's caching memory */
        orig_early_free = state->es_skip_early_free;
        state->es_skip_early_free = true;

        /* execute the given node to return a(nother) tuple from the special partition */
        result = VectorEngine(node->ps.lefttree);
        state->es_skip_early_free = orig_early_free;

        /* scan the next partition if no tuple returns */
        if (!BatchIsNull(result))
            return result;
    }
}

void ExecEndVecPartIterator(VecPartIteratorState* node)
{
    /* close down subplans */
    ExecEndNode(node->ps.lefttree);
}

void ExecReScanVecPartIterator(VecPartIteratorState* node)
{
    VecPartIterator* pi_node = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* subPartParam = NULL;

    /* do nothing if there is no partition to scan */
    int partitionScan = GetVecscanPartitionNum(node);
    if (partitionScan == 0) {
        return;
    }

    node->currentItr = -1;
    ++node->imcsLocalTaskGeneration;
    pfree_ext(node->partitionWiseWorkers); /* Rebuild from this scan's current pruning list. */
#ifdef ENABLE_HTAP
    if (node->partitionCuRanges != NULL) {
        ((IMCStore*)((CStoreScanState*)node->ps.lefttree)->m_CStore)->SetPartitionCuRange(0, 0);
        pfree_ext(node->partitionCuRanges);
    }
#endif

    pi_node = (VecPartIterator*)node->ps.plan;
    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)0;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    node->subPartCurrentItr = -1;

    subPartParamno = pi_node->param->subPartParamno;
    subPartParam = &(node->ps.state->es_param_exec_vals[subPartParamno]);
    subPartParam->isnull = false;
    subPartParam->value = (Datum)0;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, subPartParamno);

    /*
     * if the pruning result isnot null, Reset the subplan node so
     * that its output can be re-scanned.
     */
    VecExecReScan(node->ps.lefttree);
}
