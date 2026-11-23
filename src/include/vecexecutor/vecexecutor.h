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
 * ---------------------------------------------------------------------------------------
 * 
 * vecexecutor.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecexecutor.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef VECEXECUTOR_H_
#define VECEXECUTOR_H_

#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/bytea.h"
#include "utils/geo_decls.h"
#include "utils/nabstime.h"
#include "utils/timestamp.h"

#define initEcontextBatch(m_scanbatch, m_outerbatch, m_innerbatch, m_aggbatch) \
    {                                                                          \
        econtext->ecxt_scanbatch = m_scanbatch;                                \
        econtext->ecxt_outerbatch = m_outerbatch;                              \
        econtext->ecxt_innerbatch = m_innerbatch;                              \
        econtext->ecxt_aggbatch = m_aggbatch;                                  \
    }

extern VectorBatch* VectorEngine(PlanState* node);
extern VectorBatch *ExecVecProject(ProjectionInfo *projInfo, bool selReSet = true,
    ExprDoneCond *isDone = NULL);
extern ExprState* ExecInitVecExpr(Expr* node, PlanState* parent);

extern ScalarVector* ExecVecQual(List* qual, ExprContext* econtext, bool resultForNull, bool isReset = true);

typedef VectorBatch* (*ExecVecScanAccessMtd)(ScanState* node);
typedef bool (*ExecVecScanRecheckMtd)(ScanState* node, VectorBatch* batch);
extern VectorBatch* ExecVecScan(ScanState* node, ExecVecScanAccessMtd accessMtd, ExecVecScanRecheckMtd recheckMtd);
extern void ExecAssignVecScanProjectionInfo(ScanState* node);
extern void ExecVecMarkPos(PlanState* node);
extern void ExecVecRestrPos(PlanState* node);
extern void VecExecReScan(PlanState* node);
extern void VecCacheTraceEnter(PlanState* node);
extern void VecCacheTraceLeave(PlanState* node);
extern void VecCacheTraceFlush();
extern void VecCacheTraceDiscard();
struct VecCacheTraceCounters {
    uint64 instructions;
    uint64 misses;
    uint64 runningNs;
    uint64 enabledNs;
};
extern bool VecCacheTraceSnapshot(VecCacheTraceCounters* counters, uint64* cycles = NULL, uint64* stalls = NULL);
extern bool VecEngineProfileEnabled();
extern uint64 VecEngineProfileEnter(PlanState* node, int* depth, int* cpu_id);
extern void VecEngineProfileLeave(PlanState* node, VectorBatch* result, uint64 start_time, int depth, int cpu_id);
extern void VecEngineProfileFlush();
extern void VecEngineProfileDiscard();

extern void VecCacheTraceReadCounters(uint64* started, uint64* written, uint64* startFailed,
    uint64* perfUnavailable, uint64* writeFailed, uint64* discarded);
extern void RhjTrackEnter(PlanState* node);
extern void RhjTrackLeave(PlanState* node);
extern void RhjTrackReset();
extern void RhjReadCounters(uint64* builds, uint64* cacheBuilds, uint64* chipletBuilds, uint64* partitions, uint64* targetMisses);
#endif /* VECEXECUTOR_H_ */
