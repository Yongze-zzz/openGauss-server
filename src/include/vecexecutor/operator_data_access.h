/* openGauss is licensed under Mulan PSL v2. See http://license.coscl.org.cn/MulanPSL2. */
#ifndef OPERATOR_DATA_ACCESS_H
#define OPERATOR_DATA_ACCESS_H

#include "postgres.h"
#include "nodes/execnodes.h"

struct PlanState;
struct EState;
class VectorBatch;
class ScalarVector;
class SonicDatumArray;
class CU;
struct OperatorDataAccessStats;
struct OperatorDataAccessBuffer;
struct OperatorDataAccessInput;
struct ExprState;
struct ExprContext;
class OperatorDataAccessScope;

enum OperatorDataAccessOperation {
    DA_AGG_TRANSITION,
    DA_SONIC_AGG_TRANSITION,
    DA_BATCH_HANDOFF,
    DA_STREAM_SEND,
    DA_STREAM_RECV,
    DA_AGG_JIT_CANDIDATE,
    DA_SONIC_AGG_JIT_CANDIDATE,
    DA_CONSUMER_INPUT,
    DA_EXPRESSION,
    DA_OPERATOR_STATE,
    DA_JIT_CANDIDATE,
    DA_JIT_INPUT_CANDIDATE,
    DA_SCAN_ACCESS,
    DA_BATCH_COPY,
    DA_COPY_INPUT,
    DA_JIT_STATE_CANDIDATE
};
enum OperatorDataAccessObject { DA_INPUT, DA_STATE, DA_BATCH, DA_SHARED_BUFFER,
    DA_CU_VALUES, DA_CU_NULLS, DA_CU_OFFSETS };
enum OperatorDataAccessKind { DA_READ, DA_WRITE, DA_DELIVERED };

struct OperatorDataAccessKey {
    int plan, peerPlan, stream, sourceWorker, targetWorker;
    int operation, object, access, executionNode, pageNode;
};

/* Input metadata belongs to this invocation, never to an old batch lifetime. */
struct OperatorDataAccessFrame {
    PlanState* node;
    OperatorDataAccessFrame* previous;
    OperatorDataAccessInput* inputs;
    OperatorDataAccessScope* stateScope;
    OperatorDataAccessScope* jitStateScope;
    int expressionDepth;
    int executionNode;
};
extern THR_LOCAL bool operator_data_access_active;
extern void OperatorDataAccessEnter(OperatorDataAccessFrame* frame, PlanState* node);
extern void OperatorDataAccessRestore(OperatorDataAccessFrame* frame, bool discard = false);
extern void OperatorDataAccessLeave(OperatorDataAccessFrame* frame, VectorBatch* result);
extern PlanState* OperatorDataAccessCurrentNode();
extern void OperatorDataAccessFlush(EState* estate);
extern void OperatorDataAccessReadVector(ScalarVector* vector, const bool* selection = NULL,
    int begin = 0, int end = -1, bool jitCandidate = false);
extern void OperatorDataAccessReadBatch(VectorBatch* batch);
extern void OperatorDataAccessWriteVector(ScalarVector* vector, const bool* selection = NULL);
extern OperatorDataAccessScope* OperatorDataAccessState(bool jitCandidate = false);
extern void OperatorDataAccessFlushPending();
extern void OperatorDataAccessForgetInputs();
/* Resolve queued addresses before their allocation context can release them. */
static inline MemoryContext OperatorDataAccessReleaseContext(MemoryContext context)
{
#ifdef ENABLE_HTAP
    if (unlikely(operator_data_access_active))
        OperatorDataAccessFlushPending();
#endif
    return context;
}
extern void OperatorDataAccessJitInputs(PlanState* node, void* expression, ExprContext* context);
extern void OperatorDataAccessJitBatch(PlanState* node, VectorBatch* batch, void* expression);
extern void OperatorDataAccessCU(CU* cu, ScalarVector* output, int begin, int end,
    int rowCursor, int rows, int deadRows, int attlen, bool hasDeadRow);
extern void OperatorDataAccessCULate(CU* cu, ScalarVector* tids, int attlen);
extern void OperatorDataAccessCULateOutput(ScalarVector* output, int rows);
extern ScalarVector* OperatorDataAccessEval(ExprState* expression, ExprContext* context,
    bool* selection, ScalarVector* output, ExprDoneCond* done);

class OperatorDataAccessScope {
public:
    OperatorDataAccessScope(PlanState* node, OperatorDataAccessOperation operation, uint64 rows,
        int peerPlan = 0, int stream = 0, int sourceWorker = -1, int targetWorker = -1,
        int executionNode = -2);
    ~OperatorDataAccessScope();
    bool Enabled() const { return m_stats != NULL; }
    void Observe(const void* address, Size bytes, OperatorDataAccessObject object, OperatorDataAccessKind access);
    void ObserveUpdate(const void* address, Size bytes, OperatorDataAccessObject object)
    {
        ObserveRoles(address, bytes, object, (1U << DA_READ) | (1U << DA_WRITE));
    }
    void ObserveVector(ScalarVector* vector, int begin, int end, OperatorDataAccessObject object,
        OperatorDataAccessKind access, bool skipNullSlots = false);
    void ObserveSelected(ScalarVector* vector, const bool* selection, int begin, int end,
        OperatorDataAccessObject object, OperatorDataAccessKind access);
    void ObserveStorage(ScalarVector* vector, int begin, int end, OperatorDataAccessObject object,
        OperatorDataAccessKind access, bool payload, bool nullSlots);
    void ObserveCopiedSource(ScalarVector* vector, int begin, int end,
        OperatorDataAccessObject object, bool singleRow);
    void ObserveSonic(SonicDatumArray* array, uint32 index, OperatorDataAccessKind access);
    void Flush();
    void RecordCall(uint64 rows) { ++m_calls; m_rows += rows; }
    void Discard();
private:
    OperatorDataAccessScope(const OperatorDataAccessScope&);
    OperatorDataAccessScope& operator=(const OperatorDataAccessScope&);
    void ObserveRoles(const void* address, Size bytes, OperatorDataAccessObject object, unsigned int accesses);
    void ObserveVectorRoles(ScalarVector* vector, int begin, int end, OperatorDataAccessObject object,
        unsigned int liveAccesses, unsigned int nullAccesses, bool payload = true);
    OperatorDataAccessStats* m_stats;
    OperatorDataAccessKey m_key;
    OperatorDataAccessBuffer* m_buffer;
    uint64 m_calls, m_rows;
};

/* Only live during one local redistribution call; no shared Stream layout changes. */
class OperatorDataAccessStreamBatch {
public:
    explicit OperatorDataAccessStreamBatch(int channels);
    OperatorDataAccessScope* Channel(int channel, int stream);
    void Close(bool discard = false);
private:
    OperatorDataAccessStreamBatch(const OperatorDataAccessStreamBatch&);
    OperatorDataAccessStreamBatch& operator=(const OperatorDataAccessStreamBatch&);
    OperatorDataAccessStreamBatch* m_previous;
    PlanState* m_node;
    void* m_slots;
    int m_channels, m_executionNode;
};

extern void OperatorDataAccessStreamCopy(VectorBatch* source, VectorBatch* target, int sourceBegin,
    int targetBegin, int rows, bool singleRow, bool sending, int stream, int peerWorker, bool publish = false);
extern void OperatorDataAccessBatchCopy(VectorBatch* source, VectorBatch* target, int sourceBegin,
    int targetBegin, int rows, bool singleRow, bool deep);

#endif
