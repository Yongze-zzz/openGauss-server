/* openGauss is licensed under Mulan PSL v2. See http://license.coscl.org.cn/MulanPSL2. */
#include "postgres.h"
#ifdef ENABLE_HTAP
#include "knl/knl_variable.h"
#include "vecexecutor/operator_data_access.h"
#include "vecexecutor/vectorbatch.h"
#include "nodes/execnodes.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "nodes/nodeFuncs.h"
#include "storage/cu.h"
#include <algorithm>
#include <new>
#include <numaif.h>
#include <sys/syscall.h>
#include <unistd.h>

struct OperatorDataAccessEntry {
    OperatorDataAccessKey key;
    uint64 bytes, visits, calls, rows;
};
struct OperatorDataAccessInput {
    const ScalarValue* values;
    const uint8* flags;
    int producer;
    OperatorDataAccessInput* next;
};
/* Pending page addresses share placement queries across access categories. */
struct OperatorDataAccessBuffer {
    struct Page { uintptr_t address; uint64 bytes[3]; } pages[64];
    uint8 buckets[128]; /* Open addressing, storing page index + 1. */
    int roles[3], roleCount, count;
    OperatorDataAccessBuffer* next;
};

struct OperatorDataAccessStats {
    HTAB* groups;
    MemoryContext context;
    OperatorDataAccessBuffer* freeBuffers;
    OperatorDataAccessInput* freeInputs;
    Size pageSize;
    uint64 query;
    long tid;
    int worker;
    bool intra, inter;
    uint64 pageQueries, failedQueries, cpuSamples;
    int firstErrno;
};
static THR_LOCAL OperatorDataAccessFrame* currentFrame = NULL;
THR_LOCAL bool operator_data_access_active = false;
static THR_LOCAL OperatorDataAccessStreamBatch* currentStreamBatch = NULL;

static OperatorDataAccessStats* GetStats(EState* estate)
{
    if (estate->es_operator_data_access == NULL) {
        AutoContextSwitch context(estate->es_query_cxt);
        OperatorDataAccessStats* stats = (OperatorDataAccessStats*)palloc0(sizeof(OperatorDataAccessStats));
        HASHCTL info;
        MemSet(&info, 0, sizeof(info));
        info.keysize = sizeof(OperatorDataAccessKey);
        info.entrysize = sizeof(OperatorDataAccessEntry);
        info.hcxt = CurrentMemoryContext;
        stats->groups = hash_create("Operator data access", 128, &info,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
        stats->context = estate->es_query_cxt;
        stats->pageSize = (Size)getpagesize();
        stats->query = u_sess->debug_query_id;
        stats->tid = syscall(SYS_gettid);
        stats->worker = (int)u_sess->stream_cxt.smp_id;
        stats->intra = u_sess->attr.attr_sql.enable_intra_operator_data_access;
        stats->inter = u_sess->attr.attr_sql.enable_inter_operator_data_access;
        estate->es_operator_data_access = stats;
    }
    return estate->es_operator_data_access;
}

static int ExecutionNode(OperatorDataAccessStats* stats)
{
    int savedErrno = errno;
    unsigned int node = 0;
    ++stats->cpuSamples;
    int result = syscall(SYS_getcpu, NULL, &node, NULL) == 0 ? (int)node : DA_NODE_UNKNOWN;
    errno = savedErrno;
    return result;
}

static void AddRecord(OperatorDataAccessStats* stats, const OperatorDataAccessKey& key,
    uint64 bytes, uint64 visits, uint64 calls, uint64 rows)
{
    bool found = false;
    OperatorDataAccessEntry* entry = (OperatorDataAccessEntry*)hash_search(stats->groups, &key, HASH_ENTER, &found);
    if (!found)
        entry->bytes = entry->visits = entry->calls = entry->rows = 0;
    entry->bytes += bytes;
    entry->visits += visits;
    entry->calls += calls;
    entry->rows += rows;
}

OperatorDataAccessScope::OperatorDataAccessScope(PlanState* node, OperatorDataAccessOperation operation, uint64 rows,
    int peerPlan, int stream, int sourceWorker, int targetWorker, int executionNode)
    : m_stats(NULL), m_key{}, m_buffer(NULL), m_calls(0), m_rows(0)
{
    if (node == NULL)
        return;
    bool inter = operation == DA_STREAM_SEND || operation == DA_STREAM_RECV ||
        operation == DA_CONSUMER_INPUT || operation == DA_JIT_INPUT_CANDIDATE || operation == DA_COPY_INPUT;
    if (!(inter ? u_sess->attr.attr_sql.enable_inter_operator_data_access :
        u_sess->attr.attr_sql.enable_intra_operator_data_access))
        return;
    m_stats = GetStats(node->state);
    m_key.plan = node->plan->plan_node_id;
    m_key.peerPlan = peerPlan;
    m_key.stream = stream;
    m_key.sourceWorker = sourceWorker;
    m_key.targetWorker = targetWorker;
    m_key.operation = operation;
    m_key.object = m_key.access = -1;
    if (executionNode == DA_NODE_NOT_OBSERVED && currentFrame != NULL && currentFrame->node == node) {
        if (currentFrame->executionNode == DA_NODE_NOT_OBSERVED)
            currentFrame->executionNode = ExecutionNode(m_stats);
        executionNode = currentFrame->executionNode;
    }
    m_key.executionNode = executionNode == DA_NODE_NOT_OBSERVED ? ExecutionNode(m_stats) : executionNode;
    m_key.pageNode = DA_NODE_NOT_OBSERVED;
    if (rows != 0)
        RecordCall(rows);
}

OperatorDataAccessScope::~OperatorDataAccessScope()
{
    Flush();
    if (m_calls != 0)
        AddRecord(m_stats, m_key, 0, 0, m_calls, m_rows);
}

void OperatorDataAccessScope::Discard()
{
    if (m_buffer != NULL) {
        m_buffer->next = m_stats->freeBuffers;
        m_stats->freeBuffers = m_buffer;
        m_buffer = NULL;
    }
    m_calls = m_rows = 0;
}

void OperatorDataAccessScope::Observe(const void* address, Size bytes,
    OperatorDataAccessObject object, OperatorDataAccessKind access)
{
    ObserveRoles(address, bytes, object, 1U << access);
}

/* Hash pages by address; role byte counters share the same placement query. */
void OperatorDataAccessScope::ObserveRoles(const void* address, Size bytes,
    OperatorDataAccessObject object, unsigned int accesses)
{
    if (!Enabled() || bytes == 0 || accesses == 0)
        return;
    uintptr_t cursor = (uintptr_t)address;
    while (bytes > 0) {
        if (m_buffer == NULL) {
            m_buffer = m_stats->freeBuffers;
            if (m_buffer != NULL)
                m_stats->freeBuffers = m_buffer->next;
            else
                m_buffer = (OperatorDataAccessBuffer*)MemoryContextAlloc(m_stats->context, sizeof(*m_buffer));
            m_buffer->count = m_buffer->roleCount = 0;
            MemSet(m_buffer->buckets, 0, sizeof(m_buffer->buckets));
        }
        int needed = 0;
        for (int access = 0; access < 3; ++access) {
            if ((accesses & (1U << access)) == 0)
                continue;
            int role = object * 3 + access;
            int slot = 0;
            while (slot < m_buffer->roleCount && m_buffer->roles[slot] != role)
                ++slot;
            needed += slot == m_buffer->roleCount;
        }
        if (needed + m_buffer->roleCount > (int)lengthof(m_buffer->roles)) {
            Flush();
            continue;
        }
        unsigned int slots = 0;
        for (int access = 0; access < 3; ++access) {
            if ((accesses & (1U << access)) == 0)
                continue;
            int role = object * 3 + access;
            int slot = 0;
            while (slot < m_buffer->roleCount && m_buffer->roles[slot] != role)
                ++slot;
            if (slot == m_buffer->roleCount)
                m_buffer->roles[m_buffer->roleCount++] = role;
            slots |= 1U << slot;
        }
        uintptr_t page = cursor - cursor % m_stats->pageSize;
        Size length = Min(bytes, m_stats->pageSize - (Size)(cursor - page));
        uint64 hash = (uint64)(page / m_stats->pageSize) * 0x9e3779b97f4a7c15ULL;
        unsigned int bucket = (unsigned int)(hash >> 57);
        while (m_buffer->buckets[bucket] != 0 &&
            m_buffer->pages[m_buffer->buckets[bucket] - 1].address != page)
            bucket = (bucket + 1) & (lengthof(m_buffer->buckets) - 1);
        int index = m_buffer->buckets[bucket] - 1;
        if (index < 0) {
            if (m_buffer->count == (int)lengthof(m_buffer->pages)) {
                Flush();
                continue;
            }
            index = m_buffer->count++;
            m_buffer->buckets[bucket] = (uint8)(index + 1);
            m_buffer->pages[index].address = page;
            MemSet(m_buffer->pages[index].bytes, 0, sizeof(m_buffer->pages[index].bytes));
        }
        for (int slot = 0; slot < m_buffer->roleCount; ++slot)
            if ((slots & (1U << slot)) != 0)
                m_buffer->pages[index].bytes[slot] += length;
        cursor += length;
        bytes -= length;
    }
}

void OperatorDataAccessScope::Flush()
{
    if (m_buffer == NULL)
        return;
    int savedErrno = errno;
    void* pages[64];
    int nodes[64];
    struct ResolvedPage { int node, index; } resolved[64];
    for (int i = 0; i < m_buffer->count; ++i) {
        pages[i] = (void*)m_buffer->pages[i].address;
        nodes[i] = DA_NODE_UNKNOWN;
    }
    ++m_stats->pageQueries;
    /* A NULL destination-node array only queries placement; addresses are unique. */
    long result = move_pages(0, m_buffer->count, pages, NULL, nodes, 0);
    if (result < 0) {
        ++m_stats->failedQueries;
        if (m_stats->firstErrno == 0)
            m_stats->firstErrno = errno;
    }
    for (int i = 0; i < m_buffer->count; ++i)
        resolved[i] = {result < 0 || nodes[i] < 0 ? DA_NODE_UNKNOWN : nodes[i], i};
    std::sort(resolved, resolved + m_buffer->count,
        [](const ResolvedPage& left, const ResolvedPage& right) { return left.node < right.node; });
    for (int begin = 0; begin < m_buffer->count;) {
        uint64 bytes[3] = {0}, visits[3] = {0};
        int end = begin;
        while (end < m_buffer->count && resolved[end].node == resolved[begin].node) {
            for (int slot = 0; slot < m_buffer->roleCount; ++slot) {
                uint64 amount = m_buffer->pages[resolved[end].index].bytes[slot];
                bytes[slot] += amount;
                visits[slot] += amount != 0;
            }
            ++end;
        }
        for (int slot = 0; slot < m_buffer->roleCount; ++slot) {
            if (visits[slot] == 0)
                continue;
            OperatorDataAccessKey key = m_key;
            key.object = m_buffer->roles[slot] / 3;
            key.access = m_buffer->roles[slot] % 3;
            key.pageNode = resolved[begin].node;
            AddRecord(m_stats, key, bytes[slot], visits[slot], 0, 0);
        }
        begin = end;
    }
    m_buffer->next = m_stats->freeBuffers;
    m_stats->freeBuffers = m_buffer;
    m_buffer = NULL;
    errno = savedErrno;
}

/* Fixed-width slots are coalesced into runs, including when NULL slots are excluded. */
void OperatorDataAccessScope::ObserveVectorRoles(ScalarVector* vector, int begin, int end,
    OperatorDataAccessObject object, unsigned int liveAccesses, unsigned int nullAccesses, bool payload)
{
    if (!Enabled() || vector == NULL || end <= begin)
        return;
    Assert(begin >= 0 && end <= vector->m_rows);
    ObserveRoles(vector->m_flag + begin, end - begin, object, liveAccesses);
    if ((!vector->m_desc.encoded || !payload) && liveAccesses == nullAccesses) {
        ObserveRoles(vector->m_vals + begin, (Size)(end - begin) * sizeof(ScalarValue), object, liveAccesses);
        return;
    }
    for (int row = begin; row < end;) {
        int start = row;
        bool isNull = IS_NULL(vector->m_flag[row]);
        do {
            if (!isNull && vector->m_desc.encoded && payload) {
                Datum value = ScalarVector::Decode(vector->m_vals[row]);
                Size length = vector->m_desc.typeId == NAMEOID ? datumGetSize(value, false, -2) : VARSIZE_ANY(value);
                ObserveRoles(DatumGetPointer(value), length, object, liveAccesses);
            }
            ++row;
        } while (row < end && IS_NULL(vector->m_flag[row]) == isNull);
        ObserveRoles(vector->m_vals + start, (Size)(row - start) * sizeof(ScalarValue), object,
            isNull ? nullAccesses : liveAccesses);
    }
}

void OperatorDataAccessScope::ObserveVector(ScalarVector* vector, int begin, int end,
    OperatorDataAccessObject object, OperatorDataAccessKind access, bool skipNullSlots)
{
    unsigned int live = 1U << access;
    ObserveVectorRoles(vector, begin, end, object, live,
        vector != NULL && !vector->m_desc.encoded && !skipNullSlots ? live : 0);
}

void OperatorDataAccessScope::ObserveSelected(ScalarVector* vector, const bool* selection,
    int begin, int end, OperatorDataAccessObject object, OperatorDataAccessKind access)
{
    if (!Enabled() || vector == NULL || end <= begin)
        return;
    if (selection == NULL) {
        ObserveVector(vector, begin, end, object, access, true);
        return;
    }
    Observe(selection + begin, (Size)(end - begin) * sizeof(bool), object, DA_READ);
    for (int row = begin; row < end;) {
        if (!selection[row]) {
            ++row;
            continue;
        }
        int start = row++;
        while (row < end && selection[row])
            ++row;
        ObserveVector(vector, start, row, object, access, true);
    }
}

void OperatorDataAccessScope::ObserveStorage(ScalarVector* vector, int begin, int end,
    OperatorDataAccessObject object, OperatorDataAccessKind access, bool payload, bool nullSlots)
{
    unsigned int live = 1U << access;
    ObserveVectorRoles(vector, begin, end, object, live, nullSlots ? live : 0, payload);
}

void OperatorDataAccessScope::ObserveCopiedSource(ScalarVector* vector, int begin, int end,
    OperatorDataAccessObject object, bool singleRow)
{
    unsigned int live = (1U << DA_READ) | (1U << DA_DELIVERED);
    ObserveVectorRoles(vector, begin, end, object, live,
        !vector->m_desc.encoded && !singleRow ? 1U << DA_READ : 0);
}

/* A pointer handoff has logical payload, but is not a page read or copy. */
static uint64 BatchBytes(VectorBatch* batch)
{
    uint64 bytes = batch->m_checkSel ? (uint64)batch->m_rows * sizeof(bool) : 0;
    for (int col = 0; col < batch->m_cols; ++col) {
        ScalarVector* vector = &batch->m_arr[col];
        int rows = Min(batch->m_rows, vector->m_rows);
        bytes += rows;
        for (int row = 0; row < rows; ++row) {
            if (IS_NULL(vector->m_flag[row]))
                continue;
            bytes += sizeof(ScalarValue);
            if (vector->m_desc.encoded) {
                Datum value = ScalarVector::Decode(vector->m_vals[row]);
                bytes += vector->m_desc.typeId == NAMEOID ? datumGetSize(value, false, -2) : VARSIZE_ANY(value);
            }
        }
    }
    return bytes;
}

void OperatorDataAccessEnter(OperatorDataAccessFrame* frame, PlanState* node)
{
    OperatorDataAccessFlushPending();
    frame->node = node;
    frame->previous = currentFrame;
    frame->inputs = NULL;
    frame->stateScope = NULL;
    frame->jitStateScope = NULL;
    frame->expressionDepth = 0;
    frame->executionNode = DA_NODE_NOT_OBSERVED;
    currentFrame = frame;
    operator_data_access_active = true;
}

static void ForgetInputs(OperatorDataAccessFrame* frame, int producer = -1, const ScalarVector* vector = NULL)
{
    if (frame->inputs == NULL)
        return;
    OperatorDataAccessStats* stats = GetStats(frame->node->state);
    OperatorDataAccessInput** link = &frame->inputs;
    while (*link != NULL) {
        OperatorDataAccessInput* input = *link;
        if (vector != NULL ? (input->values == vector->m_vals && input->flags == vector->m_flag) :
            (producer < 0 || input->producer == producer)) {
            *link = input->next;
            input->next = stats->freeInputs;
            stats->freeInputs = input;
        } else
            link = &input->next;
    }
}

void OperatorDataAccessForgetInputs()
{
    if (currentFrame != NULL)
        ForgetInputs(currentFrame);
}

static void RegisterInput(OperatorDataAccessFrame* frame, const ScalarVector* vector, int producer)
{
    for (OperatorDataAccessInput* input = frame->inputs; input != NULL; input = input->next) {
        if (input->values == vector->m_vals && input->flags == vector->m_flag) {
            input->producer = producer;
            return;
        }
    }
    OperatorDataAccessStats* stats = GetStats(frame->node->state);
    OperatorDataAccessInput* input = stats->freeInputs;
    if (input != NULL)
        stats->freeInputs = input->next;
    else
        input = (OperatorDataAccessInput*)MemoryContextAlloc(stats->context, sizeof(*input));
    input->values = vector->m_vals;
    input->flags = vector->m_flag;
    input->producer = producer;
    input->next = frame->inputs;
    frame->inputs = input;
}

void OperatorDataAccessFlushPending()
{
    if (currentFrame != NULL) {
        if (currentFrame->stateScope != NULL)
            currentFrame->stateScope->Flush();
        if (currentFrame->jitStateScope != NULL)
            currentFrame->jitStateScope->Flush();
    }
}

OperatorDataAccessScope* OperatorDataAccessState(bool jitCandidate)
{
    if (currentFrame == NULL || !u_sess->attr.attr_sql.enable_intra_operator_data_access)
        return NULL;
    OperatorDataAccessScope*& scope = jitCandidate ? currentFrame->jitStateScope : currentFrame->stateScope;
    if (scope == NULL) {
        OperatorDataAccessStats* stats = GetStats(currentFrame->node->state);
        void* storage = MemoryContextAlloc(stats->context, sizeof(OperatorDataAccessScope));
        scope = new (storage) OperatorDataAccessScope(
            currentFrame->node, jitCandidate ? DA_JIT_STATE_CANDIDATE : DA_OPERATOR_STATE, 0);
    }
    return scope;
}

void OperatorDataAccessRestore(OperatorDataAccessFrame* frame, bool discard)
{
    if (frame->stateScope != NULL) {
        if (discard)
            frame->stateScope->Discard();
        frame->stateScope->~OperatorDataAccessScope();
        pfree(frame->stateScope);
        frame->stateScope = NULL;
    }
    if (frame->jitStateScope != NULL) {
        if (discard)
            frame->jitStateScope->Discard();
        frame->jitStateScope->~OperatorDataAccessScope();
        pfree(frame->jitStateScope);
        frame->jitStateScope = NULL;
    }
    ForgetInputs(frame);
    currentFrame = frame->previous;
    operator_data_access_active = currentFrame != NULL;
}

PlanState* OperatorDataAccessCurrentNode()
{
    return currentFrame == NULL ? NULL : currentFrame->node;
}

void OperatorDataAccessLeave(OperatorDataAccessFrame* frame, VectorBatch* result)
{
    if (frame->previous != NULL && frame->previous->node->state == frame->node->state && BatchIsNull(result))
        ForgetInputs(frame->previous, frame->node->plan->plan_node_id);
    if (frame->previous != NULL && frame->previous->node->state == frame->node->state && !BatchIsNull(result)) {
        OperatorDataAccessStats* stats = GetStats(frame->node->state);
        int producer = frame->node->plan->plan_node_id;
        /* Save addresses, not vector objects that early-free may later destroy. */
        ForgetInputs(frame->previous, producer);
        for (int col = 0; col < result->m_cols; ++col)
            RegisterInput(frame->previous, &result->m_arr[col], producer);
        if (u_sess->attr.attr_sql.enable_inter_operator_data_access) {
            OperatorDataAccessKey key = {};
            key.plan = frame->node->plan->plan_node_id;
            key.peerPlan = frame->previous->node->plan->plan_node_id;
            key.sourceWorker = key.targetWorker = stats->worker;
            key.operation = DA_BATCH_HANDOFF;
            key.object = key.access = -1;
            key.executionNode = DA_NODE_UNKNOWN;
            key.pageNode = DA_NODE_NOT_OBSERVED;
            AddRecord(stats, key, 0, 0, 1, result->m_rows);
            key.object = DA_BATCH;
            key.access = DA_DELIVERED;
            AddRecord(stats, key, BatchBytes(result), 0, 0, 0);
        }
    }
    OperatorDataAccessRestore(frame);
}

static int InputProducer(const ScalarVector* vector)
{
    for (OperatorDataAccessInput* input = currentFrame->inputs; input != NULL; input = input->next) {
        if (input->values == vector->m_vals && input->flags == vector->m_flag)
            return input->producer;
    }
    return 0;
}

void OperatorDataAccessReadVector(ScalarVector* vector, const bool* selection, int begin, int end,
    bool jitCandidate)
{
    if (currentFrame == NULL || vector == NULL)
        return;
    if (end < 0)
        end = vector->m_rows;
    if (end <= begin)
        return;
    int producer = InputProducer(vector);
    OperatorDataAccessOperation operation = producer != 0 ?
        (jitCandidate ? DA_JIT_INPUT_CANDIDATE : DA_CONSUMER_INPUT) :
        (jitCandidate ? DA_JIT_CANDIDATE : DA_EXPRESSION);
    uint64 rows = end - begin;
    if (selection != NULL) {
        rows = 0;
        for (int row = begin; row < end; ++row)
            rows += selection[row];
    }
    int worker = (int)u_sess->stream_cxt.smp_id;
    OperatorDataAccessScope scope(currentFrame->node, operation, rows, producer, 0, worker, worker);
    scope.ObserveSelected(vector, selection, begin, end, DA_INPUT, DA_READ);
}

void OperatorDataAccessReadBatch(VectorBatch* batch)
{
    if (!operator_data_access_active || BatchIsNull(batch))
        return;
    for (int col = 0; col < batch->m_cols; ++col)
        OperatorDataAccessReadVector(&batch->m_arr[col], batch->m_checkSel ? batch->m_sel : NULL,
            0, Min(batch->m_rows, batch->m_arr[col].m_rows));
}

void OperatorDataAccessWriteVector(ScalarVector* vector, const bool* selection)
{
    if (currentFrame == NULL || vector == NULL)
        return;
    OperatorDataAccessScope scope(currentFrame->node, DA_EXPRESSION, vector->m_rows);
    scope.ObserveSelected(vector, selection, 0, vector->m_rows, DA_BATCH, DA_WRITE);
    /* Computed values in a reused input allocation now belong to this operator. */
    ForgetInputs(currentFrame, -1, vector);
}

ScalarVector* OperatorDataAccessEval(ExprState* expression, ExprContext* context,
    bool* selection, ScalarVector* output, ExprDoneCond* done)
{
    OperatorDataAccessFrame* frame = currentFrame;
    int depth = frame->expressionDepth++;
    ScalarVector* result = NULL;
    PG_TRY();
    {
        result = expression->vecExprFun(expression, context, selection, output, done);
    }
    PG_CATCH();
    {
        frame->expressionDepth = depth;
        PG_RE_THROW();
    }
    PG_END_TRY();
    frame->expressionDepth = depth;
    const bool* active = context->m_fUseSelection ? selection : NULL;
    /* A root Var only forwards a pointer. Nested results are consumed by a parent expression. */
    if (result == output)
        OperatorDataAccessWriteVector(result, active);
    if (depth > 0)
        OperatorDataAccessReadVector(result, active);
    return result;
}

struct JitAccessContext { ExprContext* expression; };
static bool JitAccessWalker(Node* node, JitAccessContext* state)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        VectorBatch* batch = var->varno == INNER_VAR ? state->expression->ecxt_innerbatch :
            (var->varno == OUTER_VAR ? state->expression->ecxt_outerbatch : state->expression->ecxt_scanbatch);
        if (batch != NULL && var->varattno > 0 && var->varattno <= batch->m_cols)
            OperatorDataAccessReadVector(&batch->m_arr[var->varattno - 1],
                state->expression->m_fUseSelection && batch->m_checkSel ? batch->m_sel : NULL,
                0, Min(batch->m_rows, batch->m_arr[var->varattno - 1].m_rows), true);
        return false;
    }
    return expression_tree_walker(node, (bool (*)())JitAccessWalker, state);
}

void OperatorDataAccessJitInputs(PlanState* node, void* expression, ExprContext* context)
{
    if (node == NULL || !operator_data_access_active)
        return;
    JitAccessContext state = {context};
    JitAccessWalker((Node*)expression, &state);
}

void OperatorDataAccessJitBatch(PlanState* node, VectorBatch* batch, void* expression)
{
    if (!operator_data_access_active || BatchIsNull(batch))
        return;
    ExprContext context = {};
    context.ecxt_outerbatch = context.ecxt_scanbatch = batch;
    context.align_rows = batch->m_rows;
    context.m_fUseSelection = batch->m_checkSel;
    OperatorDataAccessJitInputs(node, expression, &context);
}

void OperatorDataAccessCU(CU* cu, ScalarVector* output, int begin, int end,
    int rowCursor, int rows, int deadRows, int attlen, bool hasDeadRow)
{
    if (currentFrame == NULL || !u_sess->attr.attr_sql.enable_intra_operator_data_access)
        return;
    OperatorDataAccessScope scope(currentFrame->node, DA_SCAN_ACCESS, rows);
    scope.Observe(cu->m_srcData + begin, end - begin, DA_CU_VALUES, DA_READ);
    if (cu->HasNullValue() && rows + deadRows > 0) {
        Size first = (Size)rowCursor / 8;
        Size last = ((Size)rowCursor + rows + deadRows + 7) / 8;
        scope.Observe(cu->m_nulls + first, last - first, DA_CU_NULLS, DA_READ);
    }
    if (attlen == -1 && !hasDeadRow && !cu->HasNullValue())
        scope.Observe(cu->m_offset + rowCursor, ((Size)rows + 1) * sizeof(int32), DA_CU_OFFSETS, DA_READ);
    scope.ObserveVector(output, 0, rows, DA_BATCH, DA_WRITE, true);
}

/* Must run before ToVectorLateRead: its output may overwrite the TID vector. */
void OperatorDataAccessCULate(CU* cu, ScalarVector* tids, int attlen)
{
    if (currentFrame == NULL || !u_sess->attr.attr_sql.enable_intra_operator_data_access)
        return;
    int rows = tids->m_rows;
    OperatorDataAccessScope scope(currentFrame->node, DA_SCAN_ACCESS, rows);
    /* TIDs occupy ScalarValue slots but retain the original column descriptor.
     * Never decode them as that column's encoded/variable-length payload. */
    scope.Observe(tids->m_vals, (Size)rows * sizeof(ScalarValue), DA_INPUT, DA_READ);
    for (int row = 0; row < rows; ++row) {
        uint32 index = cu->imcsDesc != NULL ? *(uint32*)(tids->m_vals + row) - 1 :
            ItemPointerGetOffsetNumber((ItemPointer)(tids->m_vals + row)) - 1;
        if (cu->HasNullValue()) {
            scope.Observe(cu->m_nulls + index / 8, sizeof(uint8), DA_CU_NULLS, DA_READ);
            if (cu->IsNull(index))
                continue;
        }
        int offset = (cu->HasNullValue() || attlen < 0) ? cu->m_offset[index] : index * attlen;
        if (cu->HasNullValue() || attlen < 0)
            scope.Observe(cu->m_offset + index, sizeof(int32), DA_CU_OFFSETS, DA_READ);
        Size length = attlen > 0 ? (Size)attlen :
            (attlen == -1 ? VARSIZE_ANY(cu->m_srcData + offset) : strlen(cu->m_srcData + offset) + 1);
        scope.Observe(cu->m_srcData + offset, length, DA_CU_VALUES, DA_READ);
    }
}

void OperatorDataAccessCULateOutput(ScalarVector* output, int rows)
{
    if (currentFrame == NULL || !u_sess->attr.attr_sql.enable_intra_operator_data_access)
        return;
    /* The input scope already accounts for this scan's calls and rows. */
    OperatorDataAccessScope scope(currentFrame->node, DA_SCAN_ACCESS, 0);
    scope.ObserveVector(output, 0, rows, DA_BATCH, DA_WRITE, true);
}

OperatorDataAccessStreamBatch::OperatorDataAccessStreamBatch(int channels)
    : m_previous(currentStreamBatch), m_node(OperatorDataAccessCurrentNode()), m_slots(NULL),
      m_channels(channels), m_executionNode(DA_NODE_UNKNOWN)
{
    if (m_node == NULL)
        return;
    OperatorDataAccessStats* stats = GetStats(m_node->state);
    m_slots = MemoryContextAlloc(stats->context, channels * (sizeof(OperatorDataAccessScope) + sizeof(bool)));
    MemSet((char*)m_slots + channels * sizeof(OperatorDataAccessScope), 0, channels * sizeof(bool));
    m_executionNode = ExecutionNode(stats);
    currentStreamBatch = this;
}

OperatorDataAccessScope* OperatorDataAccessStreamBatch::Channel(int channel, int stream)
{
    if (m_slots == NULL)
        return NULL;
    Assert(channel >= 0 && channel < m_channels);
    bool* active = (bool*)((char*)m_slots + m_channels * sizeof(OperatorDataAccessScope));
    OperatorDataAccessScope* scope = (OperatorDataAccessScope*)((char*)m_slots + channel * sizeof(OperatorDataAccessScope));
    if (!active[channel]) {
        int worker = (int)u_sess->stream_cxt.smp_id;
        new (scope) OperatorDataAccessScope(m_node, DA_STREAM_SEND, 0, 0, stream,
            worker, channel, m_executionNode);
        active[channel] = true;
    }
    return scope;
}

void OperatorDataAccessStreamBatch::Close(bool discard)
{
    currentStreamBatch = m_previous;
    if (m_slots == NULL)
        return;
    bool* active = (bool*)((char*)m_slots + m_channels * sizeof(OperatorDataAccessScope));
    for (int channel = 0; channel < m_channels; ++channel) {
        if (!active[channel])
            continue;
        OperatorDataAccessScope* scope = (OperatorDataAccessScope*)((char*)m_slots + channel * sizeof(OperatorDataAccessScope));
        if (discard)
            scope->Discard();
        scope->~OperatorDataAccessScope();
        active[channel] = false;
    }
    pfree(m_slots);
    m_slots = NULL;
}

static void ObserveCopy(OperatorDataAccessScope* copy, VectorBatch* source, VectorBatch* target,
    int sourceBegin, int targetBegin, int rows, bool singleRow, bool sending)
{
    for (int col = 0; col < source->m_cols; ++col) {
        copy->ObserveCopiedSource(&source->m_arr[col], sourceBegin, sourceBegin + rows,
            sending ? DA_INPUT : DA_SHARED_BUFFER, singleRow);
        copy->ObserveVector(&target->m_arr[col], targetBegin, targetBegin + rows,
            sending ? DA_SHARED_BUFFER : DA_BATCH, DA_WRITE, singleRow);
    }
}

void OperatorDataAccessBatchCopy(VectorBatch* source, VectorBatch* target, int sourceBegin,
    int targetBegin, int rows, bool singleRow, bool deep)
{
    if (currentFrame == NULL || rows <= 0 || source->m_cols == 0)
        return;
    OperatorDataAccessScope write(currentFrame->node, DA_BATCH_COPY, rows);
    for (int col = 0; col < source->m_cols; ++col) {
        int producer = InputProducer(&source->m_arr[col]);
        OperatorDataAccessScope read(currentFrame->node, producer != 0 ? DA_COPY_INPUT : DA_BATCH_COPY, rows, producer);
        bool nullSlots = !singleRow && (!deep || !source->m_arr[col].m_desc.encoded);
        read.ObserveStorage(&source->m_arr[col], sourceBegin, sourceBegin + rows, DA_INPUT,
            DA_READ, deep, nullSlots);
        write.ObserveStorage(&target->m_arr[col], targetBegin, targetBegin + rows, DA_BATCH,
            DA_WRITE, deep, nullSlots);
        ForgetInputs(currentFrame, -1, &target->m_arr[col]);
        if (producer != 0)
            RegisterInput(currentFrame, &target->m_arr[col], producer);
    }
}

void OperatorDataAccessStreamCopy(VectorBatch* source, VectorBatch* target, int sourceBegin,
    int targetBegin, int rows, bool singleRow, bool sending, int stream, int peerWorker, bool publish)
{
    if (sending && singleRow && currentStreamBatch != NULL) {
        OperatorDataAccessScope* copy = currentStreamBatch->Channel(peerWorker, stream);
        if (copy != NULL && copy->Enabled()) {
            copy->RecordCall(rows);
            ObserveCopy(copy, source, target, sourceBegin, targetBegin, rows, true, true);
            /* Query destination pages before a full batch can be consumed and reset. */
            if (publish)
                copy->Flush();
        }
        return;
    }
    int worker = (int)u_sess->stream_cxt.smp_id;
    OperatorDataAccessScope copy(OperatorDataAccessCurrentNode(), sending ? DA_STREAM_SEND : DA_STREAM_RECV,
        rows, 0, stream, sending ? worker : peerWorker, sending ? peerWorker : worker);
    if (copy.Enabled())
        ObserveCopy(&copy, source, target, sourceBegin, targetBegin, rows, singleRow, sending);
}

void OperatorDataAccessFlush(EState* estate)
{
    OperatorDataAccessStats* stats = estate->es_operator_data_access;
    if (stats == NULL)
        return;
    const char* operations[] = {"agg_transition_slots", "sonic_agg_transition_slots", "batch_handoff",
        "stream_send_copy", "stream_recv_copy", "agg_jit_state_candidate", "sonic_agg_jit_state_candidate",
        "consumer_input_read", "expression_access", "operator_state", "jit_local_candidate", "jit_input_candidate",
        "scan_access", "batch_copy", "copy_input_read", "jit_state_candidate"};
    const char* objects[] = {"input", "state", "batch", "shared_buffer", "cu_values", "cu_nulls", "cu_offsets"};
    const char* accesses[] = {"read", "write", "delivered"};
    HASH_SEQ_STATUS scan;
    hash_seq_init(&scan, stats->groups);
    OperatorDataAccessEntry* entry = NULL;
    while ((entry = (OperatorDataAccessEntry*)hash_seq_search(&scan)) != NULL) {
        const OperatorDataAccessKey& key = entry->key;
        bool input = key.operation == DA_CONSUMER_INPUT || key.operation == DA_JIT_INPUT_CANDIDATE ||
            key.operation == DA_COPY_INPUT;
        bool handoff = key.operation == DA_BATCH_HANDOFF;
        bool inter = input || handoff || key.operation == DA_STREAM_SEND || key.operation == DA_STREAM_RECV;
        int producer = input ? key.peerPlan : (handoff || key.operation == DA_STREAM_SEND ? key.plan : 0);
        int consumer = handoff ? key.peerPlan : (key.operation == DA_STREAM_SEND ? 0 : key.plan);
        const char* locality = key.pageNode == DA_NODE_NOT_OBSERVED ? "not_observed" :
            (key.executionNode < 0 || key.pageNode < 0 ? "unknown" :
            (key.executionNode == key.pageNode ? "local" : "remote"));
        ereport(LOG, (errmsg("operator_data_access record=%s query_id=%lu tid=%ld worker_id=%d instance=%p "
            "plan_id=%d peer_plan_id=%d stream_id=%d source_worker=%d target_worker=%d operation=%s "
            "access_scope=%s producer_plan_id=%d consumer_plan_id=%d "
            "object=%s access=%s execution_node=%d page_node=%d locality=%s "
            "range_bytes=%lu page_observations=%lu calls=%lu rows=%lu",
            key.access < 0 ? "operation" : "range", stats->query, stats->tid, stats->worker, (void*)stats,
            key.plan, key.peerPlan, key.stream, key.sourceWorker, key.targetWorker, operations[key.operation],
            inter ? "inter" : "intra", producer, consumer,
            key.object < 0 ? "none" : objects[key.object], key.access < 0 ? "none" : accesses[key.access],
            key.executionNode, key.pageNode, locality, entry->bytes, entry->visits, entry->calls, entry->rows),
            errhidestmt(true), ErrOutToClient(false)));
    }
    ereport(LOG, (errmsg("operator_data_access record=end query_id=%lu tid=%ld worker_id=%d instance=%p "
        "intra=%d inter=%d page_queries=%lu failed_queries=%lu first_query_errno=%d cpu_samples=%lu",
        stats->query, stats->tid, stats->worker, (void*)stats, (int)stats->intra, (int)stats->inter,
        stats->pageQueries, stats->failedQueries, stats->firstErrno, stats->cpuSamples),
        errhidestmt(true), ErrOutToClient(false)));
    estate->es_operator_data_access = NULL;
}
#endif
