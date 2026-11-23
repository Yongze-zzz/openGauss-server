/*
 * LLC-local group state for Sonic Hash Aggregate. Workers attach their own
 * Sonic workspace to one shared shard for a batch, with exclusive ownership
 * of its state. Only rows rejected by the shard's memory budget are spooled.
 */
#include "postgres.h"

#ifdef ENABLE_HTAP
#include <sched.h>
#include "access/attnum.h"
#include "access/htap/imcs_partition_wise.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "distributelayer/streamProducer.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "storage/buf/buffile.h"
#include "utils/lsyscache.h"
#include "vecexecutor/vec_cache_coord.h"
#include "vecexecutor/vecexecutor.h"
#include "vectorsonic/vsonichashagg.h"
#include "vectorsonic/vsonicsharedagg.h"

constexpr int SONIC_AGG_MIN_LANE_KB = 1024;
constexpr int SONIC_AGG_MAX_PARTITIONS = 128;

struct SonicAggInputSpool {
    uint64 rows;
};

struct SonicAggShard {
    pthread_mutex_t mutex;
    SonicAggSharedTable table;
    uint64 reportedBytes;
};

struct SonicAggDomain {
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    bool initialized;
    bool stopped;
    int arrived;
    int workers;
    int partitions;
    int initializedShards;
    uint32 nextPartition;
    uint64 budget;
    uint64 initialGroups;
    uint64 activeState;
    uint64 peakState;
    SonicAggShard* shards;
};

struct SonicAggLane {
    SonicAggDomain* domain;
    int llc;
    SonicAggInputSpool* spools;
};

struct SonicAggExchange {
    SonicAggExchange* next;
    MemoryContext memory;
    int lanes;
    int node;
    int stream;
    const char* fallback;
    SonicAggLane* lane;
    SonicAggDomain** domains;
    SharedFileSet files;
    bool hasFiles;
};

static uint64 SonicAggClock(clockid_t clock = CLOCK_MONOTONIC)
{
    timespec now;
    clock_gettime(clock, &now);
    return (uint64)now.tv_sec * 1000000000 + now.tv_nsec;
}

static void UpdatePeak(uint64* peak, uint64 value)
{
    uint64 last = __atomic_load_n(peak, __ATOMIC_RELAXED);
    while (value > last &&
           !__atomic_compare_exchange_n(peak, &last, value, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
    }
}

static const char* SonicAggPlanFallback(VecAgg* agg)
{
    Plan* plan = &agg->plan;
    if (!agg->is_sonichash || agg->aggstrategy != AGG_HASHED || agg->numCols == 0 || agg->is_dummy)
        return "non_grouped_sonic";
    Plan* input = outerPlan(plan);
    if (input != NULL && IsA(input, VecPartIterator) && ((VecPartIterator*)input)->wholePartitionPerWorker)
        return "partition_local_final";
    if (agg->groupingSets != NIL || agg->chain != NIL || agg->unique_check ||
        agg->aggParams != NULL || plan->allParam != NULL || plan->extParam != NULL ||
        plan->recursive_union_plan_nodeid > 0)
        return "grouping_or_rescan";
    if (contain_volatile_functions((Node*)plan->targetlist) || contain_volatile_functions((Node*)plan->qual))
        return "volatile_expression";
    if (SET_NODEMEM(plan->operatorMemKB[0], plan->dop) < SONIC_AGG_MIN_LANE_KB)
        return "memory_budget";
    return NULL;
}

void InitSonicAggExchange(StreamSharedContext* context, Stream* stream)
{
    Plan* root = outerPlan(&stream->scan.plan);
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group == NULL || root == NULL || !IsA(root, VecAgg) || stream->smpDesc.producerDop <= 1)
        return;

    SonicAggExchange* exchange = (SonicAggExchange*)palloc0(sizeof(SonicAggExchange));
    exchange->memory = context->localStreamMemoryCtx;
    exchange->node = root->plan_node_id;
    exchange->stream = stream->scan.plan.plan_node_id;
    exchange->lanes = stream->smpDesc.producerDop;
    exchange->fallback = SonicAggPlanFallback((VecAgg*)root);
    if (exchange->fallback == NULL &&
        (!u_sess->attr.attr_sql.enable_imcs_nps || u_sess->attr.attr_sql.enable_imcs_adapt ||
            u_sess->attr.attr_sql.enable_imcs_stream_consumer_placement))
        exchange->fallback = "unstable_lane_placement";
    if (exchange->fallback == NULL &&
        (!IsA((Plan*)stream, VecStream) || stream->sort != NULL || stream->is_sorted || stream->is_recursive_local ||
            (stream->smpDesc.distriType != LOCAL_DISTRIBUTE &&
                !(stream->smpDesc.distriType == LOCAL_ROUNDROBIN && stream->smpDesc.consumerDop == 1))))
        exchange->fallback = "stream_shape";

    context->sonicAggExchange = exchange;
    exchange->next = __atomic_load_n(&group->m_sonicAggExchanges, __ATOMIC_ACQUIRE);
    while (!__atomic_compare_exchange_n(&group->m_sonicAggExchanges, &exchange->next, exchange, true,
        __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
    }
    if (exchange->fallback != NULL)
        return;

    int cpus[CPU_SETSIZE];
    ImcsPartitionWiseGetWorkerCpus(exchange->lanes, cpus);
    exchange->domains = (SonicAggDomain**)palloc0(CPU_SETSIZE * sizeof(SonicAggDomain*));
    exchange->lane = (SonicAggLane*)palloc0(exchange->lanes * sizeof(SonicAggLane));
    for (int i = 0; i < exchange->lanes; ++i) {
        int llc = RhjLlcDomain(cpus[i]);
        if (llc < 0 || llc >= CPU_SETSIZE) {
            exchange->fallback = "missing_llc_topology";
            return;
        }
        SonicAggDomain*& domain = exchange->domains[llc];
        if (domain == NULL)
            domain = (SonicAggDomain*)palloc0(sizeof(SonicAggDomain));
        domain->workers++;
        exchange->lane[i].llc = llc;
        exchange->lane[i].domain = domain;
    }

    bool anyShared = false;
    for (int id = 0; id < CPU_SETSIZE; ++id) {
        SonicAggDomain* domain = exchange->domains[id];
        if (domain == NULL || domain->workers < 2)
            continue;
        domain->partitions = 4;
        while (domain->partitions < 2 * domain->workers && domain->partitions < SONIC_AGG_MAX_PARTITIONS)
            domain->partitions *= 2;
        if (pthread_mutex_init(&domain->mutex, NULL) != 0)
            ereport(ERROR, (errmsg("cannot initialize Sonic aggregate input mutex")));
        if (pthread_cond_init(&domain->ready, NULL) != 0) {
            pthread_mutex_destroy(&domain->mutex);
            ereport(ERROR, (errmsg("cannot initialize Sonic aggregate input condition")));
        }
        domain->initialized = true;
        domain->shards = (SonicAggShard*)palloc0(domain->partitions * sizeof(SonicAggShard));
        domain->budget = (uint64)SET_NODEMEM(root->operatorMemKB[0], root->dop) * 1024 *
            domain->workers / domain->partitions;
        domain->initialGroups = Min((uint64)Max(((VecAgg*)root)->numGroups, 1L) / domain->partitions, 65536UL);
        for (int part = 0; part < domain->partitions; ++part) {
            if (pthread_mutex_init(&domain->shards[part].mutex, NULL) != 0)
                ereport(ERROR, (errmsg("cannot initialize Sonic aggregate shard mutex")));
            ++domain->initializedShards;
        }
        anyShared = true;
    }
    if (!anyShared) {
        exchange->fallback = "one_lane_per_llc";
        return;
    }
    for (int i = 0; i < exchange->lanes; ++i) {
        SonicAggLane& lane = exchange->lane[i];
        if (lane.domain->initialized)
            lane.spools = (SonicAggInputSpool*)palloc0(lane.domain->partitions * sizeof(SonicAggInputSpool));
    }
    SharedFileSetInit(&exchange->files);
}

void DestroySonicAggExchanges(StreamNodeGroup* group)
{
    for (SonicAggExchange* exchange = group->m_sonicAggExchanges; exchange != NULL; exchange = exchange->next) {
        if (exchange->hasFiles)
            SharedFileSetDeleteAll(&exchange->files);
        if (exchange->domains == NULL)
            continue;
        for (int id = 0; id < CPU_SETSIZE; ++id) {
            SonicAggDomain* domain = exchange->domains[id];
            if (domain != NULL && domain->initialized) {
                for (int part = 0; part < domain->initializedShards; ++part)
                    pthread_mutex_destroy(&domain->shards[part].mutex);
                pthread_cond_destroy(&domain->ready);
                pthread_mutex_destroy(&domain->mutex);
            }
        }
    }
    group->m_sonicAggExchanges = NULL;
}

static bool SupportedSonicAggType(Oid type)
{
    switch (type) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case NUMERICOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
            return true;
        default:
            return false;
    }
}

static const char* SonicAggExpressionFallback(VecAggState* node, TupleDesc input)
{
    for (int col = 0; col < input->natts; ++col) {
        if (!SupportedSonicAggType(TupleDescAttr(input, col)->atttypid))
            return "input_type";
    }
    ListCell* cell;
    foreach (cell, node->aggs) {
        Aggref* ref = (Aggref*)((AggrefExprState*)lfirst(cell))->xprstate.expr;
        if (ref->aggdistinct != NIL || ref->aggorder != NIL || ref->aggdirectargs != NIL ||
            ref->aggfilter != NULL || ref->aggkind != AGGKIND_NORMAL)
            return "aggregate_modifier";
        if (ref->aggfnoid >= FirstNormalObjectId || get_func_namespace(ref->aggfnoid) != PG_CATALOG_NAMESPACE)
            return "user_aggregate";
        char* name = get_func_name(ref->aggfnoid);
        bool supported = name != NULL && (strcmp(name, "count") == 0 || strcmp(name, "sum") == 0 ||
            strcmp(name, "min") == 0 || strcmp(name, "max") == 0 || strcmp(name, "avg") == 0);
        pfree_ext(name);
        if (!supported || !SupportedSonicAggType(ref->aggtype))
            return "aggregate_function";
    }
    return node->aggs == NIL ? "no_aggregate" : NULL;
}

static void SpoolName(char* name, Size size, int lane, int part)
{
    int rc = snprintf_s(name, size, size - 1, "%d_%d", lane, part);
    securec_check_ss(rc, "", "");
}

/*
 * Row format: one flag per column, followed by either a scalar value or the
 * complete varlena representation of each non-null value. No batch pointers
 * outlive the child call. Variable-width records carry a length prefix.
 */
static void DecodeSonicAggRow(VectorBatch* batch, int row, const char* data, bool fixed)
{
    const char* flags = data;
    data += batch->m_cols;
    for (int col = 0; col < batch->m_cols; ++col) {
        ScalarVector& vec = batch->m_arr[col];
        vec.m_flag[row] = flags[col];
        if (vec.IsNull(row)) {
            if (fixed)
                data += sizeof(ScalarValue);
            continue;
        }
        if (vec.m_desc.encoded) {
            vec.AddHeaderVar(PointerGetDatum(data), row);
            data += VARSIZE_ANY(data);
        } else {
            memcpy(&vec.m_vals[row], data, sizeof(ScalarValue));
            data += sizeof(ScalarValue);
        }
    }
}

static Size SonicAggRowSize(VectorBatch* batch, int row)
{
    Size bytes = batch->m_cols;
    for (int col = 0; col < batch->m_cols; ++col) {
        ScalarVector& vec = batch->m_arr[col];
        if (vec.IsNull(row))
            continue;
        bytes += vec.m_desc.encoded ? VARSIZE_ANY(DatumGetPointer(ScalarVector::Decode(vec.m_vals[row])))
                                    : sizeof(ScalarValue);
    }
    return bytes;
}

static void EncodeSonicAggRow(VectorBatch* batch, int row, char* data, bool fixed)
{
    char* flags = data;
    data += batch->m_cols;
    for (int col = 0; col < batch->m_cols; ++col) {
        ScalarVector& vec = batch->m_arr[col];
        flags[col] = vec.m_flag[row];
        if (vec.IsNull(row)) {
            if (fixed) {
                memset(data, 0, sizeof(ScalarValue));
                data += sizeof(ScalarValue);
            }
            continue;
        }
        if (vec.m_desc.encoded) {
            const char* value = DatumGetPointer(ScalarVector::Decode(vec.m_vals[row]));
            Size bytes = VARSIZE_ANY(value);
            memcpy(data, value, bytes);
            data += bytes;
        } else {
            memcpy(data, &vec.m_vals[row], sizeof(ScalarValue));
            data += sizeof(ScalarValue);
        }
    }
}

class SonicAggPartitionInput : public SonicHashSource {
public:
    SonicAggPartitionInput(SonicAggExchange& exchange, SonicAggDomain& domain, int partition, TupleDesc desc,
        uint32 fixedWidth)
        : m_exchange(exchange), m_domain(domain), m_partition(partition), m_fixedWidth(fixedWidth)
    {
        m_batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, desc);
    }

    VectorBatch* getBatch() override
    {
        m_batch->Reset();
        int rows = 0;
        while (rows < BatchMaxSize && m_lane < m_exchange.lanes) {
            SonicAggLane& lane = m_exchange.lane[m_lane];
            if (lane.domain != &m_domain) {
                ++m_lane;
                continue;
            }
            SonicAggInputSpool& spool = lane.spools[m_partition];
            if (m_readRows == spool.rows) {
                Close();
                ++m_lane;
                m_readRows = 0;
                continue;
            }
            if (m_file == NULL) {
                char name[32];
                SpoolName(name, sizeof(name), m_lane, m_partition);
                m_file = BufFileOpenShared(&m_exchange.files, name);
            }
            uint32 size = m_fixedWidth;
            if (size == 0 &&
                (BufFileRead(m_file, &size, sizeof(size)) != sizeof(size) || !AllocSizeIsValid(size)))
                ereport(ERROR, (errmsg("invalid Sonic aggregate spill record header")));
            if (m_record.data == NULL)
                initStringInfo(&m_record);
            enlargeStringInfo(&m_record, size);
            if (BufFileRead(m_file, m_record.data, size) != size)
                ereport(ERROR, (errmsg("incomplete Sonic aggregate spill record")));
            DecodeSonicAggRow(m_batch, rows, m_record.data, m_fixedWidth != 0);
            ++rows;
            ++m_readRows;
            ++inputRows;
        }
        m_batch->FixRowCount(rows);
        return rows == 0 ? NULL : m_batch;
    }

    void Close()
    {
        if (m_file != NULL) {
            BufFileClose(m_file);
            m_file = NULL;
        }
    }

    uint64 inputRows = 0;

private:
    SonicAggExchange& m_exchange;
    SonicAggDomain& m_domain;
    int m_partition;
    uint32 m_fixedWidth;
    VectorBatch* m_batch;
    int m_lane = 0;
    uint64 m_readRows = 0;
    BufFile* m_file = NULL;
    StringInfoData m_record = {};
};

struct SonicAggProjection {
    ProjectionInfo* proj;
    ScalarVector* columns;
};

class SonicSharedAggRun : public BaseObject {
public:
    SonicSharedAggRun(VecAggState* node, SonicAggExchange& exchange, int lane, TupleDesc desc, MemoryContext memory)
        : m_node(node), m_exchange(exchange), m_lane(lane), m_desc(desc), m_memory(memory),
          m_domain(*exchange.lane[lane].domain),
          m_stats(u_sess->attr.attr_sql.enable_imcs_shared_sonic_agg_stats),
          m_observeCache(ChipletCacheObserveAggregates()), m_cacheBatches((unsigned)lane & 31)
    {
        m_worker = New(memory) SonicHashAgg(node, INIT_DATUM_ARRAY_SIZE, NULL, 0, 0, true);
        m_batch = New(memory) VectorBatch(memory, desc);
        m_fixedWidth = desc->natts * (sizeof(uint8) + sizeof(ScalarValue));
        for (int col = 0; col < desc->natts; ++col) {
            if (COL_IS_ENCODE(TupleDescAttr(desc, col)->atttypid)) {
                m_fixedWidth = 0;
                break;
            }
        }
        m_projections = (SonicAggProjection*)palloc((node->numaggs + 1) * sizeof(SonicAggProjection));
        SaveProjection(node->ss.ps.ps_ProjInfo);
        for (int i = 0; i < node->numaggs; ++i)
            SaveProjection(node->pervecagg[i].evalproj);
    }

    VectorBatch* Run();
    void End();
    MemoryContext Memory() const { return m_memory; }

private:
    void SaveProjection(ProjectionInfo* proj);
    void RestoreProjections();
    void Consume(VectorBatch* batch);
    bool UpdateShard(int part, uint16 offset, uint16 count, VectorBatch* batch, bool wait);
    void SpillRows(int part, VectorBatch* batch, const uint16* rows, int count);
    bool Seal();
    bool ClaimShard();
    void OpenSpill(uint64 rows);
    void FinishSpill(bool complete);
    void Report();

    VecAggState* m_node;
    SonicAggExchange& m_exchange;
    int m_lane;
    TupleDesc m_desc;
    MemoryContext m_memory;
    SonicAggDomain& m_domain;
    bool m_stats;
    bool m_observeCache;
    unsigned m_cacheBatches;
    bool m_sampleCache = false;
    uint64 m_sampleGroups = 0;
    SonicHashAgg* m_worker;
    VectorBatch* m_batch;
    uint32 m_fixedWidth;
    SonicAggProjection* m_projections;
    int m_projectionCount = 0;
    BufFile* m_writers[SONIC_AGG_MAX_PARTITIONS] = {};
    StringInfoData m_record = {};
    MemoryContext m_spillMemory = NULL;
    SonicAggPartitionInput* m_input = NULL;
    SonicHashAgg* m_spillAgg = NULL;
    uint32 m_hashes[BatchMaxSize];
    uint32 m_shardHashes[BatchMaxSize];
    uint16 m_indices[BatchMaxSize];
    uint16 m_spillRows[BatchMaxSize];
    int m_partition = -1;
    bool m_residentDone = false;
    bool m_sealed = false;
    bool m_done = false;
    uint64 m_inputRows = 0;
    uint64 m_updatedRows = 0;
    uint64 m_spilledRows = 0;
    uint64 m_replayedRows = 0;
    uint64 m_groups = 0;
    uint64 m_emitted = 0;
    uint64 m_parts = 0;
    uint64 m_spillParts = 0;
    uint64 m_diskBytes = 0;
    uint64 m_routeNs = 0;
    uint64 m_lockNs = 0;
    uint64 m_contended = 0;
    uint64 m_blocked = 0;
    uint64 m_updates = 0;
    uint64 m_firstUpdateRows = 0;
    uint64 m_waitNs = 0;
    uint64 m_buildNs = 0;
    uint64 m_outputNs = 0;
    uint64 m_cpuNs = 0;
    uint64 m_largestPart = 0;
    uint64 m_stateBytes = 0;
    uint64 m_statePeak = 0;
};

void SonicSharedAggRun::SaveProjection(ProjectionInfo* proj)
{
    if (proj == NULL || proj->pi_batch == NULL)
        return;
    SonicAggProjection& saved = m_projections[m_projectionCount++];
    saved.proj = proj;
    saved.columns = New(m_memory) ScalarVector[proj->pi_batch->m_cols];
    for (int col = 0; col < proj->pi_batch->m_cols; ++col)
        saved.columns[col] = proj->pi_batch->m_arr[col];
}

void SonicSharedAggRun::RestoreProjections()
{
    /* Simple Vars in an ExecVecProject result alias their source vectors.
     * Detach these aliases before deleting the partition's batches. */
    for (int i = 0; i < m_projectionCount; ++i) {
        SonicAggProjection& saved = m_projections[i];
        VectorBatch* batch = saved.proj->pi_batch;
        for (int col = 0; col < batch->m_cols; ++col)
            batch->m_arr[col] = saved.columns[col];
        batch->m_rows = 0;

        ExprContext* expr = saved.proj->pi_exprContext;
        expr->ecxt_outerbatch = NULL;
        expr->ecxt_innerbatch = NULL;
        expr->ecxt_aggbatch = NULL;
        expr->ecxt_scanbatch = NULL;
    }
}

void SonicSharedAggRun::SpillRows(int part, VectorBatch* batch, const uint16* rows, int count)
{
    AutoContextSwitch alloc(m_memory);
    BufFile*& file = m_writers[part];
    if (file == NULL) {
        char name[32];
        SpoolName(name, sizeof(name), m_lane, part);
        __atomic_store_n(&m_exchange.hasFiles, true, __ATOMIC_RELAXED);
        file = BufFileCreateShared(&m_exchange.files, name);
    }
    if (m_record.data == NULL)
        initStringInfo(&m_record);
    for (int i = 0; i < count; ++i) {
        int row = rows[i];
        bool fixed = m_fixedWidth != 0;
        Size bytes = fixed ? m_fixedWidth : SonicAggRowSize(batch, row);
        if (unlikely(bytes > MaxAllocSize - sizeof(uint32)))
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("Sonic aggregate spill row is too large")));
        uint32 size = bytes;
        Size header = fixed ? 0 : sizeof(size);
        Size framed = header + bytes;
        enlargeStringInfo(&m_record, framed);
        if (!fixed)
            memcpy(m_record.data, &size, sizeof(size));
        EncodeSonicAggRow(batch, row, m_record.data + header, fixed);
        if (BufFileWrite(file, m_record.data, framed) != framed)
            ereport(ERROR, (errmsg("cannot write Sonic aggregate spill row")));
        m_diskBytes += framed;
    }
    m_exchange.lane[m_lane].spools[part].rows += count;
    m_spilledRows += count;
}

bool SonicSharedAggRun::UpdateShard(int part, uint16 offset, uint16 count, VectorBatch* batch, bool wait)
{
    uint64 start = m_stats ? SonicAggClock() : 0;
    if (count == batch->m_rows) {
        memcpy(m_shardHashes, m_hashes, count * sizeof(uint32));
    } else {
        for (int col = 0; col < batch->m_cols; ++col) {
            ScalarVector& source = batch->m_arr[col];
            ScalarVector& target = m_batch->m_arr[col];
            for (int row = 0; row < count; ++row) {
                uint16 index = m_indices[offset + row];
                target.m_vals[row] = source.m_vals[index];
                target.m_flag[row] = source.m_flag[index];
            }
        }
        for (int row = 0; row < count; ++row)
            m_shardHashes[row] = m_hashes[m_indices[offset + row]];
        m_batch->FixRowCount(count);
        batch = m_batch;
    }
    if (m_stats)
        m_routeNs += SonicAggClock() - start;

    SonicAggShard& shard = m_domain.shards[part];
    start = m_stats ? SonicAggClock() : 0;
    if (pthread_mutex_trylock(&shard.mutex) != 0) {
        if (!wait) {
            ++m_contended;
            return false;
        }
        ++m_blocked;
        pthread_mutex_lock(&shard.mutex);
    }
    int spilled = 0;
    PG_TRY();
    {
        if (m_stats) {
            m_lockNs += SonicAggClock() - start;
            start = SonicAggClock();
        }
        uint64 cpu = m_stats ? SonicAggClock(CLOCK_THREAD_CPUTIME_ID) : 0;
        if (shard.table.context == NULL)
            m_worker->InitSharedTable(shard.table, m_exchange.memory, m_domain.budget, m_domain.initialGroups);
        m_worker->AttachSharedTable(shard.table, m_domain.budget);
        spilled = m_worker->UpdateSharedTable(shard.table, batch, m_shardHashes, m_spillRows);
        if (m_sampleCache)
            m_sampleGroups += shard.table.rows;
        if (m_stats) {
            m_buildNs += SonicAggClock() - start;
            m_cpuNs += SonicAggClock(CLOCK_THREAD_CPUTIME_ID) - cpu;
            uint64 bytes = m_worker->GetPartitionBytes();
            if (bytes > shard.reportedBytes) {
                uint64 active = __atomic_add_fetch(&m_domain.activeState, bytes - shard.reportedBytes, __ATOMIC_RELAXED);
                UpdatePeak(&m_domain.peakState, active);
                shard.reportedBytes = bytes;
            }
        }
    }
    PG_CATCH();
    {
        pthread_mutex_unlock(&shard.mutex);
        PG_RE_THROW();
    }
    PG_END_TRY();
    pthread_mutex_unlock(&shard.mutex);

    if (m_updates++ == 0)
        m_firstUpdateRows = m_inputRows;
    m_updatedRows += count - spilled;
    if (spilled != 0)
        SpillRows(part, batch, m_spillRows, spilled);
    return true;
}

void SonicSharedAggRun::Consume(VectorBatch* batch)
{
    m_sampleCache = m_observeCache && (++m_cacheBatches & 31) == 0;
    if (m_sampleCache)
        m_sampleGroups = 0;
    uint64 start = m_stats ? SonicAggClock() : 0;
    uint16 counts[SONIC_AGG_MAX_PARTITIONS] = {};
    uint16 offsets[SONIC_AGG_MAX_PARTITIONS];
    uint16 parts[BatchMaxSize];
    m_worker->HashSharedInput(batch, m_hashes);
    for (int row = 0; row < batch->m_rows; ++row) {
        uint32 hash = m_hashes[row];
        hash ^= hash >> 16;
        hash *= 0x7feb352dU;
        hash ^= hash >> 15;
        parts[row] = hash & (m_domain.partitions - 1);
        ++counts[parts[row]];
    }
    uint16 offset = 0;
    for (int part = 0; part < m_domain.partitions; ++part) {
        offsets[part] = offset;
        offset += counts[part];
    }
    for (int row = 0; row < batch->m_rows; ++row)
        m_indices[offsets[parts[row]]++] = row;
    if (m_stats)
        m_routeNs += SonicAggClock() - start;
    int firstPart = m_lane & (m_domain.partitions - 1);
    int deferred[SONIC_AGG_MAX_PARTITIONS];
    int deferredCount = 0;
    for (int step = 0; step < m_domain.partitions; ++step) {
        int part = (firstPart + step) & (m_domain.partitions - 1);
        if (counts[part] != 0 && !UpdateShard(part, offsets[part] - counts[part], counts[part], batch, false))
            deferred[deferredCount++] = part;
    }
    /* Finish busy shards before pulling the next input, holding at most one
     * mutex at a time. No input outlives this batch. */
    for (int i = 0; i < deferredCount; ++i) {
        int part = deferred[i];
        UpdateShard(part, offsets[part] - counts[part], counts[part], batch, true);
    }
    if (m_sampleCache) {
        uint64 bytes = m_worker->EstimateRecentStateBytes(m_sampleGroups, 4 * (uint64)batch->m_rows * m_domain.workers);
        ChipletCachePublishAggregate(m_exchange.lane[m_lane].llc, m_exchange.node, -1, bytes);
    }
    m_inputRows += batch->m_rows;
}

bool SonicSharedAggRun::ClaimShard()
{
    while (!executorEarlyStop()) {
        int part = __atomic_fetch_add(&m_domain.nextPartition, 1, __ATOMIC_RELAXED);
        if (part >= m_domain.partitions)
            return false;
        SonicAggShard& shard = m_domain.shards[part];
        if (shard.table.context == NULL)
            continue;
        m_partition = part;
        m_residentDone = false;
        ++m_parts;
        m_statePeak = Max(m_statePeak, shard.reportedBytes);
        m_stateBytes += shard.reportedBytes;
        m_largestPart = Max(m_largestPart, (uint64)shard.table.rows);
        m_groups += shard.table.rows;
        m_worker->AttachSharedTable(shard.table, m_domain.budget);
        return true;
    }
    return false;
}

void SonicSharedAggRun::OpenSpill(uint64 rows)
{
    ++m_spillParts;
    m_spillMemory = AllocSetContextCreate(m_memory, "SonicAggregateSpillReplay",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);
    AutoContextSwitch alloc(m_spillMemory);
    m_input = New(m_spillMemory) SonicAggPartitionInput(m_exchange, m_domain, m_partition, m_desc, m_fixedWidth);
    m_spillAgg = New(m_spillMemory) SonicHashAgg(m_node, INIT_DATUM_ARRAY_SIZE, m_input, m_domain.budget, rows);
}

void SonicSharedAggRun::FinishSpill(bool complete)
{
    if (m_spillMemory == NULL)
        return;
    m_input->Close();
    if (complete) {
        m_replayedRows += m_input->inputRows;
        m_groups += m_spillAgg->GetPartitionGroups();
        m_buildNs += m_spillAgg->GetPartitionBuildNs();
    } else {
        m_spillAgg->ClosePartitionFiles();
    }
    RestoreProjections();
    MemoryContextDelete(m_spillMemory);
    m_spillMemory = NULL;
}

VectorBatch* SonicSharedAggRun::Run()
{
    if (m_done)
        return NULL;
    if (!m_sealed) {
        for (;;) {
            CHECK_FOR_INTERRUPTS();
            if (executorEarlyStop())
                return NULL;
            VectorBatch* batch = VectorEngine(outerPlanState(m_node));
            if (BatchIsNull(batch))
                break;
            Consume(batch);
        }
        if (!Seal())
            return NULL;
        m_sealed = true;
        RestoreProjections();
        ExecEarlyFree(outerPlanState(m_node));
    }
    while (!executorEarlyStop()) {
        CHECK_FOR_INTERRUPTS();
        if (m_partition < 0 && !ClaimShard()) {
            m_done = true;
            Report();
            return NULL;
        }
        if (!m_residentDone) {
            uint64 started = m_stats ? SonicAggClock() : 0;
            VectorBatch* result = m_worker->Probe();
            if (m_stats)
                m_outputNs += SonicAggClock() - started;
            if (!BatchIsNull(result)) {
                m_emitted += result->m_rows;
                return result;
            }
            RestoreProjections();
            SonicAggShard& shard = m_domain.shards[m_partition];
            MemoryContextDelete(shard.table.context);
            shard.table.context = NULL;
            m_residentDone = true;
            uint64 spilled = 0;
            if (shard.table.spilling) {
                for (int i = 0; i < m_exchange.lanes; ++i)
                    if (m_exchange.lane[i].domain == &m_domain)
                        spilled += m_exchange.lane[i].spools[m_partition].rows;
            }
            if (spilled != 0)
                OpenSpill(spilled);
        }
        if (m_spillMemory != NULL) {
            AutoContextSwitch alloc(m_spillMemory);
            VectorBatch* result = m_spillAgg->Run();
            if (!BatchIsNull(result)) {
                m_emitted += result->m_rows;
                return result;
            }
        }
        FinishSpill(true);
        m_partition = -1;
    }
    return NULL;
}

void SonicSharedAggRun::Report()
{
    if (!m_stats)
        return;
    ereport(LOG, (errmsg("sonic_shared_agg query_id=%lu node=%d stream=%d lane=%d llc=%d lanes=%d partitions=%d owned=%lu "
        "input_rows=%lu update_rows=%lu groups=%lu emitted=%lu max_part_rows=%lu route_bytes=0 disk_bytes=%lu "
        "buffer_peak=0 state_peak=%lu state_partition_bytes=%lu llc_state_peak=%lu spill_partitions=%lu "
        "route_ns=%lu wait_ns=%lu build_ns=%lu output_ns=%lu owner_cpu_ns=%lu "
        "lock_ns=%lu contended=%lu blocked=%lu updates=%lu first_update_after_rows=%lu spill_rows=%lu replayed_rows=%lu",
        u_sess->debug_query_id, m_exchange.node, m_exchange.stream, m_lane, m_exchange.lane[m_lane].llc,
        m_domain.workers, m_domain.partitions, m_parts, m_inputRows, m_updatedRows + m_replayedRows, m_groups,
        m_emitted, m_largestPart, m_diskBytes, m_statePeak, m_stateBytes,
        __atomic_load_n(&m_domain.peakState, __ATOMIC_RELAXED), m_spillParts,
        m_routeNs, m_waitNs, m_buildNs, m_outputNs, m_cpuNs, m_lockNs, m_contended, m_blocked, m_updates,
        m_firstUpdateRows, m_spilledRows, m_replayedRows), errhidestmt(true), ErrOutToClient(false)));
}

void SonicSharedAggRun::End()
{
    if (!m_sealed) {
        pthread_mutex_lock(&m_domain.mutex);
        m_domain.stopped = true;
        pthread_cond_broadcast(&m_domain.ready);
        pthread_mutex_unlock(&m_domain.mutex);
    }
    for (int part = 0; part < m_domain.partitions; ++part) {
        if (m_writers[part] != NULL)
            BufFileClose(m_writers[part]);
    }
    FinishSpill(false);
}

bool SonicSharedAggRun::Seal()
{
    for (int part = 0; part < m_domain.partitions; ++part) {
        if (m_writers[part] != NULL) {
            BufFileClose(m_writers[part]);
            m_writers[part] = NULL;
        }
    }

    /* Timed waits exist only to observe cancellation from other branches. */
    uint64 start = m_stats ? SonicAggClock() : 0;
    pthread_mutex_lock(&m_domain.mutex);
    if (++m_domain.arrived == m_domain.workers)
        pthread_cond_broadcast(&m_domain.ready);
    while (m_domain.arrived < m_domain.workers && !m_domain.stopped) {
        timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_nsec += 50000000;
        if (deadline.tv_nsec >= 1000000000) {
            ++deadline.tv_sec;
            deadline.tv_nsec -= 1000000000;
        }
        pthread_cond_timedwait(&m_domain.ready, &m_domain.mutex, &deadline);
        pthread_mutex_unlock(&m_domain.mutex);
        CHECK_FOR_INTERRUPTS();
        if (executorEarlyStop())
            return false;
        pthread_mutex_lock(&m_domain.mutex);
    }
    bool ready = !m_domain.stopped;
    pthread_mutex_unlock(&m_domain.mutex);
    if (m_stats)
        m_waitNs = SonicAggClock() - start;
    return ready && !executorEarlyStop();
}

bool StartSonicSharedAgg(VecAggState* node)
{
    StreamProducer* producer = u_sess->stream_cxt.producer_obj;
    StreamSharedContext* context = producer == NULL ? NULL : producer->getSharedContext();
    SonicAggExchange* exchange = context == NULL ? NULL : context->sonicAggExchange;
    unsigned lane = u_sess->stream_cxt.smp_id;
    int nodeId = node->ss.ps.plan->plan_node_id;
    const char* reason = "not_stream_root";
    if (exchange != NULL && exchange->node == nodeId) {
        reason = exchange->fallback;
        if (reason == NULL && node->sharedAggRewind)
            reason = "rewind";
        if (reason == NULL && (lane >= (unsigned)exchange->lanes || exchange->lane[lane].spools == NULL))
            reason = "one_lane_per_llc";
        if (reason == NULL) {
            TupleDesc desc = outerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor;
            reason = SonicAggExpressionFallback(node, desc);
            if (reason == NULL) {
                if (RhjLlcDomain(sched_getcpu()) != exchange->lane[lane].llc)
                    ereport(ERROR, (errmsg("Sonic aggregate worker is outside its assigned LLC")));
                MemoryContext ctx = AllocSetContextCreate(node->ss.ps.state->es_query_cxt, "SonicSharedAggWorker",
                    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);
                AutoContextSwitch alloc(ctx);
                node->sharedAggRun = New(ctx) SonicSharedAggRun(node, *exchange, lane, desc, ctx);
                return true;
            }
        }
    }
    if (lane == 0 && u_sess->attr.attr_sql.enable_imcs_shared_sonic_agg_stats && ((VecAgg*)node->ss.ps.plan)->is_sonichash)
        ereport(LOG, (errmsg("sonic_shared_agg_fallback query_id=%lu node=%d reason=%s",
            u_sess->debug_query_id, nodeId, reason), errhidestmt(true), ErrOutToClient(false)));
    return false;
}

VectorBatch* RunSonicSharedAgg(VecAggState* node)
{
    return ((SonicSharedAggRun*)node->sharedAggRun)->Run();
}

void EndSonicSharedAgg(VecAggState* node)
{
    SonicSharedAggRun* run = (SonicSharedAggRun*)node->sharedAggRun;
    if (run != NULL) {
        node->sharedAggRun = NULL;
        run->End();
        MemoryContextDelete(run->Memory());
    }
}
#endif
