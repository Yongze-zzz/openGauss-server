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
 * -------------------------------------------------------------------------
 *
 * libcomm_memory.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <libcgroup.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>
#include <pthread.h>
#include <numaif.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>

#include "../libcomm_core/mc_tcp.h"
#include "../libcomm_core/mc_poller.h"
#include "../libcomm_utils/libcomm_thread.h"
#include "../libcomm_common.h"
#include "libcomm_lqueue.h"
#include "libcomm_queue.h"
#include "libcomm_lock_free_queue.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamProducer.h"
#include "pgxc/poolmgr.h"
#include "libpq/auth.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/dynahash.h"

#include "vecexecutor/vectorbatch.h"
#ifdef ENABLE_HTAP
#include "access/htap/imcs_partition_wise.h"
#include "storage/lock/s_lock.h"
#include "vecexecutor/operator_data_access.h"
#include "vecexecutor/vec_cache_coord.h"
#include "vecexecutor/vecexecutor.h"
#include "utils/datum.h"
#include <sys/syscall.h>
#include <time.h>
#endif
#include "vecexecutor/vecnodes.h"
#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"

#ifdef ENABLE_UT
#define static
#endif


#define STREAM_SCAN_FINISH 'F'
#define STREAM_SCAN_WAIT 'W'
#define STREAM_SCAN_DATA 'D'

extern bool executorEarlyStop();

/* release memory of communication layer, just for LLT */
int gs_release_comm_memory()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    gs_r_release_comm_memory();
    return 0;
}

/*
 * @Description: Add stream key to g_r_htab_nodeid_skey_to_memory_poll
 *
 * @param[IN] key_s: stream key
 */
void gs_memory_init_entry(StreamSharedContext* sharedContext, int consumerNum, int producerNum)
{
    struct hash_entry* entry = NULL;
    struct hash_entry** poll_entrys = NULL;
    struct hash_entry*** quota_entrys = NULL;

    poll_entrys = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * consumerNum);
    quota_entrys = (struct hash_entry***)palloc(sizeof(struct hash_entry**) * consumerNum);

    for (int i = 0; i < consumerNum; i++) {
        entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
        (void)entry->_init();
        poll_entrys[i] = entry;
        quota_entrys[i] = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * producerNum);
        for (int j = 0; j < producerNum; j++) {
            entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
            (void)entry->_init();
            quota_entrys[i][j] = entry;
        }
    }

    sharedContext->poll_entrys = poll_entrys;
    sharedContext->quota_entrys = quota_entrys;
}

/*
 * @Description: Send Error/Notice through memory
 *
 * @param[IN] buf: Error/Notice string info
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
void gs_message_by_memory(StringInfo buf, StreamSharedContext* sharedContext, int nthChannel)
{
    StringInfo buf_dst = NULL;
    struct hash_entry* entry = NULL;

    /* Copy Error/Notice messages to shared context. */
    buf_dst = sharedContext->messages[nthChannel][u_sess->stream_cxt.smp_id];

    /*
     * If producer is waked up and shared buffer has been consumed while waiting,
     * it can continue to append data to its messages of sharedContext.
     */
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    while (buf_dst->len > 0) {
        (void)entry->_timewait(SINGLE_WAITQUOTA);
    }
    appendBinaryStringInfo(buf_dst, buf->data, buf->len);
    buf_dst->cursor = buf->cursor;

    /* Send signal to dest consumer. */
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();

    pfree(buf->data);
    buf->data = NULL;
}

void gs_memory_disconnect(StreamSharedContext* sharedContext, int nthChannel)
{
    struct hash_entry* entry = NULL;
#ifdef __aarch64__
    pg_memory_barrier();
#endif
    sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = CONN_ERR;
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();
}

#ifdef __aarch64__
/*
 * @Description: Judge whether the databuff is empty
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
bool gs_is_databuff_empty(StreamSharedContext* sharedContext, int nthChannel)
{
    if (sharedContext->vectorized) {
        VectorBatch* batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
        if (batch->m_rows == 0) {
            return true;
        }
    } else {
        TupleVector* tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        if (tupleVec->tuplePointer == 0) {
            return true;
        }
    }
    return false;
}
#endif

/*
 * @Description: Send data to local consumer through shared memory
 *
 * @param[IN] tuple: tuple to be sent
 * @param[IN] batchsrc: batch to be send
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 * @param[IN] nthRow: the Nth row to be sent in batch
 */
#ifdef ENABLE_HTAP
static uint64 StreamFlowClock();
static int StreamFlowNode();

static StreamAggObservation* StreamAggObservationFor(StreamSharedContext* context, int worker, bool receive)
{
    StreamAggCacheDemand* demand = context->aggCacheDemand;
    if (demand == NULL || demand->receiveObservation == NULL)
        return NULL;
    return receive ? &demand->receiveObservation[worker] : &demand->sendObservation[worker];
}

static void StreamAggFlush(StreamSharedContext* context, StreamAggObservation* observation, int worker, bool receive)
{
    if (observation == NULL || observation->windowStart == 0)
        return;
    ereport(LOG, (errmsg("agg_stream_observation stream=%u worker=%d llc=%d receive=%d window_ns=%lu "
        "wait_ns=%lu local_rows=%lu cross_llc_rows=%lu local_copy_ns=%lu cross_llc_copy_ns=%lu",
        context->key_s.planNodeId, worker, context->aggCacheDemand->workerLlc[worker], (int)receive,
        observation->windowStart, observation->waitNs, observation->rows[0], observation->rows[1],
        observation->copyNs[0], observation->copyNs[1]), errhidestmt(true), ErrOutToClient(false)));
    *observation = StreamAggObservation{};
}

static void StreamAggSelectWindow(StreamSharedContext* context, StreamAggObservation* observation, int worker,
    bool receive, uint64 time)
{
    uint64 window = time - time % STREAM_AGG_OBSERVATION_NS;
    if (window != observation->windowStart) {
        StreamAggFlush(context, observation, worker, receive);
        observation->windowStart = window;
    }
}

static void StreamAggObserveWait(StreamSharedContext* context, StreamAggObservation* observation, int worker,
    bool receive, uint64 start, uint64 end)
{
    while (start < end) {
        StreamAggSelectWindow(context, observation, worker, receive, start);
        uint64 boundary = Min(end, observation->windowStart + STREAM_AGG_OBSERVATION_NS);
        observation->waitNs += boundary - start;
        start = boundary;
    }
}

static StreamBatchRing* StreamRing(StreamSharedContext* context, int consumer, int producer)
{
    if (context->batchRings == NULL)
        return NULL;
    return &context->batchRings[(Size)consumer * context->batchRingProducerCount + producer];
}

static uint32 StreamRingPending(const StreamBatchRing* ring)
{
    return __atomic_load_n(&ring->written, __ATOMIC_ACQUIRE) - __atomic_load_n(&ring->read, __ATOMIC_RELAXED);
}

static int StreamRingSlot(const StreamBatchRing* ring, uint32 position)
{
    /* Before activation all sequence numbers map to the original slot. Once
     * activated, the still-pending batch has parity opposite to the spare. */
    return __atomic_load_n(&ring->spare, __ATOMIC_ACQUIRE) != NULL &&
        (position & 1) == ring->spareParity ? 1 : 0;
}

static StreamCacheChannelProfile* StreamCacheChannel(StreamSharedContext* context, int consumer, int producer)
{
    if (context->cacheProfileChannels == NULL)
        return NULL;
    return &context->cacheProfileChannels[(Size)consumer * context->cacheProfileProducerCount + producer];
}
#endif

void gs_memory_send(
    TupleTableSlot* tuple, VectorBatch* batchsrc, StreamSharedContext* sharedContext, int nthChannel, int nthRow)
{
    VectorBatch* batch = NULL;
    TupleVector* tupleVec = NULL;
    bool ready_to_send = false;
    DataStatus dataStatus;
    struct hash_entry* entry = NULL;
#ifdef ENABLE_HTAP
    int producer = (int)u_sess->stream_cxt.smp_id;
    StreamRhjSendTimeline* delivery = sharedContext->rhjSendTimeline == NULL ? NULL :
        &sharedContext->rhjSendTimeline[producer];
    StreamCacheChannelProfile* profile = StreamCacheChannel(sharedContext, nthChannel, producer);
    StreamAggObservation* sendObservation = StreamAggObservationFor(sharedContext, producer, false);
    StreamBatchRing* ring = StreamRing(sharedContext, nthChannel, producer);
    uint32 written = ring == NULL ? 0 : __atomic_load_n(&ring->written, __ATOMIC_RELAXED);
    int slot = 0;
#endif

    WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_FLUSH_DATA,
        u_sess->pgxc_cxt.PGXCNodeId,
        -1,
        u_sess->stream_cxt.producer_obj->getParentPlanNodeId(),
        global_node_definition ? global_node_definition->num_nodes : -1);

    StreamTimeSendStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we should early stop. */
        /* Quit if the connection close, especially in a early close case. */
        if (executorEarlyStop() ||
            __atomic_load_n(&sharedContext->is_connect_end[nthChannel][u_sess->stream_cxt.smp_id], __ATOMIC_ACQUIRE)) {
            (void)pgstat_report_waitstatus(oldStatus);
            return;
        }

#ifdef ENABLE_HTAP
        if (ring != NULL) {
            if (ring->partial)
                break;
            uint32 pending = written - __atomic_load_n(&ring->read, __ATOMIC_ACQUIRE);
            if (pending == 0 || (pending == 1 && ring->spare != NULL))
                break;
            StreamBatchRingProducer* state = &sharedContext->batchRingProducers[producer];
            if (ring->spare == NULL && state->spareCount < sharedContext->batchRingSpareLimit) {
                VectorBatch* spare = New(state->batchContext) VectorBatch(
                    state->batchContext, sharedContext->sharedBatches[nthChannel][producer]);
                ring->spareParity = written & 1;
                __atomic_store_n(&ring->spare, spare, __ATOMIC_RELEASE);
                ++state->spareCount;
                break;
            }
        } else
#endif
        {
            dataStatus = sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id];
            /* Break the loop if we find quota. */
            if ((dataStatus == DATA_EMPTY
#ifdef __aarch64__
                 && gs_is_databuff_empty(sharedContext, nthChannel)
#endif
                 ) || dataStatus == DATA_PREPARE)
                break;
        }

        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);
#ifdef ENABLE_HTAP
        uint64 waitStart = (profile != NULL || sendObservation != NULL) ? StreamFlowClock() : 0;
        StreamAggCacheDemand* demand = sharedContext->aggCacheDemand;
        if (demand != NULL)
            __atomic_store_n(&demand->producerWaiting[producer], 1, __ATOMIC_RELAXED);
#endif
        (void)entry->_timewait(SINGLE_WAITQUOTA);
#ifdef ENABLE_HTAP
        if (demand != NULL)
            __atomic_store_n(&demand->producerWaiting[producer], 0, __ATOMIC_RELAXED);
        if (sendObservation != NULL)
            StreamAggObserveWait(sharedContext, sendObservation, producer, false, waitStart, StreamFlowClock());
        if (profile != NULL) {
            uint64 waitEnd = StreamFlowClock();
            uint64 elapsed = waitEnd - waitStart;
            profile->quotaWaitNs += elapsed;
            ++profile->quotaWaits;
            /* Broadcast submits whole batches; log only its long individual waits. */
            if (nthRow < 0 && elapsed >= 100000000) {
                bool closed = __atomic_load_n(&sharedContext->is_connect_end[nthChannel][producer], __ATOMIC_ACQUIRE);
                ereport(LOG, (errmsg("stream_cache_profile record=quota_stall query_id=%lu stream_id=%u producer=%d "
                    "consumer=%d wait_ns=%lu start_ns=%lu end_ns=%lu closed=%d", u_sess->debug_query_id,
                    sharedContext->key_s.planNodeId, producer, nthChannel, elapsed, waitStart, waitEnd, (int)closed),
                    errhidestmt(true), ErrOutToClient(false)));
            }
        }
#endif
        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
    }

    StreamTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Copy data to shared context. */
    if (sharedContext->vectorized) {
        Assert(sharedContext->sharedBatches != NULL);
        batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
#ifdef ENABLE_HTAP
        if (ring != NULL) {
            slot = StreamRingSlot(ring, written);
            if (slot != 0)
                batch = ring->spare;
        }
        int dataAccessTargetBegin = batch->m_rows;
#endif
        /* data copy */
        if (-1 == nthRow) {
            /* Do deep copy of all rows, for local roundrobin & local broadcast. */
            Assert(batch->m_rows == 0);
            // batch->Copy<true, false>(batchsrc);
            batch->Copy<true, false>(batchsrc, 0, -1, false);
            ready_to_send = true;
        } else {
            // batch->CopyNth(batchsrc, nthRow);
            batch->CopyNth(batchsrc, nthRow, false);
            if (BatchMaxSize == batch->m_rows) {
                ready_to_send = true;
            }
        }
#ifdef ENABLE_HTAP
        if (unlikely(u_sess->attr.attr_sql.enable_inter_operator_data_access))
            OperatorDataAccessStreamCopy(batchsrc, batch, nthRow < 0 ? 0 : nthRow, dataAccessTargetBegin,
                nthRow < 0 ? batchsrc->m_rows : 1, nthRow >= 0, true,
                sharedContext->key_s.planNodeId, nthChannel, ready_to_send);
#endif
    } else {
        Assert(sharedContext->sharedTuples != NULL);
        tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        int n = tupleVec->tuplePointer;
        ExecCopySlot(tupleVec->tupleVector[n], tuple);
        tupleVec->tuplePointer++;
        if (TupleVectorMaxSize == tupleVec->tuplePointer) {
            ready_to_send = true;
        }
    }
    StreamTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
#ifdef ENABLE_HTAP
    if (delivery != NULL) {
        delivery->lastRowNs = StreamFlowClock();
        delivery->rows += nthRow < 0 ? batchsrc->m_rows : 1;
    }
#endif

    /* send the signal if copy finished */
    if (ready_to_send) {
#ifdef ENABLE_HTAP
        if (profile != NULL && nthRow >= 0) {
            profile->producerNuma[slot] = StreamFlowNode();
            profile->readyNs[slot] = StreamFlowClock();
        }
        if (ring != NULL) {
            ring->partial = false;
            __atomic_store_n(&ring->written, written + 1, __ATOMIC_RELEASE);
        } else
#endif
        {
#ifdef __aarch64__
            pg_memory_barrier();
#endif
            sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_READY;
        }
        /* send signal */
#ifdef ENABLE_HTAP
        if (delivery != NULL)
            delivery->lastPublishNs = StreamFlowClock();
#endif
        entry = sharedContext->poll_entrys[nthChannel];
        entry->_signal();
    } else {
#ifdef ENABLE_HTAP
        if (ring != NULL)
            ring->partial = true;
        else
#endif
        {
            sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_PREPARE;
        }
    }
    StreamTimeSendEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    (void)pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: catch a tuple from stream's buffer.
 *
 * @param[IN] node: stream state
 * @return bool: true -- found data
 */
FORCE_INLINE
bool gs_return_tuple(StreamState* node)
{
    TupleVector* tupleVec = node->tempTupleVec;

    if (tupleVec->tuplePointer == tupleVec->tupleCount) {
        return false;
    }

    int n = tupleVec->tuplePointer;
    node->ss.ps.ps_ResultTupleSlot = tupleVec->tupleVector[n];
    tupleVec->tuplePointer++;

    return true;
}

/*
 * @Description: Consume the data in shared memory from local producers.
 *
 * @param[IN] node: stream state
 * @param[IN] loc: data location
 * @return bool: true -- found data
 */
#ifdef ENABLE_HTAP
/* Opt-in receiver-copy samples. Placement queries and logging run AFTER the
 * timed Copy, while the receiver still owns the shared batch. These are
 * logical source/destination bytes, never physical fabric traffic. */
static uint64 StreamFlowClock()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * 1000000000ULL + (uint64)ts.tv_nsec;
}

static int StreamFlowNode()
{
    unsigned int node = 0;
    return syscall(SYS_getcpu, NULL, &node, NULL) == 0 ? (int)node : -1;
}

struct StreamCacheCopyProfile {
    uint64 startNs;
    uint64 finishNs;
    uint64 readyNs;
    uint64 logicalBytes;
    uint32 readyBatches;
    int producerNuma;
    int consumerNuma;
    bool perfValid;
    VecCacheTraceCounters before;
    VecCacheTraceCounters after;
};

static bool StreamCacheSample()
{
    static THR_LOCAL uint64 random = 0;
    if (random == 0)
        random = ((uint64)syscall(SYS_gettid) << 32) | 1;
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    return (random & 31) == 0;
}

static void StreamCacheLogCopy(StreamState* node, int loc, const StreamCacheCopyProfile& copy)
{
    const VecCacheTraceCounters& a = copy.before;
    const VecCacheTraceCounters& b = copy.after;
    bool valid = copy.perfValid && b.instructions > a.instructions && b.misses >= a.misses &&
        b.enabledNs > a.enabledNs && b.runningNs > a.runningNs;
    ereport(LOG, (errmsg("stream_cache_profile record=copy query_id=%lu stream_id=%u producer=%d consumer=%d "
        "producer_numa=%d consumer_numa=%d ready_ns=%lu start_ns=%lu copy_ns=%lu batch_bytes=%lu ready_batches=%u "
        "ready_bytes_est=%lu "
        "sample_period=32 pmu_valid=%d instructions=%lu misses=%lu running_ns=%lu enabled_ns=%lu",
        u_sess->debug_query_id, node->sharedContext->key_s.planNodeId, loc, (int)u_sess->stream_cxt.smp_id,
        copy.producerNuma, copy.consumerNuma, copy.readyNs, copy.startNs, copy.finishNs - copy.startNs,
        copy.logicalBytes, copy.readyBatches, copy.logicalBytes * copy.readyBatches,
        (int)valid, valid ? b.instructions - a.instructions : 0,
        valid ? b.misses - a.misses : 0, valid ? b.runningNs - a.runningNs : 0,
        valid ? b.enabledNs - a.enabledNs : 0), errhidestmt(true), ErrOutToClient(false)));
}

static void StreamCacheLogQuota(StreamSharedContext* context, int consumerCount)
{
    if (context->cacheProfileChannels == NULL)
        return;
    int producer = (int)u_sess->stream_cxt.smp_id;
    uint64 totalWaitNs = 0;
    uint64 totalWaits = 0;
    int waitedChannels = 0;
    int longestConsumer = -1;
    uint64 longestWaitNs = 0;
    for (int consumer = 0; consumer < consumerCount; ++consumer) {
        StreamCacheChannelProfile* profile = StreamCacheChannel(context, consumer, producer);
        totalWaitNs += profile->quotaWaitNs;
        totalWaits += profile->quotaWaits;
        waitedChannels += profile->quotaWaits != 0;
        if (profile->quotaWaitNs > longestWaitNs) {
            longestWaitNs = profile->quotaWaitNs;
            longestConsumer = consumer;
        }
        /* Emit heavy edges independently of the receive sampling. */
        if (profile->quotaWaitNs >= 1000000) {
            ereport(LOG, (errmsg("stream_cache_profile record=quota query_id=%lu stream_id=%u producer=%d "
                "consumer=%d wait_ns=%lu waits=%u", u_sess->debug_query_id, context->key_s.planNodeId,
                producer, consumer, profile->quotaWaitNs, profile->quotaWaits),
                errhidestmt(true), ErrOutToClient(false)));
        }
    }
    ereport(LOG, (errmsg("stream_cache_profile record=quota_total query_id=%lu stream_id=%u producer=%d "
        "wait_ns=%lu waits=%lu waited_channels=%d longest_consumer=%d longest_wait_ns=%lu extra_slots=%d",
        u_sess->debug_query_id, context->key_s.planNodeId, producer, totalWaitNs, totalWaits,
        waitedChannels, longestConsumer, longestWaitNs,
        context->batchRingProducers == NULL ? 0 : context->batchRingProducers[producer].spareCount),
        errhidestmt(true), ErrOutToClient(false)));
}

static bool StreamFlowSample()
{
    static THR_LOCAL int period = -1;
    static THR_LOCAL uint64 random = 0;
    if (period < 0) {
        const char* value = getenv("OG_STREAM_FLOW_PROFILE");
        /* A single fixed inclusion probability makes byte estimates explicit. */
        period = value != NULL && strcmp(value, "64") == 0 ? 64 : 0;
        random = ((uint64)syscall(SYS_gettid) << 32) ^ StreamFlowClock();
        if (random == 0)
            random = 1;
    }
    if (period == 0)
        return false;
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    return (random & 63) == 0;
}

class StreamFlowPages {
public:
    StreamFlowPages() : queries(0), failures(0), total(0), firstErrno(0), count(0)
    {
        pageSize = (Size)getpagesize();
        MemSet(bytesByNode, 0, sizeof(bytesByNode));
    }
    void Add(const void* address, Size bytes)
    {
        uintptr_t cursor = (uintptr_t)address;
        total += bytes;
        while (bytes != 0) {
            uintptr_t page = cursor - cursor % pageSize;
            Size length = Min(bytes, pageSize - (Size)(cursor - page));
            int slot = 0;
            while (slot < count && pages[slot] != (void*)page)
                ++slot;
            if (slot == count && count == 64) {
                Flush();
                slot = 0;
            }
            if (slot == count) {
                pages[count] = (void*)page;
                pageBytes[count++] = 0;
            }
            pageBytes[slot] += length;
            cursor += length;
            bytes -= length;
        }
    }
    void Batch(VectorBatch* batch)
    {
        for (int col = 0; col < batch->m_cols; ++col) {
            ScalarVector* vector = &batch->m_arr[col];
            int rows = batch->m_rows;
            Add(vector->m_flag, rows);
            if (!vector->m_desc.encoded) {
                /* Bulk scalar copying includes NULL value slots as well. */
                Add(vector->m_vals, (Size)rows * sizeof(ScalarValue));
                continue;
            }
            for (int row = 0; row < rows; ++row) {
                if (IS_NULL(vector->m_flag[row]))
                    continue;
                Add(vector->m_vals + row, sizeof(ScalarValue));
                Datum value = ScalarVector::Decode(vector->m_vals[row]);
                Size size = vector->m_desc.typeId == NAMEOID ? datumGetSize(value, false, -2) : VARSIZE_ANY(value);
                Add(DatumGetPointer(value), size);
            }
        }
        Flush();
    }
    void Flush()
    {
        if (count == 0)
            return;
        int savedErrno = errno;
        int nodes[64];
        MemSet(nodes, -1, sizeof(nodes));
        long result = move_pages(0, count, pages, NULL, nodes, 0);
        ++queries;
        if (result < 0) {
            ++failures;
            if (firstErrno == 0)
                firstErrno = errno;
        }
        for (int i = 0; i < count; ++i) {
            /* Slot8 retains unknown/outside-platform placement explicitly. */
            int node = result >= 0 && nodes[i] >= 0 && nodes[i] < 8 ? nodes[i] : 8;
            bytesByNode[node] += pageBytes[i];
        }
        count = 0;
        errno = savedErrno;
    }
    uint64 bytesByNode[9], queries, failures, total;
    int firstErrno;
private:
    Size pageSize;
    int count;
    void* pages[64];
    uint64 pageBytes[64];
};

static void StreamFlowRecord(StreamState* node, int producer, VectorBatch* source,
    VectorBatch* destination, uint64 begin, uint64 end, int executionBegin, int executionEnd)
{
    int savedErrno = errno;
    StreamFlowPages reads, writes;
    reads.Batch(source);
    writes.Batch(destination);
    char readNodes[256], writeNodes[256];
    int readLength = 0, writeLength = 0;
    for (int n = 0; n < 9; ++n) {
        int size = snprintf_s(readNodes + readLength, sizeof(readNodes) - readLength,
            sizeof(readNodes) - readLength - 1, "%s%lu", n == 0 ? "" : ",", reads.bytesByNode[n]);
        securec_check_ss(size, "", "");
        readLength += size;
        size = snprintf_s(writeNodes + writeLength, sizeof(writeNodes) - writeLength,
            sizeof(writeNodes) - writeLength - 1, "%s%lu", n == 0 ? "" : ",", writes.bytesByNode[n]);
        securec_check_ss(size, "", "");
        writeLength += size;
    }
    ereport(LOG, (errmsg("stream_flow_profile record=copy version=1 query_id=%lu tid=%ld "
        "stream_id=%d source_worker=%d target_worker=%d rows=%d cols=%d "
        "start_ns=%lu end_ns=%lu copy_ns=%lu execution_start=%d execution_end=%d "
        "sample_period=64 read_bytes=%lu write_bytes=%lu read_nodes=%s write_nodes=%s "
        "page_queries=%lu failed_queries=%lu first_query_errno=%d",
        u_sess->debug_query_id, syscall(SYS_gettid), node->sharedContext->key_s.planNodeId,
        producer, (int)u_sess->stream_cxt.smp_id, source->m_rows, source->m_cols,
        begin, end, end - begin, executionBegin, executionEnd, reads.total, writes.total,
        readNodes, writeNodes, reads.queries + writes.queries, reads.failures + writes.failures,
        reads.firstErrno != 0 ? reads.firstErrno : writes.firstErrno),
        errhidestmt(true), ErrOutToClient(false)));
    errno = savedErrno;
}
#endif

#ifdef ENABLE_HTAP
static bool TryClaimConsumerGroup(StreamSharedContext* context, int group, int* ordinal)
{
    int value = __atomic_load_n(&context->placementGroupClaims[group], __ATOMIC_RELAXED);
    while (value < context->placementGroupCapacity[group]) {
        if (__atomic_compare_exchange_n(&context->placementGroupClaims[group], &value, value + 1,
                false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
            *ordinal = value;
            return true;
        }
    }
    return false;
}

/* Sample a few chiplet-spanning input batches, then place this consumer once.
 * Capacity preserves the original number of workers per chiplet, so affinity
 * cannot create a new compute/LLC hotspot while it reduces remote buffer reads. */
static void MaybePlaceStreamConsumer(StreamState* node, int producer, VectorBatch* source)
{
    StreamSharedContext* context = node->sharedContext;
    if (!context->consumerPlacement)
        return;

    int consumer = (int)u_sess->stream_cxt.smp_id;
    if (context->placementDone[consumer])
        return;
    int groups = context->placementGroupCount;
    int group = context->placementProducerGroup[producer];
    uint64 bytes = (uint64)source->m_rows * (uint64)Max(source->m_cols, 1) *
        (sizeof(ScalarValue) + sizeof(uint8));
    context->placementBytes[(Size)consumer * groups + group] += bytes;
    ++context->placementSamples[consumer];
    context->placementSeenGroups[consumer] |= 1ULL << group;

    int requiredGroups = Min(groups, 4);
    if (context->placementSamples[consumer] < 8 ||
        __builtin_popcountll(context->placementSeenGroups[consumer]) < requiredGroups)
        return;

    int currentGroup = context->placementProducerGroup[consumer];
    uint64 rejected = 0;
    int selected = -1;
    int ordinal = -1;
    for (int attempt = 0; attempt < groups && selected < 0; ++attempt) {
        int candidate = -1;
        uint64 bestScore = 0;
        for (int g = 0; g < groups; ++g) {
            if ((rejected & (1ULL << g)) != 0 ||
                __atomic_load_n(&context->placementGroupClaims[g], __ATOMIC_RELAXED) >=
                    context->placementGroupCapacity[g])
                continue;
            uint64 score = context->placementBytes[(Size)consumer * groups + g];
            /* Stay within the current chiplet unless another chiplet has a
             * clearly heavier sampled flow. This avoids moves caused by ties. */
            if (g == currentGroup)
                score += score / 10 + 1;
            if (candidate < 0 || score > bestScore) {
                candidate = g;
                bestScore = score;
            }
        }
        if (candidate < 0)
            break;
        if (TryClaimConsumerGroup(context, candidate, &ordinal))
            selected = candidate;
        else
            rejected |= 1ULL << candidate;
    }

    context->placementDone[consumer] = true;
    context->scanStride[consumer] = 1;
    if (selected < 0)
        return;
    int targetNode = context->placementGroupNodes[selected];
    int oldNode = ImcsPartitionWiseWorkerNode(consumer, node->conn_count);
    if (!ImcsPartitionWisePlaceConsumer(targetNode, ordinal))
        return;

    /* The original receive batch was first-touched before the decision. A new
     * one makes both the copy destination and subsequent operator access local
     * to the selected consumer chiplet. The old batch dies with query context. */
    TupleDesc descriptor = node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    ((VecStreamState*)node)->m_CurrentBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, descriptor);
    ereport(DEBUG1, (errmsg("IMCS consumer placement stream=%u consumer=%d old_node=%d new_node=%d "
        "sample_bytes=%lu samples=%u local_ordinal=%d",
        context->key_s.planNodeId, consumer, oldNode, targetNode,
        context->placementBytes[(Size)consumer * groups + selected],
        context->placementSamples[consumer], ordinal)));
}

static bool RhjCopyWithAdmission(StreamRhjCopyStats& stats, VectorBatch* dst, VectorBatch* src, uint64* traceStage)
{
    if (src->m_rows < 512)
        return false;
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group == NULL || group->m_rhjStreamCopies == NULL)
        return false;
    int llc = RhjLlcDomain(sched_getcpu());
    int selected = u_sess->attr.attr_sql.imcs_rhj_stream_schedule_llc;
    if (llc < 0 || llc >= CPU_SETSIZE || (selected != -2 && selected != llc))
        return false;

    uint32* copies = &group->m_rhjStreamCopies[llc];
    const uint64* matches = &group->m_rhjProbeOverlap[llc].match;
    const uint32 limit = (uint32)u_sess->attr.attr_sql.imcs_rhj_stream_copy_slots;
    const uint64 waitBudgetNs = 4000;
    uint64 waitStart = 0;
    bool expired = false;
    uint32 active = __atomic_load_n(copies, __ATOMIC_RELAXED);
    for (;;) {
        if ((uint32)__atomic_load_n(matches, __ATOMIC_ACQUIRE) == 0) {
            active = __atomic_fetch_add(copies, 1, __ATOMIC_ACQ_REL) + 1;
            break;
        }
        if (active < limit) {
            if (__atomic_compare_exchange_n(copies, &active, active + 1, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
                ++active;
                break;
            }
        }
        uint64 now = StreamFlowClock();
        if (waitStart == 0) {
            waitStart = now;
        } else if (now - waitStart >= waitBudgetNs) {
            /* Stream progress must survive a stalled or preempted slot holder. */
            active = __atomic_fetch_add(copies, 1, __ATOMIC_ACQ_REL) + 1;
            expired = true;
            break;
        }
#if defined(__aarch64__)
        __asm__ __volatile__("yield" ::: "memory");
#else
        SPIN_DELAY();
#endif
        active = __atomic_load_n(copies, __ATOMIC_RELAXED);
    }
    if (traceStage != NULL) {
        if (waitStart != 0) {
            ++stats.waits;
            stats.waitNs += StreamFlowClock() - waitStart;
            stats.expired += expired;
            stats.admitted += !expired;
        }
        if (stats.copies == 0)
            stats.llc = llc;
        else if (stats.llc != llc)
            stats.llc = -1;
        ++stats.copies;
        stats.peak = Max(stats.peak, active);
    }

    PG_TRY();
    {
        if (traceStage != NULL) {
            VecCacheCoordRunPeerStage(traceStage, [&] { dst->Copy<true, false>(src, 0, -1, false); });
        } else {
            dst->Copy<true, false>(src, 0, -1, false);
        }
    }
    PG_CATCH();
    {
        __atomic_fetch_sub(copies, 1, __ATOMIC_RELEASE);
        PG_RE_THROW();
    }
    PG_END_TRY();
    __atomic_fetch_sub(copies, 1, __ATOMIC_RELEASE);
    return true;
}

const uint32 StreamRhjCopyCapacity = 4096;

static StreamRhjCopyTimeline* RhjReceiveTimeline(StreamState* node, int consumer)
{
    StreamRhjCopyTimeline* timeline = node->sharedContext->rhjCopyTimeline;
    if (timeline == NULL)
        return NULL;
    StreamRhjCopyTimeline& lane = timeline[consumer];
    if (!lane.initialized) {
        lane.initialized = true;
        lane.llc = RhjLlcDomain(sched_getcpu());
        const auto* config = RhjStreamTimeline();
        if (config != NULL && config->includes(lane.llc)) {
            lane.records = (StreamRhjCopyInterval*)MemoryContextAlloc(node->ss.ps.state->es_query_cxt,
                StreamRhjCopyCapacity * sizeof(StreamRhjCopyInterval));
        }
    }
    return lane.records == NULL ? NULL : &lane;
}

static void RhjLogCopyTimeline(StreamSharedContext* context, int consumer)
{
    if (context->rhjCopyTimeline == NULL || context->rhjCopyTimeline[consumer].records == NULL)
        return;
    StreamRhjCopyTimeline& lane = context->rhjCopyTimeline[consumer];
    ereport(LOG, (errmsg("rhj_timed_copy_status node=%u llc=%d worker=%d samples=%u dropped=%u",
        context->key_s.planNodeId, lane.llc, consumer, lane.size, lane.dropped),
        errhidestmt(true), ErrOutToClient(false)));
    StringInfoData data;
    initStringInfo(&data);
    for (uint32 start = 0; start < lane.size; start += 64) {
        resetStringInfo(&data);
        uint32 end = Min(start + 64, lane.size);
        for (uint32 i = start; i < end; ++i) {
            const StreamRhjCopyInterval& copy = lane.records[i];
            appendStringInfo(&data, "%s%lu,%lu,%d", i == start ? "" : ";", copy.startNs, copy.endNs, copy.rows);
        }
        ereport(LOG, (errmsg("rhj_timed_copy node=%u llc=%d worker=%d data=%s",
            context->key_s.planNodeId, lane.llc, consumer, data.data),
            errhidestmt(true), ErrOutToClient(false)));
    }
    pfree(data.data);
    lane.size = 0;
    lane.dropped = 0;
}
#endif

bool gs_consume_memory_data(StreamState* node, int loc, bool preferred)
{
    StreamSharedContext* sharedContext = node->sharedContext;
#ifdef ENABLE_HTAP
    StreamCacheCopyProfile cacheCopy = {};
    int consumerId = (int)u_sess->stream_cxt.smp_id;
    StreamBatchRing* ring = StreamRing(sharedContext, consumerId, loc);
    uint32 read = ring == NULL ? 0 : __atomic_load_n(&ring->read, __ATOMIC_RELAXED);
#endif

    NetWorkTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Take data from the shared context. */
    if (sharedContext->vectorized) {
        VectorBatch* batchsrc = sharedContext->sharedBatches[u_sess->stream_cxt.smp_id][loc];
#ifdef ENABLE_HTAP
        int slot = ring == NULL ? 0 : StreamRingSlot(ring, read);
        if (slot != 0)
            batchsrc = __atomic_load_n(&ring->spare, __ATOMIC_RELAXED);
#endif

        if (batchsrc->m_rows == 0) {
            return false;
        }

#ifdef ENABLE_HTAP
        MaybePlaceStreamConsumer(node, loc, batchsrc);
        bool flowSample = StreamFlowSample();
        int flowStartNode = flowSample ? StreamFlowNode() : -1;
        uint64 flowBegin = flowSample ? StreamFlowClock() : 0;
        StreamCacheChannelProfile* profile = StreamCacheChannel(sharedContext, consumerId, loc);
        if (profile != NULL && profile->readyNs[slot] != 0 && StreamCacheSample()) {
            cacheCopy.readyNs = profile->readyNs[slot];
            cacheCopy.producerNuma = profile->producerNuma[slot];
            cacheCopy.consumerNuma = StreamFlowNode();
            cacheCopy.logicalBytes = (uint64)batchsrc->m_rows * batchsrc->m_cols *
                (sizeof(ScalarValue) + sizeof(uint8));
            for (int i = 0; i < sharedContext->cacheProfileProducerCount; ++i) {
                StreamBatchRing* channel = StreamRing(sharedContext, consumerId, i);
                cacheCopy.readyBatches += channel == NULL ?
                    (sharedContext->dataStatus[consumerId][i] == DATA_READY) : StreamRingPending(channel);
            }
            cacheCopy.perfValid = VecCacheTraceSnapshot(&cacheCopy.before);
            cacheCopy.startNs = StreamFlowClock();
        }
#endif
#ifdef ENABLE_HTAP
        StreamAggObservation* receiveObservation = StreamAggObservationFor(sharedContext, consumerId, true);
        uint64 copyStart = receiveObservation != NULL ? StreamFlowClock() : 0;
#endif
        VectorBatch* batchdst = ((VecStreamState*)node)->m_CurrentBatch;
#ifdef ENABLE_HTAP
        StreamRhjCopyTimeline* timeline = RhjReceiveTimeline(node, consumerId);
        uint64 timelineStart = timeline == NULL ? 0 : StreamFlowClock();
        uint64* overlap = sharedContext->rhjCopyTimeline == NULL &&
            u_sess->attr.attr_sql.imcs_rhj_overlap_peer_kind == (int)RhjOverlapPeerKind::StreamCopy ?
            VecCacheCoordOverlapPeerStage(sharedContext->key_s.planNodeId, RhjOverlapPeerKind::StreamCopy) : NULL;
        bool copied = sharedContext->rhjCopyStats != NULL &&
            RhjCopyWithAdmission(sharedContext->rhjCopyStats[consumerId], batchdst, batchsrc, overlap);
        if (unlikely(!copied && overlap != NULL)) {
            VecCacheCoordRunPeerStage(overlap, [&] { batchdst->Copy<true, false>(batchsrc, 0, -1, false); });
        } else if (!copied)
#endif
            batchdst->Copy<true, false>(batchsrc, 0, -1, false);
#ifdef ENABLE_HTAP
        if (timeline != NULL) {
            uint64 end = StreamFlowClock();
            if (timeline->size < StreamRhjCopyCapacity) {
                timeline->records[timeline->size++] = {timelineStart, end, batchsrc->m_rows};
            } else {
                ++timeline->dropped;
            }
        }
        if (receiveObservation != NULL) {
            uint64 copyEnd = StreamFlowClock();
            const int* domains = sharedContext->aggCacheDemand->workerLlc;
            int cross = domains[loc] != domains[consumerId];
            StreamAggSelectWindow(sharedContext, receiveObservation, consumerId, true, copyStart);
            receiveObservation->rows[cross] += batchsrc->m_rows;
            receiveObservation->copyNs[cross] += copyEnd - copyStart;
        }
        if (cacheCopy.readyNs != 0) {
            cacheCopy.finishNs = StreamFlowClock();
            cacheCopy.perfValid = cacheCopy.perfValid && VecCacheTraceSnapshot(&cacheCopy.after);
        }
        if (profile != NULL)
            profile->readyNs[slot] = 0;
        if (flowSample) {
            uint64 flowEnd = StreamFlowClock();
            int flowEndNode = StreamFlowNode();
            StreamFlowRecord(node, loc, batchsrc, batchdst, flowBegin, flowEnd, flowStartNode, flowEndNode);
        }
        if (unlikely(u_sess->attr.attr_sql.enable_inter_operator_data_access))
            OperatorDataAccessStreamCopy(batchsrc, batchdst, 0, 0, batchsrc->m_rows, false,
                false, sharedContext->key_s.planNodeId, loc);
#endif
        batchsrc->Reset();
    } else {
        TupleVector* tuplesrc = sharedContext->sharedTuples[u_sess->stream_cxt.smp_id][loc];
        TupleVector* tupledst = node->tempTupleVec;

        if (tuplesrc->tuplePointer == 0) {
            return false;
        }

        for (int i = 0; i < tuplesrc->tuplePointer; i++) {
            (void)ExecCopySlot(tupledst->tupleVector[i], tuplesrc->tupleVector[i]);
        }

        tupledst->tupleCount = tuplesrc->tuplePointer;
        tupledst->tuplePointer = 0;
        tuplesrc->tuplePointer = 0;
        (void)gs_return_tuple(node);
    }
    NetWorkTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    struct hash_entry* entry = sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][loc];

#ifdef ENABLE_HTAP
    if (ring != NULL)
        __atomic_store_n(&ring->read, read + 1, __ATOMIC_RELEASE);
    else
#endif
    {
#ifdef __aarch64__
        pg_memory_barrier();
#endif
        sharedContext->dataStatus[u_sess->stream_cxt.smp_id][loc] = DATA_EMPTY;
    }
    entry->_signal();

    node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = loc;
#ifdef ENABLE_HTAP
    if (cacheCopy.readyNs != 0)
        StreamCacheLogCopy(node, loc, cacheCopy);
    if (sharedContext->cacheGroupCount > 0) {
        int consumer = (int)u_sess->stream_cxt.smp_id;
        bool local = sharedContext->cacheProducerGroup[consumer] == sharedContext->cacheProducerGroup[loc];
        uint8* burst = &sharedContext->cacheLocalBurst[consumer];
        int limit = u_sess->attr.attr_sql.imcs_cache_coord_sample_interval;
        *burst = local ? (uint8)Min((int)*burst + 1, limit) : 0;
        VecCacheCoordRecordStreamBatch(local, preferred);
    }
#endif
    return true;
}

#ifdef ENABLE_HTAP
static int RhjSmallReadyCopy(StreamState* node, int consumer, int producer)
{
    StreamSharedContext* context = node->sharedContext;
    int rows = context->sharedBatches[consumer][producer]->m_rows;
    if (rows < 512)
        return -1;
    StreamNodeGroup* group = u_sess->stream_cxt.global_obj;
    if (group == NULL || group->m_rhjProbeOverlap == NULL)
        return -1;
    int llc = RhjLlcDomain(sched_getcpu());
    if (llc < 0 || llc >= CPU_SETSIZE || llc != u_sess->attr.attr_sql.imcs_rhj_stream_schedule_llc ||
        (uint32)__atomic_load_n(&group->m_rhjProbeOverlap[llc].match, __ATOMIC_ACQUIRE) == 0)
        return -1;
    ++context->rhjCopyChecks[consumer];
    int nearby = Min(node->conn_count - 1, 8);
    for (int step = 1; step <= nearby; ++step) {
        int candidate = (producer + step) % node->conn_count;
        DataStatus status = __atomic_load_n(&context->dataStatus[consumer][candidate], __ATOMIC_ACQUIRE);
        if (status != DATA_READY && (status != DATA_PREPARE ||
            !__atomic_load_n(&context->is_connect_end[consumer][candidate], __ATOMIC_ACQUIRE)))
            continue;
        StringInfo message = context->messages[consumer][candidate];
        if (message->len != 0 || message->cursor == 'R')
            continue;
        int readyRows = context->sharedBatches[consumer][candidate]->m_rows;
        if (readyRows > 0 && readyRows <= rows / 2)
            return candidate;
    }
    return -1;
}
#endif

/*
 * @Description: Scan the producer status to find the data.
 *
 * @param[IN] node: stream state
 * @return char: STREAM_SCAN_DATA -- successfully find data from producer.
 *                  STREAM_SCAN_WAIT -- still need to poll to wait for data.
 *                 STREAM_SCAN_FINISH -- stream scan finished.
 */
char gs_find_memory_data(StreamState* node, int* waitnode_count)
{
    DataStatus dataStatus;
    StringInfo buf = NULL;
    int consumer = (int)u_sess->stream_cxt.smp_id;
    int scanLoc = node->sharedContext->scanLoc[consumer];
    int stride = 1;
    int preferredProducer = -1;
#ifdef ENABLE_HTAP
    int* deferred = node->sharedContext->rhjDeferredProducer;
    bool resumeDeferred = deferred != NULL && deferred[consumer] >= 0;
    if (resumeDeferred) {
        scanLoc = (deferred[consumer] + node->conn_count - 1) % node->conn_count;
        deferred[consumer] = -1;
    }
    if (node->sharedContext->cacheGroupCount > 0 &&
        node->sharedContext->cacheLocalBurst[consumer] < u_sess->attr.attr_sql.imcs_cache_coord_sample_interval &&
        VecCacheCoordHasProbeOverlap(u_sess->debug_query_id,
            u_sess->attr.attr_sql.enable_imcs_cache_coord_llc_pressure)) {
        StreamSharedContext* context = node->sharedContext;
        int group = context->cacheProducerGroup[consumer];
        int first = context->cacheGroupStart[group];
        int end = group + 1 < context->cacheGroupCount ? context->cacheGroupStart[group + 1] : node->conn_count;
        int producer = scanLoc >= first && scanLoc < end - 1 ? scanLoc + 1 : first;
        /* Start the regular receive loop at a ready local producer, if any.
         * Rotate within the LLC group and retain the original cursor otherwise. */
        for (int checked = first; checked < end; ++checked) {
            DataStatus status = context->dataStatus[consumer][producer];
            StreamBatchRing* ring = StreamRing(context, consumer, producer);
            if (status == DATA_READY ||
                (status == DATA_PREPARE &&
                    __atomic_load_n(&context->is_connect_end[consumer][producer], __ATOMIC_ACQUIRE)) ||
                (status != CONN_ERR && ring != NULL && StreamRingPending(ring) != 0)) {
                preferredProducer = producer;
                scanLoc = producer == 0 ? node->conn_count - 1 : producer - 1;
                break;
            }
            if (++producer == end)
                producer = first;
        }
    }
    bool placementSampling = node->sharedContext->consumerPlacement &&
        !node->sharedContext->placementDone[consumer];
    if (u_sess->attr.attr_sql.enable_imcs_nps &&
        (u_sess->attr.attr_sql.enable_imcs_stream_path_rebalance || placementSampling)) {
        stride = node->sharedContext->scanStride[consumer];
        if (stride <= 0) {
            stride = ImcsPartitionWisePathStride(node->conn_count);
            node->sharedContext->scanStride[consumer] = stride;
        }
        if (scanLoc < 0) {
            int first = consumer % node->conn_count;
            scanLoc = (first - stride + node->conn_count) % node->conn_count;
            node->sharedContext->scanLoc[consumer] = scanLoc;
        }
    }
#endif
    int i = scanLoc;
    bool finished = true;
    bool is_conn_end = false;
    int waitnodeCount = 0;
    struct hash_entry* entry = NULL;

    /* Check if there is available data, and scan from last time location. */
    do {
        i = (i + stride) % node->conn_count;

        /* Update scan location. */
        node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = i;
        dataStatus = node->sharedContext->dataStatus[u_sess->stream_cxt.smp_id][i];
        is_conn_end = __atomic_load_n(&node->sharedContext->is_connect_end[consumer][i], __ATOMIC_ACQUIRE);
#ifdef ENABLE_HTAP
        StreamBatchRing* ring = StreamRing(node->sharedContext, consumer, i);
        if (ring != NULL && dataStatus != CONN_ERR && StreamRingPending(ring) != 0)
            dataStatus = DATA_READY;
#endif

        if (!is_conn_end) {
            finished = false;
            waitnodeCount++;
        }

        /*
         * Firstly, we handle error or notice messages.
         * If an error occured, we should stop scan now.
         * If an notice occured, we can still receive data.
         */
        buf = node->sharedContext->messages[u_sess->stream_cxt.smp_id][i];

        if (buf->cursor == 'R') {
            node->ss.ps.state->es_processed += node->sharedContext->rows;
            resetStringInfo(buf);
        }

        if (buf->len > 0) {
            if (buf->cursor == 'E') {
                HandleStreamError(node, buf->data, buf->len);
                return STREAM_SCAN_FINISH;
            } else if (buf->cursor == 'N') {
                HandleStreamNotice(node, buf->data, buf->len);
                resetStringInfo(buf);
#ifdef __aarch64__
    pg_memory_barrier();
#endif
                /* After one notice message has handled, send signal and wake up the dest producer. */
                entry = node->sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][i];
                entry->_signal();

                return STREAM_SCAN_WAIT;
            }
        }

        switch (dataStatus) {
            case DATA_EMPTY:
                break;

            case DATA_PREPARE:
                /* Take the rest data away when the connection is end. */
                if (is_conn_end) {
                    /* Return data if any. */
                    if (gs_consume_memory_data(node, i, i == preferredProducer)) {
                        return STREAM_SCAN_DATA;
                    }
                }
                break;

            case DATA_READY:
#ifdef ENABLE_HTAP
                if (deferred != NULL && !resumeDeferred) {
                    int smaller = RhjSmallReadyCopy(node, consumer, i);
                    if (smaller >= 0 && gs_consume_memory_data(node, smaller, false)) {
                        // Revisit the larger batch first on the next receive.
                        deferred[consumer] = i;
                        ++node->sharedContext->rhjSmallCopies[consumer];
                        return STREAM_SCAN_DATA;
                    }
                }
#endif
                if (gs_consume_memory_data(node, i, i == preferredProducer)) {
                    return STREAM_SCAN_DATA;
                } else {
                    break;
                }

            case CONN_ERR:
                ereport(ERROR,
                    (errcode(ERRCODE_STREAM_REMOTE_CLOSE_SOCKET),
                        errmsg("Failed to read response from Local Stream Node,"
                               " Detail: Node %s, Plan Node ID %u, SMP ID %d",
                            g_instance.attr.attr_common.PGXCNodeName,
                            node->sharedContext->key_s.planNodeId,
                            i)));
                break;
            // dataStatus is enum,
            default:
                break;
        }
    } while (i != scanLoc);

    *waitnode_count = waitnodeCount;

    if (finished) {
        return STREAM_SCAN_FINISH;
    } else {
        return STREAM_SCAN_WAIT;
    }
}

/*
 * @Description: Receive data from shared memory for local stream.
 *
 * @param[IN] node: stream state
 * @return bool: true -- successed to find data and need more data.
 *                  false -- all connection finished or recerive error.
 */
bool gs_memory_recv(StreamState* node)
{
    char result;
    struct hash_entry* entry = NULL;
    entry = node->sharedContext->poll_entrys[u_sess->stream_cxt.smp_id];
    bool re = true;
    int waitnode_count = 0;
#ifdef ENABLE_HTAP
    int consumer = (int)u_sess->stream_cxt.smp_id;
    StreamAggObservation* receiveObservation = StreamAggObservationFor(node->sharedContext, consumer, true);
#endif

    /* If there is already tuple in buffer, return the data at once. */
    if (!node->sharedContext->vectorized && gs_return_tuple(node)) {
        return true;
    }

    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we can early stop now. */
        if (executorEarlyStop()) {
            re = false;
            break;
        }

        /* Search all producers to find data. */
        result = gs_find_memory_data(node, &waitnode_count);
        if (result == STREAM_SCAN_DATA) {
            re = true;
            break;
        } else if (result == STREAM_SCAN_FINISH) {
#ifdef ENABLE_HTAP
            RhjLogCopyTimeline(node->sharedContext, consumer);
            if (node->sharedContext->rhjSendTimeline != NULL) {
                ereport(LOG, (errmsg("rhj_stream_receive_end node=%u worker=%d end_ns=%lu",
                    node->sharedContext->key_s.planNodeId, consumer, StreamFlowClock()),
                    errhidestmt(true), ErrOutToClient(false)));
            }
            StreamAggFlush(node->sharedContext, receiveObservation, consumer, true);
            StreamRhjCopyStats* copyStats = node->sharedContext->rhjCopyStats;
            if (copyStats != NULL && copyStats[consumer].copies != 0) {
                StreamRhjCopyStats& stats = copyStats[consumer];
                ereport(LOG, (errmsg("rhj_stream_copy_admission stream=%u worker=%d llc=%d copies=%lu "
                    "waits=%lu admitted=%lu expired=%lu wait_ns=%lu peak=%u",
                    node->sharedContext->key_s.planNodeId, consumer, stats.llc, stats.copies,
                    stats.waits, stats.admitted, stats.expired, stats.waitNs, stats.peak),
                    errhidestmt(true), ErrOutToClient(false)));
                stats = {};
            }
            uint64* checks = node->sharedContext->rhjCopyChecks;
            if (checks != NULL && checks[consumer] != 0) {
                ereport(LOG, (errmsg("rhj_stream_schedule stream=%u worker=%d llc=%d checks=%lu choices=%lu",
                    node->sharedContext->key_s.planNodeId, consumer, RhjLlcDomain(sched_getcpu()), checks[consumer],
                    node->sharedContext->rhjSmallCopies[consumer]),
                    errhidestmt(true), ErrOutToClient(false)));
                checks[consumer] = 0;
                node->sharedContext->rhjSmallCopies[consumer] = 0;
            }
#endif
            re = false;
            break;
        }

        WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);
        WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_NODE,
            u_sess->pgxc_cxt.PGXCNodeId,
            waitnode_count,
            node->sharedContext->key_s.planNodeId,
            global_node_definition ? global_node_definition->num_nodes : -1);

        /* Poll to wait data from producers. */
        NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);
#ifdef ENABLE_HTAP
        StreamAggCacheDemand* demand = node->sharedContext->aggCacheDemand;
        uint64 waitStart = receiveObservation != NULL ? StreamFlowClock() : 0;
        if (demand != NULL)
            __atomic_store_n(&demand->receiverWaiting[u_sess->stream_cxt.smp_id], 1, __ATOMIC_RELAXED);
#endif
        (void)entry->_timewait(SINGLE_WAITQUOTA);
#ifdef ENABLE_HTAP
        if (demand != NULL)
            __atomic_store_n(&demand->receiverWaiting[u_sess->stream_cxt.smp_id], 0, __ATOMIC_RELAXED);
        if (receiveObservation != NULL)
            StreamAggObserveWait(node->sharedContext, receiveObservation, consumer, true, waitStart, StreamFlowClock());
#endif
        NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

        pgstat_reset_waitStatePhase(oldStatus, oldPhase);
    }

    return re;
}

/*
 * @Description: Inform all related consuemrs that there is no more data to send.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 */
void gs_memory_send_finish(StreamSharedContext* sharedContext, int connNum)
{
    struct hash_entry* entry = NULL;
#ifdef ENABLE_HTAP
    uint64 eos = sharedContext->rhjSendTimeline == NULL ? 0 : StreamFlowClock();
#endif

    for (int i = 0; i < connNum; i++) {
#ifdef ENABLE_HTAP
        StreamBatchRing* ring = StreamRing(sharedContext, i, u_sess->stream_cxt.smp_id);
        if (ring != NULL && ring->partial) {
            uint32 written = __atomic_load_n(&ring->written, __ATOMIC_RELAXED);
            ring->partial = false;
            __atomic_store_n(&ring->written, written + 1, __ATOMIC_RELEASE);
        }
#endif
#ifdef __aarch64__
    pg_memory_barrier();
#endif
        /* Set flags. */
        __atomic_store_n(&sharedContext->is_connect_end[i][u_sess->stream_cxt.smp_id], true, __ATOMIC_RELEASE);

        /* send signal */
        entry = sharedContext->poll_entrys[i];
        entry->_signal();
    }
#ifdef ENABLE_HTAP
    int producer = (int)u_sess->stream_cxt.smp_id;
    if (eos != 0) {
        const auto& delivery = sharedContext->rhjSendTimeline[producer];
        ereport(LOG, (errmsg("rhj_stream_send_end node=%u worker=%d llc=%d rows=%lu last_row_ns=%lu "
            "last_publish_ns=%lu eos_ns=%lu end_ns=%lu", sharedContext->key_s.planNodeId, producer,
            RhjLlcDomain(sched_getcpu()), delivery.rows, delivery.lastRowNs, delivery.lastPublishNs,
            eos, StreamFlowClock()), errhidestmt(true), ErrOutToClient(false)));
    }
    StreamAggFlush(sharedContext, StreamAggObservationFor(sharedContext, producer, false), producer, false);
    StreamCacheLogQuota(sharedContext, connNum);
#endif
}

/*
 * @Description: Set all connections with this producer to close.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 * @param[IN] smpId: producer smp id
 */
void gs_memory_close_conn(StreamSharedContext* sharedContext, int connNum, int consumerId)
{
    struct hash_entry* entry = NULL;

    for (int i = 0; i < connNum; i++) {
        __atomic_store_n(&sharedContext->is_connect_end[consumerId][i], true, __ATOMIC_RELEASE);

#ifdef __aarch64__
    pg_memory_barrier();
#endif
        /*
         * Send signal to the producers which may be still waiting quota,
         * in a query like "limit XXX", when consumer don't need data anymore,
         * but the producers haven't send all data yet.
         */
        entry = sharedContext->quota_entrys[consumerId][i];
        entry->_signal();
    }
}

