/*
 * Batch-window cache trace for vectorized execution.
 */
#include "postgres.h"

#include <sched.h>
#include <stdlib.h>
#include <time.h>
#ifdef __linux__
#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "knl/knl_variable.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "threadpool/threadpool_group.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.h"

extern char* nodeTagToString(NodeTag type);

#define CACHE_TRACE_ENV "OG_CACHE_TRACE_WINDOW"
#define CACHE_TRACE_MPKI_ENV "OG_CACHE_TRACE_WINDOW_MPKI"
#define CACHE_TRACE_OPERATOR_ENV "OG_CACHE_TRACE_WINDOW_OPERATOR"
#define CACHE_TRACE_GLOBAL_SLOTS_PER_NUMA_ENV "OG_CACHE_TRACE_GLOBAL_SLOTS_PER_NUMA"
#define CACHE_TRACE_INITIAL_SAMPLES 163840
#define CACHE_TRACE_MAX_SAMPLES 1048576
#define CACHE_TRACE_STACK_DEPTH 64
#define CACHE_TRACE_LOG_BUF_SIZE 1800
#define CACHE_TRACE_GLOBAL_MAX_NUMA 8
#define CACHE_TRACE_GLOBAL_DEFAULT_SLOTS_PER_NUMA 64
#define CACHE_TRACE_GLOBAL_SLOT_EMPTY 0
#define CACHE_TRACE_GLOBAL_SLOT_WRITING 1
#define CACHE_TRACE_GLOBAL_SLOT_READY 2

// ActiveOperatorSnapshot 是一个 batch 边界上的算子快照。
typedef struct ActiveOperatorSnapshot {
    int plan_node_id;
    int node_tag;
    int dop;
    int depth;
} ActiveOperatorSnapshot;

// MpkiSnapshot 是一个 batch 边界上的 MPKI 增量结果。
typedef struct MpkiSnapshot {
    uint64 instructions;
    uint64 cache_misses;
    double mpki;
    bool done;
} MpkiSnapshot;

// CacheTraceSample 是一条历史采样记录。
// 每条记录对应一个非空 VectorBatch，保存 batch 序号、行数、算子快照和 MPKI 结果。
typedef struct CacheTraceSample {
    uint64 batch_time;
    uint64 batch_id;
    uint64 rows;
    uint64 query_id;
    int cpu_id;
    ActiveOperatorSnapshot active_operator;
    MpkiSnapshot mpki;
} CacheTraceSample;

// ActiveOperatorRuntime 活跃算子实时状态。
// 它维护当前线程正在执行的 PlanState 栈，采样时再把栈顶算子拷贝成 ActiveOperatorSnapshot。
typedef struct ActiveOperatorRuntime {
    bool enabled;
    int stack_depth;
    PlanState* node_stack[CACHE_TRACE_STACK_DEPTH];
} ActiveOperatorRuntime;

// MpkiTraceRuntime 是 MPKI 跟踪的实时状态。
// 它维护 perf event fd 和上一次硬件计数，采样时用当前计数减去上一次计数得到窗口内 MPKI。
typedef struct MpkiTraceRuntime {
    bool requested;
    bool available;
    bool has_last;
    bool warned;
    int instruction_fd;
    int cache_miss_fd;
    uint64 last_instructions;
    uint64 last_cache_misses;
} MpkiTraceRuntime;

typedef struct MpkiPerfGroupRead {
    uint64 nr;
    uint64 values[2];
} MpkiPerfGroupRead;

// CacheTraceRuntime 是每个线程的总跟踪上下文。
//   ├── samples              历史采样结果数组，指向当前 worker 独占的全局 slot buffer
//   ├── active_operator      算子堆叠的实时状态，用于生成 ActiveOperatorSnapshot
//   └── mpki                 MPKI 的实时状态，用于生成 MpkiSnapshot
typedef struct CacheTraceRuntime {
    bool inited;
    bool enabled;
    uint64 current_query_id;
    uint64 start_time;
    uint64 end_time;
    uint64 batch_num;
    int cpu_id;
    size_t sample_num;
    size_t sample_capacity;
    size_t dropped_samples;
    CacheTraceSample* samples;
    ActiveOperatorRuntime active_operator;
    MpkiTraceRuntime mpki;
} CacheTraceRuntime;

// CacheTraceGlobalSlot 是发布到 g_instance 的 per-worker 结果槽。
// worker 在运行时独占写自己的 samples，结束时只把 state 标成 READY，不做日志格式化。
typedef struct CacheTraceGlobalSlot {
    pg_atomic_uint32 state;
    uint64 epoch;
    uint64 query_id;
    uint64 start_time;
    uint64 end_time;
    ThreadId owner_tid;
    int numa_id;
    int cpu_id;
    uint64 batch_count;
    uint32 sample_capacity;
    uint32 sample_count;
    uint32 dropped_samples;
    bool trace_operators;
    bool mpki_requested;
    bool mpki_available;
    CacheTraceSample* samples;
} CacheTraceGlobalSlot;

// CacheTraceGlobalNumaSlots 按 NUMA 分组管理 slot。
// next_slot 只在领取 slot 时递增，采样热路径不共享这个原子变量。
typedef struct CacheTraceGlobalNumaSlots {
    pg_atomic_uint32 next_slot;
    CacheTraceGlobalSlot* slots;
} CacheTraceGlobalNumaSlots;

// CacheTraceGlobalContext 是 g_instance.exec_cxt.cache_trace 指向的全局 trace 区。
// 它让向量化 worker 结束后，样本仍然保留在数据库进程级内存中，后续可以单独 dump。
typedef struct CacheTraceGlobalContext {
    int numa_count;
    int slots_per_numa;
    pg_atomic_uint64 global_epoch;
    CacheTraceGlobalNumaSlots numa_slots[CACHE_TRACE_GLOBAL_MAX_NUMA];
} CacheTraceGlobalContext;

static THR_LOCAL CacheTraceRuntime cache_trace = {0};
static THR_LOCAL CacheTraceGlobalSlot* cache_trace_global_slot = NULL;
static THR_LOCAL bool cache_trace_local_samples = false;
static pthread_mutex_t cache_trace_global_init_lock = PTHREAD_MUTEX_INITIALIZER;

static uint64 CacheTraceTime()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }

    return (uint64)ts.tv_sec * 1000000000ULL + (uint64)ts.tv_nsec;
}

static bool CacheTraceIsEnabledValue(const char* value)
{
    return value != NULL && (value[0] == '1' || value[0] == 't' || value[0] == 'T' || value[0] == 'y' ||
                                value[0] == 'Y' ||
                                ((value[0] == 'o' || value[0] == 'O') &&
                                    (value[1] == 'n' || value[1] == 'N')));
}

static bool CacheTraceIsDisabledValue(const char* value)
{
    return value != NULL && (value[0] == '0' || value[0] == 'f' || value[0] == 'F' || value[0] == 'n' ||
                                value[0] == 'N' ||
                                ((value[0] == 'o' || value[0] == 'O') &&
                                    (value[1] == 'f' || value[1] == 'F')));
}

static int CacheTraceGetSlotsPerNuma()
{
    uint64 slots_per_numa = CACHE_TRACE_GLOBAL_DEFAULT_SLOTS_PER_NUMA;
    char* slot_env = gs_getenv_r(CACHE_TRACE_GLOBAL_SLOTS_PER_NUMA_ENV);
    if (slot_env != NULL && slot_env[0] != '\0') {
        uint64 env_slots = (uint64)strtoull(slot_env, NULL, 10);
        if (env_slots > 0) {
            slots_per_numa = env_slots;
        }
    }

    if (slots_per_numa > 1024) {
        slots_per_numa = 1024;
    }

    return (int)slots_per_numa;
}

static int CacheTraceGetNumaCount()
{
    int numa_count = g_instance.shmem_cxt.numaNodeNum;
    if (numa_count <= 0) {
        numa_count = 1;
    }
    if (numa_count > CACHE_TRACE_GLOBAL_MAX_NUMA) {
        numa_count = CACHE_TRACE_GLOBAL_MAX_NUMA;
    }
    return numa_count;
}

static int CacheTraceGetCurrentNumaId(int cpu_id, int numa_count)
{
    ThreadPoolGroup* group = t_thrd.threadpool_cxt.group;
    if (group != NULL) {
        int numa_id = group->GetNumaId();
        if (numa_id >= 0 && numa_id < numa_count) {
            return numa_id;
        }
    }

#ifdef __linux__
    long cpu_count = sysconf(_SC_NPROCESSORS_CONF);
    if (cpu_id >= 0 && cpu_count > 0 && numa_count > 1) {
        long cpu_per_numa = cpu_count / numa_count;
        if (cpu_per_numa > 0) {
            int numa_id = (int)(cpu_id / cpu_per_numa);
            if (numa_id >= numa_count) {
                numa_id = numa_count - 1;
            }
            return numa_id;
        }
    }
#endif

    return 0;
}

static void CacheTraceFreeGlobalContext(CacheTraceGlobalContext* global)
{
    if (global == NULL) {
        return;
    }

    for (int i = 0; i < global->numa_count; i++) {
        if (global->numa_slots[i].slots != NULL) {
            for (int j = 0; j < global->slots_per_numa; j++) {
                free(global->numa_slots[i].slots[j].samples);
            }
            free(global->numa_slots[i].slots);
        }
    }
    free(global);
}

static CacheTraceGlobalContext* CacheTraceGlobalInit()
{
    CacheTraceGlobalContext* global = (CacheTraceGlobalContext*)g_instance.exec_cxt.cache_trace;
    if (global != NULL) {
        return global;
    }

    pthread_mutex_lock(&cache_trace_global_init_lock);
    global = (CacheTraceGlobalContext*)g_instance.exec_cxt.cache_trace;
    if (global != NULL) {
        pthread_mutex_unlock(&cache_trace_global_init_lock);
        return global;
    }

    global = (CacheTraceGlobalContext*)calloc(1, sizeof(CacheTraceGlobalContext));
    if (global == NULL) {
        pthread_mutex_unlock(&cache_trace_global_init_lock);
        return NULL;
    }

    global->numa_count = CacheTraceGetNumaCount();
    global->slots_per_numa = CacheTraceGetSlotsPerNuma();
    pg_atomic_init_u64(&global->global_epoch, 1);

    for (int i = 0; i < global->numa_count; i++) {
        pg_atomic_init_u32(&global->numa_slots[i].next_slot, 0);
        global->numa_slots[i].slots =
            (CacheTraceGlobalSlot*)calloc(global->slots_per_numa, sizeof(CacheTraceGlobalSlot));
        if (global->numa_slots[i].slots == NULL) {
            CacheTraceFreeGlobalContext(global);
            pthread_mutex_unlock(&cache_trace_global_init_lock);
            return NULL;
        }

        for (int j = 0; j < global->slots_per_numa; j++) {
            pg_atomic_init_u32(&global->numa_slots[i].slots[j].state, CACHE_TRACE_GLOBAL_SLOT_EMPTY);
            global->numa_slots[i].slots[j].numa_id = i;
        }
    }

    g_instance.exec_cxt.cache_trace = global;
    pthread_mutex_unlock(&cache_trace_global_init_lock);
    return global;
}

static CacheTraceGlobalSlot* CacheTraceClaimGlobalSlot(int cpu_id)
{
    CacheTraceGlobalContext* global = CacheTraceGlobalInit();
    if (global == NULL) {
        return NULL;
    }

    int numa_id = CacheTraceGetCurrentNumaId(cpu_id, global->numa_count);
    CacheTraceGlobalNumaSlots* numa_slots = &global->numa_slots[numa_id];

    for (int i = 0; i < global->slots_per_numa; i++) {
        uint32 slot_id = pg_atomic_fetch_add_u32(&numa_slots->next_slot, 1) % (uint32)global->slots_per_numa;
        CacheTraceGlobalSlot* slot = &numa_slots->slots[slot_id];

        uint32 expected = CACHE_TRACE_GLOBAL_SLOT_READY;
        if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CACHE_TRACE_GLOBAL_SLOT_WRITING)) {
            expected = CACHE_TRACE_GLOBAL_SLOT_EMPTY;
            if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CACHE_TRACE_GLOBAL_SLOT_WRITING)) {
                continue;
            }
        }

        if (slot->samples == NULL) {
            slot->samples = (CacheTraceSample*)malloc(sizeof(CacheTraceSample) * CACHE_TRACE_INITIAL_SAMPLES);
            if (slot->samples == NULL) {
                pg_atomic_write_u32(&slot->state, CACHE_TRACE_GLOBAL_SLOT_EMPTY);
                return NULL;
            }
            slot->sample_capacity = CACHE_TRACE_INITIAL_SAMPLES;
        }

        slot->epoch = pg_atomic_fetch_add_u64(&global->global_epoch, 1);
        slot->query_id = 0;
        slot->start_time = 0;
        slot->end_time = 0;
        slot->owner_tid = t_thrd.proc_cxt.MyProcPid;
        slot->numa_id = numa_id;
        slot->cpu_id = cpu_id;
        slot->batch_count = 0;
        slot->sample_count = 0;
        slot->dropped_samples = 0;
        slot->trace_operators = false;
        slot->mpki_requested = false;
        slot->mpki_available = false;
        return slot;
    }

    return NULL;
}

static bool CacheTraceEnsureSampleBuffer(int cpu_id)
{
    if (cache_trace.samples != NULL) {
        return true;
    }

    cache_trace_global_slot = CacheTraceClaimGlobalSlot(cpu_id);
    if (cache_trace_global_slot != NULL) {
        cache_trace.samples = cache_trace_global_slot->samples;
        cache_trace.sample_capacity = cache_trace_global_slot->sample_capacity;
        cache_trace_local_samples = false;
        return true;
    }

    cache_trace.samples = (CacheTraceSample*)malloc(sizeof(CacheTraceSample) * CACHE_TRACE_INITIAL_SAMPLES);
    cache_trace_local_samples = cache_trace.samples != NULL;
    cache_trace.sample_capacity = cache_trace.samples != NULL ? CACHE_TRACE_INITIAL_SAMPLES : 0;
    return cache_trace.samples != NULL;
}

static bool CacheTraceEnsureSampleCapacity()
{
    if (cache_trace.sample_num < cache_trace.sample_capacity) {
        return true;
    }

    if (cache_trace.sample_capacity >= CACHE_TRACE_MAX_SAMPLES) {
        return false;
    }

    size_t new_capacity = cache_trace.sample_capacity * 2;
    if (new_capacity < CACHE_TRACE_INITIAL_SAMPLES) {
        new_capacity = CACHE_TRACE_INITIAL_SAMPLES;
    }
    if (new_capacity > CACHE_TRACE_MAX_SAMPLES) {
        new_capacity = CACHE_TRACE_MAX_SAMPLES;
    }

    CacheTraceSample* samples =
        (CacheTraceSample*)realloc(cache_trace.samples, sizeof(CacheTraceSample) * new_capacity);
    if (samples == NULL) {
        return false;
    }

    cache_trace.samples = samples;
    cache_trace.sample_capacity = new_capacity;
    if (cache_trace_global_slot != NULL) {
        cache_trace_global_slot->samples = samples;
        cache_trace_global_slot->sample_capacity = (uint32)new_capacity;
    }

    return true;
}

static void CacheTraceReleaseSampleBuffer()
{
    if (cache_trace_local_samples) {
        free(cache_trace.samples);
    }

    cache_trace.samples = NULL;
    cache_trace_global_slot = NULL;
    cache_trace_local_samples = false;
    cache_trace.sample_capacity = 0;
}

static void MpkiTraceClose()
{
#ifdef __linux__
    if (cache_trace.mpki.instruction_fd >= 0) {
        (void)ioctl(cache_trace.mpki.instruction_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        close(cache_trace.mpki.instruction_fd);
    }
    if (cache_trace.mpki.cache_miss_fd >= 0) {
        close(cache_trace.mpki.cache_miss_fd);
    }
#endif

    cache_trace.mpki.available = false;
    cache_trace.mpki.has_last = false;
    cache_trace.mpki.instruction_fd = -1;
    cache_trace.mpki.cache_miss_fd = -1;
    cache_trace.mpki.last_instructions = 0;
    cache_trace.mpki.last_cache_misses = 0;
}

static void MpkiTraceReset()
{
    cache_trace.mpki.has_last = false;
    cache_trace.mpki.last_instructions = 0;
    cache_trace.mpki.last_cache_misses = 0;

#ifdef __linux__
    if (cache_trace.mpki.available) {
        (void)ioctl(cache_trace.mpki.instruction_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    }
#endif
}

static int CacheTraceOpenPerfCounter(uint64 config, int group_fd)
{
#ifdef __linux__
    struct perf_event_attr hw_event;
    errno_t ret = memset_s(&hw_event, sizeof(hw_event), 0, sizeof(hw_event));
    securec_check(ret, "\0", "\0");

    hw_event.type = PERF_TYPE_HARDWARE;
    hw_event.size = sizeof(struct perf_event_attr);
    hw_event.config = config;
    hw_event.disabled = 1;
    hw_event.exclude_kernel = 1;
    hw_event.exclude_hv = 1;
    hw_event.read_format = PERF_FORMAT_GROUP;

    return (int)syscall(__NR_perf_event_open, &hw_event, 0, -1, group_fd, 0);
#else
    return -1;
#endif
}

static bool MpkiTraceReadCounters(uint64* instructions, uint64* cache_misses)
{
#ifdef __linux__
    MpkiPerfGroupRead group_read;
    int ret = read(cache_trace.mpki.instruction_fd, &group_read, sizeof(group_read));
    if (ret != (int)sizeof(group_read) || group_read.nr < 2) {
        if (!cache_trace.mpki.warned) {
            cache_trace.mpki.warned = true;
            ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_MPKI disabled: failed to read perf events")));
        }
        MpkiTraceClose();
        return false;
    }

    *instructions = group_read.values[0];
    *cache_misses = group_read.values[1];
    return true;
#else
    return false;
#endif
}

static void MpkiTraceInit()
{
    cache_trace.mpki.requested = CacheTraceIsEnabledValue(gs_getenv_r(CACHE_TRACE_MPKI_ENV));
    cache_trace.mpki.available = false;
    cache_trace.mpki.has_last = false;
    cache_trace.mpki.warned = false;
    cache_trace.mpki.instruction_fd = -1;
    cache_trace.mpki.cache_miss_fd = -1;
    cache_trace.mpki.last_instructions = 0;
    cache_trace.mpki.last_cache_misses = 0;

    if (!cache_trace.mpki.requested) {
        return;
    }

#ifndef __linux__
    cache_trace.mpki.warned = true;
    ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_MPKI disabled: perf events are only supported on Linux")));
    return;
#else
    cache_trace.mpki.instruction_fd = CacheTraceOpenPerfCounter(PERF_COUNT_HW_INSTRUCTIONS, -1);
    if (cache_trace.mpki.instruction_fd < 0) {
        MpkiTraceClose();
        cache_trace.mpki.warned = true;
        ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_MPKI disabled: failed to open instruction perf event: %m")));
        return;
    }

    cache_trace.mpki.cache_miss_fd =
        CacheTraceOpenPerfCounter(PERF_COUNT_HW_CACHE_MISSES, cache_trace.mpki.instruction_fd);
    if (cache_trace.mpki.cache_miss_fd < 0) {
        MpkiTraceClose();
        cache_trace.mpki.warned = true;
        ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_MPKI disabled: failed to open cache miss perf event: %m")));
        return;
    }

#ifdef __linux__
    (void)ioctl(cache_trace.mpki.instruction_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    (void)ioctl(cache_trace.mpki.instruction_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
#endif

    cache_trace.mpki.available = true;
#endif
}

static void MpkiTraceReadSample(CacheTraceSample* sample)
{
    if (sample == NULL || sample->mpki.done) {
        return;
    }

    sample->mpki.done = true;

    if (!cache_trace.mpki.available) {
        return;
    }

    uint64 cur_instructions = 0;
    uint64 cur_cache_misses = 0;
    if (!MpkiTraceReadCounters(&cur_instructions, &cur_cache_misses)) {
        return;
    }

    if (cache_trace.mpki.has_last) {
        uint64 instruction_delta =
            cur_instructions >= cache_trace.mpki.last_instructions ?
            cur_instructions - cache_trace.mpki.last_instructions :
            0;
        uint64 cache_miss_delta =
            cur_cache_misses >= cache_trace.mpki.last_cache_misses ?
            cur_cache_misses - cache_trace.mpki.last_cache_misses :
            0;
        sample->mpki.instructions = instruction_delta;
        sample->mpki.cache_misses = cache_miss_delta;
        if (instruction_delta > 0) {
            sample->mpki.mpki = ((double)cache_miss_delta * 1000.0) / (double)instruction_delta;
        }
    }

    cache_trace.mpki.last_instructions = cur_instructions;
    cache_trace.mpki.last_cache_misses = cur_cache_misses;
    cache_trace.mpki.has_last = true;
}

static void MpkiTracePrime()
{
    if (!cache_trace.mpki.available || cache_trace.mpki.has_last) {
        return;
    }

    uint64 cur_instructions = 0;
    uint64 cur_cache_misses = 0;
    if (!MpkiTraceReadCounters(&cur_instructions, &cur_cache_misses)) {
        return;
    }

    cache_trace.mpki.last_instructions = cur_instructions;
    cache_trace.mpki.last_cache_misses = cur_cache_misses;
    cache_trace.mpki.has_last = true;
}

static uint64 CacheTraceGetQueryId(PlanState* node)
{
    if (node != NULL && node->state != NULL && node->state->es_plannedstmt != NULL) {
        cache_trace.current_query_id = node->state->es_plannedstmt->queryId;
    }

    return cache_trace.current_query_id;
}

static bool CacheTraceInit()
{
    char* trace_env = gs_getenv_r(CACHE_TRACE_ENV);
    if (!CacheTraceIsEnabledValue(trace_env)) {
        cache_trace.inited = true;
        cache_trace.enabled = false;
        return false;
    }

    cache_trace.samples = NULL;
    cache_trace_global_slot = NULL;
    cache_trace_local_samples = false;
    cache_trace.start_time = 0;
    cache_trace.end_time = 0;
    cache_trace.batch_num = 0;
    cache_trace.cpu_id = -1;
    cache_trace.sample_num = 0;
    cache_trace.sample_capacity = 0;
    cache_trace.dropped_samples = 0;
    cache_trace.current_query_id = 0;
    cache_trace.active_operator.enabled = !CacheTraceIsDisabledValue(gs_getenv_r(CACHE_TRACE_OPERATOR_ENV));
    cache_trace.active_operator.stack_depth = 0;
    cache_trace.inited = true;
    cache_trace.enabled = true;
    MpkiTraceInit();
    return true;
}

static inline bool CacheTraceEnabled()
{
    if (unlikely(!cache_trace.inited)) {
        return CacheTraceInit();
    }

    return cache_trace.enabled;
}

static ActiveOperatorSnapshot ActiveOperatorSnapshotFor(PlanState* node)
{
    ActiveOperatorSnapshot active_operator;
    active_operator.plan_node_id = -1;
    active_operator.node_tag = 0;
    active_operator.dop = 0;
    active_operator.depth = 0;

    if (!cache_trace.active_operator.enabled || node == NULL || node->plan == NULL) {
        return active_operator;
    }

    active_operator.plan_node_id = node->plan->plan_node_id;
    active_operator.node_tag = (int)nodeTag(node);
    active_operator.dop = node->plan->dop;
    active_operator.depth = cache_trace.active_operator.stack_depth;
    return active_operator;
}

static void CacheTraceStartIfNeeded(PlanState* node)
{
    if (cache_trace.start_time != 0) {
        return;
    }

    cache_trace.start_time = CacheTraceTime();
    cache_trace.end_time = 0;
    cache_trace.cpu_id = -1;
#ifdef __linux__
    cache_trace.cpu_id = sched_getcpu();
#endif
    (void)CacheTraceGetQueryId(node);
    MpkiTracePrime();
}

static void CacheTraceAppendBatchSample(PlanState* node, VectorBatch* result)
{
    if (node == NULL || node->plan == NULL || BatchIsNull(result)) {
        return;
    }

    CacheTraceStartIfNeeded(node);
    cache_trace.batch_num++;

    int cpu_id = cache_trace.cpu_id;
#ifdef __linux__
    cpu_id = sched_getcpu();
#endif
    if (!CacheTraceEnsureSampleBuffer(cpu_id) || !CacheTraceEnsureSampleCapacity()) {
        cache_trace.dropped_samples++;
        return;
    }

    CacheTraceSample* sample = &cache_trace.samples[cache_trace.sample_num++];
    sample->batch_time = CacheTraceTime();
    sample->batch_id = cache_trace.batch_num;
    sample->rows = (uint64)result->m_rows;
    sample->query_id = CacheTraceGetQueryId(node);
    sample->cpu_id = cpu_id;
    sample->active_operator = ActiveOperatorSnapshotFor(node);
    sample->mpki.instructions = 0;
    sample->mpki.cache_misses = 0;
    sample->mpki.mpki = -1.0;
    sample->mpki.done = false;
    MpkiTraceReadSample(sample);
}

void VecCacheTraceEnter(PlanState* node)
{
    if (!CacheTraceEnabled()) {
        return;
    }

    if (node == NULL || node->plan == NULL) {
        return;
    }

    CacheTraceStartIfNeeded(node);

    if (!cache_trace.active_operator.enabled) {
        return;
    }

    if (cache_trace.active_operator.stack_depth >= CACHE_TRACE_STACK_DEPTH) {
        cache_trace.dropped_samples++;
        return;
    }

    cache_trace.active_operator.node_stack[cache_trace.active_operator.stack_depth++] = node;
}

void VecCacheTraceLeave(PlanState* node, VectorBatch* result)
{
    if (!cache_trace.enabled) {
        return;
    }

    if (node == NULL || node->plan == NULL) {
        return;
    }

    CacheTraceAppendBatchSample(node, result);

    if (!cache_trace.active_operator.enabled) {
        return;
    }

    if (cache_trace.active_operator.stack_depth > 0 &&
        cache_trace.active_operator.node_stack[cache_trace.active_operator.stack_depth - 1] == node) {
        cache_trace.active_operator.stack_depth--;
    } else {
        cache_trace.active_operator.stack_depth = 0;
    }
}

static void CacheTracePublishGlobalSlot()
{
    if (cache_trace_global_slot == NULL) {
        return;
    }

    cache_trace_global_slot->query_id = cache_trace.current_query_id;
    cache_trace_global_slot->start_time = cache_trace.start_time;
    cache_trace_global_slot->end_time = cache_trace.end_time != 0 ? cache_trace.end_time : CacheTraceTime();
    cache_trace_global_slot->batch_count = cache_trace.batch_num;
    cache_trace_global_slot->sample_count = (uint32)cache_trace.sample_num;
    cache_trace_global_slot->dropped_samples = (uint32)cache_trace.dropped_samples;
    cache_trace_global_slot->trace_operators = cache_trace.active_operator.enabled;
    cache_trace_global_slot->mpki_requested = cache_trace.mpki.requested;
    cache_trace_global_slot->mpki_available = cache_trace.mpki.available;
    pg_memory_barrier();

    if (cache_trace.sample_num > 0 || cache_trace.dropped_samples > 0) {
        pg_atomic_write_u32(&cache_trace_global_slot->state, CACHE_TRACE_GLOBAL_SLOT_READY);
    } else {
        pg_atomic_write_u32(&cache_trace_global_slot->state, CACHE_TRACE_GLOBAL_SLOT_EMPTY);
    }
}

static void CacheTraceResetAfterQuery()
{
    cache_trace.start_time = 0;
    cache_trace.end_time = 0;
    cache_trace.batch_num = 0;
    cache_trace.cpu_id = -1;
    cache_trace.sample_num = 0;
    cache_trace.dropped_samples = 0;
    cache_trace.current_query_id = 0;
    cache_trace.active_operator.stack_depth = 0;
    CacheTraceReleaseSampleBuffer();
    MpkiTraceReset();
}

static void CacheTraceLogGlobalSlot(CacheTraceGlobalSlot* slot)
{
    if (slot == NULL || slot->samples == NULL) {
        return;
    }

    ereport(LOG,
        (errmsg("CACHE_TRACE_WINDOW_BEGIN tid=%lu query=" UINT64_FORMAT " samples=%u dropped=%u batches="
                UINT64_FORMAT " start_ns=" UINT64_FORMAT " end_ns=" UINT64_FORMAT
                " numa=%d cpu=%d epoch=" UINT64_FORMAT
                " trace_operators=%d mpki_requested=%d mpki_available=%d format=%s",
            slot->owner_tid,
            slot->query_id,
            slot->sample_count,
            slot->dropped_samples,
            slot->batch_count,
            slot->start_time,
            slot->end_time,
            slot->numa_id,
            slot->cpu_id,
            slot->epoch,
            slot->trace_operators ? 1 : 0,
            slot->mpki_requested ? 1 : 0,
            slot->mpki_available ? 1 : 0,
            slot->trace_operators ?
                (slot->mpki_requested ?
                    "batch_time_ns,batch_id,query_id,cpu_id,rows,plan_node_id,node_tag,dop,depth,"
                    "instructions,cache_misses,mpki" :
                    "batch_time_ns,batch_id,query_id,cpu_id,rows,plan_node_id,node_tag,dop,depth") :
                "batch_time_ns,batch_id,query_id,cpu_id,rows,instructions,cache_misses,mpki")));

    char log_buf[CACHE_TRACE_LOG_BUF_SIZE];
    size_t used = 0;
    log_buf[0] = '\0';

    for (uint32 i = 0; i < slot->sample_count; i++) {
        CacheTraceSample* sample = &slot->samples[i];
        char row_buf[260];
        char* node_tag_name = sample->active_operator.plan_node_id >= 0 ?
            nodeTagToString((NodeTag)sample->active_operator.node_tag) :
            (char*)"NULL";
        int rc;
        if (!slot->trace_operators) {
            rc = snprintf_s(row_buf,
                sizeof(row_buf),
                sizeof(row_buf) - 1,
                UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT ",%d," UINT64_FORMAT "," UINT64_FORMAT ","
                UINT64_FORMAT ",%.6f;",
                sample->batch_time,
                sample->batch_id,
                sample->query_id,
                sample->cpu_id,
                sample->rows,
                sample->mpki.instructions,
                sample->mpki.cache_misses,
                sample->mpki.mpki);
        } else if (slot->mpki_requested) {
            rc = snprintf_s(row_buf,
                sizeof(row_buf),
                sizeof(row_buf) - 1,
                UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT ",%d," UINT64_FORMAT ",%d,%s,%d,%d,"
                UINT64_FORMAT "," UINT64_FORMAT ",%.6f;",
                sample->batch_time,
                sample->batch_id,
                sample->query_id,
                sample->cpu_id,
                sample->rows,
                sample->active_operator.plan_node_id,
                node_tag_name,
                sample->active_operator.dop,
                sample->active_operator.depth,
                sample->mpki.instructions,
                sample->mpki.cache_misses,
                sample->mpki.mpki);
        } else {
            rc = snprintf_s(row_buf,
                sizeof(row_buf),
                sizeof(row_buf) - 1,
                UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT ",%d," UINT64_FORMAT ",%d,%s,%d,%d;",
                sample->batch_time,
                sample->batch_id,
                sample->query_id,
                sample->cpu_id,
                sample->rows,
                sample->active_operator.plan_node_id,
                node_tag_name,
                sample->active_operator.dop,
                sample->active_operator.depth);
        }
        securec_check_ss(rc, "\0", "\0");
        if (rc <= 0) {
            continue;
        }

        if (used > 0 && used + (size_t)rc >= sizeof(log_buf)) {
            ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_DATA tid=%lu %s", slot->owner_tid, log_buf)));
            used = 0;
            log_buf[0] = '\0';
        }

        errno_t ret = memcpy_s(log_buf + used, sizeof(log_buf) - used, row_buf, rc);
        securec_check(ret, "\0", "\0");
        used += (size_t)rc;
        log_buf[used] = '\0';
    }

    if (used > 0) {
        ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_DATA tid=%lu %s", slot->owner_tid, log_buf)));
    }

    ereport(LOG, (errmsg("CACHE_TRACE_WINDOW_END tid=%lu", slot->owner_tid)));
}

void VecCacheTraceFlush()
{
    if (!cache_trace.inited || !cache_trace.enabled) {
        return;
    }

    cache_trace.end_time = CacheTraceTime();
    CacheTracePublishGlobalSlot();
    CacheTraceResetAfterQuery();
}

void VecCacheTraceDiscard()
{
    if (!cache_trace.inited || !cache_trace.enabled) {
        return;
    }

    if (cache_trace_global_slot != NULL) {
        pg_atomic_write_u32(&cache_trace_global_slot->state, CACHE_TRACE_GLOBAL_SLOT_EMPTY);
    }

    CacheTraceResetAfterQuery();
}

void VecCacheTraceDumpGlobal()
{
    CacheTraceGlobalContext* global = (CacheTraceGlobalContext*)g_instance.exec_cxt.cache_trace;
    if (global == NULL) {
        return;
    }

    for (int numa_id = 0; numa_id < global->numa_count; numa_id++) {
        CacheTraceGlobalNumaSlots* numa_slots = &global->numa_slots[numa_id];
        if (numa_slots->slots == NULL) {
            continue;
        }

        for (int slot_id = 0; slot_id < global->slots_per_numa; slot_id++) {
            CacheTraceGlobalSlot* slot = &numa_slots->slots[slot_id];
            uint32 expected = CACHE_TRACE_GLOBAL_SLOT_READY;
            if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CACHE_TRACE_GLOBAL_SLOT_WRITING)) {
                continue;
            }

            CacheTraceLogGlobalSlot(slot);
            slot->sample_count = 0;
            slot->dropped_samples = 0;
            pg_atomic_write_u32(&slot->state, CACHE_TRACE_GLOBAL_SLOT_EMPTY);
        }
    }
}
