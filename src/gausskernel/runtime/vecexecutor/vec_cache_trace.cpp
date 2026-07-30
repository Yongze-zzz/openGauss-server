/*
 * Synchronized per-thread active-operator and MPKI sampling for vectorized execution.
 */
#include "postgres.h"

#include <fcntl.h>
#include <sched.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#ifdef __USE_NUMA
#include <numa.h>
#endif
#ifdef __linux__
#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "gs_thread.h"
#include "knl/knl_variable.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "threadpool/threadpool_group.h"
#include "utils/atomic.h"
#include "vecexecutor/vecexecutor.h"

static const char* const CACHE_TRACE_ENV = "OG_CACHE_TRACE";
static const char* const CACHE_TRACE_INTERVAL_ENV = "OG_CACHE_TRACE_INTERVAL_US";

/* Fixed implementation limits. Only the two environment variables above are user-facing. */
enum CacheTraceLimit {
    CACHE_TRACE_DEFAULT_INTERVAL_US = 10000,
    CACHE_TRACE_MIN_INTERVAL_US = 2000,
    CACHE_TRACE_MAX_INTERVAL_US = 1000000,
    CACHE_TRACE_PROBE_INTERVAL_US = 2000,
    CACHE_TRACE_FAST_TOKEN_CAPACITY = 8,
    CACHE_TRACE_MAX_THREADS = 2048,
    CACHE_TRACE_ACTIVE_BITMAP_WORDS = (CACHE_TRACE_MAX_THREADS + 31) / 32,
    CACHE_TRACE_MAX_NUMA_NODES = 16,
    CACHE_TRACE_NUMA_SAMPLER_COUNT = CACHE_TRACE_MAX_NUMA_NODES + 1,
    CACHE_TRACE_SAMPLE_CAPACITY = 16384,
    CACHE_TRACE_STACK_DEPTH = 64,
    CACHE_TRACE_SLOT_ALIGNMENT = 128
};

enum CacheTraceSlotState {
    CACHE_TRACE_SLOT_FREE = 0,
    CACHE_TRACE_SLOT_RESERVED,
    CACHE_TRACE_SLOT_ACTIVE,
    CACHE_TRACE_SLOT_SAMPLING,
    CACHE_TRACE_SLOT_CLOSING
};

enum CacheTraceSamplerState {
    CACHE_TRACE_SAMPLER_NOT_STARTED = 0,
    CACHE_TRACE_SAMPLER_RUNNING,
    CACHE_TRACE_SAMPLER_FAILED
};

enum CacheTraceFormat {
    CACHE_TRACE_BINARY_MAGIC = 0x43545243U,
    CACHE_TRACE_BINARY_VERSION = 4,
    CACHE_TRACE_BINARY_FLAG_INCOMPLETE = 0x01U,
    CACHE_TRACE_BINARY_FLAG_MPKI_AVAILABLE = 0x02U,
    CACHE_TRACE_BINARY_FLAG_CPU_TIME_AVAILABLE = 0x04U,
    CACHE_TRACE_SAMPLE_FLAG_OPERATOR_VALID = 0x01U,
    CACHE_TRACE_SAMPLE_FLAG_PERF_VALID = 0x02U,
    CACHE_TRACE_SAMPLE_FLAG_ADAPTIVE = 0x04U
};

enum CacheTraceOperatorStateLayout {
    CACHE_TRACE_GENERATION_BITS = 24,
    CACHE_TRACE_PLAN_ID_BITS = 20,
    CACHE_TRACE_NODE_TAG_BITS = 12,
    CACHE_TRACE_DEPTH_BITS = 8
};

static const uint64 CACHE_TRACE_GENERATION_MASK = (1ULL << CACHE_TRACE_GENERATION_BITS) - 1;
static const uint64 CACHE_TRACE_PLAN_ID_MASK = (1ULL << CACHE_TRACE_PLAN_ID_BITS) - 1;
static const uint64 CACHE_TRACE_NODE_TAG_MASK = (1ULL << CACHE_TRACE_NODE_TAG_BITS) - 1;
static const uint64 CACHE_TRACE_DEPTH_MASK = (1ULL << CACHE_TRACE_DEPTH_BITS) - 1;
static const uint64 CACHE_TRACE_INVALID_PLAN_ID = CACHE_TRACE_PLAN_ID_MASK;

typedef struct CacheTracePerfGroupRead {
    uint64 nr;
    /* Returned by the existing group read; no additional perf fd or syscall. */
    uint64 time_running;
    uint64 values[2];
} CacheTracePerfGroupRead;

typedef struct CacheTracePerf {
    bool available;
    bool running;
    int instruction_fd;
    int cache_miss_fd;
} CacheTracePerf;

/* One synchronized observation point. Counter values are cumulative. */
typedef struct CacheTraceSample {
    uint64 time_ns;
    uint64 operator_generation;
    uint64 instructions;
    uint64 cache_misses;
    int32 plan_node_id;
    int32 node_tag;
    int32 dop;
    int32 depth;
    uint32 flags;
    /* Low 32 bits suffice because adjacent samples are milliseconds apart. */
    uint32 cpu_running_us;
} CacheTraceSample;

typedef struct CacheTraceBinaryHeader {
    uint32 magic;
    uint16 version;
    uint16 header_size;
    uint32 sample_size;
    uint32 flags;
    uint32 interval_us;
    uint32 stack_errors;
    uint32 output_errors;
    uint32 probe_interval_us;
    int32 numa_id;
    int32 group_id;
    uint64 query_id;
    uint64 thread_id;
    uint64 thread_start_ns;
    uint64 thread_end_ns;
    uint64 operator_changes;
    uint64 sample_count;
    uint64 dropped_samples;
    uint64 perf_read_errors;
} CacheTraceBinaryHeader;

static_assert(sizeof(CacheTraceSample) == 56, "unexpected cache trace sample layout");
static_assert(sizeof(CacheTraceBinaryHeader) == 104, "unexpected cache trace header layout");

/*
 * A worker owns the operator state and perf fds. The sampler is the only
 * writer of samples. The state machine excludes sampling while a worker is
 * flushing or releasing the slot.
 */
typedef struct __attribute__((aligned(CACHE_TRACE_SLOT_ALIGNMENT))) CacheTraceSlot {
    pg_atomic_uint32 state;
    uint32 reserved_state;
    pg_atomic_uint64 operator_state;
    volatile int32 current_dop;
    int32 instruction_fd;
    int32 cache_miss_fd;
    bool perf_available;
    bool reserved_flags[3];
    uint64 query_id;
    ThreadId thread_id;
    uint64 thread_start_ns;
    CacheTraceSample* samples;
    uint32 sample_count;
    uint32 dropped_samples;
    uint32 perf_read_errors;
    uint32 reserved_count;
} CacheTraceSlot;

static_assert(sizeof(CacheTraceSlot) % CACHE_TRACE_SLOT_ALIGNMENT == 0, "cache trace slot is not cache-line aligned");

/* Sampler-owned scheduling state. Workers never write this structure. */
typedef struct CacheTraceAdaptiveState {
    uint64 activation_id;
    uint64 last_operator_state;
    uint64 next_regular_tick;
    uint64 next_adaptive_tick;
    uint64 refill_epoch;
    uint32 stable_probes;
    uint32 fast_tokens;
    bool initialized;
} CacheTraceAdaptiveState;

typedef struct CacheTraceGlobal CacheTraceGlobal;

/* One lazily-created sampler for each NUMA node that runs traced workers. */
typedef struct __attribute__((aligned(CACHE_TRACE_SLOT_ALIGNMENT))) CacheTraceNumaSampler {
    CacheTraceGlobal* global;
    int32 numa_id;
    volatile int32 bound_numa_id;
    volatile uint64 os_thread_id;
    volatile uint32 active_bitmap[CACHE_TRACE_ACTIVE_BITMAP_WORDS];
    pg_atomic_uint32 state;
    gs_thread_t thread;
} CacheTraceNumaSampler;

struct CacheTraceGlobal {
    CacheTraceSlot* slots;
    uint32 interval_us;
    uint32 regular_interval_ticks;
    CacheTraceAdaptiveState adaptive[CACHE_TRACE_MAX_THREADS];
    CacheTraceNumaSampler samplers[CACHE_TRACE_NUMA_SAMPLER_COUNT];
    pg_atomic_uint32 sampler_running;
};

typedef struct CacheTraceFrame {
    PlanState* node;
    int32 plan_node_id;
    int32 node_tag;
    int32 dop;
    int32 depth;
} CacheTraceFrame;

/* All mutable query state below is owned by the current worker. */
typedef struct CacheTraceRuntime {
    bool inited;
    bool enabled;
    bool query_active;
    bool trace_incomplete;
    uint64 query_id;
    ThreadId thread_id;
    uint64 thread_start_ns;
    uint64 thread_end_ns;
    uint64 operator_generation;
    uint64 operator_changes;
    int32 numa_id;
    int32 group_id;
    uint32 interval_us;
    uint32 stack_errors;
    uint32 output_errors;
    int stack_depth;
    int suppressed_depth;
    CacheTraceFrame stack[CACHE_TRACE_STACK_DEPTH];
    CacheTraceSlot* slot;
    CacheTracePerf perf;
} CacheTraceRuntime;

static THR_LOCAL CacheTraceRuntime cache_trace = {0};
static CacheTraceGlobal* cache_trace_global = NULL;
static pthread_mutex_t cache_trace_global_lock = PTHREAD_MUTEX_INITIALIZER;
static pg_atomic_uint32 cache_trace_perf_warning = 0;

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

static uint32 CacheTraceParseInterval(const char* value)
{
    if (value == NULL || value[0] == '\0') {
        return CACHE_TRACE_DEFAULT_INTERVAL_US;
    }

    char* end = NULL;
    errno = 0;
    unsigned long parsed = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < CACHE_TRACE_MIN_INTERVAL_US ||
        parsed > CACHE_TRACE_MAX_INTERVAL_US) {
        return CACHE_TRACE_DEFAULT_INTERVAL_US;
    }
    return (uint32)parsed;
}

static uint64 CacheTraceGetQueryId(PlanState* node)
{
    if (node != NULL && node->state != NULL && node->state->es_plannedstmt != NULL) {
        return node->state->es_plannedstmt->queryId;
    }
    return 0;
}

static int CacheTraceOpenPerfCounter(uint64 config, int group_fd)
{
#ifdef __linux__
    struct perf_event_attr event;
    errno_t rc = memset_s(&event, sizeof(event), 0, sizeof(event));
    securec_check(rc, "\0", "\0");

    event.type = PERF_TYPE_HARDWARE;
    event.size = sizeof(struct perf_event_attr);
    event.config = config;
    event.disabled = 1;
    event.exclude_kernel = 1;
    event.exclude_hv = 1;
    event.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_TOTAL_TIME_RUNNING;
    return (int)syscall(__NR_perf_event_open, &event, 0, -1, group_fd, 0);
#else
    return -1;
#endif
}

static void CacheTracePerfClose()
{
#ifdef __linux__
    if (cache_trace.perf.instruction_fd >= 0) {
        (void)ioctl(cache_trace.perf.instruction_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    }
    if (cache_trace.perf.cache_miss_fd >= 0) {
        close(cache_trace.perf.cache_miss_fd);
    }
    if (cache_trace.perf.instruction_fd >= 0) {
        close(cache_trace.perf.instruction_fd);
    }
#endif
    cache_trace.perf.available = false;
    cache_trace.perf.running = false;
    cache_trace.perf.instruction_fd = -1;
    cache_trace.perf.cache_miss_fd = -1;
}

static void CacheTracePerfWarn(const char* message)
{
    uint32 expected = 0;
    if (pg_atomic_compare_exchange_u32(&cache_trace_perf_warning, &expected, 1)) {
        ereport(LOG, (errmsg("CACHE_TRACE_MPKI disabled: %s: %m", message)));
    }
}

static void CacheTracePerfInit()
{
    cache_trace.perf.available = false;
    cache_trace.perf.running = false;
    cache_trace.perf.instruction_fd = -1;
    cache_trace.perf.cache_miss_fd = -1;

#ifdef __linux__
    cache_trace.perf.instruction_fd = CacheTraceOpenPerfCounter(PERF_COUNT_HW_INSTRUCTIONS, -1);
    if (cache_trace.perf.instruction_fd < 0) {
        CacheTracePerfWarn("failed to open instruction perf event");
        return;
    }

    cache_trace.perf.cache_miss_fd =
        CacheTraceOpenPerfCounter(PERF_COUNT_HW_CACHE_MISSES, cache_trace.perf.instruction_fd);
    if (cache_trace.perf.cache_miss_fd < 0) {
        int saved_errno = errno;
        CacheTracePerfClose();
        errno = saved_errno;
        CacheTracePerfWarn("failed to open cache miss perf event");
        return;
    }

    if (ioctl(cache_trace.perf.instruction_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) != 0 ||
        ioctl(cache_trace.perf.instruction_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) != 0) {
        int saved_errno = errno;
        CacheTracePerfClose();
        errno = saved_errno;
        CacheTracePerfWarn("failed to start perf events");
        return;
    }

    cache_trace.perf.available = true;
    cache_trace.perf.running = true;
#endif
}

static bool CacheTracePerfReadFd(
    int instruction_fd, uint64* instructions, uint64* cache_misses, uint64* cpu_running_ns)
{
#ifdef __linux__
    CacheTracePerfGroupRead perf_read;
    int rc;
    do {
        rc = read(instruction_fd, &perf_read, sizeof(perf_read));
    } while (rc < 0 && errno == EINTR);

    if (rc != (int)sizeof(perf_read) || perf_read.nr < 2) {
        return false;
    }
    *instructions = perf_read.values[0];
    *cache_misses = perf_read.values[1];
    *cpu_running_ns = perf_read.time_running;
    return true;
#else
    return false;
#endif
}

static uint64 CacheTracePackOperator(uint64 generation, int plan_node_id, int node_tag, int depth)
{
    uint64 plan = CACHE_TRACE_INVALID_PLAN_ID;
    uint64 tag = 0;
    uint64 packed_depth = 0;

    if (plan_node_id >= 0 && (uint64)plan_node_id < CACHE_TRACE_INVALID_PLAN_ID && node_tag >= 0 &&
        (uint64)node_tag <= CACHE_TRACE_NODE_TAG_MASK && depth > 0 && (uint64)depth <= CACHE_TRACE_DEPTH_MASK) {
        plan = (uint64)plan_node_id;
        tag = (uint64)node_tag;
        packed_depth = (uint64)depth;
    }

    return (generation & CACHE_TRACE_GENERATION_MASK) |
           ((plan & CACHE_TRACE_PLAN_ID_MASK) << CACHE_TRACE_GENERATION_BITS) |
           ((tag & CACHE_TRACE_NODE_TAG_MASK) << (CACHE_TRACE_GENERATION_BITS + CACHE_TRACE_PLAN_ID_BITS)) |
           ((packed_depth & CACHE_TRACE_DEPTH_MASK)
               << (CACHE_TRACE_GENERATION_BITS + CACHE_TRACE_PLAN_ID_BITS + CACHE_TRACE_NODE_TAG_BITS));
}

static void CacheTraceUnpackOperator(
    uint64 packed, uint64* generation, int32* plan_node_id, int32* node_tag, int32* depth)
{
    *generation = packed & CACHE_TRACE_GENERATION_MASK;
    *plan_node_id =
        (int32)((packed >> CACHE_TRACE_GENERATION_BITS) & CACHE_TRACE_PLAN_ID_MASK);
    *node_tag = (int32)((packed >> (CACHE_TRACE_GENERATION_BITS + CACHE_TRACE_PLAN_ID_BITS)) &
                        CACHE_TRACE_NODE_TAG_MASK);
    *depth = (int32)((packed >> (CACHE_TRACE_GENERATION_BITS + CACHE_TRACE_PLAN_ID_BITS +
                                  CACHE_TRACE_NODE_TAG_BITS)) &
                     CACHE_TRACE_DEPTH_MASK);
}

static bool CacheTraceSampleSlot(CacheTraceSlot* slot, bool adaptive)
{
    uint32 expected = CACHE_TRACE_SLOT_ACTIVE;
    if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CACHE_TRACE_SLOT_SAMPLING)) {
        return false;
    }

    if (slot->sample_count >= CACHE_TRACE_SAMPLE_CAPACITY) {
        slot->dropped_samples++;
        pg_atomic_write_u32(&slot->state, CACHE_TRACE_SLOT_ACTIVE);
        return true;
    }

    CacheTraceSample* sample = &slot->samples[slot->sample_count];
    errno_t rc = memset_s(sample, sizeof(*sample), 0, sizeof(*sample));
    securec_check(rc, "\0", "\0");

    uint64 packed_before = __atomic_load_n(&slot->operator_state, __ATOMIC_ACQUIRE);
    int32 dop = slot->current_dop;
    uint64 instructions = 0;
    uint64 cache_misses = 0;
    uint64 cpu_running_ns = 0;
    bool perf_valid = slot->perf_available &&
                      CacheTracePerfReadFd(
                          slot->instruction_fd, &instructions, &cache_misses, &cpu_running_ns);
    uint64 packed_after = __atomic_load_n(&slot->operator_state, __ATOMIC_ACQUIRE);

    sample->time_ns = CacheTraceTime();
    sample->instructions = instructions;
    sample->cache_misses = cache_misses;
    sample->cpu_running_us = (uint32)(cpu_running_ns / 1000ULL);
    sample->dop = dop;
    CacheTraceUnpackOperator(packed_after,
        &sample->operator_generation,
        &sample->plan_node_id,
        &sample->node_tag,
        &sample->depth);

    if (packed_before == packed_after && sample->plan_node_id != (int32)CACHE_TRACE_INVALID_PLAN_ID) {
        sample->flags |= CACHE_TRACE_SAMPLE_FLAG_OPERATOR_VALID;
    }
    if (perf_valid) {
        sample->flags |= CACHE_TRACE_SAMPLE_FLAG_PERF_VALID;
    } else if (slot->perf_available) {
        slot->perf_read_errors++;
    }
    if (adaptive) {
        sample->flags |= CACHE_TRACE_SAMPLE_FLAG_ADAPTIVE;
    }

    slot->sample_count++;
    pg_atomic_write_u32(&slot->state, CACHE_TRACE_SLOT_ACTIVE);
    return true;
}

static uint32 CacheTraceAdaptiveIntervalTicks(uint32 stable_probes, uint32 regular_ticks)
{
    uint32 interval_ticks;
    if (stable_probes < 4) {
        interval_ticks = 1;
    } else if (stable_probes < 8) {
        interval_ticks = 2;
    } else if (stable_probes < 16) {
        interval_ticks = 4;
    } else if (stable_probes < 32) {
        interval_ticks = 8;
    } else {
        interval_ticks = regular_ticks;
    }
    return interval_ticks < regular_ticks ? interval_ticks : regular_ticks;
}

static void CacheTraceProbeSlot(CacheTraceGlobal* global, uint32 index, uint64 tick)
{
    CacheTraceSlot* slot = &global->slots[index];
    if (pg_atomic_read_u32(&slot->state) != CACHE_TRACE_SLOT_ACTIVE) {
        return;
    }

    uint64 activation_id = __atomic_load_n(&slot->thread_start_ns, __ATOMIC_ACQUIRE);
    uint64 packed = __atomic_load_n(&slot->operator_state, __ATOMIC_ACQUIRE);
    if (pg_atomic_read_u32(&slot->state) != CACHE_TRACE_SLOT_ACTIVE) {
        return;
    }

    CacheTraceAdaptiveState* adaptive = &global->adaptive[index];
    if (!adaptive->initialized || adaptive->activation_id != activation_id) {
        adaptive->activation_id = activation_id;
        adaptive->last_operator_state = packed;
        adaptive->next_regular_tick = tick;
        adaptive->next_adaptive_tick = tick;
        adaptive->refill_epoch = tick / global->regular_interval_ticks;
        adaptive->stable_probes = 0;
        adaptive->fast_tokens = CACHE_TRACE_FAST_TOKEN_CAPACITY;
        adaptive->initialized = true;
    } else if (adaptive->last_operator_state != packed) {
        adaptive->last_operator_state = packed;
        adaptive->stable_probes = 0;
        adaptive->next_adaptive_tick = tick;
    } else if (adaptive->stable_probes < UINT32_MAX) {
        adaptive->stable_probes++;
    }

    uint64 refill_epoch = tick / global->regular_interval_ticks;
    if (refill_epoch > adaptive->refill_epoch) {
        uint64 refill = refill_epoch - adaptive->refill_epoch;
        uint64 tokens = (uint64)adaptive->fast_tokens + refill;
        adaptive->fast_tokens =
            (uint32)(tokens > CACHE_TRACE_FAST_TOKEN_CAPACITY ? CACHE_TRACE_FAST_TOKEN_CAPACITY : tokens);
        adaptive->refill_epoch = refill_epoch;
    }

    uint32 adaptive_ticks =
        CacheTraceAdaptiveIntervalTicks(adaptive->stable_probes, global->regular_interval_ticks);
    bool regular_due = tick >= adaptive->next_regular_tick;
    bool adaptive_due = adaptive_ticks < global->regular_interval_ticks &&
                        tick >= adaptive->next_adaptive_tick;
    bool adaptive_sample = false;

    if (!regular_due) {
        if (!adaptive_due || adaptive->fast_tokens == 0) {
            if (adaptive_due) {
                adaptive->next_adaptive_tick = tick + adaptive_ticks;
            }
            return;
        }
        adaptive_sample = true;
    }

    if (!CacheTraceSampleSlot(slot, adaptive_sample)) {
        return;
    }
    if (regular_due) {
        adaptive->next_regular_tick =
            (tick / global->regular_interval_ticks + 1) * global->regular_interval_ticks;
    } else {
        adaptive->fast_tokens--;
    }
    adaptive->next_adaptive_tick = tick + adaptive_ticks;
}

static uint32 CacheTraceSamplerIndex(int32 numa_id)
{
    return numa_id >= 0 && numa_id < CACHE_TRACE_MAX_NUMA_NODES ?
               (uint32)numa_id :
               (uint32)CACHE_TRACE_MAX_NUMA_NODES;
}

static CacheTraceNumaSampler* CacheTraceGetNumaSampler(CacheTraceGlobal* global, int32 numa_id)
{
    if (global == NULL) {
        return NULL;
    }
    return &global->samplers[CacheTraceSamplerIndex(numa_id)];
}

static void* CacheTraceSamplerMain(void* arg)
{
    knl_thread_arg* thread_arg = (knl_thread_arg*)arg;
    CacheTraceNumaSampler* sampler =
        thread_arg == NULL ? NULL : (CacheTraceNumaSampler*)thread_arg->payload;
    CacheTraceGlobal* global = sampler == NULL ? NULL : sampler->global;
    if (sampler == NULL || global == NULL) {
        return NULL;
    }

#ifdef __linux__
    sampler->os_thread_id = (uint64)syscall(__NR_gettid);
#endif
#ifdef __USE_NUMA
    if (sampler->numa_id >= 0 && numa_available() != -1) {
        (void)numa_run_on_node(sampler->numa_id);
    }
    int cpu_id = sched_getcpu();
    if (cpu_id >= 0) {
        sampler->bound_numa_id = numa_node_of_cpu(cpu_id);
    }
#endif

    struct timespec sleep_time;
    sleep_time.tv_sec = CACHE_TRACE_PROBE_INTERVAL_US / 1000000;
    sleep_time.tv_nsec = (long)(CACHE_TRACE_PROBE_INTERVAL_US % 1000000) * 1000L;

    uint64 tick = 0;
    while (pg_atomic_read_u32(&global->sampler_running) != 0) {
        struct timespec remaining = sleep_time;
        while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
        }

        tick++;
        for (uint32 word = 0; word < CACHE_TRACE_ACTIVE_BITMAP_WORDS; word++) {
            uint32 active = __atomic_load_n(&sampler->active_bitmap[word], __ATOMIC_ACQUIRE);
            while (active != 0) {
                uint32 bit = (uint32)__builtin_ctz(active);
                uint32 index = word * 32 + bit;
                if (index < CACHE_TRACE_MAX_THREADS) {
                    CacheTraceProbeSlot(global, index, tick);
                }
                active &= active - 1;
            }
        }
    }
    return NULL;
}

static CacheTraceGlobal* CacheTraceGetGlobal(uint32 interval_us)
{
    (void)pthread_mutex_lock(&cache_trace_global_lock);
    if (cache_trace_global == NULL) {
        CacheTraceGlobal* global = (CacheTraceGlobal*)calloc(1, sizeof(CacheTraceGlobal));
        void* slot_memory = NULL;
        if (global != NULL &&
            posix_memalign(&slot_memory,
                CACHE_TRACE_SLOT_ALIGNMENT,
                sizeof(CacheTraceSlot) * CACHE_TRACE_MAX_THREADS) == 0) {
            errno_t rc = memset_s(
                slot_memory, sizeof(CacheTraceSlot) * CACHE_TRACE_MAX_THREADS, 0,
                sizeof(CacheTraceSlot) * CACHE_TRACE_MAX_THREADS);
            securec_check(rc, "\0", "\0");
            global->slots = (CacheTraceSlot*)slot_memory;
            global->interval_us = interval_us;
            global->regular_interval_ticks =
                (interval_us + CACHE_TRACE_PROBE_INTERVAL_US - 1) / CACHE_TRACE_PROBE_INTERVAL_US;
            pg_atomic_init_u32(&global->sampler_running, 1);
            for (uint32 i = 0; i < CACHE_TRACE_NUMA_SAMPLER_COUNT; i++) {
                global->samplers[i].global = global;
                global->samplers[i].numa_id =
                    i < CACHE_TRACE_MAX_NUMA_NODES ? (int32)i : -1;
                global->samplers[i].bound_numa_id = -1;
                global->samplers[i].os_thread_id = 0;
                pg_atomic_init_u32(
                    &global->samplers[i].state, CACHE_TRACE_SAMPLER_NOT_STARTED);
            }
            cache_trace_global = global;
        } else {
            free(slot_memory);
            free(global);
            global = NULL;
        }
    }
    CacheTraceGlobal* result = cache_trace_global;
    (void)pthread_mutex_unlock(&cache_trace_global_lock);
    return result;
}

static bool CacheTraceEnsureNumaSampler(CacheTraceGlobal* global, int32 numa_id)
{
    CacheTraceNumaSampler* sampler = CacheTraceGetNumaSampler(global, numa_id);
    if (sampler == NULL) {
        return false;
    }

    uint32 state = pg_atomic_read_u32(&sampler->state);
    if (state == CACHE_TRACE_SAMPLER_RUNNING) {
        return true;
    }
    if (state == CACHE_TRACE_SAMPLER_FAILED) {
        return false;
    }

    (void)pthread_mutex_lock(&cache_trace_global_lock);
    state = pg_atomic_read_u32(&sampler->state);
    if (state == CACHE_TRACE_SAMPLER_NOT_STARTED) {
        ThreadArg* thread_arg = (ThreadArg*)calloc(1, sizeof(ThreadArg));
        if (thread_arg != NULL) {
            thread_arg->next = (ThreadArg*)INVALID_NEXT_ADDR;
            thread_arg->m_thd_arg.payload = sampler;
        }

        if (thread_arg == NULL ||
            gs_thread_create(&sampler->thread, CacheTraceSamplerMain, 0, thread_arg) != 0) {
            free(thread_arg);
            pg_atomic_write_u32(&sampler->state, CACHE_TRACE_SAMPLER_FAILED);
        } else {
            pg_atomic_write_u32(&sampler->state, CACHE_TRACE_SAMPLER_RUNNING);
        }
        state = pg_atomic_read_u32(&sampler->state);
    }
    (void)pthread_mutex_unlock(&cache_trace_global_lock);
    return state == CACHE_TRACE_SAMPLER_RUNNING;
}

static CacheTraceSlot* CacheTraceAcquireSlot(CacheTraceGlobal* global)
{
    if (global == NULL) {
        return NULL;
    }

    uint32 start = (uint32)((uint64)t_thrd.proc_cxt.MyProcPid % CACHE_TRACE_MAX_THREADS);
    for (uint32 offset = 0; offset < CACHE_TRACE_MAX_THREADS; offset++) {
        uint32 index = (start + offset) % CACHE_TRACE_MAX_THREADS;
        uint32 expected = CACHE_TRACE_SLOT_FREE;
        if (!pg_atomic_compare_exchange_u32(&global->slots[index].state, &expected, CACHE_TRACE_SLOT_RESERVED)) {
            continue;
        }

        return &global->slots[index];
    }
    return NULL;
}

static void CacheTraceSetSlotActive(
    CacheTraceGlobal* global, CacheTraceSlot* slot, int32 numa_id, bool active)
{
    CacheTraceNumaSampler* sampler = CacheTraceGetNumaSampler(global, numa_id);
    if (sampler == NULL || slot == NULL) {
        return;
    }

    uint32 index = (uint32)(slot - global->slots);
    uint32 word = index / 32;
    uint32 mask = 1U << (index % 32);
    if (active) {
        (void)pg_atomic_fetch_or_u32(&sampler->active_bitmap[word], mask);
    } else {
        (void)pg_atomic_fetch_and_u32(&sampler->active_bitmap[word], ~mask);
    }
}

static bool CacheTraceInit()
{
    cache_trace.inited = true;
    cache_trace.enabled = CacheTraceIsEnabledValue(gs_getenv_r(CACHE_TRACE_ENV));
    cache_trace.interval_us = CacheTraceParseInterval(gs_getenv_r(CACHE_TRACE_INTERVAL_ENV));
    return cache_trace.enabled;
}

static inline bool CacheTraceEnabled()
{
    if (unlikely(!cache_trace.inited)) {
        return CacheTraceInit();
    }
    return cache_trace.enabled;
}

static void CacheTraceGetThreadPoolLocation(int32* numa_id, int32* group_id)
{
    *numa_id = -1;
    *group_id = -1;

    ThreadPoolGroup* group = t_thrd.threadpool_cxt.group;
    if (group != NULL) {
        *numa_id = group->GetNumaId();
        *group_id = group->GetGroupId();
    }

#ifdef __USE_NUMA
    if (*numa_id < 0) {
        int cpu_id = sched_getcpu();
        if (cpu_id >= 0) {
            *numa_id = numa_node_of_cpu(cpu_id);
        }
    }
#endif
}

static void CacheTracePublishOperator(const CacheTraceFrame* frame)
{
    if (cache_trace.slot == NULL) {
        return;
    }

    cache_trace.operator_generation =
        (cache_trace.operator_generation + 1) & CACHE_TRACE_GENERATION_MASK;
    cache_trace.operator_changes++;

    int plan_node_id = -1;
    int node_tag = 0;
    int depth = 0;
    int dop = 0;
    if (frame != NULL) {
        plan_node_id = frame->plan_node_id;
        node_tag = frame->node_tag;
        depth = frame->depth;
        dop = frame->dop;
    }

    cache_trace.slot->current_dop = dop;
    uint64 packed = CacheTracePackOperator(cache_trace.operator_generation, plan_node_id, node_tag, depth);
    __atomic_store_n(&cache_trace.slot->operator_state, packed, __ATOMIC_RELEASE);
}

static bool CacheTraceStartQuery(PlanState* node)
{
    int32 numa_id = -1;
    int32 group_id = -1;
    CacheTraceGetThreadPoolLocation(&numa_id, &group_id);

    CacheTraceGlobal* global = CacheTraceGetGlobal(cache_trace.interval_us);
    if (global == NULL) {
        cache_trace.enabled = false;
        ereport(LOG, (errmsg("CACHE_TRACE disabled: failed to initialize sampler state")));
        return false;
    }
    if (!CacheTraceEnsureNumaSampler(global, numa_id)) {
        cache_trace.enabled = false;
        ereport(LOG,
            (errmsg("CACHE_TRACE disabled: failed to start sampler for NUMA node %d", numa_id)));
        return false;
    }

    CacheTraceSlot* slot = CacheTraceAcquireSlot(global);
    if (slot == NULL) {
        cache_trace.enabled = false;
        ereport(LOG, (errmsg("CACHE_TRACE disabled: no free sampler slot")));
        return false;
    }

    CacheTraceSample* samples =
        (CacheTraceSample*)malloc(sizeof(CacheTraceSample) * CACHE_TRACE_SAMPLE_CAPACITY);
    if (samples == NULL) {
        pg_atomic_write_u32(&slot->state, CACHE_TRACE_SLOT_FREE);
        cache_trace.enabled = false;
        ereport(LOG, (errmsg("CACHE_TRACE disabled: failed to allocate per-thread sample buffer")));
        return false;
    }

    cache_trace.query_active = true;
    cache_trace.trace_incomplete = false;
    cache_trace.query_id = CacheTraceGetQueryId(node);
    cache_trace.thread_id = t_thrd.proc_cxt.MyProcPid;
    cache_trace.thread_start_ns = CacheTraceTime();
    cache_trace.thread_end_ns = 0;
    cache_trace.operator_generation = 0;
    cache_trace.operator_changes = 0;
    cache_trace.numa_id = numa_id;
    cache_trace.group_id = group_id;
    cache_trace.stack_errors = 0;
    cache_trace.output_errors = 0;
    cache_trace.stack_depth = 0;
    cache_trace.suppressed_depth = 0;
    cache_trace.slot = slot;
    CacheTracePerfInit();

    slot->query_id = cache_trace.query_id;
    slot->thread_id = cache_trace.thread_id;
    __atomic_store_n(&slot->thread_start_ns, cache_trace.thread_start_ns, __ATOMIC_RELEASE);
    slot->samples = samples;
    slot->sample_count = 0;
    slot->dropped_samples = 0;
    slot->perf_read_errors = 0;
    slot->current_dop = 0;
    slot->instruction_fd = cache_trace.perf.instruction_fd;
    slot->cache_miss_fd = cache_trace.perf.cache_miss_fd;
    slot->perf_available = cache_trace.perf.available;
    __atomic_store_n(&slot->operator_state,
        CacheTracePackOperator(cache_trace.operator_generation, -1, 0, 0),
        __ATOMIC_RELEASE);
    pg_atomic_write_u32(&slot->state, CACHE_TRACE_SLOT_ACTIVE);
    CacheTraceSetSlotActive(global, slot, cache_trace.numa_id, true);
    return true;
}

void VecCacheTraceEnter(PlanState* node)
{
    if (!CacheTraceEnabled() || node == NULL || node->plan == NULL) {
        return;
    }

    if (!cache_trace.query_active && !CacheTraceStartQuery(node)) {
        return;
    }
    if (cache_trace.query_id == 0) {
        cache_trace.query_id = CacheTraceGetQueryId(node);
        cache_trace.slot->query_id = cache_trace.query_id;
    }

    if (cache_trace.suppressed_depth > 0) {
        cache_trace.suppressed_depth++;
        CacheTracePublishOperator(NULL);
        return;
    }
    if (cache_trace.stack_depth >= CACHE_TRACE_STACK_DEPTH) {
        cache_trace.suppressed_depth = 1;
        cache_trace.stack_errors++;
        cache_trace.trace_incomplete = true;
        CacheTracePublishOperator(NULL);
        return;
    }

    CacheTraceFrame* frame = &cache_trace.stack[cache_trace.stack_depth];
    frame->node = node;
    frame->plan_node_id = node->plan->plan_node_id;
    frame->node_tag = (int32)nodeTag(node);
    frame->dop = node->plan->dop;
    frame->depth = cache_trace.stack_depth + 1;
    cache_trace.stack_depth++;
    CacheTracePublishOperator(frame);
}

void VecCacheTraceLeave(PlanState* node)
{
    if (!cache_trace.inited || !cache_trace.enabled || !cache_trace.query_active || node == NULL) {
        return;
    }

    if (cache_trace.suppressed_depth > 0) {
        cache_trace.suppressed_depth--;
        if (cache_trace.suppressed_depth == 0 && cache_trace.stack_depth > 0) {
            CacheTracePublishOperator(&cache_trace.stack[cache_trace.stack_depth - 1]);
        } else {
            CacheTracePublishOperator(NULL);
        }
        return;
    }

    if (cache_trace.stack_depth <= 0 || cache_trace.stack[cache_trace.stack_depth - 1].node != node) {
        cache_trace.stack_errors++;
        cache_trace.trace_incomplete = true;
        cache_trace.stack_depth = 0;
        CacheTracePublishOperator(NULL);
        return;
    }

    cache_trace.stack_depth--;
    if (cache_trace.stack_depth > 0) {
        CacheTracePublishOperator(&cache_trace.stack[cache_trace.stack_depth - 1]);
    } else {
        CacheTracePublishOperator(NULL);
    }
}

static bool CacheTraceClaimClosing(CacheTraceSlot* slot)
{
    if (slot == NULL) {
        return false;
    }

    for (;;) {
        uint32 expected = CACHE_TRACE_SLOT_ACTIVE;
        if (pg_atomic_compare_exchange_u32(&slot->state, &expected, CACHE_TRACE_SLOT_CLOSING)) {
            return true;
        }
        if (expected != CACHE_TRACE_SLOT_SAMPLING) {
            return false;
        }
        sched_yield();
    }
}

static bool CacheTraceWriteAll(int fd, const void* data, size_t size)
{
    const char* current = (const char*)data;
    while (size > 0) {
        ssize_t written = write(fd, current, size);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        current += written;
        size -= (size_t)written;
    }
    return true;
}

static bool CacheTraceWriteBinary(char* path, size_t path_size)
{
    if (path == NULL || path_size == 0 || cache_trace.slot == NULL || t_thrd.proc_cxt.DataDir == NULL) {
        return false;
    }

    char directory[MAXPGPATH];
    int rc = snprintf_s(directory,
        sizeof(directory),
        sizeof(directory) - 1,
        "%s/pg_log/cache_trace",
        t_thrd.proc_cxt.DataDir);
    securec_check_ss(rc, "\0", "\0");
    if (rc <= 0 || (mkdir(directory, S_IRWXU) != 0 && errno != EEXIST)) {
        return false;
    }

    rc = snprintf_s(path,
        path_size,
        path_size - 1,
        "%s/cache_trace_q" UINT64_FORMAT "_t%lu_s" UINT64_FORMAT ".bin",
        directory,
        cache_trace.query_id,
        cache_trace.thread_id,
        cache_trace.thread_start_ns);
    securec_check_ss(rc, "\0", "\0");
    if (rc <= 0) {
        return false;
    }

    CacheTraceBinaryHeader header;
    errno_t zero_rc = memset_s(&header, sizeof(header), 0, sizeof(header));
    securec_check(zero_rc, "\0", "\0");
    header.magic = CACHE_TRACE_BINARY_MAGIC;
    header.version = CACHE_TRACE_BINARY_VERSION;
    header.header_size = sizeof(header);
    header.sample_size = sizeof(CacheTraceSample);
    header.flags = (cache_trace.trace_incomplete || cache_trace.slot->dropped_samples > 0
                        ? CACHE_TRACE_BINARY_FLAG_INCOMPLETE
                        : 0) |
                   (cache_trace.perf.available
                           ? CACHE_TRACE_BINARY_FLAG_MPKI_AVAILABLE |
                                 CACHE_TRACE_BINARY_FLAG_CPU_TIME_AVAILABLE
                           : 0);
    header.interval_us = cache_trace.interval_us;
    header.stack_errors = cache_trace.stack_errors;
    header.output_errors = cache_trace.output_errors;
    header.probe_interval_us = CACHE_TRACE_PROBE_INTERVAL_US;
    header.numa_id = cache_trace.numa_id;
    header.group_id = cache_trace.group_id;
    header.query_id = cache_trace.query_id;
    header.thread_id = (uint64)cache_trace.thread_id;
    header.thread_start_ns = cache_trace.thread_start_ns;
    header.thread_end_ns = cache_trace.thread_end_ns;
    header.operator_changes = cache_trace.operator_changes;
    header.sample_count = cache_trace.slot->sample_count;
    header.dropped_samples = cache_trace.slot->dropped_samples;
    header.perf_read_errors = cache_trace.slot->perf_read_errors;

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return false;
    }
    bool success = CacheTraceWriteAll(fd, &header, sizeof(header)) &&
                   CacheTraceWriteAll(fd,
                       cache_trace.slot->samples,
                       cache_trace.slot->sample_count * sizeof(CacheTraceSample));
    if (close(fd) != 0) {
        success = false;
    }
    return success;
}

static void CacheTraceLogAndRelease(bool write_output)
{
    CacheTraceSlot* slot = cache_trace.slot;
    if (slot == NULL || !CacheTraceClaimClosing(slot)) {
        cache_trace.trace_incomplete = true;
        return;
    }
    CacheTraceSetSlotActive(cache_trace_global, slot, cache_trace.numa_id, false);

    char binary_path[MAXPGPATH];
    binary_path[0] = '\0';
    bool output_ok = !write_output || CacheTraceWriteBinary(binary_path, sizeof(binary_path));
    if (write_output && !output_ok) {
        cache_trace.output_errors++;
        cache_trace.trace_incomplete = true;
    }

    if (write_output) {
        CacheTraceNumaSampler* sampler =
            CacheTraceGetNumaSampler(cache_trace_global, cache_trace.numa_id);
        uint64 sampler_thread_id =
            sampler == NULL ? 0 : __atomic_load_n(&sampler->os_thread_id, __ATOMIC_ACQUIRE);
        int32 sampler_numa_id =
            sampler == NULL ? -1 : __atomic_load_n(&sampler->bound_numa_id, __ATOMIC_ACQUIRE);
        uint32 adaptive_samples = 0;
        for (uint32 i = 0; i < slot->sample_count; i++) {
            if ((slot->samples[i].flags & CACHE_TRACE_SAMPLE_FLAG_ADAPTIVE) != 0) {
                adaptive_samples++;
            }
        }
        ereport(LOG,
            (errmsg("CACHE_TRACE_THREAD_BEGIN tid=%lu query=" UINT64_FORMAT
                    " samples=%u capacity=%d dropped=%u stack_errors=%u perf_read_errors=%u "
                    "output_errors=%u incomplete=%d start_ns=" UINT64_FORMAT " end_ns=" UINT64_FORMAT
                    " operator_changes=" UINT64_FORMAT " numa=%d group=%d mpki_available=%d interval_us=%u "
                    "probe_interval_us=%u adaptive_samples=%u sampler_tid=" UINT64_FORMAT
                    " sampler_numa=%d cpu_time_available=%d binary_ok=%d binary_file=%s format=binary_v4",
                cache_trace.thread_id,
                cache_trace.query_id,
                slot->sample_count,
                CACHE_TRACE_SAMPLE_CAPACITY,
                slot->dropped_samples,
                cache_trace.stack_errors,
                slot->perf_read_errors,
                cache_trace.output_errors,
                cache_trace.trace_incomplete ? 1 : 0,
                cache_trace.thread_start_ns,
                cache_trace.thread_end_ns,
                cache_trace.operator_changes,
                cache_trace.numa_id,
                cache_trace.group_id,
                slot->perf_available ? 1 : 0,
                cache_trace.interval_us,
                CACHE_TRACE_PROBE_INTERVAL_US,
                adaptive_samples,
                sampler_thread_id,
                sampler_numa_id,
                slot->perf_available ? 1 : 0,
                output_ok ? 1 : 0,
                output_ok ? binary_path : "none")));
        ereport(LOG,
            (errmsg("CACHE_TRACE_THREAD_END tid=%lu query=" UINT64_FORMAT
                    " samples=%u dropped=%u output_errors=%u",
                cache_trace.thread_id,
                cache_trace.query_id,
                slot->sample_count,
                slot->dropped_samples,
                cache_trace.output_errors)));
    }

    CacheTracePerfClose();
    free(slot->samples);
    slot->samples = NULL;
    slot->sample_count = 0;
    slot->dropped_samples = 0;
    slot->perf_read_errors = 0;
    slot->perf_available = false;
    slot->instruction_fd = -1;
    slot->cache_miss_fd = -1;
    pg_atomic_write_u32(&slot->state, CACHE_TRACE_SLOT_FREE);
}

static void CacheTraceResetQuery()
{
    cache_trace.query_active = false;
    cache_trace.trace_incomplete = false;
    cache_trace.query_id = 0;
    cache_trace.thread_id = 0;
    cache_trace.thread_start_ns = 0;
    cache_trace.thread_end_ns = 0;
    cache_trace.operator_generation = 0;
    cache_trace.operator_changes = 0;
    cache_trace.numa_id = -1;
    cache_trace.group_id = -1;
    cache_trace.stack_errors = 0;
    cache_trace.output_errors = 0;
    cache_trace.stack_depth = 0;
    cache_trace.suppressed_depth = 0;
    cache_trace.slot = NULL;
}

void VecCacheTraceFlush()
{
    if (!cache_trace.inited || !cache_trace.enabled || !cache_trace.query_active) {
        return;
    }

    cache_trace.thread_end_ns = CacheTraceTime();
    if (cache_trace.stack_depth != 0 || cache_trace.suppressed_depth != 0) {
        cache_trace.stack_errors++;
        cache_trace.trace_incomplete = true;
    }
    CacheTraceLogAndRelease(true);
    CacheTraceResetQuery();
}

void VecCacheTraceDiscard()
{
    if (!cache_trace.inited || !cache_trace.enabled || !cache_trace.query_active) {
        return;
    }

    cache_trace.thread_end_ns = CacheTraceTime();
    CacheTraceLogAndRelease(false);
    CacheTraceResetQuery();
}
