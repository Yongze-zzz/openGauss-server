/*
 * VectorEngine call-duration profiler.
 */
#include "postgres.h"

#include <sched.h>
#include <stdlib.h>
#include <time.h>

#include "knl/knl_variable.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.h"

extern char* nodeTagToString(NodeTag type);

#define VEC_ENGINE_PROFILE_ENV "OG_VEC_ENGINE_PROFILE"
#define VEC_ENGINE_PROFILE_MAX_ENTRIES 4096
#define VEC_ENGINE_PROFILE_BUCKETS 17
#define VEC_ENGINE_PROFILE_UINT64_MAX ((uint64)~0ULL)

typedef struct VecEngineProfileEntry {
    uint64 query_id;
    int cpu_id;
    int plan_node_id;
    int node_tag;
    int dop;
    int depth;
    uint64 calls;
    uint64 null_calls;
    uint64 rows;
    uint64 total_ns;
    uint64 min_ns;
    uint64 max_ns;
    uint64 buckets[VEC_ENGINE_PROFILE_BUCKETS];
} VecEngineProfileEntry;

typedef struct VecEngineProfileContext {
    bool inited;
    bool enabled;
    uint64 query_id;
    int depth;
    size_t entry_num;
    size_t dropped_entries;
    VecEngineProfileEntry* entries;
} VecEngineProfileContext;

static THR_LOCAL VecEngineProfileContext vec_engine_profile_cxt = {
    false, false, 0, 0, 0, 0, NULL};

static const uint64 vec_engine_profile_bucket_us[VEC_ENGINE_PROFILE_BUCKETS - 1] = {
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000};

static bool VecProfileIsEnabledValue(const char* value)
{
    return value != NULL && (value[0] == '1' || value[0] == 't' || value[0] == 'T' || value[0] == 'y' ||
                                value[0] == 'Y' ||
                                ((value[0] == 'o' || value[0] == 'O') &&
                                    (value[1] == 'n' || value[1] == 'N')));
}

static uint64 VecEngineProfileTime()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }

    return (uint64)ts.tv_sec * 1000000000ULL + (uint64)ts.tv_nsec;
}

static uint64 VecEngineProfileGetQueryId(PlanState* node)
{
    if (node != NULL && node->state != NULL && node->state->es_plannedstmt != NULL) {
        vec_engine_profile_cxt.query_id = node->state->es_plannedstmt->queryId;
    }

    return vec_engine_profile_cxt.query_id;
}

static bool VecEngineProfileInit()
{
    if (!VecProfileIsEnabledValue(gs_getenv_r(VEC_ENGINE_PROFILE_ENV))) {
        vec_engine_profile_cxt.inited = true;
        vec_engine_profile_cxt.enabled = false;
        return false;
    }

    vec_engine_profile_cxt.entries =
        (VecEngineProfileEntry*)malloc(sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES);
    if (vec_engine_profile_cxt.entries == NULL) {
        vec_engine_profile_cxt.inited = true;
        vec_engine_profile_cxt.enabled = false;
        return false;
    }

    errno_t ret = memset_s(vec_engine_profile_cxt.entries,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES,
        0,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES);
    securec_check(ret, "\0", "\0");
    vec_engine_profile_cxt.query_id = 0;
    vec_engine_profile_cxt.depth = 0;
    vec_engine_profile_cxt.entry_num = 0;
    vec_engine_profile_cxt.dropped_entries = 0;
    vec_engine_profile_cxt.inited = true;
    vec_engine_profile_cxt.enabled = true;
    return true;
}

bool VecEngineProfileEnabled()
{
    if (unlikely(!vec_engine_profile_cxt.inited)) {
        return VecEngineProfileInit();
    }

    return vec_engine_profile_cxt.enabled;
}

static int VecEngineProfileBucket(uint64 duration_ns)
{
    uint64 duration_us = duration_ns / 1000ULL;
    for (int i = 0; i < VEC_ENGINE_PROFILE_BUCKETS - 1; i++) {
        if (duration_us <= vec_engine_profile_bucket_us[i]) {
            return i;
        }
    }

    return VEC_ENGINE_PROFILE_BUCKETS - 1;
}

static VecEngineProfileEntry* VecEngineProfileFindEntry(
    uint64 query_id, int cpu_id, int plan_node_id, int node_tag, int dop, int depth)
{
    for (size_t i = 0; i < vec_engine_profile_cxt.entry_num; i++) {
        VecEngineProfileEntry* entry = &vec_engine_profile_cxt.entries[i];
        if (entry->query_id == query_id && entry->cpu_id == cpu_id && entry->plan_node_id == plan_node_id &&
            entry->node_tag == node_tag && entry->dop == dop && entry->depth == depth) {
            return entry;
        }
    }

    if (vec_engine_profile_cxt.entry_num >= VEC_ENGINE_PROFILE_MAX_ENTRIES) {
        vec_engine_profile_cxt.dropped_entries++;
        return NULL;
    }

    VecEngineProfileEntry* entry = &vec_engine_profile_cxt.entries[vec_engine_profile_cxt.entry_num++];
    errno_t ret = memset_s(entry, sizeof(VecEngineProfileEntry), 0, sizeof(VecEngineProfileEntry));
    securec_check(ret, "\0", "\0");
    entry->query_id = query_id;
    entry->cpu_id = cpu_id;
    entry->plan_node_id = plan_node_id;
    entry->node_tag = node_tag;
    entry->dop = dop;
    entry->depth = depth;
    entry->min_ns = VEC_ENGINE_PROFILE_UINT64_MAX;
    return entry;
}

uint64 VecEngineProfileEnter(PlanState* node, int* depth, int* cpu_id)
{
    if (depth != NULL) {
        *depth = ++vec_engine_profile_cxt.depth;
    } else {
        vec_engine_profile_cxt.depth++;
    }

#ifdef __linux__
    if (cpu_id != NULL) {
        *cpu_id = sched_getcpu();
    }
#else
    if (cpu_id != NULL) {
        *cpu_id = -1;
    }
#endif

    (void)VecEngineProfileGetQueryId(node);
    return VecEngineProfileTime();
}

void VecEngineProfileLeave(PlanState* node, VectorBatch* result, uint64 start_time, int depth, int cpu_id)
{
    uint64 end_time = VecEngineProfileTime();
    if (start_time != 0 && end_time >= start_time && node != NULL && node->plan != NULL) {
        uint64 query_id = VecEngineProfileGetQueryId(node);
        int plan_node_id = node->plan->plan_node_id;
        int node_tag = (int)nodeTag(node);
        int dop = node->plan->dop;
        uint64 duration_ns = end_time - start_time;
        VecEngineProfileEntry* entry =
            VecEngineProfileFindEntry(query_id, cpu_id, plan_node_id, node_tag, dop, depth);
        if (entry != NULL) {
            entry->calls++;
            if (BatchIsNull(result)) {
                entry->null_calls++;
            } else {
                entry->rows += (uint64)result->m_rows;
            }
            entry->total_ns += duration_ns;
            if (duration_ns < entry->min_ns) {
                entry->min_ns = duration_ns;
            }
            if (duration_ns > entry->max_ns) {
                entry->max_ns = duration_ns;
            }
            entry->buckets[VecEngineProfileBucket(duration_ns)]++;
        }
    }

    if (vec_engine_profile_cxt.depth > 0) {
        vec_engine_profile_cxt.depth--;
    }
}

void VecEngineProfileFlush()
{
    if (!vec_engine_profile_cxt.inited || !vec_engine_profile_cxt.enabled) {
        return;
    }

    ereport(LOG,
        (errmsg("VEC_ENGINE_PROFILE_BEGIN tid=%lu query=" UINT64_FORMAT " entries=%lu dropped=%lu "
                "format=query_id,cpu_id,plan_node_id,node_tag,dop,depth,calls,null_calls,rows,total_ns,min_ns,max_ns,"
                "buckets_le_us,bucket_counts",
            t_thrd.proc_cxt.MyProcPid,
            vec_engine_profile_cxt.query_id,
            (unsigned long)vec_engine_profile_cxt.entry_num,
            (unsigned long)vec_engine_profile_cxt.dropped_entries)));

    for (size_t i = 0; i < vec_engine_profile_cxt.entry_num; i++) {
        VecEngineProfileEntry* entry = &vec_engine_profile_cxt.entries[i];
        char bucket_buf[320];
        char* cursor = bucket_buf;
        size_t left = sizeof(bucket_buf);
        bucket_buf[0] = '\0';
        for (int j = 0; j < VEC_ENGINE_PROFILE_BUCKETS; j++) {
            if (left <= 1) {
                break;
            }
            int rc = snprintf_s(cursor,
                left,
                left - 1,
                "%s" UINT64_FORMAT,
                j == 0 ? "" : "|",
                entry->buckets[j]);
            securec_check_ss(rc, "\0", "\0");
            if (rc <= 0 || (size_t)rc >= left) {
                break;
            }
            cursor += rc;
            left -= (size_t)rc;
        }

        char* node_tag_name = entry->plan_node_id >= 0 ? nodeTagToString((NodeTag)entry->node_tag) : (char*)"NULL";
        ereport(LOG,
            (errmsg("VEC_ENGINE_PROFILE_DATA tid=%lu " UINT64_FORMAT ",%d,%d,%s,%d,%d," UINT64_FORMAT ","
                    UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT
                    ",1|2|5|10|20|50|100|200|500|1000|2000|5000|10000|20000|50000|100000|inf,%s",
                t_thrd.proc_cxt.MyProcPid,
                entry->query_id,
                entry->cpu_id,
                entry->plan_node_id,
                node_tag_name,
                entry->dop,
                entry->depth,
                entry->calls,
                entry->null_calls,
                entry->rows,
                entry->total_ns,
                entry->min_ns == VEC_ENGINE_PROFILE_UINT64_MAX ? 0 : entry->min_ns,
                entry->max_ns,
                bucket_buf)));
    }

    ereport(LOG, (errmsg("VEC_ENGINE_PROFILE_END tid=%lu", t_thrd.proc_cxt.MyProcPid)));

    errno_t ret = memset_s(vec_engine_profile_cxt.entries,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES,
        0,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES);
    securec_check(ret, "\0", "\0");
    vec_engine_profile_cxt.entry_num = 0;
    vec_engine_profile_cxt.dropped_entries = 0;
    vec_engine_profile_cxt.depth = 0;
    vec_engine_profile_cxt.query_id = 0;
}

void VecEngineProfileDiscard()
{
    if (!vec_engine_profile_cxt.inited || !vec_engine_profile_cxt.enabled) {
        return;
    }

    errno_t ret = memset_s(vec_engine_profile_cxt.entries,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES,
        0,
        sizeof(VecEngineProfileEntry) * VEC_ENGINE_PROFILE_MAX_ENTRIES);
    securec_check(ret, "\0", "\0");
    vec_engine_profile_cxt.entry_num = 0;
    vec_engine_profile_cxt.dropped_entries = 0;
    vec_engine_profile_cxt.depth = 0;
    vec_engine_profile_cxt.query_id = 0;
}
