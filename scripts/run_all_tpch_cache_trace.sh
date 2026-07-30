#!/bin/bash
set -uo pipefail

DB_NAME="${DB_NAME:-tpch}"
PORT="${PORT:-19999}"
PGDATA="${PGDATA:-/mnt/nvme1p1/yanyz/data}"
CONF_FILE="${PGDATA}/postgresql.conf"
SQL_DIR="${SQL_DIR:-/mnt/nvme1p1/yanyz/TPCH-openGauss-TestTools/SQL/queries}"
DOP="${DOP:-192}"
RUNS="${RUNS:-1}"
INTERVAL_US="${OG_CACHE_TRACE_INTERVAL_US:-10000}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/mnt/nvme1p1/yanyz/tpch_cache_trace_all_dop${DOP}_$(date +%Y%m%d_%H%M%S)}"
PG_LOG_DIR="${PGDATA}/pg_log"
BINARY_SPOOL="${PG_LOG_DIR}/cache_trace"
GAUSSDB_BIN="/home/yanyz/olap/openGauss-server/mppdb_temp_install/bin/gaussdb"

case "$DOP" in
    24)
        CPU_LIST=$(cat /sys/devices/system/node/node0/cpulist)
        NUMA_NODES="0"
        ;;
    48)
        CPU_LIST="0-47"
        NUMA_NODES="0,1"
        ;;
    192)
        CPU_LIST="0-191"
        NUMA_NODES="0,1,2,3,4,5,6,7"
        ;;
    *)
        echo "unsupported DOP: ${DOP}; configured values are 24, 48, and 192" >&2
        exit 1
        ;;
esac

STREAM_NUMA_NODES="${NUMA_NODES//,/, }"
STREAM_BINDING="thread_pool_stream_attr = '4096,0.2,2,(nodebind: ${STREAM_NUMA_NODES})'"

set_stream_binding() {
    if [ ! -f "$CONF_FILE" ]; then
        echo "missing postgresql.conf: ${CONF_FILE}" >&2
        exit 1
    fi
    sed -i "/^[[:space:]]*thread_pool_stream_attr[[:space:]]*=/c\\${STREAM_BINDING}" "$CONF_FILE"
}

mkdir -p "$OUTPUT_ROOT" "$BINARY_SPOOL"
exec > >(tee -a "${OUTPUT_ROOT}/run_all.log") 2>&1
SUMMARY_CSV="${OUTPUT_ROOT}/summary.csv"
printf 'query,run,dop,status,wall_seconds,total_runtime_ms,thread_begin,thread_end,binary_files,sql_log,trace_log,binary_dir\n' \
    >"$SUMMARY_CSV"

wait_for_database() {
    local attempt
    for ((attempt = 1; attempt <= 60; attempt++)); do
        if gsql -X -d "$DB_NAME" -p "$PORT" -c 'SELECT 1;' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

latest_server_log() {
    find "$PG_LOG_DIR" -maxdepth 1 -type f -name 'postgresql-*.log' -printf '%T@ %p\n' |
        sort -n | tail -1 | awk '{print $2}'
}

wait_for_server_log_quiet() {
    local mark_file=$1
    local previous_size=-1
    local stable_count=0
    local attempt
    local current_size

    for ((attempt = 1; attempt <= 120; attempt++)); do
        current_size=$(find "$PG_LOG_DIR" -maxdepth 1 -type f -name 'postgresql-*.log' \
            -newer "$mark_file" -printf '%s\n' | awk '{total += $1} END {print total + 0}')
        if [ "$current_size" -eq "$previous_size" ]; then
            stable_count=$((stable_count + 1))
            if [ "$stable_count" -ge 3 ]; then
                return 0
            fi
        else
            stable_count=0
            previous_size=$current_size
        fi
        sleep 1
    done
    return 1
}

collect_trace_log() {
    local initial_log=$1
    local initial_size=$2
    local mark_file=$3
    local output=$4
    local log_file

    : >"$output"
    if [ -f "$initial_log" ]; then
        tail -c "+$((initial_size + 1))" "$initial_log" |
            grep -E 'CACHE_TRACE_THREAD_(BEGIN|END)' >>"$output" || true
    fi

    while IFS= read -r log_file; do
        if [ "$log_file" != "$initial_log" ]; then
            grep -E 'CACHE_TRACE_THREAD_(BEGIN|END)' "$log_file" >>"$output" || true
        fi
    done < <(find "$PG_LOG_DIR" -maxdepth 1 -type f -name 'postgresql-*.log' \
        -newer "$mark_file" | sort)
}

{
    echo "started_at=$(date --iso-8601=seconds)"
    echo "database=${DB_NAME}"
    echo "port=${PORT}"
    echo "pgdata=${PGDATA}"
    echo "sql_dir=${SQL_DIR}"
    echo "dop=${DOP}"
    echo "runs=${RUNS}"
    echo "regular_interval_us=${INTERVAL_US}"
    echo "probe_interval_us=2000"
    echo "cpu_list=${CPU_LIST}"
    echo "numa_nodes=${NUMA_NODES}"
    echo "stream_binding=${STREAM_BINDING}"
    stat -c 'gaussdb_size=%s gaussdb_mtime=%y' "$GAUSSDB_BIN"
    sha256sum "$GAUSSDB_BIN"
} >"${OUTPUT_ROOT}/metadata.txt"

echo "output=${OUTPUT_ROOT}"
set_stream_binding
echo "configured ${STREAM_BINDING}"
echo "restarting openGauss with cache trace enabled"
OG_CACHE_TRACE=1 OG_CACHE_TRACE_INTERVAL_US="$INTERVAL_US" \
    numactl --physcpubind="$CPU_LIST" --membind="$NUMA_NODES" \
    gs_ctl restart -D "$PGDATA" -Z single_node 2>&1 | tee "${OUTPUT_ROOT}/restart.log"

if ! wait_for_database; then
    echo "database did not become ready on port ${PORT}" >&2
    exit 1
fi

failures=0
database_lost=0
for query in $(seq 1 22); do
    sql_file="${SQL_DIR}/q${query}.sql"
    if [ ! -f "$sql_file" ]; then
        echo "Q${query}: missing ${sql_file}" >&2
        failures=$((failures + 1))
        continue
    fi

    for ((run = 1; run <= RUNS; run++)); do
        run_dir=$(printf '%s/q%02d/run_%02d' "$OUTPUT_ROOT" "$query" "$run")
        sql_log="${run_dir}/query.log"
        trace_log="${run_dir}/cache_trace.log"
        binary_dir="${run_dir}/binary"
        mkdir -p "$binary_dir"

        server_log=$(latest_server_log)
        if [ -z "$server_log" ]; then
            echo "Q${query} run ${run}: no server log found" >&2
            failures=$((failures + 1))
            continue
        fi
        server_log_start=$(wc -c <"$server_log")
        mark_file=$(mktemp "${OUTPUT_ROOT}/trace_mark.q${query}.r${run}.XXXXXX")

        start_time=$(date +%s.%N)
        if {
            echo "SET query_dop = ${DOP};"
            echo "EXPLAIN PERFORMANCE"
            cat "$sql_file"
        } | gsql -X -v ON_ERROR_STOP=1 -d "$DB_NAME" -p "$PORT" >"$sql_log" 2>&1; then
            status=0
        else
            status=$?
            failures=$((failures + 1))
        fi
        end_time=$(date +%s.%N)
        wall_seconds=$(awk -v start="$start_time" -v end="$end_time" \
            'BEGIN {printf "%.6f", end - start}')
        total_runtime_ms=$(awk '/Total runtime:/ {runtime = $(NF - 1)} END {print runtime + 0}' "$sql_log")

        if ! wait_for_server_log_quiet "$mark_file"; then
            echo "Q${query} run ${run}: server log did not become quiet" >&2
            failures=$((failures + 1))
        fi
        collect_trace_log "$server_log" "$server_log_start" "$mark_file" "$trace_log"

        find "$BINARY_SPOOL" -maxdepth 1 -type f -name 'cache_trace_*.bin' \
            -newer "$mark_file" -exec mv -t "$binary_dir" {} +
        rm -f "$mark_file"

        begin_count=$(grep -c 'CACHE_TRACE_THREAD_BEGIN' "$trace_log" || true)
        end_count=$(grep -c 'CACHE_TRACE_THREAD_END' "$trace_log" || true)
        binary_count=$(find "$binary_dir" -maxdepth 1 -type f -name 'cache_trace_*.bin' | wc -l)

        printf '%d,%d,%d,%d,%s,%.3f,%d,%d,%d,%s,%s,%s\n' \
            "$query" "$run" "$DOP" "$status" "$wall_seconds" "$total_runtime_ms" \
            "$begin_count" "$end_count" "$binary_count" \
            "$sql_log" "$trace_log" "$binary_dir" >>"$SUMMARY_CSV"

        echo "Q${query} run=${run}/${RUNS} status=${status} wall=${wall_seconds}s runtime=${total_runtime_ms}ms threads=${begin_count} binaries=${binary_count}"
        if [ "$total_runtime_ms" = "0" ] || [ "$begin_count" -eq 0 ] || [ "$begin_count" -ne "$end_count" ] || \
            [ "$begin_count" -ne "$binary_count" ]; then
            echo "Q${query} run ${run}: trace completeness mismatch" >&2
            failures=$((failures + 1))
        fi

        if [ "$status" -ne 0 ] && ! gsql -X -d "$DB_NAME" -p "$PORT" -c 'SELECT 1;' >/dev/null 2>&1; then
            database_lost=1
            echo "Q${query} run ${run}: database connection lost; aborting remaining runs" >&2
            break 2
        fi
    done
done

echo "finished_at=$(date --iso-8601=seconds)" | tee "${OUTPUT_ROOT}/finished.txt"
echo "database_lost=${database_lost}" | tee -a "${OUTPUT_ROOT}/finished.txt"
echo "summary=${SUMMARY_CSV}"
echo "failures=${failures}"
if [ "$failures" -ne 0 ]; then
    exit 1
fi
exit 0
