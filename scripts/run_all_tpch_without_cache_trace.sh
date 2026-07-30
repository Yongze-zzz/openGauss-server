#!/bin/bash
set -uo pipefail

DB_NAME="${DB_NAME:-tpch}"
PORT="${PORT:-19999}"
PGDATA="${PGDATA:-/mnt/nvme1p1/yanyz/data}"
CONF_FILE="${PGDATA}/postgresql.conf"
SQL_DIR="${SQL_DIR:-/mnt/nvme1p1/yanyz/TPCH-openGauss-TestTools/SQL/queries}"
DOP="${DOP:-192}"
RUNS="${RUNS:-5}"
RUN_GAP_SECONDS="${RUN_GAP_SECONDS:-3}"
START_QUERY="${START_QUERY:-1}"
END_QUERY="${END_QUERY:-22}"
START_RUN="${START_RUN:-1}"
END_RUN="${END_RUN:-$RUNS}"
APPEND_RESULTS="${APPEND_RESULTS:-0}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/mnt/nvme1p1/yanyz/tpch_cache_wo_trace_all_dop${DOP}_$(date +%Y%m%d_%H%M%S)}"
GAUSSDB_BIN="/home/yanyz/olap/openGauss-server/mppdb_temp_install/bin/gaussdb"

case "$DOP" in
    48)
        CPU_LIST="0-47"
        NUMA_NODES="0,1"
        ;;
    192)
        CPU_LIST="0-191"
        NUMA_NODES="0,1,2,3,4,5,6,7"
        ;;
    *)
        echo "unsupported DOP: ${DOP}; configured values are 48 and 192" >&2
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

mkdir -p "$OUTPUT_ROOT"
exec > >(tee -a "${OUTPUT_ROOT}/run_all.log") 2>&1
SUMMARY_CSV="${OUTPUT_ROOT}/summary.csv"
if [ "$APPEND_RESULTS" -ne 1 ] || [ ! -f "$SUMMARY_CSV" ]; then
    printf 'query,run,dop,status,wall_seconds,total_runtime_ms,sql_log\n' >"$SUMMARY_CSV"
fi

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

write_metadata() {
    echo "started_at=$(date --iso-8601=seconds)"
    echo "mode=cache_trace_disabled"
    echo "database=${DB_NAME}"
    echo "port=${PORT}"
    echo "pgdata=${PGDATA}"
    echo "sql_dir=${SQL_DIR}"
    echo "dop=${DOP}"
    echo "runs=${RUNS}"
    echo "run_gap_seconds=${RUN_GAP_SECONDS}"
    echo "query_range=${START_QUERY}-${END_QUERY}"
    echo "run_range=${START_RUN}-${END_RUN}"
    echo "cpu_list=${CPU_LIST}"
    echo "numa_nodes=${NUMA_NODES}"
    echo "stream_binding=${STREAM_BINDING}"
    stat -c 'gaussdb_size=%s gaussdb_mtime=%y' "$GAUSSDB_BIN"
    sha256sum "$GAUSSDB_BIN"
}
if [ "$APPEND_RESULTS" -eq 1 ] && [ -f "${OUTPUT_ROOT}/metadata.txt" ]; then
    echo "resumed_at=$(date --iso-8601=seconds) query_range=${START_QUERY}-${END_QUERY} run_range=${START_RUN}-${END_RUN}" \
        >>"${OUTPUT_ROOT}/metadata.txt"
else
    write_metadata >"${OUTPUT_ROOT}/metadata.txt"
fi

echo "output=${OUTPUT_ROOT}"
set_stream_binding
echo "configured ${STREAM_BINDING}"
echo "restarting openGauss with cache trace disabled"
env -u OG_CACHE_TRACE -u OG_CACHE_TRACE_INTERVAL_US -u OG_CACHE_TRACE_SAMPLE_EVERY \
    numactl --physcpubind="$CPU_LIST" --membind="$NUMA_NODES" \
    gs_ctl restart -D "$PGDATA" -Z single_node 2>&1 | tee -a "${OUTPUT_ROOT}/restart.log"

if ! wait_for_database; then
    echo "database did not become ready on port ${PORT}" >&2
    exit 1
fi

failures=0
database_lost=0
for query in $(seq "$START_QUERY" "$END_QUERY"); do
    sql_file="${SQL_DIR}/q${query}.sql"
    if [ ! -f "$sql_file" ]; then
        echo "Q${query}: missing ${sql_file}" >&2
        failures=$((failures + 1))
        continue
    fi

    for ((run = START_RUN; run <= END_RUN; run++)); do
        run_dir=$(printf '%s/q%02d/run_%02d' "$OUTPUT_ROOT" "$query" "$run")
        sql_log="${run_dir}/query.log"
        mkdir -p "$run_dir"

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

        printf '%d,%d,%d,%d,%s,%.3f,%s\n' \
            "$query" "$run" "$DOP" "$status" "$wall_seconds" \
            "$total_runtime_ms" "$sql_log" >>"$SUMMARY_CSV"
        echo "Q${query} run=${run}/${END_RUN} status=${status} wall=${wall_seconds}s runtime=${total_runtime_ms}ms"

        if [ "$status" -ne 0 ] && ! gsql -X -d "$DB_NAME" -p "$PORT" -c 'SELECT 1;' >/dev/null 2>&1; then
            database_lost=1
            echo "Q${query} run ${run}: database connection lost; aborting remaining runs" >&2
            break 2
        fi
        sleep "$RUN_GAP_SECONDS"
    done
done

echo "finished_at=$(date --iso-8601=seconds)" | tee -a "${OUTPUT_ROOT}/finished.txt"
echo "database_lost=${database_lost}" | tee -a "${OUTPUT_ROOT}/finished.txt"
echo "summary=${SUMMARY_CSV}"
echo "failures=${failures}"
if [ "$failures" -ne 0 ]; then
    exit 1
fi
