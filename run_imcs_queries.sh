#!/bin/bash
# Usage: ./run_imcs_queries.sh partition|numa|test
# partition: IMCS partition scan; numa: NUMA-aware IMCS partition scan.
# test: warm up both modes, then compare them with the same queries and DOP.
set -eo pipefail

source_dir=${SOURCE_DIR:-$(cd "$(dirname "$0")" && pwd -P)}
install_dir=${INSTALL_DIR:-$source_dir/mppdb_temp_install}
binarylibs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc_dir=${GCC_PATH:-$binarylibs/buildtools/gcc10.3}
sf=${SCALE_FACTOR:-100}
partitions=${PARTITIONS:-192}
schema=${SCHEMA:-tpch_sf${sf//./_}_imcs_h${partitions}}
dop=${DOP:-192}
runs=${RUNS:-3}
cpus=${IMCS_PARTITION_SCAN_CPUS:-}
sql_dir=${SQL_DIR:-$source_dir/SQL/queries}
output_dir=${OUTPUT_DIR:-$source_dir/imcs_partition_results/$(date +%Y%m%d_%H%M%S)}
read -r -a queries <<< "${QUERIES:-1 6}"
sql=("${GSQL_BIN:-$install_dir/bin/gsql}" -X -v ON_ERROR_STOP=1 -P pager=off
     -d "${DB_NAME:-tpch}" -p "${PORT:-${PGPORT:-19999}}")

export GAUSSHOME="$install_dir"
export LD_LIBRARY_PATH="$install_dir/lib:$gcc_dir/gcc/lib64:$binarylibs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"

run_query() {
    local mode=$1 query=$2 run=$3
    local numa=off log="$output_dir/$mode/q${query}_${run}.log" elapsed
    if [[ $mode == numa ]]; then
        numa=on
    fi
    echo "$mode Q$query $run"

    {
        echo 'SET search_path TO :"schema";'
        echo 'SET statement_timeout = 0;'
        echo 'SET client_min_messages = warning;'
        echo 'SET enable_vector_engine = on;'
        echo 'SET enable_codegen = on;'
        echo 'SHOW enable_codegen;'
        echo 'SHOW codegen_cost_threshold;'
        echo 'SET enable_imcsscan = on;'
        echo 'SET enable_cstore_partition_lane_scan = off;'
        echo 'SET enable_intra_operator_data_access = off;'
        echo 'SET enable_inter_operator_data_access = off;'
        echo "SET query_dop = $dop;"
        echo 'SET enable_imcs_partition_scan = on;'
        echo "SET enable_numa_aware_partition_scan = $numa;"
        if [[ $numa == on ]]; then
            echo "SET imcs_partition_scan_cpus = :'scan_cpus';"
        fi

        # Save an EXPLAIN PERFORMANCE warm-up with LLVM details, then time the measured SELECT.
        if [[ $run == plan ]]; then
            echo 'EXPLAIN PERFORMANCE'
        else
            echo '\timing on'
        fi
        cat "$sql_dir/q$query.sql"
    } | LC_ALL=C "${sql[@]}" -v "schema=$schema" -v "scan_cpus=$cpus" \
        2>&1 | tee "$log" || return

    if [[ $run != plan ]]; then
        elapsed=$(awk '/^Time: [0-9.]+ ms$/ {time_ms=$2} END {print time_ms}' "$log")
        if [[ -z $elapsed ]]; then
            echo "No query timing found in $log" >&2
            return 1
        fi
        printf '%s,%s,%s,%s,%s\n' "$mode" "$query" "$run" "$elapsed" "$log" \
            >> "$output_dir/summary.csv"
    fi
}

run_tests() {
    local mode query run
    [[ $dop =~ ^[1-9][0-9]*$ && $dop -gt 1 ]] ||
        { echo 'DOP must be an integer greater than 1.' >&2; return 1; }
    [[ $runs =~ ^[1-9][0-9]*$ ]] ||
        { echo 'RUNS must be a positive integer.' >&2; return 1; }
    for query in "${queries[@]}"; do
        [[ -r $sql_dir/q$query.sql ]] ||
            { echo "Query file not found: $sql_dir/q$query.sql" >&2; return 1; }
    done
    for mode in "$@"; do
        if [[ $mode == numa && -z $cpus ]]; then
            echo 'Set IMCS_PARTITION_SCAN_CPUS to comma-separated CPU IDs for NUMA tests.' >&2
            return 1
        fi
        mkdir -p "$output_dir/$mode" || return
    done

    printf 'mode,query,run,elapsed_ms,log\n' > "$output_dir/summary.csv" || return
    echo "Schema: $schema; DOP: $dop; runs: $runs; output: $output_dir"
    for query in "${queries[@]}"; do
        for mode in "$@"; do
            run_query "$mode" "$query" plan || return
        done
        # Alternate the two modes after warming both on the same data.
        for ((run = 1; run <= runs; run++)); do
            for mode in "$@"; do
                run_query "$mode" "$query" "$run" || return
            done
        done
    done
    echo "Results: $output_dir/summary.csv"
    echo 'Check the plan logs for IMCS scans, partition iterators and the actual scan DOP.'
}

case "${1:---help}" in
    partition) run_tests partition ;;
    numa) run_tests numa ;;
    test) run_tests partition numa ;;
    -h|--help)
        echo "Usage: $0 partition|numa|test"
        echo 'Defaults: SCALE_FACTOR=100 PARTITIONS=192 DOP=192 RUNS=3 QUERIES="1 6"'
        echo 'Reuse SCHEMA, DB_NAME, PORT/PGPORT and GSQL_BIN from prepare_tpch_imcs.sh.'
        echo 'For NUMA, populate with IMCS_PARTITION_NUMA_MAP and set IMCS_PARTITION_SCAN_CPUS.'
        echo 'Query files: SQL_DIR/qN.sql; logs and timings: OUTPUT_DIR.'
        ;;
    *) echo "Unknown mode: $1" >&2; exit 1 ;;
esac
