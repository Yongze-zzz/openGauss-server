#!/usr/bin/env bash
# Run against a populated 8-NUMA NPS instance, using the same SQL and optimizer hints in both modes.
set -euo pipefail
cd "$(dirname "$0")/.."
export GAUSSHOME=${INSTALL_DIR:-$PWD/mppdb_temp_install}
libs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc=${GCC_PATH:-$libs/buildtools/gcc10.3}
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$gcc/gcc/lib64:$libs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
export SCHEMA=${SCHEMA:-tpch_sf100_imcs_h192}
source <(bash scripts/imcs_numa_env.sh 8 168)
pgdata=${PGDATA:-/mnt/nvme1p1/gauss}
if [[ -n ${OUTPUT_DIR:-} ]]; then
    out=$OUTPUT_DIR
else
    mkdir -p "$PWD/results/imcs_optimization"
    out=$(mktemp -d "$PWD/results/imcs_optimization/8numa_shared_sonic_agg_XXXXXXXX")
fi
mkdir -p "$out"
out=$(realpath "$out")
printf 'Output %s\n' "$out"
archive=${BASE_SQL_DIR:-$PWD/results/imcs_optimization/8numa_modes_22_retest_dIyY54lG/06_nps_buffer_shared_rhj}
scratch=$(mktemp -d /tmp/shared-sonic-agg.XXXXXXXX)
trap 'rm -rf -- "$scratch"' EXIT

exec 9>"$pgdata/.imcs_experiment.lock"
flock -n 9
groups=$(bash prepare_tpch_nps.sh groups)
client=("$GAUSSHOME/bin/gsql" -X -qAt -v ON_ERROR_STOP=1 -p "${PORT:-19999}"
    -d "${DB_NAME:-tpch}" -F $'\x1f' -P 'null=<SQL NULL>' -v "schema=$SCHEMA" -v "cpus=$CPU_POOL"
    -v "query_groups=$groups" -v work_mem=100GB -v "stage_mixed=${STAGE_MIXED_CPUS:-}"
    -v "stage_split=${STAGE_SPLIT_CPUS:-}" -v "stage_odd=${STAGE_ODD_CPUS:-}")
pid=$(head -1 "$pgdata/postmaster.pid")
{
    date -Iseconds
    awk '/Cpus_allowed_list:|Mems_allowed_list:/' "/proc/$pid/status"
    numactl -H
    sha256sum "$GAUSSHOME/bin/gaussdb"
    printf 'SQL source %s\n' "$archive"
} > "$out/environment.txt"

make_sql() {
    local query=$1 mode=$2 kind=$3 input=$archive/nps_q${1}_run1.sql
    local shared_agg=$mode tile_policy= exchange_mode= stage_variable= radix_mb=
    case $mode in
        original)
            shared_agg=on; tile_policy=off
            if [[ ${PHASE:-} == *exchange* ]]; then exchange_mode=off; fi ;;
        fixed) shared_agg=on; tile_policy=fixed_budget ;;
        coordinated) shared_agg=on; tile_policy=aggregate_aware ;;
        owner) shared_agg=on; tile_policy=off; exchange_mode=owner_handoff ;;
        selective) shared_agg=on; tile_policy=off; exchange_mode=selective_build ;;
        fused) shared_agg=on; tile_policy=off; exchange_mode=in_lane_aggregate ;;
        fixed_fused) shared_agg=on; tile_policy=fixed_budget; exchange_mode=in_lane_aggregate ;;
        coordinated_fused) shared_agg=on; tile_policy=aggregate_aware; exchange_mode=in_lane_aggregate ;;
        feedback_fused) shared_agg=on; tile_policy=peer_aware; exchange_mode=in_lane_aggregate ;;
        reuse_fused) shared_agg=on; tile_policy=fixed_budget_reuse; exchange_mode=in_lane_aggregate ;;
        adaptive_reuse_fused) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=in_lane_aggregate ;;
        wide_fused) shared_agg=on; tile_policy=off; exchange_mode=in_lane_aggregate; radix_mb=12 ;;
        adaptive_wide_fused) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=in_lane_aggregate; radix_mb=12 ;;
        pipeline_fused) shared_agg=on; tile_policy=off; exchange_mode=pipeline_colocation ;;
        pipeline_adaptive) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=pipeline_colocation; radix_mb=12 ;;
        pipeline_sender_spread) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=broadcast_isolation; radix_mb=12; stage_variable=stage_mixed ;;
        pipeline_sender_isolated) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=broadcast_isolation; radix_mb=12; stage_variable=stage_split ;;
        pipeline_sender_odd) shared_agg=on; tile_policy=adaptive_budget_reuse; exchange_mode=broadcast_isolation; radix_mb=12; stage_variable=stage_odd ;;
        agg_window_fused) shared_agg=on; tile_policy=off; exchange_mode=in_lane_aggregate ;;
        stage_mixed) shared_agg=on; tile_policy=off; exchange_mode=in_lane_aggregate; stage_variable=stage_mixed ;;
        stage_split) shared_agg=on; tile_policy=off; exchange_mode=in_lane_aggregate; stage_variable=stage_split ;;
    esac
    sed '/^SHOW /d; /^EXPLAIN PERFORMANCE/,$d; s/enable_imcs_stream_buffer_locality/enable_imcs_stream_hotspot_split/g' "$input"
    # Suppress the existing per-worker RHJ logs during paired timing.
    cat <<SQL
SET statement_timeout = 300000;
SET log_min_messages = 'fatal';
SET enable_sonic_hashagg = on;
SET enable_imcs_nps_cu_balance = on;
SET enable_imcs_stream_batch_ring = on;
SET imcs_stream_batch_ring_extra_slots = 16;
SET enable_imcs_cache_coord_llc_pressure = off;
SET enable_radix_shared_probe_locality = off;
SET enable_imcs_cache_harmful = off;
SET enable_imcs_cache_harmful_defer = off;
SET enable_imcs_agg_cache_chiplet = off;
SET enable_imcs_agg_cache_live_pressure = off;
SET enable_imcs_agg_cache_critical = off;
SET imcs_rhj_overlap_victim_node = 0;
SET imcs_rhj_overlap_peer_node = 0;
SET enable_imcs_shared_sonic_agg_stats = off;
SET enable_imcs_shared_sonic_agg = $shared_agg;
SQL
    if [[ -n $tile_policy ]]; then
        printf "SET imcs_chiplet_probe_tile_policy = '%s';\n" "$tile_policy"
    fi
    if [[ -n $exchange_mode ]]; then
        printf "SET imcs_chiplet_exchange_mode = '%s';\n" "$exchange_mode"
    fi
    if [[ -n $stage_variable ]]; then
        printf "SET imcs_chiplet_stage_cpus = :'%s';\n" "$stage_variable"
    fi
    if [[ -n $radix_mb ]]; then
        printf 'SET radix_partition_size_mb = %s;\n' "$radix_mb"
    fi
    if [[ $mode == agg_window_fused ]]; then
        printf '%s\n' 'SET imcs_agg_cache_slots = 12;' 'SET enable_imcs_agg_cache_window = on;'
    fi
    case $kind in
        settings)
            cat <<'SQL'
SELECT name || '|' || setting || COALESCE(unit, '') FROM pg_settings WHERE
    name IN ('thread_pool_attr','thread_pool_stream_attr','query_dop','work_mem','enable_sonic_hashagg',
    'enable_sonic_hashjoin','enable_radix_hashjoin','radix_shared_build','enable_imcs_nps',
    'enable_imcs_stream_hotspot_split','enable_imcs_nps_cu_balance','enable_imcs_stream_batch_ring',
    'imcs_stream_batch_ring_extra_slots','enable_imcs_shared_sonic_agg',
    'enable_imcs_shared_sonic_agg_stats','enable_cache_trace','enable_intra_operator_data_access',
    'enable_inter_operator_data_access','enable_imcs_cache_coord','enable_imcs_adapt','numa_distribute_mode',
    'radix_partition_size_mb','imcs_agg_cache_slots','enable_imcs_agg_cache_window',
    'log_min_messages','imcs_chiplet_probe_tile_policy','imcs_chiplet_exchange_mode','imcs_chiplet_stage_cpus')
    ORDER BY name;
SQL
            return ;;
        profile)
            printf '%s\n' 'SET enable_imcs_shared_sonic_agg_stats = on;' "SET log_min_messages = 'log';"
            if [[ ${PROFILE_CACHE:-0} == 1 ]]; then
                printf '%s\n' 'SET enable_cache_trace = on;' 'SET cache_trace_interval_us = 10000;'
            fi
            printf '%s\n' 'EXPLAIN PERFORMANCE' ;;
        tile_profile)
            printf '%s\n' "SET log_min_messages = 'log';" 'EXPLAIN PERFORMANCE' ;;
        plan)
            printf '%s\n' 'EXPLAIN (VERBOSE, COSTS OFF)' ;;
        *)
            printf '%s\n' '\timing on' ;;
    esac
    sed -n '/^EXPLAIN PERFORMANCE/,$ { /^EXPLAIN PERFORMANCE/d; p; }' "$input"
}

run_sql() {
    local q=$1 mode=$2 stage=$3 run=$4
    local sampler= rc=0
    make_sql "$q" "$mode" "$stage" > "$scratch/query.sql"
    touch "$scratch/before"
    if [[ $stage == profile && ${PROFILE_RSS:-0} == 1 ]]; then
        (
            printf 'epoch_ms,rss_kb\n'
            while [[ -r /proc/$pid/status ]]; do
                awk -v now="$(date +%s%3N)" '$1 == "VmRSS:" {print now "," $2}' "/proc/$pid/status"
                sleep 0.1
            done
        ) > "$out/q${q}_${mode}_rss.csv" &
        sampler=$!
    fi
    LC_ALL=C "${client[@]}" -f "$scratch/query.sql" > "$scratch/stdout" 2> "$scratch/stderr" || rc=$?
    if [[ -n $sampler ]]; then
        kill "$sampler" 2>/dev/null || true
        wait "$sampler" 2>/dev/null || true
    fi
    if ((rc != 0)); then
        cp "$scratch/query.sql" "$scratch/stdout" "$scratch/stderr" "$out/"
        cat "$scratch/stderr" >&2
        return 1
    fi
    if [[ $stage == plan || $stage == settings || $stage == profile || $stage == tile_profile ]]; then
        cp "$scratch/stdout" "$out/q${q}_${mode}_${stage}.log"
        if [[ $stage == profile || $stage == tile_profile ]]; then
            id=$(awk '/Query Id:/ { print $3; exit }' "$scratch/stdout")
            if [[ -z $id ]]; then
                echo "Q$q $mode has no EXPLAIN Query Id" >&2
                return 1
            fi
            printf '%s,%s,%s\n' "$q" "$mode" "$id" | tee -a "$out/profiles.csv"
            if [[ $stage == tile_profile ]]; then
                find "$pgdata/pg_log" -maxdepth 1 -type f -name 'postgresql*.log' -newer "$scratch/before" -print0 |
                    xargs -0 -r rg --no-heading --no-filename " $id .* (chiplet_probe_tile|chiplet_owner_handoff|chiplet_stage_placement|agg_cache_coord|RHJ phases) node=" |
                    sed -E 's/^.*(chiplet_probe_tile|chiplet_owner_handoff|chiplet_stage_placement|agg_cache_coord|RHJ phases) node=/\1 node=/' \
                    > "$out/q${q}_${mode}_tiles.log" || true
            else
                find "$pgdata/pg_log" -maxdepth 1 -type f -name 'postgresql*.log' -newer "$scratch/before" -print0 |
                    xargs -0 -r rg --no-heading --no-filename "sonic_(shared|private)_agg[^ ]* query_id=$id " |
                    sed -E 's/^.*(sonic_(shared|private)_agg)/\1/' > "$out/q${q}_${mode}_workers.log" || true
            fi
        fi
        return
    fi

    local timed digest rows
    timed=$(awk '/^Time: [0-9.]+ ms$/ {time=$2} END {if (time) print time; else exit 1}' "$scratch/stdout")
    sed -E '/^Time: [0-9.]+ ms$/d; /^total time: [0-9]+ +ms$/d' "$scratch/stdout" |
        LC_ALL=C sort > "$scratch/result"
    rows=$(wc -l < "$scratch/result")
    digest=$(sha256sum "$scratch/result" | cut -d' ' -f1)
    if [[ ! -f $out/q${q}.sha256 ]]; then
        printf '%s\n' "$digest" > "$out/q${q}.sha256"
        cp "$scratch/result" "$scratch/q${q}_expected"
    elif [[ $digest != "$(< "$out/q${q}.sha256")" ]]; then
        cp "$scratch/result" "$scratch/stdout" "$scratch/query.sql" "$out/"
        if [[ -f $scratch/q${q}_expected ]]; then
            cp "$scratch/q${q}_expected" "$out/"
        fi
        echo "Q$q $mode $stage $run returned a different result multiset" >&2
        return 1
    fi

    printf '%s,%s,%s,%s,%s,%s,%s\n' "$q" "$mode" "$stage" "$run" "$timed" "$rows" "$digest" |
        tee -a "$out/timing.csv"
}

pairs() {
    local q=$1 stage=$2 reps=$3 pair
    for ((pair=1;pair<=reps;++pair)); do
        if ((pair % 2 == 1)); then
            run_sql "$q" off "$stage" "$pair"
            run_sql "$q" on "$stage" "$pair"
        else
            run_sql "$q" on "$stage" "$pair"
            run_sql "$q" off "$stage" "$pair"
        fi
    done
}

[[ -f $out/timing.csv ]] || printf 'query,mode,phase,pair,ms,rows,sha256\n' > "$out/timing.csv"
[[ -f $out/profiles.csv ]] || printf 'query,mode,query_id\n' > "$out/profiles.csv"
case ${PHASE:-all} in
    exchange_plan)
        for q in ${QUERIES:-18}; do
            for mode in ${EXCHANGE_MODES:-original owner selective}; do run_sql "$q" "$mode" plan 1; done
        done ;;
    exchange_profile)
        for q in ${QUERIES:-18}; do
            for mode in ${EXCHANGE_MODES:-original owner selective}; do run_sql "$q" "$mode" tile_profile 1; done
        done ;;
    tile_profile)
        for q in ${QUERIES:-5 16 8}; do
            for mode in original fixed coordinated; do run_sql "$q" "$mode" tile_profile 1; done
        done ;;
    chiplet_tiles|chiplet_exchange)
        warmups=${WARMUPS:-2}
        runs=${RUNS:-6}
        [[ $warmups =~ ^(0|[1-9][0-9]*)$ && $runs =~ ^[1-9][0-9]*$ ]] || exit 2
        compare_modes=(original fixed coordinated)
        queries=${QUERIES:-5 16 8}
        prefix=tile
        if [[ $PHASE == chiplet_exchange ]]; then
            read -r -a compare_modes <<< "${EXCHANGE_MODES:-original owner selective}"
            queries=${QUERIES:-18}
            prefix=exchange
        fi
        n_modes=${#compare_modes[@]}
        ((n_modes >= 2)) || exit 2
        for mode in "${compare_modes[@]}"; do run_sql "${queries%% *}" "$mode" settings 0; done
        for q in $queries; do
            for ((pair=1; pair<=warmups+runs; ++pair)); do
                if ((pair <= warmups)); then
                    stage=${prefix}_warmup
                    run=$pair
                else
                    stage=${prefix}_timing
                    run=$((pair - warmups))
                fi
                direction=$((1 - 2 * (((run - 1) / n_modes) % 2)))
                for ((offset=0; offset<n_modes; ++offset)); do
                    mode=${compare_modes[$(((run - 1 + direction * offset + n_modes) % n_modes))]}
                    run_sql "$q" "$mode" "$stage" "$run"
                done
            done
        done ;;
    check)
        for mode in off on; do run_sql 21 "$mode" settings 0; done
        for q in ${QUERIES:-18 21 1}; do
            pairs "$q" check 1
            for mode in off on; do run_sql "$q" "$mode" profile 1; done
        done ;;
    profile)
        for q in ${QUERIES:-18 21 1}; do
            for mode in ${PROFILE_MODES:-off on}; do run_sql "$q" "$mode" profile 1; done
        done ;;
    all|targets|regression)
        for mode in off on; do run_sql 21 "$mode" settings 0; done
        if [[ ${PHASE:-all} != regression ]]; then
            for q in ${TARGETS:-18 21 1}; do
                pairs "$q" warmup 2
                pairs "$q" target 6
            done
        fi
        if [[ ${PHASE:-all} != targets ]]; then
            for q in ${QUERIES:-$(seq 1 22)}; do
                pairs "$q" regression 3
            done
        fi ;;
    *) exit 2 ;;
esac
date -Iseconds > "$out/finished_${PHASE:-all}.txt"
