#!/usr/bin/env bash
# Intra/inter diagnostics on an already running matching instance.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
case ${1:---help} in
    -h|--help)
        cat <<'HELP'
Usage: bash run_numa_access.sh 4|8 [--env|--sql-only]
4: 96-core pool / DOP96; 8: 192-core pool / DOP168.
Default: Q1-Q22, one EXPLAIN PERFORMANCE each, no warmups, WORK_MEM=100GB.
Both intra/inter logical-memory-access statistics ON; Sonic ON, RHJ/cache trace OFF.
ACCESS_MODE=baseline (default), nps, or nps_adapt selects placement and Adapt.
Adapt defaults: period20ms, migration ON, local task queue OFF.
Requires matching running server and exclusive experiment access. Does not
build/install/restart/reconfigure the instance or recreate/import row tables.
CPU and memory nodes must both match the selected topology. A 4-NUMA server
requires launcher membind preserved by the server throughout execution.
RESET_LAYOUT=repopulate (default) explicitly permits restoring the selected
IMCS placement, preserving row tables. Use manual to refuse placement resets.
Outputs: per-query generated SQL, performance log, before/after diagnostics,
raw access log, CSV/JSON/heatmaps under analysis/, and batch_summary.csv.
Overrides: QUERIES, RUNS, WORK_MEM, SCHEMA, OUTPUT_DIR (new), RESET_LAYOUT,
ACCESS_MODE, ADAPT_PERIOD_MS, ADAPT_MIGRATION, COMPRESS_ACCESS=1|0 (default1),
QUERY_TIMEOUT_MS (default1800000), EXTRACT_TIMEOUT (default300), ANALYZE=1|0,
INSTALL_DIR/GSQL_BIN/PORT/DB_NAME/PGHOST/PGUSER/PYTHON_BIN.
--sql-only writes SQL without contacting the database. --env prints the startup environment.
Diagnostic timings must not be pooled with statistics-OFF performance results.
HELP
        exit 0 ;;
    4) count=4; dop=96 ;;
    8) count=8; dop=168 ;;
    *) echo 'Expected 4 or 8; use --help.' >&2; exit 2 ;;
esac
[[ $# -le 2 ]] || { echo 'Use 4|8 [--env|--sql-only].' >&2; exit 2; }
sql_only=${SQL_ONLY:-${DRY_RUN:-0}}
case ${2:-} in
    '') ;;
    --env) ;;
    --sql-only) sql_only=1 ;;
    *) echo 'Use 4|8 [--env|--sql-only].' >&2; exit 2 ;;
esac
export SQL_ONLY="$sql_only"
[[ $SQL_ONLY =~ ^[01]$ ]] || { echo 'SQL_ONLY must be 0 or 1.' >&2; exit 2; }
topology=$(env -u CPU_POOL -u IMCS_PARTITION_SCAN_CPUS -u NUMA_NODES bash "$root/scripts/imcs_numa_env.sh" "$count" "$dop")
if [[ ${2:-} == --env ]]; then printf '%s\n' "$topology"; exit 0; fi
eval "$topology"
export SCHEMA=${SCHEMA:-tpch_sf100_imcs_h192} WORK_MEM=${WORK_MEM:-100GB}
export RESET_LAYOUT=${RESET_LAYOUT:-repopulate}
access_mode=${ACCESS_MODE:-baseline}
[[ $access_mode =~ ^(baseline|nps|nps_adapt)$ ]] || { echo 'Invalid ACCESS_MODE.' >&2; exit 2; }
export ADAPT_PERIOD_MS=${ADAPT_PERIOD_MS:-20}
export ADAPT_MIGRATION=${ADAPT_MIGRATION:-on} ADAPT_LOCAL_TASKS=off INTRA_STATS=on INTER_STATS=on
export RHJ_MODE=sonic CACHE_TRACE=off
export DOP=$dop WARMUPS=0 QUERY_TIMEOUT_MS=${QUERY_TIMEOUT_MS:-1800000}
export INSTALL_DIR=${INSTALL_DIR:-$root/mppdb_temp_install}
libs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc=${GCC_PATH:-$libs/buildtools/gcc10.3}
export GAUSSHOME="$INSTALL_DIR"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$gcc/gcc/lib64:$libs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
python=${PYTHON_BIN:-python3}
read -r -a queries <<< "${QUERIES:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22}"
runs=${RUNS:-1}; analyze=${ANALYZE:-1}; compress=${COMPRESS_ACCESS:-1}
[[ $compress =~ ^[01]$ ]] || { echo 'Invalid COMPRESS_ACCESS.' >&2; exit 2; }
[[ $runs =~ ^[1-9][0-9]*$ && $analyze =~ ^[01]$ ]] || { echo 'Invalid RUNS or ANALYZE.' >&2; exit 2; }
for q in "${queries[@]}"; do
    [[ $q =~ ^([1-9]|1[0-9]|2[0-2])$ && -r ${SQL_DIR:-$root/SQL/queries}/q$q.sql ]] || { echo "Invalid query Q$q." >&2; exit 2; }
done
resuming=0
if [[ -n ${RESUME_DIR:-} ]]; then
    batch=$(cd "$RESUME_DIR" && pwd -P); resuming=1
    [[ -s $batch/metadata.txt && -f $batch/batch_summary.csv ]] || { echo 'Missing batch metadata.' >&2;exit 2; }
    env -u LD_LIBRARY_PATH "$python" - "$batch/metadata.txt" "$count" "$dop" "$WORK_MEM" "$access_mode" <<'PY_RESUME'
import sys
from pathlib import Path
m=dict(l.split('=',1) for l in Path(sys.argv[1]).read_text().splitlines() if '=' in l)
assert m['numa']==sys.argv[2] and m['dop']==sys.argv[3] and m['work_mem']==sys.argv[4], 'Resume configuration mismatch'
assert m.get('mode','baseline')==sys.argv[5], 'Resume policy mismatch'
if sys.argv[2]=='4' and m.get('strict_memory_nodes')!='on':
    raise SystemExit('Earlier 4NUMA batch lacks strict memory validation; start a new batch')
PY_RESUME
    sha256sum -c "$batch/query_sha256.txt" > /dev/null
elif [[ -n ${OUTPUT_DIR:-} ]]; then
    [[ ! -e $OUTPUT_DIR ]] || { echo 'OUTPUT_DIR must be a new path.' >&2; exit 2; }
    mkdir -p "$OUTPUT_DIR"; batch=$(cd "$OUTPUT_DIR" && pwd -P)
else
    mkdir -p "$root/results/baseline_numa_access"
    batch=$(mktemp -d "$root/results/baseline_numa_access/${count}numa_dop${dop}_$(date +%Y%m%d_%H%M%S)_XXXXXX")
fi
# OUTPUT_DIR belongs to the per-query runner below, not this parent batch.
unset OUTPUT_DIR
exec > >(tee -a "$batch/run.log") 2>&1
printf 'Batch: %s\n%s; %s NUMA; DOP=%s; intra/inter ON; work_mem=%s\n' "$batch" "$access_mode" "$count" "$dop" "$WORK_MEM"
[[ $resuming == 1 ]] || printf 'query,run,query_id,total_runtime_ms,artifact_dir\n' > "$batch/batch_summary.csv"
[[ $resuming == 1 ]] || printf 'numa=%s\ndop=%s\nqueries=%s\nruns=%s\nwork_mem=%s\nintra=on\ninter=on\nmode=%s\nrhj_mode=sonic\ncache_trace=off\nadapt_period_ms=%s\nadapt_migration=%s\nadapt_local_tasks=off\n' "$count" "$dop" "${queries[*]}" "$runs" "$WORK_MEM" "$access_mode" "$ADAPT_PERIOD_MS" "$ADAPT_MIGRATION" > "$batch/metadata.txt"
if [[ $SQL_ONLY != 1 ]]; then
    sql=("${GSQL_BIN:-$INSTALL_DIR/bin/gsql}" -X -q -At -v ON_ERROR_STOP=1 -p "${PORT:-${PGPORT:-19999}}" -d "${DB_NAME:-tpch}")
    collector=$("${sql[@]}" -c 'SHOW logging_collector;')
    log_name=$("${sql[@]}" -c 'SHOW log_filename;')
    [[ $collector == on && $log_name == postgresql-*.log ]] || {
        echo 'Requires logging_collector=on and log_filename matching postgresql-*.log for extraction.' >&2; exit 1;
    }
    data_dir=$("${sql[@]}" -c 'SHOW data_directory;')
    launcher_policy=$("${sql[@]}" -c 'SELECT pg_catalog.imcs_adapt_status(0::oid);')
    env -u LD_LIBRARY_PATH "$python" - "$NUMA_NODES" "$batch/memory_scope.json" "$launcher_policy" <<'PY_MEMORY'
import json,sys
from pathlib import Path
nodes=[int(n) for n in sys.argv[1].split(',')];wanted=sum(1<<n for n in nodes)
status=json.loads(sys.argv[3]);actual=status.get('launcher_membind_mask',0)
valid=actual==wanted
Path(sys.argv[2]).write_text(json.dumps({'requested_nodes':nodes,'launcher_membind_mask':actual,'valid':valid,'validation':'Server captures launcher MPOL_BIND before initialization and preserves it in pool/worker startup; every query also validates observed page nodes'},indent=2)+'\n')
if not valid:raise SystemExit('Launcher memory binding differs from selected topology, or server lacks preservation support; start the updated server with numactl --membind')
print('Verified preserved launcher MPOL_BIND nodes:',sys.argv[1])
PY_MEMORY
    [[ $resuming == 1 ]] || printf 'strict_memory_nodes=on\n' >> "$batch/metadata.txt"
    log_dir=$("${sql[@]}" -c 'SHOW log_directory;')
    [[ $log_dir == /* ]] || log_dir="$data_dir/$log_dir"
    [[ -d $log_dir && -r $log_dir ]] || { echo "Server log directory unreadable: $log_dir" >&2; exit 1; }
    printf '%s\n' "$log_dir" > "$batch/log_directory.txt"
    sha256sum "$INSTALL_DIR/bin/gaussdb" > "$batch/installed_gaussdb.sha256"
fi
for q in "${queries[@]}"; do
    sha256sum "${SQL_DIR:-$root/SQL/queries}/q$q.sql"
done > "$batch/query_sha256.txt"
for q in "${queries[@]}"; do
    for ((run=1; run<=runs; run++)); do
        point="$batch/q${q}_run${run}"
        if [[ $resuming == 1 ]] && env -u LD_LIBRARY_PATH "$python" - "$batch/batch_summary.csv" "$q" "$run" <<'PY_SKIP'
import csv,sys
rows=list(csv.DictReader(open(sys.argv[1])))
sys.exit(0 if any(r['query']==sys.argv[2] and r['run']==sys.argv[3] for r in rows) else 1)
PY_SKIP
        then echo "Already complete: Q$q run$run";continue;fi
        mkdir -p "$point"
        if [[ $SQL_ONLY != 1 ]]; then
            env -u LD_LIBRARY_PATH "$python" "$root/scripts/analyze_numa_access.py" snapshot --log-dir "$log_dir" --output "$point/log_snapshot.json"
        fi
        env -u LD_LIBRARY_PATH "$python" - "$batch" <<'PY_SPACE'
import os,sys
s=os.statvfs(sys.argv[1])
if s.f_bavail*s.f_frsize < 8*1024**3:raise SystemExit('Less than8GiB free; stop before next query')
PY_SPACE
        echo "$access_mode Q$q repetition $run/$runs; intra/inter statistics ON"
        OUTPUT_DIR="$point" QUERIES="$q" RUNS=1 bash "$root/run_imcs_optimization.sh" "$access_mode" | awk '/^Output:|^Results:|^\[20|^Baseline pool|^Layout restore|^Explicit RESET|ERROR:|Query failed/ {print;fflush()}'
        [[ $SQL_ONLY != 1 ]] || continue
        qid=$(awk '/Query Id:/ {print $NF}' "$point/${access_mode}_q${q}_run1.log" | tail -1)
        [[ $qid =~ ^[0-9]+$ ]] || { echo "Missing query ID for Q$q" >&2; exit 1; }
        printf '%s\n' "$qid" > "$point/query_id.txt"
        raw="$point/operator_data_access_${qid}.log"
        env -u LD_LIBRARY_PATH "$python" "$root/scripts/analyze_numa_access.py" extract \
            --log-dir "$log_dir" --snapshot "$point/log_snapshot.json" --query-id "$qid" \
            --min-workers 1 --timeout "${EXTRACT_TIMEOUT:-300}" --output "$raw"
        if [[ $analyze == 1 ]]; then
            env -u LD_LIBRARY_PATH MPLCONFIGDIR="$batch/.matplotlib" "$python" "$root/scripts/analyze_numa_access.py" analyze \
                --log "$raw" --nodes "$NUMA_NODES" --require-execution-scope --require-data-scope \
                --output "$point/analysis" --label "$access_mode Q$q / $count NUMA / DOP$dop"
        fi
        if [[ $compress == 1 ]]; then
            env -u LD_LIBRARY_PATH gzip -1 -- "$raw"
            env -u LD_LIBRARY_PATH gzip -t -- "$raw.gz"
        fi
        env -u LD_LIBRARY_PATH "$python" - "$point/summary.csv" "$batch/batch_summary.csv" "$q" "$run" "$qid" "$point" <<'PY'
import csv,sys
with open(sys.argv[1]) as f: rows=list(csv.DictReader(f))
if len(rows)!=1: raise SystemExit('Expected exactly one successful measurement')
with open(sys.argv[2],'a') as f: csv.writer(f).writerow([sys.argv[3],sys.argv[4],sys.argv[5],rows[0]['total_runtime_ms'],sys.argv[6]])
PY
    done
done
echo "Complete: $batch/batch_summary.csv"
