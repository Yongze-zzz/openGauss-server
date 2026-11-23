#!/usr/bin/env bash
# Collect per-thread PMU data on an existing NPS instance.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

usage() {
    echo 'Usage: bash run_nps_cache_trace.sh 4|8'
    echo 'Requires a matching NPS instance. QUERIES defaults to "9 13".'
    echo 'RESET_LAYOUT=repopulate permits an IMCS layout reset. The default is manual.'
}

case ${1:---help} in
    -h|--help) usage; exit 0 ;;
    4) count=4; dop=96 ;;
    8) count=8; dop=168 ;;
    *) usage >&2; exit 2 ;;
esac
[[ $# -eq 1 ]] || { usage >&2; exit 2; }

topology=$(env -u CPU_POOL -u NUMA_NODES -u IMCS_PARTITION_SCAN_CPUS \
    bash "$root/scripts/imcs_numa_env.sh" "$count" "$dop")
eval "$topology"
export INSTALL_DIR=${INSTALL_DIR:-$root/mppdb_temp_install}
export SCHEMA=${SCHEMA:-tpch_sf100_imcs_h192}
export DOP=$dop INTRA_STATS=off INTER_STATS=off ADAPT_MIGRATION=off ADAPT_LOCAL_TASKS=off
export WORK_MEM=${WORK_MEM:-100GB} RESET_LAYOUT=${RESET_LAYOUT:-manual}
libs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc=${GCC_PATH:-$libs/buildtools/gcc10.3}
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$gcc/gcc/lib64:$libs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
python=(env -u LD_LIBRARY_PATH PYTHONDONTWRITEBYTECODE=1 "${PYTHON_BIN:-python3}")

rhj_mode=${RHJ_MODE:-sonic}
[[ $rhj_mode =~ ^(sonic|radix|cache|chiplet)$ ]] || { echo 'Unknown RHJ_MODE.' >&2; exit 2; }
read -r -a queries <<< "${QUERIES:-9 13}"
((${#queries[@]})) || { echo 'Select at least one query.' >&2; exit 2; }
declare -A selected_queries
for query in "${queries[@]}"; do
    [[ $query =~ ^([1-9]|1[0-9]|2[0-2])$ ]] || { echo "Invalid query: $query" >&2; exit 2; }
    [[ ! -v selected_queries[$query] ]] || { echo "Repeated query: $query" >&2; exit 2; }
    selected_queries[$query]=1
done

if [[ -n ${OUTPUT_DIR:-} ]]; then
    out=$OUTPUT_DIR
    [[ ! -e $out ]] || { echo 'Use a new OUTPUT_DIR.' >&2; exit 2; }
    mkdir -p "$out"
else
    mkdir -p "$root/results/nps_cache_trace"
    out=$(mktemp -d "$root/results/nps_cache_trace/${count}numa_dop${dop}_${rhj_mode}_$(date +%Y%m%d_%H%M%S)_XXXXXX")
fi
out=$(cd "$out" && pwd -P)
echo "Results: $out"
printf 'query,control_ms,trace_ms,overhead_percent,analysis\n' > "$out/summary.csv"
shopt -s nullglob

run_query() {
    local query=$1 setting=$2
    CACHE_TRACE="$setting" RHJ_MODE="$rhj_mode" QUERIES="$query" WARMUPS=1 RUNS=1 SQL_ONLY=0 \
        OUTPUT_DIR="$out/q${query}_$setting" bash "$root/run_imcs_optimization.sh" nps
}

for query in "${queries[@]}"; do
    prefix="nps_q${query}_run1"
    run_query "$query" off
    "${python[@]}" - "$out/q${query}_off" "$prefix" <<'PY_CONTROL'
import json
import sys
from pathlib import Path

directory, prefix = Path(sys.argv[1]), sys.argv[2]
before, after = (
    json.loads((directory / f"{prefix}_{name}.json").read_text()) for name in ("before", "after")
)
counters = [name for name in after if name.startswith("cache_trace_")]
if "cache_trace_started" not in counters or any(after[name] != before[name] for name in counters):
    raise SystemExit("Cache trace counters changed in the control run. Check the server's OG_CACHE_TRACE setting.")
PY_CONTROL

    run_query "$query" on
    point="$out/q${query}_on"
    plan="$point/$prefix.log"
    query_id=$(awk '/Query Id:/ {print $NF}' "$plan" | tail -1)
    [[ $query_id =~ ^[0-9]+$ ]] || { echo "Missing Query Id in $plan" >&2; exit 1; }
    data_dir=$(cat "$point/data_directory.txt")
    files=("$data_dir"/pg_log/cache_trace/cache_trace_q"$query_id"_t*.bin)
    ((${#files[@]})) || { echo "No cache trace files for query $query_id" >&2; exit 1; }

    "${python[@]}" - "$point" "$prefix" "$rhj_mode" "${#files[@]}" <<'PY_COVERAGE'
import json
import sys
from pathlib import Path

directory, prefix = Path(sys.argv[1]), sys.argv[2]
mode, file_count = sys.argv[3], int(sys.argv[4])
before, after = (
    json.loads((directory / f"{prefix}_{name}.json").read_text()) for name in ("before", "after")
)
counts = {name: after[name] - before[name] for name in after if name.startswith("cache_trace_")}
counts.update(binary_files=file_count, rhj_mode=mode, rhj_builds=after["rhj_builds"] - before["rhj_builds"])
coverage_file = directory / "collection_coverage.json"
coverage_file.write_text(json.dumps(counts, indent=2) + "\n")
if (mode == "sonic" and counts["rhj_builds"] != 0) or (mode != "sonic" and counts["rhj_builds"] <= 0):
    raise SystemExit("Actual join executor does not match RHJ_MODE")
if not counts["cache_trace_started"] == counts["cache_trace_written"] == file_count or any(
    counts[name] for name in ("cache_trace_start_failed", "cache_trace_perf_unavailable",
                             "cache_trace_write_failed", "cache_trace_discarded")
):
    raise SystemExit(f"Incomplete cache collection. See {coverage_file}")
PY_COVERAGE

    "${python[@]}" "$root/scripts/cache_trace_metadata.py" --plan "$plan" --output "$point/trace_metadata.json"
    mkdir "$point/raw"
    mv -- "${files[@]}" "$point/raw/"
    MPLCONFIGDIR=${MPLCONFIGDIR:-/tmp/imcs-cache-matplotlib} \
        "${python[@]}" "$root/scripts/analyze_cache_timeline.py" "$point/raw" --output "$point/analysis"
    tar -czf "$point/raw.tar.gz" -C "$point/raw" .
    tar -tzf "$point/raw.tar.gz" > /dev/null
    rm -- "$point/raw"/cache_trace_*.bin
    rmdir "$point/raw"

    "${python[@]}" - "$out" "$query" <<'PY_SUMMARY'
import csv
import sys
from pathlib import Path

directory, query = Path(sys.argv[1]), sys.argv[2]

def runtime(setting):
    path = directory / f"q{query}_{setting}" / "summary.csv"
    with path.open(newline="") as source:
        return float(next(csv.DictReader(source))["total_runtime_ms"])

control, trace = runtime("off"), runtime("on")
with (directory / "summary.csv").open("a", newline="") as output:
    csv.writer(output).writerow([query, control, trace, 100 * (trace / control - 1),
                                 str(directory / f"q{query}_on" / "analysis")])
PY_SUMMARY
done
echo "Complete: $out/summary.csv"
