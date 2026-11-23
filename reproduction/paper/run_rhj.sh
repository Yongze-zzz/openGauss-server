#!/usr/bin/env bash
# Compare three join implementations on the same populated 8-chiplet NPS layout.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)

if [[ ${1:-} == --help ]]; then
    echo 'Usage: bash reproduction/paper/run_rhj.sh NEW_OUTPUT_DIRECTORY'
    echo 'Defaults: Q8, 2 warmups, 5 measurements, 100GB work_mem. Overrides: QUERIES, WARMUPS, RUNS, WORK_MEM.'
    echo 'Requires 8 NUMA / DOP168 and a populated NPS layout. SQL_ONLY=1 saves generated SQL.'
    exit 0
fi
[[ $# -eq 1 && ! -e $1 ]] || { echo 'Specify a new output directory. See --help.' >&2; exit 2; }
mkdir -p -- "$1"
out=$(cd -- "$1" && pwd -P)

export MODE=nps RESET_LAYOUT=manual STREAM_HOTSPOT_SPLIT=on
export QUERIES=${QUERIES:-8} WARMUPS=${WARMUPS:-2} RUNS=${RUNS:-5} WORK_MEM=${WORK_MEM:-100GB}
export INTRA_STATS=off INTER_STATS=off CACHE_TRACE=off
export CACHE_COORD=off CACHE_PROBE_COORD=off RHJ_SHARED_PROBE_LOCALITY=off
export RHJ_SHARED_PROBE_COUNT=off
export RHJ_SHARED_DIRECT_SEND=off RHJ_SHARED_PRESELECT_BUILDERS=off
export RADIX_PARTITION_MB=${RADIX_PARTITION_MB:-8}

RHJ_MODE=sonic RHJ_SHARED_BUILD=off OUTPUT_DIR="$out/sonic" bash "$root/run_imcs_8numa_168dop.sh"
RHJ_MODE=radix RHJ_SHARED_BUILD=off OUTPUT_DIR="$out/private_rhj" bash "$root/run_imcs_8numa_168dop.sh"
RHJ_MODE=radix RHJ_SHARED_BUILD=on OUTPUT_DIR="$out/shared_rhj" bash "$root/run_imcs_8numa_168dop.sh"
