#!/usr/bin/env bash
# Replay Q8's saved 8-chiplet Stream copy plan. No database connection is needed.
set -euo pipefail
directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ ${1:-} == --help ]]; then
    echo 'Usage: bash reproduction/paper/run_stream_replay.sh NEW_OUTPUT_DIRECTORY'
    echo 'Requires the captured 8-chiplet CPU and memory topology and Linux NUMA placement permissions.'
    exit 0
fi
[[ $# -eq 1 ]] || { echo 'Specify a new output directory. See --help.' >&2; exit 2; }
out=$1
[[ ! -e $out ]] || { echo 'Use a new output directory.' >&2; exit 2; }
scratch=$(mktemp -d "${TMPDIR:-/tmp}/stream-replay.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
gzip -dc "$directory/data/stream_q8_plan.tsv.gz" > "$scratch/plan.tsv"
"${CXX:-g++}" -std=c++17 -O3 -pthread "$directory/../../scripts/stream_consumer_replay.cpp" -o "$scratch/replay"
mkdir -p -- "$out"

# Use the archived Q8 multiplier. Run order alternates across the three pairs.
for run in 1 2 3; do
    order=(isolated concurrent)
    if ((run % 2 == 0)); then order=(concurrent isolated); fi
    for schedule in "${order[@]}"; do
        echo "Q8, pair $run, $schedule"
        # The binary reads GO after it has prepared the inputs and checked their placement.
        printf 'GO\n' | "$scratch/replay" "$scratch/plan.tsv" "$schedule" 71 \
            | sed '1{/^READY$/d;}' > "$out/run${run}_${schedule}.json"
    done
done
