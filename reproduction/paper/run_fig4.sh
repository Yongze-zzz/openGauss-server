#!/usr/bin/env bash
set -euo pipefail
directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

if [[ ${1:-} == --help ]]; then
    echo 'Usage: bash reproduction/paper/run_fig4.sh cache|memory|bandwidth NEW_OUTPUT_DIRECTORY'
    echo 'Overrides: CACHE_NODE (0), MEMORY_NODES (0,1,4,5), SOURCE_NODE (0), SAME_SOCKET_NODE (1), REMOTE_NODE (2), CXX.'
    exit 0
fi
[[ $# -eq 2 && $1 =~ ^(cache|memory|bandwidth)$ ]] || {
    echo 'Select a Fig. 4 benchmark and a new output directory. See --help.' >&2
    exit 2
}
panel=$1
out=$2
[[ ! -e $out ]] || { echo 'Use a new output directory.' >&2; exit 2; }
scratch=$(mktemp -d "${TMPDIR:-/tmp}/chiplet-latency.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
export CXX=${CXX:-/usr/bin/g++}
if [[ $CXX == /usr/bin/g++ ]]; then
    unset LD_LIBRARY_PATH
fi

if [[ $panel == bandwidth ]]; then
    bash "$directory/bandwidth/build.sh" "$scratch/read_bandwidth"
    env -u LD_LIBRARY_PATH "${PYTHON_BIN:-python3}" "$directory/bandwidth/run.py" --binary "$scratch/read_bandwidth" \
        --source-node "${SOURCE_NODE:-0}" --same-socket-node "${SAME_SOCKET_NODE:-1}" \
        --remote-node "${REMOTE_NODE:-2}" --foreground 2 --background 22 \
        --mib 128 --seconds 2 --runs 3 --output "$out"
    exit
fi

"$CXX" -O3 -std=c++17 -pthread "$directory/bandwidth/memory_latency.cpp" -o "$scratch/memory_latency"
if [[ $panel == memory ]]; then
    "$scratch/memory_latency" --nodes "${MEMORY_NODES:-0,1,4,5}" \
        --size-mib 512 --rounds 7 --loads 2000000 --output "$out"
    exit
fi

mkdir -p -- "$out"
printf 'working_set_kib,median_ns_per_load,min_ns_per_load,max_ns_per_load,page_location\n' > "$out/cache_latency.csv"
printf 'working_set_kib,round,loads,ns_per_load\n' > "$out/rounds.csv"
point="$scratch/point"
for size in 1 2 4 8 16 32 60 120 240 480 960 1920 3840 7680 15360 24576 30720 61440 122880 245760 491520 983040; do
    "$scratch/memory_latency" --nodes "${CACHE_NODE:-0}" --size-kib "$size" \
        --rounds 7 --loads 2000000 --output "$point"
    awk -F, -v size="$size" 'NR == 2 { print size "," $10 "," $11 "," $12 "," $13 }' \
        "$point/summary.csv" >> "$out/cache_latency.csv"
    awk -F, -v size="$size" 'NR > 1 { print size "," $7 "," $8 "," $9 }' \
        "$point/rounds.csv" >> "$out/rounds.csv"
done
