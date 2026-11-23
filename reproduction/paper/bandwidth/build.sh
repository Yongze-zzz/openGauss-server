#!/usr/bin/env bash
set -euo pipefail
directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
"${CXX:-g++}" -O3 -march=native -std=c++17 -pthread -Wall -Wextra \
  "$directory/read_bandwidth.cpp" -o "${1:-$directory/read_bandwidth}"
