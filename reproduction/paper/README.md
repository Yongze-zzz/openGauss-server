# Run the experiments

Run the commands from the repository root. The database experiments use TPC-H SF100 on Kunpeng 920 with 24 cores per chiplet. [Paper and offline plots](../../README.md) · [Figure inputs](../../ASSETS.md) · [GUC reference](../../README.md#guc-reference)

## Fig. 4 standalone benchmarks

Use a Linux host with C++17, Python 3, CPU affinity, and NUMA page placement. The runner builds the selected benchmark in a temporary directory. Each invocation writes a new result directory.

```bash
bash reproduction/paper/run_fig4.sh cache     results/fig04a
bash reproduction/paper/run_fig4.sh memory    results/fig04b
bash reproduction/paper/run_fig4.sh bandwidth results/fig04c
```

- **Fig. 4(a), cache latency.** One pinned core follows a dependent-load ring across working sets from 1 KiB to 960 MiB. The code follows one cache-line-sized cell at a time using `index = p[index].next`. `cache_latency.csv` contains median, minimum, and maximum ns per load for each size. This command generates a fresh curve in ns per load. The original curve's raw input has yet to be located. [Dependent-load benchmark](bandwidth/memory_latency.cpp)
- **Fig. 4(b), memory latency.** Each cell is the median of seven rounds of two million dependent loads from a 512 MiB working set. CPU placement follows the requesting chiplet and the mapping is bound to the data chiplet. Rows are requesters and columns are data homes. The published panel uses C0, C1, C4, and C5, so the runner selects those nodes by default. It writes `rounds.csv`, `summary.csv`, `matrix_ns.csv`, and `topology.csv`. [Figure and archived values](../../assets/hardware/fig04b.py) · [Measurement code](bandwidth/memory_latency.cpp)
- **Fig. 4(c), read bandwidth.** Two pinned foreground readers each scan 128 MiB. The overload cases add 22 readers on the requesting die with their pages on the same destination as the foreground readers. The benchmark measures effective foreground GB/s. `runs.csv` contains three repeats per case and `summary.csv` contains the median and range. The driver checks the same-socket and remote-socket topology. [Figure and archived values](../../assets/hardware/fig04c.py) · [Driver](bandwidth/run.py) · [Read benchmark](bandwidth/read_bandwidth.cpp)

`CACHE_NODE` and `MEMORY_NODES` select the latency nodes. `SOURCE_NODE`, `SAME_SOCKET_NODE`, and `REMOTE_NODE` select the bandwidth nodes. Check `numactl --hardware` before overriding them. To draw the cache sweep, pass its output CSV to the Fig. 4(a) script.

```bash
python3 assets/hardware/fig04a.py results/fig04a/cache_latency.csv
```

## Database access and timing

Figs. 7–9, 11, and 12 use this procedure. Build openGauss, load the TPC-H tables, and export `PGDATA`, `INSTALL_DIR`, `PORT`, `DB_NAME`, and `SCHEMA`. Use the 96-core pool on nodes 0–3 for 4 chiplets and the 192-core pool on nodes 0–7 for 8 chiplets, with memory bound to the same nodes. The commands below assume the corresponding instance has been started with its matching CPU and memory policy. [Engine setup](../../docs/reproduction/imcs_setup.md) · [Thread-pool configuration and startup](../../docs/reproduction/imcs_setup.md#start-the-4--or-8-chiplet-experiment)

`run_numa_access.sh` enables intra- and inter-operator access statistics and writes per-query matrices and heatmaps under each run's `analysis/` directory. Baseline uses native IMCS placement. NPS uses co-partitioned IMCS placement and locality-aware scheduling. NPS+Adapt also enables the Adapt controller. `RESET_LAYOUT=repopulate` permits the runner to reload the matching IMCS layout. [Collection script](../../run_numa_access.sh) · [Log analyzer](../../scripts/analyze_numa_access.py) · [Executor instrumentation](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp)

```bash
# On the 4-chiplet instance
ACCESS_MODE=baseline RESET_LAYOUT=repopulate bash run_numa_access.sh 4

# On the 8-chiplet instance
ACCESS_MODE=baseline RESET_LAYOUT=repopulate bash run_numa_access.sh 8
ACCESS_MODE=nps_adapt RESET_LAYOUT=repopulate QUERIES="3 5 7 8 9 18" bash run_numa_access.sh 8
```

Use `QUERIES=8` for a single-query run. Add `--sql-only` to inspect its SQL before collecting. A matrix cell is the logical bytes accessed by the row chiplet at a page on the column chiplet. Off-diagonal cells are remote. The Fig. 7–8 scripts contain the historical baseline ratios and byte totals. Fig. 9 uses the archived instrumented matrices. Fig. 12 uses the reference package's estimates from heatmap colors, scaled to the reported remote GiB. [Figure inputs and provenance](../../ASSETS.md)

Collect query time in separate runs with access statistics and cache tracing disabled. Both commands use Sonic Hash Join, 100 GB work memory, two warmups, and five timed runs per mode. Run each command on the instance for its chiplet count.

```bash
export MODE=all WORK_MEM=100GB ADAPT_PERIOD_MS=20 ADAPT_MIGRATION=on
export INTRA_STATS=off INTER_STATS=off CACHE_TRACE=off RHJ_MODE=sonic STREAM_HOTSPOT_SPLIT=off
export RESET_LAYOUT=repopulate WARMUPS=2 RUNS=5
QUERIES="3 5 7 8 9 18" bash run_imcs_4numa_96dop.sh
QUERIES="3 5 7 8 9 18" bash run_imcs_8numa_168dop.sh
```

The timing runner writes `summary.csv` with one row per measurement and a `total_runtime_ms` column. Fig. 11 uses Baseline median time divided by NPS+Adapt median time, and plots absolute remote GiB for the 8-chiplet runs. The archived values are embedded in the figure script. Use the medians from the same batch when comparing a new run. [Fig. 11](../../assets/nps_adapt/fig11.py)

## Fig. 13 Stream replay

Replay the saved Q8 workload on the 8-chiplet topology. The standalone program pins the consumers and places the source buffers according to the saved plan. Isolated runs one consumer chiplet at a time. Concurrent runs all eight groups together. Each pair uses the same copy plan, buffer homes, and byte counts. The runner writes three JSON pairs with per-worker bytes and timestamps. The plotted rates are effective copy GB/s.

```bash
env -u LD_LIBRARY_PATH CXX=/usr/bin/g++ \
  bash reproduction/paper/run_stream_replay.sh "$PWD/results/fig13_q8"
```

The figure script contains the publication's throughput labels and vector path widths. Both panels show the same routes. The left panel summarizes eight sequential stages. The included benchmark plan comes from the historical Q8 collection, and the replay runs the isolated and concurrent copy schedules using that plan. [Fig. 13](../../assets/stream_replay/fig13.py) · [Copy implementation](../../scripts/stream_consumer_replay.cpp)

## Hotspot Splitting

Fig. 15 compares producer-local Stream buffers on 8 chiplets. The current runner exposes the buffer policy through `STREAM_HOTSPOT_SPLIT`. Use the same NPS+Adapt workload for both cases.

```bash
for setting in off on; do
  MODE=nps_adapt STREAM_HOTSPOT_SPLIT="$setting" RHJ_MODE=sonic \
    ADAPT_PERIOD_MS=20 ADAPT_MIGRATION=on WORK_MEM=100GB RESET_LAYOUT=repopulate \
    INTRA_STATS=off INTER_STATS=off CACHE_TRACE=off \
    QUERIES="2 5 8 9" WARMUPS=2 RUNS=5 bash run_imcs_8numa_168dop.sh
done
```

## Cache tracking

Fig. 16 and Table II compare 2 and 8 chiplets. The archive includes the 2-chiplet raw counters and a transcription of Table II. The current NPS/Sonic tracing recipe collects 4 and 8 chiplets. It records per-thread LLC read misses, instructions, plan-node IDs, and the CPU-to-LLC map. Use a consistent period on each server for a fresh comparison. [Tracing code and counters](../../docs/analysis/imcs_access_and_cache_tracing.md)

```bash
# On each matching instance with NPS populated
RESET_LAYOUT=manual CACHE_INTERVAL_US=100000 QUERIES="1 2 6 9 14 21" bash run_nps_cache_trace.sh 4
RESET_LAYOUT=manual CACHE_INTERVAL_US=100000 QUERIES="1 2 6 9 14 21" bash run_nps_cache_trace.sh 8
```

## Radix Hash Join and pipeline coordination

The in-engine recipe for §V-B runs Q8 with Sonic, private RHJ, and shared RHJ on the same populated 8-chiplet NPS layout with Hotspot Splitting. RHJ partitions the build and probe keys using hash bits. The shared mode builds eligible partitions once per LLC domain and gives each worker its own probe workspace. `vechashjoin.cpp` dispatches eligible joins, and `trySharedRadixBuild()` opens the shared-build path. [Join dispatch](../../src/gausskernel/runtime/vecexecutor/vecnode/vechashjoin.cpp) · [Radix implementation](../../src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicradixjoin.cpp#L1084)

```bash
bash reproduction/paper/run_rhj.sh results/rhj_q8

# Inspect one Q8 SQL file per join mode
SQL_ONLY=1 WARMUPS=0 RUNS=1 bash reproduction/paper/run_rhj.sh results/rhj_q8_sql
```

Each mode writes its own `summary.csv` and query plans. Inspect `nps_q8_run1.log` and the RHJ before/after counters to confirm runtime selection of the shared path. The script uses these join switches.

| Mode | `enable_sonic_hashjoin` | `enable_radix_hashjoin` | `radix_shared_build` |
| --- | --- | --- | --- |
| `sonic` | on | off | off |
| `private_rhj` | on | on | off |
| `shared_rhj` | on | on | on |

The shared Sonic Hash Aggregate comparison uses the same shared RHJ query SQL in both modes. Generate the input SQL and run the paired comparison on the populated 8-chiplet NPS instance. The runner compares the full query results and records one row per timed mode and pair.

```bash
SQL_ONLY=1 QUERIES="1 18 21" WARMUPS=0 RUNS=1 \
  bash reproduction/paper/run_rhj.sh results/shared_agg_sql
BASE_SQL_DIR="$PWD/results/shared_agg_sql/shared_rhj" PHASE=targets \
  bash scripts/run_shared_sonic_agg.sh
```

Use `PHASE=profile QUERIES="18 21 1"` with the same `BASE_SQL_DIR` for worker-level profiling. The plan and worker logs show when shared state is actually used. [Shared aggregate implementation](../../src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicsharedagg.cpp)

For the current §V-C-style coordination experiment, keep NPS, Hotspot Splitting, shared RHJ, CU balancing, Stream buffering, and shared Sonic Aggregate enabled. The runner compares in-lane aggregation, pipeline colocation, and adaptive probe budgets on Q5. [Mode settings](../../docs/design/imcs_chiplet_optimizations.md) · [Archived query SQL](sql/nps_q5_run1.sql)

```bash
BASE_SQL_DIR="$PWD/reproduction/paper/sql" PHASE=chiplet_exchange \
  EXCHANGE_MODES="fused pipeline_fused pipeline_adaptive" \
  QUERIES=5 WARMUPS=2 RUNS=6 bash scripts/run_shared_sonic_agg.sh
```
