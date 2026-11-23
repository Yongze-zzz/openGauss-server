# Rethinking Data Locality in Operator-Parallel OLAP on Chiplets-Based CPUs

This artifact accompanies the paper's experimental study of memory locality and shared-cache behavior in openGauss. It contains the modified engine, scripts for TPC-H SF100 experiments, and archived measurements for offline analysis. The database experiments use a Kunpeng 920 with four sockets, eight chiplets, and 24 cores per chiplet.

[Paper PDF](_Experiment__Analysis__and_Benchmark__Rethinking_Data_Locality_in_Operator_Parallel_OLAP_on_Chiplets_Based_CPUs-15.pdf) · [Asset map](ASSETS.md) · [Figure scripts](assets/) · [Experiment commands](reproduction/paper/README.md) · [Engine setup](docs/reproduction/imcs_setup.md) · [GUC reference](#guc-reference)

## Plot the archived results

Run from the repository root with Python 3.11. Install the plotting dependencies once.

```bash
pip3 install -r assets/requirements.txt
python3 assets/nps_adapt/fig11.py
```

The example writes a tightly cropped PDF of Fig. 11 to `results/paper_figures/`. The scripts in `assets/` are grouped by experiment, and each archived figure script contains its own plot values. Fig. 4(a) takes the working-set sweep from a new benchmark run. [Figure commands and data sources](ASSETS.md)

## Run the experiments

Start with the three standalone Fig. 4 benchmarks. For the database experiments, build openGauss with HTAP and NUMA support, load TPC-H SF100, and start the instance with the CPU pool and memory binding for the selected chiplet count. The experiment commands use 4 chiplets/DOP96 or 8 chiplets/DOP168 and 100 GB `work_mem`. [Experiment commands](reproduction/paper/README.md) · [Engine setup](docs/reproduction/imcs_setup.md)

### Operator memory access

The access profiler counts logical reads and writes. Intra-operator records cover scans, private state, and workspaces. Inter-operator records cover consumption of upstream batches and Stream copies. Matrix rows identify executing chiplets and columns identify memory homes. Run query timing separately with both access counters and cache tracing off. [Access profiling and timing](reproduction/paper/README.md#database-access-and-timing) · [Access instrumentation](docs/analysis/imcs_access_and_cache_tracing.md#算子内与算子间访存)

### Cache tracking

The trace records per-thread LLC read misses and retired instructions alongside the wall time of plan nodes. The replay uses the saved CPU-to-LLC mapping to plot operator activity and LLC read-miss MPKI per chiplet. MPKI belongs to the sampled thread window. The paper compares 2 and 8 chiplets. The bundled timeline uses a separate 4/8-chiplet run. [Cache tracking and Table II inputs](ASSETS.md) · [Trace format and replay](docs/analysis/imcs_access_and_cache_tracing.md#缓存跟踪)

```bash
# On the 8-chiplet instance with the NPS layout populated
RESET_LAYOUT=manual CACHE_INTERVAL_US=100000 QUERIES="1 9 14 21" bash run_nps_cache_trace.sh 8
```

### Intra-operator cache optimization

Compare Sonic, private Radix Hash Join (RHJ), and shared RHJ on the same NPS and Hotspot Splitting configuration. Private RHJ partitions each worker's build state. For an eligible shared build, workers in an LLC domain use the same build partitions and keep private probe workspaces. Inspect the plans and runtime counters to identify the actual path. [RHJ runner](reproduction/paper/run_rhj.sh) · [RHJ executor](src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicradixjoin.cpp)

```bash
bash reproduction/paper/run_rhj.sh results/rhj_q8
```

Shared Sonic Hash Aggregate routes incoming groups to partitioned states shared within an LLC. The configuration can also select a partition-local aggregate rewrite. Use the profile logs to distinguish these paths. The following commands prepare Q1, Q18, and Q21 SQL and compare the aggregate switch on the 8-chiplet instance. [Shared aggregation implementation](src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicsharedagg.cpp) · [Aggregate runner](scripts/run_shared_sonic_agg.sh)

```bash
SQL_ONLY=1 QUERIES="1 18 21" WARMUPS=0 RUNS=1 \
  bash reproduction/paper/run_rhj.sh results/shared_agg_sql
BASE_SQL_DIR="$PWD/results/shared_agg_sql/shared_rhj" PHASE=targets \
  bash scripts/run_shared_sonic_agg.sh
```

### Inter-operator placement and cache coordination

Hotspot Splitting places local Stream buffers on producer chiplets. The replay measures how concurrent consumers change effective copy throughput. The Q5 coordination experiment keeps that data-placement policy, shared RHJ, CU balancing, Stream buffering, and shared Sonic Aggregate enabled. It compares in-lane aggregation, pipeline colocation, and pipeline colocation with an adaptive probe budget. Colocation preserves data ownership through eligible stages. The adaptive policy sizes and reuses probe buffers using the LLC budget and active operator state. [Stream replay and coordination commands](reproduction/paper/README.md#fig-13-stream-replay) · [Coordination modes](reproduction/paper/README.md#radix-hash-join-and-pipeline-coordination)

```bash
BASE_SQL_DIR="$PWD/reproduction/paper/sql" PHASE=chiplet_exchange \
  EXCHANGE_MODES="fused pipeline_fused pipeline_adaptive" QUERIES=5 WARMUPS=2 RUNS=6 \
  bash scripts/run_shared_sonic_agg.sh
```

`pipeline_adaptive` sets `imcs_chiplet_exchange_mode = 'pipeline_colocation'`, `imcs_chiplet_probe_tile_policy = 'adaptive_budget_reuse'`, and `radix_partition_size_mb = 12`. The timing output is `timing.csv`, with a row for each mode and pair. Use the plan and profile modes to check plan rewrites and budget decisions. [Mode definitions](scripts/run_shared_sonic_agg.sh) · [Optimization design](docs/design/imcs_chiplet_optimizations.md)

## GUC reference

The tables list this branch's experiment GUCs and the main openGauss settings used to run the experiments. Defaults are the values registered in the source. Runners set session values for each experiment. An enabled optimization can still select different execution paths by plan node. [SQL GUC definitions](src/common/backend/utils/misc/guc/guc_sql.cpp) · [Server GUC definitions](src/common/backend/utils/misc/guc.cpp)

### Intra-operator memory placement and scan scheduling

| GUC | Default | Role |
| --- | --- | --- |
| `enable_imcsscan` | `off` | Enables IMCS scan plans. |
| `enable_imcs_partition_scan` | `off` | Assigns whole IMCS partitions to parallel workers. |
| `enable_numa_aware_partition_scan` | `off` | Uses NUMA-local workers for partition scans. Requires `enable_imcs_partition_scan`. |
| `enable_imcs_nps` | `off` | Enables co-partitioned IMCS placement at populate time and locality-aware scheduling. |
| `imcs_nps_groups` | `''` | Comma-separated `schema.table:key` members, with semicolons between groups. Populate validates the groups. |
| `imcs_partition_scan_cpus` | `''` | Ordered, comma-separated CPU IDs for the scan worker pool. |
| `imcs_partition_numa_map` | `''` | Comma-separated `partition_oid:numa_node` pairs for explicit CU placement during populate. |
| `enable_imcs_adapt` | `off` | Enables Adapt with NPS, using CPU-load observations and background co-partition migration when migration is allowed. |
| `enable_imcs_adapt_migration` | `on` | Permits Adapt monitoring and migration when Adapt is enabled. Set `off` for the affinity-only control. |
| `imcs_adapt_period_ms` | `1000` | Adapt period in ms, range 20–60,000. |
| `enable_imcs_local_task_queue` | `off` | Enables whole-partition claiming on eligible local scans with NPS and Adapt affinity, with migration off. |
| `enable_imcs_nps_cu_balance` | `off` | Balances CU ranges within each NPS node on eligible scans, with Adapt and the local task queue off. |

### Intra-operator cache and hash state

| GUC | Default | Role |
| --- | --- | --- |
| `enable_sonic_hashjoin` | `on` | Allows the Sonic hash join executor. |
| `enable_radix_hashjoin` | `off` | Selects the independent RHJ executor for eligible Sonic join plans. |
| `radix_partition_size_mb` | `8` | Target build-partition size in MiB, range 1–1024. |
| `radix_cache_aware` | `off` | Includes bucket, chain, and null storage in the radix working-set estimate. |
| `radix_chiplet_aware` | `off` | Accounts for a worker's share of LLC in the partition budget. |
| `radix_llc_size_mb` | `24` | Configured LLC capacity per chiplet in MiB, range 1–1024. |
| `radix_shared_build` | `off` | Shares eligible local broadcast RHJ builds within an LLC. |
| `enable_radix_shared_probe_locality` | `off` | Starts a probe tile with a ready partition that was recently probed in the LLC. |
| `enable_radix_shared_probe_count` | `off` | Tracks active shared RHJ partitions independently of probe order. |
| `enable_sonic_hashagg` | `on` | Allows the Sonic hash aggregate executor. |
| `enable_imcs_shared_sonic_agg` | `off` | Enables LLC-local shared Sonic aggregation and eligible partition-local aggregate planning on NPS with Adapt off. |

### Inter-operator Stream placement and buffering

| GUC | Default | Role |
| --- | --- | --- |
| `enable_imcs_stream_hotspot_split` | `off` | First-touches eligible local Stream buffers on NPS producer chiplets. Older SQL can use the alias `enable_imcs_stream_buffer_locality`. |
| `enable_imcs_stream_batch_ring` | `off` | Gives a backpressured local vector redistribution channel a second buffer slot. |
| `imcs_stream_batch_ring_extra_slots` | `16` | Caps extra slots per producer per Stream, range 0–1024. |
| `enable_imcs_stream_consumer_placement` | `off` | Places a consumer once using sampled producer-buffer affinity and chiplet capacity. |
| `enable_imcs_stream_hotflow_colocation` | `off` | Routes eligible single-key local redistribution through NPS chiplet lanes. |
| `enable_imcs_stream_path_rebalance` | `off` | Spreads producer polling across chiplet lanes and offsets consumer polling order. |
| `radix_shared_direct_send` | `off` | Sends eligible shared RHJ broadcasts only to open receiver channels. |
| `radix_shared_preselect_builders` | `off` | Preselects one broadcast receiver per LLC for an eligible shared RHJ build. |

### Inter-operator cache coordination

| GUC | Default | Role |
| --- | --- | --- |
| `imcs_chiplet_exchange_mode` | `off` | Selects the plan rewrite for eligible exchanges and co-partitioned stages. |
| `imcs_chiplet_stage_cpus` | `''` | CPU placement for eligible stages, formatted as `plan_node:cpu,cpu;plan_node:cpu,cpu`. |
| `imcs_chiplet_probe_tile_policy` | `off` | Sets LLC budgeting and buffer reuse for eligible shared RHJ probes. The path requires NPS and runs with Adapt, consumer placement, and access profiling off. |
| `enable_imcs_cache_coord` | `off` | Prefers ready local Stream batches during recent overlapping radix probes. |
| `enable_imcs_cache_coord_llc_pressure` | `off` | Adds an estimated LLC-capacity test to Stream batch preference. |
| `imcs_cache_coord_sample_interval` | `4` | Caps consecutive local Stream batches before preferring a ready remote batch, range 1–64. |
| `enable_imcs_cache_probe_coord` | `off` | Briefly yields large radix probes under sampled LLC pressure. |
| `enable_imcs_cache_harmful` | `off` | Detects LLC-local overlap of scans, aggregate updates, and radix probes. |
| `enable_imcs_cache_harmful_defer` | `off` | Allows brief probe deferral on a detected overlap. |
| `enable_imcs_rhj_overlap_prefetch` | `off` | Prefetches buckets for a selected shared RHJ probe during local peer overlap. |
| `enable_imcs_rhj_overlap_match_yield` | `off` | Allows the selected shared RHJ match stage to yield during local peer overlap. |
| `imcs_rhj_stream_schedule_llc` | `-1` | Selects a receive-scheduling LLC. `-1` disables scheduling and `-2` applies per-LLC copy admission to all LLCs when copy slots are set. |
| `imcs_rhj_stream_copy_slots` | `0` | Soft cap on concurrent large Stream copies per LLC, range 0–128. Zero selects ready-batch ordering on a selected LLC. |
| `imcs_agg_cache_slots` | `0` | Soft cap on overlapping aggregate updates per LLC, range 0–128. Zero disables the cap. |
| `enable_imcs_agg_cache_window` | `off` | Splits update slots by branch and periodically changes preference. |
| `enable_imcs_agg_cache_borrow` | `off` | Allows the other aggregate to borrow unused preferred slots. |
| `enable_imcs_agg_cache_chiplet` | `off` | Uses LLC-local Stream demand and staggers branch preference across LLCs. |
| `enable_imcs_agg_cache_live_pressure` | `off` | Gates windowed aggregate coordination on active state exceeding estimated LLC capacity. |
| `enable_imcs_agg_cache_critical` | `off` | Gives priority to sustained downstream demand within the same LLC in chiplet-aware windowed mode. |

Use the following English values for the two policy GUCs.

| `imcs_chiplet_exchange_mode` | Behavior |
| --- | --- |
| `off` | Keeps normal plan construction. |
| `owner_handoff` | Preserves the partition owner through eligible exchanges. |
| `selective_build` | Builds eligible colocated joins from selective aggregate output. |
| `in_lane_aggregate` | Places eligible aggregates and joins in the same execution lane. |
| `pipeline_colocation` | Preserves partition ownership across eligible join stages. |
| `broadcast_isolation` | Restricts eligible broadcast stages to selected CPUs. |

| `imcs_chiplet_probe_tile_policy` | Behavior |
| --- | --- |
| `off` | Uses the normal probe workspace policy. |
| `fixed_budget` | Sizes tiles using LLC capacity and the shared build partitions. |
| `aggregate_aware` | Accounts for active aggregate state in the LLC. |
| `peer_aware` | Accounts for active aggregate state and other shared RHJ state. |
| `fixed_budget_reuse` | Uses a fixed budget and reuses probe buffers. |
| `adaptive_budget_reuse` | Adjusts the budget under LLC-local pressure and reuses probe buffers. |

### Profiling and overlap selection

| GUC | Default | Role |
| --- | --- | --- |
| `enable_intra_operator_data_access` | `off` | Records operator-local logical accesses. |
| `enable_inter_operator_data_access` | `off` | Records producer-consumer accesses, batch handoffs, and Stream copies. |
| `enable_cache_trace` | `off` | Records per-thread LLC counters and plan-node activity. |
| `cache_trace_interval_us` | `100000` | Regular trace interval in µs, range 2,000–1,000,000. |
| `enable_imcs_shared_sonic_agg_stats` | `off` | Logs aggregate execution paths and worker-level statistics. |
| `imcs_rhj_overlap_victim_node` | `0` | Plan-node ID of the RHJ probe to observe or coordinate. |
| `imcs_rhj_overlap_peer_node` | `0` | Plan-node ID of the overlapping stage. |
| `imcs_rhj_overlap_peer_kind` | `0` | Selects a radix probe (`0`), aggregate update (`1`), or Stream receive copy (`2`) as the peer. |

For timed runs, turn off tracing and access statistics. Check the server launch environment as well. `OG_CACHE_TRACE` can enable tracing independently of the session, and `OG_CACHE_TRACE_INTERVAL_US` can override the session interval. [Tracing implementation](src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp)

### Supporting openGauss settings

| GUC | Usage in the experiments |
| --- | --- |
| `query_dop` | `96` for 4 chiplets and `168` for 8 chiplets. |
| `work_mem` | `100GB` for the main database comparisons. |
| `query_mem`, `query_max_mem` | Statement memory reservation and limit, reported in the timing settings. |
| `enable_vector_engine`, `enable_codegen` | Enabled in the standard timing SQL. |
| `enable_cstore_partition_lane_scan` | Disabled in the IMCS comparison SQL. |
| `enable_thread_pool`, `thread_pool_attr`, `thread_pool_stream_attr` | Server startup settings generated for the active chiplet count. |
| `numa_distribute_mode` | Set to `all` in the database experiments. |
| `max_imcs_cache` | IMCS cache capacity, set at server start. The reference configuration uses `256GB`. |

## License

See [License](License) and [COPYRIGHT](COPYRIGHT) for the openGauss license and notices.
