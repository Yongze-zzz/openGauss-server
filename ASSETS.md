# Artifact assets

The table uses the figure numbers in manuscript revision 15. The archived figure scripts contain their plotted values and annotations. Run each script from the repository root. Each script writes one tightly cropped PDF to `results/paper_figures/`. Use `--output` to choose a different directory. [Paper PDF](_Experiment__Analysis__and_Benchmark__Rethinking_Data_Locality_in_Operator_Parallel_OLAP_on_Chiplets_Based_CPUs-15.pdf) · [Experiment commands](reproduction/paper/README.md)

| Experiment | Figure scripts | Example command |
| --- | --- | --- |
| Standalone hardware measurements | [`assets/hardware/`](assets/hardware/) | `python3 assets/hardware/fig04b.py` |
| Baseline operator memory access | [`assets/operator_memory/`](assets/operator_memory/) | `python3 assets/operator_memory/fig07.py` |
| NPS+Adapt | [`assets/nps_adapt/`](assets/nps_adapt/) | `python3 assets/nps_adapt/fig11.py` |
| Stream replay | [`assets/stream_replay/`](assets/stream_replay/) | `python3 assets/stream_replay/fig13.py` |
| Cache tracking | [`assets/cache_tracking/`](assets/cache_tracking/) | `python3 assets/cache_tracking/cache_timeline_4c_8c.py` |

| Paper | Figure script and inputs | How to collect |
| --- | --- | --- |
| Fig. 4(a) | [Working-set sweep plot](assets/hardware/fig04a.py) accepts `cache_latency.csv` from a fresh run and writes `fig04a_cache_latency.pdf`. | [Standalone runner](reproduction/paper/run_fig4.sh) · [Dependent-load benchmark](reproduction/paper/bandwidth/memory_latency.cpp) |
| Fig. 4(b) | [Memory latency](assets/hardware/fig04b.py) embeds the C0, C1, C4, and C5 medians and writes `fig04b_memory_latency.pdf`. | [Standalone runner](reproduction/paper/run_fig4.sh) · [Dependent-load benchmark](reproduction/paper/bandwidth/memory_latency.cpp) |
| Fig. 4(c) | [Read bandwidth](assets/hardware/fig04c.py) embeds the median and range over three repetitions and writes `fig04c_bandwidth.pdf`. | [Standalone runner](reproduction/paper/run_fig4.sh) · [Benchmark driver](reproduction/paper/bandwidth/run.py) · [Read benchmark](reproduction/paper/bandwidth/read_bandwidth.cpp) |
| Fig. 7 | [Per-query ratios](assets/operator_memory/fig07.py) embeds the historical 4- and 8-chiplet percentages and writes `fig07_remote_access_ratios.pdf`. | [Access collection](reproduction/paper/README.md#database-access-and-timing) |
| Fig. 8 | [Cumulative remote access](assets/operator_memory/fig08.py) embeds the historical byte totals and writes `fig08_cumulative_remote_access.pdf`. | [Access collection](reproduction/paper/README.md#database-access-and-timing) |
| Fig. 9 | [Baseline heatmaps](assets/operator_memory/fig09.py) embeds the measured 4- and 8-chiplet baseline matrices and writes `fig09_baseline_matrices.pdf`. | [Access collection](reproduction/paper/README.md#database-access-and-timing) |
| Fig. 11 | [NPS+Adapt bars](assets/nps_adapt/fig11.py) embeds the paper figure package's timings, remote GiB, and reductions. It writes `fig11_nps_adapt.pdf`. | [Access and timing commands](reproduction/paper/README.md#database-access-and-timing) |
| Fig. 12 | [NPS+Adapt heatmaps](assets/nps_adapt/fig12.py) embeds the paper figure package's estimated cells and hotspot outlines. The cell magnitudes were reconstructed from heatmap colors and scaled to reported remote GiB. It writes `fig12_nps_adapt_matrices.pdf`. | [Access collection](reproduction/paper/README.md#database-access-and-timing) |
| Fig. 13 | [Producer–consumer flows](assets/stream_replay/fig13.py) embeds the 64 publication labels and the vector path weights and writes `fig13_stream_replay.pdf`. | [Q8 replay runner](reproduction/paper/run_stream_replay.sh) · [Q8 benchmark plan](reproduction/paper/data/stream_q8_plan.tsv.gz) · [Copy benchmark](scripts/stream_consumer_replay.cpp) |
| Fig. 15 | The commands compare producer-local Stream buffers on NPS+Adapt. | [Hotspot Splitting](reproduction/paper/README.md#hotspot-splitting) |
| Fig. 16 and Table II | [Cache timeline](assets/cache_tracking/cache_timeline_4c_8c.py) embeds the samples from the separate 4/8-chiplet NPS/Sonic run and writes `cache_timeline_4c_8c.pdf`. | [Cache tracking](reproduction/paper/README.md#cache-tracking) |
| §V-B and §V-C | The commands reproduce the in-engine join comparison and the current colocation and probe-budget ablation. | [Join modes](reproduction/paper/run_rhj.sh) · [Pipeline coordination](scripts/run_shared_sonic_agg.sh) |

## Archive sources

The publication layouts for the bars, access heatmaps, and producer–consumer flows come from `imcs_partition_results/produce_figure`.

| Data | Original batch or figure package |
| --- | --- |
| Memory latency | `results_4chips/matrix_ns.csv` from the dependent-load benchmark. |
| Read bandwidth | `die_bandwidth/results_20261018_150236/summary.csv` in the OLAP experiment tree. |
| Figs. 7–8 | `4numa_current_all22.csv` and `8numa_old_all22_reference.csv` in `results/baseline_numa_access/organized_20261109_161407/`. |
| Fig. 9 | `results/p1_p2_analysis_20261110_142603/matrices.csv`, selecting baseline rows. |
| Fig. 11 | `p2_6queries_abc_four_distinct_bars_shaded.zip`, with its plotted timings and remote-access labels. |
| Fig. 12 | `p2_8chip_npsadapt_refcolor_Q3_Q5_Q7.zip`, using the exported cells and hotspot annotations. |
| Fig. 13 | `p3_flow_Q3_Q7_Q8_Q9_compact.zip`, using the displayed throughput table and concurrent routes from `source_flow_geometry.pdf`. Replay measurements and the Q8 plan came from `results/stream_flow_contention/20261110_134146/`. |
| 2-chiplet Table II counters | `chiplet_cache_profile/trial_l3c_position_2chip_20261016_211400/` in the OLAP experiment tree. |
| 4/8-chiplet cache timeline | `results/cache_operator_modes_20261110/normalized.csv`. |
| Hotspot Splitting and Q5 coordination SQL | `results/imcs_optimization/8numa_modes_22_retest_dIyY54lG/`. |
