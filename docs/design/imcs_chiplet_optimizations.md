# IMCS 芯粒优化、开关与已有收益

当前组合是 `scripts/run_shared_sonic_agg.sh` 的 `pipeline_adaptive`。HotspotSplit 指原 Buffer Locality 的生产者侧放置。Stream 通道缓冲以生产者所在芯粒为放置单位，在生产者完成 NUMA 绑定后首次触页，使分散在多个芯粒的生产者分别承载各自缓冲。这个机制保留哈希路由、分区所有权和消费者位置。

> HotspotSplit distributes inter-operator exchange buffers across producer chiplets. Each producer first-touches its channel buffers after NUMA binding, so buffer placement follows the distribution of producers. The exchange preserves the existing partition routing and consumer placement.

## 算子内优化

### 内存访问

| 优化 | GUC | 作用与已有收益 |
| --- | --- | --- |
| NPS | `enable_imcs_nps`、`enable_imcs_partition_scan`、`enable_numa_aware_partition_scan` | 按分区归属放置 IMCS 数据并分配扫描 worker。同批 8 NUMA 下，22 条查询相对 Baseline 的几何平均加速比 **1.077×**。运行需要事先按 NPS 布局 populate。 |
| CU 扫描均衡 | `enable_imcs_nps_cu_balance` | 在保留 NPS 归属的前提下均衡 worker 的 CU 任务。Q8 的中位耗时从 **2.881 s 降到 2.414 s**，下降 **16.2%**，12 对中 11 对变快。 |

22 查询逐级消融 (`results/imcs_optimization/8numa_modes_22_retest_dIyY54lG/mode_summary.csv`) · Q8 CU 均衡 (`results/imcs_optimization/q8_cu_balance_e9Rugdjg/summary.csv`)

### 缓存与算子状态

| 优化和 GUC | 作用与已有收益 |
| --- | --- |
| 共享 RHJ，`enable_radix_hashjoin`、`radix_shared_build` | 同一 LLC 域共享构建出的 Join 哈希分区，worker 各自保留探测工作区。22 条查询相对 Private RHJ 的几何平均加速 **1.078×**。Q8 相对 Private RHJ 耗时下降 **18.4%**，相对 Sonic 下降 **29.7%**。 |
| 共享 Sonic Hash Aggregate，`enable_sonic_hashagg`、`enable_imcs_shared_sonic_agg` | 同一 LLC 域共享分片聚合状态，同一分片独占更新，不同分片可并行。开启后，已确认进入共享路径的 Q21 耗时下降 **20.7%**，Q16 下降 **5.0%**。同一开关也启用合格的分区局部聚合改写，Q18 的 **29.2%** 归到这条计划路径。 |

Join 消融 (`results/imcs_optimization/8numa_modes_22_retest_dIyY54lG/per_query.csv`) · 聚合计时和触发节点 (`results/imcs_optimization/8numa_partition_agg_q18_cdaN8NaU/comparison.csv`) · 聚合状态统计 (`results/imcs_optimization/8numa_partition_agg_q18_cdaN8NaU/operators.csv`)

## 算子间优化

### 内存访问与数据交换

| 优化 | GUC | 作用与已有收益 |
| --- | --- | --- |
| HotspotSplit | `enable_imcs_stream_hotspot_split` | 按生产者芯粒拆分通道缓冲的放置。在同一批 22 条查询上，对比仅 NPS 的几何平均加速比为 **1.282×**，17 条变快。Q5 耗时从 **22.900 s 降到 11.549 s**，下降 **49.6%**。这组实验的可比指标是查询耗时。 |
| Stream 双槽 | `enable_imcs_stream_batch_ring`、`imcs_stream_batch_ring_extra_slots` | 合格的本地向量重分布在背压时可启用第二槽，缓解批次交付等待。Q8 在 CU 均衡基础上从 **2.414 s 降到 2.321 s**，再降 **3.9%**。 |

HotspotSplit 逐查询数据 (`results/imcs_optimization/8numa_modes_22_retest_dIyY54lG/per_query.csv`) · Stream 双槽增量 (`results/imcs_optimization/q8_cu_balance_e9Rugdjg/summary.csv`)

### 缓存协调

| 优化和 GUC | 作用与已有收益 |
| --- | --- |
| 空间协调（扩展同址），`imcs_chiplet_exchange_mode = 'pipeline_colocation'` | 沿 Join 流水线保留共分区数据的 owner，使有关阶段在同一执行 lane 上推进，为同一 LLC 内的中间数据复用创造条件。Q5 省掉 `orders` 中间结果广播，减少约 **3,642 万行**逻辑交付。保持相同 RHJ 预算时，相对原同址模式两轮再降 **40.8%–42.1%**。其中有多少来自 LLC 复用仍需独立量化。 |
| 容量协调（动态 LLC 预算和缓冲复用），`imcs_chiplet_probe_tile_policy = 'adaptive_budget_reuse'`、`radix_partition_size_mb` | 按 LLC 容量及同域活跃 Join／Aggregate 状态调整 RHJ 探测暂存量并复用缓冲。Q5 在扩展同址上加用这组设置及 12 MiB 分区目标，从 **3.940 s 降到 3.763 s**，再降 **4.5%**。 |

Q5 同址与 LLC 预算计时 (`results/imcs_optimization/8numa_chiplet_pipeline_243gMyPy/comparison.csv`) · Q5 广播行数 (`results/imcs_optimization/8numa_chiplet_pipeline_243gMyPy/q5_plan_evidence.csv`)

以上收益采用各自实验的对照组，按组内查询耗时中位数计算。全套查询的加速比使用逐查询加速比的几何平均。Q5 扩展同址加动态预算的共同对照已测得 **1.79–1.81×**，该对照的底座已包含 NPS、HotspotSplit、共享 RHJ、CU 均衡、双槽和共享 Aggregate。 组合消融 (`results/imcs_optimization/8numa_chiplet_pipeline_243gMyPy/comparison.csv`)

访存热图和缓存时间线的执行器埋点、采集脚本与数据口径另见 [算子访存统计与缓存跟踪](../analysis/imcs_access_and_cache_tracing.md)。

## 当前设置

```sql
SET enable_imcs_partition_scan = on;
SET enable_numa_aware_partition_scan = on;
SET enable_imcs_nps = on;
SET enable_imcs_nps_cu_balance = on;
SET enable_sonic_hashjoin = on;
SET enable_radix_hashjoin = on;
SET radix_shared_build = on;
SET enable_sonic_hashagg = on;
SET enable_imcs_shared_sonic_agg = on;

SET enable_imcs_stream_hotspot_split = on;
SET enable_imcs_stream_batch_ring = on;
SET imcs_stream_batch_ring_extra_slots = 16;
SET imcs_chiplet_exchange_mode = 'pipeline_colocation';
SET imcs_chiplet_probe_tile_policy = 'adaptive_budget_reuse';
SET radix_partition_size_mb = 12;
SET imcs_chiplet_stage_cpus = '';
```

`run_imcs_optimization.sh` 使用 `STREAM_HOTSPOT_SPLIT=on`。旧 SQL 的 `enable_imcs_stream_buffer_locality` 和旧脚本环境变量 `STREAM_BUFFER_LOCALITY` 都作为兼容别名。数字大小参数表示实际容量，执行策略和缓存策略使用英文值。

当前配置关闭 Adapt、Stream 消费者迁移和路径重排、旧版 Stream 批次协调 `enable_imcs_cache_coord`、Aggregate 窗口协调、缓存追踪和 Intra／Inter 访存统计。独立的 Aggregate 短窗口协调使用 `enable_imcs_agg_cache_window`，Q21 两轮各省 **3.2%–4.2%**。`broadcast_isolation` 是独立的芯粒分离实验，固定生产者数量后四 LLC 放置相比八 LLC 最多再省 **0.32%**。 窗口实验 (`results/imcs_optimization/8numa_chiplet_probe_retention_hHCO3wXh/comparison.csv`) · 分离实验 (`results/imcs_optimization/8numa_chiplet_sender_isolation_keaQoTEG/comparison.csv`)

## GUC 英文模式

| `imcs_chiplet_exchange_mode` | 行为 |
| --- | --- |
| `off` | 保留原始计划 |
| `owner_handoff` | 沿分区 owner 交接 |
| `selective_build` | 在适用的同址 Join 上用选择性聚合结果构建哈希表 |
| `in_lane_aggregate` | 将适用的聚合与 Join 放到同一执行 lane |
| `pipeline_colocation` | 沿 Join 流水线保留分区 owner |
| `broadcast_isolation` | 缩编合格的小表广播端，由 `imcs_chiplet_stage_cpus` 指定放置 |

| `imcs_chiplet_probe_tile_policy` | 行为 |
| --- | --- |
| `off` | 保留原探测暂存策略 |
| `fixed_budget` | 按 LLC 和共享哈希分区大小设置预算 |
| `aggregate_aware` | 将同域活跃聚合状态计入预算 |
| `peer_aware` | 将同域活跃聚合及其他共享 RHJ 状态计入预算 |
| `fixed_budget_reuse` | 固定预算并复用批次缓冲 |
| `adaptive_budget_reuse` | 感知同域压力，平滑调整批量并复用批次缓冲 |

这两个模式 GUC 的出厂默认值为 `off`，历史数字写法作为兼容别名，`SHOW` 返回英文名称。
