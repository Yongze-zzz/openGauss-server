# 算子访存统计与缓存跟踪

两套工具共用向量执行器入口和 Query ID。访存统计回答哪些执行芯粒访问了哪些数据页。缓存跟踪回答哪些线程窗口出现了较高的 LLC 读未命中，以及当时有哪些算子在运行。

| 工具 | 会话 GUC | 主要输出 |
| --- | --- | --- |
| 算子内访存 | `enable_intra_operator_data_access` | 扫描数据、算子状态和本地工作区的逻辑读写字节 |
| 算子间访存 | `enable_inter_operator_data_access` | 生产者到消费者的输入读取、批次交接及 Stream 复制 |
| 缓存跟踪 | `enable_cache_trace`、`cache_trace_interval_us` | 每线程窗口的 LLC 读 MPKI、计划节点独占墙钟时间、按 LLC 聚合的时间线 |

## 算子内与算子间访存

### 从执行器到日志

1. `VectorEngine` 在任一访存统计打开时创建 `OperatorDataAccessFrame`，用父子执行帧和批次列数组地址追踪输入的计划节点归属。子算子返回 `VectorBatch` 时记录交接量，父算子随后读该输入时才记录页级读取。异常路径清理执行帧。 [向量执行入口](../../src/gausskernel/runtime/vecexecutor/vecexecutor.cpp#L300) · [执行帧与批次归属](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp#L368)
2. 算子内的埋点覆盖 CU 扫描和延迟物化、表达式、向量暂存以及 Sonic Join、RHJ 和 Sonic Aggregate 的部分状态访问。JIT 输入与状态候选单独标记，表示依据输入和状态槽推算的访问。 [CU 扫描埋点](../../src/gausskernel/storage/cstore/cstore_am.cpp#L2362) · [访存操作、对象与方向](../../src/include/vecexecutor/operator_data_access.h#L21) · [JIT 与 CU 记账](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp#L584)
3. 算子间的埋点覆盖下游读取上游批次、本地批次拷贝的输入侧、Stream 发送端写共享缓冲以及接收端读取共享缓冲。记录保留 `plan_id`、`peer_plan_id`、`stream_id` 和两端 worker，可按算子边或 Stream 汇总。 [Stream 发送](../../src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp#L403) · [Stream 接收](../../src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp#L1034) · [批次与 Stream 复制记账](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp#L732)
4. 记账器将地址范围拆成页内片段，每批对去重后的页调用 `move_pages` 查询实际驻留节点，以 `getcpu` 快照记录执行节点。按操作、对象、读写方向和两端节点累计字节。执行结束时写出聚合后的 `operator_data_access` 记录和每个统计实例的 `end` 记录。 [页归属查询](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp#L154) · [日志字段](../../src/gausskernel/runtime/vecexecutor/operator_data_access.cpp#L773) · [执行结束](../../src/gausskernel/runtime/executor/execMain.cpp#L2098)

### 从日志到热力图

使用 `run_numa_access.sh 4|8` 接入已有实验实例。它支持 `ACCESS_MODE=baseline|nps|nps_adapt`，使用 4 NUMA／DOP96 或 8 NUMA／DOP168，默认 100GB `work_mem`。采集开启 Intra 和 Inter 统计，指定 Sonic，关闭 RHJ 和缓存跟踪。`RESET_LAYOUT=repopulate` 允许重建 IMCS 布局，可用 `RESET_LAYOUT=manual` 要求已有布局匹配。`--sql-only` 只生成 SQL。 [访存采集入口](../../run_numa_access.sh)

`analyze_numa_access.py` 先按日志偏移和 Query ID 提取，再检查重复记录及 `end` 覆盖。每次运行输出 `operations.csv`、按种类拆分的字节矩阵、`summary.json`、`intra_heatmap.png` 和 `inter_heatmap.png`。热图行是执行 NUMA，列是页归属 NUMA。对角线为本地，非对角线为远程。图中合计插桩读写与单独标记的 JIT 候选估计，`delivered` 交付量单列，未知归属留在 `excluded_bytes`。计算远程比例时使用已定位字节。 [日志提取与矩阵计算](../../scripts/analyze_numa_access.py) · 已有 P1/P2 结果和数据来源 (`results/p1_p2_analysis_20261110_142603/README.md`)

这个口径是**累计逻辑读写字节**。同一数据被多次读取会累计，Stream 的发送与接收也分别记账。硬件 DRAM、互连字节和访问延迟需要另行测量。代码覆盖限于已有向量路径埋点。 [现有分析口径](../../scripts/analyze_numa_access.py#L310)

## 缓存跟踪

### 从线程计数器到回放

1. `VecCacheTraceEnter/Leave` 跟随向量算子进入和退出，维护线程执行栈。栈顶切换时记录计划节点 ID、算子类型和 CPU，并累计该节点独占墙钟时间。上层调用下层时，时间交给当前栈顶。墙钟时间包含该节点内部的等待。 [算子栈与时间归属](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L917)
2. 每个参与线程打开用户态 perf 事件组，读取退役指令和 LLC 读未命中。按 NUMA 管理的采样线程异步读取计数器。常规间隔由 `cache_trace_interval_us` 控制，当前 SQL GUC 默认 **100,000 µs**。采样器以 **2,000 µs** 为探测周期，在算子切换附近按配额加密采样。服务端环境变量 `OG_CACHE_TRACE` 也可启用跟踪，`OG_CACHE_TRACE_INTERVAL_US` 存在时优先于会话间隔。 [perf 事件](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L332) · [采样调度](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L619) · [GUC 与环境变量](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L873)
3. 每个线程以 Query ID 和线程启动时间写出一份 v5 二进制，路径为 `$PGDATA/pg_log/cache_trace/cache_trace_q*_t*_s*.bin`。文件保存 PMU 快照和累计算子时长。`run_nps_cache_trace.sh` 先验证关闭组没有触发跟踪，再检查开启组的启动数、写出数、文件数和失败计数。每条查询保留 `qN_on/raw.tar.gz` 和 `trace_metadata.json`，后者记录实际计划节点与采集时的 CPU 到 LLC 映射。 [二进制输出](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L1158) · [采集和完整性检查](../../run_nps_cache_trace.sh)
4. `analyze_cache_timeline.py` 用相邻累计计数之差计算线程窗口的 `LLC-read MPKI = 1000 × LLC 读未命中增量 / 指令增量`。有效窗口要求两端计数有效、CPU 快照相同、指令增量为正，并通过 perf 运行时间覆盖检查。输出 `windows.csv`、`operators.csv`、`summary.json`、`topology.json` 和回放图。 [v5 解码与有效窗口](../../scripts/analyze_cache_timeline.py#L9)
5. `plot_cache_chiplets.py` 按采集时保存的 LLC 映射划分芯粒域。按采样窗口与归一化时间格的重叠比例分配指令、未命中和算子时间。芯粒 MPKI 由计数求和后相除。图中并列展示计划节点归类后的算子墙钟占比和线程窗口 MPKI。 [芯粒归并与回放](../../scripts/plot_cache_chiplets.py#L77)

使用 `run_nps_cache_trace.sh 4|8` 做 NPS 下采集，默认 `RHJ_MODE=sonic`，两种访存统计关闭。`CACHE_INTERVAL_US` 控制会话采样间隔。脚本默认要求已有 NPS 布局，使用 `RESET_LAYOUT=repopulate` 才允许重新 populate。该入口的 on/off 计时用于观察跟踪开销，每种模式每条查询各一次正式测量。 [NPS 缓存入口](../../run_nps_cache_trace.sh) · [SQL 生成](../../run_imcs_optimization.sh#L424)

历史归档可用归档里的 SQL 执行日志和 `analysis/topology.json` 补建元数据。以下命令在仓库根目录执行，`point` 指向历史采集下的 `qN_on`。

```bash
scratch=$(mktemp -d)
mkdir "$scratch/raw"
tar -xzf "$point/raw.tar.gz" -C "$scratch/raw"
python3 scripts/cache_trace_metadata.py --plan "$point/nps_q6_run1.log" \
  --topology "$point/analysis/topology.json" --output "$scratch/trace_metadata.json"
python3 scripts/analyze_cache_timeline.py "$scratch/raw" \
  --metadata "$scratch/trace_metadata.json" --output "$scratch/analysis"
```

以上以 Q6 的日志命名为例。直接绘制已有的旧分析 CSV 时，`plot_cache_chiplets.py --node-tags` 接收对应归档的 NodeTag 映射；新版采集从已记录的计划节点取得算子分类。 [论文数据及执行入口](../../reproduction/paper/README.md)

### 已有数据

| 批次 | 覆盖与配置 | 常规间隔 |
| --- | --- | --- |
| 4 NUMA 缓存跟踪 (`results/nps_cache_trace/comparison_20261109_191532_4JDfOk/4numa/summary.csv`) | 22 条查询，DOP96，NPS、Sonic、100GB | **20 ms** |
| 8 NUMA 缓存跟踪 (`results/nps_cache_trace/8numa_dop168_20261109_162829_HaDrlW/summary.csv`) | 22 条查询，DOP168，NPS、Sonic、100GB | **100 ms** |

两批查询均通过已有采集覆盖记录和有效窗口检查。常规间隔从每条查询的原始 v5 文件头核对，两批的探测周期均为 2 ms。已有总图选择了 **Q5、Q8、Q9、Q18**。归一化的时间格使用一致画法，原始采样间隔仍有差异，短暂峰值和细粒度算子切换的跨配置比较应保留这个条件。 已有总图 (`results/cache_operator_modes_20261110/overview.pdf`) · 重分箱数据 (`results/cache_operator_modes_20261110/normalized.csv`) · 绘图元数据 (`results/cache_operator_modes_20261110/figure_metadata.json`)

## 与优化实验的配置关系

- 访存图用访存统计，算子与 MPKI 回放用缓存跟踪，正式性能对照关闭三项采集开关。检查启动环境变量，避免 `OG_CACHE_TRACE` 覆盖会话关闭设置。 [会话开关](../../src/common/backend/utils/misc/guc/guc_sql.cpp) · [环境变量逻辑](../../src/gausskernel/runtime/vecexecutor/vec_cache_trace.cpp#L873)
- 历史批次使用各自的 Sonic、NPS 或 NPS+Adapt 配置。当前 `pipeline_adaptive` 的 RHJ 预算和部分探测路径在启用访存统计时会受保护条件影响，剖析前应核对实际计划和路径。 [共享 RHJ 预算保护条件](../../src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicradixjoin.cpp#L1567)
- 单条 Stream 的复制耗时、Buffer 到 Consumer 流向、排队和 PMU 对照有独立的诊断入口，可按需要接续两套主统计。 [Stream 复制采样](../../src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp#L588) · [Q8 Stream 回放](../../reproduction/paper/README.md#fig-13-stream-replay)
