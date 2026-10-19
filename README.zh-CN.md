# NUMA 系统上的 IMCS 分区扫描实验

本仓库在内存列存储（IMCS）中实现两种分区扫描方式，用于比较分区任务分配和 worker CPU 绑定对 NUMA 机器上查询执行时间的影响。

| 模式                | `enable_imcs_partition_scan` | `enable_numa_aware_partition_scan` |
| ------------------- | ------------------------------ | ------------------------------------ |
| **IMCS**      | `on`                         | `off`                              |
| **IMCS-NUMA** | `on`                         | `on`                               |

IMCS 将整分区轮转分配给 worker，沿用 Stream 原有 CPU 亲和性。IMCS-NUMA 先按数据 NUMA 节点分组，再在节点内轮转分配给 worker；扫描 worker 绑定到指定 CPU，任务结束后恢复原有亲和性。

两种模式共用分区遍历、分区切换和完整 CU/delta 扫描路径。编译一次，按 NUMA 布局 populate 一次，然后在同一份数据上通过会话 GUC 切换扫描模式。

当前实现支持本地、单节点、非共享扫描，以及 DOP 大于 1 的一级分区表。NUMA 数据放置要求使用本地 IMCS 内存，并关闭借用内存。

## 实验入口

| 文件                                        | 功能                                                                   |
| ------------------------------------------- | ---------------------------------------------------------------------- |
| [build_test_imcs.sh](build_test_imcs.sh)     | 编译安装、初始化数据库、配置并启动实例                                 |
| [prepare_tpch_imcs.sh](prepare_tpch_imcs.sh) | 生成 TPC-H 数据、建表、导入并分析数据、populate、检查完成状态          |
| [run_imcs_queries.sh](run_imcs_queries.sh)   | 分别运行 IMCS、IMCS-NUMA，或交替对比两种模式；保存计划、查询输出和耗时 |
| [SQL/queries](SQL/queries)                   | TPC-H 查询文件                                                         |

## 1. 配置环境

需要 Linux NUMA 机器、匹配架构的 openGauss 第三方依赖包、GCC 10.3、CMake 3.16.5 或更新版本、OpenJDK 11，以及 libnuma、libxml2 开发依赖。TPC-H `dbgen` 应位于下方指定的工具目录。通用编译依赖可参考 [openGauss 原版编译说明](README.openGauss.md#编译)。

在仓库根目录执行以下命令，并在后续步骤中使用同一个 shell。前三个路径按实际环境修改；显式导出变量可以覆盖脚本中原有的机器路径。

```bash
export BINARYLIBS="$HOME/binarylibs"
export TPCH_ROOT="$HOME/TPCH-openGauss-TestTools"
export PGDATA="$HOME/gauss-data"

export SCALE_FACTOR=100
export PARTITIONS=192
export DOP=192
export RUNS=3
export DB_NAME=tpch
export PORT=19999
export SCHEMA="tpch_sf${SCALE_FACTOR//./_}_imcs_h${PARTITIONS}"
export DBGEN_DIR="$TPCH_ROOT/TPC-H_Tools_v3.0.0/dbgen"
export DATA_DIR="$HOME/tpch-data/sf${SCALE_FACTOR}"

export INSTALL_DIR="$PWD/mppdb_temp_install"
export GCC_PATH="$BINARYLIBS/buildtools/gcc10.3"
export GAUSSHOME="$INSTALL_DIR"
export GSQL_BIN="$INSTALL_DIR/bin/gsql"
export PATH="$INSTALL_DIR/bin:$GCC_PATH/gcc/bin:$PATH"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$GCC_PATH/gcc/lib64:$BINARYLIBS/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
```

示例使用 SF100 和 192 个分区。建表前可以调整数据规模、分区数、DOP 和内存配置。如果继续使用已有实例，将 `PGDATA` 设置为该实例的实际目录。

## 2. 编译并启动数据库

```bash
bash build_test_imcs.sh build
bash build_test_imcs.sh init
bash build_test_imcs.sh start
```

`build` 使用单节点 CMake 构建，启用 HTAP 和 NUMA，并安装到 `INSTALL_DIR`。`init` 调用 `gs_initdb`，交互设置管理员密码；若目录已经初始化，则继续使用已有目录。`start` 向 `PGDATA/postgresql.conf` 写入实验配置，启动实例，并在需要时创建 `DB_NAME` 数据库。也可以用 `bash build_test_imcs.sh all` 按顺序执行这三步。

脚本当前使用以下资源默认值。在较小机器上运行前，通过对应环境变量调整。

| 环境变量                    | 脚本默认值             | 数据库参数                  |
| --------------------------- | ---------------------- | --------------------------- |
| `IMCS_CACHE`              | `256GB`              | `max_imcs_cache`          |
| `MAX_PROCESS_MEMORY`      | `600GB`              | `max_process_memory`      |
| `SHARED_BUFFERS`          | `50GB`               | `shared_buffers`          |
| `WORK_MEM`                | `50GB`               | `work_mem`                |
| `THREAD_POOL_ATTR`        | `192,1,(nobind)`     | `thread_pool_attr`        |
| `THREAD_POOL_STREAM_ATTR` | `192,0.2,1,(nobind)` | `thread_pool_stream_attr` |

两种扫描模式使用同一套数据库配置，IMCS-NUMA 在执行时设置扫描 worker 的 CPU 亲和性。

## 3. 准备数据和 NUMA 布局

### 生成、建表和导入

```bash
bash prepare_tpch_imcs.sh generate
bash prepare_tpch_imcs.sh create
bash prepare_tpch_imcs.sh load
```

如果已经有八张表的 `.tbl` 文件，将 `DATA_DIR` 指向这些文件所在目录，并跳过 `generate`。脚本创建八张行存表，随后通过 populate 建立 IMCS 表示。`customer`、`part`、`partsupp`、`orders`、`lineitem` 各有 `PARTITIONS` 个 HASH 分区；`region`、`nation`、`supplier` 不分区。数据导入后会执行 `ANALYZE`。

### 选择数据节点和扫描 CPU

```bash
lscpu -e=CPU,NODE,ONLINE
numactl --hardware
```

下面以 CPU 0-191、NUMA 节点 0-7 为例。按数据库进程实际可用的节点和 CPU 修改两个列表。要比较本地调度效果，扫描 CPU 应覆盖数据映射使用的节点，DOP 也应能在这些节点上分配到 worker。

```bash
export NUMA_NODES=0,1,2,3,4,5,6,7
export IMCS_PARTITION_SCAN_CPUS="$(seq -s, 0 191)"
```

### 生成 populate 映射

`IMCS_PARTITION_NUMA_MAP` 将 `relation_oid:numa_node` 列表传给 populate 会话。OID 来自当前数据库，因此在建表后生成。下面的查询覆盖八张表及其叶子分区，按 OID 顺序轮转分配到指定 NUMA 节点。

```bash
export IMCS_PARTITION_NUMA_MAP=$(
    "$GSQL_BIN" -X -q -A -t -v ON_ERROR_STOP=1 \
        -d "$DB_NAME" -p "$PORT" -v "schema=$SCHEMA" -v "nodes=$NUMA_NODES" <<'SQL'
WITH tables AS (
    SELECT c.oid
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :'schema'
      AND c.relname IN ('region', 'nation', 'supplier', 'customer',
                       'part', 'partsupp', 'orders', 'lineitem')
), objects AS (
    SELECT oid FROM tables
    UNION ALL
    SELECT p.oid FROM pg_partition p JOIN tables t ON p.parentid = t.oid
    WHERE p.parttype = 'p'
), numbered AS (
    SELECT oid, row_number() OVER (ORDER BY oid) - 1 AS ordinal FROM objects
), nodes AS (
    SELECT string_to_array(:'nodes', ',')::int[] AS ids
)
SELECT string_agg(oid::text || ':' ||
    ids[1 + (ordinal % array_length(ids, 1))::int]::text, ',' ORDER BY oid)
FROM numbered CROSS JOIN nodes;
SQL
)
```

首次 populate 前设置此映射，它决定 CU 数据缓冲区放在哪个节点。查询阶段切换扫描 GUC 不会搬动已有数据；修改数据布局需要先 unpopulate，再重新 populate。

### Populate 并检查状态

```bash
export POPULATE_WORKERS=64
bash prepare_tpch_imcs.sh populate
bash prepare_tpch_imcs.sh status
```

状态检查读取 `pg_catalog.query_imcstore_views()`。每张表应满足 `populated = complete = expected`，且 `root_status = COMPLETE`。192 个分区时，每张分区表的 `expected` 为 193，非分区表为 1。输出同时包含内存和磁盘中的 CU 数量。

两种扫描模式共用上述已 populate 的数据，使对比中的数据放置保持一致，只改变分区调度和 worker 亲和性。如果只运行普通 IMCS 分区扫描，可以不设置 NUMA populate 映射。

## 4. 运行对比

默认查询为 Q1 和 Q6。脚本先对每条查询的两种模式分别执行一次 `EXPLAIN ANALYZE` 预热，再在每轮计时中依次运行 IMCS 和 IMCS-NUMA。

```bash
export QUERIES="1 6"
export OUTPUT_DIR="$PWD/results/imcs-$(date +%Y%m%d_%H%M%S)"
bash run_imcs_queries.sh test
```

也可以分别运行两种模式：

```bash
OUTPUT_DIR="$PWD/results/imcs-only" bash run_imcs_queries.sh partition
OUTPUT_DIR="$PWD/results/imcs-numa-only" bash run_imcs_queries.sh numa
```

选择全部 22 个查询文件：

```bash
QUERIES="$(seq -s ' ' 1 22)" bash run_imcs_queries.sh test
```

脚本在每个查询会话中设置 `enable_vector_engine = on`、`enable_imcsscan = on`、`enable_cstore_partition_lane_scan = off` 和指定的 `query_dop`，再按首页对照表设置两个实验开关。两个实验开关默认均为 `off`；IMCS-NUMA 要求二者同时为 `on`。

| GUC                          | 设置阶段         | 功能                                                    |
| ---------------------------- | ---------------- | ------------------------------------------------------- |
| `imcs_partition_numa_map`  | Populate 前      | 将映射关系中的 CU 缓冲区放到指定数据节点                |
| `imcs_partition_scan_cpus` | IMCS-NUMA 查询前 | 有序、互不重复的 CPU 编号，使用逗号分隔，数量至少为 DOP |

准备脚本和查询脚本分别从 `IMCS_PARTITION_NUMA_MAP`、`IMCS_PARTITION_SCAN_CPUS` 设置这两个 GUC。CPU 必须位于服务器允许的亲和性范围内；如果某个数据节点没有分配到 worker，该节点的分区按实现中的确定性轮转规则分配。

## 5. 查看结果

`OUTPUT_DIR` 下包含：

| 输出文件                                        | 内容                              |
| ----------------------------------------------- | --------------------------------- |
| `summary.csv`                                 | `mode,query,run,elapsed_ms,log` |
| `partition/qN_plan.log`、`numa/qN_plan.log` | 预热查询的执行计划和执行统计      |
| `partition/qN_R.log`、`numa/qN_R.log`       | 第 R 次运行的查询输出和耗时       |

`elapsed_ms` 来自 `gsql` 报告的 SELECT 耗时，预热耗时单独保存在计划日志中。比较两种模式的查询结果，并确认计划中有 IMCS 扫描、分区迭代器和预期的实际 DOP。每条查询分别汇总 `partition`、`numa` 的重复运行时间，加速比可以用 IMCS 耗时中位数除以 IMCS-NUMA 耗时中位数计算。

## 代码入口

| 组件                             | 代码                                                                                                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 分区遍历与任务分配               | [vecpartiterator.cpp](src/gausskernel/runtime/vecexecutor/vecnode/vecpartiterator.cpp)、[imcs_partition_assignment.h](src/include/access/htap/imcs_partition_assignment.h) |
| NUMA 映射、CU 放置和 worker 绑定 | [imcs_partition_wise.cpp](src/gausskernel/storage/htap/imcs_partition_wise.cpp)                                                                                           |
| Populate 集成                    | [imcs_ctlg.cpp](src/gausskernel/storage/htap/imcs_ctlg.cpp)、[imcucache_mgr.cpp](src/gausskernel/storage/htap/imcucache_mgr.cpp)                                           |
| 完整 CU/delta 扫描               | [imcstore_am.cpp](src/gausskernel/storage/htap/imcstore_am.cpp)、[cstore_am.cpp](src/gausskernel/storage/cstore/cstore_am.cpp)                                             |
| Stream 亲和性恢复                | [streamMain.cpp](src/gausskernel/process/stream/streamMain.cpp)                                                                                                           |
| 会话 GUC 注册与传播              | [guc_sql.cpp](src/common/backend/utils/misc/guc/guc_sql.cpp)、[guc.cpp](src/common/backend/utils/misc/guc.cpp)                                                             |

## 上游项目与许可证

本实现基于 openGauss。原版项目说明已分别保存在[中文文档](README.openGauss.md)和[英文文档](README.openGauss.en.md)中。许可证和版权声明见 [License](License)、[COPYRIGHT](COPYRIGHT)。
