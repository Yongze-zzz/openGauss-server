# Experimental code for *Rethinking Data Locality in Operator-Parallel OLAP on Chiplets-Based CPUs: [Experiments & Analysis]*

This repository contains the openGauss code used to study data locality in operator-parallel OLAP. The current experiment set has two parts:

1. data-access instrumentation for intra- and inter-operator data access; and
2. an in-memory column store (IMCS) partition-scan baseline and a chiplet-aware partition-scan variant.

In our experiment, the IMCS modes use the same populated data, query inputs, and degree of parallelism. A session GUC selects the scan policy after one build. No recompilation is needed when switching between the baseline and chiplet-aware runs.

| Experiment                        | Main session switches                                                       | What it measures                                                                                                 |
| --------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Intra-operator data access       | `enable_intra_operator_data_access=on`                                    | Reads and writes to state owned by one operator, including Sonic HashAgg/HashJoin state                          |
| inter-operator data access        | `enable_inter_operator_data_access=on`                                    | Data handed from a producer operator to its consumer through vectors, batches, copies, and local Stream exchange |
| IMCS partition-scan baseline      | `enable_imcs_partition_scan=on`, `enable_numa_aware_partition_scan=off` | Whole-partition assignment using the existing worker placement                                                   |
| Chiplet-aware IMCS partition scan | `enable_imcs_partition_scan=on`, `enable_numa_aware_partition_scan=on`  | Partition assignment and worker CPU affinity guided by the NUMA node holding each partition                      |

## 1. Experimental setup

The reference evaluation uses openGauss RC7.0.0 with TPC-H SF100 on a Kunpeng 920 server: four sockets, eight NUMA nodes, 192 physical cores, and 1 TiB of memory. The commands below use 192 partitions and DOP 192. Change the CPU lists, scale factor, partition count, and DOP to match the machine under test.

The host should provide Linux NUMA support, the openGauss third-party binary libraries for its architecture, GCC 10.3, CMake 3.16.5 or later, OpenJDK 11, and the `libnuma` and `libxml2` development packages. TPC-H `dbgen` is expected in the tool directory referenced by `DBGEN_DIR`.

Run the commands from the repository root and keep these variables in the same shell. The first three paths are local installation choices; the scripts also provide machine-specific defaults.

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

Use the same values of `SCALE_FACTOR`, `PARTITIONS`, `DOP`, and `SCHEMA` for data preparation and for both scan modes. The data-access statistics are independent of the IMCS scan switch, so they can be enabled in either scan mode when the access pattern itself is the subject of the experiment.

## 2. Build and start the database

Build, initialize, and start the single-node instance as follows:

```bash
bash build_test_imcs.sh build
bash build_test_imcs.sh init
bash build_test_imcs.sh start
```

`build` enables the HTAP and NUMA components and installs the server into `INSTALL_DIR`. `init` runs `gs_initdb` and creates the data directory named by `PGDATA`. `start` installs the experiment configuration, starts the server, and creates `DB_NAME` if it does not exist. The combined command is:

```bash
bash build_test_imcs.sh all
```

The default data directory in the script is `/mnt/nvme1p1/gauss`. Set `PGDATA=/mnt/nvme1p1/gauss` explicitly when that is the desired location. `config/imcs_reference.conf` is the reference configuration used by the script; it contains the memory, code-generation, and thread-pool settings used for this experiment. Before replacing `postgresql.conf`, the script saves a `postgresql.conf.before_imcs.*` copy. Run only the configuration step with:

```bash
bash build_test_imcs.sh configure
```

The server is launched with `numactl` when `NUMACTL_ARGS` is non-empty. For example:

```bash
export DOP=48
export NUMACTL_ARGS="--physcpubind=0-47 --membind=0-1"
bash build_test_imcs.sh start
```

The default DOP 192 configuration uses `thread_pool_stream_attr='960,0.2'` and CPUs 0--191 on NUMA nodes 0--7. Set an empty `NUMACTL_ARGS` to disable the launcher. The binding must be applied to the database server; binding only the `gsql` client does not bind query workers.

## 3. Prepare data for TPC-H evaluation

### 3.1 Generate, create, and load the row-store tables

```bash
bash prepare_tpch_imcs.sh generate
bash prepare_tpch_imcs.sh create
bash prepare_tpch_imcs.sh load
```

`generate` invokes TPC-H `dbgen` and writes the eight `.tbl` files below `DATA_DIR`. If the files already exist, point `DATA_DIR` at that directory and skip this step. `create` creates the TPC-H tables in `SCHEMA`; `customer`, `part`, `partsupp`, `orders`, and `lineitem` are hash-partitioned, while `region`, `nation`, and `supplier` are unpartitioned. `load` uses `\copy` and runs `ANALYZE`.

### 3.2 Place IMCS partitions on NUMA nodes

Inspect the CPU and NUMA topology before choosing the placement:

```bash
lscpu -e=CPU,NODE,ONLINE
numactl --hardware
```

`IMCS_PARTITION_NUMA_MAP` is a comma-separated list of `relation_oid:numa_node` pairs. The following query builds a map for the eight TPC-H tables and their leaf partitions, distributing objects in OID order across the selected nodes. Run it after `create`, because relation OIDs are assigned by the current database.

```bash
export NUMA_NODES=0,1,2,3,4,5,6,7
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

### 3.3 Populate IMCS and verify completion

Set the map before the first populate. It controls the NUMA placement of CU buffers; changing a scan GUC later does not move existing data. Repopulate the tables after changing the map.

```bash
export POPULATE_WORKERS=64
bash prepare_tpch_imcs.sh populate
bash prepare_tpch_imcs.sh status
```

`status` reads `pg_catalog.query_imcstore_views()`. Every table must report `populated = complete = expected` and `root_status = COMPLETE`. With 192 partitions, a partitioned table normally has 193 IMCS objects including its root and an unpartitioned table has one. Use one populated data set for the baseline and chiplet-aware scans so that the scheduling policy is the only changed factor.

## 4. Experiments

### 4.1 Data-access statistics

The statistics layer is controlled by two independent GUCs. Both default to `off` and should be disabled for timing runs.

```sql
SET enable_intra_operator_data_access = on;
SET enable_inter_operator_data_access = on;
```

The instrumentation records logical byte ranges and the NUMA node containing the touched pages. It does not claim to be an LLC-miss or DRAM-bandwidth counter. Enable `enable_codegen = off` when the goal is to inspect ordinary C++ operator paths; JIT candidate records are still available when code generation is enabled.

#### 4.1.1 Intra-opperator data access

The operator-internal path measures memory owned and reused inside one operator. `VectorEngine()` creates an `OperatorDataAccessFrame` for the current `PlanState`. The frame owns reusable scopes for the operator state, expression evaluation, and JIT candidates. A scope accumulates calls, rows, logical bytes, and address ranges; it does not allocate or look up a statistics object for every row.

The Sonic hash operators use the scope directly. `vsonichash.cpp` resolves the data and NULL pages of a Sonic array. `vsonichashagg.cpp` records bucket reads/writes, next-link accesses, transition-state reads/writes, and JIT candidate state. `vsonichashjoin.cpp` records build bucket/next writes and probe-chain reads. A Sonic hash table is therefore observed at the points where it is accessed; the statistics pass does not traverse the entire hash table a second time.

For each observed address range, `OperatorDataAccessBuffer` hashes page addresses into a small in-query buffer. It sends the unique pages in the current batch to `move_pages()`, groups the returned locations by NUMA node and access role, and then flushes the buffer. The buffer is a batching mechanism, not a cap on the experiment: a full batch is flushed and the same scope continues with the next pages. Repeated reads remain repeated logical bytes, while page-location accounting is aggregated by page.

Run a query with only the intra-operator dimension enabled:

```sql
SET search_path TO tpch_sf100_imcs_h192;  -- replace with SCHEMA
SET enable_vector_engine = on;
SET enable_imcsscan = on;
SET enable_imcs_partition_scan = on;
SET enable_numa_aware_partition_scan = off;
SET enable_intra_operator_data_access = on;
SET enable_inter_operator_data_access = off;
SET enable_codegen = off;
\i SQL/queries/q1.sql
```

The server log entries use the `operator_data_access` prefix and identify the plan node, operator, object, access role, logical range, page NUMA node, calls, and rows. `DA_OPERATOR_STATE`, `DA_SONIC_AGG_TRANSITION`, and the Sonic hash roles are the main records for this dimension.

#### 4.1.2 Inter-operator data access

The inter-operator path follows a value from its producer to its consumer. When a plan node leaves `VectorEngine()`, the frame registers the result vector and its producer plan ID in the parent frame. A consumer read matches its vector and flag addresses to that producer record. The match remains valid across a shallow pointer handoff, so a consumer can be charged for data that was not deep-copied.

Copy and exchange paths are recorded separately. `OperatorDataAccessBatchCopy()` records source and destination vector slots and the copied payload. `OperatorDataAccessStreamCopy()` records local Stream send/receive handoff. `OperatorDataAccessReadBatch()` records a consumer reading an input batch. These records distinguish direct pointer reuse, a deep payload copy, and a Stream transfer; they are not collapsed into one generic copy event.

Run the same query with only the inter-operator dimension enabled:

```sql
SET search_path TO tpch_sf100_imcs_h192;  -- replace with SCHEMA
SET enable_vector_engine = on;
SET enable_imcsscan = on;
SET enable_imcs_partition_scan = on;
SET enable_numa_aware_partition_scan = off;
SET enable_intra_operator_data_access = off;
SET enable_inter_operator_data_access = on;
SET enable_codegen = off;
\i SQL/queries/q1.sql
```

Use `producer_plan_id`, `consumer_plan_id`, `DA_CONSUMER_INPUT`, `DA_BATCH_HANDOFF`, `DA_BATCH_COPY`, `DA_STREAM_SEND`, and `DA_STREAM_RECV` to group the result by an operator edge. This gives an operator-to-operator view even when the data remains in the same buffer and only its pointer is passed onward.

### 4.2 Memory-access optimization: IMCS and IMCS-Chiplet

The scan experiment keeps the populated IMCS data fixed and changes only partition assignment and worker affinity. Both modes use the same partition iterator, partition transition code, and complete CU/delta scan path.

#### 4.2.1 IMCS partition-scan baseline

```bash
export QUERIES="1 6"
OUTPUT_DIR="$PWD/results/imcs-baseline" bash run_imcs_queries.sh partition
```

The driver sets `enable_vector_engine = on`, `enable_imcsscan = on`, `enable_cstore_partition_lane_scan = off`, `enable_imcs_partition_scan = on`, and `enable_numa_aware_partition_scan = off`. The baseline assigns complete partitions in round-robin order and retains the existing Stream worker placement.

#### 4.2.2 Chiplet-aware IMCS partition scan

Set an ordered CPU list that is valid in the server's affinity mask. It must contain at least DOP entries.

```bash
export IMCS_PARTITION_SCAN_CPUS="$(seq -s, 0 191)"
OUTPUT_DIR="$PWD/results/imcs-numa" bash run_imcs_queries.sh numa
```

The chiplet-aware run enables both scan switches and sets `imcs_partition_scan_cpus`. It reads each partition's populated data NUMA node, assigns partitions to workers on that node, and restores the worker's previous affinity when the scan ends. This static version does not use a shared dynamic queue; its purpose is to isolate data placement and worker locality.

#### 4.2.3 Paired comparison

```bash
export QUERIES="1 6 8 9 14 21"
export OUTPUT_DIR="$PWD/results/imcs-$(date +%Y%m%d_%H%M%S)"
bash run_imcs_queries.sh test
```

`test` warms up both modes with `EXPLAIN ANALYZE`, then runs each query `RUNS` times in both modes. Keep `SCALE_FACTOR`, `PARTITIONS`, `DOP`, data placement, server CPU binding, and memory settings unchanged. The IMCS scan switches are independent of the two data-access statistics switches; leave the latter off for timing.

| GUC                                  | Set it                       | Role                                                                    |
| ------------------------------------ | ---------------------------- | ----------------------------------------------------------------------- |
| `imcs_partition_numa_map`          | Before `populate`          | Select the NUMA node for each mapped relation or partition's CU buffers |
| `imcs_partition_scan_cpus`         | Before an IMCS-Chiplet query | Provide the ordered CPU IDs used by chiplet-aware scan workers          |
| `enable_imcs_partition_scan`       | Query session                | Enable whole-partition IMCS assignment                                  |
| `enable_numa_aware_partition_scan` | Query session                | Select chiplet-aware assignment and worker affinity                     |

The current static implementation targets local, single-node, non-shared, one-level partition scans with DOP greater than one. It is intentionally separate from the later dynamic-queue and migration experiments.

## 5. Code map and scope

| Component                                                 | Main files                                                                                                                                                                                                                                                      |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data-access frame, scopes, page batching, and log records | [operator_data_access.cpp](src/gausskernel/runtime/vecexecutor/operator_data_access.cpp), [operator_data_access.h](src/include/vecexecutor/operator_data_access.h)                                                                                                |
| Executor and expression hooks                             | [vecexecutor.cpp](src/gausskernel/runtime/vecexecutor/vecexecutor.cpp), [vecexpression.cpp](src/gausskernel/runtime/vecexecutor/vecexpression.cpp), [vectorbatch.cpp](src/gausskernel/runtime/vecexecutor/vectorbatch.cpp)                                         |
| Sonic HashAgg and HashJoin access points                  | [vsonichash.cpp](src/gausskernel/runtime/vecexecutor/vectorsonic/vsonichash.cpp), [vsonichashagg.cpp](src/gausskernel/runtime/vecexecutor/vectorsonic/vsonichashagg.cpp), [vsonichashjoin.cpp](src/gausskernel/runtime/vecexecutor/vectorsonic/vsonichashjoin.cpp) |
| CU and IMCS access observation                            | [cstore_am.cpp](src/gausskernel/storage/cstore/cstore_am.cpp), [imcstore_am.cpp](src/gausskernel/storage/htap/imcstore_am.cpp)                                                                                                                                    |
| IMCS placement and chiplet-aware scan assignment          | [imcs_partition_wise.cpp](src/gausskernel/storage/htap/imcs_partition_wise.cpp), [imcs_partition_wise.h](src/include/access/htap/imcs_partition_wise.h), [vecpartiterator.cpp](src/gausskernel/runtime/vecexecutor/vecnode/vecpartiterator.cpp)                    |
| GUC registration                                          | [guc_sql.cpp](src/common/backend/utils/misc/guc/guc_sql.cpp), [knl_session_attr_sql.h](src/include/knl/knl_guc/knl_session_attr_sql.h)                                                                                                                            |
| Reproducibility scripts                                   | [build_test_imcs.sh](build_test_imcs.sh), [prepare_tpch_imcs.sh](prepare_tpch_imcs.sh), [run_imcs_queries.sh](run_imcs_queries.sh)                                                                                                                                 |

The data-access layer reports logical accesses and page locality. It does not replace hardware performance counters. The current IMCS-Chiplet experiment is a static partition assignment; dynamic shared queues, migration cost models, and other cache-level experiments remain separate work.

## License

See [License](License) and [COPYRIGHT](COPYRIGHT) for the openGauss license and notices.
