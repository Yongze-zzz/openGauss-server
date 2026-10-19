# IMCS NUMA-Aware Partition Scans

Experimental code for *Rethinking Data Locality in Operator-Parallel OLAP on Chiplets-Based CPUs: [Experiments & Analysis]*.

The current code provides an in-memory column store (IMCS) partition-scan baseline and a NUMA-aware version. The NUMA-aware version implements the scan-side component of the existing intra-operator locality techniques studied in §4.2: placing partition data on NUMA nodes and assigning scan workers according to that placement.

The experiment compares ordinary IMCS partition assignment with NUMA-aware assignment and worker CPU affinity. Both modes use the same populated data, query inputs, and degree of parallelism (DOP).

| Mode          | `enable_imcs_partition_scan` | `enable_numa_aware_partition_scan` |
| ------------- | ------------------------------ | ------------------------------------ |
| IMCS baseline | `on`                         | `off`                              |
| IMCS-NUMA     | `on`                         | `on`                               |

The IMCS baseline assigns whole partitions to workers in round-robin order and inherits the existing Stream CPU affinity. IMCS-NUMA assigns partitions by their data NUMA node, then round-robin among workers on that node. It binds scan workers to configured CPUs and restores their previous affinity at task end.

Both modes share partition iteration, partition switching, and complete CU/delta scanning. Build once and populate once with a NUMA layout; session GUCs select the scan mode without recompilation. Holding data placement fixed isolates the effect of scan scheduling and worker affinity.

The current implementation supports local, single-node, non-shared scans over one-level partitions with DOP greater than one. NUMA data placement uses local IMCS memory with borrowed memory disabled.

## Experiment files

| File                                        | Purpose                                                                                      |
| ------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [build_test_imcs.sh](build_test_imcs.sh)     | Compile and install; initialize the database; configure and start the instance               |
| [prepare_tpch_imcs.sh](prepare_tpch_imcs.sh) | Generate TPC-H data; create tables; load and analyze data; populate IMCS; inspect completion |
| [run_imcs_queries.sh](run_imcs_queries.sh)   | Run IMCS, IMCS-NUMA, or a paired comparison; save plans, query output, and timings           |
| [SQL/queries](SQL/queries)                   | TPC-H query inputs                                                                           |

## 1. Experimental setup

The paper evaluates openGauss RC7.0.0 with TPC-H SF100 on a Kunpeng 920 server: four sockets, eight NUMA nodes, 192 physical cores, and 1 TiB of memory. The commands below use SF100, 192 partitions, and DOP 192. Change the CPU lists, parallelism, and memory settings to match your host.

Use a Linux NUMA host, the openGauss third-party binary libraries for its architecture, GCC 10.3, CMake 3.16.5 or later, OpenJDK 11, and the libnuma and libxml2 development dependencies. TPC-H `dbgen` must be available in the tool directory below.

Run the following from the repository root, in the same shell throughout the experiment. Set the first three paths to your local directories. These exported values override the scripts' machine-specific defaults.

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

Set the scale factor and partition count before creating the dataset. Keep these settings the same for both scan modes.

## 2. Build and start

```bash
bash build_test_imcs.sh build
bash build_test_imcs.sh init
bash build_test_imcs.sh start
```

`build` enables HTAP and NUMA in a single-node CMake build and installs into `INSTALL_DIR`. `init` calls `gs_initdb`, prompts for an administrator password, and reuses an existing initialized data directory. `start` writes benchmark settings to `PGDATA/postgresql.conf`, starts the instance, and creates `DB_NAME` if needed. `bash build_test_imcs.sh all` runs these three steps in order.

The script currently uses the following resource defaults. Override them through the listed environment variables before starting a smaller instance.

| Variable                    | Script default         | Database setting            |
| --------------------------- | ---------------------- | --------------------------- |
| `IMCS_CACHE`              | `256GB`              | `max_imcs_cache`          |
| `MAX_PROCESS_MEMORY`      | `600GB`              | `max_process_memory`      |
| `SHARED_BUFFERS`          | `50GB`               | `shared_buffers`          |
| `WORK_MEM`                | `50GB`               | `work_mem`                |
| `THREAD_POOL_ATTR`        | `192,1,(nobind)`     | `thread_pool_attr`        |
| `THREAD_POOL_STREAM_ATTR` | `192,0.2,1,(nobind)` | `thread_pool_stream_attr` |

Both scan modes use this same database configuration. The NUMA mode applies worker affinity during execution.

## 3. Prepare data and NUMA placement

### Generate, create, and load

```bash
bash prepare_tpch_imcs.sh generate
bash prepare_tpch_imcs.sh create
bash prepare_tpch_imcs.sh load
```

If the eight `.tbl` files already exist, point `DATA_DIR` at their directory and skip `generate`. The script creates eight row-store tables and builds an IMCS representation during populate. `customer`, `part`, `partsupp`, `orders`, and `lineitem` each receive `PARTITIONS` hash partitions; `region`, `nation`, and `supplier` remain unpartitioned. Loading also runs `ANALYZE`.

### Choose NUMA nodes and scan CPUs

```bash
lscpu -e=CPU,NODE,ONLINE
numactl --hardware
```

The following example assumes CPUs 0-191 and NUMA nodes 0-7. Replace both lists with IDs available to your database process. To measure local scheduling, include CPUs from every data node and use a DOP that represents those nodes.

```bash
export NUMA_NODES=0,1,2,3,4,5,6,7
export IMCS_PARTITION_SCAN_CPUS="$(seq -s, 0 191)"
```

### Build the populate map

`IMCS_PARTITION_NUMA_MAP` supplies `relation_oid:numa_node` pairs to the populate session. Generate it after table creation: OIDs depend on the current database. The query below covers all eight tables and their leaf partitions, distributing objects across the selected nodes in OID order.

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

Set this map before the first populate. It controls CU data placement; changing a scan GUC does not move already populated data. A different data layout requires unpopulating and populating the tables again.

### Populate and inspect completion

```bash
export POPULATE_WORKERS=64
bash prepare_tpch_imcs.sh populate
bash prepare_tpch_imcs.sh status
```

The status command reads `pg_catalog.query_imcstore_views()`. Each table must have `populated = complete = expected`, with `root_status = COMPLETE`. At 192 partitions, `expected` is 193 for each partitioned table and 1 for each unpartitioned table. The output also reports CU counts in memory and on disk.

Use the same populated data for both scan modes. This comparison measures partition scheduling and worker affinity with data placement held fixed. An IMCS-only run may use an empty populate map.

## 4. Run the IMCS baseline and NUMA-aware scan

### IMCS baseline

Start with Q1 and Q6, which exercise scans and aggregation:

```bash
export QUERIES="1 6"
OUTPUT_DIR="$PWD/results/imcs-baseline" bash run_imcs_queries.sh partition
```

The baseline enables `enable_imcs_partition_scan` and disables `enable_numa_aware_partition_scan`. It reads each assigned partition completely, without changing worker affinity for NUMA-local scanning.

### NUMA-aware IMCS

Use the data populated in step 3 and the same query set and DOP:

```bash
OUTPUT_DIR="$PWD/results/imcs-numa" bash run_imcs_queries.sh numa
```

This mode enables both scan switches and sets `imcs_partition_scan_cpus` from `IMCS_PARTITION_SCAN_CPUS`.

### Paired comparison

```bash
export OUTPUT_DIR="$PWD/results/imcs-$(date +%Y%m%d_%H%M%S)"
bash run_imcs_queries.sh test
```

For each query, the driver first warms up both modes with `EXPLAIN ANALYZE`. Each measured repetition then runs IMCS followed by IMCS-NUMA. `RUNS=3` produces three measured executions per query and mode.

The memory-level characterization in §4.1 uses Q1, Q6, Q8, Q9, Q14, and Q21. To run this query set with the two scan modes:

```bash
QUERIES="1 6 8 9 14 21" OUTPUT_DIR="$PWD/results/imcs-six-queries" \
    bash run_imcs_queries.sh test
```

The driver sets `enable_vector_engine = on`, `enable_imcsscan = on`, `enable_cstore_partition_lane_scan = off`, and the requested `query_dop` in each query session. `enable_imcsscan` allows IMCS plans; `enable_imcs_partition_scan` selects whole-partition assignment; `enable_numa_aware_partition_scan` adds NUMA-aware assignment and CPU binding. Both experimental switches default to `off`; IMCS-NUMA requires both to be `on`.

| GUC                          | When to set it            | Purpose                                                             |
| ---------------------------- | ------------------------- | ------------------------------------------------------------------- |
| `imcs_partition_numa_map`  | Before populate           | Place each mapped relation's CU buffers on its data NUMA node       |
| `imcs_partition_scan_cpus` | Before an IMCS-NUMA query | Ordered, distinct CPU IDs separated by commas; at least DOP entries |

The preparation and query scripts set these GUCs from `IMCS_PARTITION_NUMA_MAP` and `IMCS_PARTITION_SCAN_CPUS`, respectively. CPU IDs must be in the server's affinity mask. A partition whose node has no assigned worker uses the implementation's deterministic round-robin assignment.

## 5. Read the results

The driver saves the following under `OUTPUT_DIR`:

| Output                                          | Contents                                       |
| ----------------------------------------------- | ---------------------------------------------- |
| `summary.csv`                                 | `mode,query,run,elapsed_ms,log`              |
| `partition/qN_plan.log`, `numa/qN_plan.log` | Warm-up query plans and execution statistics   |
| `partition/qN_R.log`, `numa/qN_R.log`       | Query output and elapsed time for repetition R |

`elapsed_ms` is the SELECT time reported by `gsql`; warm-up timings are kept in plan logs. Compare query output between modes and inspect the plans for IMCS scans, partition iterators, and actual DOP. For each query, summarize the repeated timings separately for `partition` and `numa`; a speedup can be computed as median IMCS time divided by median IMCS-NUMA time.

## Implementation

| Component                                   | Code                                                                                                                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Partition iteration and assignment          | [vecpartiterator.cpp](src/gausskernel/runtime/vecexecutor/vecnode/vecpartiterator.cpp), [imcs_partition_assignment.h](src/include/access/htap/imcs_partition_assignment.h) |
| NUMA map, CU placement, and worker affinity | [imcs_partition_wise.cpp](src/gausskernel/storage/htap/imcs_partition_wise.cpp)                                                                                           |
| Populate integration                        | [imcs_ctlg.cpp](src/gausskernel/storage/htap/imcs_ctlg.cpp), [imcucache_mgr.cpp](src/gausskernel/storage/htap/imcucache_mgr.cpp)                                           |
| Complete CU/delta scan path                 | [imcstore_am.cpp](src/gausskernel/storage/htap/imcstore_am.cpp), [cstore_am.cpp](src/gausskernel/storage/cstore/cstore_am.cpp)                                             |
| Stream affinity cleanup                     | [streamMain.cpp](src/gausskernel/process/stream/streamMain.cpp)                                                                                                           |
| Session GUC registration and propagation    | [guc_sql.cpp](src/common/backend/utils/misc/guc/guc_sql.cpp), [guc.cpp](src/common/backend/utils/misc/guc.cpp)                                                             |

## Other experiments

Code for the remaining memory-level and cache-level experiments is being organized and will be uploaded soon.

## License

See [License](License) and [COPYRIGHT](COPYRIGHT) for the openGauss license and notices.
