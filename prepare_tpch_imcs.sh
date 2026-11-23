#!/bin/bash
# Usage: ./prepare_tpch_imcs.sh generate|create|load|populate|status|all
# generate : generate tpch table data | create : create table partitions in database system | load : load table data to database | populate : load all column data into memory | all : one-shot flow  
set -eo pipefail

sf=${SCALE_FACTOR:-100}
partitions=${PARTITIONS:-192} # as core numbers
dbgen_dir=${DBGEN_DIR:-${TPCH_ROOT:-/mnt/nvme1p1/yanyz/TPCH-openGauss-TestTools}/TPC-H_Tools_v3.0.0/dbgen}
data_dir=${DATA_DIR:-/home/yanyz/olap/tpch-data/sf${sf}}
schema=${SCHEMA:-tpch_sf${sf//./_}_imcs_h${partitions}}
workers=${POPULATE_WORKERS:-64}
parallel_populate=${PARALLEL_POPULATE:-on}
[[ $parallel_populate == on || $parallel_populate == off ]] || { echo 'PARALLEL_POPULATE must be on or off.' >&2; exit 2; }
numa_map=${IMCS_PARTITION_NUMA_MAP:-}
nps=${ENABLE_IMCS_NPS:-off}
nps_groups=${IMCS_NPS_GROUPS:-}
scan_cpus=${IMCS_PARTITION_SCAN_CPUS:-}
tables=(region nation supplier customer part partsupp orders lineitem)
sql=("${GSQL_BIN:-gsql}" -X -v ON_ERROR_STOP=1 -P pager=off
     -d "${DB_NAME:-tpch}" -p "${PORT:-${PGPORT:-19999}}")

generate() {
    echo "生成 SF${sf} 数据：$data_dir"
    mkdir -p "$data_dir"
    local output_dir
    output_dir=$(cd "$data_dir" && pwd -P)
    (cd "$dbgen_dir" && DSS_PATH="$output_dir" ./dbgen -vf -s "$sf")
}

create() {
    echo "建表：$schema，五张大表各 $partitions 个 HASH 分区"
    [[ $partitions =~ ^[1-9][0-9]*$ ]] || { echo '分区数需为正整数' >&2; return 1; }
    local partition_list='PARTITION p0' i
    for ((i = 1; i < partitions; i++)); do
        partition_list+=", PARTITION p$i"
    done

    # 字段和主键沿用 createtab_og.sql；五张大表按主键首列分区。
    "${sql[@]}" -v "schema=$schema" -v "partitions=$partition_list" <<'SQL'
CREATE SCHEMA IF NOT EXISTS :"schema";
SET search_path TO :"schema";

CREATE TABLE region (
    r_regionkey INT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152),
    PRIMARY KEY (r_regionkey)
) WITH (orientation = row);
CREATE TABLE nation (
    n_nationkey INT NOT NULL, n_name CHAR(25) NOT NULL, n_regionkey INT NOT NULL,
    n_comment VARCHAR(152), PRIMARY KEY (n_nationkey)
) WITH (orientation = row);
CREATE TABLE supplier (
    s_suppkey BIGINT NOT NULL, s_name CHAR(25) NOT NULL, s_address VARCHAR(40) NOT NULL,
    s_nationkey INT NOT NULL, s_phone CHAR(15) NOT NULL, s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL, PRIMARY KEY (s_suppkey)
) WITH (orientation = row);
CREATE TABLE customer (
    c_custkey BIGINT NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL,
    c_nationkey INT NOT NULL, c_phone CHAR(15) NOT NULL, c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL, c_comment VARCHAR(117) NOT NULL, PRIMARY KEY (c_custkey)
) WITH (orientation = row) PARTITION BY HASH (c_custkey) (:partitions);
CREATE TABLE part (
    p_partkey BIGINT NOT NULL, p_name VARCHAR(100) NOT NULL, p_mfgr CHAR(100) NOT NULL,
    p_brand CHAR(20) NOT NULL, p_type VARCHAR(100) NOT NULL, p_size BIGINT NOT NULL,
    p_container CHAR(10) NOT NULL, p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL, PRIMARY KEY (p_partkey)
) WITH (orientation = row) PARTITION BY HASH (p_partkey) (:partitions);
CREATE TABLE partsupp (
    ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL, ps_comment VARCHAR(199) NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
) WITH (orientation = row) PARTITION BY HASH (ps_partkey) (:partitions);
CREATE TABLE orders (
    o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL, o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL, o_clerk CHAR(15) NOT NULL,
    o_shippriority BIGINT NOT NULL, o_comment VARCHAR(79) NOT NULL, PRIMARY KEY (o_orderkey)
) WITH (orientation = row) PARTITION BY HASH (o_orderkey) (:partitions);
CREATE TABLE lineitem (
    l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL,
    l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL, l_returnflag CHAR(1) NOT NULL, l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL, l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL, PRIMARY KEY (l_orderkey, l_linenumber)
) WITH (orientation = row) PARTITION BY HASH (l_orderkey) (:partitions);
SQL
}

load() {
    local table
    for table in "${tables[@]}"; do
        echo "导入 $schema.$table"
        sed 's/|$//' "$data_dir/$table.tbl" |
            "${sql[@]}" -c "\copy \"$schema\".$table FROM PSTDIN WITH (FORMAT csv, DELIMITER '|')"
        "${sql[@]}" -c "ANALYZE \"$schema\".$table;"
    done
}

populate() {
    local table
    {
        echo '\timing on'
        echo 'SET statement_timeout = 0;'
        echo "SET enable_parallel_populate = $parallel_populate;"
        echo "SET imcs_parallel_populate_workers = $workers;"
        # An empty map explicitly restores native placement, including when a
        # role/database default or PGOPTIONS supplied a nonempty mapping.
        echo "SET imcs_partition_numa_map = :'numa_map';"
        echo "SET enable_imcs_nps = :'nps';"
        echo "SET imcs_nps_groups = :'nps_groups';"
        echo "SET imcs_partition_scan_cpus = :'scan_cpus';"
        for table in "${tables[@]}"; do
            echo "ALTER TABLE :\"schema\".$table IMCSTORED;"
        done
    } | "${sql[@]}" -v "schema=$schema" -v "numa_map=$numa_map" \
        -v "nps=$nps" -v "nps_groups=$nps_groups" -v "scan_cpus=$scan_cpus"
    status
}

status() {
    # check base table and leaf table partitions
    "${sql[@]}" -v "schema=$schema" <<'SQL'
CREATE TEMP TABLE populate_status AS
WITH targets AS (
    SELECT t.table_name, c.oid AS relid,
           1 + (SELECT count(*) FROM pg_partition p
                WHERE p.parentid = c.oid AND p.parttype = 'p') AS expected
    FROM (VALUES ('region'), ('nation'), ('supplier'), ('customer'),
                 ('part'), ('partsupp'), ('orders'), ('lineitem')) AS t(table_name)
    LEFT JOIN pg_namespace n ON n.nspname = :'schema'
    LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.table_name
), imcs AS (
    SELECT * FROM pg_catalog.query_imcstore_views()
)
SELECT t.table_name, t.expected, count(v.reloid) AS populated,
       sum(CASE WHEN v.imcs_status = 'COMPLETE' THEN 1 ELSE 0 END) AS complete,
       max(CASE WHEN v.reloid = t.relid THEN v.imcs_status::text END) AS root_status,
       sum(CASE WHEN t.expected = 1 OR v.is_partition
                THEN v.cu_num_in_mem ELSE 0 END) AS memory_cus,
       sum(CASE WHEN t.expected = 1 OR v.is_partition
                THEN v.cu_num_in_disk ELSE 0 END) AS disk_cus
FROM targets t
LEFT JOIN imcs v ON v.reloid = t.relid OR v.parent_oid = t.relid
GROUP BY t.table_name, t.expected;

SELECT * FROM populate_status ORDER BY table_name;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM populate_status
               WHERE populated <> expected OR complete <> expected) THEN
        RAISE EXCEPTION 'populate 尚未全部完成';
    END IF;
END;
$$;
\echo populate 已全部完成
SQL
}

case "${1:---help}" in
    generate) generate ;;
    create) create ;;
    load) load ;;
    populate) populate ;;
    status) status ;;
    all) generate; create; load; populate ;;
    -h|--help) echo "用法：$0 generate|create|load|populate|status|all"
              echo '数据目录、SF、分区数和数据库连接参数见脚本开头。' ;;
    *) echo "未知步骤：$1" >&2; exit 1 ;;
esac
