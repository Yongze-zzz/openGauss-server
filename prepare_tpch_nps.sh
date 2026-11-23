#!/usr/bin/env bash
# Explicit, offline layout preparation from existing row-store tables.
# Does not rebuild an existing schema or run any database command on --help.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ ${1:---help} == --help ]]; then
    cat <<'HELP'
Usage: bash prepare_tpch_nps.sh groups|create|populate|repopulate|restore|status
create copies all eight tables from SOURCE_SCHEMA into a NEW SCHEMA using
consistent physical hash keys. Defaults: SOURCE_SCHEMA=tpch_sf100_imcs_h192,
SCHEMA=tpch_nps_order_line_h192, PARTITIONS=192, NPS_PROFILE=order_line.
Profiles:
 order_line      orders(o_orderkey), lineitem(l_orderkey);
                 part(p_partkey), partsupp(ps_partkey)
 customer_orders customer(c_custkey), orders(o_custkey);
                 part(p_partkey), partsupp(ps_partkey)
 part_line       part(p_partkey), partsupp(ps_partkey), lineitem(l_partkey)
populate enables NPS, validates the physical keys/types/buckets, and places
matching hash buckets on the same NUMA node. Requires explicit comma-separated
IMCS_PARTITION_SCAN_CPUS. Native or previously populated layouts must first be
unpopulated first; repopulate explicitly clears the eight IMCS caches and then
loads the NPS layout again, skipping tables not yet populated. restore is an alias for this explicit IMCS-only reset. Row-store data is retained. No restart is done here.
You can use an existing matching SCHEMA and run only populate, avoiding a copy.
The profiles are alternatives: a single partition key cannot cover every TPC-H join.
PLACEMENT=nps (default): validated co-partition placement and NPS identity.
PLACEMENT=baseline: native IMCS placement; NPS off and explicit NUMA map cleared.
restore/repopulate explicitly replace the memory placement, preserving row tables.
Use INSTALL_DIR/GSQL_BIN/DB_NAME/PORT/PGHOST/PGUSER as with the performance runner.
HELP
    exit 0
fi
placement=${PLACEMENT:-nps}
[[ $placement == baseline || $placement == nps ]] || {
    echo 'PLACEMENT must be baseline or nps.' >&2; exit 2;
}
profile=${NPS_PROFILE:-order_line}
parts=${PARTITIONS:-192}
export SCHEMA=${SCHEMA:-tpch_nps_${profile}_h${parts}}
source=${SOURCE_SCHEMA:-tpch_sf100_imcs_h192}
[[ $SCHEMA =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ && $source =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ && $parts =~ ^[1-9][0-9]*$ ]] || {
    echo 'Use simple SQL schema identifiers and a positive PARTITIONS count.' >&2; exit 2;
}
export INSTALL_DIR=${INSTALL_DIR:-$root/mppdb_temp_install}
binarylibs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc=${GCC_PATH:-$binarylibs/buildtools/gcc10.3}
export GAUSSHOME="$INSTALL_DIR"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$gcc/gcc/lib64:$binarylibs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
export GSQL_BIN=${GSQL_BIN:-$INSTALL_DIR/bin/gsql}
declare -A keys=([customer]=c_custkey [part]=p_partkey [partsupp]=ps_partkey [orders]=o_orderkey [lineitem]=l_orderkey)
case $profile in
    order_line) groups="$SCHEMA.orders:o_orderkey,$SCHEMA.lineitem:l_orderkey;$SCHEMA.part:p_partkey,$SCHEMA.partsupp:ps_partkey" ;;
    customer_orders)
        keys[orders]=o_custkey
        groups="$SCHEMA.customer:c_custkey,$SCHEMA.orders:o_custkey;$SCHEMA.part:p_partkey,$SCHEMA.partsupp:ps_partkey" ;;
    part_line)
        keys[lineitem]=l_partkey
        groups="$SCHEMA.part:p_partkey,$SCHEMA.partsupp:ps_partkey,$SCHEMA.lineitem:l_partkey" ;;
    *) echo 'Unknown NPS_PROFILE.' >&2; exit 2 ;;
esac
groups=${IMCS_NPS_GROUPS:-$groups}
case ${1:-} in
    groups) printf '%s\n' "$groups" ;;
    create)
        [[ $SCHEMA != "$source" ]] || { echo 'SCHEMA must differ from SOURCE_SCHEMA for create.' >&2; exit 2; }
        partitions='PARTITION p0'
        for ((i=1; i<parts; ++i)); do partitions+=", PARTITION p$i"; done
        {
            echo 'BEGIN;'
            echo 'CREATE SCHEMA :"schema";'
            for table in region nation supplier customer part partsupp orders lineitem; do
                printf 'CREATE TABLE :"schema".%s (LIKE :"source".%s INCLUDING DEFAULTS) WITH (orientation=row)' "$table" "$table"
                if [[ -n ${keys[$table]:-} ]]; then
                    printf ' PARTITION BY HASH (%s) (%s)' "${keys[$table]}" "$partitions"
                fi
                echo ';'
                printf 'INSERT INTO :"schema".%s SELECT * FROM :"source".%s;\n' "$table" "$table"
                printf 'ANALYZE :"schema".%s;\n' "$table"
            done
            echo 'COMMIT;'
        } | "$GSQL_BIN" -X -v ON_ERROR_STOP=1 -v "schema=$SCHEMA" -v "source=$source" \
            -p "${PORT:-${PGPORT:-19999}}" -d "${DB_NAME:-tpch}"
        ;;
    populate|repopulate|restore)
        export IMCS_PARTITION_SCAN_CPUS=${IMCS_PARTITION_SCAN_CPUS:-${CPU_POOL:-}}
        [[ $IMCS_PARTITION_SCAN_CPUS =~ ^[0-9]+(,[0-9]+)*$ ]] || {
            echo 'Explicit CPU_POOL/IMCS_PARTITION_SCAN_CPUS is required before IMCS preparation.' >&2; exit 2;
        }
        # Preparation always disables runtime Adapt; retain all other caller options.
        export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c enable_imcs_adapt=off"
        if [[ $1 == repopulate || $1 == restore ]]; then
            "$GSQL_BIN" -X -v ON_ERROR_STOP=1 -v "schema=$SCHEMA" \
                -p "${PORT:-${PGPORT:-19999}}" -d "${DB_NAME:-tpch}" <<'SQL'
SET search_path TO :"schema";
DO $restore$
DECLARE
    target record;
    namespace_oid oid;
    target_count integer;
    qualified_name text;
BEGIN
    SELECT n.oid INTO namespace_oid FROM pg_catalog.pg_namespace n
    WHERE n.nspname = current_schema();
    IF namespace_oid IS NULL THEN
        RAISE EXCEPTION 'Target SCHEMA does not exist; select an existing layout or explicitly create a new one';
    END IF;
    -- Validate all targets before clearing any cache. No implicit table creation.
    SELECT count(*) INTO target_count FROM pg_catalog.pg_class c
    WHERE c.relnamespace = namespace_oid
      AND c.relname IN ('region','nation','supplier','customer','part','partsupp','orders','lineitem');
    IF target_count <> 8 THEN
        RAISE EXCEPTION 'Target SCHEMA must contain all eight TPC-H tables';
    END IF;
    FOR target IN SELECT c.oid, c.relname FROM pg_catalog.pg_class c
                  WHERE c.relnamespace = namespace_oid
                    AND c.relname IN ('region','nation','supplier','customer','part','partsupp','orders','lineitem')
                  ORDER BY c.relname
    LOOP
        qualified_name := quote_ident(current_schema()) || '.' || quote_ident(target.relname);
        -- Serialize the presence check with competing populate/unpopulate DDL.
        EXECUTE 'LOCK TABLE ' || qualified_name || ' IN ACCESS EXCLUSIVE MODE';
        IF EXISTS (SELECT 1 FROM pg_catalog.query_imcstore_views() v WHERE v.reloid = target.oid) THEN
            EXECUTE 'ALTER TABLE ' || qualified_name || ' UNIMCSTORED';
        ELSE
            RAISE NOTICE 'Skipping unpopulate for %: no resident IMCS descriptor', qualified_name;
        END IF;
    END LOOP;
END;
$restore$;
SQL
        fi
        export IMCS_PARTITION_NUMA_MAP=''
        if [[ $placement == nps ]]; then
            export ENABLE_IMCS_NPS=on IMCS_NPS_GROUPS="$groups"
        else
            export ENABLE_IMCS_NPS=off IMCS_NPS_GROUPS=''
        fi
        echo "Populate placement: $placement; schema: $SCHEMA"
        bash "$root/prepare_tpch_imcs.sh" populate
        ;;
    status) bash "$root/prepare_tpch_imcs.sh" status ;;
    *) echo 'Expected groups, create, populate, repopulate, restore or status.' >&2; exit 2 ;;
esac
