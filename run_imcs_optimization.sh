#!/usr/bin/env bash
# Manual experiments against an already running instance. No build/restart/import.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
mode=${1:---help}
case $mode in
-h|--help)
cat <<'HELP'
Usage: bash run_imcs_optimization.sh all|baseline|nps|nps_adapt [--sql-only]
Aliases: anp_adapt, anp+adapt -> nps_adapt.
Required: CPU_POOL (explicit CPU IDs; use scripts/imcs_numa_env.sh).
Defaults: DOP=24 RUNS=3 WARMUPS=0 QUERIES="1 6" PORT=19999 WORK_MEM=100GB
SCHEMA=tpch_nps_order_line_h192 NPS_PROFILE=order_line PARTITIONS=192.
Baseline keeps whole-partition scan ON, with native IMCS placement and NPS OFF.
NPS/NPS+Adapt require NPS placement. RESET_LAYOUT=manual refuses wrong placement
or inherited migrations. Explicit RESET_LAYOUT=repopulate permits IMCS-only reset
when switching policies; never rebuilds schema/row data. Modes run in blocks to
avoid a full repopulate for every baseline/NPS pair.
Each query/mode gets WARMUPS warmups once, followed by RUNS measurements.
ADAPT_WARMUPS=nps (default) uses NPS-only warmups for Adapt; mode runs Adapt
warmups with Adapt ON. Every point verifies initial placement; a warmup that
migrates data may require an explicit restore before the next point.
Optional: IMCS_NPS_GROUPS, SQL_DIR, OUTPUT_DIR, INSTALL_DIR, GSQL_BIN, DB_NAME,
PGHOST, PGUSER, ADAPT_PERIOD_MS=1000, INTRA_STATS=off, INTER_STATS=off.
STREAM_HOTSPOT_SPLIT=off (default) first-touches local Stream buffers in
their NPS producer lanes when enabled. STREAM_BUFFER_LOCALITY is a legacy alias.
STREAM_BATCH_RING=on enables a second slot for backpressured local vector
redistribution channels. The server default is off.
STREAM_BATCH_RING_EXTRA_SLOTS caps additional batch slots per producer per Stream
(server default: 16).
STREAM_HOTFLOW_COLOCATION=off maps single-key local redistribution onto NPS
chiplet lanes. STREAM_PATH_REBALANCE=off dephases producer polling by chiplet.
CACHE_COORD=off (default); prefer ready local Stream batches during recent
radix probe overlap. CACHE_COORD_INTERVAL=4 bounds the preferred local burst.
CACHE_COORD_LLC_PRESSURE=on requires the estimated overlapping probe footprint
to exceed 75% of the local LLC before preferring local batches.
CACHE_HARMFUL=on observes LLC-local Scan and Aggregate overlap at radix probe boundaries.
CACHE_HARMFUL_DEFER=on lets a matching probe briefly defer on its current LLC.
CACHE_PROBE_COORD=off (default); briefly yield large radix probes under sampled LLC pressure.
AGG_CACHE_SLOTS optionally sets the per-LLC aggregate update limit (server default: 0).
AGG_CACHE_WINDOW=on optionally enables short-window aggregate branch preference.
AGG_CACHE_BORROW=on makes unused preferred-branch slots available to its peer.
AGG_CACHE_CHIPLET=on uses LLC-local Stream demand for aggregate window preference.
AGG_CACHE_LIVE_PRESSURE=on limits updates when active aggregate states compete for the same LLC.
AGG_CACHE_CRITICAL=on prioritizes sustained local downstream demand in the LLC window.
RADIX_PARTITION_MB optionally sets the RHJ partition target (default: server GUC, 8 MB).
RHJ_SHARED_BUILD=on shares eligible local broadcast RHJ tables within each LLC domain.
RHJ_SHARED_PROBE_LOCALITY=on starts a shared RHJ probe tile with a recently used ready partition.
RHJ_SHARED_PROBE_COUNT=on maintains shared RHJ probe counters independently of partition order.
RHJ_SHARED_DIRECT_SEND=on removes closed receivers from the producer's broadcast list.
RHJ_SHARED_PRESELECT_BUILDERS=on routes eligible broadcasts to one shared RHJ builder per LLC.
ADAPT_MIGRATION=on (default); off keeps nps_adapt node affinity without monitoring or moves.
ADAPT_LOCAL_TASKS=off (default); on requires ADAPT_MIGRATION=off and claims node-local partitions on eligible scans.
QUERY_TIMEOUT_MS=0 sets server statement_timeout (0 unlimited).
QUERY_HEARTBEAT_SEC=60 prints elapsed waiting time; 0 disables it. A heartbeat
is client liveness only, not proof of server query progress.
--sql-only generates SQL and metadata without contacting the database.
Run exclusively: global move counters also include concurrent Adapt queries.
HELP
exit 0 ;;
all|baseline|nps|nps_adapt) ;;
anp_adapt|anp+adapt) mode=nps_adapt ;;
*) echo 'Unknown mode; use --help.' >&2; exit 2 ;;
esac
sql_only=${SQL_ONLY:-${DRY_RUN:-0}}
[[ $# -le 2 ]] || { echo 'Use --help for supported options.' >&2; exit 2; }
case ${2:-} in
    '') ;;
    --sql-only) sql_only=1 ;;
    *) echo "Unknown option: $2" >&2; exit 2 ;;
esac
[[ $sql_only =~ ^[01]$ ]] || { echo 'SQL_ONLY must be 0 or 1.' >&2; exit 2; }
export INSTALL_DIR=${INSTALL_DIR:-$root/mppdb_temp_install}
libs=${BINARYLIBS:-/mnt/nvme1p1/yanyz/binarylibs}
gcc=${GCC_PATH:-$libs/buildtools/gcc10.3}
export GAUSSHOME="$INSTALL_DIR"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$gcc/gcc/lib64:$libs/kernel/dependency/openssl/comm/lib:${LD_LIBRARY_PATH:-}"
export SCHEMA=${SCHEMA:-tpch_nps_${NPS_PROFILE:-order_line}_h${PARTITIONS:-192}}
cpus=${CPU_POOL:-${IMCS_PARTITION_SCAN_CPUS:-}}
export IMCS_PARTITION_SCAN_CPUS=$cpus
[[ $cpus =~ ^[0-9]+(,[0-9]+)*$ ]] || { echo 'CPU_POOL must contain explicit comma-separated CPU IDs.' >&2; exit 2; }
[[ $SCHEMA =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] || { echo 'Use a simple SCHEMA identifier.' >&2; exit 2; }
work_mem=${WORK_MEM:-100GB}
[[ $work_mem =~ ^[0-9]+([.][0-9]+)?([kKmMgGtT][bB])?$ ]] || { echo 'WORK_MEM must be a size such as 50GB or 100GB (or numeric kB).' >&2; exit 2; }
dop=${DOP:-24}; runs=${RUNS:-3}; warmups=${WARMUPS:-0}; period=${ADAPT_PERIOD_MS:-1000}
[[ $dop =~ ^[1-9][0-9]*$ && $dop -gt 1 && $runs =~ ^[1-9][0-9]*$ && $warmups =~ ^(0|[1-9][0-9]*)$ && $period =~ ^[1-9][0-9]*$ ]] || { echo 'Invalid DOP/RUNS/WARMUPS/ADAPT_PERIOD_MS.' >&2; exit 2; }
intra=${INTRA_STATS:-off}; inter=${INTER_STATS:-off}; reset=${RESET_LAYOUT:-manual}
cache_trace=${CACHE_TRACE:-}
[[ -z $cache_trace || $cache_trace =~ ^(on|off)$ ]] || { echo 'CACHE_TRACE must be on or off.' >&2; exit 2; }
cache_coord=${CACHE_COORD:-off}
cache_coord_pressure=${CACHE_COORD_LLC_PRESSURE:-}
[[ -z $cache_coord_pressure || $cache_coord_pressure =~ ^(on|off)$ ]] ||
    { echo 'CACHE_COORD_LLC_PRESSURE must be on or off.' >&2; exit 2; }
cache_coord_pressure_set=
[[ -z $cache_coord_pressure ]] ||
    cache_coord_pressure_set="SET enable_imcs_cache_coord_llc_pressure = $cache_coord_pressure;"
cache_harmful=${CACHE_HARMFUL:-}
cache_harmful_defer=${CACHE_HARMFUL_DEFER:-}
cache_harmful_set=
for cache_harmful_value in "$cache_harmful" "$cache_harmful_defer"; do
    [[ -z $cache_harmful_value || $cache_harmful_value =~ ^(on|off)$ ]] ||
        { echo 'CACHE_HARMFUL and CACHE_HARMFUL_DEFER must be on or off.' >&2; exit 2; }
done
[[ -z $cache_harmful ]] ||
    cache_harmful_set="SET enable_imcs_cache_harmful = $cache_harmful;"
[[ -z $cache_harmful_defer ]] ||
    cache_harmful_set+=$'\n'"SET enable_imcs_cache_harmful_defer = $cache_harmful_defer;"
cache_probe_coord=${CACHE_PROBE_COORD:-off}
[[ $cache_probe_coord =~ ^(on|off)$ ]] || { echo 'CACHE_PROBE_COORD must be on or off.' >&2; exit 2; }
cache_coord_interval=${CACHE_COORD_INTERVAL:-4}
agg_cache_slots=${AGG_CACHE_SLOTS:-}
agg_cache_window=${AGG_CACHE_WINDOW:-}
agg_cache_borrow=${AGG_CACHE_BORROW:-}
agg_cache_chiplet=${AGG_CACHE_CHIPLET:-}
agg_cache_live_pressure=${AGG_CACHE_LIVE_PRESSURE:-}
agg_cache_critical=${AGG_CACHE_CRITICAL:-}
agg_cache_sql=''
if [[ -n $agg_cache_slots ]]; then
    [[ $agg_cache_slots =~ ^(0|[1-9][0-9]{0,2})$ && $agg_cache_slots -le 128 ]] ||
        { echo 'AGG_CACHE_SLOTS must be an integer from 0 to 128.' >&2; exit 2; }
    agg_cache_sql="SET imcs_agg_cache_slots=$agg_cache_slots;"
fi
if [[ -n $agg_cache_window ]]; then
    [[ $agg_cache_window =~ ^(on|off)$ ]] || { echo 'AGG_CACHE_WINDOW must be on or off.' >&2; exit 2; }
    agg_cache_sql+=$'\n'"SET enable_imcs_agg_cache_window=$agg_cache_window;"
fi
if [[ -n $agg_cache_borrow ]]; then
    [[ $agg_cache_borrow =~ ^(on|off)$ ]] || { echo 'AGG_CACHE_BORROW must be on or off.' >&2; exit 2; }
    agg_cache_sql+=$'\n'"SET enable_imcs_agg_cache_borrow=$agg_cache_borrow;"
fi
if [[ -n $agg_cache_chiplet ]]; then
    [[ $agg_cache_chiplet =~ ^(on|off)$ ]] || { echo 'AGG_CACHE_CHIPLET must be on or off.' >&2; exit 2; }
    agg_cache_sql+=$'\n'"SET enable_imcs_agg_cache_chiplet=$agg_cache_chiplet;"
fi
if [[ -n $agg_cache_live_pressure ]]; then
    [[ $agg_cache_live_pressure =~ ^(on|off)$ ]] ||
        { echo 'AGG_CACHE_LIVE_PRESSURE must be on or off.' >&2; exit 2; }
    agg_cache_sql+=$'\n'"SET enable_imcs_agg_cache_live_pressure=$agg_cache_live_pressure;"
fi
if [[ -n $agg_cache_critical ]]; then
    [[ $agg_cache_critical =~ ^(on|off)$ ]] || { echo 'AGG_CACHE_CRITICAL must be on or off.' >&2; exit 2; }
    agg_cache_sql+=$'\n'"SET enable_imcs_agg_cache_critical=$agg_cache_critical;"
fi
[[ $cache_coord =~ ^(on|off)$ && $cache_coord_interval =~ ^[1-9][0-9]*$ ]] || { echo 'Invalid CACHE_COORD/CACHE_COORD_INTERVAL.' >&2; exit 2; }
stream_hotspot_split=${STREAM_HOTSPOT_SPLIT:-${STREAM_BUFFER_LOCALITY:-off}}
[[ $stream_hotspot_split =~ ^(on|off)$ ]] || { echo 'STREAM_HOTSPOT_SPLIT must be on or off.' >&2; exit 2; }
stream_batch_ring=${STREAM_BATCH_RING:-}
[[ -z $stream_batch_ring || $stream_batch_ring =~ ^(on|off)$ ]] || { echo 'STREAM_BATCH_RING must be on or off.' >&2; exit 2; }
stream_batch_ring_extra_slots=${STREAM_BATCH_RING_EXTRA_SLOTS:-}
if [[ -n $stream_batch_ring_extra_slots ]] &&
    [[ ! $stream_batch_ring_extra_slots =~ ^(0|[1-9][0-9]{0,3})$ || $stream_batch_ring_extra_slots -gt 1024 ]]; then
    echo 'STREAM_BATCH_RING_EXTRA_SLOTS must be an integer from 0 to 1024.' >&2
    exit 2
fi
stream_batch_ring_set=''
stream_batch_ring_show=''
if [[ -n $stream_batch_ring ]]; then
    stream_batch_ring_set="SET enable_imcs_stream_batch_ring = $stream_batch_ring;"
    stream_batch_ring_show="SHOW enable_imcs_stream_batch_ring;"
fi
if [[ -n $stream_batch_ring_extra_slots ]]; then
    stream_batch_ring_set+=$'\n'"SET imcs_stream_batch_ring_extra_slots = $stream_batch_ring_extra_slots;"
    stream_batch_ring_show+=$'\n'"SHOW imcs_stream_batch_ring_extra_slots;"
fi
stream_consumer_placement=${STREAM_CONSUMER_PLACEMENT:-off}
[[ $stream_consumer_placement =~ ^(on|off)$ ]] || { echo 'STREAM_CONSUMER_PLACEMENT must be on or off.' >&2; exit 2; }
stream_hotflow_colocation=${STREAM_HOTFLOW_COLOCATION:-off}
stream_path_rebalance=${STREAM_PATH_REBALANCE:-off}
[[ $stream_hotflow_colocation =~ ^(on|off)$ ]] || { echo 'STREAM_HOTFLOW_COLOCATION must be on or off.' >&2; exit 2; }
[[ $stream_path_rebalance =~ ^(on|off)$ ]] || { echo 'STREAM_PATH_REBALANCE must be on or off.' >&2; exit 2; }
rhj_mode=${RHJ_MODE:-}
[[ -z $rhj_mode || $rhj_mode =~ ^(sonic|radix|cache|chiplet)$ ]] || { echo 'Unknown RHJ_MODE.' >&2; exit 2; }
radix_target_mb=${RADIX_PARTITION_MB:-}
[[ -z $radix_target_mb || ( $radix_target_mb =~ ^[1-9][0-9]*$ && $radix_target_mb -le 1024 ) ]] || {
    echo 'RADIX_PARTITION_MB must be an integer from 1 to 1024.' >&2; exit 2;
}
extra_join_sql=''
rhj_shared_build=${RHJ_SHARED_BUILD:-}
rhj_shared_probe_locality=${RHJ_SHARED_PROBE_LOCALITY:-}
rhj_shared_probe_count=${RHJ_SHARED_PROBE_COUNT:-}
rhj_shared_direct_send=${RHJ_SHARED_DIRECT_SEND:-}
rhj_shared_preselect_builders=${RHJ_SHARED_PRESELECT_BUILDERS:-}
[[ -z $rhj_shared_build || $rhj_shared_build =~ ^(on|off)$ ]] || {
    echo 'RHJ_SHARED_BUILD must be on or off.' >&2; exit 2;
}
[[ -z $rhj_shared_probe_locality || $rhj_shared_probe_locality =~ ^(on|off)$ ]] || {
    echo 'RHJ_SHARED_PROBE_LOCALITY must be on or off.' >&2; exit 2;
}
[[ -z $rhj_shared_probe_count || $rhj_shared_probe_count =~ ^(on|off)$ ]] || {
    echo 'RHJ_SHARED_PROBE_COUNT must be on or off.' >&2; exit 2;
}
[[ -z $rhj_shared_direct_send || $rhj_shared_direct_send =~ ^(on|off)$ ]] || {
    echo 'RHJ_SHARED_DIRECT_SEND must be on or off.' >&2; exit 2;
}
[[ -z $rhj_shared_preselect_builders || $rhj_shared_preselect_builders =~ ^(on|off)$ ]] || {
    echo 'RHJ_SHARED_PRESELECT_BUILDERS must be on or off.' >&2; exit 2;
}
if [[ -n $rhj_mode ]]; then
    radix=on; cache=off; chiplet=off
    [[ $rhj_mode != sonic ]] || radix=off
    [[ $rhj_mode != cache && $rhj_mode != chiplet ]] || cache=on
    [[ $rhj_mode != chiplet ]] || chiplet=on
    extra_join_sql="SET enable_sonic_hashjoin=on;
SET enable_radix_hashjoin=$radix;
SET radix_cache_aware=$cache;
SET radix_chiplet_aware=$chiplet;"
    [[ -z $radix_target_mb ]] || extra_join_sql+=$'\n'"SET radix_partition_size_mb=$radix_target_mb;"
fi
[[ -z $rhj_shared_build ]] || extra_join_sql+=$'\n'"SET radix_shared_build=$rhj_shared_build;"
[[ -z $rhj_shared_probe_locality ]] ||
    extra_join_sql+=$'\n'"SET enable_radix_shared_probe_locality=$rhj_shared_probe_locality;"
[[ -z $rhj_shared_probe_count ]] ||
    extra_join_sql+=$'\n'"SET enable_radix_shared_probe_count=$rhj_shared_probe_count;"
[[ -z $rhj_shared_direct_send ]] || extra_join_sql+=$'\n'"SET radix_shared_direct_send=$rhj_shared_direct_send;"
[[ -z $rhj_shared_preselect_builders ]] || extra_join_sql+=$'\n'"SET radix_shared_preselect_builders=$rhj_shared_preselect_builders;"

adapt_migration=${ADAPT_MIGRATION:-on}
adapt_local_tasks=${ADAPT_LOCAL_TASKS:-off}
[[ $adapt_local_tasks =~ ^(on|off)$ ]] || { echo 'ADAPT_LOCAL_TASKS must be on or off.' >&2; exit 2; }
[[ $adapt_local_tasks != on || $adapt_migration == off ]] || { echo 'ADAPT_LOCAL_TASKS=on requires ADAPT_MIGRATION=off.' >&2; exit 2; }
[[ $adapt_migration =~ ^(on|off)$ ]] || { echo 'ADAPT_MIGRATION must be on or off.' >&2; exit 2; }
query_timeout=${QUERY_TIMEOUT_MS:-0}; heartbeat_sec=${QUERY_HEARTBEAT_SEC:-60}
[[ $query_timeout =~ ^[0-9]+$ && $heartbeat_sec =~ ^[0-9]+$ ]] || { echo 'QUERY_TIMEOUT_MS and QUERY_HEARTBEAT_SEC must be nonnegative integers.' >&2; exit 2; }
[[ ${#query_timeout} -le 10 && ${#heartbeat_sec} -le 6 ]] || { echo 'Timeout/heartbeat value too large.' >&2; exit 2; }
query_timeout=$((10#$query_timeout)); heartbeat_sec=$((10#$heartbeat_sec))
((query_timeout<=2147483647)) || { echo 'QUERY_TIMEOUT_MS exceeds server integer range.' >&2; exit 2; }
adapt_warmups=${ADAPT_WARMUPS:-nps}
[[ $adapt_warmups == nps || $adapt_warmups == mode ]] || { echo 'ADAPT_WARMUPS must be nps or mode.' >&2; exit 2; }
[[ $intra =~ ^(on|off)$ && $inter =~ ^(on|off)$ && $reset =~ ^(manual|repopulate)$ ]] || { echo 'Invalid statistics/reset option.' >&2; exit 2; }
groups=$(bash "$root/prepare_tpch_nps.sh" groups)
read -r -a queries <<< "${QUERIES:-1 6}"
((${#queries[@]})) || { echo 'Empty QUERIES.' >&2; exit 2; }
sql_dir=${SQL_DIR:-$root/SQL/queries}
for q in "${queries[@]}"; do
 [[ $q =~ ^([1-9]|1[0-9]|2[0-2])$ && -r $sql_dir/q$q.sql ]] || { echo "Invalid/missing Q$q." >&2; exit 2; }
done
if [[ -n ${OUTPUT_DIR:-} ]]; then
 out=$OUTPUT_DIR; mkdir -p "$out"
 [[ ! -e $out/metadata.txt && ! -e $out/summary.csv ]] || { echo 'Use a new OUTPUT_DIR.' >&2; exit 2; }
else
 mkdir -p "$root/results/imcs_optimization"
 out=$(mktemp -d "$root/results/imcs_optimization/${mode}_dop${dop}_$(date +%Y%m%d_%H%M%S)_XXXXXX")
fi
out=$(cd "$out" && pwd -P)
sql=("${GSQL_BIN:-$INSTALL_DIR/bin/gsql}" -X -q -v ON_ERROR_STOP=1 -P pager=off -p "${PORT:-${PGPORT:-19999}}" -d "${DB_NAME:-tpch}" -v "schema=$SCHEMA" -v "cpus=$cpus" -v "groups=$groups" -v "work_mem=$work_mem")
cat > "$out/metadata.txt" <<META
scope=same physical HASH tables; policy-specific initial memory placement
stream_routing=originating_thread_pool_group (custom SMP-modulo placement removed)
baseline_placement=native_imcs_no_nps
nps_placement=validated_nps_copartitioned
mode_order=baseline_block,nps_block,nps_adapt_block
schema=$SCHEMA
dop=$dop
work_mem=$work_mem
cpu_pool=$cpus
queries=${queries[*]}
runs=$runs
warmups=$warmups
adaptive_warmups=$adapt_warmups
warmup_schedule=once_per_query_per_mode_then_measurements
reset_layout=$reset
nps_groups=$groups
adapt_period_ms=$period
adapt_migration=$adapt_migration
adapt_local_tasks=$adapt_local_tasks
cache_trace=${cache_trace:-off}
cache_coord=$cache_coord
cache_coord_llc_pressure=${cache_coord_pressure:-default}
cache_harmful=${cache_harmful:-default}
cache_harmful_defer=${cache_harmful_defer:-default}
cache_probe_coord=$cache_probe_coord
cache_coord_interval=$cache_coord_interval
agg_cache_slots=${agg_cache_slots:-default}
agg_cache_window=${agg_cache_window:-default}
agg_cache_borrow=${agg_cache_borrow:-default}
agg_cache_chiplet=${agg_cache_chiplet:-default}
agg_cache_live_pressure=${agg_cache_live_pressure:-default}
agg_cache_critical=${agg_cache_critical:-default}
stream_buffer_locality=$stream_hotspot_split
stream_batch_ring=${stream_batch_ring:-default}
stream_batch_ring_extra_slots=${stream_batch_ring_extra_slots:-default}
stream_consumer_placement=$stream_consumer_placement
stream_hotflow_colocation=$stream_hotflow_colocation
stream_path_rebalance=$stream_path_rebalance
rhj_mode=${rhj_mode:-existing}
rhj_shared_build=${rhj_shared_build:-default}
rhj_shared_direct_send=${rhj_shared_direct_send:-default}
rhj_shared_probe_locality=${rhj_shared_probe_locality:-default}
rhj_shared_probe_count=${rhj_shared_probe_count:-default}
rhj_shared_preselect_builders=${rhj_shared_preselect_builders:-default}
radix_partition_mb=${radix_target_mb:-default}
adapt_affinity=node_cpu_mask
intra_stats=$intra
inter_stats=$inter
query_timeout_ms=$query_timeout
query_heartbeat_sec=$heartbeat_sec
META
printf 'mode,query,run,total_runtime_ms,migrations,migration_us,failed_migrations,baseline_pool_entries,baseline_pool_cpus,work_mem\n' > "$out/summary.csv"
echo "Output: $out"
# All validation is executed only when the user runs this script normally.
if [[ $sql_only != 1 ]]; then
 "${sql[@]}" -At -c 'SHOW data_directory;' > "$out/data_directory.txt"
 data_dir=$(tail -n 1 "$out/data_directory.txt")
 [[ -r $data_dir/postmaster.pid ]] || { echo 'Local PGDATA is not readable; this runner requires a local server for affinity verification.' >&2; exit 1; }
 pid=$(head -n 1 "$data_dir/postmaster.pid")
 exec 9>"$data_dir/.imcs_experiment.lock"
 flock -n 9 || { echo 'Another experiment wrapper holds the PGDATA lock.' >&2; exit 1; }
 python3 - "$pid" "$cpus" "$dop" <<'PY'
import sys,os
from pathlib import Path
from collections import Counter
pid=int(sys.argv[1]); pool=[int(x) for x in sys.argv[2].split(',')]; wanted=set(pool)
if len(wanted)!=len(sys.argv[2].split(',')): raise SystemExit('duplicate CPU ID')
if os.sched_getaffinity(pid)!=wanted: raise SystemExit('Server CPU affinity differs from CPU_POOL. Restart manually using the topology environment.')
dop=int(sys.argv[3])
if dop>len(wanted): raise SystemExit('DOP exceeds CPU pool')
def node(cpu):
 nodes=list(Path(f'/sys/devices/system/cpu/cpu{cpu}').glob('node[0-9]*'))
 if len(nodes)!=1: raise SystemExit('Cannot determine CPU NUMA node')
 return int(nodes[0].name[4:])
nodes={node(c) for c in pool}; lanes=Counter(node(pool[i*len(pool)//dop]) for i in range(dop))
if set(lanes)!=nodes or max(lanes.values())-min(lanes.values())>1:
 raise SystemExit('CPU_POOL ordering produces uneven NPS lanes; use scripts/imcs_numa_env.sh (node-grouped CPUs)')

PY
 "${sql[@]}" -At -c "SELECT current_setting('enable_thread_pool'), current_setting('numa_distribute_mode'), current_setting('thread_pool_attr'), current_setting('thread_pool_stream_attr');" > "$out/server_config.txt"
 cat "$out/server_config.txt"
 python3 - "$out/server_config.txt" "$cpus" <<'PYCONFIG'
import sys,re
from pathlib import Path
line=Path(sys.argv[1]).read_text().strip().split('|')
nodes=set()
for c in sys.argv[2].split(','):
 nodes.update(int(p.name[4:]) for p in Path(f'/sys/devices/system/cpu/cpu{c}').glob('node[0-9]*'))
if len(line)!=4 or line[0]!='on' or line[1]!='all': raise SystemExit('Require enable_thread_pool=on, numa_distribute_mode=all')
if int(line[2].split(',')[1])!=len(nodes): raise SystemExit('Thread group count differs from active NUMA count')
if min(int(line[2].split(',')[0]),int(line[3].split(',')[0]))<len(sys.argv[2].split(',')):
 raise SystemExit('Thread/Stream configured capacity is smaller than CPU pool')
# ParseStreamAttr accepts only thread-count and proc-ratio. Stream affinity
# inherits the thread pool groups and the existing early NPS worker hook.
for spec in line[2:3]:
 m=re.search(r'nodebind:\s*([0-9, -]+)',spec)
 if not m: raise SystemExit('Thread/Stream nodebind missing; configure manually using topology environment')
 def expand(s):
  result=set()
  for part in s.strip().split(','):
   a=part.strip().split('-'); result.update(range(int(a[0]),int(a[-1])+1))
  return result
 if expand(m.group(1))!=nodes: raise SystemExit('Thread/Stream nodebind differs from CPU_POOL NUMA nodes')
PYCONFIG
fi
layout_check() {
 local placement=$1 check_nps=off
 [[ $placement != nps ]] || check_nps=on
 "${sql[@]}" -At -v "check_nps=$check_nps" <<'SQL' > "$out/layout_latest.txt"
SET enable_imcs_adapt=off;
SET enable_imcs_nps=:'check_nps';
SET imcs_partition_scan_cpus=:'cpus';
SET imcs_nps_groups=:'groups';
SET imcs_partition_numa_map='';
SELECT c.relname, greatest(1,(SELECT count(*) FROM pg_partition p WHERE p.parentid=c.oid AND p.parttype='p')),
       pg_catalog.imcs_adapt_status(c.oid)
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=:'schema' AND c.relname IN ('region','nation','supplier','customer','part','partsupp','orders','lineitem')
ORDER BY c.relname;
SQL
 python3 - "$out/layout_latest.txt" "$placement" <<'PY'
import json,sys
rows=[line.split('|',2) for line in open(sys.argv[1]) if '|' in line]
if {r[0] for r in rows}!={'region','nation','supplier','customer','part','partsupp','orders','lineitem'}: raise SystemExit('Missing TPC-H tables')
for name,expected,raw in rows:
 s=json.loads(raw)
 if 'native' not in s or s.get('experiment_policy_version',0)<3:
  print('Server lacks experiment policy v3 (worker-side CPU pool entry verification); deploy the current build and restart manually before running experiments',file=sys.stderr); sys.exit(4)
 if s['active_workers']: raise SystemExit('Other Adapt workers are active; use an exclusive test instance')
 if s['entries']!=int(expected) or not s['complete']:
  print(f'{name}: incomplete/missing IMCS populate; restore placement={sys.argv[2]}',file=sys.stderr); sys.exit(3)
 if sys.argv[2]=='baseline':
  valid=s['native'] and s['mapped']==0
 else:
  valid=not s['native'] and s['mapped']==s['entries']
 if not valid or not s['initial'] or s['disabled']:
  print(f'{name}: expected {sys.argv[2]} initial placement, found wrong policy/migration/disabled group',file=sys.stderr); sys.exit(3)
PY
}
ensure_initial() {
 local placement=$1 rc=0
 layout_check "$placement" || rc=$?
 if ((rc==3)) && [[ $reset == repopulate ]]; then
  echo "Explicit RESET_LAYOUT=repopulate: restoring placement=$placement (IMCS only; row tables retained)."
  echo "Layout restore log: $out/layout_restore.log"
  PLACEMENT="$placement" bash "$root/prepare_tpch_nps.sh" restore >> "$out/layout_restore.log" 2>&1
  echo "Layout restore finished: $placement"
  layout_check "$placement"
 elif ((rc==4)); then
  echo 'Preflight failed: server deployment is outdated. No layout reset attempted; install the current build and restart manually.' >&2
  return "$rc"
 elif ((rc==3)); then
  echo "Preflight failed. Restore manually: PLACEMENT=$placement bash prepare_tpch_nps.sh restore" >&2
  return "$rc"
 elif ((rc)); then
  echo 'Preflight failed; fix the preceding configuration/schema error before retrying. No layout reset attempted.' >&2
  return "$rc"
 fi
}
heartbeat_pid=''
stop_heartbeat() {
 if [[ -n $heartbeat_pid ]]; then
  kill "$heartbeat_pid" 2>/dev/null || true
  wait "$heartbeat_pid" 2>/dev/null || true
  heartbeat_pid=''
 fi
}
trap stop_heartbeat EXIT
trap 'stop_heartbeat; exit 130' INT
trap 'stop_heartbeat; exit 143' TERM
stats() { "${sql[@]}" -At -c 'SELECT pg_catalog.imcs_adapt_status(0::oid);'; }
modes=($mode); [[ $mode != all ]] || modes=(baseline nps nps_adapt)
for m in "${modes[@]}"; do
 nps=off; numa=off; adapt=off; placement=baseline; query_groups=''
 [[ $m == baseline ]] || { nps=on; numa=on; placement=nps; query_groups=$groups; }
 [[ $m != nps_adapt ]] || adapt=on
 for q in "${queries[@]}"; do
   for ((i=1;i<=warmups+runs;i++)); do
    effective_adapt=$adapt
    if ((i<=warmups)); then
     label=warmup$i
     [[ $adapt_warmups != nps ]] || effective_adapt=off
    else
     run=$((i-warmups)); label=run$run
    fi
    file="$out/${m}_q${q}_${label}.sql"
    effective_cache_trace=$cache_trace
    ((i>warmups)) || effective_cache_trace=off
    cache_sql=''
    if [[ -n $cache_trace ]]; then
        cache_sql="SET enable_cache_trace=$effective_cache_trace; SET cache_trace_interval_us=${CACHE_INTERVAL_US:-100000};"
    fi
    effective_local_tasks=off
    [[ $effective_adapt != on ]] || effective_local_tasks=$adapt_local_tasks
    {
    cat <<SQL
$extra_join_sql
$cache_sql
$agg_cache_sql
SET search_path TO :"schema";
SET statement_timeout = $query_timeout;
SET work_mem = :'work_mem';
SET enable_vector_engine = on;
SET enable_codegen = on;
SET enable_imcsscan = on;
SET enable_cstore_partition_lane_scan = off;
SET query_dop = $dop;
SET enable_imcs_partition_scan = on;
SET enable_numa_aware_partition_scan = $numa;
SET enable_imcs_nps = $nps;
SET enable_imcs_stream_hotspot_split = $stream_hotspot_split;
$stream_batch_ring_set
SET enable_imcs_stream_consumer_placement = $stream_consumer_placement;
SET enable_imcs_stream_hotflow_colocation = $stream_hotflow_colocation;
SET enable_imcs_stream_path_rebalance = $stream_path_rebalance;
SET enable_imcs_cache_coord = $cache_coord;
$cache_coord_pressure_set
$cache_harmful_set
SET enable_imcs_cache_probe_coord = $cache_probe_coord;
SET imcs_cache_coord_sample_interval = $cache_coord_interval;
SET imcs_partition_numa_map = '';
SET imcs_nps_groups = :'query_groups';
SET imcs_partition_scan_cpus = :'cpus';
SET imcs_adapt_period_ms = $period;
SET enable_imcs_adapt = $effective_adapt;
SET enable_imcs_adapt_migration = $adapt_migration;
SET enable_imcs_local_task_queue = $effective_local_tasks;
SET enable_intra_operator_data_access = $intra;
SET enable_inter_operator_data_access = $inter;
SHOW statement_timeout;
SHOW work_mem;
SHOW query_mem;
SHOW query_max_mem;
SHOW enable_imcs_nps;
SHOW enable_imcs_stream_hotspot_split;
$stream_batch_ring_show
SHOW enable_imcs_stream_consumer_placement;
SHOW enable_imcs_stream_hotflow_colocation;
SHOW enable_imcs_stream_path_rebalance;
SHOW enable_imcs_cache_coord;
SHOW enable_imcs_cache_probe_coord;
SHOW imcs_cache_coord_sample_interval;
SHOW enable_imcs_adapt;
SHOW enable_imcs_adapt_migration;
SHOW enable_imcs_local_task_queue;
SHOW imcs_nps_groups;
SHOW imcs_partition_scan_cpus;
SHOW imcs_adapt_period_ms;
SHOW enable_intra_operator_data_access;
SHOW enable_inter_operator_data_access;
SHOW query_dop;
SHOW enable_codegen;
SHOW enable_imcsscan;
SHOW enable_imcs_partition_scan;
SHOW enable_numa_aware_partition_scan;
SHOW numa_distribute_mode;
SHOW thread_pool_attr;
SHOW thread_pool_stream_attr;
EXPLAIN PERFORMANCE
SQL
    cat "$sql_dir/q$q.sql"
    } > "$file"
    [[ $sql_only != 1 ]] || continue
    ensure_initial "$placement"
    cp "$out/layout_latest.txt" "$out/${m}_q${q}_${label}_initial.txt"
    stats > "$out/${m}_q${q}_${label}_before.json"
    echo "[$(TZ=Asia/Shanghai date '+%F %T %z')] $m Q$q $label; timeout_ms=$query_timeout; log=$out/${m}_q${q}_${label}.log"
    if ((heartbeat_sec>0)); then
     python3 -u - "$heartbeat_sec" "$m Q$q $label" <<'PYHEARTBEAT' >&2 &
import sys,time
period=int(sys.argv[1]); label=sys.argv[2]; start=time.monotonic()
while True:
 time.sleep(period)
 print(f'[waiting {time.monotonic()-start:.0f}s] {label}: awaiting gsql output; server progress unknown. For wait status: bash scripts/diagnose_imcs_wait.sh',flush=True)
PYHEARTBEAT
     heartbeat_pid=$!
    fi
    query_rc=0
    LC_ALL=C "${sql[@]}" -v "query_groups=$query_groups" -f "$file" 2>&1 | tee "$out/${m}_q${q}_${label}.log" || query_rc=$?
    stop_heartbeat
    if ((query_rc)); then
     echo "Query failed: $m Q$q $label (exit $query_rc); inspect its log. No measurement row recorded." >&2
     exit "$query_rc"
    fi
    stats > "$out/${m}_q${q}_${label}_after.json"
    if ((i>warmups)); then
     python3 - "$out" "$m" "$q" "$label" "$run" <<'PY' >> "$out/summary.csv"
import json,re,sys
from pathlib import Path
out,m,q,label,run=sys.argv[1:]; p=Path(out); stem=f'{m}_q{q}_{label}'
a=json.loads((p/(stem+'_before.json')).read_text()); b=json.loads((p/(stem+'_after.json')).read_text())
plan=(p/(stem+'.log')).read_text()
times=re.findall(r'Total runtime\s*:\s*([0-9.]+)\s*ms',plan)
if not times: raise SystemExit('Missing server Total runtime; inspect plan (client time is not substituted)')
entries=b['baseline_pool_entries']-a['baseline_pool_entries']
cpus=b['baseline_pool_cpus'] if entries else 0
if m=='baseline':
 if not entries:
  if any(int(n)>1 for n in re.findall(r'dop:\s*\d+/(\d+)',plan)):
   raise SystemExit('Parallel Baseline has no CPU-pool entry evidence; measurement NOT recorded. Inspect worker affinity/entry path.')
  print('WARNING: no Baseline pool entries recorded; plan may be serial',file=sys.stderr)
 else:
  expected=len(next(line.split('=',1)[1] for line in (p/'metadata.txt').read_text().splitlines() if line.startswith('cpu_pool=')).split(','))
  if cpus!=expected: raise SystemExit(f'Baseline entry CPU count {cpus} differs from requested pool {expected}; measurement NOT recorded')
  print(f'Baseline pool entry confirmed: {entries} Stream tasks, allowed CPUs={cpus}',file=sys.stderr)
requested_mem=next(line.split('=',1)[1] for line in (p/'metadata.txt').read_text().splitlines() if line.startswith('work_mem='))
print(','.join([m,q,run,times[-1]]+[str(b[k]-a[k]) for k in ('moves','move_us','failures')]+[str(entries),str(cpus),requested_mem]))
PY
    fi
   done
 done
done
echo "Results: $out/summary.csv"
