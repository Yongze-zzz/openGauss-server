#!/usr/bin/env bash
# Emit a shell environment; never apply configuration or run database commands.
set -euo pipefail
if [[ ${1:-} == --help ]]; then
    echo 'Usage: bash scripts/imcs_numa_env.sh 4 DOP > numa.env  (96-core pool)'
    echo '       bash scripts/imcs_numa_env.sh 8 DOP > numa.env  (192-core pool)'
    echo 'DOP must be between 2 and pool size; CPU pool size is independent of DOP.'
    echo 'Optional CPU_POOL / NUMA_NODES override discovery; actual sysfs topology is validated.'
    exit 0
fi
python3 - "${1:-4}" "${2:-}" <<'PY'
import os,sys,shlex
from pathlib import Path

def ids(text):
    result=set()
    for item in text.split(','):
        ends=item.strip().split('-')
        if len(ends)>2: raise ValueError('invalid ID range')
        lo,hi=int(ends[0]),int(ends[-1])
        if lo<0 or hi<lo: raise ValueError('invalid ID range')
        result.update(range(lo,hi+1))
    return result

try:
    n=int(sys.argv[1]); dop=int(sys.argv[2] or (24 if n==4 else 48))
    if n not in (4,8) or dop < 2 or dop > 24*n:
        raise ValueError('use 4 or 8 NUMA with 2 <= DOP <= 24 * NUMA count')
    base=Path('/sys/devices/system')
    online=ids((base/'cpu/online').read_text().strip()) & set(os.sched_getaffinity(0))
    topology={}
    for node in sorted((base/'node').glob('node[0-9]*')):
        number=int(node.name[4:])
        for cpu in ids((node/'cpulist').read_text().strip()) & online:
            topology[cpu]=number
    supplied=os.getenv('CPU_POOL') or os.getenv('IMCS_PARTITION_SCAN_CPUS')
    requested=ids(supplied) if supplied else None
    nodes=ids(os.environ['NUMA_NODES']) if os.getenv('NUMA_NODES') else (
        {topology[c] for c in requested} if requested else set(sorted(set(topology.values()))[:n]))
    if len(nodes)!=n: raise ValueError('NUMA node count mismatch')
    # Existing server thread-pool/PGPROC NUMA indexing requires a node prefix.
    # CPU IDs themselves are always discovered and need not be contiguous.
    if nodes!=set(range(n)):
        raise ValueError('existing server NUMA groups support node prefixes 0..N-1; non-prefix node selection is unsupported')
    chosen=[]; seen=set()
    for cpu,node in sorted(topology.items()):
        if node not in nodes or (requested is not None and cpu not in requested): continue
        siblings=tuple(sorted(ids((base/f'cpu/cpu{cpu}/topology/thread_siblings_list').read_text().strip())))
        if siblings in seen:
            if requested is not None: raise ValueError('CPU_POOL includes SMT siblings, not distinct physical cores')
            continue
        seen.add(siblings); chosen.append(cpu)
    if requested is not None and set(chosen)!=requested:
        raise ValueError('CPU_POOL contains offline/disallowed CPUs or CPUs outside NUMA_NODES')
    if any(sum(topology[c]==node for c in chosen)!=24 for node in nodes):
        raise ValueError('expected 24 available physical cores per selected NUMA node; check shell affinity and CPU_POOL')
    chosen.sort(key=lambda cpu:(topology[cpu],cpu))
    pool=','.join(map(str,chosen)); node_list=','.join(map(str,sorted(nodes)))
    values={
        'NUMA_COUNT':n,'NUMA_NODES':node_list,'CPU_POOL':pool,
        'IMCS_PARTITION_SCAN_CPUS':pool,'DOP':dop,
        'THREAD_POOL_ATTR':f'{len(chosen)},{n},(nodebind: {node_list})',
        'THREAD_POOL_STREAM_ATTR':f'{5*len(chosen)},0.2',
        'NUMACTL_ARGS':f'--physcpubind={pool} --membind={node_list}',
    }
    for key,value in values.items(): print(f'export {key}={shlex.quote(str(value))}')
except (ValueError,OSError,KeyError) as error:
    raise SystemExit(str(error))
PY
