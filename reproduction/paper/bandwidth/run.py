#!/usr/bin/env python3
"""Two foreground readers, optional independent readers competing on the same home."""
import argparse
import csv
import datetime
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess

HERE = Path(__file__).resolve().parent
SYS = Path('/sys/devices/system')


def cpulist(text):
    result = []
    for part in text.strip().split(','):
        if '-' in part:
            lo, hi = map(int, part.split('-'))
            result.extend(range(lo, hi + 1))
        elif part:
            result.append(int(part))
    return result


def topology():
    allowed = os.sched_getaffinity(0)
    nodes = {}
    for directory in sorted((SYS / 'node').glob('node[0-9]*')):
        node = int(directory.name[4:])
        cpus = cpulist((directory / 'cpulist').read_text())
        sockets = {int((SYS / f'cpu/cpu{cpu}/topology/physical_package_id').read_text())
                   for cpu in cpus}
        if len(sockets) != 1:
            raise RuntimeError(f'N{node} spans multiple sockets: {sockets}')
        nodes[node] = dict(cpus=cpus, allowed_cpus=[c for c in cpus if c in allowed],
                           socket=next(iter(sockets)))
    return nodes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-node', type=int, default=0)
    parser.add_argument('--same-socket-node', type=int, default=1)
    parser.add_argument('--remote-node', type=int, default=2)
    parser.add_argument('--foreground', type=int, default=2)
    parser.add_argument('--background', type=int, default=None,
                        help='default: all remaining allowed cores on requesting die')
    parser.add_argument('--mib', type=int, default=128, help='independent buffer per thread')
    parser.add_argument('--seconds', type=float, default=2)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--seed', type=int, default=20261018)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--binary', type=Path, default=HERE / 'read_bandwidth')
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error('build first with bash build.sh or specify an executable with --binary')
    nodes = topology()
    for node in (args.source_node, args.same_socket_node, args.remote_node):
        if node not in nodes:
            parser.error(f'N{node} does not exist')
    source, same, remote = (nodes[n] for n in
                            (args.source_node, args.same_socket_node, args.remote_node))
    if (args.source_node == args.same_socket_node or source['socket'] != same['socket']
            or source['socket'] == remote['socket']):
        parser.error('expected a different die in same socket, and a die in another socket')
    cpus = source['allowed_cpus']
    bg = args.background if args.background is not None else len(cpus) - args.foreground
    if (args.foreground < 1 or bg < 1 or args.foreground + bg > len(cpus)
            or args.runs < 1 or args.mib < 1 or args.seconds < 0.1):
        parser.error('invalid reader count, buffer size, duration, or repetitions')
    # Do not count SMT siblings as independent competing cores.
    core_ids = [(SYS / f'cpu/cpu{c}/topology/core_id').read_text().strip()
                for c in cpus[:args.foreground + bg]]
    if len(set(core_ids)) != len(core_ids):
        parser.error('selected CPUs include SMT siblings; select physical cores')
    output = args.output or HERE / ('results_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
    output.mkdir(parents=True, exist_ok=False)
    metadata = dict(args=vars(args) | {'output': str(output)}, topology=nodes,
                    cpu_model=platform.machine(), timestamp=datetime.datetime.now().isoformat(),
                    allowed_cpus=sorted(os.sched_getaffinity(0)),
                    loadavg_start=os.getloadavg(), background_threads=bg,
                    metric='foreground logical sequential-read bytes/common window; decimal GB/s',
                    scope='DRAM-sized buffers; not direct physical die-link measurement',
                    page_validation='1024 evenly spaced base-page samples per thread before/after',
                    no_background='no benchmark background readers; unrelated system activity may exist')
    import hashlib
    metadata['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2, default=str) + '\n')
    cases = [('Intra-die (Decongestion)', args.source_node, 0),
             ('Intra-die (Congestion)', args.source_node, bg),
             ('Inter-die (Decongestion)', args.same_socket_node, 0),
             ('Inter-die (Congestion)', args.same_socket_node, bg),
             ('Inter-socket', args.remote_node, 0)]
    all_rows = []
    rng = random.Random(args.seed)
    for run in range(1, args.runs + 1):
        order = list(range(len(cases)))
        rng.shuffle(order)
        for index in order:
            label, home, competitors = cases[index]
            used = cpus[:args.foreground + competitors]
            argv = [str(binary), ','.join(map(str, used)), str(home),
                    str(args.foreground), str(args.mib), str(args.seconds)]
            print(f'run={run} {label}: CPUs={used}; memory=N{home}', flush=True)
            proc = subprocess.run(argv, capture_output=True, text=True)
            stem = output / f'case_{index + 1}_run_{run}'
            stem.with_suffix('.stderr').write_text(proc.stderr)
            if proc.returncode:
                raise RuntimeError(f'{label} failed: {proc.stderr.strip()}; {output}')
            raw = json.loads(proc.stdout)
            raw['command'] = argv
            raw['loadavg_after'] = os.getloadavg()
            stem.with_suffix('.json').write_text(json.dumps(raw, indent=2) + '\n')
            all_rows.append(dict(path=label, run=run, source_node=args.source_node,
                                 home_node=home, foreground_threads=args.foreground,
                                 background_threads=competitors, mib_per_thread=args.mib,
                                 foreground_GB_s=raw['foreground_GB_s'],
                                 foreground_window_s=raw['foreground_window_s'],
                                 foreground_bytes=raw['foreground_bytes']))
            with (output / 'runs.csv').open('w') as f:
                writer = csv.DictWriter(f, fieldnames=list(all_rows[0]))
                writer.writeheader(); writer.writerows(all_rows)
    summary = []
    for label, home, competitors in cases:
        values = [r['foreground_GB_s'] for r in all_rows if r['path'] == label]
        summary.append(dict(path=label, source_node=args.source_node, home_node=home,
                            foreground_threads=args.foreground, background_threads=competitors,
                            median_GB_s=statistics.median(values), min_GB_s=min(values),
                            max_GB_s=max(values), runs=len(values)))
    with (output / 'summary.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]))
        writer.writeheader(); writer.writerows(summary)
    latex = ['% Foreground effective sequential-read BW; GB=10^9 bytes; medians.',
             '% No-background vs imposed same-home competition; not physical link peak BW.',
             r'\begin{tabular}{@{}l|r@{}}', r'\hline',
             r'\textbf{Path} & \textbf{BW (GB/s)} \\', r'\hline']
    for row in summary:
        latex.append(f"{row['path']} & \\yanyz{{{row['median_GB_s']:.2f}}} \\\\")
        latex.append(r'\hline')
    latex.append(r'\end{tabular}')
    (output / 'table.tex').write_text('\n'.join(latex) + '\n')
    print('\nPath | median GB/s | min–max')
    for row in summary:
        print(f"{row['path']} | {row['median_GB_s']:.2f} | "
              f"{row['min_GB_s']:.2f}–{row['max_GB_s']:.2f}")
    print(f'\nResults: {output}', flush=True)


if __name__ == '__main__':
    main()
