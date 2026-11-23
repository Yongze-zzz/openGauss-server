#!/usr/bin/env python3
"""Compare normalized operator wall-time composition and LLC-read MPKI by LLC domain.
Reads existing per-query analysis CSVs; does not execute SQL or change raw traces.
"""
import argparse
import csv
import itertools
import json
import math
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb, Normalize
from matplotlib.patches import Patch

from cache_trace_metadata import FAMILIES

COLORS = ['#a451d6', '#50b59a', '#d5a1e2', '#a3d6be', '#c4b19e', '#abb1bd']
UNKNOWN = '#ededed'


def ids(text):
    out = set()
    for part in text.split(','):
        ends = list(map(int, part.split('-')))
        out.update(range(ends[0], ends[-1] + 1))
    return out


def thread_key(row):
    return row['os_tid'], row['thread_start_ns']


def summarize(point, count, bins, node_tags):
    analysis = point / 'analysis'
    coverage = json.loads((point / 'collection_coverage.json').read_text())
    quality = json.loads((analysis / 'summary.json').read_text())
    if quality['incomplete_threads'] or coverage['cache_trace_started'] != coverage['cache_trace_written'] or coverage['cache_trace_written'] != coverage['binary_files']:
        raise ValueError(f'{point}: incomplete cache collection')
    if any(coverage[k] for k in ['cache_trace_start_failed', 'cache_trace_perf_unavailable', 'cache_trace_write_failed', 'cache_trace_discarded']):
        raise ValueError(f'{point}: cache collection failure')
    meta = dict(line.split('=', 1) for line in (point / 'metadata.txt').read_text().splitlines() if '=' in line)
    allowed = ids(meta['cpu_pool'])
    manifest = analysis / 'trace_metadata.json'
    if manifest.is_file():
        capture = json.loads(manifest.read_text())
        topo = capture['topology']
        plan_families = capture['plan_node_families']
    else:
        if node_tags is None:
            raise ValueError(f'{point}: use --node-tags with the archived NodeTag mapping')
        topo = json.loads((analysis / 'topology.json').read_text())
        plan_families = None
    domains = sorted({tuple(sorted(ids(r['llc_shared_cpus']))) for r in topo if r['cpu'] in allowed}, key=lambda x: x[0])
    if len(domains) != count:
        raise ValueError(f'{point}: expected {count} actual LLC domains, found {len(domains)}')
    cpumap = {cpu: i for i, domain in enumerate(domains) for cpu in domain if cpu in allowed}
    windows_path = analysis / 'windows.csv'
    operators_path = analysis / 'operators.csv'
    order = {}
    t0 = None
    t1 = 0
    with windows_path.open() as f:
        reader = csv.DictReader(f)
        if 'start_observed_cpu' not in reader.fieldnames:
            raise ValueError(f'{point}: requires new analysis with start_observed_cpu')
        for row in reader:
            start, end = int(row['start_ns']), int(row['end_ns'])
            t0 = start if t0 is None else min(t0, start)
            t1 = max(t1, end)
            order.setdefault(thread_key(row), len(order))
    if t0 is None or t1 <= t0:
        raise ValueError('No observed timeline')
    span = t1 - t0
    counts = np.zeros((count, 2, bins), dtype=np.float64)
    activity = np.zeros((count, len(FAMILIES), bins), dtype=np.float64)
    observed = np.zeros((count, bins), dtype=np.float64)
    stats = dict(valid_windows=0, valid_instructions=0, valid_llc_load_misses=0,
                 cross_domain_windows=0, unknown_domain_windows=0, operator_records=0,
                 assigned_operator_wall_ns=0, unassigned_operator_wall_ns=0,
                 dop=int(meta['dop']), assigned_observed_wall_ns=0, observed_start_ns=t0, observed_end_ns=t1, normalized_bins=bins,
                 domains=[list(x) for x in domains])

    def add(array, start, end, amount):
        if end <= start or amount == 0:
            return
        first = max(0, (start - t0) * bins // span)
        last = min(bins - 1, ((end - t0) * bins - 1) // span)
        for b in range(first, last + 1):
            left = max(start - t0, span * b / bins)
            right = min(end - t0, span * (b + 1) / bins)
            if right > left:
                array[b] += amount * (right - left) / (end - start)

    def consume_unassigned(rows):
        for r in rows:
            if r['valid'] == '1':
                stats['unassigned_operator_wall_ns'] += int(r['exclusive_wall_ns'])

    with windows_path.open() as wf, operators_path.open() as of:
        window_groups = itertools.groupby(csv.DictReader(wf), thread_key)
        op_groups = iter(itertools.groupby(csv.DictReader(of), thread_key))
        current = next(op_groups, None)
        for key, window_rows in window_groups:
            positions = {}
            for row in window_rows:
                start, end = int(row['start_ns']), int(row['end_ns'])
                a = cpumap.get(int(row['start_observed_cpu']))
                b = cpumap.get(int(row['observed_cpu']))
                domain = None
                if a is None or b is None:
                    stats['unknown_domain_windows'] += 1
                elif a != b:
                    stats['cross_domain_windows'] += 1
                else:
                    domain = a
                    add(observed[domain], start, end, end - start)
                    stats['assigned_observed_wall_ns'] += end - start
                positions[(start, end)] = domain
                if row['valid'] == '1':
                    if domain is None:
                        raise ValueError('Valid PMU interval has no consistent in-scope LLC domain')
                    ins, miss = int(row['instructions']), int(row['llc_load_misses'])
                    stats['valid_windows'] += 1
                    stats['valid_instructions'] += ins
                    stats['valid_llc_load_misses'] += miss
                    add(counts[domain, 0], start, end, miss)
                    add(counts[domain, 1], start, end, ins)
            while current is not None and order.get(current[0], -1) < order[key]:
                consume_unassigned(current[1])
                current = next(op_groups, None)
            if current is not None and current[0] == key:
                for row in current[1]:
                    if row['valid'] != '1':
                        continue
                    stats['operator_records'] += 1
                    start, end = int(row['start_ns']), int(row['end_ns'])
                    wall = int(row['exclusive_wall_ns'])
                    domain = positions.get((start, end))
                    if domain is None or row['exact_window_boundary_match'] != '1':
                        stats['unassigned_operator_wall_ns'] += wall
                        continue
                    if plan_families is not None:
                        name = plan_families.get(row['plan_id'], 'Other')
                    else:
                        name = node_tags.get(row['node_tag'], 'Other')
                    family = FAMILIES.index(name)
                    add(activity[domain, family], start, end, wall)
                    stats['assigned_operator_wall_ns'] += wall
                current = next(op_groups, None)
        while current is not None:
            consume_unassigned(current[1])
            current = next(op_groups, None)
    for index, total in [(0, stats['valid_llc_load_misses']), (1, stats['valid_instructions'])]:
        if not math.isclose(float(counts[:, index].sum()), total, rel_tol=1e-10, abs_tol=.5):
            raise ValueError('Normalized rebinning did not conserve PMU counts')
    for array, total in [(activity, stats['assigned_operator_wall_ns']), (observed, stats['assigned_observed_wall_ns'])]:
        if not math.isclose(float(array.sum()), total, rel_tol=1e-10, abs_tol=1):
            raise ValueError('Normalized rebinning did not conserve wall-time totals')
    summary = json.loads((analysis / 'summary.json').read_text())
    if stats['valid_instructions'] != summary['instructions'] or stats['valid_llc_load_misses'] != summary['llc_load_misses']:
        raise ValueError('Rebinned counts do not match analyzed valid-window totals')
    unknown = np.maximum(0, observed - activity.sum(axis=1))
    stats['operator_wall_exceeds_observed_bins'] = int(np.sum(activity.sum(axis=1) > observed * 1.01 + 1))
    weights = np.concatenate([activity, unknown[:, None, :]], axis=1)
    den = weights.sum(axis=1, keepdims=True)
    shares = np.divide(weights, den, out=np.zeros_like(weights), where=den > 0)
    mpki = np.divide(1000 * counts[:, 0], counts[:, 1], out=np.full((count, bins), np.nan), where=counts[:, 1] > 0)
    stats['missing_mpki_cells'] = int(np.isnan(mpki).sum())
    stats['empty_operator_cells'] = int((den[:, 0] == 0).sum())
    return dict(shares=shares, mpki=mpki, stats=stats, counts=counts, activity=activity, observed=observed)


def operator_image(shares, height=32):
    domains, families, bins = shares.shape
    rgb = np.empty((domains * height, bins, 3))
    rgb[:] = to_rgb(UNKNOWN)
    palette = np.array([to_rgb(c) for c in COLORS + [UNKNOWN]])
    for domain in range(domains):
        for b in range(bins):
            if shares[domain, :, b].sum() == 0:
                continue
            bounds = np.rint(np.cumsum(shares[domain, :, b]) * height).astype(int)
            lo = 0
            for family, hi in enumerate(bounds):
                rgb[domain * height + lo:domain * height + hi, b] = palette[family]
                lo = hi
    return rgb


def color_scale(pair):
    valid = np.concatenate([x['mpki'][np.isfinite(x['mpki'])] for x in pair.values()])
    vmax = max(1., float(f'{np.quantile(valid, .99):.3g}')) if len(valid) else 1.
    return vmax, bool(len(valid) and valid.max() > vmax)


def draw_panel(figure, cell, query, pair, vmax, clipped):
    grid = cell.subgridspec(4, 2, height_ratios=[4, 4, 8, 8], width_ratios=[1, .035], hspace=.55, wspace=.04)
    cmap = plt.get_cmap('Blues').copy()
    cmap.set_bad(UNKNOWN)
    for pos, (count, kind) in enumerate([(4, 'operators'), (4, 'mpki'), (8, 'operators'), (8, 'mpki')]):
        ax = figure.add_subplot(grid[pos, 0])
        d = pair[count]
        if kind == 'operators':
            ax.imshow(operator_image(d['shares']), extent=(0, 100, count - .5, -.5), aspect='auto', interpolation='nearest')
            ax.set_title(f"{count}-chiplet · Operators (DOP {d['stats']['dop']})", loc='left', fontsize=9, pad=3)
        else:
            image = ax.imshow(d['mpki'], cmap=cmap, norm=Normalize(0, vmax), extent=(0, 100, count - .5, -.5), aspect='auto', interpolation='nearest')
            ax.set_title('LLC-read MPKI', loc='left', fontsize=9, pad=3)
            if count == 8:
                bar = figure.colorbar(image, cax=figure.add_subplot(grid[3, 1]))
                bar.set_ticks([0, vmax / 2, vmax])
                bar.set_ticklabels(['0', f'{vmax/2:.3g}', ('≥' if clipped else '') + f'{vmax:.3g}'])
                bar.ax.tick_params(labelsize=7)
        ax.set_yticks(range(count), [f'C{i}' for i in range(count)], fontsize=7)
        ax.set_xticks([0, 50, 100])
        ax.tick_params(axis='both', length=2, pad=1, labelsize=7)
        if pos < 3:
            ax.set_xticklabels([])
        else:
            ax.set_xlabel(f'Q{query} — normalized observed execution time (%)', fontsize=9)
        for y in np.arange(.5, count - .5, 1):
            ax.axhline(y, color='white', linewidth=.45)
        for spine in ax.spines.values():
            spine.set_linewidth(.5)


def make_figure(data, queries):
    cols = 2 if len(queries) > 1 else 1
    rows = math.ceil(len(queries) / cols)
    fig = plt.figure(figsize=(5.1 * cols, 7.1 * rows + .55))
    grid = fig.add_gridspec(rows, cols, left=.065, right=.94, bottom=.065, top=.93, hspace=.27, wspace=.33)
    for i, query in enumerate(queries):
        pair = data[query]
        vmax, clipped = color_scale(pair)
        draw_panel(fig, grid[i // cols, i % cols], query, pair, vmax, clipped)
    handles = [Patch(facecolor=c, label=f) for f, c in zip(FAMILIES + ('Unassigned / no sample',), COLORS + [UNKNOWN])]
    fig.legend(handles=handles, loc='upper center', ncol=4, frameon=False, fontsize=8)
    fig.text(.5, .012, 'Wall-time composition includes waiting. Proportional rebinning; gray PMU cells have no valid samples. Shared color limit: 99th percentile (≥ means clipped).' , ha='center', fontsize=7)
    return fig


def render(data, queries, output, filename):
    fig = make_figure(data, queries)
    fig.savefig(output / (filename + '.png'), dpi=200)
    fig.savefig(output / (filename + '.pdf'))
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--four', type=Path, required=True)
    parser.add_argument('--eight', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--queries', nargs='+', type=int, default=list(range(1, 23)))
    parser.add_argument('--bins', type=int, default=100)
    parser.add_argument('--node-tags', type=Path, help='Archived NodeTag map for traces without capture metadata')
    args = parser.parse_args()
    if args.bins < 1 or len(set(args.queries)) != len(args.queries) or any(q < 1 or q > 22 for q in args.queries):
        parser.error('Use unique TPC-H query IDs 1..22 and positive bins')
    args.output.mkdir(parents=True, exist_ok=True)
    node_tags = None
    if args.node_tags:
        mapping = json.loads(args.node_tags.read_text())
        node_tags = mapping.get('node_tag_families', mapping)
    data = {}
    manifest = dict(normalized_bins=args.bins, event='user-space LLC read misses per1000 retired instructions',
                    normalization='First to last observed PMU interval, separately per configuration/query',
                    domain_assignment='Exact operator/PMU interval endpoints; both CPU snapshots must belong to the same recorded LLC domain',
                    timing='Operator shares use exclusive wall time, including blocked time; within-sample timing is unknown',
                    interpolation='PMU totals and operator duration proportionally allocated by interval overlap; no additional temporal precision implied',
                    node_tag_families=node_tags,
                    input_four=str(args.four.resolve()), input_eight=str(args.eight.resolve()), queries={})
    with (args.output / 'normalized.csv').open('w') as f:
        writer = csv.writer(f)
        writer.writerow(['query', 'numa', 'chiplet', 'bin', 'time_start_pct', 'time_end_pct', 'llc_load_misses', 'instructions', 'llc_read_mpki'] + [x.lower() + '_wall_share' for x in FAMILIES] + ['unassigned_wall_share'])
        for q in args.queries:
            data[q] = {}
            manifest['queries'][str(q)] = {}
            for count, root in [(4, args.four), (8, args.eight)]:
                d = summarize(root / f'q{q}_on', count, args.bins, node_tags)
                data[q][count] = d
                manifest['queries'][str(q)][str(count)] = d['stats']
                for domain in range(count):
                    for b in range(args.bins):
                        v = d['mpki'][domain, b]
                        writer.writerow([q, count, domain, b, b * 100 / args.bins, (b + 1) * 100 / args.bins, d['counts'][domain, 0, b], d['counts'][domain, 1, b], float(v) if np.isfinite(v) else ''] + list(d['shares'][domain, :, b]))
            vmax, clipped = color_scale(data[q])
            manifest['queries'][str(q)]['mpki_color_scale'] = dict(vmax=vmax, clipped=clipped, percentile=99, shared_across_configurations=True)
            render(data, [q], args.output, f'q{q}')
            print(f'Q{q}: normalized counts conserved; figure complete', flush=True)
    selected = list(args.queries)
    if selected:
        render(data, selected, args.output, 'overview')
    (args.output / 'figure_metadata.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
