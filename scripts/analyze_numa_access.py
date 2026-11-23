#!/usr/bin/env python3
"""Extract one query and analyze execution-NUMA -> data-page-NUMA bytes."""
import argparse
import csv
import gzip
import json
import hashlib
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path


JIT_ACCESS_ESTIMATES = {
    "agg_jit_state_candidate",
    "sonic_agg_jit_state_candidate",
    "jit_local_candidate",
    "jit_input_candidate",
    "jit_state_candidate",
}


def _record_lines(path, start=0, end=None):
    if start == 0 and end is None:
        opener = gzip.open if str(path).endswith('.gz') else open
        with opener(path, 'rt', errors='replace') as source:
            yield from source
        return
    with open(path, 'rb') as source:
        if start:
            source.seek(start - 1)
            if source.read(1) != b'\n': source.readline()
        while end is None or source.tell() < end:
            line = source.readline()
            if not line: break
            yield line.decode(errors='replace')


def records(path, start=0, end=None):
    for line in _record_lines(path, start, end):
        if "operator_data_access record=" in line:
            payload = line.split("operator_data_access ", 1)[1]
            record = dict(token.split("=", 1) for token in payload.split() if "=" in token)
            record['_digest'] = hashlib.sha256(payload.strip().encode()).digest()
            yield record


def log_files(directory):
    files = {}
    for path in list(Path(directory).glob("postgresql-*.log")) + list(Path(directory).glob("postgresql-*.log.gz")):
        key = str(path).removesuffix('.gz')
        if key not in files or path.suffix != '.gz': files[key] = path
    return [files[key] for key in sorted(files)]


def validate_stream_coverage(counts, ambiguous, expected):
    if ambiguous or set(counts) != set(expected):
        raise SystemExit(f"Stream NUMA binding invalid: expected nodes {sorted(expected)}, "
                         f"observed {dict(counts)}; threads spanning nodes: {ambiguous}")


def affinity(args):
    def expand(value):
        result = set()
        for part in value.split(","):
            bounds = part.split("-")
            result.update(range(int(bounds[0]), int(bounds[-1]) + 1))
        return result
    allowed = expand(args.cpus)
    pid = Path(args.pid_file).read_text().splitlines()[0]
    tasks = []
    for task in sorted((Path("/proc") / pid / "task").iterdir()):
        try:
            fields = dict(line.split(":", 1) for line in (task / "status").read_text().splitlines() if ":" in line)
        except FileNotFoundError:
            continue
        mask = fields["Cpus_allowed_list"].strip()
        tasks.append({"tid": task.name, "name": fields["Name"].strip(), "cpus": mask,
                      "outside": sorted(expand(mask) - allowed)})
    stream_counts = Counter()
    ambiguous_streams = []
    if args.require_stream_coverage:
        if not args.nodes:
            raise SystemExit("--nodes is required with --require-stream-coverage")
        node_cpus = {int(p.parent.name[4:]): expand(p.read_text().strip())
                     for p in Path('/sys/devices/system/node').glob('node[0-9]*/cpulist')}
        for task in tasks:
            if task['name'] != 'TPLstream':
                continue
            mask = expand(task['cpus'])
            owners = [n for n, cpus in node_cpus.items() if mask and mask <= cpus]
            if len(owners) != 1:
                ambiguous_streams.append(task['tid'])
            else:
                stream_counts[owners[0]] += 1
    result = {"pid": pid, "requested_cpus": args.cpus, "threads": tasks,
              "stream_threads_per_numa": dict(stream_counts), "ambiguous_streams": ambiguous_streams}
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    if not tasks or any(t["outside"] for t in tasks):
        raise SystemExit(f"Thread CPU masks escape the selected scope; inspect {args.output}")
    if args.require_stream_coverage:
        expected = {int(n) for n in args.nodes.split(',')}
        validate_stream_coverage(stream_counts, ambiguous_streams, expected)
        print(f"Verified Stream thread NUMA bindings: {dict(sorted(stream_counts.items()))}")


def snapshot(args):
    result = {}
    for path in log_files(args.log_dir):
        s = path.stat()
        result[str(path)] = [s.st_dev, s.st_ino, -1 if path.suffix == '.gz' else s.st_size]
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")


def extract(args):
    before = json.loads(Path(args.snapshot).read_text())
    initial = {str(k).removesuffix('.gz'): v[2] for k, v in before.items()}
    offsets = {}
    completed_archives = set()
    instances, ends, workers = set(), set(), set()
    count = 0
    previous_count = None
    deadline = time.monotonic() + args.timeout
    output = Path(args.output)
    partial = output.with_name(output.name + ".partial")
    query_match = re.compile(rb"\bquery_id=" + re.escape(args.query_id.encode()) + rb"(?=\s|$)")
    with partial.open("wb") as dest:
        # Always permit a complete scan plus a quiet check, even if reading a
        # large log itself exceeds the wait timeout. Timeout limits waiting for
        # missing records, not the CPU time of a completed file scan.
        while True:
            for path in log_files(args.log_dir):
                try:
                    identity = str(path).removesuffix('.gz')
                    if initial.get(identity) == -1: continue
                    # Rotated archives are atomically published and immutable.
                    # Reopening gzip merely to seek to its known EOF would
                    # decompress the entire archive again on every quiet check.
                    if path.suffix == '.gz' and identity in completed_archives: continue
                    opener = gzip.open if path.suffix == '.gz' else open
                    with opener(path, "rb") as source:
                        st = os.fstat(source.fileno())
                        offset = offsets.get(identity, initial.get(identity, 0))
                        source.seek(offset if path.suffix == '.gz' or st.st_size >= offset else 0)
                        while True:
                            start = source.tell()
                            line = source.readline()
                            if not line:
                                break
                            if not line.endswith(b"\n"):
                                source.seek(start)
                                break
                            if b"operator_data_access record=" not in line or not query_match.search(line):
                                continue
                            # All identity fields precede plan_id in emitted records.
                            header = line.split(b"operator_data_access ", 1)[1].split(b" plan_id=", 1)[0]
                            fields = dict(token.split(b"=", 1) for token in header.split() if b"=" in token)
                            instance = (fields[b"tid"], fields[b"instance"])
                            instances.add(instance)
                            if fields[b"record"] == b"end":
                                ends.add(instance)
                                workers.add(fields[b"worker_id"])
                            dest.write(line)
                            count += 1
                        offsets[identity] = source.tell()
                        if path.suffix == '.gz': completed_archives.add(identity)
                except FileNotFoundError:
                    continue
            complete = instances and instances == ends and len(workers) >= args.min_workers
            if complete and count == previous_count:
                dest.flush()
                break
            if time.monotonic() >= deadline and not (complete and count != previous_count):
                raise SystemExit(f"Incomplete statistics after waiting {args.timeout:g} seconds; saved {partial}. "
                                 f"Observed {len(instances)} instances, {len(ends)} ended; no heatmap produced.")
            previous_count = count
            time.sleep(1)
    partial.replace(output)
    print(f"Extracted query {args.query_id}: {count} records, {len(ends)} completed instances")


def _aggregation_worker_init():
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"


def _aggregate_records(job):
    path, nodes, start, end = job
    import numpy as np
    queries, instances, ended, seen = set(), set(), set(), set()
    ends = []
    record_counts = Counter()
    if not nodes or min(nodes) < 0 or max(nodes) > 7:
        raise ValueError("Expected NUMA IDs within 0..7")
    matrices = defaultdict(lambda: np.zeros((8, 8), dtype=np.int64))
    excluded = Counter()
    operations = defaultdict(Counter)
    outside = Counter()
    execution_outside = Counter()
    data_outside = Counter()
    scan_nodes = set()
    for r in records(path, start, end):
        digest = r.pop('_digest')
        if digest in seen:
            raise ValueError("Duplicate statistics records; refusing to double-count")
        seen.add(digest)
        queries.add(r['query_id'])
        instance = (r['tid'], r['instance'])
        instances.add(instance)
        record_counts[r['record']] += 1
        if r['record'] == 'end':
            ended.add(instance)
            ends.append(r)
        if r["record"] != "range":
            continue
        kind = "jit_candidate" if r["operation"] in JIT_ACCESS_ESTIMATES else "instrumented"
        key = f'{r["access_scope"]}_{kind}_{r["access"]}'
        amount = int(r["range_bytes"])
        operations[(r["access_scope"], r["operation"], r["access"])][r["locality"]] += amount
        x, y = int(r["execution_node"]), int(r["page_node"])
        if not (0 <= x < 8 and 0 <= y < 8):
            excluded[key] += amount
            continue
        matrices[key][x, y] += amount
        if r['operation'] == 'scan_access' and amount:
            scan_nodes.add(x)
        if x not in nodes or y not in nodes:
            outside[key] += amount
        if x not in nodes:
            execution_outside[key] += amount
        if y not in nodes:
            data_outside[key] += amount
    state = locals()
    return {key: dict(state[key]) if key in ("matrices", "operations") else state[key] for key in ['queries', 'instances', 'ended', 'seen', 'ends', 'record_counts', 'matrices', 'excluded', 'operations', 'outside', 'execution_outside', 'data_outside', 'scan_nodes']}


def analyze(args):
    import numpy as np
    import multiprocessing
    nodes = [int(n) for n in args.nodes.split(',')]
    if not nodes or min(nodes) < 0 or max(nodes) > 7:
        raise SystemExit('Expected NUMA IDs within0..7')
    if args.jobs < 1: raise SystemExit('jobs must be positive')
    size = Path(args.log).stat().st_size
    count = 1 if str(args.log).endswith('.gz') else min(args.jobs, max(1, size // (16 * 1024**2)))
    jobs = [(args.log, nodes, size*i//count, size*(i+1)//count) for i in range(count)] if count>1 else [(args.log,nodes,0,None)]
    print(f'Aggregating access log with {count} process(es)', flush=True)
    combined = None
    def merge(part):
        nonlocal combined
        if combined is None:
            combined = part
            return
        previous = len(combined['seen'])
        combined['seen'].update(part['seen'])
        if len(combined['seen']) != previous + len(part['seen']):
            raise ValueError('Duplicate statistics records across log segments; refusing to double-count')
        for key in ('queries','instances','ended','scan_nodes'): combined[key].update(part[key])
        combined['ends'].extend(part['ends'])
        for key in ('record_counts','excluded','outside','execution_outside','data_outside'): combined[key].update(part[key])
        for key,value in part['matrices'].items():
            if key not in combined['matrices']: combined['matrices'][key] = np.zeros((8,8),dtype=np.int64)
            combined['matrices'][key] += value
        for key,value in part['operations'].items(): combined['operations'].setdefault(key,Counter()).update(value)
    try:
        if count == 1: merge(_aggregate_records(jobs[0]))
        else:
            # Fresh processes avoid inheriting NumPy/BLAS thread locks. Only
            # in-memory partial aggregates are exchanged; no segment files.
            with multiprocessing.get_context('spawn').Pool(count, initializer=_aggregation_worker_init) as pool:
                for part in pool.imap(_aggregate_records,jobs): merge(part)
    except ValueError as error:
        raise SystemExit(str(error))
    queries = combined['queries']
    instances = combined['instances']
    ended = combined['ended']
    seen = combined['seen']
    ends = combined['ends']
    record_counts = combined['record_counts']
    matrices = combined['matrices']
    excluded = combined['excluded']
    operations = combined['operations']
    outside = combined['outside']
    execution_outside = combined['execution_outside']
    data_outside = combined['data_outside']
    scan_nodes = combined['scan_nodes']
    seen.clear()  # Deduplication is complete; release its memory before plotting.
    if not record_counts:
        raise SystemExit("No statistics records")
    if len(queries) != 1:
        raise SystemExit("Expected exactly one query_id; filter the log first")
    if instances != ended:
        raise SystemExit("Missing end records: statistics may be truncated")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    summary = {
        "query_id": next(iter(queries)), "label": args.label, "nodes": nodes,
        "record_counts": dict(record_counts), "instances": len(ends),
        "page_queries": sum(int(r["page_queries"]) for r in ends),
        "failed_queries": sum(int(r["failed_queries"]) for r in ends),
        "excluded_bytes": dict(excluded), "outside_scope_bytes": dict(outside),
        "execution_outside_scope_bytes": dict(execution_outside),
        "data_outside_scope_bytes": dict(data_outside),
        "execution_scope_valid": not bool(execution_outside),
        "data_scope_valid": not bool(data_outside),
        "scan_execution_nodes": sorted(scan_nodes),
        "node_coverage_valid": set(nodes) == scan_nodes,
        "matrices": {key: m.tolist() for key, m in sorted(matrices.items())},
    }
    for key, m in matrices.items():
        with (out / (key + "_bytes.csv")).open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["execution_numa/data_numa", *range(8)])
            w.writerows([[i, *row] for i, row in enumerate(m)])
    with (out / "operations.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scope", "operation", "access", "bytes", "local", "remote", "unknown", "not_observed"])
        for key, counts in sorted(operations.items()):
            w.writerow([*key, sum(counts.values()), *(counts[k] for k in ("local", "remote", "unknown", "not_observed"))])
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    report = [f"# {args.label}", "", f'Query ID: {summary["query_id"]}', "",
              "行=线程执行 NUMA，列=数据页 NUMA；CSV 单位为字节，热图为 GiB。对角线是本地访问。",
              "热图合并普通读写与 JIT candidate 估计，按 intra/inter 展示；交付标记不相加，以免重复计数。",
              "分类明细保留于 CSV；累计逻辑字节不是 DRAM 流量或唯一内存大小，含 JIT 估计。",
              "线程位置是采样快照，不能表示线程始终固定在该节点。", "",
              f'完成实例 {len(ends)}；页位置查询 {summary["page_queries"]:,} 次；调用失败 {summary["failed_queries"]} 次。', "",
              "| 类别 | GiB | 远端比例 |", "|---|---:|---:|"]
    if not summary['node_coverage_valid']:
        report[2:2] = [f"**WARNING: configured NUMA {nodes}, observed scan execution NUMA {sorted(scan_nodes)}. "
                       "This is not a verified multi-NUMA execution.**", ""]
    if data_outside:
        report[2:2] = [f"**Data placement: {sum(data_outside.values()):,} cumulative logical bytes access pages outside "
                       f"the CPU execution set {nodes}. All data-page NUMA nodes 0–7 are retained. "
                       "This does not invalidate CPU binding; logical bytes are not unique memory size.**", ""]
    for key, m in sorted(matrices.items()):
        total = int(m.sum())
        if total:
            report.append(f"| {key} | {total / 2**30:.6f} | {100 * (total - int(np.trace(m))) / total:.2f}% |")
    report += ["", "## 未定位页的逻辑交付/访问", "", json.dumps(dict(excluded), ensure_ascii=False),
               "", "## 线程与数据页的位置（分别统计）", "",
               f"执行线程目标 NUMA：{nodes}；数据页展示范围：0–7。",
               "线程范围外：" + json.dumps(dict(execution_outside), ensure_ascii=False),
               "数据页在执行节点集合外：" + json.dumps(dict(data_outside), ensure_ascii=False),
               "二者并集（兼容字段）：" + json.dumps(dict(outside), ensure_ascii=False), "",
               "outside_scope_bytes 为线程或数据页越界的并集；execution_outside_scope_bytes 和 "
               "data_outside_scope_bytes 分别给出线程与数据页越界，二者可能重叠，不直接相加。",
               "failed_queries=0 不排除个别页返回负状态；请同时检查 operations.csv 的 unknown 字节。"]
    (out / "report.md").write_text("\n".join(report) + "\n")
    if args.require_node_coverage and (not summary['node_coverage_valid'] or execution_outside):
        raise SystemExit(f"NUMA coverage invalid: expected {nodes}, scan nodes {sorted(scan_nodes)}, "
                         f"execution out-of-scope bytes {sum(execution_outside.values())}. Saved diagnostics; no heatmap/comparison produced.")
    if args.require_execution_scope and execution_outside:
        raise SystemExit('Execution outside selected CPU NUMA nodes; diagnostics saved, no heatmaps produced.')
    if args.require_data_scope and data_outside:
        raise SystemExit('Data pages escape requested NUMA nodes. Saved diagnostics; strict data-scope check failed.')
    for scope in ("intra", "inter"):
        plot([summary], scope, out / f"{scope}_heatmap.png")
    for legacy in ("read_heatmaps.png", "write_heatmaps.png"):
        (out / legacy).unlink(missing_ok=True)
    print(f'Analysis: {out}; execution out-of-scope bytes: {sum(execution_outside.values())}; '
          f'data out-of-scope bytes: {sum(data_outside.values())}')
    if data_outside:
        print('Data pages outside the CPU execution set are retained and annotated; CPU validation is independent.')


def combined_matrix(summary, scope):
    """Sum read/write observations and JIT estimates; delivery is not an access."""
    import numpy as np
    result = np.zeros((8, 8), dtype=np.int64)
    for kind in ("instrumented", "jit_candidate"):
        for access in ("read", "write"):
            key = f"{scope}_{kind}_{access}"
            result += np.array(summary["matrices"].get(key, np.zeros((8, 8))), dtype=np.int64)
    return result


def plot(summaries, scope, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize, LinearSegmentedColormap, to_rgb
    from matplotlib.patches import Patch
    import numpy as np
    arrays = [combined_matrix(s, scope) for s in summaries]
    diagonal = np.eye(8, dtype=bool)
    # Only remote cells set the quantitative color range. Local cells have a
    # fixed categorical color and retain their exact GiB labels.
    vmax = max(float(np.where(diagonal, 0, a).max()) / 2**30 for a in arrays) or 1
    norm = Normalize(vmin=0, vmax=vmax)
    cmap = LinearSegmentedColormap.from_list("remote_access", ["#fee6ce", "#fdae6b", "#e6550d", "#a63603"])
    cmap.set_bad((0, 0, 0, 0))
    local_color = "#d6e8f5"
    fig, axes = plt.subplots(1, len(summaries), figsize=(7 * len(summaries), 7.6), squeeze=False)
    fig.subplots_adjust(left=.06, right=.94, bottom=.10, top=.72 if len(summaries)==1 else .78, wspace=.32)
    for ax, s, m in zip(axes[0], summaries, arrays):
        g = m / 2**30
        background = np.ones((8, 8, 3))
        background[diagonal & (m > 0)] = to_rgb(local_color)
        ax.imshow(background)
        im = ax.imshow(np.ma.masked_where(diagonal | (m == 0), g), cmap=cmap, norm=norm)
        total = int(m.sum())
        remote = f'{100 * (total - int(np.trace(m))) / total:.2f}%' if total else 'N/A'
        ax.set_title(f'{s["label"]} — {scope}\n{total / 2**30:.3f} GiB; remote {remote}', fontsize=12)
        ax.set_xticks(range(8)); ax.set_yticks(range(8))
        ax.set_xticklabels([str(n) if n in s['nodes'] else f'{n}*' for n in range(8)])
        ax.set_xlabel("Data-page NUMA"); ax.set_ylabel("Execution NUMA")
        ax.set_xticks(np.arange(-.5, 8, 1), minor=True)
        ax.set_yticks(np.arange(-.5, 8, 1), minor=True)
        ax.grid(which="minor", color="#ffffff", linewidth=.7)
        ax.tick_params(which="minor", bottom=False, left=False)
        for i in range(8):
            for j in range(8):
                if m[i,j] == 0:
                    continue
                ax.text(j, i, f"{g[i,j]:.2f}", ha="center", va="center", fontsize=9,
                        color="white" if i != j and norm(g[i,j]) > .65 else "#17212b")
        fig.colorbar(im, ax=ax, shrink=.75, label="Remote logical GiB (linear scale)")
    fig.suptitle(f"{scope.upper()}: execution NUMA × data-page NUMA\n"
                 "Logical GiB; JIT estimates included; delivery excluded.\n"
                 "*: data node outside CPU set.", fontsize=10, y=.98)
    fig.legend(handles=[Patch(facecolor=local_color, label="Local: fixed blue (values shown)"),
                        Patch(facecolor="#e6550d", label="Remote: shared linear GiB scale"),
                        Patch(facecolor="white", edgecolor="#cccccc", label="Zero: white")],
               loc="upper center", bbox_to_anchor=(.5, .89), ncol=1 if len(summaries)==1 else 3,
               frameon=False, fontsize=8 if len(summaries)==1 else 9)
    fig.savefig(path, dpi=160, bbox_inches='tight', pad_inches=.15)
    plt.close(fig)


def compare(args):
    left, right = Path(args.left), Path(args.right)
    # Only automatically pair comparable queries/configurations.
    def signature(analysis):
        parent = analysis.parent
        meta = dict(line.split("=", 1) for line in (parent / "metadata.txt").read_text().splitlines() if "=" in line)
        settings = {}
        for line in (parent / "postgresql.conf").read_text().splitlines():
            m = re.match(r"\s*([\w.]+)\s*=\s*(.*)", line)
            if m and m[1] not in {"thread_pool_attr", "thread_pool_stream_attr", "port", "pgxc_node_name"}:
                settings[m[1]] = m[2].strip()
        keys = ("query", "schema", "query_sha256", "gaussdb_sha256", "intra", "inter",
                "partition_scan", "numa_aware_partition_scan")
        return {k: meta[k] for k in keys}, meta.get('data_placement', 'legacy_explicit_mapping'), meta.get('launcher_memory_policy', 'legacy_membind'), settings
    if signature(left) != signature(right):
        raise SystemExit("Both runs finished, but query/schema/configurations differ. Refusing automatic comparison.")
    summaries = [json.loads((p / "summary.json").read_text()) for p in (left, right)]
    if any(s.get('node_coverage_valid') is not True or s.get('execution_scope_valid') is not True
           for s in summaries):
        raise SystemExit('NUMA execution coverage is unverified/invalid; rerun both experiments with binding checks.')
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    for scope in ("intra", "inter"):
        plot(summaries, scope, out / f"{scope}_comparison.png")
    for legacy in ("read_comparison.png", "write_comparison.png"):
        (out / legacy).unlink(missing_ok=True)
    (out / "sources.json").write_text(json.dumps([str(p.resolve()) for p in (left, right)], indent=2) + "\n")
    print(f"Comparison heatmaps: {out}")


def batch_compare(args):
    left,right=Path(args.left),Path(args.right)
    left_metadata=dict(line.split('=',1) for line in (left/'metadata.txt').read_text().splitlines() if '=' in line)
    if left_metadata.get('strict_memory_nodes')!='on':
        raise SystemExit('4NUMA batch lacks strict memory-node validation; refuse strict comparison')
    for name in ('installed_gaussdb.sha256','query_sha256.txt'):
        if (left/name).read_text() != (right/name).read_text():
            raise SystemExit('Installed binary or SQL hashes differ; refusing paired totals')
    def load(root):
        rows=list(csv.DictReader((root/'batch_summary.csv').open()))
        if {int(r['query']) for r in rows} != set(range(1,23)) or len(rows)!=22:
            raise SystemExit('Expected all22 queries once in each batch')
        return {int(r['query']):json.loads((Path(r['artifact_dir'])/'analysis/summary.json').read_text()) for r in rows}
    a,b=load(left),load(right);out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    fields=['query','scope','local_4_bytes','remote_4_bytes','unknown_4_bytes','total_4_bytes','local_8_bytes','remote_8_bytes','unknown_8_bytes','total_8_bytes','remote_delta_bytes','remote_growth_percent','remote_share_4_percent','remote_share_8_percent','remote_share_delta_pp','estimated_4_bytes','estimated_8_bytes','data_outside_cpu_4_bytes','data_outside_cpu_8_bytes']
    def totals(s,scope):
        local=remote=unknown=estimated=outside_cpu=0
        for kind in ('instrumented','jit_candidate'):
            for access in ('read','write'):
                key=f'{scope}_{kind}_{access}';matrix=s['matrices'].get(key,[])
                k=sum(sum(row) for row in matrix);l=sum(row[i] for i,row in enumerate(matrix));u=s['excluded_bytes'].get(key,0)
                local+=l;remote+=k-l;unknown+=u
                outside_cpu+=sum(v for row in matrix for j,v in enumerate(row) if j not in s['nodes'])
                if kind=='jit_candidate':estimated+=k+u
        return local,remote,unknown,local+remote+unknown,estimated,outside_cpu
    with (out/'access_comparison.csv').open('w') as f:
        writer=csv.writer(f);writer.writerow(fields)
        for q in range(1,23):
            if not a[q]['execution_scope_valid'] or not b[q]['execution_scope_valid']:raise SystemExit(f'Q{q}: execution scope invalid')
            if not a[q]['data_scope_valid'] or not b[q]['data_scope_valid']:raise SystemExit(f'Q{q}: data pages outside requested NUMA nodes')
            pair=out/f'q{q}';pair.mkdir(exist_ok=True)
            for scope in ('intra','inter'):
                x,y=totals(a[q],scope),totals(b[q],scope)
                sx=100*x[1]/(x[0]+x[1]) if x[0]+x[1] else None;sy=100*y[1]/(y[0]+y[1]) if y[0]+y[1] else None
                writer.writerow([q,scope,*x[:4],*y[:4],y[1]-x[1],100*(y[1]/x[1]-1) if x[1] else '',sx,sy,sy-sx if sx is not None and sy is not None else '',x[4],y[4],x[5],y[5]])
                plot([a[q],b[q]],scope,pair/f'{scope}.png')
    (out/'README.md').write_text('Baseline4NUMA/DOP96 vs8NUMA/DOP168. The4NUMA run requires a validated launcher MPOL_BIND preserved by the server and every query must pass execution/data scope checks. Native allocation is retained within that boundary; no explicit partition-to-node map is introduced. All8 data-node columns are retained. Cumulative logical read+write bytes; delivery markers excluded. JIT estimates are included and also reported separately. Unknown page locations remain unknown; remote fractions exclude unknown bytes. CPU count and DOP change together; this is a configuration comparison, not isolated NUMA scaling. Per-query paired heatmaps share remote-cell color scales.\n')
    print('Comparison:',out/'access_comparison.csv')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    subs = p.add_subparsers(dest="command", required=True)
    s = subs.add_parser("affinity")
    for arg in ("pid-file", "cpus", "output"):
        s.add_argument("--" + arg, required=True)
    s.set_defaults(func=affinity)
    s.add_argument('--nodes')
    s.add_argument('--require-stream-coverage', action='store_true')
    s = subs.add_parser("snapshot")
    s.add_argument("--log-dir", required=True); s.add_argument("--output", required=True); s.set_defaults(func=snapshot)
    s = subs.add_parser("extract")
    for arg in ("log-dir", "snapshot", "query-id", "output"):
        s.add_argument("--" + arg, required=True)
    s.add_argument("--min-workers", type=int, default=1)
    s.add_argument("--timeout", type=float, default=300); s.set_defaults(func=extract)
    s = subs.add_parser("analyze")
    for arg in ("log", "nodes", "output"):
        s.add_argument("--" + arg, required=True)
    s.add_argument("--jobs", type=int, default=8, help="Parallel aggregation processes; gzip input stays serial")
    s.add_argument("--label", default="NUMA access"); s.set_defaults(func=analyze)
    s.add_argument('--require-node-coverage', action='store_true')
    s.add_argument('--require-data-scope', action='store_true')
    s.add_argument('--require-execution-scope', action='store_true')
    s = subs.add_parser("compare")
    for arg in ("left", "right", "output"):
        s.add_argument("--" + arg, required=True)
    s.set_defaults(func=compare)
    s = subs.add_parser("batch")
    for arg in ("left", "right", "output"): s.add_argument("--" + arg, required=True)
    s.set_defaults(func=batch_compare)
    args = p.parse_args(); args.func(args)


if __name__ == "__main__":
    main()
