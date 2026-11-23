#!/usr/bin/env python3
"""Decode v5 cache traces and replay per-thread PMU and operator durations."""
import argparse
import csv
import json
import struct
from collections import defaultdict
from pathlib import Path

from cache_trace_metadata import FAMILIES


HEADER = struct.Struct("<IHH6Iii11Q")
SAMPLE = struct.Struct("<4Q4i2IQiI")
OPERATOR_TIME = struct.Struct("<3Q")
TRACE_MAGIC = 0x43545243
TRACE_VERSION = 5
TRACE_INCOMPLETE = 1
PERF_VALID = 2
REPLAY_BIN_NS = 100_000_000

WINDOW_FIELDS = (
    "query_id", "os_tid", "thread_start_ns", "start_ns", "end_ns", "observed_cpu",
    "instructions", "llc_load_misses", "llc_read_mpki", "counter_coverage", "valid", "start_observed_cpu",
)
OPERATOR_FIELDS = (
    "query_id", "os_tid", "thread_start_ns", "start_ns", "end_ns", "plan_id", "node_tag",
    "exclusive_wall_ns", "valid", "window_wall_share", "thread_window_llc_read_mpki",
    "counter_coverage", "exact_window_boundary_match",
)


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def read_record(source, layout):
    data = source.read(layout.size)
    if len(data) != layout.size:
        raise ValueError(f"{source.name}: truncated trace")
    return layout.unpack(data)


def read_windows(source, count, thread, complete, writer, summary):
    previous = None
    windows = {}
    thread_has_valid_window = False
    for _ in range(count):
        time_ns, _, instructions, misses, _, _, _, _, flags, running_us, enabled_ns, cpu, _ = read_record(
            source, SAMPLE
        )
        if previous is not None:
            start, start_instructions, start_misses, start_flags, start_running, start_enabled, start_cpu = previous
            instruction_delta = instructions - start_instructions
            miss_delta = misses - start_misses
            enabled_delta = enabled_ns - start_enabled
            running_delta = ((running_us - start_running) & 0xffffffff) * 1000
            coverage = running_delta / enabled_delta if enabled_delta > 0 else 0
            valid = bool(
                complete and flags & PERF_VALID and start_flags & PERF_VALID
                and cpu == start_cpu and cpu >= 0 and instruction_delta > 0
                and miss_delta >= 0 and 0.90 <= coverage <= 1.01 and time_ns > start
            )
            summary["valid_windows" if valid else "invalid_windows"] += 1
            if valid:
                summary["instructions"] += instruction_delta
                summary["llc_load_misses"] += miss_delta
                thread_has_valid_window = True
            mpki = 1000 * miss_delta / instruction_delta if valid else ""
            windows[start, time_ns] = (mpki, coverage)
            writer.writerow((*thread, start, time_ns, cpu, instruction_delta, miss_delta,
                             mpki, coverage, int(valid), start_cpu))
        previous = (time_ns, instructions, misses, flags, running_us, enabled_ns, cpu)
    summary["threads_with_valid_windows"] += int(thread_has_valid_window)
    return windows


def operator_observations(source, count):
    timestamp = None
    records = []
    for _ in range(count):
        time_ns, key, duration = read_record(source, OPERATOR_TIME)
        if timestamp is not None and time_ns != timestamp:
            yield timestamp, records
            records = []
        timestamp = time_ns
        records.append((key, duration))
    if timestamp is not None:
        yield timestamp, records


def write_operator_times(source, count, thread, complete, windows, writer):
    totals = {}
    boundary = thread[2]
    for timestamp, records in operator_observations(source, count):
        matched = (boundary, timestamp) in windows
        mpki, coverage = windows.get((boundary, timestamp), ("", ""))
        for key, cumulative in records:
            duration = cumulative - totals.get(key, 0)
            totals[key] = cumulative
            if duration > 0:
                share = duration / (timestamp - boundary) if timestamp > boundary else ""
                writer.writerow((*thread, boundary, timestamp, key >> 32, key & 0xffffffff,
                                 duration, int(complete), share, mpki, coverage, int(matched)))
        boundary = timestamp


def decode_trace(path, recorded_query, window_writer, operator_writer, summary):
    with path.open("rb") as source:
        header = read_record(source, HEADER)
        magic, version, header_size, sample_size, flags, _, stack_errors, output_errors, _, _, _ = header[:11]
        query_id, _, thread_start, _, _, sample_count, dropped, _, os_tid, observations, lost_times = header[11:]
        if (magic, version, header_size, sample_size) != (TRACE_MAGIC, TRACE_VERSION, HEADER.size, SAMPLE.size):
            raise ValueError(f"{path}: unsupported trace format")
        if path.stat().st_size != HEADER.size + sample_count * SAMPLE.size + observations * OPERATOR_TIME.size:
            raise ValueError(f"{path}: trace size does not match header")
        if query_id != recorded_query:
            raise ValueError(f"{path}: Query Id {query_id} differs from capture metadata {recorded_query}")
        complete = not (flags & TRACE_INCOMPLETE or stack_errors or output_errors or dropped or lost_times)
        summary["threads"] += 1
        summary["incomplete_threads"] += int(not complete)
        thread = query_id, os_tid, thread_start
        windows = read_windows(source, sample_count, thread, complete, window_writer, summary)
        write_operator_times(source, observations, thread, complete, windows, operator_writer)


def read_rows(path):
    with path.open(newline="") as source:
        return list(csv.DictReader(source))


def thread_key(row):
    return row["os_tid"], row["thread_start_ns"]


def bins_overlapping(start, end, origin, count):
    first = max(0, (start - origin) // REPLAY_BIN_NS)
    last = min(count - 1, (end - origin - 1) // REPLAY_BIN_NS)
    for index in range(first, last + 1):
        left = origin + index * REPLAY_BIN_NS
        overlap = min(end, left + REPLAY_BIN_NS) - max(start, left)
        if overlap > 0:
            yield index, overlap


def plot_replay(output, metadata, summary):
    import matplotlib
    import numpy as np

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    windows = read_rows(output / "windows.csv")
    operators = read_rows(output / "operators.csv")
    origin = min(int(row["start_ns"]) for row in windows)
    duration = max(int(row["end_ns"]) for row in windows) - origin
    bin_count = max(1, (duration + REPLAY_BIN_NS - 1) // REPLAY_BIN_NS)
    cpu_domains = {entry["cpu"]: entry["llc_shared_cpus"] for entry in metadata["topology"]}
    domains = sorted({cpu_domains.get(int(row["observed_cpu"]), "unknown") for row in windows})
    thread_domains = defaultdict(set)
    for row in windows:
        thread_domains[thread_key(row)].add(cpu_domains.get(int(row["observed_cpu"]), "unknown"))
    stable_domains = {
        key: next(iter(caches)) for key, caches in thread_domains.items()
        if len(caches) == 1 and "unknown" not in caches
    }
    summary["replay_threads_with_ambiguous_domain"] = len(thread_domains) - len(stable_domains)

    activity = {domain: np.zeros((len(FAMILIES), bin_count)) for domain in domains}
    counts = {domain: np.zeros((2, bin_count)) for domain in domains}
    for row in windows:
        if row["valid"] != "1":
            continue
        domain = cpu_domains.get(int(row["observed_cpu"]), "unknown")
        start, end = int(row["start_ns"]), int(row["end_ns"])
        for index, overlap in bins_overlapping(start, end, origin, bin_count):
            share = overlap / (end - start)
            counts[domain][0, index] += int(row["llc_load_misses"]) * share
            counts[domain][1, index] += int(row["instructions"]) * share
    families = metadata["plan_node_families"]
    for row in operators:
        domain = stable_domains.get(thread_key(row))
        if row["valid"] != "1" or domain is None:
            continue
        start, end = int(row["start_ns"]), int(row["end_ns"])
        family = FAMILIES.index(families.get(row["plan_id"], "Other"))
        for index, overlap in bins_overlapping(start, end, origin, bin_count):
            activity[domain][family, index] += int(row["exclusive_wall_ns"]) * overlap / (end - start) / REPLAY_BIN_NS

    colors = ("#45c696", "#b65bd1", "#a6d8bf", "#d4eadf", "#cccccc", "#8d009f")
    bin_seconds = REPLAY_BIN_NS / 1e9
    x = (np.arange(bin_count) + .5) * bin_seconds
    figure, axes = plt.subplots(len(domains), 1, figsize=(16, max(4, 2.8 * len(domains))), squeeze=False)
    for index, domain in enumerate(domains):
        axis = axes[index, 0]
        bottom = np.zeros(bin_count)
        for family, color, values in zip(FAMILIES, colors, activity[domain]):
            axis.bar(x, values, width=bin_seconds, bottom=bottom, color=color, label=family)
            bottom += values
        axis.set_ylabel("Worker equivalents")
        axis.set_title(f"LLC shared CPUs {domain} (operator wall time includes blocking)")
        mpki_axis = axis.twinx()
        misses, instructions = counts[domain]
        mpki = np.divide(1000 * misses, instructions, out=np.full(bin_count, np.nan), where=instructions > 0)
        mpki_axis.plot(x, mpki, color="#501060", linewidth=1)
        mpki_axis.set_ylabel("LLC-read MPKI")
        if index == 0:
            axis.legend(ncol=6, loc="upper left")
    axes[-1, 0].set_xlabel("Time since first observed interval (s)")
    figure.tight_layout()
    figure.savefig(output / "replay.png", dpi=140)
    figure.savefig(output / "replay.pdf")
    plt.close(figure)


def analyze(raw, output, metadata):
    files = sorted(raw.glob("cache_trace_*.bin"))
    if not files:
        raise ValueError(f"{raw}: no cache trace binaries")
    output.mkdir(parents=True, exist_ok=True)
    summary = {
        "event": "LLC-load-misses (PERF_TYPE_HW_CACHE LL/read/miss)",
        "counter_scope": "per-thread user-space",
        "runtime_scope": "exclusive wall time including blocking",
        "threads": 0, "incomplete_threads": 0, "valid_windows": 0, "invalid_windows": 0,
        "threads_with_valid_windows": 0, "instructions": 0, "llc_load_misses": 0,
        "replay_time_bin_ns": REPLAY_BIN_NS,
        "cpu_scope": "Operator-boundary CPU snapshots",
        "operator_mpki_scope": "Thread-window MPKI alongside operator wall shares",
        "replay_rebinning": "Interval totals distributed in proportion to bin overlap",
    }
    with (output / "windows.csv").open("w", newline="") as windows, (
        output / "operators.csv"
    ).open("w", newline="") as operators:
        window_writer, operator_writer = csv.writer(windows), csv.writer(operators)
        window_writer.writerow(WINDOW_FIELDS)
        operator_writer.writerow(OPERATOR_FIELDS)
        for file in files:
            decode_trace(file, metadata["query_id"], window_writer, operator_writer, summary)

    summary["weighted_llc_read_mpki"] = (
        1000 * summary["llc_load_misses"] / summary["instructions"] if summary["instructions"] else None
    )
    write_json(output / "topology.json", metadata["topology"])
    write_json(output / "trace_metadata.json", metadata)
    if summary["valid_windows"]:
        plot_replay(output, metadata, summary)
    write_json(output / "summary.json", summary)
    if not summary["valid_windows"]:
        raise ValueError("No valid PMU windows")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, help="Capture metadata (default: trace_metadata.json alongside raw)")
    args = parser.parse_args()
    metadata_path = args.metadata or args.raw.parent / "trace_metadata.json"
    if not metadata_path.is_file():
        parser.error(f"Capture metadata missing: {metadata_path}")
    try:
        summary = analyze(args.raw, args.output, json.loads(metadata_path.read_text()))
    except (OSError, ValueError, KeyError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
