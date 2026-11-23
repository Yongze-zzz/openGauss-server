#!/usr/bin/env python3
"""Save the plan and LLC topology for offline analysis of a cache trace."""
import argparse
import json
import re
from pathlib import Path


FAMILIES = ("Scan", "Join", "Aggregate", "Sort", "Other", "Stream")
PLAN_ROW = re.compile(r"^[ \t]*(\d+)[ \t]*[|\x1f][ \t]*->[ \t]*([^|\x1f\r\n]+)", re.MULTILINE)
QUERY_ID = re.compile(r"^\s*Query Id:\s*(\d+)\s*$", re.MULTILINE)


def operator_family(name):
    name = name.lower()
    if "subquery scan" in name or "subqueryscan" in name:
        return "Other"
    if "scan" in name:
        return "Scan"
    if "join" in name or "nest loop" in name or "nested loop" in name:
        return "Join"
    if "aggregate" in name or "group" in name:
        return "Aggregate"
    if "sort" in name:
        return "Sort"
    if "stream" in name or "remote query" in name:
        return "Stream"
    return "Other"


def read_llc_topology(sysfs=Path("/sys/devices/system/cpu")):
    topology = []
    for cpu in sorted(sysfs.glob("cpu[0-9]*"), key=lambda path: int(path.name[3:])):
        caches = []
        for cache in (cpu / "cache").glob("index*"):
            try:
                if (cache / "type").read_text().strip() in ("Unified", "Data"):
                    caches.append((int((cache / "level").read_text()), cache))
            except FileNotFoundError:
                continue
        if not caches:
            continue
        _, llc = max(caches)
        topology.append({
            "cpu": int(cpu.name[3:]),
            "numa": sorted(int(node.name[4:]) for node in cpu.glob("node[0-9]*")),
            "llc_shared_cpus": (llc / "shared_cpu_list").read_text().strip(),
            "llc_size": (llc / "size").read_text().strip(),
        })
    if not topology:
        raise ValueError("No LLC topology found in sysfs")
    return topology


def capture_metadata(plan_path, topology):
    plan_text = plan_path.read_text(errors="replace")
    query_ids = {int(value) for value in QUERY_ID.findall(plan_text)}
    if len(query_ids) != 1:
        raise ValueError(f"{plan_path}: expected one Query Id")
    plan_names = {int(node): name.strip() for node, name in PLAN_ROW.findall(plan_text)}
    if not plan_names:
        raise ValueError(f"{plan_path}: EXPLAIN PERFORMANCE plan table not found")
    return {
        "query_id": next(iter(query_ids)),
        "topology": topology,
        "plan_node_names": plan_names,
        "plan_node_families": {node: operator_family(name) for node, name in plan_names.items()},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--topology", type=Path, help="Existing capture topology, for archived traces")
    args = parser.parse_args()
    topology = json.loads(args.topology.read_text()) if args.topology else read_llc_topology()
    metadata = capture_metadata(args.plan, topology)
    args.output.write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
