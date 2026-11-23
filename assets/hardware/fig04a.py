"""Fig. 4(a) working-set sweep from a fresh dependent-load benchmark."""

import argparse
import csv
import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from assets.figure_style import DEFAULT_OUTPUT, GREEN, configure_style, plt, save_pdf, style_axis


def plot(output, source):
    with source.open(newline="") as readings:
        samples = sorted(
            (int(row["working_set_kib"]) / 1024, float(row["median_ns_per_load"]))
            for row in csv.DictReader(readings)
        )
    if not samples:
        raise ValueError(f"{source}: no latency measurements")
    sizes, latencies = zip(*samples)
    figure, axis = plt.subplots(figsize=(3.1, 2.2), layout="constrained")
    axis.semilogx(sizes, latencies, color=GREEN, marker="o", markersize=3)
    axis.set(xlabel="Working set size (MiB)", ylabel="Latency (ns/load)")
    style_axis(axis)
    save_pdf(figure, output, "fig04a_cache_latency")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="cache_latency.csv from run_fig4.sh cache")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    configure_style()
    plot(args.output, args.source)
