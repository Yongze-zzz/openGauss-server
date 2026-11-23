"""Common typography and command-line handling for the paper figures."""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch, Rectangle

PURPLE, PURPLE_PALE = "#A66AD4", "#F8F3FB"
GREEN, GREEN_PALE = "#7CB398", "#F4FAF6"
INK = "#171717"
SERIES = ((PURPLE_PALE, PURPLE, "xx"), (GREEN_PALE, GREEN, "++++"))
DEFAULT_OUTPUT = Path("results/paper_figures")


def configure_style():
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Arimo", "Liberation Sans", "DejaVu Sans"],
        "font.size": 8.5,
        "axes.labelsize": 9,
        "axes.linewidth": 0.75,
        "axes.edgecolor": INK,
        "hatch.linewidth": 0.7,
        "xtick.labelsize": 8.3,
        "ytick.labelsize": 8,
        "legend.fontsize": 8,
        "pdf.fonttype": 42,
        "figure.facecolor": "white",
        "savefig.facecolor": "white",
    })


def bar(axis, x, value, width, series):
    face, edge, hatch = SERIES[series]
    axis.bar(x, value, width=width, color=face, edgecolor=edge, hatch=hatch,
             linewidth=0, zorder=3)
    axis.add_patch(Rectangle((x - width / 2, 0), width, value, fill=False,
                             linewidth=0.62, edgecolor=INK, zorder=4))


def series_legend():
    return [Patch(facecolor=face, edgecolor=edge, hatch=hatch) for face, edge, hatch in SERIES]


def style_axis(axis):
    axis.set_axisbelow(True)
    axis.grid(axis="y", color="#D9D9D9", linestyle=":", linewidth=0.55)
    axis.tick_params(axis="both", direction="in", length=2.4, width=0.72, pad=1.8)


def save_pdf(figure, output, name):
    figure.savefig(output / (name + ".pdf"), bbox_inches="tight", pad_inches=0.03)
    plt.close(figure)


def run_figure(plot, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    configure_style()
    plot(args.output)
