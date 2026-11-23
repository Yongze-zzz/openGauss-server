"""Fig. 4(c) foreground read bandwidth with and without competing readers."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from assets.figure_style import bar, plt, run_figure, save_pdf, series_legend, style_axis

# Median and min/max foreground GB/s over three repetitions per case.
# Source: die_bandwidth/results_20261018_150236/summary.csv.
PATHS = ("Intra-die", "Inter-die", "Inter-socket")
BANDWIDTH_GB_S = (
    (0, 0, 16.2136956788, 16.1541095565, 16.2261821574),
    (0, 1, 3.24809465265, 3.24489523308, 3.26052147875),
    (1, 0, 15.2946920414, 15.265511092, 15.3165782093),
    (1, 1, 3.4160842231, 3.41594874226, 3.41686530627),
    (2, 0, 8.74650728406, 8.69385464107, 8.77495883875),
)


def plot(output):
    figure, axis = plt.subplots(figsize=(3.9, 2.5), layout="constrained")
    for path, loaded, center, low, high in BANDWIDTH_GB_S:
        xpos = path + (0.18 if loaded else -0.18)
        bar(axis, xpos, center, 0.34, loaded)
        axis.errorbar(xpos, center, yerr=[[center - low], [high - center]],
                      fmt="none", color="#171717", capsize=2, zorder=5)
    axis.set_xticks(range(len(PATHS)), PATHS)
    axis.set(ylabel="Foreground read bandwidth (GB/s)")
    style_axis(axis)
    axis.legend(series_legend(), ("0 background readers", "22 background readers"), frameon=False)
    save_pdf(figure, output, "fig04c_bandwidth")


if __name__ == "__main__":
    run_figure(plot, __doc__)
