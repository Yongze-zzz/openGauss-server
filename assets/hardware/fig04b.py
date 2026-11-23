"""Fig. 4(b) dependent-load latency across the selected chiplets."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np

from assets.figure_style import plt, run_figure, save_pdf

# Median ns per load from results_4chips/matrix_ns.csv. Rows are executing
# chiplets, columns are data homes. Each median uses seven repetitions.
CHIPLETS = ("C0", "C1", "C4", "C5")
LATENCY_NS = (
    (156.707, 168.956, 251.274, 268.100),
    (172.617, 156.642, 268.089, 286.301),
    (251.759, 270.236, 154.015, 171.980),
    (268.035, 286.226, 174.405, 156.518),
)


def plot(output):
    figure, axis = plt.subplots(figsize=(3.1, 2.5), layout="constrained")
    image = axis.imshow(LATENCY_NS, cmap="viridis", vmin=150, vmax=290)
    for (row, column), value in np.ndenumerate(LATENCY_NS):
        axis.text(column, row, f"{value:.1f}", ha="center", va="center",
                  color="white" if value < 255 else "black", fontsize=8)
    axis.set_xticks(range(len(CHIPLETS)), CHIPLETS)
    axis.set_yticks(range(len(CHIPLETS)), CHIPLETS)
    axis.set(xlabel="Data-page chiplet", ylabel="Executing chiplet")
    figure.colorbar(image, ax=axis, label="ns per load")
    save_pdf(figure, output, "fig04b_memory_latency")


if __name__ == "__main__":
    run_figure(plot, __doc__)
