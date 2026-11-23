"""Shared layout for the paper's chiplet-access matrices."""

import numpy as np
from matplotlib.colors import LinearSegmentedColormap, LogNorm
from matplotlib.patches import Rectangle

from assets.figure_style import plt, save_pdf

FIGURE_WIDTH = 7.06
MATRIX_SIZE = 1.06
ROW_SPACING = 1.19
COLUMN_X = (0.90, 2.08, 3.43, 4.61)
LOCAL_COLOR = "#cbdbe5"
EMPTY_COLOR = "#f3f5f7"
TEXT_COLOR = "#26323b"
HOTSPOT_EDGE = "#732381"
COLORMAP = LinearSegmentedColormap.from_list("paper_green_purple", [
    "#f5f9f6", "#e2f5e8", "#bcead8", "#77d3bc", "#3ab9b2",
    "#699cb9", "#9a82c5", "#ae5abc", "#7c258f",
], N=512)
GIB_NORM = LogNorm(vmin=0.01, vmax=50, clip=True)


def multisource_cells(matrix):
    remote = matrix.copy()
    np.fill_diagonal(remote, 0)
    total = remote.sum()
    if total <= 0:
        return []
    destinations = remote.sum(axis=0)
    highlighted = []
    count = 0
    for column in np.argsort(-destinations):
        destination = destinations[column]
        if count >= 3 or destination < 0.08 * total:
            break
        sources = np.flatnonzero((remote[:, column] >= 0.012 * total) &
                                 (remote[:, column] >= 0.10 * destination))
        if len(sources) < 2:
            continue
        largest = sorted(sources, key=lambda row: remote[row, column], reverse=True)[:4]
        highlighted.extend((row, column) for row in largest)
        count += 1
    return highlighted


def draw_cells(axis, matrix, marked):
    size = len(matrix)
    for row in range(8):
        for column in range(8):
            active = row < size and column < size
            diagonal = active and row == column
            value = matrix[row, column] if active else 0
            if diagonal:
                color = LOCAL_COLOR
            elif value > 0:
                color = COLORMAP(GIB_NORM(value))
            else:
                color = EMPTY_COLOR
            axis.add_patch(Rectangle((column - 0.5, row - 0.5), 1, 1, facecolor=color,
                                     edgecolor="white", linewidth=0.30))
            if diagonal:
                axis.add_patch(Rectangle((column - 0.46, row - 0.46), 0.92, 0.92, fill=False,
                                         edgecolor="#7c92a0", linewidth=0.55))
    for row, column in marked:
        for edgecolor, linewidth in (("white", 1.85), (HOTSPOT_EDGE, 1.05)):
            axis.add_patch(Rectangle((column - 0.49, row - 0.49), 0.98, 0.98, fill=False,
                                     edgecolor=edgecolor, linewidth=linewidth, zorder=6))
    axis.set(xlim=(-0.5, 7.5), ylim=(7.5, -0.5), aspect="equal")
    axis.xaxis.tick_top()
    axis.set_xticks(range(8))
    axis.set_yticks(range(8))
    axis.tick_params(which="both", length=0, pad=1, labelsize=7, colors="#5e6b75")
    for spine in axis.spines.values():
        spine.set_visible(False)


def plot_grid(output, name, matrices, queries, modes, hotspots=None):
    bottom = 0.9
    top = bottom + (len(queries) - 1) * ROW_SPACING
    height = top + MATRIX_SIZE + 0.65
    figure = plt.figure(figsize=(FIGURE_WIDTH, height))

    def label(x, y, value, size, **kwargs):
        figure.text(x / FIGURE_WIDTH, y / height, value, ha="center", va="center",
                    fontsize=size, color=TEXT_COLOR, **kwargs)

    for column, (scope, mode) in enumerate((scope, mode) for scope in ("intra", "inter") for mode in modes):
        x = COLUMN_X[column]
        label(x + MATRIX_SIZE / 2, top + MATRIX_SIZE + 0.25, mode, 10.2, weight="bold")
        for row, query in enumerate(queries):
            y = top - row * ROW_SPACING
            if column == 0:
                label(0.19, y + MATRIX_SIZE / 2, f"Q{query}", 10.2, weight="bold")
            axis = figure.add_axes([x / FIGURE_WIDTH, y / height, MATRIX_SIZE / FIGURE_WIDTH, MATRIX_SIZE / height])
            key = query, scope, mode
            matrix = np.array(matrices[key], dtype=float)
            marked = []
            if scope == "inter":
                marked = hotspots.get(key, ()) if hotspots is not None else multisource_cells(matrix)
            draw_cells(axis, matrix, marked)
    for group, title in enumerate(("(a) Intra-operator accesses", "(b) Inter-operator accesses")):
        left, right = COLUMN_X[2 * group:2 * group + 2]
        center = (left + right + MATRIX_SIZE) / 2
        label(center, 0.70, "Data-page chiplet", 9.5)
        label(center, 0.44, title, 11.1, weight="bold")
    label(0.60, (top + MATRIX_SIZE + bottom) / 2, "Execution chiplet", 9.5, rotation=90)
    color_axis = figure.add_axes([6.06 / FIGURE_WIDTH, bottom / height, 0.09 / FIGURE_WIDTH,
                                 (top + MATRIX_SIZE - bottom) / height])
    scale = plt.cm.ScalarMappable(norm=GIB_NORM, cmap=COLORMAP)
    colorbar = figure.colorbar(scale, cax=color_axis, ticks=[0.01, 0.1, 1, 10, 50])
    colorbar.set_ticklabels(["0.01", "0.1", "1", "10", "50"])
    colorbar.minorticks_off()
    colorbar.outline.set_visible(False)
    color_axis.tick_params(length=0, pad=3, labelsize=8)
    color_axis.set_title("GiB", fontsize=9, pad=8)
    save_pdf(figure, output, name)
