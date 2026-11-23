"""Fig. 8 cumulative remote logical accesses and inter-operator contribution."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np

from assets.figure_style import SERIES, bar, plt, run_figure, save_pdf, series_legend, style_axis

# Remote logical bytes across Q1–Q22, from the historical baseline exports.
# Each tuple contains intra-operator bytes followed by inter-operator bytes.
REMOTE_BYTES = {
    4: (1_161_733_374_558, 863_374_141_350),
    8: (1_477_812_239_994, 1_461_486_162_296),
}


def plot(output):
    volumes = np.array([REMOTE_BYTES[size] for size in (4, 8)], dtype=np.float64)
    normalized = volumes / volumes[0, 0]
    figure, (cumulative, shares) = plt.subplots(1, 2, figsize=(6.0, 2.9))
    figure.subplots_adjust(left=0.12, right=0.985, bottom=0.24, top=0.79, wspace=0.39)
    for series, offset in enumerate((-0.18, 0.18)):
        for scope in range(2):
            x = scope + offset
            height = normalized[series, scope]
            bar(cumulative, x, height, 0.34, series)
            cumulative.text(x, height + 0.035, f"{volumes[series, scope] / 1024**3:,.0f}",
                            ha="center", va="bottom", fontsize=7.4)
    for scope in range(2):
        expansion = volumes[1, scope] / volumes[0, scope]
        cumulative.text(scope, max(normalized[:, scope]) + 0.17, f"{expansion:.2f}×",
                        ha="center", va="bottom", weight="bold")
    cumulative.set_xticks([0, 1], ["Intra-op", "Inter-op"])
    cumulative.set(xlim=(-0.48, 1.48), ylim=(0, 1.62), ylabel="Normalized remote accesses")
    style_axis(cumulative)
    fractions = volumes / volumes.sum(axis=1, keepdims=True) * 100
    for series, (intra, inter) in enumerate(fractions):
        face, edge, hatch = SERIES[series]
        shares.bar(series, intra, width=0.55, color="#EEEEEE", edgecolor="#171717", linewidth=0.6)
        shares.bar(series, inter, bottom=intra, width=0.55, color=face, edgecolor=edge,
                   hatch=hatch, linewidth=0.65)
        shares.text(series, intra / 2, f"{intra:.1f}%", ha="center", va="center")
        shares.text(series, intra + inter / 2, f"{inter:.1f}%", ha="center", va="center", weight="bold")
    shares.set_xticks([0, 1], ["4 chiplets", "8 chiplets"])
    shares.set(ylim=(0, 100), ylabel="Share of remote accesses (%)")
    style_axis(shares)
    figure.legend(series_legend(), ("4 chiplets", "8 chiplets"), loc="upper center",
                  ncol=2, bbox_to_anchor=(0.54, 0.975))
    figure.text(0.29, 0.075, "(a) Cumulative remote accesses", ha="center")
    figure.text(0.806, 0.075, "(b) Inter-op contribution", ha="center")
    save_pdf(figure, output, "fig08_cumulative_remote_access")


if __name__ == "__main__":
    run_figure(plot, __doc__)
