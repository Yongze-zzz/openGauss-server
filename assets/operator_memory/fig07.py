"""Fig. 7 per-query remote logical access ratios."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np

from assets.figure_style import bar, plt, run_figure, save_pdf, series_legend, style_axis

# Percent of accounted logical accesses. Each sequence is ordered Q1–Q22.
# None marks the unavailable Q15 inter-operator measurement.
# Historical exports: 4numa_current_all22.csv and 8numa_old_all22_reference.csv.
REMOTE_PERCENT = {
    (4, 'intra'): (
        34.456116881689454, 63.67260701220272, 74.92001517622347, 74.64906972455474, 64.7744454783333,
        61.17210464771128, 74.99128278368929, 72.07199624055922, 74.15502664337097, 75.23180598576835,
        40.81396946839298, 73.59062980749624, 73.3716816406393, 68.29823427759298, 65.78647314025669,
        67.68062696788431, 70.237952655564, 74.04212380962441, 72.95823055506627, 65.40962115085773,
        70.72433610956264, 69.69429178583611,
    ),
    (4, 'inter'): (
        21.126557961190283, 68.03332937104652, 74.59037755818409, 73.69013841702036, 75.29273895583655,
        49.68255821141923, 73.92957204109933, 70.75254671707133, 67.53321745148915, 71.78765710378961,
        24.504848573209934, 73.02735030556785, 69.04134947844243, 64.0746340410437, None,
        68.82432859401622, 59.61982612282051, 67.96297745079046, 67.71440476694673, 68.28042816417943,
        56.62284621134147, 63.275364895082795,
    ),
    (8, 'intra'): (
        59.612662192994335, 73.61638519999224, 87.48537950122275, 86.7771083511146, 76.57735203669634,
        83.00768223253179, 86.75650221289999, 81.24570569140603, 87.13287947578284, 85.80491039112813,
        65.9259902671121, 86.727330636431, 78.79336726035179, 81.65220769364508, 82.66404902428113,
        81.03673862102883, 77.49193731925061, 84.90018215924403, 87.15397241011657, 76.72653454036892,
        84.43724016558018, 80.78188849803489,
    ),
    (8, 'inter'): (
        53.58359726229981, 81.87570541189055, 86.11164633991922, 87.02779435495862, 82.311247837963,
        69.75435006286583, 83.4221305565887, 78.49963300256101, 79.88451318253671, 78.51231728210564,
        41.26502452114584, 84.66026761232212, 77.07140582343955, 69.34776547234154, None,
        83.25634709175297, 66.41206831284876, 73.61808543133829, 77.8279055799896, 80.20857000331293,
        66.58097857017862, 72.70178234721241,
    ),
}


def plot(output):
    query_indices = [q for q in range(22) if all(series[q] is not None for series in REMOTE_PERCENT.values())]
    xs = np.arange(len(query_indices))
    figure, axes = plt.subplots(2, 1, figsize=(7.06, 3.45), sharex=True)
    figure.subplots_adjust(left=0.09, right=0.985, top=0.86, bottom=0.17, hspace=0.64)
    for panel, (axis, scope) in enumerate(zip(axes, ("intra", "inter"))):
        for series, (size, offset) in enumerate(((4, -0.19), (8, 0.19))):
            values = REMOTE_PERCENT[size, scope]
            for x, query in zip(xs, query_indices):
                bar(axis, x + offset, values[query], 0.36, series)
        axis.set(xlim=(-0.5, len(xs) - 0.5), ylim=(0, 100), ylabel="Remote access (%)")
        axis.set_yticks([0, 20, 40, 60, 80, 100])
        style_axis(axis)
        figure.text(0.54, (0.485, 0.052)[panel],
                    ("(a) Intra-operator accesses", "(b) Inter-operator accesses")[panel],
                    ha="center", va="center", fontsize=8.7)
    axes[1].set_xticks(xs, [f"Q{query + 1}" for query in query_indices])
    axes[1].tick_params(axis="x", labelsize=7)
    figure.legend(series_legend(), ("4 chiplets", "8 chiplets"), loc="upper center",
                  ncol=2, bbox_to_anchor=(0.54, 0.98))
    save_pdf(figure, output, "fig07_remote_access_ratios")


if __name__ == "__main__":
    run_figure(plot, __doc__)
