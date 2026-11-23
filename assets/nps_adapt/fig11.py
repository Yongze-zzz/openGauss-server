"""Fig. 11 NPS+Adapt speedup and logical remote access on the manuscript data."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np

from assets.figure_style import INK as BLACK, bar, plt, run_figure, save_pdf, series_legend, style_axis

# Embedded source values from p2_6queries_abc_four_distinct_bars_shaded.zip.
# Timings are seconds. Remote GiB and reduction labels are from the archived
# bars and include the reference measurement's JIT estimates.
QUERIES = ['Q3', 'Q5', 'Q7', 'Q8', 'Q9', 'Q18']
BASE4 = np.array([10.599,11.609,25.228,6.595,36.112,14.094],float)
ADAPT4 = np.array([7.176,6.754,12.130,3.532,20.897,10.909],float)
BASE8 = np.array([22.148,35.176,57.489,7.643,112.963,26.281],float)
ADAPT8 = np.array([12.987,29.416,36.680,6.465,83.840,16.668],float)
SPD4, SPD8 = BASE4/ADAPT4, BASE8/ADAPT8

INTRA_BASE = np.array([61.9, 89.3, 77.6, 73.2, 127.9, 137.6],float)
INTRA_OPT = np.array([9.4, 2.8, 11.8, 10.8, 26.9, 44.3],float)
INTER_BASE = np.array([91.6, 332.4, 142.2, 11.0, 250.2, 69.4],float)
INTER_OPT = np.array([55.6, 201.4, 74.2, 4.8, 157.0, 49.2],float)
INTRA_DROP = np.array([84.9, 96.8, 84.7, 85.3, 79.0, 67.8],float)
INTER_DROP = np.array([39.3, 39.4, 47.8, 56.0, 37.2, 29.1],float)

WIDTH_IN, HEIGHT_IN = 7.06, 2.85
FONT = {
    'axis_label': 9.0,
    'x_query': 8.3,
    'y_tick': 8.0,
    'legend': 8.0,
    'panel_caption': 8.7,
    'bar_annotation': 6.4,
    'small_note': 6.5,
}


def plot(output):
    fig = plt.figure(figsize=(WIDTH_IN, HEIGHT_IN))
    axes = [fig.add_axes(rect) for rect in [
        [0.073, .285, .265, .570],
        [0.411, .285, .253, .570],
        [0.736, .285, .253, .570],
    ]]
    a, b, c = axes
    # Nearly fill horizontal space within each query group.
    xs = np.arange(6) * .66
    width_a = .116  # four clearly separated bars per query
    # A four-bar group: 4C Baseline/optimized + 8C Baseline/optimized.
    # Small visual gap between 4C and 8C, with identical purple/green semantics.
    offsets = np.array([-.235, -.096, .096, .235])  # larger 4C/8C gap
    # alternating per-query shading to visually separate neighboring queries in panel (a)
    shade_halfwidth = 0.33
    for i, x in enumerate(xs):
        if i % 2 == 0:
            a.axvspan(x - shade_halfwidth, x + shade_halfwidth,
                      facecolor='#F3F3F3', edgecolor='none', zorder=0)
        bar(a, x + offsets[0], 1.0, width_a, 0)
        bar(a, x + offsets[1], SPD4[i], width_a, 1)
        bar(a, x + offsets[2], 1.0, width_a, 0)
        bar(a, x + offsets[3], SPD8[i], width_a, 1)

    width_bc = .290
    for ax, bas, opt, reduction, maxy, yticks in [
        (b, INTRA_BASE, INTRA_OPT, INTRA_DROP, 157, [0,50,100,150]),
        (c, INTER_BASE, INTER_OPT, INTER_DROP, 367, [0,100,200,300])]:
        for i,x in enumerate(xs):
            bar(ax, x-width_bc/2, bas[i], width_bc, 0)
            bar(ax, x+width_bc/2, opt[i], width_bc, 1)
            dy = 3.1 if ax is b else 5.7
            ax.text(x, max(bas[i],opt[i])+dy, f'−{reduction[i]:.1f}%',
                    fontsize=FONT['bar_annotation'], ha='center', va='bottom')
        ax.set_ylim(0,maxy)
        ax.set_yticks(yticks)
        ax.set_ylabel('Remote accesses (GiB)', labelpad=1)

    a.set_ylim(0, 2.33)
    a.set_yticks([0,.5,1,1.5,2])
    a.set_ylabel('Speedup',labelpad=1)
    a.axhline(1.0, color='#707070', lw=.58, ls='--', zorder=1)
    for ax in axes:
        ax.set_xlim(xs[0]-.35, xs[-1]+.35)
        ax.set_xticks(xs,QUERIES)
        style_axis(ax)
        ax.tick_params(axis='x', labelsize=FONT['x_query'])
        ax.tick_params(axis='y', labelsize=FONT['y_tick'])

    # Panel labels close to query labels/axes, same visual scale as Fig 7 reference.
    caption_y = .125
    for xc,t in [
        (.204, '(a) NPS+Adapt speedup'),
        (.538, '(b) Intra-op remote accesses'),
        (.862, '(c) Inter-op remote accesses')]:
        fig.text(xc,caption_y,t,ha='center',va='center',fontsize=FONT['panel_caption'])

    # In (a) the four independent bars are 4C Baseline, 4C NPS+Adapt,
    # 8C Baseline, 8C NPS+Adapt.  Label the two subgroups directly,
    # rather than using the ambiguous term 'pair'.
    # Place subgroup tags slightly below the x-axis and query names below.
    a.tick_params(axis='x', pad=13.5)
    for x in xs:
        a.text(x-.1655, -.045, '4C', ha='center', va='top',
               transform=a.get_xaxis_transform(), fontsize=6.6,
               color='#4b4b4b', clip_on=False)
        a.text(x+.1655, -.045, '8C', ha='center', va='top',
               transform=a.get_xaxis_transform(), fontsize=6.6,
               color='#4b4b4b', clip_on=False)

    # Single consistent bar legend for all three panels.
    handles = series_legend()
    fig.legend(handles,['Baseline','NPS+Adapt'],ncol=2,
               loc='upper center',bbox_to_anchor=(.531,.985),
               frameon=True,facecolor='white',edgecolor=BLACK,
               framealpha=1,borderpad=.10,handlelength=1.2,
               handleheight=.75,columnspacing=1.05,labelspacing=.12)

    # Printed figure should avoid long footnotes. Technical caveats in README.
    fig.text(.5,.052,'4C / DOP96 and 8C / DOP168 for (a); 8C / DOP168 for (b) and (c).',
             ha='center',va='center',fontsize=FONT['small_note'],color='#555555')

    save_pdf(fig, output, "fig11_nps_adapt")


if __name__ == "__main__":
    run_figure(plot, __doc__)
