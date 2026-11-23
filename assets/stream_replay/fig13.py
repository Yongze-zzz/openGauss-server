"""Fig. 13 effective Stream copy throughput by consumer chiplet."""

import sys
from pathlib import Path

if __name__ == "__main__" and not __package__:
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from matplotlib.lines import Line2D
from matplotlib.patches import PathPatch
from matplotlib.path import Path as CurvePath
from matplotlib.transforms import Affine2D

from assets.figure_style import GREEN, PURPLE, plt, run_figure, save_pdf

# Publication labels from p3_flow_Q3_Q7_Q8_Q9_compact.zip, in GB/s.
# Each isolated value comes from a separate sequential stage.
QUERIES = ('Q3', 'Q7', 'Q8', 'Q9')
ISOLATED = {
    'Q3': (38.97, 43.22, 42.23, 43.33, 15.82, 27.02, 30.16, 34.90),
    'Q7': (54.36, 55.17, 43.27, 41.89, 43.11, 42.54, 41.04, 40.86),
    'Q8': (55.97, 70.56, 32.52, 32.55, 32.55, 32.50, 32.09, 32.14),
    'Q9': (54.83, 58.74, 43.61, 42.87, 42.86, 42.43, 43.24, 42.17),
}
CONCURRENT = {
    'Q3': (12.76, 18.66, 14.31, 14.91, 11.43, 12.98, 11.37, 13.11),
    'Q7': (13.96, 23.76, 12.15, 12.00, 12.11, 11.80, 11.86, 11.93),
    'Q8': (8.87, 24.73, 5.95, 5.94, 5.93, 5.90, 5.92, 5.92),
    'Q9': (14.08, 23.33, 12.75, 12.60, 12.42, 12.39, 12.83, 12.43),
}

# Stroke widths in PDF points from the concurrent-side vector source.
# Each query has one row per producer and one column per consumer. The same
# paths summarize all stages on the isolated side, as in the reference plot.
SOURCE_WIDTHS = {'Q3': ((4.8685071035, 4.8853789296, 4.9064590999, 4.4219572772, 4.4521052118, 5.3149606203, 5.8, 5.72472009),
        (1.5985756523, 1.5426370682, 1.5355294383, 1.6347636287, 1.5621563849, 1.5241404851, 1.3652503127, 1.2580003926),
        (1.5212098136, 1.5435802946, 1.5032370847, 1.4934825384, 1.4989453476, 1.4606534138, 1.4365215161, 1.3805219163),
        (1.569689692, 1.5473819827, 1.5492335431, 1.5100787931, 1.4201406086, 1.3365694978, 1.3289086236, 1.4644256281),
        (2.2662527282, 2.3166453444, 2.3089638157, 2.2086988919, 2.0862111345, 2.0095231749, 2.0905112616, 2.268254361),
        (0.7898928401, 0.7635970619, 0.775651864, 0.7619044656, 0.7747518977, 0.7434376851, 0.7401151417, 0.7381241455),
        (1.4932342443, 1.4809447004, 1.5050065902, 1.7950159091, 1.9235197312, 1.6166043183, 1.4109341667, 1.3821387237),
        (1.4930411459, 1.5105997262, 1.5270643721, 1.7697298864, 1.8823330886, 1.5876877358, 1.422193362, 1.3848627278)),
 'Q7': ((5.0898082773, 5.5604971182, 5.6578308688, 5.7046030008, 5.728650363, 5.7412639068, 5.7823443399, 5.8),
        (1.7188897684, 1.6171685126, 1.5731887707, 1.5795781442, 1.562315496, 1.5562165898, 1.5521319316, 1.545851375),
        (1.6989777561, 1.6495894373, 1.6270767953, 1.6369307727, 1.6243540612, 1.6182441372, 1.6127766333, 1.6387053923),
        (1.6786133096, 1.6086552901, 1.6087146711, 1.5638856042, 1.5759668829, 1.5888291924, 1.5661602782, 1.5515130328),
        (1.6764355267, 1.6175234365, 1.615267662, 1.6173608507, 1.6198012468, 1.5989957142, 1.5818277701, 1.5870274201),
        (1.6883578511, 1.6517325985, 1.6623862173, 1.656886774, 1.6596296043, 1.662900467, 1.664236724, 1.6429568454),
        (1.6692186577, 1.5751014644, 1.5639541048, 1.5474591586, 1.5575116993, 1.5532993305, 1.5484263376, 1.5612517146),
        (1.6817884778, 1.6089572293, 1.6033224353, 1.5936901771, 1.5846484683, 1.578054831, 1.5883998669, 1.579272161)),
 'Q8': ((5.8, 5.670569211, 5.6971209118, 5.7009939211, 5.7115295058, 5.7095422606, 5.714975929, 5.7151768416),
        (0.8684093409, 0.8667990492, 0.8666196293, 0.8659270986, 0.8673727052, 0.8673370317, 0.8663426059, 0.8769709195),
        (0.8308884899, 0.8654426262, 0.8669549821, 0.8670362445, 0.8662693756, 0.8683050348, 0.8654232938, 0.8811510863),
        (0.8534167323, 0.8671881892, 0.8672476265, 0.8660413202, 0.8667782212, 0.8672009851, 0.8619095674, 0.8477269239),
        (0.8376183684, 0.8593073425, 0.8471077343, 0.8483797374, 0.8468201307, 0.8468378567, 0.8470835273, 0.8470059762),
        (0.842438996, 0.860814436, 0.848482991, 0.8446795007, 0.8468225126, 0.8475841192, 0.8471148246, 0.8472985098),
        (0.837997039, 0.8675933379, 0.867760405, 0.8528803007, 0.8474808655, 0.8470699004, 0.8540863301, 0.8675253146),
        (0.848980924, 0.8651908069, 0.868085233, 0.8652075912, 0.8660006613, 0.8672292912, 0.8600408645, 0.839441427)),
 'Q9': ((5.7049022246, 5.7305801499, 5.7281795733, 5.7633662532, 5.8, 5.7623901939, 5.7196825452, 5.7787338592),
        (1.5982167066, 1.5910709116, 1.5784526705, 1.5803483307, 1.5747927125, 1.5780522579, 1.5835207236, 1.5904146806),
        (1.5355117812, 1.5647360966, 1.5658012845, 1.5552992938, 1.5297430417, 1.572413186, 1.5730312238, 1.5630988671),
        (1.5679468122, 1.5812694009, 1.5752475023, 1.5803016308, 1.5778250469, 1.5598910284, 1.5601459816, 1.5741979448),
        (1.5550875538, 1.5705028375, 1.5928430933, 1.5996770807, 1.56293372, 1.5484712459, 1.5454318873, 1.5173284055),
        (1.5777044228, 1.5753862997, 1.5647791502, 1.5483974419, 1.524020805, 1.5685882497, 1.5490644559, 1.578199609),
        (1.5625421425, 1.5703391898, 1.5660963488, 1.5547410717, 1.5735798057, 1.5754711139, 1.5728155653, 1.5348738736),
        (1.5535312013, 1.5528193565, 1.5788539858, 1.55927163, 1.5476799615, 1.5842219449, 1.5633125739, 1.5729476575))}

FIGURE_WIDTH, FIGURE_HEIGHT = 7.06, 5.31
PANEL_X = ((0.62, 2.52), (4.03, 5.93))
TOP_Y = 0.34
QUERY_GAP = 1.21
NODE_GAP = 0.112
SOURCE_SPAN_PT = 226.343279
TEXT_COLOR = "#242424"


def draw_routes(figure, coordinates, widths, source_x, target_x, top, color):
    span = target_x - source_x
    # The two control points lie at 5/14 and 9/14 of the horizontal span.
    first_control = source_x + 5 / 14 * span
    last_control = source_x + 9 / 14 * span
    codes = (CurvePath.MOVETO, CurvePath.CURVE4, CurvePath.CURVE4, CurvePath.CURVE4)
    stroke_scale = 72 * span / SOURCE_SPAN_PT * 0.74
    for producer, edges in enumerate(widths):
        source_y = top + producer * NODE_GAP
        for consumer, width in enumerate(edges):
            target_y = top + consumer * NODE_GAP
            points = [(source_x, source_y), (first_control, source_y),
                      (last_control, target_y), (target_x, target_y)]
            figure.add_artist(PathPatch(CurvePath(points, codes), transform=coordinates,
                                        facecolor="none", edgecolor=color,
                                        linewidth=max(0.12, width * stroke_scale), alpha=0.45,
                                        zorder=1, capstyle="round"))


def plot(output):
    figure = plt.figure(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    coordinates = Affine2D().scale(1 / FIGURE_WIDTH, -1 / FIGURE_HEIGHT).translate(0, 1) + figure.transFigure
    for row, query in enumerate(QUERIES):
        top = TOP_Y + row * QUERY_GAP
        figure.text(0.155, top + 3.5 * NODE_GAP, query, transform=coordinates,
                    fontsize=10.1, weight="bold", ha="center", va="center", color=TEXT_COLOR)
        for (source_x, target_x), color, rates in zip(PANEL_X, (PURPLE, GREEN),
                                                       (ISOLATED[query], CONCURRENT[query])):
            figure.text(source_x, top - 0.16, "Producer", transform=coordinates,
                        ha="center", va="center", fontsize=8.9, weight="bold", color=TEXT_COLOR)
            figure.text(target_x + 0.03, top - 0.16, "Consumer", transform=coordinates,
                        ha="center", va="center", fontsize=8.9, weight="bold", color=TEXT_COLOR)
            draw_routes(figure, coordinates, SOURCE_WIDTHS[query], source_x, target_x, top, color)
            for chiplet, rate in enumerate(rates):
                y = top + chiplet * NODE_GAP
                figure.add_artist(Line2D([source_x, target_x], [y, y], transform=coordinates,
                                         linestyle="none", marker="o", markersize=2.5,
                                         color=color, markeredgewidth=0, zorder=3))
                figure.text(source_x - 0.075, y, f"C{chiplet}", transform=coordinates,
                            ha="right", va="center", color=TEXT_COLOR, fontsize=7.8)
                figure.text(target_x + 0.063, y, f"C{chiplet}  {rate:.2f} GB/s", transform=coordinates,
                            ha="left", va="center", color=TEXT_COLOR, fontsize=7.8)

    for positions, caption in zip(PANEL_X, ("(a) Isolated Producer-Consumer Data Access",
                                            "(b) Concurrent Producer-Consumer Data Access")):
        figure.text(sum(positions) / 2, 5.09, caption, transform=coordinates,
                    fontsize=9.4, ha="center", va="center", color=TEXT_COLOR)
    save_pdf(figure, output, "fig13_stream_replay")


if __name__ == "__main__":
    run_figure(plot, __doc__)
