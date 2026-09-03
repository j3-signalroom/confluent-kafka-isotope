#!/usr/bin/env python3
"""
Generate the confluent-kafka-isotope architecture diagram.
    python3 generate_isotope_diagram.py [--out-dir docs/images] [--no-png]

Emits isotope-diagram.svg, and isotope-diagram.png if cairosvg is installed
(pip install cairosvg).

Layout is data-driven: edit PALETTE / the *_ROWS lists / OPTIONAL_BOX and
re-run. Column x-origins and box sizes live in the LAYOUT block.
"""

import argparse
import os
from xml.sax.saxutils import escape

W, H = 2040, 2190
SANS = "Helvetica Neue, Helvetica, Arial, sans-serif"
MONO = "SF Mono, Menlo, Consolas, monospace"

PALETTE = {
    "bg":       "#F7F9FB",
    "line":     "#8A8A85",
    "body":     "#44443F",
    "muted":    "#5F5E5A",
    "ink":      "#2C2C2A",
    "service":  ("#DEEBF7", "#8AB6DE", "#1B5E9E", "#2E7EBF"),
    "topic":    ("#E7E3F8", "#8B7FD1", "#5B4BA8", "#5B4BA8"),
    "marker":   ("#FBE4EC", "#C2537E", "#A83A63", "#A83A63"),
    "flink":    ("#DDF2E9", "#4FAE8B", "#146A50", "#1E7A5C"),
    "sink":     ("#FCEBD5", "#E0A44E", "#8F5A14", "#8F5A14"),
    "jar":      ("#E9E6DE", "#A9A497", "#44443F", "#5F5E5A"),
    "metrics":  ("#E4E8F0", "#9AA3B5", "#3B4354", "#5A6274"),
    "optional": ("#E8F5EF", "#4FAE8B", "#146A50", "#4A7F6C"),
}

# ---------------------------------------------------------------- LAYOUT ----
SVC_X, SVC_W = 50, 435          # collector service column
TOP_X, TOP_W = 543, 362         # event/marker topic column
MID_X, MID_W = 1000, 447        # interpreter column
SNK_X, SNK_W = 1540, 490        # report sink column
ROW_H, ROW_PITCH, ROW_Y0 = 165, 215, 215   # collector rows
SNK_H, SNK_PITCH, SNK_Y0 = 105, 120, 140   # sink rows

SERVICES = [
    ("order-intake", "producer"),
    ("order-enrichment", "consume + produce"),
    ("order-fulfillment", "consume + produce"),
    ("shipping-notify", "terminal consumer"),
]
TOPICS = [
    ("orders.placed", "+ x-isotope", "topic"),
    ("orders.enriched", "+ x-isotope", "topic"),
    ("orders.fulfilled", "+ x-isotope", "topic"),
    ("isotope_consume_edge_markers", "consume-edge markers", "marker"),
    ("orders.flink_enriched", "+ x-isotope", "topic"),
]
# Index into TOPICS of the topic Flink itself produces (propagation model B).
# It gets an inbound edge FROM the Flink box instead of from a service.
FLINK_TOPIC_INDEX = 4
SINKS = [
    "latency_1m", "topology_1m", "bipartite_topology_1m", "hop_distribution_1m",
    "coverage_1m", "stuck_trace_1m (PTF)", "latency_percentiles_1m",
]
# Set to None to drop the opt-in fan-in (merge) provenance path from the chart.
# Flink produces both of these; they are gated (--merge-provenance on CP,
# var.enable_merge_provenance on CCAF), hence the dashed treatment.
MERGE_BOX = {
    "title": "Fan-in (Merge) Provenance",
    "mono": ["orders.flink_batched", "FRESH trace \u00b7 1 hop",
             "isotope_merge_edge_markers"],
    "foot": "off by default",
    "y": 1300,
}
# Set to None to drop the opt-in AI path from the chart entirely.
OPTIONAL_BOX = {
    "title": "AI Root-Cause Analysis",
    "mono": ["CREATE MODEL + ML_PREDICT", "rca_findings_1m"],
    "foot": "off by default",
    "feeds_from_sink": 6,          # 1-based index into SINKS
}

# Wrapped narrow (<= ~34 chars) so the block clears the AI box on its right.
NOTE_LEFT = [
    "Four-stage order workflow; traces",
    "ride in headers. shipping-notify",
    "can't append a header (no re-produce),",
    "so it emits a consume-edge marker",
    "to its own topic.",
    "",
    "Flink is a collector too:",
    "ISOTOPE_APPEND_HOP appends its own",
    "hop onto orders.flink_enriched, so",
    "Flink appears in the topology graph",
    "as a producer, not only a reader.",
    "",
    "A windowed merge cannot forward a",
    "trace \u2014 many parents \u2014 so",
    "ISOTOPE_MERGE_TRACE mints a fresh one",
    "and the parent edges go to their own",
    "topic. Opt-in, like AI root-cause",
    "analysis (which is CCAF-only).",
]
NOTE_X, NOTE_Y0, NOTE_SIZE, NOTE_PITCH = 1000, 1055, 30, 38

LEGEND = [
    [("swatch", "service", "Service"), ("swatch", "topic", "Event topic (x-isotope)"),
     ("swatch", "marker", "Marker topic")],
    [("swatch", "flink", "Flink interpreter + collector"), ("swatch", "sink", "Report sink (1-min)"),
     ("swatch", "jar", "Shadow JAR")],
    [("swatch", "metrics", "Metrics backend"), ("solid", None, "Produce"),
     ("dashed", None, "Edge marker emit")],
    [("dashed-swatch", "optional", "Optional \u00b7 off by default"), None, None],
]

# ---------------------------------------------------------------- HELPERS ---
out = []
def add(s): out.append(s)

def box(x, y, w, h, key, rx=24, dashed=False):
    fill, stroke, _, _ = PALETTE[key]
    dash = ' stroke-dasharray="16 12"' if dashed else ""
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2"{dash}/>')

def text(x, y, s, size, fill, anchor="middle", mono=False):
    fam = f' font-family="{MONO}"' if mono else ""
    add(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{size}" '
        f'fill="{fill}"{fam}>{escape(s)}</text>')

def line(x1, y1, x2, y2, stroke=None, dashed=False, width=2):
    dash = ' stroke-dasharray="14 12"' if dashed else ""
    add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{stroke or PALETTE["line"]}" stroke-width="{width}"{dash}/>')

def row_y(i, y0=ROW_Y0, pitch=ROW_PITCH):   return y0 + i * pitch
def row_mid(i, h=ROW_H, **kw):              return row_y(i, **kw) + h // 2

# ------------------------------------------------------------------ BUILD ---
def build():
    add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" role="img" font-family="{SANS}">')
    add('<title>Confluent Kafka Isotope \u2014 collectors and interpreter</title>')
    add('<desc>Four-stage order workflow stamps isotope headers; Flink SQL reads them '
        'to emit seven one-minute reports and also acts as a second collector, appending '
        'its own hop onto orders.flink_enriched. Two opt-in paths are off by default: '
        'fan-in (merge) provenance, where a windowed merge mints a fresh trace and writes '
        'its parent edges to their own topic, and a CCAF-only AI root-cause report.</desc>')
    add(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{PALETTE["bg"]}"/>')

    text(20, 75, "Collector", 44, PALETTE["ink"], "start")
    text(20, 128, "Stamp headers; emit markers + metrics", 32, PALETTE["muted"], "start")
    text(SNK_X, 75, "Interpreter", 44, PALETTE["ink"], "start")
    text(SNK_X, 128, "Reads headers + markers", 32, PALETTE["muted"], "start")

    # collector column
    for i, (title, sub) in enumerate(SERVICES):
        y = row_y(i)
        box(SVC_X, y, SVC_W, ROW_H, "service")
        _, _, t, s = PALETTE["service"]
        text(SVC_X + SVC_W // 2, y + 77, title, 40, t)
        text(SVC_X + SVC_W // 2, y + 130, sub, 34, s)

    # topic column
    for i, (title, sub, key) in enumerate(TOPICS):
        y = row_y(i)
        box(TOP_X, y, TOP_W, ROW_H, key)
        _, _, t, s = PALETTE[key]
        text(TOP_X + TOP_W // 2, y + 77, title, 34 if len(title) <= 21 else 22, t)
        text(TOP_X + TOP_W // 2, y + 130, sub, 34 if len(sub) <= 16 else 26, s)
        if i != FLINK_TOPIC_INDEX:
            line(SVC_X + SVC_W, row_mid(i), TOP_X, row_mid(i), dashed=(key == "marker"))

    # topics -> flink
    for i, ty in enumerate((558, 600, 662)):
        line(TOP_X + TOP_W, row_mid(i), MID_X, ty)
    line(TOP_X + TOP_W, 905, MID_X, 700)

    # micrometer rail
    add(f'<path d="M{SVC_X} {row_mid(0)} H25 V1393 H40" fill="none" '
        f'stroke="{PALETTE["line"]}" stroke-width="2"/>')
    add(f'<polygon points="34,1383 56,1393 34,1403" fill="{PALETTE["line"]}"/>')
    text(68, 1312, "Micrometer", 30, PALETTE["muted"], "start")
    box(SVC_X, 1335, SVC_W, 125, "metrics", rx=20)
    _, _, t, s = PALETTE["metrics"]
    text(SVC_X + SVC_W // 2, 1390, "Prometheus / Grafana", 36, t)
    text(SVC_X + SVC_W // 2, 1435, "always-on metrics", 30, s)

    # interpreter core
    box(MID_X, 515, MID_W, 220, "flink")
    _, _, t, s = PALETTE["flink"]
    text(MID_X + MID_W // 2, 578, "Flink SQL", 42, t)
    text(MID_X + MID_W // 2, 620, "interpreter + collector", 30, PALETTE["muted"])
    text(MID_X + MID_W // 2, 672, "CAST(headers[\u2026])", 32, s, mono=True)
    text(MID_X + MID_W // 2, 716, "TUMBLE(1 minute)", 32, s, mono=True)

    # Flink -> orders.flink_enriched (propagation model B: Flink stamps a hop).
    # Leaves the box's bottom-left corner so it clears the shadow-JAR box below.
    fy = row_mid(FLINK_TOPIC_INDEX)
    line(MID_X, 735, TOP_X + TOP_W + 26, fy)
    add(f'<polygon points="{TOP_X + TOP_W + 30},{fy - 13} {TOP_X + TOP_W},{fy} '
        f'{TOP_X + TOP_W + 30},{fy + 13}" fill="{PALETTE["line"]}"/>')
    # Flink -> the optional fan-in pair. Shares its origin with the model-B
    # arrow above: both topics are produced by Flink, this one only when the
    # merge stage is switched on.
    if MERGE_BOX:
        fill, stroke, t2, s2 = PALETTE["optional"]
        my, mh = MERGE_BOX["y"], 265
        aty = my + 120
        line(MID_X, 735, TOP_X + TOP_W + 26, aty, stroke=stroke, dashed=True)
        add(f'<polygon points="{TOP_X + TOP_W + 30},{aty - 13} {TOP_X + TOP_W},{aty} '
            f'{TOP_X + TOP_W + 30},{aty + 13}" fill="{stroke}"/>')
        box(TOP_X, my, TOP_W, mh, "optional", rx=20, dashed=True)
        mcx = TOP_X + TOP_W // 2
        text(mcx, my + 57, MERGE_BOX["title"], 28, t2)
        for j, m in enumerate(MERGE_BOX["mono"]):
            text(mcx, my + 105 + j * 42, m, 22, s2, mono=True)
        text(mcx, my + 238, MERGE_BOX["foot"], 28, s2)

    line(MID_X + MID_W // 2, 735, MID_X + MID_W // 2, 795)
    box(MID_X, 795, MID_W, 155, "jar", rx=20)
    _, _, t, s = PALETTE["jar"]
    text(MID_X + MID_W // 2, 858, "isotope-flink-udf.jar", 34, t, mono=True)
    text(MID_X + MID_W // 2, 910, "2 PTFs + 3 UDFs", 34, s)
    text(MID_X + MID_W // 2, 1000, "One JAR \u00b7 same DDL \u00b7 CP or CCAF", 34, PALETTE["muted"])

    # report sinks
    fan = [192, 312, 432, 552, 672, 792, 912]
    src = [600, 600, 605, 620, 640, 650, 660]
    for i, name in enumerate(SINKS):
        y = row_y(i, SNK_Y0, SNK_PITCH)
        box(SNK_X, y, SNK_W, SNK_H, "sink", rx=20)
        text(SNK_X + SNK_W // 2, y + 65, name, 34, PALETTE["sink"][2], mono=True)
        line(MID_X + MID_W, src[i], SNK_X, fan[i])
    last_y = row_y(len(SINKS) - 1, SNK_Y0, SNK_PITCH) + SNK_H
    text(SNK_X + SNK_W, last_y + 57, "7 reports \u00b7 CP + CCAF", 34, PALETTE["muted"], "end")
    text(SNK_X + SNK_W, last_y + 103, "percentiles is a PTF", 34, PALETTE["muted"], "end")

    # optional AI root-cause path
    if OPTIONAL_BOX:
        fill, stroke, t, s = PALETTE["optional"]
        top, ax = 1126, SNK_X + 60
        line(ax, last_y, ax, top - 16, stroke=stroke, dashed=True)
        add(f'<polygon points="{ax-10},{top-22} {ax},{top} {ax+10},{top-22}" fill="{stroke}"/>')
        box(SNK_X, top, SNK_W, 240, "optional", rx=20, dashed=True)
        cx = SNK_X + SNK_W // 2
        text(cx, top + 59, OPTIONAL_BOX["title"], 34, t)
        for j, m in enumerate(OPTIONAL_BOX["mono"]):
            text(cx, top + 110 + j * 45, m, 26, s, mono=True)
        text(cx, top + 204, OPTIONAL_BOX["foot"], 28, s)

    # notes
    for i, ln in enumerate(NOTE_LEFT):
        if ln:
            text(NOTE_X, NOTE_Y0 + i * NOTE_PITCH, ln, NOTE_SIZE,
                 PALETTE["body"], "start")

    # legend
    add('<rect x="20" y="1750" width="2000" height="400" rx="24" '
        'fill="#F0F2EF" stroke="#C9CDC9" stroke-width="2"/>')
    cols = [45, 700, 1420]
    for r, row in enumerate(LEGEND):
        y = 1815 + r * 80
        for c, item in enumerate(row):
            if not item:
                continue
            kind, key, label = item
            x = cols[c]
            if kind in ("swatch", "dashed-swatch"):
                fill, stroke, _, _ = PALETTE[key]
                dash = ' stroke-dasharray="8 6"' if kind == "dashed-swatch" else ""
                add(f'<rect x="{x}" y="{y}" width="38" height="38" rx="8" '
                    f'fill="{fill}" stroke="{stroke}" stroke-width="2"{dash}/>')
            else:
                line(x, y + 19, x + 38, y + 19, dashed=(kind == "dashed"), width=3)
            text(x + 58, y + 30, label, 34, PALETTE["body"], "start")

    add("</svg>")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=".")
    ap.add_argument("--no-png", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    svg_path = os.path.join(args.out_dir, "isotope-diagram.svg")
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(build())
    print(f"wrote {svg_path}")

    if not args.no_png:
        try:
            import cairosvg
        except (ImportError, OSError):
            print("PNG skipped: cairosvg unavailable (missing native libcairo? try DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib)")
            return
        png_path = os.path.join(args.out_dir, "isotope-diagram.png")
        cairosvg.svg2png(url=svg_path, write_to=png_path, output_width=W)
        print(f"wrote {png_path}")


if __name__ == "__main__":
    main()
