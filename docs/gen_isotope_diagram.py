#!/usr/bin/env python3
"""
Generate the confluent-kafka-isotope architecture diagram.
    pip install cairosvg          # PNG only; SVG needs no deps
    python3 gen_isotope_diagram.py [--out-dir docs/images] [--no-png]

Emits isotope_diagram.svg, and isotope_diagram.png if cairosvg is installed
(pip install cairosvg).

Layout is data-driven: edit PALETTE / the *_ROWS lists / OPTIONAL_BOX and
re-run. Column x-origins and box sizes live in the LAYOUT block.
"""

import argparse
import os
from xml.sax.saxutils import escape

W, H = 2040, 1960
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
    ("consume_events", "edge markers", "marker"),
]
SINKS = [
    "latency_1m", "topology_1m", "bipartite_topology_1m", "hop_distribution_1m",
    "coverage_1m", "stuck_trace_1m (PTF)", "latency_percentiles_1m",
]
# Set to None to drop the opt-in AI path from the chart entirely.
OPTIONAL_BOX = {
    "title": "AI root-cause analysis",
    "mono": ["CREATE MODEL \u00b7 ML_PREDICT", "rca_findings_1m"],
    "foot": "CCAF only \u00b7 off by default",
    "feeds_from_sink": 6,          # 1-based index into SINKS
}

NOTE_LEFT = [
    "Four-stage order workflow; traces ride in headers.",
    "shipping-notify can't append a header (no re-produce),",
    "so it emits a consume-edge marker to its own topic.",
    "The topology graph stays complete either way.",
    "",
    "AI root-cause analysis is opt-in and CCAF-only: it",
    "summarizes stuck and slow traces from the report",
    "topics. Off by default; CP is unaffected either way.",
]
LEGEND = [
    [("swatch", "service", "Service"), ("swatch", "topic", "Event topic (x-isotope)"),
     ("swatch", "marker", "Marker topic")],
    [("swatch", "flink", "Flink interpreter"), ("swatch", "sink", "Report sink (1-min)"),
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
    add('<title>Confluent Kafka Isotope \u2014 collector and interpreter</title>')
    add('<desc>Four-stage order workflow stamps isotope headers; Flink SQL emits '
        'seven one-minute reports, plus an optional CCAF-only AI root-cause path '
        'that is off by default.</desc>')
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
        text(TOP_X + TOP_W // 2, y + 77, title, 34, t)
        text(TOP_X + TOP_W // 2, y + 130, sub, 34, s)
        line(SVC_X + SVC_W, row_mid(i), TOP_X, row_mid(i), dashed=(key == "marker"))

    # topics -> flink
    for i, ty in enumerate((558, 600, 662)):
        line(TOP_X + TOP_W, row_mid(i), MID_X, ty)
    line(TOP_X + TOP_W, 905, MID_X, 700)

    # micrometer rail
    add(f'<path d="M{SVC_X} {row_mid(0)} H25 V1163 H40" fill="none" '
        f'stroke="{PALETTE["line"]}" stroke-width="2"/>')
    add(f'<polygon points="34,1153 56,1163 34,1173" fill="{PALETTE["line"]}"/>')
    text(68, 1082, "Micrometer", 30, PALETTE["muted"], "start")
    box(SVC_X, 1105, SVC_W, 125, "metrics", rx=20)
    _, _, t, s = PALETTE["metrics"]
    text(SVC_X + SVC_W // 2, 1160, "Prometheus / Grafana", 36, t)
    text(SVC_X + SVC_W // 2, 1205, "always-on metrics", 30, s)

    # interpreter core
    box(MID_X, 515, MID_W, 220, "flink")
    _, _, t, s = PALETTE["flink"]
    text(MID_X + MID_W // 2, 588, "Flink SQL", 42, t)
    text(MID_X + MID_W // 2, 648, "CAST(headers[\u2026])", 34, s, mono=True)
    text(MID_X + MID_W // 2, 700, "TUMBLE(1 minute)", 34, s, mono=True)
    line(MID_X + MID_W // 2, 735, MID_X + MID_W // 2, 795)
    box(MID_X, 795, MID_W, 155, "jar", rx=20)
    _, _, t, s = PALETTE["jar"]
    text(MID_X + MID_W // 2, 858, "isotope-flink-udf.jar", 34, t, mono=True)
    text(MID_X + MID_W // 2, 910, "two PTFs", 34, s)
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
            text(505, 1092 + i * 50, ln, 35, PALETTE["body"], "start")

    # legend
    add('<rect x="20" y="1520" width="2000" height="400" rx="24" '
        'fill="#F0F2EF" stroke="#C9CDC9" stroke-width="2"/>')
    cols = [45, 700, 1420]
    for r, row in enumerate(LEGEND):
        y = 1585 + r * 80
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
    svg_path = os.path.join(args.out_dir, "isotope_diagram.svg")
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(build())
    print(f"wrote {svg_path}")

    if not args.no_png:
        try:
            import cairosvg
        except ImportError:
            print("cairosvg not installed; skipping PNG (pip install cairosvg)")
            return
        png_path = os.path.join(args.out_dir, "isotope_diagram.png")
        cairosvg.svg2png(url=svg_path, write_to=png_path, output_width=W)
        print(f"wrote {png_path}")


if __name__ == "__main__":
    main()
