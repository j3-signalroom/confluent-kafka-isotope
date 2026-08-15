#!/usr/bin/env python3
"""
Generate the five-tier observability question ladder figure.

    python3 generate_tier_ladder_diagram.py [--out-dir docs/images] [--scale 2] [--no-png]

Emits tier-ladder-diagram.svg, and tier-ladder-diagram.png if cairosvg is installed
(pip install cairosvg).

Everything that changes between revisions lives in TIERS and DIVIDER:
counts, blurbs, example questions, and tier order (top of list = top rung).
The total in the legend is summed from TIERS, so re-tiering a question
can't leave a stale count behind.
"""

import argparse
import os
from xml.sax.saxutils import escape

VB_W, VB_H = 680, 540
SANS = "Helvetica Neue, Helvetica, Arial, sans-serif"

# fill, stroke, title ink, body ink
RAMPS = {
    "green":  ("#E3F3EA", "#3E8E6E", "#1B6B4F", "#2E7D5E"),
    "blue":   ("#E4EFFB", "#6C9BD2", "#1B5E9E", "#2E7EBF"),
    "purple": ("#EAE7FA", "#7E72CC", "#4B3FA0", "#5B4BA8"),
    "amber":  ("#FBEBD6", "#D9A44E", "#8A5A12", "#9C6A1E"),
    "pink":   ("#FBE7EF", "#C25781", "#A03A63", "#B04A72"),
}
RULE, MUTED = "#888780", "#5F5E5A"

# Top rung first. (title, count, blurb, example, ramp)
TIERS = [
    ("Harder", 5, "forensic replay \u00b7 compliance \u00b7 cross-system",
     "\u201cReconstruct a full per-trace journey for an audit?\u201d", "pink"),
    ("Hard", 6, "tail latency \u00b7 drift \u00b7 correlation",
     "\u201cWhat are p50 / p95 / p99 across the pipeline?\u201d", "amber"),
    ("Medium", 7, "cross-window deltas \u00b7 anomalies",
     "\u201cWhich traces went in but never came out in 60s?\u201d", "purple"),
    ("Easy \u2192 Medium", 6, "single per-minute aggregates",
     "\u201cEnd-to-end latency over the last minute?\u201d", "blue"),
    ("Easy", 6, "single-record \u00b7 single-trace",
     "\u201cDid my record get tagged, and how many hops?\u201d", "green"),
]

# Dashed capability line: drawn *below* the rung at this index (0-based).
DIVIDER = {
    "after_rung": 2,
    "above": "\u2191 needs the collector + interpreter",
    "below": "\u2193 logs \u00b7 metrics \u00b7 APM reach about here",
}

# ---------------------------------------------------------------- LAYOUT ----
X, RUNG_W, RUNG_H, GAP = 80, 520, 74, 10
Y0 = 40                 # top of the first rung
BAND = 62               # vertical space the divider band consumes
RAIL_X = 50             # difficulty arrow
LEGEND_DY = 18          # legend baseline below the last rung

out = []
def add(s): out.append(s)

def text(x, y, s, size, fill, anchor="start", italic=False, weight=None):
    style = ' font-style="italic"' if italic else ""
    w = f' font-weight="{weight}"' if weight else ""
    add(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="{size}" '
        f'fill="{fill}"{style}{w}>{escape(s)}</text>')

def build():
    add(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {VB_W} {VB_H}" '
        f'width="{VB_W}" height="{VB_H}" role="img" font-family="{SANS}">')
    total = sum(t[1] for t in TIERS)
    add('<title>Five-tier observability question ladder</title>')
    add(f'<desc>{total} observability questions the project can answer, sorted into '
        f'{len(TIERS)} tiers of increasing difficulty, each with one example question. '
        'A dashed line marks where logs, metrics, and APM run out and the '
        'collector-plus-interpreter pattern becomes necessary.</desc>')
    add('<defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" stroke-width="1.5" '
        'stroke-linecap="round" stroke-linejoin="round"/></marker></defs>')
    add(f'<rect x="0" y="0" width="{VB_W}" height="{VB_H}" fill="#FFFFFF"/>')

    y = Y0
    for i, (title, count, blurb, example, ramp) in enumerate(TIERS):
        fill, stroke, ink, body = RAMPS[ramp]
        add(f'<rect x="{X}" y="{y}" width="{RUNG_W}" height="{RUNG_H}" rx="10" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="0.8"/>')
        text(X + 20, y + 26, title, 15, ink)
        text(X + RUNG_W - 20, y + 26, f"{count} questions", 15, ink, anchor="end")
        text(X + 20, y + 46, blurb, 11.5, body)
        text(X + 20, y + 65, example, 11.5, body, italic=True)
        y += RUNG_H + GAP

        if i == DIVIDER["after_rung"]:
            text(X + 2, y + 12, DIVIDER["above"], 11.5, MUTED)
            add(f'<line x1="{X}" y1="{y + 24}" x2="{X + RUNG_W}" y2="{y + 24}" '
                f'stroke="{RULE}" stroke-width="1" stroke-dasharray="6 5"/>')
            text(X + 2, y + 42, DIVIDER["below"], 11.5, MUTED)
            y += BAND

    bottom = y - GAP
    add(f'<line x1="{RAIL_X}" y1="{bottom}" x2="{RAIL_X}" y2="{Y0 + 6}" '
        f'stroke="{RULE}" stroke-width="1.2" marker-end="url(#arrow)"/>')

    ly = bottom + LEGEND_DY
    for j, tier in enumerate(reversed(TIERS)):        # easiest swatch on the left
        fill, stroke, _, _ = RAMPS[tier[4]]
        add(f'<rect x="{X + j * 20}" y="{ly - 11}" width="14" height="14" rx="4" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="0.8"/>')
    text(X + len(TIERS) * 20 + 14, ly,
         f"tier difficulty (easier \u2192 harder) \u00b7 {total} questions, "
         f"{len(TIERS)} tiers", 11.5, MUTED)

    add("</svg>")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=".")
    ap.add_argument("--scale", type=float, default=2.0,
                    help="PNG width multiplier over the 680px viewBox")
    ap.add_argument("--no-png", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    svg_path = os.path.join(args.out_dir, "tier-ladder-diagram.svg")
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(build())
    print(f"wrote {svg_path}")

    if args.no_png:
        return
    try:
        import cairosvg
    except ImportError:
        print("cairosvg not installed; skipping PNG (pip install cairosvg)")
        return
    png_path = os.path.join(args.out_dir, "tier-ladder-diagram.png")
    cairosvg.svg2png(url=svg_path, write_to=png_path,
                     output_width=int(VB_W * args.scale))
    print(f"wrote {png_path}")


if __name__ == "__main__":
    main()
