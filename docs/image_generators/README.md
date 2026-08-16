# Image Generators

Source of truth for the two hand-built figures in this repo's documentation. Each
diagram is generated from a Python script that emits SVG directly — no drawing tool,
no binary source file. Editing a diagram means editing a data structure at the top of
its script and re-running it.

| Script | Emits | Consumed by |
| --- | --- | --- |
| `generate_isotope_diagram.py` | `isotope-diagram.svg` / `.png` | [root README §1](../../README.md) |
| `generate_tier_ladder_diagram.py` | `tier-ladder-diagram.svg` / `.png` | [root README §1](../../README.md) |

The root README embeds the **PNGs from `docs/`**, one directory up — not from here.
See [Output location](#output-location).

## Prerequisites

- **uv** — this directory is a self-contained uv project (`pyproject.toml`, `uv.lock`,
  Python pinned to 3.13 in `.python-version`). `uv run` syncs `.venv/` on first use;
  there is nothing to `pip install`.
- **cairo** — `cairosvg` is a binding, not an implementation. The native library must
  be present for PNG output: `brew install cairo`. SVG generation needs nothing beyond
  Python.

## Running

From the repo root:

```sh
make generate_isotope_diagram
```

Or directly, from this directory:

```sh
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib uv run python generate_isotope_diagram.py
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib uv run python generate_tier_ladder_diagram.py
```

### Why `DYLD_FALLBACK_LIBRARY_PATH`

`cairocffi` resolves cairo at import time by calling `dlopen` with a bare library name
(`libcairo.2.dylib`). macOS searches the dyld shared cache, `/usr/lib`, and
`/usr/local/lib` — but never `/opt/homebrew/lib`, where Homebrew installs on Apple
Silicon. The variable adds that directory to dyld's last-resort search path so the
name resolves. It is macOS/Homebrew-specific; on Intel Macs the prefix is
`/usr/local`, and on Linux the variable is not needed at all.

Note that it must be set as a command prefix. System Integrity Protection strips all
`DYLD_*` variables when a protected binary such as `/bin/zsh` is exec'd, so exporting
it from a shell profile is unreliable.

## Flags

Both scripts accept:

- `--out-dir DIR` — where to write output (default: current directory; created if absent)
- `--no-png` — emit SVG only, skipping cairo entirely

`generate_tier_ladder_diagram.py` additionally accepts `--scale N` (default `2.0`),
which multiplies the 680pt viewbox width to set PNG resolution. The isotope diagram
has no scale flag; its PNG width is fixed at the 2040pt document width.

## Output location

Both scripts default `--out-dir` to the **current working directory**, so a bare run
from here leaves the files here — while the root README embeds `docs/isotope-diagram.png`
and `docs/tier-ladder-diagram.png`. To regenerate the images the documentation actually
displays, target the parent directory:

```sh
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib uv run python generate_isotope_diagram.py --out-dir ..
```

Only the PNGs are committed; the SVGs are intermediates.

## Editing a diagram

Both scripts are data-driven — layout is computed, not hand-placed, so adding a row or
a tier reflows everything below it.

- **`generate_isotope_diagram.py`** — colors live in `PALETTE`, content in the `*_ROWS`
  lists and `OPTIONAL_BOX`, and column x-origins and box geometry in the `LAYOUT` block.
- **`generate_tier_ladder_diagram.py`** — everything that changes between revisions is
  in `TIERS` and `DIVIDER`: counts, blurbs, example questions, and tier order (first in
  the list renders as the top rung). The legend total is summed from `TIERS`, so
  re-tiering a question cannot leave a stale count behind.

## Troubleshooting

**A wall of `OSError: no library called "cairo-2" was found`** — cairo is missing or
unreachable. Install it (`brew install cairo`) and set `DYLD_FALLBACK_LIBRARY_PATH` as
above. `generate_tier_ladder_diagram.py` fails this way because it catches only
`ImportError`; `generate_isotope_diagram.py` catches `OSError` too and degrades to
SVG-only.

**`cairosvg not installed; skipping PNG`** — usually inaccurate. `cairosvg` is a
declared dependency and uv installs it; the message also fires when the package
imports fine but the native cairo library cannot be loaded. Check the
`DYLD_FALLBACK_LIBRARY_PATH` prefix before reaching for `pip`.

**`VIRTUAL_ENV ... does not match the project environment path`** — harmless. uv is
noting that an unrelated active virtualenv is being ignored in favor of this project's
`.venv/`, which is what you want.
