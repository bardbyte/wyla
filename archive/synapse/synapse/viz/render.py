"""Render chart specs to themed, dependency-free HTML + inline SVG.

Rules enforced here (not left to the model):
  * categorical palette is fixed-order and CVD-validated for BOTH themes
    (light #3B6FE0 #0E9463 #B9770C #6D3BE0 · dark #5E8DF0 #1FA478 #B87F0E
    #9B7CF0 — validated with the palette checker, all four checks pass)
  * text wears ink tokens; series color appears only on marks and chips
  * thin marks, rounded data-ends, 2px series lines, endpoint emphasis
  * one series → no legend; 2-4 series → legend chips + direct end labels
  * numbers are tabular; axis ticks compact; value labels selective
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from synapse.viz.chartspec import (
    BarChart, Dashboard, DataTable, LineChart, Series, StatTile,
)

# fixed-order categorical palette → CSS vars --s1..--s4 (theme-resolved)
_SERIES_VARS = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)"]

_W, _H = 640, 280
_ML, _MR, _MT, _MB = 58, 16, 16, 30
_MR_LABELED = 128  # right margin when direct end-labels are drawn


# ─── formatting ──────────────────────────────────────────────


def _fmt(value: float | None, fmt: str, *, compact: bool = False) -> str:
    if value is None:
        return "–"
    if fmt == "percent":
        pct = value * 100
        if compact and abs(pct) >= 10:
            return f"{pct:.0f}%"
        return f"{pct:.2f}".rstrip("0").rstrip(".") + "%"
    if fmt == "bps":
        return f"{value * 10_000:,.0f} bps"
    if fmt == "currency":
        return "-$" + _abbr(abs(value)) if value < 0 else "$" + _abbr(value)
    return _abbr(value)


def _abbr(v: float) -> str:
    a = abs(v)
    for cut, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= cut:
            out = v / cut
            return f"{out:,.1f}".rstrip("0").rstrip(".") + suffix
    if a >= 100 or v == int(v):
        return f"{v:,.0f}"
    return f"{v:,.2f}".rstrip("0").rstrip(".")


def _esc(text: Any) -> str:
    return (str(text).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


# ─── scales ──────────────────────────────────────────────────


def _domain(values: list[float], *, zero: bool) -> tuple[float, float]:
    lo, hi = min(values), max(values)
    if zero:
        lo = min(0.0, lo)
        hi = max(0.0, hi)
    if lo == hi:
        pad = abs(hi) * 0.1 or 1.0
        return lo - pad, hi + pad
    pad = (hi - lo) * 0.08
    return (lo if zero and lo == 0 else lo - pad), hi + pad


def _ticks(lo: float, hi: float, n: int = 4) -> list[float]:
    return [lo + (hi - lo) * i / (n - 1) for i in range(n)]


# ─── SVG builders ────────────────────────────────────────────


def _svg_open(right_margin: int) -> tuple[list[str], float, float]:
    plot_w = _W - _ML - right_margin
    plot_h = _H - _MT - _MB
    return ([f'<svg viewBox="0 0 {_W} {_H}" role="img" '
             f'preserveAspectRatio="xMidYMid meet">'], plot_w, plot_h)


def _grid(parts: list[str], lo: float, hi: float, plot_w: float,
          plot_h: float, fmt: str) -> None:
    for tick in _ticks(lo, hi):
        y = _MT + plot_h - (tick - lo) / (hi - lo) * plot_h
        parts.append(f'<line x1="{_ML}" y1="{y:.1f}" x2="{_ML + plot_w:.1f}" '
                     f'y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{_ML - 8}" y="{y + 3.5:.1f}" class="tick" '
                     f'text-anchor="end">{_esc(_fmt(tick, fmt, compact=True))}'
                     f'</text>')


def _line_svg(chart: LineChart) -> str:
    labeled = len(chart.series) > 1
    right = _MR_LABELED if labeled else _MR
    parts, plot_w, plot_h = _svg_open(right)
    xs = [p[0] for p in chart.series[0].points]
    values = [p[1] for s in chart.series for p in s.points if p[1] is not None]
    if not values:
        return "<svg></svg>"
    lo, hi = _domain(values, zero=False)
    _grid(parts, lo, hi, plot_w, plot_h, chart.format)

    def px(i: int) -> float:
        return _ML + (plot_w * i / max(len(xs) - 1, 1))

    def py(v: float) -> float:
        return _MT + plot_h - (v - lo) / (hi - lo) * plot_h

    # x labels: first / middle / last
    for i in {0, len(xs) // 2, len(xs) - 1}:
        parts.append(f'<text x="{px(i):.1f}" y="{_H - 8}" class="tick" '
                     f'text-anchor="middle">{_esc(xs[i])}</text>')

    for idx, series in enumerate(chart.series):
        color = _SERIES_VARS[idx]
        coords = [(px(i), py(v)) for i, (_, v) in enumerate(series.points)
                  if v is not None]
        if not coords:
            continue
        path = "M" + " L".join(f"{x:.1f} {y:.1f}" for x, y in coords)
        parts.append(f'<path d="{path}" fill="none" stroke="{color}" '
                     f'stroke-width="2" stroke-linejoin="round" '
                     f'stroke-linecap="round"/>')
        ex, ey = coords[-1]
        parts.append(f'<circle cx="{ex:.1f}" cy="{ey:.1f}" r="3.5" '
                     f'fill="{color}" stroke="var(--surface)" '
                     f'stroke-width="2"/>')
        if labeled:  # direct end label: colored dot + ink text
            parts.append(f'<circle cx="{ex + 10:.1f}" cy="{ey:.1f}" r="3" '
                         f'fill="{color}"/>')
            parts.append(f'<text x="{ex + 17:.1f}" y="{ey + 3.5:.1f}" '
                         f'class="dlabel">{_esc(series.name)}</text>')
        else:  # single series: label the endpoint value
            parts.append(f'<text x="{ex:.1f}" y="{ey - 9:.1f}" class="vlabel" '
                         f'text-anchor="middle">'
                         f'{_esc(_fmt(series.points[-1][1], chart.format))}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _bar_path(x: float, y: float, w: float, h: float, *,
              horizontal: bool) -> str:
    """Bar with a 3px rounded data-end, flat at the baseline."""
    r = min(3.0, w / 2 if not horizontal else h / 2, abs(h if not horizontal else w))
    if h <= 0 and not horizontal:
        return ""
    if horizontal:
        return (f"M{x:.1f} {y:.1f} h{max(w - r, 0):.1f} "
                f"q{r:.1f} 0 {r:.1f} {r:.1f} v{max(h - 2 * r, 0):.1f} "
                f"q0 {r:.1f} -{r:.1f} {r:.1f} h-{max(w - r, 0):.1f} Z")
    return (f"M{x:.1f} {y + h:.1f} v-{max(h - r, 0):.1f} "
            f"q0 -{r:.1f} {r:.1f} -{r:.1f} h{max(w - 2 * r, 0):.1f} "
            f"q{r:.1f} 0 {r:.1f} {r:.1f} v{max(h - r, 0):.1f} Z")


def _vbar_svg(chart: BarChart) -> str:
    parts, plot_w, plot_h = _svg_open(_MR)
    n_cat, n_ser = len(chart.categories), len(chart.series)
    if chart.stacked:
        totals = [sum((s.points[i][1] or 0) for s in chart.series)
                  for i in range(n_cat)]
        lo, hi = _domain(totals + [0], zero=True)
    else:
        values = [p[1] or 0 for s in chart.series for p in s.points]
        lo, hi = _domain(values, zero=True)
    _grid(parts, lo, hi, plot_w, plot_h, chart.format)

    def py(v: float) -> float:
        return _MT + plot_h - (v - lo) / (hi - lo) * plot_h

    slot = plot_w / n_cat
    label_values = (not chart.stacked) and n_cat * n_ser <= 12
    for ci, cat in enumerate(chart.categories):
        cx = _ML + slot * ci
        parts.append(f'<text x="{cx + slot / 2:.1f}" y="{_H - 8}" class="tick" '
                     f'text-anchor="middle">{_esc(cat)}</text>')
        if chart.stacked:
            bar_w = min(slot * 0.55, 44.0)
            x = cx + (slot - bar_w) / 2
            y_cursor = py(0)
            for si, series in enumerate(chart.series):
                v = series.points[ci][1] or 0
                h = py(0) - py(v)
                if h <= 0.5:
                    continue
                top = y_cursor - h
                parts.append(f'<path d="{_bar_path(x, top, bar_w, h - 2, horizontal=False)}" '
                             f'fill="{_SERIES_VARS[si]}"/>')
                y_cursor = top - 0  # 2px gap already carved from height
        else:
            group_w = min(slot * 0.72, 26.0 * n_ser)
            bar_w = (group_w - 2 * (n_ser - 1)) / n_ser
            x0 = cx + (slot - group_w) / 2
            for si, series in enumerate(chart.series):
                v = series.points[ci][1]
                if v is None:
                    continue
                x = x0 + si * (bar_w + 2)
                h = py(0) - py(v)
                parts.append(f'<path d="{_bar_path(x, py(v), bar_w, h, horizontal=False)}" '
                             f'fill="{_SERIES_VARS[si]}"/>')
                if label_values:
                    parts.append(f'<text x="{x + bar_w / 2:.1f}" '
                                 f'y="{py(v) - 6:.1f}" class="vlabel" '
                                 f'text-anchor="middle">'
                                 f'{_esc(_fmt(v, chart.format, compact=True))}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _hbar_svg(chart: BarChart) -> str:
    """Top-N ranking form: one series, categories on the y axis."""
    series = chart.series[0]
    n = len(chart.categories)
    row_h, gap = 26, 8
    height = _MT + n * (row_h + gap) + 8
    label_w = 150
    plot_w = _W - label_w - 84
    parts = [f'<svg viewBox="0 0 {_W} {height}" role="img" '
             f'preserveAspectRatio="xMidYMid meet">']
    values = [p[1] or 0 for p in series.points]
    hi = max(values + [0]) or 1.0
    for i, cat in enumerate(chart.categories):
        v = series.points[i][1] or 0
        y = _MT + i * (row_h + gap)
        w = plot_w * v / hi
        parts.append(f'<text x="{label_w - 10}" y="{y + row_h / 2 + 4:.1f}" '
                     f'class="dlabel" text-anchor="end">{_esc(cat)}</text>')
        parts.append(f'<path d="{_bar_path(label_w, y, w, row_h, horizontal=True)}" '
                     f'fill="{_SERIES_VARS[0]}"/>')
        parts.append(f'<text x="{label_w + w + 8:.1f}" '
                     f'y="{y + row_h / 2 + 4:.1f}" class="vlabel">'
                     f'{_esc(_fmt(v, chart.format, compact=True))}</text>')
    parts.append("</svg>")
    return "".join(parts)


# ─── HTML fragments ──────────────────────────────────────────


def _legend(series: list[Series]) -> str:
    if len(series) < 2:
        return ""
    chips = "".join(
        f'<span class="chip"><i style="background:{_SERIES_VARS[i]}"></i>'
        f'{_esc(s.name)}</span>' for i, s in enumerate(series)
    )
    return f'<div class="legend">{chips}</div>'


def _card(title: str, body: str, caption: str = "") -> str:
    cap = f'<p class="caption">{_esc(caption)}</p>' if caption else ""
    return (f'<figure class="viz-card"><figcaption>{_esc(title)}'
            f'</figcaption>{body}{cap}</figure>')


def render_spec(spec: StatTile | LineChart | BarChart | DataTable) -> str:
    """One spec → one HTML fragment (a .viz-card)."""
    if isinstance(spec, StatTile):
        delta_html = ""
        if spec.delta is not None:
            klass = ("neutral" if spec.delta_is_good is None
                     else "good" if spec.delta_is_good else "bad")
            arrow = "▲" if spec.delta >= 0 else "▼"
            delta_html = (f'<span class="delta {klass}">{arrow} '
                          f'{_esc(_fmt(abs(spec.delta), spec.format))}'
                          f'{" " + _esc(spec.delta_label) if spec.delta_label else ""}</span>')
        sub = f'<div class="sub">{_esc(spec.subtitle)}</div>' if spec.subtitle else ""
        foot = f'<div class="foot">{_esc(spec.footnote)}</div>' if spec.footnote else ""
        return (f'<figure class="viz-card stat"><figcaption>{_esc(spec.title)}'
                f'</figcaption><div class="big">{_esc(_fmt(spec.value, spec.format))}'
                f'{delta_html}</div>{sub}{foot}</figure>')

    if isinstance(spec, LineChart):
        return _card(spec.title, _line_svg(spec) + _legend(spec.series),
                     spec.caption)

    if isinstance(spec, BarChart):
        if spec.horizontal:
            if len(spec.series) > 1:
                raise ValueError("horizontal (top-N) bars take exactly one "
                                 "series — rank one measure at a time")
            return _card(spec.title, _hbar_svg(spec), spec.caption)
        return _card(spec.title, _vbar_svg(spec) + _legend(spec.series),
                     spec.caption)

    if isinstance(spec, DataTable):
        head = "".join(f"<th>{_esc(c)}</th>" for c in spec.columns)
        body = "".join(
            "<tr>" + "".join(f"<td>{_esc(cell)}</td>" for cell in row) + "</tr>"
            for row in spec.rows
        )
        note = ('<p class="caption">showing first 50 rows — full result in '
                'the data export</p>' if spec.truncated else
                (f'<p class="caption">{_esc(spec.caption)}</p>'
                 if spec.caption else ""))
        return (f'<figure class="viz-card"><figcaption>{_esc(spec.title)}'
                f'</figcaption><div class="tbl-wrap"><table><thead><tr>{head}'
                f'</tr></thead><tbody>{body}</tbody></table></div>{note}</figure>')

    raise ValueError(f"unknown spec type: {type(spec).__name__}")


_PAGE_CSS = """
:root{
  --bg:#F4F6F9;--surface:#FFFFFF;--surface-2:#EDF0F5;--border:#E2E6ED;
  --ink:#181B22;--ink-2:#4C5464;--ink-3:#7B8494;
  --s1:#3B6FE0;--s2:#0E9463;--s3:#B9770C;--s4:#6D3BE0;
  --good:#0E9463;--bad:#D62B4E;
  --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:"SF Mono","JetBrains Mono",ui-monospace,Menlo,Consolas,monospace;
}
@media (prefers-color-scheme:dark){:root{
  --bg:#0B0D12;--surface:#12151C;--surface-2:#181C25;--border:#232935;
  --ink:#E8EBF1;--ink-2:#A4ACBB;--ink-3:#69717F;
  --s1:#5E8DF0;--s2:#1FA478;--s3:#B87F0E;--s4:#9B7CF0;
  --good:#34D399;--bad:#FB7185;}}
:root[data-theme="dark"]{
  --bg:#0B0D12;--surface:#12151C;--surface-2:#181C25;--border:#232935;
  --ink:#E8EBF1;--ink-2:#A4ACBB;--ink-3:#69717F;
  --s1:#5E8DF0;--s2:#1FA478;--s3:#B87F0E;--s4:#9B7CF0;
  --good:#34D399;--bad:#FB7185;}
:root[data-theme="light"]{
  --bg:#F4F6F9;--surface:#FFFFFF;--surface-2:#EDF0F5;--border:#E2E6ED;
  --ink:#181B22;--ink-2:#4C5464;--ink-3:#7B8494;
  --s1:#3B6FE0;--s2:#0E9463;--s3:#B9770C;--s4:#6D3BE0;
  --good:#0E9463;--bad:#D62B4E;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);
  padding:clamp(16px,3vw,32px)}
.dash-head{max-width:1080px;margin:0 auto 18px}
.dash-head h1{margin:0;font-size:20px;letter-spacing:-.01em}
.dash-head p{margin:4px 0 0;color:var(--ink-2);font-size:13.5px}
.grid{max-width:1080px;margin:0 auto;display:grid;gap:14px;
  grid-template-columns:repeat(12,1fr)}
.grid>*{grid-column:span var(--span,6)}
@media (max-width:760px){.grid>*{grid-column:span 12}}
.viz-card{margin:0;background:var(--surface);border:1px solid var(--border);
  border-radius:12px;padding:14px 16px;min-width:0}
.viz-card figcaption{font-size:12px;font-weight:600;letter-spacing:.04em;
  text-transform:uppercase;color:var(--ink-3);margin-bottom:8px}
.viz-card svg{display:block;width:100%;height:auto}
.viz-card .grid{stroke:var(--border);stroke-width:1}
.tick{font-family:var(--mono);font-size:10.5px;fill:var(--ink-3)}
.dlabel{font-family:var(--sans);font-size:11.5px;fill:var(--ink-2)}
.vlabel{font-family:var(--mono);font-size:10.5px;fill:var(--ink-2)}
.legend{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px}
.legend .chip{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;
  color:var(--ink-2)}
.legend .chip i{width:9px;height:9px;border-radius:3px;display:inline-block}
.caption{margin:8px 0 0;font-size:11.5px;color:var(--ink-3)}
.stat .big{font-family:var(--mono);font-variant-numeric:tabular-nums;
  font-size:34px;font-weight:600;letter-spacing:-.02em;display:flex;
  align-items:center;gap:10px;flex-wrap:wrap}
.stat .delta{font-size:12px;font-weight:600;padding:2px 8px;border-radius:999px}
.stat .delta.good{color:var(--good);background:color-mix(in srgb,var(--good) 12%,transparent)}
.stat .delta.bad{color:var(--bad);background:color-mix(in srgb,var(--bad) 12%,transparent)}
.stat .delta.neutral{color:var(--ink-2);background:var(--surface-2)}
.stat .sub{font-size:12.5px;color:var(--ink-2);margin-top:4px}
.stat .foot{font-size:11px;color:var(--ink-3);margin-top:8px}
.tbl-wrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13px}
th{font-size:11px;text-transform:uppercase;letter-spacing:.04em;
  color:var(--ink-3);text-align:left;padding:7px 10px;
  border-bottom:1px solid var(--border)}
td{padding:7px 10px;border-bottom:1px solid var(--border);
  font-family:var(--mono);font-variant-numeric:tabular-nums;color:var(--ink)}
td:first-child{font-family:var(--sans)}
.dash-foot{max-width:1080px;margin:16px auto 0;font-size:11.5px;
  color:var(--ink-3)}
"""


def render_page(
    spec: StatTile | LineChart | BarChart | DataTable | Dashboard,
    out_path: str | Path,
    *,
    page_title: str | None = None,
) -> Path:
    """Full standalone HTML page (single card or dashboard grid)."""
    if isinstance(spec, Dashboard):
        head = (f'<div class="dash-head"><h1>{_esc(spec.title)}</h1>'
                + (f"<p>{_esc(spec.subtitle)}</p>" if spec.subtitle else "")
                + "</div>")
        cards = "".join(
            f'<div style="--span:{item.span}">{render_spec(item.spec)}</div>'
            for item in spec.items
        )
        body = f'{head}<div class="grid">{cards}</div>'
        if spec.footer:
            body += f'<p class="dash-foot">{_esc(spec.footer)}</p>'
        title = page_title or spec.title
    else:
        body = f'<div class="grid"><div style="--span:12">{render_spec(spec)}</div></div>'
        title = page_title or getattr(spec, "title", "Chart")

    html = (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>{_esc(title)}</title><style>{_PAGE_CSS}</style></head>"
            f"<body>{body}</body></html>")
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path
