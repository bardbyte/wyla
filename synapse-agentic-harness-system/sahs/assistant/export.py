"""Server-side artifact export (Synapse v2 §5/§13.5): the PPTX deck.

A dashboard exports as a deck — one panel per slide, the meridian
line in the slide NOTES (the disclosure travels with the file, where
a presenter actually looks), the build id in every footer, and an
EXPLORATORY watermark drawn on any slide whose panel still carries
one. A single chart/kpi/table exports as a one-panel deck the same
way. Charts become native PowerPoint charts (editable, not screen-
shots) where PowerPoint has the kind — line, area, bar, horizontal
bar, stacked and hundred-percent bars, histogram, combo (as clustered
columns), small multiples (as lines) — and the rest (scatter over
labels, heatmap, waterfall) land as a table under the chart's title,
the notes saying so; a chart PowerPoint cannot build never crashes
the export, it falls back to the same table. Numbers read through the
same rules as the page (number_format / format_value).
"""

from __future__ import annotations

import io
from typing import Any

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from .artifacts import format_value, number_format

SLIDE_W, SLIDE_H = Inches(13.333), Inches(7.5)
_CHART_TYPES = {"line": XL_CHART_TYPE.LINE,
                "area": XL_CHART_TYPE.AREA,
                "bar": XL_CHART_TYPE.COLUMN_CLUSTERED,
                "hbar": XL_CHART_TYPE.BAR_CLUSTERED,
                "stacked_bar": XL_CHART_TYPE.COLUMN_STACKED,
                "percent_bar": XL_CHART_TYPE.COLUMN_STACKED_100,
                "histogram": XL_CHART_TYPE.COLUMN_CLUSTERED,
                "combo": XL_CHART_TYPE.COLUMN_CLUSTERED,
                "small_multiples": XL_CHART_TYPE.LINE,
                "scatter": XL_CHART_TYPE.LINE_MARKERS}
# what the notes say when PowerPoint draws a kind another way
_CHART_NOTES = {"scatter": "scatter rendered as a marked line: "
                           "PowerPoint charts here are categorical",
                "combo": "combo rendered as clustered columns: the "
                         "line series ride as columns here",
                "small_multiples": "small multiples rendered as one "
                                   "multi-line chart",
                "histogram": "histogram rendered as columns over the "
                             "bins"}
# kinds with no native chart: a table under the chart's title
_TABLE_KINDS = ("heatmap", "waterfall")
MAX_TABLE_ROWS = 14


def _blank(prs: Presentation):
    return prs.slides.add_slide(prs.slide_layouts[6])


def _text(slide, left, top, width, height, text, *, size=14,
          bold=False, align=PP_ALIGN.LEFT, color=None):
    box = slide.shapes.add_textbox(left, top, width, height)
    frame = box.text_frame
    frame.word_wrap = True
    para = frame.paragraphs[0]
    para.text = text
    para.alignment = align
    para.font.size = Pt(size)
    para.font.bold = bold
    if color:
        para.font.color.rgb = RGBColor(*color)
    return box


def _footer(slide, build_id: str) -> None:
    _text(slide, Inches(0.4), SLIDE_H - Inches(0.45),
          SLIDE_W - Inches(0.8), Inches(0.35),
          f"Meridian build {build_id or '?'} · every number carries "
          "its definition status", size=9, color=(122, 117, 106))


def _notes(slide, spec: dict[str, Any], extra: str = "") -> None:
    prov = spec.get("provenance") or {}
    lines = []
    if prov:
        lines.append(f"[{prov.get('status', '?')}] "
                     + str(prov.get("meridian_line", "")))
    if spec.get("watermark"):
        lines.append(f"{spec['watermark']}: a check must stand "
                     "behind this number before it loses the "
                     "watermark.")
    if spec.get("reason"):
        lines.append(f"Why this chart: {spec['reason']}")
    if extra:
        lines.append(extra)
    slide.notes_slide.notes_text_frame.text = "\n".join(lines)


def _watermark(slide, text: str) -> None:
    box = _text(slide, Inches(2.5), Inches(3.0), Inches(8.3),
                Inches(1.2), text, size=54, bold=True,
                align=PP_ALIGN.CENTER, color=(200, 170, 110))
    box.rotation = -15


def _table(slide, header: list[str], rows: list[list[str]],
           top=Inches(1.2)) -> None:
    rows = rows[:MAX_TABLE_ROWS]
    shape = slide.shapes.add_table(
        len(rows) + 1, max(len(header), 1), Inches(0.6), top,
        SLIDE_W - Inches(1.2), Inches(0.4) * (len(rows) + 1))
    table = shape.table
    for c, label in enumerate(header):
        table.cell(0, c).text = str(label)
    for r, row in enumerate(rows, start=1):
        for c, value in enumerate(row[:len(header)]):
            table.cell(r, c).text = str(value)


def _chart_as_table(spec: dict[str, Any]) -> tuple[list[str],
                                                   list[list[str]]]:
    """Any chart spec as header + rows, numbers formatted: a heatmap's
    matrix, else one row per x label with a column per series."""
    kind = spec.get("kind", "")
    if kind == "heatmap":
        fmt = number_format([v for row in spec.get("values") or []
                             for v in row],
                            {"name": spec.get("name"),
                             "unit": spec.get("unit"),
                             "format": spec.get("format")})
        header = [str(spec.get("name") or "")] + [str(x) for x in
                                                  spec.get("x") or []]
        rows = [[str(y)] + [format_value(v, fmt) for v in row]
                for y, row in zip(spec.get("y") or [],
                                  spec.get("values") or [])]
        return header, rows
    series = spec.get("series") or []
    categories: list[str] = []
    for entry in series:
        for p in entry.get("points", []):
            label = str(p[0])
            if label not in categories:
                categories.append(label)
    header = [str(spec.get("x_label") or "")] + [
        str(s.get("name", "")) for s in series]
    columns = []
    for s in series:
        by_label = {str(p[0]): p[1] for p in s.get("points", [])}
        fmt = number_format([p[1] for p in s.get("points", [])],
                            {"name": s.get("name"),
                             "unit": s.get("unit") or spec.get("unit"),
                             "format": s.get("format")
                             or spec.get("format")})
        columns.append([format_value(by_label.get(c), fmt)
                        for c in categories])
    rows = [[c] + [col[i] for col in columns]
            for i, c in enumerate(categories)]
    return header, rows


def _chart_slide(slide, spec: dict[str, Any]) -> str:
    kind = spec.get("kind", "line")
    if kind in _TABLE_KINDS:
        header, rows = _chart_as_table(spec)
        _table(slide, header, rows)
        return (f"{kind.replace('_', ' ')} has no native PowerPoint "
                "chart: the values are the table on this slide")
    data = CategoryChartData()
    series = spec.get("series") or []
    # one category axis, the union of every series' x labels in order
    # of first appearance (a forecast series starts where the actuals
    # end; it is not drawn over them); a missing or null y is a gap
    categories: list[str] = []
    for entry in series:
        for p in entry.get("points", []):
            label = str(p[0])
            if label not in categories:
                categories.append(label)
    data.categories = categories
    for entry in series:
        by_label = {str(p[0]): p[1] for p in entry.get("points", [])}
        data.add_series(entry.get("name", "series"),
                        [by_label.get(c) for c in categories])
    try:
        chart = slide.shapes.add_chart(
            _CHART_TYPES.get(kind, XL_CHART_TYPE.LINE),
            Inches(0.6), Inches(1.1), SLIDE_W - Inches(1.2),
            Inches(5.4), data).chart
        if len(series) > 1:
            chart.has_legend = True
    except Exception as e:      # never a crash: the table says it all
        header, rows = _chart_as_table(spec)
        _table(slide, header, rows)
        return (f"PowerPoint could not draw this {kind} "
                f"({type(e).__name__}): the values are the table on "
                "this slide")
    return _CHART_NOTES.get(kind, "")


def _table_slide(slide, spec: dict[str, Any]) -> None:
    columns = spec.get("columns") or []
    rows = (spec.get("rows") or [])[:MAX_TABLE_ROWS]
    formats = {}
    for col in columns:
        formats[col["key"]] = number_format(
            [r.get(col["key"]) for r in spec.get("rows") or []],
            {"name": col["key"], "label": col.get("label"),
             "unit": col.get("unit"), "format": col.get("format")})
    header = [str(col.get("label", col["key"])) for col in columns]
    body = [[format_value(row.get(col["key"]), formats[col["key"]])
             for col in columns] for row in rows]
    _table(slide, header, body)


def _kpi_slide(slide, spec: dict[str, Any]) -> None:
    if spec.get("label"):
        _text(slide, Inches(1), Inches(2.0), SLIDE_W - Inches(2),
              Inches(0.6), str(spec["label"]).upper(), size=18,
              align=PP_ALIGN.CENTER, color=(122, 117, 106))
    fmt = number_format([spec.get("value")],
                        {"name": spec.get("label"),
                         "unit": spec.get("unit"),
                         "format": spec.get("format")})
    value = format_value(spec.get("value"), fmt)
    if spec.get("unit") and fmt["kind"] == "count":
        value += f" {spec['unit']}"
    _text(slide, Inches(1), Inches(2.7), SLIDE_W - Inches(2),
          Inches(1.6), value, size=66, bold=True,
          align=PP_ALIGN.CENTER)
    if isinstance(spec.get("delta"), (int, float)):
        arrow = "▲" if spec["delta"] >= 0 else "▼"
        delta_fmt = number_format(
            [spec["delta"]],
            {"name": "delta", "format": spec["delta_format"]}) \
            if spec.get("delta_format") else dict(fmt, compact=False)
        shown = format_value(abs(spec["delta"]), delta_fmt)
        if spec.get("compare_label"):
            shown += f" {spec['compare_label']}"
        _text(slide, Inches(1), Inches(4.3), SLIDE_W - Inches(2),
              Inches(0.6), f"{arrow} {shown}", size=20,
              align=PP_ALIGN.CENTER,
              color=(46, 125, 50) if spec["delta"] >= 0
              else (179, 57, 47))


def _doc_slide(slide, spec: dict[str, Any]) -> None:
    text = str(spec.get("markdown", ""))
    plain = "\n".join(line.lstrip("#>- ").rstrip()
                      for line in text.splitlines())[:1800]
    _text(slide, Inches(0.6), Inches(1.2), SLIDE_W - Inches(1.2),
          Inches(5.4), plain, size=14)


def _panel_slide(prs: Presentation, ptype: str, title: str,
                 spec: dict[str, Any], build_id: str) -> None:
    slide = _blank(prs)
    _text(slide, Inches(0.4), Inches(0.25), SLIDE_W - Inches(0.8),
          Inches(0.7), title or ptype, size=24, bold=True)
    extra = ""
    if ptype == "chart":
        extra = _chart_slide(slide, spec)
    elif ptype == "table":
        _table_slide(slide, spec)
    elif ptype == "kpi":
        _kpi_slide(slide, spec)
    else:
        _doc_slide(slide, spec)
    if spec.get("watermark"):
        _watermark(slide, str(spec["watermark"]))
    _notes(slide, spec, extra)
    _footer(slide, build_id)


def artifact_pptx(row: dict[str, Any]) -> bytes:
    """A validated artifact row → a .pptx deck, provenance riding in
    the notes. Dashboards get one slide per panel; chart / table /
    kpi / document get a one-panel deck."""
    spec = row.get("spec") or {}
    build_id = str(spec.get("build_id", ""))
    prs = Presentation()
    prs.slide_width, prs.slide_height = SLIDE_W, SLIDE_H

    cover = _blank(prs)
    _text(cover, Inches(0.8), Inches(2.6), SLIDE_W - Inches(1.6),
          Inches(1.2), row.get("title") or row.get("type", "artifact"),
          size=40, bold=True)
    _text(cover, Inches(0.8), Inches(3.9), SLIDE_W - Inches(1.6),
          Inches(0.8),
          f"Synapse · Meridian build {build_id or '?'} · "
          f"v{row.get('version', 1)} · statuses and meridian lines "
          "are in each slide's notes", size=14,
          color=(122, 117, 106))
    _footer(cover, build_id)

    if row.get("type") == "dashboard":
        for panel in spec.get("panels") or []:
            _panel_slide(prs, panel.get("type", "document"),
                         panel.get("title", ""),
                         panel.get("spec") or {}, build_id)
        if spec.get("notes"):
            slide = _blank(prs)
            _text(slide, Inches(0.4), Inches(0.25),
                  SLIDE_W - Inches(0.8), Inches(0.7), "Notes",
                  size=24, bold=True)
            _doc_slide(slide, {"markdown": spec["notes"]})
            _footer(slide, build_id)
    else:
        _panel_slide(prs, row.get("type", "document"),
                     row.get("title", ""), spec, build_id)

    out = io.BytesIO()
    prs.save(out)
    return out.getvalue()


__all__ = ["artifact_pptx"]
