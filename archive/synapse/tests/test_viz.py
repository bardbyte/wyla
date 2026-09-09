"""Chart specs + renderer — the constraints are code, not taste."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from synapse.viz.chartspec import (
    BarChart, Dashboard, DataTable, LineChart, Series, StatTile, parse_spec,
)
from synapse.viz.render import _fmt, render_page, render_spec


# ─── spec validation teaches the model ───────────────────────


def test_five_series_rejected_with_guidance():
    with pytest.raises(ValidationError, match="Fold the tail into"):
        LineChart(title="too many", series=[
            Series(name=f"s{i}", points=[("a", 1.0)]) for i in range(5)
        ])


def test_bar_series_must_match_categories():
    with pytest.raises(ValidationError, match="one value per category"):
        BarChart(title="mismatch", categories=["a", "b"],
                 series=[Series(name="x", points=[("a", 1.0)])])


def test_table_truncates_past_50_rows():
    table = DataTable(title="big", columns=["n"],
                      rows=[[i] for i in range(200)])
    assert len(table.rows) == 50
    assert table.truncated is True


def test_parse_spec_discriminates_on_kind():
    spec = parse_spec({"kind": "stat", "title": "C-30", "value": 0.0231,
                       "format": "percent"})
    assert isinstance(spec, StatTile)
    dash = parse_spec({"kind": "dashboard", "title": "D", "items": [
        {"spec": {"kind": "stat", "title": "x", "value": 1.0}, "span": 4},
    ]})
    assert isinstance(dash, Dashboard)


# ─── formatting ──────────────────────────────────────────────


def test_number_formats():
    assert _fmt(0.0231, "percent") == "2.31%"
    assert _fmt(0.146, "percent", compact=True) == "15%"
    assert _fmt(0.0014, "bps") == "14 bps"
    assert _fmt(1_240_000, "currency") == "$1.2M"
    assert _fmt(6_300_000_000, "number") == "6.3B"
    assert _fmt(None, "number") == "–"


# ─── rendering ───────────────────────────────────────────────


LINE = LineChart(
    title="C-30 dollar roll rate",
    series=[Series(name="Consumer", points=[
        ("Jan", 0.0205), ("Feb", 0.0211), ("Mar", 0.0202),
        ("Apr", 0.0217), ("May", 0.0217), ("Jun", 0.0231)])],
    format="percent",
)


def test_single_series_line_has_no_legend_and_labels_endpoint():
    html = render_spec(LINE)
    assert "<svg" in html and "</svg>" in html
    assert 'class="legend"' not in html          # 1 series → no legend
    assert "2.31%" in html                        # endpoint value labeled
    assert "var(--s1)" in html                    # palette by position


def test_multi_series_line_gets_legend_and_direct_labels():
    chart = LineChart(title="by segment", format="percent", series=[
        Series(name="Consumer", points=[("Jan", 0.02), ("Feb", 0.021)]),
        Series(name="Small Biz", points=[("Jan", 0.031), ("Feb", 0.029)]),
    ])
    html = render_spec(chart)
    assert 'class="legend"' in html
    assert html.count('class="dlabel"') == 2      # direct end labels
    assert "var(--s2)" in html


def test_grouped_bar_renders_value_labels_when_few():
    chart = BarChart(title="write-offs", categories=["Q1", "Q2", "Q3"],
                     series=[Series(name="gross", points=[
                         ("Q1", 1.2e6), ("Q2", 1.4e6), ("Q3", 1.1e6)])],
                     format="currency")
    html = render_spec(chart)
    assert "$1.4M" in html


def test_hbar_requires_single_series():
    chart = BarChart(title="top", categories=["a"], horizontal=True,
                     series=[Series(name="x", points=[("a", 1.0)]),
                             Series(name="y", points=[("a", 2.0)])])
    with pytest.raises(ValueError, match="exactly one series"):
        render_spec(chart)


def test_stat_tile_delta_direction_styling():
    up_bad = StatTile(title="C-30", value=0.0231, format="percent",
                      delta=0.0014, delta_is_good=False, delta_label="MoM")
    html = render_spec(up_bad)
    assert "▲" in html and "delta bad" in html and "14 bps" not in html


def test_dashboard_page_is_self_contained_and_themed(tmp_path):
    dash = Dashboard(
        title="Consumer Portfolio Health",
        subtitle="June 2026 · graph v20260706",
        items=[
            {"spec": StatTile(title="C-30", value=0.0231, format="percent",
                              delta=0.0014, delta_is_good=False), "span": 4},
            {"spec": LINE, "span": 8},
        ],
        footer="sources: bq · skills · corpus — all facts grounded",
    )
    path = render_page(dash, tmp_path / "dash.html")
    html = path.read_text()
    assert html.startswith("<!doctype html>")
    assert "http" not in html.split("</title>")[1]   # no external requests
    assert 'data-theme="dark"' in html               # both themes present
    assert "Consumer Portfolio Health" in html
    assert "--span:4" in html and "--span:8" in html
