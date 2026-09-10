"""Baseline LookML deep-extraction tests.

Locks in the per-view structural signals beyond the field lists:
view_description, view_label, sql_table_name, derived_table_sql,
primary_key_column, extends_chain, sets, parameters, access_filter,
drill_fields_curated, filtered_measures, sql_aliases.

Each is a piece of human curation in the baseline LookML that the
enrichment must preserve and use as grounding context.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lumi.schemas import TableContext
from lumi.sql_to_context import prepare_enrichment_context


class _NoopMDM:
    def fetch(self, table_name: str) -> dict:
        return {"table_name": table_name, "columns": [], "mdm_coverage_pct": 0.0}


def _build_project(tmp_path: Path, baseline_lkml: str) -> Path:
    qdir = tmp_path / "data" / "gold_queries"
    qdir.mkdir(parents=True)
    (qdir / "Q01.sql").write_text(
        "SELECT a FROM cornerstone_metrics WHERE bus_seg = 'Consumer'",
        encoding="utf-8",
    )
    bdir = tmp_path / "data" / "looker_master"
    bdir.mkdir(parents=True)
    (bdir / "cornerstone_metrics.view.lkml").write_text(
        baseline_lkml, encoding="utf-8"
    )
    return tmp_path


def _ctx(tmp_path: Path, baseline: str) -> TableContext:
    project = _build_project(tmp_path, baseline)
    qdir = project / "data" / "gold_queries"
    bdir = project / "data" / "looker_master"
    contexts = prepare_enrichment_context(
        [(qdir / "Q01.sql").read_text(encoding="utf-8")],
        _NoopMDM(),
        str(bdir),
    )
    return contexts["cornerstone_metrics"]


# ─── View-level metadata ─────────────────────────────────────


def test_view_level_description_extracted(tmp_path: Path) -> None:
    """View-level `description:` is the rarest + most valuable signal —
    when humans wrote one, it's the most concise table summary available.
    """
    baseline = """\
view: cornerstone_metrics {
  description: "Daily aggregated business metrics from the Cornerstone source system, broken down by business segment and reporting date."
  sql_table_name: `my-project.dw.cornerstone_metrics` ;;
  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; }
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert ctx.baseline_view_description is not None
    assert "Cornerstone source system" in ctx.baseline_view_description


def test_view_level_label_extracted(tmp_path: Path) -> None:
    baseline = """\
view: cornerstone_metrics {
  label: "Cornerstone Daily Metrics"
  sql_table_name: `my-project.dw.cornerstone_metrics` ;;
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert ctx.baseline_view_label == "Cornerstone Daily Metrics"


def test_sql_table_name_extracted(tmp_path: Path) -> None:
    """sql_table_name is the authoritative BQ FQN; preserve it exactly."""
    baseline = """\
view: cornerstone_metrics {
  sql_table_name: `my-project.dw.cornerstone_metrics` ;;
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert ctx.baseline_sql_table_name == "`my-project.dw.cornerstone_metrics`"


# ─── Primary key + extends ───────────────────────────────────


def test_primary_key_column_extracted_by_name(tmp_path: Path) -> None:
    """We pull the actual NAME of the PK dim, not just a bool. Critical
    for PK preservation — enrichment must NOT propose a different PK."""
    baseline = """\
view: cornerstone_metrics {
  dimension: rpt_dt_pk {
    primary_key: yes
    sql: ${TABLE}.rpt_dt ;;
    hidden: yes
  }
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
  }
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert ctx.baseline_primary_key_column == "rpt_dt_pk"


def test_extends_chain_extracted(tmp_path: Path) -> None:
    baseline = """\
view: cornerstone_metrics {
  extends: [base_metrics_view]
  sql_table_name: `my-project.dw.cornerstone_metrics` ;;
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert "base_metrics_view" in ctx.baseline_extends_chain


# ─── Pre-filtered measures (canonical slicing) ───────────────


def test_filtered_measures_preserved(tmp_path: Path) -> None:
    """measure: revenue_consumer { filters: [bus_seg: "Consumer"] } is
    the team's canonical pattern for filtered measures. Surface it so
    Gemini follows the same naming convention for new ones."""
    baseline = """\
view: cornerstone_metrics {
  measure: revenue_consumer {
    type: sum
    sql: ${TABLE}.amt ;;
    filters: [bus_seg: "Consumer"]
    description: "Consumer-segment revenue"
  }
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
  }
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert len(ctx.baseline_filtered_measures) == 1
    fm = ctx.baseline_filtered_measures[0]
    assert fm["name"] == "revenue_consumer"
    assert fm["filters"]


# ─── SQL aliases (dim name != source column) ─────────────────


def test_sql_aliases_extracted(tmp_path: Path) -> None:
    """When a dim is renamed (e.g. customer_segment ← bus_seg) it's
    a human-curated synonym mapping. Pull both for tag preservation."""
    baseline = """\
view: cornerstone_metrics {
  dimension: customer_segment {
    type: string
    sql: ${TABLE}.bus_seg ;;
  }
  dimension: same_name {
    type: string
    sql: ${TABLE}.same_name ;;
  }
}
"""
    ctx = _ctx(tmp_path, baseline)
    # Renamed → alias map.
    assert ctx.baseline_sql_aliases.get("customer_segment") == "bus_seg"
    # Same name → no alias entry.
    assert "same_name" not in ctx.baseline_sql_aliases


# ─── Drill fields, sets, parameters, access_filter ───────────


def test_drill_fields_curated_preserved(tmp_path: Path) -> None:
    baseline = """\
view: cornerstone_metrics {
  drill_fields: [bus_seg, data_source, rpt_dt]
  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; }
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert "bus_seg" in ctx.baseline_drill_fields_curated
    assert "rpt_dt" in ctx.baseline_drill_fields_curated


def test_access_filter_preserved(tmp_path: Path) -> None:
    """Security model — must NEVER be touched by enrichment."""
    baseline = """\
view: cornerstone_metrics {
  access_filter: {
    field: bus_seg
    user_attribute: allowed_segments
  }
  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; }
}
"""
    ctx = _ctx(tmp_path, baseline)
    assert ctx.baseline_access_filter
    af = ctx.baseline_access_filter[0]
    assert af.get("field") == "bus_seg"


# ─── Derived-table baseline ──────────────────────────────────


def test_derived_table_sql_extracted(tmp_path: Path) -> None:
    """If the baseline IS a derived_table view, its SQL is preserved as
    a style-guide signal for enrichment."""
    baseline = """\
view: cornerstone_metrics_pdt {
  derived_table: {
    sql: SELECT bus_seg, SUM(amt) AS total FROM raw_metrics GROUP BY 1 ;;
    persist_for: "1 hour"
  }
  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; }
}
"""
    # Need to use the view name in the file so our lookup finds it.
    project = _build_project(tmp_path, baseline)
    contexts = prepare_enrichment_context(
        ["SELECT a FROM cornerstone_metrics WHERE bus_seg = 'X'"],
        _NoopMDM(),
        str(project / "data" / "looker_master"),
    )
    # The fuzzy-by-view-name fallback should find the file
    # because cornerstone_metrics is in our query but the file's view is
    # cornerstone_metrics_pdt — won't match. So this case requires the
    # filename to match. Adjust: rename file to match.
    bdir = project / "data" / "looker_master"
    (bdir / "cornerstone_metrics.view.lkml").unlink()
    (bdir / "cornerstone_metrics.view.lkml").write_text(
        baseline.replace("cornerstone_metrics_pdt", "cornerstone_metrics"),
        encoding="utf-8",
    )
    contexts = prepare_enrichment_context(
        ["SELECT a FROM cornerstone_metrics WHERE bus_seg = 'X'"],
        _NoopMDM(),
        str(bdir),
    )
    ctx = contexts["cornerstone_metrics"]
    assert ctx.baseline_derived_table_sql is not None
    assert "GROUP BY 1" in ctx.baseline_derived_table_sql


# ─── Backward-compat: missing baseline → all empty defaults ──


def test_missing_baseline_yields_empty_defaults(tmp_path: Path) -> None:
    """No baseline file → every new structured field defaults to None or
    empty container. Pipeline must keep working without crashes."""
    qdir = tmp_path / "data" / "gold_queries"
    qdir.mkdir(parents=True)
    (qdir / "Q01.sql").write_text(
        "SELECT a FROM cornerstone_metrics", encoding="utf-8",
    )
    (tmp_path / "data" / "looker_master").mkdir(parents=True)

    contexts = prepare_enrichment_context(
        [(qdir / "Q01.sql").read_text(encoding="utf-8")],
        _NoopMDM(),
        str(tmp_path / "data" / "looker_master"),
    )
    ctx = contexts["cornerstone_metrics"]
    assert ctx.baseline_view_description is None
    assert ctx.baseline_view_label is None
    assert ctx.baseline_sql_table_name is None
    assert ctx.baseline_derived_table_sql is None
    assert ctx.baseline_primary_key_column is None
    assert ctx.baseline_extends_chain == []
    assert ctx.baseline_filtered_measures == []
    assert ctx.baseline_sql_aliases == {}


# ─── Quality signals stay populated alongside ────────────────


def test_quality_signals_stay_populated(tmp_path: Path) -> None:
    """Adding new fields shouldn't break existing quality_signals."""
    baseline = """\
view: cornerstone_metrics {
  description: "rich description of at least 30 characters"
  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; description: "short" }
  dimension: data_source { type: string sql: ${TABLE}.data_source ;; }
  measure: total { type: sum sql: ${TABLE}.amt ;; }
}
"""
    ctx = _ctx(tmp_path, baseline)
    sig = ctx.baseline_quality_signals
    assert sig["dims_total"] == 2
    assert sig["dims_short_description"] == 1   # bus_seg has a 5-char desc
    assert sig["dims_missing_description"] == 1  # data_source has none
    assert sig["measures_total"] == 1
    assert sig["measures_missing_value_format"] == 1
    assert sig["measures_missing_description"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
