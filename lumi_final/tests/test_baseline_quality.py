"""Baseline-aware quality threshold tests.

Covers the three behaviors added when we decided to treat short
auto-generated descriptions as replaceable while preserving longer
human-curated copy:

1. discover_tables parses baseline.view.lkml and surfaces structured
   baseline_dimensions / baseline_measures / baseline_quality_signals
   on TableContext.
2. publish.additive_merge_view replaces baseline descriptions < 30 chars
   with enriched alternatives (and records each replacement on a ledger).
   Descriptions ≥ 30 chars are preserved.
3. publish_to_disk emits proposed_overwrites.md with the human-readable
   ledger so reviewers can sanity-check the threshold's decisions.
"""

from __future__ import annotations

from pathlib import Path

import lkml

from lumi.publish import (
    _DESCRIPTION_QUALITY_THRESHOLD,
    additive_merge_view,
    publish_to_disk,
)
from lumi.schemas import EnrichedOutput
from lumi.sql_to_context import prepare_enrichment_context


# ─── Mock MDM client (mirrors test_sql_to_context.py) ────────


class _NoopMDM:
    def fetch(self, table_name: str) -> dict:
        return {
            "table_name": table_name,
            "table_business_name": None,
            "table_description": None,
            "column_count": 0,
            "mdm_coverage_pct": 0.0,
            "columns": [],
        }


_AUTOGEN_BASELINE = """\
view: cornerstone_metrics {
  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
    description: "Bus seg"
  }
  dimension: data_source {
    type: string
    sql: ${TABLE}.data_source ;;
  }
  dimension: rpt_dt {
    type: date
    sql: ${TABLE}.rpt_dt ;;
  }
  measure: total_billed_business {
    type: sum
    sql: ${TABLE}.billed_business ;;
  }
}
"""


_HUMAN_CURATED_BASELINE = """\
view: cornerstone_metrics {
  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
    label: "Business Segment"
    description: "Top-level customer slice — Consumer, Commercial, or GNS. \
Drives most reporting cuts across the metrics fact table."
  }
}
"""


# ─── 1. TableContext sees structured baseline ─────────────────


def test_baseline_parser_surfaces_quality_signals(tmp_path: Path) -> None:
    """Auto-generated baseline → quality_signals counts the gaps."""
    baseline_dir = tmp_path / "looker_master" / "views"
    baseline_dir.mkdir(parents=True)
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        _AUTOGEN_BASELINE, encoding="utf-8"
    )

    sqls = [
        "SELECT SUM(billed_business) FROM cornerstone_metrics "
        "WHERE bus_seg = 'Consumer' AND rpt_dt = DATE('2025-01-01')"
    ]
    contexts = prepare_enrichment_context(sqls, _NoopMDM(), str(tmp_path / "looker_master"))
    ctx = contexts["cornerstone_metrics"]

    sig = ctx.baseline_quality_signals
    assert sig, "quality_signals should be populated when baseline exists"
    assert sig["dims_total"] == 3
    assert sig["measures_total"] == 1
    # bus_seg has a description but it's "Bus seg" (7 chars) → short.
    assert sig["dims_short_description"] == 1
    # data_source + rpt_dt have no description at all.
    assert sig["dims_missing_description"] == 2
    # No measure has value_format_name set.
    assert sig["measures_missing_value_format"] == 1
    # No dim_groups in the baseline → if rpt_dt was used as a date column in
    # a fp.date_functions it would be flagged. (Q1 above doesn't EXTRACT from
    # rpt_dt so date_functions is empty here — assertion is permissive.)
    assert sig["dates_as_plain_dim"] >= 0
    # No primary_key in the baseline.
    assert sig["has_primary_key"] is False

    # The structured lists are also surfaced.
    assert {d["name"] for d in ctx.baseline_dimensions} == {
        "bus_seg", "data_source", "rpt_dt"
    }
    assert ctx.baseline_measures[0]["name"] == "total_billed_business"


def test_baseline_parser_handles_missing_baseline(tmp_path: Path) -> None:
    """No .view.lkml in baseline_dir → empty structured fields, no crash."""
    sqls = ["SELECT a FROM unknown_table"]
    # Empty baseline dir
    (tmp_path / "looker_master").mkdir()
    contexts = prepare_enrichment_context(
        sqls, _NoopMDM(), str(tmp_path / "looker_master")
    )
    ctx = contexts["unknown_table"]
    assert ctx.baseline_dimensions == []
    assert ctx.baseline_quality_signals == {}


# ─── 2. Quality-threshold merge ──────────────────────────────


_ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS = """\
view: cornerstone_metrics {
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
    label: "Business Segment"
    description: "Customer business segment — Consumer, Commercial, or GNS. \
Used as the top-level slicer in revenue and account-acquisition reporting."
    tags: ["segment", "consumer", "commercial", "gns"]
  }
}
"""


def test_short_baseline_description_replaced_by_enriched() -> None:
    """A 7-char baseline description is replaced by the enriched one,
    and the replacement lands on the ledger."""
    ledger: list[dict] = []
    merged = additive_merge_view(
        _AUTOGEN_BASELINE,
        _ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS,
        ledger=ledger,
    )

    parsed = lkml.load(merged)
    bus_seg = next(
        d for d in parsed["views"][0]["dimensions"] if d["name"] == "bus_seg"
    )
    # The enriched description (way > 30 chars) replaced the "Bus seg" stub.
    assert "Consumer, Commercial, or GNS" in bus_seg["description"]
    # Tags from enrichment landed (cumulative).
    assert "consumer" in bus_seg["tags"]

    # Ledger captured the replacement with reason + before/after values.
    desc_overrides = [e for e in ledger if e["attribute"] == "description"]
    assert len(desc_overrides) == 1
    entry = desc_overrides[0]
    assert entry["field_name"] == "bus_seg"
    assert entry["baseline_value"] == "Bus seg"
    assert "Consumer, Commercial, or GNS" in entry["proposed_value"]
    assert "stub" in entry["reason"].lower()


def test_long_baseline_description_preserved() -> None:
    """A ≥ 30-char baseline description is human-curated → preserved."""
    ledger: list[dict] = []
    merged = additive_merge_view(
        _HUMAN_CURATED_BASELINE,
        _ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS,
        ledger=ledger,
    )
    parsed = lkml.load(merged)
    bus_seg = next(
        d for d in parsed["views"][0]["dimensions"] if d["name"] == "bus_seg"
    )
    # Baseline's curated description survived.
    assert "Drives most reporting cuts" in bus_seg["description"]
    # No description override on the ledger.
    desc_overrides = [e for e in ledger if e["attribute"] == "description"]
    assert desc_overrides == []
    # Tags are still cumulative — enriched tags landed even though desc preserved.
    assert "segment" in bus_seg["tags"]


def test_threshold_constant_consistent() -> None:
    """publish + sql_to_context use the same threshold so the planner's
    'short_description' count agrees with merge behavior."""
    from lumi.sql_to_context import _DESCRIPTION_QUALITY_THRESHOLD as ctx_thresh
    assert ctx_thresh == _DESCRIPTION_QUALITY_THRESHOLD


# ─── 3. proposed_overwrites.md written ───────────────────────


def test_proposed_overwrites_written_to_disk(tmp_path: Path) -> None:
    """publish_to_disk emits proposed_overwrites.md listing every
    quality-threshold replacement and every LLM-flagged overwrite."""
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        _AUTOGEN_BASELINE, encoding="utf-8"
    )

    eo = EnrichedOutput(
        view_lkml=_ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS,
        proposed_overwrites=[
            {
                "field_kind": "measure",
                "field_name": "total_billed_business",
                "attribute": "description",
                "baseline_value": "(none)",
                "proposed_value": "Sum of billed business in USD",
                "reason": "LLM judged this measure missing critical context",
            }
        ],
    )

    out_dir = tmp_path / "output"
    result = publish_to_disk(
        {"cornerstone_metrics": eo},
        baseline_dir=str(baseline_dir),
        output_dir=str(out_dir),
    )
    assert result["status"] == "ok"

    overrides_path = out_dir / "proposed_overwrites.md"
    assert overrides_path.exists()
    body = overrides_path.read_text(encoding="utf-8")
    # Quality-threshold entry (bus_seg) AND the LLM-flagged entry both surface.
    assert "bus_seg" in body
    assert "total_billed_business" in body
    assert "LLM-flagged" in body or "stub" in body


def test_proposed_overwrites_empty_when_baseline_curated(tmp_path: Path) -> None:
    """When the baseline is already in good shape and nothing was
    LLM-flagged, the file says so explicitly."""
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        _HUMAN_CURATED_BASELINE, encoding="utf-8"
    )

    eo = EnrichedOutput(view_lkml=_ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS)
    out_dir = tmp_path / "output"
    publish_to_disk(
        {"cornerstone_metrics": eo},
        baseline_dir=str(baseline_dir),
        output_dir=str(out_dir),
    )
    body = (out_dir / "proposed_overwrites.md").read_text(encoding="utf-8")
    assert "No baseline values were replaced" in body


# ─── 4. Integration: end-to-end discover → merge ─────────────


def test_baseline_lookup_finds_prefixed_filename(tmp_path: Path) -> None:
    """Looker repos sometimes prefix view files (bq_, dw_, edw_, etc.).
    The lookup must still find them when the canonical name doesn't exist.
    """
    baseline_dir = tmp_path / "looker_master"
    baseline_dir.mkdir()
    # Only the prefixed variant exists — no cornerstone_metrics.view.lkml.
    (baseline_dir / "dw_cornerstone_metrics.view.lkml").write_text(
        _AUTOGEN_BASELINE, encoding="utf-8"
    )

    sqls = ["SELECT a FROM cornerstone_metrics WHERE bus_seg = 'X'"]
    contexts = prepare_enrichment_context(sqls, _NoopMDM(), str(baseline_dir))
    ctx = contexts["cornerstone_metrics"]
    # Baseline was found despite filename mismatch.
    assert ctx.existing_view_lkml is not None
    assert "view: cornerstone_metrics" in ctx.existing_view_lkml
    assert ctx.baseline_quality_signals  # populated => parse worked


def test_baseline_lookup_canonical_wins_over_prefix(tmp_path: Path) -> None:
    """If both <table>.view.lkml and bq_<table>.view.lkml exist, the
    canonical filename wins — we don't shadow it with a prefix variant
    that might be from a different team's mirror.
    """
    baseline_dir = tmp_path / "looker_master"
    baseline_dir.mkdir()
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        '# canonical\nview: cornerstone_metrics { sql_table_name: `t` ;; }',
        encoding="utf-8",
    )
    (baseline_dir / "bq_cornerstone_metrics.view.lkml").write_text(
        '# prefixed\nview: cornerstone_metrics { sql_table_name: `other` ;; }',
        encoding="utf-8",
    )

    sqls = ["SELECT a FROM cornerstone_metrics"]
    contexts = prepare_enrichment_context(sqls, _NoopMDM(), str(baseline_dir))
    ctx = contexts["cornerstone_metrics"]
    assert "# canonical" in (ctx.existing_view_lkml or "")


def test_baseline_lookup_view_name_fallback(tmp_path: Path) -> None:
    """When the FILENAME has no obvious link to the table but the VIEW
    NAME inside the file matches, the fuzzy fallback finds it.
    Mirrors how Looker actually resolves explores against view names.
    """
    baseline_dir = tmp_path / "looker_master"
    baseline_dir.mkdir()
    # Filename is unrelated; view declaration inside matches.
    (baseline_dir / "weird_legacy_filename.view.lkml").write_text(
        'view: cornerstone_metrics {\n'
        '  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;\n'
        '}\n',
        encoding="utf-8",
    )

    sqls = ["SELECT a FROM cornerstone_metrics"]
    contexts = prepare_enrichment_context(sqls, _NoopMDM(), str(baseline_dir))
    ctx = contexts["cornerstone_metrics"]
    assert ctx.existing_view_lkml is not None
    assert "axp-lumi.dw.cornerstone_metrics" in ctx.existing_view_lkml


def test_end_to_end_baseline_aware_pipeline(tmp_path: Path) -> None:
    """Discover sees the gaps; merge fixes them; ledger documents what changed."""
    baseline_dir = tmp_path / "looker_master"
    baseline_dir.mkdir()
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        _AUTOGEN_BASELINE, encoding="utf-8"
    )

    sqls = [
        "SELECT SUM(billed_business) FROM cornerstone_metrics "
        "WHERE bus_seg = 'Consumer' AND rpt_dt = DATE('2025-01-01')"
    ]
    contexts = prepare_enrichment_context(sqls, _NoopMDM(), str(baseline_dir))
    ctx = contexts["cornerstone_metrics"]

    # Discover correctly identified the gaps.
    assert ctx.baseline_quality_signals["dims_short_description"] == 1
    assert ctx.baseline_quality_signals["dims_missing_description"] == 2

    # Merge with enriched fills the gaps.
    eo = EnrichedOutput(view_lkml=_ENRICHED_VIEW_WITH_GOOD_DESCRIPTIONS)
    out_dir = tmp_path / "out"
    publish_to_disk(
        {"cornerstone_metrics": eo},
        baseline_dir=str(baseline_dir),
        output_dir=str(out_dir),
    )
    merged_view = (out_dir / "views" / "cornerstone_metrics.view.lkml").read_text()
    parsed = lkml.load(merged_view)
    bus_seg = next(
        d for d in parsed["views"][0]["dimensions"] if d["name"] == "bus_seg"
    )
    assert len(bus_seg["description"]) >= _DESCRIPTION_QUALITY_THRESHOLD
