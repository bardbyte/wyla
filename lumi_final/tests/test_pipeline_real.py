"""End-to-end pipeline tests — Phase 1 → approval → Phase 2.

Exercises the real ``run_plan_phase`` and ``run_execute_phase`` functions
(no NotImplementedError stubs) against an in-process fixture project so
we can prove the wiring works before burning real Gemini tokens on the
work laptop.

LLM calls are bypassed via Phase 2's ``dry_run=True`` path (loads
fixture EnrichedOutputs from ``tests/fixtures/llm_responses/``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lumi.config import LumiConfig
from lumi.pipeline import (
    PipelineHaltError,
    LumiPipeline,
    run_execute_phase,
    run_plan_phase,
)


# ─── Fixture project layout ──────────────────────────────────


_BASELINE_VIEW = """\
view: cornerstone_metrics {
  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;
  dimension: bus_seg {
    type: string
    sql: ${TABLE}.bus_seg ;;
    description: "Bus seg"
  }
  measure: total_billed_business {
    type: sum
    sql: ${TABLE}.billed_business ;;
  }
}
"""


_MDM_DIGEST = {
    "table_name": "cornerstone_metrics",
    "table_business_name": "Cornerstone Metrics",
    "table_description": "Daily aggregated business metrics from Cornerstone source.",
    "column_count": 3,
    "mdm_coverage_pct": 1.0,
    "columns": [
        {"name": "bus_seg", "type": "STRING",
         "description": "Business segment: Consumer, Commercial, or GNS"},
        {"name": "billed_business", "type": "NUMERIC",
         "description": "Total billed business volume in USD"},
        {"name": "rpt_dt", "type": "DATE",
         "description": "Report date for the metric snapshot"},
    ],
}


@pytest.fixture
def fixture_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build a self-contained LUMI project under tmp_path and chdir into it."""
    # gold_queries
    qdir = tmp_path / "data" / "gold_queries"
    qdir.mkdir(parents=True)
    (qdir / "Q01.sql").write_text(
        "SELECT SUM(billed_business) FROM `axp-lumi.dw.cornerstone_metrics` "
        "WHERE bus_seg = 'Consumer' AND rpt_dt = DATE('2025-01-01')",
        encoding="utf-8",
    )
    (qdir / "Q02.sql").write_text(
        "SELECT bus_seg, SUM(billed_business) "
        "FROM `axp-lumi.dw.cornerstone_metrics` "
        "WHERE rpt_dt = DATE('2025-02-01') GROUP BY bus_seg",
        encoding="utf-8",
    )

    # baseline view
    bdir = tmp_path / "data" / "looker_master" / "views"
    bdir.mkdir(parents=True)
    (bdir / "cornerstone_metrics.view.lkml").write_text(
        _BASELINE_VIEW, encoding="utf-8"
    )

    # MDM cache
    mdir = tmp_path / "data" / "mdm_cache"
    mdir.mkdir(parents=True)
    (mdir / "cornerstone_metrics.json").write_text(
        json.dumps(_MDM_DIGEST), encoding="utf-8"
    )

    # tests/fixtures/llm_responses — copy the real fixture so dry-run
    # has a realistic enriched output to load.
    fixture_src = Path(__file__).parent / "fixtures" / "llm_responses"
    fixture_dst = tmp_path / "tests" / "fixtures" / "llm_responses"
    fixture_dst.mkdir(parents=True)
    src_file = fixture_src / "enrich_cornerstone_metrics.json"
    if src_file.exists():
        (fixture_dst / "enrich_cornerstone_metrics.json").write_text(
            src_file.read_text(encoding="utf-8"), encoding="utf-8"
        )

    monkeypatch.chdir(tmp_path)
    return tmp_path


def _build_cfg() -> LumiConfig:
    """Config relative to the cwd set by fixture_project."""
    cfg = LumiConfig()
    cfg.gold_queries_dir = "data/gold_queries"
    cfg.baseline_views_dir = "data/looker_master"
    cfg.mdm_cache_dir = "data/mdm_cache"
    cfg.output_dir = "output"
    return cfg


# ─── Phase 1 ─────────────────────────────────────────────────


def test_phase1_writes_expected_artifacts(fixture_project: Path) -> None:
    """Phase 1 produces session1_output.json + plan files + REVIEW.md
    without spending any LLM tokens."""
    result = run_plan_phase(_build_cfg())

    assert result.phase == "plan"
    assert result.tables_total == 1
    assert result.tables_succeeded == 1
    assert result.tables_failed == 0

    # Discovery output for Phase 2.
    assert Path("data/session1_output.json").exists()
    s1 = json.loads(Path("data/session1_output.json").read_text(encoding="utf-8"))
    assert "cornerstone_metrics" in s1
    # Baseline parsing populated structured fields.
    assert s1["cornerstone_metrics"]["baseline_quality_signals"]["dims_total"] == 1

    # Plan files.
    plan_md = Path("review_queue/cornerstone_metrics.plan.md")
    plan_json = Path("data/plans/cornerstone_metrics.plan.json")
    assert plan_md.exists()
    assert plan_json.exists()

    # The plan markdown has the reviewer-decision footer parsed by approval.
    body = plan_md.read_text(encoding="utf-8")
    assert "## Reviewer decision" in body
    assert "[ ] ✅ APPROVED" in body
    assert "[ ] ❌ REJECTED" in body

    # The plan JSON round-trips cleanly into EnrichmentPlan.
    pdata = json.loads(plan_json.read_text(encoding="utf-8"))
    assert pdata["table_name"] == "cornerstone_metrics"
    assert any(
        d.get("source_column") == "bus_seg" for d in pdata["proposed_dimensions"]
    )
    assert any(
        m.get("source_column") == "billed_business"
        for m in pdata["proposed_measures"]
    )

    # REVIEW.md summary written.
    assert Path("review_queue/REVIEW.md").exists()


def test_phase1_halts_on_missing_input(tmp_path: Path,
                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty queries dir → PipelineHaltError, no half-written state."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "gold_queries").mkdir(parents=True)
    (tmp_path / "data" / "looker_master").mkdir(parents=True)
    (tmp_path / "data" / "mdm_cache").mkdir(parents=True)

    with pytest.raises(PipelineHaltError, match="no .sql files"):
        run_plan_phase(_build_cfg())
    assert not Path("data/session1_output.json").exists()


def test_phase1_only_tables_filter(fixture_project: Path) -> None:
    """--table NAME filter writes only that table's plan file."""
    run_plan_phase(_build_cfg(), only_tables=["cornerstone_metrics"])
    assert Path("review_queue/cornerstone_metrics.plan.md").exists()


# ─── Approval gate ───────────────────────────────────────────


def _tick_approved(plan_md_path: Path) -> None:
    """Simulate the human ticking the APPROVED checkbox."""
    body = plan_md_path.read_text(encoding="utf-8")
    body = body.replace("- [ ] ✅ APPROVED", "- [x] ✅ APPROVED", 1)
    plan_md_path.write_text(body, encoding="utf-8")


def test_phase2_halts_when_pending_approval(fixture_project: Path) -> None:
    """Phase 2 must not run if any plan has no checkbox ticked."""
    run_plan_phase(_build_cfg())  # writes review_queue/<table>.plan.md
    # Don't tick anything — should halt.
    with pytest.raises(PipelineHaltError, match="approval gate FAIL"):
        run_execute_phase(_build_cfg(), dry_run=True)


def test_phase2_runs_after_approval_dry_run(fixture_project: Path) -> None:
    """Tick approval, run Phase 2 with dry_run=True, verify the full
    enrich → validate → publish chain produces output/."""
    run_plan_phase(_build_cfg())
    _tick_approved(Path("review_queue/cornerstone_metrics.plan.md"))

    result = run_execute_phase(_build_cfg(), dry_run=True)

    assert result.phase == "execute"
    assert result.tables_total == 1
    assert result.tables_succeeded == 1
    assert result.tables_failed == 0
    # Per-table enriched checkpoint written.
    assert Path("data/enriched/cornerstone_metrics.json").exists()
    # Coverage report produced.
    assert Path("output/coverage_report.json").exists()
    assert result.coverage_pct is not None
    # Final view written.
    out_view = Path("output/views/cornerstone_metrics.view.lkml")
    assert out_view.exists()


def test_phase2_resume_skips_cached(fixture_project: Path) -> None:
    """Re-running Phase 2 reuses data/enriched/ instead of re-enriching."""
    run_plan_phase(_build_cfg())
    _tick_approved(Path("review_queue/cornerstone_metrics.plan.md"))

    first = run_execute_phase(_build_cfg(), dry_run=True)
    assert first.tables_succeeded == 1
    assert first.tables_skipped_resume == 0

    second = run_execute_phase(_build_cfg(), dry_run=True)
    assert second.tables_succeeded == 0
    assert second.tables_skipped_resume == 1


def test_phase2_force_redoes_cached(fixture_project: Path) -> None:
    """--force ignores the resume cache."""
    run_plan_phase(_build_cfg())
    _tick_approved(Path("review_queue/cornerstone_metrics.plan.md"))

    run_execute_phase(_build_cfg(), dry_run=True)
    second = run_execute_phase(_build_cfg(), dry_run=True, force=True)
    assert second.tables_succeeded == 1
    assert second.tables_skipped_resume == 0


# ─── Class-level wrapper still works (back-compat) ───────────


def test_lumi_pipeline_class_runs_end_to_end(fixture_project: Path) -> None:
    """LumiPipeline.run_plan_phase + run_execute_phase still drive the flow."""
    pipeline = LumiPipeline(_build_cfg())
    plan_result = pipeline.run_plan_phase()
    assert plan_result.tables_succeeded == 1

    _tick_approved(Path("review_queue/cornerstone_metrics.plan.md"))

    execute_result = pipeline.run_execute_phase(dry_run=True)
    assert execute_result.tables_succeeded == 1
    # Status reads from disk markers — should now show progress on every stage.
    pipeline.print_status()  # smoke — must not raise
