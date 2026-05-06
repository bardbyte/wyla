"""Sparse-table guardrails + placeholder-detection tests.

When MDM coverage = 0%, baseline is empty, AND fewer than 3 queries
touch a table, no amount of LLM reasoning will produce a useful plan
— the model has nothing to ground on. Our guardrails:

  1. Skeleton plan flags such tables with a loud "UNREVIEWABLE" risk
  2. The markdown render opens with a banner before the authoring badge
  3. LLM-authored plans with >50% placeholder names fall back to skeleton
"""

from __future__ import annotations

from unittest.mock import patch

from lumi.plan_builder import (
    _has_excessive_placeholders,
    _is_unreviewable,
    build_enrichment_plan,
    build_enrichment_plan_skeleton,
    format_enrichment_plan_markdown,
)
from lumi.schemas import EnrichmentPlan, TableContext


def _ctx(
    *,
    mdm_pct: float = 0.0,
    has_baseline: bool = False,
    n_queries: int = 0,
    columns: list[str] | None = None,
) -> TableContext:
    return TableContext(
        table_name="t",
        columns_referenced=columns or [],
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[],
        joins_involving_this=[], filters_on_this=[],
        date_functions=[],
        mdm_columns=[], mdm_table_description=None,
        mdm_coverage_pct=mdm_pct,
        existing_view_lkml="view: t {}" if has_baseline else None,
        queries_using_this=[f"Q{i:02d}" for i in range(n_queries)],
    )


# ─── _is_unreviewable detection ──────────────────────────────


def test_unreviewable_when_all_three_signals_absent():
    ctx = _ctx(mdm_pct=0.0, has_baseline=False, n_queries=1)
    flag, why = _is_unreviewable(ctx)
    assert flag is True
    assert "MDM coverage" in why
    assert "no baseline" in why


def test_reviewable_with_just_mdm():
    ctx = _ctx(mdm_pct=0.6, has_baseline=False, n_queries=0)
    flag, _ = _is_unreviewable(ctx)
    assert flag is False


def test_reviewable_with_just_baseline():
    ctx = _ctx(mdm_pct=0.0, has_baseline=True, n_queries=0)
    flag, _ = _is_unreviewable(ctx)
    assert flag is False


def test_reviewable_with_enough_queries():
    ctx = _ctx(mdm_pct=0.0, has_baseline=False, n_queries=5)
    flag, _ = _is_unreviewable(ctx)
    assert flag is False


# ─── Plan stamps unreviewable into risks/questions ───────────


def test_skeleton_marks_unreviewable_in_risks():
    ctx = _ctx(mdm_pct=0.0, has_baseline=False, n_queries=1)
    plan = build_enrichment_plan_skeleton(ctx)
    assert any("UNREVIEWABLE" in r for r in plan.risks)
    assert any("Sparse-source" in plan.reasoning for _ in [None])


def test_skeleton_no_unreviewable_when_anchored():
    ctx = _ctx(mdm_pct=0.5, has_baseline=False, n_queries=1)
    plan = build_enrichment_plan_skeleton(ctx)
    assert not any("UNREVIEWABLE" in r for r in plan.risks)


# ─── Markdown banner for unreviewable tables ─────────────────


def test_markdown_renders_loud_banner_for_unreviewable():
    ctx = _ctx(mdm_pct=0.0, has_baseline=False, n_queries=1)
    plan = build_enrichment_plan_skeleton(ctx)
    md = format_enrichment_plan_markdown(plan, ctx)
    assert "⚠ UNREVIEWABLE" in md
    assert "no semantic anchors" in md
    # Banner should appear BEFORE the authoring badge.
    banner_idx = md.find("UNREVIEWABLE")
    badge_idx = md.find("Authored by")
    assert banner_idx < badge_idx


def test_markdown_no_banner_when_anchored():
    ctx = _ctx(mdm_pct=0.5, has_baseline=True, n_queries=5)
    plan = build_enrichment_plan_skeleton(ctx)
    md = format_enrichment_plan_markdown(plan, ctx)
    assert "UNREVIEWABLE" not in md


# ─── Placeholder detection ───────────────────────────────────


def test_placeholder_detection_triggers_at_50pct():
    plan = EnrichmentPlan(
        table_name="t",
        proposed_dimensions=[
            {"name": "?", "type": "string", "source_column": "?"},
            {"name": "?", "type": "string", "source_column": "?"},
            {"name": "real_col", "type": "string", "source_column": "real_col"},
        ],
        proposed_measures=[],
        complexity="simple",
        reasoning="r",
    )
    # 2 of 3 are placeholders → >50% → triggers
    assert _has_excessive_placeholders(plan) is True


def test_placeholder_detection_passes_when_mostly_real():
    plan = EnrichmentPlan(
        table_name="t",
        proposed_dimensions=[
            {"name": "real_a", "type": "string", "source_column": "real_a"},
            {"name": "real_b", "type": "string", "source_column": "real_b"},
            {"name": "?", "type": "string", "source_column": "?"},
        ],
        proposed_measures=[],
        complexity="simple",
        reasoning="r",
    )
    assert _has_excessive_placeholders(plan) is False


def test_placeholder_recognizes_null_and_TBD_tokens():
    plan = EnrichmentPlan(
        table_name="t",
        proposed_dimensions=[
            {"name": None, "type": "string", "source_column": "x"},
            {"name": "TBD", "type": "string", "source_column": "y"},
            {"name": "real", "type": "string", "source_column": "z"},
        ],
        proposed_measures=[],
        complexity="simple",
        reasoning="r",
    )
    # 2 of 3 placeholders → triggers
    assert _has_excessive_placeholders(plan) is True


# ─── LLM fallback when placeholder detection trips ───────────


def test_llm_placeholder_output_falls_back_to_skeleton():
    """When the LLM emits a placeholder-heavy plan (sparse context
    confused it), we fall back to the skeleton instead of serving
    garbage to the human."""
    from lumi.sql_to_context import parse_sqls

    ctx = _ctx(mdm_pct=0.5, columns=["a", "b"], n_queries=2)
    fps = parse_sqls(["SELECT a FROM t"])
    bad_llm_plan = EnrichmentPlan(
        table_name="t",
        proposed_dimensions=[
            {"name": "?", "type": "?", "source_column": "?"},
            {"name": "?", "type": "?", "source_column": "?"},
        ],
        proposed_measures=[],
        complexity="simple",
        reasoning="r",
    )
    with patch("lumi.plan_builder._invoke_plan_agent", return_value=bad_llm_plan):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True,
        )
    # Should have fallen back; authoring stamps reason.
    assert plan.authoring["mode"] == "skeleton"
    assert "placeholder" in (plan.authoring.get("reason") or "").lower()
