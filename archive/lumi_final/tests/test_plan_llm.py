"""Plan-stage LLM authoring tests.

Verifies the with_llm=True path:
  - falls back to skeleton when LLM unavailable / errors
  - falls back to skeleton when LLM returns empty proposals
  - preserves token estimates and fields_to_enrich from skeleton
  - prompt assembly includes narrative + grounding sections
"""

from __future__ import annotations

from unittest.mock import patch

from lumi.plan_builder import (
    _build_plan_prompt,
    build_enrichment_plan,
    build_enrichment_plan_skeleton,
)
from lumi.schemas import EnrichmentPlan, TableContext
from lumi.sql_to_context import parse_sqls


def _ctx() -> TableContext:
    return TableContext(
        table_name="cornerstone_metrics",
        columns_referenced=["bus_seg", "billed_business", "rpt_dt"],
        aggregations=[
            {"function": "SUM", "column": "billed_business",
             "alias": "total", "distinct": False, "outer_expr": ""},
        ],
        case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[],
        joins_involving_this=[],
        filters_on_this=[
            {"column": "bus_seg", "operator": "=",
             "value": "'Consumer'", "is_structural": False},
        ],
        date_functions=[],
        mdm_columns=[
            {
                "name": "billed_business", "type": "NUMERIC",
                "business_name": "Billed Business",
                "description": "Total billed business volume in USD",
            },
        ],
        mdm_table_description="Daily metrics from cornerstone",
        mdm_coverage_pct=0.5,
        existing_view_lkml=None,
        queries_using_this=["Q01", "Q02"],
    )


# ─── Skeleton (deterministic) ────────────────────────────────


def test_skeleton_path_works_without_kwargs():
    """Existing call shape still works — back-compat preserved."""
    ctx = _ctx()
    plan = build_enrichment_plan_skeleton(ctx)
    assert plan.table_name == "cornerstone_metrics"
    assert plan.proposed_measures
    assert plan.reasoning  # deterministic, but non-empty


def test_with_llm_false_returns_skeleton():
    ctx = _ctx()
    plan = build_enrichment_plan(ctx, with_llm=False)
    skeleton = build_enrichment_plan_skeleton(ctx)
    # Same content shape (reasoning may differ if we ever change defaults
    # but proposals should be identical).
    assert plan.proposed_measures == skeleton.proposed_measures
    assert plan.proposed_dimensions == skeleton.proposed_dimensions


def test_with_llm_true_no_fingerprints_falls_back():
    """LLM path needs all_fingerprints — without them, skeleton returned."""
    ctx = _ctx()
    plan = build_enrichment_plan(ctx, with_llm=True, all_fingerprints=None)
    skeleton = build_enrichment_plan_skeleton(ctx)
    assert plan.proposed_measures == skeleton.proposed_measures


# ─── LLM path (mocked) ───────────────────────────────────────


def test_llm_returns_refined_plan_used():
    """When the LLM returns a valid EnrichmentPlan, we use it."""
    ctx = _ctx()
    fps = parse_sqls([
        "SELECT SUM(billed_business) AS total FROM cornerstone_metrics "
        "WHERE bus_seg = 'Consumer'",
    ])

    refined = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {
                "name": "bus_seg", "type": "string",
                "source_column": "bus_seg",
                "description_summary": "LLM-refined description grounded in MDM",
            },
        ],
        proposed_measures=[
            {
                "name": "total_billed_business", "type": "sum",
                "source_column": "billed_business", "value_format_name": "usd",
                "description_summary": "Sum of billed business — LLM authored",
            },
        ],
        complexity="simple",
        reasoning=(
            "This is the cornerstone daily metrics fact table — billed_business "
            "is the key revenue measure, segmented by Consumer/Commercial/GNS. "
            "Plan adds value_format and richer descriptions."
        ),
        risks=["No primary_key in baseline — need to pick one"],
        questions_for_reviewer=[],
        estimated_input_tokens=4000,
        estimated_output_tokens=1500,
    )

    with patch("lumi.plan_builder._invoke_plan_agent", return_value=refined):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    assert "LLM-refined" in plan.proposed_dimensions[0]["description_summary"]
    assert "cornerstone daily metrics" in plan.reasoning


def test_llm_error_falls_back_to_skeleton():
    """ImportError or exception during LLM invocation → skeleton."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT a FROM cornerstone_metrics"])
    with patch(
        "lumi.plan_builder._invoke_plan_agent",
        side_effect=RuntimeError("Vertex unreachable"),
    ):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    skeleton = build_enrichment_plan_skeleton(ctx)
    assert plan.reasoning == skeleton.reasoning


def test_llm_empty_proposals_falls_back():
    """If the LLM returns an EnrichmentPlan with NO dims AND NO measures,
    we fall back to the skeleton — defensive guard against bad output."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT a FROM cornerstone_metrics"])
    bad = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[],
        proposed_measures=[],
        reasoning="Empty plan oh no",
        complexity="simple",
    )
    with patch("lumi.plan_builder._invoke_plan_agent", return_value=bad):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    skeleton = build_enrichment_plan_skeleton(ctx)
    assert plan.proposed_measures == skeleton.proposed_measures
    assert plan.reasoning == skeleton.reasoning


def test_llm_returns_none_falls_back():
    """When the LLM agent returns None (parse fail, etc.) → skeleton."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT a FROM cornerstone_metrics"])
    with patch("lumi.plan_builder._invoke_plan_agent", return_value=None):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    skeleton = build_enrichment_plan_skeleton(ctx)
    assert plan.reasoning == skeleton.reasoning


def test_llm_preserves_skeleton_token_estimates():
    """Token estimates are deterministic; we don't let the LLM rewrite
    them (it tends to underestimate)."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT a FROM cornerstone_metrics"])
    refined = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[{"name": "a", "type": "string", "source_column": "a"}],
        proposed_measures=[{"name": "b", "type": "sum", "source_column": "b"}],
        reasoning="r",
        complexity="simple",
        estimated_input_tokens=0,    # LLM lazily put 0
        estimated_output_tokens=0,
    )
    with patch("lumi.plan_builder._invoke_plan_agent", return_value=refined):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    skeleton = build_enrichment_plan_skeleton(ctx)
    assert plan.estimated_input_tokens == skeleton.estimated_input_tokens
    assert plan.estimated_output_tokens == skeleton.estimated_output_tokens


# ─── Prompt assembly ─────────────────────────────────────────


def test_parse_plan_handles_markdown_codefence():
    """Gemini sometimes wraps in ```json ... ``` despite mime hint."""
    from lumi.plan_builder import _parse_plan_response
    payload = (
        "```json\n"
        '{"table_name": "t", "reasoning": "r", "complexity": "simple",\n'
        ' "proposed_dimensions": [{"name": "a", "type": "string", "source_column": "a"}],\n'
        ' "proposed_measures": []}\n'
        "```"
    )
    plan = _parse_plan_response(payload, "t")
    assert plan is not None
    assert plan.table_name == "t"


def test_parse_plan_handles_trailing_commas():
    """Trailing commas are JSON-invalid but common LLM output."""
    from lumi.plan_builder import _parse_plan_response
    payload = (
        '{"table_name": "t", "reasoning": "r", "complexity": "simple",\n'
        ' "proposed_dimensions": [\n'
        '   {"name": "a", "type": "string", "source_column": "a"},\n'  # trailing
        ' ],\n'
        ' "proposed_measures": [],\n'    # trailing comma
        '}'
    )
    plan = _parse_plan_response(payload, "t")
    assert plan is not None
    assert plan.proposed_dimensions[0]["name"] == "a"


def test_parse_plan_handles_prose_before_json():
    """Sometimes Gemini puts text before the JSON object."""
    from lumi.plan_builder import _parse_plan_response
    payload = (
        "Here is the plan you requested:\n\n"
        '{"table_name": "t", "reasoning": "r", "complexity": "simple",\n'
        ' "proposed_dimensions": [{"name": "a", "type": "string", "source_column": "a"}],\n'
        ' "proposed_measures": []}\n'
        "Hope this helps!"
    )
    plan = _parse_plan_response(payload, "t")
    assert plan is not None
    assert plan.table_name == "t"


def test_parse_plan_returns_none_on_unrecoverable():
    """When all repair strategies fail, return None so the caller falls
    back to the skeleton."""
    from lumi.plan_builder import _parse_plan_response
    plan = _parse_plan_response("this is not json at all", "t")
    assert plan is None


# ─── Self-repair loop ────────────────────────────────────────


def test_self_repair_retries_on_blocking_critique():
    """When the critic returns a blocking issue, the planner re-prompts
    once with the critique appended and uses the second-round plan."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT bus_seg FROM cornerstone_metrics"])

    bad_plan = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {"name": "?", "type": "string", "source_column": "bus_seg"},
        ],
        proposed_measures=[
            {"name": "total_bb", "type": "sum", "source_column": "billed_business",
             "value_format_name": "usd"},
        ],
        complexity="simple",
        reasoning="Plan adds dims for cornerstone metrics on cardmember entity. "
                  "Two new measures plus PK preservation.",
    )
    good_plan = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {"name": "business_segment", "type": "string",
             "source_column": "bus_seg",
             "description_summary": "Cardmember business segment"},
        ],
        proposed_measures=[
            {"name": "total_bb", "type": "sum", "source_column": "billed_business",
             "value_format_name": "usd"},
        ],
        complexity="simple",
        reasoning="Plan adds dims for cornerstone metrics on cardmember entity. "
                  "Two new measures plus PK preservation.",
    )

    call_log: list[str] = []

    def fake_invoke(prompt, table, config):
        call_log.append(prompt)
        return bad_plan if len(call_log) == 1 else good_plan

    with patch("lumi.plan_builder._invoke_plan_agent", side_effect=fake_invoke), \
         patch("lumi.critic._llm_critique", return_value=None):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=True,
        )

    # Two rounds happened — the second prompt has the critique addendum.
    assert len(call_log) >= 2
    assert "Critic feedback" in call_log[1]
    # We ended up with the good plan (no placeholder).
    assert plan.proposed_dimensions[0]["name"] == "business_segment"


def test_self_repair_caps_at_max_rounds():
    """If the model never resolves block issues, we stop at max_rounds + 1
    invocations and return whatever the last refined plan was."""
    from lumi.config import LumiConfig
    cfg = LumiConfig()
    cfg.plan_repair_max_rounds = 1  # 1 retry → 2 total invocations max

    ctx = _ctx()
    fps = parse_sqls(["SELECT bus_seg FROM cornerstone_metrics"])

    bad_plan = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {"name": "?", "type": "string", "source_column": "bus_seg"},
        ],
        proposed_measures=[
            {"name": "x", "type": "number", "source_column": "billed_business"},
        ],
        complexity="simple",
        reasoning="Plan adds dims. Cardmember entity context goes here too.",
    )
    call_count = [0]

    def fake_invoke(prompt, table, config):
        call_count[0] += 1
        return bad_plan

    with patch("lumi.plan_builder._invoke_plan_agent", side_effect=fake_invoke), \
         patch("lumi.critic._llm_critique", return_value=None):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=True, config=cfg,
        )

    # 1 initial + 1 retry = 2 total
    assert call_count[0] == 2
    # Plan still has the placeholder — guard kicks in only when first
    # candidate triggers excessive-placeholder fallback. Here we have just
    # one placeholder out of two, under the threshold, so the plan is used.
    assert plan is not None


def test_critic_disabled_no_repair_loop():
    """with_critic=False bypasses the loop — single planner invocation."""
    ctx = _ctx()
    fps = parse_sqls(["SELECT bus_seg FROM cornerstone_metrics"])
    refined = EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {"name": "bus_seg", "type": "string", "source_column": "bus_seg"},
        ],
        proposed_measures=[
            {"name": "total_bb", "type": "sum", "source_column": "billed_business",
             "value_format_name": "usd"},
        ],
        complexity="simple",
        reasoning="Cardmember table at point-in-time grain. Plan adds bus_seg + total_bb.",
    )
    call_count = [0]

    def fake_invoke(prompt, table, config):
        call_count[0] += 1
        return refined

    with patch("lumi.plan_builder._invoke_plan_agent", side_effect=fake_invoke):
        build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=False,
        )
    assert call_count[0] == 1


# ─── Prompt assembly ─────────────────────────────────────────


def test_plan_prompt_includes_narrative_and_grounding():
    """The prompt must include the table narrative + grounding signal
    sections + the deterministic skeleton + authoring rules."""
    from lumi.grounding import build_grounding_signals
    from lumi.narrative import build_table_narrative

    ctx = _ctx()
    fps = parse_sqls(["SELECT a FROM cornerstone_metrics"])
    skeleton = build_enrichment_plan_skeleton(ctx)
    grounding = build_grounding_signals(ctx, fps, {})
    narrative = build_table_narrative(ctx, all_fingerprints=fps)

    prompt = _build_plan_prompt(ctx, skeleton, narrative, grounding)
    # Major sections present
    assert "Planning task" in prompt
    assert "Table narrative" in prompt
    assert "Grounding signals" in prompt
    assert "Deterministic skeleton" in prompt
    assert "Authoring rules" in prompt
    # The skeleton's content surfaces.
    assert "billed_business" in prompt
    # Authoring instructions present.
    assert "PRESERVE" in prompt
    assert "REFINE descriptions" in prompt
