"""Session 3 tests for ``lumi.enrich``.

The LLM call itself is mocked (we monkeypatch
``lumi.enrich._invoke_enrichment_agent`` to return a fixture
:class:`EnrichedOutput` parsed from JSON on disk). What we verify here:

  - Prompt assembly interpolates every placeholder, drops the SKILL excerpt
    in at the bottom, and includes the approved plan as a scope contract.
  - Fixture LookML for cornerstone_metrics passes
    :func:`lumi.guardrails.check_enrichment` with no blocking failures and
    contains the canonical structural pieces (primary_key, dimension_group).
  - Fixture LookML for risk_pers_acct_history (Q9) produces a derived_table
    view with TRIUMPH + business-unit structural filters baked in.
  - Patterns from SKILL.md sections 1-4 land in the assembled prompt.

The fixtures live under ``tests/fixtures/llm_responses/`` and are realistic
LookML strings (each parses with ``lkml.load``).
"""

from __future__ import annotations

import json
from pathlib import Path

import lkml
import pytest

from lumi import enrich as enrich_mod
from lumi.config import LumiConfig
from lumi.enrich import (
    _load_skill_excerpt,
    build_enrichment_prompt,
    enrich_table,
)
from lumi.guardrails import check_enrichment
from lumi.schemas import EnrichedOutput, EnrichmentPlan, TableContext


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "llm_responses"


# ─── Helpers ────────────────────────────────────────────────────


def _load_fixture(table_name: str) -> EnrichedOutput:
    raw = json.loads(
        (FIXTURE_DIR / f"enrich_{table_name}.json").read_text(encoding="utf-8")
    )
    return EnrichedOutput.model_validate(raw)


def _patch_invoker(monkeypatch: pytest.MonkeyPatch, fixture: EnrichedOutput) -> list[str]:
    """Replace the LLM call with one that returns ``fixture``.

    Returns a list that the test can inspect to capture the prompt that the
    (fake) LLM would have seen.
    """
    captured: list[str] = []

    def fake_invoke(agent, prompt, table_name):  # type: ignore[no-untyped-def]
        captured.append(prompt)
        return fixture

    monkeypatch.setattr(enrich_mod, "_invoke_enrichment_agent", fake_invoke)
    return captured


# ─── Context fixtures ───────────────────────────────────────────


@pytest.fixture
def cornerstone_context() -> TableContext:
    """Q1-style cornerstone_metrics context — single table, simple aggregations."""
    return TableContext(
        table_name="cornerstone_metrics",
        columns_referenced=[
            "bus_seg",
            "data_source",
            "rpt_dt",
            "billed_business",
            "new_accounts_acquired",
            "generation",
            "sub_product_group",
            "fico_band",
            "accounts_in_force",
        ],
        aggregations=[
            {
                "function": "SUM",
                "column": "billed_business",
                "alias": None,
                "distinct": False,
                "outer_expr": "SUM(billed_business)",
            },
            {
                "function": "SUM",
                "column": "new_accounts_acquired",
                "alias": None,
                "distinct": False,
                "outer_expr": "SUM(new_accounts_acquired)",
            },
            {
                "function": "AVG",
                "column": "billed_business",
                "alias": "avg_bb",
                "distinct": False,
                "outer_expr": "AVG(billed_business)",
            },
        ],
        case_whens=[],
        ctes_referencing_this=[],
        joins_involving_this=[],
        filters_on_this=[
            {
                "column": "bus_seg",
                "operator": "=",
                "value": "'Consumer'",
                "is_structural": False,
            },
            {
                "column": "data_source",
                "operator": "=",
                "value": "'cornerstone'",
                "is_structural": False,
            },
        ],
        date_functions=[
            {"column": "rpt_dt", "function": "YEAR"},
            {"column": "rpt_dt", "function": "MONTH"},
            {"column": "rpt_dt", "function": "DATE_CAST"},
        ],
        mdm_columns=[
            {
                "name": "billed_business",
                "type": "NUMBER",
                "business_name": "Billed Business",
                "description": "Total billed business in USD",
            },
            {
                "name": "bus_seg",
                "type": "STRING",
                "business_name": "Business Segment",
                "description": "Consumer / Commercial / GNS",
            },
        ],
        mdm_table_description="Daily aggregated cornerstone metrics from the Cornerstone source.",
        mdm_coverage_pct=0.83,
        existing_view_lkml=(
            "view: cornerstone_metrics {\n"
            "  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;\n"
            "  dimension: bus_seg { type: string sql: ${TABLE}.bus_seg ;; }\n"
            "}\n"
        ),
        queries_using_this=["Q01", "Q02", "Q03", "Q04"],
    )


@pytest.fixture
def cornerstone_plan() -> EnrichmentPlan:
    return EnrichmentPlan(
        table_name="cornerstone_metrics",
        proposed_dimensions=[
            {
                "name": "business_segment",
                "type": "string",
                "source_column": "bus_seg",
                "description_summary": "Card portfolio segment classification",
            },
            {
                "name": "data_source",
                "type": "string",
                "source_column": "data_source",
                "description_summary": "Source system identifier",
            },
            {
                "name": "generation",
                "type": "string",
                "source_column": "generation",
                "description_summary": "Cardmember generational cohort",
            },
        ],
        proposed_dimension_groups=[
            {"name": "report", "source_column": "rpt_dt"},
        ],
        proposed_measures=[
            {
                "name": "total_billed_business",
                "type": "sum",
                "source_column": "billed_business",
                "description_summary": "Sum of billed business in USD",
            },
            {
                "name": "total_new_accounts_acquired",
                "type": "sum",
                "source_column": "new_accounts_acquired",
                "description_summary": "Count of new accounts opened",
            },
        ],
        proposed_filter_catalog_count=3,
        proposed_metric_catalog_count=3,
        proposed_nl_question_count=4,
        complexity="simple",
        reasoning=(
            "Single-table aggregations across four input queries with a "
            "shared rpt_dt date column and consistent data_source default."
        ),
    )


@pytest.fixture
def risk_history_context() -> TableContext:
    """Q9-style risk_pers_acct_history with a CTE wrapping it (TRIUMPH + bus 1,2)."""
    return TableContext(
        table_name="risk_pers_acct_history",
        columns_referenced=[
            "acct_cust_xref_id",
            "acct_bal_age_mth01_cd",
            "acct_bill_bal_mth01_amt",
            "acct_wrt_off_am",
            "acct_rcvr_mo_01_am",
            "acct_bus_unit_cd",
            "acct_as_of_dt",
            "acct_srce_sys_cd",
        ],
        aggregations=[
            {"function": "SUM", "column": "acct_bill_bal_mth01_amt", "alias": "total_ar"},
            {"function": "SUM", "column": "acct_wrt_off_am", "alias": "total_writeoffs"},
            {"function": "SUM", "column": "acct_rcvr_mo_01_am", "alias": "total_recoveries"},
        ],
        case_whens=[
            {
                "alias": "age_bucket",
                "source_column": "acct_bal_age_mth01_cd",
                "sql": "CASE WHEN ... END",
                "mapped_values": [
                    {"when": "IN ('00','01')", "then": "Current"},
                    {"when": "= '99'", "then": "Written Off"},
                ],
            }
        ],
        ctes_referencing_this=[
            {
                "alias": "rpah",
                "structural_filters": [
                    {
                        "column": "acct_srce_sys_cd",
                        "operator": "=",
                        "value": "'TRIUMPH'",
                        "is_structural": True,
                    },
                    {
                        "column": "acct_bus_unit_cd",
                        "operator": "IN",
                        "value": "(1, 2)",
                        "is_structural": True,
                    },
                ],
                "sql": "SELECT ... FROM risk_pers_acct_history WHERE ...",
                "source_tables": ["risk_pers_acct_history"],
                "cte_dependencies": [],
            }
        ],
        joins_involving_this=[],
        filters_on_this=[],
        date_functions=[
            {"column": "acct_as_of_dt", "function": "DATE_CAST"},
        ],
        mdm_columns=[],
        mdm_table_description="Risk personal account history snapshot table.",
        mdm_coverage_pct=0.7,
        queries_using_this=["Q09"],
    )


@pytest.fixture
def risk_history_plan() -> EnrichmentPlan:
    return EnrichmentPlan(
        table_name="risk_pers_acct_history",
        proposed_dimensions=[
            {
                "name": "acct_cust_xref_id",
                "type": "string",
                "source_column": "acct_cust_xref_id",
                "description_summary": "Account-customer cross-reference ID",
            },
        ],
        proposed_dimension_groups=[
            {"name": "acct_as_of", "source_column": "acct_as_of_dt"},
        ],
        proposed_measures=[
            {"name": "total_ar", "type": "sum", "source_column": "acct_bill_bal_mth01_amt",
             "description_summary": "Sum of month-one AR balance"},
            {"name": "total_writeoffs", "type": "sum", "source_column": "acct_wrt_off_am",
             "description_summary": "Sum of write-offs"},
            {"name": "total_recoveries", "type": "sum", "source_column": "acct_rcvr_mo_01_am",
             "description_summary": "Sum of recoveries"},
        ],
        proposed_derived_tables=[
            {
                "name": "risk_acct_triumph_consumer",
                "source_cte": "rpah",
                "structural_filters": [
                    {"column": "acct_srce_sys_cd", "value": "'TRIUMPH'"},
                    {"column": "acct_bus_unit_cd", "value": "(1, 2)"},
                ],
                "primary_key": "acct_cust_xref_id",
            }
        ],
        proposed_filter_catalog_count=1,
        proposed_metric_catalog_count=3,
        proposed_nl_question_count=3,
        complexity="complex",
        reasoning=(
            "Q9 wraps this table in a TRIUMPH consumer CTE with two structural "
            "filters and derives a delinquency bucket via CASE WHEN; build a "
            "derived_table view to bake in scope."
        ),
        risks=["primary_key on derived view inherits from source — verify uniqueness"],
    )


# ─── Required tests (per LUMI_BUILD_PLAN.md Session 3) ──────────


def test_enrich_simple_table(
    monkeypatch: pytest.MonkeyPatch,
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """Q1's cornerstone_metrics → valid LookML with primary_key + dim_groups."""
    fixture = _load_fixture("cornerstone_metrics")
    captured = _patch_invoker(monkeypatch, fixture)

    out = enrich_table(cornerstone_context, cornerstone_plan)

    # Returned the fixture verbatim.
    assert out is fixture
    assert len(captured) == 1, "expected exactly one LLM invocation"

    # The view parses with lkml.
    parsed = lkml.load(out.view_lkml)
    views = parsed["views"]
    assert len(views) == 1
    view = views[0]
    assert view["name"] == "cornerstone_metrics"
    assert view.get("sql_table_name") == "`axp-lumi.dw.cornerstone_metrics`"

    # Exactly one primary_key dimension.
    pks = [d for d in view["dimensions"] if d.get("primary_key") == "yes"]
    assert len(pks) == 1, f"expected exactly one primary_key, got {len(pks)}"

    # The date column lands in a dimension_group, not a plain dimension.
    dim_groups = view.get("dimension_groups", [])
    assert any(dg.get("type") == "time" for dg in dim_groups)
    assert any("rpt_dt" in dg.get("sql", "") for dg in dim_groups)
    assert all(
        "rpt_dt" not in d.get("sql", "")
        or d.get("primary_key") == "yes"  # PK includes rpt_dt in CONCAT — allowed
        for d in view["dimensions"]
    )

    # Guardrail: enrichment must not produce blocking failures on this fixture.
    gate = check_enrichment("cornerstone_metrics", out, cornerstone_context)
    assert gate.status != "fail", gate.blocking_failures


def test_enrich_cte_produces_derived_table(
    monkeypatch: pytest.MonkeyPatch,
    risk_history_context: TableContext,
    risk_history_plan: EnrichmentPlan,
) -> None:
    """Q9 → derived_table view with TRIUMPH + bus_unit_cd filters baked in."""
    fixture = _load_fixture("risk_pers_acct_history")
    _patch_invoker(monkeypatch, fixture)

    out = enrich_table(risk_history_context, risk_history_plan)

    # One derived_table view emitted (1 CTE in scope).
    assert len(out.derived_table_views) == 1
    derived = out.derived_table_views[0]

    # Parses with lkml and is genuinely a derived_table view.
    parsed = lkml.load(derived)
    views = parsed["views"]
    assert len(views) == 1
    derived_view = views[0]
    assert "derived_table" in derived_view
    assert derived_view["name"] == "risk_acct_triumph_consumer"
    assert derived_view.get("sql_table_name") is None  # mutually exclusive

    # Structural filters are baked into the derived_table SQL.
    dt_sql = derived_view["derived_table"]["sql"]
    assert "TRIUMPH" in dt_sql
    assert "acct_srce_sys_cd" in dt_sql
    assert "acct_bus_unit_cd IN (1, 2)" in dt_sql

    # The derived view has its own primary_key.
    pks = [d for d in derived_view["dimensions"] if d.get("primary_key") == "yes"]
    assert len(pks) == 1

    # And carries the CASE WHEN derived dimension with order_by_field.
    case_dims = [
        d for d in derived_view["dimensions"]
        if "CASE" in d.get("sql", "") and not d.get("hidden")
    ]
    assert case_dims, "expected a visible CASE WHEN derived dimension"
    assert any(d.get("order_by_field") for d in case_dims)

    # Guardrail clean.
    gate = check_enrichment("risk_pers_acct_history", out, risk_history_context)
    assert gate.status != "fail", gate.blocking_failures


def test_enrich_skill_injected(
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """The assembled prompt includes the SKILL.md sections 1-4 patterns."""
    prompt = build_enrichment_prompt(cornerstone_context, cornerstone_plan)

    # Section anchors land verbatim.
    assert "1. SQL Pattern → LookML Pattern Map" in prompt
    assert "2. Required Attributes Checklist" in prompt
    assert "3. The primary_key and Symmetric Aggregates" in prompt
    assert "4. Relationship Inference from SQL Patterns" in prompt

    # Pattern names from sections 1-4 are present.
    pattern_signatures = [
        "type: count_distinct",          # COUNT(DISTINCT) → measure pattern
        "dimension_group",                # date pattern
        "convert_tz: no",                 # BigQuery date rule
        "primary_key: yes",               # PK rule
        "symmetric aggregate",            # PK rationale
        "many_to_one",                    # relationship inference
    ]
    for sig in pattern_signatures:
        assert sig in prompt, f"missing SKILL pattern in prompt: {sig!r}"

    # The approved plan is included as a scope contract.
    assert "Approved enrichment plan" in prompt
    assert "total_billed_business" in prompt  # measure name from the plan

    # Compressed sections (6, 7) bring the anti-patterns headlines.
    assert "Anti-Patterns" in prompt or "anti-patterns" in prompt.lower()


# ─── Extra coverage on the prompt-assembly seams ────────────────


def test_prompt_interpolates_table_specific_fields(
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """Every ``{placeholder}`` in the template is filled — no raw braces left."""
    prompt = build_enrichment_prompt(cornerstone_context, cornerstone_plan)

    assert "{table_name}" not in prompt
    assert "{table_mdm_description}" not in prompt
    assert "{selected_mdm_columns}" not in prompt
    assert "{fingerprint_summary}" not in prompt
    assert "{ecosystem_brief}" not in prompt
    assert "{table_specific_learnings}" not in prompt
    assert "{existing_view_lkml}" not in prompt
    assert "{bq_project}" not in prompt
    assert "{bq_dataset}" not in prompt

    # Real values land in the right places.
    assert "cornerstone_metrics" in prompt
    assert "axp-lumi" in prompt
    cfg = LumiConfig()
    assert cfg.bq_dataset in prompt


def test_prompt_renders_ecosystem_for_joined_table(
    risk_history_context: TableContext,
    risk_history_plan: EnrichmentPlan,
) -> None:
    """The ecosystem brief surfaces wrapping CTE aliases."""
    prompt = build_enrichment_prompt(risk_history_context, risk_history_plan)
    assert "Wrapped by CTE alias" in prompt
    assert "rpah" in prompt


def test_skill_excerpt_includes_refinement_section() -> None:
    """Section 5 (Refinements / additive merge) is REQUIRED.

    CLAUDE.md rule 6 ("merge into existing LookML — never regenerate. Additive
    only.") binds the model's behaviour to the refinement pattern. Sections 8
    (model file — generated by code, not Gemini) and 9 (meta) stay out.
    """
    excerpt = _load_skill_excerpt()
    # Section 5 IS in (regression guard against accidental removal).
    assert "5. Refinements" in excerpt
    assert "view: +" in excerpt  # the canonical refinement syntax
    # Sections 8 and 9 stay out.
    assert "8. Model File Structure" not in excerpt
    assert "9. How This Skill Gets Used" not in excerpt
    # Runtime patterns are present.
    assert "primary_key" in excerpt
    assert "dimension_group" in excerpt


def test_fixtures_parse_with_lkml() -> None:
    """Every LookML string in the fixture set parses cleanly with ``lkml.load``.

    Gold-standard fixtures must round-trip through the same parser the
    guardrail uses. If a fixture stops parsing, every downstream assertion
    that depends on its structure becomes meaningless.
    """
    for table in ("cornerstone_metrics", "risk_pers_acct_history"):
        fixture = _load_fixture(table)
        # Base view.
        parsed = lkml.load(fixture.view_lkml)
        assert parsed.get("views"), f"{table}: base view did not yield any views"
        # Derived table views.
        for i, dtv in enumerate(fixture.derived_table_views):
            dt_parsed = lkml.load(dtv)
            assert dt_parsed.get("views"), f"{table}: derived view {i} empty"
            assert dt_parsed["views"][0].get("derived_table"), (
                f"{table}: derived view {i} missing derived_table block"
            )
        # Explore (needs a connection wrapper for lkml).
        if fixture.explore_lkml:
            wrapped = f'connection: "test"\n{fixture.explore_lkml}'
            ex_parsed = lkml.load(wrapped)
            assert ex_parsed.get("explores"), f"{table}: explore did not parse"


def test_select_relevant_mdm_columns_caps_wide_tables() -> None:
    """Wide MDM tables should be capped, narrow tables should pass through."""
    from lumi.enrich import _select_relevant_mdm_columns

    # Narrow context — pass through.
    ctx_narrow = TableContext(
        table_name="t",
        columns_referenced=["a", "b", "c"],
        aggregations=[],
        case_whens=[],
        ctes_referencing_this=[],
        joins_involving_this=[],
        filters_on_this=[],
        date_functions=[],
        mdm_columns=[],
        queries_using_this=[],
    )
    assert _select_relevant_mdm_columns(ctx_narrow) == ["a", "b", "c"]

    # Wide context — capped.
    ctx_wide = TableContext(
        table_name="t",
        columns_referenced=[f"col_{i}" for i in range(80)],
        aggregations=[],
        case_whens=[],
        ctes_referencing_this=[],
        joins_involving_this=[],
        filters_on_this=[],
        date_functions=[],
        mdm_columns=[],
        queries_using_this=[],
    )
    out = _select_relevant_mdm_columns(ctx_wide, cap=50)
    assert len(out) == 50
    # Cap preserves original order — first 50 columns_referenced come through.
    assert out[0] == "col_0"
    assert out[-1] == "col_49"


def test_select_relevant_mdm_columns_unions_join_filter_agg_columns() -> None:
    """The union picks up join keys, filter cols, agg cols, and case-when sources."""
    from lumi.enrich import _select_relevant_mdm_columns

    ctx = TableContext(
        table_name="t",
        columns_referenced=["a"],
        aggregations=[{"function": "SUM", "column": "agg_col"}],
        case_whens=[{"alias": "x", "source_column": "case_col"}],
        ctes_referencing_this=[],
        joins_involving_this=[
            {"left_key": "join_left", "right_key": "join_right", "order": 0}
        ],
        filters_on_this=[{"column": "filter_col", "operator": "="}],
        date_functions=[{"column": "date_col", "function": "YEAR"}],
        mdm_columns=[],
        queries_using_this=[],
    )
    out = set(_select_relevant_mdm_columns(ctx))
    assert {"a", "agg_col", "case_col", "join_left", "join_right",
            "filter_col", "date_col"} <= out


def test_enrich_self_repair_on_blocking_failure(
    monkeypatch: pytest.MonkeyPatch,
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """If the first attempt has a blocking failure, retry once with a repair appendix.

    Builds a deliberately-broken fixture (no sql_table_name, plain dim on the
    date column) so the first attempt FAILS the guardrail. The second attempt
    (still mocked) returns the real fixture, which passes. Verifies:

      - Two LLM invocations happen
      - The second prompt contains the SELF-REPAIR appendix
      - The returned output is the second (good) one
    """
    good_fixture = _load_fixture("cornerstone_metrics")
    # Build a broken view that will fail check_enrichment with at least one
    # blocking failure (lkml will still parse it — it's just incomplete LookML).
    broken_view = "view: cornerstone_metrics { not_a_real_field: ;; }\n"
    broken_fixture = good_fixture.model_copy(update={"view_lkml": broken_view})

    captured: list[str] = []
    call_count = {"n": 0}

    def fake_invoke(agent, prompt, table_name):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        captured.append(prompt)
        return broken_fixture if call_count["n"] == 1 else good_fixture

    monkeypatch.setattr(enrich_mod, "_invoke_enrichment_agent", fake_invoke)

    out = enrich_table(cornerstone_context, cornerstone_plan, max_attempts=2)

    assert call_count["n"] == 2, "expected exactly two LLM invocations"
    assert "SELF-REPAIR" in captured[1], "second prompt must carry the repair appendix"
    # Final result is the recovered (good) one.
    assert out is good_fixture


def test_enrich_no_retry_on_first_attempt_pass(
    monkeypatch: pytest.MonkeyPatch,
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """When the first attempt is clean, no second invocation happens."""
    fixture = _load_fixture("cornerstone_metrics")
    captured = _patch_invoker(monkeypatch, fixture)

    out = enrich_table(cornerstone_context, cornerstone_plan, max_attempts=2)
    assert out is fixture
    assert len(captured) == 1, "clean first attempt must not trigger self-repair"


def test_build_enrich_agent_uses_temperature_zero(
    cornerstone_context: TableContext,
    cornerstone_plan: EnrichmentPlan,
) -> None:
    """Agent factory respects rule 4: temperature 0.0 on every LlmAgent."""
    from lumi.enrich import build_enrich_agent

    agent = build_enrich_agent(cornerstone_context, cornerstone_plan)
    cfg = agent.generate_content_config
    assert cfg is not None
    assert cfg.temperature == 0.0
    assert agent.output_schema is EnrichedOutput
    assert agent.name.startswith("enrich_")
