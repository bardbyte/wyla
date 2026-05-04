"""Session 4 — Validate (LoopAgent evaluator).

Tests are deterministic: no Vertex calls. The gap_fixer LlmAgent is
replaced with a mock when we exercise the LoopAgent. Coverage rules
themselves are pure-Python checks against synthetic enriched LookML.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any
import pytest

from lumi.guardrails import check_evaluation
from lumi.schemas import (
    CoverageReport,
    EnrichedOutput,
    GateResult,
    QueryCoverage,
)
from lumi.validate import (
    build_evaluator_loop,
    coverage_check,
    reconstruct_sql_check,
)


# ─── Synthetic SQLFingerprint (lightweight stand-in) ─────────────────


@dataclass
class FakeFP:
    """Minimal fingerprint shape matching SQLFingerprint's surface area
    used by coverage_check."""

    query_id: str
    tables: list[str] = field(default_factory=list)
    columns_referenced: list[str] = field(default_factory=list)
    aggregations: list[dict[str, Any]] = field(default_factory=list)
    case_whens: list[dict[str, Any]] = field(default_factory=list)
    ctes: list[dict[str, Any]] = field(default_factory=list)
    temp_tables: list[dict[str, Any]] = field(default_factory=list)
    joins: list[dict[str, Any]] = field(default_factory=list)
    filters: list[dict[str, Any]] = field(default_factory=list)


# ─── Synthetic LookML: Q1-shape coverage (full happy path) ───────────

_VIEW_Q1_FULL = """
view: cornerstone_metrics {
  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;

  dimension: rpt_dt_pk {
    primary_key: yes
    sql: ${TABLE}.rpt_dt ;;
  }

  dimension: bus_seg {
    type: string
    description: "Business segment for the cornerstone metric"
    sql: ${TABLE}.bus_seg ;;
  }

  dimension: data_source {
    type: string
    description: "Source system that produced the row"
    sql: ${TABLE}.data_source ;;
  }

  dimension_group: rpt_dt {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.rpt_dt ;;
  }

  measure: total_billed_business {
    type: sum
    description: "Total billed business amount"
    value_format_name: usd
    sql: ${TABLE}.billed_business ;;
  }
}
""".strip()


_EXPLORE_Q1 = """
explore: cornerstone_metrics {
  sql_always_where: ${cornerstone_metrics.data_source} = 'cornerstone' ;;
}
""".strip()


def _q1_fingerprint() -> FakeFP:
    return FakeFP(
        query_id="Q1",
        tables=["cornerstone_metrics"],
        columns_referenced=["billed_business", "bus_seg", "data_source", "rpt_dt"],
        aggregations=[
            {"function": "SUM", "column": "billed_business", "alias": None,
             "distinct": False, "outer_expr": ""},
        ],
        filters=[
            {"column": "bus_seg", "operator": "=", "value": "'Consumer'",
             "is_structural": False},
            {"column": "data_source", "operator": "=", "value": "'cornerstone'",
             "is_structural": True},
            {"column": "rpt_dt", "operator": "=", "value": "DATE('2025-01-01')",
             "is_structural": False},
        ],
    )


def _q1_enriched() -> dict[str, EnrichedOutput]:
    return {
        "cornerstone_metrics": EnrichedOutput(
            view_lkml=_VIEW_Q1_FULL,
            explore_lkml=_EXPLORE_Q1,
        )
    }


# ─── Synthetic LookML: Q9-shape with derived_table for CTE ───────────

_VIEW_RPAH_BASE = """
view: risk_pers_acct_history {
  sql_table_name: `axp-lumi.dw.risk_pers_acct_history` ;;

  dimension: acct_cust_xref_id {
    primary_key: yes
    sql: ${TABLE}.acct_cust_xref_id ;;
  }

  dimension: acct_bal_age_mth01_cd {
    description: "Bucket code for account balance age"
    sql: ${TABLE}.acct_bal_age_mth01_cd ;;
  }

  dimension: acct_srce_sys_cd {
    description: "Source system code feeding this row"
    sql: ${TABLE}.acct_srce_sys_cd ;;
  }

  dimension: acct_bus_unit_cd {
    description: "Business unit code"
    sql: ${TABLE}.acct_bus_unit_cd ;;
  }

  dimension_group: acct_as_of {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.acct_as_of_dt ;;
  }

  measure: total_ar {
    type: sum
    description: "Sum of accounts receivable balance"
    value_format_name: usd
    sql: ${TABLE}.acct_bill_bal_mth01_amt ;;
  }
}
""".strip()


_DERIVED_RPAH = """
view: rpah {
  derived_table: {
    sql:
      SELECT acct_cust_xref_id, acct_bill_bal_mth01_amt, acct_bus_unit_cd, acct_as_of_dt
      FROM `axp-lumi`.dw.risk_pers_acct_history
      WHERE acct_as_of_dt = DATE('2025-05-01')
        AND acct_srce_sys_cd = 'TRIUMPH'
        AND acct_bus_unit_cd IN (1, 2)
    ;;
  }

  dimension: acct_cust_xref_id {
    primary_key: yes
    sql: ${TABLE}.acct_cust_xref_id ;;
  }

  measure: total_ar {
    type: sum
    sql: ${TABLE}.acct_bill_bal_mth01_amt ;;
  }
}
""".strip()


def _q9like_fingerprint() -> FakeFP:
    """Synthetic CTE-driven query whose enriched output must include a
    derived_table view for the rpah CTE with structural filters baked.
    """
    return FakeFP(
        query_id="Q9",
        tables=["risk_pers_acct_history"],
        columns_referenced=[
            "acct_cust_xref_id",
            "acct_bal_age_mth01_cd",
            "acct_bill_bal_mth01_amt",
            "acct_bus_unit_cd",
            "acct_as_of_dt",
        ],
        aggregations=[
            {"function": "SUM", "column": "acct_bill_bal_mth01_amt",
             "alias": "total_ar", "distinct": False, "outer_expr": ""},
        ],
        filters=[],
        ctes=[
            {
                "alias": "rpah",
                "structural_filters": [
                    {"column": "acct_as_of_dt", "operator": "=",
                     "value": "DATE('2025-05-01')", "is_structural": True},
                    {"column": "acct_srce_sys_cd", "operator": "=",
                     "value": "'TRIUMPH'", "is_structural": True},
                ],
                "sql": "SELECT ... WHERE acct_srce_sys_cd = 'TRIUMPH'",
                "source_tables": ["risk_pers_acct_history"],
                "cte_dependencies": [],
            },
        ],
    )


def _q9like_enriched() -> dict[str, EnrichedOutput]:
    return {
        "risk_pers_acct_history": EnrichedOutput(
            view_lkml=_VIEW_RPAH_BASE,
            derived_table_views=[_DERIVED_RPAH],
            explore_lkml="explore: risk_pers_acct_history {}",
        )
    }


# ─── Tests: coverage_check ───────────────────────────────────────────


def test_coverage_full_when_all_fields_present() -> None:
    """All measures / dims / structural filters present → 100% coverage."""
    fps = [_q1_fingerprint()]
    enriched = _q1_enriched()
    report = coverage_check(fps, enriched)

    assert report.total_queries == 1
    assert report.covered == 1
    assert report.coverage_pct == 100.0
    assert report.per_query[0].covered is True
    assert report.per_query[0].measures_present == ["SUM(billed_business)"]
    assert report.per_query[0].measures_missing == []
    assert "billed_business" not in report.per_query[0].dimensions_missing
    assert report.per_query[0].structural_filters_baked is True


def test_coverage_identifies_missing_measure() -> None:
    """Drop the SUM measure → query is not covered, gap names the measure."""
    fps = [_q1_fingerprint()]

    # Strip the measure block out of the view
    no_measure_view = _VIEW_Q1_FULL.replace(
        """  measure: total_billed_business {
    type: sum
    description: "Total billed business amount"
    value_format_name: usd
    sql: ${TABLE}.billed_business ;;
  }""",
        "",
    )
    enriched = {
        "cornerstone_metrics": EnrichedOutput(
            view_lkml=no_measure_view,
            explore_lkml=_EXPLORE_Q1,
        )
    }
    report = coverage_check(fps, enriched)

    assert report.covered == 0
    assert report.coverage_pct == 0.0
    qc = report.per_query[0]
    assert qc.covered is False
    assert "SUM(billed_business)" in qc.measures_missing
    assert any(
        "missing_measure:SUM(billed_business)" in g for g in report.top_gaps
    )


def test_coverage_with_cte_derived_table() -> None:
    """Q9-shape: CTE has matching derived_table view with baked filters."""
    fps = [_q9like_fingerprint()]
    enriched = _q9like_enriched()
    report = coverage_check(fps, enriched)
    qc = report.per_query[0]
    assert qc.derived_tables_exist is True
    assert qc.covered is True


def test_coverage_flags_missing_derived_table() -> None:
    """Same Q9-shape but enrichment forgot the derived_table view."""
    fps = [_q9like_fingerprint()]
    enriched = {
        "risk_pers_acct_history": EnrichedOutput(
            view_lkml=_VIEW_RPAH_BASE,
            derived_table_views=[],  # gap
            explore_lkml="explore: risk_pers_acct_history {}",
        )
    }
    report = coverage_check(fps, enriched)
    qc = report.per_query[0]
    assert qc.derived_tables_exist is False
    assert qc.covered is False
    assert any("missing_derived_table:rpah" in g for g in report.top_gaps)


# ─── Tests: reconstruct_sql_check (smoke — wraps existing guardrail) ─


def test_reconstruct_sql_check_returns_gate_result() -> None:
    """Smoke test — wrapper round-trips to a GateResult."""
    fp_dict = {
        "query_id": "Q1",
        "tables": ["cornerstone_metrics"],
        "aggregations": [
            {"function": "SUM", "column": "billed_business"},
        ],
        "filters": [
            {"column": "data_source", "operator": "=",
             "value": "'cornerstone'", "is_structural": True},
        ],
    }
    enriched = _q1_enriched()
    gate = reconstruct_sql_check(["SELECT 1"], [fp_dict], enriched)
    assert isinstance(gate, GateResult)
    assert gate.stage == "sql_reconstruction"


# ─── Tests: LoopAgent control flow (mocked LLM) ──────────────────────


class _FakeSession:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state


class _FakeCtx:
    """Minimal stand-in for InvocationContext sufficient for the
    deterministic sub-agents to read state and emit events.

    Only carries the surface area our custom agents touch. We exercise the
    LoopAgent's exit semantics in `_drive_loop` directly (not by calling
    LoopAgent._run_async_impl) so we don't have to construct a full ADK
    runtime context for unit tests.
    """

    def __init__(self, state: dict[str, Any]) -> None:
        self.session = _FakeSession(state)
        self.invocation_id = "test-invocation"


def _drive_loop(loop_agent: Any, state: dict[str, Any]) -> list[Any]:
    """Replicate LoopAgent semantics against our fake ctx so we don't
    have to bring up the full ADK runtime in unit tests:

      while iters < max_iterations:
          for sub_agent in sub_agents:
              for event in sub_agent.run(...):
                  if event.actions.escalate: exit outer loop
          iters += 1

    Deterministic sub-agents (_CoverageCheckerAgent, _SqlReconstructorAgent)
    are called via their _run_async_impl directly. The third sub-agent
    (gap_fixer) is invoked via .run_async(ctx) to allow the tests to swap
    in either an LlmAgent (production) or a mock (tests).
    """
    ctx = _FakeCtx(state)
    events: list[Any] = []
    max_iters = loop_agent.max_iterations or 1

    async def _run() -> None:
        for _ in range(max_iters):
            should_exit = False
            for sub in loop_agent.sub_agents:
                runner = getattr(sub, "_run_async_impl", None) or sub.run_async
                async for ev in runner(ctx):
                    events.append(ev)
                    if getattr(getattr(ev, "actions", None), "escalate", False):
                        should_exit = True
                if should_exit:
                    break
            if should_exit:
                return

    asyncio.run(_run())
    return events


def test_loop_exits_on_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """First-iter coverage=100% AND sql_recon passes → loop exits.
    Mocked gap_fixer must NEVER be invoked.
    """
    fps = [_q1_fingerprint()]
    enriched = _q1_enriched()

    loop_agent = build_evaluator_loop(model="dummy")

    # Replace gap_fixer with a mock that records call count.
    gap_fixer_calls = {"n": 0}

    class _ForbiddenGapFixer:
        name = "gap_fixer"

        async def run_async(self, _ctx: Any) -> AsyncGenerator[Any, None]:
            gap_fixer_calls["n"] += 1
            raise AssertionError("gap_fixer must not run when both gates pass")
            yield  # pragma: no cover — make this a generator function

    loop_agent.sub_agents[2] = _ForbiddenGapFixer()  # type: ignore[list-item]

    state: dict[str, Any] = {
        "fingerprints": fps,
        "enriched": enriched,
        "gold_sqls": ["SELECT 1"],
    }

    events = _drive_loop(loop_agent, state)

    # The deterministic agents emit 1 event each on the first pass.
    assert len(events) >= 2
    assert state["evaluator_status"] == "pass"
    coverage: CoverageReport = state["coverage_report"]
    assert coverage.coverage_pct == 100.0
    assert gap_fixer_calls["n"] == 0


def test_loop_max_iterations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Perma-failing fixture (missing measure → coverage stays at 0).
    Loop must terminate at max_iterations=3 with status='warn'.
    """
    fps = [_q1_fingerprint()]
    # Strip measure → coverage will fail every iteration
    no_measure_view = _VIEW_Q1_FULL.replace(
        """  measure: total_billed_business {
    type: sum
    description: "Total billed business amount"
    value_format_name: usd
    sql: ${TABLE}.billed_business ;;
  }""",
        "",
    )
    enriched = {
        "cornerstone_metrics": EnrichedOutput(
            view_lkml=no_measure_view,
            explore_lkml=_EXPLORE_Q1,
        )
    }

    loop_agent = build_evaluator_loop(model="dummy")

    # Replace gap_fixer with an async mock that yields one event without
    # patching the LookML — keeps the loop failing.
    call_counter = {"n": 0}

    class _MockGapFixer:
        name = "gap_fixer"

        async def run_async(self, _ctx: Any) -> AsyncGenerator[Any, None]:
            call_counter["n"] += 1
            from google.adk.events import Event, EventActions
            from google.genai import types
            yield Event(
                invocation_id=_ctx.invocation_id,
                author=self.name,
                content=types.Content(parts=[types.Part(text="no-op")]),
                actions=EventActions(),
            )

    loop_agent.sub_agents[2] = _MockGapFixer()  # type: ignore[list-item]

    state: dict[str, Any] = {
        "fingerprints": fps,
        "enriched": enriched,
        "gold_sqls": ["SELECT 1"],
    }
    events = _drive_loop(loop_agent, state)

    # 3 iterations × 3 sub-agents = 9 events from sub-agents
    assert call_counter["n"] == 3
    assert state["evaluator_status"] == "warn"
    assert state["coverage_report"].coverage_pct == 0.0
    assert len(events) >= 9


# ─── Tests: regression detection via guardrails.check_evaluation ─────


def test_no_regression_blocking() -> None:
    """A previously-covered query that becomes uncovered must block."""
    prev = CoverageReport(
        total_queries=2,
        covered=2,
        coverage_pct=100.0,
        per_query=[
            QueryCoverage(query_id="Q1", covered=True,
                          structural_filters_baked=True, joins_correct=True),
            QueryCoverage(query_id="Q2", covered=True,
                          structural_filters_baked=True, joins_correct=True),
        ],
        all_lookml_valid=True,
    )
    curr = CoverageReport(
        total_queries=2,
        covered=1,
        coverage_pct=50.0,
        per_query=[
            QueryCoverage(query_id="Q1", covered=True,
                          structural_filters_baked=True, joins_correct=True),
            QueryCoverage(query_id="Q2", covered=False,
                          structural_filters_baked=True, joins_correct=True),
        ],
        all_lookml_valid=True,
    )
    gate = check_evaluation(curr, previous_coverage=prev)
    assert gate.status == "fail"
    assert any("Q2" in b for b in gate.blocking_failures)
    assert any("REGRESSION" in b for b in gate.blocking_failures)
