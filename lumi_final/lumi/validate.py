"""Stage 6: Validate — deterministic coverage check + LoopAgent evaluator.

Two-layer design:

1. Deterministic checks (no LLM):
   - coverage_check(fingerprints, enriched) -> CoverageReport
       For each fingerprint, decide whether the enriched LookML can answer
       it: every column gets a dim, every aggregation gets a measure of the
       right type whose sql contains the source column, every CTE has a
       derived_table view, structural filters are baked, joins are present.
   - reconstruct_sql_check(...) -> GateResult
       Thin wrapper around guardrails.check_sql_reconstruction (the SQL
       rebuild safety net that runs before publish).

2. ADK LoopAgent (max 3 iterations):
   - coverage_checker_agent  (deterministic, wraps coverage_check)
   - sql_reconstructor_agent (deterministic, wraps reconstruct_sql_check)
   - gap_fixer_agent         (LlmAgent — only fires if either gate failed)

   The loop exits when both deterministic gates pass (sub-agent emits an
   event with `actions.escalate=True`). Otherwise it iterates until
   max_iterations (3), then exits with status="warn".

This module is the "validate" verb in the 7-stage flow — it consumes
EnrichedOutputs from Enrich and produces a CoverageReport + GateResults
the Publish stage reads.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

from lumi.guardrails import check_sql_reconstruction
from lumi.schemas import (
    CoverageReport,
    EnrichedOutput,
    GateResult,
    QueryCoverage,
)

if TYPE_CHECKING:
    from google.adk.agents import LoopAgent
    from google.adk.agents.invocation_context import InvocationContext
    from google.adk.events import Event

    from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.validate")


# ─── Aggregation type mapping (sqlglot class name → LookML measure type) ──

_AGG_TO_MEASURE_TYPE: dict[str, set[str]] = {
    "SUM": {"sum", "sum_distinct"},
    "COUNT": {"count", "count_distinct"},
    "AVG": {"average", "average_distinct"},
    "MIN": {"min"},
    "MAX": {"max"},
    "STDDEV": {"number"},
    "VARIANCE": {"number"},
}


# ─── Helpers: parse enriched LookML once per call ──────────────────────

def _parse_views(enriched: EnrichedOutput) -> list[dict[str, Any]]:
    """Return all view dicts (main + derived_table_views) from one enrichment.

    Returns empty list on parse failure — coverage will then mark the
    relevant queries uncovered, which is the correct behaviour.
    """
    import lkml

    out: list[dict[str, Any]] = []
    try:
        parsed = lkml.load(enriched.view_lkml)
        out.extend(parsed.get("views", []) or [])
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not parse main view_lkml: %s", e)
    for dtv in enriched.derived_table_views:
        try:
            parsed = lkml.load(dtv)
            out.extend(parsed.get("views", []) or [])
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not parse derived_table view: %s", e)
    return out


def _all_views(enriched: dict[str, EnrichedOutput]) -> dict[str, list[dict[str, Any]]]:
    """Map each table name to the list of view dicts produced for it."""
    return {name: _parse_views(out) for name, out in enriched.items()}


def _has_dimension_for_column(
    column: str,
    views: list[dict[str, Any]],
    *,
    allow_measure_source: bool = True,
) -> bool:
    """A column is covered if some dimension or dimension_group either:
      - has matching `name` (case-insensitive), or
      - has `sql` containing the column reference.

    When `allow_measure_source` is True (default for general column-reference
    coverage), a column also counts as covered if it appears in any measure's
    sql expression — Looker can answer queries that only use a column as the
    body of an aggregation without exposing it as a separate dimension.
    Filter / WHERE-clause columns set this to False because they DO need a
    queryable dimension.
    """
    if not column:
        return True
    col_lower = column.lower()
    for v in views:
        for d in (v.get("dimensions") or []) + (v.get("dimension_groups") or []):
            name = (d.get("name") or "").lower()
            if name == col_lower:
                return True
            sql = (d.get("sql") or "").lower()
            if col_lower in sql:
                return True
        if allow_measure_source:
            for m in v.get("measures") or []:
                msql = (m.get("sql") or "").lower()
                if col_lower in msql:
                    return True
    return False


def _has_measure_for_aggregation(
    function: str,
    column: str | None,
    views: list[dict[str, Any]],
) -> bool:
    """An aggregation is covered if some measure has:
      - `type` in the allowed set for `function`, AND
      - `sql` containing the column substring (or column is None — bare
        COUNT(*) has no source column and any count measure suffices).
    """
    allowed_types = _AGG_TO_MEASURE_TYPE.get(function.upper(), set())
    if not allowed_types:
        return False
    col_lower = (column or "").lower()
    for v in views:
        for m in v.get("measures") or []:
            mtype = (m.get("type") or "").lower()
            if mtype not in allowed_types:
                continue
            if not col_lower:
                return True
            msql = (m.get("sql") or "").lower()
            if col_lower in msql:
                return True
    return False


def _structural_filter_baked(
    column: str,
    value: str,
    views: list[dict[str, Any]],
    enriched: EnrichedOutput,
) -> bool:
    """A structural filter is baked if it lives in:
      - a derived_table.sql for any view, OR
      - the explore's sql_always_where (substring check, both column + value).

    We accept either column+value substring OR sql_always_where containing
    the column (the value may be templated).
    """
    col_lower = column.lower()
    val_str = str(value).lower()

    for v in views:
        dt = v.get("derived_table") or {}
        dt_sql = (dt.get("sql") or "").lower()
        if col_lower in dt_sql and val_str in dt_sql:
            return True

    explore = (enriched.explore_lkml or "").lower()
    if "sql_always_where" in explore and col_lower in explore:
        return True
    return False


def _has_case_when(
    cw: dict[str, Any],
    views: list[dict[str, Any]],
) -> bool:
    """A CASE WHEN is covered when either:
      (a) a dimension's name matches the alias, OR
      (b) some dimension's sql contains the case_when expression (cheap
          substring check on a normalised version), OR
      (c) all mapped THEN values appear in some single dimension's sql
          (the LLM may have built a derived dim that maps the values).
    """
    alias = (cw.get("alias") or "").lower()
    target_sql = (cw.get("sql") or "").lower()
    mapped_thens = {
        (m.get("then") or "").strip().lower()
        for m in (cw.get("mapped_values") or [])
        if (m.get("then") or "").strip()
    }

    for v in views:
        for d in v.get("dimensions") or []:
            name = (d.get("name") or "").lower()
            sql = (d.get("sql") or "").lower()
            if alias and name == alias:
                return True
            if target_sql and "case" in sql and "when" in sql:
                # Compare the WHEN→THEN values; if all THENs we expect
                # appear in this dim's sql, treat as covered.
                if mapped_thens and all(t in sql for t in mapped_thens):
                    return True
                # Or if a substantial chunk of the original sql matches,
                # accept (cheap proxy: first 30 chars).
                if target_sql[:30] in sql:
                    return True
    return False


def _derived_table_for_cte(
    cte: dict[str, Any],
    views: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    """A CTE/temp-table is covered if some view has a derived_table whose
    name matches the alias AND whose sql contains every structural filter
    column the CTE introduced.

    Returns (covered, list_of_missing_filter_columns).
    """
    alias = (cte.get("alias") or "").lower()
    if not alias:
        return False, []

    filter_cols = [
        (f.get("column") or "").lower()
        for f in (cte.get("structural_filters") or [])
        if f.get("column")
    ]

    for v in views:
        vname = (v.get("name") or "").lower()
        dt = v.get("derived_table") or {}
        if not dt:
            continue
        if vname != alias and alias not in vname:
            continue
        dt_sql = (dt.get("sql") or "").lower()
        missing = [c for c in filter_cols if c not in dt_sql]
        if not missing:
            return True, []
        return False, missing
    return False, filter_cols


# ─── Public: coverage_check ────────────────────────────────────────────

def coverage_check(
    fingerprints: list[SQLFingerprint],
    enriched: dict[str, EnrichedOutput],
) -> CoverageReport:
    """Deterministic coverage assessment. No LLM, no I/O.

    For each fingerprint, determine if the enriched LookML can answer the
    query. A query is COVERED iff:
      - every table referenced has a TableContext with an enriched view
      - every column referenced has a dimension/dimension_group somewhere
      - every aggregation has a matching measure (right type + col in sql)
      - every CASE WHEN has a derived dimension or matching sql snippet
      - every CTE / temp table has a derived_table view with structural
        filters baked into the derived_table.sql
      - structural filters at the query level are either baked into a
        derived_table or live in the explore's sql_always_where
      - every joined table also has an enriched view (join path complete)

    Args:
        fingerprints: Stage 1 output. Each has tables/aggregations/filters
            /case_whens/ctes/temp_tables/joins.
        enriched: dict[table_name, EnrichedOutput] from Stage 5.

    Returns:
        CoverageReport with per-query QueryCoverage, top_gaps, and
        all_lookml_valid.
    """
    parsed_views = _all_views(enriched)
    all_lookml_valid = all(
        bool(views) or not enriched[name].view_lkml.strip()
        for name, views in parsed_views.items()
    )

    per_query: list[QueryCoverage] = []
    gap_counter: dict[str, int] = {}

    for idx, fp in enumerate(fingerprints, start=1):
        # SQLFingerprint can be the dataclass OR a dict (for compat with
        # check_sql_reconstruction). Normalise field access.
        get = _fingerprint_getter(fp)
        query_id = get("query_id") or f"Q{idx:02d}"

        tables = list(get("tables") or [])
        aggregations = list(get("aggregations") or [])
        filters = list(get("filters") or [])
        case_whens = list(get("case_whens") or [])
        ctes = list(get("ctes") or [])
        temp_tables = list(get("temp_tables") or [])
        joins = list(get("joins") or [])
        columns_referenced = list(get("columns_referenced") or [])

        # Build the union of views available to this query: every referenced
        # table's view PLUS every joined table's view.
        all_relevant: list[dict[str, Any]] = []
        joined_tables = {
            j.get("right_table") or j.get("other_table")
            for j in joins
            if j.get("right_table") or j.get("other_table")
        }
        relevant_table_names = {*tables, *(t for t in joined_tables if t)}

        explore_views_present = True
        for t in relevant_table_names:
            if t not in enriched:
                explore_views_present = False
                continue
            all_relevant.extend(parsed_views.get(t, []))

        # --- Aggregations ---
        measures_present: list[str] = []
        measures_missing: list[str] = []
        for agg in aggregations:
            label = f"{agg.get('function')}({agg.get('column') or '*'})"
            if _has_measure_for_aggregation(
                agg.get("function") or "", agg.get("column"), all_relevant
            ):
                measures_present.append(label)
            else:
                measures_missing.append(label)
                gap_counter[f"missing_measure:{label}"] = (
                    gap_counter.get(f"missing_measure:{label}", 0) + 1
                )

        # --- Dimensions ---
        # General column references can be satisfied by EITHER a dimension
        # OR a measure body that selects them (Looker can answer it).
        # Filter / WHERE columns and CASE WHEN sources MUST be real
        # queryable dimensions.
        filter_cols = {f["column"] for f in filters if f.get("column")}
        case_when_cols = {
            cw["source_column"] for cw in case_whens if cw.get("source_column")
        }
        strict_cols = filter_cols | case_when_cols
        loose_cols = set(columns_referenced) - strict_cols

        dimensions_present: list[str] = []
        dimensions_missing: list[str] = []
        for col in sorted(loose_cols):
            if _has_dimension_for_column(col, all_relevant, allow_measure_source=True):
                dimensions_present.append(col)
            else:
                dimensions_missing.append(col)
                gap_counter[f"missing_dimension:{col}"] = (
                    gap_counter.get(f"missing_dimension:{col}", 0) + 1
                )
        for col in sorted(strict_cols):
            if _has_dimension_for_column(col, all_relevant, allow_measure_source=False):
                dimensions_present.append(col)
            else:
                dimensions_missing.append(col)
                gap_counter[f"missing_dimension:{col}"] = (
                    gap_counter.get(f"missing_dimension:{col}", 0) + 1
                )

        # --- Filters: every filter column must resolve, and structural
        #     filters must be baked. ---
        filters_resolvable: list[str] = []
        filters_missing: list[str] = []
        structural_baked = True
        for f in filters:
            col = f.get("column")
            if not col:
                continue
            label = f"{col} {f.get('operator', '')} {f.get('value', '')}".strip()
            covered = _has_dimension_for_column(
                col, all_relevant, allow_measure_source=False
            )
            if f.get("is_structural"):
                # Structural filters live in derived_table.sql or the
                # explore's sql_always_where — we look across every
                # enriched output in the relevant table set.
                baked = any(
                    _structural_filter_baked(
                        col, f.get("value", ""), parsed_views.get(t, []), enriched[t]
                    )
                    for t in relevant_table_names
                    if t in enriched
                )
                if not baked:
                    structural_baked = False
                    filters_missing.append(f"structural:{label}")
                    gap_counter[f"unbaked_filter:{col}"] = (
                        gap_counter.get(f"unbaked_filter:{col}", 0) + 1
                    )
                    continue
            if covered:
                filters_resolvable.append(label)
            else:
                filters_missing.append(label)

        # --- CASE WHENs ---
        for cw in case_whens:
            if not _has_case_when(cw, all_relevant):
                alias = cw.get("alias") or cw.get("source_column") or "case_when"
                gap_counter[f"missing_case_when:{alias}"] = (
                    gap_counter.get(f"missing_case_when:{alias}", 0) + 1
                )
                dimensions_missing.append(f"case_when:{alias}")

        # --- CTEs / temp tables (each needs a derived_table view) ---
        derived_tables_exist = True
        # A view "with a derived_table" must have something matching the CTE
        # alias. If there are NO derived_table views at all in any relevant
        # enriched output, every CTE is "missing entirely" (not just filter-
        # incomplete). This distinction matters for the gap category.
        any_dt_present = any(
            (v.get("derived_table") or {}) for v in all_relevant
        )
        for cte in (*ctes, *temp_tables):
            ok, missing_cols = _derived_table_for_cte(cte, all_relevant)
            if not ok:
                derived_tables_exist = False
                alias = cte.get("alias", "cte")
                if missing_cols and any_dt_present:
                    gap_counter[f"derived_table_filters_missing:{alias}"] = (
                        gap_counter.get(
                            f"derived_table_filters_missing:{alias}", 0
                        )
                        + 1
                    )
                else:
                    gap_counter[f"missing_derived_table:{alias}"] = (
                        gap_counter.get(f"missing_derived_table:{alias}", 0) + 1
                    )

        # --- Joins: every joined right_table must have an enriched view ---
        joins_correct = True
        for j in joins:
            right = j.get("right_table") or j.get("other_table")
            if right and right not in enriched:
                joins_correct = False
                gap_counter[f"missing_join_view:{right}"] = (
                    gap_counter.get(f"missing_join_view:{right}", 0) + 1
                )

        explore_exists = explore_views_present and any(
            enriched[t].explore_lkml for t in tables if t in enriched
        )

        covered = (
            explore_views_present
            and not measures_missing
            and not dimensions_missing
            and not filters_missing
            and structural_baked
            and derived_tables_exist
            and joins_correct
        )

        gap_category = None
        if not covered:
            if measures_missing or dimensions_missing:
                gap_category = "prompt_fix"
            elif not derived_tables_exist or not structural_baked:
                gap_category = "prompt_fix"
            elif not joins_correct or not explore_views_present:
                gap_category = "mdm_fix"

        per_query.append(
            QueryCoverage(
                query_id=query_id,
                covered=covered,
                measures_present=measures_present,
                measures_missing=measures_missing,
                dimensions_present=dimensions_present,
                dimensions_missing=dimensions_missing,
                filters_resolvable=filters_resolvable,
                filters_missing=filters_missing,
                explore_exists=explore_exists,
                joins_correct=joins_correct,
                derived_tables_exist=derived_tables_exist,
                structural_filters_baked=structural_baked,
                gap_category=gap_category,
            )
        )

    total = len(per_query)
    covered_count = sum(1 for q in per_query if q.covered)
    pct = (covered_count / total * 100.0) if total else 100.0

    top_gaps = [
        f"{label} (x{count})"
        for label, count in sorted(
            gap_counter.items(), key=lambda kv: kv[1], reverse=True
        )[:10]
    ]

    return CoverageReport(
        total_queries=total,
        covered=covered_count,
        coverage_pct=pct,
        per_query=per_query,
        all_lookml_valid=all_lookml_valid,
        top_gaps=top_gaps,
    )


def _fingerprint_getter(fp: SQLFingerprint | dict[str, Any]):
    """Return a callable get(key) that works on either a SQLFingerprint
    dataclass or a plain dict (used by reconstruct_sql_check for compat
    with check_sql_reconstruction's dict-based contract).
    """
    if isinstance(fp, dict):
        return fp.get
    return lambda k, default=None: getattr(fp, k, default)


# ─── Public: reconstruct_sql_check ─────────────────────────────────────

def reconstruct_sql_check(
    gold_sqls: list[str],
    fingerprints: list[dict[str, Any]],
    enriched: dict[str, EnrichedOutput],
) -> GateResult:
    """SQL-rebuild safety net. Wraps guardrails.check_sql_reconstruction.

    For each gold SQL, finds the matching explore in `enriched` and asks
    "would Looker MCP generate the right SQL from this LookML?". Catches
    structural mismatches (missing measures, joins without sql_on,
    structural filters not baked) before publish.

    Args:
        gold_sqls: original input SQL strings.
        fingerprints: list of fingerprint dicts (NOT dataclass — the
            underlying check uses dict access for flexibility).
        enriched: dict[table_name, EnrichedOutput] from Stage 5.

    Returns:
        GateResult from guardrails.check_sql_reconstruction.
    """
    return check_sql_reconstruction(gold_sqls, enriched, fingerprints)


# ─── ADK loop construction ──────────────────────────────────────────────

def build_evaluator_loop(model: str | None = None) -> LoopAgent:
    """Build the Stage-6 LoopAgent (max 3 iterations).

    Sub-agents (in order):
      1. coverage_checker_agent  (deterministic)
      2. sql_reconstructor_agent (deterministic)
      3. gap_fixer_agent         (LlmAgent — only re-runs on gap)

    Loop input (read from `ctx.session.state`):
      - "fingerprints": list[SQLFingerprint] (or dict-equivalent)
      - "enriched":     dict[str, EnrichedOutput]
      - "gold_sqls":    list[str]
      - (optional) "previous_coverage": CoverageReport

    Loop output (written to `ctx.session.state`):
      - "coverage_report": CoverageReport
      - "sql_recon_gate": GateResult
      - "evaluator_status": "pass" | "warn"

    Exit conditions (whichever first):
      - both deterministic gates pass → escalate=True (clean exit)
      - max_iterations reached → loop terminates naturally with status="warn"

    The LlmAgent (gap_fixer) is constructed lazily and is only invoked
    when the deterministic gates report failures. Tests typically mock
    this sub-agent to avoid real Vertex calls.
    """
    from google.adk.agents import LoopAgent  # local import: optional dep

    coverage_checker = _CoverageCheckerAgent(name="coverage_checker")
    sql_recon = _SqlReconstructorAgent(name="sql_reconstructor")
    gap_fixer = _build_gap_fixer_agent(model)

    return LoopAgent(
        name="evaluator",
        max_iterations=3,
        sub_agents=[coverage_checker, sql_recon, gap_fixer],
    )


def _build_gap_fixer_agent(model: str | None) -> Any:
    """Construct the LlmAgent that consumes a CoverageReport and emits
    patched EnrichedOutputs for tables with gaps.

    Kept thin on purpose: the prompt + structured output schema for
    patches will be tuned in Session 6. Tests replace this agent with
    a mock to avoid Vertex calls.
    """
    from google.adk.agents import LlmAgent

    instruction = (
        "You are the LUMI gap-fixer. The previous step produced a "
        "CoverageReport in session state under 'coverage_report' and a "
        "SQL-reconstruction GateResult under 'sql_recon_gate'. For each "
        "uncovered query, identify the minimal patch to the corresponding "
        "EnrichedOutput in session state 'enriched' that would close the "
        "gap (add a missing measure, add a derived_table view, bake a "
        "structural filter into sql_always_where). DO NOT reduce coverage "
        "for already-covered queries. Emit only the patched fields."
    )
    return LlmAgent(
        name="gap_fixer",
        model=model or "gemini-3.1-pro-preview",
        instruction=instruction,
    )


# ─── Custom deterministic agents ───────────────────────────────────────

class _DeterministicAgent:
    """Mixin marker — these agents do no LLM I/O; they read session state,
    run a pure function, write back, and optionally escalate.
    """


def _make_event(
    ctx: InvocationContext,
    author: str,
    text: str,
    *,
    escalate: bool = False,
) -> Event:
    """Build a single ADK Event with optional escalation flag."""
    from google.adk.events import Event, EventActions
    from google.genai import types

    return Event(
        invocation_id=ctx.invocation_id,
        author=author,
        content=types.Content(parts=[types.Part(text=text)]),
        actions=EventActions(escalate=escalate),
    )


# Imported here so the BaseAgent subclass below can resolve at class-build
# time when this module is imported.
from google.adk.agents import BaseAgent  # noqa: E402


class _CoverageCheckerAgent(BaseAgent, _DeterministicAgent):
    """LoopAgent sub-agent: runs coverage_check on session state.

    Reads:  state['fingerprints'], state['enriched']
    Writes: state['coverage_report'] (CoverageReport)
    Escalates: never on its own — the SqlReconstructorAgent makes that call
        once it has both signals.
    """

    async def _run_async_impl(  # type: ignore[override]
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        state = ctx.session.state
        fingerprints = state.get("fingerprints") or []
        enriched = state.get("enriched") or {}
        report = coverage_check(fingerprints, enriched)
        state["coverage_report"] = report
        msg = (
            f"coverage_check: {report.covered}/{report.total_queries} "
            f"({report.coverage_pct:.1f}%) — top_gaps={report.top_gaps[:3]}"
        )
        yield _make_event(ctx, self.name, msg)


class _SqlReconstructorAgent(BaseAgent, _DeterministicAgent):
    """LoopAgent sub-agent: runs reconstruct_sql_check then decides whether
    to escalate (loop exit).

    Reads:  state['gold_sqls'], state['fingerprints'], state['enriched'],
            state['coverage_report']
    Writes: state['sql_recon_gate'] (GateResult), state['evaluator_status']
    Escalates when BOTH gates pass:
      - coverage_report.coverage_pct == 100 (or no per_query rows)
      - sql_recon_gate.status == 'pass'
    """

    async def _run_async_impl(  # type: ignore[override]
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        state = ctx.session.state
        gold_sqls = state.get("gold_sqls") or []
        fingerprints = state.get("fingerprints") or []
        # check_sql_reconstruction expects dicts. Normalise.
        fp_dicts: list[dict[str, Any]] = []
        for fp in fingerprints:
            if isinstance(fp, dict):
                fp_dicts.append(fp)
            else:
                fp_dicts.append({
                    "query_id": getattr(fp, "query_id", None),
                    "tables": getattr(fp, "tables", []),
                    "aggregations": getattr(fp, "aggregations", []),
                    "filters": getattr(fp, "filters", []),
                    "ctes": getattr(fp, "ctes", []),
                    "temp_tables": getattr(fp, "temp_tables", []),
                    "joins": getattr(fp, "joins", []),
                })
        enriched = state.get("enriched") or {}
        gate = reconstruct_sql_check(gold_sqls, fp_dicts, enriched)
        state["sql_recon_gate"] = gate

        coverage: CoverageReport | None = state.get("coverage_report")
        coverage_pass = coverage is not None and (
            coverage.total_queries == 0 or coverage.coverage_pct >= 100.0
        )
        recon_pass = gate.status == "pass"
        loop_done = coverage_pass and recon_pass
        state["evaluator_status"] = "pass" if loop_done else "warn"

        text = (
            f"sql_recon: status={gate.status} "
            f"blocking={len(gate.blocking_failures)} "
            f"coverage_pass={coverage_pass} → escalate={loop_done}"
        )
        yield _make_event(ctx, self.name, text, escalate=loop_done)


__all__ = [
    "coverage_check",
    "reconstruct_sql_check",
    "build_evaluator_loop",
]
