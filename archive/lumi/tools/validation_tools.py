"""Deterministic coverage validation — no LLM, pure field matching.

For each gold query, check:
  - Does a measure exist for each SQL aggregation?
  - Does a dimension exist for each WHERE / GROUP BY column?
  - Does an explore (join pattern) exist covering the query's tables?
  - Is the column even present in the view?
"""

from __future__ import annotations

import logging
import re
from typing import Any

from lumi.schemas import (
    CoverageFailure,
    CoverageReport,
    EnrichedField,
    EnrichedView,
    JoinPattern,
    ParsedQuery,
)

_REF_RE = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*)\}")

# SQL aggregation name → set of LookML measure types that can satisfy it.
_AGG_TO_LOOKML_TYPES: dict[str, set[str]] = {
    "SUM": {"sum", "sum_distinct", "number"},
    "COUNT": {"count", "count_distinct", "number"},
    "AVG": {"average", "average_distinct", "number"},
    "MIN": {"min", "number"},
    "MAX": {"max", "number"},
}

logger = logging.getLogger(__name__)


def validate_coverage(
    gold_queries: list[ParsedQuery],
    enriched_views: dict[str, EnrichedView],
    view_name_to_table: dict[str, str],
    explore_patterns: list[JoinPattern] | None = None,
) -> dict[str, Any]:
    """Produce a CoverageReport for the gold-query set."""
    explore_patterns = explore_patterns or []
    table_to_view = {t: v for v, t in view_name_to_table.items()}

    field_index = _build_field_index(enriched_views)
    explore_sigs = {p.signature for p in explore_patterns}

    report = CoverageReport(
        total_queries=len(gold_queries),
        passed=0,
        partial=0,
        failed=0,
    )

    for q in gold_queries:
        failures = _validate_query(q, field_index, table_to_view, explore_sigs)
        if not failures:
            report.passed += 1
        else:
            report.failures.extend(failures)
            if _is_partial(q, failures):
                report.partial += 1
            else:
                report.failed += 1

    report.coverage_by_source = _count_by_source(enriched_views)

    logger.info(
        "Coverage: %d/%d pass (%.1f%%), %d partial, %d fail",
        report.passed,
        report.total_queries,
        report.coverage_pct,
        report.partial,
        report.failed,
    )
    return {"status": "success", "report": report, "error": None}


def _build_field_index(
    views: dict[str, EnrichedView],
) -> dict[str, dict[str, EnrichedField]]:
    """view_name -> {field_name: EnrichedField}."""
    return {view_name: {f.name: f for f in ev.fields} for view_name, ev in views.items()}


def _measure_covers(field: EnrichedField, agg_fn: str, agg_column: str | None) -> bool:
    """Does this LookML measure resolve the given SQL aggregation?"""
    if field.kind != "measure":
        return False

    type_ok = True
    if field.type:
        allowed = _AGG_TO_LOOKML_TYPES.get(agg_fn.upper(), set())
        type_ok = not allowed or field.type.lower() in allowed

    if not type_ok:
        return False

    # If the measure's name matches the bare column, that's a legacy convention but works.
    if agg_column and field.name == agg_column:
        return True

    # Otherwise, the measure's SQL must reference ${agg_column} somewhere.
    if agg_column and field.sql:
        refs = {m.group(1).split(".")[0] for m in _REF_RE.finditer(field.sql)}
        if agg_column in refs:
            return True

    # COUNT(*) or COUNT(1) with no column — any count measure counts.
    return agg_column is None and agg_fn.upper() == "COUNT"


def _validate_query(
    q: ParsedQuery,
    field_index: dict[str, dict[str, EnrichedField]],
    table_to_view: dict[str, str],
    explore_sigs: set[str],
) -> list[CoverageFailure]:
    failures: list[CoverageFailure] = []

    if q.parse_error:
        failures.append(
            CoverageFailure(
                query_id=q.query_id,
                user_prompt=q.user_prompt,
                reason="parse_error",
                detail=q.parse_error,
            )
        )
        return failures

    primary_view = table_to_view.get(q.primary_table or "")
    if primary_view is None:
        failures.append(
            CoverageFailure(
                query_id=q.query_id,
                user_prompt=q.user_prompt,
                reason="schema_gap",
                detail=f"Primary table '{q.primary_table}' has no matching enriched view",
            )
        )
        return failures

    primary_fields = field_index.get(primary_view, {})

    for m in q.measures:
        covering = any(
            _measure_covers(f, m.function, m.column) for f in primary_fields.values()
        )
        if not covering:
            failures.append(
                CoverageFailure(
                    query_id=q.query_id,
                    user_prompt=q.user_prompt,
                    reason="missing_measure",
                    detail=(
                        f"No measure resolves {m.function}({m.column or '*'}) in {primary_view}"
                    ),
                    suggested_fix=(
                        f"Add measure type={_sql_to_lkml_type(m.function)} "
                        f"sql=${{{m.column}}}" if m.column else None
                    ),
                )
            )

    for d in q.dimensions:
        if d not in primary_fields:
            failures.append(
                CoverageFailure(
                    query_id=q.query_id,
                    user_prompt=q.user_prompt,
                    reason="missing_dimension",
                    detail=f"Dimension '{d}' missing in {primary_view}",
                )
            )
    for f in q.filters:
        if f.column not in primary_fields:
            failures.append(
                CoverageFailure(
                    query_id=q.query_id,
                    user_prompt=q.user_prompt,
                    reason="missing_dimension",
                    detail=f"Filter column '{f.column}' missing in {primary_view}",
                )
            )

    if q.joins and explore_sigs:
        q_tables = sorted(
            {
                q.primary_table or "",
                *(j.left_table for j in q.joins),
                *(j.right_table for j in q.joins),
            }
        )
        q_tables = [t for t in q_tables if t]
        sig = JoinPattern(tables=q_tables, joins=q.joins).signature
        if sig not in explore_sigs:
            failures.append(
                CoverageFailure(
                    query_id=q.query_id,
                    user_prompt=q.user_prompt,
                    reason="missing_explore",
                    detail=f"No explore covers join tables {q_tables}",
                )
            )

    return failures


def _is_partial(
    query: ParsedQuery, failures: list[CoverageFailure]
) -> bool:
    """Partial = the query has failures but they cover a minority of its facets.

    Facets are measures + dimensions + filters + joins. If fewer than half the
    facets failed, it's partial (the LLM can probably still return something
    useful). Otherwise the query is fully failed.
    """
    total_facets = (
        len(query.measures)
        + len(query.dimensions)
        + len(query.filters)
        + (1 if query.joins else 0)
    )
    if total_facets == 0:
        return False
    return len(failures) < (total_facets / 2)


def _count_by_source(views: dict[str, EnrichedView]) -> dict[str, int]:
    counts = {"gold_query": 0, "mdm": 0, "inferred": 0, "existing_preserved": 0}
    for ev in views.values():
        for f in ev.fields:
            counts[f.origin] = counts.get(f.origin, 0) + 1
    return counts


def _sql_to_lkml_type(function: str) -> str:
    return {
        "SUM": "sum",
        "COUNT": "count",
        "AVG": "average",
        "MIN": "min",
        "MAX": "max",
    }.get(function.upper(), "number")
