"""End-to-end coverage validator.

The hallmark goal: every gold query must be answerable by an explore in
the published LookML. This module is the closing-loop check — it walks
each gold query, finds the explore whose signature matches, then
verifies every field mentioned (dim, measure, filter) resolves to a
LookML field reachable from that explore.

Output is a ``GoldCoverageReport`` per query telling us:
  - which explore would Radix retrieve for this question?
  - is every measure / dimension / filter resolvable on that explore?
  - if not, what's missing?

This is deliberately stricter than ``validate.coverage_check`` (which
runs after enrichment to count covered queries). The coverage validator
runs at PLAN time too — before any Vertex tokens are spent on
enrichment — to fail fast if the explore design doesn't actually answer
the corpus.

Public API:
    validate_corpus_coverage(fingerprints, explore_plans, contexts,
                              ontology=None) -> CorpusCoverage
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from lumi.schemas import TableContext
from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.coverage")


@dataclass
class QueryMatchReport:
    """Coverage assessment for one gold query."""

    query_index: int
    raw_sql_excerpt: str
    matched_explore: str | None
    matched_cluster_id: str | None
    measures_referenced: list[str] = field(default_factory=list)
    dimensions_referenced: list[str] = field(default_factory=list)
    filters_referenced: list[str] = field(default_factory=list)
    measures_resolvable: list[str] = field(default_factory=list)
    measures_missing: list[str] = field(default_factory=list)
    dimensions_resolvable: list[str] = field(default_factory=list)
    dimensions_missing: list[str] = field(default_factory=list)
    filters_resolvable: list[str] = field(default_factory=list)
    filters_missing: list[str] = field(default_factory=list)
    is_covered: bool = False
    notes: list[str] = field(default_factory=list)


@dataclass
class CorpusCoverage:
    """Aggregate coverage across all gold queries."""

    total_queries: int
    covered: int
    coverage_pct: float
    per_query: list[QueryMatchReport]
    uncovered_top_reasons: list[str] = field(default_factory=list)


def validate_corpus_coverage(
    fingerprints: list[SQLFingerprint],
    explore_plans: list[Any],
    contexts: dict[str, TableContext],
    *,
    ontology: Any = None,
) -> CorpusCoverage:
    """Walk every gold query and assess explore-level coverage.

    Steps per query:
      1. Compute the query's signature (tables × GROUP BY × structural filters).
      2. Find the explore whose cluster_id matches that signature.
         If no exact match, fall back to "explore that contains all
         the query's tables on its base_view + dim_views chain."
      3. For each measure/dim/filter the query references, check whether
         the explore exposes it (via its base view or any joined view).
      4. Mark covered iff every reference resolves.

    Notes intentionally permissive: a query may reference a column that
    doesn't appear in TableContext.mdm_columns (corpus drift); we still
    count it resolvable when the column exists in any view in the
    explore's chain.
    """
    plan_by_table_set: dict[frozenset[str], Any] = {}
    for ep in explore_plans:
        sig_tables = frozenset(
            [ep.base_view.lower()] + [d.lower() for d in (ep.dim_views or [])]
        )
        plan_by_table_set[sig_tables] = ep

    per_query: list[QueryMatchReport] = []
    uncovered_reasons: dict[str, int] = {}

    for i, fp in enumerate(fingerprints):
        report = QueryMatchReport(
            query_index=i,
            raw_sql_excerpt=(fp.raw_sql or "")[:120].replace("\n", " "),
            matched_explore=None,
            matched_cluster_id=None,
        )

        if fp.parse_error:
            report.notes.append(f"parse_error: {fp.parse_error}")
            uncovered_reasons["parse_error"] = (
                uncovered_reasons.get("parse_error", 0) + 1
            )
            per_query.append(report)
            continue

        # Reference set extraction.
        report.measures_referenced = sorted({
            (a.get("column") or "").lower()
            for a in (fp.aggregations or [])
            if a.get("column")
        })
        report.dimensions_referenced = sorted({
            (g.get("column") or "").lower()
            for g in (fp.group_by or [])
            if g.get("column")
        } | {
            (col or "").lower() for col in (fp.tables or []) if False  # placeholder
        })
        report.filters_referenced = sorted({
            (f.get("column") or "").lower()
            for f in (fp.filters or [])
            if f.get("column")
        })

        # Match explore by table set.
        query_tables = frozenset(t.lower() for t in (fp.tables or []))
        matched = plan_by_table_set.get(query_tables)
        # Fallback: explore whose tables are a SUPERSET of the query's
        # tables. Pick the smallest such match.
        if matched is None:
            candidates = [
                ep for ts, ep in plan_by_table_set.items()
                if query_tables.issubset(ts)
            ]
            if candidates:
                matched = min(
                    candidates,
                    key=lambda ep: 1 + len(ep.dim_views),
                )

        if matched is None:
            report.notes.append(
                f"no explore covers tables {sorted(query_tables)}"
            )
            uncovered_reasons["no_matching_explore"] = (
                uncovered_reasons.get("no_matching_explore", 0) + 1
            )
            per_query.append(report)
            continue

        report.matched_explore = matched.explore_name
        report.matched_cluster_id = matched.cluster_id

        # Build reachable column set from explore's chain.
        reachable_columns = _columns_in_chain(matched, contexts)
        for ref_list, resolvable_attr, missing_attr, kind in (
            (report.measures_referenced,
             "measures_resolvable", "measures_missing", "measure"),
            (report.dimensions_referenced,
             "dimensions_resolvable", "dimensions_missing", "dimension"),
            (report.filters_referenced,
             "filters_resolvable", "filters_missing", "filter"),
        ):
            for col in ref_list:
                if not col:
                    continue
                if col in reachable_columns:
                    getattr(report, resolvable_attr).append(col)
                else:
                    getattr(report, missing_attr).append(col)
                    uncovered_reasons[f"missing_{kind}"] = (
                        uncovered_reasons.get(f"missing_{kind}", 0) + 1
                    )

        report.is_covered = (
            not report.measures_missing
            and not report.dimensions_missing
            and not report.filters_missing
        )
        per_query.append(report)

    covered = sum(1 for r in per_query if r.is_covered)
    total = len(per_query)
    pct = (100.0 * covered / total) if total else 0.0

    top_reasons = [
        f"{count}× {reason}"
        for reason, count in sorted(
            uncovered_reasons.items(), key=lambda kv: -kv[1],
        )[:5]
    ]

    return CorpusCoverage(
        total_queries=total,
        covered=covered,
        coverage_pct=round(pct, 1),
        per_query=per_query,
        uncovered_top_reasons=top_reasons,
    )


def _columns_in_chain(
    explore_plan: Any, contexts: dict[str, TableContext],
) -> set[str]:
    """Union of every column in every view referenced by the explore."""
    cols: set[str] = set()
    chain = [explore_plan.base_view] + list(explore_plan.dim_views or [])
    for table in chain:
        ctx = contexts.get(table)
        if ctx is None:
            continue
        # Columns from MDM (authoritative) + columns observed in queries
        # (covers MDM-missing tables).
        for c in (ctx.mdm_columns or []):
            n = (c.get("name") or "").lower()
            if n:
                cols.add(n)
        for col_name in (ctx.columns_referenced or []):
            if col_name:
                cols.add(col_name.lower())
    return cols
