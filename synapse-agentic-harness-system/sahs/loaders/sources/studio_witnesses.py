"""Studio witness pair — observed certified-metric SQL, CTE-scoped.

Two jsonl files produced by the Studio analysis pass (an upstream
normalization of ``studio_results_*_cte_or_subqueries.csv``):

    query_semantic_witnesses.jsonl   evidence about METRICS
    join_witnesses.jsonl             evidence about JOINS

Custody rule: these are INPUTS (they live in sources/, not the graph
store) — Meridian re-canonicalizes the SQL bytes itself and derives
conflicts/duplicates with its own machinery, so the upstream ``dedup``
and quality-report blocks are deliberately ignored. What IS consumed:

- the metric evidence fuses onto canonical identity via
  ``metric_ref = dmp:<metric_catalog_id>`` — same-id-same-SQL merges as
  corroboration, same-id-different-SQL lands as a second class in the
  same mgroup and the census flags it (retained, flagged, never
  overwriting), a novel id becomes a candidate;
- the full referenced SQL rides whole as a doc node;
- join witnesses become ``joins_via`` edges that are **scoped by
  default**: the observed join is between TRANSFORMED CTEs, so
  ``scope: scoped_only`` + preconditions ride to the index, the
  resolver, and the cards — never advertised as a raw-safe join.
  Same-table (temporal/self-join) patterns are counted, never edges.
"""

from __future__ import annotations

import json
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import table_id
from sahs.graph.quads import GraphDir, Prov, Quad
from sahs.loaders.records import ExpressionRecord, Quarantined

SOURCE = "studio_queries"              # → witness "studio"


def _first(value) -> str:
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def _short(physical: str) -> str:
    return str(physical or "").strip().split(".")[-1].lower()


def load_query_witnesses(path: Path) -> tuple[list[ExpressionRecord],
                                              list[Quarantined]]:
    records: list[ExpressionRecord] = []
    quarantined: list[Quarantined] = []
    for n, line in enumerate(
            Path(path).read_text(encoding="utf-8").split("\n"), 1):
        if not line.strip():
            continue
        ref = f"{Path(path).name}:{n}"
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            quarantined.append(Quarantined(
                source=SOURCE, category="parse_error",
                detail=f"unparsable witness line ({e.msg})",
                evidence_ref=ref))
            continue
        mid = str(row.get("metric_catalog_id")
                  or row.get("metricCatalogId") or "").strip()
        metric = row.get("metric") or {}
        sql_block = row.get("sql") or {}
        sql = str(sql_block.get("expression")
                  or row.get("sqlExpression") or "").strip()
        referenced = str(sql_block.get("referenced_query")
                         or row.get("referencedSqlQuery") or "").strip()
        if not sql and referenced:
            sql = referenced
        if not mid or not sql:
            quarantined.append(Quarantined(
                source=SOURCE, category="missing_field",
                detail="witness without "
                       + ("metric_catalog_id" if not mid else "SQL"),
                evidence_ref=ref))
            continue
        sql_tables = [str(t) for t in
                      (row.get("sql_referenced_tables") or [])]
        associated = [str(t) for t in
                      (row.get("associated_tables") or [])]
        lineage = row.get("table_lineage") or {}
        governance = row.get("governance") or {}
        hint = _short(_first(sql_tables) or _first(associated))
        records.append(ExpressionRecord(
            raw_sql=sql, kind="metric_expr", source=SOURCE,
            authority=Authority.MINED,
            # the SAME ref namespace the dmp loader mints — a matching
            # catalog id FUSES onto the canonical metric ("enrich, not
            # duplicate"); a novel id keys a candidate group that will
            # fuse the day the catalog adopts it
            metric_ref=f"dmp:{mid}",
            concept_label=str(metric.get("business_friendly_name")
                              or metric.get("name") or ""),
            table_hint=hint or None,
            first_seen=str(governance.get("created_at") or ""),
            last_seen=str(governance.get("updated_at") or ""),
            evidence_ref=ref,
            extra={
                "question_answered":
                    _first(metric.get("question_answered")),
                "description": metric.get("description"),
                "domain": _first(metric.get("domain")),
                "status": metric.get("status"),
                "author": governance.get("author"),
                "query_shape": row.get("query_semantics") or {},
                "quality_flags": row.get("quality_flags") or [],
                "tables_associated_not_referenced":
                    lineage.get("associated_not_referenced") or [],
                "referenced_query": referenced,
            }))
    return records, quarantined


def emit_join_witnesses(path: Path, graph: GraphDir,
                        crosswalk: Crosswalk, run_id: str) -> dict:
    """→ report. Scoped joins_via edges — the discipline pinned by the
    field: an equality observed between TRANSFORMED CTEs is evidence a
    relationship EXISTS, never that the raw tables join safely."""
    report = {"join_edges": 0, "pattern_only": 0,
              "join_out_of_scope": 0, "join_quarantined": 0}
    seen: set[tuple[str, str, str]] = set()
    for n, line in enumerate(
            Path(path).read_text(encoding="utf-8").split("\n"), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            report["join_quarantined"] += 1
            continue
        left = crosswalk.physical_for_short(
            _short(row.get("left_table")))
        right = crosswalk.physical_for_short(
            _short(row.get("right_table")))
        if left is None or right is None:
            report["join_out_of_scope"] += 1
            continue
        if left == right:
            # temporal/self-join CTE patterns are query patterns, not
            # physical relationships — counted, never an edge
            report["pattern_only"] += 1
            continue
        on = [f"{p.get('left_column')} {p.get('operator', '=')} "
              f"{p.get('right_column')}"
              for p in (row.get("predicates") or [])
              if isinstance(p, dict)]
        witness_block = row.get("witness") or {}
        applicability = row.get("applicability") or {}
        raw_safe = bool(row.get("safe_for_raw_table_join"))
        key = (left, right, ";".join(on))
        if key in seen:
            continue
        seen.add(key)
        props = {
            "on": on,
            "join_type": str(row.get("join_type") or ""),
            "scope": ("raw_safe" if raw_safe else "scoped_only"),
            "semantic_keys": row.get("semantic_keys") or [],
            "grain": str(applicability.get("grain") or ""),
            "purpose": str(applicability.get("purpose") or ""),
            "preconditions": applicability.get("preconditions") or [],
            "witness_metric":
                str(witness_block.get("metric_catalog_id") or ""),
            "confidence": str(row.get("confidence") or "observed"),
        }
        graph.append_edge(Quad(
            s=table_id(left), r="joins_via", o=table_id(right),
            props={k: v for k, v in props.items() if v},
            prov=Prov(source=SOURCE, run=run_id, witness="studio",
                      support=1,
                      evidence=f"{Path(path).name}:"
                               f"{row.get('join_id') or n}")))
        report["join_edges"] += 1
    return report
