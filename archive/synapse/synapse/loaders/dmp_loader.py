"""Data Marketplace metric catalog → ``dmp`` witness facts on the graph.

The DMP export is the CURATED counterpart of the mined measures catalog:
a small set of author-owned metric definitions (name, business-friendly
name, description, the question it answers, a SQL expression, and the
data products it lives on). Authorship is the trust signal, so it joins
the curated family at weight 5 — and like every connector it fuses as
one more witness, never as an authority: gap-fill merge only, one
export = one testimony per fact.

Input JSON — ``{"metric_catalog": [...]}`` or a bare list. Per metric
the loader understands:

    metricCatalogId              → dmp_id
    metricName                   → the metric URI token
    businessFriendlyMetricName   → business_name (fallback metricName)
    metricDescription            → description
    questionAnswered             → question_answered
    metricDomain                 → domain; lineOfBusiness → line_of_business
    sqlExpression                → formula
    referencedSqlQuery           table-resolution fallback (FROM/JOIN scan)
    associatedDataProductNames[] data products ≈ tables (primary path)
    author / authorId / status / createdAt / updatedAt

Table resolution: data-product names first; when a metric names none,
a lenient FROM/JOIN scan over ``referencedSqlQuery`` (corpus-pass
style, no sqlglot). Metrics resolving to no table are skipped WITH a
reason — nothing is invented. Resolved tables the graph doesn't know
are minted at the honest single-witness floor, flagged ``is_in_dmp``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from synapse.graph.store import (
    GraphStore, canonical_uri, normalize_table_name,
)

SOURCE = "dmp"

_FROM_JOIN = re.compile(
    r"\b(?:from|join)\s+[`\"\[]?([A-Za-z0-9_$][A-Za-z0-9_$.-]*)",
    re.IGNORECASE)
_NOT_TABLES = {"select", "unnest", "lateral", "values"}


def _resolve_table(name: str, aliases: dict[str, str] | None) -> str:
    norm = normalize_table_name(str(name))
    if aliases:
        return aliases.get(norm, norm)
    return norm


def _tables_from_sql(sql: str, aliases: dict[str, str] | None) -> list[str]:
    """Lenient FROM/JOIN table scan — fallback only, never authoritative."""
    out: list[str] = []
    for match in _FROM_JOIN.finditer(sql or ""):
        table = _resolve_table(match.group(1), aliases)
        if table and table not in _NOT_TABLES and table not in out:
            out.append(table)
    return out


def _fill_only(store: GraphStore, uri: str,
               props: dict[str, Any]) -> dict[str, Any]:
    """Gap-filling merge: DMP wording fills blanks, never replaces.

    In a full build the internal metric-catalog CSV runs earlier (order
    encodes authority); in an append everything pre-existing came first.
    Either way a key the node already holds non-empty is kept — the
    ``dmp`` witness lands regardless and the tier recomputes.
    """
    node = store.get(uri)
    if node is None:
        return props
    return {k: v for k, v in props.items() if not node.properties.get(k)}


def load_dmp_export(
    store: GraphStore,
    export_path: "Path | str",
    *,
    aliases: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fuse a DMP metric-catalog export into the store as ``dmp`` facts.

    Metric URIs use the same ``metric/<table>/<name>`` scheme as the
    curated CSV and mined passes, so the same metric seen by several
    catalogs FUSES into one node whose tier climbs with each distinct
    witness. Returns ``{"metrics", "tables_minted", "skipped"}``.
    """
    path = Path(export_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = raw.get("metric_catalog") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return {"metrics": 0, "tables_minted": 0,
                "skipped": ["export is neither {'metric_catalog': [...]} "
                            "nor a list of metrics"]}

    n_metrics = n_tables_minted = 0
    skipped: list[str] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            skipped.append(f"metric[{i}]: not an object")
            continue
        name = str(row.get("metricName") or "").strip()
        if not name:
            skipped.append(f"metric[{i}]: no metricName")
            continue

        product_names = [str(p) for p in
                         (row.get("associatedDataProductNames") or [])
                         if str(p).strip()]
        tables = []
        for p in product_names:
            t = _resolve_table(p, aliases)
            if t and t not in tables:
                tables.append(t)
        if not tables:
            tables = _tables_from_sql(
                str(row.get("referencedSqlQuery") or ""), aliases)
        if not tables:
            skipped.append(
                f"metric '{name}': no data products and no FROM/JOIN "
                "tables in referencedSqlQuery")
            continue

        formula = str(row.get("sqlExpression") or "").strip()
        for table in tables:
            t_uri = canonical_uri("table", table)
            if store.get(t_uri) is None:
                n_tables_minted += 1
            store.upsert_node(
                "Table", t_uri,
                _fill_only(store, t_uri,
                           {"table_name": table, "is_in_dmp": True}),
                source=SOURCE)

        primary = tables[0]
        m_uri = canonical_uri("metric", primary, name)
        props: dict[str, Any] = {
            "business_name": str(row.get("businessFriendlyMetricName")
                                 or name),
            "formula": formula,
            "description": str(row.get("metricDescription") or ""),
            "question_answered": str(row.get("questionAnswered") or ""),
            "domain": str(row.get("metricDomain") or ""),
            "line_of_business": str(row.get("lineOfBusiness") or ""),
            "author": str(row.get("author") or ""),
            "author_id": str(row.get("authorId") or ""),
            "catalog_status": str(row.get("status") or ""),
            "dmp_id": str(row.get("metricCatalogId") or ""),
            "data_products": product_names,
            "sourced_from_table": primary,
            "created_at": str(row.get("createdAt") or ""),
            "updated_at": str(row.get("updatedAt") or ""),
        }
        props = {k: v for k, v in props.items()
                 if v is not None and v != "" and v != []}
        store.upsert_node("Metric", m_uri,
                          _fill_only(store, m_uri, props), source=SOURCE)
        n_metrics += 1
        for table in tables:
            store.upsert_edge(
                "COMPUTED_FROM", m_uri, canonical_uri("table", table),
                {"formula": formula}, source=SOURCE)

    return {"metrics": n_metrics, "tables_minted": n_tables_minted,
            "skipped": skipped}
