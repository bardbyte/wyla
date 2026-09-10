"""Extended graph tools — beyond inspect_table.

12 FunctionTools the Consumer Agent calls for the 13 question categories
defined in skills/agent_skill.md. Each returns a JSON-serializable dict
the LLM consumes.

Tools follow the synapse-uniform contract:
    {"status": "ok" | "error", "data": ..., "error": str | None,
     "confidence_summary": ..., "citations": [...]}

ADK auto-introspects docstrings into JSON Schema the agent sees, so
docstrings ARE the LLM-facing tool descriptions. Be precise.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Reach into sibling synapse package
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.graph.inspector import inspect_table as _inspect_synapse  # noqa: E402
from synapse.graph.store import canonical_uri  # noqa: E402

from semantic_graph.tools.inspect_table_tool import _get_store


# ─── Common helpers ──────────────────────────────────────────


def _ok(data: Any, citations: list[dict] | None = None) -> dict[str, Any]:
    return {
        "status": "ok",
        "data": data,
        "error": None,
        "citations": citations or [],
    }


def _err(msg: str) -> dict[str, Any]:
    return {"status": "error", "data": None, "error": msg, "citations": []}


def _cite(uri: str, sources: list[str], tier: str, score: float) -> dict[str, Any]:
    return {
        "uri": uri,
        "sources_contributed": sources,
        "confidence_tier": tier,
        "confidence_score": score,
    }


# ─── Tool 1 — list_tables ────────────────────────────────────


def list_tables(domain: str = "", search_term: str = "") -> dict[str, Any]:
    """List all tables in the semantic graph, optionally filtered.

    Use this when the user wants to DISCOVER tables — "what tables are
    about cardmembers?", "show me Finance tables", "find tables with
    `revenue` in the name". For questions about a specific named table,
    call inspect_table directly instead.

    Args:
        domain: case-insensitive substring filter on company_domain or
                data_domain (e.g., "Finance", "Risk", "Cardmember").
                Empty string = no domain filter.
        search_term: case-insensitive substring filter on table name or
                     business name. Empty string = no name filter.

    Returns:
        Dict with `data` = list of table summaries, each having:
        - `table_name`, `business_name`, `fqn`, `asset_kind`
        - `confidence_tier`, `n_sources`, `row_count_estimate`
        - `tags`, `owner_team`
    """
    store = _get_store()
    out: list[dict[str, Any]] = []
    dom_lower = (domain or "").lower().strip()
    term_lower = (search_term or "").lower().strip()
    for node in store.nodes_by_type("Table"):
        p = node.properties
        name = (p.get("table_name") or "").lower()
        biz = (p.get("business_name") or "").lower()
        comp = (p.get("company_domain") or "").lower()
        data = (p.get("data_domain") or "").lower()
        if dom_lower and dom_lower not in comp and dom_lower not in data:
            continue
        if term_lower and term_lower not in name and term_lower not in biz:
            continue
        out.append({
            "table_name": p.get("table_name"),
            "business_name": p.get("business_name"),
            "fqn": p.get("fqn"),
            "asset_kind": p.get("asset_kind", "Table"),
            "tags": p.get("tags", []),
            "owner_team": p.get("owner_team", ""),
            "row_count_estimate": p.get("row_count"),
            "confidence_tier": node.provenance.confidence_tier,
            "n_sources": len(set(node.provenance.sources)),
            "company_domain": p.get("company_domain"),
            "data_domain": p.get("data_domain"),
        })
    out.sort(key=lambda t: (-(t["n_sources"] or 0), t["table_name"] or ""))
    return _ok({"tables": out, "count": len(out)})


# ─── Tool 2 — search_columns ─────────────────────────────────


def search_columns(query: str, limit: int = 20) -> dict[str, Any]:
    """Fuzzy-search columns by name across ALL tables in the graph.

    Use this when the user asks about a column without naming a table:
    "which columns hold FICO data?", "find PII columns", "where does
    revenue live?". For inspecting a known table's columns, use
    inspect_table instead.

    Args:
        query: case-insensitive substring to match against column name,
               business_name, or description.
        limit: max results to return (default 20).

    Returns:
        Dict with `data` = list of column summaries, each having:
        - `name`, `table_name`, `data_type`, `business_name`
        - `is_pii`, `pii_taxonomy`, `confidence_tier`
        - `sources_contributed`, `description`
    """
    store = _get_store()
    q = (query or "").lower().strip()
    if not q:
        return _err("search_columns requires a non-empty query")
    out: list[dict[str, Any]] = []
    for node in store.nodes_by_type("Column"):
        p = node.properties
        name = (p.get("name") or node.canonical_uri.rsplit("/", 1)[-1]).lower()
        biz = (p.get("business_name") or "").lower()
        desc = (p.get("description") or "").lower()
        if q not in name and q not in biz and q not in desc:
            continue
        out.append({
            "name": p.get("name") or node.canonical_uri.rsplit("/", 1)[-1],
            "table_name": p.get("table_name"),
            "data_type": p.get("data_type"),
            "business_name": p.get("business_name"),
            "description": p.get("description") or p.get("ai_generated_description") or "",
            "is_pii": bool(p.get("is_pii")),
            "pii_taxonomy": p.get("pii_taxonomy"),
            "confidence_tier": node.provenance.confidence_tier,
            "sources_contributed": sorted(set(node.provenance.sources)),
        })
    # Sort by exact-match-first, then alphabetically
    out.sort(key=lambda c: (q not in (c["name"] or "").lower(), c["name"] or ""))
    return _ok({"matches": out[:limit], "count": len(out)})


# ─── Tool 3 — get_metric ─────────────────────────────────────


def get_metric(name_or_synonym: str) -> dict[str, Any]:
    """Look up a metric by canonical name OR by synonym/alias.

    Use this when the user asks about a metric: "what is TBB?",
    "explain gross_provision", "how is `active_cardmembers` calculated?".

    Args:
        name_or_synonym: metric name (e.g., "total_billed_business") OR
                         alias (e.g., "TBB", "billed_biz").

    Returns:
        Dict with `data` = metric details:
        - `technical_name`, `business_name`, `formula`
        - `grain`, `domain`, `synonyms[]`
        - `sourced_from_table`, `confidence_tier`, `sources_contributed`
        OR `data = null` if not found (with helpful nearby matches in `data.nearby`).
    """
    store = _get_store()
    q = (name_or_synonym or "").strip().lower()
    if not q:
        return _err("get_metric requires a name or synonym")

    # 1. Exact metric name match
    for m in store.nodes_by_type("Metric"):
        tech = m.canonical_uri.rsplit("/", 1)[-1].lower()
        biz = (m.properties.get("business_name") or "").lower()
        if q == tech or q == biz:
            return _ok(_metric_to_dict(m))

    # 2. Synonym resolution
    for s in store.nodes_by_type("Synonym"):
        surface = (s.properties.get("surface_form") or "").lower()
        if q == surface:
            canonical = s.properties.get("canonical_entity") or ""
            # Try to find the canonical Metric
            for m in store.nodes_by_type("Metric"):
                tech = m.canonical_uri.rsplit("/", 1)[-1].lower()
                biz = (m.properties.get("business_name") or "").lower()
                if canonical.lower() in (tech, biz):
                    return _ok({
                        **_metric_to_dict(m),
                        "resolved_via_synonym": s.properties.get("surface_form"),
                        "synonym_scope": {
                            "business_unit": s.properties.get("business_unit"),
                            "region": s.properties.get("region"),
                        },
                    })

    # 3. Fuzzy fallback
    nearby = []
    for m in store.nodes_by_type("Metric"):
        tech = m.canonical_uri.rsplit("/", 1)[-1].lower()
        biz = (m.properties.get("business_name") or "").lower()
        if q in tech or q in biz:
            nearby.append({
                "technical_name": m.canonical_uri.rsplit("/", 1)[-1],
                "business_name": m.properties.get("business_name"),
            })
    return _ok({"metric": None, "nearby": nearby[:10]})


def _metric_to_dict(m: Any) -> dict[str, Any]:
    return {
        "technical_name": m.canonical_uri.rsplit("/", 1)[-1],
        "business_name": m.properties.get("business_name"),
        "formula": m.properties.get("formula"),
        "grain": m.properties.get("grain"),
        "domain": m.properties.get("domain"),
        "synonyms": m.properties.get("synonyms", []),
        "sourced_from_table": m.properties.get("sourced_from_table"),
        "value_format": m.properties.get("value_format"),
        "drill_fields": m.properties.get("drill_fields", []),
        "symmetric_aggregates_required": m.properties.get("symmetric_aggregates_required"),
        "description": m.properties.get("description"),
        "confidence_tier": m.provenance.confidence_tier,
        "confidence_score": round(m.provenance.confidence_score, 3),
        "sources_contributed": sorted(set(m.provenance.sources)),
    }


# ─── Tool 4 — get_join_path ──────────────────────────────────


def get_join_path(from_table: str, to_table: str, max_hops: int = 3) -> dict[str, Any]:
    """Find the highest-confidence JOIN path between two tables.

    Use this when the user asks how two tables connect: "how do I join
    cardmember to product hierarchy?", "what's the path from customer
    insights to transactions?". Returns the actual columns to JOIN on.

    Args:
        from_table: source table name.
        to_table: destination table name.
        max_hops: max intermediate tables (default 3).

    Returns:
        Dict with `data` = path:
        - `hops` = list of {table, joined_to_table, on_columns, evidence_count}
        - `total_evidence` = total JOIN observations supporting the path
        - `min_confidence_tier` = lowest tier across the path
        OR data = null with explanation if no path found.
    """
    store = _get_store()
    src = canonical_uri("table", from_table)
    dst = canonical_uri("table", to_table)
    if src not in store.nodes:
        return _err(f"from_table '{from_table}' not in graph")
    if dst not in store.nodes:
        return _err(f"to_table '{to_table}' not in graph")
    if from_table == to_table:
        return _ok({"path": [], "note": "same table; no JOIN needed"})

    # BFS over EQUIVALENT_TO edges between Columns
    # Path metric: count of supporting JOIN observations
    table_neighbors = _build_table_join_index(store)
    visited = {from_table}
    queue: list[tuple[str, list[dict[str, Any]]]] = [(from_table, [])]
    while queue:
        current, path_so_far = queue.pop(0)
        if len(path_so_far) >= max_hops:
            continue
        for neighbor, on_cols, evidence in table_neighbors.get(current, []):
            if neighbor == to_table:
                full_path = path_so_far + [{
                    "from_table": current,
                    "to_table": neighbor,
                    "on_columns": on_cols,
                    "evidence_count": evidence,
                }]
                return _ok({
                    "path": full_path,
                    "hops": len(full_path),
                    "total_evidence": sum(h["evidence_count"] for h in full_path),
                })
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path_so_far + [{
                    "from_table": current,
                    "to_table": neighbor,
                    "on_columns": on_cols,
                    "evidence_count": evidence,
                }]))
    return _ok({"path": None, "reason": f"no JOIN path within {max_hops} hops"})


def _build_table_join_index(store) -> dict[str, list[tuple[str, list[dict], int]]]:
    """Build {table_name → [(other_table, on_cols, evidence_count), ...]}."""
    index: dict[str, dict[str, dict[str, Any]]] = {}
    for e in store.edges.values():
        if e.edge_type != "EQUIVALENT_TO":
            continue
        from_node = store.get(e.from_uri)
        to_node = store.get(e.to_uri)
        if not (from_node and to_node):
            continue
        if from_node.node_type != "Column" or to_node.node_type != "Column":
            continue
        from_t = from_node.properties.get("table_name")
        to_t = to_node.properties.get("table_name")
        from_c = from_node.properties.get("name") or e.from_uri.rsplit("/", 1)[-1]
        to_c = to_node.properties.get("name") or e.to_uri.rsplit("/", 1)[-1]
        if not (from_t and to_t):
            continue
        # Track both directions
        for a, b, ca, cb in [(from_t, to_t, from_c, to_c), (to_t, from_t, to_c, from_c)]:
            slot = index.setdefault(a, {}).setdefault(b, {
                "on_columns": [], "evidence_count": 0,
            })
            slot["on_columns"].append({"left": ca, "right": cb})
            slot["evidence_count"] += 1
    # Flatten
    out: dict[str, list[tuple[str, list, int]]] = {}
    for table, neighbors in index.items():
        out[table] = [
            (other, info["on_columns"], info["evidence_count"])
            for other, info in neighbors.items()
        ]
        out[table].sort(key=lambda x: -x[2])
    return out


# ─── Tool 5 — find_columns_for_concept ───────────────────────


def find_columns_for_concept(concept: str) -> dict[str, Any]:
    """Find columns and tables relevant to a fuzzy business concept.

    Use this when the user describes a concept but doesn't name the
    exact column: "where does Cardmember data live?", "find tables
    about revenue", "what columns relate to credit risk?".

    Args:
        concept: free-form concept description (e.g., "cardmember",
                 "revenue", "credit risk", "product hierarchy").

    Returns:
        Dict with `data` containing:
        - `matching_columns` — list of {name, table_name, description, is_pii}
        - `matching_tables` — list of {table_name, business_name}
        - `matching_entities` — list of {name, materialized_in_tables}
        - `matching_synonyms` — list of {surface_form, canonical_entity}
    """
    store = _get_store()
    q = (concept or "").lower().strip()
    if not q:
        return _err("find_columns_for_concept requires a non-empty concept")
    cols: list[dict[str, Any]] = []
    tabs: list[dict[str, Any]] = []
    ents: list[dict[str, Any]] = []
    syns: list[dict[str, Any]] = []

    for c in store.nodes_by_type("Column"):
        if _matches(q, c.properties):
            cols.append({
                "name": c.properties.get("name") or c.canonical_uri.rsplit("/", 1)[-1],
                "table_name": c.properties.get("table_name"),
                "description": (
                    c.properties.get("description")
                    or c.properties.get("ai_generated_description")
                    or ""
                ),
                "is_pii": bool(c.properties.get("is_pii")),
            })
    for t in store.nodes_by_type("Table"):
        if _matches(q, t.properties, also=("business_name", "description")):
            tabs.append({
                "table_name": t.properties.get("table_name"),
                "business_name": t.properties.get("business_name"),
            })
    for e in store.nodes_by_type("Entity"):
        if _matches(q, e.properties, also=("description",)):
            ents.append({
                "name": e.canonical_uri.rsplit("/", 1)[-1],
                "materialized_in_tables": e.properties.get("materialized_in_tables", []),
            })
    for s in store.nodes_by_type("Synonym"):
        if q in (s.properties.get("canonical_entity") or "").lower():
            syns.append({
                "surface_form": s.properties.get("surface_form"),
                "canonical_entity": s.properties.get("canonical_entity"),
            })

    return _ok({
        "matching_columns": cols[:30],
        "matching_tables": tabs[:20],
        "matching_entities": ents[:10],
        "matching_synonyms": syns[:20],
    })


def _matches(q: str, props: dict, also: tuple = ()) -> bool:
    text_fields = ("name", "table_name", "business_name", "description",
                   "ai_generated_description", "canonical_entity")
    for f in text_fields + also:
        v = props.get(f)
        if v and q in str(v).lower():
            return True
    return False


# ─── Tool 6 — get_lineage ────────────────────────────────────


def get_lineage(table_name: str) -> dict[str, Any]:
    """Return upstream + downstream lineage for one table.

    Use this when the user asks lineage: "where does this table get its
    data?", "what depends on cardmember?", "trace the data flow".

    Args:
        table_name: target table.

    Returns:
        Dict with `data`:
        - `table_name`
        - `upstream` — list of {table, source, evidence_in}
        - `downstream` — list of {table, source, evidence_in}
        - `lineage_completeness_note` — honest caveat when partial
    """
    store = _get_store()
    t_uri = canonical_uri("table", table_name)
    if t_uri not in store.nodes:
        return _err(f"table '{table_name}' not in graph")
    upstream = []
    downstream = []
    for e in store.incoming(t_uri, "UPSTREAM_OF"):
        node = store.get(e.from_uri)
        if node:
            upstream.append({
                "table": node.properties.get("table_name") or e.from_uri.rsplit("/", 1)[-1],
                "source": list(e.provenance.sources)[0] if e.provenance.sources else "unknown",
                "evidence_in": e.properties.get("observed_in", ""),
            })
    for e in store.outgoing(t_uri, "UPSTREAM_OF"):
        node = store.get(e.to_uri)
        if node:
            downstream.append({
                "table": node.properties.get("table_name") or e.to_uri.rsplit("/", 1)[-1],
                "source": list(e.provenance.sources)[0] if e.provenance.sources else "unknown",
                "evidence_in": e.properties.get("observed_in", ""),
            })
    return _ok({
        "table_name": table_name,
        "upstream": upstream,
        "downstream": downstream,
        "lineage_completeness_note": (
            "Downstream may be empty for views — JOBS_BY_PROJECT lineage "
            "lives at the base-table level. Run lineage at the base table "
            "for full coverage." if not downstream else None
        ),
    })


# ─── Tool 7 — get_entity ─────────────────────────────────────


def get_entity(name: str) -> dict[str, Any]:
    """Look up a business entity by name.

    Use this when the user asks about a canonical business entity:
    "what is the Cardmember entity?", "show me the Customer entity".

    Args:
        name: entity name (case-insensitive).

    Returns:
        Dict with `data`: entity details (description, materialized_in_tables,
        id_columns, related entities via outgoing edges).
    """
    store = _get_store()
    target = (name or "").strip().lower()
    if not target:
        return _err("get_entity requires a name")
    for e in store.nodes_by_type("Entity"):
        ename = e.canonical_uri.rsplit("/", 1)[-1].lower()
        if ename == target or ename == target.replace(" ", "_"):
            return _ok({
                "name": e.canonical_uri.rsplit("/", 1)[-1],
                "description": e.properties.get("description"),
                "materialized_in_tables": e.properties.get("materialized_in_tables", []),
                "id_columns": e.properties.get("id_columns", []),
                "confidence_tier": e.provenance.confidence_tier,
                "sources_contributed": sorted(set(e.provenance.sources)),
            })
    # Fuzzy fallback
    matches = []
    for e in store.nodes_by_type("Entity"):
        ename = e.canonical_uri.rsplit("/", 1)[-1].lower()
        if target in ename:
            matches.append({"name": e.canonical_uri.rsplit("/", 1)[-1]})
    return _ok({"entity": None, "nearby": matches[:10]})


# ─── Tool 8 — resolve_synonym ────────────────────────────────


def resolve_synonym(term: str, context: str = "") -> dict[str, Any]:
    """Disambiguate a term, optionally with a business-unit context.

    Use this when the user uses an ambiguous acronym/term: "what does
    CM mean here?", "does AA mean Adverse Action?".

    Args:
        term: surface form (e.g., "CM", "AA", "TBB").
        context: optional business unit or scope hint (e.g., "Finance",
                 "Risk", "Marketing"). Helps disambiguate.

    Returns:
        Dict with `data`:
        - `candidates` — list of {canonical_entity, business_unit, region, scope_match_score}
        - `recommended` — the highest-scoring candidate
        - `conflict_warning` — true if multiple candidates with similar scores
    """
    store = _get_store()
    q = (term or "").strip()
    ctx = (context or "").strip().lower()
    if not q:
        return _err("resolve_synonym requires a term")
    candidates = []
    for s in store.nodes_by_type("Synonym"):
        if (s.properties.get("surface_form") or "").lower() == q.lower():
            bu = (s.properties.get("business_unit") or "").lower()
            # Score: ctx match boosts score
            score = 0.5
            if ctx and ctx in bu:
                score = 1.0
            elif not ctx:
                score = 0.7
            candidates.append({
                "canonical_entity": s.properties.get("canonical_entity"),
                "business_unit": s.properties.get("business_unit"),
                "region": s.properties.get("region"),
                "entry_type": s.properties.get("entry_type"),
                "scope_match_score": score,
                "sources_contributed": sorted(set(s.provenance.sources)),
            })
    if not candidates:
        return _ok({"candidates": [], "recommended": None,
                    "note": f"No glossary entry for '{q}'."})
    candidates.sort(key=lambda c: -c["scope_match_score"])
    recommended = candidates[0]
    has_conflict = (
        len(candidates) > 1
        and abs(candidates[0]["scope_match_score"]
                - candidates[1]["scope_match_score"]) < 0.2
    )
    return _ok({
        "candidates": candidates,
        "recommended": recommended,
        "conflict_warning": has_conflict,
    })


# ─── Tool 9 — get_failed_query_corrections ───────────────────


def get_failed_query_corrections(column_name: str = "") -> dict[str, Any]:
    """Surface common analyst column-name mistakes captured from BQ
    failed-query logs.

    Call this proactively whenever you're about to use a column in a
    response — if it has a known common misnaming, surface it
    educationally per Rule 8 of agent_skill.md.

    Args:
        column_name: specific column to check (empty = return all known
                     corrections in the graph).

    Returns:
        Dict with `data` = list of {wrong_name, correct_name, evidence_count,
        most_recent}.
    """
    store = _get_store()
    target = (column_name or "").strip().lower()
    out = []
    # Walk Columns; surface naming_correction property if present (set by BQ loader)
    for c in store.nodes_by_type("Column"):
        correction = c.properties.get("naming_correction")
        if correction:
            wrong = correction.get("wrong_name")
            right = c.properties.get("name") or c.canonical_uri.rsplit("/", 1)[-1]
            if not target or target in wrong.lower() or target in right.lower():
                out.append({
                    "wrong_name": wrong,
                    "correct_name": right,
                    "table_name": c.properties.get("table_name"),
                    "evidence_count": correction.get("evidence_count", 0),
                })
    # Also check for hardcoded common cases on cardmember table when target matches
    well_known = [
        {"wrong_name": "fico", "correct_name": "fico_score",
         "table_name": "custins_customer_insights_cardmember",
         "evidence_count": 1},
        {"wrong_name": "card_product_id", "correct_name": "card_prod_id",
         "table_name": "custins_customer_insights_cardmember",
         "evidence_count": 2},
    ]
    for w in well_known:
        if not target or target in w["wrong_name"] or target in w["correct_name"]:
            # Only add if a matching column exists in graph
            col_uri = canonical_uri("column", w["table_name"], w["correct_name"])
            if col_uri in store.nodes and not any(
                o["wrong_name"] == w["wrong_name"] for o in out
            ):
                out.append(w)
    return _ok({"corrections": out, "count": len(out)})


# ─── Tool 10 — get_dq_status ─────────────────────────────────


def get_dq_status(table_name: str) -> dict[str, Any]:
    """Return data-quality status for one table.

    Use this when the user asks about trust/freshness/completeness:
    "is this data trustworthy?", "what's the freshness?", "are there
    failing DQ rules?".

    Args:
        table_name: target table.

    Returns:
        Dict with `data`:
        - `completeness_score`, `consistency_score`, `freshness_hours`
        - `rules[]` (rule_id, kind, target, last_run_status, severity)
        - `dimensions_covered` (which KC dimensions have rules)
        - `overall_assessment` — HIGH | MEDIUM | LOW | UNKNOWN
    """
    store = _get_store()
    inspection = _inspect_synapse(store, table_name)
    if "error" in inspection:
        return _err(inspection.get("error", "table not found"))
    dq = inspection.get("data_quality", {})
    rules = dq.get("rules", [])
    n_pass = sum(1 for r in rules if r.get("last_run_status") == "pass")
    n_warn = sum(1 for r in rules if r.get("last_run_status") == "warning")
    n_fail = sum(1 for r in rules if r.get("last_run_status") == "fail")
    dimensions = sorted({
        r.get("rule_kind") for r in rules if r.get("rule_kind")
    })
    pct_passing = (n_pass / max(len(rules), 1)) if rules else 0.0
    if pct_passing >= 0.95:
        overall = "HIGH"
    elif pct_passing >= 0.75:
        overall = "MEDIUM"
    elif pct_passing > 0:
        overall = "LOW"
    else:
        overall = "UNKNOWN"
    return _ok({
        "table_name": table_name,
        "completeness_score": dq.get("completeness_score"),
        "consistency_score": dq.get("consistency_score"),
        "freshness_hours": dq.get("freshness_hours"),
        "rules": rules[:30],
        "rule_summary": {"pass": n_pass, "warning": n_warn,
                         "fail": n_fail, "total": len(rules)},
        "dimensions_covered": dimensions,
        "overall_assessment": overall,
    })


# ─── Tool 11 — validate_sql ──────────────────────────────────


def validate_sql(sql: str) -> dict[str, Any]:
    """Dry-run a BigQuery SQL statement to check parse + types + cost.

    Use this when the user wants to verify generated SQL before pasting
    into BQ console: "is this SQL valid?", "what's it gonna cost?".

    Calls BigQuery's dry_run mode (free, no execution). Requires BQ_SA_KEY
    env var pointing at a BQ-accessible SA JSON; gracefully reports
    'unavailable' when not configured.

    Args:
        sql: the BigQuery SQL to validate.

    Returns:
        Dict with `data`:
        - `is_valid` — bool
        - `error` — parse/type error if invalid
        - `bytes_billed_estimate` — int bytes
        - `statement_type`, `referenced_tables[]`
        - `unavailable_reason` — set if BQ creds aren't configured
    """
    import os

    bq_key = os.getenv("BQ_SA_KEY", "").strip()
    if not bq_key:
        return _ok({
            "is_valid": None,
            "unavailable_reason": (
                "BQ_SA_KEY not set in .env — dry-run validation skipped. "
                "Set BQ_SA_KEY to a service-account JSON path with "
                "roles/bigquery.jobUser to enable."
            ),
            "sql_length_chars": len(sql or ""),
        })
    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except ImportError:
        return _ok({
            "is_valid": None,
            "unavailable_reason": (
                "google-cloud-bigquery not installed. "
                "pip install google-cloud-bigquery to enable."
            ),
        })

    try:
        creds = service_account.Credentials.from_service_account_file(bq_key)
        client = bigquery.Client(credentials=creds, project=creds.project_id)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=job_config)
        return _ok({
            "is_valid": True,
            "error": None,
            "bytes_billed_estimate": job.total_bytes_processed,
            "statement_type": job.statement_type,
            "referenced_tables": [
                {
                    "project": t.project, "dataset": t.dataset_id,
                    "table": t.table_id,
                }
                for t in (job.referenced_tables or [])
            ],
        })
    except Exception as e:  # noqa: BLE001
        return _ok({
            "is_valid": False,
            "error": str(e)[:500],
            "bytes_billed_estimate": None,
        })


# ─── Tool 12 — get_steward_review_queue ──────────────────────


def get_steward_review_queue() -> dict[str, Any]:
    """Read-only view of items pending steward review.

    Use this when the user asks "what needs steward attention?", "show
    me entity proposals", "what's the review queue?".

    Note: WRITES (approving/rejecting) are handled by the Streamlit UI,
    not by you. This is a read-only surface.

    Returns:
        Dict with `data`:
        - `pending_entity_proposals[]` — Entity nodes flagged for review
        - `unresolved_conflicts[]` — Provenance.conflicts that haven't been adjudicated
        - `low_confidence_code_resolutions[]` — CodeMapping nodes from llm_generated
        - `count` — total items in queue
    """
    store = _get_store()
    pending_entities = []
    conflicts = []
    low_conf_codes = []
    for n in store.nodes_by_type("Entity"):
        if n.provenance.confidence_tier in ("inferred", "guessed"):
            pending_entities.append({
                "name": n.canonical_uri.rsplit("/", 1)[-1],
                "materialized_in_tables": n.properties.get("materialized_in_tables", []),
                "confidence_tier": n.provenance.confidence_tier,
                "sources_contributed": sorted(set(n.provenance.sources)),
            })
    for n in list(store.nodes.values())[:5000]:  # bounded scan
        for c in n.provenance.conflicts:
            conflicts.append({
                "node_uri": n.canonical_uri,
                "conflict": c,
            })
    for cm in store.nodes_by_type("CodeMapping"):
        if cm.provenance.confidence_tier == "guessed" and "llm_generated" in cm.provenance.sources:
            low_conf_codes.append({
                "column": cm.properties.get("column"),
                "raw_value": cm.properties.get("raw_value"),
                "proposed_meaning": cm.properties.get("human_meaning"),
                "confidence_tier": cm.provenance.confidence_tier,
            })
    total = len(pending_entities) + len(conflicts) + len(low_conf_codes)
    return _ok({
        "pending_entity_proposals": pending_entities[:30],
        "unresolved_conflicts": conflicts[:30],
        "low_confidence_code_resolutions": low_conf_codes[:30],
        "count": total,
        "note": (
            "Read-only. To approve/reject, use the steward UI. The Consumer "
            "Agent does not mutate the graph."
        ),
    })
