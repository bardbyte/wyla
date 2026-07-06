"""Inspector — the read API the UI / agentic consumer hits.

`inspect_table(store, "custins_customer_insights_cardmember")` returns
ONE traversal that lays out everything we know about that table, broken
down by which of the 7 sources contributed each fact. The dict shape is
designed to be rendered directly by a Streamlit / MCP-tool surface
without any further transformation.

This is the "click to expand into 7 sources" panel data.
"""

from __future__ import annotations

from typing import Any

from synapse.graph.store import GraphStore, canonical_uri


def inspect_table(store: GraphStore, table_name: str) -> dict[str, Any]:
    """Return the full structured view of a single table.

    Output shape:
        {
          identity: {table, fqn, table_type, is_in_dmp, domains},
          per_source_view: {                       # the "7 source" breakdown
              mdm:             {...},
              corpus:          {...},
              bq:              {...},
              baseline_lookml: {...},
              glossary:        {...},
              metric_catalog:  {...},
              table_catalog:   {...},
              usage:           {...},
          },
          fused_view: {                            # the calibrated graph view
              confidence_tier, confidence_score,
              n_sources_agree, conflicts,
          },
          columns: [...],                          # per-column 7-source breakdown
          metrics: [...],                          # metrics sourced from this table
          related_tables: [...],                   # via JOIN keys + declared FKs
          usage: {total_queries, top_users, peak_hours},
          governance: {has_pii, pii_columns, owner_team},
          data_quality: {freshness_hours, completeness, consistency},
          code_resolutions: [...],                 # 005 → Platinum mappings
        }
    """
    t_uri = canonical_uri("table", table_name)
    node = store.get(t_uri)
    if node is None:
        return {
            "error": "table_not_found",
            "table": table_name,
            "available": [
                n.properties.get("table_name")
                for n in store.nodes_by_type("Table")
            ],
        }

    props = node.properties
    prov = node.provenance

    # ── identity ──
    identity = {
        "table": table_name,
        "fqn": props.get("fqn"),
        "is_in_dmp": props.get("is_in_dmp"),
        "company_domain": props.get("company_domain"),
        "data_domain": props.get("data_domain"),
        "business_name": props.get("business_name"),
        "description": props.get("description"),
        "asset_kind": props.get("asset_kind", "Table"),
        "tags": props.get("tags", []),
    }

    # ── per-source view ──
    per_source = _build_per_source_view(store, node)

    # ── fused view ──
    fused = {
        "confidence_tier": prov.confidence_tier,
        "confidence_score": round(prov.confidence_score, 3),
        "n_sources_agree": len(set(prov.sources)),
        "sources_contributed": sorted(set(prov.sources)),
        "evidence_count": sum(prov.evidence_count_by_source.values()),
        "first_observed_at": prov.first_observed_at,
        "last_observed_at": prov.last_observed_at,
        "conflicts": prov.conflicts,
    }

    # ── columns (per-column 7-source breakdown) ──
    columns = _columns_for_table(store, t_uri)

    # ── metrics sourced from this table ──
    metrics = _metrics_for_table(store, table_name)

    # ── related tables (via JOIN observations) ──
    related = _related_tables(store, t_uri, table_name)

    # ── usage ──
    usage = {
        "total_queries_observed": props.get("total_queries_observed", 0),
        "top_users": props.get("top_users", []),
        "peak_query_hours": props.get("peak_query_hours", []),
    }

    # ── governance ──
    pii_cols = [c for c in columns if c.get("is_pii")]
    governance = {
        "has_pii": bool(pii_cols),
        "pii_columns": [
            {"name": c["name"], "pii_taxonomy": c.get("pii_taxonomy")}
            for c in pii_cols
        ],
        "owner_team": props.get("owner_team", ""),
        # crawler-era spine facts (empty on legacy blobs)
        "business_unit": props.get("business_unit", ""),
        "dataset_parent_id": props.get("dataset_parent_id", ""),
        "lifecycle_status": props.get("lifecycle_status", ""),
        "pipeline_name": props.get("pipeline_name", ""),
        "is_decommissioned": props.get("is_decommissioned", False),
    }

    # ── data quality (derived from profiling + DQ rules) ──
    data_quality = _compute_quality(node, columns)
    data_quality["rules"] = _dq_rules_for_table(store, t_uri, columns)

    # ── lineage (Dataplex Catalog parallel) ──
    lineage = _lineage_for_table(store, t_uri)

    # ── code resolutions for this table's columns ──
    code_resolutions = _code_resolutions_for_table(store, table_name)

    return {
        "identity": identity,
        "per_source_view": per_source,
        "fused_view": fused,
        "columns": columns,
        "metrics": metrics,
        "related_tables": related,
        "lineage": lineage,
        "usage": usage,
        "governance": governance,
        "data_quality": data_quality,
        "code_resolutions": code_resolutions,
    }


# ─── Per-source view extraction ──────────────────────────────


def _build_per_source_view(store: GraphStore, node: Any) -> dict[str, Any]:
    """Project the Table node's facts BY SOURCE — this is what the
    'click to expand into 7 sources' panel renders. Each block answers:
    'what does THIS source independently tell us about this table?'

    The actual per-source facts live in `node.properties` (merged
    across sources). The split here is by SOURCE'S DOMAIN OF AUTHORITY,
    not by which source last wrote what — because the user needs to
    see what each source is RESPONSIBLE FOR.
    """
    props = node.properties
    prov = node.provenance
    sources = set(prov.sources)
    counts = prov.evidence_count_by_source

    def _has(source: str) -> bool:
        return source in sources

    return {
        "mdm": {
            "contributed": _has("mdm"),
            "evidence_count": counts.get("mdm", 0),
            "table_business_name": props.get("business_name"),
            "description": props.get("description"),
            "owner_team": props.get("owner_team"),
            "data_category": props.get("data_domain"),
            "partition_field": props.get("partition_field"),
            "row_count_estimate": props.get("row_count"),
        },
        "corpus": {
            "contributed": _has("corpus"),
            "evidence_count": counts.get("corpus", 0),
            "queries_touching": prov.evidence_count_by_source.get("corpus", 0),
            "note": (
                "Tribal knowledge: SQL extraction emits JOIN ON equivalences, "
                "metric formulas, GROUP BY dimensions, WHERE filters, and "
                "CASE WHEN code mappings. See the 'columns' and 'metrics' "
                "blocks for derived facts."
            ),
        },
        "bq": {
            "contributed": _has("bq"),
            "evidence_count": counts.get("bq", 0),
            "actual_row_count": props.get("row_count"),
            "freshness": props.get("last_modified"),
            "partition_field_confirmed": props.get("partition_field"),
            "clustering_fields": props.get("clustering_fields", []),
            "note": (
                "Ground truth — INFORMATION_SCHEMA + Phase B/C profiling. "
                "Per-column cardinality, null fractions, distinct values, "
                "policy tags live on each Column node."
            ),
        },
        "baseline_lookml": {
            "contributed": _has("baseline_lookml"),
            "evidence_count": counts.get("baseline_lookml", 0),
            "view_exists": _has("baseline_lookml"),
            "note": (
                "Human-vouched primary_key declarations and sql_aliases "
                "promote columns to grounded confidence."
            ),
        },
        "glossary": {
            "contributed": False,
            "evidence_count": 0,
            "note": "Glossary contributes at the Synonym / Acronym layer, "
                    "not table-level. See 'metrics[].synonyms' and any "
                    "table-name acronyms below.",
        },
        "metric_catalog": {
            "contributed": _has("metric_catalog"),
            "evidence_count": counts.get("metric_catalog", 0),
            "note": (
                "Metrics sourced from this table (see 'metrics' below) carry "
                "human-vouched business definitions + formulas + grain."
            ),
        },
        "table_catalog": {
            "contributed": _has("table_catalog"),
            "evidence_count": counts.get("table_catalog", 0),
            "is_in_dmp": props.get("is_in_dmp"),
            "company_domain": props.get("company_domain"),
            "data_domain": props.get("data_domain"),
        },
        "usage": {
            "contributed": _has("usage"),
            "evidence_count": counts.get("usage", 0),
            "total_queries_observed": props.get("total_queries_observed", 0),
            "top_users_count": len(props.get("top_users") or []),
            "peak_query_hours": props.get("peak_query_hours", []),
        },
        # ─── Dataplex parallels ───
        "dq_engine": {
            "contributed": _has("dq_engine"),
            "evidence_count": counts.get("dq_engine", 0),
            "note": (
                "Auto-DQ-style rule evaluator. See 'data_quality.rules' for "
                "the per-rule pass/fail breakdown. Mirrors Dataplex Auto DQ."
            ),
        },
        "llm_generated": {
            "contributed": _has("llm_generated"),
            "evidence_count": counts.get("llm_generated", 0),
            "note": (
                "AI-suggested column descriptions (Knowledge Catalog parallel). "
                "Surfaced on each column as 'ai_generated_description'. Held "
                "at low confidence until corroborated by MDM or human review."
            ),
        },
    }


# ─── Per-column 7-source breakdown ───────────────────────────


def _columns_for_table(store: GraphStore, t_uri: str) -> list[dict[str, Any]]:
    col_edges = store.outgoing(t_uri, "CONTAINS")
    out: list[dict[str, Any]] = []
    for edge in col_edges:
        c = store.get(edge.to_uri)
        if not c:
            continue
        cp = c.properties
        prov = c.provenance
        out.append({
            "name": c.canonical_uri.split("/")[-1],
            "data_type": cp.get("data_type"),
            "description": cp.get("description"),
            "business_name": cp.get("business_name"),
            "is_primary": cp.get("is_primary", False),
            "is_dedupe_key": cp.get("is_dedupe_key", False),
            "is_partitioning": cp.get("is_partitioning", False),
            "cluster_position": cp.get("cluster_position"),
            # Profiling
            "cardinality_bucket": cp.get("cardinality_bucket", "unknown"),
            "approx_distinct": cp.get("approx_distinct"),
            "null_fraction": cp.get("null_fraction"),
            "distinct_sample": cp.get("distinct_sample", [])[:5],
            # Usage
            "reference_count": cp.get("reference_count", 0),
            "is_filter": cp.get("is_filter", False),
            "is_group_by": cp.get("is_group_by", False),
            "is_join_key": cp.get("is_join_key", False),
            # PII / governance
            "is_pii": cp.get("is_pii", False),
            "pii_taxonomy": cp.get("pii_taxonomy"),
            "is_critical_data_element": cp.get("is_critical_data_element", False),
            # Provenance — which of the 7 sources confirmed this column
            "sources_contributed": sorted(set(prov.sources)),
            "confidence_tier": prov.confidence_tier,
            "confidence_score": round(prov.confidence_score, 3),
            # Code resolution path
            "is_coded": cp.get("is_coded", False),
            # Knowledge Catalog parallel — AI-suggested description
            "ai_generated_description": cp.get("ai_generated_description", ""),
        })
    # Stable ordering: PK first, then partition, then by name
    out.sort(key=lambda c: (
        not c["is_primary"], not c["is_partitioning"], c["name"],
    ))
    return out


# ─── Metrics sourced from this table ─────────────────────────


def _metrics_for_table(store: GraphStore, table_name: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in store.nodes_by_type("Metric"):
        if m.properties.get("sourced_from_table") != table_name:
            continue
        mp = m.properties
        prov = m.provenance
        out.append({
            "technical_name": m.canonical_uri.split("/")[-1],
            "business_name": mp.get("business_name"),
            "formula": mp.get("formula"),
            "grain": mp.get("grain"),
            "domain": mp.get("domain"),
            "synonyms": mp.get("synonyms", []),
            "sources_contributed": sorted(set(prov.sources)),
            "confidence_tier": prov.confidence_tier,
            "confidence_score": round(prov.confidence_score, 3),
            "evidence_count": sum(prov.evidence_count_by_source.values()),
        })
    return out


# ─── Related tables ──────────────────────────────────────────


def _related_tables(
    store: GraphStore, t_uri: str, table_name: str,
) -> list[dict[str, Any]]:
    """Tables connected via JOIN-observed Column EQUIVALENT_TO edges.

    For each column in this table that's in an EQUIVALENT_TO edge,
    look at the other side and emit the related table + the linking columns."""
    related: dict[str, dict[str, Any]] = {}
    col_edges = store.outgoing(t_uri, "CONTAINS")
    my_cols = [e.to_uri for e in col_edges]
    for c_uri in my_cols:
        # Equivalence edges going OUT from this column
        for eq in store.outgoing(c_uri, "EQUIVALENT_TO"):
            other = store.get(eq.to_uri)
            if not other:
                continue
            other_table = other.properties.get("table_name")
            if not other_table or other_table == table_name:
                continue
            entry = related.setdefault(other_table, {
                "table": other_table,
                "linking_columns": [],
                "n_join_observations": 0,
            })
            entry["linking_columns"].append({
                "from": c_uri.split("/")[-1],
                "to": eq.to_uri.split("/")[-1],
            })
            entry["n_join_observations"] += 1
        # Also incoming
        for eq in store.incoming(c_uri, "EQUIVALENT_TO"):
            other = store.get(eq.from_uri)
            if not other:
                continue
            other_table = other.properties.get("table_name")
            if not other_table or other_table == table_name:
                continue
            entry = related.setdefault(other_table, {
                "table": other_table,
                "linking_columns": [],
                "n_join_observations": 0,
            })
            entry["linking_columns"].append({
                "from": c_uri.split("/")[-1],
                "to": eq.from_uri.split("/")[-1],
            })
            entry["n_join_observations"] += 1
    return sorted(related.values(), key=lambda r: -r["n_join_observations"])


# ─── Quality scoring ─────────────────────────────────────────


def _compute_quality(node: Any, columns: list[dict[str, Any]]) -> dict[str, Any]:
    """Simple quality heuristics. Phase 3 will replace with calibrated metrics.

    completeness  = 1 - (fraction of nullable cols with no null_fraction
                          measured OR with no description)
    consistency   = fraction of cols where MDM and BQ both contributed
                    (proxy for "two sources agree on schema")
    freshness_hours = wall-clock since last_modified (BQ-reported)
    """
    if not columns:
        return {
            "completeness_score": 0.0,
            "consistency_score": 0.0,
            "freshness_hours": None,
        }
    described = sum(1 for c in columns if c.get("description"))
    profiled = sum(1 for c in columns if c.get("approx_distinct") is not None)
    multi_sourced = sum(
        1 for c in columns
        if {"mdm", "bq"} <= set(c.get("sources_contributed", []))
    )
    completeness = round(
        (described + profiled) / (2 * len(columns)), 3,
    )
    consistency = round(multi_sourced / len(columns), 3)

    freshness_hours: float | None = None
    last_mod = node.properties.get("last_modified")
    if last_mod:
        try:
            import datetime as _dt
            t = _dt.datetime.fromisoformat(last_mod.replace("Z", "+00:00"))
            delta = _dt.datetime.now(_dt.timezone.utc) - t
            freshness_hours = round(delta.total_seconds() / 3600.0, 1)
        except (ValueError, TypeError):
            pass

    return {
        "completeness_score": completeness,
        "consistency_score": consistency,
        "freshness_hours": freshness_hours,
        "n_columns_described": described,
        "n_columns_profiled": profiled,
        "n_columns_multi_sourced": multi_sourced,
    }


# ─── DQ rules + lineage (Dataplex parallels) ────────────────


def _dq_rules_for_table(
    store: GraphStore, t_uri: str, columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """All DataQualityRule nodes attached to this table or its columns."""
    rule_uris: set[str] = set()
    # Rules attached directly to the table
    for e in store.outgoing(t_uri, "VALIDATED_BY"):
        rule_uris.add(e.to_uri)
    # Rules attached to each column of this table
    for c in columns:
        c_uri = canonical_uri("column", t_uri.rsplit("/", 1)[-1], c["name"])
        for e in store.outgoing(c_uri, "VALIDATED_BY"):
            rule_uris.add(e.to_uri)
    out: list[dict[str, Any]] = []
    for r_uri in sorted(rule_uris):
        rule = store.get(r_uri)
        if not rule:
            continue
        rp = rule.properties
        out.append({
            "rule_id": r_uri.split("/")[-1],
            "target_column": rp.get("target_column"),
            "rule_kind": rp.get("rule_kind"),
            "threshold": rp.get("threshold"),
            "last_run_status": rp.get("last_run_status"),
            "last_run_value": rp.get("last_run_value"),
            "severity": rp.get("severity"),
            "auto_suggested": rp.get("auto_suggested", False),
        })
    # Sort: failing/warning rules first, then by column
    status_order = {"fail": 0, "warning": 1, "unknown": 2, "pass": 3}
    out.sort(key=lambda r: (
        status_order.get(r.get("last_run_status") or "unknown", 4),
        r.get("target_column") or "",
        r.get("rule_id") or "",
    ))
    return out


def _lineage_for_table(store: GraphStore, t_uri: str) -> dict[str, Any]:
    """Upstream + downstream tables via UPSTREAM_OF edges."""
    upstream = [
        {
            "table": (store.get(e.from_uri).properties.get("table_name")  # type: ignore[union-attr]
                      if store.get(e.from_uri) else None),
            "source": "mdm_lineage",
        }
        for e in store.incoming(t_uri, "UPSTREAM_OF")
        if store.get(e.from_uri)
    ]
    downstream = [
        {
            "table": (store.get(e.to_uri).properties.get("table_name")  # type: ignore[union-attr]
                      if store.get(e.to_uri) else None),
            "source": "mdm_lineage",
        }
        for e in store.outgoing(t_uri, "UPSTREAM_OF")
        if store.get(e.to_uri)
    ]
    return {
        "upstream": [u for u in upstream if u["table"]],
        "downstream": [d for d in downstream if d["table"]],
    }


# ─── Code resolutions for this table's columns ───────────────


def _code_resolutions_for_table(
    store: GraphStore, table_name: str,
) -> list[dict[str, Any]]:
    """All CodeMapping nodes whose column also appears in this table."""
    out: list[dict[str, Any]] = []
    # Names of columns in this table
    t_uri = canonical_uri("table", table_name)
    my_col_names = {
        store.get(e.to_uri).canonical_uri.split("/")[-1]  # type: ignore[union-attr]
        for e in store.outgoing(t_uri, "CONTAINS")
        if store.get(e.to_uri)
    }
    for cm in store.nodes_by_type("CodeMapping"):
        col = cm.properties.get("column")
        if col not in my_col_names:
            continue
        prov = cm.provenance
        out.append({
            "column": col,
            "raw_value": cm.properties.get("raw_value"),
            "human_meaning": cm.properties.get("human_meaning"),
            "source": cm.properties.get("source"),
            "confidence_tier": prov.confidence_tier,
            "confidence_score": round(prov.confidence_score, 3),
        })
    return out
