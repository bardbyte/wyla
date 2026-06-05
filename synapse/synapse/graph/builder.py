"""Build the typed graph from all seven sources.

Pure function: reads source artifacts off disk → returns populated
GraphStore. No network, no LLM, no AGE. Deterministic.

Each source contributes via its own pass:
    1. table_catalog    → Table nodes (scope + domain tags)
    2. mdm              → Table props + Column nodes + governance + FKs
    3. bq               → Column profiling, partition/cluster confirmation,
                          DataQualityRule suggestions (Dataplex-style)
    4. baseline_lookml  → primary_key promotion, alias → Synonym
    5. metric_catalog   → Metric nodes + SOURCED_FROM edges
    6. glossary         → Synonym nodes (context-keyed)
    7. corpus (SQL)     → Equivalence/Cardinality edges, Metric/Threshold/Filter
                          events, code-resolution from CASE WHEN
    8. usage            → User nodes + QUERIED_BY edges + per-column ref counts

Each upsert tags the contributing source on the node's provenance — so a
node touched by mdm + corpus + bq has all three in `sources`, calibrated
confidence reflects multi-source agreement.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from synapse.graph.store import (
    GraphStore,
    canonical_uri,
)


# ─── Pass orchestrator ───────────────────────────────────────


def build_graph_from_sources(sources_dir: Path) -> GraphStore:
    """One-shot graph build from synthetic / real source artifacts.

    Expected layout under sources_dir:
        registries/raw/glossary.csv
        registries/raw/metric_catalog.csv
        registries/raw/table_catalog.csv
        mdm_cache/<table>.json
        bq_cache/<table>.json
        gold_queries/Q*.sql
        usage_history/<table>.json
        baseline_views/<table>.view.lkml
    """
    store = GraphStore()

    # Order matters — table_catalog seeds Table nodes; everything else attaches.
    _ingest_table_catalog(store, sources_dir / "registries" / "raw" / "table_catalog.csv")
    _ingest_mdm(store, sources_dir / "mdm_cache")
    _ingest_bq_profile(store, sources_dir / "bq_cache")
    _ingest_baseline_lookml(store, sources_dir / "baseline_views")
    _ingest_metric_catalog(store, sources_dir / "registries" / "raw" / "metric_catalog.csv")
    _ingest_glossary(store, sources_dir / "registries" / "raw" / "glossary.csv")
    _ingest_corpus(store, sources_dir / "gold_queries")
    _ingest_usage(store, sources_dir / "usage_history")
    # Dataplex-style additions
    _ingest_dq_rules(store, sources_dir / "dq_rules")
    _ingest_ai_descriptions(store, sources_dir / "ai_descriptions")
    _ingest_lineage_from_mdm(store, sources_dir / "mdm_cache")
    # BQ-derived empirical lineage (real-loader outputs only; no-op on synthetic)
    _ingest_lineage_from_bq(store, sources_dir / "lineage")

    # Code-resolution pass — runs after corpus to mine CASE WHENs
    _resolve_codes_from_lookup_tables(store)

    return store


# ─── Per-source ingesters ────────────────────────────────────


def _ingest_table_catalog(store: GraphStore, csv_path: Path) -> None:
    import csv as _csv
    if not csv_path.exists():
        return
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            name = (row.get("table_name") or "").strip()
            if not name:
                continue
            uri = canonical_uri("table", name)
            store.upsert_node(
                "Table", uri,
                properties={
                    "table_name": name,
                    "is_in_dmp": (
                        (row.get("IS IN DMP") or "").strip().lower()
                        in {"yes", "y", "true", "1"}
                    ),
                    "company_domain": (row.get("company_domain") or "").strip(),
                    "data_domain": (row.get("data_domain") or "").strip(),
                },
                source="table_catalog",
            )


def _ingest_mdm(store: GraphStore, cache_dir: Path) -> None:
    if not cache_dir.exists():
        return
    for path in sorted(cache_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        name = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", name)
        store.upsert_node(
            "Table", t_uri,
            properties={
                "table_name": name,
                "business_name": blob.get("table_business_name") or "",
                "description": blob.get("table_description") or "",
                "fqn": ".".join(filter(None, [
                    blob.get("bq_project"), blob.get("bq_dataset"),
                    blob.get("bq_table"),
                ])),
                "owner_team": (
                    (blob.get("ownership") or {})
                    .get("business_contacts", [{}])[0].get("email", "")
                    if (blob.get("ownership") or {}).get("business_contacts")
                    else ""
                ),
                "row_count": blob.get("row_count_estimate"),
                "partition_field": blob.get("partition_field"),
                "asset_kind": blob.get("asset_kind") or "Table",
                "tags": blob.get("tags") or [],
                "lineage_upstream": blob.get("lineage_upstream") or [],
            },
            source="mdm",
        )
        for c in (blob.get("columns") or []):
            if not isinstance(c, dict) or not c.get("name"):
                continue
            c_uri = canonical_uri("column", name, c["name"])
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": name,
                    "data_type": c.get("type") or "",
                    "is_nullable": True,
                    "description": c.get("description") or "",
                    "business_name": c.get("business_name") or "",
                    "is_primary": bool(c.get("is_primary")),
                    "is_dedupe_key": bool(c.get("is_dedupe_key")),
                    "is_partitioning": bool(c.get("is_partitioned")),
                    "cluster_position": c.get("cluster_position"),
                    "pii_taxonomy": c.get("pii_role_id") or "Internal",
                    "is_pii": bool(c.get("is_pii")),
                    "is_critical_data_element": bool(c.get("is_critical_data_element")),
                },
                source="mdm",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, c_uri,
                properties={"ordinal": 0}, source="mdm",
            )


def _ingest_bq_profile(store: GraphStore, cache_dir: Path) -> None:
    if not cache_dir.exists():
        return
    for path in sorted(cache_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        name = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", name)
        store.upsert_node(
            "Table", t_uri,
            properties={
                "table_name": name,
                "row_count": blob.get("row_count"),
                "last_modified": blob.get("last_modified"),
                "partition_field": blob.get("partition_field"),
                "clustering_fields": blob.get("clustering_fields") or [],
            },
            source="bq",
        )
        col_stats = blob.get("column_stats") or {}
        distinct_vals = blob.get("distinct_values") or {}
        policy_tags = blob.get("policy_tags_by_column") or {}
        for c in (blob.get("columns") or []):
            cname = c.get("name") if isinstance(c, dict) else None
            if not cname:
                continue
            c_uri = canonical_uri("column", name, cname)
            stats = col_stats.get(cname, {})
            approx = stats.get("approx_distinct")
            null_frac = stats.get("null_fraction")
            bucket = _cardinality_bucket(approx)
            samples = distinct_vals.get(cname, [])
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": name,
                    "data_type": c.get("data_type") or "",
                    "is_nullable": c.get("is_nullable", True),
                    "description": c.get("description_bq") or "",
                    "is_partitioning": bool(c.get("is_partitioning_column")),
                    "cluster_position": c.get("clustering_ordinal"),
                    "approx_distinct": approx,
                    "null_fraction": null_frac,
                    "cardinality_bucket": bucket,
                    "distinct_sample": samples[:10],
                    "pii_taxonomy": (policy_tags.get(cname) or ["Internal"])[0],
                    "is_pii": bool(policy_tags.get(cname)),
                },
                source="bq",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, c_uri, properties={}, source="bq",
            )
            # Distinct values → FilterValue nodes for low-cardinality cols
            if bucket in ("low", "medium") and samples:
                for v in samples[:10]:
                    val = v.get("value") if isinstance(v, dict) else v
                    if val is None or val == "":
                        continue
                    fv_uri = canonical_uri("filtervalue", name, cname, str(val))
                    store.upsert_node(
                        "FilterValue", fv_uri,
                        properties={
                            "table_name": name,
                            "column_name": cname,
                            "value": str(val),
                            "count_obs": (
                                v.get("count") if isinstance(v, dict) else 0
                            ),
                            "is_structural": False,
                        },
                        source="bq",
                    )


def _ingest_baseline_lookml(store: GraphStore, baseline_dir: Path) -> None:
    if not baseline_dir.exists():
        return
    for path in sorted(baseline_dir.glob("*.view.lkml")):
        text = path.read_text(encoding="utf-8")
        name = path.stem.replace(".view", "")
        t_uri = canonical_uri("table", name)
        store.upsert_node(
            "Table", t_uri, properties={"table_name": name},
            source="baseline_lookml",
        )
        # Look for primary_key: yes blocks
        # Pattern matches `dimension: <name> { ... primary_key: yes ... }`
        for m in re.finditer(
            r"dimension:\s*(\w+)\s*\{[^}]*primary_key:\s*yes",
            text, re.DOTALL,
        ):
            col = m.group(1)
            c_uri = canonical_uri("column", name, col)
            store.upsert_node(
                "Column", c_uri,
                properties={"table_name": name, "is_primary": True},
                source="baseline_lookml",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, c_uri, properties={},
                source="baseline_lookml",
            )


def _ingest_metric_catalog(store: GraphStore, csv_path: Path) -> None:
    import csv as _csv
    if not csv_path.exists():
        return
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            tech = (row.get("technical_name") or "").strip()
            primary_tbl = (row.get("primary_data_product") or "").strip()
            if not (tech and primary_tbl):
                continue
            m_uri = canonical_uri("metric", primary_tbl, tech)
            store.upsert_node(
                "Metric", m_uri,
                properties={
                    "business_name": (row.get("business_name") or "").strip(),
                    "formula": (row.get("calculation_logic") or "").strip(),
                    "grain": (row.get("metric_grain") or "").strip(),
                    "domain": (row.get("associated_domain") or "").strip(),
                    "sourced_from_table": primary_tbl,
                    "synonyms": [
                        s.strip() for s in
                        (row.get("business_synonyms") or "").split(";") if s.strip()
                    ],
                },
                source="metric_catalog",
            )
            t_uri = canonical_uri("table", primary_tbl)
            # SOURCED_FROM edge using COMPUTED_FROM semantics
            store.upsert_edge(
                "COMPUTED_FROM", m_uri, t_uri,
                properties={"formula": (row.get("calculation_logic") or "").strip()},
                source="metric_catalog",
            )


def _ingest_glossary(store: GraphStore, csv_path: Path) -> None:
    import csv as _csv
    if not csv_path.exists():
        return
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            sym = (row.get("Symbol") or "").strip()
            defn = (row.get("Definition") or "").strip()
            bu = (row.get("BusinessUnit") or "").strip()
            region = (row.get("Region") or "").strip()
            entry_type = (row.get("EntryType") or "").strip()
            if not (sym and defn):
                continue
            s_uri = canonical_uri("synonym", sym, bu or "global", region or "global")
            store.upsert_node(
                "Synonym", s_uri,
                properties={
                    "surface_form": sym,
                    "canonical_entity": defn,
                    "business_unit": bu,
                    "region": region,
                    "entry_type": entry_type,
                },
                source="glossary",
            )


def _ingest_corpus(store: GraphStore, sql_dir: Path) -> None:
    """Lightweight corpus ingestion that does NOT require sqlglot.

    Parses just enough to extract: JOIN ON pairs, GROUP BY columns,
    aggregations, CASE WHEN code mappings. Stronger extraction lives
    in synapse.curation.corpus_signals (which uses sqlglot)."""
    if not sql_dir.exists():
        return

    join_re = re.compile(
        r"JOIN\s+`?[\w\-]+\.[\w\-]+\.(\w+)`?\s+\w+\s+ON\s+\w+\.(\w+)\s*=\s*\w+\.(\w+)",
        re.IGNORECASE,
    )
    case_re = re.compile(
        r"WHEN\s+(\w+)\s*=\s*'([^']+)'\s+THEN\s+'([^']+)'",
        re.IGNORECASE,
    )
    from_re = re.compile(r"FROM\s+`?[\w\-]+\.[\w\-]+\.(\w+)`?", re.IGNORECASE)
    agg_re = re.compile(
        r"(SUM|COUNT|AVG|MIN|MAX|COUNT\s*\(\s*DISTINCT)\s*\(\s*(\w+)\s*\)",
        re.IGNORECASE,
    )
    groupby_re = re.compile(r"GROUP\s+BY\s+([^\n;]+)", re.IGNORECASE)
    where_re = re.compile(
        r"WHERE\s+(\w+)\s*=\s*'([^']+)'", re.IGNORECASE,
    )

    for path in sorted(sql_dir.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        qid = path.stem
        # Primary table
        primary_match = from_re.search(sql)
        primary = primary_match.group(1) if primary_match else None
        # JOINs → EQUIVALENT_TO edges between Columns
        for join_match in join_re.finditer(sql):
            other_tbl, left_key, right_key = join_match.groups()
            if not primary:
                continue
            a_uri = canonical_uri("column", primary, left_key)
            b_uri = canonical_uri("column", other_tbl, right_key)
            store.upsert_node(
                "Column", a_uri,
                properties={"table_name": primary, "is_join_key": True},
                source="corpus", evidence_event_id=qid,
            )
            store.upsert_node(
                "Column", b_uri,
                properties={"table_name": other_tbl, "is_join_key": True},
                source="corpus", evidence_event_id=qid,
            )
            store.upsert_edge(
                "EQUIVALENT_TO", a_uri, b_uri,
                properties={"observed_in_query": qid},
                source="corpus", evidence_event_id=qid,
            )
        # Aggregations
        if primary:
            for agg_match in agg_re.finditer(sql):
                fn, col = agg_match.groups()
                m_uri = canonical_uri("metric", primary, f"{fn.lower()}_{col}")
                store.upsert_node(
                    "Metric", m_uri,
                    properties={
                        "business_name": f"{fn}({col})",
                        "formula": f"{fn}({col})",
                        "sourced_from_table": primary,
                    },
                    source="corpus", evidence_event_id=qid,
                )
                c_uri = canonical_uri("column", primary, col)
                store.upsert_edge(
                    "COMPUTED_FROM", m_uri, c_uri,
                    properties={"aggregation": fn},
                    source="corpus", evidence_event_id=qid,
                )
            # GROUP BY → SLICEABLE_BY edges from Metric → Column
            gb_match = groupby_re.search(sql)
            if gb_match:
                dims = [d.strip().split(".")[-1] for d in gb_match.group(1).split(",")]
                for dim in dims:
                    if not dim.isidentifier():
                        continue
                    dim_uri = canonical_uri("column", primary, dim)
                    store.upsert_node(
                        "Column", dim_uri,
                        properties={"table_name": primary, "is_group_by": True},
                        source="corpus", evidence_event_id=qid,
                    )
                    for agg_match in agg_re.finditer(sql):
                        fn, col = agg_match.groups()
                        m_uri = canonical_uri("metric", primary, f"{fn.lower()}_{col}")
                        store.upsert_edge(
                            "SLICEABLE_BY", m_uri, dim_uri,
                            properties={"observed_in_query": qid},
                            source="corpus", evidence_event_id=qid,
                        )
            # WHERE filters → FilterValue nodes (is_structural=True for the
            # corpus-frequent ones)
            for where_match in where_re.finditer(sql):
                col, val = where_match.groups()
                fv_uri = canonical_uri("filtervalue", primary, col, val)
                store.upsert_node(
                    "FilterValue", fv_uri,
                    properties={
                        "table_name": primary,
                        "column_name": col,
                        "value": val,
                        "is_structural": True,
                    },
                    source="corpus", evidence_event_id=qid,
                )
        # CASE WHEN → CodeMapping nodes
        for case_match in case_re.finditer(sql):
            col, raw_val, meaning = case_match.groups()
            cm_uri = canonical_uri("codemapping", col, raw_val)
            store.upsert_node(
                "CodeMapping", cm_uri,
                properties={
                    "column": col,
                    "raw_value": raw_val,
                    "human_meaning": meaning,
                    "source": "case_when",
                },
                source="corpus", evidence_event_id=qid,
            )


def _ingest_usage(store: GraphStore, usage_dir: Path) -> None:
    if not usage_dir.exists():
        return
    for path in sorted(usage_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        name = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", name)
        store.upsert_node(
            "Table", t_uri,
            properties={
                "table_name": name,
                "total_queries_observed": blob.get("total_queries", 0),
                "top_users": blob.get("top_users", []),
                "peak_query_hours": blob.get("peak_query_hours", []),
            },
            source="usage",
        )
        for u in (blob.get("top_users") or []):
            email = u.get("email")
            if not email:
                continue
            u_uri = canonical_uri("user", email)
            store.upsert_node(
                "User", u_uri,
                properties={
                    "email": email,
                    "team": u.get("team", ""),
                },
                source="usage",
            )
            store.upsert_edge(
                "QUERIED_BY", t_uri, u_uri,
                properties={
                    "query_count": u.get("query_count", 0),
                    "bytes_billed": u.get("total_bytes_billed", 0),
                },
                source="usage",
            )
        # Per-column reference count → enriches Column.reference_count
        for col_name, n in (blob.get("per_column_reference_count") or {}).items():
            c_uri = canonical_uri("column", name, col_name)
            if c_uri in store.nodes:
                store.upsert_node(
                    "Column", c_uri,
                    properties={
                        "table_name": name,
                        "reference_count": n,
                    },
                    source="usage",
                )


# ─── Dataplex-style ingesters ────────────────────────────────


def _ingest_dq_rules(store: GraphStore, rules_dir: Path) -> None:
    """Mint DataQualityRule nodes + VALIDATED_BY edges from per-table JSON.

    Source attribution: `dq_engine` (parallel to Dataplex Auto DQ)."""
    if not rules_dir.exists():
        return
    for path in sorted(rules_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        table = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", table)
        last_run_at = blob.get("last_run_at", "")
        # Tag the table with dq_engine as a contributing source so the
        # per-source breakdown reflects DQ coverage.
        store.upsert_node(
            "Table", t_uri,
            properties={"table_name": table},
            source="dq_engine",
        )
        for rule in (blob.get("rules") or []):
            rid = rule.get("rule_id")
            if not rid:
                continue
            r_uri = canonical_uri("dataqualityrule", table, rid)
            target_col = rule.get("target_column")
            store.upsert_node(
                "DataQualityRule", r_uri,
                properties={
                    "target_table": table,
                    "target_column": target_col,
                    "rule_kind": rule.get("rule_kind", ""),
                    "threshold": rule.get("threshold", ""),
                    "last_run_status": rule.get("last_run_status", "unknown"),
                    "last_run_value": rule.get("last_run_value", ""),
                    "last_run_at": last_run_at,
                    "severity": rule.get("severity", "warning"),
                    "auto_suggested": bool(rule.get("auto_suggested")),
                },
                source="dq_engine",
                evidence_event_id=rid,
            )
            # Attach to table or column via VALIDATED_BY
            if target_col:
                c_uri = canonical_uri("column", table, target_col)
                store.upsert_edge(
                    "VALIDATED_BY", c_uri, r_uri,
                    properties={"severity": rule.get("severity", "warning")},
                    source="dq_engine",
                    evidence_event_id=rid,
                )
            else:
                store.upsert_edge(
                    "VALIDATED_BY", t_uri, r_uri,
                    properties={"severity": rule.get("severity", "warning")},
                    source="dq_engine",
                    evidence_event_id=rid,
                )


def _ingest_ai_descriptions(store: GraphStore, ai_dir: Path) -> None:
    """Layer AI-suggested column descriptions in as `llm_generated` source."""
    if not ai_dir.exists():
        return
    for path in sorted(ai_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        table = blob.get("table_name") or path.stem
        attached_any = False
        for col, suggestion in (blob.get("column_descriptions") or {}).items():
            c_uri = canonical_uri("column", table, col)
            # Only attach if the column node already exists — we don't
            # want LLM-only ghost columns
            if c_uri not in store.nodes:
                continue
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": table,
                    "ai_generated_description": suggestion,
                },
                source="llm_generated",
            )
            attached_any = True
        if attached_any:
            t_uri = canonical_uri("table", table)
            store.upsert_node(
                "Table", t_uri,
                properties={"table_name": table},
                source="llm_generated",
            )


def _ingest_lineage_from_mdm(store: GraphStore, mdm_dir: Path) -> None:
    """Materialize UPSTREAM_OF edges from MDM lineage hints."""
    if not mdm_dir.exists():
        return
    for path in sorted(mdm_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        name = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", name)
        for upstream in (blob.get("lineage_upstream") or []):
            u_uri = canonical_uri("table", upstream)
            # Ensure upstream node exists (it should, but be defensive)
            if u_uri not in store.nodes:
                store.upsert_node(
                    "Table", u_uri,
                    properties={"table_name": upstream},
                    source="mdm",
                )
            store.upsert_edge(
                "UPSTREAM_OF", u_uri, t_uri,
                properties={"observed_in": "mdm_lineage"},
                source="mdm",
            )


def _ingest_lineage_from_bq(store: GraphStore, lineage_dir: Path) -> None:
    """BQ-derived empirical lineage from JOBS_BY_PROJECT.

    Source attribution: `bq` (warehouse ground truth — outranks MDM-declared
    lineage on conflict via source weight). Reads files produced by
    `synapse.loaders.bq_loader._build_lineage_blob` (real data only;
    no-op when synthetic generator is the source)."""
    if not lineage_dir.exists():
        return
    for path in sorted(lineage_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        name = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", name)
        for upstream in (blob.get("lineage_upstream") or []):
            u_uri = canonical_uri("table", upstream)
            if u_uri not in store.nodes:
                store.upsert_node(
                    "Table", u_uri,
                    properties={"table_name": upstream},
                    source="bq",
                )
            store.upsert_edge(
                "UPSTREAM_OF", u_uri, t_uri,
                properties={"observed_in": "bq_jobs_history"},
                source="bq",
            )
        for downstream in (blob.get("lineage_downstream") or []):
            d_uri = canonical_uri("table", downstream)
            if d_uri not in store.nodes:
                store.upsert_node(
                    "Table", d_uri,
                    properties={"table_name": downstream},
                    source="bq",
                )
            store.upsert_edge(
                "UPSTREAM_OF", t_uri, d_uri,
                properties={"observed_in": "bq_jobs_history"},
                source="bq",
            )


# ─── Code resolution from lookup tables ──────────────────────


def _resolve_codes_from_lookup_tables(store: GraphStore) -> None:
    """For every coded Column, look for a dim/lookup table with a matching
    column whose row count is small; mint CodeMapping nodes from BQ
    distinct values."""
    lookup_tables = [
        n for n in store.nodes_by_type("Table")
        if (n.properties.get("row_count") or 0) < 10_000
        and any(prefix in n.properties.get("table_name", "")
                for prefix in ("dim_", "drm_", "ref_", "lookup_", "_hier"))
    ]
    for lookup in lookup_tables:
        lookup_name = lookup.properties.get("table_name", "")
        # Get its columns + their distinct values
        cols = [
            store.nodes[e.to_uri] for e in store.outgoing(lookup.canonical_uri, "CONTAINS")
        ]
        # Heuristic: 1st coded column with a "_name" sibling = code→meaning pair
        code_col = None
        meaning_col = None
        for c in cols:
            n = c.properties.get("table_name") or ""  # placeholder unused
            col_name = c.canonical_uri.split("/")[-1]
            if col_name.endswith("_id") or c.properties.get("is_primary"):
                code_col = c
            if "name" in col_name:
                meaning_col = c
        if not (code_col and meaning_col):
            continue
        # For each FilterValue under the code column, emit a CodeMapping
        for fv in store.nodes_by_type("FilterValue"):
            if (fv.properties.get("table_name") == lookup_name
                    and fv.properties.get("column_name") == code_col.canonical_uri.split("/")[-1]):
                raw = fv.properties.get("value", "")
                cm_uri = canonical_uri(
                    "codemapping", code_col.canonical_uri.split("/")[-1], raw,
                )
                store.upsert_node(
                    "CodeMapping", cm_uri,
                    properties={
                        "column": code_col.canonical_uri.split("/")[-1],
                        "raw_value": raw,
                        "human_meaning": f"(see {lookup_name})",
                        "source": "lookup_table",
                    },
                    source="bq",
                )
                store.upsert_edge(
                    "RESOLVED_BY", cm_uri, lookup.canonical_uri,
                    properties={"via_column": code_col.canonical_uri.split("/")[-1]},
                    source="bq",
                )


# ─── Helpers ─────────────────────────────────────────────────


def _cardinality_bucket(approx_distinct: int | None) -> str:
    if approx_distinct is None:
        return "unknown"
    if approx_distinct < 100:
        return "low"
    if approx_distinct < 10_000:
        return "medium"
    if approx_distinct < 1_000_000:
        return "high"
    return "very_high"
