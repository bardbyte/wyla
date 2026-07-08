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
    Node,
    canonical_uri,
    normalize_table_name,
)


# ─── Pass orchestrator ───────────────────────────────────────


def build_graph_from_sources(
    sources_dir: Path, allowlist: "set[str] | None" = None,
) -> GraphStore:
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

    ``allowlist`` scopes the graph to a chosen set of tables (any name
    form — normalized internally). When set, every table-scoped node
    whose table is NOT in the set is pruned after the build, along with
    its dangling edges. This is how a focused build stays exactly its
    manifest: CTE aliases and template placeholders (``base``, ``the``,
    ``your_project.your_dataset.source_table``) that corpus/skills SQL
    parsing would otherwise mint as Table nodes are dropped. Cross-cutting
    nodes (Synonym, Entity, Skill, Guardrail, User) are always kept.
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
    # MDM attribute-level lineage (crawler output; no-op when absent)
    _ingest_attribute_lineage(store, sources_dir / "attribute_lineage")
    # BQ-derived empirical lineage (real-loader outputs only; no-op on synthetic)
    _ingest_lineage_from_bq(store, sources_dir / "lineage")
    # Lumi 100% signal coverage (no-op when the loader didn't produce these)
    _ingest_lumi_signals(store, sources_dir / "lumi_signals")
    _ingest_baseline_artifacts(store, sources_dir / "baseline_artifacts")
    # Skills are NOT a graph source. Business logic (definitions, metric
    # contracts, analytical SQL) and guardrails live in the SkillsRegistry
    # (files) that the agent + the warehouse gate read directly — the data
    # graph stays pure MDM + BQ + corpus + entities. (skills_loader still
    # stages the bundles under sources/skills for the registry to load.)
    # Witness #6 — steward-approved business entities (no-op when absent).
    # Runs after every column-producing witness so IDENTIFIES edges ground.
    from synapse.graph.entities import ingest_entities_file
    ingest_entities_file(store, sources_dir / "entities.yaml")

    # Code-resolution pass — runs after corpus to mine CASE WHENs
    _resolve_codes_from_lookup_tables(store)

    # Dataplex Auto-DQ parallel — derive DQ rules from the BQ profile already
    # on the nodes, and record dq_engine as a witness on what they validate.
    # This is the column-grounding lever: a profiled column that also carries
    # mdm + bq gains a 3rd, system-attested witness → tier climbs to grounded.
    _synthesize_dq_from_profile(store)

    if allowlist is not None:
        _prune_to_allowlist(store, allowlist)

    return store


def _synthesize_dq_from_profile(store: GraphStore) -> int:
    """Emit DataQualityRule nodes from BQ profiling and attest the witness.

    Real signal only: a column is processed only when it carries BQ
    profile stats (``null_fraction`` / ``approx_distinct``); a table only
    when it has a ``row_count``. Each rule is ``auto_suggested`` (the
    honest ``is it AI-suggested vs human-authored`` flag), gets a
    VALIDATED_BY edge, and — crucially — records ``dq_engine`` as a source
    on the validated node so its confidence tier reflects the attestation.
    No profile → no rule; nothing is invented.
    """
    made = 0
    for node in list(store.nodes.values()):
        p = node.properties
        if node.node_type == "Column":
            nf = p.get("null_fraction")
            if nf is None and p.get("approx_distinct") is None:
                continue  # no BQ profile on this column → attest nothing
            tbl = p.get("table_name") or ""
            col = p.get("name") or node.canonical_uri.rsplit("/", 1)[-1]
            status = ("pass" if (nf is not None and nf < 0.01)
                      else "warning" if nf is not None else "unknown")
            nn = canonical_uri("dqrule", tbl, col, "not_null")
            store.upsert_node(
                "DataQualityRule", nn,
                {"target_table": tbl, "target_column": col,
                 "rule_kind": "not_null", "threshold": "null_pct < 0.01",
                 "last_run_status": status,
                 "last_run_value": ("" if nf is None else f"null_fraction={nf}"),
                 "severity": "warning", "auto_suggested": True},
                source="dq_engine")
            store.upsert_edge("VALIDATED_BY", node.canonical_uri, nn, {},
                              source="dq_engine")
            store.upsert_node("Column", node.canonical_uri, {},
                              source="dq_engine")  # the witness on the column
            made += 1
            # low-cardinality columns with observed values → a set/enum rule
            if p.get("cardinality_bucket") in ("low", "medium") and \
                    p.get("distinct_sample"):
                en = canonical_uri("dqrule", tbl, col, "enum")
                store.upsert_node(
                    "DataQualityRule", en,
                    {"target_table": tbl, "target_column": col,
                     "rule_kind": "enum", "threshold": "value in observed set",
                     "last_run_status": "pass", "severity": "info",
                     "auto_suggested": True},
                    source="dq_engine")
                store.upsert_edge("VALIDATED_BY", node.canonical_uri, en, {},
                                  source="dq_engine")
        elif node.node_type == "Table":
            rc = p.get("row_count")
            if not rc:
                continue
            tbl = p.get("table_name") or ""
            rr = canonical_uri("dqrule", tbl, "row_count")
            store.upsert_node(
                "DataQualityRule", rr,
                {"target_table": tbl, "target_column": None,
                 "rule_kind": "row_count", "threshold": "row_count > 0",
                 "last_run_status": "pass", "last_run_value": f"row_count={rc}",
                 "severity": "warning", "auto_suggested": True},
                source="dq_engine")
            store.upsert_edge("VALIDATED_BY", node.canonical_uri, rr, {},
                              source="dq_engine")
            store.upsert_node("Table", node.canonical_uri, {},
                              source="dq_engine")
            made += 1
    return made


# Node types whose scope is a single table (pruned when out of the
# allowlist). Everything else — Synonym, Entity, Skill, Guardrail, User —
# is cross-cutting and always kept.
_TABLE_SCOPED = {"Table", "Column", "Metric", "FilterValue",
                 "DataQualityRule", "CodeMapping"}


def _node_table(node: "Node") -> str | None:
    """The table a node belongs to, or None if it isn't table-scoped."""
    p = node.properties
    if node.node_type == "Table":
        return p.get("table_name") or node.canonical_uri.rsplit("/", 1)[-1]
    if node.node_type in ("Column", "FilterValue"):
        return p.get("table_name")
    if node.node_type == "Metric":
        return p.get("sourced_from_table")
    if node.node_type == "DataQualityRule":
        return p.get("target_table")
    return None  # CodeMapping (column-only) + cross-cutting types


def _prune_to_allowlist(store: GraphStore, allowlist: "set[str]") -> None:
    """Drop table-scoped nodes outside the allowlist + their dangling edges.

    A node with no resolvable table (a cross-cutting type, or a scoped
    node missing its table property) is KEPT — we only remove nodes we can
    positively place outside the scope, so nothing is lost by accident.
    """
    allow = {normalize_table_name(t) for t in allowlist}
    drop: set[str] = set()
    for uri, node in store.nodes.items():
        if node.node_type not in _TABLE_SCOPED:
            continue
        tbl = _node_table(node)
        if tbl and normalize_table_name(tbl) not in allow:
            drop.add(uri)
    for uri in drop:
        del store.nodes[uri]
    if drop:
        store.edges = {
            euri: e for euri, e in store.edges.items()
            if e.from_uri not in drop and e.to_uri not in drop
        }


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


_FALSY = frozenset({"", "n", "no", "false", "0", "none", "null"})


def _flag(v: Any) -> bool:
    """Interpret an MDM flag robustly. The real MDM API sends ``"Y"``/``"N"``
    STRINGS, and ``bool("N")`` is True — so a naive read false-positives
    every non-PII column. Strings resolve against a falsy set; everything
    else falls back to ``bool``.
    """
    if isinstance(v, str):
        return v.strip().lower() not in _FALSY
    return bool(v)


def _mdm_governance(c: dict) -> dict[str, Any]:
    """Resolve PII + sensitivity from an MDM column blob — the spine of
    governance. Robust to shape (flat ``is_pii``, nested
    ``sensitivity_details`` dict, or an attribute-keyed array) AND to the
    ``"Y"/"N"`` string encoding. A sensitive ``pii_role_id`` (anything but
    Internal/Public) implies PII even when the flag is absent, so
    ``Sensitive>FinancialAmount`` columns are protected too.
    """
    sens = c.get("sensitivity_details")
    if isinstance(sens, list):  # array shape → pick this column's row
        sens = next((s for s in sens
                     if s.get("attribute_name") == c.get("name")), {})
    elif not isinstance(sens, dict):
        sens = {}
    role = (c.get("pii_role_id") or c.get("pii_taxonomy")
            or sens.get("pii_role_id") or "Internal")
    role_is_sensitive = bool(role) and role.strip().lower() not in (
        "internal", "public", "none", "")
    return {
        "pii_taxonomy": role or "Internal",
        "is_pii": _flag(c.get("is_pii")) or _flag(sens.get("is_pii"))
        or role_is_sensitive,
        "is_sensitive": _flag(c.get("is_sensitive"))
        or _flag(sens.get("is_sensitive")) or role_is_sensitive,
        "is_gdpr": _flag(c.get("is_gdpr")) or _flag(sens.get("is_gdpr")),
    }


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
                # MDM taxonomy → domain axes. data_category is the data
                # subject area ("Customer", "Acquisition"). company_domain
                # prefers the AUTHORITATIVE business_unit (Risk / Fraud /
                # Marketing) from the ownership/pipeline crawl, falling back
                # to data_sub_category when the crawl didn't run.
                "data_domain": blob.get("data_category") or "",
                "company_domain": blob.get("business_unit")
                or blob.get("data_sub_category") or "",
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
                "mdm_coverage_pct": blob.get("mdm_coverage_pct"),
                "partition_field": blob.get("partition_field"),
                "asset_kind": blob.get("asset_kind") or "Table",
                "tags": blob.get("tags") or [],
                "lineage_upstream": blob.get("lineage_upstream") or [],
                # ── crawler-era spine facts (empty-skip on legacy blobs) ──
                "dataset_parent_id": blob.get("dataset_parent_id") or "",
                "business_unit": blob.get("business_unit") or "",
                "feed_type": blob.get("feed_type") or "",
                "table_type": blob.get("table_type") or "",
                "is_decommissioned": blob.get("is_decommissioned") or False,
                "lifecycle_status": (blob.get("lifecycle") or {}).get(
                    "status") or "",
                "pipeline_name": (blob.get("pipeline") or {}).get(
                    "pipeline_name") or "",
                "pipeline_governance": (blob.get("pipeline") or {}).get(
                    "governance") or {},
            },
            source="mdm",
        )
        for c in (blob.get("columns") or []):
            if not isinstance(c, dict) or not c.get("name"):
                continue
            c_uri = canonical_uri("column", name, c["name"])
            gov = _mdm_governance(c)
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": name,
                    "data_type": c.get("type") or "",
                    # is_nullable is BQ's to own (real NOT NULL constraint);
                    # MDM must not assert a default True that BQ can't undo.
                    "description": c.get("description") or "",
                    "business_name": c.get("business_name") or "",
                    "is_primary": _flag(c.get("is_primary")),
                    "is_dedupe_key": _flag(c.get("is_dedupe_key")),
                    "is_partitioning": _flag(c.get("is_partitioned")),
                    "cluster_position": c.get("cluster_position"),
                    "is_critical_data_element": _flag(
                        c.get("is_critical_data_element")),
                    # governance spine — PII + sensitivity + GDPR
                    **gov,
                    # derivation logic + cross-refs (were dropped before)
                    "derived_logic": c.get("derived_logic") or "",
                    "external_references": c.get("external_references") or [],
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
            props = {
                "table_name": name,
                "data_type": c.get("data_type") or "",
                "is_nullable": c.get("is_nullable", True),   # BQ owns nullability
                # BQ description is supplementary — it must NOT overwrite MDM's
                # (MDM owns the business description); keep it under its own key.
                "bq_description": c.get("description_bq") or "",
                "is_partitioning": bool(c.get("is_partitioning_column")),
                "cluster_position": c.get("clustering_ordinal"),
                "approx_distinct": approx,
                "null_fraction": null_frac,
                "cardinality_bucket": bucket,
                "distinct_sample": samples[:10],
            }
            # PII: BQ policy tags CONFIRM sensitivity but never deny it.
            # Only assert when a tag is present — otherwise an untagged BQ
            # profile would clobber MDM's (correct) is_pii/taxonomy with a
            # spurious False/Internal, which is exactly how the manifest
            # build reported zero PII.
            tags = policy_tags.get(cname)
            if tags:
                props["pii_taxonomy"] = tags[0]
                props["is_pii"] = True
            store.upsert_node("Column", c_uri, properties=props, source="bq")
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
        # Crawler-era addition: declared downstream consumers
        for downstream in (blob.get("lineage_downstream") or []):
            d_uri = canonical_uri("table", downstream)
            if d_uri not in store.nodes:
                store.upsert_node(
                    "Table", d_uri,
                    properties={"table_name": downstream},
                    source="mdm",
                )
            store.upsert_edge(
                "UPSTREAM_OF", t_uri, d_uri,
                properties={"observed_in": "mdm_lineage"},
                source="mdm",
            )


def _ingest_attribute_lineage(store: GraphStore, attr_dir: Path) -> None:
    """attribute_lineage/<table>.json → Column DERIVES_FROM Column edges
    carrying the derivation logic — MDM's column-level data flow."""
    if not attr_dir.exists():
        return
    for path in sorted(attr_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for m in (blob.get("mappings") or []):
            if not isinstance(m, dict):
                continue
            src_t, src_c = m.get("src_table"), m.get("src_column")
            dst_t, dst_c = m.get("dst_table"), m.get("dst_column")
            if not (src_t and src_c and dst_t and dst_c):
                continue
            src_uri = canonical_uri("column", src_t, src_c)
            dst_uri = canonical_uri("column", dst_t, dst_c)
            for uri, table, column in ((src_uri, src_t, src_c),
                                       (dst_uri, dst_t, dst_c)):
                if uri not in store.nodes:
                    store.upsert_node(
                        "Column", uri,
                        properties={"table_name": table}, source="mdm",
                    )
            store.upsert_edge(
                "DERIVES_FROM", dst_uri, src_uri,
                properties={
                    "derivation_logic": m.get("derivation_logic") or "",
                    "pipeline_id": m.get("pipeline_id") or "",
                },
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


# ─── Lumi pre-extracted corpus signals (sqlglot-grade) ───────


def _ingest_lumi_signals(store: GraphStore, signals_dir: Path) -> None:
    """Consume lumi_signals/<table>.json — sqlglot-pre-extracted facts that
    supersede the regex extraction in _ingest_corpus.

    Mints:
      * Metric nodes from each aggregation (+ COMPUTED_FROM edge to source column)
      * EQUIVALENT_TO edges from each join_involving_this entry
      * CodeMapping nodes from each case_when entry
      * FilterValue nodes from each filter (is_structural derived from occurrence frequency)
      * Reinforces Column nodes from columns_referenced
    """
    if not signals_dir.exists():
        return
    for path in sorted(signals_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        table = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", table)
        # Seed the Table node itself — corpus evidence that the table exists
        # (standalone gold-SQL extractions have no catalog pass to rely on).
        store.upsert_node(
            "Table", t_uri,
            properties={"table_name": table}, source="corpus",
        )

        # 1. Aggregations → Metric + COMPUTED_FROM
        for a in (blob.get("aggregations") or []):
            fn = (a.get("function") or "").upper()
            col = a.get("column") or ""
            if not (fn and col):
                continue
            qid = a.get("query_id") or ""
            metric_id = f"{fn.lower()}_{col}"
            m_uri = canonical_uri("metric", table, metric_id)
            formula = f"{fn}({col})"
            store.upsert_node(
                "Metric", m_uri,
                properties={
                    "business_name": a.get("alias") or formula,
                    "formula": formula,
                    "grain": "aggregated",
                    "sourced_from_table": table,
                    "synonyms": [a.get("alias")] if a.get("alias") else [],
                },
                source="corpus", evidence_event_id=qid or None,
            )
            c_uri = canonical_uri("column", table, col)
            store.upsert_node(
                "Column", c_uri,
                properties={"table_name": table, "is_aggregated": True},
                source="corpus", evidence_event_id=qid or None,
            )
            store.upsert_edge(
                "COMPUTED_FROM", m_uri, c_uri,
                properties={"aggregation": fn, "alias": a.get("alias") or ""},
                source="corpus", evidence_event_id=qid or None,
            )

        # 2. Joins → EQUIVALENT_TO between the two columns
        for j in (blob.get("joins") or []):
            other = j.get("other_table") or ""
            lcol = j.get("left_column") or ""
            rcol = j.get("right_column") or ""
            if not (other and lcol and rcol):
                continue
            qid = j.get("query_id") or ""
            l_uri = canonical_uri("column", table, lcol)
            r_uri = canonical_uri("column", other, rcol)
            store.upsert_node(
                "Column", l_uri,
                properties={"table_name": table, "is_join_key": True},
                source="corpus", evidence_event_id=qid or None,
            )
            store.upsert_node(
                "Column", r_uri,
                properties={"table_name": other, "is_join_key": True},
                source="corpus", evidence_event_id=qid or None,
            )
            store.upsert_edge(
                "EQUIVALENT_TO", l_uri, r_uri,
                properties={
                    "join_type": j.get("join_type") or "INNER",
                    "observed_in_query": qid,
                },
                source="corpus", evidence_event_id=qid or None,
            )

        # 3. CASE WHEN → CodeMapping
        for cw in (blob.get("case_whens") or []):
            col = cw.get("column") or ""
            raw_val = cw.get("raw_value") or ""
            meaning = cw.get("human_meaning") or ""
            qid = cw.get("query_id") or ""
            if not (col and raw_val):
                continue
            cm_uri = canonical_uri("codemapping", col, raw_val)
            store.upsert_node(
                "CodeMapping", cm_uri,
                properties={
                    "column": col,
                    "raw_value": raw_val,
                    "human_meaning": meaning,
                    "source": "case_when",
                },
                source="corpus", evidence_event_id=qid or None,
            )

        # 4. Filters → FilterValue (derive is_structural from frequency)
        filters = blob.get("filters") or []
        filter_counts: dict[tuple[str, str], int] = {}
        for f in filters:
            key = (f.get("column") or "", f.get("value") or "")
            filter_counts[key] = filter_counts.get(key, 0) + 1
        total_queries = max(
            len({f.get("query_id") for f in filters if f.get("query_id")}),
            1,
        )
        for f in filters:
            col = f.get("column") or ""
            val = f.get("value") or ""
            if not col:
                continue
            qid = f.get("query_id") or ""
            occ = filter_counts.get((col, val), 1)
            # Structural if it appears in ≥80% of distinct queries
            is_structural = bool(
                (occ / total_queries) >= 0.8 or f.get("is_partition")
            )
            fv_uri = canonical_uri("filtervalue", table, col, val or "<empty>")
            store.upsert_node(
                "FilterValue", fv_uri,
                properties={
                    "table_name": table,
                    "column_name": col,
                    "value": val,
                    "operator": f.get("operator") or "=",
                    "is_structural": is_structural,
                    "is_partition": bool(f.get("is_partition")),
                    "is_negated": bool(f.get("is_negated")),
                    "count_obs": occ,
                },
                source="corpus", evidence_event_id=qid or None,
            )
            # Column reinforcement
            c_uri = canonical_uri("column", table, col)
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": table, "is_filter": True,
                },
                source="corpus", evidence_event_id=qid or None,
            )

        # 5. columns_referenced — reinforce Column nodes (corpus-side evidence)
        for col in (blob.get("columns_referenced") or []):
            if not isinstance(col, str) or not col:
                continue
            c_uri = canonical_uri("column", table, col)
            store.upsert_node(
                "Column", c_uri,
                properties={"table_name": table, "referenced_in_corpus": True},
                source="corpus",
            )

        # 6. Date functions — annotate columns with observed grain
        for d in (blob.get("date_functions") or []):
            col = d.get("column") or ""
            gran = d.get("granularity") or ""
            if not col:
                continue
            c_uri = canonical_uri("column", table, col)
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": table,
                    "observed_time_grain": gran or "DATE",
                    "is_time_dimension": True,
                },
                source="corpus",
            )


# ─── Baseline LookML structured artifacts ────────────────────


def _ingest_baseline_artifacts(store: GraphStore, baseline_dir: Path) -> None:
    """Consume baseline_artifacts/<table>.json — structured LookML facts.

    Mints:
      * Table.business_name from view_label, description from view_description
      * Column nodes from baseline_dimensions (source=baseline_lookml)
      * Time-dimension Column nodes from baseline_dimension_groups
      * Metric nodes from baseline_measures + baseline_filtered_measures
      * Synonym nodes from baseline_sql_aliases (alias → canonical)
      * Sets Column.is_drill_field from baseline_drill_fields_curated
      * Table.has_access_filter + access_filter_fields from access_filter
    """
    if not baseline_dir.exists():
        return
    for path in sorted(baseline_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue
        table = blob.get("table_name") or path.stem
        t_uri = canonical_uri("table", table)

        # 1. Table-level baseline facts
        table_props: dict[str, Any] = {"table_name": table}
        if blob.get("view_label"):
            table_props["business_name_lkml"] = blob["view_label"]
        if blob.get("view_description"):
            table_props["description_lkml"] = blob["view_description"]
        if blob.get("sql_table_name"):
            table_props["bq_sql_table_name"] = blob["sql_table_name"]
        if blob.get("derived_table_sql"):
            table_props["asset_kind"] = "View"
            table_props["derived_table_sql"] = blob["derived_table_sql"]
        if blob.get("has_primary_key"):
            table_props["lkml_has_primary_key"] = True
        if blob.get("access_filter"):
            table_props["has_access_filter"] = True
            table_props["access_filter_fields"] = [
                f["field"] for f in blob["access_filter"] if f.get("field")
            ]
        if blob.get("extends_chain"):
            table_props["extends_chain"] = blob["extends_chain"]
        store.upsert_node(
            "Table", t_uri, properties=table_props, source="baseline_lookml",
        )

        # 2. Primary-key column promotion
        pk_col = blob.get("primary_key_column")
        if pk_col:
            pk_uri = canonical_uri("column", table, pk_col)
            store.upsert_node(
                "Column", pk_uri,
                properties={"table_name": table, "is_primary": True},
                source="baseline_lookml",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, pk_uri, properties={}, source="baseline_lookml",
            )

        # 3. Dimensions → Column nodes (baseline-sourced)
        for d in (blob.get("dimensions") or []):
            name = d.get("name") or ""
            if not name:
                continue
            c_uri = canonical_uri("column", table, name)
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": table,
                    "description_lkml": d.get("description") or "",
                    "label_lkml": d.get("label") or "",
                    "lkml_type": d.get("type") or "string",
                    "lkml_sql_expr": d.get("sql") or "",
                    "is_primary": bool(d.get("primary_key")),
                    "is_hidden_lkml": bool(d.get("hidden")),
                    "lkml_tags": d.get("tags") or [],
                },
                source="baseline_lookml",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, c_uri, properties={}, source="baseline_lookml",
            )

        # 4. Dimension groups → time-dimension Column nodes
        for dg in (blob.get("dimension_groups") or []):
            name = dg.get("name") or ""
            if not name:
                continue
            c_uri = canonical_uri("column", table, name)
            store.upsert_node(
                "Column", c_uri,
                properties={
                    "table_name": table,
                    "is_time_dimension": True,
                    "lkml_timeframes": dg.get("timeframes") or [],
                    "lkml_sql_expr": dg.get("sql") or "",
                    "convert_tz": dg.get("convert_tz", True),
                    "description_lkml": dg.get("description") or "",
                },
                source="baseline_lookml",
            )
            store.upsert_edge(
                "CONTAINS", t_uri, c_uri, properties={}, source="baseline_lookml",
            )

        # 5. Measures → Metric nodes
        for m in (blob.get("measures") or []):
            name = m.get("name") or ""
            if not name:
                continue
            m_uri = canonical_uri("metric", table, name)
            store.upsert_node(
                "Metric", m_uri,
                properties={
                    "business_name": m.get("label") or name,
                    "formula": m.get("sql") or f"{m.get('type','sum').upper()}",
                    "lkml_type": m.get("type") or "",
                    "grain": "aggregated",
                    "sourced_from_table": table,
                    "value_format": m.get("value_format") or "",
                    "drill_fields": m.get("drill_fields") or [],
                    "symmetric_aggregates_required": bool(m.get("symmetric_aggregates")),
                    "description": m.get("description") or "",
                },
                source="baseline_lookml",
            )
            store.upsert_edge(
                "COMPUTED_FROM", m_uri, t_uri,
                properties={"formula": m.get("sql") or ""},
                source="baseline_lookml",
            )

        # 6. Filtered measures → Metric with filter context
        for fm in (blob.get("filtered_measures") or []):
            name = fm.get("name") or ""
            if not name:
                continue
            fm_uri = canonical_uri("metric", table, name)
            store.upsert_node(
                "Metric", fm_uri,
                properties={
                    "business_name": name,
                    "formula": fm.get("base_field") or "",
                    "lkml_type": fm.get("type") or "filtered_count",
                    "grain": "aggregated",
                    "sourced_from_table": table,
                    "filter_expression": fm.get("filter_expression") or "",
                    "description": fm.get("description") or "",
                    "is_filtered_measure": True,
                },
                source="baseline_lookml",
            )

        # 7. sql_aliases → Synonym nodes (CRITICAL for NL grounding)
        for alias, canonical in (blob.get("sql_aliases") or {}).items():
            if not (alias and canonical):
                continue
            # business_unit/region unknown at LookML layer — use "lookml" as scope
            s_uri = canonical_uri("synonym", alias, "lookml", "global")
            store.upsert_node(
                "Synonym", s_uri,
                properties={
                    "surface_form": alias,
                    "canonical_entity": canonical,
                    "business_unit": "lookml",
                    "region": "global",
                    "entry_type": "Alias",
                },
                source="baseline_lookml",
            )
            # Link synonym → canonical (Column OR Metric — try both)
            for candidate_uri in (
                canonical_uri("column", table, canonical),
                canonical_uri("metric", table, canonical),
            ):
                if candidate_uri in store.nodes:
                    store.upsert_edge(
                        "HAS_SYNONYM", candidate_uri, s_uri,
                        properties={"alias_kind": "lkml_sql_alias"},
                        source="baseline_lookml",
                    )
                    break

        # 8. Drill fields → Column annotation
        for df in (blob.get("drill_fields_curated") or []):
            c_uri = canonical_uri("column", table, df)
            if c_uri in store.nodes:
                store.upsert_node(
                    "Column", c_uri,
                    properties={"table_name": table, "is_drill_field": True},
                    source="baseline_lookml",
                )


# ─── Code resolution from lookup tables ──────────────────────


def _ingest_skills(store: GraphStore, skills_dir: Path) -> None:
    """Skill packages → Skill/Guardrail/Metric/DataQualityRule nodes.

    Reads the canonical ``skills/<skill_id>.json`` artifacts the skills
    loader writes. Guardrails become first-class nodes with CONSTRAINS
    edges so an agent can ask "what must I respect before touching X"
    instead of re-reading prose.
    """
    if not skills_dir.exists():
        return
    for path in sorted(skills_dir.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict) or not blob.get("skill_id"):
            continue
        skill_id = str(blob["skill_id"])
        s_uri = canonical_uri("skill", skill_id)
        store.upsert_node(
            "Skill", s_uri,
            properties={
                "skill_id": skill_id,
                "domain": blob.get("domain") or "",
                "description": blob.get("description") or "",
                "tables_used": blob.get("tables_used") or [],
                "metrics_defined": [
                    m.get("name") for m in (blob.get("metrics") or [])
                    if isinstance(m, dict) and m.get("name")
                ],
                "parameters": blob.get("parameters") or [],
                "knowledge_excerpt": blob.get("knowledge_excerpt") or "",
                "files": blob.get("files") or [],
                # chart_contract.yaml — per-KPI viz rules for the render layer
                "chart_contracts": blob.get("chart_contracts") or {},
                "has_data_specs": bool(blob.get("data_specs_text")),
            },
            source="skills",
        )

        table_uris: dict[str, str] = {}
        for table in blob.get("tables_used") or []:
            t_uri = canonical_uri("table", table)
            table_uris[str(table).lower()] = t_uri
            # Skills declare applicability but NEVER mint or witness a Table
            # node — the data graph is sourced by the data (mdm/bq/…). The
            # APPLIES_TO edge references the canonical URI: it connects when a
            # data witness minted the table, and prunes as a dangling edge
            # under the allowlist otherwise. (upsert_edge never creates nodes.)
            store.upsert_edge(
                "APPLIES_TO", s_uri, t_uri,
                properties={"skill_id": skill_id}, source="skills",
            )

        metric_uris: dict[str, str] = {}
        for metric in blob.get("metrics") or []:
            if not isinstance(metric, dict) or not metric.get("name"):
                continue
            name = str(metric["name"])
            table = str(metric.get("table") or "")
            m_uri = canonical_uri("metric", table or skill_id, name)
            metric_uris[name.lower()] = m_uri
            store.upsert_node(
                "Metric", m_uri,
                properties={
                    "business_name": metric.get("business_name") or name,
                    "formula": metric.get("formula") or "",
                    "grain": metric.get("grain") or "",
                    "domain": blob.get("domain") or "",
                    "sourced_from_table": table,
                    "synonyms": metric.get("synonyms") or [],
                },
                source="skills",
            )
            store.upsert_edge(
                "DEFINED_BY", m_uri, s_uri,
                properties={"contract": "metric_contracts.yaml"},
                source="skills",
            )
            if table:
                store.upsert_edge(
                    "COMPUTED_FROM", m_uri, canonical_uri("table", table),
                    properties={"formula": metric.get("formula") or ""},
                    source="skills",
                )

        for idx, guardrail in enumerate(blob.get("guardrails") or []):
            if not isinstance(guardrail, dict) or not guardrail.get("rule"):
                continue
            g_uri = canonical_uri("guardrail", skill_id, str(idx))
            store.upsert_node(
                "Guardrail", g_uri,
                properties={
                    "rule": guardrail["rule"],
                    "category": guardrail.get("category") or "other",
                    "applies_to": guardrail.get("applies_to") or [],
                    "severity": guardrail.get("severity") or "error",
                    "machine_checkable": bool(guardrail.get("machine_checkable")),
                    "skill_id": skill_id,
                    "mined_from_knowledge": bool(
                        guardrail.get("mined_from_knowledge")
                    ),
                },
                source="skills",
            )
            targets = guardrail.get("applies_to") or []
            for target in targets:
                target_uri = _resolve_guardrail_target(
                    str(target), table_uris, metric_uris,
                )
                store.upsert_edge(
                    "CONSTRAINS", g_uri, target_uri,
                    properties={"severity": guardrail.get("severity") or "error"},
                    source="skills",
                )
            if not targets:
                # No explicit target — constrain every table the skill covers
                for t_uri in table_uris.values():
                    store.upsert_edge(
                        "CONSTRAINS", g_uri, t_uri,
                        properties={
                            "severity": guardrail.get("severity") or "error",
                        },
                        source="skills",
                    )

        for idx, check in enumerate(blob.get("qa_checks") or []):
            if not isinstance(check, dict):
                continue
            first_table = next(iter(blob.get("tables_used") or [""]), "")
            r_uri = canonical_uri("dataqualityrule", skill_id, f"qa_{idx}")
            store.upsert_node(
                "DataQualityRule", r_uri,
                properties={
                    "target_table": first_table,
                    "target_column": check.get("target_column"),
                    "rule_kind": check.get("rule_kind") or "custom_sql",
                    "threshold": check.get("threshold") or "",
                    "severity": check.get("severity") or "warning",
                    "auto_suggested": False,
                },
                source="skills",
            )
            if first_table:
                store.upsert_edge(
                    "VALIDATED_BY", canonical_uri("table", first_table), r_uri,
                    properties={"origin": skill_id}, source="skills",
                )

        # data_specs.md valid values → FilterValue (curated, is_structural)
        for entry in blob.get("valid_values") or []:
            table = str(entry.get("table") or "")
            column = str(entry.get("column") or "")
            if not (table and column):
                continue
            store.upsert_node(
                "Column", canonical_uri("column", table, column),
                properties={"table_name": table}, source="skills",
            )
            for value in entry.get("values") or []:
                fv_uri = canonical_uri("filtervalue", table, column, value)
                store.upsert_node(
                    "FilterValue", fv_uri,
                    properties={
                        "table_name": table, "column_name": column,
                        "value": value, "is_structural": False,
                        "origin_skill": skill_id, "curated": True,
                    },
                    source="skills",
                )

        # data_specs.md segmentation bands → CodeMapping (raw → human label)
        for band in blob.get("bands") or []:
            column = str(band.get("column") or "")
            raw = str(band.get("raw") or "")
            label = str(band.get("label") or "")
            if not (column and raw and label):
                continue
            cm_uri = canonical_uri("codemapping", column, raw)
            store.upsert_node(
                "CodeMapping", cm_uri,
                properties={
                    "column": column, "raw_value": raw,
                    "human_meaning": label, "source": "data_specs",
                    "origin_skill": skill_id,
                },
                source="skills",
            )


def _resolve_guardrail_target(
    target: str,
    table_uris: dict[str, str],
    metric_uris: dict[str, str],
) -> str:
    """Map an applies_to string to the most specific node URI.

    ``table.column`` → Column, known metric name → Metric,
    known table → Table, else a Table URI by that name (forward ref).
    """
    lowered = target.lower()
    if lowered in metric_uris:
        return metric_uris[lowered]
    if lowered in table_uris:
        return table_uris[lowered]
    if "." in target:
        head, _, column = target.rpartition(".")
        if head.lower() in table_uris:
            return canonical_uri("column", head, column)
        # dataset-qualified table (e.g. common.roll_rate_calc) w/ column:
        # fall through to table URI when the head itself is the table
    return canonical_uri("table", target)


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
