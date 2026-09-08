"""Generate all seven sources from SYNTHETIC_TABLES — deterministic, seeded.

Each function writes one source's artifacts to disk in the same layout
synapse's loaders expect, so the rest of the pipeline (preflight,
curation, graph builder) can run end-to-end on this laptop without
real warehouse access.

Outputs (under <out_dir>):
    registries/raw/glossary.csv
    registries/raw/metric_catalog.csv
    registries/raw/table_catalog.csv
    mdm_cache/<table>.json
    gold_queries/Q01.sql … Qnn.sql
    bq_cache/<table>.json
    usage_history/<table>.json
    baseline_views/<table>.view.lkml
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from synapse.synthetic.schema import (
    SYNTHETIC_TABLES,
    SyntheticColumn,
    SyntheticTable,
)


_RNG = random.Random(20260604)  # deterministic seed


# ─── Glossary CSV ────────────────────────────────────────────


_GLOSSARY_ROWS: list[dict[str, str]] = [
    # (symbol, definition, business_unit, region, entry_type)
    {"Symbol": "CM",   "Definition": "Cardmember",                "BusinessUnit": "Finance",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "CM",   "Definition": "Communication Module",      "BusinessUnit": "Marketing",      "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "CMID", "Definition": "Cardmember ID",             "BusinessUnit": "Finance",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "TBB",  "Definition": "Total Billed Business",     "BusinessUnit": "Finance",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "NAA",  "Definition": "New Accounts Acquired",     "BusinessUnit": "Acquisitions",   "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "FICO", "Definition": "Fair Isaac Corporation Score","BusinessUnit": "Risk",         "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "AA",   "Definition": "Account Adjustment",        "BusinessUnit": "Finance",        "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "AA",   "Definition": "Account Acquisition",       "BusinessUnit": "Marketing",      "Region": "EU",     "EntryType": "Acronym"},
    {"Symbol": "AA",   "Definition": "Adverse Action",            "BusinessUnit": "Risk",           "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "MCC",  "Definition": "Merchant Category Code",    "BusinessUnit": "Merchant",       "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "MR",   "Definition": "Membership Rewards",        "BusinessUnit": "Loyalty",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "PLAT", "Definition": "Platinum Card Product",     "BusinessUnit": "Loyalty",        "Region": "Global", "EntryType": "Code"},
    {"Symbol": "TXN",  "Definition": "Transaction",               "BusinessUnit": "Finance",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "BUS",  "Definition": "Business Segment",          "BusinessUnit": "Finance",        "Region": "Global", "EntryType": "Acronym"},
    {"Symbol": "DM",   "Definition": "Direct Mail",               "BusinessUnit": "Acquisitions",   "Region": "US",     "EntryType": "Acronym"},
    {"Symbol": "DM",   "Definition": "Data Mart",                 "BusinessUnit": "Engineering",    "Region": "Global", "EntryType": "Acronym"},
]


def generate_glossary_csv(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(_GLOSSARY_ROWS[0].keys()))
        w.writeheader()
        w.writerows(_GLOSSARY_ROWS)
    return len(_GLOSSARY_ROWS)


# ─── Metric catalog CSV ──────────────────────────────────────


def generate_metric_catalog_csv(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    for tbl in SYNTHETIC_TABLES:
        for m in tbl.metrics:
            rows.append({
                "technical_name": m.technical_name,
                "business_name": m.business_name,
                "business_definition": (
                    f"{m.business_name} computed at {m.grain} grain from "
                    f"{tbl.name} via {m.formula}."
                ),
                "calculation_logic": m.formula,
                "primary_data_product": tbl.name,
                "associated_domain": m.domain,
                "metric_grain": m.grain,
                "business_synonyms": ";".join(m.synonyms),
                "technical_references": (
                    f"{tbl.name}.{m.technical_name}"
                ),
            })
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return len(rows)


# ─── Table catalog CSV ───────────────────────────────────────


def generate_table_catalog_csv(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "table_name": t.name,
            "IS IN DMP": "Yes" if t.is_in_dmp else "No",
            "company_domain": t.company_domain,
            "data_domain": t.data_domain,
        }
        for t in SYNTHETIC_TABLES
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return len(rows)


# ─── MDM cache (per-table JSON) ──────────────────────────────


def _mdm_column_digest(c: SyntheticColumn) -> dict:
    return {
        "name": c.name,
        "attribute_name": c.name,
        "business_name": c.business_name,
        "description": c.description,
        "attribute_desc": c.description,
        "type": c.data_type,
        "attribute_type": c.data_type,
        "is_primary": c.is_primary,
        "is_dedupe_key": c.is_dedupe_key,
        "is_partitioned": c.is_partitioning,
        "is_clustered": c.cluster_position is not None,
        "cluster_position": c.cluster_position,
        "is_pii": c.pii_taxonomy.startswith(("Sensitive", "Restricted")),
        "pii_role_id": c.pii_taxonomy if c.pii_taxonomy != "Internal" else None,
        "is_critical_data_element": c.pii_taxonomy.startswith("Sensitive"),
    }


def generate_mdm_cache(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    for tbl in SYNTHETIC_TABLES:
        partition_field = next(
            (c.name for c in tbl.columns if c.is_partitioning), None,
        )
        payload = {
            "table_name": tbl.name,
            "display_name": tbl.business_name,
            "table_business_name": tbl.business_name,
            "table_description": tbl.description,
            "data_category": tbl.data_domain,
            "data_sub_category": tbl.company_domain,
            "bq_project": "synthetic-project",
            "bq_dataset": "dw",
            "bq_table": tbl.name,
            "feed_type": "LumiFirst" if tbl.is_in_dmp else "Hydration",
            "table_type": "DERIVED",
            "is_decommissioned": False,
            "partition_field": partition_field,
            "row_count_estimate": tbl.row_count,
            # Dataplex-catalog-style fields
            "asset_kind": tbl.asset_kind,
            "tags": list(tbl.tags),
            "lineage_upstream": list(tbl.lineage_upstream),
            "ownership": {
                "imr_queue": f"queue-{tbl.company_domain.lower().replace(' ', '-')}",
                "aim_id": f"aim-{tbl.name[:6]}",
                "business_contacts": [
                    {"email": tbl.owner_team, "type": "business_owner"},
                ],
                "tech_contacts": [
                    {"email": tbl.owner_team, "type": "tech_owner"},
                ],
            },
            "columns": [_mdm_column_digest(c) for c in tbl.columns],
            "mdm_coverage_pct": 1.0,
        }
        (out_dir / f"{tbl.name}.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8",
        )
    return len(SYNTHETIC_TABLES)


# ─── SQL corpus generation ───────────────────────────────────


def _query_aggregate(tbl: SyntheticTable, dim: str) -> str | None:
    if not tbl.metrics:
        return None
    m = tbl.metrics[0]
    where = ""
    if tbl.typical_filters:
        col, val = tbl.typical_filters[0]
        where = f"WHERE {col} = '{val}'"
    return (
        f"-- Q: total {m.business_name.lower()} by {dim}\n"
        f"SELECT {dim}, {m.formula} AS {m.technical_name}\n"
        f"FROM `synthetic-project.dw.{tbl.name}`\n"
        f"{where}\n"
        f"GROUP BY {dim}\n"
        f"ORDER BY 2 DESC"
    )


def _query_top_n(tbl: SyntheticTable, dim: str) -> str | None:
    if not tbl.metrics:
        return None
    m = tbl.metrics[0]
    return (
        f"-- Q: top 10 {dim} by {m.business_name.lower()}\n"
        f"SELECT {dim}, {m.formula} AS metric\n"
        f"FROM `synthetic-project.dw.{tbl.name}`\n"
        f"GROUP BY {dim}\n"
        f"ORDER BY 2 DESC\n"
        f"LIMIT 10"
    )


def _query_join_with_lookup(tbl: SyntheticTable) -> str | None:
    # Build a query joining tbl with a referenced lookup
    if not tbl.foreign_keys or not tbl.metrics:
        return None
    fk = tbl.foreign_keys[0]
    m = tbl.metrics[0]
    return (
        f"-- Q: {m.business_name.lower()} by joined attribute\n"
        f"SELECT b.{fk.to_column}, {m.formula.replace('SUM(', 'SUM(a.')} AS metric\n"
        f"FROM `synthetic-project.dw.{tbl.name}` a\n"
        f"JOIN `synthetic-project.dw.{fk.to_table}` b\n"
        f"  ON a.{fk.from_column} = b.{fk.to_column}\n"
        f"GROUP BY b.{fk.to_column}"
    )


def _query_case_when(tbl: SyntheticTable) -> str | None:
    # Pick a coded column with sample_distinct values → CASE WHEN that decodes
    coded = next(
        (c for c in tbl.columns if c.is_coded and c.sample_distinct),
        None,
    )
    if not coded:
        return None
    cases = "\n    ".join(
        f"WHEN {coded.name} = '{v}' THEN '{v}_decoded'"
        for v in coded.sample_distinct[:5]
    )
    return (
        f"-- Q: decode {coded.name} via CASE WHEN\n"
        f"SELECT\n"
        f"  CASE\n    {cases}\n    ELSE 'OTHER'\n  END AS {coded.name}_label,\n"
        f"  COUNT(*) AS n\n"
        f"FROM `synthetic-project.dw.{tbl.name}`\n"
        f"GROUP BY 1"
    )


def generate_sql_corpus(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    queries: list[str] = []
    for tbl in SYNTHETIC_TABLES:
        for c in tbl.columns:
            if c.name not in {"bus_seg", "card_product_id", "data_source",
                              "trip_type", "redemption_type", "industry_code"}:
                continue
            q = _query_aggregate(tbl, c.name)
            if q:
                queries.append(q)
            q = _query_top_n(tbl, c.name)
            if q:
                queries.append(q)
        q = _query_join_with_lookup(tbl)
        if q:
            queries.append(q)
        q = _query_case_when(tbl)
        if q:
            queries.append(q)
    # Add some single-lookup queries
    cm_tbl = next(t for t in SYNTHETIC_TABLES
                  if t.name == "custins_customer_insights_cardmember")
    queries.append(
        f"-- Q: lookup one cardmember\n"
        f"SELECT * FROM `synthetic-project.dw.{cm_tbl.name}` "
        f"WHERE cm11 = '12345678901' AND rpt_dt = '2026-05-30'"
    )

    # Write deterministically ordered
    for i, q in enumerate(queries, start=1):
        (out_dir / f"Q{i:03d}.sql").write_text(q + "\n", encoding="utf-8")
    return len(queries)


# ─── BQ profile (per-table JSON) ─────────────────────────────


def generate_bq_profile(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    last_modified = datetime.now(timezone.utc) - timedelta(hours=4)
    for tbl in SYNTHETIC_TABLES:
        col_stats: dict[str, dict] = {}
        distinct_values: dict[str, list[dict]] = {}
        for c in tbl.columns:
            n_distinct = (
                len(c.sample_distinct) if c.sample_distinct
                else tbl.row_count // 1000 if c.is_primary
                else _RNG.choice([5, 50, 500, 50000])
            )
            null_fraction = (
                0.0 if c.is_primary or not c.nullable
                else round(_RNG.uniform(0.0, 0.1), 4)
            )
            col_stats[c.name] = {
                "approx_distinct": n_distinct,
                "null_fraction": null_fraction,
                "data_type": c.data_type,
            }
            if c.sample_distinct:
                # Plausible counts: zipf-ish skew
                vals = c.sample_distinct
                counts = []
                base = tbl.row_count
                for i, v in enumerate(vals):
                    counts.append({"value": v, "count": base // (i + 1) // 2})
                distinct_values[c.name] = counts
        partition_field = next(
            (c.name for c in tbl.columns if c.is_partitioning), None,
        )
        cluster_fields = sorted(
            (c for c in tbl.columns if c.cluster_position is not None),
            key=lambda c: c.cluster_position or 0,
        )
        payload = {
            "table_name": tbl.name,
            "bq_fqn": f"synthetic-project.dw.{tbl.name}",
            "row_count": tbl.row_count,
            "logical_bytes": tbl.row_count * 256,  # rough estimate
            "table_type": tbl.table_type,
            "last_modified": last_modified.isoformat(),
            "partition_field": partition_field,
            "clustering_fields": [c.name for c in cluster_fields],
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "is_nullable": c.nullable,
                    "description_bq": c.description,
                    "is_partitioning_column": c.is_partitioning,
                    "clustering_ordinal": c.cluster_position,
                }
                for c in tbl.columns
            ],
            "column_stats": col_stats,
            "distinct_values": distinct_values,
            "policy_tags_by_column": {
                c.name: [c.pii_taxonomy]
                for c in tbl.columns
                if c.pii_taxonomy and c.pii_taxonomy != "Internal"
            },
        }
        (out_dir / f"{tbl.name}.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8",
        )
    return len(SYNTHETIC_TABLES)


# ─── Usage history (mock JOBS_BY_PROJECT) ────────────────────


_TEAMS = [
    ("risk-modeling", "Risk Modeling"),
    ("finance-fpa", "Finance FP&A"),
    ("loyalty-analytics", "Loyalty Analytics"),
    ("acquisitions-analytics", "Acquisitions Analytics"),
    ("merchant-strategy", "Merchant Strategy"),
    ("travel-insights", "Travel Insights"),
    ("data-science", "Data Science"),
    ("cardmember-insights", "Cardmember Insights"),
]


def generate_usage_history(out_dir: Path, *, days: int = 30) -> int:
    """Per-table top-users + query counts for last N days."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for tbl in SYNTHETIC_TABLES:
        # Different team affinity per domain
        domain_to_teams = {
            "Finance": ["finance-fpa", "risk-modeling", "data-science"],
            "Risk": ["risk-modeling", "data-science"],
            "Loyalty": ["loyalty-analytics", "data-science"],
            "Acquisitions Tracking": ["acquisitions-analytics", "finance-fpa"],
            "Merchant": ["merchant-strategy", "data-science"],
            "Travel": ["travel-insights", "finance-fpa"],
            "Cardmember": ["cardmember-insights", "data-science"],
        }
        primary_teams = domain_to_teams.get(
            tbl.company_domain, ["data-science", "finance-fpa"],
        )
        top_users = []
        for team_slug in primary_teams:
            for i in range(1, 4):
                count = _RNG.randint(20, 200)
                top_users.append({
                    "email": f"{team_slug}-{i}@example.com",
                    "team": next(t[1] for t in _TEAMS if t[0] == team_slug),
                    "query_count": count,
                    "total_bytes_billed": count * _RNG.randint(10**6, 10**9),
                })
        top_users.sort(key=lambda u: -u["query_count"])
        total = sum(u["query_count"] for u in top_users)

        # Per-column reference frequency
        per_col_freq: dict[str, int] = {}
        for c in tbl.columns:
            base = 10 if c.is_partitioning else 5 if c.cluster_position else 2
            per_col_freq[c.name] = base * _RNG.randint(5, 50)

        payload = {
            "table_name": tbl.name,
            "lookback_days": days,
            "total_queries": total,
            "unique_users": len(top_users),
            "top_users": top_users[:8],
            "per_column_reference_count": per_col_freq,
            "peak_query_hours": _RNG.sample(range(8, 20), k=4),
            "total_bytes_billed": sum(u["total_bytes_billed"] for u in top_users),
        }
        (out_dir / f"{tbl.name}.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8",
        )
    return len(SYNTHETIC_TABLES)


# ─── Baseline LookML (per-table .view.lkml) ──────────────────


def _lkml_type(data_type: str) -> str:
    return {
        "STRING": "string",
        "INT64": "number",
        "NUMERIC": "number",
        "FLOAT64": "number",
        "DATE": "date",
        "TIMESTAMP": "time",
        "BOOL": "yesno",
    }.get(data_type, "string")


def generate_baseline_lookml(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    for tbl in SYNTHETIC_TABLES:
        pk = next((c for c in tbl.columns if c.is_primary), None)
        lines = [
            f"# Baseline view for {tbl.name}",
            f"# {tbl.description}",
            f"view: {tbl.name} {{",
            f"  sql_table_name: `synthetic-project.dw.{tbl.name}` ;;",
            f'  description: "{tbl.business_name}"',
            "",
        ]
        if pk:
            lines.append(f"  dimension: {pk.name} {{")
            lines.append("    primary_key: yes")
            lines.append(f"    type: {_lkml_type(pk.data_type)}")
            lines.append(f"    sql: ${{TABLE}}.{pk.name} ;;")
            lines.append(f'    description: "{pk.description}"')
            lines.append("  }")
            lines.append("")
        for c in tbl.columns:
            if c is pk:
                continue
            if c.data_type in {"DATE", "TIMESTAMP"}:
                lines.append(f"  dimension_group: {c.name} {{")
                lines.append("    type: time")
                lines.append("    timeframes: [date, week, month, quarter, year]")
                lines.append(f"    sql: ${{TABLE}}.{c.name} ;;")
                lines.append("  }")
            else:
                lines.append(f"  dimension: {c.name} {{")
                lines.append(f"    type: {_lkml_type(c.data_type)}")
                lines.append(f"    sql: ${{TABLE}}.{c.name} ;;")
                if c.pii_taxonomy and c.pii_taxonomy != "Internal":
                    lines.append("    tags: [\"pii\"]")
                lines.append("  }")
            lines.append("")
        for m in tbl.metrics:
            lines.append(f"  measure: {m.technical_name} {{")
            lines.append(f'    description: "{m.business_name}"')
            lines.append("    type: number")
            lines.append(f"    sql: {m.formula} ;;")
            lines.append("  }")
            lines.append("")
        lines.append("}")
        (out_dir / f"{tbl.name}.view.lkml").write_text(
            "\n".join(lines), encoding="utf-8",
        )
    return len(SYNTHETIC_TABLES)


# ─── Top-level orchestrator ──────────────────────────────────


def generate_dq_rules(out_dir: Path) -> int:
    """Dataplex Auto-DQ-style rules per table. One JSON per table.

    Each rule carries: target column, rule kind, threshold, last-run
    status, severity, and an `auto_suggested` flag. Mix of rule kinds
    so the graph exercises every variant.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(20260604)
    n = 0
    for t in SYNTHETIC_TABLES:
        rules: list[dict[str, object]] = []
        # Table-level row_count + freshness checks
        rules.append({
            "rule_id": f"{t.name}__row_count",
            "target_column": None,
            "rule_kind": "row_count",
            "threshold": f"row_count > {max(1, t.row_count // 10):,}",
            "last_run_status": "pass",
            "last_run_value": str(t.row_count),
            "severity": "error",
            "auto_suggested": False,
        })
        rules.append({
            "rule_id": f"{t.name}__freshness",
            "target_column": None,
            "rule_kind": "freshness",
            "threshold": "freshness_hours < 24",
            "last_run_status": "pass",
            "last_run_value": "4.0",
            "severity": "warning",
            "auto_suggested": True,
        })
        # Per-column rules
        for c in t.columns:
            if c.is_primary or c.is_dedupe_key:
                rules.append({
                    "rule_id": f"{t.name}__{c.name}__unique",
                    "target_column": c.name,
                    "rule_kind": "unique",
                    "threshold": "duplicate_count == 0",
                    "last_run_status": "pass",
                    "last_run_value": "0",
                    "severity": "error",
                    "auto_suggested": False,
                })
            if not c.nullable:
                rules.append({
                    "rule_id": f"{t.name}__{c.name}__not_null",
                    "target_column": c.name,
                    "rule_kind": "not_null",
                    "threshold": "null_pct < 0.01",
                    "last_run_status": "pass" if rng.random() > 0.1 else "warning",
                    "last_run_value": f"{rng.uniform(0, 0.02):.4f}",
                    "severity": "error" if not c.nullable else "warning",
                    "auto_suggested": False,
                })
            if c.sample_distinct and c.data_type == "STRING":
                rules.append({
                    "rule_id": f"{t.name}__{c.name}__enum",
                    "target_column": c.name,
                    "rule_kind": "enum",
                    "threshold": (
                        "value IN ("
                        + ", ".join(f"'{v}'" for v in c.sample_distinct[:5])
                        + ")"
                    ),
                    "last_run_status": "pass",
                    "last_run_value": "100% in domain",
                    "severity": "warning",
                    "auto_suggested": True,
                })
        blob = {
            "table_name": t.name,
            "engine": "dataplex_auto_dq_compatible",
            "last_run_at": (
                datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc)
                .isoformat()
            ),
            "rules": rules,
        }
        path = out_dir / f"{t.name}.json"
        path.write_text(
            json.dumps(blob, indent=2, sort_keys=True), encoding="utf-8",
        )
        n += 1
    return n


def generate_ai_descriptions(out_dir: Path) -> int:
    """Knowledge-Catalog-style AI-suggested descriptions per column.

    Only emitted for columns whose MDM description is sparse — this is
    where Dataplex AI / our `llm_generated` source actually adds value.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for t in SYNTHETIC_TABLES:
        suggestions: dict[str, str] = {}
        for c in t.columns:
            # Skip if MDM already has a rich description (> 40 chars)
            if c.description and len(c.description) > 40:
                continue
            # Compose a plausible AI-generated suggestion
            base = c.business_name or c.name.replace("_", " ").title()
            type_hint = {
                "DATE": "date field",
                "TIMESTAMP": "timestamp",
                "STRING": "categorical string",
                "INT64": "integer count",
                "NUMERIC": "decimal amount",
                "FLOAT64": "floating-point measurement",
                "BOOL": "boolean flag",
            }.get(c.data_type, "field")
            suggestions[c.name] = (
                f"[AI-suggested] {base} — {type_hint} in {t.business_name}. "
                f"Likely used for {('grouping' if c.is_coded else 'filtering or aggregation')}."
            )
        if not suggestions:
            continue
        blob = {
            "table_name": t.name,
            "model": "synapse-ai-describer-v1",
            "generated_at": "2026-06-04T12:00:00+00:00",
            "column_descriptions": suggestions,
            "confidence_note": (
                "AI-generated; should be corroborated by MDM or human review "
                "before promotion past 'inferred' tier."
            ),
        }
        path = out_dir / f"{t.name}.json"
        path.write_text(
            json.dumps(blob, indent=2, sort_keys=True), encoding="utf-8",
        )
        n += 1
    return n


def generate_all_sources(out_dir: Path) -> dict[str, int]:
    """Generate all sources under one directory tree.

    Returns counts per source. Deterministic — same call, same bytes."""
    out_dir = Path(out_dir)
    counts: dict[str, int] = {}
    counts["glossary"] = generate_glossary_csv(
        out_dir / "registries" / "raw" / "glossary.csv",
    )
    counts["metric_catalog"] = generate_metric_catalog_csv(
        out_dir / "registries" / "raw" / "metric_catalog.csv",
    )
    counts["table_catalog"] = generate_table_catalog_csv(
        out_dir / "registries" / "raw" / "table_catalog.csv",
    )
    counts["mdm_cache"] = generate_mdm_cache(out_dir / "mdm_cache")
    counts["sql_corpus"] = generate_sql_corpus(out_dir / "gold_queries")
    counts["bq_profile"] = generate_bq_profile(out_dir / "bq_cache")
    counts["usage_history"] = generate_usage_history(out_dir / "usage_history")
    counts["baseline_lookml"] = generate_baseline_lookml(
        out_dir / "baseline_views",
    )
    # Dataplex-style additions
    counts["dq_rules"] = generate_dq_rules(out_dir / "dq_rules")
    counts["ai_descriptions"] = generate_ai_descriptions(out_dir / "ai_descriptions")
    return counts
