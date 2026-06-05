"""Lumi-fused-output loader.

Reads `lumi_final/data/session1_output.json` — a per-table fused snapshot
of MDM + baseline LookML + sqlglot-extracted SQL corpus — and splits the
entry for one table into the canonical source JSONs the graph builder
already consumes:

    out_dir/
      mdm_cache/<table>.json         ← MDM digest
      baseline_views/<table>.view.lkml ← raw LookML
      gold_queries/Q__<table>__<n>.sql ← one SQL per `queries_using_this`
      registries/raw/table_catalog.csv ← single-row catalog seed

Lumi-output entry shape (per screenshot of session1_output.json):

    {
      "<table_name>": {
        "table_name": str,
        "columns_referenced": list[str],
        "aggregations": list, "case_whens": list,
        "joins_involving_this": list, "filters_on_this": list,
        "date_functions": list,
        "mdm_columns": list[dict],
        "mdm_table_description": str,
        "mdm_dataset_details": dict, "mdm_ownership": dict,
        "existing_view_lkml": str,
        "baseline_dimensions": list, "baseline_measures": list,
        "baseline_quality_signals": dict,
        "baseline_view_description": str | None,
        "baseline_primary_key_column": str | None,
        "baseline_sql_aliases": dict, ...
        "queries_using_this": list[dict]
      }
    }

The loader is robust to two MDM-column shapes (raw API or pre-flattened);
unknown keys are passed through.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

from synapse.loaders.types import LoadResult


def load_lumi_for_table(
    table_name: str,
    *,
    lumi_path: Path,
    out_dir: Path,
    dry_run: bool = False,
) -> LoadResult:
    """Split a Lumi session1 entry into canonical source JSONs.

    Args:
        table_name: which table entry to extract from the top-level dict.
        lumi_path: path to session1_output.json (or any file with the same shape).
        out_dir: parent folder under which mdm_cache/, baseline_views/, gold_queries/,
                 registries/ land.
        dry_run: validate + count without writing.
    """
    t0 = time.time()
    warnings: list[str] = []
    written: list[Path] = []

    if not lumi_path.exists():
        return LoadResult(
            status="error", source="lumi", table_id=table_name,
            error=f"lumi output not found: {lumi_path}",
        )

    try:
        payload = json.loads(lumi_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return LoadResult(
            status="error", source="lumi", table_id=table_name,
            error=f"lumi output is not valid JSON: {e}",
        )

    if not isinstance(payload, dict):
        return LoadResult(
            status="error", source="lumi", table_id=table_name,
            error=f"lumi output root is {type(payload).__name__}, expected dict-of-tables",
        )

    entry = payload.get(table_name)
    if not isinstance(entry, dict):
        available = sorted(payload.keys())[:20]
        return LoadResult(
            status="error", source="lumi", table_id=table_name,
            error=(
                f"table '{table_name}' not in lumi output. "
                f"Available (first 20 of {len(payload)}): {available}"
            ),
        )

    # ── 1. MDM digest ──────────────────────────────────────────
    mdm_blob = _build_mdm_blob(table_name, entry, warnings)
    if not dry_run:
        path = out_dir / "mdm_cache" / f"{table_name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(mdm_blob, indent=2, default=str), encoding="utf-8")
        written.append(path)

    # ── 2. Baseline LookML ─────────────────────────────────────
    lkml_text = entry.get("existing_view_lkml") or ""
    if lkml_text:
        if not dry_run:
            path = out_dir / "baseline_views" / f"{table_name}.view.lkml"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(lkml_text, encoding="utf-8")
            written.append(path)
    else:
        warnings.append("no existing_view_lkml in lumi entry — baseline source skipped")

    # ── 3. Gold queries from queries_using_this ────────────────
    queries = entry.get("queries_using_this") or []
    n_queries = 0
    if queries and not dry_run:
        qdir = out_dir / "gold_queries"
        qdir.mkdir(parents=True, exist_ok=True)
        for i, q in enumerate(queries):
            sql_text = _extract_sql(q)
            if not sql_text:
                continue
            qid = _extract_query_id(q, fallback=f"Q__{table_name}__{i:03d}")
            user = _extract_user(q)
            ts = _extract_timestamp(q)
            path = qdir / f"{qid}.sql"
            header = (
                f"-- query_id: {qid}\n"
                + (f"-- user: {user}\n" if user else "")
                + (f"-- created_at: {ts}\n" if ts else "")
                + "-- source: lumi session1_output\n"
            )
            path.write_text(header + sql_text.strip() + "\n", encoding="utf-8")
            written.append(path)
            n_queries += 1
    elif queries and dry_run:
        n_queries = sum(1 for q in queries if _extract_sql(q))

    if not queries:
        warnings.append("no queries_using_this — corpus signal skipped")

    # ── 4. Single-row table_catalog seed ───────────────────────
    if not dry_run:
        cat_path = out_dir / "registries" / "raw" / "table_catalog.csv"
        cat_path.parent.mkdir(parents=True, exist_ok=True)
        if not cat_path.exists():
            with cat_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["table_name", "IS IN DMP", "company_domain", "data_domain"])
                w.writerow([
                    table_name, "Yes",
                    (entry.get("mdm_dataset_details") or {}).get("data_sub_category", "Finance"),
                    (entry.get("mdm_dataset_details") or {}).get("data_category", "Cardmember"),
                ])
            written.append(cat_path)

    # ── 5. Records accounting (for the LoadResult summary) ─────
    n_records = (
        len(mdm_blob.get("columns", []))
        + n_queries
        + (1 if lkml_text else 0)
    )

    return LoadResult(
        status="ok" if not warnings else "partial",
        source="lumi",
        table_id=table_name,
        artifacts_written=written,
        records_count=n_records,
        warnings=warnings,
        latency_ms=int((time.time() - t0) * 1000),
        metadata={
            "n_columns_mdm": len(mdm_blob.get("columns", [])),
            "n_queries": n_queries,
            "has_lkml": bool(lkml_text),
            "has_primary_key": bool(
                (entry.get("baseline_quality_signals") or {}).get("has_primary_key")
            ),
            "mdm_coverage_pct": entry.get("mdm_coverage_pct", 0.0),
        },
    )


# ─── Internals ───────────────────────────────────────────────


def _build_mdm_blob(
    table_name: str, entry: dict[str, Any], warnings: list[str],
) -> dict[str, Any]:
    """Map a Lumi entry to the MDM digest shape the graph builder reads.

    Mirrors synapse/loaders/mdm_loader.py::_digest_mdm_response so the
    resulting JSON is interchangeable."""
    dataset = entry.get("mdm_dataset_details") or {}
    owners  = entry.get("mdm_ownership") or {}
    mdm_cols = entry.get("mdm_columns") or []

    cols_out: list[dict[str, Any]] = []
    partition_field: str | None = None
    for c in mdm_cols:
        if not isinstance(c, dict):
            continue
        normalized = _normalize_mdm_column(c)
        if not normalized.get("name"):
            continue
        if normalized.get("is_partitioned") and not partition_field:
            partition_field = normalized["name"]
        cols_out.append(normalized)

    business_contacts = owners.get("business_contacts") or []
    tech_contacts = owners.get("tech_contacts") or []

    return {
        "table_name": table_name,
        "display_name": dataset.get("business_name") or table_name,
        "table_business_name": dataset.get("business_name") or "",
        "table_description": (
            entry.get("mdm_table_description")
            or dataset.get("data_desc")
            or ""
        ),
        "data_category": dataset.get("data_category") or "",
        "data_sub_category": dataset.get("data_sub_category") or "",
        "bq_project": _resolve_templated(
            (entry.get("mdm_dataset_details", {}) or {}).get("project_id", "")
        ),
        "bq_dataset": dataset.get("dataset_name") or "",
        "bq_table": dataset.get("table_name") or table_name,
        "feed_type": dataset.get("feed_type") or "",
        "table_type": dataset.get("table_type") or "",
        "is_decommissioned": bool(dataset.get("is_decommissioned")),
        "partition_field": partition_field,
        "row_count_estimate": _maybe_int(dataset.get("row_count")),
        "ownership": {
            "imr_queue": owners.get("imr_queue"),
            "aim_id": owners.get("aim_id"),
            "business_contacts": business_contacts,
            "tech_contacts": tech_contacts,
        },
        "columns": cols_out,
        "mdm_coverage_pct": float(entry.get("mdm_coverage_pct") or 0.0),
        "asset_kind": _infer_asset_kind(entry),
        "tags": [],
        "lineage_upstream": [],
    }


def _normalize_mdm_column(c: dict[str, Any]) -> dict[str, Any]:
    """Handle two possible shapes: raw MDM API (attribute_details/sensitivity_details
    nested) OR pre-flattened by lumi_final's MDM module."""
    # Already-flat shape — pass through
    if "name" in c and "attribute_details" not in c:
        return {
            "name": c.get("name"),
            "type": c.get("type") or c.get("data_type") or "",
            "description": c.get("description") or c.get("attribute_desc") or "",
            "business_name": c.get("business_name") or "",
            "is_primary": bool(c.get("is_primary") or c.get("is_pk")),
            "is_dedupe_key": bool(c.get("is_dedupe_key")),
            "is_partitioned": bool(c.get("is_partitioned")),
            "cluster_position": c.get("cluster_position"),
            "derived_logic": c.get("derived_logic"),
            "is_pii": bool(c.get("is_pii")),
            "is_critical_data_element": bool(c.get("is_critical_data_element")),
            "pii_role_id": c.get("pii_role_id") or c.get("pii_taxonomy") or "Internal",
            "is_gdpr": bool(c.get("is_gdpr")),
        }
    # Raw API shape
    details = c.get("attribute_details") or {}
    sens = c.get("sensitivity_details") or {}
    name = c.get("attribute_name") or details.get("attribute_name")
    return {
        "name": name,
        "type": details.get("attribute_type") or details.get("data_type") or "",
        "description": details.get("attribute_desc") or "",
        "business_name": details.get("business_name") or "",
        "is_primary": bool(details.get("is_primary_key") or details.get("is_pk")),
        "is_dedupe_key": bool(details.get("is_dedupe_key")),
        "is_partitioned": bool(details.get("is_partitioned") or details.get("partition_role") in ("PARTITION", "PARTITION_KEY")),
        "cluster_position": details.get("clustering_ordinal_position"),
        "derived_logic": details.get("derived_logic"),
        "is_pii": bool(sens.get("is_pii")),
        "is_critical_data_element": bool(sens.get("is_critical_data_element")),
        "pii_role_id": sens.get("pii_role_id") or "Internal",
        "is_gdpr": bool(sens.get("is_gdpr")),
    }


def _extract_sql(q: Any) -> str:
    if isinstance(q, str):
        return q
    if isinstance(q, dict):
        return q.get("query") or q.get("sql") or q.get("statement") or ""
    return ""


def _extract_query_id(q: Any, *, fallback: str) -> str:
    if isinstance(q, dict):
        return str(q.get("query_id") or q.get("id") or q.get("job_id") or fallback)
    return fallback


def _extract_user(q: Any) -> str | None:
    if isinstance(q, dict):
        return q.get("user_email") or q.get("user") or None
    return None


def _extract_timestamp(q: Any) -> str | None:
    if isinstance(q, dict):
        return q.get("creation_time") or q.get("created_at") or q.get("timestamp") or None
    return None


def _infer_asset_kind(entry: dict) -> str:
    """View if baseline_derived_table_sql is set; else Table."""
    if entry.get("baseline_derived_table_sql"):
        return "View"
    table_type = (entry.get("mdm_dataset_details") or {}).get("table_type", "")
    return {
        "VIEW": "View",
        "MATERIALIZED VIEW": "MaterializedView",
        "EXTERNAL": "ExternalTable",
    }.get((table_type or "").upper(), "Table")


def _resolve_templated(s: str | None) -> str:
    """Pass through templated `@context.system/...` MDM values."""
    return s or ""


def _maybe_int(v: Any) -> int | None:
    if v in (None, "", "null"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None
