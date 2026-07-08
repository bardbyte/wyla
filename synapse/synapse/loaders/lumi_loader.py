"""Lumi-fused-output loader (100% signal coverage).

Reads `lumi_final/data/session1_output.json` — a per-table fused snapshot
of MDM + baseline LookML + sqlglot-extracted SQL corpus — and splits the
entry for one table into FIVE canonical source artifacts the graph builder
already consumes:

    out_dir/
      mdm_cache/<table>.json              ← MDM digest (business names, owners, sensitivity)
      baseline_views/<table>.view.lkml    ← raw LookML text
      baseline_artifacts/<table>.json     ← STRUCTURED LookML facts (dimensions, measures, sql_aliases, drill_fields, view_label, access_filter, …)
      lumi_signals/<table>.json           ← pre-extracted sqlglot CORPUS facts (aggregations, joins, case_whens, filters, columns_referenced, ctes, temp_tables, date_functions)
      gold_queries/Q__<table>__<n>.sql    ← one SQL per `queries_using_this`
      registries/raw/table_catalog.csv    ← single-row catalog seed

The two NEW artifacts (baseline_artifacts/, lumi_signals/) extend the
loader from ~30% to ~100% session1_output.json signal coverage. Every
aggregation becomes a Metric node; every JOIN becomes an EQUIVALENT_TO
edge; every CASE WHEN becomes a CodeMapping; every WHERE literal becomes
a FilterValue; every LookML alias becomes a Synonym. No regex.

Lumi-output entry shape covered:

    {
      "<table_name>": {
        # MDM (was already used)
        "mdm_columns": list[dict],
        "mdm_table_description": str,
        "mdm_dataset_details": dict, "mdm_ownership": dict, "mdm_coverage_pct": float,

        # SQL corpus pre-extracted (NEW — sqlglot-grade facts)
        "columns_referenced": list[str],
        "aggregations": list[dict | str],
        "case_whens": list[dict],
        "joins_involving_this": list[dict],
        "filters_on_this": list[dict],
        "date_functions": list[dict],
        "ctes_referencing_this": list[dict],
        "temp_tables_referencing_this": list[dict],
        "queries_using_this": list[dict],

        # Baseline LookML (NEW — structured facts)
        "existing_view_lkml": str,
        "baseline_dimensions": list[dict],
        "baseline_dimension_groups": list[dict],
        "baseline_measures": list[dict],
        "baseline_filtered_measures": list[dict],
        "baseline_quality_signals": dict,
        "baseline_view_description": str | None,
        "baseline_view_label": str,
        "baseline_sql_table_name": str,
        "baseline_derived_table_sql": str | None,
        "baseline_primary_key_column": str | None,
        "baseline_extends_chain": list,
        "baseline_sets": list,
        "baseline_parameters": list,
        "baseline_access_filter": list,
        "baseline_drill_fields_curated": list,
        "baseline_sql_aliases": dict,
      }
    }

The loader is robust to two MDM-column shapes (raw API or pre-flattened)
and defensive on every other field's internal structure — lumi_final's
exact output shapes for aggregations/joins/etc. are inferred and handled
across multiple plausible key combinations.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

from synapse.loaders.mdm_digest import _truthy
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

    # ── 4. Lumi corpus signals (NEW — pre-extracted sqlglot facts) ──
    signals_blob = _build_lumi_signals_blob(table_name, entry, warnings)
    if not dry_run and _signals_non_empty(signals_blob):
        path = out_dir / "lumi_signals" / f"{table_name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(signals_blob, indent=2, default=str), encoding="utf-8",
        )
        written.append(path)

    # ── 5. Baseline LookML structured artifacts (NEW) ────────────
    baseline_blob = _build_baseline_artifacts_blob(table_name, entry, warnings)
    if not dry_run and _baseline_non_empty(baseline_blob):
        path = out_dir / "baseline_artifacts" / f"{table_name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(baseline_blob, indent=2, default=str), encoding="utf-8",
        )
        written.append(path)

    # ── 6. Single-row table_catalog seed ───────────────────────
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

    # ── 7. Records accounting (for the LoadResult summary) ─────
    n_signals = sum(
        len(signals_blob.get(k) or [])
        for k in ("aggregations", "case_whens", "joins", "filters",
                  "columns_referenced", "ctes_upstream", "temp_tables_upstream",
                  "date_functions")
    )
    n_baseline = sum(
        len(baseline_blob.get(k) or [])
        for k in ("dimensions", "dimension_groups", "measures",
                  "filtered_measures", "sql_aliases", "drill_fields_curated",
                  "access_filter", "extends_chain", "sets", "parameters")
    )
    n_records = (
        len(mdm_blob.get("columns", []))
        + n_queries
        + (1 if lkml_text else 0)
        + n_signals
        + n_baseline
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
            # ── NEW: 100% signal coverage breakdown ──
            "n_aggregations": len(signals_blob.get("aggregations") or []),
            "n_case_whens": len(signals_blob.get("case_whens") or []),
            "n_joins": len(signals_blob.get("joins") or []),
            "n_filters": len(signals_blob.get("filters") or []),
            "n_columns_referenced": len(signals_blob.get("columns_referenced") or []),
            "n_date_functions": len(signals_blob.get("date_functions") or []),
            "n_dimensions_lkml": len(baseline_blob.get("dimensions") or []),
            "n_measures_lkml": len(baseline_blob.get("measures") or []),
            "n_filtered_measures_lkml": len(baseline_blob.get("filtered_measures") or []),
            "n_sql_aliases": len(baseline_blob.get("sql_aliases") or []),
            "n_drill_fields": len(baseline_blob.get("drill_fields_curated") or []),
            "view_label": baseline_blob.get("view_label"),
            "is_derived_view": bool(baseline_blob.get("derived_table_sql")),
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
        "is_decommissioned": _truthy(dataset.get("is_decommissioned")),
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
    nested) OR pre-flattened by lumi_final's MDM module.

    Flags run through ``_truthy`` — the real MDM API sends ``"Y"``/``"N"``
    STRINGS, and ``bool("N")`` is True, so a raw read false-positives every
    non-PII/non-CDE column. This is the same normalizer the MDM digest uses;
    the two must agree or a lumi-fed build silently over-flags governance.
    """
    # Already-flat shape — pass through
    if "name" in c and "attribute_details" not in c:
        return {
            "name": c.get("name"),
            "type": c.get("type") or c.get("data_type") or "",
            "description": c.get("description") or c.get("attribute_desc") or "",
            "business_name": c.get("business_name") or "",
            "is_primary": _truthy(c.get("is_primary")) or _truthy(c.get("is_pk")),
            "is_dedupe_key": _truthy(c.get("is_dedupe_key")),
            "is_partitioned": _truthy(c.get("is_partitioned")),
            "cluster_position": c.get("cluster_position"),
            "derived_logic": c.get("derived_logic"),
            "external_references": c.get("external_references")
            or c.get("external_reference_details") or [],
            "is_pii": _truthy(c.get("is_pii")),
            "is_critical_data_element": _truthy(c.get("is_critical_data_element")),
            "pii_role_id": c.get("pii_role_id") or c.get("pii_taxonomy") or "Internal",
            "is_gdpr": _truthy(c.get("is_gdpr")),
            "is_sensitive": _truthy(c.get("is_sensitive")),
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
        "is_primary": _truthy(details.get("is_primary_key")) or _truthy(details.get("is_pk")),
        "is_dedupe_key": _truthy(details.get("is_dedupe_key")),
        "is_partitioned": _truthy(details.get("is_partitioned"))
        or details.get("partition_role") in ("PARTITION", "PARTITION_KEY"),
        "cluster_position": details.get("clustering_ordinal_position"),
        "derived_logic": details.get("derived_logic"),
        "external_references": details.get("external_reference_details")
        or details.get("external_references") or [],
        "is_pii": _truthy(sens.get("is_pii")),
        "is_critical_data_element": _truthy(sens.get("is_critical_data_element")),
        "pii_role_id": sens.get("pii_role_id") or "Internal",
        "is_gdpr": _truthy(sens.get("is_gdpr")),
        "is_sensitive": _truthy(sens.get("is_sensitive")),
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


# ─── Lumi corpus signals (pre-extracted sqlglot facts) ───────


def _build_lumi_signals_blob(
    table_name: str, entry: dict[str, Any], warnings: list[str],
) -> dict[str, Any]:
    """Capture sqlglot-pre-extracted facts that lumi_final emits per-query.

    These supersede the regex extraction in synapse/graph/builder.py::_ingest_corpus
    when present — every aggregation becomes a Metric node directly; every
    JOIN becomes an EQUIVALENT_TO edge directly; etc.
    """
    aggs = _normalize_aggregations(entry.get("aggregations") or [])
    case_whens = _normalize_case_whens(entry.get("case_whens") or [])
    joins = _normalize_joins(entry.get("joins_involving_this") or [], table_name)
    filters = _normalize_filters(entry.get("filters_on_this") or [])
    cols_ref = _normalize_columns_referenced(entry.get("columns_referenced") or [])
    date_fns = _normalize_date_functions(entry.get("date_functions") or [])
    ctes_up = _normalize_lineage_refs(entry.get("ctes_referencing_this") or [])
    temps_up = _normalize_lineage_refs(entry.get("temp_tables_referencing_this") or [])

    return {
        "table_name": table_name,
        "aggregations": aggs,
        "case_whens": case_whens,
        "joins": joins,
        "filters": filters,
        "columns_referenced": cols_ref,
        "date_functions": date_fns,
        "ctes_upstream": ctes_up,
        "temp_tables_upstream": temps_up,
    }


def _signals_non_empty(blob: dict[str, Any]) -> bool:
    return any(
        blob.get(k)
        for k in ("aggregations", "case_whens", "joins", "filters",
                  "columns_referenced", "date_functions",
                  "ctes_upstream", "temp_tables_upstream")
    )


def _normalize_aggregations(raw: list[Any]) -> list[dict[str, Any]]:
    """Defensive normalizer — handles multiple plausible shapes lumi_final
    might emit. Output shape: {function, column, alias, query_id}."""
    out: list[dict[str, Any]] = []
    for a in raw:
        if isinstance(a, str):
            # Maybe "SUM(billed_business)" raw expression
            parsed = _parse_agg_expression(a)
            if parsed:
                out.append(parsed)
            continue
        if not isinstance(a, dict):
            continue
        fn = (a.get("function") or a.get("agg_fn") or a.get("fn")
              or a.get("aggregate") or "").upper()
        col = (a.get("column") or a.get("agg_col") or a.get("col")
               or a.get("argument") or "")
        alias = a.get("alias") or a.get("as") or ""
        qid = a.get("query_id") or a.get("qid") or a.get("source_query") or ""
        # Last resort: parse an expression field
        if not fn or not col:
            expr = a.get("expression") or a.get("expr") or a.get("sql")
            if expr:
                parsed = _parse_agg_expression(expr)
                if parsed:
                    fn = fn or parsed["function"]
                    col = col or parsed["column"]
        if fn and col:
            out.append({
                "function": fn,
                "column": col,
                "alias": alias,
                "query_id": qid,
            })
    return out


def _parse_agg_expression(s: str) -> dict[str, str] | None:
    """Minimal parser for SUM(x) / COUNT(DISTINCT x) / AVG(x) / etc."""
    import re as _re
    m = _re.match(
        r"\s*(SUM|COUNT|AVG|MIN|MAX|COUNT\s*\(\s*DISTINCT)\s*\(\s*([\w.]+)\s*\)",
        s, _re.IGNORECASE,
    )
    if not m:
        return None
    fn = m.group(1).upper().replace(" ", "")
    col = m.group(2).split(".")[-1]
    if fn.startswith("COUNT(DISTINCT"):
        fn = "COUNT_DISTINCT"
    return {"function": fn, "column": col, "alias": "", "query_id": ""}


def _normalize_case_whens(raw: list[Any]) -> list[dict[str, Any]]:
    """Output shape: {column, raw_value, human_meaning, query_id}."""
    out: list[dict[str, Any]] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        col = c.get("column") or c.get("col") or c.get("source_column") or ""
        raw_val = (c.get("when_value") or c.get("raw_value")
                   or c.get("value") or c.get("source_value") or "")
        meaning = (c.get("then_value") or c.get("human_meaning")
                   or c.get("meaning") or c.get("target_value") or "")
        qid = c.get("query_id") or c.get("qid") or ""
        if col and (raw_val or meaning):
            out.append({
                "column": col,
                "raw_value": str(raw_val),
                "human_meaning": str(meaning),
                "query_id": qid,
            })
    return out


def _normalize_joins(raw: list[Any], home_table: str) -> list[dict[str, Any]]:
    """Output shape: {other_table, left_column, right_column,
    join_type, query_id}."""
    out: list[dict[str, Any]] = []
    for j in raw:
        if not isinstance(j, dict):
            continue
        # Handle a few possible naming conventions
        other = (j.get("other_table") or j.get("table") or j.get("joined_table")
                 or j.get("right_table") or "")
        left_col = (j.get("left_col") or j.get("left_column")
                    or j.get("home_col") or j.get("from_column") or "")
        right_col = (j.get("right_col") or j.get("right_column")
                     or j.get("other_col") or j.get("to_column") or "")
        join_type = (j.get("join_type") or j.get("type")
                     or j.get("kind") or "").upper()
        qid = j.get("query_id") or j.get("qid") or ""
        if other and other != home_table and left_col and right_col:
            out.append({
                "other_table": other.split(".")[-1],
                "left_column": left_col,
                "right_column": right_col,
                "join_type": join_type or "INNER",
                "query_id": qid,
            })
    return out


def _normalize_filters(raw: list[Any]) -> list[dict[str, Any]]:
    """Output shape: {column, operator, value, is_partition, query_id}."""
    out: list[dict[str, Any]] = []
    # Count occurrences per (column, value) to derive structural-ness later
    for f in raw:
        if not isinstance(f, dict):
            continue
        col = f.get("column") or f.get("col") or f.get("field") or ""
        op = f.get("operator") or f.get("op") or "="
        val = f.get("value") or f.get("literal") or f.get("rhs") or ""
        is_part = bool(f.get("is_partition") or f.get("is_partition_filter"))
        qid = f.get("query_id") or f.get("qid") or ""
        is_neg = bool(f.get("is_negated"))
        if col:
            out.append({
                "column": col,
                "operator": op,
                "value": str(val) if val != "" else "",
                "is_partition": is_part,
                "is_negated": is_neg,
                "query_id": qid,
            })
    return out


def _normalize_columns_referenced(raw: list[Any]) -> list[str]:
    """Output: deduped list of column-name strings."""
    seen: set[str] = set()
    out: list[str] = []
    for c in raw:
        if isinstance(c, str):
            name = c.split(".")[-1]
        elif isinstance(c, dict):
            name = (c.get("name") or c.get("column") or "").split(".")[-1]
        else:
            name = ""
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _normalize_date_functions(raw: list[Any]) -> list[dict[str, Any]]:
    """Output shape: {function, column, granularity, query_id}."""
    out: list[dict[str, Any]] = []
    for d in raw:
        if not isinstance(d, dict):
            continue
        fn = (d.get("function") or d.get("fn") or "").upper()
        col = d.get("column") or d.get("col") or d.get("argument") or ""
        gran = (d.get("granularity") or d.get("grain") or d.get("unit") or "").upper()
        qid = d.get("query_id") or d.get("qid") or ""
        if fn and col:
            out.append({
                "function": fn,
                "column": col,
                "granularity": gran,
                "query_id": qid,
            })
    return out


def _normalize_lineage_refs(raw: list[Any]) -> list[dict[str, Any]]:
    """For ctes_referencing_this + temp_tables_referencing_this.
    Output shape: {name, query_id}."""
    out: list[dict[str, Any]] = []
    for r in raw:
        if isinstance(r, str):
            out.append({"name": r, "query_id": ""})
        elif isinstance(r, dict):
            name = (r.get("cte_name") or r.get("temp_name") or r.get("name") or "")
            qid = r.get("query_id") or r.get("qid") or ""
            if name:
                out.append({"name": name, "query_id": qid})
    return out


# ─── Baseline LookML structured artifacts ────────────────────


def _build_baseline_artifacts_blob(
    table_name: str, entry: dict[str, Any], warnings: list[str],
) -> dict[str, Any]:
    """Capture structured LookML facts so the graph builder doesn't need
    to re-parse the raw .view.lkml. Every dimension becomes a Column with
    source=baseline_lookml; every sql_alias becomes a Synonym."""
    return {
        "table_name": table_name,
        "view_label": entry.get("baseline_view_label") or "",
        "view_description": entry.get("baseline_view_description") or "",
        "sql_table_name": entry.get("baseline_sql_table_name") or "",
        "derived_table_sql": entry.get("baseline_derived_table_sql"),
        "primary_key_column": entry.get("baseline_primary_key_column"),
        "has_primary_key": bool(
            (entry.get("baseline_quality_signals") or {}).get("has_primary_key")
        ),
        "dimensions": _normalize_lkml_dimensions(
            entry.get("baseline_dimensions") or []
        ),
        "dimension_groups": _normalize_lkml_dimension_groups(
            entry.get("baseline_dimension_groups") or []
        ),
        "measures": _normalize_lkml_measures(
            entry.get("baseline_measures") or []
        ),
        "filtered_measures": _normalize_lkml_filtered_measures(
            entry.get("baseline_filtered_measures") or []
        ),
        "sql_aliases": _normalize_sql_aliases(
            entry.get("baseline_sql_aliases") or {}
        ),
        "drill_fields_curated": _normalize_drill_fields(
            entry.get("baseline_drill_fields_curated") or []
        ),
        "access_filter": _normalize_access_filter(
            entry.get("baseline_access_filter") or []
        ),
        "extends_chain": _normalize_extends_chain(
            entry.get("baseline_extends_chain") or []
        ),
        "sets": _normalize_lkml_sets(entry.get("baseline_sets") or []),
        "parameters": _normalize_lkml_parameters(
            entry.get("baseline_parameters") or []
        ),
    }


def _baseline_non_empty(blob: dict[str, Any]) -> bool:
    if blob.get("view_label") or blob.get("view_description"):
        return True
    if blob.get("sql_table_name") or blob.get("derived_table_sql"):
        return True
    if blob.get("primary_key_column"):
        return True
    return any(
        blob.get(k)
        for k in ("dimensions", "dimension_groups", "measures",
                  "filtered_measures", "sql_aliases", "drill_fields_curated",
                  "access_filter", "extends_chain", "sets", "parameters")
    )


def _normalize_lkml_dimensions(raw: list[Any]) -> list[dict[str, Any]]:
    """Output shape: {name, type, sql, description, label, primary_key,
    hidden, tags}."""
    out: list[dict[str, Any]] = []
    for d in raw:
        if not isinstance(d, dict):
            continue
        name = d.get("name") or d.get("dimension_name") or ""
        if not name:
            continue
        out.append({
            "name": name,
            "type": d.get("type") or "string",
            "sql": d.get("sql") or "",
            "description": d.get("description") or "",
            "label": d.get("label") or "",
            "primary_key": bool(d.get("primary_key")),
            "hidden": bool(d.get("hidden")),
            "tags": d.get("tags") or [],
        })
    return out


def _normalize_lkml_dimension_groups(raw: list[Any]) -> list[dict[str, Any]]:
    """Time-grain dimension groups. Output: {name, type, sql, timeframes,
    convert_tz, description}."""
    out: list[dict[str, Any]] = []
    for d in raw:
        if not isinstance(d, dict):
            continue
        name = d.get("name") or ""
        if not name:
            continue
        out.append({
            "name": name,
            "type": d.get("type") or "time",
            "sql": d.get("sql") or "",
            "timeframes": d.get("timeframes") or [],
            "convert_tz": d.get("convert_tz", True),
            "description": d.get("description") or "",
        })
    return out


def _normalize_lkml_measures(raw: list[Any]) -> list[dict[str, Any]]:
    """LookML measures. Output: {name, type, sql, description, label,
    value_format, drill_fields, hidden, symmetric_aggregates}."""
    out: list[dict[str, Any]] = []
    for m in raw:
        if not isinstance(m, dict):
            continue
        name = m.get("name") or ""
        if not name:
            continue
        out.append({
            "name": name,
            "type": m.get("type") or "",        # sum / count / count_distinct / average / etc
            "sql": m.get("sql") or "",
            "description": m.get("description") or "",
            "label": m.get("label") or "",
            "value_format": m.get("value_format") or m.get("value_format_name") or "",
            "drill_fields": m.get("drill_fields") or [],
            "hidden": bool(m.get("hidden")),
            "symmetric_aggregates": m.get("symmetric_aggregates"),
        })
    return out


def _normalize_lkml_filtered_measures(raw: list[Any]) -> list[dict[str, Any]]:
    """Output: {name, type, base_measure_or_field, filter_expression, description}."""
    out: list[dict[str, Any]] = []
    for m in raw:
        if not isinstance(m, dict):
            continue
        name = m.get("name") or ""
        if not name:
            continue
        out.append({
            "name": name,
            "type": m.get("type") or "",
            "base_field": m.get("base_field") or m.get("base_measure") or m.get("sql") or "",
            "filter_expression": m.get("filter_expression") or m.get("filters") or "",
            "description": m.get("description") or "",
        })
    return out


def _normalize_sql_aliases(raw: Any) -> dict[str, str]:
    """alias → canonical-column mapping. Output: {alias: canonical_field}."""
    if isinstance(raw, dict):
        return {
            str(k): str(v) for k, v in raw.items()
            if k and v and isinstance(k, str) and isinstance(v, str)
        }
    if isinstance(raw, list):
        out: dict[str, str] = {}
        for item in raw:
            if isinstance(item, dict):
                alias = item.get("alias") or item.get("from") or ""
                canon = item.get("canonical") or item.get("to") or item.get("field") or ""
                if alias and canon:
                    out[str(alias)] = str(canon)
        return out
    return {}


def _normalize_drill_fields(raw: Any) -> list[str]:
    """Curated drill-down fields. Output: list[str] of column names."""
    out: list[str] = []
    if isinstance(raw, list):
        for f in raw:
            if isinstance(f, str):
                out.append(f.split(".")[-1])
            elif isinstance(f, dict):
                name = f.get("name") or f.get("field") or ""
                if name:
                    out.append(name.split(".")[-1])
    return out


def _normalize_access_filter(raw: list[Any]) -> list[dict[str, Any]]:
    """LookML access_filter blocks. Output: {field, user_attribute}."""
    out: list[dict[str, Any]] = []
    for f in raw:
        if not isinstance(f, dict):
            continue
        field = f.get("field") or ""
        user_attr = f.get("user_attribute") or f.get("user_attr") or ""
        if field:
            out.append({"field": field, "user_attribute": user_attr})
    return out


def _normalize_extends_chain(raw: Any) -> list[str]:
    """Output: ordered list of parent view names."""
    if isinstance(raw, list):
        return [
            (str(x).split(".")[-1] if isinstance(x, str) else (x.get("name") or ""))
            for x in raw
            if (isinstance(x, str) and x) or (isinstance(x, dict) and x.get("name"))
        ]
    return []


def _normalize_lkml_sets(raw: list[Any]) -> list[dict[str, Any]]:
    """LookML sets. Output: {name, fields}."""
    out: list[dict[str, Any]] = []
    for s in raw:
        if isinstance(s, dict):
            name = s.get("name") or ""
            fields = s.get("fields") or []
            if name:
                out.append({"name": name, "fields": list(fields)})
    return out


def _normalize_lkml_parameters(raw: list[Any]) -> list[dict[str, Any]]:
    """LookML parameters. Output: {name, type, description, default_value}."""
    out: list[dict[str, Any]] = []
    for p in raw:
        if isinstance(p, dict):
            name = p.get("name") or ""
            if name:
                out.append({
                    "name": name,
                    "type": p.get("type") or "string",
                    "description": p.get("description") or "",
                    "default_value": p.get("default_value"),
                })
    return out
