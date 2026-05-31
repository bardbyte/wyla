"""Exploratory BQ probe — dumps EVERY signal BigQuery exposes for one
table, plus the analytical signals that would feed a semantic graph.

Same spirit as scripts/explore_mdm_payload.py: hit the API in every way
we know, save the dump, look at it, then design the production probe.

Usage:
    python scripts/explore_bq_payload.py --table cornerstone_metrics
    python scripts/explore_bq_payload.py --table T --no-catalog --no-jobs
    python scripts/explore_bq_payload.py --table T --save out.json

Env:
    GOOGLE_APPLICATION_CREDENTIALS   service-account JSON
    LUMI_BQ_PROJECT                  default: prj-d-ea-poc
    LUMI_BQ_DATASET                  default: dw

What it pulls (every available source):

  ── Structural / metadata (all FREE) ──────────────────────────────
    01  INFORMATION_SCHEMA.TABLES        identity, type, creation, ddl
    02  INFORMATION_SCHEMA.COLUMNS       schema + BQ description per col
    03  INFORMATION_SCHEMA.TABLE_OPTIONS labels, friendly_name, desc
    04  INFORMATION_SCHEMA.TABLE_STORAGE row count, byte size, freshness
    05  INFORMATION_SCHEMA.PARTITIONS    per-partition skew + dormancy
    06  INFORMATION_SCHEMA.COLUMN_FIELD_PATHS  nested STRUCT/ARRAY paths
    07  INFORMATION_SCHEMA.VIEWS         view_definition (if view)
    08  INFORMATION_SCHEMA.MATERIALIZED_VIEWS  MV refresh metadata
    09  INFORMATION_SCHEMA.TABLE_CONSTRAINTS + KEY_COLUMN_USAGE +
        CONSTRAINT_COLUMN_USAGE        engineer-declared PK/FK

  ── Governance (FREE, optional) ──────────────────────────────────
    10  Data Catalog policy tags         hierarchical PII taxonomy
    11  Data Catalog user-applied tags   business glossary, steward, etc.

  ── Analytical (1 scan / table, optional) ────────────────────────
    12  Per-column APPROX_COUNT_DISTINCT + NULL count + MIN/MAX
    13  Top-N distinct values per column (cardinality-gated)
    14  Sample rows (TABLESAMPLE 0.01% or LIMIT for tiny tables)

  ── The "code-value resolution" problem (NEW — corpus-wide scan) ──
    15  Coded-column heuristic           which cols look like opaque codes?
    16  Candidate lookup tables          dim_*/ref_*/lookup_*/code_* small
                                        tables in the same dataset whose
                                        primary column has a value-set
                                        overlap with our suspected coded
                                        columns. Maps `005 → Platinum`.

  ── Usage (FREE in some quotas, optional) ────────────────────────
    17  INFORMATION_SCHEMA.JOBS_BY_PROJECT  recent queries that
                                        touched this table (the "verb
                                        layer beyond the 122 gold SQLs")

Output: one comprehensive JSON to --save (or stdout). Plus a
human-readable summary printed to terminal.

This is exploratory. After you run it on one table and look at what's
there, we design the production probe / record_bq_facts hook based on
what's actually populated in your warehouse.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# ── TLS / corporate-MITM bootstrap ────────────────────────────────
try:
    import truststore  # type: ignore[import-not-found]
    truststore.inject_into_ssl()
except ImportError:
    pass

try:
    from google.api_core import exceptions as gcp_exceptions
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError as e:
    print(
        f"ERROR: {e}. Install with:\n"
        "  pip install google-cloud-bigquery truststore",
        file=sys.stderr,
    )
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger("explore_bq_payload")

DEFAULT_PROJECT = os.environ.get("LUMI_BQ_PROJECT", "your-bq-project")
DEFAULT_DATASET = os.environ.get("LUMI_BQ_DATASET", "dw")

# ─── Pretty print ────────────────────────────────────────────

def _hdr(msg: str) -> None: print(f"\n\033[1;36m═══ {msg} ═══\033[0m")
def _sub(msg: str) -> None: print(f"\033[1;34m── {msg} ──\033[0m")
def _pass(msg: str) -> None: print(f"  \033[1;32m✓\033[0m {msg}")
def _fail(msg: str) -> None: print(f"  \033[1;31m✗\033[0m {msg}")
def _info(msg: str) -> None: print(f"    \033[2m{msg}\033[0m")
def _warn(msg: str) -> None: print(f"  \033[1;33m!\033[0m {msg}")


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _to_int(v: Any) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _json_safe(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _json_safe(vv) for k, vv in v.items()}
    return str(v)


# ─── BQ client ───────────────────────────────────────────────

def _build_client(project: str) -> bigquery.Client:
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and Path(creds_path).exists():
        creds = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)


def _q(bq: bigquery.Client, sql: str, label: str) -> list[dict[str, Any]]:
    """Run a query, return rows as dicts. On failure logs + returns []."""
    try:
        return [dict(r) for r in bq.query(sql).result()]
    except gcp_exceptions.GoogleAPICallError as e:
        logger.warning("[%s] failed: %s", label, e)
        return []


# ─── 01-09: INFORMATION_SCHEMA dump ──────────────────────────

def dump_info_schema(
    bq: bigquery.Client, project: str, dataset: str, table: str,
) -> dict[str, Any]:
    p, d, t = project, dataset, table

    cols = _q(bq, f"""
        SELECT column_name, data_type, is_nullable, is_partitioning_column,
               clustering_ordinal_position, ordinal_position, column_default,
               description AS column_description
        FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{t}'
        ORDER BY ordinal_position
    """, "COLUMNS")

    tables = _q(bq, f"""
        SELECT table_type, creation_time, ddl
        FROM `{p}.{d}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{t}'
    """, "TABLES")

    options = _q(bq, f"""
        SELECT option_name, option_type, option_value
        FROM `{p}.{d}.INFORMATION_SCHEMA.TABLE_OPTIONS`
        WHERE table_name = '{t}'
    """, "TABLE_OPTIONS")

    storage = _q(bq, f"""
        SELECT total_rows, total_logical_bytes, total_physical_bytes,
               storage_last_modified_time
        FROM `{p}.{d}.INFORMATION_SCHEMA.TABLE_STORAGE`
        WHERE table_name = '{t}'
    """, "TABLE_STORAGE")

    partitions = _q(bq, f"""
        SELECT partition_id, total_rows, total_logical_bytes,
               last_modified_time
        FROM `{p}.{d}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = '{t}'
          AND partition_id IS NOT NULL
        ORDER BY last_modified_time DESC NULLS LAST
        LIMIT 50
    """, "PARTITIONS")

    nested = _q(bq, f"""
        SELECT column_name AS root_column, field_path, data_type, description
        FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
        WHERE table_name = '{t}' AND field_path != column_name
    """, "COLUMN_FIELD_PATHS")

    views = _q(bq, f"""
        SELECT view_definition, use_standard_sql
        FROM `{p}.{d}.INFORMATION_SCHEMA.VIEWS`
        WHERE table_name = '{t}'
    """, "VIEWS")

    mviews = _q(bq, f"""
        SELECT last_refresh_time, refresh_interval_minutes,
               enable_refresh, allow_non_incremental_definition
        FROM `{p}.{d}.INFORMATION_SCHEMA.MATERIALIZED_VIEWS`
        WHERE table_name = '{t}'
    """, "MATERIALIZED_VIEWS")

    constraints = _q(bq, f"""
        SELECT tc.constraint_name, tc.constraint_type,
               kcu.column_name AS from_column,
               ccu.table_name  AS to_table,
               ccu.column_name AS to_column
        FROM `{p}.{d}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
        LEFT JOIN `{p}.{d}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
          USING (constraint_name)
        LEFT JOIN `{p}.{d}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
          USING (constraint_name)
        WHERE tc.table_name = '{t}'
    """, "TABLE_CONSTRAINTS")

    routines = _q(bq, f"""
        SELECT routine_name, routine_type, data_type AS return_type,
               language, routine_body
        FROM `{p}.{d}.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_definition LIKE '%{t}%'
        LIMIT 20
    """, "ROUTINES referencing this table")

    return {
        "columns": cols,
        "tables": tables,
        "options": options,
        "storage": storage,
        "partitions": partitions,
        "nested_field_paths": nested,
        "views": views,
        "materialized_views": mviews,
        "constraints": constraints,
        "routines_referencing": routines,
    }


# ─── 10-11: Data Catalog (governance, optional) ──────────────

def dump_catalog(project: str, dataset: str, table: str) -> dict[str, Any]:
    try:
        from google.cloud import datacatalog_v1  # type: ignore[import-not-found]
    except ImportError:
        return {"available": False, "reason": "google-cloud-datacatalog not installed"}
    try:
        client = datacatalog_v1.DataCatalogClient()
        resource_name = (
            f"//bigquery.googleapis.com/projects/{project}/"
            f"datasets/{dataset}/tables/{table}"
        )
        entry = client.lookup_entry(request={"linked_resource": resource_name})

        column_policy_tags: dict[str, list[str]] = {}
        for col in (getattr(entry.schema, "columns", []) or []):
            pt = getattr(col, "policy_tags", None)
            if pt and getattr(pt, "names", None):
                column_policy_tags[col.column] = list(pt.names)

        table_tags = []
        try:
            for tag in client.list_tags(parent=entry.name):
                table_tags.append({
                    "template": tag.template,
                    "fields": {
                        k: str(getattr(v, "string_value", "")
                               or getattr(v, "bool_value", "")
                               or getattr(v, "double_value", ""))
                        for k, v in (tag.fields or {}).items()
                    },
                })
        except Exception as e:  # noqa: BLE001
            table_tags = [{"_error": str(e)}]

        return {
            "available": True,
            "entry_name": entry.name,
            "linked_resource": resource_name,
            "display_name": getattr(entry, "display_name", None),
            "description": getattr(entry, "description", None),
            "column_policy_tags": column_policy_tags,
            "table_tags": table_tags,
        }
    except Exception as e:  # noqa: BLE001
        return {"available": False, "reason": f"{type(e).__name__}: {e}"}


# ─── 12-13: Per-column profile + value samples ───────────────

def profile_columns(
    bq: bigquery.Client, project: str, dataset: str, table: str,
    columns: list[dict[str, Any]],
    partition_field: str | None,
    *, top_n: int = 20, partition_window_days: int = 30,
) -> dict[str, Any]:
    """One batched scan: APPROX_COUNT_DISTINCT + NULL + MIN/MAX per col."""
    fqn = f"`{project}.{dataset}.{table}`"
    selects = ["COUNT(*) AS total"]
    for c in columns:
        n = c["column_name"]
        dt = (c.get("data_type") or "").upper()
        safe = f"`{n}`"
        selects.append(f"APPROX_COUNT_DISTINCT({safe}) AS d__{n}")
        selects.append(f"COUNTIF({safe} IS NULL)            AS n__{n}")
        if any(t in dt for t in ("INT", "NUMERIC", "FLOAT", "BIGNUMERIC")):
            selects.append(f"MIN({safe}) AS mn__{n}")
            selects.append(f"MAX({safe}) AS mx__{n}")
        elif any(t in dt for t in ("DATE", "TIMESTAMP", "DATETIME")):
            selects.append(f"CAST(MIN({safe}) AS STRING) AS mn__{n}")
            selects.append(f"CAST(MAX({safe}) AS STRING) AS mx__{n}")

    where = ""
    if partition_field:
        where = (f"WHERE {partition_field} >= DATE_SUB(CURRENT_DATE(), "
                 f"INTERVAL {partition_window_days} DAY)")

    sql = f"SELECT {', '.join(selects)} FROM {fqn} {where}"
    try:
        row = next(iter(bq.query(sql).result()))
        d = dict(row)
    except Exception as e:  # noqa: BLE001
        logger.warning("profile scan failed: %s", e)
        return {"error": str(e)}

    profile: dict[str, dict[str, Any]] = {}
    for c in columns:
        n = c["column_name"]
        profile[n] = {
            "approx_distinct": _to_int(d.get(f"d__{n}")),
            "null_count": _to_int(d.get(f"n__{n}")),
            "min": _json_safe(d.get(f"mn__{n}")),
            "max": _json_safe(d.get(f"mx__{n}")),
        }

    # Phase C — top-N distinct values for low-cardinality cols
    distinct_values: dict[str, list[dict[str, Any]]] = {}
    for c in columns:
        n = c["column_name"]
        ad = profile[n].get("approx_distinct") or 0
        # Coded columns + categorical: enumerate top-N
        if 0 < ad <= 500:
            sql_dv = (
                f"SELECT `{n}` AS v, COUNT(*) AS c "
                f"FROM {fqn} {where} GROUP BY 1 "
                f"ORDER BY 2 DESC LIMIT {top_n}"
            )
            try:
                rows = list(bq.query(sql_dv).result())
                distinct_values[n] = [
                    {"value": _json_safe(r["v"]), "count": int(r["c"])}
                    for r in rows if r["v"] is not None
                ]
            except Exception as e:  # noqa: BLE001
                distinct_values[n] = [{"_error": str(e)}]

    return {
        "scanned_rows": _to_int(d.get("total")),
        "partition_window_days": partition_window_days if partition_field else None,
        "per_column": profile,
        "distinct_values": distinct_values,
    }


def sample_rows(
    bq: bigquery.Client, project: str, dataset: str, table: str,
    *, n: int = 5,
) -> list[dict[str, Any]]:
    """Pull N sample rows for visual inspection. Uses TABLESAMPLE for
    big tables, LIMIT for small."""
    fqn = f"`{project}.{dataset}.{table}`"
    try:
        rows = list(bq.query(
            f"SELECT * FROM {fqn} TABLESAMPLE SYSTEM (0.01 PERCENT) LIMIT {n}"
        ).result())
        if not rows:
            rows = list(bq.query(f"SELECT * FROM {fqn} LIMIT {n}").result())
    except Exception:
        try:
            rows = list(bq.query(f"SELECT * FROM {fqn} LIMIT {n}").result())
        except Exception as e:  # noqa: BLE001
            logger.warning("sample fetch failed: %s", e)
            return []
    return [{k: _json_safe(v) for k, v in dict(r).items()} for r in rows]


# ─── 15-16: Coded-column heuristic + lookup-table discovery ──

_CODED_CHAR_RE = None
def _looks_coded(values: list[Any]) -> bool:
    """Heuristic: short alphanumeric values, no spaces, mostly uppercase
    or numeric, no obvious English shape. Examples: 'S', 'B', '005',
    'PLAT', 'PLT', 'CC01'. Filters out things like 'Consumer',
    'Small Business', natural sentences."""
    import re
    global _CODED_CHAR_RE
    if _CODED_CHAR_RE is None:
        _CODED_CHAR_RE = re.compile(r"^[A-Z0-9_\-]{1,8}$")
    strs = [str(v) for v in values if v is not None]
    if len(strs) < 2:
        return False
    coded = sum(1 for s in strs if _CODED_CHAR_RE.match(s))
    return coded / len(strs) >= 0.8


def detect_coded_columns(
    profile: dict[str, Any], columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return columns whose value-shape looks like an enterprise code."""
    distinct = profile.get("distinct_values") or {}
    per_col = profile.get("per_column") or {}
    out: list[dict[str, Any]] = []
    for c in columns:
        n = c["column_name"]
        dt = (c.get("data_type") or "").upper()
        # Skip non-string-ish types — codes are usually STRING/INT
        if dt not in {"STRING", "INT64", "NUMERIC"} and "INT" not in dt:
            continue
        vals = [r.get("value") for r in distinct.get(n, [])]
        if not vals:
            continue
        if _looks_coded(vals):
            out.append({
                "column": n,
                "data_type": c.get("data_type"),
                "approx_distinct": per_col.get(n, {}).get("approx_distinct"),
                "sample_values": vals[:10],
                "description_bq": c.get("column_description"),
            })
    return out


def find_lookup_table_candidates(
    bq: bigquery.Client, project: str, dataset: str,
    *, coded_columns: list[dict[str, Any]], small_table_max_rows: int = 1000,
) -> list[dict[str, Any]]:
    """Scan the dataset for small reference/lookup tables that could
    resolve the coded columns.

    Heuristics:
      - Table name contains: dim_/ref_/lookup_/code/category/master/mapping
      - OR table is small (<small_table_max_rows rows)
      - For each candidate, list its columns; if any column name matches
        one of our coded columns (or differs by a common prefix), it's a
        strong resolution candidate.
    """
    sql = f"""
    SELECT
      t.table_name,
      ts.total_rows,
      ts.total_logical_bytes,
      ARRAY_AGG(STRUCT(c.column_name, c.data_type) ORDER BY c.ordinal_position) AS columns
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` t
    LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_STORAGE` ts
      ON t.table_name = ts.table_name
    LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` c
      ON t.table_name = c.table_name
    WHERE (
        LOWER(t.table_name) LIKE 'dim_%' OR
        LOWER(t.table_name) LIKE 'ref_%' OR
        LOWER(t.table_name) LIKE 'lookup_%' OR
        LOWER(t.table_name) LIKE '%_code%' OR
        LOWER(t.table_name) LIKE '%_category%' OR
        LOWER(t.table_name) LIKE '%_master%' OR
        LOWER(t.table_name) LIKE '%_mapping%' OR
        ts.total_rows < {small_table_max_rows}
    )
    AND t.table_type = 'BASE TABLE'
    GROUP BY t.table_name, ts.total_rows, ts.total_logical_bytes
    ORDER BY ts.total_rows ASC NULLS FIRST
    LIMIT 50
    """
    rows = _q(bq, sql, "lookup table scan")

    # For each coded column, find candidate lookup tables whose columns
    # contain a matching name (substring match handles "card_type" →
    # "dim_card_type.card_type_id").
    coded_names = {c["column"].lower() for c in coded_columns}
    coded_tokens = set()
    for n in coded_names:
        coded_tokens.update(n.split("_"))
    coded_tokens.discard("")

    candidates: list[dict[str, Any]] = []
    for r in rows:
        cols = r.get("columns") or []
        col_names = [_json_safe(c).get("column_name") if isinstance(c, dict) else c.column_name
                     for c in cols]
        col_names_lc = [n.lower() for n in col_names if n]
        # Name overlap with coded column
        matched_codes = [
            cn for cn in coded_names
            if any(cn in c or c in cn for c in col_names_lc)
        ]
        matched_tokens = [
            tok for tok in coded_tokens
            if any(tok in c for c in col_names_lc)
        ]
        if matched_codes or matched_tokens:
            candidates.append({
                "table_name": r["table_name"],
                "row_count": _to_int(r.get("total_rows")),
                "byte_size": _to_int(r.get("total_logical_bytes")),
                "columns": col_names,
                "matched_coded_columns": matched_codes,
                "matched_tokens": matched_tokens,
            })
    return candidates


# ─── 17: JOBS history (the gold mine — query-history mining) ─

def dump_jobs_history(
    bq: bigquery.Client, project: str, table: str,
    *, lookback_days: int = 30, limit: int = 100,
) -> dict[str, Any]:
    """Recent queries that referenced this table. Requires JOBS access."""
    sql = f"""
    SELECT
      job_id,
      user_email,
      creation_time,
      total_bytes_billed,
      total_slot_ms,
      statement_type,
      query
    FROM `{project}.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),
                                          INTERVAL {lookback_days} DAY)
      AND state = 'DONE'
      AND error_result IS NULL
      AND REGEXP_CONTAINS(LOWER(query), r'\\b{table.lower()}\\b')
    ORDER BY creation_time DESC
    LIMIT {limit}
    """
    try:
        rows = [dict(r) for r in bq.query(sql).result()]
    except Exception as e:  # noqa: BLE001
        return {"available": False, "reason": f"{type(e).__name__}: {e}"}
    # Aggregate stats
    users = Counter(r["user_email"] for r in rows if r.get("user_email"))
    statements = Counter(r["statement_type"] for r in rows if r.get("statement_type"))
    total_bytes = sum(int(r.get("total_bytes_billed") or 0) for r in rows)
    return {
        "available": True,
        "lookback_days": lookback_days,
        "rows_returned": len(rows),
        "unique_users": len(users),
        "top_users": users.most_common(10),
        "statement_breakdown": dict(statements),
        "total_bytes_billed": total_bytes,
        "sample_queries": [
            {
                "user": r.get("user_email"),
                "ts": _iso(r.get("creation_time")),
                "bytes": _to_int(r.get("total_bytes_billed")),
                "type": r.get("statement_type"),
                "query": (r.get("query") or "")[:500],
            }
            for r in rows[:10]
        ],
    }


# ─── Orchestration + summary ─────────────────────────────────

def run(
    project: str, dataset: str, table: str,
    *, include_catalog: bool, include_jobs: bool, include_profile: bool,
    sample_n: int = 5, jobs_lookback_days: int = 30,
) -> dict[str, Any]:
    _hdr(f"Probing {project}.{dataset}.{table}")
    bq = _build_client(project)

    _sub("Section A — INFORMATION_SCHEMA")
    info = dump_info_schema(bq, project, dataset, table)
    _pass(f"COLUMNS: {len(info['columns'])}")
    _pass(f"PARTITIONS: {len(info['partitions'])}")
    _pass(f"NESTED FIELDS: {len(info['nested_field_paths'])}")
    _pass(f"CONSTRAINTS: {len(info['constraints'])}")
    _pass(f"VIEWS: {len(info['views'])}  MVs: {len(info['materialized_views'])}")
    _pass(f"OPTIONS: {len(info['options'])}  ROUTINES: {len(info['routines_referencing'])}")
    if not info["columns"]:
        _fail(f"Table {project}.{dataset}.{table} not found in INFORMATION_SCHEMA")
        return {"error": "table not found", "info": info}

    catalog: dict[str, Any] = {}
    if include_catalog:
        _sub("Section B — Data Catalog")
        catalog = dump_catalog(project, dataset, table)
        if catalog.get("available"):
            _pass(f"entry: {catalog.get('entry_name')}")
            _pass(f"column_policy_tags: {len(catalog.get('column_policy_tags') or {})}")
            _pass(f"table_tags: {len(catalog.get('table_tags') or [])}")
        else:
            _warn(f"catalog unavailable: {catalog.get('reason')}")

    partition_field = next(
        (c["column_name"] for c in info["columns"]
         if c.get("is_partitioning_column") == "YES"),
        None,
    )

    profile: dict[str, Any] = {}
    samples: list[dict[str, Any]] = []
    if include_profile:
        _sub("Section C — per-column profile + distinct values (1 scan)")
        profile = profile_columns(bq, project, dataset, table, info["columns"],
                                  partition_field=partition_field)
        if not profile.get("error"):
            _pass(f"profiled {len(profile.get('per_column') or {})} columns")
            _pass(f"distinct-value lists for {len(profile.get('distinct_values') or {})} cols")
        else:
            _fail(f"profile failed: {profile.get('error')}")

        _sub("Section D — sample rows")
        samples = sample_rows(bq, project, dataset, table, n=sample_n)
        _pass(f"sampled {len(samples)} rows")

    coded: list[dict[str, Any]] = []
    lookup_candidates: list[dict[str, Any]] = []
    if include_profile and profile:
        _sub("Section E — coded-column heuristic")
        coded = detect_coded_columns(profile, info["columns"])
        if coded:
            _pass(f"flagged {len(coded)} columns that look CODED:")
            for c in coded:
                _info(f"  {c['column']}: {c['sample_values']}")
        else:
            _info("no coded columns detected")

        _sub("Section F — candidate lookup tables (for code resolution)")
        lookup_candidates = find_lookup_table_candidates(
            bq, project, dataset, coded_columns=coded,
        )
        if lookup_candidates:
            _pass(f"{len(lookup_candidates)} candidate lookup table(s):")
            for c in lookup_candidates[:10]:
                _info(f"  {c['table_name']} ({c['row_count']} rows) — "
                      f"matches: {c['matched_coded_columns'] or c['matched_tokens'][:3]}")
        else:
            _info("no candidate lookup tables found")

    jobs: dict[str, Any] = {}
    if include_jobs:
        _sub("Section G — JOBS history (last 30 days)")
        jobs = dump_jobs_history(bq, project, table)
        if jobs.get("available"):
            _pass(f"{jobs['rows_returned']} queries by {jobs['unique_users']} users")
            for user, n in jobs.get("top_users", [])[:5]:
                _info(f"  {user}: {n} queries")
        else:
            _warn(f"JOBS unavailable: {jobs.get('reason')}")

    return {
        "fqn": f"{project}.{dataset}.{table}",
        "info_schema": info,
        "catalog": catalog,
        "partition_field": partition_field,
        "profile": profile,
        "sample_rows": samples,
        "coded_columns": coded,
        "lookup_table_candidates": lookup_candidates,
        "jobs": jobs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="Single table to probe")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument(
        "--save", type=Path,
        help="Save full JSON dump to this path "
             "(default: data/bq_explore/<table>.json)",
    )
    parser.add_argument("--no-catalog", action="store_true",
                        help="Skip Data Catalog policy tags")
    parser.add_argument("--no-jobs", action="store_true",
                        help="Skip INFORMATION_SCHEMA.JOBS_BY_PROJECT")
    parser.add_argument("--no-profile", action="store_true",
                        help="Skip Phase B/C (cardinality + distinct values)")
    parser.add_argument("--sample-n", type=int, default=5,
                        help="Number of sample rows to fetch (default 5)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    result = run(
        args.project, args.dataset, args.table,
        include_catalog=not args.no_catalog,
        include_jobs=not args.no_jobs,
        include_profile=not args.no_profile,
        sample_n=args.sample_n,
    )

    out_path = args.save or (
        REPO_ROOT / "data" / "bq_explore" / f"{args.table}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(_json_safe(result), indent=2, default=str))
    _hdr("Saved")
    _pass(str(out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
