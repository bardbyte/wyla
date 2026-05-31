"""BigQuery probe — pulls every graph-relevant signal for one table or for
every table the gold-query corpus touches, in a cost-disciplined way.

Output: per-table JSON under data/bq_cache/<table>.json, same caching
discipline as scripts/probe_mdm.py. A follow-up emitter
(record_bq_facts) projects these into ontology events the same way the
MDM hook does.

Three phases:

  Phase A — METADATA (FREE)
    INFORMATION_SCHEMA.COLUMNS + TABLE_OPTIONS + TABLE_STORAGE +
    PARTITIONS. Pulls column list + types + nullability + partition +
    clustering + row count + freshness + table-level options. Zero
    scan cost — INFORMATION_SCHEMA is metadata.

  Phase B — CARDINALITY (ONE SCAN PER TABLE)
    Single batched query: APPROX_COUNT_DISTINCT for every corpus-touched
    column + COUNTIF(col IS NULL) for null fraction + MIN/MAX for
    numeric / date columns. One scan; partition pushdown when partition
    field known.

  Phase C — DISTINCT VALUES (CARDINALITY-GATED)
    Only for columns where (Phase B approx_distinct < threshold)
    AND (column appears in corpus WHERE / CASE WHEN / GROUP BY). Pulls
    top-N distinct values with empirical counts. These become real
    FilterValue nodes in the graph.

Cost discipline:
  - Cache files at data/bq_cache/<table>.json; rerun is a no-op unless
    --refresh is set.
  - --dry-run estimates bytes-scanned before any paid query, prints a
    table of (table, phase, bytes) and exits.
  - --max-bytes-per-table caps each table's total Phase B + C scan
    (default 5 GB). Tables exceeding the cap fall back to Phase A only.
  - --phase-cutoff lets you stop at A or B (skip the expensive ones).
  - Partition pushdown applied wherever a partition field is known.

Usage:

    # Probe every table the 122 gold SQLs touch:
    python scripts/probe_bq.py --from-sqls data/gold_queries/ \\
        --save data/bq_cache/

    # Single table:
    python scripts/probe_bq.py --table cornerstone_metrics \\
        --save data/bq_cache/

    # Dry run — see what each phase will cost before paying:
    python scripts/probe_bq.py --from-sqls data/gold_queries/ --dry-run

    # Metadata-only mode (zero scan cost):
    python scripts/probe_bq.py --from-sqls data/gold_queries/ \\
        --phase-cutoff A --save data/bq_cache/

Env vars:
  GOOGLE_APPLICATION_CREDENTIALS  service-account JSON path
  LUMI_BQ_PROJECT                 BQ project (default: prj-d-ea-poc)
  LUMI_BQ_DATASET                 default dataset for unqualified tables
                                  (default: dw)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── TLS / corporate-MITM bootstrap (same pattern as check_bq_access.py) ──
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
sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger("probe_bq")


# ─── Defaults ────────────────────────────────────────────────

DEFAULT_PROJECT = os.environ.get("LUMI_BQ_PROJECT", "prj-d-ea-poc")
DEFAULT_DATASET = os.environ.get("LUMI_BQ_DATASET", "dw")
DEFAULT_CACHE_DIR = REPO_ROOT / "data" / "bq_cache"

# Cardinality threshold for Phase C: a column with > N distinct values
# isn't enumerable. 1000 = ~all enum columns + most low-cardinality
# categoricals, no high-cardinality keys.
PHASE_C_CARDINALITY_THRESHOLD = 1000
PHASE_C_TOP_N = 100  # how many top values per column to keep

# Hard cap: any single table's Phase B+C bytes cannot exceed this.
DEFAULT_MAX_BYTES_PER_TABLE = 5 * 1024**3  # 5 GB


# ─── Pretty print ────────────────────────────────────────────

def _hdr(msg: str) -> None: print(f"\n\033[1;36m═══ {msg} ═══\033[0m")
def _sub(msg: str) -> None: print(f"\033[1;34m── {msg} ──\033[0m")
def _pass(msg: str) -> None: print(f"  \033[1;32m✓\033[0m {msg}")
def _fail(msg: str) -> None: print(f"  \033[1;31m✗\033[0m {msg}")
def _info(msg: str) -> None: print(f"    \033[2m{msg}\033[0m")
def _warn(msg: str) -> None: print(f"  \033[1;33m!\033[0m {msg}")


# ─── Table discovery from SQL corpus ─────────────────────────

def discover_tables_from_sqls(sql_dir: Path) -> list[str]:
    """Parse every .sql under sql_dir; return sorted unique table set."""
    from lumi.sql_to_context import parse_sqls

    sqls = [f.read_text(encoding="utf-8") for f in sorted(sql_dir.glob("*.sql"))]
    fps = parse_sqls(sqls)
    tables: set[str] = set()
    for fp in fps:
        if fp.parse_error:
            continue
        for t in fp.tables or []:
            tables.add(t)
        # Include CTE source tables too (they're real tables).
        for cte in (fp.ctes or []):
            for src in cte.get("source_tables") or []:
                tables.add(src)
    return sorted(tables)


def corpus_columns_per_table(sql_dir: Path) -> dict[str, set[str]]:
    """For each table, return the set of column names the corpus references.

    Used to scope Phase B/C — we only spend money on columns the queries
    actually touch."""
    from lumi.sql_to_context import parse_sqls

    out: dict[str, set[str]] = {}
    sqls = [f.read_text(encoding="utf-8") for f in sorted(sql_dir.glob("*.sql"))]
    fps = parse_sqls(sqls)
    for fp in fps:
        if fp.parse_error:
            continue
        primary = fp.primary_table or (fp.tables[0] if fp.tables else None)
        for t in fp.tables or []:
            out.setdefault(t, set())
        # Aggregations + case_whens + filters + group_by + joins + date_funcs
        # all contribute the columns we'll cardinality-probe.
        cols_per_query: set[str] = set()
        for x in (fp.aggregations or []):
            if x.get("column"):
                cols_per_query.add(str(x["column"]))
        for x in (fp.case_whens or []):
            if x.get("source_column"):
                cols_per_query.add(str(x["source_column"]))
        for x in (fp.filters or []):
            if x.get("column"):
                cols_per_query.add(str(x["column"]))
        for x in (fp.group_by or []):
            if x.get("column"):
                cols_per_query.add(str(x["column"]))
        for x in (fp.date_functions or []):
            if x.get("column"):
                cols_per_query.add(str(x["column"]))
        for j in (fp.joins or []):
            if j.get("left_key"):
                cols_per_query.add(str(j["left_key"]))
            if j.get("right_key"):
                cols_per_query.add(str(j["right_key"]))
        # Attribute to primary; we don't have proper qualifier-aware
        # extraction yet so this is a heuristic.
        if primary:
            out.setdefault(primary, set()).update(cols_per_query)
    return out


def corpus_filter_columns(sql_dir: Path) -> dict[str, set[str]]:
    """For each table, the columns the corpus FILTERS / GROUPS / CASE-WHENs on.

    Phase C only enumerates these — they're the categorical-dimension
    signal."""
    from lumi.sql_to_context import parse_sqls

    out: dict[str, set[str]] = {}
    sqls = [f.read_text(encoding="utf-8") for f in sorted(sql_dir.glob("*.sql"))]
    fps = parse_sqls(sqls)
    for fp in fps:
        if fp.parse_error:
            continue
        primary = fp.primary_table or (fp.tables[0] if fp.tables else None)
        if not primary:
            continue
        cols: set[str] = set()
        for x in (fp.filters or []):
            if x.get("column"):
                cols.add(str(x["column"]))
        for x in (fp.case_whens or []):
            if x.get("source_column"):
                cols.add(str(x["source_column"]))
        for x in (fp.group_by or []):
            if x.get("column"):
                cols.add(str(x["column"]))
        out.setdefault(primary, set()).update(cols)
    return out


# ─── BQ client ───────────────────────────────────────────────

def _build_client() -> bigquery.Client:
    """Build a BQ client from the standard ADC / service-account chain."""
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and Path(creds_path).exists():
        creds = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=DEFAULT_PROJECT, credentials=creds)
    # Fall back to ADC
    return bigquery.Client(project=DEFAULT_PROJECT)


# ─── Phase A — metadata (FREE) ───────────────────────────────

def phase_a_metadata(
    bq: bigquery.Client, project: str, dataset: str, table: str,
) -> dict[str, Any]:
    """Pull INFORMATION_SCHEMA + TABLE_OPTIONS + TABLE_STORAGE in 2 queries.

    Zero scan cost — INFORMATION_SCHEMA is metadata."""
    fqn = f"`{project}.{dataset}.{table}`"

    # 1. Column metadata (+ BQ-native description, often fresher than MDM)
    cols_sql = f"""
    SELECT
      column_name,
      data_type,
      is_nullable = 'YES'                AS is_nullable,
      is_partitioning_column = 'YES'     AS is_partitioning_column,
      clustering_ordinal_position,
      ordinal_position,
      column_default,
      description                         AS column_description
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    ORDER BY ordinal_position
    """

    tbl_sql = f"""
    SELECT
      table_type,
      creation_time,
      ddl
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{table}'
    """

    # 2. TABLE_OPTIONS — labels, description, friendly_name. Each option is a
    #    separate row (option_name, option_value). We pivot into a dict.
    options_sql = f"""
    SELECT option_name, option_type, option_value
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE table_name = '{table}'
    """

    # 3. If the "table" is actually a VIEW, pull the view_definition —
    #    the SQL itself is graph signal (joins it makes, filters it carries).
    views_sql = f"""
    SELECT view_definition, check_option, use_standard_sql
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    WHERE table_name = '{table}'
    """

    # 4. Materialized views — definition + last_refresh + refresh_interval
    mviews_sql = f"""
    SELECT
      last_refresh_time,
      refresh_interval_minutes,
      allow_non_incremental_definition,
      enable_refresh
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.MATERIALIZED_VIEWS`
    WHERE table_name = '{table}'
    """

    storage_sql = f"""
    SELECT
      total_rows,
      total_logical_bytes,
      total_physical_bytes,
      storage_last_modified_time
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_STORAGE`
    WHERE table_name = '{table}'
    """

    columns = [dict(row) for row in bq.query(cols_sql).result()]
    if not columns:
        raise ValueError(f"Table {fqn} not found in INFORMATION_SCHEMA")

    tbl_rows = list(bq.query(tbl_sql).result())
    tbl_info = dict(tbl_rows[0]) if tbl_rows else {}

    # Table options (labels, friendly_name, description) — degrade gracefully
    try:
        opt_rows = [dict(r) for r in bq.query(options_sql).result()]
    except gcp_exceptions.GoogleAPICallError as e:
        logger.warning("TABLE_OPTIONS unavailable for %s: %s", table, e)
        opt_rows = []
    table_options: dict[str, Any] = {}
    table_labels: dict[str, str] = {}
    table_description_bq: str | None = None
    table_friendly_name: str | None = None
    for r in opt_rows:
        name = r.get("option_name")
        value = r.get("option_value")
        if not name:
            continue
        # option_value is a STRING; for labels it's a struct literal
        if name == "labels" and value:
            # Format: [STRUCT("k1","v1"),STRUCT("k2","v2")]
            # Best-effort parse; if it fails we just keep the raw string.
            import re as _re
            for k, v in _re.findall(r'STRUCT\("([^"]+)",\s*"([^"]*)"\)', value or ""):
                table_labels[k] = v
            table_options[name] = value
        elif name == "description":
            table_description_bq = (value or "").strip('"') if value else None
        elif name == "friendly_name":
            table_friendly_name = (value or "").strip('"') if value else None
        else:
            table_options[name] = value

    # VIEW definition (if applicable)
    try:
        view_rows = [dict(r) for r in bq.query(views_sql).result()]
    except gcp_exceptions.GoogleAPICallError:
        view_rows = []
    view_info = view_rows[0] if view_rows else None

    # MATERIALIZED VIEW (if applicable)
    try:
        mview_rows = [dict(r) for r in bq.query(mviews_sql).result()]
    except gcp_exceptions.GoogleAPICallError:
        mview_rows = []
    mview_info = mview_rows[0] if mview_rows else None

    try:
        storage_rows = list(bq.query(storage_sql).result())
        storage = dict(storage_rows[0]) if storage_rows else {}
    except gcp_exceptions.GoogleAPICallError as e:
        # TABLE_STORAGE not always available; degrade gracefully
        logger.warning("TABLE_STORAGE unavailable for %s: %s", table, e)
        storage = {}

    # Derive partitioning + clustering summary
    partition_field = next(
        (c["column_name"] for c in columns if c.get("is_partitioning_column")),
        None,
    )
    cluster_cols = sorted(
        (c for c in columns if c.get("clustering_ordinal_position") is not None),
        key=lambda c: c["clustering_ordinal_position"],
    )
    clustering_fields = [c["column_name"] for c in cluster_cols]

    return {
        "columns": [
            {
                "name": c["column_name"],
                "data_type": c["data_type"],
                "is_nullable": bool(c.get("is_nullable")),
                "is_partitioning_column": bool(c.get("is_partitioning_column")),
                "clustering_ordinal": c.get("clustering_ordinal_position"),
                "ordinal_position": c.get("ordinal_position"),
                "default": c.get("column_default"),
                "description_bq": c.get("column_description"),
            }
            for c in columns
        ],
        "table_type": tbl_info.get("table_type"),
        "creation_time": _iso(tbl_info.get("creation_time")),
        "ddl": tbl_info.get("ddl"),  # full CREATE TABLE — source of truth
        "row_count": _to_int(storage.get("total_rows")),
        "logical_bytes": _to_int(storage.get("total_logical_bytes")),
        "physical_bytes": _to_int(storage.get("total_physical_bytes")),
        "storage_last_modified_time": _iso(storage.get("storage_last_modified_time")),
        "partition_field": partition_field,
        "clustering_fields": clustering_fields,
        # NEW — table-level governance + naming
        "table_description_bq": table_description_bq,
        "table_friendly_name": table_friendly_name,
        "table_labels": table_labels,
        "table_options_raw": table_options,
        # NEW — view + materialized view definitions
        "view_definition": (view_info or {}).get("view_definition") if view_info else None,
        "view_use_standard_sql": (view_info or {}).get("use_standard_sql") if view_info else None,
        "is_view": bool(view_info),
        "materialized_view": ({
            "last_refresh_time": _iso((mview_info or {}).get("last_refresh_time")),
            "refresh_interval_minutes": (mview_info or {}).get("refresh_interval_minutes"),
            "enable_refresh": (mview_info or {}).get("enable_refresh"),
        } if mview_info else None),
    }


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


# ─── Cost estimation (dry-run) ───────────────────────────────

def estimate_bytes(bq: bigquery.Client, sql: str) -> int:
    """Use dry-run to estimate scanned bytes without paying."""
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = bq.query(sql, job_config=cfg)
    return int(job.total_bytes_processed or 0)


# ─── Phase B — cardinality (ONE SCAN) ────────────────────────

def phase_b_cardinality(
    bq: bigquery.Client, project: str, dataset: str, table: str,
    *, columns_to_probe: list[dict[str, Any]],
    partition_field: str | None,
    partition_window_days: int = 7,
    dry_run: bool = False,
) -> dict[str, Any]:
    """One batched query: APPROX_COUNT_DISTINCT + COUNTIF(NULL) +
    MIN/MAX (for numeric/date) for every corpus-touched column.

    Partition pushdown if partition_field known."""
    if not columns_to_probe:
        return {"scanned_bytes": 0, "column_stats": {}}

    fqn = f"`{project}.{dataset}.{table}`"
    selects = ["COUNT(*) AS total_rows"]
    safe_names = []
    for c in columns_to_probe:
        name = c["name"]
        safe = f"`{name}`"
        safe_names.append(name)
        selects.append(
            f"APPROX_COUNT_DISTINCT({safe}) AS approx_distinct__{name}"
        )
        selects.append(
            f"COUNTIF({safe} IS NULL) AS null_count__{name}"
        )
        dt = (c.get("data_type") or "").upper()
        if any(t in dt for t in ("INT", "NUMERIC", "FLOAT", "BIGNUMERIC")):
            selects.append(f"MIN({safe}) AS min__{name}")
            selects.append(f"MAX({safe}) AS max__{name}")
        elif any(t in dt for t in ("DATE", "TIMESTAMP", "DATETIME")):
            selects.append(f"CAST(MIN({safe}) AS STRING) AS min__{name}")
            selects.append(f"CAST(MAX({safe}) AS STRING) AS max__{name}")

    where = ""
    if partition_field:
        where = (
            f"WHERE {partition_field} >= DATE_SUB(CURRENT_DATE(), "
            f"INTERVAL {partition_window_days} DAY)"
        )

    sql = f"SELECT {', '.join(selects)} FROM {fqn} {where}"

    bytes_est = estimate_bytes(bq, sql)
    if dry_run:
        return {"scanned_bytes": bytes_est, "column_stats": {}, "dry_run": True}

    row = next(iter(bq.query(sql).result()))
    d = dict(row)
    stats: dict[str, dict[str, Any]] = {}
    for name in safe_names:
        stats[name] = {
            "approx_distinct": _to_int(d.get(f"approx_distinct__{name}")),
            "null_count": _to_int(d.get(f"null_count__{name}")),
            "min": d.get(f"min__{name}"),
            "max": d.get(f"max__{name}"),
        }
        # JSON-safe coercion
        for k in ("min", "max"):
            if stats[name][k] is not None and not isinstance(
                stats[name][k], (int, float, str),
            ):
                stats[name][k] = str(stats[name][k])
    return {
        "scanned_bytes": bytes_est,
        "scanned_rows": _to_int(d.get("total_rows")),
        "partition_pushdown": bool(partition_field),
        "column_stats": stats,
    }


# ─── Phase C — DISTINCT values (CARDINALITY-GATED) ───────────

def phase_c_distinct_values(
    bq: bigquery.Client, project: str, dataset: str, table: str,
    *, columns: list[str],
    partition_field: str | None,
    partition_window_days: int = 7,
    top_n: int = PHASE_C_TOP_N,
    dry_run: bool = False,
) -> dict[str, Any]:
    """SELECT col, COUNT(*) FROM t GROUP BY col LIMIT N — one query per
    column (UNION ALL would lose per-column LIMIT). Each query gets
    partition pushdown when available."""
    if not columns:
        return {"scanned_bytes": 0, "distinct_values": {}}

    fqn = f"`{project}.{dataset}.{table}`"
    where = ""
    if partition_field:
        where = (
            f"WHERE {partition_field} >= DATE_SUB(CURRENT_DATE(), "
            f"INTERVAL {partition_window_days} DAY)"
        )

    total_bytes = 0
    out: dict[str, list[dict[str, Any]]] = {}
    for col in columns:
        sql = (
            f"SELECT `{col}` AS v, COUNT(*) AS c "
            f"FROM {fqn} {where} "
            f"GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n}"
        )
        try:
            bytes_est = estimate_bytes(bq, sql)
            total_bytes += bytes_est
            if dry_run:
                continue
            rows = list(bq.query(sql).result())
            out[col] = [
                {"value": _json_safe(r["v"]), "count": int(r["c"])}
                for r in rows if r["v"] is not None
            ]
        except gcp_exceptions.GoogleAPICallError as e:
            logger.warning("Phase C for %s.%s failed: %s", table, col, e)
            out[col] = []
    return {
        "scanned_bytes": total_bytes,
        "distinct_values": out,
        "dry_run": dry_run,
    }


def _json_safe(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


# ─── Per-table orchestration ─────────────────────────────────

@dataclass
class TableProbeResult:
    table: str
    bq_fqn: str
    phase_a: dict[str, Any] = field(default_factory=dict)
    phase_b: dict[str, Any] = field(default_factory=dict)
    phase_c: dict[str, Any] = field(default_factory=dict)
    total_scanned_bytes: int = 0
    error: str = ""


def probe_one_table(
    bq: bigquery.Client, project: str, dataset: str, table: str,
    *, corpus_columns: set[str], filter_columns: set[str],
    phase_cutoff: str = "C",
    max_bytes_per_table: int = DEFAULT_MAX_BYTES_PER_TABLE,
    dry_run: bool = False,
) -> TableProbeResult:
    fqn = f"{project}.{dataset}.{table}"
    result = TableProbeResult(table=table, bq_fqn=fqn)

    # ── Phase A ──
    try:
        result.phase_a = phase_a_metadata(bq, project, dataset, table)
        _pass(f"{table}: Phase A — {len(result.phase_a['columns'])} cols, "
              f"{result.phase_a.get('row_count') or '?'} rows")
    except Exception as e:
        result.error = f"Phase A: {type(e).__name__}: {e}"
        _fail(f"{table}: {result.error}")
        return result

    if phase_cutoff == "A":
        return result

    # Decide which columns to probe in Phase B
    # Strategy: only corpus-touched columns. If corpus_columns is empty
    # (single-table mode), probe all of them.
    all_cols = result.phase_a["columns"]
    if corpus_columns:
        probe_cols = [
            c for c in all_cols
            if c["name"].lower() in {n.lower() for n in corpus_columns}
        ]
        if not probe_cols:
            _warn(f"{table}: no corpus-touched columns; skipping Phase B/C")
            return result
    else:
        probe_cols = all_cols

    # ── Phase B ──
    try:
        b_dry = phase_b_cardinality(
            bq, project, dataset, table,
            columns_to_probe=probe_cols,
            partition_field=result.phase_a.get("partition_field"),
            dry_run=True,
        )
        b_bytes = b_dry["scanned_bytes"]
        if b_bytes > max_bytes_per_table:
            _warn(
                f"{table}: Phase B would scan {b_bytes/1e9:.2f} GB "
                f"> cap {max_bytes_per_table/1e9:.2f} GB — skipping"
            )
            return result
        if dry_run:
            result.phase_b = b_dry
            result.total_scanned_bytes += b_bytes
            _info(f"{table}: Phase B est. {b_bytes/1e6:.1f} MB (dry-run)")
        else:
            result.phase_b = phase_b_cardinality(
                bq, project, dataset, table,
                columns_to_probe=probe_cols,
                partition_field=result.phase_a.get("partition_field"),
            )
            result.total_scanned_bytes += result.phase_b["scanned_bytes"]
            _pass(f"{table}: Phase B — {result.phase_b['scanned_bytes']/1e6:.1f} MB scanned")
    except Exception as e:
        logger.exception("Phase B failed for %s", table)
        result.error = f"Phase B: {type(e).__name__}: {e}"
        return result

    if phase_cutoff == "B":
        return result

    # ── Phase C ── (cardinality-gated + filter-corpus-gated)
    if dry_run:
        # Use Phase A column list as a rough gate (no Phase B data yet)
        candidate_cols = sorted(filter_columns & {c["name"] for c in probe_cols})
    else:
        col_stats = result.phase_b.get("column_stats", {})
        candidate_cols = [
            name for name, stats in col_stats.items()
            if (stats.get("approx_distinct") or 0) <= PHASE_C_CARDINALITY_THRESHOLD
            and name in filter_columns
        ]

    if not candidate_cols:
        _info(f"{table}: Phase C — no eligible columns")
        return result

    remaining_budget = max_bytes_per_table - result.total_scanned_bytes
    try:
        c_result = phase_c_distinct_values(
            bq, project, dataset, table,
            columns=candidate_cols,
            partition_field=result.phase_a.get("partition_field"),
            dry_run=dry_run,
        )
        if c_result["scanned_bytes"] > remaining_budget:
            _warn(
                f"{table}: Phase C would scan "
                f"{c_result['scanned_bytes']/1e9:.2f} GB > remaining "
                f"budget {remaining_budget/1e9:.2f} GB — skipping"
            )
            return result
        result.phase_c = c_result
        result.total_scanned_bytes += c_result["scanned_bytes"]
        action = "would scan" if dry_run else "scanned"
        _pass(
            f"{table}: Phase C {action} "
            f"{c_result['scanned_bytes']/1e6:.1f} MB across "
            f"{len(candidate_cols)} columns"
        )
    except Exception as e:
        result.error = f"Phase C: {type(e).__name__}: {e}"

    return result


# ─── Save / load cache ───────────────────────────────────────

def save_table_cache(out_dir: Path, r: TableProbeResult) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "table_name": r.table,
        "bq_fqn": r.bq_fqn,
        "probe_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "phase_a": r.phase_a,
        "phase_b": r.phase_b,
        "phase_c": r.phase_c,
        "total_scanned_bytes": r.total_scanned_bytes,
        "error": r.error,
    }
    path = out_dir / f"{r.table}.json"
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path


# ─── Main ─────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--from-sqls", type=Path,
                   help="Probe every table referenced under this SQL dir")
    g.add_argument("--table", type=str, help="Single table to probe")

    parser.add_argument("--save", type=Path, default=DEFAULT_CACHE_DIR,
                        help=f"Cache dir (default: {DEFAULT_CACHE_DIR})")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument(
        "--phase-cutoff", choices=["A", "B", "C"], default="C",
        help="Stop after Phase A / B / C (default C = all)",
    )
    parser.add_argument(
        "--max-bytes-per-table", type=float,
        default=DEFAULT_MAX_BYTES_PER_TABLE,
        help=f"Per-table scan cap (default {DEFAULT_MAX_BYTES_PER_TABLE/1e9:.0f} GB)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Estimate bytes without paid queries")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-probe even if cache exists")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    _hdr("Building BQ client")
    try:
        bq = _build_client()
        _pass(f"connected to project={args.project}")
    except Exception as e:
        _fail(f"BQ client init failed: {e}")
        return 1

    # Resolve tables
    if args.table:
        tables = [args.table]
        corpus_cols: dict[str, set[str]] = {}
        filter_cols: dict[str, set[str]] = {}
    else:
        if not args.from_sqls.exists():
            _fail(f"--from-sqls dir not found: {args.from_sqls}")
            return 1
        tables = discover_tables_from_sqls(args.from_sqls)
        corpus_cols = corpus_columns_per_table(args.from_sqls)
        filter_cols = corpus_filter_columns(args.from_sqls)
        _info(f"Discovered {len(tables)} tables from {args.from_sqls}")

    _hdr(f"Probing {len(tables)} table(s) "
         f"(phase cutoff: {args.phase_cutoff}, dry-run: {args.dry_run})")

    total_bytes = 0
    results: list[TableProbeResult] = []
    for table in tables:
        cache_path = args.save / f"{table}.json"
        if cache_path.exists() and not args.refresh:
            _info(f"{table}: cached at {cache_path.name} — skip (use --refresh)")
            continue
        result = probe_one_table(
            bq, args.project, args.dataset, table,
            corpus_columns=corpus_cols.get(table, set()),
            filter_columns=filter_cols.get(table, set()),
            phase_cutoff=args.phase_cutoff,
            max_bytes_per_table=int(args.max_bytes_per_table),
            dry_run=args.dry_run,
        )
        results.append(result)
        total_bytes += result.total_scanned_bytes
        if not args.dry_run:
            save_table_cache(args.save, result)

    _hdr("Summary")
    _info(f"Total bytes scanned: {total_bytes/1e9:.3f} GB "
          f"(approx ${total_bytes/1e12 * 5:.2f} at $5/TB)")
    n_err = sum(1 for r in results if r.error)
    if n_err:
        _warn(f"{n_err} table(s) had errors")
        for r in results:
            if r.error:
                _info(f"  {r.table}: {r.error}")
    else:
        _pass(f"{len(results)} table(s) probed cleanly")
    if not args.dry_run:
        _pass(f"Cached under {args.save}")

    return 0 if n_err == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
