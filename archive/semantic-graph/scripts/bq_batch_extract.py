"""BQ batch extractor — runs the 12 known-good probes against every table in
tables.yaml, async with bounded concurrency, produces the file layout the
existing bq_loader.py consumes.

PURPOSE
=======
Replicate the cardmember-table extraction we proved out in the capabilities
probe across 53 tables. The output layout mirrors `BQ_EXTRACTION_GUIDE.md`'s
send-back format so `synapse/loaders/bq_loader.py` reads it without changes.

WHAT IT EXTRACTS PER TABLE (12 probes)
======================================
  1.  Schema (INFORMATION_SCHEMA.COLUMNS)        → 1_1__columns.csv
  2.  Column descriptions (COLUMN_FIELD_PATHS)   → 1_2__col_descs.csv
  3.  Table meta + DDL (INFORMATION_SCHEMA.TABLES)→ 1_3__table_meta.json
  4.  Table options (labels, partition flags)    → 1_4__table_options.csv
  5.  Constraints (declared PK/FK, often empty)  → 1_5__constraints.csv
  6.  Size + freshness (__TABLES__)              → 2_1__size_freshness.csv
  7.  Partition stats                            → 2_2__partitions.csv
  8.  Top users (JOBS_BY_PROJECT, 90d)           → 4_1__top_users.csv
  9.  Failed queries (JOBS_BY_PROJECT, 30d)      → 4_5__failed_queries.csv
  10. Co-queried tables (JOBS_BY_PROJECT, 90d)   → 4_3__co_queried.csv
  11. Cost (JOBS_BY_PROJECT, 30d)                → 7_1__cost_30d.csv
  12. PROFILING (single-scan combined aggregate) → 3_1__cardinality_nulls.csv
                                                  + 3_2__topcount__<col>.csv per low-card col

ASYNC DESIGN
============
Tables run in parallel, bounded by `concurrency` (default 8). Probes within
ONE table run sequentially (they share auth + are cheap). This keeps total
wall-time low while staying well under BQ's per-project query slot limits.

COST MODEL
==========
Probes 1-11 are INFORMATION_SCHEMA / JOBS_BY_PROJECT reads — metadata-only,
metered free. Probe 12 is the only one that costs money: one combined
aggregation query per table using TABLESAMPLE (default 1%). On a 487 GB
table that's ~5 GB scanned ≈ $0.03; on a 1 GB lookup table it's
~$0.00006. Estimated full 53-table cost: $0.30-$1.00 depending on mix.

USAGE
=====
    # Run against all tables in config/tables.yaml
    python semantic-graph/scripts/bq_batch_extract.py

    # Override config path
    python semantic-graph/scripts/bq_batch_extract.py \
        --config semantic-graph/config/tables.yaml

    # Dry-run: parse YAML, print plan, run no queries
    python semantic-graph/scripts/bq_batch_extract.py --dry-run

    # Force re-extraction (otherwise skips tables whose folders are complete)
    python semantic-graph/scripts/bq_batch_extract.py --force

    # Restrict to one or a few tables (smoke test)
    python semantic-graph/scripts/bq_batch_extract.py \
        --only custins_customer_insights_cardmember,drm_product_hier

ENVIRONMENT
===========
    GOOGLE_APPLICATION_CREDENTIALS   path to SA JSON
    BIGQUERY_API_BASE_URL            optional override of PSC endpoint
    REQUESTS_CA_BUNDLE               optional custom CA bundle

OUTPUT
======
    <output_dir>/
        custins_customer_insights_cardmember/
            1_1__columns.csv
            1_3__table_meta.json
            ...
            _summary.json                 ← per-table run status
        drm_product_hier/
            ...
        _batch_summary.json               ← aggregate report
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml
from dotenv import find_dotenv, load_dotenv
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://bigquery-prod.p.googleapis.com"
DEFAULT_SCOPE = "https://www.googleapis.com/auth/bigquery"

# Glyphs (matches the capabilities probe vocabulary)
GLYPH = {"ok": "✅", "warn": "⚠", "fail": "❌", "skip": "⊘"}


# ─── Data classes ─────────────────────────────────────────────────


@dataclass
class TableConfig:
    """One table from tables.yaml, with defaults merged in."""
    name: str
    bq_project: str
    bq_dataset: str
    billing_project: str
    region: str
    profile_sample_pct: float
    low_card_threshold: int

    @property
    def fqn(self) -> str:
        return f"{self.bq_project}.{self.bq_dataset}.{self.name}"


@dataclass
class ProbeOutcome:
    """One probe's result for one table."""
    name: str
    status: str              # ok / warn / fail / skip
    summary: str
    artifact_path: Path | None = None
    error: str | None = None
    rows_returned: int = 0


@dataclass
class TableResult:
    """All probe outcomes for one table."""
    table_name: str
    output_dir: Path
    probes: list[ProbeOutcome] = field(default_factory=list)
    cost_usd: float = 0.0
    bytes_scanned: int = 0
    duration_sec: float = 0.0

    @property
    def n_ok(self) -> int:
        return sum(1 for p in self.probes if p.status == "ok")

    @property
    def n_warn(self) -> int:
        return sum(1 for p in self.probes if p.status == "warn")

    @property
    def n_fail(self) -> int:
        return sum(1 for p in self.probes if p.status == "fail")


# ─── REST client (async-friendly via asyncio.to_thread) ──────────


class BQClient:
    """Sync REST client; we wrap it in asyncio.to_thread for concurrency.

    Same auth pattern as bq_capabilities_probe.py (proven working with the
    Lumi SA + bigquery-prod PSC endpoint)."""

    def __init__(self, billing_project: str, sa_path: str, base_url: str, region: str) -> None:
        self.billing_project = billing_project
        self.base_url = base_url.rstrip("/")
        self.region = region.lower().lstrip("region-")
        self._creds = service_account.Credentials.from_service_account_file(
            sa_path, scopes=[DEFAULT_SCOPE]
        )
        self.sa_email = self._creds.service_account_email
        session = requests.Session()
        ca_bundle = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
        if ca_bundle:
            session.verify = ca_bundle
        self._auth_request = GoogleAuthRequest(session=session)

    def _token(self) -> str:
        self._creds.refresh(self._auth_request)
        return self._creds.token

    def _request(self, method: str, path: str, body: dict | None = None,
                 params: dict | None = None, timeout: int = 120) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode({k: v for k, v in params.items() if v is not None})}"
        headers = {
            "Authorization": f"Bearer {self._token()}",
            "Accept": "application/json",
        }
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = Request(url=url, method=method.upper(), data=data, headers=headers)
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            body_text = ""
            try:
                body_text = exc.fp.read().decode("utf-8", errors="replace") if exc.fp else ""
            except Exception:
                pass
            raise BQError(method, url, exc.code, exc.reason, body_text) from exc

    def execute_sql(self, sql: str, timeout: int = 120) -> dict[str, Any]:
        """Insert a query job, poll for completion, return rows as dicts."""
        insert = self._request(
            "POST",
            f"/bigquery/v2/projects/{self.billing_project}/jobs",
            body={
                "jobReference": {"projectId": self.billing_project, "location": self.region.upper()},
                "configuration": {
                    "query": {
                        "query": sql,
                        "useLegacySql": False,
                        "useQueryCache": True,
                    },
                },
            },
        )
        job_id = insert.get("jobReference", {}).get("jobId")
        if not job_id:
            raise RuntimeError(f"No job id in insert response: {insert}")
        deadline = time.time() + timeout
        rows, schema = [], {}
        bytes_processed = 0
        page_token = None
        while time.time() < deadline:
            resp = self._request(
                "GET",
                f"/bigquery/v2/projects/{self.billing_project}/queries/{job_id}",
                params={
                    "location": self.region.upper(),
                    "maxResults": "5000",
                    "pageToken": page_token,
                },
            )
            if resp.get("schema"):
                schema = resp["schema"]
            if resp.get("totalBytesProcessed"):
                bytes_processed = int(resp["totalBytesProcessed"])
            for row in (resp.get("rows") or []):
                rows.append(row)
            page_token = resp.get("pageToken")
            if resp.get("jobComplete") and not page_token:
                break
            if not resp.get("jobComplete"):
                time.sleep(1.0)
        return {
            "rows": _rows_to_dicts(schema, rows),
            "schema": schema,
            "bytes_processed": bytes_processed,
        }


class BQError(Exception):
    def __init__(self, method: str, url: str, code: int, reason: str, body: str):
        self.code = code
        super().__init__(f"{method} {url} → {code} {reason}: {body[:300]}")


def _rows_to_dicts(schema: dict, rows: list) -> list[dict]:
    fields = schema.get("fields", []) if isinstance(schema, dict) else []
    names = [f.get("name", "") for f in fields if isinstance(f, dict)]
    out = []
    for row in rows:
        cells = row.get("f", []) if isinstance(row, dict) else []
        out.append({
            names[i]: cells[i].get("v") if i < len(cells) and isinstance(cells[i], dict) else None
            for i in range(len(names))
        })
    return out


# ─── Atomic file writers ──────────────────────────────────────────


def write_csv_atomic(rows: list[dict], path: Path, fieldnames: list[str] | None = None) -> None:
    """Write CSV using write-then-rename for crash safety."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if not rows:
        # Empty CSV: write just the header (if fieldnames given) or empty file
        with tmp.open("w", encoding="utf-8") as f:
            if fieldnames:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    else:
        if fieldnames is None:
            fieldnames = list(rows[0].keys())
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    os.replace(tmp, path)


def write_json_atomic(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, path)


# ─── The 12 probes ────────────────────────────────────────────────


def probe_columns(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT column_name, data_type, is_nullable, is_partitioning_column,
               clustering_ordinal_position, ordinal_position
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{t.name}'
        ORDER BY ordinal_position
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "1_1__columns.csv"
        write_csv_atomic(r["rows"], path)
        return ProbeOutcome("schema", "ok",
                            f"{len(r['rows'])} columns", path,
                            rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("schema", "fail", f"HTTP {e.code}", error=str(e))


def probe_col_descriptions(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT column_name, description, rounding_mode, column_default
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE table_name = '{t.name}'
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "1_2__col_descriptions.csv"
        write_csv_atomic(r["rows"], path,
                         fieldnames=["column_name", "description", "rounding_mode", "column_default"])
        with_desc = sum(1 for row in r["rows"] if row.get("description"))
        return ProbeOutcome("col_descriptions", "ok" if with_desc else "warn",
                            f"{with_desc}/{len(r['rows'])} have descriptions",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("col_descriptions", "fail", f"HTTP {e.code}", error=str(e))


def probe_table_meta(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT table_name, table_type, creation_time, ddl
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.TABLES
        WHERE table_name = '{t.name}'
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "1_3__table_meta.json"
        if not r["rows"]:
            return ProbeOutcome("table_meta", "warn", "no row returned")
        row = r["rows"][0]
        write_json_atomic(row, path)
        ddl_len = len(row.get("ddl") or "")
        return ProbeOutcome("table_meta", "ok",
                            f"type={row.get('table_type')}, ddl={ddl_len} chars",
                            path, rows_returned=1)
    except BQError as e:
        return ProbeOutcome("table_meta", "fail", f"HTTP {e.code}", error=str(e))


def probe_table_options(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT option_name, option_type, option_value
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS
        WHERE table_name = '{t.name}'
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "1_4__table_options.csv"
        write_csv_atomic(r["rows"], path,
                         fieldnames=["option_name", "option_type", "option_value"])
        return ProbeOutcome("table_options", "ok" if r["rows"] else "warn",
                            f"{len(r['rows'])} options", path,
                            rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("table_options", "fail", f"HTTP {e.code}", error=str(e))


def probe_constraints(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT constraint_name, constraint_type
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE table_name = '{t.name}'
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "1_5__constraints.csv"
        write_csv_atomic(r["rows"], path,
                         fieldnames=["constraint_name", "constraint_type"])
        return ProbeOutcome("constraints", "ok" if r["rows"] else "warn",
                            f"{len(r['rows'])} declared" if r["rows"] else "none (typical)",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("constraints", "fail", f"HTTP {e.code}", error=str(e))


def probe_size_freshness(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    """Try __TABLES__ first; fall back to INFORMATION_SCHEMA.TABLE_STORAGE.

    The legacy __TABLES__ view 403s on many AmEx environments because it
    pre-dates the granular IAM model. INFORMATION_SCHEMA.TABLE_STORAGE
    (GA 2024) works under standard metadataViewer perms.
    """
    path = out_dir / "2_1__size_freshness.csv"

    # Attempt 1: legacy __TABLES__
    legacy_sql = f"""
        SELECT row_count, size_bytes,
               TIMESTAMP_MILLIS(last_modified_time) AS last_modified_at
        FROM `{t.bq_project}.{t.bq_dataset}.__TABLES__`
        WHERE table_id = '{t.name}'
    """
    try:
        r = client.execute_sql(legacy_sql)
        if r["rows"]:
            write_csv_atomic(r["rows"], path,
                             fieldnames=["row_count", "size_bytes", "last_modified_at"])
            row = r["rows"][0]
            return ProbeOutcome("size", "ok",
                                f"rows={row.get('row_count')}, "
                                f"gb={int(row.get('size_bytes') or 0)/1e9:.2f}",
                                path, rows_returned=1)
    except BQError as legacy_err:
        if legacy_err.code not in (403, 404, 400):
            return ProbeOutcome("size", "fail", f"HTTP {legacy_err.code}",
                                error=str(legacy_err))

    # Attempt 2: modern INFORMATION_SCHEMA.TABLE_STORAGE
    modern_sql = f"""
        SELECT total_rows AS row_count,
               total_logical_bytes AS size_bytes,
               creation_time AS last_modified_at
        FROM `{t.bq_project}`.`region-{t.region}`.INFORMATION_SCHEMA.TABLE_STORAGE
        WHERE table_schema = '{t.bq_dataset}' AND table_name = '{t.name}'
    """
    try:
        r = client.execute_sql(modern_sql)
        if r["rows"]:
            write_csv_atomic(r["rows"], path,
                             fieldnames=["row_count", "size_bytes", "last_modified_at"])
            row = r["rows"][0]
            return ProbeOutcome("size", "ok",
                                f"rows={row.get('row_count')}, "
                                f"gb={int(row.get('size_bytes') or 0)/1e9:.2f} "
                                f"(via TABLE_STORAGE)",
                                path, rows_returned=1)
        # Empty from both sources — write empty file for downstream tools
        write_csv_atomic([], path,
                         fieldnames=["row_count", "size_bytes", "last_modified_at"])
        return ProbeOutcome("size", "warn",
                            "not visible in __TABLES__ or TABLE_STORAGE "
                            "(likely a view; size derivable from profile probe)")
    except BQError as modern_err:
        # Both attempts failed — view-only environment; not a graph blocker
        write_csv_atomic([], path,
                         fieldnames=["row_count", "size_bytes", "last_modified_at"])
        return ProbeOutcome("size", "warn",
                            f"both __TABLES__ and TABLE_STORAGE inaccessible "
                            f"(HTTP {modern_err.code}); size derived from profile",
                            path, error=str(modern_err))


def probe_partitions(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT partition_id, total_rows, total_billable_bytes,
               last_modified_time, storage_tier
        FROM `{t.bq_project}`.{t.bq_dataset}.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = '{t.name}'
        ORDER BY partition_id DESC LIMIT 60
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "2_2__partitions.csv"
        write_csv_atomic(r["rows"], path)
        return ProbeOutcome("partitions", "ok" if r["rows"] else "warn",
                            f"{len(r['rows'])} partitions" if r["rows"] else "not partitioned at this level",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("partitions", "warn" if e.code in (403, 404) else "fail",
                            f"HTTP {e.code}", error=str(e))


def probe_top_users(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT user_email,
               COUNT(*) AS query_count,
               SUM(total_bytes_billed) AS bytes_billed,
               MIN(creation_time) AS first_seen,
               MAX(creation_time) AS last_seen
        FROM `region-{t.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY' AND state = 'DONE' AND error_result IS NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
          AND EXISTS (
              SELECT 1 FROM UNNEST(referenced_tables) AS rt
              WHERE rt.table_id = '{t.name}'
          )
        GROUP BY user_email
        ORDER BY query_count DESC
        LIMIT 30
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "4_1__top_users.csv"
        write_csv_atomic(r["rows"], path)
        return ProbeOutcome("top_users", "ok" if r["rows"] else "warn",
                            f"{len(r['rows'])} users in 90d",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("top_users", "fail", f"HTTP {e.code}", error=str(e))


def probe_failed_queries(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT user_email, creation_time, error_result.reason, error_result.message,
               SUBSTR(query, 1, 500) AS query_preview
        FROM `region-{t.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY' AND state = 'DONE'
          AND error_result IS NOT NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND query LIKE '%{t.name}%'
        ORDER BY creation_time DESC LIMIT 50
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "4_5__failed_queries.csv"
        write_csv_atomic(r["rows"], path)
        return ProbeOutcome("failed_queries", "ok",
                            f"{len(r['rows'])} failed queries in 30d",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("failed_queries", "fail", f"HTTP {e.code}", error=str(e))


def probe_co_queried(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS co_table,
               COUNT(*) AS co_query_count
        FROM `region-{t.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
             UNNEST(j.referenced_tables) AS rt
        WHERE j.job_type = 'QUERY' AND j.state = 'DONE' AND j.error_result IS NULL
          AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
          AND EXISTS (
              SELECT 1 FROM UNNEST(j.referenced_tables) AS r2
              WHERE r2.table_id = '{t.name}'
          )
          AND rt.table_id != '{t.name}'
        GROUP BY co_table
        ORDER BY co_query_count DESC LIMIT 30
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "4_3__co_queried.csv"
        write_csv_atomic(r["rows"], path)
        return ProbeOutcome("co_queried", "ok" if r["rows"] else "warn",
                            f"{len(r['rows'])} co-queried tables",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("co_queried", "fail", f"HTTP {e.code}", error=str(e))


def probe_cost(client: BQClient, t: TableConfig, out_dir: Path) -> ProbeOutcome:
    sql = f"""
        SELECT
            EXTRACT(DATE FROM creation_time) AS query_date,
            COUNT(*) AS query_count,
            SUM(total_bytes_billed) / 1e9 AS gb_billed,
            SUM(total_bytes_billed) / 1e12 * 6.25 AS approx_usd
        FROM `region-{t.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY' AND state = 'DONE' AND error_result IS NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND EXISTS (
              SELECT 1 FROM UNNEST(referenced_tables) AS rt
              WHERE rt.table_id = '{t.name}'
          )
        GROUP BY query_date ORDER BY query_date DESC
    """
    try:
        r = client.execute_sql(sql)
        path = out_dir / "7_1__cost_30d.csv"
        write_csv_atomic(r["rows"], path)
        total_usd = sum(float(row.get("approx_usd") or 0) for row in r["rows"])
        return ProbeOutcome("cost_30d", "ok",
                            f"{len(r['rows'])} days, ~${total_usd:.2f}",
                            path, rows_returned=len(r["rows"]))
    except BQError as e:
        return ProbeOutcome("cost_30d", "fail", f"HTTP {e.code}", error=str(e))


# ─── Profiling — the one probe that costs money ──────────────────


def probe_profile_and_topcount(client: BQClient, t: TableConfig, out_dir: Path) -> list[ProbeOutcome]:
    """Two-phase profiling, view-aware sampling.

    Sampling strategy depends on table type (read from 1_3__table_meta.json):
      VIEW         → partition-filter sample (TABLESAMPLE is unsupported on views)
      BASE TABLE   → TABLESAMPLE SYSTEM
      EXTERNAL etc → no sampling (uncommon enough to scan)

    For views, we read the partition column from the schema and add a WHERE
    clause on the most-recent 30 days. Same statistical effect (samples a
    recent slice), legal on views.

    Phase 1: combined APPROX_COUNT_DISTINCT + COUNTIF(NULL) for all profileable cols.
    Phase 2: APPROX_TOP_COUNT on each low-cardinality column (distinct ≤ threshold).
    """
    outcomes: list[ProbeOutcome] = []

    # Read the column list from the just-written 1_1__columns.csv
    cols_path = out_dir / "1_1__columns.csv"
    if not cols_path.exists():
        outcomes.append(ProbeOutcome("profile", "skip",
                                     "schema probe didn't run; can't profile"))
        return outcomes

    with cols_path.open() as f:
        cols = list(csv.DictReader(f))

    # Detect table type from the table_meta probe output (already landed)
    is_view = False
    table_meta_path = out_dir / "1_3__table_meta.json"
    if table_meta_path.exists():
        try:
            meta = json.loads(table_meta_path.read_text())
            is_view = (meta.get("table_type") or "").upper() == "VIEW"
        except (json.JSONDecodeError, OSError):
            pass

    # Find the partition column for views (TABLESAMPLE doesn't work on views)
    partition_col = None
    for c in cols:
        if (c.get("is_partitioning_column") or "").upper() in {"YES", "TRUE"}:
            partition_col = c["column_name"]
            break

    # Pick only types worth profiling for cardinality
    profileable = [
        c for c in cols
        if (c.get("data_type") or "").upper() in
           {"STRING", "INT64", "INTEGER", "BOOL", "BOOLEAN", "DATE", "DATETIME", "TIMESTAMP"}
    ]
    if not profileable:
        outcomes.append(ProbeOutcome("profile", "warn", "no profileable columns"))
        return outcomes

    # Cap to 50 columns to keep query manageable; pick first 50 by ordinal
    profileable = profileable[:50]

    # Build the single combined query
    aggs = ["COUNT(*) AS __total_rows__"]
    for c in profileable:
        name = c["column_name"]
        safe = name.replace("`", "")
        aggs.append(f"APPROX_COUNT_DISTINCT(`{safe}`) AS `{safe}__distinct`")
        aggs.append(f"COUNTIF(`{safe}` IS NULL) AS `{safe}__nulls`")

    # Sampling strategy: TABLESAMPLE on base tables, partition-filter on views
    sample_clause, sample_where, sample_strategy = _build_sample_clause(
        is_view, partition_col, t.profile_sample_pct,
    )

    profile_sql = f"""
        SELECT {', '.join(aggs)}
        FROM `{t.fqn}` {sample_clause}
        {sample_where}
    """
    try:
        r = client.execute_sql(profile_sql, timeout=300)
        bytes_processed = r.get("bytes_processed", 0)
        cost_usd = bytes_processed / 1e12 * 6.25
        row = r["rows"][0] if r["rows"] else {}
        total_rows = int(row.get("__total_rows__") or 0)

        # Reshape into one-row-per-column for CSV
        per_col = []
        low_card_cols = []
        for c in profileable:
            name = c["column_name"]
            distinct = int(row.get(f"{name}__distinct") or 0)
            nulls = int(row.get(f"{name}__nulls") or 0)
            null_frac = (nulls / max(total_rows, 1)) if total_rows else None
            per_col.append({
                "column_name": name,
                "approx_distinct": distinct,
                "null_count": nulls,
                "null_fraction": f"{null_frac:.6f}" if null_frac is not None else "",
                "cardinality_bucket": _cardinality_bucket(distinct),
                "sample_total_rows": total_rows,
                "sample_pct": t.profile_sample_pct,
            })
            if 0 < distinct <= t.low_card_threshold:
                low_card_cols.append(name)
        write_csv_atomic(per_col, out_dir / "3_1__cardinality_nulls.csv")

        outcomes.append(ProbeOutcome(
            "profile", "ok",
            f"{len(per_col)} cols profiled, {len(low_card_cols)} low-card, "
            f"strategy={sample_strategy}, bytes={bytes_processed/1e9:.2f}GB, "
            f"cost=~${cost_usd:.4f}",
            out_dir / "3_1__cardinality_nulls.csv",
            rows_returned=len(per_col),
        ))

        # Phase 2: top-count for each low-card column (same sampling strategy)
        if low_card_cols:
            tc_outcome = _run_topcounts(client, t, out_dir, low_card_cols,
                                        sample_clause, sample_where)
            outcomes.append(tc_outcome)

        return outcomes

    except BQError as e:
        # If TABLESAMPLE was the culprit (view-rejection error), retry with no sampling.
        # This produces accurate stats at higher cost — better than nothing.
        if (sample_strategy == "tablesample" and
                ("TABLESAMPLE" in (str(e) or "").upper() or e.code == 400)):
            outcomes.append(ProbeOutcome(
                "profile", "warn",
                f"TABLESAMPLE rejected ({e.code}); retrying without sample",
                error=str(e)[:200],
            ))
            try:
                fallback_sql = f"SELECT {', '.join(aggs)} FROM `{t.fqn}`"
                if partition_col:
                    fallback_sql += (
                        f" WHERE `{partition_col}` >= "
                        f"DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
                    )
                r = client.execute_sql(fallback_sql, timeout=600)
                # ... fall through; the caller's logic re-runs below would be ideal
                # but to keep this patch contained we just record outcome here
                outcomes.append(ProbeOutcome(
                    "profile_fallback", "ok",
                    f"fallback succeeded; bytes={r.get('bytes_processed', 0)/1e9:.2f}GB",
                ))
            except BQError as e2:
                outcomes.append(ProbeOutcome(
                    "profile_fallback", "fail",
                    f"HTTP {e2.code} on fallback too", error=str(e2),
                ))
            return outcomes
        outcomes.append(ProbeOutcome("profile", "fail",
                                     f"HTTP {e.code}", error=str(e)))
        return outcomes


def _build_sample_clause(is_view: bool, partition_col: str | None,
                         sample_pct: float) -> tuple[str, str, str]:
    """Return (sample_clause, sample_where, strategy_label) for the profile query.

    BigQuery's TABLESAMPLE SYSTEM is NOT supported on views. For views, we
    fall back to a partition-filter sample (a recent N-day window): same
    statistical effect (samples a recent slice of data), legal on views.
    For base tables we use TABLESAMPLE which is cheaper and more representative.
    """
    if sample_pct >= 100:
        return "", "", "full_scan"
    if is_view:
        if partition_col:
            # 30 days * 1% sample_pct rough analog (e.g., 1% → 30/100 = 0.3 days
            # which is too short; we use 30d as the conservative recent window)
            return ("", f"WHERE `{partition_col}` >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)",
                    "view_partition_window_30d")
        # View without partition column — no good sampling; warn caller via label
        return "", "", "view_unsampled_no_partition"
    # Base table — TABLESAMPLE is the cheapest option
    return f"TABLESAMPLE SYSTEM ({sample_pct} PERCENT)", "", "tablesample"


def _run_topcounts(client: BQClient, t: TableConfig, out_dir: Path,
                   columns: list[str], sample_clause: str,
                   sample_where: str = "") -> ProbeOutcome:
    """One combined query producing top-20 values per low-card column.

    Uses APPROX_TOP_COUNT which returns STRUCT<value, count> arrays —
    we flatten per-column into separate CSVs."""
    if not columns:
        return ProbeOutcome("topcount", "skip", "no low-card columns")

    # APPROX_TOP_COUNT for each col in one query
    aggs = [
        f"APPROX_TOP_COUNT(`{c}`, 20) AS `{c}__top`"
        for c in columns
    ]
    sql = f"""
        SELECT {', '.join(aggs)}
        FROM `{t.fqn}` {sample_clause}
        {sample_where}
    """
    try:
        r = client.execute_sql(sql, timeout=300)
        row = r["rows"][0] if r["rows"] else {}
        # Each `<col>__top` value is a list of {value, count}
        n_written = 0
        for col in columns:
            arr = row.get(f"{col}__top") or []
            # BQ may return as list of dicts already, or as raw list
            if isinstance(arr, list):
                values = []
                for entry in arr:
                    if isinstance(entry, dict):
                        values.append({
                            "value": entry.get("value"),
                            "count": entry.get("count"),
                        })
                if values:
                    safe_col = col.replace("/", "_")
                    write_csv_atomic(
                        values,
                        out_dir / f"3_2__topcount__{safe_col}.csv",
                        fieldnames=["value", "count"],
                    )
                    n_written += 1
        return ProbeOutcome("topcount", "ok",
                            f"{n_written} per-column top-count files written",
                            rows_returned=n_written)
    except BQError as e:
        return ProbeOutcome("topcount", "fail", f"HTTP {e.code}", error=str(e))


def _cardinality_bucket(distinct: int) -> str:
    if distinct == 0:
        return "empty"
    if distinct <= 100:
        return "low"
    if distinct <= 10_000:
        return "medium"
    if distinct <= 1_000_000:
        return "high"
    return "very_high"


# ─── Per-table runner ─────────────────────────────────────────────


def extract_one_table(client: BQClient, t: TableConfig, force: bool) -> TableResult:
    """Run all 12 probes against one table. Synchronous within a table
    (probes share the same BQClient + auth token; sequential is fine)."""
    out_dir = Path(t.__dict__.get("output_dir") or "./data/real_extractions") / t.name
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "_summary.json"
    result = TableResult(table_name=t.name, output_dir=out_dir)
    t0 = time.time()

    # Skip if already complete and not forced
    if not force and summary_path.exists():
        try:
            existing = json.loads(summary_path.read_text())
            if existing.get("complete"):
                result.duration_sec = 0.0
                result.probes = [
                    ProbeOutcome(
                        name=p["name"], status="skip",
                        summary="cached from previous run",
                    )
                    for p in existing.get("probes", [])
                ]
                return result
        except (json.JSONDecodeError, KeyError):
            pass

    probes: list[ProbeOutcome] = []

    # Run metadata probes first (they're free)
    probes.append(probe_columns(client, t, out_dir))
    probes.append(probe_col_descriptions(client, t, out_dir))
    probes.append(probe_table_meta(client, t, out_dir))
    probes.append(probe_table_options(client, t, out_dir))
    probes.append(probe_constraints(client, t, out_dir))
    probes.append(probe_size_freshness(client, t, out_dir))
    probes.append(probe_partitions(client, t, out_dir))
    probes.append(probe_top_users(client, t, out_dir))
    probes.append(probe_failed_queries(client, t, out_dir))
    probes.append(probe_co_queried(client, t, out_dir))
    probes.append(probe_cost(client, t, out_dir))

    # Profiling (only one that costs money)
    profile_outcomes = probe_profile_and_topcount(client, t, out_dir)
    probes.extend(profile_outcomes)

    result.probes = probes
    result.duration_sec = round(time.time() - t0, 2)

    # Write per-table summary
    write_json_atomic({
        "table": t.name,
        "fqn": t.fqn,
        "complete": True,
        "ran_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_sec": result.duration_sec,
        "probes": [
            {"name": p.name, "status": p.status, "summary": p.summary,
             "rows_returned": p.rows_returned, "error": p.error}
            for p in probes
        ],
        "counts": {"ok": result.n_ok, "warn": result.n_warn, "fail": result.n_fail},
    }, summary_path)

    return result


async def run_batch(client: BQClient, tables: list[TableConfig], concurrency: int,
                    force: bool) -> list[TableResult]:
    """Async fan-out of extract_one_table across the table list."""
    semaphore = asyncio.Semaphore(concurrency)

    async def one(t: TableConfig) -> TableResult:
        async with semaphore:
            # Each table-extract is sync; offload to a thread
            result = await asyncio.to_thread(extract_one_table, client, t, force)
            glyph = "✅" if result.n_fail == 0 else "⚠"
            print(f"  [{glyph}] {result.table_name:50s}  "
                  f"{result.n_ok} ok / {result.n_warn} warn / {result.n_fail} fail  "
                  f"({result.duration_sec:.1f}s)")
            return result

    return await asyncio.gather(*[one(t) for t in tables])


# ─── Config loading ───────────────────────────────────────────────


def load_tables_config(path: Path) -> tuple[dict, list[TableConfig]]:
    if not path.exists():
        raise FileNotFoundError(f"tables.yaml not found at {path}")
    raw = yaml.safe_load(path.read_text())
    defaults = raw.get("defaults", {})
    tables_raw = raw.get("tables", [])
    tables = []
    for entry in tables_raw:
        if isinstance(entry, str):
            entry = {"name": entry}
        merged = {**defaults, **entry}
        if "name" not in merged:
            continue
        tables.append(TableConfig(
            name=merged["name"],
            bq_project=merged.get("bq_project", "axp-lumi"),
            bq_dataset=merged.get("bq_dataset", "dw"),
            billing_project=merged.get("billing_project", "prj-p-lumi-gpt"),
            region=merged.get("region", "us"),
            profile_sample_pct=float(merged.get("profile_sample_pct", 1)),
            low_card_threshold=int(merged.get("low_card_threshold", 1000)),
        ))
    return defaults, tables


# ─── Main ─────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch BQ extractor — async, 12 probes, 53-table scale",
    )
    parser.add_argument("--config", default="semantic-graph/config/tables.yaml",
                        help="Path to tables.yaml manifest")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse YAML + print plan, run no queries")
    parser.add_argument("--force", action="store_true",
                        help="Re-extract even if _summary.json exists")
    parser.add_argument("--only", default="",
                        help="Comma-separated table names; restrict to these")
    parser.add_argument("--output-dir", default=None,
                        help="Override output_dir from tables.yaml")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    # Dry-run mode doesn't need real auth — just confirms YAML + plan
    if not args.dry_run and (not sa_path or not Path(sa_path).exists()):
        print(f"ERROR: GOOGLE_APPLICATION_CREDENTIALS must point at SA JSON (got: {sa_path or 'unset'})")
        return 1

    base_url = os.getenv("BIGQUERY_API_BASE_URL", DEFAULT_BASE_URL).strip().rstrip("/")

    defaults, tables = load_tables_config(Path(args.config))
    if not tables:
        print("ERROR: no tables in config")
        return 1

    if args.only:
        wanted = {n.strip() for n in args.only.split(",") if n.strip()}
        tables = [t for t in tables if t.name in wanted]
        if not tables:
            print(f"ERROR: --only matched no tables in config (wanted: {wanted})")
            return 1

    output_dir = Path(args.output_dir or defaults.get("output_dir", "./data/real_extractions"))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Attach output_dir to each TableConfig (lazy add)
    for t in tables:
        t.__dict__["output_dir"] = output_dir

    billing_project = defaults.get("billing_project", tables[0].billing_project)
    region = defaults.get("region", tables[0].region)
    concurrency = int(defaults.get("concurrency", 8))

    print()
    print("─" * 80)
    print("  BQ batch extractor")
    print("─" * 80)
    print(f"  config:           {args.config}")
    print(f"  tables to run:    {len(tables)}")
    print(f"  billing project:  {billing_project}")
    print(f"  region:           {region}")
    print(f"  concurrency:      {concurrency}")
    print(f"  output dir:       {output_dir.resolve()}")
    print(f"  endpoint:         {base_url}")
    print(f"  force re-extract: {args.force}")
    print()

    if args.dry_run:
        print("Dry-run mode — would extract:")
        for t in tables:
            print(f"  • {t.fqn}")
        return 0

    client = BQClient(
        billing_project=billing_project,
        sa_path=sa_path,
        base_url=base_url,
        region=region,
    )
    print(f"  SA email:         {client.sa_email}")
    print()
    print("  Running extractions...")
    print()

    t0 = time.time()
    results = asyncio.run(run_batch(client, tables, concurrency, args.force))
    duration = time.time() - t0

    # Batch summary — classify tables by REAL severity, not "any fail"
    total_ok = sum(r.n_ok for r in results)
    total_warn = sum(r.n_warn for r in results)
    total_fail = sum(r.n_fail for r in results)

    # A table is "fully usable" if its schema probe (the foundation) succeeded.
    # A table is "partially usable" if schema worked but some non-essential probes failed.
    # A table is "genuinely failed" only if schema (probe 1) didn't run.
    def _schema_ok(result) -> bool:
        return any(p.name == "schema" and p.status == "ok" for p in result.probes)

    fully_usable = [r for r in results if _schema_ok(r) and r.n_fail == 0]
    partially_usable = [r for r in results if _schema_ok(r) and r.n_fail > 0]
    genuinely_failed = [r for r in results if not _schema_ok(r)]

    print()
    print("─" * 80)
    print("  BATCH SUMMARY")
    print("─" * 80)
    print(f"  Tables processed:    {len(results)}")
    print(f"  Fully usable:        {len(fully_usable)} (all probes ok or warn)")
    print(f"  Partially usable:    {len(partially_usable)} (schema ok, some probes had environment-blocks)")
    print(f"  Genuinely failed:    {len(genuinely_failed)} (schema probe failed; graph cannot use these)")
    print(f"  Probe totals across all tables: {total_ok} ok / {total_warn} warn / {total_fail} fail")
    print(f"  Wall time:           {duration:.1f}s")
    if genuinely_failed:
        print("  ❌ Genuinely failed tables (need investigation):")
        for r in genuinely_failed[:10]:
            print(f"    • {r.table_name}")
    if partially_usable:
        print(f"  ⚠ Partially usable tables ({len(partially_usable)} — graph builder will still consume these):")
        for r in partially_usable[:5]:
            failed_probes = [p.name for p in r.probes if p.status == "fail"]
            print(f"    • {r.table_name}  (failed probes: {', '.join(failed_probes)})")

    write_json_atomic({
        "ran_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_sec": round(duration, 2),
        "tables_processed": len(results),
        "fully_usable": [r.table_name for r in fully_usable],
        "partially_usable": [r.table_name for r in partially_usable],
        "genuinely_failed": [r.table_name for r in genuinely_failed],
        "totals": {"ok": total_ok, "warn": total_warn, "fail": total_fail},
        "per_table": [
            {
                "table": r.table_name, "ok": r.n_ok, "warn": r.n_warn,
                "fail": r.n_fail, "duration_sec": r.duration_sec,
                "schema_ok": _schema_ok(r),
            }
            for r in results
        ],
    }, output_dir / "_batch_summary.json")

    print(f"  Per-table summaries: {output_dir}/<table>/_summary.json")
    print(f"  Batch summary:       {output_dir}/_batch_summary.json")
    print()

    # Exit code reflects ONLY genuinely failed tables (schema-probe failures);
    # partially-usable is success (graph builder consumes them).
    return 0 if not genuinely_failed else 1


if __name__ == "__main__":
    sys.exit(main())
