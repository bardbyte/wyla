"""BigQuery capabilities probe — what can our service account see for ONE table?

PURPOSE
=======
Before building 53-table loaders, discover what extractions the service-account
can actually perform. For a target table:

  1. Probe IAM permissions (testIamPermissions REST call)
  2. Run 23 representative queries spanning the 10 graph-fact categories
  3. Report ✅/⚠/❌ per probe with concrete failure explanations
  4. Print a final capability matrix the loader code can rely on

This is the diagnostic phase. Run this once per environment, capture the
output, then build loaders that only attempt what the SA can reach.

USAGE
=====
    # Defaults to cardmember table in axp-lumi.dw
    python scripts/bq_capabilities_probe.py

    # Different table
    python scripts/bq_capabilities_probe.py \\
        --project axp-lumi \\
        --dataset dw \\
        --table custins_customer_insights_cardmember

    # Save full output to JSON
    python scripts/bq_capabilities_probe.py --out probe_results.json

    # Verbose: print full query results, not just summaries
    python scripts/bq_capabilities_probe.py --verbose

ENVIRONMENT
===========
Required:
    GOOGLE_APPLICATION_CREDENTIALS   path to SA JSON
    BQ_PROJECT_ID                    project for billing the queries
                                     (can differ from data project)

Optional:
    BIGQUERY_API_BASE_URL            override REST endpoint
                                     (defaults to bigquery-prod-PSC)
    BQ_REGION                        region for JOBS_BY_PROJECT
                                     (defaults to region-us)
    BQ_FORCE_PROXY                   set to 1 to keep proxy on Google hosts
    REQUESTS_CA_BUNDLE               custom CA bundle path

OUTPUT
======
Per-probe lines like:
    [✅] section_1_schema           190 columns; 4 DATE/DATETIME, 35 STRING, 147 FLOAT64, 4 INT64
    [⚠] section_3_profiling        ran (0 rows; expected if RLS-gated view)
    [❌] section_6_dataplex_dq      403 PermissionDenied; needs roles/dataplex.viewer
    [✅] iam_testIamPermissions      can: get, getIamPolicy. cannot: getData, updateData

Plus a final capability matrix:
    GRAPH FACT CATEGORY        REACHABLE   NOTES
    ─────────────────────────  ─────────  ─────────────────────────────
    Schema + types             YES        full coverage
    Column profiling           PARTIAL    base-table works; view RLS-gated
    Top users                  NO         needs roles/bigquery.resourceViewer
    ...
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import find_dotenv, load_dotenv
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account
import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://bigquery-prod.p.googleapis.com"
DEFAULT_SCOPE = "https://www.googleapis.com/auth/bigquery"

# Google hosts that should bypass the corporate proxy by default.
DEFAULT_NO_PROXY_HOSTS = (
    "oauth2.googleapis.com",
    "oauth2-dev.p.googleapis.com",
    "oauth2-prod.p.googleapis.com",
    "bigquery.googleapis.com",
    "bigquery-dev.p.googleapis.com",
    "bigquery-prod.p.googleapis.com",
)

# Status glyphs — match the docs' visual language.
GLYPH = {"ok": "✅", "warn": "⚠", "fail": "❌", "info": "ℹ"}


# ─── ProbeResult ────────────────────────────────────────────────


@dataclass
class ProbeResult:
    """One probe's outcome. Uniform shape so the summary matrix is easy."""
    name: str                           # human label, e.g. "section_1_schema"
    category: str                       # graph-fact category, e.g. "Schema"
    status: str                         # "ok" / "warn" / "fail"
    summary: str                        # one-line human summary
    data: Any = None                    # raw result (for verbose mode)
    error: str | None = None            # full error message if status=fail
    fix_hint: str | None = None         # what to grant/configure to unblock

    def glyph(self) -> str:
        return GLYPH[self.status]


# ─── BigQuery REST client (extracted from your existing pattern) ────


class BQClient:
    """Minimal REST client with the same auth pattern as your existing script."""

    def __init__(self, project: str, sa_path: str, base_url: str, region: str) -> None:
        self.project = project
        self.base_url = base_url.rstrip("/")
        self.region = region.lstrip(".").lstrip("region-")
        # JOBS_BY_PROJECT views require the "region-" prefix
        self.region_qualified = f"region-{self.region}"
        self._creds = service_account.Credentials.from_service_account_file(
            sa_path, scopes=[DEFAULT_SCOPE]
        )
        self.sa_email = self._creds.service_account_email
        self.sa_project = self._creds.project_id
        session = requests.Session()
        if os.getenv("BQ_DISABLE_PROXY", "").strip().lower() in {"1", "true"}:
            session.trust_env = False
        ca_bundle = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
        if ca_bundle:
            session.verify = ca_bundle
        self._auth_request = GoogleAuthRequest(session=session)

    def _token(self) -> str:
        self._creds.refresh(self._auth_request)
        return self._creds.token

    def _request(self, method: str, path: str, body: dict | None = None,
                 params: dict | None = None) -> dict[str, Any]:
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
            with urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            body_text = ""
            try:
                body_text = exc.fp.read().decode("utf-8", errors="replace") if exc.fp else ""
            except Exception:
                pass
            raise BQError(method, url, exc.code, exc.reason, body_text) from exc

    def execute_sql(self, sql: str, timeout: int = 90,
                    max_rows: int = 1000) -> dict[str, Any]:
        """Insert a query job, poll for completion, return rows as dicts."""
        insert = self._request(
            "POST",
            f"/bigquery/v2/projects/{self.project}/jobs",
            body={
                "jobReference": {"projectId": self.project, "location": self.region.upper()},
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
        rows, schema, total = [], {}, None
        page_token = None
        completed = False
        while time.time() < deadline:
            resp = self._request(
                "GET",
                f"/bigquery/v2/projects/{self.project}/queries/{job_id}",
                params={
                    "location": self.region.upper(),
                    "maxResults": str(max_rows),
                    "pageToken": page_token,
                },
            )
            completed = bool(resp.get("jobComplete"))
            if resp.get("schema"):
                schema = resp["schema"]
            if resp.get("totalRows"):
                total = resp["totalRows"]
            for row in (resp.get("rows") or []):
                rows.append(row)
            page_token = resp.get("pageToken")
            if completed and not page_token:
                break
            if not completed:
                time.sleep(1.0)
        if not completed:
            raise RuntimeError(f"Query timeout after {timeout}s; job_id={job_id}")
        return {
            "rows": _rows_to_dicts(schema, rows),
            "schema": schema,
            "total_rows": total,
            "job_id": job_id,
        }

    def test_iam(self, resource_path: str, permissions: list[str]) -> dict:
        """Call testIamPermissions to discover what the SA can do."""
        return self._request(
            "POST",
            f"{resource_path}:testIamPermissions",
            body={"permissions": permissions},
        )


class BQError(Exception):
    """Structured BQ REST error so probes can react to specific codes."""

    def __init__(self, method: str, url: str, code: int, reason: str, body: str):
        self.method = method
        self.url = url
        self.code = code
        self.reason = reason
        self.body = body
        super().__init__(f"{method} {url} → {code} {reason}: {body[:300]}")


def _rows_to_dicts(schema: dict, rows: list) -> list[dict]:
    """BQ returns rows as {"f": [{"v": value}, ...]}; flatten to dicts."""
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


# ─── The 23 probes ──────────────────────────────────────────────


def probe_iam(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """What can the SA do on this table? Returns the granted permissions list."""
    permissions_to_test = [
        "bigquery.tables.get",
        "bigquery.tables.getData",
        "bigquery.tables.getIamPolicy",
        "bigquery.tables.update",
        "bigquery.tables.updateData",
        "bigquery.tables.delete",
        "bigquery.jobs.create",
    ]
    try:
        resp = client.test_iam(
            f"/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}",
            permissions_to_test,
        )
        granted = resp.get("permissions", [])
        return ProbeResult(
            name="iam_testIamPermissions",
            category="IAM probe",
            status="ok",
            summary=f"granted: {', '.join(granted) if granted else 'NONE'}; "
                    f"denied: {', '.join(set(permissions_to_test) - set(granted))}",
            data=granted,
        )
    except BQError as e:
        return ProbeResult(
            name="iam_testIamPermissions",
            category="IAM probe",
            status="fail",
            summary=f"HTTP {e.code}: cannot probe IAM",
            error=str(e),
            fix_hint="SA needs at least roles/bigquery.metadataViewer to probe permissions",
        )


def probe_schema(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 1 — column names + types."""
    sql = f"""
        SELECT column_name, data_type, is_nullable, is_partitioning_column,
               clustering_ordinal_position
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        type_counts: dict[str, int] = {}
        for r in rows:
            type_counts[r.get("data_type", "?")] = type_counts.get(r.get("data_type", "?"), 0) + 1
        type_summary = ", ".join(f"{n} {t}" for t, n in sorted(type_counts.items(), key=lambda kv: -kv[1])[:5])
        return ProbeResult(
            name="section_1_schema",
            category="Schema",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} columns; {type_summary}" if rows else "0 columns returned",
            data=rows,
        )
    except BQError as e:
        return ProbeResult(
            name="section_1_schema", category="Schema", status="fail",
            summary=f"HTTP {e.code}",
            error=str(e),
            fix_hint="Grant roles/bigquery.metadataViewer on the dataset",
        )


def probe_ddl(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 1.3 — table metadata + full DDL (reveals RLS predicate)."""
    sql = f"""
        SELECT table_name, table_type, creation_time, ddl
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.TABLES
        WHERE table_name = '{table}'
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        if not rows:
            return ProbeResult(
                name="section_1_3_ddl", category="DDL + asset_kind",
                status="warn", summary="0 rows — table not visible to SA",
            )
        row = rows[0]
        ddl_len = len(row.get("ddl") or "")
        has_rls = "ROW ACCESS POLICY" in (row.get("ddl") or "").upper() or "ONCOP" in (row.get("ddl") or "").upper()
        return ProbeResult(
            name="section_1_3_ddl", category="DDL + asset_kind",
            status="ok",
            summary=f"table_type={row.get('table_type')}; ddl={ddl_len} chars; "
                    f"RLS detected in DDL: {has_rls}",
            data=row,
        )
    except BQError as e:
        return ProbeResult(
            name="section_1_3_ddl", category="DDL + asset_kind", status="fail",
            summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/bigquery.metadataViewer; DDL requires this",
        )


def probe_table_options(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 1.4 — labels, partition_expiration, require_partition_filter."""
    sql = f"""
        SELECT option_name, option_type, option_value
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS
        WHERE table_name = '{table}'
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        opts = {r.get("option_name"): r.get("option_value") for r in rows}
        return ProbeResult(
            name="section_1_4_table_options", category="Table options",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} options: {', '.join(opts.keys())}" if rows else "no table options set",
            data=opts,
        )
    except BQError as e:
        return ProbeResult(
            name="section_1_4_table_options", category="Table options",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
        )


def probe_constraints(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 1.5 — declared PK / FK / UNIQUE."""
    sql = f"""
        SELECT constraint_name, constraint_type
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE table_name = '{table}'
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        if not rows:
            return ProbeResult(
                name="section_1_5_constraints", category="Declared keys",
                status="warn", summary="no declared PK/FK constraints (typical for AmEx)",
            )
        return ProbeResult(
            name="section_1_5_constraints", category="Declared keys",
            status="ok",
            summary=f"{len(rows)} constraints: {[r.get('constraint_type') for r in rows]}",
            data=rows,
        )
    except BQError as e:
        return ProbeResult(
            name="section_1_5_constraints", category="Declared keys",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
        )


def probe_size_freshness(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 2.1 — row count, size, last modified."""
    sql = f"""
        SELECT row_count, size_bytes,
               TIMESTAMP_MILLIS(last_modified_time) AS last_modified_at
        FROM `{project}.{dataset}.__TABLES__`
        WHERE table_id = '{table}'
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        if not rows:
            return ProbeResult(
                name="section_2_1_size", category="Size + freshness",
                status="warn", summary="0 rows — table not in __TABLES__",
            )
        row = rows[0]
        return ProbeResult(
            name="section_2_1_size", category="Size + freshness",
            status="ok",
            summary=f"row_count={row.get('row_count')}, "
                    f"size_gb={int(row.get('size_bytes') or 0)/1e9:.2f}, "
                    f"last_modified={row.get('last_modified_at')}",
            data=row,
        )
    except BQError as e:
        return ProbeResult(
            name="section_2_1_size", category="Size + freshness",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
        )


def probe_partitions(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 2.2 — per-partition stats."""
    sql = f"""
        SELECT partition_id, total_rows, total_billable_bytes,
               last_modified_time, storage_tier
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = '{table}'
        ORDER BY partition_id DESC LIMIT 10
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        if not rows:
            return ProbeResult(
                name="section_2_2_partitions", category="Partition stats",
                status="warn", summary="0 partitions — table likely not partitioned at this level",
            )
        return ProbeResult(
            name="section_2_2_partitions", category="Partition stats",
            status="ok",
            summary=f"{len(rows)} most-recent partitions; newest={rows[0].get('partition_id')}",
            data=rows,
        )
    except BQError as e:
        return ProbeResult(
            name="section_2_2_partitions", category="Partition stats",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="May be view-level; try probing the underlying physical table",
        )


def probe_profile(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 3 — row count + a profile sample.

    This is the one that gets RLS-blocked on the cardmember view. Reports
    explicitly whether RLS is in play.
    """
    sql = f"""
        SELECT COUNT(*) AS total_rows
        FROM `{project}.{dataset}.{table}`
        WHERE 1=1
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        total = int(rows[0].get("total_rows") or 0) if rows else 0
        if total == 0:
            return ProbeResult(
                name="section_3_profile", category="Profiling (data access)",
                status="warn",
                summary="0 rows visible — almost certainly RLS-gated for this SA",
                data=rows,
                fix_hint="Add SA to security.s_users_map super-user table, "
                         "OR query the underlying physical table directly",
            )
        return ProbeResult(
            name="section_3_profile", category="Profiling (data access)",
            status="ok", summary=f"{total:,} rows visible to SA",
            data=rows,
        )
    except BQError as e:
        return ProbeResult(
            name="section_3_profile", category="Profiling (data access)",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/bigquery.dataViewer on the dataset",
        )


def probe_top_users(client: BQClient, region: str) -> ProbeResult:
    """Section 4.1 — JOBS_BY_PROJECT for the target table's top users."""
    sql = f"""
        SELECT user_email, COUNT(*) AS query_count
        FROM `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY'
          AND state = 'DONE'
          AND error_result IS NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        GROUP BY user_email
        ORDER BY query_count DESC
        LIMIT 10
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_4_1_top_users", category="Usage telemetry (JOBS_BY_PROJECT)",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} top users visible to SA in last 90d" if rows else
                    "0 users — SA may lack JOBS visibility OR table unreferenced",
            data=rows[:5],
            fix_hint=None if rows else "Grant roles/bigquery.resourceViewer on the project",
        )
    except BQError as e:
        return ProbeResult(
            name="section_4_1_top_users", category="Usage telemetry (JOBS_BY_PROJECT)",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/bigquery.resourceViewer on the project",
        )


def probe_failed_queries(client: BQClient, region: str, table: str) -> ProbeResult:
    """Section 4.5 — failed queries (the gold for naming corrections)."""
    sql = f"""
        SELECT error_result.reason, error_result.message
        FROM `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY'
          AND state = 'DONE'
          AND error_result IS NOT NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND query LIKE '%{table}%'
        LIMIT 20
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_4_5_failed_queries",
            category="Failed-query naming corrections",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} failed queries last 30d mention this table"
                    if rows else "0 failed queries (table unreferenced or SA lacks JOBS access)",
            data=rows[:5],
        )
    except BQError as e:
        return ProbeResult(
            name="section_4_5_failed_queries",
            category="Failed-query naming corrections",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/bigquery.resourceViewer on the project",
        )


def probe_lineage_upstream(client: BQClient, region: str, table: str) -> ProbeResult:
    """Section 9.1 — what writes TO this table (upstream lineage)."""
    sql = f"""
        SELECT CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS upstream_table,
               COUNT(*) AS write_count
        FROM `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
             UNNEST(j.referenced_tables) AS rt
        WHERE j.job_type = 'QUERY'
          AND j.state = 'DONE'
          AND j.error_result IS NULL
          AND j.statement_type IN ('CREATE_TABLE_AS_SELECT', 'MERGE', 'INSERT',
                                   'CREATE_OR_REPLACE_TABLE')
          AND j.destination_table.table_id = '{table}'
          AND rt.table_id != '{table}'
          AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        GROUP BY upstream_table ORDER BY write_count DESC LIMIT 20
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_9_1_lineage_upstream",
            category="Lineage (UPSTREAM_OF edges)",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} upstream tables" if rows else
                    "0 upstream — table is a VIEW (no destination_table tracking)",
            data=rows[:5],
        )
    except BQError as e:
        return ProbeResult(
            name="section_9_1_lineage_upstream",
            category="Lineage (UPSTREAM_OF edges)",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
        )


def probe_policy_tags(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 5.1 — column-level policy tags (PII enforcement)."""
    sql = f"""
        SELECT column_name, policy_tags
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table}'
          AND ARRAY_LENGTH(policy_tags.names) > 0
        LIMIT 50
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_5_1_policy_tags", category="PII (policy tags)",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} columns have policy tags" if rows else
                    "0 columns with policy tags (PII via MDM only, not enforced at BQ layer)",
            data=rows[:5],
            fix_hint="If needed, add roles/datacatalog.viewer for tag-name lookups",
        )
    except BQError as e:
        return ProbeResult(
            name="section_5_1_policy_tags", category="PII (policy tags)",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/datacatalog.viewer",
        )


def probe_object_privileges(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 5.2 — who has access to this table."""
    sql = f"""
        SELECT grantee, grantee_type, privilege_type
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
        WHERE object_name = '{table}' LIMIT 30
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_5_2_object_privileges", category="Access grants",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} grants visible" if rows else
                    "OBJECT_PRIVILEGES returned empty",
            data=rows[:5],
        )
    except BQError as e:
        return ProbeResult(
            name="section_5_2_object_privileges", category="Access grants",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Often 404 — try the underlying physical project",
        )


def probe_row_access_policies(client: BQClient, project: str, dataset: str, table: str) -> ProbeResult:
    """Section 5.3 — RLS policy text."""
    sql = f"""
        SELECT row_access_policy_name, filter_predicate, creator
        FROM `{project}`.{dataset}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
        WHERE table_name = '{table}'
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        return ProbeResult(
            name="section_5_3_row_access_policies", category="RLS predicate (programmatic)",
            status="ok" if rows else "warn",
            summary=f"{len(rows)} RLS policies declared" if rows else
                    "no native ROW_ACCESS_POLICIES (RLS likely via DDL view definition)",
            data=rows,
        )
    except BQError as e:
        return ProbeResult(
            name="section_5_3_row_access_policies", category="RLS predicate (programmatic)",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
        )


def probe_cost_30d(client: BQClient, region: str, table: str) -> ProbeResult:
    """Section 7 — cost (bytes billed) last 30 days."""
    sql = f"""
        SELECT
            COUNT(*) AS query_count,
            SUM(total_bytes_billed) / 1e9 AS gb_billed,
            SUM(total_bytes_billed) / 1e12 * 6.25 AS approx_usd_on_demand
        FROM `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE job_type = 'QUERY'
          AND state = 'DONE'
          AND error_result IS NULL
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND EXISTS (
              SELECT 1 FROM UNNEST(referenced_tables) AS rt
              WHERE rt.table_id = '{table}'
          )
    """
    try:
        result = client.execute_sql(sql)
        rows = result["rows"]
        if not rows or not rows[0].get("query_count"):
            return ProbeResult(
                name="section_7_cost", category="Cost telemetry",
                status="warn", summary="0 jobs referenced this table in 30d",
            )
        r = rows[0]
        return ProbeResult(
            name="section_7_cost", category="Cost telemetry",
            status="ok",
            summary=f"{r.get('query_count')} jobs, {float(r.get('gb_billed') or 0):.2f} GB billed, "
                    f"~${float(r.get('approx_usd_on_demand') or 0):.2f} USD",
            data=r,
        )
    except BQError as e:
        return ProbeResult(
            name="section_7_cost", category="Cost telemetry",
            status="fail", summary=f"HTTP {e.code}", error=str(e),
            fix_hint="Grant roles/bigquery.resourceViewer on the project",
        )


# ─── Orchestration ──────────────────────────────────────────────


def run_all_probes(client: BQClient, project: str, dataset: str, table: str,
                   region: str) -> list[ProbeResult]:
    """Run every probe sequentially; return list of results."""
    probes = [
        # IAM first — sets the expectation for everything else
        ("iam", lambda: probe_iam(client, project, dataset, table)),
        # Schema layer
        ("schema", lambda: probe_schema(client, project, dataset, table)),
        ("ddl", lambda: probe_ddl(client, project, dataset, table)),
        ("table_options", lambda: probe_table_options(client, project, dataset, table)),
        ("constraints", lambda: probe_constraints(client, project, dataset, table)),
        # Size + freshness
        ("size", lambda: probe_size_freshness(client, project, dataset, table)),
        ("partitions", lambda: probe_partitions(client, project, dataset, table)),
        # Data access (THIS is where RLS bites)
        ("profile", lambda: probe_profile(client, project, dataset, table)),
        # Usage telemetry
        ("top_users", lambda: probe_top_users(client, region)),
        ("failed_queries", lambda: probe_failed_queries(client, region, table)),
        # Lineage
        ("lineage_upstream", lambda: probe_lineage_upstream(client, region, table)),
        # Governance
        ("policy_tags", lambda: probe_policy_tags(client, project, dataset, table)),
        ("object_privileges", lambda: probe_object_privileges(client, project, dataset, table)),
        ("row_access_policies", lambda: probe_row_access_policies(client, project, dataset, table)),
        # Cost
        ("cost", lambda: probe_cost_30d(client, region, table)),
    ]
    results = []
    for key, fn in probes:
        try:
            r = fn()
        except Exception as e:  # noqa: BLE001
            r = ProbeResult(
                name=key, category="(probe runtime error)",
                status="fail", summary=str(e)[:200], error=str(e),
            )
        results.append(r)
        print(f"  [{r.glyph()}] {r.name:35s} {r.summary[:120]}")
    return results


def print_capability_matrix(results: list[ProbeResult], sa_email: str,
                            sa_project: str, target: str) -> None:
    """Print the final summary the loader code can rely on."""
    print()
    print("─" * 80)
    print("  CAPABILITY MATRIX — what the SA can do for this table")
    print("─" * 80)
    print(f"  Service account: {sa_email}")
    print(f"  SA project:      {sa_project}")
    print(f"  Target:          {target}")
    print()
    print(f"  {'CATEGORY':<40s}  {'STATUS':<8s}  NOTES")
    print(f"  {'-'*40}  {'-'*8}  {'-'*40}")
    by_status = {"ok": 0, "warn": 0, "fail": 0}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1
        status_label = {"ok": "REACHABLE", "warn": "PARTIAL", "fail": "BLOCKED"}[r.status]
        notes = (r.fix_hint or "")[:40] if r.status != "ok" else r.summary[:40]
        print(f"  {r.category:<40s}  {status_label:<8s}  {notes}")
    print()
    total = len(results)
    pct_ok = 100 * by_status["ok"] / max(total, 1)
    pct_warn = 100 * by_status["warn"] / max(total, 1)
    pct_fail = 100 * by_status["fail"] / max(total, 1)
    print(f"  Summary: {by_status['ok']}/{total} reachable ({pct_ok:.0f}%), "
          f"{by_status['warn']} partial ({pct_warn:.0f}%), "
          f"{by_status['fail']} blocked ({pct_fail:.0f}%)")
    print()

    # Action items
    fixes = [r for r in results if r.fix_hint]
    if fixes:
        print("─" * 80)
        print("  ACTION ITEMS — grants needed to unblock more facts")
        print("─" * 80)
        seen = set()
        for r in fixes:
            if r.fix_hint not in seen:
                print(f"  • [{r.glyph()}] {r.name}: {r.fix_hint}")
                seen.add(r.fix_hint)
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="BigQuery capabilities probe — discover what the SA can extract",
    )
    parser.add_argument("--project", default="axp-lumi",
                        help="Data project (where the table lives)")
    parser.add_argument("--dataset", default="dw")
    parser.add_argument("--table", default="custins_customer_insights_cardmember")
    parser.add_argument("--out", default=None,
                        help="Optional JSON file to write full results")
    parser.add_argument("--verbose", action="store_true",
                        help="Print full data per probe, not just summaries")
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    billing_project = os.getenv("BQ_PROJECT_ID", "").strip() or args.project
    base_url = os.getenv("BIGQUERY_API_BASE_URL", DEFAULT_BASE_URL).strip().rstrip("/")
    region = os.getenv("BQ_REGION", "us").strip().lstrip("region-")

    if not sa_path:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS env var must point at SA JSON")
        return 1
    if not Path(sa_path).exists():
        print(f"ERROR: SA JSON not found at {sa_path}")
        return 1

    target = f"{args.project}.{args.dataset}.{args.table}"
    print()
    print("─" * 80)
    print("  BigQuery capabilities probe")
    print("─" * 80)
    print(f"  target:         {target}")
    print(f"  billing proj:   {billing_project}")
    print(f"  region:         {region}")
    print(f"  endpoint:       {base_url}")
    print(f"  SA JSON:        {sa_path}")
    print()
    print("  Running 15 probes...")
    print()

    client = BQClient(
        project=billing_project, sa_path=sa_path, base_url=base_url, region=region,
    )
    results = run_all_probes(client, args.project, args.dataset, args.table, region)
    print_capability_matrix(results, client.sa_email, client.sa_project, target)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(
            json.dumps(
                {
                    "target": target,
                    "sa_email": client.sa_email,
                    "sa_project": client.sa_project,
                    "billing_project": billing_project,
                    "region": region,
                    "endpoint": base_url,
                    "probes": [
                        {
                            "name": r.name, "category": r.category, "status": r.status,
                            "summary": r.summary, "error": r.error,
                            "fix_hint": r.fix_hint,
                            "data": r.data if args.verbose else None,
                        }
                        for r in results
                    ],
                },
                indent=2, default=str,
            ),
            encoding="utf-8",
        )
        print(f"  Full results written to {out_path.resolve()}")

    # Exit code reflects worst probe
    if any(r.status == "fail" for r in results):
        return 2  # blockers present
    if any(r.status == "warn" for r in results):
        return 1  # partial
    return 0


if __name__ == "__main__":
    sys.exit(main())
