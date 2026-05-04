#!/usr/bin/env python3
"""Probe BigQuery access with the Vertex service-account credentials.

Verifies three things our enrichment pipeline needs from BigQuery:
  1. The SA can authenticate against the BQ API at all (basic IAM check).
  2. INFORMATION_SCHEMA.COLUMNS is readable for a given table — that's
     where we'll pull authoritative types, nullability, and partition info.
  3. SELECT DISTINCT works on low-cardinality columns — that's how we get
     the allowed_values that MDM doesn't carry.

The same SA JSON used for Vertex (`prj-d-ea-poc`) is what we point at
the BQ API. A `roles/bigquery.dataViewer` + `roles/bigquery.jobUser` grant
on the target project is enough — we do not need write access.

Usage (on Saheb's work laptop, on VPN):

    export GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/key.json

    # Default probe — pulls schema + samples DISTINCT on a known-good table
    python scripts/check_bq_access.py \\
        --project axp-lumi --dataset dw \\
        --table custins_customer_insights_cardmember

    # Multi-table probe (run after run_session1.py, picks discovered tables)
    python scripts/check_bq_access.py --from-session1 data/session1_output.json \\
        --project axp-lumi --dataset dw

    # Just the auth check, don't run any queries
    python scripts/check_bq_access.py --auth-only

    # Save per-table digests for the enrichment pipeline to consume
    python scripts/check_bq_access.py --table foo --save data/bq_cache/

Exit codes:
    0  all probes succeeded
    1  query / permission failure (printed diagnosis)
    2  auth or setup error (no key file, library missing, etc.)

Tool-ready: the public functions return the standard {status, ..., error}
dict and can be lifted into an ADK tool wrapper later.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# --- Corporate-network TLS handling -------------------------------------- #
# Same pattern as check_vertex_gemini.py — the BQ HTTP client also goes
# through the corporate proxy and needs the company root CA. truststore
# pulls it from macOS Keychain where the corporate root is pre-installed.
try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
    _TRUSTSTORE_LOADED = True
except ImportError:
    _TRUSTSTORE_LOADED = False
# ------------------------------------------------------------------------- #

try:
    from google.api_core import exceptions as gcp_exceptions
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError as e:
    print(
        f"ERROR: missing dep — {e}. Install with:\n"
        "  pip install google-cloud-bigquery truststore",
        file=sys.stderr,
    )
    sys.exit(2)

logger = logging.getLogger("check_bq_access")

DEFAULT_PROJECT = "axp-lumi"
DEFAULT_DATASET = "dw"
# Tables small enough to enumerate distinct values without scanning TBs.
# Strings + bool + small int. We skip wide types (numeric/float/timestamp).
LOW_CARD_TYPES = {"STRING", "BOOL", "BOOLEAN", "INT64", "INTEGER"}
# Ceiling for "low cardinality" — anything above this is too wide to be a
# useful enum and we just record the cap was hit.
DISTINCT_LIMIT = 50
# Hard byte ceiling on probe queries so a misconfigured probe can't run up
# a multi-TB bill. 100 MiB is enough for INFORMATION_SCHEMA + a SELECT
# DISTINCT on a partitioned column.
MAX_BYTES_BILLED = 100 * 1024 * 1024


@dataclass
class ColumnDigest:
    column_name: str
    data_type: str
    is_nullable: bool
    is_partitioning: bool
    distinct_values: list[str] | None = None
    distinct_truncated: bool = False
    distinct_error: str | None = None


@dataclass
class TableDigest:
    project: str
    dataset: str
    table: str
    fully_qualified: str
    column_count: int
    columns: list[ColumnDigest] = field(default_factory=list)
    error: str | None = None


# ─── Auth ────────────────────────────────────────────────────


def _resolve_key_path(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not env:
        raise SystemExit(
            "ERROR: no service-account key. Set GOOGLE_APPLICATION_CREDENTIALS "
            "or pass --key-file /path/to/key.json"
        )
    return Path(env).expanduser()


def _build_client(project: str, key_path: Path) -> bigquery.Client:
    """Build a BQ client from the SA JSON. Uses the BQ scope explicitly so
    we get a clear permission error if the SA doesn't carry the BQ role.
    """
    if not key_path.exists():
        raise SystemExit(f"ERROR: key file not found: {key_path}")
    creds = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
    )
    return bigquery.Client(project=project, credentials=creds)


# ─── Probes ──────────────────────────────────────────────────


def probe_auth(client: bigquery.Client) -> dict[str, Any]:
    """List datasets in the project. Cheapest call that proves the SA can
    talk to the BQ API and has at least dataViewer on something.
    """
    try:
        # max_results=1 — we only need the call to succeed, not the data.
        datasets = list(client.list_datasets(max_results=5))
        return {
            "status": "ok",
            "project": client.project,
            "datasets_visible": [d.dataset_id for d in datasets],
            "error": None,
        }
    except gcp_exceptions.Forbidden as e:
        return {"status": "error", "error": f"Forbidden — SA lacks BQ access: {e}"}
    except gcp_exceptions.GoogleAPICallError as e:
        return {"status": "error", "error": f"BQ API error: {e}"}
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


def probe_information_schema(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Read INFORMATION_SCHEMA.COLUMNS for one table. Returns (rows, error)."""
    sql = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            is_partitioning_column
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
        ORDER BY ordinal_position
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table),
        ],
        maximum_bytes_billed=MAX_BYTES_BILLED,
    )
    try:
        job = client.query(sql, job_config=cfg)
        rows = [dict(r) for r in job.result()]
        return rows, None
    except gcp_exceptions.NotFound as e:
        return [], f"Dataset/table not found: {e}"
    except gcp_exceptions.Forbidden as e:
        return [], f"Forbidden on INFORMATION_SCHEMA: {e}"
    except gcp_exceptions.GoogleAPICallError as e:
        return [], f"BQ API error: {e}"


def probe_distinct_values(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    column: str,
) -> tuple[list[str] | None, bool, str | None]:
    """SELECT DISTINCT <col> ... LIMIT N+1. Returns (values, truncated, error).

    LIMIT N+1 lets us tell "exactly N distinct values" from "more than N
    so we capped" without paying for the full scan after.
    """
    # Identifier-safe quoting — column names from INFORMATION_SCHEMA can
    # contain reserved words. Use backticks.
    sql = f"""
        SELECT DISTINCT `{column}` AS v
        FROM `{project}.{dataset}.{table}`
        WHERE `{column}` IS NOT NULL
        LIMIT {DISTINCT_LIMIT + 1}
    """
    cfg = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED,
        use_query_cache=True,
    )
    try:
        job = client.query(sql, job_config=cfg)
        values = [str(r["v"]) for r in job.result()]
        truncated = len(values) > DISTINCT_LIMIT
        if truncated:
            values = values[:DISTINCT_LIMIT]
        return values, truncated, None
    except gcp_exceptions.Forbidden as e:
        return None, False, f"Forbidden: {e}"
    except gcp_exceptions.BadRequest as e:
        # Most common cause: hit MAX_BYTES_BILLED — column isn't on a
        # partition or has too many rows to scan within the cap.
        return None, False, f"BadRequest (likely byte cap hit): {e}"
    except gcp_exceptions.GoogleAPICallError as e:
        return None, False, f"BQ API error: {e}"


def digest_one_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    skip_distinct: bool = False,
) -> TableDigest:
    """Schema + (optionally) DISTINCT samples for low-cardinality columns."""
    fq = f"{project}.{dataset}.{table}"
    digest = TableDigest(
        project=project, dataset=dataset, table=table,
        fully_qualified=fq, column_count=0,
    )

    rows, err = probe_information_schema(client, project, dataset, table)
    if err:
        digest.error = err
        return digest
    if not rows:
        digest.error = f"INFORMATION_SCHEMA returned 0 rows for table='{table}'"
        return digest

    digest.column_count = len(rows)
    for row in rows:
        col = ColumnDigest(
            column_name=row["column_name"],
            data_type=row["data_type"],
            is_nullable=(row["is_nullable"] == "YES"),
            is_partitioning=(row["is_partitioning_column"] == "YES"),
        )
        if (
            not skip_distinct
            and col.data_type.upper() in LOW_CARD_TYPES
            # Skip obvious wide-id columns — they're never useful as enums.
            and not col.column_name.lower().endswith(("_id", "_uuid", "_xref_id"))
        ):
            values, truncated, derr = probe_distinct_values(
                client, project, dataset, table, col.column_name
            )
            col.distinct_values = values
            col.distinct_truncated = truncated
            col.distinct_error = derr
        digest.columns.append(col)

    return digest


# ─── Output ──────────────────────────────────────────────────


def _print_digest(d: TableDigest) -> None:
    print()
    print("=" * 78)
    print(f"  {d.fully_qualified}")
    print("=" * 78)
    if d.error:
        print(f"  ERROR: {d.error}")
        return

    print(f"  {d.column_count} columns")
    print()
    print(f"  {'COLUMN':<40} {'TYPE':<14} {'NULL':<5} {'PART':<5} {'DISTINCT':<8}")
    print("  " + "-" * 76)
    for c in d.columns:
        nul = "Y" if c.is_nullable else "N"
        part = "Y" if c.is_partitioning else "—"
        if c.distinct_error:
            distinct_summary = f"err({c.distinct_error[:18]})"
        elif c.distinct_values is None:
            distinct_summary = "—"
        else:
            n = len(c.distinct_values)
            distinct_summary = f"{n}{'+' if c.distinct_truncated else ''}"
        print(
            f"  {c.column_name[:39]:<40} "
            f"{c.data_type[:13]:<14} {nul:<5} {part:<5} {distinct_summary:<8}"
        )

    # Spotlight on low-cardinality enums (5 best examples) — most useful
    # for downstream LookML enrichment.
    enum_candidates = [
        c for c in d.columns
        if c.distinct_values and not c.distinct_truncated and 1 < len(c.distinct_values) <= 20
    ]
    if enum_candidates:
        print()
        print("  Enum-like columns (allowed_values for LookML):")
        for c in enum_candidates[:5]:
            sample = ", ".join(repr(v) for v in c.distinct_values[:8])
            more = " …" if len(c.distinct_values) > 8 else ""
            print(f"    {c.column_name}: [{sample}{more}]")


# ─── Driver ──────────────────────────────────────────────────


def _tables_from_session1(path: Path) -> list[str]:
    """Pull the discovered table list from `data/session1_output.json`."""
    if not path.exists():
        raise SystemExit(
            f"ERROR: {path} not found. Run `python scripts/run_session1.py` first."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    return sorted(data.keys())


def _save_digest(d: TableDigest, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{d.table}.json"
    target.write_text(
        json.dumps(asdict(d), indent=2, default=str), encoding="utf-8"
    )
    return target


def main() -> int:
    p = argparse.ArgumentParser(prog="check_bq_access")
    p.add_argument("--key-file", help="SA JSON; defaults to $GOOGLE_APPLICATION_CREDENTIALS")
    p.add_argument("--project", default=DEFAULT_PROJECT, help=f"BQ project (default: {DEFAULT_PROJECT})")
    p.add_argument("--dataset", default=DEFAULT_DATASET, help=f"BQ dataset (default: {DEFAULT_DATASET})")
    p.add_argument("--table", action="append", help="Specific table(s) to probe; repeat for many")
    p.add_argument(
        "--from-session1",
        help="Path to data/session1_output.json — probes every discovered table",
    )
    p.add_argument(
        "--auth-only",
        action="store_true",
        help="Just verify the SA can talk to BQ; skip schema + DISTINCT",
    )
    p.add_argument(
        "--skip-distinct",
        action="store_true",
        help="Pull schema only — no SELECT DISTINCT (cheaper, faster)",
    )
    p.add_argument(
        "--save",
        help="Directory to write per-table JSON digests (e.g. data/bq_cache/)",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS verification (only when truststore unavailable)",
    )
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    if args.insecure:
        # Last-resort bypass — same approach as check_vertex_gemini.py.
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
        os.environ["PYTHONHTTPSVERIFY"] = "0"
        print("WARN: TLS verification disabled (--insecure)", file=sys.stderr)
    else:
        print(
            f"truststore: {'loaded' if _TRUSTSTORE_LOADED else 'NOT loaded — pip install truststore'}",
            file=sys.stderr,
        )

    key_path = _resolve_key_path(args.key_file)
    print(f"Using credentials: {key_path}")
    print(f"Project:           {args.project}")
    print(f"Dataset:           {args.dataset}")
    print()

    client = _build_client(args.project, key_path)

    # Step 1: auth probe
    auth_result = probe_auth(client)
    if auth_result["status"] != "ok":
        print(f"AUTH FAIL: {auth_result['error']}", file=sys.stderr)
        return 1
    print(
        f"AUTH OK — SA can list datasets in {auth_result['project']} "
        f"({len(auth_result['datasets_visible'])} visible: "
        f"{auth_result['datasets_visible'][:3]}"
        f"{' …' if len(auth_result['datasets_visible']) >= 5 else ''})"
    )

    if args.auth_only:
        return 0

    # Step 2: assemble target table list
    targets: list[str] = []
    if args.table:
        targets.extend(args.table)
    if args.from_session1:
        targets.extend(_tables_from_session1(Path(args.from_session1)))
    if not targets:
        print(
            "\nNo tables specified. Pass --table NAME (repeat to add more) "
            "or --from-session1 data/session1_output.json",
            file=sys.stderr,
        )
        return 2

    # De-dup, preserve order
    seen: set[str] = set()
    targets = [t for t in targets if not (t in seen or seen.add(t))]

    save_dir = Path(args.save) if args.save else None
    failures = 0
    for t in targets:
        digest = digest_one_table(
            client, args.project, args.dataset, t,
            skip_distinct=args.skip_distinct,
        )
        _print_digest(digest)
        if digest.error:
            failures += 1
        if save_dir:
            target = _save_digest(digest, save_dir)
            print(f"  → wrote {target}")

    print()
    print("=" * 78)
    print(
        f"Summary: probed {len(targets)} table(s), "
        f"{len(targets) - failures} ok, {failures} failed"
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
