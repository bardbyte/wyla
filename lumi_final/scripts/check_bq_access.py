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
    # (--billing-project defaults to prj-d-lumi-gpt, --data-project to axp-lumi)
    python scripts/check_bq_access.py \\
        --table custins_customer_insights_cardmember

    # Multi-table probe (run after run_session1.py, picks discovered tables)
    python scripts/check_bq_access.py --from-session1 data/session1_output.json

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
import ssl
import sys
import warnings
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
    from google.api_core.client_options import ClientOptions
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


# ─── TLS / corporate-MITM helpers ────────────────────────────


def _set_ca_bundle(path: str) -> None:
    """Point every standard CA-bundle env var at `path`. Works for requests,
    httpx, urllib, and the gRPC layer that BQ uses for the storage API.

    Use this when behind a TLS-intercepting corporate proxy and you have the
    corporate root CA exported as a .pem file. Cleaner than --insecure.
    """
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"CA bundle not found: {p}")
    bundle_str = str(p)
    os.environ["REQUESTS_CA_BUNDLE"] = bundle_str
    os.environ["SSL_CERT_FILE"] = bundle_str
    os.environ["CURL_CA_BUNDLE"] = bundle_str
    # gRPC — google-cloud-bigquery uses gRPC for storage / streaming
    os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = bundle_str


def _disable_ssl_verification() -> None:
    """Disable SSL verification across stdlib, requests, httpx, and
    google-auth's AuthorizedSession (which is what google-cloud-bigquery
    uses for HTTP calls).

    Call this BEFORE building the BQ client — patches must be in place
    when AuthorizedSession is instantiated.
    """
    # stdlib ssl + env-var hint for child processes
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
    os.environ["PYTHONHTTPSVERIFY"] = "0"

    # Silence the InsecureRequestWarning spam from urllib3
    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass

    # google-auth's AuthorizedSession (a requests.Session subclass) — this is
    # what google-cloud-bigquery's HTTP transport uses. Patch __init__ in
    # place rather than subclassing — google-cloud-bigquery introspects the
    # class identity and a swapped subclass loses the original signature
    # ("AuthorizedSession is missing 1 required positional argument:
    # 'credentials'" on Client construction).
    try:
        import google.auth.transport.requests as gat

        _orig_init = gat.AuthorizedSession.__init__

        def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
            _orig_init(self, *args, **kwargs)
            self.verify = False

        gat.AuthorizedSession.__init__ = _patched_init  # type: ignore[method-assign]
    except ImportError:
        pass

    # requests.Session itself — covers any path that doesn't go through
    # AuthorizedSession (e.g. google-resumable-media's plain Session).
    try:
        import requests

        _orig_req_init = requests.Session.__init__

        def _patched_req_init(self: Any, *args: Any, **kwargs: Any) -> None:
            _orig_req_init(self, *args, **kwargs)
            self.verify = False

        requests.Session.__init__ = _patched_req_init  # type: ignore[method-assign]
    except ImportError:
        pass

    # httpx — newer google-cloud clients sometimes route through httpx as well
    try:
        import httpx

        _orig_client = httpx.Client
        _orig_async = httpx.AsyncClient

        def _client_no_verify(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("verify", False)
            return _orig_client(*args, **kwargs)

        def _async_no_verify(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("verify", False)
            return _orig_async(*args, **kwargs)

        httpx.Client = _client_no_verify  # type: ignore[misc]
        httpx.AsyncClient = _async_no_verify  # type: ignore[misc]
    except ImportError:
        pass

    warnings.warn(
        "SSL verification disabled — only safe on networks you already trust.",
        stacklevel=2,
    )

# BQ has TWO distinct project concepts:
#   - billing project  : pays for the query; SA needs roles/bigquery.jobUser
#   - data project     : where the tables physically live; SA needs
#                        roles/bigquery.dataViewer on the target dataset
# These are usually different. Our SA is grant'd jobUser on prj-d-lumi-gpt
# but the data lives under axp-lumi.dw.<table>. Don't conflate them.
DEFAULT_BILLING_PROJECT = "prj-d-lumi-gpt"
DEFAULT_DATA_PROJECT = "axp-lumi"
DEFAULT_DATASET = "dw"
# Amex routes BigQuery through Private Service Connect (PSC) — the public
# bigquery.googleapis.com endpoint isn't reachable from the corp network,
# but bigquery-dev.p.googleapis.com is. google-cloud-bigquery defaults to
# the public endpoint and gets 404; we have to override via client_options.
DEFAULT_API_ENDPOINT = "https://bigquery-dev.p.googleapis.com"
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
    """Resolve which SA JSON to use. Precedence: --key-file → $LUMI_BQ_KEY_FILE
    → $GOOGLE_APPLICATION_CREDENTIALS.

    LUMI_BQ_KEY_FILE lets BQ use a different SA than Vertex (which reads the
    standard GOOGLE_APPLICATION_CREDENTIALS via google-genai), so both can
    coexist in the same shell.
    """
    if explicit:
        return Path(explicit).expanduser()
    env = (
        os.environ.get("LUMI_BQ_KEY_FILE")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if not env:
        raise SystemExit(
            "ERROR: no service-account key. Set LUMI_BQ_KEY_FILE (preferred for "
            "BQ-only SA), or GOOGLE_APPLICATION_CREDENTIALS, or pass --key-file."
        )
    return Path(env).expanduser()


def _read_sa_project(key_path: Path) -> str | None:
    """Pull the project_id out of an SA JSON. The SA's home project is the
    most reliable default for billing — using anything else risks 404s on
    list_datasets when the project name is even slightly wrong.
    """
    try:
        data = json.loads(key_path.read_text(encoding="utf-8"))
        pid = data.get("project_id")
        return pid if isinstance(pid, str) and pid else None
    except (OSError, json.JSONDecodeError):
        return None


def _build_client(
    billing_project: str,
    key_path: Path,
    api_endpoint: str | None = None,
) -> bigquery.Client:
    """Build a BQ client from the SA JSON.

    Args:
        billing_project: project that pays for queries — SA needs jobUser here.
        key_path: SA JSON.
        api_endpoint: override BQ API URL (e.g. PSC endpoint on corp networks).
            None → google-cloud-bigquery's default (bigquery.googleapis.com).

    The data project (where tables live) is passed per-query via the
    fully-qualified `project.dataset.table` reference in the SQL.
    """
    if not key_path.exists():
        raise SystemExit(f"ERROR: key file not found: {key_path}")
    creds = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
    )
    client_options: ClientOptions | None = None
    if api_endpoint:
        client_options = ClientOptions(api_endpoint=api_endpoint)
    return bigquery.Client(
        project=billing_project,
        credentials=creds,
        client_options=client_options,
    )


# ─── Probes ──────────────────────────────────────────────────


def probe_auth(
    client: bigquery.Client, data_project: str
) -> dict[str, Any]:
    """Two-part auth probe:
      1. list_datasets on the BILLING project — proves SA can talk to BQ.
      2. list_datasets on the DATA project — proves SA has dataViewer where
         the tables actually live (different project, separate IAM grant).
    """
    out: dict[str, Any] = {
        "status": "ok",
        "billing_project": client.project,
        "data_project": data_project,
        "billing_datasets_visible": [],
        "data_datasets_visible": [],
        "error": None,
    }
    try:
        billing_ds = list(client.list_datasets(max_results=5))
        out["billing_datasets_visible"] = [d.dataset_id for d in billing_ds]
    except gcp_exceptions.NotFound as e:
        out["status"] = "error"
        out["error"] = (
            f"404 Not Found on billing project '{client.project}'. The project "
            f"name doesn't exist or the SA has zero visibility. Check the "
            f"`project_id` field in your SA JSON — that's almost always the "
            f"right billing project. Original: {e}"
        )
        return out
    except gcp_exceptions.Forbidden as e:
        out["status"] = "error"
        out["error"] = f"Forbidden listing billing project '{client.project}': {e}"
        return out
    except gcp_exceptions.GoogleAPICallError as e:
        out["status"] = "error"
        out["error"] = f"BQ API error on billing project: {e}"
        return out

    if data_project == client.project:
        # Same project for both — nothing more to probe.
        return out

    try:
        data_ds = list(client.list_datasets(project=data_project, max_results=5))
        out["data_datasets_visible"] = [d.dataset_id for d in data_ds]
    except gcp_exceptions.Forbidden as e:
        out["status"] = "warn"
        out["error"] = (
            f"Cannot list datasets in data project '{data_project}': {e}. "
            "SA may still have direct dataset-level grants — INFORMATION_SCHEMA "
            "probe will reveal the truth."
        )
    except gcp_exceptions.GoogleAPICallError as e:
        out["status"] = "warn"
        out["error"] = f"BQ API error on data project: {e}"
    return out


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
    p.add_argument("--key-file", help="SA JSON; defaults to $LUMI_BQ_KEY_FILE / $GOOGLE_APPLICATION_CREDENTIALS")
    p.add_argument(
        "--billing-project",
        default=None,
        help=(
            "Project that pays for queries (SA needs jobUser here). "
            "Default: $LUMI_BQ_BILLING_PROJECT, else the project_id from "
            f"your SA JSON, else '{DEFAULT_BILLING_PROJECT}'."
        ),
    )
    p.add_argument(
        "--data-project",
        default=DEFAULT_DATA_PROJECT,
        help=f"Project where tables live (SA needs dataViewer). Default: {DEFAULT_DATA_PROJECT}",
    )
    p.add_argument("--dataset", default=DEFAULT_DATASET, help=f"BQ dataset (default: {DEFAULT_DATASET})")
    p.add_argument(
        "--api-endpoint",
        default=os.environ.get("LUMI_BQ_API_ENDPOINT", DEFAULT_API_ENDPOINT),
        help=(
            "BQ API URL. Default: $LUMI_BQ_API_ENDPOINT or "
            f"'{DEFAULT_API_ENDPOINT}' (Amex PSC endpoint). Pass empty "
            "string to use Google's public default."
        ),
    )
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
        "--ca-bundle",
        help=(
            "Path to corporate root CA bundle (.pem). Sets REQUESTS_CA_BUNDLE / "
            "SSL_CERT_FILE / CURL_CA_BUNDLE / GRPC_DEFAULT_SSL_ROOTS_FILE_PATH. "
            "Use this when truststore can't see the corp root CA in Keychain."
        ),
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "Disable SSL verification across stdlib, requests, httpx, and "
            "google-auth. Last resort — only on networks you already trust."
        ),
    )
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    # TLS setup — order matters: bundle env vars first (so any subsequent
    # client picks them up), then insecure patches (must run BEFORE the BQ
    # client builds its AuthorizedSession).
    if args.ca_bundle:
        try:
            _set_ca_bundle(args.ca_bundle)
            print(f"CA bundle: {args.ca_bundle}", file=sys.stderr)
        except FileNotFoundError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 2
    if args.insecure:
        _disable_ssl_verification()
        print("WARN: TLS verification disabled (--insecure)", file=sys.stderr)
    if not args.insecure and not args.ca_bundle:
        print(
            f"truststore: {'loaded' if _TRUSTSTORE_LOADED else 'NOT loaded — pip install truststore'}",
            file=sys.stderr,
        )

    key_path = _resolve_key_path(args.key_file)
    if args.key_file:
        key_source = "--key-file"
    elif os.environ.get("LUMI_BQ_KEY_FILE"):
        key_source = "$LUMI_BQ_KEY_FILE"
    else:
        key_source = "$GOOGLE_APPLICATION_CREDENTIALS (fallback)"
    sa_project = _read_sa_project(key_path)
    # Resolve billing project: --billing-project > $LUMI_BQ_BILLING_PROJECT >
    # SA's own project_id > hardcoded default.
    billing_project = (
        args.billing_project
        or os.environ.get("LUMI_BQ_BILLING_PROJECT")
        or sa_project
        or DEFAULT_BILLING_PROJECT
    )
    if args.billing_project:
        bp_source = "--billing-project"
    elif os.environ.get("LUMI_BQ_BILLING_PROJECT"):
        bp_source = "$LUMI_BQ_BILLING_PROJECT"
    elif sa_project:
        bp_source = "SA JSON project_id"
    else:
        bp_source = "hardcoded default"

    print(f"Using credentials: {key_path}  ({key_source})")
    if sa_project:
        print(f"SA's home project: {sa_project}  (from SA JSON project_id)")
    print(f"Billing project:   {billing_project}  ({bp_source}; needs jobUser)")
    if sa_project and sa_project != billing_project:
        print(
            f"  ⚠  billing project differs from SA's home project — that's fine "
            f"if intended, but a 404 here usually means you should use "
            f"--billing-project {sa_project}"
        )
    print(f"Data project:      {args.data_project}  (where tables live; needs dataViewer)")
    print(f"Dataset:           {args.dataset}")
    api_endpoint = args.api_endpoint or None  # empty string → None → use default
    if api_endpoint:
        print(f"API endpoint:      {api_endpoint}  (override; PSC / corp routing)")
    else:
        print("API endpoint:      bigquery.googleapis.com  (Google default)")
    print()

    client = _build_client(billing_project, key_path, api_endpoint=api_endpoint)

    # Step 1: auth probe — covers BOTH billing and data project access.
    auth_result = probe_auth(client, args.data_project)
    if auth_result["status"] == "error":
        print(f"AUTH FAIL: {auth_result['error']}", file=sys.stderr)
        return 1

    bp_n = len(auth_result["billing_datasets_visible"])
    print(
        f"AUTH OK (billing) — SA can list datasets in {auth_result['billing_project']} "
        f"({bp_n} visible: {auth_result['billing_datasets_visible'][:3]}"
        f"{' …' if bp_n >= 5 else ''})"
    )
    if args.data_project != args.billing_project:
        if auth_result["status"] == "warn":
            print(f"AUTH WARN (data)  — {auth_result['error']}")
        else:
            dp_n = len(auth_result["data_datasets_visible"])
            print(
                f"AUTH OK (data)    — SA can list datasets in {auth_result['data_project']} "
                f"({dp_n} visible: {auth_result['data_datasets_visible'][:3]}"
                f"{' …' if dp_n >= 5 else ''})"
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
            client, args.data_project, args.dataset, t,
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
