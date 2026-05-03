#!/usr/bin/env python3
"""Probe the MDM API for every table referenced by the gold queries.

The set of tables is DERIVED from the SQL files — we don't hardcode 6 (or
30, or 137). Whatever sits in `data/gold_queries/*.sql` (the 10 fixtures
today, 129 production queries tomorrow) determines what we fetch. Same script
works for any corpus size.

Usage (on Saheb's work laptop, on VPN):

    python scripts/probe_mdm.py                              # all tables in data/gold_queries/
    python scripts/probe_mdm.py --from-sqls path/to/sqls/    # different dir
    python scripts/probe_mdm.py --table cornerstone_metrics  # single table override
    python scripts/probe_mdm.py --list                       # just list discovered tables
    python scripts/probe_mdm.py --save data/mdm_cache/       # save digested per-table
    python scripts/probe_mdm.py --raw --table X --save f.json  # save raw response

Saves digested + raw responses under `data/mdm_cache/` so Session 1's
`discover_tables()` can use real MDM shapes without re-fetching.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# From parent CLAUDE.md (verified via earlier probe runs):
DEFAULT_ENDPOINT = (
    "https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas"
)
DEFAULT_GOLD_QUERIES_DIR = "data/gold_queries"
DEFAULT_TIMEOUT_SECS = 30


# ─── Discover tables from the SQL corpus ─────────────────────


def discover_tables_from_sqls(sql_dir: Path) -> list[str]:
    """Parse every .sql under sql_dir, return sorted unique table set.

    Uses our own parser (lumi.sql_to_context) so the table-discovery logic
    stays in one place — same code path that the production pipeline uses.
    Includes both top-level FROM tables and CTE source tables.
    """
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        return []

    # Make `lumi` importable when this script is run from the repo root.
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from lumi.sql_to_context import parse_sqls  # noqa: E402

    sqls = [f.read_text(encoding="utf-8") for f in sql_files]
    fps = parse_sqls(sqls)

    tables: set[str] = set()
    parse_failures = 0
    for fp in fps:
        if fp.parse_error:
            parse_failures += 1
            continue
        tables.update(fp.tables)
        for cte in fp.ctes:
            tables.update(cte.get("source_tables") or [])

    if parse_failures:
        print(
            f"WARN: {parse_failures}/{len(sql_files)} SQL files failed to parse",
            file=sys.stderr,
        )
    return sorted(tables)


# ─── HTTP fetch + digest ─────────────────────────────────────


def fetch_mdm(table: str, endpoint: str = DEFAULT_ENDPOINT) -> dict[str, Any] | list:
    """GET {endpoint}?tableName=<table>. Returns parsed JSON or raises."""
    qs = urllib.parse.urlencode({"tableName": table})
    url = f"{endpoint}?{qs}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
        body = resp.read()
        return json.loads(body)


def digest(payload: list | dict) -> dict[str, Any]:
    """Pull the fields we actually use into a flat per-column structure.

    MDM response is array[1] (verified). Columns at [0].schema.schema_attributes;
    meaty info under each col's attribute_details.
    """
    if not isinstance(payload, list) or not payload:
        return {
            "_error": "expected non-empty list at top level",
            "_raw_type": type(payload).__name__,
        }

    data = payload[0]
    schema = data.get("schema", {})
    cols = schema.get("schema_attributes") or []
    dataset = data.get("dataset_details", {})
    source = data.get("dataset_source_details", {})

    columns = []
    for col in cols:
        attr = col.get("attribute_details", {}) or {}
        sens = col.get("sensitivity_details", {}) or {}
        columns.append(
            {
                "name": attr.get("attribute_name") or col.get("attribute_name"),
                "business_name": attr.get("business_name"),
                "type": attr.get("attribute_type"),
                "description": attr.get("attribute_desc"),
                "is_partitioned": attr.get("is_partitioned"),
                "is_pii": sens.get("is_pii"),
                "is_gdpr": sens.get("is_gdpr"),
            }
        )

    described = sum(1 for c in columns if c["description"])
    coverage_pct = round(described / max(len(columns), 1), 3)

    return {
        "table_name": data.get("display_name"),
        "table_business_name": dataset.get("business_name"),
        "table_description": dataset.get("data_desc"),
        "data_category": dataset.get("data_category"),
        "storage_type": data.get("storage_type"),
        "load_type": data.get("load_type"),
        "bq_project": source.get("project_id"),
        "bq_dataset": source.get("dataset_name"),
        "bq_table": source.get("table_name"),
        "column_count": len(columns),
        "mdm_coverage_pct": coverage_pct,
        "columns": columns,
    }


# ─── CLI ─────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_mdm")
    parser.add_argument(
        "--from-sqls",
        default=DEFAULT_GOLD_QUERIES_DIR,
        help=f"Directory of .sql files. Default: {DEFAULT_GOLD_QUERIES_DIR}/",
    )
    parser.add_argument(
        "--table",
        help="Single table — overrides --from-sqls discovery",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Just list the tables discovered from --from-sqls and exit",
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument(
        "--raw", action="store_true", help="Print/save the full raw JSON"
    )
    parser.add_argument(
        "--save",
        help="Save to PATH (json). With multiple tables, treats PATH as a directory.",
    )
    args = parser.parse_args()

    # Resolve table set
    if args.table:
        tables = [args.table]
    else:
        sql_dir = Path(args.from_sqls)
        if not sql_dir.exists():
            print(f"ERROR: {sql_dir} does not exist", file=sys.stderr)
            return 2
        tables = discover_tables_from_sqls(sql_dir)
        if not tables:
            print(
                f"ERROR: no .sql files under {sql_dir} (or all failed to parse)",
                file=sys.stderr,
            )
            return 2

    # --list mode just prints and exits
    if args.list:
        print(f"# Discovered {len(tables)} unique tables across {args.from_sqls}/")
        for t in tables:
            print(t)
        return 0

    print(f"# Probing MDM for {len(tables)} tables (from {args.from_sqls}/)\n")

    save_path = Path(args.save) if args.save else None
    if save_path and len(tables) > 1:
        save_path.mkdir(parents=True, exist_ok=True)

    failures: list[str] = []
    for t in tables:
        try:
            payload = fetch_mdm(t, args.endpoint)
        except urllib.error.HTTPError as e:
            failures.append(f"{t}: HTTP {e.code} {e.reason}")
            print(f"[{t}] FAIL — HTTP {e.code} {e.reason}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            failures.append(f"{t}: connection — {e.reason}")
            print(f"[{t}] FAIL — connection {e.reason}", file=sys.stderr)
            continue
        except Exception as e:
            failures.append(f"{t}: {type(e).__name__} {e}")
            print(f"[{t}] FAIL — {type(e).__name__} {e}", file=sys.stderr)
            continue

        d = digest(payload)
        cov = d.get("mdm_coverage_pct", 0.0)
        cnt = d.get("column_count", 0)
        bq = f"{d.get('bq_dataset')}.{d.get('bq_table')}" if d.get("bq_table") else "?"
        print(
            f"[{t}] OK — {cnt} cols, {cov:.0%} described, BQ table = {bq}"
        )

        if save_path:
            target = save_path if (args.table and save_path.suffix == ".json") else (
                save_path / f"{t}.json" if save_path.is_dir() or len(tables) > 1 else save_path
            )
            payload_to_save = payload if args.raw else d
            target.write_text(
                json.dumps(payload_to_save, indent=2), encoding="utf-8"
            )
            print(f"   saved → {target}")
        elif args.raw:
            print(json.dumps(payload, indent=2))

    print(f"\nDone — {len(tables) - len(failures)}/{len(tables)} tables fetched.")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
