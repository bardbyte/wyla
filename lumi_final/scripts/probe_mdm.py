#!/usr/bin/env python3
"""Probe the MDM API for the tables Q1-Q10 reference. No auth (intranet).

Usage (on Saheb's work laptop, on VPN):

    python scripts/probe_mdm.py                      # all 6 tables Q1-Q10 reference
    python scripts/probe_mdm.py --table cornerstone_metrics
    python scripts/probe_mdm.py --table cornerstone_metrics --raw  # full JSON dump
    python scripts/probe_mdm.py --table cornerstone_metrics \\
        --save data/mdm_cache/cornerstone_metrics.json

Saves digested + raw responses under `data/mdm_cache/` so Session 1 tests
can use real MDM shapes without re-fetching.

Pure-stdlib (urllib only) — runs from a fresh laptop with no pip installs.
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

# Tables Q1-Q10 reference (from tests/fixtures/sample_sqls.py).
DEFAULT_TABLES = [
    "cornerstone_metrics",
    "risk_pers_acct_history",
    "risk_indv_cust_hist",
    "drm_product_member",
    "drm_product_hier",
    "acquisitions",
]

DEFAULT_TIMEOUT_SECS = 30


def fetch_mdm(table: str, endpoint: str = DEFAULT_ENDPOINT) -> dict[str, Any]:
    """GET {endpoint}?tableName=<table>. Returns parsed JSON or raises."""
    qs = urllib.parse.urlencode({"tableName": table})
    url = f"{endpoint}?{qs}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
        body = resp.read()
        return json.loads(body)


def digest(payload: list | dict) -> dict[str, Any]:
    """Pull the fields we actually use into a flat per-column structure.

    MDM response is array[1] (verified). Columns live under
    [0].schema.schema_attributes; meaty info under attribute_details.
    """
    if not isinstance(payload, list) or not payload:
        return {"_error": "expected non-empty list at top level", "_raw_type": type(payload).__name__}

    data = payload[0]
    schema = data.get("schema", {})
    cols = schema.get("schema_attributes") or []
    dataset = data.get("dataset_details", {})
    source = data.get("dataset_source_details", {})

    columns = []
    for col in cols:
        attr = col.get("attribute_details", {}) or {}
        sens = col.get("sensitivity_details", {}) or {}
        columns.append({
            "name": attr.get("attribute_name") or col.get("attribute_name"),
            "business_name": attr.get("business_name"),
            "type": attr.get("attribute_type"),
            "description": attr.get("attribute_desc"),
            "is_partitioned": attr.get("is_partitioned"),
            "is_pii": sens.get("is_pii"),
            "is_gdpr": sens.get("is_gdpr"),
        })

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


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_mdm")
    parser.add_argument("--table", help="Single table; default: all 6 Q1-Q10 tables")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--raw", action="store_true", help="Print/save the full raw JSON")
    parser.add_argument(
        "--save",
        help="Save to PATH (json). With multiple tables, treats PATH as a directory.",
    )
    args = parser.parse_args()

    tables = [args.table] if args.table else DEFAULT_TABLES
    save_path = Path(args.save) if args.save else None

    if save_path and not args.table:
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
        print(f"[{t}] OK — {cnt} columns, {cov:.0%} have descriptions, "
              f"BQ table = {d.get('bq_dataset')}.{d.get('bq_table')}")

        if save_path:
            target = save_path if args.table else (save_path / f"{t}.json")
            payload_to_save = payload if args.raw else d
            target.write_text(json.dumps(payload_to_save, indent=2), encoding="utf-8")
            print(f"   saved → {target}")
        elif args.raw:
            print(json.dumps(payload, indent=2))

    print(f"\nDone — {len(tables) - len(failures)}/{len(tables)} tables fetched.")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
