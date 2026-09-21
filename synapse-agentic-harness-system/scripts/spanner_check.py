#!/usr/bin/env python3
"""Spanner connectivity and schema check — prove the plane BEFORE
wiring the chat store to it.

    python scripts/spanner_check.py                # connect, list tables
    python scripts/spanner_check.py --counts       # with row counts
    python scripts/spanner_check.py --sql "SELECT 1"
    python scripts/spanner_check.py --json --out docs/evals/spanner_state.json

Runs the same bootstrap the store will: .env → validate the contract
→ resolve the endpoint → the route (direct, pinned on the connection)
→ OAuth token → the database's state and DDL → INFORMATION_SCHEMA
for tables, columns and indexes → the diff against the designed DDL
under db/spanner. Prints the resolved configuration (never secrets).
Exit 0 = connected · 3 = env/auth problem · 1 = a request failed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import AuthError                       # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH, EXIT_GATE_FAILURE, EXIT_OK  # noqa: E402
from sahs.util.spanner import (SpannerClient, SpannerConnection,   # noqa: E402
                               SpannerError, format_report, inspect)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="spanner_check.py")
    parser.add_argument("--counts", action="store_true",
                        help="row count per table (one query each)")
    parser.add_argument("--sql", default="",
                        help="run one read query and print its rows")
    parser.add_argument("--no-design", action="store_true",
                        help="skip the diff against db/spanner")
    parser.add_argument("--json", action="store_true", dest="json_out")
    parser.add_argument("--out", default="",
                        help="write the report as JSON to this path")
    args = parser.parse_args(argv)

    try:
        connection = SpannerConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  the contract, in synapse-agentic-harness-system/.env:\n"
              "    SPANNER_PROJECT_ID=<project>\n"
              "    SPANNER_INSTANCE_ID=<instance>\n"
              "    SPANNER_DATABASE_ID=<database>\n"
              "    SPANNER_URL=https://<private endpoint>   # optional\n"
              "    SYNAPSE_SPANNER_SA_KEY=/path/to/key.json",
              file=sys.stderr)
        return EXIT_ENV_AUTH

    client = SpannerClient(connection)
    try:
        connection.token()
    except Exception as e:                                 # noqa: BLE001
        print(f"✗ token: {type(e).__name__}: {e}", file=sys.stderr)
        print(f"  route {connection.route()} to {connection.endpoint}; "
              "SPANNER_FORCE_PROXY=1 rides the corporate proxy instead",
              file=sys.stderr)
        return EXIT_ENV_AUTH

    try:
        if args.sql:
            columns, rows = client.query(args.sql)
            print("\t".join(columns))
            for row in rows:
                print("\t".join(str(v) for v in row))
            return EXIT_OK
        report = inspect(client, ddl_dir=None if args.no_design
                         else SILO / "db" / "spanner", counts=args.counts)
    except SpannerError as e:
        print(f"✗ {e}", file=sys.stderr)
        return EXIT_GATE_FAILURE

    if args.json_out:
        print(json.dumps(report, indent=1))
    else:
        print(format_report(report))
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=1) + "\n",
                       encoding="utf-8")
        print(f"→ {out}", file=sys.stderr)
    return EXIT_GATE_FAILURE if report["errors"] and not report["tables"] \
        else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
