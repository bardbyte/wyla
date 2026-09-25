#!/usr/bin/env python3
"""Spanner connectivity and schema check — prove the plane BEFORE
wiring the chat store to it.

    python scripts/spanner_check.py                # connect, list tables
    python scripts/spanner_check.py --counts       # with row counts
    python scripts/spanner_check.py --sql "SELECT 1"
    python scripts/spanner_check.py --json --out docs/evals/spanner_state.json
    python scripts/spanner_check.py --emit-ddl   # CREATE TABLE for every live
                                                 # table the repo's DDL lacks

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
                               SpannerError, designed_tables, format_report,
                               inspect)

_KEYS_SQL = ("SELECT TABLE_NAME, COLUMN_NAME FROM "
             "INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_SCHEMA = '' "
             "AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY TABLE_NAME, "
             "ORDINAL_POSITION")
_NULLABLE_SQL = ("SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM "
                 "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''")


def emit_ddl(report: dict, *, designed: set[str] | None = None,
             keys: dict[str, list[str]] | None = None,
             nullable: dict[tuple[str, str], bool] | None = None) -> str:
    """CREATE TABLE statements for the live tables the repo's DDL lacks,
    from the schema listing ``inspect`` fetched: each column with the
    type INFORMATION_SCHEMA reports (already GoogleSQL: STRING(64),
    BYTES(MAX), ARRAY<STRING(64)>), NOT NULL where the catalog says so,
    the primary key when ``keys`` carries it (a ``?`` to fill in when it
    does not), and the interleave when the table has a parent. Parents
    come before children so the text applies as one batch. It is a
    starting point to check in under db/spanner, not a substitute for
    reading it: defaults, constraints, indexes and commit-timestamp
    options are not in the listing."""
    live = {t["name"]: t for t in report.get("tables", [])}
    if designed is None:
        known = report.get("designed") or {}
        designed = set(known.get("present", [])) | set(known.get("missing", []))
    missing = [name for name in live if name not in designed]
    if not missing:
        return "-- every live table is in db/spanner\n"
    # parents first: a child interleaves in a table defined earlier
    ordered: list[str] = []

    def add(name: str) -> None:
        if name in ordered or name not in live:
            return
        parent = live[name].get("parent")
        if parent and parent in missing:
            add(parent)
        ordered.append(name)

    for name in missing:
        add(name)
    out: list[str] = []
    for name in ordered:
        table = live[name]
        columns = table.get("columns") or []
        width = max((len(str(c[0])) for c in columns), default=0)
        lines = [f"-- {name}: in the live database, not in db/spanner",
                 f"CREATE TABLE {name} ("]
        for column in columns:
            cname = str(column[0])
            ctype = str(column[1]) if len(column) > 1 and column[1] else "STRING(MAX)"
            required = (nullable or {}).get((name, cname), True) is False
            lines.append(f"  {cname:<{width}}  {ctype}"
                         + ("  NOT NULL" if required else "") + ",")
        key = (keys or {}).get(name) or []
        tail = f") PRIMARY KEY ({', '.join(key) if key else '?'})"
        parent = table.get("parent")
        if parent:
            tail += f",\n  INTERLEAVE IN PARENT {parent} ON DELETE CASCADE"
        lines.append(tail + ";")
        out.append("\n".join(lines))
    return "\n\n".join(out) + "\n"


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
    parser.add_argument("--emit-ddl", action="store_true",
                        help="print CREATE TABLE statements for the live "
                             "tables db/spanner lacks, and nothing else")
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
        if args.emit_ddl:
            _, key_rows = client.query(_KEYS_SQL)
            keys: dict[str, list[str]] = {}
            for table, column in key_rows:
                keys.setdefault(str(table), []).append(str(column))
            _, null_rows = client.query(_NULLABLE_SQL)
            nullable = {(str(t), str(c)): str(n).upper() == "YES"
                        for t, c, n in null_rows}
            print(emit_ddl(report, designed=set(designed_tables(
                SILO / "db" / "spanner")), keys=keys, nullable=nullable),
                end="")
            return EXIT_OK
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
