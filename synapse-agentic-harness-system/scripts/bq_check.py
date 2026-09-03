#!/usr/bin/env python3
"""BQ connectivity check — prove the laptop can dry-run BEFORE P1.

    python scripts/bq_check.py [--sql "SELECT 1"] [--table dw.gms_transaction]

``--table`` dry-runs ONE known table the way the sandbox will — qualified
with the data project (LUMI_BQ_DATA_PROJECT, e.g. axp-lumi) — so a
"Not found: Table <query-project>:dw.x" surprise is caught here, not
in a chat.

Runs the exact bootstrap the substrate uses (the proven bq_connect
contract): .env → validate key on disk → resolve endpoint → NO_PROXY
injection → SSL settings → OAuth token → ONE dry-run. Prints the
resolved configuration (never secrets) and the dry-run outcome.
Exit 0 = connected · 3 = env/auth problem · 1 = dry-run refused.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs.util.auth import AuthError, BQConnection        # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH               # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bq_check.py")
    parser.add_argument("--sql", default="SELECT 1")
    parser.add_argument("--table", default="",
                        help="dataset.table that must resolve, e.g. "
                             "dw.gms_transaction")
    args = parser.parse_args(argv)

    try:
        connection = BQConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  put the three variables in "
              "synapse-agentic-harness-system/.env:", file=sys.stderr)
        print("    GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json\n"
              "    BQ_PROJECT_ID=<billing project>\n"
              "    BIGQUERY_URL=https://bigquery-prod.p.googleapis.com",
              file=sys.stderr)
        return EXIT_ENV_AUTH

    import os
    print("resolved configuration:")
    print(f"  project    {connection.project}   (runs and bills the query)")
    print(f"  data proj  {connection.data_project}"
          + ("   (same project hosts the tables; set "
             "LUMI_BQ_DATA_PROJECT if they live elsewhere)"
             if connection.data_project == connection.project
             else "   (hosts the tables: dataset.table is qualified "
                  "with it)"))
    print(f"  endpoint   {connection.endpoint}")
    print(f"  location   {connection.location}")
    print(f"  key file   {connection.key_path} (exists)")
    print(f"  route      {connection.route()} — pinned on this "
          "connection (the PSC contract); the environment's NO_PROXY "
          "is neither read nor written")
    if not connection.ssl_verify:
        print("  ⚠ TLS verification DISABLED (BQ_SSL_NO_VERIFY=1) — "
              "prefer REQUESTS_CA_BUNDLE with the corporate root cert "
              "when available")
    elif connection.ca_bundle:
        print(f"  CA bundle  {connection.ca_bundle}")
    else:
        print("  TLS        system default verification")

    from sahs.evals.substrate import BQDryRun
    print(f"\ndry-running: {args.sql}")
    outcome = BQDryRun(connection).dry_run(args.sql)
    if outcome.valid:
        print(f"✓ CONNECTED — dry-run valid · "
              f"bytes {outcome.bytes_processed:,} · result schema "
              f"{[c['name'] for c in outcome.result_schema or []]}")
        print("P1 is go: python scripts/run_evals.py --tasks "
              "graph/runs/p0_census/tasks/gold.jsonl --sut oracle "
              "--fail-under 1.0 --out graph/runs/p1_ground_oracle --json")
        if args.table:
            return _probe_table(connection, args.table)
        return 0
    print(f"✗ dry-run refused: {outcome.error}", file=sys.stderr)
    _hint(outcome.error)
    return 1


def _hint(error: str) -> None:
    if "Not found: Table" in (error or "") or "not found in location" \
            in (error or ""):
        print("  the table resolved against the wrong project or "
              "location: set LUMI_BQ_DATA_PROJECT to the project that "
              "HOSTS the tables (e.g. axp-lumi) and BQ_LOCATION to the "
              "dataset's location if it is regional, then rerun with "
              "--table", file=sys.stderr)
        return
    print("  (an auth/transport error here usually means proxy or TLS "
          "— try BQ_DISABLE_PROXY=1, or BQ_SSL_NO_VERIFY=1 as the "
          "last resort)", file=sys.stderr)


def _probe_table(connection, table: str) -> int:
    """Dry-run one table exactly as the sandbox qualifies it."""
    from sahs.evals.substrate import BQDryRun
    name = table.strip().replace("`", "")
    if name.count(".") == 1:
        name = f"{connection.data_project}.{name}"
    sql = f"SELECT 1 FROM `{name}` LIMIT 1"
    print(f"\nresolving: {sql}")
    outcome = BQDryRun(connection).dry_run(sql)
    if outcome.valid:
        print(f"✓ `{name}` resolves · bytes {outcome.bytes_processed:,}")
        return 0
    print(f"✗ `{name}` did not resolve: {outcome.error}", file=sys.stderr)
    _hint(outcome.error)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
