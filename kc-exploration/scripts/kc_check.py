#!/usr/bin/env python3
"""Catalog connectivity check: prove this machine can search BEFORE the
agent runs.

    python scripts/kc_check.py                        # config, token, one small search
    python scripts/kc_check.py --query "type=table" --page-size 5
    python scripts/kc_check.py --json

Runs the same bootstrap the agent uses: the .env files → the key on disk
→ endpoint and route → the OAuth token → ONE searchEntries call. Prints
the resolved configuration (never a secret) and the outcome, with the
remedy when the catalog refuses. Exit 0 = connected · 3 = env/auth
problem · 1 = the catalog refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kcx.catalog import CatalogClient, CatalogConnection, CatalogError  # noqa: E402
from kcx.env import (EXIT_CONFIG, EXIT_OK, EXIT_REFUSED, ConfigError,      # noqa: E402
                     load_env)
from kcx.transport import TransportError                                   # noqa: E402


def remedy(error: CatalogError, connection: CatalogConnection) -> str:
    """What to change, keyed on the machine reason when Google gave one."""
    reason = error.reason
    quota = connection.quota_project or connection.project
    if reason == "IAM_PERMISSION_DENIED" or (
            error.status == 403 and "dataplex.projects.search" in error.message):
        return (f"the key lacks dataplex.projects.search on {connection.project}: "
                "grant roles/dataplex.viewer to the service account there")
    if reason == "USER_PROJECT_DENIED":
        return (f"the key may not use {quota} as the quota project: grant "
                "roles/serviceusage.serviceUsageConsumer there, or set "
                "KC_QUOTA_PROJECT=none to bill the key's own project")
    if reason == "SERVICE_DISABLED":
        return ("the Knowledge Catalog API (dataplex.googleapis.com) is not "
                "enabled on the project the message names: enable it there")
    if error.status == 403:
        return ("a 403: check roles/dataplex.viewer on KC_PROJECT_ID and "
                "roles/serviceusage.serviceUsageConsumer on the quota project")
    if error.status == 404:
        return "a 404: KC_PROJECT_ID or KC_API_BASE_URL is wrong for this endpoint"
    if error.status == 0:
        return ("nothing answered: check the proxy and the network path "
                "(KC_DISABLE_PROXY=1 for direct egress), then TLS (pip install truststore, or "
                "KC_CA_BUNDLE with the root certificate)")
    return ""


def _files(report: dict) -> str:
    kc = report["kc"]
    text = f"{kc['path']} ({'read, ' + str(len(kc['loaded'])) + ' names' if kc['found'] else 'absent'})"
    extra = report.get("extra")
    if extra:
        text += (f"; extra {extra['path']} "
                 f"({'read, ' + str(len(extra['loaded'])) + ' names' if extra['found'] else 'absent'})")
    return text


def main(argv: list[str] | None = None, *, http=None, token=None) -> int:
    parser = argparse.ArgumentParser(prog="kc_check.py")
    parser.add_argument("--query", default="type=table",
                        help="the one search to run (default: type=table)")
    parser.add_argument("--page-size", type=int, default=3)
    parser.add_argument("--json", action="store_true",
                        help="print the outcome as JSON instead of prose")
    args = parser.parse_args(argv)
    report = load_env()
    outcome: dict = {"env_files": report}

    try:
        connection = CatalogConnection.from_env()
    except ConfigError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  put the catalog variables in this folder's .env (copy "
              ".env.example):\n    KC_PROJECT_ID=your-project\n"
              "    KC_SA_KEY=/absolute/path/to/catalog-sa-key.json",
              file=sys.stderr)
        return EXIT_CONFIG
    info = connection.describe()
    outcome["config"] = info
    if not args.json:
        print("resolved configuration:")
        for key, value in info.items():
            print(f"  {key:<15} {value}")
        print(f"  {'env files':<15} {_files(report)}")

    client = CatalogClient(connection, http=http, token=token)
    if not args.json:
        print("\nfetching OAuth token…")
    try:
        acquired = client.token()
    except TransportError as e:
        print(f"✗ token refresh failed: {e}", file=sys.stderr)
        print("  (proxy or TLS, usually: the default rides HTTPS_PROXY when "
              "one is set; KC_DISABLE_PROXY=1 goes direct; pip install "
              "truststore for an intercepting network)", file=sys.stderr)
        return EXIT_CONFIG
    if not args.json:
        print(f"✓ token acquired ({len(acquired)} chars)")
        print(f"searching ({args.query!r}, {args.page_size} rows)…")
    try:
        result = client.search(args.query, page_size=args.page_size)
    except CatalogError as e:
        print(f"✗ {e}", file=sys.stderr)
        fix = remedy(e, connection)
        if fix:
            print(f"  {fix}", file=sys.stderr)
        return EXIT_REFUSED
    except TransportError as e:
        print(f"✗ {e}", file=sys.stderr)
        return EXIT_REFUSED
    rows = result["results"]
    outcome["results"] = rows
    outcome["total_size"] = result["total_size"]
    outcome["unreachable"] = result["unreachable"]
    if args.json:
        print(json.dumps(outcome, indent=2, ensure_ascii=False))
        return EXIT_OK
    print(f"✓ CONNECTED — {len(rows)} of {result['total_size']} entries")
    for row in rows:
        print(f"  {row['system'] or '?':<10} "
              f"{row['display_name'] or row['entry_name']}")
    if result["unreachable"]:
        print(f"  unreachable: {', '.join(result['unreachable'])}")
    print('next: python scripts/explore.py "which tables hold customer '
          'transactions?"')
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
