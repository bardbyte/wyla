#!/usr/bin/env python3
"""Vertex connectivity check — prove the laptop can generate BEFORE B1.

    python scripts/vertex_check.py             # config + token only
    python scripts/vertex_check.py --generate  # one tiny model call

Runs the same bootstrap the enricher uses: .env → validate key on
disk → resolve endpoint → NO_PROXY injection → SSL settings → OAuth
token → (optionally) ONE generateContent call. Prints the resolved
configuration (never secrets) and the outcome.
Exit 0 = connected · 3 = env/auth problem · 1 = call refused.

The Vertex SVC-ID and PROJECT are different from the BQ ones — this
check never borrows BQ settings, so a green bq_check proves nothing
here and vice versa.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs.util.auth import AuthError, VertexConnection    # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH               # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="vertex_check.py")
    parser.add_argument("--generate", action="store_true",
                        help="also make one tiny model call")
    args = parser.parse_args(argv)

    try:
        connection = VertexConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  put the Vertex variables in "
              "synapse-agentic-harness-system/.env (alongside the BQ "
              "ones — these are SEPARATE):", file=sys.stderr)
        print("    LUMI_VERTEX_SA_KEY=/path/to/vertex-key.json\n"
              "    VERTEX_PROJECT_ID=<the Vertex project — NOT the BQ "
              "one>\n"
              "    VERTEX_LOCATION=us-central1\n"
              "    VERTEX_MODEL=<the model id your project serves>\n"
              "    VERTEX_API_BASE_URL=<PSC endpoint if applicable>",
              file=sys.stderr)
        return EXIT_ENV_AUTH

    print("resolved configuration:")
    print(f"  project    {connection.project}")
    print(f"  location   {connection.location}")
    print(f"  model      {connection.model}")
    print(f"  endpoint   {connection.endpoint}")
    print(f"  key file   {connection.key_path} (exists)")
    print(f"  NO_PROXY   {os.environ.get('NO_PROXY', '(none)')}")
    if not connection.ssl_verify:
        print("  ⚠ TLS verification DISABLED (BQ_SSL_NO_VERIFY=1) — "
              "prefer REQUESTS_CA_BUNDLE with the corporate root cert "
              "when available")
    elif connection.ca_bundle:
        print(f"  CA bundle  {connection.ca_bundle}")
    else:
        print("  TLS        system default verification")

    from sahs.enrich.client import EnrichTransportError, VertexClient
    client = VertexClient(connection)
    print("\nfetching OAuth token…")
    try:
        token = client._token()
        print(f"✓ token acquired ({len(token)} chars)")
    except Exception as e:
        print(f"✗ token refresh failed: {e}", file=sys.stderr)
        print("  (proxy or TLS, usually — try BQ_DISABLE_PROXY=1, or "
              "BQ_SSL_NO_VERIFY=1 as the last resort)", file=sys.stderr)
        return EXIT_ENV_AUTH

    if not args.generate:
        print("token-only check passed — add --generate for one tiny "
              "model call")
        return 0
    print(f"generating (1 call, {connection.model})…")
    try:
        text = client.generate(
            'Return exactly this JSON: {"ok": true}',
            max_output_tokens=32)
    except EnrichTransportError as e:
        print(f"✗ generate refused: {e}", file=sys.stderr)
        print("  (a 404 here usually means the model id or location "
              "is wrong for this project; a 403 means the SVC-ID "
              "lacks aiplatform.endpoints.predict)", file=sys.stderr)
        return 1
    print(f"✓ CONNECTED — model answered: {text.strip()[:80]}")
    print(f"  usage: {client.usage}")
    print("B1 is go: python scripts/laptop.py enrich --graph graph "
          "--builds builds --plan --out graph/runs/b1_plan --plain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
