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
              "ones — these are SEPARATE; the proven ADK laptop "
              "values):", file=sys.stderr)
        print("    LUMI_VERTEX_SA_KEY=~/.gcp/prj-d-ea-poc.json\n"
              "    VERTEX_PROJECT_ID=prj-d-ea-poc   # or your existing "
              "GOOGLE_CLOUD_PROJECT\n"
              "    # location defaults to 'global', model to "
              "gemini-3.1-pro-preview — the proven pair;\n"
              "    # override with VERTEX_LOCATION / VERTEX_MODEL "
              "(or GOOGLE_CLOUD_LOCATION / GEMINI_MODEL)",
              file=sys.stderr)
        return EXIT_ENV_AUTH

    print("resolved configuration:")
    print(f"  project    {connection.project}")
    print(f"  location   {connection.location}")
    print(f"  model      {connection.model}")
    print(f"  endpoint   {connection.endpoint}")
    print(f"  key file   {connection.key_path} (exists)")
    proxy = (os.environ.get("HTTPS_PROXY")
             or os.environ.get("https_proxy") or "")
    print("  proxy      "
          + (f"via {proxy} — the proven contract (token + model "
             "calls ride the corporate proxy)" if proxy
             else "none configured — direct"))
    print(f"  NO_PROXY   {os.environ.get('NO_PROXY', '(none)')}")
    trust_note = ("ACTIVE (OS keychain trust — the clean "
                  "corporate-TLS fix)" if connection.truststore_active
                  else "not installed (pip install truststore "
                  "recommended on the corporate network)")
    print(f"  truststore {trust_note}")
    if not connection.ssl_verify:
        print("  ⚠ TLS verification DISABLED (GEMINI_TLS_INSECURE / "
              "BQ_SSL_NO_VERIFY) — prefer truststore or "
              "GEMINI_CA_BUNDLE with the corporate root cert")
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
        print("  (proxy or TLS, usually. The default rides the "
              "corporate proxy — the proven contract. On a "
              "direct-egress network try VERTEX_DISABLE_PROXY=1; on a "
              "private-DNS/restricted-VIP network try "
              "VERTEX_NO_PROXY_GOOGLE=1; TLS last resort "
              "GEMINI_TLS_INSECURE=1)", file=sys.stderr)
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
