#!/usr/bin/env python3
"""Inspect a service-account JSON for non-standard fields.

When OAuth token exchange fails with 404 at _token_endpoint_request,
the SA JSON usually has a non-standard `token_uri` — e.g. corporate IT
issued the SA with an internal OAuth endpoint instead of Google's
public one.

Usage:
    python lumi_final/scripts/inspect_sa_json.py ~/Downloads/bq-sa.json

    # Compare two SAs (e.g. Vertex vs BQ) side by side:
    python lumi_final/scripts/inspect_sa_json.py \\
        ~/Downloads/vertex-sa.json ~/Downloads/bq-sa.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Anything not in this list is non-standard and probably the cause of
# 404 at token exchange.
EXPECTED_TOKEN_URI = "https://oauth2.googleapis.com/token"
EXPECTED_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
EXPECTED_AUTH_PROVIDER = "https://www.googleapis.com/oauth2/v1/certs"
KNOWN_TYPES = {"service_account"}


def inspect_one(path: Path) -> dict[str, str | bool | None]:
    if not path.exists():
        return {"path": str(path), "error": "file not found"}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return {"path": str(path), "error": f"invalid JSON: {e}"}
    return {
        "path": str(path),
        "type": data.get("type"),
        "project_id": data.get("project_id"),
        "client_email": data.get("client_email"),
        "token_uri": data.get("token_uri"),
        "auth_uri": data.get("auth_uri"),
        "auth_provider_x509_cert_url": data.get("auth_provider_x509_cert_url"),
        "private_key_id_present": bool(data.get("private_key_id")),
        "private_key_present": bool(data.get("private_key")),
        "universe_domain": data.get("universe_domain"),
    }


def _flag(actual: str | None, expected: str) -> str:
    if actual is None:
        return "  ✗ MISSING"
    if actual == expected:
        return "  ✓"
    return f"  ⚠ NON-STANDARD (expected {expected})"


def print_one(info: dict[str, str | bool | None]) -> None:
    print()
    print("=" * 78)
    print(f"  {info['path']}")
    print("=" * 78)
    if info.get("error"):
        print(f"  ERROR: {info['error']}")
        return

    print(f"  type:                {info['type']}")
    if info["type"] not in KNOWN_TYPES:
        print("    ⚠ unexpected type — should be 'service_account'")

    print(f"  project_id:          {info['project_id']}")
    print(f"  client_email:        {info['client_email']}")
    print(f"  private_key_id:      {'present' if info['private_key_id_present'] else 'MISSING'}")
    print(f"  private_key:         {'present' if info['private_key_present'] else 'MISSING'}")

    print(f"  token_uri:           {info['token_uri']}")
    print(_flag(info["token_uri"], EXPECTED_TOKEN_URI))  # type: ignore[arg-type]

    print(f"  auth_uri:            {info['auth_uri']}")
    print(_flag(info["auth_uri"], EXPECTED_AUTH_URI))  # type: ignore[arg-type]

    print(f"  auth_provider_cert:  {info['auth_provider_x509_cert_url']}")
    print(_flag(info["auth_provider_x509_cert_url"], EXPECTED_AUTH_PROVIDER))  # type: ignore[arg-type]

    if info["universe_domain"] and info["universe_domain"] != "googleapis.com":
        print(f"  universe_domain:     {info['universe_domain']}")
        print("    ⚠ non-standard universe — talks to a different Google cloud")


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    paths = [Path(p).expanduser() for p in sys.argv[1:]]
    infos = [inspect_one(p) for p in paths]
    for info in infos:
        print_one(info)

    if len(infos) >= 2:
        print()
        print("=" * 78)
        print("  Field-by-field comparison")
        print("=" * 78)
        keys = ["type", "project_id", "token_uri", "auth_uri",
                "auth_provider_x509_cert_url", "universe_domain"]
        for k in keys:
            vals = {info["path"]: info.get(k) for info in infos}
            if len(set(str(v) for v in vals.values())) > 1:
                print(f"\n  DIFFERS — {k}:")
                for path, v in vals.items():
                    print(f"    {Path(str(path)).name}: {v}")

    # Exit 1 if any SA has a non-standard token_uri — most common 404 cause.
    bad = any(
        info.get("token_uri") and info["token_uri"] != EXPECTED_TOKEN_URI
        for info in infos
    )
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
