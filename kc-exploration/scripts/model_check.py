#!/usr/bin/env python3
"""Model plane check: prove this machine can talk to the model BEFORE the
agent runs.

    python scripts/model_check.py                            # the planes, the default, a token
    python scripts/model_check.py --plane gateway            # that plane's token
    python scripts/model_check.py --plane vertex --converse  # one tool round trip

--converse proves the whole contract the agent stands on without
touching the catalog: the declaration goes out, a function call comes
back, the model's parts are echoed verbatim (thought signatures
included) with a canned search result, and grounded text arrives on the
second call. Exit 0 = connected · 3 = env/auth problem · 1 = the model
refused or never called the tool.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kcx.agent import DiscoveryAgent                                        # noqa: E402
from kcx.env import EXIT_CONFIG, EXIT_OK, EXIT_REFUSED, load_env            # noqa: E402
from kcx.gateway import fingerprint                                         # noqa: E402
from kcx.planes import (ModelUnavailable, model_for, model_plane,           # noqa: E402
                        plane_catalog, plane_note)
from kcx.tools import TOOL_NAME                                             # noqa: E402
from kcx.transport import TransportError                                    # noqa: E402

CANNED = {
    "results": [
        {"entry_name": "projects/demo/locations/us/entryGroups/@bigquery/"
                       "entries/customer_account",
         "system": "BIGQUERY", "resource_id": "//bigquery.googleapis.com/"
         "projects/demo/datasets/crm/tables/customer_account",
         "display_name": "customer_account",
         "description": "One row per customer account, with the segment."},
        {"entry_name": "projects/demo/locations/us/entryGroups/@bigquery/"
                       "entries/customer_contact",
         "system": "BIGQUERY", "resource_id": "//bigquery.googleapis.com/"
         "projects/demo/datasets/crm/tables/customer_contact",
         "display_name": "customer_contact",
         "description": "Contact channels per customer."},
        {"entry_name": "projects/demo/locations/us/entryGroups/@bigquery/"
                       "entries/daily_weather",
         "system": "BIGQUERY", "resource_id": "//bigquery.googleapis.com/"
         "projects/demo/datasets/ext/tables/daily_weather",
         "display_name": "daily_weather",
         "description": "Daily weather by station: not a customer table."},
    ],
    "total_size": 3, "next_page_token": ""}
QUESTION = ("Which catalog entries hold customer tables? Search once with "
            "the tool, then list the full entry names of the ones that do.")


def canned_search(query: str = "") -> dict:
    return {"query": query, **CANNED}


def main(argv: list[str] | None = None, *, model=None) -> int:
    parser = argparse.ArgumentParser(prog="model_check.py")
    parser.add_argument("--plane", choices=["vertex", "gateway"], default="",
                        help="which plane to check (default: the environment's)")
    parser.add_argument("--converse", action="store_true",
                        help="also run one tool round trip with a canned result")
    parser.add_argument("--thinking", default="low",
                        choices=["low", "medium", "high"])
    args = parser.parse_args(argv)
    load_env()

    print("planes:")
    for row in plane_catalog():
        state = "ready" if row["available"] else f"not configured: {row['reason']}"
        mark = "  (default)" if row["default"] else ""
        print(f"  {row['id']:<8} {row['label']:<24} {state}{mark}")
    print(f"  {plane_note()}")
    plane = args.plane or model_plane()
    if model is None:
        try:
            model = model_for(plane)
        except ModelUnavailable as e:
            print(f"✗ {e}", file=sys.stderr)
            return EXIT_CONFIG
    print(f"\nplane {plane}: {model.describe()}")

    tokens = getattr(model, "tokens", None)
    token = getattr(model, "token", None)
    try:
        if tokens is not None:
            minted = tokens.token()
            print(f"✓ token minted: {fingerprint(minted)} · {tokens.describe()}")
        elif callable(token):
            acquired = token()
            print(f"✓ token acquired ({len(acquired)} chars)")
    except TransportError as e:
        print(f"✗ token failed: {e}", file=sys.stderr)
        return EXIT_CONFIG
    if not args.converse:
        print("token-only check passed — add --converse for one tool round trip")
        return EXIT_OK

    print(f"\nconverse ({model.model}): the question, a canned search, the answer")
    agent = DiscoveryAgent(model, kit={TOOL_NAME: canned_search},
                           max_calls=3, thinking_level=args.thinking)
    try:
        result = agent.run(QUESTION,
                           on_text=lambda d: print(d, end="", flush=True))
    except TransportError as e:
        print(f"\n✗ {e}", file=sys.stderr)
        return EXIT_REFUSED
    print()
    if not result.searches:
        print(f"✗ the model never called {TOOL_NAME}: {result.stop_reason}",
              file=sys.stderr)
        return EXIT_REFUSED
    print(f"✓ the model called {TOOL_NAME} {len(result.searches)}× "
          f"(queries: {[s['query'] for s in result.searches]})")
    if result.status != "answered":
        print(f"✗ no grounded answer: {result.stop_reason}", file=sys.stderr)
        return EXIT_REFUSED
    grounded = any(row["display_name"] in result.answer
                   for row in CANNED["results"][:2])
    print("✓ grounded answer: the model named the canned entries" if grounded
          else "⚠ the answer names none of the canned entries")
    print(f"  usage: {result.usage} · model calls {result.model_calls} · "
          f"{result.elapsed_ms / 1000:.1f} s")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
