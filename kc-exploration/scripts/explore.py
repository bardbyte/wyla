#!/usr/bin/env python3
"""Explore the catalog with a question.

    python scripts/explore.py "which tables hold card transactions in project demo-warehouse?"
    python scripts/explore.py "..." --plane gateway --thinking medium
    python scripts/explore.py "..." --json

The model reads SKILL.md, rewrites the question into a baseline search
and up to three variations, fires them in one batch, merges what came
back, and answers with the entries that matter. Under the answer: every
search that ran, and every entry found with how many searches found it
(its witnesses). Exit 0 = answered · 1 = the model or the catalog
refused · 3 = env/auth problem.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kcx.agent import DiscoveryAgent, Exploration                           # noqa: E402
from kcx.catalog import CatalogClient, CatalogConnection                    # noqa: E402
from kcx.env import (EXIT_CONFIG, EXIT_OK, EXIT_REFUSED, ConfigError,       # noqa: E402
                     load_env)
from kcx.planes import ModelUnavailable, model_for                          # noqa: E402
from kcx.tools import make_kit                                              # noqa: E402
from kcx.transport import TransportError                                    # noqa: E402


def short_type(entry_type: str) -> str:
    return entry_type.rsplit("/", 1)[-1] if entry_type else ""


def render(result: Exploration, *, top: int, out=sys.stdout) -> None:
    print("\n── searches", file=out)
    print(f"  {'call':<5}{'count':<7}query", file=out)
    for row in result.searches:
        tail = f"   ✗ {row['error']}" if row["error"] else ""
        print(f"  {row['call']:<5}{row['count']:<7}{row['query']}{tail}", file=out)
    total = len(result.entries)
    shown = result.entries[:top]
    print(f"\n── entries ({'top ' + str(len(shown)) + ' of ' if total > len(shown) else ''}{total})",
          file=out)
    print(f"  {'wit':<5}{'system':<10}{'type':<18}name", file=out)
    for row in shown:
        name = row.get("display_name") or row["entry_name"].rsplit("/", 1)[-1]
        print(f"  {row['witnesses']:<5}{(row.get('system') or '?'):<10}"
              f"{short_type(row.get('entry_type', '')):<18}{name}", file=out)
        print(f"       {row['entry_name']}", file=out)
    usage = result.usage
    print(f"\n── usage: prompt {usage['prompt_tokens']:,} · output "
          f"{usage['output_tokens']:,} · thought {usage['thought_tokens']:,} · "
          f"model calls {result.model_calls} · {result.elapsed_ms / 1000:.1f} s",
          file=out)
    if result.stop_reason:
        print(f"── {result.status}: {result.stop_reason}", file=out)


def main(argv: list[str] | None = None, *, model=None, http=None,
         token=None) -> int:
    parser = argparse.ArgumentParser(prog="explore.py")
    parser.add_argument("question")
    parser.add_argument("--plane", default="",
                        help="vertex | gateway (default: the environment's)")
    parser.add_argument("--thinking", default=None,
                        choices=["low", "medium", "high"])
    parser.add_argument("--max-calls", type=int, default=None)
    parser.add_argument("--top", type=int, default=30,
                        help="entries to list (default 30)")
    parser.add_argument("--json", action="store_true",
                        help="print the whole result as JSON, nothing else")
    parser.add_argument("--quiet", action="store_true",
                        help="no streaming: the answer and the tables at the end")
    args = parser.parse_args(argv)
    load_env()

    try:
        connection = CatalogConnection.from_env()
    except ConfigError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  python scripts/kc_check.py names what the catalog plane "
              "needs", file=sys.stderr)
        return EXIT_CONFIG
    client = CatalogClient(connection, http=http, token=token)
    if model is None:
        try:
            model = model_for(args.plane)
        except ModelUnavailable as e:
            print(f"✗ {e}", file=sys.stderr)
            print("  python scripts/model_check.py names what each plane "
                  "needs", file=sys.stderr)
            return EXIT_CONFIG

    agent = DiscoveryAgent(model, make_kit(client), max_calls=args.max_calls,
                           thinking_level=args.thinking)
    streaming = not (args.json or args.quiet)
    on_text = (lambda d: print(d, end="", flush=True)) if streaming else None
    try:
        result = agent.run(args.question, on_text=on_text)
    except TransportError as e:
        print(f"\n✗ {e}", file=sys.stderr)
        return EXIT_REFUSED
    if args.json:
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    else:
        if not streaming and result.answer:
            print(result.answer)
        render(result, top=max(0, args.top))
    return EXIT_OK if (result.answer or result.entries) else EXIT_REFUSED


if __name__ == "__main__":
    raise SystemExit(main())
