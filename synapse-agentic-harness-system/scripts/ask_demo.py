#!/usr/bin/env python3
"""ask_demo.py — drive the Ask loop (E18 Stage A) with no UI at all.

    python scripts/ask_demo.py "acquirer net spend by day"
    python scripts/ask_demo.py "..." --pick 1        # auto-answer chips
    python scripts/ask_demo.py "..." --then "same for Canada"

Runs the real harness in-process against the promoted build and real
Vertex, printing the event stream as it arrives. This is the Stage A
exit: a conversation you can watch end to end before any pixel of
chat UI exists, so every later UI bug is a UI bug.

Paths and the Vertex contract come from the silo .env (see
.env.example). Nothing here is mocked: no build, no answer.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.ask import AskRuntime, BuildUnavailable      # noqa: E402
from sahs.ask.model import ModelUnavailable            # noqa: E402
from sahs.util.auth import load_dotenv                 # noqa: E402

DIM, BOLD, GREEN, RED, BLUE, OFF = (
    "\033[2m", "\033[1m", "\033[32m", "\033[31m", "\033[34m", "\033[0m")


def render(event: dict) -> None:
    ev = event["ev"]
    if ev == "turn_started":
        print(f"\n{BOLD}> {event['text']}{OFF}  {DIM}[{event['kind']} · "
              f"build {event['build_id']}]{OFF}")
    elif ev == "classify_result":
        how = "model" if event.get("model_used") else "deterministic"
        print(f"{DIM}  classify: {event['kind']} ({how}) "
              f"{event.get('why', '')}{OFF}")
    elif ev == "plan_delta":
        for change in event.get("changes", []):
            print(f"{BLUE}  plan v{event['version']}: {change['slot']} "
                  f"{change['from']!r} → {change['to']!r}{OFF}")
    elif ev == "resolve_result":
        bound = event.get("bound") or {}
        print(f"{DIM}  resolved in {event.get('elapsed_ms')}ms: "
              f"{bound.get('label', '(nothing new)')}{OFF}")
    elif ev == "clarify_request":
        print(f"\n{BOLD}? {event['question']}{OFF}")
        for i, option in enumerate(event["options"], 1):
            print(f"    {i}. {option['label']}   {DIM}{option.get('why','')}"
                  f"{OFF}")
    elif ev == "contract_ready":
        print(f"{DIM}  contract (all false until proven):{OFF}")
        for criterion in event["contract"]["will_verify"]:
            print(f"    {RED}✗{OFF} {criterion['text']}")
    elif ev == "generate_token":
        print(event["delta"], end="", flush=True)
    elif ev == "verify_progress":
        criterion = event["criterion"]
        mark = f"{GREEN}✓{OFF}" if criterion["passed"] else f"{RED}✗{OFF}"
        print(f"\n    {mark} {criterion['text']}  {DIM}"
              f"{criterion['evidence']}{OFF}")
    elif ev == "verify_verdict":
        colour = GREEN if event["verdict"] == "pass" else RED
        print(f"  {colour}verdict: {event['verdict']}{OFF}")
    elif ev == "answer_payload":
        payload = event["payload"]
        print(f"\n{DIM}  meridian line: {payload['meridian_line']}{OFF}")
        print(f"{DIM}  grain: {payload['grain']}{OFF}")
        for limit in payload["limits"]:
            print(f"{DIM}  limit: {limit}{OFF}")
    elif ev == "error":
        print(f"\n{RED}  error [{event['code']}] {event['message']}{OFF}")
        for action in event.get("next_actions", []):
            print(f"{DIM}    → {action}{OFF}")
    elif ev == "turn_done":
        print(f"{DIM}  turn {event['status']} in {event['elapsed_ms']}ms · "
              f"{event['tokens']} tokens{OFF}")


def pump(runtime, session_id: str, seq: int) -> tuple[int, list[dict]]:
    """Print everything new until the turn closes."""
    rt = runtime.runtime(session_id)
    collected: list[dict] = []
    while True:
        batch = rt.bus.since(seq)
        for event in batch:
            seq = event["seq"]
            collected.append(event)
            render(event)
            if event["ev"] == "turn_done":
                return seq, collected
        if not batch:
            if not rt.running:
                return seq, collected
            time.sleep(0.05)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("question")
    parser.add_argument("--then", default="",
                        help="a follow-up turn (e.g. 'same for Canada')")
    parser.add_argument("--pick", type=int, default=0,
                        help="auto-answer a clarify with this option (1-based)")
    parser.add_argument("--kind", default="analyst",
                        choices=("analyst", "steward"))
    parser.add_argument("--graph", default="")
    parser.add_argument("--builds", default="")
    args = parser.parse_args(argv)

    load_dotenv()
    import os
    graph = Path(args.graph or os.environ.get("MERIDIAN_GRAPH_DIR")
                 or SILO / "graph")
    builds = Path(args.builds or os.environ.get("MERIDIAN_BUILDS_DIR")
                  or SILO / "builds")
    ask_dir = graph / "runs" / "ask"
    runtime = AskRuntime(builds_root=builds, graph_root=graph,
                         store_path=ask_dir / "sessions.sqlite3",
                         events_dir=ask_dir / "events")
    try:
        runtime.build()
    except BuildUnavailable as e:
        print(f"{RED}{e}{OFF}")
        return 1

    session = runtime.create_session(args.kind)
    print(f"{DIM}session {session['id']} · {args.kind}{OFF}")
    seq = 0
    try:
        runtime.start_turn(session["id"], args.question)
        seq, events = pump(runtime, session["id"], seq)

        clarify = next((e for e in events if e["ev"] == "clarify_request"),
                       None)
        while clarify:
            options = clarify["options"]
            if args.pick:
                index = min(args.pick, len(options)) - 1
            else:
                raw = input(f"\n{BOLD}pick 1-{len(options)}: {OFF}").strip()
                index = (int(raw) - 1) if raw.isdigit() else 0
            option = options[index]
            runtime.start_turn(session["id"], option["label"],
                               choice={"slot": clarify["slot"],
                                       "value": option["value"],
                                       "label": option["label"]})
            seq, events = pump(runtime, session["id"], seq)
            clarify = next((e for e in events
                            if e["ev"] == "clarify_request"), None)

        if args.then:
            runtime.start_turn(session["id"], args.then)
            seq, events = pump(runtime, session["id"], seq)
    except ModelUnavailable as e:
        print(f"{RED}{e}{OFF}")
        return 3

    versions = runtime.store.plan_versions(session["id"])
    print(f"\n{DIM}plan chain: "
          + " → ".join(f"v{v['version']}" for v in versions)
          + f" · events at {ask_dir / 'events' / (session['id'] + '.jsonl')}"
          + f"{OFF}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
