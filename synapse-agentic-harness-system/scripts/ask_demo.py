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


def pump(runtime, session_id: str, seq: int,
         arrivals: dict | None = None) -> tuple[int, list[dict]]:
    """Print everything new until the turn closes."""
    rt = runtime.runtime(session_id)
    collected: list[dict] = []
    while True:
        batch = rt.bus.since(seq)
        for event in batch:
            seq = event["seq"]
            collected.append(event)
            if arrivals is not None:
                arrivals[id(event)] = time.perf_counter()
            render(event)
            if event["ev"] == "turn_done":
                return seq, collected
        if not batch:
            if not rt.running:
                return seq, collected
            time.sleep(0.05)


class _Recorder:
    """Wraps the model for --report: instrumentation OUTSIDE the
    product, exactly as tests wrap the transport. Records per call the
    routed step, wall time, time-to-first-token for streams, token
    deltas from the client's own counters, and strict-JSON drift
    (a json() call whose text did not parse is how drift shows up in
    this loop: the judge fails closed, the classify degrades)."""

    def __init__(self, inner, calls: list) -> None:
        self._inner = inner
        self._calls = calls

    def _usage(self) -> dict:
        client = getattr(self._inner, "client", None)
        return dict(getattr(client, "usage", {}) or {})

    @staticmethod
    def _step(system: str) -> str:
        if "You classify ONE turn" in system:
            return "classify"
        if "You compose ONE BigQuery SELECT" in system:
            return "compose"
        if "skeptical reviewer" in system:
            return "judge"
        return "other"

    def json(self, prompt, *, system="", temperature=0.0, max_tokens=1024):
        before, t0 = self._usage(), time.perf_counter()
        result, error = None, ""
        try:
            result = self._inner.json(prompt, system=system,
                                      temperature=temperature,
                                      max_tokens=max_tokens)
            return result
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise
        finally:
            after = self._usage()
            self._calls.append({
                "kind": "json", "step": self._step(system),
                "seconds": round(time.perf_counter() - t0, 3),
                "parsed": result is not None, "error": error,
                "tokens_in": after.get("prompt_tokens", 0)
                - before.get("prompt_tokens", 0),
                "tokens_out": after.get("output_tokens", 0)
                - before.get("output_tokens", 0),
                "thought_tokens": after.get("thought_tokens", 0)
                - before.get("thought_tokens", 0),
            })

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        before, t0 = self._usage(), time.perf_counter()
        first, chunks, error = None, 0, ""
        try:
            for chunk in self._inner.stream(prompt, system=system,
                                            temperature=temperature,
                                            max_tokens=max_tokens):
                if first is None:
                    first = time.perf_counter() - t0
                chunks += 1
                yield chunk
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise
        finally:
            after = self._usage()
            self._calls.append({
                "kind": "stream", "step": "compose",
                "seconds": round(time.perf_counter() - t0, 3),
                "ttft_seconds": round(first, 3) if first is not None
                else None,
                "chunks": chunks, "error": error,
                "tokens_in": after.get("prompt_tokens", 0)
                - before.get("prompt_tokens", 0),
                "tokens_out": after.get("output_tokens", 0)
                - before.get("output_tokens", 0),
                "thought_tokens": after.get("thought_tokens", 0)
                - before.get("thought_tokens", 0),
            })


def write_report(out_dir: Path, *, build_id: str, events: list[dict],
                 calls: list[dict], arrivals: dict[int, float],
                 store_versions: list[dict]) -> Path:
    """The Step-0a run report (E21): what a real conversation actually
    cost and where the time went. Written from observations only; no
    estimate is ever printed as a measurement."""
    import json as _json
    import os
    out_dir.mkdir(parents=True, exist_ok=True)

    turns: list[dict] = []
    for event in events:
        if event["ev"] == "turn_started":
            turns.append({"turn_id": event.get("turn_id"),
                          "text": event.get("text", ""),
                          "steps": [], "status": "?",
                          "elapsed_ms": None, "tokens": None})
        if not turns:
            continue
        turn = turns[-1]
        t0 = arrivals.get(id(event))
        start = next((arrivals.get(id(e)) for e in events
                      if e.get("turn_id") == event.get("turn_id")), None)
        turn["steps"].append({
            "ev": event["ev"],
            "at_ms": round((t0 - start) * 1000, 1)
            if (t0 is not None and start is not None) else None})
        if event["ev"] == "turn_done":
            turn["status"] = event.get("status")
            turn["elapsed_ms"] = event.get("elapsed_ms")
            turn["tokens"] = event.get("tokens")

    drift = {
        "json_calls": sum(1 for c in calls if c["kind"] == "json"),
        "json_unparsed": sum(1 for c in calls
                             if c["kind"] == "json" and not c["parsed"]),
        "call_errors": [c["error"] for c in calls if c.get("error")],
    }
    report = {
        "schema": "meridian.vertex_run_report/1",
        "build_id": build_id,
        "model": {
            "model": os.environ.get("VERTEX_MODEL")
            or os.environ.get("GEMINI_MODEL") or "(default)",
            "location": os.environ.get("VERTEX_LOCATION", "global"),
            "project_configured": bool(os.environ.get("VERTEX_PROJECT_ID")
                                       or os.environ.get(
                                           "GOOGLE_CLOUD_PROJECT")),
        },
        "turns": turns,
        "model_calls": calls,
        "drift": drift,
        "plan_chain": [v["version"] for v in store_versions],
        "note": "step timings are consumer-side arrivals over a 50ms "
                "poll: read them as +/-50ms",
    }
    (out_dir / "report.json").write_text(
        _json.dumps(report, indent=1) + "\n", encoding="utf-8")

    lines = ["# Vertex run report (E21 Step 0a)", "",
             f"- build: `{build_id}`",
             f"- model: `{report['model']['model']}` · location "
             f"`{report['model']['location']}`", ""]
    lines.append("## turns")
    for turn in turns:
        lines.append(f"- {turn['status']} in {turn['elapsed_ms']}ms · "
                     f"{turn['tokens']} tokens · {turn['text'][:60]!r}")
    lines.append("")
    lines.append("## model calls")
    lines.append("| step | kind | seconds | ttft | in | out | thought |"
                 " parsed |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for c in calls:
        lines.append(
            f"| {c['step']} | {c['kind']} | {c['seconds']} | "
            f"{c.get('ttft_seconds', '')} | {c['tokens_in']} | "
            f"{c['tokens_out']} | {c['thought_tokens']} | "
            f"{c.get('parsed', '')} |")
    lines += ["", f"## strict-JSON drift",
              f"- json calls: {drift['json_calls']} · unparsed "
              f"(fail-closed downstream): {drift['json_unparsed']}"]
    if drift["call_errors"]:
        lines.append("- errors: " + "; ".join(drift["call_errors"][:5]))
    (out_dir / "report.md").write_text("\n".join(lines) + "\n",
                                       encoding="utf-8")
    return out_dir / "report.md"


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
    parser.add_argument("--report", default="",
                        help="write the E21 Step-0a run report to this "
                             "directory (report.json + report.md)")
    args = parser.parse_args(argv)

    load_dotenv()
    import os
    graph = Path(args.graph or os.environ.get("MERIDIAN_GRAPH_DIR")
                 or SILO / "graph")
    builds = Path(args.builds or os.environ.get("MERIDIAN_BUILDS_DIR")
                  or SILO / "builds")
    ask_dir = graph / "runs" / "ask"
    calls: list[dict] = []
    arrivals: dict[int, float] = {}
    factory = None
    if args.report:
        from sahs.ask.model import VertexModel

        def factory(budget):                     # noqa: E731 (scoped)
            return _Recorder(VertexModel.from_env(budget), calls)
    runtime = AskRuntime(builds_root=builds, graph_root=graph,
                         store_path=ask_dir / "sessions.sqlite3",
                         events_dir=ask_dir / "events",
                         model_factory=factory)
    try:
        runtime.build()
    except BuildUnavailable as e:
        print(f"{RED}{e}{OFF}")
        return 1

    session = runtime.create_session(args.kind)
    print(f"{DIM}session {session['id']} · {args.kind}{OFF}")
    seq = 0
    all_events: list[dict] = []
    try:
        runtime.start_turn(session["id"], args.question)
        seq, events = pump(runtime, session["id"], seq, arrivals)
        all_events += events

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
            seq, events = pump(runtime, session["id"], seq, arrivals)
            all_events += events
            clarify = next((e for e in events
                            if e["ev"] == "clarify_request"), None)

        if args.then:
            runtime.start_turn(session["id"], args.then)
            seq, events = pump(runtime, session["id"], seq, arrivals)
            all_events += events
    except ModelUnavailable as e:
        print(f"{RED}{e}{OFF}")
        return 3

    versions = runtime.store.plan_versions(session["id"])
    if args.report:
        path = write_report(
            Path(args.report), build_id=runtime.build().version,
            events=all_events, calls=calls, arrivals=arrivals,
            store_versions=versions)
        print(f"{DIM}run report: {path}{OFF}")
    print(f"\n{DIM}plan chain: "
          + " → ".join(f"v{v['version']}" for v in versions)
          + f" · events at {ask_dir / 'events' / (session['id'] + '.jsonl')}"
          + f"{OFF}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
