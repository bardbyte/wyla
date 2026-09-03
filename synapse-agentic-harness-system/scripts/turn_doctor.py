#!/usr/bin/env python3
"""Where is the turn? Read one chat session's event file and say, per
turn, how long each model call and each tool took — and, for a turn
that has not finished, where it sits right now.

    python scripts/turn_doctor.py                      # newest session
    python scripts/turn_doctor.py --events <dir-or-file> [--turn t_xxx]

The event file is the chat's single source of truth (graph/runs/chat/
events/<session>.jsonl). A turn is a chain of model calls and tool
calls; this prints that chain with wall-clock gaps, so "stuck at
Checking the query" resolves to one of: the tool never returned, the
model call never answered, or the turn finished and the page did not
hear it.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _ts(value: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _secs(a: str, b: str) -> float:
    return round((_ts(b) - _ts(a)).total_seconds(), 1)


def load_events(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def newest_events_file(events_dir: Path) -> Path | None:
    files = sorted(events_dir.glob("*.jsonl"), key=os.path.getmtime)
    return files[-1] if files else None


def timeline(events: list[dict[str, Any]], *,
             now: _dt.datetime | None = None) -> list[dict[str, Any]]:
    """One record per turn: its segments (model calls and tools, each
    with a duration) and, for an unfinished turn, the open segment."""
    now = now or _dt.datetime.now(_dt.timezone.utc)
    turns: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for e in events:
        tid = e.get("turn_id") or "?"
        if tid not in turns:
            turns[tid] = {"turn_id": tid, "text": "", "segments": [],
                          "status": "", "open": None, "started": "",
                          "ended": "", "tokens": None, "thought": ""}
            order.append(tid)
        t = turns[tid]
        ev = e.get("ev")
        ts = e.get("ts", "")
        if ev == "turn_started":
            t["text"] = e.get("text", "")
            t["started"] = ts
        elif ev == "model_prompt" and e.get("kind") == "call":
            t["open"] = {"what": f"model call #{e.get('n')}", "since": ts,
                         "first_sign": None}
            t["thought"] = ""
        elif ev == "thinking":
            if t["open"] and t["open"]["first_sign"] is None:
                t["open"]["first_sign"] = ts
            t["thought"] = (t["thought"] + e.get("delta", ""))[-300:]
        elif ev == "say_token":
            if t["open"] and t["open"]["first_sign"] is None:
                t["open"]["first_sign"] = ts
        elif ev == "tool_call":
            if t["open"]:
                t["segments"].append(_close(t["open"], ts))
            t["open"] = {"what": f"tool {e.get('tool')}", "since": ts,
                         "first_sign": None, "args": e.get("args", "")}
        elif ev == "tool_step":
            if t["open"] and t["open"]["what"].startswith("tool "):
                seg = _close(t["open"], ts)
                seg["outcome"] = str(e.get("summary", ""))[:120]
                t["segments"].append(seg)
                t["open"] = None
        elif ev == "budget_tick":
            t["tokens"] = e.get("tokens")
        elif ev in ("turn_done", "error"):
            if t["open"]:
                t["segments"].append(_close(t["open"], ts))
                t["open"] = None
            if ev == "turn_done":
                t["status"] = e.get("status", "")
                t["ended"] = ts
            elif e.get("code") != "trace":
                t["status"] = "error: " + str(e.get("message", ""))[:160]
                t["ended"] = ts
    for t in turns.values():
        if t["open"]:
            since = t["open"]["since"]
            t["open"]["for_seconds"] = round(
                (now - _ts(since)).total_seconds(), 1)
    return [turns[tid] for tid in order]


def _close(seg: dict[str, Any], end: str) -> dict[str, Any]:
    out = dict(seg)
    out["seconds"] = _secs(seg["since"], end)
    if seg.get("first_sign"):
        out["first_sign_after"] = _secs(seg["since"], seg["first_sign"])
    return out


def render(turns: list[dict[str, Any]]) -> str:
    lines = []
    for t in turns:
        head = f"turn {t['turn_id']} · {t['text'][:70]!r}"
        if t["status"]:
            head += f" · {t['status']}"
            if t["started"] and t["ended"]:
                head += f" · {_secs(t['started'], t['ended'])}s"
        lines.append(head)
        for seg in t["segments"]:
            note = ""
            if seg.get("first_sign_after") is not None:
                note = f" (first sign after {seg['first_sign_after']}s)"
            if seg.get("outcome"):
                note += f" → {seg['outcome']}"
            lines.append(f"  {seg['seconds']:>7}s  {seg['what']}{note}")
        if t["open"]:
            o = t["open"]
            sign = ("nothing has arrived" if o.get("first_sign") is None
                    else "text or thoughts were arriving")
            lines.append(f"  >>> OPEN: in {o['what']} for "
                         f"{o['for_seconds']}s — {sign}")
            if t["thought"]:
                lines.append(f"      last thought: {t['thought'][-160:]!r}")
            if o["what"].startswith("model call"):
                lines.append("      the model call has not returned: the "
                             "proxy or Vertex is holding it (the client "
                             "gives up after 120s of silence, once, then "
                             "the turn closes in plain language)")
                if any(seg.get("what", "").startswith("tool run_sql")
                       for seg in t["segments"]):
                    lines.append(
                        "      it follows a warehouse call: check the "
                        "planes above — the Vertex route must still be "
                        "the proxy after a BigQuery call (a BigQuery "
                        "connection used to write googleapis.com into "
                        "NO_PROXY, sending every later model call "
                        "direct into the corporate blackhole)")
            else:
                lines.append("      the tool has not returned: the "
                             "warehouse or the token trip is holding it")
        if t["tokens"] is not None:
            lines.append(f"  tokens so far: {t['tokens']}")
    return "\n".join(lines) if lines else "no turns in this file"


def planes() -> str:
    """The route each network plane pins on its connection, read from
    the environment without a single network call — the first thing
    to check when a turn hangs right after a dry run."""
    from sahs.util.auth import AuthError, BQConnection, VertexConnection
    lines = ["network planes (each route is pinned on its connection; "
             "the environment's NO_PROXY is never consulted):"]
    for label, cls in (("BigQuery", BQConnection),
                       ("Vertex", VertexConnection)):
        try:
            connection = cls.from_env()
        except AuthError as e:
            lines.append(f"  {label:<9} not configured: {e}")
            continue
        except Exception as e:                       # noqa: BLE001
            lines.append(f"  {label:<9} could not resolve: {e}")
            continue
        lines.append(f"  {label:<9} {connection.route()} · "
                     f"{connection.endpoint}")
    leak = (os.environ.get("NO_PROXY", "")
            + os.environ.get("no_proxy", "")).lower()
    if "googleapis" in leak:
        lines.append("  note      NO_PROXY names googleapis in the "
                     "environment: ignored on both planes by design")
    from sahs.tools.sandbox import (human_bytes, live_switch_note,
                                    scan_ceiling)
    lines.append(f"  live      {live_switch_note()} · scan ceiling "
                 f"{human_bytes(scan_ceiling())}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="turn_doctor.py")
    parser.add_argument("--events", default="",
                        help="an events dir or one session file "
                             "(default: <graph>/runs/chat/events, newest)")
    parser.add_argument("--turn", default="", help="one turn id")
    args = parser.parse_args(argv)
    if args.events:
        path = Path(args.events)
    else:
        from sahs.util.auth import load_dotenv
        load_dotenv()
        graph = Path(os.environ.get("MERIDIAN_GRAPH_DIR")
                     or Path(__file__).resolve().parents[1] / "graph")
        path = graph / "runs" / "chat" / "events"
    if path.is_dir():
        found = newest_events_file(path)
        if found is None:
            print(f"no session event files under {path}", file=sys.stderr)
            return 1
        path = found
    turns = timeline(load_events(path))
    if args.turn:
        turns = [t for t in turns if t["turn_id"] == args.turn]
    print(f"{path}\n")
    print(planes() + "\n")
    print(render(turns[-3:] if not args.turn else turns))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
