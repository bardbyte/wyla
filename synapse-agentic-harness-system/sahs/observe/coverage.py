"""The coverage check: each Langfuse concept → the Spanner table and
columns it is built from → present or missing in the live rows,
computed through the record reader. One table, printed by
``langfuse_sync.py coverage``; the honest gaps are the rows that stay
missing whatever the rows say (docs/runbooks/langfuse-insight.md).
"""

from __future__ import annotations

from typing import Any

from .datasets import is_silver
from .record import RecordReader, turns_of

# concept, the source, what makes it present
CONCEPTS: tuple[tuple[str, str], ...] = (
    ("trace", "ChatEvents (turn_started … turn_done; TurnId, Ev, Payload)"),
    ("session", "ChatSessions.SessionId"),
    ("user", "ChatSessions.OwnerUserId → Users.UserId"),
    ("generation", "ChatEvents model_prompt (n, kind, content[:12000])"),
    ("generation usage", "ChatEvents budget_tick (tokens_in, tokens_out, "
                         "tokens: deltas per call)"),
    ("tool span", "ChatEvents tool_call / tool_step / tool_result "
                  "(n, tool, args, summary, ref)"),
    ("task span", "ChatEvents task_started / task_done + the sub-turn's "
                  "records (task, sub_turn)"),
    ("artifact event", "ChatEvents artifact (artifact_id, version, type) "
                       "≈ ChatArtifacts"),
    ("proposal event", "ChatEvents proposal / ChatMessages.Payload.proposal "
                       "(sql, title, metric_id)"),
    ("chips event", "ChatEvents chips (suggestions)"),
    ("score turn_status", "ChatEvents turn_done.status"),
    ("score refused", "ChatEvents tool_step.summary starting ERROR"),
    ("score tasks_failed", "ChatEvents task_done.status"),
    ("score feedback", "ChatFeedback (TurnId, Vote, Note) joined by turn"),
    ("prompt version", "ChatEvents model_prompt n=0 (prompt_version, "
                       "prompt_parts) — turns before this build lack it"),
    ("dataset item: silver", "turns with ChatFeedback up, or turn_done "
                             "done and nothing refused; SQL from "
                             "ChatMessages.Payload.proposal"),
    ("dataset item: scenarios", "ChatEvents plan_made (tasks, synthesis, "
                                "waves)"),
    ("dataset item: precedents", "a checked-in JSONL, not Spanner "
                                 "(tests/fixtures/precedents)"),
    ("experiment run", "run_evals.py --langfuse: the harness's verdicts, "
                       "not Spanner"),
    ("per-task usage", "budget_tick is a session counter; concurrent "
                       "tasks share it — not attributable by design"),
    ("full prompt text", "model_prompt content is cut at 12 000 chars on "
                         "purpose; the fingerprint stands for the rest"),
    ("human labels beyond thumbs", "no table: the annotation queue in "
                                   "Langfuse is the only place"),
)
NEVER_FROM_ROWS = {"dataset item: precedents", "experiment run",
                   "per-task usage", "full prompt text",
                   "human labels beyond thumbs"}


def coverage(reader: RecordReader, *, limit: int = 2000) -> list[dict[str, Any]]:
    """The table as data: concept, source, present, count, note."""
    sessions = reader.sessions(limit=limit)
    counts = reader.counts()
    seen: dict[str, int] = {}
    silver = scenarios = 0
    for session in sessions:
        events = reader.events(session)
        feedback = reader.feedback(session)
        messages = reader.messages(session)
        for record in events:
            ev = str(record.get("ev") or "")
            if ev == "turn_done" and "." in str(record.get("turn_id") or ""):
                continue                  # a task's sub-turn: not a trace
            seen[ev] = seen.get(ev, 0) + 1
            if ev == "budget_tick" and int(record.get("tokens") or 0):
                seen["budget_tick:tokens"] = seen.get("budget_tick:tokens", 0) + 1
            if ev == "tool_step" and str(record.get("summary") or "").startswith("ERROR"):
                seen["tool_step:refused"] = seen.get("tool_step:refused", 0) + 1
            if ev == "model_prompt" and record.get("kind") == "system" \
                    and record.get("prompt_version"):
                seen["prompt_version"] = seen.get("prompt_version", 0) + 1
        for message in messages:
            payload = message.get("payload")
            if isinstance(payload, dict) and isinstance(payload.get("proposal"), dict):
                seen["message:proposal"] = seen.get("message:proposal", 0) + 1
        seen["feedback:turn"] = seen.get("feedback:turn", 0) + sum(
            1 for v in feedback if v.get("turn_id"))
        turns = turns_of(session, events, messages, feedback)
        silver += sum(1 for t in turns if t.get("question") and is_silver(t))
        scenarios += sum(1 for t in turns if isinstance(t.get("plan"), dict)
                         and t["plan"].get("tasks"))

    def n(*keys: str) -> int:
        return sum(seen.get(k, 0) for k in keys)

    present_by: dict[str, tuple[bool, int]] = {
        "trace": (n("turn_done") > 0, n("turn_done")),
        "session": (counts["ChatSessions"] > 0, counts["ChatSessions"]),
        "user": (counts["owners"] > 0, counts["owners"]),
        "generation": (n("model_prompt") > 0, n("model_prompt")),
        "generation usage": (n("budget_tick:tokens") > 0, n("budget_tick:tokens")),
        "tool span": (n("tool_call") > 0, n("tool_call")),
        "task span": (n("task_started") > 0, n("task_started")),
        "artifact event": (n("artifact") > 0, n("artifact")),
        "proposal event": (n("proposal", "message:proposal") > 0,
                           n("proposal", "message:proposal")),
        "chips event": (n("chips") > 0, n("chips")),
        "score turn_status": (n("turn_done") > 0, n("turn_done")),
        "score refused": (n("tool_step") > 0, n("tool_step:refused")),
        "score tasks_failed": (n("task_done") > 0, n("task_done")),
        "score feedback": (n("feedback:turn") > 0, n("feedback:turn")),
        "prompt version": (n("prompt_version") > 0, n("prompt_version")),
        "dataset item: silver": (silver > 0, silver),
        "dataset item: scenarios": (scenarios > 0, scenarios),
    }
    rows = []
    for concept, source in CONCEPTS:
        if concept in NEVER_FROM_ROWS:
            rows.append({"concept": concept, "source": source,
                         "present": None, "count": 0,
                         "note": "not from the rows"})
            continue
        present, count = present_by[concept]
        note = ""
        if concept == "score refused" and present and count == 0:
            note = "no refusal yet (a zero score on every turn)"
        if concept == "prompt version" and not present and n("model_prompt"):
            note = "turns predate the fingerprint"
        rows.append({"concept": concept, "source": source,
                     "present": present, "count": count, "note": note})
    return rows


def format_coverage(rows: list[dict[str, Any]]) -> str:
    width = max(len(r["concept"]) for r in rows)
    lines = [f"{'concept':{width}}  {'state':8} {'rows':>6}  source"]
    for r in rows:
        state = ("n/a" if r["present"] is None
                 else "present" if r["present"] else "MISSING")
        note = f"  ({r['note']})" if r.get("note") else ""
        lines.append(f"{r['concept']:{width}}  {state:8} {r['count']:>6}  "
                     f"{r['source']}{note}")
    return "\n".join(lines)


def format_coverage_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["| concept | built from | state | rows |", "|---|---|---|---|"]
    for r in rows:
        state = ("not from the rows" if r["present"] is None
                 else "present" if r["present"] else "missing")
        if r.get("note") and r["present"] is not None:
            state += f" ({r['note']})"
        lines.append(f"| {r['concept']} | {r['source']} | {state} | "
                     f"{r['count']} |")
    return "\n".join(lines)


__all__ = ["CONCEPTS", "NEVER_FROM_ROWS", "coverage", "format_coverage",
           "format_coverage_markdown"]
