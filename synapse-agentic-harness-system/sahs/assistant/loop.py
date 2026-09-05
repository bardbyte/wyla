"""The assistant loop (Synapse v3 §1/§3/§5): one interaction per
turn, the model driving through native tool calls.

The harness is thin on purpose. It builds a stable system prompt
(identity, the chain of command, the graph digest, the skill shelf,
what is remembered), replays the conversation as messages with the
newest ask last, hands the model the kit's declarations, and then
streams: text goes to the user as it arrives, thought summaries feed
the one live line, tool calls run whole and their results go back
whole (capped at ~20K characters with an explicit note). No strict
JSON, no strikes, no per-step prompt rebuild, no ``think`` field.

Governance stays where it always was — in the validators and gates
the tools call (hooks.py names them). Limits are a wall clock, a
call ceiling, and the session breaker; each ends the turn in plain
language with what was already said, never a vanished turn.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import time
from pathlib import Path
from typing import Any

from sahs.ask.budget import Aborted
from sahs.ask.model import ModelUnavailable
from sahs.loop.digest import synapse_digest
from sahs.loop.loop import _short, compact_result
from sahs.loop.skills import Skill, render_skills
from sahs.tools.api import Build

from .agent import ROUTING_KEY, declarations
from .events import EventBus
from .kit import RESULT_CAP, build_kit
from .sandbox import prepare_workspace
from .skills_loader import all_skills, render_skill_index
from .state import AssistantState
from .store import AssistantStore

ASSISTANT_VERSION = "assistant/3"
MAX_CALLS = 40             # model calls in one turn: a ceiling, not a plan
WALL_SECONDS = 600.0
MAX_OUTPUT_TOKENS = 16384
HISTORY_MESSAGES = 30      # stored messages replayed into the interaction
THINKING_LEVELS = {"quick": "low", "standard": "medium", "deep": "high"}
DEFAULT_THINKING = "medium"

# the first sentence is the transport routing key (agent.ROUTING_KEY)
IDENTITY = ROUTING_KEY + """ over the Meridian governed \
graph — warm, brief, plain, never mystical about yourself.

You are a general reasoner first: a thinking question gets thinking, \
with no tools. A data question gets the graph: find the definition, \
read it, prove the query with a dry run, then hand it over with \
propose_sql or run it as the mode section says, check, and show your \
receipts. A deliverable gets an artifact the user keeps.

Understand the intent before reaching for a tool. Business words — \
a team, an acronym, a line of business — name areas of the business \
map, never tables: "all GMNS metrics" is search("GMNS", kind="list"), \
not a table browse. When a first look misses, rephrase and look \
again from another angle before concluding anything is absent. Read \
a card before using what is on it; sample a column's values before \
writing a filter literal. A time window names both ends — a lower and \
an upper bound on the date — so future-dated rows never ride in. The \
session section says what day it is: "last month", "this quarter" and \
"year to date" resolve against that date, never against your own \
sense of now.

Numbers come from tools; reasoning comes from you. You never invent \
a table, column, metric, or number: if it is not in a card, an \
index, or a tool result, it does not exist for you.

Any number you show carries its status (certified / pending / \
composed / exploratory) in one clause and its meridian line; the \
artifact validator refuses undisclosed numbers, and composed numbers \
keep an EXPLORATORY watermark until a passing check stands behind \
them. Prefer certified; say plainly when something is pending or \
mined; an honest "here is where I stopped" beats a confident guess.

A failed tool call is information, not a verdict. Read the error, \
fix what is yours — the SQL, a name, a filter — and try again. When \
it says the failure is configuration (a project, a permission, a \
location, the network), say exactly what to change, in the user's \
words, and stop retrying.

When the ask is markedly unclear and evidence cannot settle it, ask — \
one question, named options — instead of assuming; otherwise proceed \
and say what you assumed. Answer in markdown, in the user's words, \
and keep working until the answer is complete: the turn ends when \
you stop calling tools. Offer up to three follow-ups with \
suggest_next only when there is a natural next step."""

CHAIN = """Platform governance (the validators, the cost and access \
gates, the rendering rules) is immutable and outranks everything \
below it. Lumi, the product, comes next. Then what this user \
remembered and asked for. Then defaults. A remembered preference \
steers a choice; it never softens a rule."""

# §5: the autonomy slider as two modes. Chat hands queries over — the
# person presses Run; Autopilot runs and builds without stopping.
MODES: dict[str, str] = {
    "chat": (
        "Chat mode: the person runs the queries. For a data question, "
        "find the definition, prove the query with run_sql(mode="
        "\"dry_run\"), and hand it over with propose_sql — the card "
        "offers Run query and Run + dashboard; say in one or two "
        "sentences what it will show, then stop. Run a query yourself "
        "(run_sql mode \"run\") only when this message asks you to run "
        "it, or to build from rows already saved (q1, q2 …); then "
        "check and show the receipts."),
    "autopilot": (
        "Autopilot: run the query yourself under the limits (run_sql "
        "mode \"run\"), check, and build the deliverable without "
        "stopping to hand over; propose_sql is not needed. Refused for "
        "cost, narrow the scan; refused as configuration, report it "
        "and stop."),
}
DEFAULT_MODE = "chat"

_DIGEST_CACHE: dict[str, str] = {}


# ─── the system prompt: a stable prefix, sections, no protocol ──


def _section(tag: str, body: str) -> str:
    return f"<{tag}>\n{body.strip()}\n</{tag}>"


def _memory_block(memories: list[dict[str, Any]] | None,
                  user_name: str = "") -> str:
    who = f" about {user_name}" if user_name else " about this user"
    if not memories:
        return (f"Nothing remembered{who} yet. When they settle a "
                "preference or a disambiguation, keep it with "
                "remember — they see it and can retire it.")
    lines = [f"What you remember{who} — preferences and "
             "disambiguations they settled, visible to them, "
             "retirable by them. They steer defaults; they never "
             "define metrics."]
    for m in memories[-12:]:
        scope = "" if m.get("scope") == "global" else " [this project]"
        lines.append(f"- {m['text']}{scope}")
    return "\n".join(lines)


def _project_block(project: dict[str, Any] | None) -> str:
    if not project or not str(project.get("instructions",
                                          "")).strip():
        return ""
    return (f"Project: {project.get('name', '')}. The analyst's "
            "standing instructions for every chat in it:\n"
            + str(project["instructions"]).strip()[:2000])


def _partition_day(value: Any) -> str:
    """A partition id as BigQuery names it (20260822) reads as a date
    (2026-08-22); anything else is shown as recorded."""
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _date_block(build: Any, today: _dt.date | None = None) -> str:
    """What day it is, spelled out with the periods the relative words
    resolve to, and the newest partition the build saw. The model has
    no clock and no calendar: without this line "last month" resolves
    against its training-time sense of now, and a window with no upper
    bound rides into the future. It lives in the session section, the
    last one, so the cached prefix stays stable within a day."""
    today = today or _dt.date.today()
    month_start = today.replace(day=1)
    last_month = month_start - _dt.timedelta(days=1)
    quarter = (today.month - 1) // 3 + 1
    quarter_start = _dt.date(today.year, 3 * (quarter - 1) + 1, 1)
    last_quarter_end = quarter_start - _dt.timedelta(days=1)
    last_quarter = (last_quarter_end.month - 1) // 3 + 1
    lines = [
        f"Today is {today:%A}, {today.isoformat()}. This month is "
        f"{today:%B %Y}; last month was {last_month:%B %Y}; this quarter "
        f"is Q{quarter} {today.year}, from {quarter_start.isoformat()}; "
        f"last quarter was Q{last_quarter} {last_quarter_end.year}; the "
        f"year to date runs from {today.year}-01-01. Relative words "
        "resolve against this date, never against your own sense of "
        "now."]
    horizon = max((_partition_day(t.get("partition_latest"))
                   for t in (getattr(build, "tables", None) or [])),
                  default="")
    if horizon:
        lines.append(
            f"Data on record runs to {horizon}, the newest partition the "
            "build saw; each table card's partitioned line says its own, "
            "and rows after it may not exist yet.")
    return "\n".join(lines)


def _session_block(build: Any, artifacts: list[dict[str, Any]] | None,
                   notes: list[str] | None,
                   today: _dt.date | None = None) -> str:
    lines = [_date_block(build, today)]
    if artifacts:
        lines.append("Artifacts kept in this chat (pass artifact_id "
                     "to publish a new version instead of a copy):")
        for row in artifacts[-12:]:
            lines.append(f"- {row['artifact_id']} · {row['type']} "
                         f"\"{row['title']}\" v{row['version']}")
    if notes:
        lines.append("Your working notes (note() updates them):")
        lines += [f"- {n}" for n in notes[-8:]]
    return "\n".join(lines)


def system_prompt(build: Build, skills: list[Skill] | None = None,
                  skill_index: list[Any] | None = None,
                  memories: list[dict[str, Any]] | None = None,
                  project: dict[str, Any] | None = None,
                  artifacts: list[dict[str, Any]] | None = None,
                  notes: list[str] | None = None,
                  user_name: str = "", mode: str = DEFAULT_MODE,
                  today: _dt.date | None = None) -> str:
    """Identity → chain → mode → the graph digest (business map +
    skills (loaded whole, the rest by name) → memory → this session
    (today's date first, then the artifacts and notes). Stable parts
    first so the prefix caches; the tools are declared to the
    transport, never pasted here."""
    digest = _DIGEST_CACHE.get(build.version)
    if digest is None:
        digest = synapse_digest(build,
                                list_hint='search("GMNS", kind="list")')
        _DIGEST_CACHE[build.version] = digest
    parts = [_section("identity", IDENTITY), _section("chain", CHAIN),
             _section("mode", MODES.get(mode, MODES[DEFAULT_MODE])),
             _section("graph", digest)]
    skill_text = render_skills(skills or [])
    shelf = render_skill_index(
        skill_index or [],
        exclude=frozenset(s.name for s in (skills or [])))
    if skill_text or shelf:
        parts.append(_section("skills", "\n\n".join(
            p for p in (skill_text, shelf) if p)))
    memory = "\n\n".join(p for p in (_project_block(project),
                                     _memory_block(memories, user_name))
                         if p)
    parts.append(_section("memory", memory))
    parts.append(_section("session",
                          _session_block(build, artifacts, notes, today)))
    return "\n\n".join(parts)


# ─── the conversation as messages, newest ask last ───────────


def _history(store: AssistantStore, session_id: str, turn_id: str,
             limit: int = HISTORY_MESSAGES) -> list[dict[str, Any]]:
    contents: list[dict[str, Any]] = []
    for row in store.messages(session_id)[-(limit + 1):]:
        if row.get("turn_id") == turn_id and row["role"] == "user":
            continue                         # this turn's ask goes last
        text = (row.get("text") or "").strip()
        payload = row.get("payload") or {}
        if row["role"] != "user" and isinstance(payload, dict):
            if payload.get("clarify") and not text:
                text = str(payload["clarify"].get("question", ""))
            if payload.get("artifacts"):
                text += ("\n(artifacts in the panel: "
                         + ", ".join(payload["artifacts"]) + ")")
        if not text.strip():
            continue
        role = "user" if row["role"] == "user" else "model"
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"].append({"text": text})
        else:
            contents.append({"role": role, "parts": [{"text": text}]})
    return contents[-limit:]


def _tail(contents: list[dict[str, Any]], cap: int = 8000) -> str:
    """What the model saw on this call, for the transcript record:
    the last two contents, compact."""
    lines = []
    for content in contents[-2:]:
        for part in content.get("parts", []):
            if "functionResponse" in part:
                fr = part["functionResponse"]
                lines.append(f"[{content['role']}] {fr.get('name')} → "
                             + _short(fr.get("response"), 600))
            elif "functionCall" in part:
                fc = part["functionCall"]
                lines.append(f"[{content['role']}] call {fc.get('name')}"
                             f"({_short(fc.get('args'), 300)})")
            elif part.get("text") and not part.get("thought"):
                lines.append(f"[{content['role']}] "
                             + str(part["text"])[:1500])
    return "\n".join(lines)[:cap]


# ─── what the surface shows for one tool call (never the model) ──


INPUT_CAP = 4000


def _bytes(n: Any) -> str:
    try:
        value = float(n)
    except (TypeError, ValueError):
        return "an unknown amount"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1000 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" \
                else f"{value:.1f} {unit}"
        value /= 1000
    return f"{value:.1f} TB"


def tool_input(name: str, args: dict[str, Any]) -> str:
    """What the transcript shows as the call's input — the SQL, the
    code, the query — capped, never the whole argument dict."""
    keys = {"run_sql": "sql", "python": "code", "search": "query",
            "read": "id", "artifact": "title", "check": "kind",
            "sample_values": "column", "load_skill": "name",
            "remember": "text", "note": "text", "ask": "question",
            "propose_sql": "sql"}
    key = keys.get(name)
    value = args.get(key) if key else None
    return str(value)[:INPUT_CAP] if value else ""


def summarize(tool: str, result: Any) -> str:
    """One line for the activity row. The model never sees this —
    it gets the whole result — so it can be as short as the UI
    wants."""
    if not isinstance(result, dict):
        return _short(result, 160)
    if result.get("error"):
        # what refused it: the validator's violations or the artifact
        # validator's problems, so the row says WHY, not just "invalid"
        found = (result.get("problems") or result.get("violations")
                 or [])
        problems = "; ".join(
            f"{p.get('code')}: {p.get('detail')}"
            for p in found[:2] if isinstance(p, dict))[:300]
        whose = ("configuration, not the query: "
                 if result.get("kind") in ("environment", "access")
                 else "")
        return (f"ERROR: {whose}{_short(result['error'], 160)}"
                + (f" — {problems}" if problems else ""))
    if tool == "search":
        rows = result.get("results") or result.get("metrics") \
            or result.get("hits") or []
        names = "; ".join(
            _short(r.get("label") or r.get("name") or r.get("text")
                   or r.get("card") or "", 40)
            for r in rows[:5] if isinstance(r, dict))
        scope = f" ({result['scope']})" if result.get("scope") else ""
        return f"{result.get('count', len(rows))} results{scope}: {names}"
    if tool == "read":
        if result.get("card"):
            return (f"{result['card']} · "
                    f"{len(str(result.get('text', '')))} chars, "
                    f"sections: "
                    f"{', '.join(result.get('sections', []))[:120]}")
        return (f"subgraph: {len(result.get('nodes', []))} nodes, "
                f"{len(result.get('edges', []))} edges")
    if tool == "sample_values":
        return compact_result("sample_values", result)
    if tool == "run_sql":
        mode = result.get("mode")
        scanned = _bytes(result.get("bytes_processed"))
        if mode == "dry_run":
            cols = [c.get("name") for c in (result.get("result_schema")
                                            or []) if isinstance(c, dict)]
            line = f"valid · would scan {scanned}" + (
                f" · columns: {', '.join(cols[:6])}" if cols else "")
        elif mode == "run":
            line = (f"{result.get('row_count', 0)} rows"
                    + (f" (LIMIT {result.get('limit')})"
                       if result.get("capped") else "")
                    + f" · scanned {scanned}")
        else:
            line = compact_result("run_sql", result)
        if result.get("saved_as"):
            line += f" · saved as {result['saved_as']}"
        if result.get("warnings"):
            line += f" · {len(result['warnings'])} warning(s)"
        return line[:240]
    if tool == "python":
        head = str(result.get("stdout", "")).strip().splitlines()
        return (f"{'ok' if result.get('ok') else 'FAILED'} in "
                f"{result.get('elapsed_ms')}ms"
                + (f": {head[0][:140]}" if head else ""))
    if tool == "check":
        return (f"{result.get('kind')} "
                f"{'passed' if result.get('passed') else 'did not pass'}"
                f" · {result.get('fact_id', '')} · "
                f"{_short(result.get('detail', ''), 120)}")
    if tool == "artifact":
        mark = f" · {result['watermark']}" if result.get("watermark") \
            else ""
        return (f"{result.get('type')} \"{result.get('title')}\" "
                f"v{result.get('version')} is in the panel{mark}")
    if tool == "propose_sql":
        p = result.get("proposal") or {}
        return (f"handed over \"{_short(p.get('title', ''), 60)}\" · "
                f"would scan {_bytes(p.get('bytes_processed'))} · "
                f"{p.get('status', '')}")
    if tool == "ask":
        return "asked: " + _short(
            (result.get("clarify") or {}).get("question", ""), 140)
    if tool == "load_skill":
        return (f"skill {result.get('name')} loaded"
                if result.get("text") else str(result.get("note", "")))
    if tool == "remember":
        return "remembered"
    if tool == "note":
        return f"noted ({result.get('notes')})"
    if tool == "suggest_next":
        return f"offered {len(result.get('chips', []))} follow-ups"
    return _short(result, 200)


def _response_payload(result: Any) -> tuple[dict[str, Any], str]:
    """The functionResponse body: the result whole, as an object,
    capped with an explicit note. Returns (object, event text)."""
    payload = {k: v for k, v in result.items() if k != "_artifact"} \
        if isinstance(result, dict) else {"result": result}
    text = json.dumps(payload, default=str)
    if len(text) <= RESULT_CAP:
        return payload, text
    kept = text[:RESULT_CAP]
    note = (f"truncated at {RESULT_CAP:,} characters "
            f"({len(text) - RESULT_CAP:,} more): narrow the call — "
            "read(section=…), a tighter query, a smaller limit — "
            "for the rest")
    return {"truncated": True, "note": note, "text": kept}, \
        kept + f"\n…[{note}]"


def _closing(reason: str, said: bool) -> str:
    if said:
        return f"\n\n— I stopped there: {reason}"
    return f"I stopped before I could answer: {reason}"


# ─── the turn ────────────────────────────────────────────────


def run_assistant_turn(*, build: Build, store: AssistantStore,
                       bus: EventBus, budget: Any, abort: Any,
                       model: Any, session: dict[str, Any],
                       turn_id: str, text: str, workspace: Path,
                       skills: list[Skill] | None = None,
                       substrate: Any = None,
                       snapshot_runner: Any = None, runner: Any = None,
                       graph_root: Path | None = None,
                       memories: list[dict[str, Any]] | None = None,
                       project: dict[str, Any] | None = None,
                       thinking_level: str = DEFAULT_THINKING,
                       user_name: str = "",
                       max_calls: int = MAX_CALLS,
                       wall_seconds: float = WALL_SECONDS,
                       mode: str = DEFAULT_MODE) -> str:
    session_id = session["id"]
    started = time.perf_counter()
    mode = mode if mode in MODES else DEFAULT_MODE
    bus.emit("turn_started", turn_id=turn_id, text=text,
             build_id=build.version, version=ASSISTANT_VERSION,
             skills=[s.name for s in (skills or [])],
             memories=len(memories or []),
             project=(project or {}).get("name", ""),
             thinking_level=thinking_level, mode=mode)
    budget.start_turn()
    prepare_workspace(workspace, build.root)

    state = AssistantState()
    state.notes = list(session.get("notes") or [])
    kit = build_kit(build, state, store=store, session_id=session_id,
                    turn_id=turn_id, workspace=workspace, model=model,
                    substrate=substrate, snapshot_runner=snapshot_runner,
                    runner=runner, graph_root=graph_root,
                    project_id=(project or {}).get("id", ""))
    tools = declarations(kit)
    system = system_prompt(
        build, skills, skill_index=all_skills(graph_root),
        memories=memories, project=project,
        artifacts=store.list_artifacts(session_id), notes=state.notes,
        user_name=user_name, mode=mode)
    bus.emit("model_prompt", turn_id=turn_id, n=0, kind="system",
             content=system[:12000])
    contents = _history(store, session_id, turn_id)
    contents.append({"role": "user", "parts": [{"text": text}]})

    said: list[str] = []
    calls = 0
    steps = 0
    stop_reason = ""
    status = "partial"
    clarify: dict[str, Any] | None = None
    proposal: dict[str, Any] | None = None   # propose_sql: handed over
    offered = False        # suggest_next after prose closes the turn
    # the thinking trace the transcript keeps (§6): the model's own
    # thought summaries per call, interleaved with the steps
    trace: list[dict[str, Any]] = []

    def _stream(prose: str) -> None:
        bus.emit("say_token", turn_id=turn_id, delta=prose)

    try:
        while True:
            if calls >= max_calls:
                stop_reason = (f"I hit my ceiling of {max_calls} model "
                               "calls for one turn. Ask me to continue "
                               "and I will pick it up from here.")
                break
            if time.perf_counter() - started >= wall_seconds:
                stop_reason = ("this took longer than I allow for one "
                               "turn. Ask me to continue and I will "
                               "pick it up from here.")
                break
            tripped = budget.exceeded()
            if tripped:
                stop_reason = (f"the {tripped} is used up for this "
                               "session. Start a new chat to keep "
                               "going.")
                break
            abort.check()

            calls += 1
            bus.emit("model_prompt", turn_id=turn_id, n=calls,
                     kind="call", content=_tail(contents))
            pending: list[dict[str, Any]] = []
            spoken: list[str] = []
            done: dict[str, Any] = {}
            for event in model.converse(contents, system=system,
                                        tools=tools,
                                        thinking_level=thinking_level,
                                        max_output_tokens=
                                        MAX_OUTPUT_TOKENS):
                kind = event.get("kind")
                if kind == "text":
                    delta = str(event.get("delta") or "")
                    if delta:
                        spoken.append(delta)
                        _stream(delta)
                elif kind == "thought":
                    delta = str(event.get("delta") or "")
                    if delta.strip():
                        bus.emit("thinking", turn_id=turn_id, delta=delta)
                        if trace and trace[-1].get("kind") == "thought" \
                                and trace[-1].get("call") == calls:
                            trace[-1]["text"] += delta
                        else:
                            trace.append({"kind": "thought",
                                          "call": calls, "text": delta})
                elif kind == "call":
                    pending.append(event)
                elif kind == "done":
                    done = event
                abort.check()
            bus.emit("budget_tick", turn_id=turn_id, **budget.tick())
            if spoken:
                said.append("".join(spoken))

            if not pending:
                if said or state.artifacts_touched:
                    status = "answered"
                else:
                    finish = str(done.get("finish") or "")
                    stop_reason = (
                        "I came back with nothing usable"
                        + (f" (the model finished with {finish})"
                           if finish and finish != "STOP" else "")
                        + ". Ask again, maybe with a little more "
                        "detail, and I will do better.")
                break

            contents.append({"role": "model",
                             "parts": done.get("parts") or [
                                 {"functionCall": {
                                     "name": c["name"],
                                     "args": c.get("args") or {}}}
                                 for c in pending]})
            responses: list[dict[str, Any]] = []
            for call in pending:
                name = str(call.get("name", ""))
                args = call.get("args") if isinstance(call.get("args"),
                                                      dict) else {}
                steps += 1
                shown = tool_input(name, args)
                bus.emit("tool_call", turn_id=turn_id, n=steps,
                         tool=name, args=_short(args, 160), input=shown)
                spec = kit.get(name)
                t0 = time.perf_counter()
                if spec is None:
                    result: Any = {"error": f"unknown tool {name!r}",
                                   "hint": "the tools are "
                                           + ", ".join(kit)}
                else:
                    try:
                        result = spec.fn(**args)
                    except TypeError as e:
                        result = {"error": "the arguments did not "
                                           f"match: {e}",
                                  "hint": spec.signature}
                    except Aborted:
                        raise
                    except Exception as e:      # noqa: BLE001
                        result = {"error": f"{type(e).__name__}: {e}",
                                  "hint": "try a different call"}
                ref = f"a{steps}"
                payload, content = _response_payload(result)
                summary = summarize(name, result)
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
                trace.append({"kind": "tool", "tool": name,
                              "args": _short(args, 160),
                              "input": shown, "summary": summary,
                              "elapsed_ms": elapsed_ms})
                bus.emit("tool_step", turn_id=turn_id, n=steps,
                         tool=name, args=_short(args, 120),
                         input=shown, summary=summary, ref=ref,
                         elapsed_ms=elapsed_ms)
                bus.emit("tool_result", turn_id=turn_id, ref=ref,
                         tool=name, content=content)
                if isinstance(result, dict) and result.get("_artifact"):
                    row = result["_artifact"]
                    bus.emit("artifact", turn_id=turn_id,
                             artifact_id=row["artifact_id"],
                             version=row["version"], type=row["type"],
                             title=row["title"], spec=row["spec"])
                if name == "ask" and isinstance(result, dict) \
                        and result.get("ok"):
                    clarify = result["clarify"]
                if name == "propose_sql" and isinstance(result, dict) \
                        and result.get("ok"):
                    proposal = result["proposal"]
                if name == "suggest_next" and isinstance(result, dict) \
                        and result.get("ok") and said:
                    offered = True
                response = {"name": name, "response": payload}
                if call.get("id"):
                    response["id"] = call["id"]
                responses.append({"functionResponse": response})
            contents.append({"role": "user", "parts": responses})

            if clarify is not None:
                # the turn ends on the question; the answer arrives as
                # the next message, chips carrying the evidence
                prose = "\n\n".join(said)
                store.add_message(
                    session_id, "assistant",
                    prose or clarify["question"], turn_id=turn_id,
                    payload={"clarify": clarify,
                             "artifacts": list(dict.fromkeys(
                                 state.artifacts_touched)),
                             "trace": _trim_trace(trace),
                             "elapsed_ms": round(
                                 (time.perf_counter() - started) * 1000,
                                 1)})
                bus.emit("chips", turn_id=turn_id, clarify=clarify)
                _persist(store, session_id, state, "clarify",
                         clarify["question"],
                         [str(o.get("label", o))
                          if isinstance(o, dict) else str(o)
                          for o in clarify.get("options", [])])
                _finish(bus, budget, turn_id, "clarify", started,
                        model_calls=calls, steps=steps,
                        thinking_level=thinking_level,
                        skills_loaded=list(state.skills_loaded))
                return "clarify"

            if proposal is not None:
                # the handover: the person runs it from the card, so
                # the turn ends here without another model call
                prose = "\n\n".join(s for s in said if s.strip())
                if not prose:
                    prose = (f"Here is the query for "
                             f"{proposal['title']}. Run it when you are "
                             "ready, or ask for a dashboard.")
                    _stream(prose)
                    said.append(prose)
                chips = list(state.chips)
                row = store.add_message(
                    session_id, "assistant", prose, turn_id=turn_id,
                    payload={"proposal": proposal, "chips": chips,
                             "artifacts": list(dict.fromkeys(
                                 state.artifacts_touched)),
                             "trace": _trim_trace(trace),
                             "elapsed_ms": round(
                                 (time.perf_counter() - started) * 1000,
                                 1)})
                bus.emit("proposal", turn_id=turn_id,
                         message_id=row["id"], proposal=proposal)
                if chips:
                    bus.emit("chips", turn_id=turn_id, suggestions=chips)
                if not (session.get("title") or "").strip():
                    title = text.strip()[:60]
                    session["title"] = title
                    store.set_title(session_id, title)
                _persist(store, session_id, state, "proposed", prose,
                         chips)
                _finish(bus, budget, turn_id, "proposed", started,
                        model_calls=calls, steps=steps,
                        thinking_level=thinking_level,
                        skills_loaded=list(state.skills_loaded))
                return "proposed"

            if offered:
                # the follow-ups came after the answer: that is the
                # end of the turn, no extra model call to say so
                status = "answered"
                break

    except Aborted:
        stop_reason = "you stopped me."
    except ModelUnavailable as e:
        if calls <= 1 and not said and steps == 0:
            raise            # nothing happened yet: the honest error card
        # streamed text is never discarded (§5): close in plain language
        stop_reason = (f"I lost the connection to the model ({e}). Ask "
                       "me to continue and I will pick it up from here.")

    if status != "answered" and stop_reason:
        closing = _closing(stop_reason, bool(said))
        if not said and state.notes:
            closing += (" What I had so far: "
                        + "; ".join(state.notes[-3:]) + ".")
        _stream(closing)
        said.append(closing)
        status = "stopped" if "you stopped" in stop_reason else "partial"

    prose = "\n\n".join(s for s in said if s.strip())
    chips = list(state.chips)
    if prose or state.artifacts_touched:
        store.add_message(
            session_id, "assistant", prose, turn_id=turn_id,
            payload={"chips": chips,
                     "artifacts": list(dict.fromkeys(
                         state.artifacts_touched)),
                     "trace": _trim_trace(trace),
                     "elapsed_ms": round(
                         (time.perf_counter() - started) * 1000, 1)})
    if chips:
        bus.emit("chips", turn_id=turn_id, suggestions=chips)
    if not (session.get("title") or "").strip() and prose:
        title = text.strip()[:60]
        session["title"] = title
        store.set_title(session_id, title)
    _persist(store, session_id, state, status, prose, chips)
    _finish(bus, budget, turn_id, status, started, model_calls=calls,
            steps=steps, thinking_level=thinking_level,
            subgraph_used=state.subgraph,
            skills_loaded=list(state.skills_loaded))
    return status


def run_proposal_turn(*, build: Build, store: AssistantStore,
                      bus: EventBus, budget: Any,
                      session: dict[str, Any], turn_id: str,
                      proposal: dict[str, Any], sql: str = "",
                      limit: int = 200, workspace: Path,
                      substrate: Any = None, snapshot_runner: Any = None,
                      runner: Any = None,
                      graph_root: Path | None = None) -> str:
    """The person pressed Run: the proposed query executes under the
    limits with NO model call, the rows land as a table artifact and
    as q1 in the workspace, and the turn ends with the receipts — the
    one step of a chat that never waits on the model."""
    session_id = session["id"]
    started = time.perf_counter()
    title = str(proposal.get("title") or "the query")
    written = str(proposal.get("sql_written") or proposal.get("sql")
                  or "")
    sql = (sql or "").strip() or written
    edited = sql != written
    bus.emit("turn_started", turn_id=turn_id, text=f"Run: {title}",
             build_id=build.version, version=ASSISTANT_VERSION,
             skills=[], memories=0,
             project=str(session.get("project_id") or ""),
             thinking_level="none", mode="run")
    budget.start_turn()
    prepare_workspace(workspace, build.root)
    state = AssistantState()
    state.notes = list(session.get("notes") or [])
    kit = build_kit(build, state, store=store, session_id=session_id,
                    turn_id=turn_id, workspace=workspace,
                    substrate=substrate, snapshot_runner=snapshot_runner,
                    runner=runner, graph_root=graph_root,
                    project_id=str(session.get("project_id") or ""))
    limit = max(1, min(int(limit or 200), 1000))
    args = {"sql": sql, "mode": "run", "limit": limit}
    shown = sql[:INPUT_CAP]
    bus.emit("tool_call", turn_id=turn_id, n=1, tool="run_sql",
             args=_short(args, 160), input=shown)
    t0 = time.perf_counter()
    try:
        result: Any = kit["run_sql"].fn(sql, mode="run", limit=limit)
    except Exception as e:                          # noqa: BLE001
        result = {"error": f"{type(e).__name__}: {e}",
                  "hint": "the run failed before the warehouse answered"}
    summary = summarize("run_sql", result)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    trace = [{"kind": "tool", "tool": "run_sql",
              "args": _short(args, 160), "input": shown,
              "summary": summary, "elapsed_ms": elapsed_ms}]
    _payload, content = _response_payload(result)
    bus.emit("tool_step", turn_id=turn_id, n=1, tool="run_sql",
             args=_short(args, 120), input=shown, summary=summary,
             ref="a1", elapsed_ms=elapsed_ms)
    bus.emit("tool_result", turn_id=turn_id, ref="a1", tool="run_sql",
             content=content)

    artifacts: list[str] = []
    if not isinstance(result, dict) or result.get("error"):
        err = result if isinstance(result, dict) else {"error": result}
        hint = str(err.get("hint") or "").strip()
        if err.get("kind") in ("environment", "access"):
            said = (f"I could not run it: {err.get('error')}. This is "
                    "configuration, not the query"
                    + (f" — {hint}" if hint else "") + ".")
            chips = ["Explain what to change"]
        else:
            said = (f"I could not run it: {err.get('error')}."
                    + (f" {hint}" if hint else ""))
            chips = ["Narrow the query", "Explain the error"]
        status = "partial"
    else:
        rows = result.get("rows") or []
        columns = [c.get("name") for c in (result.get("result_schema")
                                           or [])
                   if isinstance(c, dict) and c.get("name")]
        if not columns and rows:
            columns = list(rows[0].keys())
        spec = {"columns": [{"key": c, "label": c} for c in columns],
                "rows": rows,
                "provenance": {
                    "status": proposal.get("status") or "exploratory",
                    "meridian_line": proposal.get("meridian_line") or "",
                    **({"metric_id": proposal["metric_id"]}
                       if proposal.get("metric_id") else {})}}
        made = kit["artifact"].fn("table", title, json.dumps(spec))
        if isinstance(made, dict) and made.get("_artifact"):
            row = made["_artifact"]
            artifacts.append(row["artifact_id"])
            bus.emit("artifact", turn_id=turn_id,
                     artifact_id=row["artifact_id"],
                     version=row["version"], type=row["type"],
                     title=row["title"], spec=row["spec"])
        count = result.get("row_count", len(rows))
        said = (f"Ran it{' with your edits' if edited else ''}: "
                f"{count} rows"
                + (f" (LIMIT {limit})" if result.get("capped") else "")
                + f", scanned {_bytes(result.get('bytes_processed'))} "
                f"of a {_bytes(result.get('scan_ceiling_bytes'))} "
                "ceiling. " + str(proposal.get("meridian_line") or ""))
        if result.get("saved_as"):
            said += (f" The rows are saved as {result['saved_as']} for "
                     "the next step.")
        if isinstance(made, dict) and made.get("error"):
            said += (" The table could not be published: "
                     + "; ".join(str(p.get("detail", ""))
                                 for p in (made.get("problems") or [])[:2]))
        if result.get("warnings"):
            said += "\n\nNote: " + " ".join(
                str(w) for w in result["warnings"][:2])
        said += _future_note(rows)
        chips = []
        if result.get("saved_as"):
            # the first picture needs no model: the chart turn draws
            # the saved rows under the same provenance
            chips.append({"label": "Chart these rows", "action": "chart",
                          "saved_as": result["saved_as"]})
        chips += ["Build a dashboard from these rows",
                  "Refine the query"]
        status = "answered"
    bus.emit("say_token", turn_id=turn_id, delta=said)
    state.chips = chips
    store.add_message(
        session_id, "assistant", said, turn_id=turn_id,
        payload={"chips": chips, "artifacts": artifacts,
                 "trace": _trim_trace(trace),
                 "elapsed_ms": round(
                     (time.perf_counter() - started) * 1000, 1),
                 "ran": {"title": title, "edited": edited,
                         "status": status,
                         "saved_as": (result.get("saved_as")
                                      if isinstance(result, dict)
                                      else None),
                         "provenance": (spec["provenance"]
                                        if status == "answered"
                                        else None)}})
    if chips:
        bus.emit("chips", turn_id=turn_id, suggestions=chips)
    _persist(store, session_id, state, status, said, chips)
    _finish(bus, budget, turn_id, status, started, model_calls=0,
            steps=1, thinking_level="none", skills_loaded=[])
    return status


_DATE_LIKE = re.compile(r"^\d{4}-\d{2}(-\d{2})?([T ].*)?$")


def _read_rows(workspace: Path, saved_as: str) -> list[dict[str, Any]]:
    path = workspace / f"{saved_as}.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows") if isinstance(data, dict) else data
    return [r for r in (rows or []) if isinstance(r, dict)]


def _future_dates(rows: list[dict[str, Any]],
                  today: _dt.date | None = None) -> dict[str, Any]:
    """Rows dated after today, per date-like column — the sign of a
    window with no upper bound, or of future-dated rows in the table.
    {column: {"rows": n, "latest": date}}; empty when none."""
    today = today or _dt.date.today()
    found: dict[str, Any] = {}
    for row in rows:
        for column, value in row.items():
            text = str(value or "")
            if not _DATE_LIKE.match(text):
                continue
            try:
                day = _dt.date.fromisoformat(
                    text[:10] if len(text) >= 10 else text[:7] + "-01")
            except ValueError:
                continue
            if day > today:
                entry = found.setdefault(column, {"rows": 0, "latest": day})
                entry["rows"] += 1
                if day > entry["latest"]:
                    entry["latest"] = day
    return found


def _future_note(rows: list[dict[str, Any]], *, verb: str = "rows") -> str:
    found = _future_dates(rows)
    if not found:
        return ""
    column, info = max(found.items(), key=lambda kv: kv[1]["rows"])
    n = info["rows"]
    return (f"\n\nNote: {n} of these {verb} {'is' if n == 1 else 'are'} "
            f"dated after today in {column} (up to "
            f"{info['latest'].isoformat()}): the query has no upper bound "
            "on the date, or the table holds future-dated rows. Refine it "
            f"with {column} <= CURRENT_DATE() before trusting the tail.")


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def chart_rows_turn(*, build: Build, store: AssistantStore,
                    bus: EventBus, budget: Any, session: dict[str, Any],
                    turn_id: str, saved_as: str, title: str,
                    provenance: dict[str, Any], workspace: Path,
                    kind: str = "", x: str = "",
                    y: list[str] | None = None,
                    graph_root: Path | None = None) -> str:
    """The person asked for the picture: the rows a run saved become a
    chart under the run's own provenance, with NO model call. The x
    axis is the first date-like or text column, the series are the
    numeric columns (at most four), a date axis draws a line and any
    other a bar; the person can name x, y and the kind instead."""
    session_id = session["id"]
    started = time.perf_counter()
    label = f"Chart: {title}"
    bus.emit("turn_started", turn_id=turn_id, text=label,
             build_id=build.version, version=ASSISTANT_VERSION,
             skills=[], memories=0,
             project=str(session.get("project_id") or ""),
             thinking_level="none", mode="chart")
    budget.start_turn()
    state = AssistantState()
    state.notes = list(session.get("notes") or [])
    kit = build_kit(build, state, store=store, session_id=session_id,
                    turn_id=turn_id, workspace=workspace,
                    graph_root=graph_root,
                    project_id=str(session.get("project_id") or ""))
    rows = _read_rows(workspace, saved_as)
    columns = list(rows[0].keys()) if rows else []
    numeric = [c for c in columns
               if any(_number(r.get(c)) is not None for r in rows)]
    wanted_y = [c for c in (y or []) if c in columns]
    if x and x in columns:
        axis = x
    else:
        axis = next((c for c in columns
                     if any(_DATE_LIKE.match(str(r.get(c, "")))
                            for r in rows)), None) \
            or next((c for c in columns if c not in numeric), None) \
            or (columns[0] if columns else "")
    series_cols = wanted_y or [c for c in numeric if c != axis][:4]
    dated = bool(axis) and any(_DATE_LIKE.match(str(r.get(axis, "")))
                               for r in rows)
    chart_kind = kind if kind in ("line", "bar", "area", "scatter") \
        else ("line" if dated else "bar")
    args = {"saved_as": saved_as, "x": axis, "y": series_cols,
            "kind": chart_kind}
    bus.emit("tool_call", turn_id=turn_id, n=1, tool="chart",
             args=_short(args, 160), input=_short(args, INPUT_CAP))
    t0 = time.perf_counter()
    artifacts: list[str] = []
    made: Any = None
    if not rows:
        said = (f"Nothing to chart: no rows are saved as {saved_as} "
                "in this chat. Run the query first.")
        summary = "ERROR: no saved rows"
        status = "partial"
    elif not series_cols or not axis:
        said = ("Nothing to chart: the rows have no numeric column "
                f"beside {axis or 'the axis'} (columns: "
                + ", ".join(columns[:8]) + ").")
        summary = "ERROR: no numeric column"
        status = "partial"
    else:
        spec = {"kind": chart_kind,
                "series": [{"name": c, "points": [
                    [str(r.get(axis, "")), _number(r.get(c))]
                    for r in rows if _number(r.get(c)) is not None]}
                    for c in series_cols],
                "provenance": dict(provenance or {})}
        made = kit["artifact"].fn("chart", f"{title} — chart",
                                  json.dumps(spec))
        if isinstance(made, dict) and made.get("_artifact"):
            row = made["_artifact"]
            artifacts.append(row["artifact_id"])
            bus.emit("artifact", turn_id=turn_id,
                     artifact_id=row["artifact_id"],
                     version=row["version"], type=row["type"],
                     title=row["title"], spec=row["spec"])
            said = (f"Drew it: a {chart_kind} chart of "
                    + ", ".join(series_cols) + f" by {axis}, {len(rows)} "
                    f"rows, under the query's own provenance. "
                    + str((provenance or {}).get("meridian_line") or "")
                    + _future_note(rows, verb="points"))
            summary = (f"{chart_kind} · {len(series_cols)} series · "
                       f"{len(rows)} points · x={axis}")
            status = "answered"
        else:
            problems = (made or {}).get("problems") or []
            said = ("The chart could not be published: "
                    + "; ".join(str(p.get("detail", p))
                                for p in problems[:2]))
            summary = "ERROR: " + str((made or {}).get("error", ""))
            status = "partial"
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    trace = [{"kind": "tool", "tool": "chart", "args": _short(args, 160),
              "input": _short(args, INPUT_CAP), "summary": summary,
              "elapsed_ms": elapsed_ms}]
    bus.emit("tool_step", turn_id=turn_id, n=1, tool="chart",
             args=_short(args, 120), input=_short(args, INPUT_CAP),
             summary=summary, ref="a1", elapsed_ms=elapsed_ms)
    bus.emit("tool_result", turn_id=turn_id, ref="a1", tool="chart",
             content=json.dumps({"rows": len(rows), "x": axis,
                                 "y": series_cols, "kind": chart_kind,
                                 "status": status}))
    bus.emit("say_token", turn_id=turn_id, delta=said)
    chips = (["Build a dashboard from these rows", "Refine the query"]
             if status == "answered" else [])
    state.chips = chips
    store.add_message(
        session_id, "assistant", said, turn_id=turn_id,
        payload={"chips": chips, "artifacts": artifacts,
                 "trace": _trim_trace(trace),
                 "elapsed_ms": round(
                     (time.perf_counter() - started) * 1000, 1),
                 "charted": {"saved_as": saved_as, "x": axis,
                             "y": series_cols, "kind": chart_kind,
                             "status": status}})
    if chips:
        bus.emit("chips", turn_id=turn_id, suggestions=chips)
    _persist(store, session_id, state, status, said, chips)
    _finish(bus, budget, turn_id, status, started, model_calls=0,
            steps=1, thinking_level="none", skills_loaded=[])
    return status


def _trim_trace(trace: list[dict[str, Any]], *, max_entries: int = 60,
                max_chars: int = 2000) -> list[dict[str, Any]]:
    """What the transcript keeps of the thinking: bounded, the call
    index dropped, the thought text capped."""
    out = []
    for entry in trace[-max_entries:]:
        item = {k: v for k, v in entry.items() if k != "call"}
        if item.get("kind") == "thought":
            item["text"] = str(item.get("text", ""))[:max_chars]
        out.append(item)
    return out


def _persist(store: AssistantStore, session_id: str,
             state: AssistantState, status: str, prose: str,
             chips: list[str]) -> None:
    """The working notes survive the turn, and the handoff says where
    you left off when the session reopens tomorrow."""
    store.set_notes(session_id, state.notes)
    checked = [f["kind"] for f in state.facts_log if f.get("passed")]
    store.set_handoff(session_id, {
        "status": status,
        "say": prose.replace("\n", " ")[:300],
        "chips": [c.get("label", "") if isinstance(c, dict) else str(c)
                  for c in chips[:3]],
        "artifacts": list(dict.fromkeys(state.artifacts_touched)),
        "checked": checked[-6:],
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


def _finish(bus: EventBus, budget: Any, turn_id: str, status: str,
            started: float, **extra: Any) -> None:
    bus.emit("turn_done", turn_id=turn_id, status=status,
             elapsed_ms=round((time.perf_counter() - started) * 1000,
                              1),
             **extra, **budget.tick())


__all__ = ["ASSISTANT_VERSION", "IDENTITY", "THINKING_LEVELS",
           "DEFAULT_THINKING", "MODES", "DEFAULT_MODE", "system_prompt",
           "summarize", "run_assistant_turn", "run_proposal_turn",
           "chart_rows_turn"]
