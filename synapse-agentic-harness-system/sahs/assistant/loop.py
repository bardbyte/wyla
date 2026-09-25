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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sahs.ask.budget import Aborted
from sahs.ask.model import ModelUnavailable
from sahs.loop.digest import synapse_digest
from sahs.loop.loop import _short, compact_result
from sahs.loop.skills import Skill, SkillRefused, render_skills
from sahs.tools.api import Build
from sahs.util.profiles import prompt_style

from .agent import ROUTING_KEY, declarations
from .artifacts import validate_artifact
from .events import EventBus
from .kit import RESULT_CAP, build_kit
from .planner import plan_for, waves
from .prompt_version import prompt_fingerprint
from .sandbox import prepare_workspace
from .skills_loader import all_skills, render_skill_index, skill_context
from .state import AssistantState
from .store import AssistantStore

ASSISTANT_VERSION = "assistant/3"
MAX_CALLS = 40             # model calls in one turn: a ceiling, not a plan
WALL_SECONDS = 600.0
MAX_OUTPUT_TOKENS = 16384
HISTORY_MESSAGES = 30      # stored messages replayed into the interaction
# a compound ask (docs/multi-task-turns.md): the tasks of one turn run
# as sub-turns of the same session, independent ones side by side on a
# small pool, each under a share of the turn's call ceiling so the
# total never exceeds it; the synthesis keeps a couple of calls back
TASK_POOL = 2              # tasks running side by side
SYNTHESIS_CALLS = 2        # model calls kept for the final composition
MIN_TASK_CALLS = 4         # no task gets fewer than this
QUERY_STRIDE = 20          # q<N> names per task: t1 saves q1…, t2 q21…
FINDINGS_CAP = 3000        # chars of a task's prose handed to the next
REPORT_TITLE = "What was done"
THINKING_LEVELS = {"minimal": "minimal", "quick": "low", "standard": "medium",
                   "deep": "high", "max": "max"}
# the depth dial as the composer explains it (§5): five stops on one
# slider, each changing how much the model thinks before every step —
# nothing else. The call ceiling and the wall clock are the same at
# every depth. A level a model does not know is mapped to the nearest
# it does (gateway: GATEWAY_THINKING_LEVELS; Vertex: VERTEX_THINKING_LEVELS).
DEPTHS: dict[str, dict[str, str]] = {
    "minimal": {
        "label": "Minimal", "level": "minimal",
        "means": "Almost no thinking before a step: a one-line answer, "
                 "a rename, a yes or no on what is already here.",
    },
    "quick": {
        "label": "Quick", "level": "low",
        "means": "A short think before each step. Right for a lookup, "
                 "a definition, or a follow-up on rows already here.",
    },
    "standard": {
        "label": "Standard", "level": "medium",
        "means": "The default. Enough thinking to find the right "
                 "metric, prove the query and hand it over.",
    },
    "deep": {
        "label": "Deep", "level": "high",
        "means": "More thinking per step: a multi-step analysis, an "
                 "unfamiliar join, or a question with several ways to "
                 "read it. Slower, and it costs more.",
    },
    "max": {
        "label": "Extra deep", "level": "max",
        "means": "The most thinking the model allows, on every step. "
                 "For the hardest questions only: slowest, and the "
                 "costliest.",
    },
}
DEFAULT_THINKING = "medium"

# the first sentence is the transport routing key (agent.ROUTING_KEY)
IDENTITY = ROUTING_KEY + """ over the Radix Graph, the company's \
governed knowledge layer — warm, brief, plain, never mystical about \
yourself.

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
below it. Synapse, the product, comes next. Then what this user \
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
# the autonomy slider as the composer explains it
MODE_MEANS: dict[str, dict[str, str]] = {
    "chat": {
        "label": "Chat",
        "means": "Synapse finds the definition, proves the query with "
                 "a dry run and hands it over on a card. You press Run. "
                 "Nothing is scanned until you do.",
    },
    "autopilot": {
        "label": "Autopilot",
        "means": "Synapse runs the query itself under the limits, "
                 "checks the rows and builds the deliverable without "
                 "stopping to hand over.",
    },
}

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
                  today: _dt.date | None = None, style: str = "",
                  retrieval: str = "",
                  library: list[str] | None = None,
                  likely: tuple[str, ...] = ()) -> str:
    """Identity → chain → mode → style (the model family's, when it has
    one) → the graph digest (business map + skills (loaded whole, then
    the library packs' contents and matched passages (``retrieval``,
    per turn), the rest by name) → memory → this session (today's date
    first, then the artifacts and notes). Stable parts first so the
    prefix caches; the tools are declared to the transport, never
    pasted here."""
    digest = _DIGEST_CACHE.get(build.version)
    if digest is None:
        digest = synapse_digest(build,
                                list_hint='search("GMNS", kind="list")')
        _DIGEST_CACHE[build.version] = digest
    parts = [_section("identity", IDENTITY), _section("chain", CHAIN),
             _section("mode", MODES.get(mode, MODES[DEFAULT_MODE]))]
    if style:
        parts.append(_section("style", style))
    parts.append(_section("graph", digest))
    skill_text = render_skills(skills or [])
    shelf = render_skill_index(
        skill_index or [],
        exclude=frozenset(s.name for s in (skills or []))
        | frozenset(library or []), likely=tuple(likely or ()))
    if skill_text or retrieval or shelf:
        parts.append(_section("skills", "\n\n".join(
            p for p in (skill_text, retrieval, shelf) if p)))
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
        if row["role"] != "user" and isinstance(payload, dict) \
                and payload.get("task"):
            # a task's own message: the turn's synthesis carries what
            # it found, so the history stays one answer per ask
            continue
        if row["role"] == "user" and isinstance(payload, dict) \
                and payload.get("files"):
            # the bytes rode their own turn; later turns know a file
            # was sent, by name, not its contents
            text += ("\n(attached files on that message: "
                     + ", ".join(f.get("name", "") for f in payload["files"])
                     + ")")
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
            elif "inlineData" in part:
                blob = part["inlineData"]
                lines.append(f"[{content['role']}] inline "
                             f"{blob.get('mimeType', '?')} · "
                             f"{len(blob.get('data', '')) * 3 // 4:,} bytes")
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
            "skill_toc": "name", "skill_search": "query",
            "skill_read": "section",
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
        if result.get("searchable"):
            return (f"skill {result.get('name')} loaded as a library · "
                    f"{result.get('sections')} sections")
        return (f"skill {result.get('name')} loaded"
                if result.get("text") else str(result.get("note", "")))
    if tool == "skill_toc":
        return (f"{result.get('name')} · {result.get('sections')} sections, "
                f"{len(result.get('toc') or [])} listed")
    if tool == "skill_search":
        heads = "; ".join(_short(h.get("heading_path", ""), 50)
                          for h in (result.get("hits") or [])[:3])
        return f"{result.get('count', 0)} passages: {heads}"
    if tool == "skill_read":
        return (f"{result.get('skill')} · {result.get('heading_path')} · "
                f"chars {result.get('start')}–{result.get('end')}"
                + (" (more)" if result.get("truncated") else ""))
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


@dataclass
class SubTurn:
    """How a turn runs inside another (a task, the synthesis, or the
    plain turn a planner declined). The parent owns the turn budget,
    the workspace and the title; the sub-turn tags nothing itself —
    the bus it is handed does (a ``task`` field on every record)."""

    task: str = ""                  # the task id, "" for the synthesis
    label: str = ""                 # what the events call this turn
    announced: bool = True          # the parent emitted turn_started
    finish: bool = False            # emit turn_done (else the parent)
    title: bool = False             # may set the session title
    query_offset: int = 0           # q<N> starts after this
    tools: frozenset[str] | None = None   # None: the whole kit
    payload: dict[str, Any] = field(default_factory=dict)
    artifacts: list[str] = field(default_factory=list)


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
                       mode: str = DEFAULT_MODE,
                       plane: str = "",
                       attachments: list[dict[str, Any]] | None = None,
                       file_names: list[str] | None = None,
                       owner: str = "",
                       model_label: str = "",
                       model_name: str = "",
                       sub_turn: SubTurn | None = None) -> str:
    session_id = session["id"]
    started = time.perf_counter()
    mode = mode if mode in MODES else DEFAULT_MODE
    sub = sub_turn
    if sub is None or not sub.announced:
        bus.emit("turn_started", turn_id=turn_id,
                 text=(sub.label if sub and sub.label else text),
                 build_id=build.version, version=ASSISTANT_VERSION,
                 skills=[s.name for s in (skills or [])],
                 memories=len(memories or []),
                 project=(project or {}).get("name", ""),
                 thinking_level=thinking_level, mode=mode, plane=plane,
                 model=model_label,
                 files=list(file_names or []))
    if sub is None:
        # a sub-turn spends its parent's turn budget and workspace
        budget.start_turn()
        prepare_workspace(workspace, build.root)
    extra_payload = dict(sub.payload) if sub else {}
    prior_artifacts = list(sub.artifacts) if sub else []

    state = AssistantState()
    state.notes = list(session.get("notes") or [])
    if sub and sub.query_offset:
        state.queries_saved = int(sub.query_offset)
    # the skills, split at this engine's whole-load limit: whole ones
    # paste verbatim; a pack over it is a library — its contents and
    # the passages that match this ask, under the engine's budget at
    # this depth — unless its frontmatter demands the whole file, which
    # refuses the turn by name (fail closed, never a partial load)
    shelf = all_skills(graph_root, owner)
    try:
        library = skill_context(graph_root, list(skills or []), text,
                                model_name, thinking_level, shelf=shelf)
    except SkillRefused as refused:
        partial = getattr(refused, "context", None)
        if partial is not None:
            bus.emit("skills_loaded", turn_id=turn_id, **partial.event())
        bus.emit("error", turn_id=turn_id, code="skill_refused",
                 message="I could not load a skill this chat pins: "
                         + str(refused),
                 retryable=False,
                 next_actions=["switch to a model with a larger window",
                               "or mark the skill sectioned "
                               "(runtime_loading: sectioned) in its "
                               "frontmatter",
                               "or unpin it for this chat"])
        # a task's sub-turn leaves the closing to its foreman
        if sub is None or sub.finish:
            _finish(bus, budget, turn_id, "error", started, model_calls=0,
                    steps=0, thinking_level=thinking_level, skills_loaded=[])
        return "error"
    bus.emit("skills_loaded", turn_id=turn_id, **library.event())
    kit = build_kit(build, state, store=store, session_id=session_id,
                    turn_id=turn_id, workspace=workspace, model=model,
                    substrate=substrate, snapshot_runner=snapshot_runner,
                    runner=runner, graph_root=graph_root,
                    project_id=(project or {}).get("id", ""),
                    owner=owner, retriever=library.index,
                    searchable=library.searchable_names,
                    skill_limit=library.limit, model_name=model_name)
    if sub and sub.tools is not None:
        kit = {name: spec for name, spec in kit.items()
               if name in sub.tools}
    tools = declarations(kit)
    system = system_prompt(
        build, library.whole, skill_index=shelf,
        memories=memories, project=project,
        artifacts=store.list_artifacts(session_id), notes=state.notes,
        user_name=user_name, mode=mode, style=prompt_style(model_name),
        retrieval=library.block, library=library.searchable_names,
        likely=library.likely)
    bus.emit("model_prompt", turn_id=turn_id, n=0, kind="system",
             content=system[:12000],
             **prompt_fingerprint(system, ASSISTANT_VERSION))
    # a task's sub-turn (<parent>.<task>) leaves the parent's compound
    # ask out too: its goal is self-contained, and the whole ask would
    # invite it to do every job at once
    contents = _history(store, session_id, turn_id.split(".", 1)[0])
    # the files ride this ask, before the words: the model reads them
    # as part of the same turn
    contents.append({"role": "user",
                     "parts": [*(attachments or []), {"text": text}]})

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
                                 prior_artifacts
                                 + state.artifacts_touched)),
                             "trace": _trim_trace(trace),
                             "elapsed_ms": round(
                                 (time.perf_counter() - started) * 1000,
                                 1), **extra_payload})
                bus.emit("chips", turn_id=turn_id, clarify=clarify)
                _persist(store, session_id, state, "clarify",
                         clarify["question"],
                         [str(o.get("label", o))
                          if isinstance(o, dict) else str(o)
                          for o in clarify.get("options", [])])
                if sub is None or sub.finish:
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
                                 prior_artifacts
                                 + state.artifacts_touched)),
                             "trace": _trim_trace(trace),
                             "elapsed_ms": round(
                                 (time.perf_counter() - started) * 1000,
                                 1), **extra_payload})
                bus.emit("proposal", turn_id=turn_id,
                         message_id=row["id"], proposal=proposal)
                if chips:
                    bus.emit("chips", turn_id=turn_id, suggestions=chips)
                if (sub is None or sub.title) \
                        and not (session.get("title") or "").strip():
                    title = text.strip()[:60]
                    session["title"] = title
                    store.set_title(session_id, title)
                _persist(store, session_id, state, "proposed", prose,
                         chips)
                if sub is None or sub.finish:
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
    if prose or state.artifacts_touched or extra_payload:
        store.add_message(
            session_id, "assistant", prose, turn_id=turn_id,
            payload={"chips": chips,
                     "artifacts": list(dict.fromkeys(
                         prior_artifacts + state.artifacts_touched)),
                     "trace": _trim_trace(trace),
                     "elapsed_ms": round(
                         (time.perf_counter() - started) * 1000, 1),
                     **extra_payload})
    if chips:
        bus.emit("chips", turn_id=turn_id, suggestions=chips)
    if (sub is None or sub.title) \
            and not (session.get("title") or "").strip() and prose:
        title = text.strip()[:60]
        session["title"] = title
        store.set_title(session_id, title)
    _persist(store, session_id, state, status, prose, chips)
    if sub is None or sub.finish:
        _finish(bus, budget, turn_id, status, started, model_calls=calls,
                steps=steps, thinking_level=thinking_level,
                subgraph_used=state.subgraph,
                skills_loaded=list(state.skills_loaded))
    return status


# ─── a compound ask: the tasks of one turn ───────────────────


class _TaggedBus:
    """The bus a task's sub-turn writes to: every record carries the
    task id, so the page groups it under the task, and a collector
    sees each record so the task's own receipts (artifacts, saved
    rows, checks, refusals, prose, its turn_done) are read off the
    record rather than kept separately."""

    def __init__(self, bus: EventBus, task: str,
                 record: "TaskRecord") -> None:
        self._bus = bus
        self._task = task
        self._record = record

    def emit(self, ev: str, *, turn_id: str = "",
             **fields: Any) -> dict[str, Any]:
        got = self._bus.emit(ev, turn_id=turn_id,
                             task=self._task or None, **fields)
        self._record.observe(got)
        return got

    def __getattr__(self, name: str) -> Any:
        return getattr(self._bus, name)


_SAVED_AS = re.compile(r"saved as (q\d+)")


@dataclass
class TaskRecord:
    """One task's row in the record: what it was asked, how it went,
    and what it left behind — read off the events its sub-turn emitted."""

    id: str
    goal: str
    kind: str = "answer"
    depends_on: list[str] = field(default_factory=list)
    sub_turn: str = ""
    status: str = "planned"       # planned · running · done · partial · failed · stopped
    reason: str = ""
    elapsed_ms: float = 0.0
    model_calls: int = 0
    steps: int = 0
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    saved: list[str] = field(default_factory=list)
    checked: list[str] = field(default_factory=list)
    refused: list[str] = field(default_factory=list)
    say: str = ""
    started_at: str = ""
    done_at: str = ""

    def observe(self, record: dict[str, Any]) -> None:
        ev = record.get("ev")
        if ev == "say_token":
            if len(self.say) < FINDINGS_CAP * 2:
                self.say += str(record.get("delta") or "")
        elif ev == "artifact":
            self.artifacts.append({
                "artifact_id": record.get("artifact_id"),
                "type": record.get("type"), "title": record.get("title"),
                "version": record.get("version")})
        elif ev == "tool_step":
            summary = str(record.get("summary") or "")
            tool = str(record.get("tool") or "")
            if tool == "check":
                self.checked.append(summary[:200])
            if summary.startswith("ERROR"):
                self.refused.append(f"{tool}: {summary[7:].strip()}"[:240])
            for name in _SAVED_AS.findall(summary):
                if name not in self.saved:
                    self.saved.append(name)
        elif ev == "model_prompt" and record.get("kind") == "call":
            self.model_calls += 1
        elif ev == "turn_done":
            self.elapsed_ms = float(record.get("elapsed_ms") or 0)
        if ev == "tool_step":
            self.steps += 1

    def cost(self) -> dict[str, Any]:
        return {"model_calls": self.model_calls, "steps": self.steps,
                "elapsed_ms": round(self.elapsed_ms, 1)}

    def row(self) -> dict[str, Any]:
        """What the events and the transcript carry for this task."""
        return {"id": self.id, "goal": self.goal, "kind": self.kind,
                "depends_on": list(self.depends_on),
                "sub_turn": self.sub_turn, "status": self.status,
                "reason": self.reason, "cost": self.cost(),
                "artifacts": [dict(a) for a in self.artifacts],
                "saved": list(self.saved), "checked": list(self.checked),
                "refused": list(self.refused),
                "say": self.say.strip()[:300]}


_STATUS_OF = {"answered": "done", "proposed": "done", "clarify": "done",
              "partial": "partial", "stopped": "stopped"}


def _findings(rec: TaskRecord) -> str:
    """One task's output as the next task (or the synthesis) reads
    it: the status, the prose, the artifacts and saved rows by id."""
    head = f"### {rec.id} — {rec.goal} ({rec.status}"
    if rec.reason:
        head += f": {rec.reason}"
    head += ")"
    lines = [head]
    prose = rec.say.strip()
    if prose:
        if len(prose) > FINDINGS_CAP:
            prose = prose[:FINDINGS_CAP] + " …[cut]"
        lines.append(prose)
    else:
        lines.append("(nothing was written)")
    if rec.artifacts:
        lines.append("Artifacts already in the panel: " + "; ".join(
            f"{a['artifact_id']} ({a['type']} \"{a['title']}\" "
            f"v{a['version']})" for a in rec.artifacts))
    if rec.saved:
        lines.append("Saved rows: " + ", ".join(rec.saved)
                     + " (python reads them as meridian.rows('<name>'); "
                     "check compares them by name)")
    if rec.checked:
        lines.append("Checks: " + "; ".join(rec.checked))
    if rec.refused:
        lines.append("Refused: " + "; ".join(rec.refused))
    return "\n".join(lines)


def _task_text(task: dict[str, Any],
               inputs: list[TaskRecord]) -> str:
    text = str(task["goal"]).strip()
    if not inputs:
        return text
    return (text + "\n\nThis is one task of a larger ask; the tasks it "
            "builds on are done. Their findings, to build on rather "
            "than redo:\n\n" + "\n\n".join(_findings(r) for r in inputs))


def _synthesis_text(text: str, plan: dict[str, Any],
                    records: list[TaskRecord]) -> str:
    hint = str(plan.get("synthesis") or "").strip()
    lines = [
        "The person asked:",
        f"\"\"\"{text.strip()}\"\"\"",
        "",
        f"I split it into {len(records)} tasks and ran them; what each "
        "found is below. Compose the final answer to the whole ask from "
        "these findings only: do not run queries or tools, do not redo "
        "the work, and never write a number that is not in the findings. "
        "Every number keeps the status and meridian line the task gave "
        "it. Where a task failed, was stopped or came back partial, say "
        "so plainly rather than filling the gap. The artifacts named are "
        "already in the panel: refer to them by title, do not rebuild "
        "them. Answer in the person's words, in markdown, leading with "
        "what they asked for first."]
    if hint:
        lines.append(f"How to put it together: {hint}")
    lines.append("")
    lines.extend(_findings(r) for r in records)
    return "\n".join(lines)


def _secs(ms: float) -> str:
    s = max(0.0, float(ms or 0)) / 1000
    return f"{s:.0f}s" if s >= 10 else f"{s:.1f}s"


def report_markdown(text: str, plan: dict[str, Any],
                    records: list[TaskRecord], *, pool: int,
                    total_calls: int, total_steps: int,
                    elapsed_ms: float, stopped: bool = False,
                    budget_tick: dict[str, Any] | None = None) -> str:
    """The "What was done" document: one row per task — the goal,
    how it ended, what was checked, what it left behind, what it
    cost — honest about refusals, limits, stops and partial results."""
    order = waves([{"id": r.id, "depends_on": r.depends_on}
                   for r in records])
    side = sum(1 for w in order if len(w) > 1 for _ in w)
    lines = [f"# {REPORT_TITLE}", "",
             f"Asked: \"{text.strip()[:300]}\"", "",
             f"Split into {len(records)} tasks in {len(order)} "
             f"{'wave' if len(order) == 1 else 'waves'}: "
             + "; then ".join(", ".join(w) for w in order)
             + f". Up to {pool} tasks ran side by side"
             + (f" ({side} did)" if side else "") + "."]
    if plan.get("repairs"):
        lines.append("Plan repairs: " + "; ".join(
            str(r) for r in plan["repairs"][:6]) + ".")
    if stopped:
        lines.append("**Stopped by you before every task finished.**")
    lines += ["", "| # | Task | Status | Checked | Left behind | Cost |",
              "|---|---|---|---|---|---|"]
    for r in records:
        status = r.status
        if r.reason:
            status += f" — {r.reason}"
        checked = "; ".join(r.checked) if r.checked else "—"
        if r.refused:
            checked += (" · refused: " if r.checked else "refused: ") \
                + "; ".join(r.refused)
        left = "; ".join(
            f"{a['title']} ({a['type']} v{a['version']})"
            for a in r.artifacts)
        if r.saved:
            left += (", " if left else "") + "rows " + ", ".join(r.saved)
        after = f" (after {', '.join(r.depends_on)})" if r.depends_on \
            else ""
        cost = (f"{r.model_calls} calls · {r.steps} steps · "
                f"{_secs(r.elapsed_ms)}")
        cell = lambda s: str(s).replace("|", "\\|").replace("\n", " ")  # noqa: E731
        lines.append(f"| {r.id} | {cell(r.goal)}{after} | {cell(status)} | "
                     f"{cell(checked)} | {cell(left or '—')} | {cost} |")
    unfinished = [r for r in records if r.status != "done"]
    lines.append("")
    if unfinished:
        lines.append("Not finished: " + "; ".join(
            f"{r.id} {r.status}" + (f" ({r.reason})" if r.reason else "")
            for r in unfinished) + ". The answer above says what that "
            "leaves open.")
    else:
        lines.append("Every task finished.")
    tick = budget_tick or {}
    lines.append(
        f"Totals: {total_calls} model calls, {total_steps} tool steps, "
        f"{_secs(elapsed_ms)} on the clock"
        + (f", {tick.get('tokens', 0):,} tokens this session"
           if tick.get("tokens") is not None else "") + ". The same "
        "checks, gates and limits applied inside every task as in any "
        "turn; each number above traces to the task that produced it.")
    return "\n".join(lines)


def run_task_turn(*, build: Build, store: AssistantStore,
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
                  mode: str = DEFAULT_MODE,
                  plane: str = "",
                  attachments: list[dict[str, Any]] | None = None,
                  file_names: list[str] | None = None,
                  owner: str = "",
                  model_label: str = "",
                  model_name: str = "",
                  plan: dict[str, Any] | None = None,
                  pool_size: int = TASK_POOL) -> str:
    """A turn that may be several jobs (docs/multi-task-turns.md).

    The turn is announced, the planner is asked once; with no plan the
    turn runs as one, exactly as ``run_assistant_turn`` would. With a
    plan, each task is a sub-turn of the same session on the same bus,
    budget and abort flag: independent tasks side by side on a small
    pool, dependent ones after their inputs with the finished tasks'
    findings as context, each under a share of the call ceiling. Then
    the "What was done" document is published and a synthesis sub-turn
    composes the final answer from the findings alone. One user
    message, one assistant message per task, the synthesis last."""
    session_id = session["id"]
    started = time.perf_counter()
    mode = mode if mode in MODES else DEFAULT_MODE
    bus.emit("turn_started", turn_id=turn_id, text=text,
             build_id=build.version, version=ASSISTANT_VERSION,
             skills=[s.name for s in (skills or [])],
             memories=len(memories or []),
             project=(project or {}).get("name", ""),
             thinking_level=thinking_level, mode=mode, plane=plane,
             model=model_label, files=list(file_names or []),
             planning=True)
    budget.start_turn()
    prepare_workspace(workspace, build.root)
    common: dict[str, Any] = dict(
        build=build, store=store, budget=budget, abort=abort, model=model,
        session=session, workspace=workspace, skills=skills,
        substrate=substrate, snapshot_runner=snapshot_runner,
        runner=runner, graph_root=graph_root, memories=memories,
        project=project, thinking_level=thinking_level,
        user_name=user_name, mode=mode, plane=plane, owner=owner,
        model_label=model_label, model_name=model_name)

    if plan is None:
        plan = plan_for(model, text, mode=mode, depth=thinking_level)
    bus.emit("budget_tick", turn_id=turn_id, **budget.tick())
    if plan is None:
        # one job after all: the ordinary turn, already announced
        return run_assistant_turn(
            bus=bus, turn_id=turn_id, text=text,
            attachments=attachments, file_names=file_names,
            max_calls=max_calls, wall_seconds=wall_seconds,
            sub_turn=SubTurn(announced=True, finish=True, title=True),
            **common)

    tasks = list(plan["tasks"])
    records = {t["id"]: TaskRecord(
        id=t["id"], goal=t["goal"], kind=t.get("kind", "answer"),
        depends_on=list(t.get("depends_on") or []),
        sub_turn=f"{turn_id}.{t['id']}") for t in tasks}
    bus.emit("plan_made", turn_id=turn_id,
             tasks=[{"id": t["id"], "goal": t["goal"],
                     "kind": t.get("kind", "answer"),
                     "depends_on": list(t.get("depends_on") or [])}
                    for t in tasks],
             synthesis=plan.get("synthesis", ""),
             repairs=list(plan.get("repairs") or []),
             pool=pool_size, waves=waves(tasks))
    if not (session.get("title") or "").strip():
        title = text.strip()[:60]
        session["title"] = title
        store.set_title(session_id, title)

    share = max(MIN_TASK_CALLS,
                (max_calls - SYNTHESIS_CALLS) // max(1, len(tasks)))
    share = min(share, max(1, max_calls - SYNTHESIS_CALLS))

    def remaining_wall() -> float:
        return max(5.0, wall_seconds - (time.perf_counter() - started))

    def run_one(task: dict[str, Any], index: int) -> TaskRecord:
        rec = records[task["id"]]
        inputs = [records[d] for d in rec.depends_on if d in records]
        tagged = _TaggedBus(bus, rec.id, rec)
        t0 = time.perf_counter()
        try:
            status = run_assistant_turn(
                bus=tagged, turn_id=rec.sub_turn,
                text=_task_text(task, inputs),
                max_calls=share, wall_seconds=remaining_wall(),
                sub_turn=SubTurn(
                    task=rec.id, label=rec.goal, announced=False,
                    finish=True, title=False,
                    query_offset=QUERY_STRIDE * index,
                    payload={"task": {"id": rec.id, "goal": rec.goal,
                                      "kind": rec.kind,
                                      "parent": turn_id,
                                      "depends_on": rec.depends_on}}),
                **common)
            rec.status = _STATUS_OF.get(status, "partial")
            if rec.status == "partial":
                rec.reason = "ended early; see its own line"
        except Aborted:
            rec.status = "stopped"
            rec.reason = "you stopped me"
        except ModelUnavailable as e:
            rec.status = "failed"
            rec.reason = f"the model was unreachable: {e}"[:200]
        except Exception as e:                  # noqa: BLE001
            rec.status = "failed"
            rec.reason = f"{type(e).__name__}: {e}"[:200]
        if not rec.elapsed_ms:
            rec.elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        return rec

    def announce_done(rec: TaskRecord) -> None:
        rec.done_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        bus.emit("task_done", turn_id=turn_id, task=rec.id,
                 status=rec.status, reason=rec.reason,
                 sub_turn=rec.sub_turn, cost=rec.cost(),
                 artifacts=[dict(a) for a in rec.artifacts],
                 saved=list(rec.saved), checked=list(rec.checked),
                 refused=list(rec.refused), say=rec.say.strip()[:300])

    # ── the schedule: whatever is ready runs, the pool bounds how
    #    many at once, a finished task frees what depended on it; a
    #    stop starts nothing new and lets the running ones close ──
    queue = list(tasks)
    finished: set[str] = set()
    pending: dict[Any, TaskRecord] = {}
    pool = ThreadPoolExecutor(max_workers=max(1, int(pool_size)),
                              thread_name_prefix=f"task-{turn_id}")
    try:
        while queue or pending:
            if not abort.fired():
                ready = [t for t in queue
                         if all(d in finished for d in
                                (t.get("depends_on") or []))]
                for task in ready:
                    queue.remove(task)
                    rec = records[task["id"]]
                    rec.status = "running"
                    rec.started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                   time.gmtime())
                    bus.emit("task_started", turn_id=turn_id, task=rec.id,
                             goal=rec.goal, kind=rec.kind,
                             depends_on=list(rec.depends_on),
                             sub_turn=rec.sub_turn,
                             n=tasks.index(task) + 1, of=len(tasks),
                             max_calls=share)
                    pending[pool.submit(run_one, task,
                                        tasks.index(task))] = rec
            if not pending:
                break
            done_set, _ = wait(list(pending), return_when=FIRST_COMPLETED)
            for fut in done_set:
                rec = pending.pop(fut)
                try:
                    fut.result()
                except Exception as e:              # noqa: BLE001
                    rec.status = "failed"
                    rec.reason = f"{type(e).__name__}: {e}"[:200]
                finished.add(rec.id)
                announce_done(rec)
    finally:
        pool.shutdown(wait=True)
    for task in queue:
        rec = records[task["id"]]
        rec.status = "stopped" if abort.fired() else "failed"
        rec.reason = ("you stopped me" if abort.fired()
                      else "its inputs never finished")
        announce_done(rec)

    ordered = [records[t["id"]] for t in tasks]
    total_calls = sum(r.model_calls for r in ordered)
    total_steps = sum(r.steps for r in ordered)
    stopped = abort.fired()

    # ── the record: "What was done", a document the person keeps ──
    markdown = report_markdown(
        text, plan, ordered, pool=pool_size, total_calls=total_calls,
        total_steps=total_steps,
        elapsed_ms=(time.perf_counter() - started) * 1000,
        stopped=stopped, budget_tick=budget.tick())
    report_id = ""
    spec, problems = validate_artifact("document", {"markdown": markdown},
                                       build_id=build.version)
    if spec is not None and not problems:
        row = store.add_artifact(session_id, turn_id=turn_id,
                                 type="document", title=REPORT_TITLE,
                                 spec=spec)
        report_id = row["artifact_id"]
        bus.emit("artifact", turn_id=turn_id,
                 artifact_id=row["artifact_id"], version=row["version"],
                 type=row["type"], title=row["title"], spec=row["spec"])
    plan_payload = {"tasks": [r.row() for r in ordered],
                    "synthesis": plan.get("synthesis", ""),
                    "repairs": list(plan.get("repairs") or []),
                    "pool": pool_size, "report": report_id,
                    "stopped": stopped}

    def close_without_model(status: str, closing: str) -> str:
        bus.emit("say_token", turn_id=turn_id, delta=closing)
        store.add_message(
            session_id, "assistant", closing, turn_id=turn_id,
            payload={"chips": [], "artifacts": [report_id] if report_id
                     else [], "trace": [], "plan": plan_payload,
                     "elapsed_ms": round(
                         (time.perf_counter() - started) * 1000, 1)})
        state = AssistantState()
        state.notes = list(session.get("notes") or [])
        _persist(store, session_id, state, status, closing, [])
        _finish(bus, budget, turn_id, status, started,
                model_calls=total_calls, steps=total_steps,
                thinking_level=thinking_level, tasks=len(ordered),
                skills_loaded=[])
        return status

    if stopped:
        done_ids = [r.id for r in ordered if r.status == "done"]
        left = [r.id for r in ordered if r.status != "done"]
        return close_without_model(
            "stopped",
            "You stopped me. "
            + (f"Finished: {', '.join(done_ids)}. " if done_ids else "")
            + (f"Not finished: {', '.join(left)}. " if left else "")
            + f"Each task's own line is above, and \"{REPORT_TITLE}\" "
            "in the panel says what was checked.")

    # ── the synthesis: the answer from the findings, no tools ──
    synthesis_calls = max(1, min(SYNTHESIS_CALLS,
                                 max_calls - total_calls))
    synth = TaskRecord(id="synthesis", goal="the final answer")
    try:
        status = run_assistant_turn(
            bus=_TaggedBus(bus, "", synth), turn_id=turn_id,
            text=_synthesis_text(text, plan, ordered),
            max_calls=synthesis_calls, wall_seconds=remaining_wall(),
            sub_turn=SubTurn(
                task="", label="", announced=True, finish=False,
                title=False, tools=frozenset({"suggest_next"}),
                payload={"plan": plan_payload},
                artifacts=[report_id] if report_id else []),
            **common)
    except ModelUnavailable as e:
        return close_without_model(
            "partial",
            f"I lost the connection to the model before I could put the "
            f"answer together ({e}). Each task's own findings are above, "
            f"and \"{REPORT_TITLE}\" in the panel says what was done.")
    final = "answered" if status in ("answered", "proposed") \
        else ("stopped" if status == "stopped" else "partial")
    _finish(bus, budget, turn_id, final, started,
            model_calls=total_calls + synth.model_calls,
            steps=total_steps + synth.steps,
            thinking_level=thinking_level, tasks=len(ordered),
            skills_loaded=[])
    return final


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


__all__ = ["ASSISTANT_VERSION", "IDENTITY", "THINKING_LEVELS", "DEPTHS",
           "MODE_MEANS",
           "DEFAULT_THINKING", "MODES", "DEFAULT_MODE", "system_prompt",
           "summarize", "run_assistant_turn", "run_proposal_turn",
           "chart_rows_turn", "run_task_turn", "SubTurn", "TaskRecord",
           "report_markdown", "TASK_POOL", "SYNTHESIS_CALLS",
           "MIN_TASK_CALLS", "QUERY_STRIDE", "REPORT_TITLE"]
