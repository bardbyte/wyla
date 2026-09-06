"""The assistant runtime (Synapse v3): sessions, buses, budgets, and
the worker thread that carries one turn — the ask runtime's shape,
reused for the chat surface. One turn per session at a time; the
model is built at first use (an unconfigured machine gets an honest
error event, never a 500); the stop button and the breaker share one
abort path. Budgets are generous and visible (§5): a wall clock and
a call ceiling in the loop, a session token ceiling here.
"""

from __future__ import annotations

import os
import re
import threading
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable

from sahs.ask.budget import Abort, Budget
from sahs.ask.model import ModelUnavailable
from sahs.ask.runtime import BuildUnavailable, LazyModel, TurnBusy
from sahs.tools.api import Build

from .events import ASSISTANT_EVENTS, EventBus
from .loop import (DEFAULT_MODE, DEFAULT_THINKING, MAX_CALLS, MODES,
                   THINKING_LEVELS, chart_rows_turn, run_assistant_turn,
                   run_proposal_turn)
from .skills_loader import all_skills, load_packs
from .store import AssistantStore

# §5: generous, visible. The loop's own ceilings (MAX_CALLS, the wall
# clock) end a turn in plain language; the session ceiling is the
# breaker behind them. A native-tool turn re-sends its whole context
# on every call, so the turn cap must hold forty calls of a long
# context, not twelve of a short one.
# "/lumi-data-connect how do I …": a slash command names a skill pack
# to load for this turn — the composer's "Type / for skills"
SLASH = re.compile(r"^/([A-Za-z0-9][A-Za-z0-9_\-]*)\s*")

CHAT_BUDGET = {"session_tokens": 6_000_000, "session_calls": 800,
               "turn_tokens": 2_500_000, "turn_calls": MAX_CALLS + 20}


class _SessionRuntime:
    def __init__(self, session_id: str, events_dir: Path | None) -> None:
        path = (events_dir / f"{session_id}.jsonl") if events_dir else None
        self.bus = EventBus(session_id, path,
                            events=ASSISTANT_EVENTS)
        self.budget = Budget(**CHAT_BUDGET)
        self.abort = Abort()
        self.thread: threading.Thread | None = None
        self.current_turn: str = ""

    @property
    def running(self) -> bool:
        return bool(self.thread and self.thread.is_alive())


class AssistantRuntime:
    def __init__(self, *, builds_root: Path, graph_root: Path,
                 store_path: Path, events_dir: Path | None = None,
                 model_factory: Callable[[Budget], Any] | None = None,
                 snapshot_runner: Any = None, runner: Any = None,
                 user_name: str | None = None,
                 substrate: Any = None) -> None:
        self.builds_root = Path(builds_root)
        # the dry-run substrate: None = the laptop's BigQuery; the evals
        # inject a static or fault-injecting one
        self.substrate = substrate
        # memory is bound to the person (§7): the name rides into the
        # prompt's memory section; LUMI_USER_NAME sets it on a laptop
        self.user_name = (user_name if user_name is not None
                          else os.environ.get("LUMI_USER_NAME", "")).strip()
        self.graph_root = Path(graph_root)
        self.events_dir = Path(events_dir) if events_dir else None
        if self.events_dir:
            self.events_dir.mkdir(parents=True, exist_ok=True)
        self.store = AssistantStore(Path(store_path))
        self.snapshot_runner = snapshot_runner
        self.runner = runner          # live rows; None = BQ jobs.query
        self._model_factory = model_factory
        self._runtimes: dict[str, _SessionRuntime] = {}
        self._lock = threading.Lock()
        self._build: Build | None = None
        self._build_stamp: float = -1.0

    # ── the promoted build, mtime-cached ─────────────────────
    def build(self) -> Build:
        current = self.builds_root / "CURRENT"
        stamp = current.stat().st_mtime if current.exists() else -1.0
        if self._build is None or stamp != self._build_stamp:
            if not current.exists():
                raise BuildUnavailable(
                    f"no compiled build: {current} missing. Run "
                    "`python scripts/laptop.py compile` first.")
            self._build = Build.open(self.builds_root)
            self._build_stamp = stamp
        return self._build

    def model_for(self, budget: Budget) -> Any:
        if self._model_factory is not None:
            return self._model_factory(budget)
        from .agent import agent_from_env       # env-bound, late: the
        return agent_from_env(budget)           # plane is the .env's

    def workspace(self, session_id: str) -> Path:
        return (self.graph_root / "runs" / "chat" / "workspaces"
                / session_id)

    # ── sessions ─────────────────────────────────────────────
    def runtime(self, session_id: str) -> _SessionRuntime:
        with self._lock:
            rt = self._runtimes.get(session_id)
            if rt is None:
                rt = _SessionRuntime(session_id, self.events_dir)
                self._runtimes[session_id] = rt
            return rt

    def create_session(self, *, actor: str = "admin") -> dict:
        try:
            build_id = self.build().version
        except BuildUnavailable:
            build_id = ""
        session = self.store.create_session(
            "assistant", build_id=build_id, actor=actor)
        self.runtime(session["id"])
        return session

    def sessions(self, limit: int = 50,
                 include_archived: bool = False) -> list[dict]:
        rows = [r for r in self.store.list_sessions(limit * 2)
                if r.get("kind") == "assistant"
                and (include_archived or not r.get("archived"))
                ][:limit]
        for row in rows:
            rt = self._runtimes.get(row["id"])
            row["running"] = bool(rt and rt.running)
        return rows

    # ── §8 organization: projects, flags, memory panel ───────
    def set_session_project(self, session_id: str,
                            project_id: str) -> dict:
        if self.store.get_session(session_id) is None:
            raise KeyError(session_id)
        if project_id and self.store.get_project(project_id) is None:
            return {"ok": False,
                    "reason": f"no project {project_id}"}
        self.store.set_project(session_id, project_id)
        return {"ok": True, "project_id": project_id}

    def set_session_flag(self, session_id: str, flag: str,
                         on: bool) -> dict:
        if self.store.get_session(session_id) is None:
            raise KeyError(session_id)
        self.store.set_flag(session_id, flag, on)
        return {"ok": True, flag: bool(on)}

    # ── skills: both shelves, browsable (§13.3/V2.7) ─────────
    # the agent loads packs itself by intent; this listing feeds the
    # Skills page where people READ them, full text included
    def skills(self) -> list[dict]:
        from .skills_loader import all_skills
        return [{"name": p.name, "title": p.title,
                 "description": p.description, "origin": p.origin,
                 "text": p.text}
                for p in all_skills(self.graph_root)]

    def set_skills(self, session_id: str, names: list[str]) -> dict:
        from sahs.loop.skills import MAX_LOADED

        from .skills_loader import load_packs
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        if len(names) > MAX_LOADED:
            return {"ok": False,
                    "reason": f"at most {MAX_LOADED} skills load at "
                              "once"}
        loaded, missing = load_packs(self.graph_root, list(names))
        if missing:
            return {"ok": False,
                    "reason": "no such skill: " + ", ".join(missing)}
        self.store.set_skills(session_id, [p.name for p in loaded])
        return {"ok": True, "skills": [p.name for p in loaded]}

    # ── turns ────────────────────────────────────────────────
    @staticmethod
    def thinking_level(depth: str = "") -> str:
        """The depth dial (§5): quick / standard / deep, or a raw
        level; anything else is Standard."""
        key = (depth or "").strip().lower()
        if key in THINKING_LEVELS:
            return THINKING_LEVELS[key]
        if key in THINKING_LEVELS.values():
            return key
        return DEFAULT_THINKING

    @staticmethod
    def mode_for(mode: str = "") -> str:
        """The autonomy slider (§5): chat hands queries over for the
        person to run; autopilot runs and builds without stopping."""
        key = (mode or "").strip().lower()
        return key if key in MODES else DEFAULT_MODE

    @property
    def model_label(self) -> str:
        """The model as the composer names it: the Vertex model id
        prettified (gemini-2.5-pro → Gemini 2.5 Pro); a scripted
        transport says so."""
        if self._model_factory is not None:
            return "scripted"
        from sahs.util.eag import Config, model_plane
        pretty = lambda raw: " ".join(                       # noqa: E731
            w.capitalize() if w.isalpha() else w
            for w in raw.replace("_", "-").split("-") if w)
        if model_plane() == "eag":
            return pretty(Config.from_env().model) + " via EAG"
        from sahs.util.auth import DEFAULT_VERTEX_MODEL
        raw = (os.environ.get("VERTEX_MODEL")
               or os.environ.get("LUMI_VERTEX_MODEL")
               or os.environ.get("GEMINI_MODEL")
               or DEFAULT_VERTEX_MODEL).strip()
        return pretty(raw)

    def slash_skill(self, text: str) -> tuple[str, list[str]]:
        """"/lumi-data-connect how do I …" loads that pack for this
        turn and hands the model the rest; an unknown name stays
        text, so a question that happens to start with / still asks."""
        m = SLASH.match(text or "")
        if not m:
            return text, []
        wanted = m.group(1).lower()
        for pack in all_skills(self.graph_root):
            if pack.name.lower() == wanted:
                rest = text[m.end():].strip()
                return (rest or f"Apply the {pack.name} skill to what "
                                "we were doing."), [pack.name]
        return text, []

    def _model_turn(self, session_id: str, session: dict, rt: Any,
                    build: Build, turn_id: str, text: str, *,
                    depth: str = "", mode: str = "") -> Any:
        """One model turn as a callable: start_turn runs it on a
        thread; a run with dashboard=true chains it after the rows."""
        model = LazyModel(lambda: self.model_for(rt.budget))
        project = self.store.get_project(
            session.get("project_id") or "") \
            if session.get("project_id") else None
        prompt_text, slashed = self.slash_skill(text)
        # the project's pinned packs load with the session's own, and
        # a slash command's pack loads for this turn
        names = list(dict.fromkeys(
            ((project or {}).get("skills") or [])
            + list(session.get("skills") or []) + slashed))
        loaded, _missing = load_packs(self.graph_root, names)
        memories = self.store.list_memories(
            project_id=(project or {}).get("id", ""))
        level = self.thinking_level(depth)
        chosen = self.mode_for(mode)

        def worker() -> None:
            try:
                run_assistant_turn(
                    build=build, store=self.store, bus=rt.bus,
                    budget=rt.budget, abort=rt.abort, model=model,
                    session=session, turn_id=turn_id, text=prompt_text,
                    workspace=self.workspace(session_id),
                    skills=loaded, graph_root=self.graph_root,
                    memories=memories, project=project,
                    snapshot_runner=self.snapshot_runner,
                    runner=self.runner,
                    substrate=self.substrate,
                    thinking_level=level, user_name=self.user_name,
                    mode=chosen)
            except ModelUnavailable as e:
                rt.bus.emit("error", turn_id=turn_id,
                            code="model_unavailable",
                            message="I could not reach the model: "
                                    + str(e),
                            retryable=False,
                            next_actions=[
                                "check the Vertex contract in the "
                                "silo .env",
                                "python scripts/vertex_check.py"])
                rt.bus.emit("turn_done", turn_id=turn_id,
                            status="error", **rt.budget.tick())
            except Exception as e:       # never a silent dead turn
                rt.bus.emit("error", turn_id=turn_id, code="internal",
                            message="Something broke on my side: "
                                    f"{type(e).__name__}: {e}",
                            retryable=True,
                            next_actions=["ask again"])
                rt.bus.emit("error", turn_id=turn_id, code="trace",
                            message=traceback.format_exc(
                                limit=3)[-800:],
                            retryable=False, next_actions=[])
                rt.bus.emit("turn_done", turn_id=turn_id,
                            status="error", **rt.budget.tick())

        return worker

    def start_turn(self, session_id: str, text: str,
                   depth: str = "", mode: str = "") -> dict:
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is already running in this "
                           "session: stop it before sending another")
        build = self.build()
        turn_id = f"t_{uuid.uuid4().hex[:10]}"
        rt.abort = Abort()
        rt.current_turn = turn_id
        self.store.add_message(session_id, "user", text,
                               turn_id=turn_id)
        worker = self._model_turn(session_id, session, rt, build,
                                  turn_id, text, depth=depth, mode=mode)
        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"chat-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id,
                "mode": self.mode_for(mode)}

    def find_proposal(self, session_id: str,
                      message_id: str = "") -> dict:
        """The proposal the person pressed Run on: by message id, or
        the latest one handed over in this chat."""
        for row in reversed(self.store.messages(session_id)):
            payload = row.get("payload") or {}
            if row["role"] != "assistant" or not isinstance(payload, dict):
                continue
            if not payload.get("proposal"):
                continue
            if message_id and row["id"] != message_id:
                continue
            return payload["proposal"]
        raise ValueError("no query has been proposed in this chat yet: "
                         "ask a data question and Synapse hands one over")

    def run_proposal(self, session_id: str, *, message_id: str = "",
                     sql: str = "", limit: int = 200,
                     dashboard: bool = False, depth: str = "") -> dict:
        """The person pressed Run: the query executes under the limits
        with no model call. dashboard=true chains a model turn that
        builds from the rows once they are in."""
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is already running in this "
                           "session: stop it before running a query")
        proposal = self.find_proposal(session_id, message_id)
        build = self.build()
        turn_id = f"t_{uuid.uuid4().hex[:10]}"
        rt.abort = Abort()
        rt.current_turn = turn_id
        written = str(proposal.get("sql_written") or proposal.get("sql")
                      or "")
        edited = bool(sql.strip()) and sql.strip() != written
        label = (f"Run: {proposal.get('title', 'the query')}"
                 + (" (edited)" if edited else "")
                 + (" and build a dashboard" if dashboard else ""))
        self.store.add_message(session_id, "user", label,
                               turn_id=turn_id,
                               payload={"run": {"message_id": message_id,
                                                "dashboard": dashboard,
                                                "edited": edited}})

        def worker() -> None:
            try:
                status = run_proposal_turn(
                    build=build, store=self.store, bus=rt.bus,
                    budget=rt.budget, session=session, turn_id=turn_id,
                    proposal=proposal, sql=sql, limit=limit,
                    workspace=self.workspace(session_id),
                    substrate=self.substrate,
                    snapshot_runner=self.snapshot_runner,
                    runner=self.runner, graph_root=self.graph_root)
            except Exception as e:       # never a silent dead turn
                rt.bus.emit("error", turn_id=turn_id, code="internal",
                            message="Something broke on my side: "
                                    f"{type(e).__name__}: {e}",
                            retryable=True, next_actions=["run again"])
                rt.bus.emit("turn_done", turn_id=turn_id,
                            status="error", **rt.budget.tick())
                return
            if dashboard and status == "answered":
                # the rows are in: the model builds from them, on
                # autopilot, in the same thread so the tab sees one
                # continuous piece of work
                follow = f"t_{uuid.uuid4().hex[:10]}"
                rt.current_turn = follow
                ask = ("Build a dashboard from the rows you just ran "
                       "(saved as q1): the tiles that answer the "
                       "question, each with its own provenance, and "
                       "a line on what they show.")
                self.store.add_message(session_id, "user", ask,
                                       turn_id=follow)
                self._model_turn(session_id, session, rt, build, follow,
                                 ask, depth=depth, mode="autopilot")()

        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"chat-run-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id,
                "dashboard": dashboard, "edited": edited}

    def find_run(self, session_id: str, saved_as: str = "") -> dict:
        """The run whose rows to draw: the one that saved ``saved_as``,
        or the latest run that answered."""
        for row in reversed(self.store.messages(session_id)):
            payload = row.get("payload") or {}
            ran = payload.get("ran") if isinstance(payload, dict) else None
            if not ran or ran.get("status") != "answered" \
                    or not ran.get("saved_as"):
                continue
            if saved_as and ran["saved_as"] != saved_as:
                continue
            return ran
        raise ValueError("no rows to chart yet: run a query first, then "
                         "ask for the picture")

    def chart_rows(self, session_id: str, *, saved_as: str = "",
                   kind: str = "", x: str = "",
                   y: list[str] | None = None) -> dict:
        """The person asked for the picture: the saved rows become a
        chart under the run's provenance, with no model call."""
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is already running in this "
                           "session: stop it before charting")
        ran = self.find_run(session_id, saved_as)
        build = self.build()
        turn_id = f"t_{uuid.uuid4().hex[:10]}"
        rt.abort = Abort()
        rt.current_turn = turn_id
        title = str(ran.get("title") or "the rows")
        self.store.add_message(session_id, "user", f"Chart: {title}",
                               turn_id=turn_id,
                               payload={"chart": {
                                   "saved_as": ran["saved_as"],
                                   "kind": kind, "x": x, "y": y or []}})

        def worker() -> None:
            try:
                chart_rows_turn(
                    build=build, store=self.store, bus=rt.bus,
                    budget=rt.budget, session=session, turn_id=turn_id,
                    saved_as=str(ran["saved_as"]), title=title,
                    provenance=dict(ran.get("provenance") or {}),
                    workspace=self.workspace(session_id), kind=kind,
                    x=x, y=list(y or []), graph_root=self.graph_root)
            except Exception as e:       # never a silent dead turn
                rt.bus.emit("error", turn_id=turn_id, code="internal",
                            message="Something broke on my side: "
                                    f"{type(e).__name__}: {e}",
                            retryable=True, next_actions=["chart again"])
                rt.bus.emit("turn_done", turn_id=turn_id,
                            status="error", **rt.budget.tick())

        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"chat-chart-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id,
                "saved_as": ran["saved_as"]}

    def turn_window(self, session_id: str) -> dict:
        """§6: a turn runs on the server, not in the tab. When the
        page comes back to a session mid-turn, this says where the
        in-flight turn began so the stream replays it whole —
        switching chats or tabs never stops or loses a turn."""
        rt = self._runtimes.get(session_id)
        if rt is None or not rt.running:
            return {"running": False, "turn_id": "", "after": None}
        first = rt.bus.first_seq(rt.current_turn)
        return {"running": True, "turn_id": rt.current_turn,
                "after": (first - 1) if first is not None else None}

    def stop(self, session_id: str) -> dict:
        rt = self.runtime(session_id)
        if not rt.running:
            return {"stopped": False, "reason": "no turn is running"}
        rt.abort.fire("stopped by the analyst")
        return {"stopped": True, "turn_id": rt.current_turn}

    def wait(self, session_id: str, timeout: float = 60.0) -> bool:
        rt = self.runtime(session_id)
        if rt.thread is None:
            return True
        rt.thread.join(timeout)
        return not rt.thread.is_alive()
