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
from .loop import (DEFAULT_THINKING, MAX_CALLS, THINKING_LEVELS,
                   run_assistant_turn)
from .skills_loader import load_packs
from .store import AssistantStore

# §5: generous, visible. The loop's own ceilings (MAX_CALLS, the wall
# clock) end a turn in plain language; the session ceiling is the
# breaker behind them. A native-tool turn re-sends its whole context
# on every call, so the turn cap must hold forty calls of a long
# context, not twelve of a short one.
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
                 snapshot_runner: Any = None,
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
        from .agent import VertexAgent           # env-bound, late
        return VertexAgent.from_env(budget)

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

    def start_turn(self, session_id: str, text: str,
                   depth: str = "") -> dict:
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
        model = LazyModel(lambda: self.model_for(rt.budget))
        project = self.store.get_project(
            session.get("project_id") or "") \
            if session.get("project_id") else None
        # the project's pinned packs load with the session's own
        names = list(dict.fromkeys(
            ((project or {}).get("skills") or [])
            + list(session.get("skills") or [])))
        loaded, _missing = load_packs(self.graph_root, names)
        memories = self.store.list_memories(
            project_id=(project or {}).get("id", ""))
        level = self.thinking_level(depth)

        def worker() -> None:
            try:
                run_assistant_turn(
                    build=build, store=self.store, bus=rt.bus,
                    budget=rt.budget, abort=rt.abort, model=model,
                    session=session, turn_id=turn_id, text=text,
                    workspace=self.workspace(session_id),
                    skills=loaded, graph_root=self.graph_root,
                    memories=memories, project=project,
                    snapshot_runner=self.snapshot_runner,
                    substrate=self.substrate,
                    thinking_level=level, user_name=self.user_name)
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

        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"chat-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id}

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
