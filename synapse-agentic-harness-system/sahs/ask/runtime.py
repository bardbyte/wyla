"""The Ask runtime (E18): sessions, buses, budgets, and the worker
thread that carries one turn.

In-process with the app by design — the harness is our loop, not a
subprocess. One turn per session at a time: a second message while a
turn runs is refused with a reason rather than queued invisibly.
"""

from __future__ import annotations

import threading
import uuid
from pathlib import Path
from typing import Any, Callable

from sahs.tools.api import Build

from .budget import Abort, Budget
from .events import EventBus
from .loop import run_turn
from .store import SessionStore


class LazyModel:
    """The model, built at FIRST USE rather than at turn start.

    Two things fall out of that: an unconfigured machine surfaces
    ModelUnavailable as an honest error event at the step that needed
    it (never a 500 from the route), and a turn that only needs the
    resolver — a clarify — completes with no model at all. The
    deterministic half of the product does not depend on Vertex."""

    def __init__(self, factory: Callable[[], Any]) -> None:
        self._factory = factory
        self._model: Any = None

    def _resolve(self) -> Any:
        if self._model is None:
            self._model = self._factory()
        return self._model

    def json(self, *args: Any, **kwargs: Any) -> Any:
        return self._resolve().json(*args, **kwargs)

    def stream(self, *args: Any, **kwargs: Any) -> Any:
        return self._resolve().stream(*args, **kwargs)


class BuildUnavailable(RuntimeError):
    """No promoted build on this machine: every surface says so."""


class TurnBusy(RuntimeError):
    """A turn is already running in this session."""


class _SessionRuntime:
    def __init__(self, session_id: str, events_dir: Path | None) -> None:
        path = (events_dir / f"{session_id}.jsonl") if events_dir else None
        self.bus = EventBus(session_id, path)
        self.budget = Budget()
        self.abort = Abort()
        self.thread: threading.Thread | None = None
        self.current_turn: str = ""

    @property
    def running(self) -> bool:
        return bool(self.thread and self.thread.is_alive())


class AskRuntime:
    def __init__(self, *, builds_root: Path, graph_root: Path,
                 store_path: Path, events_dir: Path | None = None,
                 model_factory: Callable[[Budget], Any] | None = None,
                 snapshot_runner: Any = None) -> None:
        self.builds_root = Path(builds_root)
        self.graph_root = Path(graph_root)
        # the exploratory lane (Agent Loop v1 §9.5) is just the loop
        # with a frozen-extract runner attached; None keeps run_sql's
        # snapshot mode honest ("no frozen snapshot is attached")
        self.snapshot_runner = snapshot_runner
        self.events_dir = Path(events_dir) if events_dir else None
        if self.events_dir:
            self.events_dir.mkdir(parents=True, exist_ok=True)
        self.store = SessionStore(Path(store_path))
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
                    "`python scripts/laptop.py compile` and Ask lights up.")
            self._build = Build.open(self.builds_root)
            self._build_stamp = stamp
        return self._build

    def model_for(self, budget: Budget) -> Any:
        if self._model_factory is not None:
            return self._model_factory(budget)
        from .model import VertexModel          # imported late: env-bound
        return VertexModel.from_env(budget)

    # ── sessions ─────────────────────────────────────────────
    def runtime(self, session_id: str) -> _SessionRuntime:
        with self._lock:
            rt = self._runtimes.get(session_id)
            if rt is None:
                rt = _SessionRuntime(session_id, self.events_dir)
                self._runtimes[session_id] = rt
            return rt

    def create_session(self, kind: str = "analyst", *,
                       actor: str = "admin") -> dict:
        try:
            build_id = self.build().version
        except BuildUnavailable:
            build_id = ""
        session = self.store.create_session(kind, build_id=build_id,
                                            actor=actor)
        self.runtime(session["id"])
        return session

    # ── skills (Agent Loop v1 §2: session.skills) ────────────
    def skills(self) -> list[dict]:
        """The skills available to load: markdown files the analyst
        put in <graph>/skills. Name, title, description — the text
        itself only travels into a session that loads it."""
        from sahs.loop.skills import list_skills
        return [{"name": s.name, "title": s.title,
                 "description": s.description}
                for s in list_skills(self.graph_root)]

    def set_skills(self, session_id: str, names: list[str]) -> dict:
        """Replace the session's loaded skills. Unknown names are
        refused by name — a skill that does not exist cannot be
        silently 'loaded'."""
        from sahs.loop.skills import MAX_LOADED, load_skills
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        if len(names) > MAX_LOADED:
            return {"ok": False,
                    "reason": f"at most {MAX_LOADED} skills load at "
                              "once: choose what matters for this "
                              "session"}
        loaded, missing = load_skills(self.graph_root, list(names))
        if missing:
            return {"ok": False,
                    "reason": "no such skill: " + ", ".join(missing),
                    "missing": missing}
        self.store.set_skills(session_id, [s.name for s in loaded])
        return {"ok": True, "skills": [s.name for s in loaded]}

    def sessions(self, limit: int = 50) -> list[dict]:
        rows = self.store.list_sessions(limit)
        for row in rows:
            rt = self._runtimes.get(row["id"])
            row["running"] = bool(rt and rt.running)
        return rows

    # ── turns ────────────────────────────────────────────────
    def start_turn(self, session_id: str, text: str, *,
                   choice: dict[str, Any] | None = None) -> dict:
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is already running in this session: "
                           "stop it before sending another")
        build = self.build()                     # raises honestly
        turn_id = f"t_{uuid.uuid4().hex[:10]}"
        rt.abort = Abort()
        rt.current_turn = turn_id
        # the session's loaded skills ride the session dict into the
        # turn (Agent Loop v1 §2: Context(..., skills=session.skills))
        from sahs.loop.skills import load_skills
        loaded, _missing = load_skills(self.graph_root,
                                       list(session.get("skills") or []))
        session["_skills_loaded"] = loaded
        self.store.add_message(session_id, "user", text, turn_id=turn_id,
                               payload={"choice": choice} if choice else None)
        model = LazyModel(lambda: self.model_for(rt.budget))

        def worker() -> None:
            run_turn(build=build, store=self.store, bus=rt.bus,
                     budget=rt.budget, abort=rt.abort, model=model,
                     session=session, turn_id=turn_id, text=text,
                     choice=choice,
                     snapshot_runner=self.snapshot_runner)

        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"ask-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id}

    def restore_plan(self, session_id: str, version: int) -> dict:
        """Restore is a NEW version carrying an old one's content, never
        a rewind. The chain stays append-only, so "what did we actually
        ask" remains answerable after any amount of undo, and the
        restore itself is a defensible event rather than a hole."""
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is running: stop it before restoring "
                           "a plan version under it")
        versions = self.store.plan_versions(session_id)
        if not versions:
            raise KeyError(f"no plan versions in {session_id}")
        wanted = next((v for v in versions
                       if int(v["version"]) == int(version)), None)
        if wanted is None:
            raise KeyError(f"no plan version {version} in {session_id}")
        top = versions[-1]
        if int(top["version"]) == int(version):
            return {"restored": False, "version": int(version),
                    "reason": "that version is already the current plan"}
        plan = dict(wanted["plan"])
        parent = int(top["version"])
        plan["version"] = parent + 1
        plan["parent"] = parent
        row = self.store.add_plan_version(
            session_id, plan, parent=parent,
            summary=f"restored from v{version}: {wanted.get('summary', '')}")
        return {"restored": True, "version": int(row["version"]),
                "from_version": int(version), "summary": row["summary"]}

    def stop(self, session_id: str) -> dict:
        """Stop means stop: the same abort path the breaker uses. The
        loop halts between steps, keeps partial state, and the budget
        is charged only for what actually ran."""
        rt = self.runtime(session_id)
        if not rt.running:
            return {"stopped": False, "reason": "no turn is running"}
        rt.abort.fire("stopped by the analyst")
        return {"stopped": True, "turn_id": rt.current_turn}

    def wait(self, session_id: str, timeout: float = 30.0) -> bool:
        """Test/CLI helper: block until the running turn finishes."""
        rt = self.runtime(session_id)
        if rt.thread is None:
            return True
        rt.thread.join(timeout)
        return not rt.thread.is_alive()
