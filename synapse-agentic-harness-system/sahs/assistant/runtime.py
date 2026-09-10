"""The assistant runtime (Synapse v3): sessions, buses, budgets, and
the worker thread that carries one turn — the ask runtime's shape,
reused for the chat surface. One turn per session at a time; the
model is built at first use (an unconfigured machine gets an honest
error event, never a 500); the stop button and the breaker share one
abort path. Budgets are generous and visible (§5): a wall clock and
a call ceiling in the loop, a session token ceiling here.
"""

from __future__ import annotations

import inspect
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
from .loop import (DEFAULT_MODE, DEFAULT_THINKING, DEPTHS, MAX_CALLS,
                   MODE_MEANS, MODES,
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
        # a factory that takes (budget, plane) hears the composer's
        # switch; the older (budget) shape serves every plane
        self._factory_hears_plane = False
        if model_factory is not None:
            try:
                self._factory_hears_plane = len(
                    inspect.signature(model_factory).parameters) >= 2
            except (TypeError, ValueError):
                self._factory_hears_plane = False
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

    def model_for(self, budget: Budget, plane: str = "") -> Any:
        if self._model_factory is not None:
            if self._factory_hears_plane:
                return self._model_factory(budget, plane)
            return self._model_factory(budget)
        from .agent import agent_for            # env-bound, late: the
        return agent_for(plane, budget)         # .env is the switchboard

    # ── the planes and the dials, as the composer shows them ──
    def planes(self) -> list[dict[str, Any]]:
        from .agent import plane_catalog
        return plane_catalog()

    @staticmethod
    def plane_of(session: dict[str, Any] | None) -> str:
        """The plane a chat is on: its remembered switch, else the
        .env default — the id, whether or not this machine can ride
        it (the composer shows it greyed when it cannot)."""
        from sahs.util.eag import model_plane
        return (((session or {}).get("model") or "").strip().lower()
                or model_plane())

    def plane_for(self, session: dict[str, Any] | None,
                  wanted: str = "") -> str:
        """The plane a turn rides: the composer's choice for this
        message, else the chat's remembered one, else the .env
        default. A name that is not a plane, or a plane this machine
        cannot ride, is a typed refusal with the reason — never a
        silent fallback to a different model than the one picked."""
        plane = (wanted or "").strip().lower() or self.plane_of(session)
        rows = {row["id"]: row for row in self.planes()}
        if plane not in rows:
            raise ModelUnavailable(f"no model plane called {plane!r}: "
                                   "the planes are vertex and eag")
        if not rows[plane]["available"] and self._model_factory is None:
            raise ModelUnavailable(
                f"the {rows[plane]['label']} plane is not configured on "
                f"this machine: {rows[plane]['reason']}")
        return plane

    def label_for(self, plane: str = "") -> str:
        """The model as the composer names it, for a plane: "Gemini
        2.5 Pro" (the plane is the catalog's business, not the
        label's); a scripted transport says so."""
        if self._model_factory is not None:
            return "scripted"
        from sahs.util.eag import model_plane
        plane = (plane or "").strip().lower() or model_plane()
        for row in self.planes():
            if row["id"] == plane:
                return row["label"]
        return plane

    def set_session_model(self, session_id: str, plane: str) -> dict:
        """The composer's model switch: remembered on the chat, so it
        rides the next message and survives a reload. '' forgets it."""
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        plane = (plane or "").strip().lower()
        if plane:
            plane = self.plane_for(session, plane)     # validated
        self.store.set_model(session_id, plane)
        session["model"] = plane
        now = self.plane_of(session)
        return {"ok": True, "plane": now, "model": self.label_for(now)}

    # ── files on a chat (the composer's Add files) ────────────
    def files(self, session_id: str) -> list[dict[str, Any]]:
        from . import files as files_mod
        if self.store.get_session(session_id) is None:
            raise KeyError(session_id)
        return files_mod.manifest(self.workspace(session_id))

    def add_file(self, session_id: str, name: str,
                 data: bytes) -> dict[str, Any]:
        """Keep a file on the chat for its next message; a refusal
        (type, size, a deck without its reader) is the reason."""
        from . import files as files_mod
        if self.store.get_session(session_id) is None:
            raise KeyError(session_id)
        return files_mod.store(self.workspace(session_id), name,
                               data).row()

    def remove_file(self, session_id: str, file_id: str) -> bool:
        from . import files as files_mod
        if self.store.get_session(session_id) is None:
            raise KeyError(session_id)
        return files_mod.remove(self.workspace(session_id), file_id)

    @staticmethod
    def file_support() -> dict[str, Any]:
        from . import files as files_mod
        return {"accepted": files_mod.support_table(),
                "not_offered": [{"suffix": k, "reason": v}
                                for k, v in files_mod.NOT_OFFERED.items()],
                "max_file_mb": files_mod.MAX_FILE_BYTES // 1048576,
                "max_inline_mb": files_mod.MAX_INLINE_BYTES // 1048576,
                "max_files_per_message": files_mod.MAX_FILES_PER_TURN,
                "note": "A file rides the message it is sent with; the "
                        "conversation remembers that it was sent, not "
                        "its bytes. Attach it again to ask more."}

    # ── memory.md: what Synapse remembers, as a document a person
    #    can read and edit; a line is a memory ──────────────────
    MEMORY_HEAD = ("# What Synapse remembers about {name}\n\n"
                   "One line per memory: a preference or a choice you "
                   "settled in chat. Edit the list and save — a line you "
                   "add is remembered, a line you remove is retired. "
                   "Never a metric definition or a number: those live in "
                   "the graph.\n\n")

    def memory_markdown(self) -> dict[str, Any]:
        rows = self.store.list_memories()
        name = self.user_name or "you"
        lines = [self.MEMORY_HEAD.format(name=name)]
        lines.append("## Everywhere\n")
        everywhere = [m for m in rows if m.get("scope") == "global"]
        lines.extend(f"- {m['text']}" for m in everywhere)
        if not everywhere:
            lines.append("<!-- nothing remembered yet: settle a preference "
                         "in chat, or add a line here -->")
        return {"text": "\n".join(lines).rstrip() + "\n",
                "count": len(everywhere)}

    def save_memory_markdown(self, text: str) -> dict[str, Any]:
        """The document back to rows: bullets are the memories; a line
        no longer present retires its memory, a new line becomes one.
        Matching is by the text, which is what a memory is."""
        import re
        wanted: list[str] = []
        for line in (text or "").splitlines():
            m = re.match(r"^\s*[-*]\s+(.+?)\s*$", line)
            if m and not m.group(1).startswith("<!--"):
                item = m.group(1).strip()[:400]
                if item and item not in wanted:
                    wanted.append(item)
        current = {m["text"]: m for m in self.store.list_memories()
                   if m.get("scope") == "global"}
        added, retired = 0, 0
        for item in wanted:
            if item not in current:
                self.store.add_memory(item, scope="global", source="person")
                added += 1
        for item, row in current.items():
            if item not in wanted:
                self.store.retire_memory(row["id"])
                retired += 1
        out = self.memory_markdown()
        return {"added": added, "retired": retired, **out}

    def dials(self) -> dict[str, Any]:
        """Everything the composer lets a person set, explained in one
        place: the modes, the depths (with what each does on each
        plane), and the planes. One source for both surfaces."""
        from sahs.util.eag import thinking_budgets
        budgets = thinking_budgets()
        depths = []
        for key, row in DEPTHS.items():
            level = THINKING_LEVELS[key]
            depths.append({
                "id": key, "label": row["label"], "level": level,
                "means": row["means"],
                "on": {"vertex": f"thinking level {level}",
                       "eag": f"{budgets.get(level, 0):,} thinking "
                              "tokens per call"},
                "default": level == DEFAULT_THINKING})
        modes = [{"id": key, "label": row["label"], "means": row["means"],
                  "default": key == DEFAULT_MODE}
                 for key, row in MODE_MEANS.items()]
        return {"modes": modes, "depths": depths, "planes": self.planes(),
                "notes": {
                    "depth": "Depth changes how much the model thinks "
                             "before each step, nothing else: the call "
                             f"ceiling ({MAX_CALLS} per turn) and the "
                             "clock are the same at every depth.",
                    "plane": "A switch applies from the next message. "
                             "The conversation carries over as text, so "
                             "a chat can change model mid-way."}}

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
    @property
    def owner(self) -> str:
        """Whose own packs load: the configured person today, the
        signed-in one once identity lands (the same seam)."""
        from .skills_loader import owner_slug
        return owner_slug(self.user_name) or "anon"

    def skills(self) -> list[dict]:
        from .skills_loader import all_skills, author_of
        return [{"name": p.name, "title": p.title,
                 "description": p.description, "origin": p.origin,
                 "owner": p.owner, "mine": bool(p.owner),
                 "updated": p.updated,
                 # the author as the shelf shows it: Synapse for what
                 # ships with the assistant, You for your own, and a
                 # shared pack's own word for itself (an author line)
                 "author": ("Synapse" if p.origin == "built-in"
                            else "You" if p.owner
                            else author_of(p.text) or "Shared"),
                 "text": p.text}
                for p in all_skills(self.graph_root, self.owner)]

    # ── authoring: a skill or a knowledge file, drafted and saved ──
    def draft(self, kind: str, title: str, material: str,
              hint: str = "", plane: str = "") -> dict:
        """The model rewrites the person's material into the house
        format; the person reads it before anything is saved."""
        from . import authoring
        if kind not in authoring.KINDS:
            return {"ok": False, "reason": "kind is skill or knowledge"}
        agent = self.model_for(Budget(**CHAT_BUDGET),
                               self.plane_for(None, plane))
        return authoring.draft(agent, kind, title, material, hint)

    def save_my_skill(self, name: str, text: str) -> dict:
        from . import authoring
        return authoring.save_skill(self.graph_root, self.owner, name, text)

    def delete_my_skill(self, name: str) -> bool:
        from . import authoring
        gone = authoring.delete_skill(self.graph_root, self.owner, name)
        # a published submission whose file is gone is withdrawn too
        sub = self.reviews.find("skill", authoring.slug(name), self.owner)
        if sub is not None and sub["status"] == "published":
            self.reviews.withdraw(sub["id"], by=self.user_name)
        return gone

    # ── the approval workflow (the PRD): a submission goes to the
    #    manager, with the model's read, before the agent sees it ──
    # where approved knowledge files land: a folder, or a callable the
    # app gives so the answer follows its configuration at publish time
    knowledge_dir: Any = None

    @property
    def reviews(self) -> Any:
        from .reviews import Reviews
        if getattr(self, "_reviews", None) is None:
            self._reviews = Reviews(self.graph_root / "runs" / "reviews")
        return self._reviews

    def submit_for_review(self, **fields: Any) -> dict:
        """A skill or a knowledge file from the person, filed for their
        manager: pending from this moment, the checks at once, the
        model's read in the background. Nothing reaches the loader."""
        from .reviews import approver_for
        from .skills_loader import builtin_skills
        got = self.reviews.submit(
            submitter=self.user_name or "you", submitter_slug=self.owner,
            approver=approver_for(self.user_name),
            reserved={p.name for p in builtin_skills()}, **fields)
        if got.get("ok"):
            sid = got["submission"]["id"]
            self.reviews.start_ai(sid, self._read_for_review)
            got["submission"] = self.reviews.get(sid)
        return got

    def _read_for_review(self, sub: dict, text: str) -> dict:
        from . import reviews as reviews_mod
        try:
            plane = self.plane_for(None, "")
            agent = self.model_for(Budget(**CHAT_BUDGET), plane)
        except ModelUnavailable as e:
            return {"ok": False, "reason": f"the model is unavailable: {e}"}
        return reviews_mod.ai_review(agent, sub["kind"], sub.get("title", ""),
                                     text, purpose=sub.get("purpose", ""),
                                     by=self.label_for(plane))

    def resubmit_review(self, sid: str, text: str, *, description: str = "",
                        purpose: str = "", comment: str = "") -> dict:
        got = self.reviews.resubmit(sid, text=text, description=description,
                                    purpose=purpose, by=self.user_name,
                                    comment=comment)
        if got.get("ok"):
            self.reviews.start_ai(sid, self._read_for_review)
            got["submission"] = self.reviews.get(sid)
        return got

    def decide_review(self, sid: str, decision: str,
                      comment: str = "") -> dict:
        return self.reviews.decide(sid, decision, comment=comment,
                                   publish=self._publish_submission)

    def _publish_submission(self, sub: dict, text: str) -> dict:
        """Approval opens the door the file was waiting at: an own pack
        on the shelf for its owner, or a knowledge file staged for its
        business unit — the same two places the creators wrote to."""
        from . import authoring
        if sub["kind"] == "skill":
            # the owner's folder is the submitter's slug (a slug slugs
            # to itself), so it lists for the person who filed it
            got = authoring.save_skill(self.graph_root,
                                       sub.get("submitter_slug") or self.owner,
                                       sub["name"], text)
            return {"ok": bool(got.get("ok")), "reason": got.get("reason", ""),
                    "path": got.get("path", "")}
        root = self.knowledge_dir() if callable(self.knowledge_dir) \
            else self.knowledge_dir
        root = Path(root) if root else (
            self.graph_root.parent / "sources" / "artifacts")
        root.mkdir(parents=True, exist_ok=True)
        ext = sub.get("ext") or "md"
        path = root / f"{sub['business_unit'].lower()}_{sub['name']}.{ext}"
        header = (f"<!-- staged via Synapse by Lumi · actor {sub['submitter']} "
                  f"· business unit {sub['business_unit']} · approved by "
                  f"{(sub.get('approver') or {}).get('name', '')} -->\n"
                  if ext == "md" else "")
        path.write_text(header + text, encoding="utf-8")
        return {"ok": True, "path": str(path)}

    def withdraw_review(self, sid: str) -> dict:
        return self.reviews.withdraw(sid, by=self.user_name)

    def review_board(self) -> dict:
        """The Skills page's view of the workflow: every submission
        without its text, the notices, who approves and the band rule."""
        from dataclasses import asdict

        from .reviews import MIN_BAND, approver_for
        board = self.reviews.notices(self.owner)
        return {"submissions": self.reviews.list(),
                "approver": asdict(approver_for(self.user_name)),
                "min_band": MIN_BAND, "me": self.user_name, **board}

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
        loaded, missing = load_packs(self.graph_root, list(names),
                                     owner=self.owner)
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
        """The model a new chat starts on, as the composer names it
        ("Gemini 2.5 Pro"); a scripted transport says so."""
        return self.label_for("")

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
                    depth: str = "", mode: str = "",
                    plane: str = "",
                    attachments: list[dict] | None = None,
                    file_names: list[str] | None = None) -> Any:
        """One model turn as a callable: start_turn runs it on a
        thread; a run with dashboard=true chains it after the rows."""
        model = LazyModel(lambda: self.model_for(rt.budget, plane))
        project = self.store.get_project(
            session.get("project_id") or "") \
            if session.get("project_id") else None
        prompt_text, slashed = self.slash_skill(text)
        # the project's pinned packs load with the session's own, and
        # a slash command's pack loads for this turn
        names = list(dict.fromkeys(
            ((project or {}).get("skills") or [])
            + list(session.get("skills") or []) + slashed))
        loaded, _missing = load_packs(self.graph_root, names,
                                      owner=self.owner)
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
                    mode=chosen, plane=plane,
                    attachments=attachments or [],
                    file_names=file_names or [],
                    owner=self.owner)
            except ModelUnavailable as e:
                rt.bus.emit("error", turn_id=turn_id,
                            code="model_unavailable",
                            message="I could not reach the model: "
                                    + str(e),
                            retryable=False,
                            next_actions=[
                                "check the EAG contract in the silo "
                                ".env" if plane == "eag" else
                                "check the Vertex contract in the "
                                "silo .env",
                                "python scripts/eag_check.py"
                                if plane == "eag" else
                                "python scripts/vertex_check.py",
                                "or switch the model in the composer"])
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
                   depth: str = "", mode: str = "",
                   model: str = "",
                   files: list[str] | None = None) -> dict:
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        rt = self.runtime(session_id)
        if rt.running:
            raise TurnBusy("a turn is already running in this "
                           "session: stop it before sending another")
        build = self.build()
        # the plane is settled before anything is stored: a refusal
        # (an unconfigured plane) leaves the chat as it was
        plane = self.plane_for(session, model)
        if (model or "").strip().lower() and plane != (
                session.get("model") or ""):
            self.store.set_model(session_id, plane)   # remembered
        # the files ride this message: their parts are built before
        # anything is stored, so an over-budget attachment is refused
        # with the reason and the chat stays as it was
        from . import files as files_mod
        attachments: list[dict] = []
        used: list[dict] = []
        if files:
            attachments, used = files_mod.parts_for(
                self.workspace(session_id), list(files))
        turn_id = f"t_{uuid.uuid4().hex[:10]}"
        rt.abort = Abort()
        rt.current_turn = turn_id
        names = [r["name"] for r in used]
        self.store.add_message(
            session_id, "user", text, turn_id=turn_id,
            payload={"files": [{"id": r["id"], "name": r["name"],
                                "family": r["family"], "size": r["size"],
                                "rides": r["rides"]} for r in used]}
            if used else None)
        if used:
            files_mod.mark_sent(self.workspace(session_id),
                                [r["id"] for r in used], turn_id)
        worker = self._model_turn(session_id, session, rt, build,
                                  turn_id, text, depth=depth, mode=mode,
                                  plane=plane, attachments=attachments,
                                  file_names=names)
        rt.thread = threading.Thread(target=worker, daemon=True,
                                     name=f"chat-{turn_id}")
        rt.thread.start()
        return {"turn_id": turn_id, "session_id": session_id,
                "mode": self.mode_for(mode), "plane": plane,
                "files": names}

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
                                 ask, depth=depth, mode="autopilot",
                                 plane=self.plane_for(session, ""))()

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
