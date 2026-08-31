"""Ask (E18) mounted on the Synapse app: sessions, messages, and the
event stream.

The harness runs IN-PROCESS — `sahs.ask.AskRuntime`, not a
subprocess — so a turn is a thread inside this server and the events
it emits are the same objects the browser receives. The frontend is a
pure consumer of that stream; it never calls a model, never holds a
key, and never learns an endpoint.

Paths follow the E18 contract:
    POST /api/sessions                     {kind} → session
    GET  /api/sessions                     → history for the sidebar
    GET  /api/sessions/{id}                → transcript + plan chain
    POST /api/sessions/{id}/messages       {text, choice?} → turn_id
    GET  /api/sessions/{id}/stream         → SSE (meridian.event/1)
    POST /api/sessions/{id}/stop           → cancel server-side
    POST /api/sessions/{id}/plan/restore   → an old plan, as a NEW version
    POST /api/sessions/{id}/feedback       → 👍/👎 on an answer
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from apps.lumi.backend.meridian import _builds_root, _graph_root, _silo_import

router = APIRouter(prefix="/api")

POLL_SECONDS = 0.05          # 50ms: invisible to a reader, trivial to serve
HEARTBEAT_SECONDS = 15.0


def _ask():
    """Import the harness from the silo (same path contract as the
    read plane) and hold one runtime for the process."""
    _silo_import()                       # puts the silo on sys.path
    from sahs.ask import AskRuntime      # noqa: WPS433 (late by design)
    from sahs.ask.events import sse_frame
    global _RUNTIME
    if _RUNTIME is None:
        ask_dir = _graph_root() / "runs" / "ask"
        _RUNTIME = AskRuntime(
            builds_root=_builds_root(), graph_root=_graph_root(),
            store_path=ask_dir / "sessions.sqlite3",
            events_dir=ask_dir / "events")
    return _RUNTIME, sse_frame


_RUNTIME: Any = None


class NewSession(BaseModel):
    kind: str = Field(default="analyst", pattern="^(analyst|steward)$")
    actor: str = "admin"


class NewMessage(BaseModel):
    text: str = Field(min_length=1, max_length=4000)
    choice: dict | None = None       # a chip answer, already structured


class NewFeedback(BaseModel):
    subject: str = Field(default="answer", max_length=40)
    vote: str = Field(pattern="^(up|down)$")
    note: str = Field(default="", max_length=2000)
    turn_id: str = ""


def _unavailable(reason: str) -> dict:
    return {"available": False, "reason": reason}


@router.post("/sessions", status_code=201)
def create_session(req: NewSession) -> dict:
    runtime, _ = _ask()
    session = runtime.create_session(req.kind, actor=req.actor)
    return {"available": True, "session": session}


@router.get("/sessions")
def list_sessions(limit: int = 50) -> dict:
    runtime, _ = _ask()
    return {"available": True, "sessions": runtime.sessions(limit)}


@router.get("/sessions/{session_id}")
def get_session(session_id: str) -> dict:
    runtime, _ = _ask()
    session = runtime.store.get_session(session_id)
    if session is None:
        return _unavailable(f"no session {session_id}")
    rt = runtime.runtime(session_id)
    return {"available": True, "session": session,
            "messages": runtime.store.messages(session_id),
            "plan_versions": runtime.store.plan_versions(session_id),
            "running": rt.running, "head": rt.bus.head(),
            "budget": rt.budget.tick()}


@router.post("/sessions/{session_id}/messages", status_code=202)
def post_message(session_id: str, req: NewMessage) -> dict:
    runtime, _ = _ask()
    from sahs.ask.model import ModelUnavailable
    from sahs.ask.runtime import BuildUnavailable, TurnBusy
    try:
        return {"available": True,
                **runtime.start_turn(session_id, req.text,
                                     choice=req.choice)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except TurnBusy as e:
        return {"available": False, "reason": str(e), "busy": True}
    except (BuildUnavailable, ModelUnavailable) as e:
        return _unavailable(str(e))


@router.post("/sessions/{session_id}/stop")
def stop_turn(session_id: str) -> dict:
    runtime, _ = _ask()
    return {"available": True, **runtime.stop(session_id)}


class RestorePlan(BaseModel):
    version: int = Field(ge=1)


@router.post("/sessions/{session_id}/plan/restore")
def restore_plan(session_id: str, req: RestorePlan) -> dict:
    """Undo as scrubbing, not archaeology. The restored plan is
    appended as the newest version, so the chain never loses a step."""
    runtime, _ = _ask()
    from sahs.ask.runtime import TurnBusy
    if runtime.store.get_session(session_id) is None:
        return _unavailable(f"no session {session_id}")
    try:
        return {"available": True,
                **runtime.restore_plan(session_id, req.version)}
    except TurnBusy as e:
        return {"available": False, "reason": str(e), "busy": True}
    except KeyError as e:
        return _unavailable(str(e).strip("'"))


@router.post("/sessions/{session_id}/feedback", status_code=201)
def session_feedback(session_id: str, req: NewFeedback) -> dict:
    runtime, _ = _ask()
    row = runtime.store.add_feedback(session_id, req.subject, req.vote,
                                     turn_id=req.turn_id, note=req.note)
    return {"available": True, "recorded": row["id"]}


@router.get("/sessions/{session_id}/stream")
async def stream(session_id: str, request: Request, after: int = 0,
                 once: bool = False,
                 last_event_id: str | None = Header(default=None)) -> Any:
    """One stream per session: every turn flows through it. Resume is
    ``Last-Event-ID`` (or ?after=), and because the bus keeps the log,
    a reconnect replays exactly what was missed."""
    runtime, sse_frame = _ask()
    if runtime.store.get_session(session_id) is None:
        return StreamingResponse(
            iter([f"event: error\ndata: "
                  f'{{"reason": "no session {session_id}"}}\n\n']),
            media_type="text/event-stream")
    rt = runtime.runtime(session_id)
    start = int(last_event_id) if (last_event_id or "").isdigit() else after

    async def pump():
        seq = start
        idle = 0.0
        while True:
            if await request.is_disconnected():
                return
            batch = rt.bus.since(seq)
            if batch:
                idle = 0.0
                for record in batch:
                    seq = record["seq"]
                    yield sse_frame(record)
                    if once and record["ev"] == "turn_done":
                        return
            else:
                await asyncio.sleep(POLL_SECONDS)
                idle += POLL_SECONDS
                if idle >= HEARTBEAT_SECONDS:
                    idle = 0.0
                    yield ": keep-alive\n\n"

    return StreamingResponse(
        pump(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache",
                 "Connection": "keep-alive",
                 "X-Accel-Buffering": "no"})
