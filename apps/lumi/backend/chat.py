"""Synapse chat (docs/specs/synapse_v3_harness.md) mounted on the app:
the assistant loop, in-process, with the same contract shape as Ask —
the frontend is a pure consumer of the event stream, never holds a
key, never calls a model.

    POST /api/chat/sessions                        → session
    GET  /api/chat/sessions                        → the sidebar
    GET  /api/chat/sessions/{id}                   → transcript + artifacts (+ turn_after when a turn is running)
    POST /api/chat/sessions/{id}/messages          {text, depth?, mode?, model?, files?} → turn_id
    GET  /api/chat/files/support                   what a person may attach, and how it rides
    GET  /api/chat/sessions/{id}/files             the files on this chat (pending and sent)
    POST /api/chat/sessions/{id}/files             {name, data_b64} → the stored file, or why not
    DELETE /api/chat/sessions/{id}/files/{file_id}
    GET  /api/chat/dials                           the modes, depths and planes, explained
    POST /api/chat/sessions/{id}/model             {model} → the plane this chat rides
    POST /api/chat/sessions/{id}/run               {message_id?, sql?, limit?, dashboard?} → turn_id (no model call)
    POST /api/chat/sessions/{id}/chart             {saved_as?, kind?, x?, y?} → turn_id (no model call)
    GET  /api/chat/sessions/{id}/stream            → SSE (meridian.event/1)
    POST /api/chat/sessions/{id}/stop
    POST /api/chat/sessions/{id}/rename            {title}
    GET  /api/chat/skills                          → both shelves
    POST /api/chat/sessions/{id}/skills            {names}
    GET  /api/chat/projects · POST /api/chat/projects
    POST /api/chat/projects/{id}                   {…updates}
    POST /api/chat/sessions/{id}/project           {project_id}
    POST /api/chat/sessions/{id}/star|archive      {on}
    GET  /api/chat/memories[?project_id=]
    POST /api/chat/memories/{id}/retire
    GET  /api/chat/artifacts/{artifact_id}[?version=]
    GET  /api/chat/artifacts/{artifact_id}/versions
    GET  /api/chat/artifacts/{artifact_id}/export.pptx
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from apps.lumi.backend.meridian import (_builds_root, _graph_root,
                                        _silo_import)

router = APIRouter(prefix="/api/chat")

POLL_SECONDS = 0.05
HEARTBEAT_SECONDS = 15.0

_RUNTIME: Any = None


def _chat():
    _silo_import()
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.events import sse_frame
    global _RUNTIME
    if _RUNTIME is None:
        chat_dir = _graph_root() / "runs" / "chat"
        _RUNTIME = AssistantRuntime(
            builds_root=_builds_root(), graph_root=_graph_root(),
            store_path=chat_dir / "sessions.sqlite3",
            events_dir=chat_dir / "events")
    return _RUNTIME, sse_frame


class NewMessage(BaseModel):
    text: str = Field(min_length=1, max_length=8000)
    # the depth dial (v3 §5): quick | standard | deep — thinking level
    depth: str = Field(default="", max_length=12)
    # the autonomy slider (v3 §5): chat hands queries over for the
    # person to run; autopilot runs and builds without stopping
    mode: str = Field(default="", max_length=12)
    # the model switch: vertex | eag, or empty for the chat's own
    model: str = Field(default="", max_length=12)
    # the files that ride this message (ids from POST …/files)
    files: list[str] = Field(default_factory=list, max_length=10)


class FileUpload(BaseModel):
    """One file for the chat: the bytes base64, the name for its type.
    JSON rather than multipart so the laptop needs no extra package;
    a 10 MB file is 14 MB of JSON, within the app's body limit."""
    name: str = Field(min_length=1, max_length=200)
    data_b64: str = Field(min_length=1, max_length=15_000_000)


class SessionModel(BaseModel):
    """The composer's model switch, remembered on the chat."""
    model: str = Field(default="", max_length=12)


class RunProposal(BaseModel):
    """The person pressed Run on a proposed query."""
    message_id: str = Field(default="", max_length=40)
    # the card's SQL as run — edited on the card, or empty for as proposed
    sql: str = Field(default="", max_length=20000)
    limit: int = Field(default=200, ge=1, le=1000)
    dashboard: bool = False
    depth: str = Field(default="", max_length=12)


class ChartRows(BaseModel):
    """The person asked for the picture of a run's rows."""
    saved_as: str = Field(default="", max_length=16)
    kind: str = Field(default="", max_length=12)     # line | bar | area
    x: str = Field(default="", max_length=120)
    y: list[str] = Field(default_factory=list, max_length=6)


class Rename(BaseModel):
    title: str = Field(min_length=1, max_length=120)


class SetSkills(BaseModel):
    names: list[str] = Field(default_factory=list, max_length=8)


class NewProject(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    instructions: str = Field(default="", max_length=4000)
    skills: list[str] = Field(default_factory=list, max_length=4)


class UpdateProject(BaseModel):
    name: str | None = None
    instructions: str | None = None
    skills: list[str] | None = None
    archived: bool | None = None


class SetProject(BaseModel):
    project_id: str = Field(default="", max_length=40)


class Flag(BaseModel):
    on: bool = True


def _unavailable(reason: str) -> dict:
    return {"available": False, "reason": reason}


@router.post("/sessions", status_code=201)
def create_session() -> dict:
    runtime, _ = _chat()
    return {"available": True, "session": runtime.create_session()}


@router.get("/sessions")
def list_sessions(limit: int = 50) -> dict:
    runtime, _ = _chat()
    return {"available": True, "sessions": runtime.sessions(limit)}


@router.get("/search")
def search_chats(q: str = "", limit: int = 200) -> dict:
    """Search across the chats: every session's title and messages,
    fuzzy (a typo still finds it), the matching lines as snippets;
    an empty q lists every chat newest first."""
    runtime, _ = _chat()
    from sahs.assistant.search import search_sessions
    return {"available": True, "query": q,
            "sessions": search_sessions(runtime.sessions(1000),
                                        runtime.store.messages, q,
                                        limit=limit)}


@router.get("/sessions/{session_id}")
def get_session(session_id: str) -> dict:
    runtime, _ = _chat()
    session = runtime.store.get_session(session_id)
    if session is None:
        return _unavailable(f"no session {session_id}")
    rt = runtime.runtime(session_id)
    window = runtime.turn_window(session_id)
    # the chat's plane, configured here or not: the composer shows it
    # greyed with the reason when it is not, and the switch is right
    # beside it
    plane = runtime.plane_of(session)
    return {"available": True, "session": session,
            "messages": runtime.store.messages(session_id),
            "artifacts": runtime.store.list_artifacts(session_id),
            "running": rt.running, "head": rt.bus.head(),
            # an in-flight turn: the page replays it from here
            "turn_id": window["turn_id"], "turn_after": window["after"],
            "budget": rt.budget.tick(),
            # the composer's greeting, and the plane this chat rides
            # with its label
            "user_name": runtime.user_name,
            "plane": plane, "model": runtime.label_for(plane),
            # the files on this chat: the composer shows the pending ones
            "files": runtime.files(session_id)}


@router.get("/files/support")
def file_support() -> dict:
    runtime, _ = _chat()
    return {"available": True, **runtime.file_support()}


@router.get("/sessions/{session_id}/files")
def list_files(session_id: str) -> dict:
    runtime, _ = _chat()
    try:
        return {"available": True, "files": runtime.files(session_id)}
    except KeyError:
        return _unavailable(f"no session {session_id}")


@router.post("/sessions/{session_id}/files", status_code=201)
def add_file(session_id: str, req: FileUpload) -> dict:
    runtime, _ = _chat()
    import base64
    import binascii
    from sahs.assistant.files import FileRefused
    try:
        data = base64.b64decode(req.data_b64, validate=True)
    except (binascii.Error, ValueError):
        return _unavailable("the file bytes were not valid base64")
    try:
        return {"available": True,
                "file": runtime.add_file(session_id, req.name, data)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except FileRefused as e:
        return _unavailable(str(e))


@router.delete("/sessions/{session_id}/files/{file_id}")
def remove_file(session_id: str, file_id: str) -> dict:
    runtime, _ = _chat()
    try:
        gone = runtime.remove_file(session_id, file_id)
    except KeyError:
        return _unavailable(f"no session {session_id}")
    return {"available": True, "removed": gone}


@router.get("/dials")
def dials() -> dict:
    """Everything the composer lets a person set, explained: the
    modes, the depths and the model planes — one source for both
    surfaces, read from the environment each time."""
    runtime, _ = _chat()
    return {"available": True, **runtime.dials()}


@router.post("/sessions/{session_id}/model")
def set_model(session_id: str, req: SessionModel) -> dict:
    runtime, _ = _chat()
    from sahs.ask.model import ModelUnavailable
    try:
        return {"available": True,
                **runtime.set_session_model(session_id, req.model)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except ModelUnavailable as e:
        return _unavailable(str(e))


@router.post("/sessions/{session_id}/messages", status_code=202)
def post_message(session_id: str, req: NewMessage) -> dict:
    runtime, _ = _chat()
    from sahs.ask.model import ModelUnavailable
    from sahs.ask.runtime import BuildUnavailable, TurnBusy
    from sahs.assistant.files import FileRefused
    try:
        return {"available": True,
                **runtime.start_turn(session_id, req.text,
                                     depth=req.depth, mode=req.mode,
                                     model=req.model, files=req.files)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except TurnBusy as e:
        return {"available": False, "reason": str(e), "busy": True}
    except (BuildUnavailable, ModelUnavailable, FileRefused) as e:
        return _unavailable(str(e))


@router.post("/sessions/{session_id}/run", status_code=202)
def run_proposal(session_id: str, req: RunProposal) -> dict:
    """Run a proposed query under the limits with no model call; with
    dashboard=true a model turn builds from the rows afterwards."""
    runtime, _ = _chat()
    from sahs.ask.model import ModelUnavailable
    from sahs.ask.runtime import BuildUnavailable, TurnBusy
    try:
        return {"available": True,
                **runtime.run_proposal(session_id,
                                       message_id=req.message_id,
                                       sql=req.sql, limit=req.limit,
                                       dashboard=req.dashboard,
                                       depth=req.depth)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except ValueError as e:
        return _unavailable(str(e))
    except TurnBusy as e:
        return {"available": False, "reason": str(e), "busy": True}
    except (BuildUnavailable, ModelUnavailable) as e:
        return _unavailable(str(e))


@router.post("/sessions/{session_id}/chart", status_code=202)
def chart_rows(session_id: str, req: ChartRows) -> dict:
    """Draw a run's saved rows as a chart under the run's provenance,
    with no model call."""
    runtime, _ = _chat()
    from sahs.ask.runtime import BuildUnavailable, TurnBusy
    try:
        return {"available": True,
                **runtime.chart_rows(session_id, saved_as=req.saved_as,
                                     kind=req.kind, x=req.x, y=req.y)}
    except KeyError:
        return _unavailable(f"no session {session_id}")
    except ValueError as e:
        return _unavailable(str(e))
    except TurnBusy as e:
        return {"available": False, "reason": str(e), "busy": True}
    except BuildUnavailable as e:
        return _unavailable(str(e))


@router.post("/sessions/{session_id}/stop")
def stop_turn(session_id: str) -> dict:
    runtime, _ = _chat()
    return {"available": True, **runtime.stop(session_id)}


@router.post("/sessions/{session_id}/rename")
def rename(session_id: str, req: Rename) -> dict:
    runtime, _ = _chat()
    if runtime.store.get_session(session_id) is None:
        return _unavailable(f"no session {session_id}")
    runtime.store.set_title(session_id, req.title)
    return {"available": True, "title": req.title[:120]}


@router.get("/skills")
def list_skills() -> dict:
    runtime, _ = _chat()
    return {"available": True, "skills": runtime.skills()}


@router.post("/sessions/{session_id}/skills")
def set_skills(session_id: str, req: SetSkills) -> dict:
    runtime, _ = _chat()
    try:
        return {"available": True,
                **runtime.set_skills(session_id, req.names)}
    except KeyError:
        return _unavailable(f"no session {session_id}")


@router.get("/projects")
def list_projects() -> dict:
    runtime, _ = _chat()
    return {"available": True,
            "projects": runtime.store.list_projects()}


@router.post("/projects", status_code=201)
def create_project(req: NewProject) -> dict:
    runtime, _ = _chat()
    return {"available": True,
            "project": runtime.store.create_project(
                req.name, instructions=req.instructions,
                skills=req.skills)}


@router.post("/projects/{project_id}")
def update_project(project_id: str, req: UpdateProject) -> dict:
    runtime, _ = _chat()
    row = runtime.store.update_project(
        project_id, name=req.name, instructions=req.instructions,
        skills=req.skills, archived=req.archived)
    if row is None:
        return _unavailable(f"no project {project_id}")
    return {"available": True, "project": row}


@router.post("/sessions/{session_id}/project")
def set_session_project(session_id: str, req: SetProject) -> dict:
    runtime, _ = _chat()
    try:
        return {"available": True,
                **runtime.set_session_project(session_id,
                                              req.project_id)}
    except KeyError:
        return _unavailable(f"no session {session_id}")


@router.post("/sessions/{session_id}/star")
def star_session(session_id: str, req: Flag) -> dict:
    runtime, _ = _chat()
    try:
        return {"available": True,
                **runtime.set_session_flag(session_id, "starred",
                                           req.on)}
    except KeyError:
        return _unavailable(f"no session {session_id}")


@router.post("/sessions/{session_id}/archive")
def archive_session(session_id: str, req: Flag) -> dict:
    runtime, _ = _chat()
    try:
        return {"available": True,
                **runtime.set_session_flag(session_id, "archived",
                                           req.on)}
    except KeyError:
        return _unavailable(f"no session {session_id}")


@router.get("/memories")
def list_memories(project_id: str = "") -> dict:
    runtime, _ = _chat()
    return {"available": True,
            "memories": runtime.store.list_memories(
                project_id=project_id)}


@router.post("/memories/{memory_id}/retire")
def retire_memory(memory_id: str) -> dict:
    runtime, _ = _chat()
    return {"available": True,
            "retired": runtime.store.retire_memory(memory_id)}


@router.get("/artifacts/{artifact_id}/export.pptx")
def export_pptx(artifact_id: str, version: int | None = None) -> Any:
    runtime, _ = _chat()
    row = runtime.store.get_artifact(artifact_id, version)
    if row is None:
        return _unavailable(f"no artifact {artifact_id}")
    try:
        from sahs.assistant.export import artifact_pptx
    except ImportError:
        return _unavailable(
            "python-pptx is not installed: "
            "pip install -e '.[assistant]' in the silo")
    name = (row["title"] or row["type"]).lower()
    name = "".join(c if c.isalnum() else "-" for c in name)[:40]
    return Response(
        content=artifact_pptx(row),
        media_type="application/vnd.openxmlformats-officedocument"
                   ".presentationml.presentation",
        headers={"Content-Disposition":
                 f'attachment; filename="{name}.pptx"'})


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: str, version: int | None = None) -> dict:
    runtime, _ = _chat()
    row = runtime.store.get_artifact(artifact_id, version)
    if row is None:
        return _unavailable(f"no artifact {artifact_id}")
    return {"available": True, "artifact": row}


@router.get("/artifacts/{artifact_id}/versions")
def artifact_versions(artifact_id: str) -> dict:
    runtime, _ = _chat()
    return {"available": True,
            "versions": runtime.store.artifact_versions(artifact_id)}


@router.get("/sessions/{session_id}/stream")
async def stream(session_id: str, request: Request, after: int = 0,
                 once: bool = False,
                 last_event_id: str | None = Header(default=None)
                 ) -> Any:
    runtime, sse_frame = _chat()
    if runtime.store.get_session(session_id) is None:
        return StreamingResponse(
            iter([f"event: error\ndata: "
                  f'{{"reason": "no session {session_id}"}}\n\n']),
            media_type="text/event-stream")
    rt = runtime.runtime(session_id)
    start = int(last_event_id) if (last_event_id or "").isdigit() \
        else after

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

    return StreamingResponse(pump(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})
