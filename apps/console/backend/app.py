"""FastAPI console server — streams the agent loop to the React SPA.

Endpoints:
  GET  /health                 liveness + which runner is active
  POST /chat        (SSE)      stream ConsoleEvents for one user turn
  POST /approve                resolve an open sql_gate (HITL)

Runner selection (env SYNAPSE_CONSOLE_RUNNER):
  "scripted" (default)  offline golden transcripts — no creds, demoable
  "adk"                 real Gemini 3.1 Pro on Vertex (laptop)

The browser talks ONLY to this server. Vertex creds, MCP tools, and the
warehouse gate all live server-side; the frontend is a pure function of
the event stream. CORS is open for local dev — lock it down before any
non-localhost deployment.
"""

from __future__ import annotations

import os
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from apps.console.backend.events import to_sse
from apps.console.backend.runner import ADKRunner, Runner, ScriptedRunner


class ChatRequest(BaseModel):
    message: str
    turn_id: str | None = None


class ApproveRequest(BaseModel):
    gate_id: str
    approved: bool


def _make_runner() -> Runner:
    if os.environ.get("SYNAPSE_CONSOLE_RUNNER", "scripted").lower() == "adk":
        return ADKRunner()
    return ScriptedRunner()


def create_app(runner: Runner | None = None) -> FastAPI:
    app = FastAPI(title="Synapse Console", version="0.1.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"])
    app.state.runner = runner or _make_runner()
    # gate_id → decision, set by /approve, read by the (future) resumable
    # loop. Phase 0 records the decision; the resumable ADK loop consumes
    # it in Phase 3.
    app.state.gate_decisions = {}

    @app.get("/health")
    def health() -> dict:
        return {"ok": True,
                "runner": type(app.state.runner).__name__,
                "model": os.environ.get("GEMINI_MODEL",
                                        "gemini-3.1-pro-preview")}

    @app.post("/chat")
    async def chat(req: ChatRequest) -> StreamingResponse:
        turn_id = req.turn_id or uuid.uuid4().hex[:12]

        async def event_stream():
            async for event in app.state.runner.stream(
                    req.message, turn_id=turn_id):
                yield to_sse(event)

        return StreamingResponse(
            event_stream(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache",
                     "X-Accel-Buffering": "no"})

    @app.post("/approve")
    def approve(req: ApproveRequest) -> dict:
        app.state.gate_decisions[req.gate_id] = req.approved
        return {"gate_id": req.gate_id, "approved": req.approved}

    return app


app = create_app()
