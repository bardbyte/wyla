"""FastAPI console server — one process serving the Radix SPA, the
agent stream, and the read-side API.

Endpoints:
  GET  /health                    liveness + active runner + model
  GET  /api/config                the (non-secret) environment contract
  POST /chat            (SSE)     stream ConsoleEvents for one turn
  POST /approve                   resolve an open sql_gate (HITL)
  GET  /api/products              data products + readiness scorecards
  GET  /api/metrics               the metric canon
  POST /api/metrics/viability     canon-first check for a metric draft
  GET  /api/terms/resolve         canonical term + witnesses
  GET  /api/graph/summary         node/edge/tier/witness counts
  GET  /api/graph/thread          the curated one-thread storyline
  GET  /api/briefs, /api/briefs/{id}
  GET  /api/questions             suggested (verified-answerable) questions
  GET  /api/witness?ref=          evidence panel behind any chip
  GET  /                          the built SPA, when frontend/dist exists

Runner selection (env SYNAPSE_CONSOLE_RUNNER):
  "scripted" (default)  offline golden transcripts — no credentials
  "adk"                 Gemini 3.1 Pro on Vertex, same env contract as
                        the pipeline (GEMINI_MODEL, GEMINI_THINKING_BUDGET,
                        GEMINI_TLS_INSECURE / GEMINI_CA_BUNDLE, …)

The browser talks ONLY to this server. Vertex credentials, tools, and
the warehouse gates all live server-side; the frontend is a pure
function of the event stream. CORS is open for local dev — lock it
down before any non-localhost deployment.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from apps.console.backend.data import ConsoleData
from apps.console.backend.events import to_sse
from apps.console.backend.runner import ADKRunner, Runner, ScriptedRunner

_FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"


class ChatRequest(BaseModel):
    message: str
    turn_id: str | None = None
    conversation_id: str | None = None     # multi-turn memory (ADK)


class ApproveRequest(BaseModel):
    gate_id: str
    approved: bool


class ViabilityRequest(BaseModel):
    name: str
    description: str = ""


def _make_runner() -> Runner:
    if os.environ.get("SYNAPSE_CONSOLE_RUNNER", "scripted").lower() == "adk":
        return ADKRunner()
    return ScriptedRunner()


def _configure_tls() -> dict:
    """Apply the SAME TLS posture as the pipeline, at server startup —
    the console must not rediscover the corporate-proxy lesson the hard
    way. Returns the detail dict for /api/config. Never raises."""
    try:
        from synapse.enrichment.vertex_client import _apply_tls, _tls_mode
        mode = _tls_mode()
        detail = _apply_tls(mode)
        return {"mode": mode, **{k: v for k, v in (detail or {}).items()
                                 if isinstance(v, (str, bool, int))}}
    except Exception as exc:
        return {"mode": "default", "note": f"tls setup skipped: {exc}"}


def create_app(runner: Runner | None = None,
               data: ConsoleData | None = None) -> FastAPI:
    app = FastAPI(title="Radix Console", version="0.3.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"])
    app.state.runner = runner or _make_runner()
    app.state.data = data or ConsoleData()
    app.state.tls = (_configure_tls()
                     if isinstance(app.state.runner, ADKRunner)
                     else {"mode": "n/a (scripted)"})
    # gate_id → decision, set by /approve; the resumable loop consumes it
    app.state.gate_decisions = {}

    # ── agent stream ─────────────────────────────────────────

    @app.get("/health")
    def health() -> dict:
        return {"ok": True,
                "runner": type(app.state.runner).__name__,
                "model": os.environ.get("GEMINI_MODEL",
                                        "gemini-3.1-pro-preview")}

    @app.get("/api/config")
    def config() -> dict:
        """The environment contract, echoed without secrets: booleans
        for anything credential-shaped, values only for tuning knobs.
        The sdk block reports what THIS PROCESS actually imported —
        version-mismatch bugs are invisible without it."""
        import sys
        from importlib import metadata

        def _ver(pkg: str) -> str | None:
            try:
                return metadata.version(pkg)
            except Exception:
                return None

        env = os.environ.get
        return {
            "sdk": {
                "python": sys.version.split()[0],
                "google_adk": _ver("google-adk"),
                "google_genai": _ver("google-genai"),
                "fastapi": _ver("fastapi"),
            },
            "runner": type(app.state.runner).__name__,
            "model": env("GEMINI_MODEL", "gemini-3.1-pro-preview"),
            "model_flash": env("GEMINI_MODEL_FLASH", ""),
            "thinking_budget": env("GEMINI_THINKING_BUDGET", "-1"),
            "max_context_chars": env("GEMINI_MAX_CONTEXT_CHARS", "400000"),
            "vertexai": env("GOOGLE_GENAI_USE_VERTEXAI", "") in
                        ("1", "true", "True"),
            "project_set": bool(env("GOOGLE_CLOUD_PROJECT")),
            "location": env("GOOGLE_CLOUD_LOCATION", ""),
            "credentials_set": bool(env("GOOGLE_APPLICATION_CREDENTIALS")),
            "tls": app.state.tls,
            "graph": {"path": str(app.state.data.snapshot_path),
                      "live": app.state.data.live},
        }

    @app.post("/chat")
    async def chat(req: ChatRequest) -> StreamingResponse:
        turn_id = req.turn_id or uuid.uuid4().hex[:12]
        conversation_id = req.conversation_id or turn_id

        async def event_stream():
            async for event in app.state.runner.stream(
                    req.message, turn_id=turn_id,
                    conversation_id=conversation_id):
                yield to_sse(event)

        return StreamingResponse(
            event_stream(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache",
                     "X-Accel-Buffering": "no"})

    @app.post("/approve")
    def approve(req: ApproveRequest) -> dict:
        app.state.gate_decisions[req.gate_id] = req.approved
        return {"gate_id": req.gate_id, "approved": req.approved}

    # ── read-side API (graph-backed or labeled sample) ───────

    @app.get("/api/products")
    def products(q: str = "") -> dict:
        return app.state.data.products(q)

    @app.get("/api/metrics")
    def metrics(q: str = "") -> dict:
        return app.state.data.metrics(q)

    @app.post("/api/metrics/viability")
    def viability(req: ViabilityRequest) -> dict:
        return app.state.data.metric_viability(req.name, req.description)

    @app.get("/api/terms/resolve")
    def resolve(term: str) -> dict:
        return app.state.data.resolve_term(term)

    @app.get("/api/graph/summary")
    def graph_summary() -> dict:
        return app.state.data.graph_summary()

    @app.get("/api/graph/thread")
    def graph_thread(table: str = "") -> dict:
        return app.state.data.graph_thread(table)

    @app.get("/api/questions")
    def questions() -> dict:
        return app.state.data.questions()

    @app.get("/api/witness")
    def witness(ref: str) -> dict:
        return app.state.data.witness(ref)

    # ── the SPA (after API routes, so /api wins) ─────────────

    if _FRONTEND_DIST.exists():
        from fastapi.staticfiles import StaticFiles
        app.mount("/", StaticFiles(directory=str(_FRONTEND_DIST),
                                   html=True), name="spa")

    return app


app = create_app()
