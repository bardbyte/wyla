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
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from apps.console.backend.data import ConsoleData
from apps.console.backend.events import to_sse
from apps.console.backend.pins import (
    NoSqlError, PinStore,
)
from apps.console.backend.evaluator import EvalLog, TurnEvaluator
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


class PinCreateRequest(BaseModel):
    question: str
    answer: str = ""
    citations: list[dict] = []
    sql: str | None = None
    rows: list[dict] | None = None
    ledger_id: str | None = None
    actor: str = "user"
    source: str = "live"


class PinActorRequest(BaseModel):
    actor: str = "user"


class PinVerifyRequest(BaseModel):
    verified: bool = True
    actor: str = "steward"


def _default_warehouse_factory():
    """The gated warehouse runner the agent itself uses — pins replay
    through the SAME chain. Missing snapshot/import → None, and reruns
    return a structured no_graph refusal instead of skipping gates."""
    try:
        from apps.analyst.tools import _runner
        return _runner()
    except Exception:
        return None


def _make_runner() -> Runner:
    """LIVE AGENT BY DEFAULT. The scripted demo answered every question
    with a canned transcript, and an unset env var made that the silent
    default — a canned answer that looks real is worse than an error
    that names its fix. The demo is now an explicit opt-in
    (SYNAPSE_CONSOLE_RUNNER=scripted), and the UI banners it."""
    if os.environ.get("SYNAPSE_CONSOLE_RUNNER",
                      "adk").lower() == "scripted":
        return ScriptedRunner()
    return ADKRunner()


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
               data: ConsoleData | None = None, *,
               pins: PinStore | None = None,
               warehouse_factory=None) -> FastAPI:
    app = FastAPI(title="Radix Console", version="0.3.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"])
    app.state.runner = runner or _make_runner()
    app.state.data = data or ConsoleData()
    app.state.pins = pins or PinStore(
        tier_resolver=app.state.data.tier_for)
    app.state.warehouse_factory = (warehouse_factory
                                   or _default_warehouse_factory)
    app.state.tls = (_configure_tls()
                     if isinstance(app.state.runner, ADKRunner)
                     else {"mode": "n/a (scripted)"})
    # gate_id → decision, set by /approve; the resumable loop consumes it
    app.state.gate_decisions = {}
    # every turn scored the moment it completes, from the same events
    # the UI rendered — the /api/evals feed
    app.state.evals = EvalLog(TurnEvaluator(app.state.data))

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
            recorded: list[dict] = []
            async for event in app.state.runner.stream(
                    req.message, turn_id=turn_id,
                    conversation_id=conversation_id):
                try:
                    recorded.append(event.model_dump(mode="python"))
                except Exception:
                    pass  # evals never get to break the stream
                yield to_sse(event)
            # score the finished turn; failures are invisible to the UI
            try:
                app.state.evals.record(turn_id, req.message, recorded)
            except Exception:
                pass

        return StreamingResponse(
            event_stream(), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache",
                     "X-Accel-Buffering": "no"})

    @app.post("/approve")
    def approve(req: ApproveRequest) -> dict:
        app.state.gate_decisions[req.gate_id] = req.approved
        return {"gate_id": req.gate_id, "approved": req.approved}

    @app.get("/api/evals/recent")
    def evals_recent() -> dict:
        """Every recent turn, scored: deterministic checks with
        plain-language explanations, newest first."""
        return app.state.evals.recent()

    # ── read-side API (graph-backed; honest-empty when no snapshot) ──

    @app.get("/api/products")
    def products(q: str = "") -> dict:
        return app.state.data.products(q)

    @app.get("/api/products/by-unit")
    def products_by_unit(q: str = "") -> dict:
        return app.state.data.products_by_unit(q)

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

    @app.get("/api/graph/map")
    def graph_map() -> dict:
        return app.state.data.graph_map()

    @app.get("/api/graph/insights")
    def graph_insights(table: str) -> dict:
        return app.state.data.table_insights(table)

    @app.get("/api/agent/selftest")
    def agent_selftest() -> dict:
        """Build the live agent server-side WITHOUT calling Vertex, so
        'the console doesn't work' names itself: snapshot missing, adk /
        genai version drift, import errors — each mapped to its fix by
        _explain_failure. Scripted runner → trivially ok."""
        runner = app.state.runner
        if not isinstance(runner, ADKRunner):
            return {"ok": True, "runner": type(runner).__name__,
                    "note": "demo transcripts — no live agent to test"}
        try:
            runner._ensure()
            return {"ok": True, "runner": "ADKRunner",
                    "model": os.environ.get("GEMINI_MODEL",
                                            "gemini-3.1-pro-preview")}
        except Exception as exc:
            from apps.console.backend.runner import _explain_failure
            return {"ok": False, "runner": "ADKRunner",
                    "error": _explain_failure(exc)}

    @app.get("/api/graph/thread")
    def graph_thread(table: str = "") -> dict:
        return app.state.data.graph_thread(table)

    @app.get("/api/questions")
    def questions() -> dict:
        return app.state.data.questions()

    @app.get("/api/questions/starters")
    def starter_questions() -> dict:
        """Capability-tour starters derived from the loaded graph."""
        return app.state.data.starter_questions()

    @app.get("/api/witness")
    def witness(ref: str) -> dict:
        return app.state.data.witness(ref)

    @app.get("/api/lexicon")
    def lexicon() -> dict:
        return app.state.data.lexicon()

    # ── pins: the verified-query escrow ──────────────────────

    @app.get("/api/pins")
    def pins_list() -> dict:
        return {"live": app.state.data.live,
                "source": "graph" if app.state.data.live else "empty",
                "pins": app.state.pins.list()}

    @app.post("/api/pins", status_code=201)
    def pin_create(req: PinCreateRequest) -> dict:
        return {"pin": app.state.pins.create(
            question=req.question, answer=req.answer,
            citations=req.citations, sql=req.sql, rows=req.rows,
            ledger_id=req.ledger_id, actor=req.actor,
            source=req.source)}

    @app.post("/api/pins/{pin_id}/rerun")
    def pin_rerun(pin_id: str, req: PinActorRequest) -> object:
        try:
            return app.state.pins.rerun(
                pin_id, app.state.warehouse_factory(), actor=req.actor)
        except KeyError:
            return JSONResponse({"code": "not_found"}, status_code=404)
        except NoSqlError:
            return JSONResponse({"code": "no_sql"}, status_code=409)

    @app.post("/api/pins/{pin_id}/verify")
    def pin_verify(pin_id: str, req: PinVerifyRequest) -> object:
        try:
            return {"pin": app.state.pins.verify(
                pin_id, verified=req.verified, actor=req.actor)}
        except KeyError:
            return JSONResponse({"code": "not_found"}, status_code=404)

    @app.delete("/api/pins/{pin_id}")
    def pin_delete(pin_id: str) -> object:
        try:
            app.state.pins.delete(pin_id)
            return {"deleted": pin_id}
        except KeyError:
            return JSONResponse({"code": "not_found"}, status_code=404)

    # ── Meridian read plane (Synapse by Lumi admin, E17) ─────

    from apps.console.backend.meridian import router as meridian_router
    app.include_router(meridian_router)

    # ── the SPA (after API routes, so /api wins) ─────────────

    if _FRONTEND_DIST.exists():
        from fastapi.staticfiles import StaticFiles
        app.mount("/", StaticFiles(directory=str(_FRONTEND_DIST),
                                   html=True), name="spa")

    return app


app = create_app()
