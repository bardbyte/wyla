"""Synapse by Lumi — the product server (apps/lumi).

One process, zero build steps: FastAPI serves the Meridian read plane
under ``/api/meridian/*`` and the hand-authored frontend (ES modules,
no bundler, three.js vendored locally) from ``frontend/``. Run it
from the repo root:

    uvicorn apps.lumi.backend.app:app --port 8400

Environment: the same ``.env`` contract as the pipeline. The app
itself only READS the compiled build (``MERIDIAN_SILO_DIR`` /
``MERIDIAN_BUILDS_DIR`` / ``MERIDIAN_GRAPH_DIR`` override the
defaults); the BQ and Vertex planes are reported by ``/api/lumi/
planes`` as booleans — configured or not, never secrets — so the
Home page can say honestly which capabilities this machine carries.
CORS is open for local dev — lock it down before any non-localhost
deployment.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from apps.lumi.backend.ask import router as ask_router
from apps.lumi.backend.meridian import router as meridian_router

_FRONTEND = Path(__file__).resolve().parents[1] / "frontend"


def _load_env_file() -> None:
    """Pick up the silo's ``.env`` (the pipeline's own loader: first
    file found among $SAHS_ENV_FILE → <silo>/.env → ./.env; NEVER
    overrides variables already exported in the shell). So pasting
    ``MERIDIAN_SOURCES_DIR=/path/to/data/sources`` into the .env is
    enough for the Knowledge Files shelf to find its files."""
    import sys
    silo = os.environ.get(
        "MERIDIAN_SILO_DIR",
        str(Path(__file__).resolve().parents[3]
            / "synapse-agentic-harness-system"))
    if silo not in sys.path:
        sys.path.insert(0, silo)
    try:
        from sahs.util.auth import load_dotenv
        load_dotenv()
    except ImportError:                 # silo not present: env only
        pass


def create_app() -> FastAPI:
    _load_env_file()
    app = FastAPI(title="Synapse by Lumi", version="0.1.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"])

    @app.get("/health")
    def health() -> dict:
        return {"ok": True, "app": "synapse-by-lumi"}

    @app.get("/api/lumi/planes")
    def planes() -> dict:
        """The two network planes, as booleans — configured or not,
        never values. BQ rides the PSC/NO_PROXY contract; Vertex
        rides the proven proxy contract. The app itself calls
        neither; enrichment and dry-runs stay with laptop.py."""
        env = os.environ.get

        def _set(*names: str) -> bool:
            return any(bool(env(n)) for n in names)

        return {
            "bq": {
                "key": _set("LUMI_BQ_SA_KEY",
                            "GOOGLE_APPLICATION_CREDENTIALS"),
                "project": _set("LUMI_BQ_PROJECT",
                                "GOOGLE_CLOUD_PROJECT"),
                "endpoint": _set("LUMI_BQ_API_BASE_URL"),
            },
            "vertex": {
                "key": _set("LUMI_VERTEX_SA_KEY",
                            "GOOGLE_APPLICATION_CREDENTIALS"),
                "project": _set("VERTEX_PROJECT_ID",
                                "GOOGLE_CLOUD_PROJECT"),
                "model": env("VERTEX_MODEL",
                             env("GEMINI_MODEL",
                                 "gemini-3.1-pro-preview")),
            },
        }

    app.include_router(meridian_router)
    app.include_router(ask_router)      # Ask (E18), in-process

    if _FRONTEND.exists():
        app.mount("/", StaticFiles(directory=str(_FRONTEND),
                                   html=True), name="app")
    return app


app = create_app()
