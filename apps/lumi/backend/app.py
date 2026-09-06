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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from apps.lumi.backend.ask import router as ask_router
from apps.lumi.backend.chat import router as chat_router
from apps.lumi.backend.meridian import router as meridian_router

_FRONTEND = Path(__file__).resolve().parents[1] / "frontend"
# the second surface: Synapse Semantic Intelligence — the same API and
# build, a stripped nav (chat, search, data products, metrics,
# artifacts), artifacts published inside the chat
_SYNAPSE = Path(__file__).resolve().parents[2] / "synapse" / "frontend"

# the brand image for the second surface: SYNAPSE_LOGO in the silo .env
# names an image file on this machine; the page swaps its words for it
_LOGO_TYPES = {".png": "image/png", ".jpg": "image/jpeg",
               ".jpeg": "image/jpeg", ".svg": "image/svg+xml",
               ".webp": "image/webp", ".gif": "image/gif"}
LOGO_VAR = "SYNAPSE_LOGO"


def _logo_path() -> Path | None:
    """The configured logo when it is an image file that exists; None
    otherwise (unset, missing, or not an image)."""
    raw = (os.environ.get(LOGO_VAR) or "").strip().strip("'\"")
    if not raw:
        return None
    path = Path(raw).expanduser()
    if path.suffix.lower() not in _LOGO_TYPES or not path.is_file():
        return None
    return path


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

        def _plane() -> str:
            try:
                from sahs.util.eag import model_plane
                return model_plane()
            except ImportError:                 # silo not present
                return "vertex"

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
            # the second model plane: Gemini through EAG behind a
            # OneIdentity token, and which plane the chat rides
            "eag": {
                "app_id": _set("APP_ID"),
                "secret": _set("APP_SECRET"),
                "bearer": _set("GEMINI_BEARER_TOKEN"),
                "model": env("EAG_MODEL", "gemini-2.5-pro"),
            },
            "plane": _plane(),
        }

    @app.get("/api/lumi/brand")
    def brand() -> dict:
        """Whether a logo is served, and why not when it is not — the
        path itself never leaves the machine."""
        raw = (os.environ.get(LOGO_VAR) or "").strip()
        path = _logo_path()
        return {
            "logo": path is not None,
            "configured": bool(raw),
            "reason": ("" if path is not None or not raw else
                       f"{LOGO_VAR} is set but no image file is at that "
                       "path (png, jpg, jpeg, svg, webp or gif)"),
            # a cache-buster: the file's mtime, so a replaced logo shows
            "stamp": str(int(path.stat().st_mtime)) if path else "",
        }

    @app.get("/api/lumi/logo")
    def logo():
        path = _logo_path()
        if path is None:
            return JSONResponse(
                {"available": False,
                 "reason": f"no logo: set {LOGO_VAR}=/path/to/logo.png in "
                           "the silo .env and restart the app"},
                status_code=404)
        return FileResponse(str(path),
                            media_type=_LOGO_TYPES[path.suffix.lower()])

    app.include_router(meridian_router)
    app.include_router(ask_router)      # Ask (E18), in-process
    app.include_router(chat_router)     # Synapse v2 chat, in-process

    if _SYNAPSE.exists():
        # mounted before "/" so the root mount cannot swallow it
        app.mount("/synapse", StaticFiles(directory=str(_SYNAPSE),
                                          html=True), name="synapse")
    if _FRONTEND.exists():
        app.mount("/", StaticFiles(directory=str(_FRONTEND),
                                   html=True), name="app")
    return app


app = create_app()
