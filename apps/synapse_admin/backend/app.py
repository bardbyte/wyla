"""Synapse by Lumi — the product server (apps/synapse_admin).

One process, zero build steps: FastAPI serves the Meridian read plane
under ``/api/meridian/*`` and the hand-authored frontend (ES modules,
no bundler, three.js vendored locally) from ``frontend/``. Run it
from the repo root:

    uvicorn apps.synapse_admin.backend.app:app --port 8080

Environment: the same ``.env`` contract as the pipeline. The app
itself only READS the compiled build (``MERIDIAN_SILO_DIR`` /
``MERIDIAN_BUILDS_DIR`` / ``MERIDIAN_GRAPH_DIR`` override the
defaults); the BQ and Vertex planes are reported by ``/api/synapse/
planes`` as booleans — configured or not, never secrets — so the
Home page can say honestly which capabilities this machine carries.
CORS is open for local dev — lock it down before any non-localhost
deployment.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from apps.synapse_admin.backend.auth import callback_router, router as auth_router
from apps.synapse_admin.backend.admin import router as admin_router
from apps.synapse_admin.backend.access import router as access_router
from apps.synapse_admin.backend.okta import (callback_router as okta_callback_router,
                                             router as okta_router)
from apps.synapse_admin.backend.ask import router as ask_router
from apps.synapse_admin.backend.chat import router as chat_router
from apps.synapse_admin.backend.kc import router as kc_router
from apps.synapse_admin.backend.meridian import router as meridian_router

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
logger = logging.getLogger(__name__)


# what the first bytes of an image say it is: a .png that is really a
# HEIC export would be served as image/png and dropped by the browser
# in silence — so the bytes are read, and the mismatch is the reason
_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", ".png"), (b"\xff\xd8\xff", ".jpg"),
    (b"GIF87a", ".gif"), (b"GIF89a", ".gif"), (b"RIFF", ".webp"),
)


def _looks_like(path: Path) -> str:
    """'.png', '.jpg', '.gif', '.webp', '.svg' from the bytes, or ''."""
    try:
        with path.open("rb") as fh:
            head = fh.read(512)
    except OSError:
        return ""
    for magic, suffix in _SIGNATURES:
        if head.startswith(magic):
            if suffix == ".webp" and head[8:12] != b"WEBP":
                continue
            return suffix
    text = head.lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    if text.startswith(b"<svg") or (text.startswith(b"<?xml")
                                    and b"<svg" in head.lower()):
        return ".svg"
    return ""


def _logo_status() -> dict:
    """Everything the brand endpoint says about the configured logo:
    which .env was read, the value as read, whether the file exists,
    whether its suffix is an image type, what its bytes say it is,
    and the one reason when it will not be served."""
    try:
        from sahs.util.auth import dotenv_path
        env_file = dotenv_path()
    except ImportError:
        env_file = None
    raw = (os.environ.get(LOGO_VAR) or "").strip().strip("'\"")
    status = {"configured": bool(raw), "value": raw,
              "env_file": str(env_file) if env_file else "",
              "path": "", "exists": False, "suffix_ok": False,
              "looks_like": "", "ok": False, "reason": ""}
    if not raw:
        return status
    path = Path(raw).expanduser()
    status["path"] = str(path)
    status["exists"] = path.is_file()
    suffix = path.suffix.lower()
    status["suffix_ok"] = suffix in _LOGO_TYPES
    if not status["exists"]:
        status["reason"] = (f"{LOGO_VAR} is set but no file is at {path} "
                            "(check the path as the .env spells it; a "
                            "note after the path on the same line must "
                            "start with ' #')")
        return status
    if not status["suffix_ok"]:
        status["reason"] = (f"{path.name} is not an image type: png, jpg, "
                            "jpeg, svg, webp or gif")
        return status
    seen = _looks_like(path)
    status["looks_like"] = seen
    expected = {".jpeg": ".jpg"}.get(suffix, suffix)
    if seen != expected:
        status["reason"] = (f"{path.name} is named {suffix} but its bytes "
                            f"are {'not an image the browser knows' if not seen else seen} "
                            "— export it again as a real "
                            f"{suffix.lstrip('.').upper()}")
        return status
    status["ok"] = True
    return status


def _logo_path() -> Path | None:
    """The configured logo when it is a real image file that exists;
    None otherwise (unset, missing, mis-typed, or not what it says)."""
    status = _logo_status()
    return Path(status["path"]) if status["ok"] else None


def _load_env_file() -> None:
    """Pick up the workspace's ``.env`` (the pipeline's own loader:
    $SAHS_ENV_FILE → ./.env; it NEVER overrides variables already
    exported in the shell). So pasting
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
    from apps.synapse_admin.backend.security import (
        CSRF_COOKIE, allowed_origins, csrf_is_required, csrf_is_valid,
        set_csrf_cookie)
    app = FastAPI(title="Synapse by Lumi", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-CSRF-Token"],
    )

    @app.middleware("http")
    async def bind_request_user(request: Request, call_next):
        """Make the cookie user available to owner-scoped route handlers."""
        from apps.synapse_admin.backend.auth import (COOKIE, _cached_session_user,
                                                     _google_api_error,
                                                     request_user)
        if csrf_is_required(request) and not csrf_is_valid(request):
            return JSONResponse(status_code=403, content={"detail": "CSRF validation failed"})
        from apps.synapse_admin.backend.meridian import _silo_import
        _silo_import()
        from sahs.spanner import SpannerConfigurationError, spanner_is_enabled
        user = None
        session = request.cookies.get(COOKIE, "")
        try:
            if (spanner_is_enabled() and session
                    and not request.url.path.startswith("/api/auth/")):
                user = _cached_session_user(session)
        except SpannerConfigurationError as exc:
            return JSONResponse(
                {"detail": f"identity service is unavailable: {exc}"},
                status_code=503)
        except _google_api_error() as exc:
            return JSONResponse(
                {"detail": "identity service is unavailable; verify Cloud "
                           "Spanner API access and try again"},
                status_code=503)
        except OSError as exc:
            return JSONResponse(
                {"detail": f"identity service is unavailable: {exc}"},
                status_code=503)
        token = request_user.set(user)
        try:
            try:
                response = await call_next(request)
            except _google_api_error() as exc:
                logger.error("Google Cloud backend request failed: %s",
                             exc, exc_info=exc)
                return JSONResponse(
                    {"detail": "storage service is unavailable; verify "
                               "the deployed Spanner schema and access"},
                    status_code=503)
            if session and not request.cookies.get(CSRF_COOKIE):
                set_csrf_cookie(response, secure=request.url.scheme == "https")
            return response
        finally:
            request_user.reset(token)

    @app.get("/health")
    def health() -> dict:
        return {"ok": True, "app": "synapse-by-lumi"}

    # the deployment's platform probes a second health path whose name is
    # the deployment's own, so it comes from the environment
    # (SYNAPSE_HEALTH_ALIAS=/some/path); unset means /health alone
    alias = (os.environ.get("SYNAPSE_HEALTH_ALIAS") or "").strip()
    if alias.startswith("/") and alias != "/health":
        app.add_api_route(alias, health, methods=["GET"])

    @app.get("/api/synapse/planes")
    def planes() -> dict:
        """The two network planes, as booleans — configured or not,
        never values. BQ rides the PSC/NO_PROXY contract; Vertex
        rides the proven proxy contract. The app itself calls
        neither; enrichment and dry-runs stay with pipeline.py."""
        try:
            from config.settings import Settings
            configured = Settings.load()
        except ImportError:
            configured = None

        def _set(*names: str) -> bool:
            return any(bool(configured.get(name) if configured is not None
                            else os.environ.get(name)) for name in names)

        def _plane() -> str:
            try:
                from sahs.util.gateway import model_plane
                return model_plane()
            except ImportError:                 # silo not present
                return "vertex"

        return {
            "bq": {
                "key": _set("SYNAPSE_BQ_SA_KEY",
                            "GOOGLE_APPLICATION_CREDENTIALS"),
                "project": _set("SYNAPSE_BQ_PROJECT",
                                "GOOGLE_CLOUD_PROJECT"),
                "endpoint": _set("SYNAPSE_BQ_API_BASE_URL"),
            },
            "vertex": {
                "key": _set("SYNAPSE_VERTEX_SA_KEY",
                            "GOOGLE_APPLICATION_CREDENTIALS"),
                "project": _set("VERTEX_PROJECT_ID",
                                "GOOGLE_CLOUD_PROJECT"),
                "model": (configured.get("VERTEX_MODEL")
                          if configured is not None else
                          os.environ.get("VERTEX_MODEL")) or
                         (configured.get("GEMINI_MODEL")
                          if configured is not None else
                          os.environ.get("GEMINI_MODEL")) or
                         "gemini-3.1-pro-preview",
            },
            # the second model plane: Gemini through the gateway behind a
            # the identity service token, and which plane the chat rides
            "gateway": {
                "app_id": _set("APP_ID"),
                "secret": _set("APP_SECRET"),
                "bearer": _set("GEMINI_BEARER_TOKEN"),
                "model": (configured.get("GATEWAY_MODEL")
                          if configured is not None else
                          os.environ.get("GATEWAY_MODEL")) or
                         "gemini-2.5-pro",
            },
            "plane": _plane(),
        }

    @app.get("/api/synapse/brand")
    def brand() -> dict:
        """Whether a logo is served, and when it is not, exactly why:
        the .env that was read, the value as read, the path tried,
        whether it exists, and what its bytes are. This is the local
        admin's own machine; the path is theirs to see."""
        status = _logo_status()
        path = Path(status["path"]) if status["ok"] else None
        return {
            "logo": path is not None,
            "configured": status["configured"],
            "reason": status["reason"],
            # a cache-buster: the file's mtime, so a replaced logo shows
            "stamp": str(int(path.stat().st_mtime)) if path else "",
            "env_file": status["env_file"],
            "path": status["path"],
            "exists": status["exists"],
            "looks_like": status["looks_like"],
        }

    @app.get("/api/synapse/logo")
    def logo():
        status = _logo_status()
        path = Path(status["path"]) if status["ok"] else None
        if path is None:
            return JSONResponse(
                {"available": False,
                 "reason": status["reason"] or
                 f"no logo: set {LOGO_VAR}=/path/to/logo.png in "
                 "the silo .env and restart the app"},
                status_code=404)
        return FileResponse(str(path),
                            media_type=_LOGO_TYPES[path.suffix.lower()])

    app.include_router(auth_router)
    app.include_router(callback_router)
    app.include_router(okta_router)             # Okta sign-in, and the shared /callback
    app.include_router(okta_callback_router)
    app.include_router(admin_router)
    app.include_router(access_router)
    app.include_router(meridian_router)
    app.include_router(ask_router)      # Ask (E18), in-process
    app.include_router(chat_router)     # Synapse v2 chat, in-process
    app.include_router(kc_router)       # KC Enrichment (E23), in-process

    if _SYNAPSE.exists():
        # mounted before "/" so the root mount cannot swallow it
        app.mount("/synapse", StaticFiles(directory=str(_SYNAPSE),
                                          html=True), name="synapse")
    if _FRONTEND.exists():
        app.mount("/synapse-admin", StaticFiles(directory=str(_FRONTEND),
                                                html=True),
                  name="synapse-admin")
        app.mount("/", StaticFiles(directory=str(_FRONTEND),
                                   html=True), name="app")
    return app


app = create_app()
