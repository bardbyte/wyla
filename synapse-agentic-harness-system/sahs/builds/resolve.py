"""Where the promoted build comes from on this host.

One resolver for every reader. ``MERIDIAN_BUILDS_SOURCE`` chooses:

``local`` (the default)
    ``MERIDIAN_BUILDS_DIR`` — the laptop's own ``pipeline.py compile``
    output, or the directory a container image baked in. Unchanged
    behaviour.

``spanner``
    the build promoted in Spanner, unpacked once into a local cache and
    served from there. A pod that was rescheduled, or a laptop that
    never compiled, gets the same build the rest of the fleet serves.

The cache root is a real, writable directory on either OS: an explicit
``MERIDIAN_BUILD_CACHE_DIR``, else the platform's cache location
(``%LOCALAPPDATA%`` on Windows, ``$XDG_CACHE_HOME`` or ``~/.cache`` on
Linux), else the temporary directory. Nothing is hard-coded to /tmp.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Mapping

_CACHE_DIR_NAME = "meridian-builds"
_DEFAULT_REFRESH_SECONDS = 300.0

_lock = threading.Lock()
_last_checked: float = 0.0
_last_root: Path | None = None


def _values(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    if environ is not None:
        return environ
    from sahs.util.auth import load_dotenv
    load_dotenv()
    return os.environ


def silo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def local_builds_root(environ: Mapping[str, str] | None = None) -> Path:
    values = _values(environ)
    return Path(values.get("MERIDIAN_BUILDS_DIR")
                or silo_root() / "builds")


def builds_source(environ: Mapping[str, str] | None = None) -> str:
    """→ ``local`` or ``spanner``; anything else is a configuration error."""
    values = _values(environ)
    source = (values.get("MERIDIAN_BUILDS_SOURCE") or "local").strip().lower()
    if source not in ("local", "spanner"):
        raise ValueError(
            f"MERIDIAN_BUILDS_SOURCE is local or spanner, not {source!r}")
    return source


def build_cache_root(environ: Mapping[str, str] | None = None) -> Path:
    """→ the directory Spanner-sourced builds are unpacked into."""
    values = _values(environ)
    explicit = (values.get("MERIDIAN_BUILD_CACHE_DIR") or "").strip()
    if explicit:
        return Path(explicit)
    if os.name == "nt":
        base = values.get("LOCALAPPDATA") or values.get("TEMP")
    else:
        base = values.get("XDG_CACHE_HOME") or (
            str(Path.home() / ".cache") if _home_is_writable() else "")
    return Path(base or tempfile.gettempdir()) / _CACHE_DIR_NAME


def _home_is_writable() -> bool:
    try:
        return os.access(Path.home(), os.W_OK)
    except (OSError, RuntimeError):
        return False


def refresh_seconds(environ: Mapping[str, str] | None = None) -> float:
    values = _values(environ)
    try:
        return float(values.get("MERIDIAN_BUILDS_REFRESH_SECONDS")
                     or _DEFAULT_REFRESH_SECONDS)
    except ValueError:
        return _DEFAULT_REFRESH_SECONDS


def builds_root(environ: Mapping[str, str] | None = None, *,
                force: bool = False) -> Path:
    """→ a builds root holding ``CURRENT``, whatever the source is.

    With the Spanner source this asks Spanner which build is promoted
    at most once per ``MERIDIAN_BUILDS_REFRESH_SECONDS``, so a request
    path may call it freely; a newly promoted build is picked up on the
    next check without a restart.
    """
    global _last_checked, _last_root
    if builds_source(environ) == "local":
        return local_builds_root(environ)
    with _lock:
        now = time.monotonic()
        if (not force and _last_root is not None
                and now - _last_checked < refresh_seconds(environ)):
            return _last_root
        from sahs.builds.spanner_store import SpannerBuildStore
        root = SpannerBuildStore.from_env().ensure_local(
            build_cache_root(environ))
        _last_checked, _last_root = now, root
        return root


def reset_cache() -> None:
    """Forget the memoised root — for tests and for a forced re-check."""
    global _last_checked, _last_root
    with _lock:
        _last_checked, _last_root = 0.0, None
