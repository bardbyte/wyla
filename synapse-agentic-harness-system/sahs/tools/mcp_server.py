"""MCP shim — the eight tools behind one envelope.

Thin by design: every handler is `envelope(tool_name, plain_function)`;
the plain functions in ``tools/api.py`` / ``resolver.py`` /
``validate_sql.py`` / ``sandbox.py`` ARE the product (ADK-wrappable as
they stand); this file only adds transport, the response envelope

    {status: ok|denied|error, data, error,
     meta: {tool, build_version, latency_ms}}

and a small TTL cache for the read-only tools (a compiled build is
immutable, so the cache can only ever be stale about WHICH build is
CURRENT — hence the short TTL, and no caching for execute_sandboxed).

``FastMCP`` is imported lazily inside :func:`build_server` so the
envelope and cache stay unit-testable without the ``mcp`` extra.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable

from sahs.tools.api import (
    Build,
    describe_table,
    get_definition_line,
    sample_values,
    search_concepts,
    search_metrics,
)
from sahs.tools.resolver import resolve
from sahs.tools.sandbox import execute_sandboxed
from sahs.tools.validate_sql import validate_sql

CACHE_TTL_S = 60.0


class TTLCache:
    """Tiny monotonic-clock TTL cache — enough, and auditable."""

    def __init__(self, ttl_s: float = CACHE_TTL_S,
                 max_entries: int = 512) -> None:
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self._store: dict[Any, tuple[float, Any]] = {}

    def get(self, key: Any) -> Any | None:
        hit = self._store.get(key)
        if hit is None:
            return None
        stamp, value = hit
        if time.monotonic() - stamp > self.ttl_s:
            del self._store[key]
            return None
        return value

    def put(self, key: Any, value: Any) -> None:
        if len(self._store) >= self.max_entries:
            oldest = min(self._store, key=lambda k: self._store[k][0])
            del self._store[oldest]
        self._store[key] = (time.monotonic(), value)


def envelope(tool: str, fn: Callable[..., dict],
             build: Build, cache: TTLCache | None = None
             ) -> Callable[..., dict]:
    """Wrap a plain tool function into the response envelope."""

    def call(**kwargs: Any) -> dict:
        started = time.monotonic()
        key = (tool, tuple(sorted(kwargs.items())))
        if cache is not None:
            held = cache.get(key)
            if held is not None:
                return held
        def stamp(extra: dict[str, Any] | None = None) -> dict[str, Any]:
            return {**(extra or {}), "tool": tool,
                    "build_version": build.version,
                    "latency_ms": round(
                        (time.monotonic() - started) * 1000, 2)}

        try:
            data = fn(build, **kwargs)
        except Exception as e:                      # transport boundary
            return {"status": "error", "data": None,
                    "error": f"{type(e).__name__}: {e}", "meta": stamp()}
        if isinstance(data, dict) and {"status", "data", "meta"} <= \
                set(data):                    # sandbox: native envelope
            response = {"status": data["status"], "data": data["data"],
                        "error": data.get("error"),
                        "meta": stamp(data["meta"])}
        elif isinstance(data, dict) and data.get("error"):
            response = {"status": "error", "data": data,
                        "error": str(data["error"]), "meta": stamp()}
        else:
            response = {"status": "ok", "data": data, "error": None,
                        "meta": stamp()}
        if cache is not None and response["status"] == "ok":
            cache.put(key, response)
        return response

    call.__name__ = tool
    call.__doc__ = fn.__doc__
    return call


READ_TOOLS: dict[str, Callable[..., dict]] = {
    "search_metrics": search_metrics,
    "search_concepts": search_concepts,
    "describe_table": describe_table,
    "sample_values": sample_values,
    "resolve": resolve,
    "validate_sql": validate_sql,
    "get_definition_line": get_definition_line,
}


def build_handlers(builds_root: Path) -> dict[str, Callable[..., dict]]:
    """The eight enveloped handlers over the CURRENT build — the exact
    surface the MCP server (and any ADK agent) mounts."""
    build = Build.open(builds_root)
    cache = TTLCache()
    handlers = {name: envelope(name, fn, build, cache)
                for name, fn in READ_TOOLS.items()}
    handlers["execute_sandboxed"] = envelope(
        "execute_sandboxed", execute_sandboxed, build, cache=None)
    return handlers


def build_server(builds_root: Path):
    """→ FastMCP server exposing the eight tools (requires the ``mcp``
    extra: ``pip install -e .[mcp]``)."""
    from mcp.server.fastmcp import FastMCP       # lazy: optional extra
    server = FastMCP("meridian")
    for name, handler in build_handlers(builds_root).items():
        server.tool(name=name)(handler)
    return server
