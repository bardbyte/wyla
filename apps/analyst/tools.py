"""Analyst agent tools = shared graph tools + the python sandbox.

The 15 graph tools come from `synapse.mcp.adk_tools` — the same
GraphService implementation the MCP server exposes, so the ADK agent and
any MCP host behave identically. This module only adds process wiring
(snapshot discovery, lazy singleton) and the sandbox tool.
"""

from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SYNAPSE_ROOT = _REPO_ROOT / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:  # until synapse ships as a package
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.graph.store import GraphStore  # noqa: E402
from synapse.mcp.adk_tools import build_adk_tools  # noqa: E402
from synapse.mcp.service import GraphService  # noqa: E402
from synapse.utils.sandbox import run_python  # noqa: E402

_DEFAULT_SNAPSHOT = _SYNAPSE_ROOT / "data" / "cache" / "graph_snapshot.json"


def snapshot_path() -> Path:
    return Path(os.environ.get("SYNAPSE_GRAPH_PATH", str(_DEFAULT_SNAPSHOT)))


@lru_cache(maxsize=1)
def _service() -> GraphService:
    path = snapshot_path()
    if not path.exists():
        raise FileNotFoundError(
            f"no graph snapshot at {path} — build one first:\n"
            "  python synapse/scripts/pipeline.py --demo\n"
            "or point SYNAPSE_GRAPH_PATH at an existing snapshot."
        )
    return GraphService(
        GraphStore.load_json(path),
        tenant_id=os.environ.get("SYNAPSE_TENANT_ID", "default"),
    )


def run_python_analysis(code: str, timeout_seconds: int = 30) -> dict[str, Any]:
    """Run python analysis code in an isolated sandbox and return
    {status, stdout, stderr, artifacts}.

    Use for math on numbers already in the conversation: rate
    recomputation, contribution decomposition, simple forecasting,
    scenario what-ifs. stdlib only (json, statistics, math, csv,
    datetime...). print() everything you want to see; files written to
    the working directory come back in `artifacts`. No network, no
    credentials, no warehouse access — never try."""
    return run_python(code, timeout_seconds=timeout_seconds)


def build_analyst_tools() -> list[Callable[..., dict[str, Any]]]:
    """Graph tools (lazy service init) + sandbox, ready for ADK."""
    return [*build_adk_tools(_service()), run_python_analysis]
