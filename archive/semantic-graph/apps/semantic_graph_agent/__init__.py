"""ADK-web discovery shim for the Semantic Graph BQ Agent.

`adk web apps/` finds this folder, imports `agent.py`, and registers its
`root_agent`. We re-export from the real package (`semantic_graph.agent`)
so all production code lives in src/ and this folder is pure discovery."""

import sys
from pathlib import Path

# Make `semantic_graph` importable without requiring `pip install -e .`.
# ADK web re-imports this module on each request — sys.path mutation here
# is fine because it always runs first.
_REPO = Path(__file__).resolve().parents[2]   # semantic-graph/
_SRC = _REPO / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Truststore + Vertex env vars get set the moment semantic_graph is imported
from semantic_graph.agent import root_agent  # noqa: E402, F401

__all__ = ["root_agent"]
