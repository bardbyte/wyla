"""ADK agent — NL→BigQuery SQL grounded in the semantic graph.

Importing this module sets up Vertex env vars (via config.load_config())
and exports `root_agent` for `adk web` discovery."""

# Trigger truststore + .env load
from semantic_graph import __version__  # noqa: F401
from semantic_graph.config import load_config  # noqa: E402

load_config()

from semantic_graph.agent.agent import root_agent  # noqa: E402, F401

__all__ = ["root_agent"]
