"""Synapse MCP surface — the graph exposed as task-shaped tools.

Layering (implements docs/MCP_SERVER_SPEC.md):

    GraphStore snapshot  →  GraphService (pure python, testable)
                             ├─ server.py    MCP stdio/http (FastMCP, optional dep)
                             └─ adk_tools.py Google-ADK FunctionTool adapters

The service is the single implementation; MCP and ADK are thin transports
over it, so Claude-family hosts and the in-house Gemini agents call the
exact same code and get the exact same provenance-carrying envelopes.
"""

from synapse.mcp.envelope import ErrorDetail, ResponseMeta, SynapseResponse
from synapse.mcp.service import GraphService

__all__ = [
    "ErrorDetail",
    "GraphService",
    "ResponseMeta",
    "SynapseResponse",
]
