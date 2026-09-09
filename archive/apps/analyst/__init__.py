"""analyst — the warehouse Data/Business Analyst agent.

Self-contained ADK app over the compiled Synapse graph snapshot: the
agent definition, graph tools (shared with the MCP server), a python
analysis sandbox, and the TLS bootstrap all resolve from here.
`adk web apps/` discovers `agent.py:root_agent`.
"""

try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    # truststore not installed — caller will see SSL errors on corp networks.
    # Run: pip install truststore
    pass
