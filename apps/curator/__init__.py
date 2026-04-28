"""curator — gold-query Excel auditor agent.

Self-contained: the agent definition, tools, and TLS bootstrap all live
in this directory. ADK loads `agent.py` and grabs `root_agent`; truststore
is injected here so corporate-MITM HTTPS works on first call without a
wrapper script.
"""

try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    # truststore not installed — caller will see SSL errors on corp networks.
    # Run: pip install truststore
    pass
