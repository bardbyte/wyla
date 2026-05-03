"""apps/lumi — ADK web entry point for the LUMI pipeline agent.

Loaded by `adk web apps/`. Truststore is injected here so corporate-MITM
HTTPS works on first call without a wrapper script.

The actual agent will be wired in Session 5 once we have the SequentialAgent
of all 7 stages composed in lumi/pipeline.py. Until then, this directory
exists so the structure is in place and `adk web apps/` finds zero agents
gracefully (vs. failing on missing dir).
"""

try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    # truststore not installed — caller will see SSL errors on corp networks.
    # Run: pip install truststore
    pass
