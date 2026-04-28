"""agent_test — Vertex AI → ADK smoke-test agent.

When `adk web` discovers this package via the `apps/` AGENTS_DIR, it imports
`agent.py` and grabs `root_agent`. Truststore is injected here so corporate-
MITM TLS works without requiring a wrapper script.
"""

# Inject truststore (uses macOS Keychain) before any google.* HTTP code runs.
# This makes `adk web` work on a corporate-MITM network with no extra flags.
try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    # truststore not installed — caller will see SSL errors on corp networks.
    # Run: pip install truststore
    pass
