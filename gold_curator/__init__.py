"""gold_curator — agent that inspects gold-query Excels with LLM reasoning.

Imported by `apps/curator/agent.py` (the adk web entry) and importable
directly. Inject truststore here so corporate-MITM TLS is handled before
any google.* HTTP code initializes.
"""

try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    pass
