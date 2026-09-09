"""LUMI — LookML Understanding and Metric Intelligence.

A pipeline that reads SQL queries, extracts metrics, and generates
enriched LookML views + explores + metric catalog + golden NL questions.

Corporate-MITM TLS: truststore is injected on import so any code path
that imports the ``lumi`` package (CLI via ``python -m lumi``, scripts/,
tests, ADK Runner) picks up macOS Keychain's corporate root CA without
needing a per-script wrapper. Same pattern as ``apps/lumi/__init__.py``.
Safe no-op when truststore isn't installed.
"""

try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
except ImportError:
    # truststore not installed — caller will see SSL errors on corp
    # networks. Run: pip install truststore
    pass

__version__ = "0.1.0"
