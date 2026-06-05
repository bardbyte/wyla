"""Semantic Graph demo — confidence-typed semantic graph + NL→BigQuery ADK agent.

On import, injects truststore so corporate-MITM TLS works against Google /
Vertex without certifi failures (per AmEx CLAUDE.md guidance)."""

from __future__ import annotations

try:
    import truststore as _ts
    _ts.inject_into_ssl()
except ImportError:
    pass

__version__ = "0.1.0"
