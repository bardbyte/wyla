"""Tiny shared utilities. Keep this module dependency-free."""

from __future__ import annotations


def safe_key(name: str) -> str:
    """Make a name safe for use as an ADK state key or agent name.

    Used in two places that MUST stay in sync:
      - DataLoader writes `parsed_view__{safe_key(view_name)}` into session.state
      - build_view_enricher's instruction template reads the same key

    If these diverge, ParallelAgent templating silently fails at runtime with a
    KeyError that's hard to trace.
    """
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)
