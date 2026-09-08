"""Graph layer — typed nodes + edges with per-fact provenance.

Three modules:
    store.py       — in-memory typed graph (no AGE / no Postgres dep)
    builder.py     — assembles graph from synthetic / real source bundles
    inspector.py   — answers "what is X" with the 7-source breakdown
"""

from synapse.graph.store import (
    GraphStore,
    Node,
    Edge,
    Provenance,
    ConfidenceTier,
    SourceName,
)
from synapse.graph.builder import build_graph_from_sources
from synapse.graph.inspector import inspect_table

__all__ = [
    "GraphStore",
    "Node",
    "Edge",
    "Provenance",
    "ConfidenceTier",
    "SourceName",
    "build_graph_from_sources",
    "inspect_table",
]
