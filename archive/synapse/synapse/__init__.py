"""Synapse — confidence-typed knowledge graph for the enterprise warehouse.

The package layout is layered by responsibility:

    registry/   — load curated catalogs (glossary, metrics, tables)
                  + corpus-derived signals into typed objects
    curation/   — assemble evidence + prompt the LLM + parse + render
                  for human review
    utils/      — shared helpers

The first step of the system is `curation` — propose the canonical
entity backbone from all available sources, get it human-approved, and
THEN start extracting events into the graph.
"""

__version__ = "0.1.0"
