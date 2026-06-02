"""Registry — typed loaders for curated catalogs + corpus signals.

Five modules:
    schemas.py          — pydantic models (the contract)
    glossary.py         — load acronym/synonym CSV (context-keyed)
    metric_catalog.py   — load business-metric CSV (the Freebase prior)
    table_catalog.py    — load table-catalog CSV (scope + domain)
    corpus_signals.py   — aggregate noun frequency from gold SQLs
"""

from synapse.registry.schemas import (
    GlossaryEntry,
    MetricCatalogEntry,
    TableCatalogEntry,
    CorpusNounStat,
    MDMTableDigest,
    EvidenceBundle,
    ProposedEntity,
    ProposedRelationship,
    CurationProposal,
)

__all__ = [
    "GlossaryEntry",
    "MetricCatalogEntry",
    "TableCatalogEntry",
    "CorpusNounStat",
    "MDMTableDigest",
    "EvidenceBundle",
    "ProposedEntity",
    "ProposedRelationship",
    "CurationProposal",
]
