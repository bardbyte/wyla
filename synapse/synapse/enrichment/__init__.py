"""Synapse enrichment — LLM disambiguation + entity-proposal pipeline.

After the deterministic graph is built from the 10 sources, this module
runs the LLM enrichment pass guided by `skill.md`. Output is structured
EnrichmentBundles that get folded back into the graph as `llm_generated`
facts (capped at `inferred` confidence — see skill.md rule 2).

Architecture:

    enrich_graph(store, llm_client)
      ↳ for each Table in store:
          context = build_context(store, table)
          bundle  = llm_client.call(skill_md, context) → EnrichmentBundle
          apply_bundle(store, bundle)         # writes back to the graph
          memory.append(bundle.column_observations)
      ↳ propose_entities(memory) → list[EntityProposal] → review_queue/

The `llm_client` is dependency-injected so tests can pass a mock and the
production agent can pass a Vertex Gemini client.
"""

from synapse.enrichment.schemas import (
    CandidateSynonym,
    CodeResolution,
    ColumnObservation,
    EnrichmentBundle,
    EntityProposal,
    FilterRationale,
    RelationProposal,
    SelfAssessment,
)
from synapse.enrichment.enricher import enrich_graph, propose_entities

__all__ = [
    "enrich_graph",
    "propose_entities",
    "EnrichmentBundle",
    "ColumnObservation",
    "RelationProposal",
    "CandidateSynonym",
    "CodeResolution",
    "FilterRationale",
    "SelfAssessment",
    "EntityProposal",
]
