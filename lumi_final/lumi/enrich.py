"""Stage 2: Enrich tables via Gemini (one call per table, ParallelAgent).

Built in Session 2. One LlmAgent per table, all run in parallel.

Key functions:
  enrich_table(table_context, ecosystem_brief, model) -> EnrichedOutput
  merge_enrichment(existing_lkml, enriched) -> str  # additive merge
"""

# TODO: Session 2 — implement here
