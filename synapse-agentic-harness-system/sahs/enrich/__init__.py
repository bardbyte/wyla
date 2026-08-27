"""B1 — the metric enrichment loop (E13).

The graph knows WHAT (canonical SQL, tables, support); the certified
catalogs know WHY for only ~41 metrics. B1 asks the L5 model to draft
the why — questions, grains, concept descriptions — under three
non-negotiables:

1. **Certified is never clobbered**: enrichment writes ONLY the
   ``*_enriched`` keys, and only for nodes whose catalog value is
   empty. Compile prefers the catalog value forever.
2. **The A5 blind gate runs first**: strip the names from the certified
   metrics, make the model recover them from expression + card context
   alone. ≥80% recovery → batch-tier review; 60–80% → item-review
   tier; <60% → the run HALTS and writes nothing (iterate the prompt,
   not the graph).
3. **Everything is witnessed**: every write carries
   ``witness: llm_enriched`` + the model id in evidence, and lands as
   ordinary append-only quads a steward can retract (B2).
"""
