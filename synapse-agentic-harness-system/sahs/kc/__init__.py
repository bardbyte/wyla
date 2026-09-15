"""Knowledge Catalog enrichment — the graph's knowledge about one table,
shaped for the catalog a human maintains.

    from sahs.kc.bundle import build_bundle
    bundle = build_bundle("dw.gms_transaction", use_llm=False)

The package is a projection over the promoted build plus the graph fold:
every fact carries its witness, status and provenance, an include
verdict computed in code decides whether it may be copied into the
catalog, and a model (when asked) writes prose from those facts and
nothing else. Writes back into the graph go through the clerk (push
records) or the read-back loader (catalog exports as a pending witness).
Submodules import lazily so ``sahs.kc.coverage`` stands alone.
"""
