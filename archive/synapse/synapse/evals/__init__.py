"""Graph evals — measuring whether the context graph actually works.

``retrieval`` grades findability: given how analysts phrase things, does
search surface the right node? The gold set is extracted from evidence
already in the graph (DMP curated questions, mined measure names,
business names) — the catalogs that grow the graph double as the labels
that grade it.
"""
