"""Loaders for one specific table.

Thin orchestration around the synapse loaders + a corpus copier. Produces
the canonical sources_dir layout that `build_graph_from_sources()` reads:

    cache/
      bq_cache/<table>.json
      usage_history/<table>.json
      dq_rules/<table>.json
      lineage/<table>.json
      mdm_cache/<table>.json
      gold_queries/<n>.sql ...
"""

from semantic_graph.loaders.runner import load_all_sources

__all__ = ["load_all_sources"]
