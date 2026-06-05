"""ADK FunctionTool — the agent's one entry point into the semantic graph.

ADK auto-introspects this function's signature and docstring to generate
the JSON Schema Gemini sees. Keep the signature simple and the docstring
precise — they are the LLM's tool description."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Reach into sibling synapse package
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.graph.inspector import inspect_table as _inspect  # noqa: E402

from semantic_graph.config import load_config
from semantic_graph.graph import load_cached_graph


# Lazy-load the graph once per process
_STORE = None


def _get_store():
    global _STORE
    if _STORE is None:
        cfg = load_config()
        _STORE = load_cached_graph(cfg)
    return _STORE


def inspect_table(table_name: str) -> dict[str, Any]:
    """Return everything the semantic graph knows about one BigQuery table.

    Use this as the FIRST step on every user question. The returned dict is
    your authoritative source of truth — column names, types, partition
    column, business meanings, observed JOINs from the corpus, code
    resolutions, PII flags, data-quality rules, lineage, top users, and
    per-fact confidence + provenance.

    NEVER invent column names. Use only what this tool returns.

    Args:
        table_name: the unqualified table name (no project/dataset prefix).
            For this demo, only `custins_customer_insights_cardmember` is
            populated.

    Returns:
        A dict with these keys:
        - identity      — asset_kind, business_name, owner, tags, FQN
        - fused_view    — confidence_tier, score, n_sources_agree
        - per_source_view — what each of the 10+ ingest sources contributed
        - columns       — list of {name, data_type, is_nullable, is_pii,
                          pii_taxonomy, candidate_role, candidate_entity_name,
                          ai_generated_description, is_partitioning,
                          confidence_tier, sources_contributed, ...}
        - metrics       — formulas + business_names from metric_catalog + corpus
        - related_tables — JOIN evidence from the SQL corpus
        - lineage       — upstream + downstream tables (BQ JOBS-derived)
        - usage         — top_users, peak_query_hours, total_queries_observed
        - governance    — has_pii, pii_columns, owner_team
        - data_quality  — completeness, consistency, freshness, rules
        - code_resolutions — coded values → human meanings
    """
    store = _get_store()
    return _inspect(store, table_name)
