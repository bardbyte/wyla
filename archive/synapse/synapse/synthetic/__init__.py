"""Synthetic data generator — realistic dummy data for the seven sources.

Deterministic (seeded). Used to demo the end-to-end pipeline on a laptop
without real warehouse access. Schema patterns mirror what we see in the
real AmEx tables (cardmember-day fact, product-rollup dim, transaction
events, risk profiles, loyalty ledger, etc.).
"""

from synapse.synthetic.schema import SYNTHETIC_TABLES, SyntheticTable
from synapse.synthetic.generator import (
    generate_all_sources,
    generate_ai_descriptions,
    generate_baseline_lookml,
    generate_bq_profile,
    generate_dq_rules,
    generate_glossary_csv,
    generate_mdm_cache,
    generate_metric_catalog_csv,
    generate_sql_corpus,
    generate_table_catalog_csv,
    generate_usage_history,
)

__all__ = [
    "SYNTHETIC_TABLES",
    "SyntheticTable",
    "generate_all_sources",
    "generate_ai_descriptions",
    "generate_baseline_lookml",
    "generate_bq_profile",
    "generate_dq_rules",
    "generate_glossary_csv",
    "generate_mdm_cache",
    "generate_metric_catalog_csv",
    "generate_sql_corpus",
    "generate_table_catalog_csv",
    "generate_usage_history",
]
