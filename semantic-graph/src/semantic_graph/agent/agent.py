"""Root ADK agent — NL → BigQuery SQL via the semantic graph.

`adk web` discovers `root_agent` here.
"""

from __future__ import annotations

from google.adk.agents import Agent
from google.genai.types import GenerateContentConfig

from semantic_graph.config import load_config
from semantic_graph.tools import (
    find_columns_for_concept,
    get_dq_status,
    get_entity,
    get_failed_query_corrections,
    get_join_path,
    get_lineage,
    get_metric,
    get_steward_review_queue,
    inspect_table,
    list_tables,
    resolve_synonym,
    search_columns,
    validate_sql,
)


_cfg = load_config()
_skill_md = _cfg.agent_skill_path.read_text(encoding="utf-8")


root_agent = Agent(
    name="semantic_graph_nl_agent",
    model=_cfg.gemini_model,
    description=(
        "Synapse NL Agent. Single conversational surface over the AmEx "
        "semantic knowledge graph spanning 53 tables. Answers 13 question "
        "categories: NL→BigQuery SQL generation (with optional dry-run "
        "validation), schema explanation, metric definitions, lineage, "
        "governance, usage, comparisons, disambiguation, data quality, "
        "cross-table queries, examples, provenance diagnostics, and "
        "documentation generation. Always grounded; always cited; always "
        "stamped with confidence tier."
    ),
    instruction=_skill_md,
    tools=[
        # 13 tools — one per category per agent_skill.md
        inspect_table,
        list_tables,
        search_columns,
        get_metric,
        get_join_path,
        find_columns_for_concept,
        get_lineage,
        get_entity,
        resolve_synonym,
        get_failed_query_corrections,
        get_dq_status,
        validate_sql,
        get_steward_review_queue,
    ],
    generate_content_config=GenerateContentConfig(
        temperature=0.0,
        max_output_tokens=8192,   # bumped from 4096 for documentation responses
    ),
)
