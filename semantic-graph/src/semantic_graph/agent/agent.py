"""Root ADK agent — NL → BigQuery SQL via the semantic graph.

`adk web` discovers `root_agent` here.
"""

from __future__ import annotations

from google.adk.agents import Agent
from google.genai.types import GenerateContentConfig

from semantic_graph.config import load_config
from semantic_graph.tools import inspect_table


_cfg = load_config()
_skill_md = _cfg.agent_skill_path.read_text(encoding="utf-8")


root_agent = Agent(
    name="semantic_graph_bq_agent",
    model=_cfg.gemini_model,
    description=(
        "NL→BigQuery SQL agent grounded in the AmEx semantic graph. "
        "Answers questions about custins_customer_insights_cardmember by "
        "calling inspect_table first, then composing fully-cited SQL."
    ),
    instruction=_skill_md,
    tools=[inspect_table],
    generate_content_config=GenerateContentConfig(
        temperature=0.0,
        max_output_tokens=4096,
    ),
)
