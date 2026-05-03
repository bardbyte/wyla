"""ADK web entry point — placeholder until Session 5.

Session 5 will replace `root_agent` with the full SequentialAgent of all
7 stages composed from lumi/pipeline.py. For now, a minimal LlmAgent so
`adk web apps/` shows the agent in its sidebar and lets you smoke-test
auth/TLS + the Vertex backend before the pipeline is built out.
"""

from __future__ import annotations

from google.adk import Agent
from google.genai import types

PLACEHOLDER_INSTRUCTION = """\
You are LUMI, a LookML enrichment agent under construction.

The full pipeline (Parse → Discover → Stage → Plan → Enrich → Validate →
Publish) is being built across Sessions 1-6. Until then, you can answer
questions about the LUMI architecture.

When asked to actually run the pipeline, respond with:
  "The full pipeline isn't wired up yet — it lands in Session 5.
   For now use:  python -m lumi plan --input data/gold_queries/"
"""


def build_placeholder_agent(model: str = "gemini-3.1-pro-preview") -> Agent:
    return Agent(
        model=model,
        name="lumi",
        description=(
            "LookML enrichment pipeline agent. Reads SQL + MDM + baseline "
            "LookML, plans + enriches, publishes to GitHub."
        ),
        instruction=PLACEHOLDER_INSTRUCTION,
        generate_content_config=types.GenerateContentConfig(
            temperature=0.0,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
        ),
    )


# Module-level instance — required for `adk web` discovery.
root_agent = build_placeholder_agent()
