"""analyst agent — provenance-first Data/Business Analyst over the graph.

Same runtime pattern as apps/curator: Google ADK Agent, Gemini on
Vertex, temperature 0, module-level `root_agent` for `adk web apps/`.

Run:
    python synapse/scripts/pipeline.py --demo     # or a real snapshot
    adk web apps/                                 # pick "warehouse_analyst"
"""

from __future__ import annotations

import os

from .prompts import ANALYST_INSTRUCTION
from .tools import build_analyst_tools

DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.1-pro-preview")


def build_agent(model: str = DEFAULT_MODEL):
    from google.adk import Agent
    from google.genai import types

    return Agent(
        model=model,
        name="warehouse_analyst",
        description=(
            "Senior data & business analyst over the semantic knowledge "
            "graph — grounds every answer in provenance-typed facts, "
            "honors skill guardrails, validates SQL statically, and runs "
            "sandboxed python for on-the-fly analysis."
        ),
        instruction=ANALYST_INSTRUCTION,
        tools=build_analyst_tools(),
        generate_content_config=types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=8192,
        ),
    )


try:  # module-level instance required for `adk web` discovery
    root_agent = build_agent()
except ImportError:  # google-adk not installed — tools/prompts remain usable
    root_agent = None
