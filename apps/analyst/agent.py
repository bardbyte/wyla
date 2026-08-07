"""analyst agent — provenance-first Data/Business Analyst over the graph.

Same runtime pattern as apps/curator: Google ADK Agent, Gemini on
Vertex, temperature 0, module-level `root_agent` for `adk web apps/`.

Run:
    python synapse/scripts/pipeline.py --demo     # or a real snapshot
    adk web apps/                                 # pick "warehouse_analyst"
"""

from __future__ import annotations

import os

from .prompts import ANALYST_INSTRUCTION, CLASSIC_INSTRUCTION
from .tools import build_analyst_tools, build_classic_tools

DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.1-pro-preview")


def build_agent(model: str = DEFAULT_MODEL, toolset: str | None = None):
    from google.adk import Agent
    from google.genai import types

    # Toolset selection (env SYNAPSE_AGENT_TOOLSET):
    #   "classic" (default) — the original single-graph agent's 12
    #       capabilities + the gated dry_run/execute pair. The chat
    #       experience the user validated, on the multi-table graph.
    #   "full" — all 24 tools incl. charts, sandbox, craft skills.
    mode = (toolset or os.environ.get(
        "SYNAPSE_AGENT_TOOLSET", "classic")).lower()
    classic = mode != "full"
    tools = build_classic_tools() if classic else build_analyst_tools()
    instruction = CLASSIC_INSTRUCTION if classic else ANALYST_INSTRUCTION

    # Same environment contract as the enrichment pipeline:
    # GEMINI_THINKING_BUDGET=-1 → dynamic thinking (Gemini 3.1 Pro's
    # strongest mode), 0 → off, N → capped. include_thoughts streams
    # reasoning into the console's work log while the user waits.
    config = types.GenerateContentConfig(
        temperature=0.0,
        max_output_tokens=8192,
    )
    try:
        budget = int(os.environ.get("GEMINI_THINKING_BUDGET", "-1"))
        config.thinking_config = types.ThinkingConfig(
            include_thoughts=True, thinking_budget=budget)
    except (AttributeError, TypeError, ValueError):
        pass                       # older SDK: run without thinking config

    # Domain sub-agents (opt-in, SYNAPSE_DOMAIN_AGENTS=1): one specialist
    # per BusinessUnit node in the snapshot. The root stays the generalist
    # front door; ADK transfer + route_question let it identify the
    # company domain first, then hand the thread to the specialist that
    # knows that segment's tables and playbooks. No units in the graph
    # (rollup never ran) → no sub-agents, root behaves exactly as before.
    sub_agents = []
    if os.environ.get("SYNAPSE_DOMAIN_AGENTS") == "1":
        try:
            from .domains import build_domain_agents
            from .tools import _service
            sub_agents = build_domain_agents(model, tools, _service())
        except Exception:
            sub_agents = []        # a broken snapshot must not kill chat

    def _agent(cfg):
        kwargs = {"sub_agents": sub_agents} if sub_agents else {}
        return Agent(
            model=model,
            name="warehouse_analyst",
            description=(
                "Senior data & business analyst over the semantic "
                "knowledge graph — grounds every answer in "
                "provenance-typed facts, honors skill guardrails, "
                "validates SQL statically, and runs sandboxed python "
                "for on-the-fly analysis."
            ),
            instruction=instruction,
            tools=tools,
            generate_content_config=cfg,
            **kwargs,
        )

    try:
        return _agent(config)
    except Exception:
        # older google-adk validates generate_content_config and rejects
        # thinking_config at Agent construction — degrade to no thought
        # streaming rather than no agent
        if getattr(config, "thinking_config", None) is None:
            raise
        config.thinking_config = None
        return _agent(config)


try:  # module-level instance required for `adk web` discovery
    root_agent = build_agent()
except ImportError:  # google-adk not installed — tools/prompts remain usable
    root_agent = None
