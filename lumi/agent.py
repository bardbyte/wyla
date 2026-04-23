"""LUMI root agent — composes the full pipeline.

    SequentialAgent(
      DataLoader,                             # Phase 0 — deterministic, no LLM
      ParallelAgent(ViewEnricher × N),        # Phase 2 — Gemini Pro per view
      Aggregator,                             # Phase 2.5 — collect enriched views
      SequentialAgent(ExploreBuilder,         # Phase 4a — Gemini Pro
                       VocabChecker),         # Phase 4b — Gemini Flash
      Validator,                              # Phase 5 — deterministic, no LLM
    )

Call `build_root_agent(cfg)` with a loaded LumiConfig. The returned agent is
ready to pass to a Runner. Remember to seed `session.state["lumi_config"] =
cfg.model_dump()` before `runner.run_async`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.adk.agents import ParallelAgent, SequentialAgent

from lumi.agents.aggregator import Aggregator
from lumi.agents.data_loader import DataLoader
from lumi.agents.explore_builder import build_explore_builder
from lumi.agents.validator import Validator
from lumi.agents.view_enricher import build_view_enricher
from lumi.agents.vocab_checker import build_vocab_checker

if TYPE_CHECKING:
    from lumi.schemas import LumiConfig

logger = logging.getLogger(__name__)


def build_root_agent(cfg: LumiConfig) -> SequentialAgent:
    """Build the full LUMI pipeline from a validated config.

    LLM instantiation is deferred until here so importing `lumi.agent` doesn't
    require SafeChain to be installed.
    """
    from src.adapters.adk_safechain_llm import make_safechain_llm

    strong = make_safechain_llm(cfg.llm.strong_model_idx, temperature=cfg.llm.temperature)
    fast = make_safechain_llm(cfg.llm.fast_model_idx, temperature=cfg.llm.temperature)

    view_names = cfg.resolved_view_names()
    view_enrichers = [
        build_view_enricher(name, model=strong, temperature=cfg.llm.temperature)
        for name in view_names
    ]

    pipeline = SequentialAgent(
        name="LUMI",
        sub_agents=[
            DataLoader(),
            ParallelAgent(name="EnrichmentTeam", sub_agents=view_enrichers),
            Aggregator(),
            SequentialAgent(
                name="Finalization",
                sub_agents=[
                    build_explore_builder(model=strong),
                    build_vocab_checker(model=fast),
                ],
            ),
            Validator(),
        ],
    )
    logger.info("Built LUMI pipeline with %d view enrichers", len(view_enrichers))
    return pipeline


# Entry point referenced by `adk run lumi/`: ADK looks for `root_agent` or a
# factory. We provide a factory users can call from a __main__ script; the
# canonical entrypoint is `python -m lumi`.
root_agent: SequentialAgent | None = None
