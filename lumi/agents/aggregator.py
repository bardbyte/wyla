"""Aggregator — collects per-view enriched outputs into a single state key that
ExploreBuilder and VocabChecker can reference via instruction templates.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types as genai_types

logger = logging.getLogger(__name__)

_PREFIX = "enriched_view__"


class Aggregator(BaseAgent):
    """Gathers every `enriched_view__{name}` key into `enriched_views_for_prompt`
    (JSON string) so downstream LlmAgents can templatize it cleanly.
    """

    def __init__(self, name: str = "Aggregator") -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        state = ctx.session.state
        flat: dict[str, dict[str, Any]] = {}
        for key, val in state.items():
            if key.startswith(_PREFIX) and isinstance(val, dict):
                flat[key[len(_PREFIX):]] = val

        if not flat:
            raise RuntimeError(
                "Aggregator: no enriched views found in session.state. "
                "ViewEnricher agents must run before Aggregator."
            )

        flat_json = json.dumps(flat, default=str, separators=(",", ":"))
        logger.info("Aggregator collected %d enriched views (%d bytes)", len(flat), len(flat_json))

        yield Event(
            author=self.name,
            actions=EventActions(
                state_delta={
                    "enriched_views": flat,
                    "enriched_views_for_prompt": flat_json,
                }
            ),
            content=genai_types.Content(
                role="model",
                parts=[genai_types.Part(text=f"Aggregated {len(flat)} enriched views.")],
            ),
        )
