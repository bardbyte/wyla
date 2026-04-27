"""ViewEnricher — one LlmAgent instance per view, emitting EnrichedView.

The agent reads its slice of session.state (parsed view + gold queries for this
view + MDM metadata + frequency/defaults/vocabulary) and emits a structured
EnrichedView. No tools — all data is pre-populated by DataLoader.
"""

from __future__ import annotations

from pathlib import Path

from google.adk.agents import LlmAgent

from lumi.schemas import EnrichedView
from lumi.util import safe_key

_PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "view_enricher.md"


def _load_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


def build_view_enricher(view_name: str, model: object, temperature: float = 0.0) -> LlmAgent:
    """Create a ViewEnricher agent bound to one view.

    Args:
        view_name: The parsed view's name — drives the session.state keys read.
        model: A BaseLlm (use make_safechain_llm("1")).
        temperature: Bound separately on the model; passed here only for clarity.

    The instruction uses template variables `{parsed_view}`, `{queries_for_view}`,
    etc. ADK resolves these from session.state at runtime. We scope them to this
    view by using scoped keys like `state["enrichment_slice"][view_name]` that
    DataLoader writes, and the orchestrator copies the slice into the agent's
    invocation context. See lumi.agent for the wiring.
    """
    del temperature  # informational; bind on model, not on LlmAgent
    safe = safe_key(view_name)
    instruction = (
        f"View being enriched: `{view_name}`.\n\n"
        f"{_load_prompt()}\n\n"
        "### Data for this view (from session.state)\n"
        "parsed_view:\n{"
        f"parsed_view__{safe}"
        "}\n\n"
        "queries_for_view:\n{"
        f"queries_for_view__{safe}"
        "}\n\n"
        "mdm_metadata:\n{"
        f"mdm_metadata_for_view__{safe}"
        "}\n\n"
        "field_frequency:\n{"
        f"field_frequency_for_view__{safe}"
        "}\n\n"
        "filter_defaults:\n{"
        f"filter_defaults_for_view__{safe}"
        "}\n\n"
        "user_vocabulary:\n{"
        f"user_vocabulary_for_view__{safe}"
        "}\n\n"
        "Emit EnrichedView now."
    )
    return LlmAgent(
        name=f"ViewEnricher__{safe}",
        description=f"Enriches the `{view_name}` LookML view using gold queries + MDM.",
        model=model,
        instruction=instruction,
        output_schema=EnrichedView,
        output_key=f"enriched_view__{safe}",
    )


