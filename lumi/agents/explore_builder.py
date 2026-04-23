"""ExploreBuilder — single LlmAgent that emits the .model.lkml explores."""

from __future__ import annotations

from pathlib import Path

from google.adk.agents import LlmAgent

_PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "explore_builder.md"


def build_explore_builder(model: object) -> LlmAgent:
    """Create the ExploreBuilder. Reads enriched views + join_graphs from state."""
    instruction = (
        f"{_PROMPT_PATH.read_text(encoding='utf-8')}\n\n"
        "### Inputs (from session.state)\n"
        "enriched_views:\n{enriched_views_for_prompt}\n\n"
        "join_graphs:\n{join_graphs}\n\n"
        "mdm_metadata:\n{mdm_metadata}\n\n"
        "filter_defaults:\n{filter_defaults}\n\n"
        "Emit the LookML model file text."
    )
    return LlmAgent(
        name="ExploreBuilder",
        description=(
            "Produces a .model.lkml file with explores covering every valid join "
            "pattern from gold queries and MDM relationships."
        ),
        model=model,
        instruction=instruction,
        output_key="model_file_text_enriched",
    )
