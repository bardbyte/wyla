"""VocabChecker — LlmAgent (Flash) that emits a VocabReport across all views."""

from __future__ import annotations

from pathlib import Path

from google.adk.agents import LlmAgent

from lumi.schemas import VocabReport

_PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "vocab_checker.md"


def build_vocab_checker(model: object) -> LlmAgent:
    instruction = (
        f"{_PROMPT_PATH.read_text(encoding='utf-8')}\n\n"
        "### Enriched views (from session.state)\n"
        "{enriched_views_for_prompt}\n\n"
        "Emit VocabReport now."
    )
    return LlmAgent(
        name="VocabChecker",
        description="Flags vocabulary drift across enriched views.",
        model=model,
        instruction=instruction,
        output_schema=VocabReport,
        output_key="vocab_report",
    )
