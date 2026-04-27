"""Smoke tests for build_view_enricher — no LLM needed.

Verifies the factory produces a correctly-named, correctly-keyed LlmAgent whose
instruction template matches the keys DataLoader writes. This is the exact
contract that breaks silently if safe_key ever diverges.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from google.adk.models import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse

from lumi.agents.view_enricher import build_view_enricher
from lumi.util import safe_key


class _StubModel(BaseLlm):
    """Minimal BaseLlm subclass so LlmAgent accepts us without SafeChain."""

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        yield LlmResponse()
        return

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"stub/.*"]


def test_factory_produces_agent_with_matching_state_keys() -> None:
    view_name = "acqdw_acquisition_us"
    agent = build_view_enricher(view_name, model=_StubModel(model="stub/test"))

    assert agent.name == f"ViewEnricher__{safe_key(view_name)}"
    assert agent.output_key == f"enriched_view__{safe_key(view_name)}"

    # The instruction template must reference the exact per-view keys
    # DataLoader writes (parsed_view__, queries_for_view__, etc.).
    for prefix in (
        "parsed_view__",
        "queries_for_view__",
        "mdm_metadata_for_view__",
        "field_frequency_for_view__",
        "filter_defaults_for_view__",
        "user_vocabulary_for_view__",
    ):
        expected = "{" + f"{prefix}{safe_key(view_name)}" + "}"
        assert expected in agent.instruction, f"Missing template key: {expected}"


def test_factory_sanitizes_view_name_with_special_chars() -> None:
    agent = build_view_enricher("my.view-name", model=_StubModel(model="stub/test2"))
    assert "my_view_name" in agent.name
    assert "my_view_name" in agent.output_key
    assert "{parsed_view__my_view_name}" in agent.instruction
