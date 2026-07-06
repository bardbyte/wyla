"""VertexLLMClient — Gemini 3.1 Pro capabilities actually engaged.

Installs a fake `google.genai` into sys.modules so the client's lazy
import binds to it; every test asserts on what would hit the wire:
thinking config, explicit context truncation, and the corrective retry
that skill.md promises.
"""

from __future__ import annotations

import json
import sys
import types as pytypes

import pytest

from synapse.enrichment.vertex_client import (
    _TRUNCATION_MARKER, _serialize_context,
)

VALID_BUNDLE = json.dumps({
    "table_name": "t",
    "column_observations": [{
        "column_name": "c1", "candidate_role": "attribute",
        "self_confidence": 0.9, "evidence_used": ["mdm"],
    }],
})


def _install_fake_genai(monkeypatch, responses: list):
    """responses: str → returned as response.text; Exception → raised."""
    calls: list[dict] = []

    class FakeModels:
        def generate_content(self, *, model, contents, config):
            outcome = responses.pop(0)
            calls.append({"model": model, "contents": contents,
                          "config": config})
            if isinstance(outcome, Exception):
                raise outcome
            return pytypes.SimpleNamespace(text=outcome)

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.models = FakeModels()

    class ThinkingConfig:
        def __init__(self, thinking_budget):
            self.thinking_budget = thinking_budget

    class GenerateContentConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_types = pytypes.ModuleType("google.genai.types")
    fake_types.ThinkingConfig = ThinkingConfig
    fake_types.GenerateContentConfig = GenerateContentConfig
    fake_genai = pytypes.ModuleType("google.genai")
    fake_genai.Client = FakeClient
    fake_genai.types = fake_types
    fake_google = pytypes.ModuleType("google")
    fake_google.genai = fake_genai
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    monkeypatch.setitem(sys.modules, "google.genai.types", fake_types)
    return calls


def _client(monkeypatch, responses: list):
    calls = _install_fake_genai(monkeypatch, responses)
    from synapse.enrichment.vertex_client import VertexLLMClient
    return VertexLLMClient(), calls


def test_thinking_enabled_by_default_with_dynamic_budget(monkeypatch):
    monkeypatch.delenv("GEMINI_THINKING_BUDGET", raising=False)
    client, calls = _client(monkeypatch, [VALID_BUNDLE])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert bundle.column_observations[0].column_name == "c1"
    cfg = calls[0]["config"].kwargs
    assert cfg["thinking_config"].thinking_budget == -1   # dynamic
    assert cfg["response_mime_type"] == "application/json"
    assert client.stats == {"calls": 1, "corrective_retries": 0,
                            "thinking_fallbacks": 0}


def test_thinking_budget_zero_disables_thinking(monkeypatch):
    monkeypatch.setenv("GEMINI_THINKING_BUDGET", "0")
    client, calls = _client(monkeypatch, [VALID_BUNDLE])
    client.enrich(skill_md="s", context={}, table_name="t")
    assert "thinking_config" not in calls[0]["config"].kwargs


def test_endpoint_rejecting_thinking_degrades_gracefully(monkeypatch):
    monkeypatch.delenv("GEMINI_THINKING_BUDGET", raising=False)
    client, calls = _client(monkeypatch, [
        RuntimeError("thinking_config is not supported for this model"),
        VALID_BUNDLE,           # retried without thinking
        VALID_BUNDLE,           # next enrich() call — thinking stays off
    ])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert bundle.column_observations                 # recovered
    assert client.stats["thinking_fallbacks"] == 1
    assert "thinking_config" not in calls[1]["config"].kwargs
    client.enrich(skill_md="s", context={}, table_name="t")
    assert "thinking_config" not in calls[2]["config"].kwargs
    assert client.stats["thinking_fallbacks"] == 1    # no re-fail per call


def test_corrective_retry_recovers_bad_first_response(monkeypatch):
    client, calls = _client(monkeypatch, [
        "I cannot produce JSON, sorry.",              # failure #1
        VALID_BUNDLE,                                 # corrected
    ])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert bundle.column_observations[0].column_name == "c1"
    assert client.stats["corrective_retries"] == 1
    assert client.stats["calls"] == 2
    assert "FAILED VALIDATION" in calls[1]["contents"]
    assert any("recovered after corrective retry" in n
               for n in bundle.self_assessment.requires_steward_attention)


def test_second_failure_keeps_original_in_band_note(monkeypatch):
    client, calls = _client(monkeypatch, [
        "garbage one", "garbage two",                 # both fail
    ])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert len(calls) == 2                            # exactly ONE retry
    assert bundle.column_observations == []
    assert any("no JSON object" in n
               for n in bundle.self_assessment.requires_steward_attention)


def test_network_error_is_in_band_not_raised(monkeypatch):
    client, _ = _client(monkeypatch, [RuntimeError("503 unavailable")])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert any("vertex call failed" in n
               for n in bundle.self_assessment.requires_steward_attention)


def test_context_cap_default_and_env_override(monkeypatch):
    monkeypatch.delenv("GEMINI_MAX_CONTEXT_CHARS", raising=False)
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    assert client.max_context_chars == 400_000        # not 60K anymore
    monkeypatch.setenv("GEMINI_MAX_CONTEXT_CHARS", "500")
    client2, calls2 = _client(monkeypatch, [VALID_BUNDLE])
    client2.enrich(skill_md="s",
                   context={"blob": "x" * 2000}, table_name="t")
    assert "EVIDENCE TRUNCATED" in calls2[0]["contents"]


def test_serialize_context_truncation_is_explicit():
    small = _serialize_context({"a": 1}, 1000)
    assert _TRUNCATION_MARKER not in small
    big = _serialize_context({"a": "y" * 5000}, 100)
    assert big.endswith(_TRUNCATION_MARKER)
    assert len(big) == 100 + len(_TRUNCATION_MARKER)


def test_import_error_without_google_genai():
    """No fake installed + no real package → actionable RuntimeError."""
    try:
        import google.genai  # noqa: F401
        pytest.skip("google-genai actually installed in this env")
    except ImportError:
        pass
    from synapse.enrichment.vertex_client import VertexLLMClient
    with pytest.raises(RuntimeError, match="google-genai is required"):
        VertexLLMClient()
