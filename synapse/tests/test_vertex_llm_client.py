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
        inits: list[dict] = []          # shared: records constructor kwargs

        def __init__(self, *args, **kwargs):
            FakeClient.inits.append(kwargs)
            self.models = FakeModels()

    class ThinkingConfig:
        def __init__(self, thinking_budget):
            self.thinking_budget = thinking_budget

    class GenerateContentConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class HttpOptions:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_types = pytypes.ModuleType("google.genai.types")
    fake_types.ThinkingConfig = ThinkingConfig
    fake_types.GenerateContentConfig = GenerateContentConfig
    fake_types.HttpOptions = HttpOptions
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
    monkeypatch.setenv("GEMINI_RETRY_BACKOFF_S", "0")   # no sleeps in tests
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
                            "thinking_fallbacks": 0, "call_retries": 0,
                            "context_truncations": 0}


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


def test_transient_call_failure_is_retried_and_recovers(monkeypatch):
    client, calls = _client(monkeypatch, [
        RuntimeError("503 unavailable"),          # transient blip
        VALID_BUNDLE,                             # retry succeeds
    ])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    assert bundle.column_observations[0].column_name == "c1"
    assert client.stats["call_retries"] == 1
    assert len(calls) == 2


def test_persistent_call_failure_is_in_band_with_type(monkeypatch):
    client, _ = _client(monkeypatch, [
        RuntimeError("503 unavailable"), RuntimeError("503 unavailable"),
    ])
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    notes = bundle.self_assessment.requires_steward_attention
    assert any("vertex call failed" in n and "503" in n for n in notes)
    assert client.stats["call_retries"] == 1      # exactly one retry


def test_empty_model_response_carries_finish_reason(monkeypatch):
    client, _ = _client(monkeypatch, ["", ""])    # empty text both tries
    bundle = client.enrich(skill_md="s", context={}, table_name="t")
    notes = bundle.self_assessment.requires_steward_attention
    assert any("empty model response" in n and "finish_reason" in n
               for n in notes)


def test_context_truncation_is_counted(monkeypatch):
    monkeypatch.setenv("GEMINI_MAX_CONTEXT_CHARS", "200")
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    client.enrich(skill_md="s", context={"blob": "x" * 2000},
                  table_name="t")
    assert client.stats["context_truncations"] == 1


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


# ─── corporate-proxy TLS handling ────────────────────────────


def test_tls_mode_decision_from_env(monkeypatch):
    from synapse.enrichment.vertex_client import _tls_mode
    for var in ("GEMINI_CA_BUNDLE", "GEMINI_TLS_INSECURE"):
        monkeypatch.delenv(var, raising=False)
    assert _tls_mode() == "default"
    monkeypatch.setenv("GEMINI_TLS_INSECURE", "1")
    assert _tls_mode() == "insecure"
    monkeypatch.setenv("GEMINI_CA_BUNDLE", "/corp/root.pem")
    assert _tls_mode() == "ca-bundle"          # bundle wins over insecure


def test_client_applies_tls_before_building_transport(monkeypatch):
    import synapse.enrichment.vertex_client as vc
    monkeypatch.setenv("GEMINI_TLS_INSECURE", "true")
    applied: list[str] = []
    monkeypatch.setattr(
        vc, "_apply_tls", lambda mode: (applied.append(mode), {})[1])
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    assert client.tls_mode == "insecure"
    assert applied == ["insecure"]


def test_insecure_verify_is_handed_to_sdk_directly(monkeypatch):
    """The field lesson: monkey-patching httpx was defeated by import
    order. verify MUST reach genai.Client via HttpOptions.client_args."""
    import sys as _sys

    import synapse.enrichment.vertex_client as vc
    monkeypatch.setenv("GEMINI_TLS_INSECURE", "1")
    monkeypatch.setattr(vc, "_apply_tls", lambda mode: {"truststore": False})
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    assert client.tls_sdk_direct is True
    inits = _sys.modules["google.genai"].Client.inits
    http_options = inits[-1]["http_options"]
    assert http_options.kwargs["client_args"] == {"verify": False}
    assert http_options.kwargs["async_client_args"] == {"verify": False}


def test_ca_bundle_verify_path_reaches_sdk(monkeypatch, tmp_path):
    import sys as _sys

    import synapse.enrichment.vertex_client as vc
    pem = tmp_path / "corp.pem"
    pem.write_text("dummy", encoding="utf-8")
    monkeypatch.setenv("GEMINI_CA_BUNDLE", str(pem))
    monkeypatch.setattr(vc, "_apply_tls", lambda mode: {"truststore": False})
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    inits = _sys.modules["google.genai"].Client.inits
    verify = inits[-1]["http_options"].kwargs["client_args"]["verify"]
    assert verify == str(pem.resolve())


def test_default_mode_passes_no_http_options(monkeypatch):
    import sys as _sys
    for var in ("GEMINI_TLS_INSECURE", "GEMINI_CA_BUNDLE"):
        monkeypatch.delenv(var, raising=False)
    client, _ = _client(monkeypatch, [VALID_BUNDLE])
    assert client.tls_sdk_direct is False
    assert "http_options" not in _sys.modules["google.genai"].Client.inits[-1]


def test_old_sdk_without_client_args_degrades_to_patches(monkeypatch):
    import synapse.enrichment.vertex_client as vc

    class _Rejecting:
        def __init__(self, **kwargs):
            raise TypeError("unexpected keyword argument 'client_args'")

    monkeypatch.setenv("GEMINI_TLS_INSECURE", "1")
    monkeypatch.setattr(vc, "_apply_tls", lambda mode: {"truststore": False})
    calls = _install_fake_genai(monkeypatch, [VALID_BUNDLE])
    import sys as _sys
    _sys.modules["google.genai.types"].HttpOptions = _Rejecting
    _sys.modules["google.genai"].types.HttpOptions = _Rejecting
    from synapse.enrichment.vertex_client import VertexLLMClient
    client = VertexLLMClient()
    assert client.tls_sdk_direct is False        # fell back, didn't crash
    del calls


def test_ca_bundle_mode_sets_standard_env_vars(monkeypatch, tmp_path):
    from synapse.enrichment.vertex_client import _apply_tls
    pem = tmp_path / "corp.pem"
    pem.write_text("dummy", encoding="utf-8")
    monkeypatch.setenv("GEMINI_CA_BUNDLE", str(pem))
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE",
                "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"):
        monkeypatch.delenv(var, raising=False)
    _apply_tls("ca-bundle")
    import os
    assert os.environ["REQUESTS_CA_BUNDLE"] == str(pem.resolve())
    assert os.environ["SSL_CERT_FILE"] == str(pem.resolve())


# ─── tiered routing: pro for reasoning, flash for breadth ────


def _tiered(monkeypatch, responses: list, **env):
    calls = _install_fake_genai(monkeypatch, responses)
    for var in ("GEMINI_MODEL_PRO", "GEMINI_MODEL_FLASH", "GEMINI_MODEL"):
        monkeypatch.delenv(var, raising=False)
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    from synapse.enrichment.vertex_client import TieredLLMClient
    return TieredLLMClient(), calls


def test_tiered_routes_chunk1_to_pro_rest_to_flash(monkeypatch):
    client, calls = _tiered(
        monkeypatch, [VALID_BUNDLE] * 3,
        GEMINI_MODEL_PRO="pro-x", GEMINI_MODEL_FLASH="flash-y")
    for chunk in (1, 2, 3):
        client.enrich(skill_md="s",
                      context={"batch": {"chunk": chunk, "of": 3}},
                      table_name="t")
    assert [c["model"] for c in calls] == ["pro-x", "flash-y", "flash-y"]
    stats = client.stats
    assert stats["calls"] == 3
    assert stats["pro_calls"] == 1
    assert stats["flash_calls"] == 2


def test_tiered_single_chunk_table_stays_on_pro(monkeypatch):
    client, calls = _tiered(
        monkeypatch, [VALID_BUNDLE],
        GEMINI_MODEL_PRO="pro-x", GEMINI_MODEL_FLASH="flash-y")
    client.enrich(skill_md="s", context={"batch": {"chunk": 1, "of": 1}},
                  table_name="t")
    assert calls[0]["model"] == "pro-x"


def test_tiered_without_flash_model_falls_back_to_pro(monkeypatch):
    client, calls = _tiered(
        monkeypatch, [VALID_BUNDLE] * 2, GEMINI_MODEL_PRO="pro-x")
    for chunk in (1, 2):
        client.enrich(skill_md="s",
                      context={"batch": {"chunk": chunk, "of": 2}},
                      table_name="t")
    assert [c["model"] for c in calls] == ["pro-x", "pro-x"]
    assert client.stats["flash_calls"] == 0
    assert client.stats["calls"] == 2                 # not double-counted


def test_tiered_missing_batch_metadata_defaults_to_pro(monkeypatch):
    client, calls = _tiered(
        monkeypatch, [VALID_BUNDLE],
        GEMINI_MODEL_PRO="pro-x", GEMINI_MODEL_FLASH="flash-y")
    client.enrich(skill_md="s", context={}, table_name="t")
    assert calls[0]["model"] == "pro-x"


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
