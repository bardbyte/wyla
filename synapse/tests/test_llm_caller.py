"""Tests for the LLM caller — focus on the dry-run / no-creds path.

The live Vertex call is exercised only when GOOGLE_APPLICATION_CREDENTIALS
is set; otherwise the function MUST return dry_run=True without raising."""

from __future__ import annotations


from synapse.curation.llm import call_gemini


def test_dry_run_returns_prompt_as_response_text():
    result = call_gemini("hello world", dry_run=True)
    assert result.dry_run is True
    assert result.response_text == "hello world"
    assert result.error == ""


def test_unset_creds_forces_dry_run(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("LUMI_VERTEX_SA_KEY", raising=False)
    result = call_gemini("hello", dry_run=False)
    assert result.dry_run is True
    # Error explains both env vars are unresolvable
    assert "no Vertex SA key resolvable" in result.error


def test_lumi_vertex_sa_key_satisfies_creds(monkeypatch, tmp_path):
    """LUMI_VERTEX_SA_KEY pointing at a real (json) file should be
    enough — we don't need GOOGLE_APPLICATION_CREDENTIALS set."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    key = tmp_path / "vertex.json"
    key.write_text('{"type":"service_account","client_email":"a@b","private_key":"x","project_id":"p"}')
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(key))
    # Forced dry_run avoids actual API call; we just want to verify
    # the env-resolution short-circuit doesn't fire.
    result = call_gemini("hi", dry_run=True)
    assert result.dry_run is True
    assert result.error == ""  # not the "no creds" error


def test_dry_run_carries_default_model_string():
    result = call_gemini("x", dry_run=True, model="custom-model-id")
    assert result.model == "custom-model-id"


def test_invalid_creds_path_treated_as_unset(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "does_not_exist.json"),
    )
    result = call_gemini("x", dry_run=False)
    # Path doesn't exist → forced dry-run, no crash
    assert result.dry_run is True
