"""Tests for synapse.utils.auth — env resolution, truststore injection."""

from __future__ import annotations


from synapse.utils.auth import (
    inject_truststore,
    resolve_bq_key_path,
    resolve_vertex_key_path,
)


def test_inject_truststore_safe_to_call(monkeypatch):
    """Always returns (ok, message); never raises. ok=True even when
    truststore isn't installed (no-op path)."""
    ok, msg = inject_truststore()
    assert ok is True
    assert isinstance(msg, str)
    assert len(msg) > 0


def test_resolve_vertex_prefers_lumi_var(monkeypatch, tmp_path):
    k1 = tmp_path / "vertex.json"
    k2 = tmp_path / "fallback.json"
    k1.write_text("{}")
    k2.write_text("{}")
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(k1))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(k2))
    assert resolve_vertex_key_path() == k1


def test_resolve_vertex_falls_back_to_gac(monkeypatch, tmp_path):
    k = tmp_path / "fallback.json"
    k.write_text("{}")
    monkeypatch.delenv("LUMI_VERTEX_SA_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(k))
    assert resolve_vertex_key_path() == k


def test_resolve_vertex_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv("LUMI_VERTEX_SA_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert resolve_vertex_key_path() is None


def test_resolve_vertex_returns_none_when_path_missing(monkeypatch, tmp_path):
    """An env var pointing at a non-existent file is treated as unset."""
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(tmp_path / "nope.json"))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert resolve_vertex_key_path() is None


def test_resolve_bq_prefers_lumi_var(monkeypatch, tmp_path):
    k1 = tmp_path / "bq.json"
    k2 = tmp_path / "fallback.json"
    k1.write_text("{}")
    k2.write_text("{}")
    monkeypatch.setenv("LUMI_BQ_SA_KEY", str(k1))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(k2))
    assert resolve_bq_key_path() == k1


def test_vertex_and_bq_can_resolve_to_different_files(monkeypatch, tmp_path):
    """The two-key pattern: each service points at its own SA key."""
    vk = tmp_path / "vertex.json"
    bk = tmp_path / "bq.json"
    vk.write_text("{}")
    bk.write_text("{}")
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(vk))
    monkeypatch.setenv("LUMI_BQ_SA_KEY", str(bk))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert resolve_vertex_key_path() == vk
    assert resolve_bq_key_path() == bk
    assert resolve_vertex_key_path() != resolve_bq_key_path()
