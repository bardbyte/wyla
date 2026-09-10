"""Tests for enterprise BQ network resolution (endpoint, project, proxy, CA)."""

from __future__ import annotations

import os

import pytest

from synapse.utils import auth as auth_mod
from synapse.utils.auth import (
    DEFAULT_BQ_ENDPOINT,
    DEFAULT_BQ_LOCATION,
    _BYPASS_HOSTS,
    _truthy,
    resolve_bq_endpoint,
    resolve_bq_location,
    resolve_bq_project,
    setup_bq_network_env,
)


# ─── _truthy ─────────────────────────────────────────────────


@pytest.mark.parametrize("v", ["1", "true", "TRUE", "Yes", "ON", "yes"])
def test_truthy_returns_true_for_common_truth_values(v):
    assert _truthy(v) is True


@pytest.mark.parametrize("v", ["0", "false", "no", "", None, "  "])
def test_truthy_returns_false_for_falsy(v):
    assert _truthy(v) is False


# ─── project / endpoint / location resolution ────────────────


def test_resolve_bq_project_prefers_bq_project_id(monkeypatch):
    monkeypatch.setenv("BQ_PROJECT_ID", "first-wins")
    monkeypatch.setenv("LUMI_BQ_PROJECT", "fallback")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "last-resort")
    assert resolve_bq_project() == "first-wins"


def test_resolve_bq_project_falls_back_to_lumi(monkeypatch):
    monkeypatch.delenv("BQ_PROJECT_ID", raising=False)
    monkeypatch.setenv("LUMI_BQ_PROJECT", "fallback")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "last-resort")
    assert resolve_bq_project() == "fallback"


def test_resolve_bq_project_falls_back_to_google_cloud_project(monkeypatch):
    monkeypatch.delenv("BQ_PROJECT_ID", raising=False)
    monkeypatch.delenv("LUMI_BQ_PROJECT", raising=False)
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "last-resort")
    assert resolve_bq_project() == "last-resort"


def test_resolve_bq_project_none_when_unset(monkeypatch):
    for k in ("BQ_PROJECT_ID", "LUMI_BQ_PROJECT", "GOOGLE_CLOUD_PROJECT"):
        monkeypatch.delenv(k, raising=False)
    assert resolve_bq_project() is None


def test_resolve_bq_endpoint_default_when_unset(monkeypatch):
    monkeypatch.delenv("BIGQUERY_API_BASE_URL", raising=False)
    monkeypatch.delenv("BIGQUERY_URL", raising=False)
    assert resolve_bq_endpoint() == DEFAULT_BQ_ENDPOINT


def test_resolve_bq_endpoint_prefers_api_base_url(monkeypatch):
    monkeypatch.setenv(
        "BIGQUERY_API_BASE_URL", "https://bigquery-prod.p.googleapis.com",
    )
    monkeypatch.setenv("BIGQUERY_URL", "https://something-else.example.com")
    assert resolve_bq_endpoint() == "https://bigquery-prod.p.googleapis.com"


def test_resolve_bq_endpoint_falls_back_to_bigquery_url(monkeypatch):
    monkeypatch.delenv("BIGQUERY_API_BASE_URL", raising=False)
    monkeypatch.setenv("BIGQUERY_URL", "https://fallback.example.com/")
    # trailing slash stripped
    assert resolve_bq_endpoint() == "https://fallback.example.com"


def test_resolve_bq_location_default(monkeypatch):
    monkeypatch.delenv("BQ_LOCATION", raising=False)
    assert resolve_bq_location() == DEFAULT_BQ_LOCATION


def test_resolve_bq_location_honors_env(monkeypatch):
    monkeypatch.setenv("BQ_LOCATION", "EU")
    assert resolve_bq_location() == "EU"


# ─── setup_bq_network_env ────────────────────────────────────


def test_setup_bq_network_env_merges_no_proxy(monkeypatch):
    monkeypatch.delenv("BQ_FORCE_PROXY", raising=False)
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1")
    state = setup_bq_network_env()
    merged = os.environ["NO_PROXY"].split(",")
    # Original entries preserved
    assert "localhost" in merged
    assert "127.0.0.1" in merged
    # Google bypass hosts injected
    for h in _BYPASS_HOSTS:
        assert h in merged
    # both NO_PROXY and no_proxy populated
    assert os.environ["no_proxy"] == os.environ["NO_PROXY"]
    assert state["force_proxy"] is False


def test_setup_bq_network_env_skips_no_proxy_when_force_proxy(monkeypatch):
    monkeypatch.setenv("BQ_FORCE_PROXY", "1")
    monkeypatch.setenv("NO_PROXY", "localhost")
    setup_bq_network_env()
    # NO_PROXY unchanged
    assert os.environ["NO_PROXY"] == "localhost"


def test_setup_bq_network_env_reports_disable_proxy(monkeypatch):
    monkeypatch.setenv("BQ_DISABLE_PROXY", "yes")
    state = setup_bq_network_env()
    assert state["disable_proxy_for_auth"] is True


def test_setup_bq_network_env_mirrors_ssl_cert_to_requests_ca_bundle(
    monkeypatch, tmp_path,
):
    cert = tmp_path / "corp.pem"
    cert.write_text("dummy cert")
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    state = setup_bq_network_env()
    assert state["ca_bundle"] == str(cert)
    assert os.environ.get("REQUESTS_CA_BUNDLE") == str(cert)


def test_setup_bq_network_env_returns_endpoint_state(monkeypatch):
    monkeypatch.setenv(
        "BIGQUERY_API_BASE_URL", "https://bigquery-prod.p.googleapis.com",
    )
    state = setup_bq_network_env()
    assert state["endpoint"] == "https://bigquery-prod.p.googleapis.com"
    assert state["endpoint_host"] == "bigquery-prod.p.googleapis.com"
    # Custom endpoint host gets added to bypass list
    assert state["endpoint_host"] in state["no_proxy_hosts"]


def test_setup_bq_network_env_no_ca_bundle_when_unset(monkeypatch):
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    state = setup_bq_network_env()
    assert state["ca_bundle"] is None


# ─── BQ_SCOPE constant ───────────────────────────────────────


def test_bq_scope_is_canonical():
    assert auth_mod.BQ_SCOPE == "https://www.googleapis.com/auth/bigquery"
