"""Tests for the Vertex endpoint resolver + BQ default change."""

from __future__ import annotations

from synapse.utils.auth import (
    DEFAULT_BQ_ENDPOINT,
    DEFAULT_VERTEX_ENDPOINT,
    resolve_bq_endpoint,
    resolve_vertex_endpoint,
)


def test_bq_default_is_enterprise_psc():
    """Default BQ endpoint must be the enterprise PSC one — public is
    unreachable from corporate networks."""
    assert DEFAULT_BQ_ENDPOINT == "https://bigquery-prod.p.googleapis.com"


def test_vertex_default_is_public():
    """Vertex public endpoint IS reachable from most enterprise gateways."""
    assert DEFAULT_VERTEX_ENDPOINT == "https://aiplatform.googleapis.com"


def test_resolve_vertex_endpoint_default(monkeypatch):
    monkeypatch.delenv("VERTEX_API_BASE_URL", raising=False)
    assert resolve_vertex_endpoint() == DEFAULT_VERTEX_ENDPOINT


def test_resolve_vertex_endpoint_env_override(monkeypatch):
    monkeypatch.setenv(
        "VERTEX_API_BASE_URL", "https://vertex-prod.p.googleapis.com/",
    )
    # trailing slash stripped
    assert resolve_vertex_endpoint() == "https://vertex-prod.p.googleapis.com"


def test_resolve_bq_endpoint_default_is_enterprise(monkeypatch):
    monkeypatch.delenv("BIGQUERY_API_BASE_URL", raising=False)
    monkeypatch.delenv("BIGQUERY_URL", raising=False)
    assert resolve_bq_endpoint() == "https://bigquery-prod.p.googleapis.com"


def test_resolve_bq_endpoint_can_be_overridden_to_public(monkeypatch):
    """Non-enterprise users (public internet) can still set the public endpoint."""
    monkeypatch.setenv("BIGQUERY_API_BASE_URL", "https://bigquery.googleapis.com")
    assert resolve_bq_endpoint() == "https://bigquery.googleapis.com"
