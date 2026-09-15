"""Isolation: no test ever reads a real .env, a real key, or the shell's
proxy. Every variable this package reads is cleared before each test,
and the two file paths point at files that do not exist."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CLEARED_PREFIXES = ("KC_", "VERTEX_", "GEMINI_", "GOOGLE_", "GATEWAY_", "IDP_")
CLEARED_NAMES = ("APP_ID", "APP_SECRET", "AUTH_MODE", "AUTH_VERSION",
                 "SAHS_MODEL_PLANE", "SYNAPSE_VERTEX_SA_KEY",
                 "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy",
                 "NO_PROXY", "no_proxy", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for name in list(os.environ):
        if name.startswith(CLEARED_PREFIXES) or name in CLEARED_NAMES:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KC_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setenv("KC_EXTRA_ENV_FILE", str(tmp_path / "absent-extra.env"))
    yield


@pytest.fixture
def sa_key(tmp_path):
    """A key file that exists; the connection checks nothing else, and
    the token is always injected."""
    key = tmp_path / "sa.json"
    key.write_text("{}")
    return key


@pytest.fixture
def catalog_env(monkeypatch, sa_key):
    monkeypatch.setenv("KC_PROJECT_ID", "demo-project")
    monkeypatch.setenv("KC_SA_KEY", str(sa_key))
    return sa_key


@pytest.fixture
def vertex_env(monkeypatch, sa_key):
    monkeypatch.setenv("VERTEX_PROJECT_ID", "demo-vertex")
    monkeypatch.setenv("VERTEX_SA_KEY", str(sa_key))
    return sa_key


@pytest.fixture
def gateway_env(monkeypatch):
    monkeypatch.setenv("GATEWAY_BASE_URL", "https://gw.example.com/genai/google/v1")
    monkeypatch.setenv("IDP_TOKEN_URL", "https://idp.example.com/v1/application/token")
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", "YS0zMi1ieXRlLXNlY3JldC1mb3ItdGhlLXRlc3RzISE=")


@pytest.fixture
def search_payload():
    return json.loads((ROOT / "tests" / "fixtures"
                       / "search_response.json").read_text())
