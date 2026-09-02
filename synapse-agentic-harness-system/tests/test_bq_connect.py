"""The laptop BQ connectivity contract (ported from the proven
bq_connect flow): .env bootstrap, NO_PROXY injection, SSL controls,
fail-fast validation. No network anywhere."""

from __future__ import annotations

import ssl
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import (                     # noqa: E402
    AuthError,
    BQConnection,
    configure_network,
    load_dotenv,
    resolve_ssl,
)

_BQ_VARS = ("LUMI_BQ_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS",
            "BQ_PROJECT_ID", "LUMI_BQ_PROJECT", "GOOGLE_CLOUD_PROJECT",
            "BIGQUERY_API_BASE_URL", "BIGQUERY_URL", "BQ_LOCATION",
            "BQ_SSL_NO_VERIFY", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE",
            "BQ_DISABLE_PROXY", "BQ_FORCE_PROXY", "SAHS_ENV_FILE",
            "NO_PROXY", "no_proxy", "HTTPS_PROXY", "https_proxy",
            "HTTP_PROXY", "http_proxy")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for name in _BQ_VARS:
        monkeypatch.delenv(name, raising=False)
    # point the .env search at an empty tmp file so a developer's real
    # .env never leaks into the tests
    empty = tmp_path / "empty.env"
    empty.write_text("")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty))


def test_dotenv_loads_without_overriding(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text(
        "# laptop config\n"
        "GOOGLE_APPLICATION_CREDENTIALS=/keys/prj-p-lumi-gpt.json\n"
        "export BQ_PROJECT_ID='prj-p-lumi-gpt'\n"
        "BIGQUERY_URL=\"https://bigquery-prod.p.googleapis.com\"\n")
    monkeypatch.setenv("BQ_PROJECT_ID", "already-exported")
    loaded = load_dotenv(env)
    assert "GOOGLE_APPLICATION_CREDENTIALS" in loaded
    assert "BIGQUERY_URL" in loaded
    assert "BQ_PROJECT_ID" not in loaded          # shell export wins
    import os
    assert os.environ["BQ_PROJECT_ID"] == "already-exported"
    assert os.environ["BIGQUERY_URL"] == \
        "https://bigquery-prod.p.googleapis.com"


def test_no_proxy_injection_and_overrides(monkeypatch):
    import os
    monkeypatch.setenv("NO_PROXY", "internal.corp")
    configure_network("https://bigquery-prod.p.googleapis.com")
    for host in ("bigquery-prod.p.googleapis.com",
                 "oauth2.googleapis.com", "internal.corp"):
        assert host in os.environ["NO_PROXY"]
    before = os.environ["NO_PROXY"]
    configure_network("https://bigquery-prod.p.googleapis.com")
    assert os.environ["NO_PROXY"] == before       # idempotent, no dupes

    monkeypatch.setenv("BQ_FORCE_PROXY", "1")
    monkeypatch.setenv("NO_PROXY", "x")
    summary = configure_network("https://bigquery-prod.p.googleapis.com")
    assert "forced" in summary["proxy"]
    assert os.environ["NO_PROXY"] == "x"          # untouched

    monkeypatch.delenv("BQ_FORCE_PROXY")
    monkeypatch.setenv("BQ_DISABLE_PROXY", "1")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.corp:8080")
    summary = configure_network("https://bigquery-prod.p.googleapis.com")
    assert "disabled" in summary["proxy"]
    assert "HTTPS_PROXY" not in os.environ


def test_ssl_controls(monkeypatch, tmp_path):
    assert resolve_ssl() == (True, None)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/certs/amex-root.pem")
    assert resolve_ssl() == (True, "/certs/amex-root.pem")
    monkeypatch.setenv("BQ_SSL_NO_VERIFY", "1")
    assert resolve_ssl() == (False, None)

    key = tmp_path / "key.json"
    key.write_text("{}")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setenv("BQ_PROJECT_ID", "prj-p-lumi-gpt")
    connection = BQConnection.from_env()
    assert connection.ssl_verify is False
    context = connection.ssl_context()
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


def test_from_env_fails_fast(monkeypatch, tmp_path):
    with pytest.raises(AuthError, match="project"):
        BQConnection.from_env()
    monkeypatch.setenv("BQ_PROJECT_ID", "prj-p-lumi-gpt")
    with pytest.raises(AuthError, match="SA key"):
        BQConnection.from_env()
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS",
                       str(tmp_path / "missing.json"))
    with pytest.raises(AuthError, match="not found on disk"):
        BQConnection.from_env()


def test_from_env_full_bootstrap_from_dotenv(monkeypatch, tmp_path):
    import os
    key = tmp_path / "prj-p-lumi-gpt.json"
    key.write_text("{}")
    env = tmp_path / "laptop.env"
    env.write_text(
        f"GOOGLE_APPLICATION_CREDENTIALS={key}\n"
        "BQ_PROJECT_ID=prj-p-lumi-gpt\n"
        "LUMI_BQ_DATA_PROJECT=axp-lumi\n"
        "BIGQUERY_URL=https://bigquery-prod.p.googleapis.com\n")
    monkeypatch.setenv("SAHS_ENV_FILE", str(env))
    connection = BQConnection.from_env()
    assert connection.project == "prj-p-lumi-gpt"
    # the tables live in another project: the sandbox qualifies with it
    assert connection.data_project == "axp-lumi"
    assert connection.endpoint == "https://bigquery-prod.p.googleapis.com"
    assert connection.key_path == key
    assert connection.ssl_verify is True
    # the bootstrap injected the direct-connection hosts
    assert "oauth2.googleapis.com" in os.environ["NO_PROXY"]
    assert "bigquery-prod.p.googleapis.com" in os.environ["NO_PROXY"]


def test_data_project_defaults_to_the_query_project(monkeypatch, tmp_path):
    key = tmp_path / "key.json"
    key.write_text("{}")
    monkeypatch.setenv("BQ_PROJECT_ID", "prj-p-lumi-gpt")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.delenv("BQ_DATA_PROJECT", raising=False)
    monkeypatch.delenv("LUMI_BQ_DATA_PROJECT", raising=False)
    connection = BQConnection.from_env()
    assert connection.data_project == "prj-p-lumi-gpt"
    monkeypatch.setenv("BQ_DATA_PROJECT", "axp-lumi")
    assert BQConnection.from_env().data_project == "axp-lumi"
