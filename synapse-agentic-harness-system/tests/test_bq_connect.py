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


def test_the_bq_route_is_pinned_never_injected(monkeypatch):
    """The route is the connection's, not the process's: NO_PROXY is
    neither read nor written (the injection used to leak into the
    Vertex plane and hang every model call after the first dry run)."""
    import os
    from sahs.util.auth import bq_proxies
    monkeypatch.setenv("NO_PROXY", "internal.corp")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.corp:8080")
    summary = configure_network("https://bigquery-prod.p.googleapis.com")
    assert "direct" in summary["proxy"]
    assert os.environ["NO_PROXY"] == "internal.corp"      # never written
    assert bq_proxies() == {}

    monkeypatch.setenv("BQ_FORCE_PROXY", "1")
    summary = configure_network("https://bigquery-prod.p.googleapis.com")
    assert "forced" in summary["proxy"]
    assert bq_proxies() == {"https": "http://proxy.corp:8080"}
    assert os.environ["NO_PROXY"] == "internal.corp"      # untouched

    monkeypatch.delenv("BQ_FORCE_PROXY")
    monkeypatch.setenv("BQ_DISABLE_PROXY", "1")
    summary = configure_network("https://bigquery-prod.p.googleapis.com")
    assert "disabled" in summary["proxy"]
    assert bq_proxies() == {}
    # the environment is read, never edited: the proxy stays for the
    # plane that rides it
    assert os.environ["HTTPS_PROXY"] == "http://proxy.corp:8080"


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
    # the route is pinned on the connection: direct, and the process
    # environment did not gain a NO_PROXY
    assert connection.proxies == {} and connection.route() == "direct"
    assert "NO_PROXY" not in os.environ
    session = connection.token_session()
    assert session.trust_env is False and session.proxies == {}


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


def test_bq_token_is_cached_per_key_file(monkeypatch, tmp_path):
    from sahs.util import auth
    monkeypatch.setattr(auth, "_BQ_CREDENTIALS", {})
    made = {"n": 0}
    refreshed = {"n": 0}

    class Creds:
        valid = False
        token = ""

    def make():
        made["n"] += 1
        return Creds()

    def refresh(creds):
        refreshed["n"] += 1
        creds.valid = True
        creds.token = f"tok{refreshed['n']}"

    conn = BQConnection(project="p", endpoint="https://bq", location="US",
                        key_path=tmp_path / "k.json")
    assert conn.token(make_credentials=make, refresh=refresh) == "tok1"
    assert conn.token(make_credentials=make, refresh=refresh) == "tok1"
    assert made["n"] == 1 and refreshed["n"] == 1     # one trip, cached
    auth._BQ_CREDENTIALS[str(conn.key_path)].valid = False   # expired
    assert conn.token(make_credentials=make, refresh=refresh) == "tok2"
    assert made["n"] == 1 and refreshed["n"] == 2
