"""The two network planes never reroute each other.

The laptop stall: every turn hung on the model call right after the
first successful dry run. BQConnection.from_env wrote googleapis.com
into the process's NO_PROXY for the PSC contract, and urllib's proxy
handler consults NO_PROXY on every request, so from that moment every
Vertex call (the OAuth refresh and the stream) went direct — into the
corporate blackhole — until the 120s silence timeout. Each plane now
pins its route on its own connection and its opener never consults
NO_PROXY; these tests hold that line.
"""

from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

VERTEX = "https://us-central1-aiplatform.googleapis.com/v1/x:stream"


def _proxy_handler(opener):
    from sahs.util.auth import PinnedProxyHandler
    return next(h for h in opener.handlers
                if isinstance(h, PinnedProxyHandler))


def test_the_pinned_handler_ignores_no_proxy(monkeypatch):
    """With googleapis in NO_PROXY the stdlib handler bypasses the
    proxy (the bug); the pinned handler routes what it was given and
    turns the URL's credentials into the Proxy-authorization header."""
    from sahs.util.auth import PinnedProxyHandler
    # both spellings: urllib reads the lowercase one last, so it wins;
    # the old injection wrote both
    monkeypatch.setenv("NO_PROXY", "googleapis.com")
    monkeypatch.setenv("no_proxy", "googleapis.com")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.corp:8080")
    assert urllib.request.proxy_bypass(
        "us-central1-aiplatform.googleapis.com")      # the bug's premise
    proxy = "http://user:p%40ss@proxy.corp:8080"

    stock = urllib.request.ProxyHandler({"https": proxy})
    stock.add_parent(urllib.request.OpenerDirector())
    req = urllib.request.Request(VERTEX)
    assert stock.proxy_open(req, proxy, "https") is None
    assert req.host == "us-central1-aiplatform.googleapis.com"   # bypassed

    pinned = PinnedProxyHandler({"https": proxy})
    pinned.add_parent(urllib.request.OpenerDirector())
    req = urllib.request.Request(VERTEX)
    assert pinned.proxy_open(req, proxy, "https") is None
    assert req.host == "proxy.corp:8080"                          # routed
    assert req.get_header("Proxy-authorization", "").startswith("Basic ")
    # an empty mapping registers no scheme handler at all: direct
    direct = PinnedProxyHandler({})
    assert not hasattr(direct, "https_open")


def test_a_bigquery_connection_never_reroutes_vertex(tmp_path,
                                                     monkeypatch):
    """The sequence that hung the laptop: BQ first, then Vertex."""
    from sahs.util.auth import BQConnection, VertexConnection
    empty = tmp_path / "empty.env"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty))
    for name in ("NO_PROXY", "no_proxy", "BQ_FORCE_PROXY",
                 "BQ_DISABLE_PROXY", "VERTEX_DISABLE_PROXY",
                 "VERTEX_NO_PROXY_GOOGLE", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.corp:8080")
    key = tmp_path / "k.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setenv("BQ_PROJECT_ID", "prj-p-lumi-gpt")
    monkeypatch.setenv("BIGQUERY_URL",
                       "https://bigquery-prod.p.googleapis.com")
    monkeypatch.setenv("VERTEX_PROJECT_ID", "prj-d-ea-poc")

    bq = BQConnection.from_env()             # the first dry run's setup
    vertex = VertexConnection.from_env()     # the next model call's
    assert bq.proxies == {} and bq.route() == "direct"
    assert vertex.proxies == {"https": "http://proxy.corp:8080"}
    assert "NO_PROXY" not in os.environ and "no_proxy" not in os.environ

    # the openers carry the routes: BQ direct, Vertex via the proxy —
    # and the Vertex one keeps routing with googleapis in NO_PROXY
    monkeypatch.setenv("NO_PROXY", "googleapis.com")
    monkeypatch.setenv("no_proxy", "googleapis.com")
    # direct means NO proxy handler of any kind: the environment-derived
    # one is replaced and the pinned one registers no scheme
    assert not any(isinstance(h, urllib.request.ProxyHandler)
                   for h in bq.opener().handlers)
    handler = _proxy_handler(vertex.opener())
    assert handler.proxies == {"https": "http://proxy.corp:8080"}
    req = urllib.request.Request(VERTEX)
    handler.proxy_open(req, "http://proxy.corp:8080", "https")
    assert req.host == "proxy.corp:8080"
    # the token refresh sessions ride the same routes, environment off
    assert bq.token_session().trust_env is False
    assert bq.token_session().proxies == {}
    assert vertex.token_session().trust_env is False
    assert vertex.token_session().proxies == {
        "https": "http://proxy.corp:8080"}
    # TLS travels with the opener too
    https = next(h for h in vertex.opener().handlers
                 if isinstance(h, urllib.request.HTTPSHandler))
    assert https._context is not None                    # noqa: SLF001


def test_the_doctor_reads_the_planes_without_the_network(tmp_path,
                                                         monkeypatch):
    sys.path.insert(0, str(SILO / "scripts"))
    import turn_doctor
    empty = tmp_path / "empty.env"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty))
    for name in ("BQ_PROJECT_ID", "LUMI_BQ_PROJECT", "GOOGLE_CLOUD_PROJECT",
                 "VERTEX_PROJECT_ID", "LUMI_VERTEX_PROJECT",
                 "GOOGLE_APPLICATION_CREDENTIALS", "LUMI_BQ_SA_KEY",
                 "LUMI_VERTEX_SA_KEY", "BQ_FORCE_PROXY",
                 "VERTEX_DISABLE_PROXY", "VERTEX_NO_PROXY_GOOGLE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "googleapis.com")
    monkeypatch.setenv("no_proxy", "googleapis.com")
    text = turn_doctor.planes()
    assert "BigQuery  not configured" in text
    assert "Vertex    not configured" in text
    assert "ignored on both planes by design" in text
    key = tmp_path / "k.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setenv("BQ_PROJECT_ID", "p")
    monkeypatch.setenv("VERTEX_PROJECT_ID", "v")
    monkeypatch.setenv("HTTPS_PROXY", "http://u:pw@proxy.corp:8080")
    text = turn_doctor.planes()
    assert "BigQuery  direct" in text
    assert "Vertex    via http://proxy.corp:8080" in text
    assert "pw" not in text                                # redacted
