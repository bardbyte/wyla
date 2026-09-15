"""The wire: the pinned route, the readers of a refusal, the SSE frames,
the service-account token. Every opener here is a fake."""

from __future__ import annotations

import io
import ssl
import urllib.error
import urllib.request

import pytest

from kcx.transport import (PinnedProxyHandler, ServiceAccountToken,
                           TransportError, error_reason, error_text,
                           http_call, plane_opener, resolve_tls, sse_stream)
from tests.doubles import google_error

VERTEX = "https://us-central1-aiplatform.googleapis.com/v1/x:generateContent"


def test_the_pinned_handler_ignores_no_proxy(monkeypatch):
    """With googleapis in NO_PROXY the stdlib handler bypasses the proxy;
    the pinned handler routes what it was given and turns the URL's
    credentials into the Proxy-authorization header."""
    monkeypatch.setenv("NO_PROXY", "googleapis.com")
    monkeypatch.setenv("no_proxy", "googleapis.com")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.com:8080")
    assert urllib.request.proxy_bypass("us-central1-aiplatform.googleapis.com")
    proxy = "http://user:p%40ss@proxy.example.com:8080"

    stock = urllib.request.ProxyHandler({"https": proxy})
    stock.add_parent(urllib.request.OpenerDirector())
    req = urllib.request.Request(VERTEX)
    assert stock.proxy_open(req, proxy, "https") is None
    assert req.host == "us-central1-aiplatform.googleapis.com"   # bypassed

    pinned = PinnedProxyHandler({"https": proxy})
    pinned.add_parent(urllib.request.OpenerDirector())
    req = urllib.request.Request(VERTEX)
    assert pinned.proxy_open(req, proxy, "https") is None
    assert req.host == "proxy.example.com:8080"                   # routed
    assert req.get_header("Proxy-authorization", "").startswith("Basic ")
    # an empty mapping registers no scheme handler at all: direct
    assert not hasattr(PinnedProxyHandler({}), "https_open")
    opener = plane_opener({}, ssl.create_default_context())
    assert isinstance(opener, urllib.request.OpenerDirector)


class _Opener:
    def __init__(self, outcome):
        self.outcome = outcome

    def open(self, request, timeout=None):
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class _Response:
    status = 200

    def __init__(self, lines):
        self.lines = lines
        self.headers = {"content-type": "text/event-stream"}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def __iter__(self):
        return iter(self.lines)

    def read(self):
        return b"".join(self.lines)


def test_http_call_treats_http_errors_as_answers_and_failures_as_typed():
    err = urllib.error.HTTPError(
        "u", 429, "slow", {"Retry-After": "2"},
        io.BytesIO(b'{"error":{"message":"slow down"}}'))
    assert http_call(_Opener(err), "POST", "https://x", {}, b"{}") == (
        429, {"Retry-After": "2"}, b'{"error":{"message":"slow down"}}')
    ok = _Response([b"hello"])
    assert http_call(_Opener(ok), "GET", "https://x", {}, None) == (
        200, {"content-type": "text/event-stream"}, b"hello")
    with pytest.raises(TransportError, match="unreachable: dns"):
        http_call(_Opener(urllib.error.URLError("dns")), "GET", "https://x",
                  {}, None)
    with pytest.raises(TransportError, match="transport: TimeoutError"):
        http_call(_Opener(TimeoutError("t")), "GET", "https://x", {}, None)


def test_error_text_reads_the_message_then_common_keys_then_headers():
    assert error_text(403, google_error(403, "X", "no way")) == \
        "HTTP 403: no way"
    assert error_text(500, b'{"message": "boom", "error_code": "E1"}') == \
        "HTTP 500: boom [E1]"
    empty = error_text(401, b"", {"WWW-Authenticate": "Bearer realm=x"})
    assert empty.startswith("HTTP 401 with an empty body")
    assert "WWW-Authenticate: Bearer realm=x" in empty
    assert error_text(502, b"<html>bad gateway</html>") == \
        "HTTP 502: <html>bad gateway</html>"


def test_error_reason_reads_error_info_then_the_status_word():
    assert error_reason(google_error(403, "USER_PROJECT_DENIED", "m")) == \
        "USER_PROJECT_DENIED"
    assert error_reason(google_error(403, "", "m")) == "PERMISSION_DENIED"
    assert error_reason(b"nope") == ""
    assert error_reason(b'{"error": "a string"}') == ""


def test_sse_stream_parses_data_frames_and_skips_the_rest():
    lines = [b'data: {"a": 1}\n', b'\n', b': keepalive\n', b'data: not json\n',
             b'data: [DONE]\n', b'data: {"b": 2}\n']
    frames = list(sse_stream(_Opener(_Response(lines)), "https://x", {},
                             b"{}", timeout=5))
    assert frames == [{"a": 1}, {"b": 2}]
    err = urllib.error.HTTPError("u", 403, "no", {}, io.BytesIO(b"denied"))
    with pytest.raises(TransportError,
                       match=r"Vertex refused the stream \(HTTP 403\): denied"):
        list(sse_stream(_Opener(err), "https://x", {}, b"{}", timeout=5,
                        what="Vertex"))
    with pytest.raises(TransportError, match="unreachable"):
        list(sse_stream(_Opener(urllib.error.URLError("down")), "https://x",
                        {}, b"{}", timeout=5))


class _Creds:
    def __init__(self):
        self.valid = False
        self.token = None


def test_the_service_account_token_is_cached_refreshed_when_stale_and_typed():
    creds = _Creds()
    refreshes: list[int] = []

    def refresh(c):
        refreshes.append(1)
        c.valid = True
        c.token = f"tok{len(refreshes)}"
    made: list[int] = []

    def make():
        made.append(1)
        return creds
    token = ServiceAccountToken("k.json", lambda: None,
                                make_credentials=make, refresh=refresh)
    assert token() == "tok1" and made == [1] and refreshes == [1]
    assert token() == "tok1" and refreshes == [1]     # still valid: reused
    creds.valid = False
    assert token() == "tok2" and made == [1]          # one credentials object

    def broken(c):
        raise OSError("dns down")
    dead = ServiceAccountToken("k.json", lambda: None,
                               make_credentials=_Creds, refresh=broken)
    with pytest.raises(TransportError, match="token: dns down"):
        dead()


def test_resolve_tls_prefers_the_named_bundle_and_only_the_flag_disables(
        monkeypatch):
    assert resolve_tls("KC_CA_BUNDLE", insecure_var="KC_TLS_INSECURE") == \
        (True, None)
    monkeypatch.setenv("SSL_CERT_FILE", "/certs/root.pem")
    assert resolve_tls("KC_CA_BUNDLE") == (True, "/certs/root.pem")
    monkeypatch.setenv("KC_CA_BUNDLE", "/certs/kc.pem")
    assert resolve_tls("KC_CA_BUNDLE") == (True, "/certs/kc.pem")
    monkeypatch.setenv("KC_TLS_INSECURE", "1")
    assert resolve_tls("KC_CA_BUNDLE") == (True, "/certs/kc.pem")
    assert resolve_tls("KC_CA_BUNDLE", insecure_var="KC_TLS_INSECURE") == \
        (False, None)
