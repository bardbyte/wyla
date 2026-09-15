"""The wire, shared by the catalog and both model planes.

One route per connection, pinned. The stdlib proxy handler asks
``proxy_bypass(host)`` on every request, which reads NO_PROXY from the
process environment, so one connection's bypass list can silently
reroute another connection's calls; the handler here routes exactly
what it was given. TLS comes from the OS keychain when ``truststore`` is
installed (the clean fix for networks that intercept TLS), else a named
bundle, else the system default. A refusal is read for its message and
its machine reason, so a check script can name the remedy.
"""

from __future__ import annotations

import base64
import functools
import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator

from .env import env_flag, first_env, redact_url

CLOUD_PLATFORM = ("https://www.googleapis.com/auth/cloud-platform",)


class TransportError(RuntimeError):
    """A service refused or was unreachable. The message carries the
    server's own explanation, never a stack trace."""


class PinnedProxyHandler(urllib.request.ProxyHandler):
    """A proxy decision made per connection, never by the environment.

    An empty mapping is a direct connection; a mapping is that proxy
    (credentials in the URL become the Proxy-authorization header); the
    process's NO_PROXY is never consulted."""

    def proxy_open(self, req, proxy, type):            # noqa: A002
        orig_type = req.type
        proxy_type, user, password, hostport = \
            urllib.request._parse_proxy(proxy)         # noqa: SLF001
        if proxy_type is None:
            proxy_type = orig_type
        if user and password:
            user_pass = f"{urllib.parse.unquote(user)}:" \
                        f"{urllib.parse.unquote(password)}"
            creds = base64.b64encode(user_pass.encode()).decode("ascii")
            req.add_header("Proxy-authorization", "Basic " + creds)
        req.set_proxy(urllib.parse.unquote(hostport), proxy_type)
        if orig_type == proxy_type or orig_type == "https":
            return None
        return self.parent.open(req, timeout=req.timeout)


def plane_opener(proxies: dict[str, str],
                 context: ssl.SSLContext) -> urllib.request.OpenerDirector:
    """A urllib opener with one connection's route and TLS pinned.
    Passing the handlers replaces build_opener's environment-derived
    ones."""
    return urllib.request.build_opener(
        PinnedProxyHandler(dict(proxies)),
        urllib.request.HTTPSHandler(context=context))


_TRUSTSTORE = {"tried": False, "active": False}


def inject_truststore() -> bool:
    """Make the OS keychain the trust source, once per process, best
    effort: the injection is global, and a second call is a no-op."""
    if not _TRUSTSTORE["tried"]:
        _TRUSTSTORE["tried"] = True
        try:
            import truststore                        # type: ignore
            truststore.inject_into_ssl()
            _TRUSTSTORE["active"] = True
        except ImportError:
            pass
    return _TRUSTSTORE["active"]


def resolve_tls(*bundle_vars: str, insecure_var: str = ""
                ) -> tuple[bool, str | None]:
    """→ (verify, ca_bundle). Verification is off only under the named
    opt-out flag; the bundle comes from the named variables, then the
    usual REQUESTS_CA_BUNDLE / SSL_CERT_FILE."""
    if insecure_var and env_flag(insecure_var):
        return False, None
    return True, first_env(*bundle_vars, "REQUESTS_CA_BUNDLE",
                           "SSL_CERT_FILE")


def ssl_context(verify: bool = True,
                ca_bundle: str | None = None) -> ssl.SSLContext:
    if not verify:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    return ssl.create_default_context(cafile=ca_bundle)


Http = Callable[..., tuple[int, dict[str, str], bytes]]


def http_call(opener: urllib.request.OpenerDirector, method: str, url: str,
              headers: dict[str, str], body: bytes | None, *,
              timeout: float = 90.0) -> tuple[int, dict[str, str], bytes]:
    """One request → (status, headers, body). A 4xx/5xx is an answer,
    not an exception; only a transport failure raises."""
    request = urllib.request.Request(url, data=body, headers=headers,
                                     method=method)
    try:
        with opener.open(request, timeout=timeout) as response:
            return (response.status, dict(response.headers.items()),
                    response.read())
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers.items()), e.read()
    except urllib.error.URLError as e:
        raise TransportError(
            f"unreachable: {redact_url(str(e.reason))}") from e
    except (TimeoutError, OSError) as e:
        raise TransportError(f"transport: {type(e).__name__}: {e}") from e


def sse_stream(opener: urllib.request.OpenerDirector, url: str,
               headers: dict[str, str], body: bytes, *, timeout: float,
               what: str = "the model") -> Iterator[dict[str, Any]]:
    """The JSON frames of one server-sent-events answer, as they arrive."""
    request = urllib.request.Request(url, data=body, headers=headers,
                                     method="POST")
    try:
        with opener.open(request, timeout=timeout) as response:
            for raw in response:
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    yield json.loads(payload)
                except json.JSONDecodeError:
                    continue
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:400]
        raise TransportError(
            f"{what} refused the stream (HTTP {e.code}): {detail}") from e
    except urllib.error.URLError as e:
        raise TransportError(
            f"{what} unreachable: {redact_url(str(e.reason))}") from e


# ── reading a refusal ───────────────────────────────────────────

def json_body(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8", "replace") or "null")
    except json.JSONDecodeError:
        return None


_REASON_HEADERS = ("www-authenticate", "x-error", "x-error-message",
                   "x-reason", "x-apigw-error", "x-amzn-errortype",
                   "x-kong-response", "x-envoy-upstream-service-time")


def _header_hints(headers: dict[str, str] | None) -> str:
    """What a gateway says in its headers when it says nothing in the
    body: WWW-Authenticate and any header that names an error."""
    hints = []
    for key, value in (headers or {}).items():
        low = key.lower()
        if low in _REASON_HEADERS or "error" in low or "reason" in low:
            hints.append(f"{key}: {str(value)[:120]}")
    return "; ".join(hints)


def error_text(status: int, body: bytes,
               headers: dict[str, str] | None = None) -> str:
    """The server's own explanation of a refusal, in one line."""
    payload = json_body(body)
    if isinstance(payload, dict):
        code = payload.get("error_code") or payload.get("code") or ""
        tail = f" [{code}]" if isinstance(code, (str, int)) and code else ""
        err = payload.get("error")
        if isinstance(err, dict) and err.get("message"):
            return f"HTTP {status}: {err['message']}"[:300] + tail
        for key in ("message", "error_description", "error", "detail",
                    "description"):
            if isinstance(payload.get(key), str):
                return f"HTTP {status}: {payload[key]}"[:300] + tail
    text = body.decode("utf-8", "replace")[:200].strip()
    if not text:
        hints = _header_hints(headers)
        return (f"HTTP {status} with an empty body (the gateway answered "
                "before the service did)" + (f" · {hints}" if hints else ""))
    return f"HTTP {status}: {text}"


def error_reason(body: bytes) -> str:
    """The machine-readable reason of a Google refusal: the
    ``google.rpc.ErrorInfo`` detail's ``reason`` (IAM_PERMISSION_DENIED,
    USER_PROJECT_DENIED, SERVICE_DISABLED, …), else the error's status
    word, else empty. The check script keys its remedies on it."""
    payload = json_body(body)
    err = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(err, dict):
        return ""
    for detail in err.get("details") or []:
        if isinstance(detail, dict) and str(detail.get("@type", "")).endswith(
                "google.rpc.ErrorInfo"):
            reason = detail.get("reason")
            if isinstance(reason, str) and reason:
                return reason
    status = err.get("status")
    return status if isinstance(status, str) else ""


# ── the service-account token ───────────────────────────────────

def token_session(proxies: dict[str, str], verify: bool | str):
    """A requests session for the OAuth refresh, on the connection's
    route with its TLS settings. ``trust_env`` is off: the route is the
    connection's, never the environment's."""
    import requests                              # a google-auth companion
    session = requests.Session()
    session.trust_env = False
    session.proxies = dict(proxies)
    session.verify = verify
    return session


class ServiceAccountToken:
    """The OAuth access token for a service-account key: one credentials
    object per key, refreshed only when google-auth says it is no longer
    valid, the refresh bounded to 15 s so a dead network is a quick typed
    error instead of a two-minute hang. A refresh failure is raised at
    once: a token endpoint that cannot be reached is a dead network, not
    a rate limit, and must never ride a backoff ladder."""

    def __init__(self, key_path: Path | str | None,
                 session_factory: Callable[[], Any],
                 scopes: tuple[str, ...] = CLOUD_PLATFORM, *,
                 make_credentials: Callable[[], Any] | None = None,
                 refresh: Callable[[Any], None] | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.key_path = Path(key_path) if key_path else None
        self.scopes = tuple(scopes)
        self._session_factory = session_factory
        self._make_credentials = make_credentials
        self._refresh = refresh
        self._sleep = sleep
        self._lock = threading.Lock()
        self._creds: Any = None

    def __call__(self) -> str:
        with self._lock:
            try:
                if self._creds is None:
                    self._creds = self._make()
                if not getattr(self._creds, "valid", False):
                    self._refresh_bounded()
                return str(self._creds.token)
            except TransportError:
                raise
            except Exception as e:
                raise TransportError(
                    f"token: {e}: the OAuth fetch to oauth2.googleapis.com "
                    "failed before any call was made. Check the proxy and the "
                    "network path, then the key file") from e

    def _make(self) -> Any:
        if self._make_credentials is not None:
            return self._make_credentials()
        from google.oauth2 import service_account      # type: ignore
        return service_account.Credentials.from_service_account_file(
            str(self.key_path), scopes=list(self.scopes))

    def _refresh_bounded(self) -> None:
        if self._refresh is not None:
            self._refresh(self._creds)
            return
        from google.auth.transport.requests import Request  # type: ignore
        request = Request(session=self._session_factory())
        attempt: Any = functools.partial(request, timeout=15)
        try:
            self._creds.refresh(attempt)
        except TypeError:            # a google-auth that objects to the
            attempt = request        # pinned timeout: once, unbounded
            self._creds.refresh(attempt)
        except Exception:
            self._sleep(2)           # one quick retry for a blip
            self._creds.refresh(attempt)
