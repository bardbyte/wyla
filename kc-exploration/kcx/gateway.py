"""The gateway plane: Gemini 2.5 Pro through an enterprise gateway.

An identity service mints a short-lived bearer token from an HMAC-signed
request (APP_ID + APP_SECRET); the gateway fronts Gemini's own REST
protocol behind that token. What this plane does differently from
Vertex, all learned on a real deployment: the model is addressed with a
slash (``…/gemini-2.5-pro/generateContent``, the gateway's path-pattern
scopes); the token answer does not say when the token dies, so the
manager reads the JWT's ``exp`` and mints again at 80 % of the lifetime
or on a 401; thinking is a BUDGET under a cap that leaves room for the
answer (2.5 counts the thinking against maxOutputTokens); each call
lands whole, because the gateway serves no stream. The route is decided
by the first real request: direct first, then the proxy the environment
declares. The two URLs have no defaults: a deployment's hostnames belong
in its ``.env``, not in code.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

from .env import ConfigError, describe_route, env_proxies, first_env
from .transport import (Http, TransportError, error_text, http_call,
                        inject_truststore, json_body, plane_opener)

DEFAULT_MODEL = "gemini-2.5-pro"
# the identity service answers {"authorization_token": "…"}; the other
# names are the usual suspects, tried after it
TOKEN_FIELDS = ("authorization_token", "authorizationToken", "access_token",
                "accessToken", "token", "bearer", "bearerToken", "jwt",
                "id_token")
EXPIRY_FIELDS = ("expires_in", "expiresIn", "expiry", "expires_at",
                 "expiresAt", "exp", "ttl", "validity")
DEFAULT_LIFETIME = 300.0     # when a token says nothing about its life
REFRESH_FRACTION = 0.8       # mint again at 80 % of the lifetime
MIN_REMAINING = 45.0         # and never start a call this close to the end
CALL_TIMEOUT = 180.0         # one whole answer, thinking included
MAX_CAP = 65536              # 2.5 Pro's output ceiling
RETRY_STATUSES = {429, 500, 502, 503, 504}
BACKOFFS = (2, 4, 8, 16)
# the depth dial's levels as token budgets; 2.5 counts the thinking
# against maxOutputTokens, so the client raises the cap by the budget
THINKING_BUDGETS = {"low": 1024, "medium": 4096, "high": 16384}


class GatewayError(TransportError):
    """The gateway or the identity service refused, or was unreachable."""


# ── the identity service: the signed token request ──────────────

def hmac_signature(app_id: str, version: str | int, timestamp: str,
                   secret_b64: str) -> str:
    """``<appID>-<version>-<timestamp>`` signed with HMAC-SHA256 under the
    base64-DECODED secret, as URL-safe base64 without ``=`` padding."""
    key = base64.b64decode(secret_b64.strip())
    message = f"{app_id}-{version}-{timestamp}".encode("utf-8")
    digest = hmac.new(key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def timestamp_now(unit: str = "ms",
                  now: Callable[[], float] = time.time) -> str:
    """The X-Auth-Timestamp: epoch milliseconds by default, seconds when
    the identity service wants those."""
    return str(int(now() * 1000)) if unit == "ms" else str(int(now()))


def token_headers(app_id: str, secret_b64: str, *, version: str = "2",
                  timestamp: str) -> dict[str, str]:
    return {"Content-Type": "application/json",
            "Accept": "application/json",
            "X-Auth-AppID": app_id,
            "X-Auth-Version": str(version),
            "X-Auth-Timestamp": timestamp,
            "X-Auth-Signature": hmac_signature(app_id, version, timestamp,
                                               secret_b64)}


def jwt_claims(token: str) -> dict[str, Any] | None:
    """The payload of a JWT, read WITHOUT verification, only to learn
    ``exp`` and ``iat``; None when the token is not a JWT."""
    parts = (token or "").split(".")
    if len(parts) != 3:
        return None
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return None
    return claims if isinstance(claims, dict) else None


def find_expiry(payload: dict[str, Any]) -> dict[str, Any]:
    """Whatever the token answer says about its lifetime, by any of the
    usual names, top level or one level down; {} when it says nothing."""
    found: dict[str, Any] = {}
    stack = [payload]
    while stack:
        row = stack.pop()
        for key, value in row.items():
            if key in EXPIRY_FIELDS and not isinstance(value, (dict, list)):
                found[key] = value
            elif isinstance(value, dict):
                stack.append(value)
    return found


def fingerprint(secret: str) -> str:
    """A secret for a report: its length and a short hash, never a single
    character of it."""
    if not secret:
        return "empty"
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:8]
    return f"{len(secret)} chars · sha256 {digest}"


def extract_token(payload: Any) -> tuple[str, str]:
    """→ (token, the field it was under); ("", "") when none. A known
    name first, one level down next, and as a last resort the single
    long space-free string in the answer, so an unfamiliar field name
    never turns a 200 into a false refusal."""
    if not isinstance(payload, dict):
        return "", ""
    for key in TOKEN_FIELDS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip(), key
    for key, value in payload.items():        # one level down
        if isinstance(value, dict):
            inner, name = extract_token(value)
            if inner:
                return inner, f"{key}.{name}"
    shaped = [(key, value) for key, value in payload.items()
              if isinstance(value, str) and len(value) >= 40
              and " " not in value]
    if len(shaped) == 1:
        return shaped[0][1], f"{shaped[0][0]} (by shape)"
    return "", ""


def thinking_budgets(env: dict[str, str] | None = None) -> dict[str, int]:
    """The level → budget table, with GATEWAY_THINKING_BUDGETS overrides
    (``low:512,medium:2048,…``)."""
    env = dict(os.environ if env is None else env)
    out = dict(THINKING_BUDGETS)
    for item in (env.get("GATEWAY_THINKING_BUDGETS") or "").split(","):
        key, _, value = item.strip().partition(":")
        if key.strip() in out and value.strip().isdigit():
            out[key.strip()] = int(value.strip())
    return out


def default_scopes(base_url: str, model: str) -> list[str]:
    """The token scope the gateway's path-pattern grants use, derived from
    the deployment's base path and the model: nothing about a deployment
    is a constant here."""
    path = urlparse(base_url).path.rstrip("/")
    return [f"{path}/models/{model}/**::post"]


@dataclass
class GatewayConfig:
    app_id: str = ""
    secret: str = ""
    auth_mode: str = "generated"   # generated: mint; env: use the bearer
    bearer: str = ""
    token_url: str = ""
    base_url: str = ""
    model: str = DEFAULT_MODEL
    version: str = "2"
    scopes: list[str] = field(default_factory=list)
    timestamp_unit: str = "ms"
    # slash (…/model/generateContent, the gateway's form) or colon
    # (…/model:generateContent, Google's own REST)
    path_form: str = "slash"
    route: str = "auto"            # direct | proxy | auto (direct first)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "GatewayConfig":
        env = dict(os.environ if env is None else env)
        base_url = (env.get("GATEWAY_BASE_URL") or "").strip().rstrip("/")
        # GATEWAY_MODEL first: GEMINI_MODEL is also read by the Vertex
        # plane, so a shared file's GEMINI_MODEL moves both planes
        model = (env.get("GATEWAY_MODEL") or env.get("GEMINI_MODEL")
                 or DEFAULT_MODEL).strip()
        scopes = [s.strip() for s in (env.get("GATEWAY_SCOPES") or "")
                  .split(",") if s.strip()]
        return cls(
            app_id=(env.get("APP_ID") or "").strip(),
            secret=(env.get("APP_SECRET") or "").strip(),
            auth_mode=(env.get("AUTH_MODE") or "generated").strip().lower(),
            bearer=(env.get("GEMINI_BEARER_TOKEN") or "").strip(),
            token_url=(env.get("IDP_TOKEN_URL") or "").strip(),
            base_url=base_url, model=model,
            version=str(env.get("AUTH_VERSION") or "2").strip(),
            scopes=scopes or (default_scopes(base_url, model)
                              if base_url else []),
            timestamp_unit=(env.get("IDP_TIMESTAMP_UNIT") or "ms").strip(),
            path_form=(env.get("GATEWAY_PATH_FORM") or "slash").strip().lower(),
            route=(env.get("GATEWAY_ROUTE") or "auto").strip().lower())

    def validate(self) -> None:
        """Every missing piece named at once, never one per attempt."""
        missing = []
        if not self.base_url:
            missing.append("GATEWAY_BASE_URL (the gateway's base, up to "
                           "and including /genai/google/v1)")
        if self.auth_mode == "env":
            if not self.bearer:
                missing.append("GEMINI_BEARER_TOKEN (AUTH_MODE=env)")
        else:
            if not self.token_url:
                missing.append("IDP_TOKEN_URL (the identity service's "
                               "token endpoint)")
            if not (self.app_id and self.secret):
                missing.append("APP_ID and APP_SECRET (or AUTH_MODE=env "
                               "with GEMINI_BEARER_TOKEN)")
        if missing:
            raise ConfigError("the gateway plane needs "
                              + "; ".join(missing) + ", e.g. in .env")

    def display(self) -> dict[str, Any]:
        """The configuration for a report: fingerprints, never secrets."""
        return {"auth_mode": self.auth_mode,
                "app_id": self.app_id or "(unset)",
                "app_secret": fingerprint(self.secret),
                "bearer_from_env": fingerprint(self.bearer),
                "token_url": self.token_url or "(unset)",
                "base_url": self.base_url or "(unset)",
                "model": self.model, "version": self.version,
                "scopes": list(self.scopes),
                "timestamp_unit": self.timestamp_unit,
                "path_form": self.path_form, "route": self.route}


# ── the transport: urllib over a pinned route ───────────────────

@dataclass
class Route:
    proxies: dict[str, str]
    context: ssl.SSLContext
    label: str

    def opener(self):
        return plane_opener(self.proxies, self.context)


def gateway_ssl_context() -> tuple[ssl.SSLContext, str]:
    """TLS for the gateway hosts: the OS keychain when truststore is
    installed, else the bundle named by GATEWAY_CA_BUNDLE →
    REQUESTS_CA_BUNDLE → SSL_CERT_FILE, else the system default.
    Verification is never disabled on this plane."""
    note = "truststore (OS keychain)" if inject_truststore() \
        else "system default"
    bundle = first_env("GATEWAY_CA_BUNDLE", "REQUESTS_CA_BUNDLE",
                       "SSL_CERT_FILE")
    context = ssl.create_default_context(cafile=bundle)
    if bundle:
        note += f" + bundle {bundle}"
    return context, note


def candidate_routes(wanted: str = "auto",
                     context: ssl.SSLContext | None = None) -> list[Route]:
    """The routes to try, in order. ``direct`` or ``proxy`` pins one;
    ``auto`` tries direct first, then the proxy the environment declares:
    internal gateways usually answer direct, Google's endpoints never
    do."""
    if context is None:
        context, note = gateway_ssl_context()
    else:
        note = "pinned"
    wanted = (wanted or "auto").strip().lower()
    proxied = env_proxies()
    direct = Route({}, context, f"direct · TLS {note}")
    via = Route(proxied, context,
                f"{describe_route(proxied)} · TLS {note}") if proxied else None
    if wanted == "direct":
        return [direct]
    if wanted == "proxy":
        return [via] if via else [direct]
    return [direct] + ([via] if via else [])


class RouteChooser:
    """The route is decided by the FIRST real request, not by a probe:
    each candidate is tried in order until one gets an HTTP answer of
    any status (a 403 is still an answer), and that route serves every
    later call."""

    def __init__(self, routes: list[Route], *, call: Any = None) -> None:
        self.routes = list(routes)
        self.chosen: Route | None = None
        self.failures: list[str] = []
        self._call = call or http_call

    @property
    def label(self) -> str:
        return self.chosen.label if self.chosen else "undecided"

    def http(self, method: str, url: str, headers: dict[str, str],
             body: bytes | None, **kw: Any):
        if self.chosen is not None:
            return self._call(self.chosen.opener(), method, url, headers,
                              body, **kw)
        self.failures = []                  # this request's, not a pile
        for route in self.routes:
            try:
                result = self._call(route.opener(), method, url, headers,
                                    body, **kw)
            except TransportError as e:
                self.failures.append(f"{route.label}: {e}")
                continue
            self.chosen = route
            return result
        raise GatewayError("no route reaches the gateway — "
                           + "; ".join(self.failures))


# ── the token manager ───────────────────────────────────────────

class TokenManager:
    """One identity-service token at a time: minted on demand, reused
    while comfortably valid, minted again at 80 % of its lifetime (read
    from its exp claim) or when a call is refused with a 401.
    Thread-safe. With AUTH_MODE=env the environment's bearer is used as
    it is."""

    def __init__(self, cfg: GatewayConfig, http: Http, *,
                 now: Callable[[], float] = time.time) -> None:
        self.cfg = cfg
        self._http = http
        self._now = now
        self._lock = threading.Lock()
        self._token = ""
        self._minted_at = 0.0
        self._expires_at = 0.0
        self.mints = 0

    def token(self) -> str:
        with self._lock:
            if self.cfg.auth_mode == "env":
                if not self.cfg.bearer:
                    raise GatewayError("AUTH_MODE=env but GEMINI_BEARER_TOKEN "
                                       "is empty")
                if not self._token:
                    self._adopt(self.cfg.bearer)
                return self._token
            if self._token and not self._stale():
                return self._token
            self._mint()
            return self._token

    def invalidate(self) -> None:
        """A 401 said the token is dead: the next call mints anew."""
        with self._lock:
            self._token = ""

    def remaining(self) -> float:
        return max(0.0, self._expires_at - self._now()) if self._token \
            else 0.0

    def describe(self) -> str:
        if not self._token:
            return "no token yet (minted on the first call)"
        age = int(self._now() - self._minted_at)
        return (f"token minted {age} s ago · {int(self.remaining())} s left "
                f"· refresh at "
                f"{int(REFRESH_FRACTION * (self._expires_at - self._minted_at))} s")

    def _stale(self) -> bool:
        now = self._now()
        lifetime = self._expires_at - self._minted_at
        return (now >= self._minted_at + REFRESH_FRACTION * lifetime
                or self._expires_at - now < MIN_REMAINING)

    def _adopt(self, token: str) -> None:
        now = self._now()
        claims = jwt_claims(token) or {}
        exp = claims.get("exp")
        self._token = token
        self._minted_at = now
        self._expires_at = (float(exp) if isinstance(exp, int) and exp > now
                            else now + DEFAULT_LIFETIME)

    def _mint(self) -> None:
        if not self.cfg.app_id or not self.cfg.secret:
            raise GatewayError("APP_ID and APP_SECRET are needed to mint a "
                               "token (or AUTH_MODE=env with "
                               "GEMINI_BEARER_TOKEN)")
        if not self.cfg.token_url:
            raise GatewayError("IDP_TOKEN_URL is needed to mint a token")
        stamp = timestamp_now(self.cfg.timestamp_unit, self._now)
        headers = token_headers(self.cfg.app_id, self.cfg.secret,
                                version=self.cfg.version, timestamp=stamp)
        body = json.dumps({"scope": self.cfg.scopes}).encode("utf-8")
        status, answer_headers, raw = self._http(
            "POST", self.cfg.token_url, headers, body, timeout=30)
        if status != 200:
            raise GatewayError("the identity service refused the token "
                               "request: "
                               + error_text(status, raw, answer_headers))
        token, _field = extract_token(json_body(raw))
        if not token:
            raise GatewayError("the identity service answered 200 but no "
                               "token field was recognized")
        self._adopt(token)
        self.mints += 1


# ── the model client ────────────────────────────────────────────

def parts_of(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("candidates") or []
    if not candidates:
        return []
    return list(((candidates[0].get("content") or {}).get("parts")) or [])


@dataclass
class GatewayModel:
    """Gemini through the gateway, speaking the same event contract as
    ``VertexModel.converse``, delivered in ONE burst per call."""

    cfg: GatewayConfig
    tokens: TokenManager
    http: Http
    log: Callable[[str], None] | None = None
    sleep: Callable[[float], None] = time.sleep
    budgets: dict[str, int] = field(
        default_factory=lambda: dict(THINKING_BUDGETS))
    usage: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "prompt_tokens": 0, "output_tokens": 0,
        "thought_tokens": 0})
    thinking_ok: bool = True
    plane: str = "gateway"
    chooser: RouteChooser | None = None

    @property
    def model(self) -> str:
        return self.cfg.model

    @classmethod
    def from_env(cls, log: Callable[[str], None] | None = None
                 ) -> "GatewayModel":
        cfg = GatewayConfig.from_env()
        cfg.validate()
        chooser = RouteChooser(candidate_routes(cfg.route))
        return cls(cfg=cfg, tokens=TokenManager(cfg, chooser.http),
                   http=chooser.http, log=log, budgets=thinking_budgets(),
                   chooser=chooser)

    def describe(self) -> str:
        route = f" · route {self.chooser.label}" if self.chooser else ""
        return (f"{self.cfg.model} · {self.cfg.base_url} · "
                f"{self.tokens.describe()}{route}")

    def _note(self, message: str) -> None:
        if self.log is not None:
            self.log(f"    [gateway] {message}")

    def _url(self, method: str = "generateContent") -> str:
        sep = ":" if self.cfg.path_form == "colon" else "/"
        return f"{self.cfg.base_url}/models/{self.cfg.model}{sep}{method}"

    def _post(self, body: dict[str, Any], *,
              timeout: float = CALL_TIMEOUT) -> dict[str, Any]:
        """The body to the gateway → Gemini's answer as a dict. A dead
        token is minted anew once; transient refusals back off; a
        rejected thinkingConfig is dropped for the rest of the run."""
        auth_retried = False
        last = "no attempt made"
        for attempt, backoff in enumerate(BACKOFFS + (None,)):
            token = self.tokens.token()
            headers = {"Content-Type": "application/json",
                       "cache-control": "no-cache",
                       "Accept": "application/json",
                       "Authorization": f"Bearer {token}"}
            try:
                status, answer_headers, raw = self.http(
                    "POST", self._url(), headers,
                    json.dumps(body).encode("utf-8"), timeout=timeout)
            except TransportError as e:
                last = f"transport: {e}"
                if backoff is None:
                    break
                self._note(f"{last} — retrying in {backoff}s")
                self.sleep(backoff)
                continue
            if status == 200:
                payload = json_body(raw)
                if not isinstance(payload, dict):
                    raise GatewayError("the gateway answered 200 with a "
                                       "body that is not JSON")
                return payload
            reason = error_text(status, raw, answer_headers)
            if status in (401, 403) and not auth_retried:
                auth_retried = True
                self.tokens.invalidate()
                self._note("the token was refused — minting a fresh one")
                last = reason
                continue
            if status == 400 and "thought" in reason.lower() \
                    and "thinkingConfig" in body.get("generationConfig", {}):
                del body["generationConfig"]["thinkingConfig"]
                self.thinking_ok = False
                self._note("the endpoint rejects thinkingConfig — disabled "
                           "for the rest of the run")
                last = reason
                continue
            if status in RETRY_STATUSES and backoff is not None:
                self._note(f"{reason} — retrying in {backoff}s (attempt "
                           f"{attempt + 2}/{len(BACKOFFS) + 1})")
                last = reason
                self.sleep(backoff)
                continue
            raise GatewayError(f"the gateway refused: {reason}")
        raise GatewayError(
            f"the gateway was unreachable after {len(BACKOFFS) + 1} "
            f"attempts — last: {last}")

    def _config(self, level: str, max_output_tokens: int,
                include_thoughts: bool = True) -> dict[str, Any]:
        budget = (self.budgets.get(level, self.budgets["medium"])
                  if self.thinking_ok else 0)
        # 2.5 counts the thinking against the cap: leave room
        config: dict[str, Any] = {
            "maxOutputTokens": min(max_output_tokens + budget, MAX_CAP)}
        if budget:
            config["thinkingConfig"] = {
                "includeThoughts": bool(include_thoughts),
                "thinkingBudget": budget}
        return config

    @staticmethod
    def _usable(parts: list[dict[str, Any]]) -> bool:
        return any("functionCall" in p
                   or (p.get("text") and not p.get("thought"))
                   for p in parts)

    def _account(self, payload: dict[str, Any]) -> dict[str, int]:
        usage = payload.get("usageMetadata") or {}
        got = {"prompt_tokens": int(usage.get("promptTokenCount") or 0),
               "output_tokens": int(usage.get("candidatesTokenCount") or 0),
               "thought_tokens": int(usage.get("thoughtsTokenCount") or 0),
               "cached_tokens": int(usage.get("cachedContentTokenCount")
                                    or 0)}
        for key in ("prompt_tokens", "output_tokens", "thought_tokens"):
            self.usage[key] = self.usage.get(key, 0) + got[key]
        return got

    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "",
                 tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 include_thoughts: bool = True,
                 max_output_tokens: int = 8192,
                 timeout: float = CALL_TIMEOUT) -> Iterator[dict[str, Any]]:
        """One model call in Gemini's native tool protocol, delivered
        whole: the same events ``VertexModel.converse`` yields, in one
        burst, then ``done`` with the parts verbatim for the echo."""
        body: dict[str, Any] = {
            "contents": contents,
            "generationConfig": self._config(thinking_level or "medium",
                                             max_output_tokens,
                                             include_thoughts)}
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        if tools:
            body["tools"] = [{"functionDeclarations": list(tools)}]
        self.usage["calls"] = self.usage.get("calls", 0) + 1
        payload = self._post(body, timeout=timeout)
        parts = parts_of(payload)
        finish = str(((payload.get("candidates") or [{}])[0]).get(
            "finishReason") or "")
        if finish == "MAX_TOKENS" and not self._usable(parts):
            # the budget went to thinking: grow the cap once and retry
            cap = body["generationConfig"]["maxOutputTokens"]
            body["generationConfig"]["maxOutputTokens"] = min(cap * 2, MAX_CAP)
            self._note("empty answer at MAX_TOKENS — growing the cap to "
                       f"{body['generationConfig']['maxOutputTokens']}")
            payload = self._post(body, timeout=timeout)
            parts = parts_of(payload)
            finish = str(((payload.get("candidates") or [{}])[0]).get(
                "finishReason") or "")
        for part in parts:
            if "functionCall" in part:
                call = part["functionCall"]
                yield {"kind": "call", "name": call.get("name", ""),
                       "args": call.get("args") or {},
                       "id": call.get("id", "")}
            elif part.get("thought"):
                if part.get("text"):
                    yield {"kind": "thought", "delta": part["text"]}
            elif part.get("text"):
                yield {"kind": "text", "delta": part["text"]}
        got = self._account(payload)
        yield {"kind": "done", "parts": parts, "finish": finish,
               "usage": got}
