"""Gemini through EAG with a OneIdentity bearer token — the pieces a
validation needs, pure and injectable, BEFORE any of it enters the
harness.

OneIdentity mints a short-lived bearer token from an HMAC-signed
request (APP_ID + APP_SECRET); EAG fronts Gemini's own REST protocol
(generateContent / streamGenerateContent) behind that token. The
token answer does not say when the token dies (the guide says about
five minutes), so a client has to keep time itself: this module
measures the lifetime instead of assuming it, and reports what the
gateway does with the parts of the protocol our harness stands on —
native tool calls with their thought signatures echoed back, a system
instruction, thought summaries, and a real stream (or a burst that
only looks like one).

Everything here takes its transport as an argument; the check script
supplies urllib over a pinned route, the tests supply fakes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from sahs.util.auth import (_first_env, describe_route, env_proxies,
                            plane_opener, redact_url)

ONEID_TOKEN_URL = ("https://oneidentityapi-dev.aexp.com/security/digital"
                   "/v1/application/token")
EAG_BASE_URL = "https://eag-dev.aexp.com/genai/google/v1"
DEFAULT_MODEL = "gemini-2.5-pro"
DEFAULT_SCOPES = [
    "/genai/google/v1/models/gemini-2.5-pro/**::post",
    "/genai/google/v1/models/gemini-2.5-flash/**::post",
    "/genai/google/v1/models/bge-large-en/embeddings/**::post",
    "/genai/google/v1/models/bge-large-en/**::post",
]
TOKEN_FIELDS = ("access_token", "accessToken", "token", "bearer",
                "bearerToken", "jwt", "id_token")
EXPIRY_FIELDS = ("expires_in", "expiresIn", "expiry", "expires_at",
                 "expiresAt", "exp", "ttl", "validity")
PROBE_INTERVAL = 20.0          # seconds between token-lifetime probes


class EagError(RuntimeError):
    """A transport failure with its reason; never a stack trace."""


# ── OneIdentity: the signed token request ──────────────────────


def hmac_signature(app_id: str, version: str | int, timestamp: str,
                   secret_b64: str) -> str:
    """The OneIdentity signature: ``<appID>-<version>-<timestamp>``
    signed with HMAC-SHA256 under the base64-DECODED secret, as
    URL-safe base64 without ``=`` padding."""
    key = base64.b64decode(secret_b64.strip())
    message = f"{app_id}-{version}-{timestamp}".encode("utf-8")
    digest = hmac.new(key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def timestamp_now(unit: str = "ms", now: Callable[[], float] = time.time
                  ) -> str:
    """The X-Auth-Timestamp: epoch milliseconds by default, seconds
    when the gateway wants those (the check tries both)."""
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
    """The payload of a JWT, read WITHOUT verification — only to learn
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
    """Whatever the token answer says about its lifetime, by any of
    the usual names, top level or one level down; {} when it says
    nothing — the case the guide describes."""
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
    """A secret for the report: its length and a short hash, never a
    single character of it."""
    if not secret:
        return "empty"
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:8]
    return f"{len(secret)} chars · sha256 {digest}"


def extract_token(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in TOKEN_FIELDS:
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        for value in payload.values():        # one level down
            if isinstance(value, dict):
                inner = extract_token(value)
                if inner:
                    return inner
    return ""


# ── the transport: urllib over a pinned route ──────────────────


@dataclass
class Route:
    proxies: dict[str, str]
    context: ssl.SSLContext
    label: str

    def opener(self) -> urllib.request.OpenerDirector:
        return plane_opener(self.proxies, self.context)


def ssl_context() -> tuple[ssl.SSLContext, str]:
    """TLS for the corporate endpoints: truststore (the OS keychain)
    when installed, else the bundle named by EAG_CA_BUNDLE →
    REQUESTS_CA_BUNDLE → SSL_CERT_FILE, else the system default.
    Verification is never disabled."""
    note = "system default"
    try:
        import truststore                       # type: ignore
        truststore.inject_into_ssl()
        note = "truststore (OS keychain)"
    except ImportError:
        pass
    bundle = _first_env("EAG_CA_BUNDLE", "REQUESTS_CA_BUNDLE",
                        "SSL_CERT_FILE")
    context = ssl.create_default_context(cafile=bundle)
    if bundle:
        note += f" + bundle {bundle}"
    return context, note


def candidate_routes(env: dict[str, str]) -> list[Route]:
    """The routes to try, in order. EAG_ROUTE=direct or proxy pins
    one; auto (the default) tries direct first, then the corporate
    proxy the environment declares — the internal gateways usually
    answer direct, Google's endpoints never do."""
    context, note = ssl_context()
    wanted = (env.get("EAG_ROUTE") or "auto").strip().lower()
    proxied = env_proxies()
    direct = Route({}, context, f"direct · TLS {note}")
    via = Route(proxied, context,
                f"{describe_route(proxied)} · TLS {note}") if proxied else None
    if wanted == "direct":
        return [direct]
    if wanted == "proxy":
        return [via] if via else [direct]
    return [direct] + ([via] if via else [])


def http_call(route: Route, method: str, url: str,
              headers: dict[str, str], body: bytes | None, *,
              timeout: float = 90.0) -> tuple[int, dict[str, str], bytes]:
    """One request → (status, headers, body). A 4xx/5xx is an answer,
    not an exception; only a transport failure raises EagError."""
    request = urllib.request.Request(url, data=body, headers=headers,
                                     method=method)
    try:
        with route.opener().open(request, timeout=timeout) as response:
            return (response.status, dict(response.headers.items()),
                    response.read())
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers.items()), e.read()
    except urllib.error.URLError as e:
        raise EagError(f"unreachable via {redact_url(str(e.reason))}") from e
    except (TimeoutError, OSError) as e:
        raise EagError(f"transport: {type(e).__name__}: {e}") from e


def http_stream(route: Route, url: str, headers: dict[str, str],
                body: bytes, *, timeout: float = 120.0
                ) -> tuple[int, dict[str, str], Iterator[tuple[float, bytes]]]:
    """A streamed request → (status, headers, chunks as they arrive,
    each stamped with the seconds since the request was sent)."""
    request = urllib.request.Request(url, data=body, headers=headers,
                                     method="POST")
    started = time.perf_counter()
    try:
        response = route.opener().open(request, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers.items()), iter([(0.0, e.read())])
    except urllib.error.URLError as e:
        raise EagError(f"unreachable via {redact_url(str(e.reason))}") from e

    def chunks() -> Iterator[tuple[float, bytes]]:
        with response:
            while True:
                piece = response.read1(65536) if hasattr(response, "read1") \
                    else response.read(65536)
                if not piece:
                    return
                yield time.perf_counter() - started, piece

    return response.status, dict(response.headers.items()), chunks()


# ── reading Gemini's answers ───────────────────────────────────


def parts_of(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("candidates") or []
    if not candidates:
        return []
    return list(((candidates[0].get("content") or {}).get("parts")) or [])


def summarize_answer(payload: dict[str, Any]) -> dict[str, Any]:
    parts = parts_of(payload)
    thoughts = [p for p in parts if p.get("thought")]
    texts = [str(p.get("text") or "") for p in parts
             if not p.get("thought") and p.get("text")]
    calls = [p["functionCall"] for p in parts if "functionCall" in p]
    usage = payload.get("usageMetadata") or {}
    candidates = payload.get("candidates") or []
    return {
        "finish": (candidates[0].get("finishReason") if candidates else ""),
        "text": " ".join(texts).strip(),
        "thought_parts": len(thoughts),
        "thought_preview": (str(thoughts[0].get("text") or "")[:160]
                            if thoughts else ""),
        "function_calls": calls,
        "signature": any("thoughtSignature" in p for p in parts),
        "usage": {"prompt": usage.get("promptTokenCount"),
                  "output": usage.get("candidatesTokenCount"),
                  "thoughts": usage.get("thoughtsTokenCount"),
                  "total": usage.get("totalTokenCount")},
        "model_version": payload.get("modelVersion", ""),
    }


def classify_stream(content_type: str, chunks: list[tuple[float, bytes]]
                    ) -> dict[str, Any]:
    """What the gateway actually did with a streaming request: SSE
    frames, a JSON array of chunks (alt=sse ignored), or a single
    burst — and whether the pieces arrived over time or all at once.
    The answer text is assembled from whichever form came back."""
    raw = b"".join(piece for _t, piece in chunks)
    text = raw.decode("utf-8", "replace")
    stamps = [t for t, _p in chunks]
    head = text.lstrip()[:24]
    if "data:" in text:
        form = "sse"
        frames = [line[5:].strip() for line in text.splitlines()
                  if line.startswith("data:")]
    elif head.startswith("["):
        form = "json-array"
        try:
            frames = [json.dumps(item) for item in json.loads(text)]
        except json.JSONDecodeError:
            frames = []
    elif head.startswith("{"):
        form = "single-json"
        frames = [text]
    else:
        form = "unknown"
        frames = []
    answer, thoughts, finish = [], 0, ""
    for frame in frames:
        if not frame or frame == "[DONE]":
            continue
        try:
            payload = json.loads(frame)
        except json.JSONDecodeError:
            continue
        got = summarize_answer(payload)
        if got["text"]:
            answer.append(got["text"])
        thoughts += got["thought_parts"]
        finish = got["finish"] or finish
    first = stamps[0] if stamps else 0.0
    last = stamps[-1] if stamps else 0.0
    streamed = len(chunks) >= 3 and (last - first) >= 0.5 \
        and form in ("sse", "json-array")
    return {"form": form, "content_type": content_type,
            "chunks": len(chunks), "frames": len(frames),
            "first_chunk_s": round(first, 2), "last_chunk_s": round(last, 2),
            "streamed": streamed, "thought_parts": thoughts,
            "finish": finish, "text": " ".join(answer).strip(),
            "head": head[:24]}


def classify_probe(status: int) -> str:
    """A cheap, deliberately invalid request tells the token's state
    without spending model tokens: the gateway answers 401/403 to a
    dead token and 400 (a bad body) to a live one."""
    if status in (401, 403):
        return "dead"
    if status in (200, 400, 422):
        return "alive"
    return f"unclear ({status})"


# ── the check itself ───────────────────────────────────────────


Http = Callable[..., tuple[int, dict[str, str], bytes]]
Stream = Callable[..., tuple[int, dict[str, str],
                             Iterator[tuple[float, bytes]]]]


@dataclass
class Config:
    app_id: str = ""
    secret: str = ""
    auth_mode: str = "generated"
    bearer: str = ""
    token_url: str = ONEID_TOKEN_URL
    base_url: str = EAG_BASE_URL
    model: str = DEFAULT_MODEL
    version: str = "2"
    scopes: list[str] = field(default_factory=lambda: list(DEFAULT_SCOPES))
    timestamp_unit: str = "ms"
    thinking_budget: int = 1056
    show_thoughts: bool = True
    prompt: str = "In two sentences, what is a cash rewards credit card?"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = dict(os.environ if env is None else env)
        scopes = [s.strip() for s in (env.get("EAG_SCOPES") or "").split(",")
                  if s.strip()] or list(DEFAULT_SCOPES)
        try:
            budget = int(env.get("THINKING_BUDGET") or 1056)
        except ValueError:
            budget = 1056
        return cls(
            app_id=(env.get("APP_ID") or "").strip(),
            secret=(env.get("APP_SECRET") or "").strip(),
            auth_mode=(env.get("AUTH_MODE") or "generated").strip().lower(),
            bearer=(env.get("GEMINI_BEARER_TOKEN") or "").strip(),
            token_url=(env.get("ONEID_TOKEN_URL") or ONEID_TOKEN_URL).strip(),
            base_url=(env.get("EAG_BASE_URL") or EAG_BASE_URL).rstrip("/"),
            model=(env.get("GEMINI_MODEL") or DEFAULT_MODEL).strip(),
            version=str(env.get("AUTH_VERSION") or "2").strip(),
            scopes=scopes,
            timestamp_unit=(env.get("ONEID_TIMESTAMP_UNIT") or "ms").strip(),
            thinking_budget=budget,
            show_thoughts=(env.get("SHOW_THOUGHTS") or "true").lower()
            in ("1", "true", "yes", "on"),
            prompt=env.get("GEMINI_PROMPT") or cls.prompt,
        )

    def display(self) -> dict[str, Any]:
        return {"auth_mode": self.auth_mode, "app_id": self.app_id or "(unset)",
                "app_secret": fingerprint(self.secret),
                "bearer_from_env": fingerprint(self.bearer),
                "token_url": self.token_url, "base_url": self.base_url,
                "model": self.model, "version": self.version,
                "scopes": self.scopes, "timestamp_unit": self.timestamp_unit,
                "thinking_budget": self.thinking_budget,
                "show_thoughts": self.show_thoughts}


def _json(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8", "replace") or "null")
    except json.JSONDecodeError:
        return None


def _error_text(status: int, body: bytes) -> str:
    payload = _json(body)
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict) and err.get("message"):
            return f"HTTP {status}: {err['message']}"[:300]
        for key in ("message", "error_description", "error", "detail"):
            if isinstance(payload.get(key), str):
                return f"HTTP {status}: {payload[key]}"[:300]
    return f"HTTP {status}: {body.decode('utf-8', 'replace')[:200].strip()}"


def _generation_config(cfg: Config, thinking_key: str,
                       max_tokens: int = 1024) -> dict[str, Any]:
    out: dict[str, Any] = {"temperature": 0.3, "topP": 0.9, "topK": 40,
                           "maxOutputTokens": max_tokens}
    if cfg.thinking_budget:
        out["thinkingConfig"] = {thinking_key: cfg.show_thoughts,
                                 "thinkingBudget": cfg.thinking_budget}
    return out


def _bearer(token: str) -> dict[str, str]:
    return {"Content-Type": "application/json",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {token}"}


def run_checks(cfg: Config, http: Http, stream: Stream, *,
               now: Callable[[], float] = time.time,
               clock: Callable[[], float] = time.perf_counter,
               sleep: Callable[[float], None] = time.sleep,
               probe_minutes: float = 0.0,
               only: set[str] | None = None) -> dict[str, Any]:
    """Every check, in the order a client would need them, each
    recorded whether it passed or not; nothing stops early except a
    missing token, without which nothing else can be tried."""
    want = only or {"token", "generate", "stream", "tools", "system",
                    "probe"}
    report: dict[str, Any] = {"config": cfg.display(), "checks": []}

    def record(name: str, ok: bool | None, detail: str, **data: Any) -> None:
        report["checks"].append({"name": name, "ok": ok, "detail": detail})
        if data:
            report[name] = data

    def timed(fn, *a, **k):
        t0 = clock()
        out = fn(*a, **k)
        return out, round(clock() - t0, 2)

    # ── the token ──
    token, minted_at = "", now()
    if cfg.auth_mode == "env":
        token = cfg.bearer
        if not token:
            record("token", False, "AUTH_MODE=env but GEMINI_BEARER_TOKEN "
                   "is empty")
            return report
        claims = jwt_claims(token) or {}
        record("token", True, f"from the environment ({fingerprint(token)})",
               source="env", jwt=bool(claims),
               exp=claims.get("exp"), iat=claims.get("iat"),
               ttl_s=(claims["exp"] - claims["iat"]) if
               isinstance(claims.get("exp"), int)
               and isinstance(claims.get("iat"), int) else None)
    else:
        if not cfg.app_id or not cfg.secret:
            record("token", False, "APP_ID and APP_SECRET are needed "
                   "(AUTH_MODE=generated), or AUTH_MODE=env with "
                   "GEMINI_BEARER_TOKEN")
            return report
        units = [cfg.timestamp_unit] + [u for u in ("ms", "s")
                                        if u != cfg.timestamp_unit]
        attempts: list[dict[str, Any]] = []
        payload: Any = None
        for unit in units:
            stamp = timestamp_now(unit, now)
            headers = token_headers(cfg.app_id, cfg.secret,
                                    version=cfg.version, timestamp=stamp)
            body = json.dumps({"scope": cfg.scopes}).encode("utf-8")
            try:
                (status, _h, raw), seconds = timed(
                    http, "POST", cfg.token_url, headers, body)
            except EagError as e:
                attempts.append({"unit": unit, "error": str(e)})
                continue
            payload = _json(raw)
            token = extract_token(payload)
            attempts.append({"unit": unit, "status": status,
                             "seconds": seconds,
                             "keys": sorted(payload.keys())
                             if isinstance(payload, dict) else [],
                             "note": "" if token else _error_text(status, raw)})
            if token:
                minted_at = now()
                break
        if not token:
            record("token", False, "OneIdentity refused every attempt: "
                   + "; ".join(f"{a['unit']}: {a.get('note') or a.get('error')}"
                               for a in attempts), attempts=attempts)
            return report
        claims = jwt_claims(token) or {}
        expiry = find_expiry(payload) if isinstance(payload, dict) else {}
        ttl = ((claims["exp"] - claims["iat"])
               if isinstance(claims.get("exp"), int)
               and isinstance(claims.get("iat"), int) else None)
        used = attempts[-1]
        record("token", True,
               f"minted in {used['seconds']} s with a {used['unit']} "
               f"timestamp · answer keys: {', '.join(used['keys']) or '?'}"
               + (f" · says expiry: {expiry}" if expiry else
                  " · says nothing about expiry")
               + (f" · JWT exp−iat = {ttl} s" if ttl is not None else
                  " · not a JWT (no exp to read)"),
               source="oneidentity", unit=used["unit"], attempts=attempts,
               keys=used["keys"], expiry_fields=expiry, jwt=bool(claims),
               exp=claims.get("exp"), iat=claims.get("iat"), ttl_s=ttl,
               token=fingerprint(token))

    auth = _bearer(token)
    model_url = f"{cfg.base_url}/models/{cfg.model}"
    thinking_key = "includeThoughts"

    def generate(body: dict[str, Any]) -> tuple[int, Any, bytes, float]:
        (status, _h, raw), seconds = timed(
            http, "POST", f"{model_url}:generateContent",
            {**auth, "Accept": "application/json"},
            json.dumps(body).encode("utf-8"))
        return status, _json(raw), raw, seconds

    # ── generate ──
    if "generate" in want:
        body = {"contents": [{"role": "user",
                              "parts": [{"text": cfg.prompt}]}],
                "generationConfig": _generation_config(cfg, thinking_key)}
        try:
            status, payload, raw, seconds = generate(body)
            if status == 400 and cfg.thinking_budget and "thought" in \
                    _error_text(status, raw).lower():
                thinking_key = "include_thoughts"    # the guide's spelling
                body["generationConfig"] = _generation_config(cfg,
                                                              thinking_key)
                status, payload, raw, seconds = generate(body)
            if status == 200 and isinstance(payload, dict):
                got = summarize_answer(payload)
                record("generate", True,
                       f"200 in {seconds} s · finish {got['finish']} · "
                       f"tokens {got['usage']} · thoughts {got['thought_parts']}"
                       f" part(s) · text: {got['text'][:120]!r}",
                       seconds=seconds, thinking_key=thinking_key, **got)
            else:
                record("generate", False, _error_text(status, raw),
                       status=status, seconds=seconds)
        except EagError as e:
            record("generate", False, str(e))

    # ── stream ──
    if "stream" in want:
        body = {"contents": [{"role": "user",
                              "parts": [{"text": cfg.prompt}]}],
                "generationConfig": _generation_config(cfg, thinking_key)}
        raw_body = json.dumps(body).encode("utf-8")
        for suffix in ("?alt=sse", ""):
            try:
                status, headers, chunks = stream(
                    f"{model_url}:streamGenerateContent{suffix}",
                    {**auth, "Accept": "text/event-stream"
                     if suffix else "application/json"}, raw_body)
                collected = list(chunks)
            except EagError as e:
                record("stream", False, str(e), suffix=suffix)
                break
            content_type = {k.lower(): v for k, v in headers.items()}.get(
                "content-type", "")
            if status != 200:
                raw = b"".join(p for _t, p in collected)
                if suffix:
                    continue                    # try the plain form
                record("stream", False, _error_text(status, raw),
                       status=status)
                break
            got = classify_stream(content_type, collected)
            record("stream", got["streamed"] or None,
                   (f"{'streams' if got['streamed'] else 'arrives in one burst'}"
                    f" as {got['form']} ({content_type or 'no content-type'})"
                    f" · {got['chunks']} chunk(s), {got['frames']} frame(s)"
                    f" · first at {got['first_chunk_s']} s, last at "
                    f"{got['last_chunk_s']} s · thoughts {got['thought_parts']}"
                    f" · finish {got['finish']} · text: {got['text'][:80]!r}"
                    + ("" if suffix else " · alt=sse was refused; this is "
                       "the plain streamGenerateContent form")),
                   suffix=suffix, status=status, **got)
            break

    # ── native tools, with the signature echoed back ──
    if "tools" in want:
        tools = [{"functionDeclarations": [{
            "name": "lookup_metric",
            "description": "Look up a governed metric by name and return "
                           "its certified definition.",
            "parameters": {"type": "OBJECT",
                           "properties": {"name": {"type": "STRING"}},
                           "required": ["name"]}}]}]
        contents = [{"role": "user", "parts": [{
            "text": "Use the lookup_metric tool to look up 'Acquirer Net "
                    "Spend'. Do not answer from memory; call the tool."}]}]
        body = {"contents": contents, "tools": tools,
                "generationConfig": _generation_config(cfg, thinking_key)}
        try:
            status, payload, raw, seconds = generate(body)
            if status != 200 or not isinstance(payload, dict):
                record("tools", False, _error_text(status, raw), status=status)
            else:
                first = summarize_answer(payload)
                call = first["function_calls"][0] if first["function_calls"] \
                    else None
                if call is None:
                    record("tools", False, "the model answered without a "
                           f"functionCall part: {first['text'][:100]!r}",
                           first=first)
                else:
                    model_parts = parts_of(payload)      # verbatim, signed
                    contents = contents + [
                        {"role": "model", "parts": model_parts},
                        {"role": "user", "parts": [{"functionResponse": {
                            "name": call.get("name", ""),
                            **({"id": call["id"]} if call.get("id") else {}),
                            "response": {"status": "certified",
                                         "expression": "sum(trans_usd_am)",
                                         "table": "dw.gms_transaction"}}}]}]
                    body = {"contents": contents, "tools": tools,
                            "generationConfig": _generation_config(
                                cfg, thinking_key)}
                    status2, payload2, raw2, seconds2 = generate(body)
                    second = (summarize_answer(payload2)
                              if status2 == 200 and isinstance(payload2, dict)
                              else None)
                    record("tools", status2 == 200,
                           f"functionCall {call.get('name')}({call.get('args')})"
                           f" in {seconds} s · thoughtSignature "
                           f"{'present' if first['signature'] else 'absent'}"
                           + (f" · round trip 200 in {seconds2} s · text: "
                              f"{second['text'][:100]!r}" if second else
                              f" · round trip {_error_text(status2, raw2)}"),
                           call=call, signature=first["signature"],
                           round_trip_status=status2,
                           round_trip_text=(second or {}).get("text", ""),
                           seconds=seconds, round_trip_seconds=seconds2)
        except EagError as e:
            record("tools", False, str(e))

    # ── a system instruction ──
    if "system" in want:
        body = {"systemInstruction": {"parts": [{
                    "text": "Reply with exactly the single word PONG."}]},
                "contents": [{"role": "user", "parts": [{"text": "ping"}]}],
                "generationConfig": _generation_config(cfg, thinking_key, 64)}
        try:
            status, payload, raw, seconds = generate(body)
            if status == 200 and isinstance(payload, dict):
                got = summarize_answer(payload)
                record("system", True,
                       f"accepted in {seconds} s · text: {got['text'][:40]!r}"
                       + ("" if "pong" in got["text"].lower()
                          else " (the instruction was not followed)"),
                       followed="pong" in got["text"].lower(),
                       text=got["text"], seconds=seconds)
            else:
                record("system", False, _error_text(status, raw), status=status)
        except EagError as e:
            record("system", False, str(e))

    # ── the token's real lifetime ──
    if "probe" in want and probe_minutes > 0:
        timeline: list[dict[str, Any]] = []
        verdict = "alive"
        died_at: float | None = None
        deadline = minted_at + probe_minutes * 60
        while True:
            try:
                status, _h, raw = http("POST", f"{model_url}:generateContent",
                                       {**auth, "Accept": "application/json"},
                                       b"{}")
                state = classify_probe(status)
            except EagError as e:
                state, status = f"error: {e}", 0
            age = round(now() - minted_at)
            timeline.append({"age_s": age, "status": status, "state": state})
            if state == "dead":
                verdict, died_at = "dead", age
                break
            if now() >= deadline:
                break
            sleep(PROBE_INTERVAL)
        record("probe", died_at is not None or None,
               (f"the token died between {timeline[-2]['age_s'] if len(timeline) > 1 else 0}"
                f" s and {died_at} s after minting" if died_at is not None
                else f"still alive {timeline[-1]['age_s']} s after minting "
                     f"(the probe stopped at {probe_minutes} min)"),
               timeline=timeline, verdict=verdict, died_at_s=died_at,
               refresh_hint=(f"refresh at ~{int(died_at * 0.8)} s, or on the "
                             "first 401" if died_at else ""))

    return report


# ── the paste block ────────────────────────────────────────────


def render_report(report: dict[str, Any]) -> str:
    mark = {True: "✓", False: "✗", None: "·"}
    lines = ["=== EAG check ==="]
    cfg = report.get("config", {})
    lines.append(f"model     {cfg.get('model')} at {cfg.get('base_url')}")
    lines.append(f"token url {cfg.get('token_url')}")
    lines.append(f"auth      {cfg.get('auth_mode')} · APP_ID {cfg.get('app_id')}"
                 f" · APP_SECRET {cfg.get('app_secret')}")
    if report.get("route"):
        lines.append(f"route     {report['route']}")
    for check in report.get("checks", []):
        lines.append(f"{mark[check['ok']]} {check['name']:<9} {check['detail']}")
    verdict = []
    by_name = {c["name"]: c for c in report.get("checks", [])}
    if by_name.get("generate", {}).get("ok"):
        verdict.append("generateContent works")
    stream = by_name.get("stream")
    if stream:
        verdict.append("streams" if stream["ok"] else
                       "streaming is a burst" if stream["ok"] is None
                       else "streaming failed")
    tools = by_name.get("tools")
    if tools:
        verdict.append("native tools " + ("work" if tools["ok"] else "failed")
                       + (" with signatures" if report.get("tools", {})
                          .get("signature") else ""))
    token = report.get("token", {})
    if token.get("ttl_s"):
        verdict.append(f"token lives {token['ttl_s']} s by its JWT")
    probe = report.get("probe", {})
    if probe.get("died_at_s"):
        verdict.append(f"measured lifetime ≤ {probe['died_at_s']} s "
                       f"({probe.get('refresh_hint')})")
    if verdict:
        lines.append("verdict   " + " · ".join(verdict))
    return "\n".join(lines)
