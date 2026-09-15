"""The Vertex plane: Gemini 3.1 Pro Preview on a service-account key.

``VertexModel.converse`` speaks Gemini's native tool protocol over
``streamGenerateContent`` and yields events as they arrive: thought
summaries, prose, tool calls, then ``done`` with the model's parts
verbatim (every thought signature included), ready to be echoed back as
the model turn before the tool answers go in. The route rides the proxy
the environment declares, pinned on this connection; the token comes
from the key. The same shape as the catalog plane, with a different key
and a different project on purpose: one plane never borrows the other's.
"""

from __future__ import annotations

import http.client
import json
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator

from .env import ConfigError, describe_route, first_env, google_proxies
from .transport import (ServiceAccountToken, TransportError,
                        inject_truststore, plane_opener, resolve_tls,
                        sse_stream, ssl_context, token_session)

DEFAULT_ENDPOINT = "https://aiplatform.googleapis.com"
DEFAULT_LOCATION = "global"          # the globally routed endpoint: right
                                     # for the Gemini previews
DEFAULT_MODEL = "gemini-3.1-pro-preview"
# how long the stream may stay silent between chunks before the transport
# is called dead: a proxy's idle cut is usually 60–120 s, and thought
# summaries arrive well inside that
STREAM_SILENCE_SECONDS = 120.0


def resolve_endpoint(location: str) -> str:
    """VERTEX_API_BASE_URL wins; otherwise derived from the location. REST
    needs the regional host for a region, the plain host for global."""
    v = first_env("VERTEX_API_BASE_URL")
    if v:
        return v.rstrip("/")
    location = (location or "").strip().lower()
    if location and location != "global":
        return f"https://{location}-aiplatform.googleapis.com"
    return DEFAULT_ENDPOINT


@dataclass(frozen=True)
class VertexConnection:
    """Everything a model call needs to reach Vertex."""

    project: str
    location: str
    model: str
    endpoint: str
    key_path: Path | None
    ssl_verify: bool = True
    ca_bundle: str | None = None
    truststore_active: bool = False
    # THIS connection's route: the proxy as the environment declares it,
    # pinned here and in opener(); NO_PROXY is never consulted
    proxies: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "VertexConnection":
        """Validate → location → endpoint → route → TLS, failing fast with
        a typed error that names the variable. Reads the environment
        only; the scripts load the ``.env`` files first."""
        project = first_env("VERTEX_PROJECT_ID", "GOOGLE_CLOUD_PROJECT")
        if not project:
            raise ConfigError(
                "no Vertex project configured: set VERTEX_PROJECT_ID (or "
                "GOOGLE_CLOUD_PROJECT), e.g. in .env")
        key = first_env("VERTEX_SA_KEY", "SYNAPSE_VERTEX_SA_KEY",
                        "GOOGLE_APPLICATION_CREDENTIALS")
        if not key:
            raise ConfigError(
                "no Vertex key configured: set VERTEX_SA_KEY (or "
                "GOOGLE_APPLICATION_CREDENTIALS) to the key-file path, "
                "e.g. in .env")
        key_path = Path(key).expanduser()
        if not key_path.is_file():
            raise ConfigError(f"Vertex key not found on disk: {key_path}")
        active = inject_truststore()
        location = (first_env("VERTEX_LOCATION", "GOOGLE_CLOUD_LOCATION")
                    or DEFAULT_LOCATION)
        verify, bundle = resolve_tls("GEMINI_CA_BUNDLE",
                                     insecure_var="GEMINI_TLS_INSECURE")
        return cls(project=project, location=location,
                   model=first_env("VERTEX_MODEL", "GEMINI_MODEL")
                   or DEFAULT_MODEL,
                   endpoint=resolve_endpoint(location), key_path=key_path,
                   ssl_verify=verify, ca_bundle=bundle,
                   truststore_active=active,
                   proxies=google_proxies("VERTEX_DISABLE_PROXY"))

    def url(self, method: str = "generateContent") -> str:
        return (f"{self.endpoint}/v1/projects/{self.project}/locations/"
                f"{self.location}/publishers/google/models/"
                f"{self.model}:{method}")

    def ssl_context(self):
        return ssl_context(self.ssl_verify, self.ca_bundle)

    def opener(self):
        return plane_opener(self.proxies, self.ssl_context())

    def route(self) -> str:
        return describe_route(self.proxies)

    def token_session(self):
        return token_session(self.proxies,
                             (self.ca_bundle or True) if self.ssl_verify
                             else False)

    def describe(self) -> dict[str, Any]:
        """The resolved configuration, never a secret."""
        return {"project": self.project, "location": self.location,
                "model": self.model, "endpoint": self.endpoint,
                "key_file": str(self.key_path),
                "key_exists": bool(self.key_path and self.key_path.is_file()),
                "route": self.route(),
                "tls": ("verification DISABLED (GEMINI_TLS_INSECURE)"
                        if not self.ssl_verify else
                        f"bundle {self.ca_bundle}" if self.ca_bundle else
                        "system default")
                       + (" + truststore (OS keychain)"
                          if self.truststore_active else "")}


@dataclass
class VertexModel:
    """One model call at a time, streamed. ``token`` and ``stream`` are
    injectable so tests never touch a network."""

    connection: VertexConnection
    token: Callable[[], str] | None = None
    stream: Callable[[dict[str, Any]], Iterator[dict[str, Any]]] | None = None
    log: Callable[[str], None] | None = None
    usage: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "prompt_tokens": 0, "output_tokens": 0,
        "thought_tokens": 0})
    plane: str = "vertex"

    def __post_init__(self) -> None:
        if self.token is None:
            self.token = ServiceAccountToken(self.connection.key_path,
                                             self.connection.token_session)

    @property
    def model(self) -> str:
        return self.connection.model

    def describe(self) -> str:
        c = self.connection
        return f"{c.model} · {c.project} · {c.location} · {c.route()}"

    def _sse(self, body: dict[str, Any], *, timeout: float
             ) -> Iterator[dict[str, Any]]:
        headers = {"Authorization": f"Bearer {self.token()}",
                   "Content-Type": "application/json"}
        return sse_stream(self.connection.opener(),
                          self.connection.url("streamGenerateContent")
                          + "?alt=sse", headers,
                          json.dumps(body).encode("utf-8"),
                          timeout=timeout, what="Vertex")

    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "",
                 tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 include_thoughts: bool = True,
                 max_output_tokens: int = 8192,
                 timeout: float = STREAM_SILENCE_SECONDS
                 ) -> Iterator[dict[str, Any]]:
        """One model call in Gemini's native tool protocol, streamed.

        Yields ``{"kind": "text", "delta"}``, ``{"kind": "thought",
        "delta"}``, ``{"kind": "call", "name", "args", "id"}`` as they
        arrive, then ``{"kind": "done", "parts", "finish", "usage"}``
        where ``parts`` is the model's content VERBATIM, ready to be
        appended as the model turn before the tool answers go back.
        Temperature is left at the model default on purpose: lowering
        it loops a reasoning model.
        """
        body: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"maxOutputTokens": max_output_tokens}}
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        if tools:
            body["tools"] = [{"functionDeclarations": list(tools)}]
        if thinking_level:
            body["generationConfig"]["thinkingConfig"] = {
                "thinkingLevel": thinking_level,
                "includeThoughts": bool(include_thoughts)}
        self.usage["calls"] += 1

        chunks = (self.stream(body) if self.stream is not None
                  else self._sse(body, timeout=timeout))
        parts: list[dict[str, Any]] = []
        usage: dict[str, Any] = {}
        finish = ""
        # a stream that goes silent or is cut mid-way (a proxy idle
        # timeout, a reset) is a transport failure with a reason, never a
        # bare exception the turn dies on
        try:
            for chunk in chunks:
                usage = chunk.get("usageMetadata") or usage
                for candidate in chunk.get("candidates", []):
                    finish = candidate.get("finishReason") or finish
                    for part in (candidate.get("content") or {}).get(
                            "parts", []):
                        if "functionCall" in part:
                            parts.append(part)          # whole, signed
                            call = part["functionCall"]
                            yield {"kind": "call",
                                   "name": call.get("name", ""),
                                   "args": call.get("args") or {},
                                   "id": call.get("id", "")}
                        elif part.get("thought"):
                            parts.append(part)
                            if part.get("text"):
                                yield {"kind": "thought",
                                       "delta": part["text"]}
                        elif part.get("text") is not None:
                            # merge plain text runs; a signed part stays
                            # its own part so the echo keeps the signature
                            if (parts and "thoughtSignature" not in part
                                    and "thoughtSignature" not in parts[-1]
                                    and set(parts[-1]) == {"text"}):
                                parts[-1] = {"text": parts[-1]["text"]
                                             + part["text"]}
                            else:
                                parts.append(dict(part))
                            if part["text"]:
                                yield {"kind": "text", "delta": part["text"]}
        except TransportError:
            raise
        except (TimeoutError, socket.timeout) as e:
            raise TransportError(
                f"the model stream went silent for {timeout:.0f}s "
                f"({e or 'read timed out'}): the proxy or Vertex stopped "
                "answering") from e
        except (http.client.IncompleteRead, ConnectionError, OSError) as e:
            raise TransportError(
                f"the model stream was cut off: {type(e).__name__}: "
                f"{e}") from e
        got = {"prompt_tokens": int(usage.get("promptTokenCount") or 0),
               "output_tokens": int(usage.get("candidatesTokenCount") or 0),
               "thought_tokens": int(usage.get("thoughtsTokenCount") or 0),
               "cached_tokens": int(usage.get("cachedContentTokenCount")
                                    or 0)}
        for key in ("prompt_tokens", "output_tokens", "thought_tokens"):
            self.usage[key] += got[key]
        yield {"kind": "done", "parts": parts, "finish": finish,
               "usage": got}
