"""Vertex generateContent client — urllib + SA token, no SDK.

Mirrors the proven BQ substrate transport exactly (same NO_PROXY, SSL
and token machinery via VertexConnection); ``token_provider`` and
``transport`` are injectable so tests never touch the network. Retries
transient refusals (429/5xx) with exponential backoff; a non-transient
error surfaces as a typed EnrichTransportError with the server's own
message — never a stack trace."""

from __future__ import annotations

import http.client
import json
import socket
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

from sahs.util.auth import VertexConnection

_RETRY_STATUSES = {429, 500, 502, 503, 504}
_BACKOFFS = (2, 4, 8, 16)


# how long the streamed model call may stay silent between chunks
# before we call the transport dead (a corporate proxy's idle cut is
# usually 60–120 s; Gemini's thought summaries arrive well inside that)
STREAM_SILENCE_SECONDS = 120.0


class EnrichTransportError(RuntimeError):
    """Vertex refused or was unreachable — message carries the server's
    own explanation."""


@dataclass
class VertexClient:
    connection: VertexConnection
    token_provider: Callable[[], str] | None = None
    transport: Callable[[dict], dict] | None = None   # tests inject
    stream_transport: Callable[[dict], Any] | None = None  # ditto,
                                 # for the streaming path (E18 Ask)
    sleep: Callable[[float], None] = time.sleep
    usage: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "prompt_tokens": 0, "output_tokens": 0})
    thinking_ok: bool = True     # flips off for the run when the
                                 # endpoint rejects thinking_config
                                 # (proven graceful-degrade behavior)
    log: Callable[[str], None] | None = None   # live progress hook —
                                 # retries and self-heals are invisible
                                 # without it (a call can legitimately
                                 # sit through 5 attempts × 120s)
    _creds: Any = field(default=None, repr=False)  # cached credentials:
                                 # one token per lifetime, not per call

    def _note(self, message: str) -> None:
        if self.log is not None:
            self.log(f"    [vertex] {message}")

    def _token(self) -> str:
        """One credentials object per client, refreshed only when the
        cached token is no longer valid (google-auth tracks expiry).
        A refresh failure raises EnrichTransportError IMMEDIATELY: a
        token endpoint that cannot even be reached is a dead network,
        not a rate limit, so it must never ride the backoff ladder —
        the loop turns it into the honest model_unavailable card
        instead of a 6-minute hang (E21 0a field lesson)."""
        if self.token_provider is not None:
            return str(self.token_provider())
        try:
            from google.auth.transport.requests import Request  # type: ignore
            from google.oauth2 import service_account           # type: ignore
            if self._creds is None:
                self._creds = \
                    service_account.Credentials.from_service_account_file(
                        str(self.connection.key_path),
                        scopes=["https://www.googleapis.com/auth/"
                                "cloud-platform"])
            if not self._creds.valid:
                request = Request(session=self.connection.token_session())
                try:                     # bound the wait: 15s, not the
                    import functools     # transport's 120s default
                    shim = functools.partial(request, timeout=15)
                except Exception:
                    shim = request
                attempt = shim
                try:
                    self._creds.refresh(attempt)
                except TypeError:        # a google-auth that objects to
                    attempt = request    # the pinned timeout: retry
                    self._creds.refresh(attempt)   # unbounded, once
                except Exception:
                    self.sleep(2)        # one quick retry for a blip,
                    self._creds.refresh(attempt)   # still bounded
            return self._creds.token
        except EnrichTransportError:
            raise
        except Exception as e:
            raise EnrichTransportError(
                f"token: {e}: the OAuth fetch to oauth2.googleapis.com "
                "failed before any model call was made. Check the "
                "proxy/VPN, then python scripts/vertex_check.py") from e

    def _url(self, method: str = "generateContent") -> str:
        c = self.connection
        return (f"{c.endpoint}/v1/projects/{c.project}/locations/"
                f"{c.location}/publishers/google/models/"
                f"{c.model}:{method}")

    def _post(self, body: dict) -> dict:
        if self.transport is not None:
            return self.transport(body)
        request = urllib.request.Request(
            self._url(), data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        with self.connection.opener().open(
                request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))

    def generate(self, prompt: str, *, system: str = "",
                 temperature: float = 0.2,
                 max_output_tokens: int = 1024) -> str:
        """→ the model's text. JSON-mode is requested via
        responseMimeType; parsing/validation is the caller's job (the
        loop counts invalid outputs — a bad generation is data, not a
        crash)."""
        body: dict[str, Any] = {
            "contents": [{"role": "user",
                          "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
                "responseMimeType": "application/json",
            },
        }
        # GEMINI_THINKING_BUDGET (proven laptop knob): set → attach;
        # "0" disables thinking explicitly; unset → model default
        budget = (os.environ.get("GEMINI_THINKING_BUDGET") or "").strip()
        if budget and self.thinking_ok:
            try:
                body["generationConfig"]["thinkingConfig"] = {
                    "thinkingBudget": int(budget)}
            except ValueError:
                pass
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        last_error = "no attempt made"
        for attempt, backoff in enumerate(_BACKOFFS + (None,)):
            try:
                payload = self._post(body)
            except urllib.error.HTTPError as e:
                try:
                    detail = json.loads(e.read().decode("utf-8"))
                    message = detail.get("error", {}).get(
                        "message", str(e))
                except Exception:
                    message = str(e)
                if e.code == 400 and "thinking" in message.lower() \
                        and "thinkingConfig" in body["generationConfig"]:
                    # endpoint rejects thinking — degrade gracefully
                    # for the whole run, retry this call without it
                    del body["generationConfig"]["thinkingConfig"]
                    self.thinking_ok = False
                    self._note("endpoint rejects thinking_config — "
                               "disabled for the rest of the run")
                    last_error = f"thinking rejected: {message}"
                    continue
                if e.code in _RETRY_STATUSES and backoff is not None:
                    self._note(f"HTTP {e.code} — retrying in {backoff}s "
                               f"(attempt {attempt + 2}/"
                               f"{len(_BACKOFFS) + 1})")
                    last_error = f"HTTP {e.code}: {message}"
                    self.sleep(backoff)
                    continue
                raise EnrichTransportError(
                    f"vertex refused (HTTP {e.code}): {message}") from e
            except EnrichTransportError:
                raise
            except Exception as e:
                if backoff is not None:
                    self._note(f"transport error — retrying in "
                               f"{backoff}s (attempt {attempt + 2}/"
                               f"{len(_BACKOFFS) + 1}): "
                               f"{str(e)[:80]}")
                    last_error = f"transport: {e}"
                    self.sleep(backoff)
                    continue
                raise EnrichTransportError(f"transport: {e}") from e
            meta = payload.get("usageMetadata", {})
            self.usage["calls"] += 1
            self.usage["prompt_tokens"] += int(
                meta.get("promptTokenCount") or 0)
            self.usage["output_tokens"] += int(
                meta.get("candidatesTokenCount") or 0)
            self.usage["thought_tokens"] = (
                self.usage.get("thought_tokens", 0)
                + int(meta.get("thoughtsTokenCount") or 0))
            candidates = payload.get("candidates") or []
            parts = ((candidates[0].get("content") or {}).get("parts")
                     if candidates else None) or []
            text = "".join(str(p.get("text") or "") for p in parts)
            finish = (candidates[0].get("finishReason")
                      if candidates else "?")
            if finish == "MAX_TOKENS" and backoff is not None:
                # reasoning models (gemini 3.x) burn output budget on
                # internal thought BEFORE any text. Two field faces of
                # the same cap: a 200 with empty parts, OR a 200 with
                # PARTIAL text — half a JSON object cut mid-emission
                # (the b1.4 smoke's empty predictions). Either way the
                # answer is unusable: grow the cap and retry (bounded
                # by the attempt budget; no sleep — not a rate issue).
                grown = min(int(body["generationConfig"]
                                ["maxOutputTokens"]) * 4, 8192)
                body["generationConfig"]["maxOutputTokens"] = grown
                self._note(("truncated" if text.strip() else "empty")
                           + " answer at finishReason=MAX_TOKENS "
                           "(model spent the budget thinking) — "
                           f"growing maxOutputTokens to {grown}, "
                           "retrying")
                last_error = ("MAX_TOKENS truncation — retrying with "
                              f"maxOutputTokens={grown}")
                continue
            if text.strip():
                return text
            raise EnrichTransportError(
                f"vertex returned no text (finishReason={finish})"
                + (" — the model spent the whole budget thinking; "
                   "raise max_output_tokens or set "
                   "GEMINI_THINKING_BUDGET"
                   if finish == "MAX_TOKENS" else ""))
        raise EnrichTransportError(
            f"vertex unreachable after {len(_BACKOFFS) + 1} attempts — "
            f"last: {last_error}")


    # ── streaming (E18 Ask): the chat surface needs first-token
    # latency, so the conversational lane rides :streamGenerateContent.
    # The batch enrichment path above is untouched.
    def generate_stream(self, prompt: str, *, system: str = "",
                        temperature: float = 0.3,
                        max_output_tokens: int = 1500,
                        json_mode: bool = False):
        """Yield text deltas as the model produces them.

        No retry loop here on purpose: a stream that dies mid-answer
        cannot be silently restarted without lying about what the user
        already read. The caller surfaces the break as an honest error
        event and offers regenerate."""
        body: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
            },
        }
        if json_mode:
            body["generationConfig"]["responseMimeType"] = "application/json"
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        budget = (os.environ.get("GEMINI_THINKING_BUDGET") or "").strip()
        if budget and self.thinking_ok:
            body["generationConfig"]["thinkingConfig"] = {
                "thinkingBudget": int(budget)}
        self.usage["calls"] = self.usage.get("calls", 0) + 1

        if self.stream_transport is not None:
            for chunk in self.stream_transport(body):
                yield chunk
            return

        request = urllib.request.Request(
            self._url("streamGenerateContent") + "?alt=sse",
            data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        try:
            with self.connection.opener().open(
                    request, timeout=120) as response:
                for raw in response:
                    line = raw.decode("utf-8").strip()
                    if not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if not payload or payload == "[DONE]":
                        continue
                    try:
                        chunk = json.loads(payload)
                    except json.JSONDecodeError:
                        continue
                    usage = chunk.get("usageMetadata") or {}
                    if usage:
                        self.usage["prompt_tokens"] = usage.get(
                            "promptTokenCount", self.usage["prompt_tokens"])
                        self.usage["output_tokens"] = usage.get(
                            "candidatesTokenCount",
                            self.usage["output_tokens"])
                    for candidate in chunk.get("candidates", []):
                        for part in (candidate.get("content", {})
                                     .get("parts", [])):
                            text = part.get("text")
                            if text:
                                yield text
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:400]
            raise EnrichTransportError(
                f"vertex refused the stream (HTTP {e.code}): {detail}")
        except urllib.error.URLError as e:
            raise EnrichTransportError(f"vertex unreachable: {e.reason}")


    # ── v3: the native tool protocol, streamed ───────────────
    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "",
                 tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 include_thoughts: bool = True,
                 max_output_tokens: int = 8192,
                 timeout: float = STREAM_SILENCE_SECONDS):
        """One model call in Gemini's native tool protocol, streamed.

        Yields events as they arrive:
          {"kind": "text",    "delta": str}       prose for the user
          {"kind": "thought", "delta": str}       a thought summary
          {"kind": "call",    "name", "args", "id"}   a tool call
        and finally
          {"kind": "done", "parts": [...], "usage": {...},
           "finish": str}
        where ``parts`` is the model's content VERBATIM — text,
        thoughts, functionCall parts, and every thoughtSignature —
        ready to be appended to ``contents`` as the model turn before
        the functionResponse parts go back. Temperature is left at
        the model default on purpose (lowering it loops a reasoning
        model); JSON mode is never requested here.
        """
        body: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"maxOutputTokens": max_output_tokens},
        }
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        if tools:
            body["tools"] = [{"functionDeclarations": list(tools)}]
        if thinking_level and self.thinking_ok:
            body["generationConfig"]["thinkingConfig"] = {
                "thinkingLevel": thinking_level,
                "includeThoughts": bool(include_thoughts)}
        self.usage["calls"] = self.usage.get("calls", 0) + 1

        chunks = (self.stream_transport(body)
                  if self.stream_transport is not None
                  else self._sse(body, timeout=timeout))
        parts: list[dict[str, Any]] = []
        usage: dict[str, Any] = {}
        finish = ""
        # a stream that goes silent or is cut mid-way (a proxy idle
        # timeout, a reset) is a transport failure with a reason, never
        # a bare exception the turn dies on
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
                            # merge plain text runs; a signed part
                            # stays its own part so the echo keeps
                            # the signature
                            if (parts and "thoughtSignature" not in part
                                    and "thoughtSignature"
                                    not in parts[-1]
                                    and set(parts[-1]) == {"text"}):
                                parts[-1] = {"text": parts[-1]["text"]
                                             + part["text"]}
                            else:
                                parts.append(dict(part))
                            if part["text"]:
                                yield {"kind": "text",
                                       "delta": part["text"]}
        except EnrichTransportError:
            raise
        except (TimeoutError, socket.timeout) as e:
            raise EnrichTransportError(
                f"the model stream went silent for {timeout:.0f}s "
                f"({e or 'read timed out'}) — the proxy or Vertex "
                "stopped answering") from e
        except (http.client.IncompleteRead, ConnectionError,
                OSError) as e:
            raise EnrichTransportError(
                f"the model stream was cut off: {type(e).__name__}: "
                f"{e}") from e
        self.usage["prompt_tokens"] = self.usage.get("prompt_tokens", 0) \
            + int(usage.get("promptTokenCount") or 0)
        self.usage["output_tokens"] = self.usage.get("output_tokens", 0) \
            + int(usage.get("candidatesTokenCount") or 0)
        self.usage["thought_tokens"] = self.usage.get("thought_tokens", 0) \
            + int(usage.get("thoughtsTokenCount") or 0)
        yield {"kind": "done", "parts": parts, "finish": finish,
               "usage": {
                   "prompt_tokens": int(usage.get("promptTokenCount")
                                        or 0),
                   "output_tokens": int(usage.get("candidatesTokenCount")
                                        or 0),
                   "thought_tokens": int(usage.get("thoughtsTokenCount")
                                         or 0),
                   "cached_tokens": int(usage.get(
                       "cachedContentTokenCount") or 0)}}

    def _sse(self, body: dict[str, Any], *, timeout: float = 300.0):
        """The raw SSE chunks of one streamGenerateContent call."""
        request = urllib.request.Request(
            self._url("streamGenerateContent") + "?alt=sse",
            data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        try:
            # the connection's opener pins the Vertex route: via the
            # corporate proxy even after a BigQuery call in this process
            with self.connection.opener().open(
                    request, timeout=timeout) as response:
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
            raise EnrichTransportError(
                f"vertex refused the stream (HTTP {e.code}): {detail}")
        except urllib.error.URLError as e:
            raise EnrichTransportError(f"vertex unreachable: {e.reason}")


def parse_json_answer(text: str) -> dict | None:
    """Model output → dict, tolerating a fenced block; None when the
    text is not a JSON object (counted upstream, never a crash)."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None
