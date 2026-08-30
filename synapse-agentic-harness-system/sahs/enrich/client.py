"""Vertex generateContent client — urllib + SA token, no SDK.

Mirrors the proven BQ substrate transport exactly (same NO_PROXY, SSL
and token machinery via VertexConnection); ``token_provider`` and
``transport`` are injectable so tests never touch the network. Retries
transient refusals (429/5xx) with exponential backoff; a non-transient
error surfaces as a typed EnrichTransportError with the server's own
message — never a stack trace."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

from sahs.util.auth import VertexConnection

_RETRY_STATUSES = {429, 500, 502, 503, 504}
_BACKOFFS = (2, 4, 8, 16)


class EnrichTransportError(RuntimeError):
    """Vertex refused or was unreachable — message carries the server's
    own explanation."""


@dataclass
class VertexClient:
    connection: VertexConnection
    token_provider: Callable[[], str] | None = None
    transport: Callable[[dict], dict] | None = None   # tests inject
    sleep: Callable[[float], None] = time.sleep
    usage: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "prompt_tokens": 0, "output_tokens": 0})
    thinking_ok: bool = True     # flips off for the run when the
                                 # endpoint rejects thinking_config
                                 # (proven graceful-degrade behavior)

    def _token(self) -> str:
        if self.token_provider is not None:
            return str(self.token_provider())
        from google.auth.transport.requests import Request  # type: ignore
        from google.oauth2 import service_account           # type: ignore
        creds = service_account.Credentials.from_service_account_file(
            str(self.connection.key_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(Request(session=self.connection.token_session()))
        return creds.token

    def _url(self) -> str:
        c = self.connection
        return (f"{c.endpoint}/v1/projects/{c.project}/locations/"
                f"{c.location}/publishers/google/models/"
                f"{c.model}:generateContent")

    def _post(self, body: dict) -> dict:
        if self.transport is not None:
            return self.transport(body)
        request = urllib.request.Request(
            self._url(), data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(
                request, timeout=120,
                context=self.connection.ssl_context()) as response:
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
                    last_error = f"thinking rejected: {message}"
                    continue
                if e.code in _RETRY_STATUSES and backoff is not None:
                    last_error = f"HTTP {e.code}: {message}"
                    self.sleep(backoff)
                    continue
                raise EnrichTransportError(
                    f"vertex refused (HTTP {e.code}): {message}") from e
            except EnrichTransportError:
                raise
            except Exception as e:
                if backoff is not None:
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
            if text.strip():
                return text
            finish = (candidates[0].get("finishReason")
                      if candidates else "?")
            if finish == "MAX_TOKENS" and backoff is not None:
                # reasoning models (gemini 3.x) burn output budget on
                # internal thought BEFORE any text — the field symptom
                # is a 200 with empty parts. Grow the cap and retry
                # (bounded by the attempt budget; no sleep — this is
                # not a rate issue).
                grown = min(int(body["generationConfig"]
                                ["maxOutputTokens"]) * 4, 8192)
                body["generationConfig"]["maxOutputTokens"] = grown
                last_error = ("no text at finishReason=MAX_TOKENS — "
                              f"retrying with maxOutputTokens={grown}")
                continue
            raise EnrichTransportError(
                f"vertex returned no text (finishReason={finish})"
                + (" — the model spent the whole budget thinking; "
                   "raise max_output_tokens or set "
                   "GEMINI_THINKING_BUDGET"
                   if finish == "MAX_TOKENS" else ""))
        raise EnrichTransportError(
            f"vertex unreachable after {len(_BACKOFFS) + 1} attempts — "
            f"last: {last_error}")


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
