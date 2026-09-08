"""Gemini through EAG, as the harness's model client.

The same event contract ``VertexClient.converse`` speaks — thought,
text, call, then done with the model's parts verbatim — delivered in
ONE burst per call, because EAG serves ``generateContent`` and not
the stream (the laptop's check, 2026-09-05: a 404 before Gemini on
both stream forms). The loop, the hooks, the store and the page
consume the events without knowing which plane produced them.

What this plane does differently, all measured on the laptop:
the model is addressed with a slash (``…/gemini-2.5-pro/generateContent``,
EAG's path-pattern scopes); the bearer token comes from a
TokenManager that mints on demand and refreshes before the token's
exp; thinking is a BUDGET under a cap that leaves room for the answer
(2.5 counts the thinking against maxOutputTokens); there is no prompt
cache to lean on. A 401 mid-turn mints a fresh token and retries the
same call once — safe, because nothing was streamed.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from sahs.enrich.client import _BACKOFFS, _RETRY_STATUSES, EnrichTransportError
from sahs.util.eag import (THINKING_BUDGETS, Config, EagError, Http,
                           RouteChooser, TokenManager, _error_text, _json,
                           candidate_routes, parts_of, thinking_budgets)

CALL_TIMEOUT = 180.0        # one whole answer, thinking included
MAX_CAP = 65536             # 2.5 Pro's output ceiling


@dataclass
class EagClient:
    cfg: Config
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
    plane: str = "eag"

    @property
    def model(self) -> str:
        return self.cfg.model

    @classmethod
    def from_env(cls, log: Callable[[str], None] | None = None
                 ) -> "EagClient":
        env = dict(os.environ)
        cfg = Config.from_env(env)
        if cfg.auth_mode != "env" and not (cfg.app_id and cfg.secret):
            raise EagError("the EAG plane needs APP_ID and APP_SECRET in "
                           "the silo .env (or AUTH_MODE=env with "
                           "GEMINI_BEARER_TOKEN)")
        if cfg.auth_mode == "env" and not cfg.bearer:
            raise EagError("AUTH_MODE=env but GEMINI_BEARER_TOKEN is empty")
        chooser = RouteChooser(candidate_routes(env))
        return cls(cfg=cfg, tokens=TokenManager(cfg, chooser.http),
                   http=chooser.http, log=log, budgets=thinking_budgets(env))

    def describe(self) -> str:
        return (f"{self.cfg.model} · {self.cfg.base_url} · "
                f"{self.tokens.describe()}")

    # ── one call ─────────────────────────────────────────────
    def _note(self, message: str) -> None:
        if self.log is not None:
            self.log(f"    [eag] {message}")

    def _url(self, method: str = "generateContent") -> str:
        sep = ":" if self.cfg.path_form == "colon" else "/"
        return f"{self.cfg.base_url}/models/{self.cfg.model}{sep}{method}"

    def _post(self, body: dict[str, Any], *,
              timeout: float = CALL_TIMEOUT) -> dict[str, Any]:
        """The body to EAG → Gemini's answer as a dict. A dead token is
        minted anew once; transient refusals back off; a rejected
        thinkingConfig is dropped for the rest of the run."""
        auth_retried = False
        last = "no attempt made"
        for attempt, backoff in enumerate(_BACKOFFS + (None,)):
            try:
                token = self.tokens.token()
            except EagError as e:
                raise EnrichTransportError(f"token: {e}") from e
            headers = {"Content-Type": "application/json",
                       "cache-control": "no-cache",
                       "Accept": "application/json",
                       "Authorization": f"Bearer {token}"}
            try:
                status, answer_headers, raw = self.http(
                    "POST", self._url(), headers,
                    json.dumps(body).encode("utf-8"), timeout=timeout)
            except EagError as e:
                last = f"transport: {e}"
                if backoff is None:
                    break
                self._note(f"{last} — retrying in {backoff}s")
                self.sleep(backoff)
                continue
            if status == 200:
                payload = _json(raw)
                if not isinstance(payload, dict):
                    raise EnrichTransportError(
                        "EAG answered 200 with a body that is not JSON")
                return payload
            reason = _error_text(status, raw, answer_headers)
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
                self._note("endpoint rejects thinkingConfig — disabled "
                           "for the rest of the run")
                last = reason
                continue
            if status in _RETRY_STATUSES and backoff is not None:
                self._note(f"{reason} — retrying in {backoff}s "
                           f"(attempt {attempt + 2}/{len(_BACKOFFS) + 1})")
                last = reason
                self.sleep(backoff)
                continue
            raise EnrichTransportError(f"EAG refused: {reason}")
        raise EnrichTransportError(
            f"EAG unreachable after {len(_BACKOFFS) + 1} attempts — "
            f"last: {last}")

    def _config(self, level: str, max_output_tokens: int,
                include_thoughts: bool = True, **extra: Any
                ) -> dict[str, Any]:
        budget = (self.budgets.get(level, self.budgets["medium"])
                  if self.thinking_ok else 0)
        # 2.5 counts the thinking against the cap: leave room
        config: dict[str, Any] = {
            "maxOutputTokens": min(max_output_tokens + budget, MAX_CAP),
            **extra}
        if budget:
            config["thinkingConfig"] = {"includeThoughts": bool(include_thoughts),
                                        "thinkingBudget": budget}
        return config

    @staticmethod
    def _usable(parts: list[dict[str, Any]]) -> bool:
        return any("functionCall" in p or (p.get("text") and not p.get("thought"))
                   for p in parts)

    def _account(self, payload: dict[str, Any]) -> dict[str, int]:
        usage = payload.get("usageMetadata") or {}
        got = {"prompt_tokens": int(usage.get("promptTokenCount") or 0),
               "output_tokens": int(usage.get("candidatesTokenCount") or 0),
               "thought_tokens": int(usage.get("thoughtsTokenCount") or 0),
               "cached_tokens": int(usage.get("cachedContentTokenCount") or 0)}
        for key in ("prompt_tokens", "output_tokens", "thought_tokens"):
            self.usage[key] = self.usage.get(key, 0) + got[key]
        return got

    # ── the conversation, one burst per call ─────────────────
    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "",
                 tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 include_thoughts: bool = True,
                 max_output_tokens: int = 8192,
                 timeout: float = CALL_TIMEOUT) -> Iterator[dict[str, Any]]:
        """One model call in Gemini's native tool protocol, delivered
        whole: the same events VertexClient.converse yields, in one
        burst — thought summaries, prose, tool calls, then done with
        the parts verbatim (thought signatures included) for the echo."""
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
        for n, part in enumerate(parts, start=1):
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

    # ── the one-shots (judge, title, memory): JSON mode ──────
    def generate(self, prompt: str, *, system: str = "",
                 temperature: float = 0.2,
                 max_output_tokens: int = 1024) -> str:
        body: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": self._config(
                "json", max_output_tokens, include_thoughts=False,
                temperature=temperature,
                responseMimeType="application/json")}
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        self.usage["calls"] = self.usage.get("calls", 0) + 1
        for _attempt in range(3):
            payload = self._post(body)
            parts = parts_of(payload)
            text = "".join(str(p.get("text") or "") for p in parts
                           if not p.get("thought"))
            finish = str(((payload.get("candidates") or [{}])[0]).get(
                "finishReason") or "")
            self._account(payload)
            if text.strip():
                return text
            if finish != "MAX_TOKENS":
                break
            cap = body["generationConfig"]["maxOutputTokens"]
            body["generationConfig"]["maxOutputTokens"] = min(cap * 4, MAX_CAP)
            self._note("empty JSON answer at MAX_TOKENS — growing the cap")
        raise EnrichTransportError(
            "EAG returned no text (the model spent the budget thinking; "
            "raise EAG_JSON_THINKING_BUDGET or max_output_tokens)")

    def generate_stream(self, prompt: str, *, system: str = "",
                        temperature: float = 0.3,
                        max_output_tokens: int = 1500,
                        json_mode: bool = False) -> Iterator[str]:
        """No stream through EAG: the whole answer, yielded once."""
        extra: dict[str, Any] = {"temperature": temperature}
        if json_mode:
            extra["responseMimeType"] = "application/json"
        body: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": self._config("low", max_output_tokens,
                                             include_thoughts=False, **extra)}
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        self.usage["calls"] = self.usage.get("calls", 0) + 1
        payload = self._post(body)
        self._account(payload)
        text = "".join(str(p.get("text") or "") for p in parts_of(payload)
                       if not p.get("thought"))
        if text:
            yield text
