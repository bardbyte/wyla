"""The agent's model seam (Synapse v3 §3): one interaction, native
tools, streamed.

``VertexAgent`` rides the proven REST client's ``converse`` and
charges the session budget from real usage; ``ScriptedAgent`` is the
test double — it emits the same events from scripted PARTS (text and
tool calls), so the loop, the tools, the store, and the surface are
exercised for real while only the model is stand-in. Both expose
``json`` for the one-shot calls (judge, memory pass) that still want
a structured answer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterator

from sahs.ask.budget import Budget
from sahs.ask.model import ModelUnavailable, VertexModel
from sahs.enrich.client import EnrichTransportError, VertexClient
from sahs.util.auth import AuthError, VertexConnection

# the first sentence of the v3 identity: scripted doubles route on it
ROUTING_KEY = "You are Synapse, an analytical colleague"


@dataclass
class VertexAgent:
    client: VertexClient
    budget: Budget | None = None

    @staticmethod
    def from_env(budget: Budget | None = None,
                 log: Any = None) -> "VertexAgent":
        try:
            connection = VertexConnection.from_env()
        except AuthError as e:
            raise ModelUnavailable(
                f"{e}: the chat needs the Vertex contract in the silo "
                ".env (LUMI_VERTEX_SA_KEY, VERTEX_PROJECT_ID, "
                "VERTEX_LOCATION, VERTEX_MODEL)") from e
        return VertexAgent(VertexClient(connection, log=log), budget)

    def _charge(self, before: dict[str, int]) -> None:
        if self.budget is None:
            return
        usage = self.client.usage
        self.budget.charge(
            tokens_in=usage.get("prompt_tokens", 0)
            - before.get("prompt_tokens", 0),
            tokens_out=usage.get("output_tokens", 0)
            - before.get("output_tokens", 0)
            + usage.get("thought_tokens", 0)
            - before.get("thought_tokens", 0),
            calls=max(1, usage.get("calls", 0) - before.get("calls", 0)))

    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "", tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 max_output_tokens: int = 8192) -> Iterator[dict[str, Any]]:
        before = dict(self.client.usage)
        yielded = False
        attempts = 0
        try:
            while True:
                attempts += 1
                try:
                    for event in self.client.converse(
                            contents, system=system, tools=tools,
                            thinking_level=thinking_level,
                            max_output_tokens=max_output_tokens):
                        yielded = True
                        yield event
                    return
                except EnrichTransportError as e:
                    # the same contents, the same call: safe to ask
                    # once more — but only while nothing has reached
                    # the user, or the retry would duplicate it
                    if yielded or attempts >= 2:
                        raise ModelUnavailable(str(e)) from e
        finally:
            self._charge(before)

    def json(self, prompt: str, *, system: str = "",
             temperature: float = 0.0,
             max_tokens: int = 1024) -> dict | None:
        return VertexModel(self.client, self.budget).json(
            prompt, system=system, temperature=temperature,
            max_tokens=max_tokens)


class EagAgent(VertexAgent):
    """The same agent over Gemini through EAG: the client delivers each
    call in one burst (EAG serves no stream), the retry-once rule
    therefore always applies, and the budget is charged from the same
    usage counters."""

    @staticmethod
    def from_env(budget: Budget | None = None,
                 log: Any = None) -> "EagAgent":
        from sahs.enrich.eag_client import EagClient
        from sahs.util.eag import EagError
        try:
            client = EagClient.from_env(log=log)
        except EagError as e:
            raise ModelUnavailable(
                f"{e}: the EAG plane needs APP_ID and APP_SECRET (or "
                "AUTH_MODE=env with GEMINI_BEARER_TOKEN) in the silo .env; "
                "python scripts/eag_check.py proves the path") from e
        return EagAgent(client, budget)


# ── the planes as the composer lists them ─────────────────────
PLANE_IDS = ("vertex", "eag")


def pretty_model(raw: str) -> str:
    """gemini-2.5-pro → Gemini 2.5 Pro; gemini-3.1-pro-preview →
    Gemini 3.1 Pro Preview."""
    return " ".join(w.capitalize() if w.isalpha() else w
                    for w in (raw or "").replace("_", "-").split("-")
                    if w)


def plane_catalog() -> list[dict[str, Any]]:
    """Both model planes, whether or not this machine can ride them:
    id, the label the composer shows, the model id, availability with
    the reason when not, what choosing it means for the person, and
    which one a new chat starts on. Read from the environment each
    time, never cached: the .env is the switchboard."""
    from sahs.util.auth import (resolve_vertex_key_path,
                                resolve_vertex_model,
                                resolve_vertex_project)
    from sahs.util.eag import Config, eag_configured, model_plane
    default = model_plane()
    key = resolve_vertex_key_path()
    vertex_ok = bool(resolve_vertex_project() and key and key.exists())
    vertex_model = resolve_vertex_model()
    eag_model = Config.from_env().model
    return [
        {"id": "vertex",
         "label": f"{pretty_model(vertex_model)} via Vertex",
         "model": vertex_model,
         "available": vertex_ok,
         "reason": "" if vertex_ok else
         "needs the Vertex contract in the silo .env: LUMI_VERTEX_SA_KEY "
         "(a key file that exists) and VERTEX_PROJECT_ID",
         "means": "Google Cloud's Vertex AI with the service-account "
                  "key. Thinking streams as it happens and the answer "
                  "arrives word by word.",
         "feel": "streams",
         "default": default == "vertex"},
        {"id": "eag",
         "label": f"{pretty_model(eag_model)} via EAG",
         "model": eag_model,
         "available": eag_configured(),
         "reason": "" if eag_configured() else
         "needs APP_ID and APP_SECRET (or GEMINI_BEARER_TOKEN) in the "
         "silo .env; python scripts/eag_check.py proves the path",
         "means": "The enterprise AI gateway with a OneIdentity token "
                  "that renews itself every ten minutes. Each call "
                  "lands whole: a thinking pause, then the text at "
                  "once. No streaming.",
         "feel": "whole calls",
         "default": default == "eag"},
    ]


def agent_for(plane: str = "", budget: Budget | None = None,
              log: Any = None) -> VertexAgent:
    """The chat's model on a named plane — ``vertex`` or ``eag`` — or
    on the environment's default when the name is empty. An unknown
    name is a typed error, not a silent fallback: the composer only
    ever sends the two ids."""
    from sahs.util.eag import model_plane
    plane = (plane or "").strip().lower() or model_plane()
    if plane == "eag":
        return EagAgent.from_env(budget, log)
    if plane == "vertex":
        return VertexAgent.from_env(budget, log)
    raise ModelUnavailable(f"no model plane called {plane!r}: the "
                           "planes are vertex and eag")


def agent_from_env(budget: Budget | None = None,
                   log: Any = None) -> VertexAgent:
    """The chat's model, on whichever plane the environment names:
    SAHS_MODEL_PLANE=vertex|eag, or auto — EAG when its credentials
    are present, Vertex otherwise."""
    return agent_for("", budget, log)


@dataclass
class ScriptedAgent:
    """Scripted PARTS per model call: each step is a list of
    ``{"text": …}`` and/or ``{"call": {"name", "args"}}`` parts (or a
    callable returning one, resolved at call time). Off the routing
    key, json() answers ``{}`` and converse says nothing — other model
    users in the process stay untouched."""

    steps: list[Any] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)
    json_answers: list[Any] = field(default_factory=list)
    _n: int = 0

    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "", tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 max_output_tokens: int = 8192) -> Iterator[dict[str, Any]]:
        self.calls.append({"contents": contents, "system": system,
                           "tools": [t["name"] for t in (tools or [])],
                           "thinking_level": thinking_level})
        step = (self.steps.pop(0) if self.steps
                and ROUTING_KEY in system else [])
        if callable(step):
            step = step()
        parts: list[dict[str, Any]] = []
        for item in step or []:
            if "text" in item:
                yield {"kind": "text", "delta": item["text"]}
                parts.append({"text": item["text"]})
            elif "thought" in item:
                yield {"kind": "thought", "delta": item["thought"]}
                parts.append({"thought": True, "text": item["thought"]})
            elif "call" in item:
                self._n += 1
                call = {"name": item["call"]["name"],
                        "args": item["call"].get("args") or {},
                        "id": f"call_{self._n}"}
                parts.append({"functionCall": call,
                              "thoughtSignature": "scripted"})
                yield {"kind": "call", **call}
        yield {"kind": "done", "parts": parts, "finish": "STOP",
               "usage": {"prompt_tokens": 100, "output_tokens": 20,
                         "thought_tokens": 5, "cached_tokens": 0}}

    def json(self, prompt: str, *, system: str = "",
             temperature: float = 0.0, max_tokens: int = 1024) -> Any:
        return self.json_answers.pop(0) if self.json_answers else {}

    def stream(self, *a: Any, **k: Any) -> Iterator[str]:
        yield ""


def declarations(kit: dict[str, Any]) -> list[dict[str, Any]]:
    """The functionDeclarations block for a kit of ToolSpecs."""
    out = []
    for spec in kit.values():
        if spec.schema is None:
            continue
        out.append({"name": spec.name,
                    "description": spec.description,
                    "parameters": spec.schema})
    return out


__all__ = ["ROUTING_KEY", "VertexAgent", "EagAgent", "agent_from_env",
           "agent_for", "plane_catalog", "pretty_model", "PLANE_IDS",
           "ScriptedAgent", "declarations", "json"]
