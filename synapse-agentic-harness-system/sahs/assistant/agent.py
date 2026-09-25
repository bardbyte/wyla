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
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from sahs.ask.budget import Budget
from sahs.ask.model import ModelUnavailable, VertexModel
from sahs.enrich.client import EnrichTransportError, VertexClient
from sahs.util.auth import AuthError, VertexConnection
from sahs.util.profiles import profile_for, temperature_for

# the first sentence of the v3 identity: scripted doubles route on it
ROUTING_KEY = "You are Radix, an analytical colleague"


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
                ".env (SYNAPSE_VERTEX_SA_KEY, VERTEX_PROJECT_ID, "
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
                 max_output_tokens: int = 8192,
                 should_stop: Callable[[], bool] | None = None
                 ) -> Iterator[dict[str, Any]]:
        """One model call, streamed. ``should_stop`` is the stop
        button's flag: the client stops reading at the next chunk and
        closes the response; a stream left mid-way for any other
        reason is closed here too."""
        before = dict(self.client.usage)
        yielded = False
        attempts = 0
        try:
            while True:
                attempts += 1
                stream = self.client.converse(
                    contents, system=system, tools=tools,
                    thinking_level=thinking_level,
                    max_output_tokens=max_output_tokens,
                    should_stop=should_stop)
                try:
                    for event in stream:
                        yielded = True
                        yield event
                        if should_stop is not None and should_stop():
                            return
                    return
                except EnrichTransportError as e:
                    # the same contents, the same call: safe to ask
                    # once more — but only while nothing has reached
                    # the user, or the retry would duplicate it
                    if yielded or attempts >= 2:
                        raise ModelUnavailable(str(e)) from e
                finally:
                    close = getattr(stream, "close", None)
                    if callable(close):
                        close()
        finally:
            self._charge(before)

    @property
    def model_name(self) -> str:
        """The model id this agent rides (gemini-3.1-pro-preview, …):
        the engine the prompt style and the sampling policy key on."""
        return model_name_of(self.client)

    def _json_client(self) -> Any:
        """The client the one-shot JSON calls ride: this agent's own,
        unless a plane routes them elsewhere (the gateway can)."""
        return self.client

    def json(self, prompt: str, *, system: str = "",
             temperature: float = 0.0,
             max_tokens: int = 1024) -> dict | None:
        client = self._json_client()
        # Gemini 3 keeps the model's default temperature (Google's
        # guidance: lowering it loops a thinking model); the older
        # dialect takes the caller's number. SAHS_TEMPERATURE_POLICY
        # =explicit sends it regardless
        return VertexModel(client, self.budget).json(
            prompt, system=system,
            temperature=temperature_for(model_name_of(client), temperature),
            max_tokens=max_tokens)


def model_name_of(client: Any) -> str:
    """The model id a client rides: the gateway client's ``model``, the
    Vertex client's ``connection.model``; "" for a double."""
    name = getattr(client, "model", "")
    if not name:
        name = getattr(getattr(client, "connection", None), "model", "")
    return str(name or "")


class GatewayAgent(VertexAgent):
    """The same agent over Gemini through the gateway: the client delivers each
    call in one burst (the gateway serves no stream), the retry-once rule
    therefore always applies, and the budget is charged from the same
    usage counters. The one-shot JSON calls (judge, title, memory,
    reviews) can ride a lighter model of the same plane:
    GATEWAY_JSON_MODEL=gemini-3.1-flash-lite."""

    _json_cache: Any = None

    @staticmethod
    def from_env(budget: Budget | None = None,
                 log: Any = None, model: str = "") -> "GatewayAgent":
        from sahs.enrich.gateway_client import GatewayClient
        from sahs.util.gateway import GatewayError
        try:
            client = GatewayClient.from_env(log=log, model=model)
        except GatewayError as e:
            raise ModelUnavailable(
                f"{e}: the gateway plane needs APP_ID and APP_SECRET (or "
                "AUTH_MODE=env with GEMINI_BEARER_TOKEN) in the silo .env; "
                "python scripts/gateway_check.py proves the path") from e
        return GatewayAgent(client, budget)

    def _json_client(self) -> Any:
        wanted = (os.environ.get("GATEWAY_JSON_MODEL") or "").strip()
        if not wanted or wanted == model_name_of(self.client):
            return self.client
        cached = self._json_cache
        if cached is not None and model_name_of(cached) == wanted:
            return cached
        from sahs.util.gateway import GatewayError
        try:
            self._json_cache = self.client.for_model(wanted)
        except (GatewayError, AttributeError) as e:
            # a model the plane does not serve: the JSON calls stay on
            # this agent's model, and the log says why, once
            note = getattr(self.client, "_note", None)
            if callable(note):
                note(f"GATEWAY_JSON_MODEL={wanted} not used: {e}")
            self._json_cache = self.client
        return self._json_cache


# ── the planes as the composer lists them ─────────────────────
PLANE_IDS = ("vertex", "gateway")
# a model choice as the composer sends it: a plane alone (its default
# model) or plane:model, e.g. gateway:gemini-3.5-flash
CHOICE_SEP = ":"


def split_choice(choice: str) -> tuple[str, str]:
    """"gateway:gemini-3.5-flash" → ("gateway", "gemini-3.5-flash");
    "gateway" → ("gateway", ""); "" → ("", "")."""
    text = (choice or "").strip().lower()
    plane, _, model = text.partition(CHOICE_SEP)
    return plane.strip(), model.strip()


def join_choice(plane: str, model: str = "") -> str:
    return f"{plane}{CHOICE_SEP}{model}" if model else plane


def pretty_model(raw: str) -> str:
    """gemini-3.7-flash → Gemini 3.7 Flash; gemini-3.1-pro-preview →
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
    from sahs.util.gateway import Config, gateway_configured, model_plane
    default = model_plane()
    key = resolve_vertex_key_path()
    vertex_ok = bool(resolve_vertex_project() and key and key.exists())
    vertex_model = resolve_vertex_model()
    gateway_model = Config.from_env().model
    return [
        {"id": "vertex",
         "label": pretty_model(vertex_model),
         "plane_name": "Vertex",
         "model": vertex_model,
         "available": vertex_ok,
         "reason": "" if vertex_ok else
         "needs the Vertex contract in the silo .env: SYNAPSE_VERTEX_SA_KEY "
         "(a key file that exists) and VERTEX_PROJECT_ID",
         "means": "Google Cloud's Vertex AI with the service-account "
                  "key. Thinking streams as it happens and the answer "
                  "arrives word by word.",
         "feel": "streams",
         "default": default == "vertex"},
        {"id": "gateway",
         "label": pretty_model(gateway_model),
         "plane_name": "Gateway",
         "model": gateway_model,
         "available": gateway_configured(),
         "reason": "" if gateway_configured() else
         "needs APP_ID and APP_SECRET (or GEMINI_BEARER_TOKEN) in the "
         "silo .env; python scripts/gateway_check.py proves the path",
         "means": "The enterprise AI gateway with an identity-service token "
                  "that renews itself every ten minutes. Each call "
                  "lands whole: a thinking pause, then the text at "
                  "once. No streaming.",
         "feel": "whole calls",
         "default": default == "gateway"},
    ]


def model_catalog() -> list[dict[str, Any]]:
    """Every model the composer can pick, one row per plane × model:
    the choice id (plane, or plane:model), the plane, the model, the
    label, availability with the reason, what choosing it means, its
    engine map (how it takes its depth — level | budget | none — the
    levels it accepts, ``fit``: one plain sentence on when to pick it,
    ``facts``: the engineer's line for the hover), and which one a new
    chat starts on. Vertex serves its one model; the gateway serves GATEWAY_MODELS,
    the default first."""
    from sahs.util.gateway import gateway_models
    rows: list[dict[str, Any]] = []
    planes = {p["id"]: p for p in plane_catalog()}
    vertex = planes["vertex"]
    engine = profile_for(vertex["model"])
    rows.append({"id": "vertex", "plane": "vertex", "plane_name": "Vertex",
                 "model": vertex["model"], "label": vertex["label"],
                 "available": vertex["available"], "reason": vertex["reason"],
                 "means": vertex["means"], "feel": vertex["feel"],
                 "thinking": engine.thinking, "levels": list(engine.accepts),
                 "family": engine.family, "fit": engine.fit,
                 "facts": engine.facts,
                 "default": vertex["default"]})
    gateway = planes["gateway"]
    for index, model in enumerate(gateway_models()):
        engine = profile_for(model)
        rows.append({"id": "gateway" if index == 0 else join_choice("gateway", model),
                     "plane": "gateway", "plane_name": "Gateway", "model": model,
                     "label": pretty_model(model),
                     "available": gateway["available"], "reason": gateway["reason"],
                     "means": gateway["means"], "feel": gateway["feel"],
                     "thinking": engine.thinking, "levels": list(engine.accepts),
                     "family": engine.family, "fit": engine.fit,
                     "facts": engine.facts,
                     "default": gateway["default"] and index == 0})
    return rows


def agent_for(plane: str = "", budget: Budget | None = None,
              log: Any = None, model: str = "") -> VertexAgent:
    """The chat's model on a named plane — ``vertex`` or ``gateway`` — or
    on the environment's default when the name is empty; on the gateway,
    ``model`` picks one of GATEWAY_MODELS (empty: the default). An
    unknown name is a typed error, not a silent fallback: the composer
    only ever sends catalog ids."""
    from sahs.util.gateway import model_plane
    plane = (plane or "").strip().lower() or model_plane()
    if plane == "gateway":
        return GatewayAgent.from_env(budget, log, model)
    if plane == "vertex":
        return VertexAgent.from_env(budget, log)
    raise ModelUnavailable(f"no model plane called {plane!r}: the "
                           "planes are vertex and gateway")


def agent_from_env(budget: Budget | None = None,
                   log: Any = None) -> VertexAgent:
    """The chat's model, on whichever plane the environment names:
    SAHS_MODEL_PLANE=vertex|gateway, or auto — the gateway when its credentials
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
                 max_output_tokens: int = 8192,
                 should_stop: Callable[[], bool] | None = None
                 ) -> Iterator[dict[str, Any]]:
        self.calls.append({"contents": contents, "system": system,
                           "tools": [t["name"] for t in (tools or [])],
                           "thinking_level": thinking_level})
        step = (self.steps.pop(0) if self.steps
                and ROUTING_KEY in system else [])
        if callable(step):
            step = step()
        parts: list[dict[str, Any]] = []
        for item in step or []:
            if should_stop is not None and should_stop():
                # the stop button mid-stream, as the real client does
                # it: the parts so far, done says STOPPED, nothing more
                yield {"kind": "done", "parts": parts, "finish": "STOPPED",
                       "usage": {"prompt_tokens": 100, "output_tokens": 20,
                                 "thought_tokens": 5, "cached_tokens": 0}}
                return
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


__all__ = ["ROUTING_KEY", "VertexAgent", "GatewayAgent", "agent_from_env",
           "agent_for", "plane_catalog", "pretty_model", "PLANE_IDS",
           "ScriptedAgent", "declarations", "json"]
