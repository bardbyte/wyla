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
        try:
            yield from self.client.converse(
                contents, system=system, tools=tools,
                thinking_level=thinking_level,
                max_output_tokens=max_output_tokens)
        except EnrichTransportError as e:
            raise ModelUnavailable(str(e)) from e
        finally:
            self._charge(before)

    def json(self, prompt: str, *, system: str = "",
             temperature: float = 0.0,
             max_tokens: int = 1024) -> dict | None:
        return VertexModel(self.client, self.budget).json(
            prompt, system=system, temperature=temperature,
            max_tokens=max_tokens)


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


__all__ = ["ROUTING_KEY", "VertexAgent", "ScriptedAgent",
           "declarations", "json"]
