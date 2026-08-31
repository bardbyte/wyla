"""The model seam for Ask (E18).

One wrapper over the proven VertexClient so the loop never learns the
transport: JSON calls for the deterministic-adjacent steps (classify,
judge) at temperature 0, and a streamed call for composition. Every
call charges the session budget from the client's own usage counters,
so the meter reports what actually happened rather than an estimate.

ALL model calls are server-side. No key, no token, no endpoint ever
reaches the browser: the frontend consumes events, nothing else.

When Vertex is not configured this raises ModelUnavailable carrying
the env contract. It does NOT fall back to a canned answer: an
unconfigured machine gets an honest error card, never a pretend one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from sahs.enrich.client import (EnrichTransportError, VertexClient,
                                parse_json_answer)
from sahs.util.auth import AuthError, VertexConnection

from .budget import Budget


class ModelUnavailable(RuntimeError):
    """Vertex is not configured/reachable on this machine."""


class Model(Protocol):
    """What the loop needs. Tests supply a scripted implementation;
    the product supplies Vertex."""

    def json(self, prompt: str, *, system: str = ...,
             temperature: float = ..., max_tokens: int = ...) -> dict | None:
        ...

    def stream(self, prompt: str, *, system: str = ...,
               temperature: float = ..., max_tokens: int = ...
               ) -> Iterator[str]:
        ...


@dataclass
class VertexModel:
    client: VertexClient
    budget: Budget | None = None

    @staticmethod
    def from_env(budget: Budget | None = None,
                 log: Any = None) -> "VertexModel":
        try:
            connection = VertexConnection.from_env()
        except AuthError as e:
            raise ModelUnavailable(
                f"{e} — Ask needs the Vertex contract in the silo .env: "
                "LUMI_VERTEX_SA_KEY, VERTEX_PROJECT_ID, VERTEX_LOCATION "
                "(default global), VERTEX_MODEL") from e
        return VertexModel(client=VertexClient(connection=connection,
                                               log=log), budget=budget)

    # ── accounting ───────────────────────────────────────────
    def _charge(self, before: dict[str, int]) -> None:
        if self.budget is None:
            return
        usage = self.client.usage
        self.budget.charge(
            tokens_in=usage.get("prompt_tokens", 0)
            - before.get("prompt_tokens", 0),
            tokens_out=usage.get("output_tokens", 0)
            - before.get("output_tokens", 0),
            calls=max(1, usage.get("calls", 0) - before.get("calls", 0)))

    # ── calls ────────────────────────────────────────────────
    def json(self, prompt: str, *, system: str = "",
             temperature: float = 0.0,
             max_tokens: int = 1024) -> dict | None:
        before = dict(self.client.usage)
        try:
            text = self.client.generate(
                prompt if not system else f"{system}\n\n{prompt}",
                temperature=temperature, max_output_tokens=max_tokens)
        except EnrichTransportError as e:
            raise ModelUnavailable(str(e)) from e
        finally:
            self._charge(before)
        return parse_json_answer(text)

    def stream(self, prompt: str, *, system: str = "",
               temperature: float = 0.3,
               max_tokens: int = 1500) -> Iterator[str]:
        before = dict(self.client.usage)
        try:
            for chunk in self.client.generate_stream(
                    prompt, system=system, temperature=temperature,
                    max_output_tokens=max_tokens):
                yield chunk
        except EnrichTransportError as e:
            raise ModelUnavailable(str(e)) from e
        finally:
            self._charge(before)
