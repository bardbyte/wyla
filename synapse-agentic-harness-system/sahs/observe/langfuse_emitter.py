"""The Langfuse backend of the tracer (Python SDK v4, OpenTelemetry
based; verified against 4.15). Imported only when the switch is on,
so the package costs nothing when it is off.

Trace ids are the tracer's own 32-hex ids, passed as ``trace_context``
so a backfill lands on the same trace a live turn would. Observation
ids are pinned the same way — a hash of the trace id and the tracer's
key, handed to the SDK through an OpenTelemetry id generator
(``PinnedIds``) — and score ids likewise, so a second backfill of the
same turn updates every object in place and creates nothing new.
Observation handles are kept per ``(trace_id, key)`` and ended when
the tracer says so; anything still open when the trace closes is
ended with it.
"""

from __future__ import annotations

import hashlib
import threading
from typing import Any

# the trace-level attributes, as the SDK's own propagate_attributes()
# writes them on a span (langfuse.LangfuseOtelSpanAttributes); set on
# the root directly because the tracer is call-driven, not a context
# manager around the turn
_SESSION = "session.id"
_USER = "user.id"
_TAGS = "langfuse.trace.tags"
_TRACE_NAME = "langfuse.trace.name"
_TRACE_META = "langfuse.trace.metadata"
_PROMPT_NAME = "langfuse.observation.prompt.name"
_PROMPT_VERSION = "langfuse.observation.prompt.version"


def set_trace_attributes(root: Any, *, name: str = "", session_id: str = "",
                         user_id: str = "", tags: list[str] | None = None,
                         metadata: dict[str, Any] | None = None) -> None:
    """Trace-level fields on a root observation, the way the SDK's
    own propagate_attributes() writes them. Metadata values become
    strings: that is what the trace-level keys accept."""
    otel = getattr(root, "_otel_span", None)
    if otel is None or not otel.is_recording():
        return
    if name:
        otel.set_attribute(_TRACE_NAME, name)
    if session_id:
        otel.set_attribute(_SESSION, session_id)
    if user_id:
        otel.set_attribute(_USER, user_id)
    if tags:
        otel.set_attribute(_TAGS, list(tags))
    for key, value in (metadata or {}).items():
        if value is not None:
            otel.set_attribute(f"{_TRACE_META}.{key}", str(value))


def observation_id_for(trace_id: str, key: str) -> str:
    """16 hex chars (an OpenTelemetry span id): the same trace and
    tracer key always name the same observation."""
    return hashlib.sha256(f"{trace_id}/{key}".encode()).hexdigest()[:16]


def score_id_for(trace_id: str, name: str) -> str:
    return hashlib.sha256(f"{trace_id}/score/{name}".encode()).hexdigest()[:32]


class PinnedIds:
    """An OpenTelemetry id generator the emitter primes: the next span
    id on this thread is the pinned one, otherwise random. Built lazily
    (the SDK is imported only when the switch is on)."""

    def __init__(self) -> None:
        self._local = threading.local()
        self._random: Any = None

    def pin(self, span_id: str) -> None:
        self._local.next = int(span_id, 16)

    def _fallback(self) -> Any:
        if self._random is None:
            from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
            self._random = RandomIdGenerator()
        return self._random

    def generate_span_id(self) -> int:
        pinned = getattr(self._local, "next", None)
        if pinned is not None:
            self._local.next = None
            return pinned
        return self._fallback().generate_span_id()

    def generate_trace_id(self) -> int:
        return self._fallback().generate_trace_id()


# the one generator a process pins through: setup.langfuse_client()
# hands it to the SDK, the emitter primes it before every observation
PINNED = PinnedIds()


class LangfuseEmitter:
    def __init__(self, client: Any = None, ids: PinnedIds | None = None) -> None:
        if client is None:
            from langfuse import Langfuse      # lazy: the switch decides
            client = Langfuse(id_generator=PINNED)
        self.client = client
        self.ids = ids if ids is not None else PINNED
        self._roots: dict[str, Any] = {}
        self._children: dict[tuple[str, str], Any] = {}
        self._events: dict[str, int] = {}

    def _pin(self, trace_id: str, key: str) -> None:
        self.ids.pin(observation_id_for(trace_id, key))

    def trace_open(self, trace_id: str, *, name: str, session_id: str,
                   user_id: str, input: Any, metadata: dict[str, Any],
                   tags: list[str]) -> None:
        self._pin(trace_id, "root")
        root = self.client.start_observation(
            name=name, as_type="span", input=input, metadata=metadata,
            trace_context={"trace_id": trace_id})
        set_trace_attributes(root, name=name, session_id=session_id,
                             user_id=user_id, tags=tags)
        self._roots[trace_id] = root

    def _holder(self, trace_id: str, parent: str) -> Any:
        """The observation a child opens under: the task span named
        by ``parent`` when it is open, else the trace's root."""
        root = self._roots.get(trace_id)
        if root is None:
            return None
        if parent:
            return self._children.get((trace_id, parent), root)
        return root

    def generation_open(self, trace_id: str, key: str, *, name: str,
                        model: str, input: Any, metadata: dict[str, Any],
                        prompt: tuple[str, int] | None = None,
                        parent: str = "") -> None:
        root = self._holder(trace_id, parent)
        if root is None:
            return
        self._pin(trace_id, key)
        gen = root.start_observation(
            name=name, as_type="generation", model=model or None,
            input=input, metadata=metadata)
        otel = getattr(gen, "_otel_span", None)
        if prompt and otel is not None and otel.is_recording():
            # the registered prompt this generation ran under
            # (sahs.observe.prompts): name + Langfuse's numeric version
            otel.set_attribute(_PROMPT_NAME, prompt[0])
            otel.set_attribute(_PROMPT_VERSION, int(prompt[1]))
        self._children[(trace_id, key)] = gen

    def generation_close(self, trace_id: str, key: str, *, output: Any,
                         usage: dict[str, int] | None,
                         metadata: dict[str, Any]) -> None:
        gen = self._children.pop((trace_id, key), None)
        if gen is None:
            return
        fields: dict[str, Any] = {"output": output, "metadata": metadata}
        if usage:
            fields["usage_details"] = usage
        gen.update(**fields).end()

    def span_open(self, trace_id: str, key: str, *, name: str, input: Any,
                  metadata: dict[str, Any], parent: str = "") -> None:
        root = self._holder(trace_id, parent)
        if root is None:
            return
        self._pin(trace_id, key)
        self._children[(trace_id, key)] = root.start_observation(
            name=name, as_type="span" if key.startswith("task:") else "tool",
            input=input, metadata=metadata)

    def span_close(self, trace_id: str, key: str, *, output: Any,
                   metadata: dict[str, Any], level: str) -> None:
        span = self._children.pop((trace_id, key), None)
        if span is None:
            return
        span.update(output=output, metadata=metadata, level=level).end()

    def event(self, trace_id: str, *, name: str,
              metadata: dict[str, Any], parent: str = "") -> None:
        root = self._holder(trace_id, parent)
        if root is not None:
            n = self._events[trace_id] = self._events.get(trace_id, 0) + 1
            self._pin(trace_id, f"event{n}:{name}")
            root.create_event(name=name, metadata=metadata)

    def score(self, trace_id: str, *, name: str, value: Any,
              comment: str, data_type: str) -> None:
        self.client.create_score(trace_id=trace_id, name=name, value=value,
                                 comment=comment or None,
                                 data_type=data_type,
                                 score_id=score_id_for(trace_id, name))

    def trace_close(self, trace_id: str, *, output: Any,
                    metadata: dict[str, Any], level: str,
                    status_message: str) -> None:
        root = self._roots.pop(trace_id, None)
        self._events.pop(trace_id, None)
        if root is None:
            return
        for key in [k for k in self._children if k[0] == trace_id]:
            self._children.pop(key).end()
        root.update(output=output, metadata=metadata, level=level,
                    status_message=status_message or None).end()

    def flush(self) -> None:
        self.client.flush()


__all__ = ["LangfuseEmitter", "PINNED", "PinnedIds", "observation_id_for",
           "score_id_for", "set_trace_attributes"]
