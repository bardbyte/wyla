"""The Langfuse backend of the tracer (Python SDK v4, OpenTelemetry
based; verified against 4.15). Imported only when the switch is on,
so the package costs nothing when it is off.

Trace ids are the tracer's own 32-hex ids, passed as ``trace_context``
so a backfill lands on the same trace a live turn would. Observation
handles are kept per ``(trace_id, key)`` and ended when the tracer
says so; anything still open when the trace closes is ended with it.
"""

from __future__ import annotations

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


class LangfuseEmitter:
    def __init__(self, client: Any = None) -> None:
        if client is None:
            from langfuse import Langfuse      # lazy: the switch decides
            client = Langfuse()
        self.client = client
        self._roots: dict[str, Any] = {}
        self._children: dict[tuple[str, str], Any] = {}

    def trace_open(self, trace_id: str, *, name: str, session_id: str,
                   user_id: str, input: Any, metadata: dict[str, Any],
                   tags: list[str]) -> None:
        root = self.client.start_observation(
            name=name, as_type="span", input=input, metadata=metadata,
            trace_context={"trace_id": trace_id})
        set_trace_attributes(root, name=name, session_id=session_id,
                             user_id=user_id, tags=tags)
        self._roots[trace_id] = root

    def generation_open(self, trace_id: str, key: str, *, name: str,
                        model: str, input: Any, metadata: dict[str, Any],
                        prompt: tuple[str, int] | None = None) -> None:
        root = self._roots.get(trace_id)
        if root is None:
            return
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
                  metadata: dict[str, Any]) -> None:
        root = self._roots.get(trace_id)
        if root is None:
            return
        self._children[(trace_id, key)] = root.start_observation(
            name=name, as_type="tool", input=input, metadata=metadata)

    def span_close(self, trace_id: str, key: str, *, output: Any,
                   metadata: dict[str, Any], level: str) -> None:
        span = self._children.pop((trace_id, key), None)
        if span is None:
            return
        span.update(output=output, metadata=metadata, level=level).end()

    def event(self, trace_id: str, *, name: str,
              metadata: dict[str, Any]) -> None:
        root = self._roots.get(trace_id)
        if root is not None:
            root.create_event(name=name, metadata=metadata)

    def score(self, trace_id: str, *, name: str, value: Any,
              comment: str, data_type: str) -> None:
        self.client.create_score(trace_id=trace_id, name=name, value=value,
                                 comment=comment or None,
                                 data_type=data_type)

    def trace_close(self, trace_id: str, *, output: Any,
                    metadata: dict[str, Any], level: str,
                    status_message: str) -> None:
        root = self._roots.pop(trace_id, None)
        if root is None:
            return
        for key in [k for k in self._children if k[0] == trace_id]:
            self._children.pop(key).end()
        root.update(output=output, metadata=metadata, level=level,
                    status_message=status_message or None).end()

    def flush(self) -> None:
        self.client.flush()


__all__ = ["LangfuseEmitter", "set_trace_attributes"]
