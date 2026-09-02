"""v3 Stage 1 — the native-tool transport, pinned without a network.

converse() streams Gemini's parts as events and hands back the model
content VERBATIM (signatures included) so the loop can echo it; the
agent seam charges the budget from real usage and wraps transport
failures as ModelUnavailable; the scripted double speaks the same
events from scripted parts."""

from __future__ import annotations

import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

CHUNKS = [
    {"candidates": [{"content": {"parts": [
        {"thought": True, "text": "The user wants the spend metric."}]}}]},
    {"candidates": [{"content": {"parts": [{"text": "Let me look"}]}}]},
    {"candidates": [{"content": {"parts": [{"text": " that up."}]}}]},
    {"candidates": [{"content": {"parts": [
        {"functionCall": {"name": "search", "args": {"query": "spend"},
                          "id": "call_9"},
         "thoughtSignature": "sig=="}]},
        "finishReason": "STOP"}],
     "usageMetadata": {"promptTokenCount": 1200,
                       "candidatesTokenCount": 30,
                       "thoughtsTokenCount": 80,
                       "cachedContentTokenCount": 900}},
]


def _client(chunks=CHUNKS, bodies=None):
    from sahs.enrich.client import VertexClient
    from sahs.util.auth import VertexConnection
    conn = VertexConnection(project="p", location="global",
                            model="gemini-3.1-pro-preview",
                            endpoint="https://x", key_path=None)

    def transport(body):
        if bodies is not None:
            bodies.append(body)
        yield from chunks
    return VertexClient(conn, token_provider=lambda: "t",
                        stream_transport=transport)


def test_converse_streams_events_and_returns_parts_verbatim():
    bodies = []
    client = _client(bodies=bodies)
    events = list(client.converse(
        [{"role": "user", "parts": [{"text": "spend?"}]}],
        system="You are Synapse", tools=[{"name": "search",
                                          "description": "d",
                                          "parameters": {}}],
        thinking_level="medium"))
    kinds = [e["kind"] for e in events]
    assert kinds == ["thought", "text", "text", "call", "done"]
    assert events[3]["name"] == "search" and events[3]["id"] == "call_9"
    done = events[-1]
    # text runs merged, the signed call kept whole, thought kept
    assert done["parts"] == [
        {"thought": True, "text": "The user wants the spend metric."},
        {"text": "Let me look that up."},
        {"functionCall": {"name": "search", "args": {"query": "spend"},
                          "id": "call_9"}, "thoughtSignature": "sig=="}]
    assert done["usage"] == {"prompt_tokens": 1200, "output_tokens": 30,
                             "thought_tokens": 80, "cached_tokens": 900}
    assert done["finish"] == "STOP"
    # the request: system instruction, declarations, thinking level,
    # NO temperature, NO json mode
    body = bodies[0]
    assert body["systemInstruction"] == {"parts": [{"text": "You are Synapse"}]}
    assert body["tools"][0]["functionDeclarations"][0]["name"] == "search"
    assert body["generationConfig"]["thinkingConfig"] == {
        "thinkingLevel": "medium", "includeThoughts": True}
    assert "temperature" not in body["generationConfig"]
    assert "responseMimeType" not in body["generationConfig"]
    assert client.usage["thought_tokens"] == 80


def test_vertex_agent_charges_the_budget_and_wraps_failures():
    from sahs.ask.budget import Budget
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant.agent import VertexAgent
    from sahs.enrich.client import EnrichTransportError
    budget = Budget()
    agent = VertexAgent(_client(), budget)
    list(agent.converse([{"role": "user", "parts": [{"text": "x"}]}],
                        system="s"))
    assert budget.tokens_in == 1200 and budget.tokens_out == 110
    assert budget.calls == 1

    def dead(body):
        raise EnrichTransportError("vertex unreachable: proxy")
        yield  # pragma: no cover
    agent.client.stream_transport = dead
    try:
        list(agent.converse([{"role": "user", "parts": [{"text": "x"}]}]))
    except ModelUnavailable as e:
        assert "unreachable" in str(e)
    else:  # pragma: no cover
        raise AssertionError("transport failure must surface as "
                             "ModelUnavailable")


def test_scripted_agent_speaks_the_same_events():
    from sahs.assistant.agent import ROUTING_KEY, ScriptedAgent
    agent = ScriptedAgent([
        [{"text": "Looking."}, {"call": {"name": "search",
                                          "args": {"query": "spend"}}}],
        lambda: [{"text": "Done."}],
    ])
    first = list(agent.converse([], system=ROUTING_KEY + " over X",
                                tools=[{"name": "search"}]))
    assert [e["kind"] for e in first] == ["text", "call", "done"]
    assert first[1]["id"] == "call_1"
    assert first[-1]["parts"][1]["thoughtSignature"] == "scripted"
    second = list(agent.converse([], system=ROUTING_KEY))
    assert second[0]["delta"] == "Done."
    # off the key: silence, and json answers {}
    assert [e["kind"] for e in agent.converse([], system="other")] \
        == ["done"]
    assert agent.json("q") == {}
    assert agent.calls[0]["tools"] == ["search"]


def test_declarations_only_for_schema_bearing_tools():
    from sahs.assistant.agent import declarations
    from sahs.loop.tools import ToolSpec
    kit = {"a": ToolSpec(name="a", signature="a()", maps_to="", description="A",
                         fn=lambda: {}, schema={"type": "OBJECT",
                                               "properties": {}}),
           "b": ToolSpec(name="b", signature="b()", maps_to="",
                         description="B", fn=lambda: {})}
    got = declarations(kit)
    assert [d["name"] for d in got] == ["a"]
    assert got[0]["parameters"]["type"] == "OBJECT"


def test_a_silent_or_cut_stream_is_a_transport_failure_with_a_reason():
    from sahs.enrich.client import EnrichTransportError
    import pytest

    def silent(body):
        yield CHUNKS[0]
        raise TimeoutError("The read operation timed out")
    client = _client()
    client.stream_transport = silent
    with pytest.raises(EnrichTransportError, match="went silent"):
        list(client.converse([{"role": "user", "parts": [{"text": "x"}]}]))

    def cut(body):
        yield CHUNKS[0]
        raise ConnectionResetError(104, "Connection reset by peer")
    client.stream_transport = cut
    with pytest.raises(EnrichTransportError, match="cut off"):
        list(client.converse([{"role": "user", "parts": [{"text": "x"}]}]))


def test_the_agent_retries_once_only_before_anything_reached_the_user():
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant.agent import VertexAgent
    from sahs.enrich.client import EnrichTransportError
    import pytest

    attempts = {"n": 0}

    def flaky(body):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise EnrichTransportError("vertex unreachable: proxy reset")
        yield from CHUNKS
    agent = VertexAgent(_client())
    agent.client.stream_transport = flaky
    events = list(agent.converse([{"role": "user", "parts": [{"text": "x"}]}]))
    assert [e["kind"] for e in events] == ["thought", "text", "text",
                                           "call", "done"]
    assert attempts["n"] == 2                 # one retry, then success

    # once text has streamed, a retry would duplicate it: surface instead
    attempts["n"] = 0

    def cut_midway(body):
        attempts["n"] += 1
        yield CHUNKS[1]
        raise EnrichTransportError("the model stream was cut off")
    agent.client.stream_transport = cut_midway
    seen = []
    with pytest.raises(ModelUnavailable, match="cut off"):
        for event in agent.converse([{"role": "user",
                                      "parts": [{"text": "x"}]}]):
            seen.append(event)
    assert attempts["n"] == 1 and seen[0]["delta"] == "Let me look"

    # a second failure is the verdict
    def dead(body):
        attempts["n"] += 1
        raise EnrichTransportError("vertex unreachable")
        yield  # pragma: no cover
    attempts["n"] = 0
    agent.client.stream_transport = dead
    with pytest.raises(ModelUnavailable):
        list(agent.converse([{"role": "user", "parts": [{"text": "x"}]}]))
    assert attempts["n"] == 2
