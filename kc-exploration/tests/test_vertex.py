"""The Vertex plane: resolution order, the request body, the event stream
with the parts kept verbatim, and the two ways a stream dies."""

from __future__ import annotations

import pytest

from kcx.env import ConfigError
from kcx.transport import TransportError
from kcx.vertex import DEFAULT_MODEL, VertexConnection, VertexModel
from tests.doubles import sse

CHUNKS = [
    {"candidates": [{"content": {"parts": [
        {"thought": True, "text": "The user wants the spend metric."}]}}]},
    {"candidates": [{"content": {"parts": [{"text": "Let me look"}]}}]},
    {"candidates": [{"content": {"parts": [{"text": " that up."}]}}]},
    {"candidates": [{"content": {"parts": [
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "spend"}, "id": "call_9"},
         "thoughtSignature": "sig=="}]},
        "finishReason": "STOP"}],
     "usageMetadata": {"promptTokenCount": 1200, "candidatesTokenCount": 30,
                       "thoughtsTokenCount": 80,
                       "cachedContentTokenCount": 900}},
]
PARALLEL = [
    {"candidates": [{"content": {"parts": [
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "one"}, "id": "c1"},
         "thoughtSignature": "sig-1"}]}}]},
    {"candidates": [{"content": {"parts": [
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "two"}, "id": "c2"}}]},
        "finishReason": "STOP"}],
     "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5}},
]


def _model(chunks):
    conn = VertexConnection(project="p", location="global",
                            model=DEFAULT_MODEL, endpoint="https://x",
                            key_path=None)
    return VertexModel(conn, token=lambda: "t", stream=sse(chunks))


def test_from_env_resolution_order_and_aliases(monkeypatch, sa_key):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "from-google")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_key))
    c = VertexConnection.from_env()
    assert c.project == "from-google" and c.key_path == sa_key
    assert c.location == "global" and c.model == DEFAULT_MODEL
    assert c.endpoint == "https://aiplatform.googleapis.com"
    assert c.url() == ("https://aiplatform.googleapis.com/v1/projects/"
                       "from-google/locations/global/publishers/google/"
                       f"models/{DEFAULT_MODEL}:generateContent")
    monkeypatch.setenv("VERTEX_PROJECT_ID", "from-vertex")
    monkeypatch.setenv("SYNAPSE_VERTEX_SA_KEY", str(sa_key.with_name("alias.json")))
    sa_key.with_name("alias.json").write_text("{}")
    monkeypatch.setenv("VERTEX_LOCATION", "us-central1")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:8080")
    c = VertexConnection.from_env()
    assert c.project == "from-vertex"
    assert c.key_path.name == "alias.json"
    assert c.endpoint == "https://us-central1-aiplatform.googleapis.com"
    assert c.model == "gemini-2.5-pro"
    assert c.proxies == {"https": "http://proxy:8080"}
    monkeypatch.setenv("VERTEX_SA_KEY", str(sa_key))
    monkeypatch.setenv("VERTEX_MODEL", "gemini-3.1-pro-preview")
    monkeypatch.setenv("VERTEX_API_BASE_URL", "https://vertex.example.com/")
    monkeypatch.setenv("VERTEX_DISABLE_PROXY", "1")
    c = VertexConnection.from_env()
    assert c.key_path == sa_key and c.model == "gemini-3.1-pro-preview"
    assert c.endpoint == "https://vertex.example.com" and c.proxies == {}
    assert c.describe()["key_exists"] is True


def test_from_env_refusals_name_the_variable(monkeypatch, sa_key):
    with pytest.raises(ConfigError, match="VERTEX_PROJECT_ID"):
        VertexConnection.from_env()
    monkeypatch.setenv("VERTEX_PROJECT_ID", "p")
    with pytest.raises(ConfigError, match="VERTEX_SA_KEY"):
        VertexConnection.from_env()
    monkeypatch.setenv("VERTEX_SA_KEY", str(sa_key.with_name("gone.json")))
    with pytest.raises(ConfigError, match="not found on disk"):
        VertexConnection.from_env()


def test_converse_streams_events_and_returns_the_parts_verbatim():
    model = _model(CHUNKS)
    events = list(model.converse(
        [{"role": "user", "parts": [{"text": "spend?"}]}],
        system="You search a catalog.",
        tools=[{"name": "knowledge_catalog_search", "description": "d",
                "parameters": {"type": "OBJECT"}}],
        thinking_level="low"))
    assert [e["kind"] for e in events] == ["thought", "text", "text", "call",
                                           "done"]
    assert events[3]["name"] == "knowledge_catalog_search"
    assert events[3]["args"] == {"query": "spend"} and events[3]["id"] == "call_9"
    done = events[-1]
    assert done["parts"] == [
        {"thought": True, "text": "The user wants the spend metric."},
        {"text": "Let me look that up."},
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "spend"}, "id": "call_9"},
         "thoughtSignature": "sig=="}]
    assert done["finish"] == "STOP"
    assert done["usage"] == {"prompt_tokens": 1200, "output_tokens": 30,
                             "thought_tokens": 80, "cached_tokens": 900}
    assert model.usage == {"calls": 1, "prompt_tokens": 1200,
                           "output_tokens": 30, "thought_tokens": 80}
    body = model.stream.bodies[0]
    assert body["systemInstruction"] == {"parts": [{"text": "You search a catalog."}]}
    assert body["tools"][0]["functionDeclarations"][0]["name"] == \
        "knowledge_catalog_search"
    assert body["generationConfig"] == {
        "maxOutputTokens": 8192,
        "thinkingConfig": {"thinkingLevel": "low", "includeThoughts": True}}
    assert "temperature" not in body["generationConfig"]
    assert "responseMimeType" not in body["generationConfig"]


def test_two_function_calls_across_chunks_are_two_events_in_order():
    events = list(_model(PARALLEL).converse(
        [{"role": "user", "parts": [{"text": "q"}]}]))
    calls = [e for e in events if e["kind"] == "call"]
    assert [c["args"]["query"] for c in calls] == ["one", "two"]
    assert [c["id"] for c in calls] == ["c1", "c2"]
    parts = events[-1]["parts"]
    assert len(parts) == 2
    assert parts[0]["thoughtSignature"] == "sig-1"
    assert "thoughtSignature" not in parts[1]
    body = _model(PARALLEL).stream.bodies
    assert body == []                              # a fresh double, unused


def test_a_silent_or_cut_stream_is_a_typed_error():
    with pytest.raises(TransportError, match="went silent for 120s"):
        list(_model([CHUNKS[0], TimeoutError("read timed out")]).converse(
            [{"role": "user", "parts": [{"text": "q"}]}]))
    with pytest.raises(TransportError, match="cut off: ConnectionResetError"):
        list(_model([CHUNKS[0], ConnectionResetError("peer")]).converse(
            [{"role": "user", "parts": [{"text": "q"}]}]))
    with pytest.raises(TransportError, match=r"refused the stream \(HTTP 403\)"):
        list(_model([TransportError("Vertex refused the stream (HTTP 403): "
                                    "no")]).converse(
            [{"role": "user", "parts": [{"text": "q"}]}]))
