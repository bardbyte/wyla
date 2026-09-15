"""The tool round: the batch of parallel searches goes back in ONE user
turn, entries are merged with witness counts, and nothing in a tool can
kill the run."""

from __future__ import annotations

import pytest

from kcx.agent import DiscoveryAgent, load_instruction
from kcx.catalog import CatalogClient, CatalogConnection
from kcx.tools import TOOL_NAME, make_kit
from kcx.transport import TransportError
from tests.doubles import FakeCatalog, ScriptedModel, entry, google_error

A, B, C = entry("a"), entry("b"), entry("c")
NAME = "projects/demo/locations/us/entryGroups/@bigquery/entries/{}"


def _call(query: str) -> dict:
    return {"call": {"name": TOOL_NAME, "args": {"query": query}}}


def _kit(fake: FakeCatalog):
    conn = CatalogConnection(project="demo", key_path=None)
    return make_kit(CatalogClient(conn, http=fake.http, token=fake.token))


def _agent(model, fake, **kw) -> DiscoveryAgent:
    kw.setdefault("instruction", "You search a catalog.")
    return DiscoveryAgent(model, _kit(fake), **kw)


def test_three_parallel_searches_go_back_in_one_user_turn_and_witnesses_count():
    fake = FakeCatalog({"q1": [A, B], "q2": [B, C], "q3": [B]})
    model = ScriptedModel([
        [{"thought": "decompose"}, _call("q1"), _call("q2"), _call("q3")],
        [{"text": "Found: b, a, c."}]])
    result = _agent(model, fake).run("which tables?")

    assert result.status == "answered" and result.answer == "Found: b, a, c."
    assert result.model_calls == 2 and result.stop_reason == ""
    assert [(e["entry_name"].rsplit("/", 1)[-1], e["witnesses"])
            for e in result.entries] == [("b", 3), ("a", 1), ("c", 1)]
    assert result.entries[0]["queries"] == ["q1", "q2", "q3"]
    assert [(s["call"], s["query"], s["count"]) for s in result.searches] == \
        [(1, "q1", 2), (1, "q2", 2), (1, "q3", 1)]
    assert result.usage == {"prompt_tokens": 200, "output_tokens": 40,
                            "thought_tokens": 10}
    assert result.trace[0] == {"kind": "thought", "call": 1, "text": "decompose"}

    second = model.calls[1]["contents"]
    assert second[0] == {"role": "user", "parts": [{"text": "which tables?"}]}
    assert second[1]["role"] == "model"
    assert second[1]["parts"][0] == {"thought": True, "text": "decompose"}
    calls = second[1]["parts"][1:]
    assert [p["functionCall"]["args"]["query"] for p in calls] == ["q1", "q2", "q3"]
    assert calls[0]["thoughtSignature"] == "scripted-signature"
    assert "thoughtSignature" not in calls[1]
    answers = second[2]
    assert answers["role"] == "user" and len(answers["parts"]) == 3
    for part, call_id, count in zip(answers["parts"], ["call_1", "call_2",
                                                        "call_3"], [2, 2, 1]):
        response = part["functionResponse"]
        assert response["name"] == TOOL_NAME and response["id"] == call_id
        assert len(response["response"]["results"]) == count
    assert fake.requests[0]["body"]["query"] == "q1"
    assert len(fake.requests) == 3


def test_ids_are_echoed_only_when_the_model_sent_them():
    fake = FakeCatalog({"q": [A]})
    model = ScriptedModel([[_call("q")], [{"text": "a."}]], ids=False,
                          signed=False)
    _agent(model, fake).run("?")
    response = model.calls[1]["contents"][2]["parts"][0]["functionResponse"]
    assert "id" not in response and response["name"] == TOOL_NAME
    assert "thoughtSignature" not in model.calls[1]["contents"][1]["parts"][0]


def test_a_second_round_dedupes_and_a_repeated_query_counts_once():
    fake = FakeCatalog({"q1": [A, B], "q2": [B]})
    model = ScriptedModel([[_call("q1")], [_call("q2"), _call("q1")],
                           [{"text": "done"}]])
    result = _agent(model, fake).run("?")
    assert result.model_calls == 3
    assert [(e["entry_name"], e["witnesses"]) for e in result.entries] == \
        [(NAME.format("b"), 2), (NAME.format("a"), 1)]
    assert result.entries[0]["queries"] == ["q1", "q2"]
    assert [s["call"] for s in result.searches] == [1, 2, 2]
    assert len(model.calls[2]["contents"]) == 5


def test_tool_errors_go_back_as_data_and_the_run_continues():
    fake = FakeCatalog({"q2": [A]})
    fake.fail_next = [(403, google_error(403, "IAM_PERMISSION_DENIED",
                                         "no search for you"))]
    model = ScriptedModel([[_call("q1"), _call("q2")], [{"text": "a."}]])
    result = _agent(model, fake).run("?")
    assert result.status == "answered"
    assert result.searches[0]["error"].startswith("Permission denied: HTTP 403")
    assert result.searches[0]["count"] == 0 and result.searches[1]["count"] == 1
    sent = model.calls[1]["contents"][2]["parts"][0]["functionResponse"]
    assert sent["response"]["reason"] == "IAM_PERMISSION_DENIED"
    assert [e["entry_name"] for e in result.entries] == [NAME.format("a")]


def test_unknown_tools_and_bad_arguments_are_answered_not_raised():
    fake = FakeCatalog()
    model = ScriptedModel([
        [{"call": {"name": "frobnicate", "args": {}}},
         {"call": {"name": TOOL_NAME, "args": {"q": "x"}}}],
        [{"text": "sorry"}]])
    result = _agent(model, fake).run("?")
    responses = [p["functionResponse"]["response"]
                 for p in model.calls[1]["contents"][2]["parts"]]
    assert responses[0]["error"] == "unknown tool 'frobnicate'"
    assert TOOL_NAME in responses[0]["hint"]
    assert responses[1]["error"].startswith("the arguments did not match")
    assert result.status == "answered" and fake.requests == []

    def explode(query=""):
        raise RuntimeError("kaboom")
    model = ScriptedModel([[_call("q")], [{"text": "ok"}]])
    agent = DiscoveryAgent(model, {TOOL_NAME: explode}, instruction="i")
    agent.run("?")
    sent = model.calls[1]["contents"][2]["parts"][0]["functionResponse"]
    assert sent["response"]["error"] == "RuntimeError: kaboom"


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_the_call_ceiling_and_the_wall_clock_end_the_run_in_plain_language():
    fake = FakeCatalog({"q": [A]})
    always = ScriptedModel([[_call("q")] for _ in range(10)])
    result = _agent(always, fake, max_calls=2).run("?")
    assert result.status == "partial" and result.model_calls == 2
    assert "ceiling of 2 model calls" in result.stop_reason
    assert "KC_MAX_MODEL_CALLS" in result.stop_reason
    assert [e["entry_name"] for e in result.entries] == [NAME.format("a")]
    assert result.answer == ""

    clock = _Clock()

    def slow(contents):
        clock.now += 1000
        return [_call("q")]
    model = ScriptedModel([slow, [{"text": "never reached"}]])
    result = _agent(model, fake, wall_seconds=300, clock=clock).run("?")
    assert result.status == "partial" and result.model_calls == 1
    assert "longer than the 300 s" in result.stop_reason
    assert result.elapsed_ms == 1000000.0


def test_an_empty_answer_reports_the_finish_reason():
    model = ScriptedModel([[{"finish": "MAX_TOKENS"}]])
    result = _agent(model, FakeCatalog()).run("?")
    assert result.status == "empty" and result.answer == ""
    assert "nothing usable (it finished with MAX_TOKENS)" in result.stop_reason
    result = _agent(ScriptedModel([[]]), FakeCatalog()).run("?")
    assert result.status == "empty" and "finished with" not in result.stop_reason


def test_text_streams_in_order_and_thoughts_are_traced():
    heard: list[str] = []
    thoughts: list[str] = []
    called: list = []
    fake = FakeCatalog({"q": [A]})
    model = ScriptedModel([[{"thought": "plan"}, {"text": "Looking… "}, _call("q")],
                           [{"text": "Here "}, {"text": "it is."}]])
    result = _agent(model, fake).run(
        "?", on_text=heard.append, on_thought=thoughts.append,
        on_call=lambda name, args: called.append((name, args)))
    assert heard == ["Looking… ", "Here ", "it is."]
    assert thoughts == ["plan"] and called == [(TOOL_NAME, {"query": "q"})]
    assert result.answer == "Looking… \n\nHere it is."


def test_a_transport_failure_before_any_event_is_retried_once_then_surfaced():
    model = ScriptedModel([[{"text": "hi"}]], fail_first=1)
    result = _agent(model, FakeCatalog()).run("?")
    assert result.status == "answered" and len(model.calls) == 2
    with pytest.raises(TransportError, match="scripted transport failure"):
        _agent(ScriptedModel([[{"text": "hi"}]], fail_first=2),
               FakeCatalog()).run("?")


def test_the_system_prompt_is_the_skill_file_and_the_tools_are_declared(
        monkeypatch):
    model = ScriptedModel([[{"text": "hi"}]])
    DiscoveryAgent(model, _kit(FakeCatalog())).run("?")
    call = model.calls[0]
    assert call["system"] == load_instruction()
    assert "Knowledge Catalog Search" in call["system"]
    assert call["tools"] == [TOOL_NAME] and call["thinking_level"] == "low"
    monkeypatch.setenv("KC_THINKING_LEVEL", "high")
    monkeypatch.setenv("KC_MAX_MODEL_CALLS", "9")
    monkeypatch.setenv("KC_WALL_SECONDS", "12.5")
    agent = DiscoveryAgent(ScriptedModel(), {}, instruction="i")
    assert agent.thinking_level == "high" and agent.max_calls == 9
    assert agent.wall_seconds == 12.5
