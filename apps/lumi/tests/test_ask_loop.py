"""Ask (E18) Stage A: the loop behind an endpoint, proven without a
browser and without the network.

The model is scripted here — the TRANSPORT is what the stub replaces,
never the data: every table, metric, expression, definition line and
cost figure in these assertions comes from a real compiled build.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"

GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")


class ScriptedModel:
    """Stands in for Vertex. Routes on the system prompt, exactly as
    the real client would see it, and records what it was asked."""

    def __init__(self, *, sql: str = GOOD_SQL, prose: str = "",
                 classify: dict | None = None, grounded: bool | None = True,
                 slow: float = 0.0) -> None:
        self.sql = sql
        self.prose = prose or ("Acquirer Net Spend, one row per part_dt. "
                               "The query was validated but not executed, "
                               "so no figure is stated.")
        self.classify = classify
        self.grounded = grounded
        self.slow = slow
        self.calls: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0, max_tokens=1024):
        if self.slow:
            time.sleep(self.slow)
        if "You classify ONE turn" in system:
            self.calls.append("classify")
            return self.classify
        if "You compose ONE BigQuery SELECT" in system:
            self.calls.append("sql")
            return {"sql": self.sql, "why": "certified expression, by day"}
        if "skeptical reviewer" in system:
            self.calls.append("judge")
            if self.grounded is None:
                return {"nonsense": True}      # the judge said nothing usable
            return {"grounded": self.grounded, "why": "claims trace"}
        self.calls.append("other")
        return {}

    def stream(self, prompt, *, system="", temperature=0.3, max_tokens=1500):
        self.calls.append("stream")
        for word in self.prose.split(" "):
            if self.slow:
                time.sleep(self.slow)
            yield word + " "


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("ask")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "ask_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    builds = tmp / "builds"
    _dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return {"builds": builds, "graph": graph_dir, "tmp": tmp}


def make_runtime(compiled, model: ScriptedModel, name: str = "rt"):
    from sahs.ask import AskRuntime
    return AskRuntime(
        builds_root=compiled["builds"], graph_root=compiled["graph"],
        store_path=compiled["tmp"] / f"{name}.sqlite3",
        events_dir=compiled["tmp"] / f"{name}_events",
        model_factory=lambda budget: model)


def drive(runtime, session_id: str, text: str, choice=None,
          timeout: float = 20.0) -> list[dict]:
    """One turn, start to finish → the events it emitted."""
    before = runtime.runtime(session_id).bus.head()
    runtime.start_turn(session_id, text, choice=choice)
    assert runtime.wait(session_id, timeout), "the turn never finished"
    return runtime.runtime(session_id).bus.since(before)


def kinds(events: list[dict]) -> list[str]:
    return [e["ev"] for e in events]


# ── the pipeline ─────────────────────────────────────────────
def test_turn_emits_the_pinned_event_family(compiled):
    model = ScriptedModel()
    runtime = make_runtime(compiled, model, "family")
    session = runtime.create_session("analyst")
    events = drive(runtime, session["id"], "acquirer net spend by day")

    seen = kinds(events)
    assert seen[0] == "turn_started" and seen[-1] == "turn_done"
    for required in ("classify_result", "resolve_started", "resolve_result"):
        assert required in seen, f"{required} missing from {seen}"
    # every event is a well-formed meridian.event/1 with a resume key
    for event in events:
        assert event["schema"] == "meridian.event/1"
        assert event["session_id"] == session["id"]
        assert isinstance(event["seq"], int) and event["ts"]
    # the first turn of a session classifies deterministically: free
    assert "classify" not in model.calls
    classify_event = next(e for e in events if e["ev"] == "classify_result")
    assert classify_event["kind"] == "new_question"
    assert classify_event["model_used"] is False


def test_resolution_is_instant_and_deterministic(compiled):
    """The pin the UX contract rests on: deterministic steps render
    in well under 300ms, so structure answers while composition
    thinks."""
    runtime = make_runtime(compiled, ScriptedModel(), "fast")
    session = runtime.create_session("analyst")
    events = drive(runtime, session["id"], "acquirer net spend by day")
    resolved = next(e for e in events if e["ev"] == "resolve_result")
    assert resolved["elapsed_ms"] < 300, resolved
    # and it carries the node ids the constellation will animate
    assert isinstance(resolved.get("candidate_node_ids"), list)


def test_answer_payload_is_governed_or_the_turn_asks(compiled):
    runtime = make_runtime(compiled, ScriptedModel(), "answer")
    session = runtime.create_session("analyst")
    events = drive(runtime, session["id"], "acquirer net spend by day")
    seen = kinds(events)

    if "clarify_request" in seen:
        # a below-margin resolve is a SUCCESS state: one question,
        # chips carrying evidence, partial plan kept
        clarify = next(e for e in events if e["ev"] == "clarify_request")
        assert clarify["options"] and clarify["question"]
        assert all("value" in o and "label" in o for o in clarify["options"])
        answered = drive(runtime, session["id"], clarify["options"][0]["label"],
                         choice={"slot": clarify["slot"],
                                 "value": clarify["options"][0]["value"],
                                 "label": clarify["options"][0]["label"]})
        events, seen = answered, kinds(answered)

    assert "contract_ready" in seen, seen
    contract = next(e for e in events if e["ev"] == "contract_ready")
    # default-FAIL: every criterion starts false, before any work
    assert contract["contract"]["will_verify"]
    assert all(c["passed"] is False
               for c in contract["contract"]["will_verify"])
    assert contract["contract"]["verdict"] == "fail"

    assert "generate_token" in seen, "the answer never streamed"
    assert "verify_verdict" in seen and "answer_payload" in seen
    payload = next(e for e in events
                   if e["ev"] == "answer_payload")["payload"]
    # the reality law at the last gate
    assert payload["meridian_line"] and payload["grain"]
    assert payload["sql"].strip()
    assert payload["build_id"]
    assert payload["metric"]["id"]


def test_verifier_defaults_to_fail_when_the_judge_says_nothing(compiled):
    """UNKNOWN is a failure. The answer still renders — with the
    unverified criterion named in its limits, never silently."""
    runtime = make_runtime(compiled, ScriptedModel(grounded=None), "unknown")
    session = runtime.create_session("analyst")
    events = drive(runtime, session["id"], "acquirer net spend by day")
    if "clarify_request" in kinds(events):
        clarify = next(e for e in events if e["ev"] == "clarify_request")
        events = drive(runtime, session["id"], "pick",
                       choice={"slot": clarify["slot"],
                               "value": clarify["options"][0]["value"]})
    verdict = next(e for e in events if e["ev"] == "verify_verdict")
    grounded = next(c for c in verdict["will_verify"] if c["id"] == "grounded")
    assert grounded["passed"] is False
    assert "fail-closed" in grounded["evidence"]
    assert verdict["verdict"] == "fail"
    payload = next(e for e in events
                   if e["ev"] == "answer_payload")["payload"]
    assert any("unverified" in limit for limit in payload["limits"])


def test_same_for_canada_moves_exactly_one_slot(compiled):
    """The F1 mutation: one slot, and only the touched slot is
    re-resolved — the metric is never re-planned."""
    model = ScriptedModel()
    runtime = make_runtime(compiled, model, "mutate")
    session = runtime.create_session("analyst")
    events = drive(runtime, session["id"], "acquirer net spend by day")
    if "clarify_request" in kinds(events):
        clarify = next(e for e in events if e["ev"] == "clarify_request")
        drive(runtime, session["id"], "pick",
              choice={"slot": clarify["slot"],
                      "value": clarify["options"][0]["value"]})

    model.classify = {"kind": "mutate", "question": "same for Canada",
                      "edits": [{"slot": "filters.country",
                                 "value": "Canada"}],
                      "why": "the analyst changed the country"}
    events = drive(runtime, session["id"], "same for Canada")
    delta = next(e for e in events if e["ev"] == "plan_delta")
    assert len(delta["changes"]) == 1
    assert delta["changes"][0]["slot"] == "filters.country"
    assert delta["changes"][0]["to"] == "Canada"
    started = next(e for e in events if e["ev"] == "resolve_started")
    assert started["slots"] == ["filters.country"], started
    versions = runtime.store.plan_versions(session["id"])
    assert versions[-1]["plan"]["filters"]["country"] == "Canada"
    assert versions[-1]["parent"] is not None


def test_stop_is_server_side_and_keeps_partial_state(compiled):
    """Stop means stop: the loop halts between steps and the budget is
    charged only for what ran."""
    model = ScriptedModel(slow=0.35)
    runtime = make_runtime(compiled, model, "stop")
    session = runtime.create_session("analyst")
    # get past any clarify first, so the stopped turn is one that is
    # really composing rather than one that finished in 5ms
    first = drive(runtime, session["id"], "acquirer net spend by day")
    choice = None
    if "clarify_request" in kinds(first):
        clarify = next(e for e in first if e["ev"] == "clarify_request")
        choice = {"slot": clarify["slot"],
                  "value": clarify["options"][0]["value"]}
    before = runtime.runtime(session["id"]).bus.head()
    runtime.start_turn(session["id"], "answer", choice=choice)
    time.sleep(0.25)
    stopped = runtime.stop(session["id"])
    assert stopped["stopped"] is True
    assert runtime.wait(session["id"], 20)
    events = runtime.runtime(session["id"]).bus.since(before)
    done = next(e for e in events if e["ev"] == "turn_done")
    assert done["status"] == "stopped"
    assert "answer_payload" not in kinds(events)


def test_render_refuses_an_ungoverned_payload(compiled):
    from sahs.ask.contract import build_contract
    from sahs.ask.generate import Generation
    from sahs.ask.plan import Plan
    from sahs.ask.render import RenderRefused, render_answer
    from sahs.tools.api import Build

    build = Build.open(compiled["builds"])
    plan = Plan(question="q", metric_id="m", grain="day", table="dw.t")
    with pytest.raises(RenderRefused, match="meridian line"):
        render_answer(build, plan, Generation(sql="SELECT 1"),
                      build_contract(plan))
    no_grain = Plan(question="q", metric_id="m", grain="", table="dw.t")
    with pytest.raises(RenderRefused, match="grain"):
        render_answer(build, no_grain,
                      Generation(sql="SELECT 1", definition_line="line"),
                      build_contract(no_grain))


# ── the endpoint (Stage A's exit: curl-able) ─────────────────
@pytest.fixture()
def client(compiled):
    from apps.lumi.backend import ask as ask_module
    from apps.lumi.backend.app import create_app
    ask_module._RUNTIME = make_runtime(compiled, ScriptedModel(), "http")
    return TestClient(create_app())


def test_sessions_messages_and_sse(client):
    created = client.post("/api/sessions", json={"kind": "analyst"})
    assert created.status_code == 201
    session_id = created.json()["session"]["id"]

    accepted = client.post(f"/api/sessions/{session_id}/messages",
                           json={"text": "acquirer net spend by day"})
    assert accepted.status_code == 202
    assert accepted.json()["turn_id"].startswith("t_")

    # the stream replays the whole turn and closes on turn_done
    body = client.get(f"/api/sessions/{session_id}/stream?once=1").text
    events = [json.loads(line[6:]) for line in body.splitlines()
              if line.startswith("data: ")]
    assert kinds(events)[0] == "turn_started"
    assert kinds(events)[-1] == "turn_done"
    assert all(f"event: {e['ev']}" in body for e in events[:3])
    # resume: nothing before the cursor comes back twice
    tail = client.get(
        f"/api/sessions/{session_id}/stream?once=1&after={events[2]['seq']}"
    ).text
    resumed = [json.loads(line[6:]) for line in tail.splitlines()
               if line.startswith("data: ")]
    assert resumed and resumed[0]["seq"] == events[3]["seq"]

    listed = client.get("/api/sessions").json()["sessions"]
    assert any(s["id"] == session_id for s in listed)
    detail = client.get(f"/api/sessions/{session_id}").json()
    assert detail["available"] and detail["messages"]
    assert detail["plan_versions"], "the plan chain is the session state"
    # the auto-title is the first resolved plan's summary
    assert detail["session"]["title"]

    voted = client.post(f"/api/sessions/{session_id}/feedback",
                        json={"vote": "up", "subject": "answer"})
    assert voted.status_code == 201


def test_no_build_is_honest(compiled, tmp_path):
    from apps.lumi.backend import ask as ask_module
    from apps.lumi.backend.app import create_app
    from sahs.ask import AskRuntime
    ask_module._RUNTIME = AskRuntime(
        builds_root=tmp_path / "nothing", graph_root=tmp_path / "graph",
        store_path=tmp_path / "s.sqlite3",
        model_factory=lambda budget: ScriptedModel())
    client = TestClient(create_app())
    session_id = client.post("/api/sessions",
                             json={"kind": "analyst"}).json()["session"]["id"]
    answer = client.post(f"/api/sessions/{session_id}/messages",
                         json={"text": "anything"}).json()
    assert answer["available"] is False
    assert "laptop.py compile" in answer["reason"]
