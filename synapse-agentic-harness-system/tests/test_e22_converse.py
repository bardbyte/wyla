"""E22: the conversational layer.

The pins that matter are negative ones: a chat turn must not plan,
must not resolve, must not emit a number, and (for the common ones)
must not cost a model call. And self-description must be grounded:
the system may never claim a capability it does not have.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("e22")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "e22"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


class Counting:
    """Records every model call so 'costs nothing' is measured."""

    def __init__(self, classify=None, chat=None):
        self.calls: list[str] = []
        self._classify = classify
        self._chat = chat

    def json(self, prompt, *, system="", temperature=0.0, max_tokens=1024):
        if "conversational voice" in system:
            self.calls.append("chat")
            return self._chat
        if "You classify ONE turn" in system:
            self.calls.append("classify")
            return self._classify
        if "You compose ONE BigQuery SELECT" in system:
            self.calls.append("sql")
            return {"sql": GOOD_SQL, "why": "certified"}
        if "skeptical reviewer" in system:
            self.calls.append("judge")
            return {"grounded": True, "why": "traces"}
        self.calls.append("other")
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        self.calls.append("stream")
        yield "prose"


def _run(compiled, text, model, turns=()):
    from sahs.ask import AskRuntime
    build, _ = compiled
    tmp = Path(tempfile.mkdtemp())
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp / "graph",
                         store_path=tmp / "s.sqlite3",
                         model_factory=lambda budget: model)
    session = runtime.create_session("analyst")
    for one in (text, *turns):
        runtime.start_turn(session["id"], one)
        assert runtime.wait(session["id"], 30)
    rt = runtime.runtime(session["id"])
    return rt.bus.since(0), runtime, session["id"]


def _prose(events):
    return "".join(e.get("delta", "") for e in events
                   if e["ev"] == "generate_token")


# ── the free path ────────────────────────────────────────────
@pytest.mark.parametrize("text,kind", [
    ("hi", "chat"), ("Hello!", "chat"), ("hey", "chat"),
    ("good morning", "chat"), ("how's it going?", "chat"),
    ("thanks", "chat"), ("thank you so much", "chat"),
    ("cheers", "chat"), ("perfect", "chat"),
    ("bye", "chat"), ("see you", "chat"),
    ("what can you do", "meta"), ("who are you", "meta"),
    ("how do you work", "meta"), ("can I trust this", "meta"),
    ("which build is this", "meta"),
    ("that's wrong", "feedback"), ("not what I meant", "feedback"),
])
def test_conversation_costs_nothing_and_touches_nothing(
        compiled, text, kind):
    model = Counting()
    events, runtime, session_id = _run(compiled, text, model)
    kinds = [e["ev"] for e in events]
    classified = next(e for e in events if e["ev"] == "classify_result")

    assert classified["kind"] == kind, text
    assert model.calls == [], f"{text!r} cost {model.calls}"
    assert runtime.store.plan_versions(session_id) == []
    assert "resolve_started" not in kinds
    assert "contract_ready" not in kinds
    assert "answer_payload" not in kinds
    assert _prose(events).strip(), "a conversation must say something"


def test_a_data_question_is_never_mistaken_for_small_talk(compiled):
    """The matchers are tight on purpose: answering a question with a
    greeting is far worse than paying for one classify call."""
    for text in ("hi spend by day", "thanks, now show me spend",
                 "what can you do with acquirer net spend",
                 "hello, acquirer net spend by day"):
        model = Counting(classify={"kind": "new_question",
                                   "question": text, "edits": [],
                                   "why": "a question"})
        events, _rt, _s = _run(compiled, text, model)
        classified = next(e for e in events
                          if e["ev"] == "classify_result")
        assert classified["kind"] != "chat", f"{text!r} swallowed"


# ── grounding ────────────────────────────────────────────────
def test_self_description_is_read_from_the_build(compiled):
    from sahs.ask.converse import world
    build, _ = compiled
    facts = world(build)
    assert facts["build_id"] == build.version
    assert facts["tables"] == len(build.schema)
    assert facts["metrics"] == len(build.metrics)
    assert facts["certified"] == sum(
        1 for m in build.metrics
        if (m.get("status_served") or m.get("status")) == "certified")

    model = Counting()
    events, _rt, _s = _run(compiled, "what can you do", model)
    said = _prose(events)
    assert build.version in said
    assert str(len(build.metrics)) in said


def test_it_never_claims_a_capability_it_does_not_have(compiled):
    """The bridge test, in BOTH directions: a tier that ships without
    being claimed, or a claim with no tier behind it, fails here
    rather than reaching a user as a lie."""
    from sahs.ask.converse import GATED, SERVES
    from sahs.evals.capability import TIERS

    claimed = {tier for tier, _what in SERVES}
    built = {tier for tier, meta in TIERS.items()
             if meta["built"] and tier != "T11"}   # T11 is the chat
    assert claimed == built, (                     # layer itself
        f"claimed but not built: {claimed - built}; "
        f"built but not claimed: {built - claimed}")

    model = Counting()
    events, _rt, _s = _run(compiled, "what can you do", model)
    said = _prose(events).lower()
    # every unbuilt capability appears ONLY under the honest heading
    assert "not yet" in said
    for gated in GATED:
        assert gated.lower() in said, f"{gated!r} not disclosed"
    head, _, tail = said.partition("not yet")
    for gated in GATED:
        assert gated.lower() not in head, (
            f"{gated!r} offered as available")


# ── mixed, feedback, guards ──────────────────────────────────
def test_a_mixed_opener_splits_in_code_and_answers_both(compiled):
    """E22's canonical example, "hi! can you show me spend", IS an
    opener, and the first-turn no-model-call pin survives it: the
    split happens in code, hello first, then the full pipeline."""
    model = Counting()
    events, runtime, session_id = _run(
        compiled, "morning! acquirer net spend by day", model)
    kinds = [e["ev"] for e in events]
    said = _prose(events)

    assert model.calls == [], "a mixed opener must cost nothing"
    first_word = said.strip().split()[0].rstrip(".!")
    assert first_word in {"Hi", "Hello", "Morning"}   # chat half first
    assert "resolve_started" in kinds                 # then the data half
    assert runtime.store.plan_versions(session_id), "no plan was built"
    assert "clarify_request" in kinds                 # asked, on merit
    classified = [e for e in events if e["ev"] == "classify_result"]
    assert len(classified) == 1 and classified[0]["kind"] == "mixed"


def test_a_mid_session_mixed_still_splits_via_the_model(compiled):
    """Past the first turn the model classifier is available; a subtle
    mixed phrasing the code splitter misses still lands as both."""
    model = Counting(classify={
        "kind": "mixed", "chat": "Sure.",
        "question": "acquirer net spend by day", "edits": [],
        "why": "both"})
    events, runtime, session_id = _run(
        compiled, "acquirer net spend by day",
        model, turns=("appreciate it, could I see acquirer net spend "
                      "by day again",))
    said = _prose(events)
    assert "Sure." in said
    assert "classify" in model.calls


def test_negative_feedback_is_recorded_not_just_acknowledged(compiled):
    model = Counting()
    events, runtime, session_id = _run(compiled, "that's wrong", model)
    recorded = runtime.store.feedback(session_id)
    assert len(recorded) == 1
    assert recorded[0]["vote"] == "down"
    assert "that's wrong" in recorded[0]["note"]
    assert "noted" in _prose(events).lower()


def test_a_chat_turn_can_never_emit_data(compiled):
    """The hard guard: even if the model tries to answer with a
    number, a chat kind has no path to an answer payload."""
    model = Counting(
        classify={"kind": "off_topic", "question": "", "edits": [],
                  "why": "outside"},
        chat={"reply": "Spend was 4.2 billion dollars last quarter."})
    # a data turn first, so a plan exists and the model classifier is
    # reachable; then the off-topic turn under test
    events, runtime, session_id = _run(
        compiled, "acquirer net spend by day", model,
        turns=("tell me a number",))
    kinds = [e["ev"] for e in events]
    assert "answer_payload" not in kinds
    assert "contract_ready" not in kinds
    assert len(runtime.store.plan_versions(session_id)) == 1, (
        "the off-topic turn added a plan version")
    # the reply is prose only: no card, no meridian line, nothing the
    # UI would render as a governed number
    messages = runtime.store.messages(session_id)
    payloads = [m["payload"] for m in messages if m["payload"]]
    assert all(p.get("schema") != "a2ui.answer/1" for p in payloads)


def test_the_voice_stays_brief_and_unmystical(compiled):
    from sahs.ask.converse import world
    build, _ = compiled
    facts = world(build)
    from sahs.ask.converse import pre_classify
    banned = ("as an ai", "i'm just", "i am just", "delve", "unleash",
              "i feel", "excited to", "happy to help you on your")
    for text in ("hi", "thanks", "bye", "how do you work"):
        spoken = pre_classify(text, facts, first_turn=True)
        assert spoken is not None
        low = spoken.text.lower()
        for phrase in banned:
            assert phrase not in low, f"{text!r} said {phrase!r}"
    greeting = pre_classify("hi", facts, first_turn=True)
    assert len(greeting.text) < 220, "a greeting should be brief"


def test_variation_is_deterministic_per_turn(compiled):
    """Light variation, but the same turn always reads the same way:
    a transcript has to be reproducible."""
    from sahs.ask.converse import pre_classify, world
    build, _ = compiled
    facts = world(build)
    first = pre_classify("hi", facts, first_turn=True).text
    again = pre_classify("hi", facts, first_turn=True).text
    other = pre_classify("hello", facts, first_turn=True).text
    assert first == again
    assert isinstance(other, str) and other
