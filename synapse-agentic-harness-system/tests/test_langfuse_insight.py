"""The Langfuse mirror rebuilt from the record (docs/runbooks/
langfuse-insight.md): the prompt fingerprint, nested task spans,
pinned ids, the backfill from the chat tables, the three dataset
builders, the experiment run on their items, and the coverage table —
every one of them on the fake SDK database and the in-memory exporter,
never on a real Spanner, Langfuse or model host.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from fake_spanner import FakeSpannerDatabase                        # noqa: E402
from sahs.assistant.events import ASSISTANT_EVENTS, EventBus       # noqa: E402
from sahs.assistant.prompt_version import prompt_fingerprint       # noqa: E402
from sahs.assistant.spanner_store import SpannerAssistantStore     # noqa: E402
from sahs.identity.database import SpannerDatabase                 # noqa: E402
from sahs.observe import datasets as D                             # noqa: E402
from sahs.observe.coverage import (coverage, format_coverage,      # noqa: E402
                                   format_coverage_markdown)
from sahs.observe.record import RecordReader, backfill, turns_of   # noqa: E402
from sahs.observe.tracer import Recorder, TurnTracer, trace_id_for  # noqa: E402
from test_observe import (_emit_turn, _FakeClient, _PromptClient,  # noqa: E402
                          sdk_client)                              # noqa: F401
from test_v3_loop import compiled                                  # noqa: E402,F401

PRECEDENTS = SILO / "tests" / "fixtures" / "precedents" / "precedents.jsonl"
GOLD_SQL = ("SELECT part_dt, COUNT(1) AS n FROM wwcas_authorization "
            "WHERE approval_cd = 'D' GROUP BY part_dt")


# ── the prompt fingerprint ───────────────────────────────────
def _prompt(mode="chat", memory="nothing yet", skills="pack A"):
    parts = [f"<identity>\nRadix\n</identity>", "<chain>\nrules\n</chain>",
             f"<mode>\n{mode}\n</mode>", "<graph>\ndigest\n</graph>",
             f"<skills>\n{skills}\n</skills>", f"<memory>\n{memory}\n</memory>",
             "<session>\nToday is Monday\n</session>"]
    return "\n\n".join(parts)


def test_fingerprint_is_stable_and_moves_with_the_part_that_changed():
    a = prompt_fingerprint(_prompt(), "assistant/3")
    b = prompt_fingerprint(_prompt(), "assistant/3")
    assert a == b
    assert a["prompt_version"] == f"assistant/3+{a['prompt_prefix']}"
    assert set(a["prompt_parts"]) == {"identity", "chain", "mode", "graph",
                                      "skills", "memory", "session"}
    # a memory added: the prefix (before <skills>) is unchanged, the
    # memory part moved, so the version string stays comparable
    c = prompt_fingerprint(_prompt(memory="prefers EMEA"), "assistant/3")
    assert c["prompt_version"] == a["prompt_version"]
    assert c["prompt_parts"]["memory"] != a["prompt_parts"]["memory"]
    assert c["prompt_parts"]["identity"] == a["prompt_parts"]["identity"]
    # the mode changed: that is a different prompt version
    d = prompt_fingerprint(_prompt(mode="autopilot"), "assistant/3")
    assert d["prompt_version"] != a["prompt_version"]
    assert d["prompt_parts"]["mode"] != a["prompt_parts"]["mode"]


def test_two_identical_live_turns_carry_the_same_fingerprint(compiled):
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    import tempfile
    build, _ = compiled
    tmp = Path(tempfile.mkdtemp())
    model = ScriptedAgent([[{"text": "Approvals are counted per day."}],
                           [{"text": "Approvals are counted per day."}],
                           [{"text": "Approvals are counted per day."}]])
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=lambda budget: model)
    session = runtime.create_session()
    versions = []
    for _ in range(2):
        runtime.start_turn(session["id"], "what is an approval?")
        assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    systems = [e for e in events if e["ev"] == "model_prompt"
               and e.get("kind") == "system"]
    assert len(systems) == 2
    versions = [e["prompt_version"] for e in systems]
    assert versions[0] == versions[1]
    assert versions[0].startswith("assistant/3+")
    assert systems[0]["prompt_parts"] == systems[1]["prompt_parts"]
    # a memory settles: the memory part moves, the prefix does not
    runtime.store.add_memory("prefers the EMEA region")
    runtime.start_turn(session["id"], "what is an approval?")
    assert runtime.wait(session["id"], 60)
    third = [e for e in runtime.runtime(session["id"]).bus.since(0)
             if e["ev"] == "model_prompt" and e.get("kind") == "system"][-1]
    assert third["prompt_version"] == versions[0]
    assert third["prompt_parts"]["memory"] != systems[0]["prompt_parts"]["memory"]
    assert third["prompt_parts"]["identity"] == systems[0]["prompt_parts"]["identity"]


# ── a compound turn: task spans under one trace ──────────────
def _emit_compound(bus: EventBus, turn_id: str = "t9") -> None:
    e = bus.emit
    e("turn_started", turn_id=turn_id, text="declines per day, and a chart",
      build_id="b_1", version="assistant/3", skills=[], mode="chat",
      plane="vertex", planning=True)
    e("budget_tick", turn_id=turn_id, tokens_in=40, tokens_out=10,
      tokens=50, calls=1, turn_tokens=50)                # the planner's call
    e("plan_made", turn_id=turn_id,
      tasks=[{"id": "a", "goal": "declines per day", "kind": "query",
              "depends_on": []},
             {"id": "b", "goal": "a chart of it", "kind": "artifact",
              "depends_on": ["a"]}],
      synthesis="lead with the chart", repairs=[], pool=2,
      waves=[["a"], ["b"]])
    e("task_started", turn_id=turn_id, task="a", goal="declines per day",
      kind="query", depends_on=[], sub_turn=f"{turn_id}.a", n=1, of=2,
      max_calls=10)
    sub = f"{turn_id}.a"
    e("turn_started", turn_id=sub, task="a", text="declines per day",
      build_id="b_1", version="assistant/3", plane="vertex", mode="chat")
    e("model_prompt", turn_id=sub, task="a", n=0, kind="system",
      content="SYS", prompt_version="assistant/3+abcd1234",
      prompt_parts={"identity": "1111"})
    e("model_prompt", turn_id=sub, task="a", n=1, kind="call", content="[u]")
    e("tool_call", turn_id=sub, task="a", n=1, tool="run_sql",
      args={"sql": "SELECT 1"}, input="run")
    e("tool_step", turn_id=sub, task="a", n=1, tool="run_sql",
      args={}, input="run", summary="ERROR: over the ceiling", ref="a1",
      elapsed_ms=3.0)
    e("tool_result", turn_id=sub, task="a", ref="a1", tool="run_sql",
      content="{}")
    e("say_token", turn_id=sub, task="a", delta="rows per day")
    e("budget_tick", turn_id=sub, task="a", tokens_in=140, tokens_out=30,
      tokens=170, calls=2, turn_tokens=170)
    e("turn_done", turn_id=sub, task="a", status="answered", elapsed_ms=5.0)
    e("task_done", turn_id=turn_id, task="a", status="done", reason="",
      sub_turn=sub, cost={"model_calls": 1}, artifacts=[], saved=["q1"],
      checked=[], refused=["run_sql: over the ceiling"], say="rows per day")
    e("task_started", turn_id=turn_id, task="b", goal="a chart", kind="artifact",
      depends_on=["a"], sub_turn=f"{turn_id}.b", n=2, of=2, max_calls=10)
    e("task_done", turn_id=turn_id, task="b", status="failed",
      reason="its inputs never finished", sub_turn=f"{turn_id}.b",
      cost={}, artifacts=[], saved=[], checked=[], refused=[], say="")
    # the synthesis runs on the parent's own turn id
    e("model_prompt", turn_id=turn_id, n=2, kind="call", content="[findings]")
    e("say_token", turn_id=turn_id, delta="Here is the chart.")
    e("budget_tick", turn_id=turn_id, tokens_in=200, tokens_out=50,
      tokens=250, calls=3, turn_tokens=250)
    e("turn_done", turn_id=turn_id, status="answered", elapsed_ms=90.0,
      tasks=2)


def test_a_compound_turn_is_one_trace_with_task_spans():
    rec = Recorder()
    tracer = TurnTracer(rec, user_id="sam")
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    _emit_compound(bus)
    tid = trace_id_for("s1", "t9")
    assert trace_id_for("s1", "t9.a") == tid
    (opened,) = rec.of("trace_open")
    assert opened["trace_id"] == tid and "compound" in opened["tags"]
    spans = {s["key"]: s for s in rec.of("span_open")}
    assert spans["task:a"]["parent"] == "" and spans["task:a"]["name"] == "task a"
    assert spans["task:a"]["metadata"]["kind"] == "query"
    assert spans["a/tool1"]["parent"] == "task:a"     # the sub-turn's tool
    gens = {g["key"]: g for g in rec.of("generation_open")}
    assert gens["gen-tick1"]["name"] == "planner"      # the one-shot's usage
    assert gens["a/gen1"]["parent"] == "task:a"
    assert gens["a/gen1"]["metadata"]["prompt_version"] == "assistant/3+abcd1234"
    assert gens["gen2"]["parent"] == ""                # the synthesis
    closes = {c["key"]: c for c in rec.of("generation_close")}
    assert closes["gen-tick1"]["usage"]["total"] == 50
    assert closes["a/gen1"]["usage"]["total"] == 120
    assert closes["gen2"]["usage"]["total"] == 80
    task_closes = {c["key"]: c for c in rec.of("span_close")
                   if c["key"].startswith("task:")}
    assert task_closes["task:a"]["output"]["text"] == "rows per day"
    assert task_closes["task:a"]["metadata"]["status"] == "done"
    assert task_closes["task:a"]["level"] == "DEFAULT"
    assert task_closes["task:b"]["level"] == "WARNING"
    (done,) = rec.of("trace_close")
    assert done["metadata"]["tasks"] == {"a": "done", "b": "failed"}
    assert done["metadata"]["refused"] == ["run_sql: over the ceiling"]
    scores = {s["name"]: s["value"] for s in rec.of("score")}
    assert scores == {"turn_status": "answered", "refused": 1.0,
                      "tasks_failed": 1.0}
    events = [e["name"] for e in rec.of("event")]
    assert "plan" in events
    assert tracer._turns == {}


def test_emitter_nests_task_spans_and_pins_ids_across_replays(sdk_client):
    from sahs.observe.langfuse_emitter import LangfuseEmitter
    client, exporter, posted = sdk_client
    tracer = TurnTracer(LangfuseEmitter(client), user_id="sam")
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    _emit_compound(bus)
    tracer.flush()
    first = {s.name: s for s in exporter.get_finished_spans()}
    assert {"assistant.turn", "task a", "task b", "run_sql", "model call 1",
            "planner", "model call 2"} <= set(first)
    root = first["assistant.turn"]
    assert format(root.context.trace_id, "032x") == trace_id_for("s1", "t9")
    assert first["task a"].parent.span_id == root.context.span_id
    assert first["run_sql"].parent.span_id == first["task a"].context.span_id
    assert first["model call 1"].parent.span_id == first["task a"].context.span_id
    assert first["model call 2"].parent.span_id == root.context.span_id
    exporter.clear()
    # the same records again: the same observation ids, so Langfuse
    # updates in place and a second backfill creates nothing new
    again = TurnTracer(LangfuseEmitter(client), user_id="sam")
    for record in bus.since(0):
        again.handle(record)
    again.flush()
    second = {s.name: s for s in exporter.get_finished_spans()}
    assert {n: s.context.span_id for n, s in first.items()} == \
        {n: s.context.span_id for n, s in second.items()}
    scores = [e["body"] for batch in posted for e in batch.get("batch", [])
              if e.get("type", "").startswith("score")]
    by_name = {}
    for sc in scores:
        by_name.setdefault(sc["name"], set()).add(sc["id"])
    assert set(by_name) == {"turn_status", "refused", "tasks_failed"}
    assert all(len(ids_) == 1 for ids_ in by_name.values())  # same id twice


# ── the record on the fake SDK database ──────────────────────
def _seed(db, owner="u-ana"):
    """Two sessions in the chat tables: a plain proposed turn with a
    thumbs up and a compound turn, every record through add_event."""
    store = SpannerAssistantStore(db, owner)
    s1 = store.create_session("assistant", build_id="b_1")
    bus = EventBus(s1["id"], None, events=ASSISTANT_EVENTS)
    bus.sinks.append(lambda r: store.add_event(s1["id"], r))
    _emit_turn(bus, "t1")
    e = bus.emit
    e("turn_started", turn_id="t2", text="declines per day last week",
      build_id="b_1", version="assistant/3", skills=["authorizations"],
      mode="chat", plane="vertex")
    e("skills_loaded", turn_id="t2", skills=[{"skill_name": "authorizations",
                                             "mode": "whole"}],
      skills_loaded=["authorizations"], aggregate_skill_chars=1200,
      whole_load_limit=200000, retrieval_budget=0, retrieval_chunks=0)
    e("model_prompt", turn_id="t2", n=0, kind="system", content="SYS",
      prompt_version="assistant/3+deadbeef", prompt_parts={"identity": "1"})
    e("model_prompt", turn_id="t2", n=1, kind="call", content="[u]")
    e("say_token", turn_id="t2", delta="Here is the query.")
    e("budget_tick", turn_id="t2", tokens_in=400, tokens_out=90, tokens=490,
      calls=3, turn_tokens=180)
    row = store.add_message(s1["id"], "assistant", "Here is the query.",
                            turn_id="t2",
                            payload={"proposal": {"sql": GOLD_SQL,
                                                  "title": "Declines per day",
                                                  "metric_id": "m_declines"},
                                     "chips": []})
    e("proposal", turn_id="t2", message_id=row["id"],
      proposal={"sql": GOLD_SQL, "title": "Declines per day"})
    e("turn_done", turn_id="t2", status="proposed", elapsed_ms=800.0,
      tokens_in=400, tokens_out=90, tokens=490, calls=3, turn_tokens=180)
    store.add_feedback(s1["id"], "answer", "up", turn_id="t2", note="spot on")
    s2 = store.create_session("assistant", build_id="b_1")
    bus2 = EventBus(s2["id"], None, events=ASSISTANT_EVENTS)
    bus2.sinks.append(lambda r: store.add_event(s2["id"], r))
    _emit_compound(bus2, "t9")
    store.add_feedback(s2["id"], "answer", "down", turn_id="t9")
    return store, s1, s2


@pytest.fixture()
def record():
    fake = FakeSpannerDatabase()
    db = SpannerDatabase(fake)
    store, s1, s2 = _seed(db)
    return RecordReader(db), store, s1, s2


def test_reader_groups_the_rows_into_turns(record):
    reader, _store, s1, s2 = record
    sessions = reader.sessions()
    assert [s["id"] for s in sessions] == [s1["id"], s2["id"]]
    assert sessions[0]["owner"] == "u-ana"
    turns = reader.turns(sessions[0])
    assert [t["turn_id"] for t in turns] == ["t1", "t2"]
    t2 = turns[1]
    assert t2["question"] == "declines per day last week"
    assert t2["status"] == "proposed" and t2["sql"] == GOLD_SQL
    assert t2["skills_loaded"] == ["authorizations"]
    assert t2["skill_modes"] == {"authorizations": "whole"}
    assert t2["prompt_version"] == "assistant/3+deadbeef"
    assert t2["feedback"][0]["vote"] == "up"
    assert t2["tokens"]["tokens"] == 490 and t2["model_calls"] == 1
    (t9,) = reader.turns(sessions[1])
    assert t9["plan"]["tasks"][1]["depends_on"] == ["a"]
    assert [t["status"] for t in t9["tasks"]] == ["done", "failed"]
    assert t9["refused"] == ["run_sql: over the ceiling"]
    assert t9["answer"] == "Here is the chart."     # the parent's prose only
    assert reader.sessions(owner="nobody") == []
    assert reader.sessions(session_id=s2["id"])[0]["id"] == s2["id"]
    assert reader.sessions(since="2099-01-01") == []
    assert len(reader.sessions(since="2000-01-01T00:00:00Z")) == 2


def test_backfill_from_the_store_equals_live_and_is_idempotent(record):
    reader, _store, s1, s2 = record
    rec = Recorder()
    stats = backfill(reader, TurnTracer(rec, user_id="ignored"))
    assert stats == {"sessions": 2, "turns": 3, "records": stats["records"],
                     "feedback": 2, "skipped_turns": 0}
    opened = rec.of("trace_open")
    assert [o["trace_id"] for o in opened] == [
        trace_id_for(s1["id"], "t1"), trace_id_for(s1["id"], "t2"),
        trace_id_for(s2["id"], "t9")]
    assert {o["user_id"] for o in opened} == {"u-ana"}    # the owner
    feedback = [s for s in rec.of("score") if s["name"] == "feedback"]
    assert [(s["trace_id"], s["value"], s["comment"]) for s in feedback] == [
        (trace_id_for(s1["id"], "t2"), 1.0, "answer — spot on"),
        (trace_id_for(s2["id"], "t9"), 0.0, "answer")]
    # the same calls as a live tracer on the same bus would make
    live = Recorder()
    tracer = TurnTracer(live, user_id="u-ana")
    for session in reader.sessions():
        for r in reader.events(session):
            tracer.handle(r)
    stored = [c for c in rec.calls if c[0] not in ("flush",)
              and not (c[0] == "score" and c[1]["name"] == "feedback")]
    assert stored == live.calls
    # again: identical calls, nothing new (ids are a function of the record)
    again = Recorder()
    backfill(reader, TurnTracer(again))
    assert again.calls == rec.calls
    # narrowed by session, by owner, and by a --since after everything
    one = Recorder()
    backfill(reader, TurnTracer(one), session_id=s2["id"])
    assert [o["trace_id"] for o in one.of("trace_open")] == \
        [trace_id_for(s2["id"], "t9")]
    none = Recorder()
    assert backfill(reader, TurnTracer(none), since="2099-01-01")["turns"] == 0
    assert none.of("trace_open") == []
    later = Recorder()
    stats = backfill(reader, TurnTracer(later), owner="u-ana",
                     since="2000-01-01")
    assert stats["turns"] == 3


# ── the three dataset builders ───────────────────────────────
def test_precedents_load_from_the_documented_shape(tmp_path):
    items = D.precedent_items(D.read_precedents(PRECEDENTS))
    assert [i["id"] for i in items] == ["prec_declines_per_day",
                                        "prec_approval_rate_region",
                                        "prec_top_merchants_volume"]
    first = items[0]
    assert first["schema"] == D.PRECEDENT_SCHEMA
    assert first["input"]["prompt"].startswith("how many declined")
    assert first["expected_output"]["skill"] == "authorizations"
    assert first["expected_output"]["frame"]["grain"] == "day (part_dt)"
    assert first["metadata"]["canonical_fp"]
    out = D.write_jsonl(items, tmp_path / "precedents.jsonl")
    assert D.read_items(out) == items
    assert D.is_item_file(D.read_items(out))
    # a row without a question or SQL is not an item
    assert D.precedent_items([{"question": "x"}, {"sql": "SELECT 1"}]) == []


def test_silver_keeps_the_thumbed_up_and_the_clean_done_turns(record):
    reader, _store, s1, s2 = record
    turns = [t for s in reader.sessions() for t in reader.turns(s)]
    items = D.silver_items(turns)
    # t1 (answered, nothing refused) and t2 (thumbs up); t9 was thumbed
    # down and refused a step
    assert [i["id"] for i in items] == [f"{s1['id']}/t1", f"{s1['id']}/t2"]
    t2 = items[1]
    assert t2["schema"] == D.SILVER_SCHEMA
    assert t2["expected_output"]["sql"] == GOLD_SQL
    assert t2["expected_output"]["answer"] == "Here is the query."
    assert t2["expected_output"]["metric_id"] == "m_declines"
    assert t2["metadata"]["skills_loaded"] == ["authorizations"]
    assert t2["metadata"]["build_id"] == "b_1"
    assert t2["metadata"]["prompt_version"] == "assistant/3+deadbeef"
    assert t2["metadata"]["source_turn"] == {"session_id": s1["id"],
                                             "turn_id": "t2",
                                             "owner": "u-ana"}
    assert t2["metadata"]["feedback"] == ["up"]
    down = dict(turns[2], feedback=[{"vote": "up"}], refused=[])
    assert D.is_silver(down)                               # up wins
    assert not D.is_silver(dict(turns[0], status="partial"))
    assert not D.is_silver(dict(turns[0], refused=["x"]))


def test_scenarios_come_from_plan_made(record):
    reader, _store, _s1, s2 = record
    turns = [t for s in reader.sessions() for t in reader.turns(s)]
    (item,) = D.scenario_items(turns)
    assert item["id"] == f"{s2['id']}/t9"
    assert item["input"]["prompt"] == "declines per day, and a chart"
    assert [t["id"] for t in item["expected_output"]["tasks"]] == ["a", "b"]
    assert item["expected_output"]["waves"] == [["a"], ["b"]]
    assert item["expected_output"]["synthesis"] == "lead with the chart"
    assert item["metadata"]["task_status"] == {"a": "done", "b": "failed"}


def test_items_become_tasks_the_harness_grades():
    from sahs.evals.grading import SutAnswer, grade
    from sahs.evals.harness import run_suite
    from sahs.evals.suts import null, oracle
    items = D.precedent_items(D.read_precedents(PRECEDENTS))
    tasks = D.items_to_tasks(items)
    assert [t.kind for t in tasks] == ["nl2sql"] * 3
    assert "skill=authorizations" in tasks[0].tags
    assert tasks[0].grading.accepted_fps == [items[0]["metadata"]["canonical_fp"]]
    report = run_suite(tasks, oracle)
    assert report["overall"]["pass@1"] == 1.0
    assert run_suite(tasks, null)["overall"]["pass@1"] == 0.0
    # a silver item with prose only is skipped; one with SQL is a task
    silver = [{"schema": D.SILVER_SCHEMA, "id": "s/1",
               "input": {"prompt": "why"}, "expected_output": {"sql": "",
                                                               "answer": "…"},
               "metadata": {}},
              {"schema": D.SILVER_SCHEMA, "id": "s/2",
               "input": {"prompt": "declines"},
               "expected_output": {"sql": GOLD_SQL},
               "metadata": {"source_turn": {"turn_id": "t2"}}}]
    (task,) = D.items_to_tasks(silver)
    assert task.id == "s/2" and task.provenance.source_id == "t2"
    # scenarios grade the plan's shape, never the prose
    scenario = {"schema": D.SCENARIO_SCHEMA, "id": "sc/1",
                "input": {"prompt": "x and y"},
                "expected_output": {"tasks": [
                    {"id": "a", "goal": "x", "kind": "query", "depends_on": []},
                    {"id": "b", "goal": "y", "kind": "artifact",
                     "depends_on": ["a"]}], "synthesis": ""},
                "metadata": {}}
    (task,) = D.items_to_tasks([scenario])
    assert task.kind == "decompose"
    assert grade(task, oracle(task), None).verdict == "pass"
    assert grade(task, null(task), None).reason == "no_plan"
    same_shape = SutAnswer(kind="plan", tasks=[
        {"id": "1", "goal": "other words", "kind": "query", "depends_on": []},
        {"id": "2", "goal": "…", "kind": "artifact", "depends_on": ["1"]}])
    assert grade(task, same_shape, None).verdict == "pass"
    flat = SutAnswer(kind="plan", tasks=[
        {"id": "1", "kind": "query", "depends_on": []},
        {"id": "2", "kind": "artifact", "depends_on": []}])
    assert grade(task, flat, None).verdict == "ambiguous"
    assert grade(task, SutAnswer(kind="plan", tasks=[]), None).verdict == "fail"


# ── the experiment run on the items ──────────────────────────
def test_run_on_an_item_file_scores_verdict_and_skill_on_each_item(tmp_path):
    from sahs.evals.grading import SutAnswer
    from sahs.evals.harness import run_suite
    from sahs.observe.experiments import (dataset_name, experiment_recorder,
                                          read_any_tasks)
    items = D.precedent_items(D.read_precedents(PRECEDENTS))
    path = D.write_jsonl(items, tmp_path / "precedents.jsonl")
    assert dataset_name(path) == "wyla-precedents"
    tasks = read_any_tasks(path)
    client = _FakeClient()
    recorder = experiment_recorder(client, [path], sut="scripted",
                                   canon_version="c1", run_name="run-p",
                                   queue=False)
    assert set(client.datasets) == {"wyla-precedents"}
    assert client.items["wyla-precedents"]["prec_declines_per_day"][
        "expected_output"]["skill"] == "authorizations"

    def scripted(task):         # right SQL, the skill only on the first
        return SutAnswer(kind="sql", sql=task.gold.sql,
                         skills=["authorizations"]
                         if task.id == "prec_declines_per_day" else ["other"])
    report = run_suite(tasks, recorder.sut(scripted), on_trial=recorder.on_trial)
    assert report["overall"]["pass@1"] == 1.0
    created = client.api.dataset_run_items.created
    assert {c["dataset_item_id"] for c in created} == {i["id"] for i in items}
    scores = {}
    for span in client.spans:
        scores[span.kw["metadata"]["task_id"]] = {
            s["name"]: s["value"] for s in span.scores}
    assert scores["prec_declines_per_day"] == {"verdict": "pass", "pass": 1.0,
                                               "skill_hit": 1.0}
    assert scores["prec_top_merchants_volume"]["skill_hit"] == 0.0


def test_assistant_answer_is_read_off_the_record_and_the_planner_sut():
    from sahs.assistant.agent import ScriptedAgent
    from sahs.evals.assistant_sut import answer_from_events, planner_sut
    from sahs.evals.schema import Task, TaskProvenance
    events = [{"ev": "turn_started", "turn_id": "t1"},
              {"ev": "skills_loaded", "turn_id": "t1",
               "skills_loaded": ["authorizations"]},
              {"ev": "tool_call", "turn_id": "t1", "tool": "run_sql",
               "args": {"sql": "SELECT 2", "mode": "dry_run"}},
              {"ev": "proposal", "turn_id": "t1",
               "proposal": {"sql": GOLD_SQL}},
              {"ev": "turn_done", "turn_id": "t1", "status": "proposed"}]
    answer = answer_from_events(events)
    assert answer.kind == "sql" and answer.sql == GOLD_SQL
    assert answer.skills == ["authorizations"]
    ran = answer_from_events([e for e in events if e["ev"] != "proposal"])
    assert ran.sql == "SELECT 2"                    # the query it ran
    silent = answer_from_events(events[:2] + [events[-1]])
    assert silent.kind == "abstain" and "proposed" in silent.reason
    plan = {"tasks": [{"id": "t1", "goal": "declines per day",
                       "depends_on": [], "kind": "query"},
                      {"id": "t2", "goal": "a chart of them",
                       "depends_on": ["t1"], "kind": "artifact"}],
            "synthesis": "chart first"}
    sut = planner_sut(ScriptedAgent(json_answers=[plan]))
    assert sut.answerable_kinds == ("decompose",)
    task = Task(id="sc", kind="decompose",
                prompt="Show me declines per day for last week, then chart "
                       "them, and also list the top merchants by volume",
                provenance=TaskProvenance(source="test"))
    got = sut(task)
    assert got.kind == "plan" and [t["id"] for t in got.tasks] == ["t1", "t2"]
    single = Task(id="one", kind="decompose", prompt="what is an approval?",
                  provenance=TaskProvenance(source="test"))
    assert planner_sut(ScriptedAgent(json_answers=[plan]))(single).tasks == []


def test_assistant_sut_runs_a_real_turn_on_the_scripted_engine(compiled):
    from sahs.assistant.agent import ScriptedAgent
    from sahs.evals.assistant_sut import assistant_sut
    from sahs.evals.grading import grade
    from sahs.evals.harness import run_suite
    build, _ = compiled
    items = D.precedent_items(D.read_precedents(PRECEDENTS))[:1]
    (task,) = D.items_to_tasks(items)
    model = ScriptedAgent([[{"text": "Counted per day, handed over."}]])
    sut = assistant_sut(build, lambda budget: model, wait_seconds=60)
    assert sut.answerable_kinds == ("nl2sql",)
    answer = sut(task)
    assert answer.kind == "abstain"           # the script proposed nothing
    trial = grade(task, answer, None)
    assert trial.verdict == "fail" and trial.reason == "abstained_on_answerable"
    assert run_suite([task], sut)["overall"]["pass@1"] == 0.0


# ── the coverage table ───────────────────────────────────────
def test_coverage_is_computed_from_the_live_rows(record):
    reader, _store, _s1, _s2 = record
    rows = coverage(reader)
    state = {r["concept"]: r["present"] for r in rows}
    assert state["trace"] and state["session"] and state["user"]
    assert state["generation usage"] and state["tool span"] and state["task span"]
    assert state["artifact event"] and state["proposal event"] and state["chips event"]
    assert state["score feedback"] and state["prompt version"]
    assert state["dataset item: silver"] and state["dataset item: scenarios"]
    assert state["per-task usage"] is None and state["experiment run"] is None
    counts = {r["concept"]: r["count"] for r in rows}
    assert counts["trace"] == 3 and counts["score feedback"] == 2
    assert counts["dataset item: silver"] == 2
    text = format_coverage(rows)
    assert "present" in text and "n/a" in text and "MISSING" not in text
    assert format_coverage_markdown(rows).startswith("| concept |")
    # an empty store: every row-built concept is missing, honestly
    empty = RecordReader(SpannerDatabase(FakeSpannerDatabase()))
    assert all(r["present"] is False for r in coverage(empty)
               if r["present"] is not None)


# ── prompt versions: the parts, idempotent ───────────────────
def test_prompt_parts_register_once_with_production_and_family_labels(tmp_path):
    from sahs.observe import prompts as P
    names = {r["name"]: r for r in P.registry()}
    assert {"wyla-assistant-identity", "wyla-assistant-chain",
            "wyla-assistant-mode-chat", "wyla-assistant-mode-autopilot",
            "wyla-assistant-style-gemini-3", "wyla-planner-system",
            "wyla-judge-system", "wyla-review-system"} <= set(names)
    assert names["wyla-assistant-style-gemini-3"]["labels"] == \
        ["production", "gemini-3"]
    assert names["wyla-planner-system"]["version"].startswith("text-")
    assert "{{style}}" in names["wyla-assistant-system"]["prompt"]
    client = _PromptClient()
    out = tmp_path / "prompts.json"
    first = P.register_prompts(client, out, root=None)
    assert all(r["created"] for r in first) and len(first) == len(names)
    stored = client.store["wyla-assistant-style-gemini-3"][0]["labels"]
    assert {"assistant-3", "production", "gemini-3"} <= set(stored)
    second = P.register_prompts(client, out, root=None)
    assert not any(r["created"] or r["drift"] for r in second)


# ── the command line ─────────────────────────────────────────
def test_sync_builds_item_files_and_prints_coverage(tmp_path, monkeypatch,
                                                    capsys):
    import scripts.langfuse_sync as sync
    fake = FakeSpannerDatabase(tmp_path / "chat.sqlite3")
    db = SpannerDatabase(fake)
    _seed(db)
    monkeypatch.setattr(sync, "_record_reader", lambda: RecordReader(db))
    monkeypatch.delenv("SAHS_LANGFUSE", raising=False)
    out = tmp_path / "precedents.jsonl"
    assert sync.main(["datasets", "--build", "precedents",
                      "--out", str(out)]) == 0
    assert len(D.read_items(out)) == 3
    out = tmp_path / "silver.jsonl"
    assert sync.main(["datasets", "--build", "silver", "--out", str(out)]) == 0
    assert len(D.read_items(out)) == 2
    out = tmp_path / "scenarios.jsonl"
    assert sync.main(["datasets", "--build", "scenarios", "--out", str(out),
                      "--since", "2000-01-01"]) == 0
    assert len(D.read_items(out)) == 1
    # --push without the switch sends nothing and says so
    assert sync.main(["datasets", "--build", "silver", "--out", str(out),
                      "--push"]) == 2
    assert sync.main(["coverage"]) == 0
    printed = capsys.readouterr().out
    assert "trace" in printed and "0 missing" in printed
    assert sync.main(["backfill", "--from", "spanner"]) == 2   # no Langfuse
    assert sync.main(["backfill"]) == 2                        # no files


def test_run_evals_takes_an_item_file(tmp_path, capsys):
    import scripts.run_evals as run_evals
    items = D.precedent_items(D.read_precedents(PRECEDENTS))
    path = D.write_jsonl(items, tmp_path / "precedents.jsonl")
    code = run_evals.main(["--tasks", str(path), "--sut", "oracle",
                           "--plain", "--json"])
    assert code == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["overall"]["pass@1"] == 1.0
