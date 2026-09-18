"""sahs.observe — the Langfuse mirror of the record.

The translator is tested on a Recorder (every emitter call as data),
the SDK adapter on the real SDK with an in-memory OTel exporter (no
network), and the experiment recorder on a fake client that keeps
what it was asked to create. Replay equals live is a test, not a
claim.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.assistant.events import ASSISTANT_EVENTS, EventBus   # noqa: E402
from sahs.observe.tracer import (Recorder, TurnTracer,          # noqa: E402
                                 replay_file, trace_id_for)

TASKS = SILO / "tests" / "tasks"


def _emit_turn(bus: EventBus, turn_id: str = "t1", *, error: bool = False):
    """One healthy assistant turn, in the order the loop emits it."""
    e = bus.emit
    e("turn_started", turn_id=turn_id, text="approvals last week",
      build_id="b_1", version="assistant/3", skills=["charts"],
      memories=0, project="", thinking_level="medium", mode="chat",
      plane="vertex", files=[])
    e("model_prompt", turn_id=turn_id, n=0, kind="system", content="SYS")
    e("model_prompt", turn_id=turn_id, n=1, kind="call", content="[user] q")
    e("thinking", turn_id=turn_id, delta="look first")
    e("budget_tick", turn_id=turn_id, tokens_in=100, tokens_out=20,
      tokens=120, calls=1, turn_tokens=120, cost_usd=None)
    e("tool_call", turn_id=turn_id, n=1, tool="search",
      args={"q": "approvals"}, input="search approvals")
    e("tool_step", turn_id=turn_id, n=1, tool="search",
      args={"q": "approvals"}, input="search approvals",
      summary="3 hits", ref="a1", elapsed_ms=4.2)
    e("tool_result", turn_id=turn_id, ref="a1", tool="search",
      content='{"hits": [1, 2, 3]}')
    e("model_prompt", turn_id=turn_id, n=2, kind="call", content="[model]")
    e("say_token", turn_id=turn_id, delta="Here ")
    e("say_token", turn_id=turn_id, delta="you go.")
    e("budget_tick", turn_id=turn_id, tokens_in=250, tokens_out=60,
      tokens=310, calls=2, turn_tokens=310, cost_usd=None)
    e("artifact", turn_id=turn_id, artifact_id="art1", version=1,
      type="chart", title="Approvals", spec={"x": 1})
    if error:
        e("error", turn_id=turn_id, code="internal", message="boom",
          retryable=True, next_actions=[])
    e("chips", turn_id=turn_id, suggestions=[{"label": "more"}])
    e("turn_done", turn_id=turn_id, status="error" if error else "answered",
      elapsed_ms=900.0, tokens_in=250, tokens_out=60, tokens=310,
      calls=2, turn_tokens=310)


def _traced(tmp_path, **kw):
    rec = Recorder()
    tracer = TurnTracer(rec, user_id="sam", model_of=lambda p: f"model:{p}",
                        **kw)
    bus = EventBus("s1", tmp_path / "s1.jsonl", events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    return rec, tracer, bus


# ── the bus ──────────────────────────────────────────────────
def test_sinks_see_every_record_and_never_break_emit(tmp_path):
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    seen = []

    def boom(_record):
        raise RuntimeError("observer down")
    bus.sinks += [seen.append, boom]
    record = bus.emit("turn_started", turn_id="t1", text="hi")
    assert seen == [record]
    assert bus.sink_errors == 1
    assert bus.since(0) == [record]          # the bus is unharmed


# ── the translator ───────────────────────────────────────────
def test_one_turn_is_one_trace_with_generations_tools_and_a_score(tmp_path):
    rec, tracer, bus = _traced(tmp_path)
    _emit_turn(bus)
    tid = trace_id_for("s1", "t1")
    assert len(tid) == 32

    (opened,) = rec.of("trace_open")
    assert opened["trace_id"] == tid
    assert opened["session_id"] == "s1" and opened["user_id"] == "sam"
    assert opened["input"] == "approvals last week"
    assert opened["metadata"]["build_id"] == "b_1"
    assert opened["metadata"]["version"] == "assistant/3"
    assert opened["metadata"]["model"] == "model:vertex"
    assert {"assistant", "plane:vertex", "mode:chat", "skill:charts"} \
        <= set(opened["tags"])

    gens = rec.of("generation_open")
    assert [g["key"] for g in gens] == ["gen1", "gen2"]
    assert gens[0]["model"] == "model:vertex"
    assert gens[0]["input"]["system"] == "SYS"      # once, on call 1
    assert "system" not in gens[1]["input"]

    closes = rec.of("generation_close")
    assert closes[0]["usage"] == {"input": 100, "output": 20, "total": 120}
    assert closes[1]["usage"] == {"input": 150, "output": 40, "total": 190}
    assert closes[0]["output"]["thought"] == "look first"
    assert closes[1]["output"]["text"] == "Here you go."

    (span,) = rec.of("span_open")
    assert span["name"] == "search" and span["key"] == "tool1"
    assert span["input"]["args"] == {"q": "approvals"}
    (closed,) = rec.of("span_close")
    assert closed["output"] == {"summary": "3 hits"}   # no rows by default
    assert closed["metadata"]["elapsed_ms"] == 4.2
    assert closed["level"] == "DEFAULT"

    (event,) = rec.of("event")
    assert event["name"] == "artifact:chart"
    assert event["metadata"]["artifact_id"] == "art1"

    (done,) = rec.of("trace_close")
    assert done["output"] == "Here you go."
    assert done["metadata"]["status"] == "answered"
    assert done["level"] == "DEFAULT"
    (score,) = rec.of("score")
    assert score["name"] == "turn_status" and score["value"] == "answered"
    assert tracer._turns == {}                    # forgotten on turn_done


def test_full_results_dial_puts_the_tool_content_on_the_span(tmp_path):
    rec, _tracer, bus = _traced(tmp_path, full_results=True)
    _emit_turn(bus)
    (closed,) = rec.of("span_close")
    assert closed["output"]["content"] == '{"hits": [1, 2, 3]}'


def test_an_error_turn_closes_at_error_level_with_the_message(tmp_path):
    rec, _tracer, bus = _traced(tmp_path)
    _emit_turn(bus, error=True)
    (done,) = rec.of("trace_close")
    assert done["level"] == "ERROR"
    assert done["status_message"] == "internal: boom"
    assert done["metadata"]["errors"] == ["internal: boom"]


def test_usage_deltas_carry_across_turns_of_one_session(tmp_path):
    rec, _tracer, bus = _traced(tmp_path)
    _emit_turn(bus, "t1")
    # the second turn's first tick is measured against the first turn's
    # last: session counters never reset, so neither does the baseline
    bus.emit("turn_started", turn_id="t2", text="and by region?",
             build_id="b_1", version="assistant/3", plane="vertex")
    bus.emit("model_prompt", turn_id="t2", n=1, kind="call", content="[u]")
    bus.emit("budget_tick", turn_id="t2", tokens_in=300, tokens_out=70,
             tokens=370, calls=3, turn_tokens=60)
    bus.emit("turn_done", turn_id="t2", status="answered")
    assert rec.of("generation_close")[-1]["usage"] == \
        {"input": 50, "output": 10, "total": 60}
    assert len(rec.of("trace_open")) == 2


def test_replay_of_the_events_file_equals_live(tmp_path):
    live, _tracer, bus = _traced(tmp_path)
    _emit_turn(bus)
    replayed = Recorder()
    n = replay_file(bus.path, TurnTracer(
        replayed, user_id="sam", model_of=lambda p: f"model:{p}"))
    assert n == len(bus.since(0))
    assert replayed.calls == live.calls


def test_records_from_a_turn_never_started_are_ignored(tmp_path):
    rec, tracer, _bus = _traced(tmp_path)
    tracer.handle({"ev": "say_token", "session_id": "s9", "turn_id": "t9",
                   "delta": "orphan"})
    tracer.handle({"ev": "turn_done", "session_id": "s9", "turn_id": "t9",
                   "status": "answered"})
    assert rec.calls == []


# ── the runtime hook ─────────────────────────────────────────
def test_runtime_attaches_its_observer_to_every_new_session(tmp_path):
    from sahs.assistant import AssistantRuntime
    runtime = AssistantRuntime(builds_root=tmp_path / "builds",
                               graph_root=tmp_path / "graph",
                               store_path=tmp_path / "chat.sqlite3",
                               model_factory=lambda budget: None)
    assert runtime.runtime("before").bus.sinks == []
    seen = []
    runtime.observer = seen.append
    rt = runtime.runtime("after")
    assert rt.bus.sinks == [seen.append]
    rt.bus.emit("turn_started", turn_id="t1", text="x")
    assert seen and seen[0]["ev"] == "turn_started"


def test_observer_is_none_unless_the_switch_is_on():
    from sahs.observe import langfuse_observer
    assert langfuse_observer(env={}) is None
    assert langfuse_observer(env={"SAHS_LANGFUSE": "0"}) is None


# ── the SDK adapter, on the real SDK, exported to memory ─────
@pytest.fixture
def sdk_client():
    pytest.importorskip("langfuse")
    import uuid

    import httpx
    from langfuse import Langfuse
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter)
    exporter = InMemorySpanExporter()
    posted: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        # the score ingestion endpoint, answered the way Langfuse does
        posted.append(json.loads(request.content or b"{}"))
        return httpx.Response(207, json={"successes": [], "errors": []})
    # a fresh public key per client: the SDK keeps one resource set
    # per key, and an earlier test's exporter must not receive ours
    client = Langfuse(public_key=f"pk-test-{uuid.uuid4().hex[:8]}",
                      secret_key="sk-test", base_url="http://127.0.0.1:1",
                      httpx_client=httpx.Client(
                          transport=httpx.MockTransport(handler)),
                      span_exporter=exporter, flush_interval=0.2)
    yield client, exporter, posted
    client.shutdown()


def test_langfuse_emitter_writes_the_trace_the_tracer_asked_for(
        tmp_path, sdk_client):
    from sahs.observe.langfuse_emitter import LangfuseEmitter
    client, exporter, posted = sdk_client
    tracer = TurnTracer(LangfuseEmitter(client), user_id="sam",
                        model_of=lambda p: "gemini-test")
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    _emit_turn(bus)
    assert bus.sink_errors == 0
    tracer.flush()
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert {"assistant.turn", "model call 1", "model call 2", "search"} \
        <= set(spans)
    root = spans["assistant.turn"]
    assert format(root.context.trace_id, "032x") == trace_id_for("s1", "t1")
    assert root.attributes["session.id"] == "s1"
    assert root.attributes["user.id"] == "sam"
    assert "plane:vertex" in root.attributes["langfuse.trace.tags"]
    gen = spans["model call 1"]
    assert gen.attributes["langfuse.observation.type"] == "generation"
    assert gen.attributes["langfuse.observation.model.name"] == "gemini-test"
    usage = json.loads(gen.attributes["langfuse.observation.usage_details"])
    assert usage == {"input": 100, "output": 20, "total": 120}
    tool = spans["search"]
    assert tool.attributes["langfuse.observation.type"] == "tool"
    assert tool.parent.span_id == root.context.span_id
    scores = [e["body"] for batch in posted for e in batch.get("batch", [])
              if e.get("type", "").startswith("score")]
    assert [(sc["name"], sc["value"], sc["traceId"]) for sc in scores] == \
        [("turn_status", "answered", trace_id_for("s1", "t1"))]


# ── datasets and runs ────────────────────────────────────────
def test_task_items_keep_the_whole_task_in_metadata():
    from sahs.observe.experiments import read_rows, task_item
    curated = read_rows(TASKS / "curated" / "curated.jsonl")
    item = task_item(curated[0])
    assert item["id"] == curated[0]["id"]
    assert item["input"]["prompt"] == curated[0]["prompt"]
    assert item["metadata"]["schema"] == "meridian.task/1"
    matrix = read_rows(TASKS / "capability" / "matrix.jsonl")
    item = task_item(matrix[0])
    assert item["id"] == matrix[0]["id"]
    assert item["input"]["prompt"] == matrix[0]["prompt"]
    assert item["expected_output"] == matrix[0]["expect"]
    assert item["metadata"] == matrix[0]


def test_dataset_names_and_versions_are_stable(tmp_path):
    from sahs.observe.experiments import dataset_name, tasks_version
    assert dataset_name(TASKS / "curated" / "curated.jsonl") == "wyla-curated"
    assert dataset_name(TASKS / "capability" / "matrix.jsonl") == \
        "wyla-capability-matrix"
    a = tasks_version([TASKS / "curated" / "curated.jsonl"])
    assert a == tasks_version([TASKS / "curated" / "curated.jsonl"])
    other = tmp_path / "curated.jsonl"
    other.write_text("{}\n")
    assert tasks_version([other]) != a


class _FakeSpan:
    def __init__(self, log, **kw):
        self.log = log
        self.trace_id = kw["trace_context"]["trace_id"]
        self.id = "obs-" + self.trace_id[:6]
        self.kw = kw
        self.scores = []

    def score_trace(self, **kw):
        self.scores.append(kw)

    def update(self, **kw):
        self.kw.update(kw)
        return self

    def end(self):
        self.log.append(self)


class _FakeRunItems:
    def __init__(self):
        self.created = []

    def create(self, **kw):
        self.created.append(kw)


class _FakeItem:
    def __init__(self, row):
        self.id = row["id"]
        self.input = row["input"]
        self.metadata = row["metadata"]


class _FakeClient:
    def __init__(self):
        self.spans = []
        self.datasets = {}
        self.items = {}

        class _Api:
            dataset_run_items = _FakeRunItems()
        self.api = _Api()

    def start_observation(self, **kw):
        return _FakeSpan(self.spans, **kw)

    def create_dataset(self, *, name, **kw):
        self.datasets[name] = kw

    def create_dataset_item(self, *, dataset_name, **item):
        self.items.setdefault(dataset_name, {})[item["id"]] = item

    def get_dataset(self, name):
        class _D:
            items = [_FakeItem(r) for r in self.items[name].values()]
        return _D()

    def flush(self):
        pass


def test_experiment_recorder_scores_every_trial_on_its_item():
    from sahs.evals.harness import run_suite
    from sahs.evals.schema import read_tasks
    from sahs.evals.suts import oracle
    from sahs.observe.experiments import experiment_recorder
    path = TASKS / "curated" / "curated.jsonl"
    tasks = read_tasks(path)
    client = _FakeClient()
    recorder = experiment_recorder(client, [path], sut="oracle",
                                   canon_version="c1", run_name="run-1")
    assert set(client.datasets) == {"wyla-curated"}
    assert len(client.items["wyla-curated"]) == len(tasks)

    report = run_suite(tasks, recorder.sut(oracle), on_trial=recorder.on_trial)
    assert report["overall"]["pass@1"] == 1.0
    assert recorder.recorded == len(tasks) and not recorder.missing
    created = client.api.dataset_run_items.created
    assert {c["dataset_item_id"] for c in created} == {t.id for t in tasks}
    assert all(c["run_name"] == "run-1" for c in created)
    assert created[0]["metadata"]["sut"] == "oracle"
    assert created[0]["metadata"]["canon_version"] == "c1"
    by_name = {s.trace_id: s for s in client.spans}
    assert created[0]["trace_id"] in by_name
    span = by_name[created[0]["trace_id"]]
    assert {s["name"]: s["value"] for s in span.scores} == \
        {"verdict": "pass", "pass": 1.0}
    assert span.kw["output"]["kind"] in ("sql", "abstain", "disambiguate",
                                         "bindings")


def test_recorder_keeps_the_suts_declared_kinds():
    from sahs.observe.experiments import ExperimentRecorder

    def sut(task):
        return None
    sut.answerable_kinds = ("resolve_bind",)
    wrapped = ExperimentRecorder(_FakeClient(), [], run_name="r").sut(sut)
    assert wrapped.answerable_kinds == ("resolve_bind",)
