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

    events = {e["name"]: e for e in rec.of("event")}
    assert set(events) == {"artifact:chart", "chips"}
    assert events["artifact:chart"]["metadata"]["artifact_id"] == "art1"
    assert events["chips"]["metadata"]["suggestions"] == ["more"]

    (done,) = rec.of("trace_close")
    assert done["output"] == "Here you go."
    assert done["metadata"]["status"] == "answered"
    assert done["level"] == "DEFAULT"
    scores = {s["name"]: s for s in rec.of("score")}
    assert set(scores) == {"turn_status", "refused"}
    assert scores["turn_status"]["value"] == "answered"
    assert scores["refused"]["value"] == 0.0
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
    # the id generator the emitter pins observation ids through, as
    # setup.langfuse_client() hands it to the real client
    from sahs.observe.langfuse_emitter import PINNED
    client = Langfuse(public_key=f"pk-test-{uuid.uuid4().hex[:8]}",
                      secret_key="sk-test", base_url="http://127.0.0.1:1",
                      httpx_client=httpx.Client(
                          transport=httpx.MockTransport(handler)),
                      span_exporter=exporter, flush_interval=0.2,
                      id_generator=PINNED)
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
        [("turn_status", "answered", trace_id_for("s1", "t1")),
         ("refused", 0.0, trace_id_for("s1", "t1"))]


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
                                   canon_version="c1", run_name="run-1",
                                   queue=False)
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


# ── prompt versions as labels ────────────────────────────────
class _FakePrompts:
    """The prompts API: get raises until create registers."""

    def __init__(self, store):
        self.store = store

    def get(self, name, *, label=None, **_kw):
        for row in self.store.get(name, []):
            if label in row["labels"]:
                return row["obj"]
        raise LookupError(f"no prompt {name} @ {label}")


class _PromptObj:
    def __init__(self, name, prompt, version, labels):
        self.name, self.prompt, self.version, self.labels = \
            name, prompt, version, labels


class _PromptClient:
    def __init__(self):
        self.store = {}

        class _Api:
            prompts = _FakePrompts(self.store)
        self.api = _Api()

    def create_prompt(self, *, name, prompt, labels, **_kw):
        version = len(self.store.get(name, [])) + 1
        obj = _PromptObj(name, prompt, version, list(labels))
        # a label moves to the newest version, as Langfuse does
        for row in self.store.get(name, []):
            row["labels"] = [l for l in row["labels"] if l not in labels]
        self.store.setdefault(name, []).append(
            {"labels": list(labels), "obj": obj})
        return obj

    def flush(self):
        pass


def test_prompt_templates_carry_the_static_prose_and_variables():
    from sahs.assistant.loop import ASSISTANT_VERSION, IDENTITY
    from sahs.loop.prompt import PROMPT_VERSION
    from sahs.observe.prompts import (assistant_template, label_for,
                                      loop_template, registry)
    text = assistant_template()
    assert IDENTITY.strip() in text
    assert "{{graph}}" in text and "{{session}}" in text
    assert "{{digest}}" in loop_template()
    names = {(r["name"], r["version"]) for r in registry()}
    assert ("wyla-assistant-system", ASSISTANT_VERSION) in names
    assert ("wyla-loop-system", PROMPT_VERSION) in names
    assert label_for("assistant/3") == "assistant-3"


def test_register_prompts_is_idempotent_and_flags_drift(tmp_path,
                                                        monkeypatch):
    from sahs.observe import prompts as P
    client = _PromptClient()
    out = tmp_path / "langfuse" / "prompts.json"
    first = P.register_prompts(client, out, root=None)
    assert all(r["created"] for r in first)
    assert not any(r["drift"] for r in first)
    links = P.load_links(out)
    assert links["wyla-assistant-system"] == {first[0]["version"]: 1}

    second = P.register_prompts(client, out, root=None)
    assert not any(r["created"] for r in second)      # same text: no-op

    # the text moved but the version string did not: a new Langfuse
    # version under the same label, and the drift is reported
    monkeypatch.setattr(P, "assistant_template", lambda: "changed words")
    third = P.register_prompts(client, out, root=None)
    row = next(r for r in third if r["name"] == "wyla-assistant-system")
    assert row["created"] and row["drift"]
    assert P.load_links(out)["wyla-assistant-system"][row["version"]] == 2
    # the reader follows the file as it changes
    link = P.PromptLinks(out)
    assert link(row["version"]) == ("wyla-assistant-system", 2)
    assert link("never-registered") is None


def test_generations_carry_the_registered_prompt_link(tmp_path):
    rec = Recorder()
    tracer = TurnTracer(rec, prompt_of=lambda v: ("wyla-assistant-system", 7)
                        if v == "assistant/3" else None)
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    _emit_turn(bus)
    assert [g["prompt"] for g in rec.of("generation_open")] == \
        [("wyla-assistant-system", 7)] * 2


def test_langfuse_emitter_links_the_generation_to_the_prompt(sdk_client):
    from sahs.observe.langfuse_emitter import LangfuseEmitter
    client, exporter, _posted = sdk_client
    tracer = TurnTracer(LangfuseEmitter(client),
                        prompt_of=lambda v: ("wyla-assistant-system", 7))
    bus = EventBus("s1", None, events=ASSISTANT_EVENTS)
    bus.sinks.append(tracer)
    _emit_turn(bus)
    tracer.flush()
    gen = {s.name: s for s in exporter.get_finished_spans()}["model call 1"]
    assert gen.attributes["langfuse.observation.prompt.name"] == \
        "wyla-assistant-system"
    assert gen.attributes["langfuse.observation.prompt.version"] == 7


# ── the annotation queue round trip ──────────────────────────
class _Page:
    def __init__(self, data, pages=1):
        self.data = data

        class _Meta:
            total_pages = pages
        self.meta = _Meta()


class _Obj:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _QueueClient(_FakeClient):
    def __init__(self):
        super().__init__()
        outer = self
        self.queues, self.configs, self.items_queued = [], [], []
        self.scores, self.traces = [], {}

        class _Queues:
            @staticmethod
            def list_queues(page=1, limit=100):
                return _Page(list(outer.queues))

            @staticmethod
            def create_queue(*, name, score_config_ids, description=""):
                q = _Obj(id=f"q{len(outer.queues) + 1}", name=name,
                         score_config_ids=score_config_ids)
                outer.queues.append(q)
                return q

            @staticmethod
            def create_queue_item(queue_id, *, object_id, object_type):
                if (queue_id, object_id) in outer.items_queued:
                    raise ValueError("already queued")
                outer.items_queued.append((queue_id, object_id))

        class _Configs:
            @staticmethod
            def get(page=1, limit=100):
                return _Page(list(outer.configs))

            @staticmethod
            def create(*, name, data_type, categories, description=""):
                c = _Obj(id=f"c{len(outer.configs) + 1}", name=name,
                         is_archived=False)
                outer.configs.append(c)
                return c

        class _Scores:
            @staticmethod
            def get_many(*, name, page=1, limit=100):
                return _Page([s for s in outer.scores if s.name == name])

        class _Trace:
            @staticmethod
            def get(trace_id):
                return outer.traces[trace_id]

        self.api.annotation_queues = _Queues()
        self.api.score_configs = _Configs()
        self.api.scores = _Scores()
        self.api.trace = _Trace()


def test_ambiguous_trials_land_on_the_queue_and_resolutions_write_back(
        tmp_path):
    from sahs.canon.canonical import c
    from sahs.evals.grading import SutAnswer, TrialResult
    from sahs.evals.harness import run_suite
    from sahs.evals.schema import (Task, TaskGold, TaskGrading,
                                   TaskProvenance, read_tasks, write_tasks)
    from sahs.evals.substrate import DryRunOutcome, StaticSubstrate
    from sahs.evals.suts import oracle
    from sahs.observe.annotations import (QUEUE_NAME, SCORE_NAME,
                                          pull_resolutions)
    from sahs.observe.experiments import experiment_recorder

    # the seeded semantic twin from the P1 suite: same shape, different
    # fingerprint — AMBIGUOUS until a person says accept or fail
    gold_sql = ("SELECT part_dt, COUNT(1) AS n FROM wwcas_authorization "
                "WHERE approval_cd = 'D' GROUP BY part_dt")
    twin_sql = ("SELECT part_dt, COUNT(approval_cd) AS n "
                "FROM wwcas_authorization WHERE approval_cd = 'D' "
                "GROUP BY part_dt")
    schema = [{"name": "part_dt", "type": "DATE"},
              {"name": "n", "type": "INT64"}]
    twin_task = Task(
        id="nl2sql_twin", kind="nl2sql", prompt="declines per day",
        gold=TaskGold(sql=gold_sql, canonical_fp=c(gold_sql).fp_expr),
        grading=TaskGrading(graders=["parse", "canon_ast", "dry_run"],
                            accepted_fps=[c(gold_sql).fp_expr],
                            result_schema={"fields": schema},
                            dry_run="required"),
        provenance=TaskProvenance(source="test"))
    path = tmp_path / "curated" / "curated.jsonl"
    write_tasks(read_tasks(TASKS / "curated" / "curated.jsonl")
                + [twin_task], path)
    before = path.read_text(encoding="utf-8").splitlines()
    tasks = read_tasks(path)
    substrate = StaticSubstrate({
        c(twin_sql).fp_expr: DryRunOutcome(valid=True,
                                           result_schema=schema)})

    def twin(task):
        if task.id == twin_task.id:
            return SutAnswer(kind="sql", sql=twin_sql)
        return oracle(task)

    client = _QueueClient()
    recorder = experiment_recorder(client, [path], sut="twin",
                                   canon_version="c1", run_name="run-q")
    assert [q.name for q in client.queues] == [QUEUE_NAME]
    assert [c_.name for c_ in client.configs] == [SCORE_NAME]
    report = run_suite(tasks, recorder.sut(twin), substrate=substrate,
                       on_trial=recorder.on_trial)
    assert report["overall"]["ambiguous_rate"] > 0
    assert recorder.queued == [twin_task.id] and not recorder.queue_errors
    (queue_id, trace_id), = client.items_queued
    # a second run re-queues the same trace: reported, never fatal
    recorder.on_trial(TrialResult(twin_task.id, "nl2sql", "ambiguous",
                                  "again", answer_fp=c(twin_sql).fp_expr))
    assert recorder.queue_errors and "already queued" in \
        recorder.queue_errors[0]

    # the trace the recorder wrote, as the API returns it, and the
    # steward's resolution scored on it
    span = next(sp for sp in client.spans if sp.trace_id == trace_id)
    assert span.kw["metadata"]["answer_fp"] == c(twin_sql).fp_expr
    client.traces[trace_id] = _Obj(metadata=None, observations=[
        _Obj(name="eval.trial", metadata=span.kw["metadata"])])
    client.scores.append(_Obj(id="sc1", name=SCORE_NAME, trace_id=trace_id,
                              string_value="accept", value=1, comment=""))
    result = pull_resolutions(client, [path])
    assert result["accepted"] == [twin_task.id]
    assert result["files_written"] == [str(path)]
    reread = {t.id: t for t in read_tasks(path)}
    assert c(twin_sql).fp_expr in reread[twin_task.id].grading.accepted_fps
    # the rewrite is surgical: exactly one line differs
    after = path.read_text(encoding="utf-8").splitlines()
    assert len(before) == len(after)
    assert sum(1 for a, b in zip(before, after) if a != b) == 1
    # pulling again is a no-op, and the suite now passes the twin
    again = pull_resolutions(client, [path])
    assert again["already_accepted"] == [twin_task.id]
    assert not again["files_written"]
    rerun = run_suite(read_tasks(path), twin, substrate=substrate)
    assert rerun["overall"]["ambiguous_rate"] == 0.0
    assert rerun["overall"]["pass@1"] == 1.0


def test_pull_reports_what_it_cannot_match():
    from sahs.observe.annotations import SCORE_NAME, pull_resolutions
    client = _QueueClient()
    client.scores.append(_Obj(id="sc9", name=SCORE_NAME, trace_id="t9",
                              string_value="accept", value=1, comment=""))
    client.traces["t9"] = _Obj(metadata={"task_id": "not_a_task"},
                               observations=[])
    result = pull_resolutions(client, [TASKS / "curated" / "curated.jsonl"])
    assert result["unmatched"][0]["task_id"] == "not_a_task"
    assert not result["files_written"]
