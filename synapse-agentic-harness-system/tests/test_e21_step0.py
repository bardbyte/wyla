"""E21 Step 0: the gates every later step measures against.

0a: the run-report instrumentation (a recorder OUTSIDE the product,
    wrapping the model exactly as tests wrap the transport).
0b: the E19 capability matrix — RECONSTRUCTED, and these tests pin
    that the reconstruction stays loudly labelled until the real E19
    text reconciles it.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"


def _demo():
    spec = importlib.util.spec_from_file_location(
        "ask_demo", SILO / "scripts" / "ask_demo.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TinyModel:
    """json→parsed, json→None (drift), stream→3 chunks."""

    class client:                                    # noqa: N801
        usage = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0,
                 "thought_tokens": 0}

    def __init__(self):
        self.junk_next = False

    def json(self, prompt, *, system="", temperature=0.0, max_tokens=1024):
        TinyModel.client.usage["calls"] += 1
        TinyModel.client.usage["prompt_tokens"] += 10
        TinyModel.client.usage["output_tokens"] += 5
        if self.junk_next:
            self.junk_next = False
            return None
        return {"ok": True}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        TinyModel.client.usage["output_tokens"] += 3
        yield "a "
        yield "b "
        yield "c"


# ── 0a: the recorder ─────────────────────────────────────────
def test_recorder_counts_calls_ttft_and_drift(tmp_path):
    demo = _demo()
    calls: list = []
    model = TinyModel()
    recorder = demo._Recorder(model, calls)

    assert recorder.json("q", system="You classify ONE turn") == {"ok": True}
    model.junk_next = True
    assert recorder.json("q", system="skeptical reviewer") is None
    assert "".join(recorder.stream(
        "q", system="You compose ONE BigQuery SELECT")) == "a b c"

    assert [c["step"] for c in calls] == ["classify", "judge", "compose"]
    assert calls[0]["parsed"] is True
    assert calls[1]["parsed"] is False        # strict-JSON drift, counted
    assert calls[2]["kind"] == "stream"
    assert calls[2]["chunks"] == 3
    assert calls[2]["ttft_seconds"] is not None
    assert calls[0]["tokens_in"] == 10 and calls[0]["tokens_out"] == 5

    path = demo.write_report(
        tmp_path, build_id="b_test",
        events=[{"ev": "turn_started", "turn_id": "t1", "text": "q"},
                {"ev": "turn_done", "turn_id": "t1", "status": "answered",
                 "elapsed_ms": 5, "tokens": 18}],
        calls=calls, arrivals={}, store_versions=[{"version": 1}])
    text = path.read_text()
    assert "json calls: 2 · unparsed (fail-closed downstream): 1" in text
    assert "b_test" in text
    assert (tmp_path / "report.json").exists()


def test_report_written_even_when_no_model_ever_answered(tmp_path):
    demo = _demo()
    path = demo.write_report(tmp_path, build_id="b_x", events=[],
                             calls=[], arrivals={}, store_versions=[])
    assert "json calls: 0" in path.read_text()


# ── 0a field lesson: the token seam (first real-graph run) ───
class _FakeCreds:
    def __init__(self):
        self.refreshes = 0
        self.token = "tok"

    @property
    def valid(self):
        return self.refreshes > 0

    def refresh(self, request):
        self.refreshes += 1


def _client(monkeypatch, tmp_path, creds):
    import types
    from sahs.enrich.client import VertexClient
    from sahs.util.auth import VertexConnection
    fake_sa = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda *a, **k: creds))
    fake_req = types.SimpleNamespace(Request=lambda session=None: (
        lambda *a, **k: None))
    # this container has no google-auth: fake the whole package chain
    # (the laptop runs the real one; the seam under test is ours)
    modules = {
        "google": types.SimpleNamespace(oauth2=None, auth=None),
        "google.oauth2": types.SimpleNamespace(service_account=fake_sa),
        "google.oauth2.service_account": fake_sa,
        "google.auth": types.SimpleNamespace(),
        "google.auth.transport": types.SimpleNamespace(requests=fake_req),
        "google.auth.transport.requests": fake_req,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    connection = VertexConnection(
        project="p", location="global", model="m",
        endpoint="https://example.invalid", key_path=tmp_path / "k.json")
    monkeypatch.setattr(VertexConnection, "token_session",
                        lambda self: None)
    return VertexClient(connection=connection)


def test_one_token_per_client_not_per_call(monkeypatch, tmp_path):
    """E21 0a: fresh credentials per call meant every model call risked
    a token round-trip; one proxy blip between the SQL call and the
    prose stream killed the turn. Cached credentials refresh once."""
    creds = _FakeCreds()
    client = _client(monkeypatch, tmp_path, creds)
    assert client._token() == "tok"
    assert client._token() == "tok"
    assert client._token() == "tok"
    assert creds.refreshes == 1


def test_a_dead_token_endpoint_fails_typed_fast_and_off_the_ladder(
        monkeypatch, tmp_path):
    """The 405-second failure: token errors rode the generic retry
    ladder as if they were rate limits. Now they raise
    EnrichTransportError immediately — generate() must not sleep the
    backoff ladder on them, and the stream path gets the same typed
    error the loop renders as the honest model_unavailable card."""
    from sahs.enrich.client import EnrichTransportError

    class _DeadCreds(_FakeCreds):
        def refresh(self, request):
            raise OSError("Connection to oauth2.googleapis.com timed out")

    slept: list[float] = []
    client = _client(monkeypatch, tmp_path, _DeadCreds())
    client.sleep = slept.append
    with pytest.raises(EnrichTransportError) as err:
        client.generate("hi")
    assert "oauth2.googleapis.com" in str(err.value)
    assert "vertex_check" in str(err.value)
    assert slept == [2], (
        "one quick token retry is allowed; the 2/4/8/16 ladder is not")

    slept.clear()
    with pytest.raises(EnrichTransportError):
        list(client.generate_stream("hi"))
    assert slept == [2]


# ── 0b: the matrix ───────────────────────────────────────────
@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("e21")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "e21"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _dir, _manifest, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _tasks():
    from sahs.evals.capability import load_tasks
    tasks = load_tasks(SILO / "tests" / "tasks" / "capability"
                       / "matrix.jsonl")
    for task in tasks:
        if "curated_path" in task:
            task["curated_path"] = str(SILO / task["curated_path"])
    return tasks


def test_the_baseline_is_green_on_every_built_tier(compiled, tmp_path):
    from sahs.evals.capability import TIERS, run_matrix
    build, _ = compiled
    run = run_matrix(build, tmp_path, _tasks(), config="pinned",
                     margin=None)
    failures = [(r.task_id, r.detail) for r in run.results if not r.passed]
    assert not failures, failures
    scored = set(run.tier_scores())
    built = {t for t, m in TIERS.items() if m["built"]}
    assert scored == built, "every built tier is measured, only those"
    line = run.line()
    assert line["answered_pct"] == 100.0
    assert line["wrong_when_answered_pct"] == 0.0
    assert line["false_answer_pct"] == 0.0


def test_a_configuration_is_a_copied_manifest_never_a_ranker_edit(compiled):
    from sahs.evals.capability import build_with_margin
    build, _ = compiled
    before = dict(build.manifest.get("resolver_constants") or {})
    strict = build_with_margin(build, 0.30)
    assert strict is not build
    assert strict.manifest["resolver_constants"][
        "margin_threshold"] == 0.30
    # the original build object is untouched: the ablation can never
    # leak into anything that reads the real manifest afterwards
    assert (build.manifest.get("resolver_constants") or {}) == before
    others = {k: v for k, v in strict.manifest[
        "resolver_constants"].items() if k != "margin_threshold"}
    for key, value in others.items():
        assert before.get(key, value) == value


def test_unbuilt_tiers_are_absent_not_scored(compiled, tmp_path):
    from sahs.evals.capability import render_report, run_matrix
    build, _ = compiled
    run = run_matrix(build, tmp_path, _tasks()[:4], config="pinned",
                     margin=None)
    report = render_report([run], build_id=build.version,
                           transport="scripted")
    assert "T8" in report and "absent — not built, not scored" in report
    # and no capability task may claim an unbuilt tier
    assert not [t for t in _tasks() if t["tier"] in ("T8", "T9")]


def test_the_reconstruction_stays_loudly_labelled():
    """Until the real E19 text reconciles this suite, both the module
    and every published report must say the suite is a reconstruction.
    Deleting the label without the reconciliation is the failure."""
    module = (SILO / "sahs" / "evals" / "capability.py").read_text()
    assert "RECONSTRUCTION NOTICE" in module
    from sahs.evals.capability import MatrixResult, render_report
    report = render_report(
        [MatrixResult(config="pinned", margin=None)],
        build_id="b", transport="scripted")
    assert "RECONSTRUCTION" in report
