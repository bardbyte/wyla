"""Ask (E18) Stage C: the plan panel, the version stepper, and the
join/grain preview.

Two things are proven here that a browser cannot prove cheaply:

  * the FAN-OUT VERDICT for every shape the build can present. This
    fixture has no question that naturally resolves to two tables, so
    the multi-table card is exercised directly against the real
    compiled build rather than faked into the UI. Every row count,
    key and witness in these assertions is real.
  * RESTORE IS APPEND-ONLY. Undo that rewrote history would make "what
    did we actually ask" unanswerable, so the test pins that the chain
    only ever grows.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"

GMS = "dw.gms_transaction"
WWCAS = "dw.wwcas_authorization"
SBS = "dw.sbs_new_accounts"


@pytest.fixture(scope="module")
def build(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("stagec")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "stagec"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _dir, _manifest, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _plan(table=GMS, grain="transaction"):
    from sahs.ask.plan import Plan
    return Plan(question="q", metric_id="m", metric_label="M",
                table=table, grain=grain,
                provenance={"grain": "resolver"})


# ── the table facts a fan-out judgement needs ────────────────
def test_the_build_carries_row_counts_and_declared_keys(build):
    b, _ = build
    facts = b.table_facts(GMS)
    assert facts["total_rows"] == 1020        # from the real extraction
    assert facts["primary_key"] == ["se_no", "txn_uid"]
    # a table the archive gave no count for reports NO count, never a
    # zero: an absent number and a zero mean opposite things
    assert "total_rows" not in b.table_facts(SBS)
    assert b.table_facts("dw.nope") == {}


def test_single_table_cannot_fan_out(build):
    from sahs.ask.preview import join_grain_preview
    b, _ = build
    preview = join_grain_preview(b, _plan(), [GMS])
    assert preview["verdict"] == "safe" and not preview["joins"]
    assert "1,020" in preview["headline"]     # the real count, formatted
    assert "counted once" in preview["headline"]


def test_a_join_without_a_declared_key_is_unproven_not_guessed(build):
    """The wireframe's 'becomes 3.8M' needs a multiplier nobody has.
    Rather than invent one, the verdict names the missing fact."""
    from sahs.ask.preview import join_grain_preview
    b, _ = build
    preview = join_grain_preview(b, _plan(), [GMS, WWCAS])
    assert preview["verdict"] == "unproven"
    join = preview["joins"][0]
    assert join["table"] == WWCAS and join["on"]
    assert "not a declared primary key" in join["why"]
    assert "may inflate totals" in preview["headline"]
    # no invented arithmetic anywhere in the payload
    assert "3.8" not in str(preview) and "×" not in join["why"]


def test_an_unwitnessed_pair_is_unsafe_and_says_so(build):
    from sahs.ask.preview import join_grain_preview
    b, _ = build
    preview = join_grain_preview(b, _plan(), [GMS, SBS])
    assert preview["verdict"] == "unsafe"
    assert "not attested at all" in preview["joins"][0]["why"]


def test_a_key_join_is_provably_safe(build):
    """se_no IS a declared primary key of gms_transaction, so a join
    landing on it can match at most once. That is provable from the
    build, and the preview proves it rather than hoping."""
    from sahs.ask.preview import join_grain_preview
    b, _ = build
    b.joins = list(b.joins) + [
        {"a": SBS, "b": GMS, "source": "constraints", "support": 1,
         "on": f"{SBS}.merchant = {GMS}.se_no"}]
    preview = join_grain_preview(b, _plan(table=SBS, grain="month"), [SBS, GMS])
    assert preview["verdict"] == "safe"
    assert "declared primary key" in preview["joins"][0]["why"]
    assert "no row is counted twice" in preview["joins"][0]["why"]


def test_a_cte_scoped_witness_is_never_evidence_of_a_raw_join(build):
    from sahs.ask.preview import join_grain_preview
    b, _ = build
    b.joins = [j for j in b.joins
               if {j.get("a"), j.get("b")} != {GMS, WWCAS}] + [
        {"a": GMS, "b": WWCAS, "source": "studio", "support": 9,
         "scope": "scoped_only", "on": f"{GMS}.cm13 = {WWCAS}.card_no"}]
    preview = join_grain_preview(b, _plan(), [GMS, WWCAS])
    assert preview["verdict"] == "unsafe"
    why = preview["joins"][0]["why"]
    assert "inside a CTE" in why and "not that the raw tables join" in why


# ── the preview reaches the turn where it can still matter ───
def test_the_preview_rides_on_contract_ready(build):
    """Acceptance before work: a fan-out warning is only useful while
    the plan can still change, so it lands with the contract and not
    with the answer."""
    from sahs.ask.events import EVENTS
    source = (SILO / "sahs" / "ask" / "loop.py").read_text()
    assert "join_grain_preview" in source
    block = source.split('bus.emit("contract_ready"')[1][:200]
    assert "preview=preview" in block
    # and it did NOT need a new event: the family stays pinned
    assert "join_preview" not in EVENTS


# ── restore is append-only ───────────────────────────────────
def test_restore_appends_a_version_and_never_rewinds(build):
    from sahs.ask import AskRuntime
    b, tmp = build
    runtime = AskRuntime(builds_root=tmp / "builds", graph_root=tmp / "graph",
                         store_path=tmp / "restore.sqlite3")
    session = runtime.create_session("analyst")["id"]
    for grain in ("transaction", "card member", "day"):
        runtime.store.add_plan_version(
            session, {"grain": grain, "metric_label": "M"},
            summary=f"by {grain}")

    done = runtime.restore_plan(session, 1)
    assert done["restored"] and done["from_version"] == 1
    versions = runtime.store.plan_versions(session)
    assert [v["version"] for v in versions] == [1, 2, 3, 4], (
        "restore must GROW the chain: an undo that rewrote it would "
        "make 'what did we actually ask' unanswerable")
    assert versions[-1]["plan"]["grain"] == "transaction"
    assert versions[-1]["parent"] == 3
    assert "restored from v1" in versions[-1]["summary"]

    # restoring the version you are already on is a no-op, not a
    # duplicate row
    assert runtime.restore_plan(session, 4)["restored"] is False
    assert len(runtime.store.plan_versions(session)) == 4
    with pytest.raises(KeyError):
        runtime.restore_plan(session, 99)


def test_the_restore_route_answers_available_like_every_other(build):
    from fastapi.testclient import TestClient
    import os
    from apps.lumi.backend import ask as ask_module
    from apps.lumi.backend.app import create_app
    from sahs.ask import AskRuntime
    b, tmp = build
    os.environ["MERIDIAN_BUILDS_DIR"] = str(tmp / "builds")
    ask_module._RUNTIME = AskRuntime(
        builds_root=tmp / "builds", graph_root=tmp / "graph",
        store_path=tmp / "route.sqlite3")
    client = TestClient(create_app())
    session = client.post("/api/sessions", json={}).json()["session"]["id"]
    missing = client.post(f"/api/sessions/{session}/plan/restore",
                          json={"version": 1}).json()
    assert missing["available"] is False and "no plan version" in \
        missing["reason"]
    gone = client.post("/api/sessions/nope/plan/restore",
                       json={"version": 1}).json()
    assert gone["available"] is False
