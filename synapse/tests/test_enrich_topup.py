"""Top-up enrichment — finish what the budget cut, touch nothing else.

Pins: the plan finds exactly the unobserved remainder; skip_columns
sends only those columns (fully-covered tables cost zero calls); the
memory merge preserves every prior observation; the CLI plan mode runs
offline with zero client construction.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from synapse.enrichment.enricher import (
    compute_topup_plan, enrich_graph, merge_memories, plan_enrichment,
)
from synapse.enrichment.schemas import (
    ColumnObservation, EnrichmentBundle, SelfAssessment,
)
from synapse.graph.entities import load_bundles_from_memory
from synapse.graph.store import GraphStore, canonical_uri

REPO_ROOT = Path(__file__).resolve().parents[2]


def _sa() -> SelfAssessment:
    return SelfAssessment(
        tables_skipped_for_lack_of_signal=[], columns_marked_ambiguous=0,
        proposed_entities_with_low_evidence=[], requires_steward_attention=[])


def _obs(col: str) -> ColumnObservation:
    return ColumnObservation(
        column_name=col, proposed_description=f"about {col}",
        candidate_role="attribute", self_confidence=0.8,
        evidence_used=["mdm"])


def _store() -> GraphStore:
    """covered(2 cols, fully observed) · partial(4 cols, 2 observed) ·
    untouched(3 cols, never enriched)."""
    store = GraphStore()
    for table, cols in [("covered", ["a", "b"]),
                        ("partial", ["p1", "p2", "p3", "p4"]),
                        ("untouched", ["x", "y", "z"])]:
        t_uri = canonical_uri("table", table)
        store.upsert_node("Table", t_uri, {"table_name": table}, source="mdm")
        for col in cols:
            c_uri = canonical_uri("column", table, col)
            store.upsert_node("Column", c_uri, {"table_name": table},
                              source="mdm")
            store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    return store


def _old_bundles() -> dict[str, EnrichmentBundle]:
    return {
        "covered": EnrichmentBundle(
            table_name="covered",
            table_description_proposal="the original description",
            column_observations=[_obs("a"), _obs("b")],
            self_assessment=_sa()),
        "partial": EnrichmentBundle(
            table_name="partial",
            column_observations=[_obs("p1"), _obs("p2")],
            self_assessment=_sa()),
    }


class RecordingClient:
    def __init__(self) -> None:
        self.seen: dict[str, list[str]] = {}

    def enrich(self, *, skill_md, context, table_name) -> EnrichmentBundle:
        cols = [c["name"] for c in context["inspection"]["columns"]]
        self.seen.setdefault(table_name, []).extend(cols)
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[_obs(c) for c in cols],
            self_assessment=_sa())


def test_plan_finds_exactly_the_unobserved_remainder():
    plan = compute_topup_plan(_store(), _old_bundles())
    assert set(plan) == {"partial", "untouched"}      # covered absent
    assert plan["partial"] == ["p3", "p4"]
    assert plan["untouched"] == ["x", "y", "z"]


# ─── enrichment horizon: the pre-run plan + ETA denominator ──────


def test_plan_enrichment_counts_calls_before_any_llm_call():
    # 3 tables, 9 columns; a wide batch → one call per table
    plan = plan_enrichment(_store(), column_batch_size=40)
    assert plan["total_columns"] == 9
    assert plan["total_calls"] == 3
    assert plan["budget_capped_calls"] == 3
    assert plan["per_table"]["partial"] == {"columns": 4, "calls": 1}


def test_plan_enrichment_chunks_wide_tables_by_batch():
    # batch 2 splits partial's 4 columns into 2 calls; covered 1, untouched 2
    plan = plan_enrichment(_store(), column_batch_size=2)
    assert plan["per_table"]["partial"]["calls"] == 2
    assert plan["per_table"]["untouched"]["calls"] == 2
    assert plan["total_calls"] == 1 + 2 + 2


def test_plan_enrichment_scopes_to_only_tables():
    plan = plan_enrichment(_store(), only_tables=["partial"],
                           column_batch_size=40)
    assert set(plan["per_table"]) == {"partial"}
    assert plan["total_columns"] == 4


def test_plan_enrichment_caps_at_budget_but_keeps_true_total():
    plan = plan_enrichment(_store(), column_batch_size=2, max_calls=3)
    assert plan["total_calls"] == 5            # the honest size
    assert plan["budget_capped_calls"] == 3    # what will actually be spent


def test_plan_enrichment_drops_fully_skipped_tables():
    # the top-up's skip set: covered is fully observed → absent, zero calls
    skip = {"covered": {"a", "b"}}
    plan = plan_enrichment(_store(), column_batch_size=40, skip_columns=skip)
    assert "covered" not in plan["per_table"]
    assert plan["total_calls"] == 2            # partial + untouched only


def test_topup_enriches_only_remaining_columns():
    store, old = _store(), _old_bundles()
    plan = compute_topup_plan(store, old)
    client = RecordingClient()
    skip = {t.lower(): {o.column_name for o in b.column_observations}
            for t, b in old.items()}
    new = enrich_graph(store, client, only_tables=sorted(plan),
                       skip_columns=skip)
    assert set(client.seen) == {"partial", "untouched"}
    assert sorted(client.seen["partial"]) == ["p3", "p4"]   # remainder only
    assert sorted(client.seen["untouched"]) == ["x", "y", "z"]
    # new facts landed for the remainder…
    node = store.get(canonical_uri("column", "partial", "p3"))
    assert node.properties["ai_generated_description"] == "about p3"
    # …and the fully-covered table cost zero calls
    assert "covered" not in new


def test_fully_covered_table_is_skipped_even_when_requested():
    store, old = _store(), _old_bundles()
    client = RecordingClient()
    skip = {t.lower(): {o.column_name for o in b.column_observations}
            for t, b in old.items()}
    bundles = enrich_graph(store, client, only_tables=["covered"],
                           skip_columns=skip)
    assert bundles == {}
    assert client.seen == {}                          # zero calls


def test_merge_memories_preserves_prior_observations():
    old = _old_bundles()
    new = {
        "partial": EnrichmentBundle(
            table_name="partial",
            table_description_proposal="a re-proposed description",
            column_observations=[_obs("p3"), _obs("p4")],
            self_assessment=_sa()),
        "untouched": EnrichmentBundle(
            table_name="untouched",
            column_observations=[_obs("x")],
            self_assessment=_sa()),
    }
    merged = merge_memories(old, new)
    assert set(merged) == {"covered", "partial", "untouched"}
    assert {o.column_name for o in merged["partial"].column_observations} \
        == {"p1", "p2", "p3", "p4"}                  # union, nothing lost
    assert merged["covered"].table_description_proposal \
        == "the original description"                # untouched carry-over
    # old-first merge: an existing description outranks a re-proposal
    assert merged["partial"].table_description_proposal is None or \
        merged["partial"].table_description_proposal == \
        "a re-proposed description"


def test_memory_roundtrip_through_entities_loader(tmp_path):
    old = _old_bundles()
    path = tmp_path / "enrichment_memory.json"
    path.write_text(json.dumps(
        {t: b.model_dump() for t, b in old.items()}, default=str),
        encoding="utf-8")
    loaded = load_bundles_from_memory(path)
    assert compute_topup_plan(_store(), loaded)["partial"] == ["p3", "p4"]


def test_cli_plan_mode_is_offline_and_correct(tmp_path):
    snap = tmp_path / "graph_snapshot.json"
    _store().save_json(snap)
    mem = tmp_path / "enrichment_memory.json"
    mem.write_text(json.dumps(
        {t: b.model_dump() for t, b in _old_bundles().items()}, default=str),
        encoding="utf-8")
    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "enrich_topup.py"),
         "--plan", "--all-tables", "--snapshot", str(snap),
         "--memory", str(mem)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "partial" in proc.stdout and "untouched" in proc.stdout
    assert "covered" not in proc.stdout               # fully observed
    assert "2 column(s) remaining" in proc.stdout
    assert "never enriched" in proc.stdout


def test_cli_plan_nothing_to_do(tmp_path):
    store = GraphStore()
    t = canonical_uri("table", "t")
    store.upsert_node("Table", t, {"table_name": "t"}, source="mdm")
    c = canonical_uri("column", "t", "only")
    store.upsert_node("Column", c, {"table_name": "t"}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    snap = tmp_path / "s.json"
    store.save_json(snap)
    mem = tmp_path / "m.json"
    mem.write_text(json.dumps({"t": EnrichmentBundle(
        table_name="t", column_observations=[_obs("only")],
        self_assessment=_sa()).model_dump()}, default=str), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "enrich_topup.py"),
         "--plan", "--all-tables", "--snapshot", str(snap),
         "--memory", str(mem)],
        capture_output=True, text=True)
    assert proc.returncode == 0
    assert "nothing to do" in proc.stdout


class CrashingClient:
    """Succeeds on the first table, dies on the second — simulates a
    mid-run interrupt so the checkpoint property is testable."""

    def __init__(self) -> None:
        self.calls = 0

    def enrich(self, *, skill_md, context, table_name) -> EnrichmentBundle:
        self.calls += 1
        if self.calls > 1:
            raise RuntimeError("simulated crash")
        cols = [c["name"] for c in context["inspection"]["columns"]]
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[_obs(c) for c in cols],
            self_assessment=_sa())


def test_memory_checkpoints_after_every_table(tmp_path):
    """A run that dies on table 2 must leave table 1's observations on
    disk — hours of calls survive an interrupt."""
    import pytest

    store = _store()
    out = tmp_path / "partial.json"
    with pytest.raises(RuntimeError, match="simulated crash"):
        enrich_graph(store, CrashingClient(),
                     only_tables=["covered", "partial"], memory_out=out)
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert list(saved) == ["covered"]          # table 1 checkpointed
    assert len(saved["covered"]["column_observations"]) == 2


def test_verbose_progress_prints_tables_and_calls(capsys):
    store = _store()
    enrich_graph(store, RecordingClient(), only_tables=["covered"],
                 verbose=True, planned_calls=1)
    out = capsys.readouterr().out
    assert "enriching 1 table(s)" in out
    assert "▶ [1/1] covered: 2 column(s) in 1 call(s)" in out
    assert "[1/~1] chunk 1/1" in out and "2 obs" in out
    assert "✓ covered:" in out                 # gate one-liner


def test_verbose_eta_self_computes_when_plan_not_supplied(capsys):
    """A verbose caller that omits planned_calls still gets the horizon —
    enrich_graph derives it from the same selection the loop walks, so the
    per-chunk counter keeps its denominator (the pipeline relied on this)."""
    store = _store()
    enrich_graph(store, RecordingClient(), only_tables=["untouched"],
                 verbose=True)                 # no planned_calls
    out = capsys.readouterr().out
    assert "planned ~1 call(s)" in out         # header shows the horizon
    assert "[1/~1] chunk 1/1" in out           # denominator, not a bare [1]


def test_cli_resumes_interrupted_run_without_respending_calls(tmp_path):
    """A partial checkpoint covering the whole remainder → the resumed
    run folds it into memory + snapshot and exits with nothing to do,
    constructing no Vertex client at all."""
    snap = tmp_path / "graph_snapshot.json"
    _store().save_json(snap)
    mem = tmp_path / "enrichment_memory.json"
    mem.write_text(json.dumps(
        {t: b.model_dump() for t, b in _old_bundles().items()}, default=str),
        encoding="utf-8")
    partial = {
        "partial": EnrichmentBundle(
            table_name="partial",
            column_observations=[_obs("p3"), _obs("p4")],
            self_assessment=_sa()),
        "untouched": EnrichmentBundle(
            table_name="untouched",
            column_observations=[_obs("x"), _obs("y"), _obs("z")],
            self_assessment=_sa()),
    }
    partial_path = snap.with_name("_topup_memory_partial.json")
    partial_path.write_text(json.dumps(
        {t: b.model_dump() for t, b in partial.items()}, default=str),
        encoding="utf-8")

    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "enrich_topup.py"),
         "--all-tables", "--snapshot", str(snap), "--memory", str(mem)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "resumed interrupted run: 2 table(s)" in proc.stdout
    assert "nothing to do" in proc.stdout
    assert not partial_path.exists()           # checkpoint consumed
    # observations landed durably: memory has the union…
    merged = json.loads(mem.read_text(encoding="utf-8"))
    assert {o["column_name"] for o in
            merged["partial"]["column_observations"]} \
        == {"p1", "p2", "p3", "p4"}
    # …and the snapshot got the facts re-applied
    loaded = GraphStore.load_json(snap)
    node = loaded.get(canonical_uri("column", "untouched", "x"))
    assert node.properties["ai_generated_description"] == "about x"


def test_cli_plan_scopes_to_manifest_by_default(tmp_path):
    """The graph carries out-of-scope tables (staged by earlier runs);
    the plan must spend calls on manifest tables only."""
    snap = tmp_path / "graph_snapshot.json"
    _store().save_json(snap)                 # covered, partial, untouched
    mem = tmp_path / "enrichment_memory.json"
    mem.write_text(json.dumps(
        {t: b.model_dump() for t, b in _old_bundles().items()}, default=str),
        encoding="utf-8")
    manifest = tmp_path / "tables.yaml"
    manifest.write_text("tables:\n  - name: partial\n", encoding="utf-8")

    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "enrich_topup.py"),
         "--plan", "--snapshot", str(snap), "--memory", str(mem),
         "--manifest", str(manifest)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "manifest scope" in proc.stdout
    assert "1 out-of-scope skipped" in proc.stdout    # untouched excluded
    assert "partial" in proc.stdout
    assert "untouched" not in proc.stdout
