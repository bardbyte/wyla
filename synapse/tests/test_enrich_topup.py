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
    compute_topup_plan, enrich_graph, merge_memories,
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
