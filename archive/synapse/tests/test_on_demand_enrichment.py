"""On-demand column enrichment (S4) — the lazy-loading layer.

The flywheel, but safe: a cache read-through, a gated one-column LLM fill
at capped provenance, honest abstention when the gate holds, and a durable
overlay that doubles as the steward-proposal record.
"""

from __future__ import annotations

import json
from pathlib import Path

from synapse.enrichment.on_demand import OverlayStore, explain_column
from synapse.enrichment.schemas import (
    ColumnObservation, EnrichmentBundle, SelfAssessment)
from synapse.graph.store import GraphStore, canonical_uri


def _sa() -> SelfAssessment:
    return SelfAssessment(
        tables_skipped_for_lack_of_signal=[], columns_marked_ambiguous=0,
        proposed_entities_with_low_evidence=[], requires_steward_attention=[])


class _FillClient:
    """Returns one observation for the requested column, with configurable
    evidence/confidence so the grounding gate can be exercised."""

    def __init__(self, *, evidence=("bq",), confidence=0.8,
                 desc="the adjusted risk score, seasonally weighted"):
        self.evidence = list(evidence)
        self.confidence = confidence
        self.desc = desc
        self.calls = 0

    def enrich(self, *, skill_md, context, table_name) -> EnrichmentBundle:
        self.calls += 1
        name = context["inspection"]["columns"][0]["name"]
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[ColumnObservation(
                column_name=name, proposed_description=self.desc,
                candidate_role="attribute", self_confidence=self.confidence,
                evidence_used=self.evidence)],
            self_assessment=_sa())


def _store(*, with_desc=False) -> GraphStore:
    s = GraphStore()
    t = canonical_uri("table", "risk_pers_acct")
    s.upsert_node("Table", t, {"table_name": "risk_pers_acct",
                               "description": "Account-level risk."},
                  source="mdm")
    props = {"table_name": "risk_pers_acct", "data_type": "FLOAT64",
             "min_value": "-500.0", "max_value": "980.5", "null_fraction": 0.02}
    if with_desc:
        props["description"] = "MDM-described risk score"
    c = canonical_uri("column", "risk_pers_acct", "risk_score_adj")
    s.upsert_node("Column", c, props, source="mdm")
    s.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return s


# ─── read-through ───────────────────────────────────────────────


def test_cache_hit_returns_without_calling_the_model():
    store = _store(with_desc=True)
    client = _FillClient()
    res = explain_column(store, "risk_pers_acct", "risk_score_adj",
                         llm_client=client)
    assert res["status"] == "ok" and res["cached"] is True
    assert res["description"] == "MDM-described risk score"
    assert client.calls == 0                       # never touched the model


def test_no_client_returns_grounded_profile():
    store = _store()
    res = explain_column(store, "risk_pers_acct", "risk_score_adj")
    assert res["status"] == "partial" and res["cached"] is False
    assert res["grounded_facts"]["data_type"] == "FLOAT64"
    assert res["grounded_facts"]["max_value"] == "980.5"
    assert "no grounded meaning yet" in res["note"]


def test_missing_column_is_a_structured_error():
    res = explain_column(_store(), "risk_pers_acct", "nope")
    assert res["status"] == "error" and "not in graph" in res["reason"]


# ─── the gated fill ─────────────────────────────────────────────


def test_fill_writes_description_and_records_overlay(tmp_path: Path):
    store = _store()
    overlay = OverlayStore(tmp_path / "overlay.json")
    client = _FillClient(evidence=("bq",), confidence=0.8)
    res = explain_column(store, "risk_pers_acct", "risk_score_adj",
                         llm_client=client, overlay=overlay)
    assert res["status"] == "ok" and res["cached"] is False
    assert "seasonally weighted" in res["description"]
    # persisted to the live graph at the capped llm_generated provenance
    node = store.get(canonical_uri("column", "risk_pers_acct",
                                   "risk_score_adj"))
    assert "seasonally weighted" in node.properties["ai_generated_description"]
    assert "llm_generated" in node.provenance.sources
    assert node.provenance.confidence_tier in ("inferred", "guessed")
    # recorded as a pending steward proposal
    proposals = overlay.proposals(pending_only=True)
    assert len(proposals) == 1
    assert proposals[0]["column"] == "risk_score_adj"
    assert proposals[0]["reviewed"] is False


def test_gate_holds_evidence_poor_fill(tmp_path: Path):
    store = _store()
    overlay = OverlayStore(tmp_path / "overlay.json")
    # no evidence → the grounding gate must HOLD the description
    client = _FillClient(evidence=(), confidence=0.9)
    res = explain_column(store, "risk_pers_acct", "risk_score_adj",
                         llm_client=client, overlay=overlay)
    assert res["status"] == "partial" and res["description"] == ""
    assert "held by the grounding gate" in res["note"]
    node = store.get(canonical_uri("column", "risk_pers_acct",
                                   "risk_score_adj"))
    assert not node.properties.get("ai_generated_description")
    assert overlay.proposals() == []               # nothing invented, nothing stored


# ─── the durable overlay ────────────────────────────────────────


def test_overlay_persists_and_replays_onto_a_fresh_store(tmp_path: Path):
    path = tmp_path / "overlay.json"
    overlay = OverlayStore(path)
    overlay.record(table="risk_pers_acct", column="risk_score_adj",
                   description="seasonally weighted score", tier="inferred",
                   evidence=["bq"])
    # a fill for a column absent from the target store is skipped, not error
    overlay.record(table="ghost", column="x", description="d",
                   tier="inferred", evidence=[])

    # reload from disk (survives a restart) and replay
    reloaded = OverlayStore(path)
    fresh = _store()
    applied = reloaded.apply(fresh)
    assert applied == 1
    node = fresh.get(canonical_uri("column", "risk_pers_acct",
                                   "risk_score_adj"))
    assert node.properties["ai_generated_description"] == \
        "seasonally weighted score"
    assert "llm_generated" in node.provenance.sources
    # the canonical build never wrote these — proof it's a separable layer
    assert json.loads(path.read_text())["fills"][0]["reviewed"] is False
