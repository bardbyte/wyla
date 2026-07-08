"""S3 — the prioritized batch enricher, end to end.

Two contracts:
  1. Only a human analyst's query votes on salience — an operational
     (service-account) query does not create corpus salience signals.
  2. Prioritized selection → the enricher's skip_columns complement means
     only the high-value head gets an LLM call; the tail is left grounded.
"""

from __future__ import annotations

import json
from pathlib import Path

from synapse.enrichment.enricher import enrich_graph
from synapse.enrichment.prioritize import (
    prioritize_columns, select_for_enrichment)
from synapse.enrichment.schemas import (
    ColumnObservation, EnrichmentBundle, SelfAssessment)
from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import GraphStore, canonical_uri


# ─── 1. operational queries don't vote on salience ──────────────


def test_operational_queries_do_not_create_salience(tmp_path: Path):
    sources = tmp_path / "sources"
    mdm = sources / "mdm_cache"
    mdm.mkdir(parents=True)
    (mdm / "sbs_new_accounts.json").write_text(json.dumps({
        "table_name": "sbs_new_accounts",
        "columns": [{"name": "decision_cd", "type": "STRING"},
                    {"name": "region_cd", "type": "STRING"}]}))
    gold = sources / "gold_queries"
    gold.mkdir()
    # a human analyst filters on decision_cd
    (gold / "Q__analyst.sql").write_text(
        "-- user: jane.smith@axp.com\n"
        "SELECT COUNT(*) FROM `p.d.sbs_new_accounts` WHERE decision_cd = 'A'\n",
        encoding="utf-8")
    # a service account filters on region_cd (a load/plumbing query)
    (gold / "Q__svc.sql").write_text(
        "-- user: svc-etl@prj.iam.gserviceaccount.com\n"
        "SELECT * FROM `p.d.sbs_new_accounts` WHERE region_cd = 'US'\n",
        encoding="utf-8")

    store = build_graph_from_sources(sources)
    # the analyst's filter value landed…
    assert store.get(canonical_uri(
        "filtervalue", "sbs_new_accounts", "decision_cd", "A")) is not None
    # …the service account's did NOT (excluded from salience)
    assert store.get(canonical_uri(
        "filtervalue", "sbs_new_accounts", "region_cd", "US")) is None


def test_curated_query_without_user_is_kept(tmp_path: Path):
    """Lumi/manual gold has no `-- user:` line — trusted as analyst."""
    sources = tmp_path / "sources"
    (sources / "mdm_cache").mkdir(parents=True)
    (sources / "mdm_cache" / "t.json").write_text(json.dumps({
        "table_name": "t", "columns": [{"name": "seg_cd", "type": "STRING"}]}))
    gold = sources / "gold_queries"
    gold.mkdir()
    (gold / "Q__curated.sql").write_text(
        "SELECT * FROM `p.d.t` WHERE seg_cd = 'X'\n", encoding="utf-8")
    store = build_graph_from_sources(sources)
    assert store.get(canonical_uri(
        "filtervalue", "t", "seg_cd", "X")) is not None


# ─── 2. prioritized selection → enricher sends only the head ────


class _Recorder:
    def __init__(self) -> None:
        self.seen: dict[str, list[str]] = {}

    def enrich(self, *, skill_md, context, table_name) -> EnrichmentBundle:
        cols = [c["name"] for c in context["inspection"]["columns"]]
        self.seen.setdefault(table_name, []).extend(cols)
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[ColumnObservation(
                column_name=c, proposed_description=f"about {c}",
                candidate_role="attribute", self_confidence=0.8,
                evidence_used=["mdm"]) for c in cols],
            self_assessment=SelfAssessment(
                tables_skipped_for_lack_of_signal=[],
                columns_marked_ambiguous=0,
                proposed_entities_with_low_evidence=[],
                requires_steward_attention=[]))


def _store_with_head_and_tail() -> GraphStore:
    store = GraphStore()
    t = canonical_uri("table", "risk_pers_acct")
    store.upsert_node("Table", t, {"table_name": "risk_pers_acct",
                                   "row_count": 1_000_000}, source="mdm")
    cols = {
        # identifier (declared) — head
        "acct_id": {"data_type": "STRING", "is_primary": True,
                    "approx_distinct": 990_000, "null_fraction": 0.0},
        # coded — head
        "status_cd": {"data_type": "STRING", "cardinality_bucket": "low",
                      "distinct_sample": [{"value": "A"}, {"value": "C"}]},
        # bare numeric feature — tail
        "score_feat_9": {"data_type": "FLOAT64", "approx_distinct": 700_000,
                         "null_fraction": 0.4},
    }
    for name, props in cols.items():
        c = canonical_uri("column", "risk_pers_acct", name)
        store.upsert_node("Column", c, {"table_name": "risk_pers_acct",
                                        **props}, source="mdm")
        store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return store


def test_prioritized_enrichment_sends_only_the_head():
    store = _store_with_head_and_tail()
    ranked = prioritize_columns(store, "risk_pers_acct")
    keep = {c.lower() for c in select_for_enrichment(ranked)}
    all_cols = {p.column.lower() for p in ranked}
    skip = {"risk_pers_acct": all_cols - keep}

    rec = _Recorder()
    enrich_graph(store, rec, only_tables=["risk_pers_acct"], skip_columns=skip)
    sent = rec.seen.get("risk_pers_acct", [])
    assert "acct_id" in sent and "status_cd" in sent   # the head
    assert "score_feat_9" not in sent                  # the grounded tail
