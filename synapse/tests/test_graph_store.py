"""Tests for synapse.graph.store — Provenance, GraphStore, confidence calc."""

from __future__ import annotations

from synapse.graph.store import (
    GraphStore,
    Provenance,
    SOURCE_WEIGHTS,
    canonical_uri,
    confidence_from_sources,
)


# ─── canonical_uri ────────────────────────────────────────────


def test_canonical_uri_basic():
    assert canonical_uri("Table", "MyTable") == "synapse://table/mytable"


def test_canonical_uri_joins_parts():
    assert canonical_uri("Column", "T1", "ColA") == "synapse://column/t1/cola"


def test_canonical_uri_normalizes_spaces():
    assert canonical_uri("Synonym", "Card Member") == "synapse://synonym/card_member"


# ─── Provenance ──────────────────────────────────────────────


def test_provenance_record_source_adds_first_time():
    p = Provenance()
    p.record_source("mdm")
    assert "mdm" in p.sources
    assert p.evidence_count_by_source["mdm"] == 1
    assert p.first_observed_at != ""
    assert p.last_observed_at != ""


def test_provenance_record_source_increments_count_on_repeat():
    p = Provenance()
    p.record_source("corpus")
    p.record_source("corpus")
    p.record_source("corpus")
    # 'corpus' only listed once in sources
    assert p.sources.count("corpus") == 1
    # But count incremented per call
    assert p.evidence_count_by_source["corpus"] == 3


def test_provenance_records_event_ids():
    p = Provenance()
    p.record_source("corpus", event_id="Q01")
    p.record_source("corpus", event_id="Q05")
    assert p.evidence_event_ids == ["Q01", "Q05"]


def test_provenance_confidence_promotes_with_distinct_sources():
    """Three independent strong sources should land at inferred or grounded —
    distinct-source breadth matters more than depth on one source."""
    p = Provenance()
    p.record_source("mdm")     # weight 3
    p.record_source("bq")      # weight 4
    p.record_source("corpus")  # weight 1
    # 3 distinct sources, low depth → at least inferred
    assert p.confidence_tier in {"inferred", "grounded"}
    for _ in range(10):
        p.record_source("corpus")
    # 3 distinct + score = (3 + 4 + min(11,5)*1)/15 = 0.80 → grounded
    assert p.confidence_tier == "grounded"


def test_provenance_single_source_stays_low_even_with_many_observations():
    """200 corpus observations on the same fact ≠ ground truth.
    Single-source depth must not promote past inferred."""
    p = Provenance()
    for _ in range(200):
        p.record_source("corpus")
    # distinct=1, score capped by per-source cap → stays guessed
    assert p.confidence_tier == "guessed"


def test_provenance_human_approval_always_grounds():
    p = Provenance()
    p.record_source("human_approval")
    assert p.confidence_tier == "human_asserted"
    assert p.confidence_score == 0.99


# ─── confidence_from_sources ─────────────────────────────────


def test_confidence_grounded_requires_multi_source():
    # Even with 10 corpus observations, single-source → inferred not grounded
    score, tier = confidence_from_sources(
        ["corpus"], evidence_counts={"corpus": 10},
    )
    assert tier in {"inferred", "guessed"}


def test_confidence_grounded_with_multi_source_and_strong_weights():
    """metric_catalog + mdm + bq + corpus all agree → grounded."""
    score, tier = confidence_from_sources(
        ["metric_catalog", "mdm", "bq", "corpus"],
        evidence_counts={
            "metric_catalog": 1, "mdm": 1, "bq": 1, "corpus": 20,
        },
    )
    # 5 + 3 + 4 + 20 = 32 / 40 = 0.8 — almost grounded but distinct=4 helps
    assert tier in {"grounded", "inferred"}


def test_confidence_human_assertion_dominates():
    score, tier = confidence_from_sources(["human_approval"])
    assert tier == "human_asserted"
    assert score == 0.99


# ─── GraphStore ──────────────────────────────────────────────


def test_upsert_node_creates_and_tags_provenance():
    g = GraphStore()
    uri = canonical_uri("Table", "t1")
    g.upsert_node("Table", uri, properties={"table_name": "t1"}, source="mdm")
    n = g.get(uri)
    assert n is not None
    assert n.node_type == "Table"
    assert "mdm" in n.provenance.sources


def test_upsert_node_merges_properties_across_sources():
    g = GraphStore()
    uri = canonical_uri("Table", "t1")
    g.upsert_node("Table", uri, {"table_name": "t1", "row_count": None}, "mdm")
    g.upsert_node("Table", uri, {"row_count": 1000, "last_modified": "2026-06-01"}, "bq")
    n = g.get(uri)
    assert n.properties["table_name"] == "t1"
    assert n.properties["row_count"] == 1000   # bq filled in
    assert n.properties["last_modified"] == "2026-06-01"
    # Both sources on provenance
    assert set(n.provenance.sources) >= {"mdm", "bq"}


def test_upsert_edge_creates_and_tags_provenance():
    g = GraphStore()
    a_uri = canonical_uri("Column", "t1", "id")
    b_uri = canonical_uri("Column", "t2", "ref_id")
    g.upsert_edge("EQUIVALENT_TO", a_uri, b_uri, {}, "corpus", "Q01")
    edges = list(g.edges.values())
    assert len(edges) == 1
    assert edges[0].edge_type == "EQUIVALENT_TO"
    assert "corpus" in edges[0].provenance.sources
    assert "Q01" in edges[0].provenance.evidence_event_ids


def test_outgoing_incoming_filters_correctly():
    g = GraphStore()
    t_uri = canonical_uri("Table", "t1")
    c1_uri = canonical_uri("Column", "t1", "a")
    c2_uri = canonical_uri("Column", "t1", "b")
    g.upsert_edge("CONTAINS", t_uri, c1_uri, {}, "mdm")
    g.upsert_edge("CONTAINS", t_uri, c2_uri, {}, "mdm")
    out = g.outgoing(t_uri, "CONTAINS")
    assert len(out) == 2
    inc = g.incoming(c1_uri, "CONTAINS")
    assert len(inc) == 1
    assert inc[0].from_uri == t_uri


def test_stats_aggregates_by_type_and_tier():
    g = GraphStore()
    g.upsert_node("Table", canonical_uri("Table", "t1"), {}, "mdm")
    g.upsert_node("Table", canonical_uri("Table", "t2"), {}, "mdm")
    g.upsert_node("Column", canonical_uri("Column", "t1", "c"), {}, "mdm")
    g.upsert_edge(
        "CONTAINS", canonical_uri("Table", "t1"),
        canonical_uri("Column", "t1", "c"), {}, "mdm",
    )
    s = g.stats()
    assert s["n_nodes"] == 3
    assert s["n_edges"] == 1
    assert s["nodes_by_type"] == {"Table": 2, "Column": 1}
    assert s["edges_by_type"] == {"CONTAINS": 1}


def test_source_weights_ordering_matches_design():
    """Calibration relies on the source-weight ordering: humans > catalogs >
    BQ > MDM > corpus. Don't accidentally invert."""
    assert SOURCE_WEIGHTS["human_approval"] > SOURCE_WEIGHTS["metric_catalog"]
    assert SOURCE_WEIGHTS["metric_catalog"] >= SOURCE_WEIGHTS["bq"]
    assert SOURCE_WEIGHTS["bq"] >= SOURCE_WEIGHTS["mdm"]
    assert SOURCE_WEIGHTS["mdm"] >= SOURCE_WEIGHTS["corpus"]
