"""Tier 3: symmetric_aggregates + aggregate_table + BQ probe tests."""

from __future__ import annotations

from unittest.mock import MagicMock

from lumi.bq_probe import _select_columns_to_probe, probe_distinct_values
from lumi.critic import _check_symmetric_aggregates
from lumi.explore_clusters import propose_aggregate_tables
from lumi.joins import infer_join_cardinalities
from lumi.schemas import EnrichmentPlan
from lumi.sql_to_context import parse_sqls


# ─── T3.1: symmetric_aggregates critic ─────────────────────


def test_blocks_when_many_to_many_join_and_measure_lacks_symmetric():
    plan = EnrichmentPlan(
        table_name="t1",
        proposed_dimensions=[],
        proposed_measures=[
            {"name": "total_amt", "source_column": "amt", "type": "sum"},
        ],
        proposed_explore={
            "base_view": "t1",
            "joins": [{
                "right_table": "bridge",
                "relationship": "many_to_many",
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_symmetric_aggregates(plan, cardinalities=[])
    blocks = [i for i in issues if i.severity == "block"]
    assert blocks
    assert blocks[0].category == "symmetric_aggregates"


def test_passes_when_symmetric_aggregates_set():
    plan = EnrichmentPlan(
        table_name="t1",
        proposed_dimensions=[],
        proposed_measures=[
            {"name": "total_amt", "source_column": "amt", "type": "sum",
             "symmetric_aggregates": "yes"},
        ],
        proposed_explore={
            "base_view": "t1",
            "joins": [{
                "right_table": "bridge",
                "relationship": "many_to_many",
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_symmetric_aggregates(plan, cardinalities=[])
    assert not issues


def test_no_issue_when_no_many_to_many_and_single_fact():
    plan = EnrichmentPlan(
        table_name="t1",
        proposed_dimensions=[],
        proposed_measures=[
            {"name": "total_amt", "source_column": "amt", "type": "sum"},
        ],
        proposed_explore={
            "base_view": "t1",
            "joins": [{"right_table": "dim_x", "relationship": "many_to_one"}],
        },
        reasoning="x" * 100,
    )
    issues = _check_symmetric_aggregates(plan, cardinalities=[])
    assert not issues


def test_blocks_on_multi_fact_join():
    """Multi-fact heuristic: 2+ tables in explore are facts → require symmetric."""
    fps = parse_sqls([
        # Fact A: transaction with cardmember dim
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        # Fact B: revenue with cardmember dim
        "SELECT cm.cm11, SUM(r.revenue) FROM cardmember cm "
        "JOIN revenue r ON cm.cm11 = r.cm_id GROUP BY cm.cm11",
    ] * 3)
    cardinalities = infer_join_cardinalities(fps)
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[],
        proposed_measures=[
            {"name": "total_amt", "source_column": "amt", "type": "sum"},
        ],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [
                {"right_table": "transaction", "relationship": "one_to_many"},
                {"right_table": "revenue", "relationship": "one_to_many"},
            ],
        },
        reasoning="x" * 100,
    )
    issues = _check_symmetric_aggregates(plan, cardinalities=cardinalities)
    assert any(i.severity == "block" for i in issues)


# ─── T3.2: aggregate_table proposals ───────────────────────


def test_aggregate_table_proposed_when_groupby_repeats():
    fps = parse_sqls([
        "SELECT cm11, SUM(amount) FROM transaction GROUP BY cm11",
    ] * 4)
    proposals = propose_aggregate_tables(fps, min_query_count=3)
    assert proposals
    p = proposals[0]
    assert p["base_view"] == "transaction"
    assert p["group_by"] == ["cm11"]
    assert "amount" in p["measures"]
    assert p["frequency"] == 4


def test_aggregate_table_skipped_below_threshold():
    fps = parse_sqls([
        "SELECT cm11, SUM(amount) FROM transaction GROUP BY cm11",
        "SELECT cm11, SUM(amount) FROM transaction GROUP BY cm11",
    ])
    proposals = propose_aggregate_tables(fps, min_query_count=3)
    assert proposals == []


def test_aggregate_table_distinguishes_group_by_shapes():
    fps = parse_sqls([
        "SELECT cm11, SUM(amount) FROM transaction GROUP BY cm11",
    ] * 3 + [
        "SELECT merchant_id, SUM(amount) FROM transaction GROUP BY merchant_id",
    ] * 3)
    proposals = propose_aggregate_tables(fps, min_query_count=3)
    # Two distinct GROUP BY shapes → two proposals
    shapes = {tuple(p["group_by"]) for p in proposals}
    assert ("cm11",) in shapes
    assert ("merchant_id",) in shapes


def test_aggregate_table_capped_at_max():
    fps = []
    for i in range(20):
        fps.extend(parse_sqls([
            f"SELECT col{i}, SUM(amount) FROM table_{i} GROUP BY col{i}"
        ] * 3))
    proposals = propose_aggregate_tables(fps, min_query_count=3, max_proposals=5)
    assert len(proposals) == 5


# ─── T3.3: BQ probe with mock client ───────────────────────


def test_select_columns_to_probe_skips_partitions_and_full_lists():
    catalog = {
        "t.country": {"type": "string", "partition": False, "values": [],
                      "synonyms": {"Country": "country"}, "namespace": "t",
                      "mandatory": False},
        "t.dt": {"type": "string", "partition": True, "values": [],
                 "synonyms": {}, "namespace": "t", "mandatory": False},
        "t.full": {"type": "string", "partition": False,
                   "values": [str(i) for i in range(15)],
                   "synonyms": {}, "namespace": "t", "mandatory": False},
        "t.amount": {"type": "number", "partition": False, "values": [],
                     "synonyms": {}, "namespace": "t", "mandatory": False},
    }
    selected = _select_columns_to_probe(catalog, max_columns=10)
    selected_keys = [k for k, _ in selected]
    assert "t.country" in selected_keys     # eligible
    assert "t.dt" not in selected_keys      # partition skipped
    assert "t.full" not in selected_keys    # already 15 values
    assert "t.amount" not in selected_keys  # not string


def test_probe_populates_values_with_mock_client():
    """probe_distinct_values fills `values` from a mocked BQ client."""
    catalog = {
        "finance_fact.bu": {
            "type": "string", "partition": False, "values": [],
            "synonyms": {"BU": "bu"}, "namespace": "finance_fact",
            "mandatory": False,
        },
    }

    # Mock BQ client: client.query(sql).result() yields row-like objects.
    class _Row:
        def __init__(self, value):
            self.v = value

        def __getitem__(self, idx):
            return self.v

    rows = [_Row("GCS"), _Row("GMNS"), _Row("SBS")]

    mock_query = MagicMock()
    mock_query.result.return_value = iter(rows)
    mock_client = MagicMock()
    mock_client.query.return_value = mock_query

    probe_distinct_values(catalog, bq_client=mock_client)
    assert "GCS" in catalog["finance_fact.bu"]["values"]
    assert "GMNS" in catalog["finance_fact.bu"]["values"]
    assert "SBS" in catalog["finance_fact.bu"]["values"]


def test_probe_no_op_when_disabled(monkeypatch):
    """Without LUMI_BQ_ENABLE and without an explicit client, probe is a no-op."""
    monkeypatch.delenv("LUMI_BQ_ENABLE", raising=False)
    catalog = {
        "t.col": {"type": "string", "partition": False, "values": [],
                  "synonyms": {}, "namespace": "t", "mandatory": False},
    }
    result = probe_distinct_values(catalog)
    assert result["t.col"]["values"] == []


def test_probe_swallows_query_errors():
    catalog = {
        "t.col": {"type": "string", "partition": False, "values": [],
                  "synonyms": {"alt": "col"}, "namespace": "t",
                  "mandatory": False},
    }
    failing = MagicMock()
    failing.query.side_effect = RuntimeError("permission denied")
    # Should not raise.
    probe_distinct_values(catalog, bq_client=failing)
    assert catalog["t.col"]["values"] == []
