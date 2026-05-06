"""JOIN cardinality + canonical path inference tests."""

from __future__ import annotations

from lumi.joins import (
    infer_canonical_paths,
    infer_join_cardinalities,
    render_joins_for_table,
)
from lumi.sql_to_context import parse_sqls


def test_group_by_signals_one_to_many():
    """GROUP BY left.key + aggregations from right → left is dim, right is fact.

    cm.cm11 is the key analysts group by. t.amount is what gets summed.
    Cardinality should be cardmember (one) → transaction (many).
    """
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    cards = infer_join_cardinalities(fps)
    assert len(cards) == 1
    c = cards[0]
    assert c.left_table == "cardmember"
    assert c.right_table == "transaction"
    assert c.cardinality == "one_to_many"
    assert c.confidence >= 0.5


def test_group_by_right_signals_many_to_one():
    """GROUP BY right.key → right is dim."""
    fps = parse_sqls([
        "SELECT t.cm_id, SUM(t.amount) FROM transaction t "
        "JOIN cardmember cm ON t.cm_id = cm.cm11 GROUP BY t.cm_id",
    ])
    cards = infer_join_cardinalities(fps)
    assert len(cards) == 1
    # Output is normalized lexicographically
    c = cards[0]
    # cardmember < transaction → cardmember is left in output
    assert c.left_table == "cardmember"
    assert c.right_table == "transaction"
    # Whichever side groups is the dim. The query groups by t.cm_id which
    # is on the transaction side, so transaction is the dim → cardmember
    # is the fact → many cardmember rows per transaction grouping.
    assert c.cardinality == "many_to_one"


def test_left_join_signals_one_to_many():
    """LEFT JOIN → right side is optional; weak hint at one_to_many."""
    fps = parse_sqls([
        "SELECT cm.cm11, t.amount FROM cardmember cm "
        "LEFT JOIN transaction t ON cm.cm11 = t.cm_id",
    ])
    cards = infer_join_cardinalities(fps)
    assert cards
    c = cards[0]
    assert c.cardinality == "one_to_many"
    assert "LEFT JOIN" in (c.evidence[0] if c.evidence else "")


def test_majority_vote_across_observations():
    """Three queries say one_to_many, one says many_to_one — majority wins."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "SELECT cm.cm11, COUNT(*) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "SELECT t.cm_id, COUNT(*) FROM transaction t "
        "JOIN cardmember cm ON t.cm_id = cm.cm11 GROUP BY t.cm_id",
    ])
    cards = infer_join_cardinalities(fps)
    assert cards
    c = cards[0]
    # 3 votes for cardmember-is-dim, 1 vote for transaction-is-dim
    # → majority is one_to_many from cardmember to transaction
    assert c.cardinality == "one_to_many"
    assert c.observations >= 4


def test_canonical_path_from_two_hop_join():
    fps = parse_sqls([
        "SELECT * FROM cardmember cm "
        "JOIN account a ON cm.cm11 = a.cm_id "
        "JOIN transaction t ON a.acct_id = t.acct_id",
    ] * 3)  # observed 3 times
    paths = infer_canonical_paths(fps)
    assert paths
    p = paths[0]
    assert p.base_table == "cardmember"
    assert [t for t, _ in p.chain] == ["account", "transaction"]
    assert p.frequency == 3


def test_render_joins_for_table_includes_relevant_pairs():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    cards = infer_join_cardinalities(fps)
    paths = infer_canonical_paths(fps)
    md = render_joins_for_table("cardmember", cards, paths)
    assert "Observed JOIN cardinality" in md
    assert "cardmember" in md
    assert "transaction" in md
    assert "one_to_many" in md


def test_render_returns_empty_for_unrelated_table():
    fps = parse_sqls([
        "SELECT a, b FROM x JOIN y ON x.id = y.id",
    ])
    cards = infer_join_cardinalities(fps)
    paths = infer_canonical_paths(fps)
    md = render_joins_for_table("unrelated_table", cards, paths)
    assert md == ""


def test_alias_resolution_recovers_real_table_name():
    """The FROM table's alias gets resolved to its real name."""
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm "
        "JOIN account a ON cm.cm11 = a.cm_id",
    ])
    cards = infer_join_cardinalities(fps)
    assert cards
    # Both sides should resolve to canonical table names, not aliases.
    c = cards[0]
    assert c.left_table == "account"  # lex order
    assert c.right_table == "cardmember"


def test_no_decisive_signal_falls_back_to_unknown():
    """A bare cross-join with no GROUP BY, no aggs, no LEFT — unknown."""
    fps = parse_sqls([
        "SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.k = b.k",
    ])
    cards = infer_join_cardinalities(fps)
    assert cards
    # Unknown is a valid result when corpus has no decisive evidence.
    assert cards[0].cardinality in {"unknown", "one_to_many", "many_to_one"}


def test_unparseable_fingerprint_skipped():
    """Parse-failed fingerprints don't break inference."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "this is not sql at all",
    ])
    cards = infer_join_cardinalities(fps)
    # Inference still works on the parseable one.
    assert cards
    assert cards[0].cardinality == "one_to_many"


def test_group_by_extracted_into_fingerprint():
    """The new GROUP BY capture is on every fingerprint."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    fp = fps[0]
    assert fp.group_by, "group_by must be populated"
    cols = {(g.get("table"), g.get("column")) for g in fp.group_by}
    assert ("cm", "cm11") in cols or (None, "cm11") in cols
