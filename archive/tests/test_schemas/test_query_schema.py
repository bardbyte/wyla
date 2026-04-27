from __future__ import annotations

from lumi.schemas import JoinCondition, JoinPattern, Measure, ParsedQuery


def test_parsed_query_defaults() -> None:
    q = ParsedQuery(
        query_id="q_0001",
        user_prompt="How many accounts?",
        expected_sql="SELECT COUNT(*) FROM x",
    )
    assert q.measures == []
    assert q.dimensions == []
    assert q.primary_table is None


def test_join_pattern_signature_stable_regardless_of_join_order() -> None:
    j1 = JoinCondition(
        left_table="a", left_column="id", right_table="b", right_column="aid", join_type="left"
    )
    j2 = JoinCondition(
        left_table="b", left_column="cid", right_table="c", right_column="id", join_type="inner"
    )
    p1 = JoinPattern(tables=["a", "b", "c"], joins=[j1, j2])
    p2 = JoinPattern(tables=["c", "a", "b"], joins=[j2, j1])
    assert p1.signature == p2.signature


def test_measure_roundtrip() -> None:
    m = Measure(function="SUM", column="x", distinct=False, expression="SUM(x)")
    assert m.function == "SUM"
    assert m.column == "x"
