"""Cross-table semantic equivalence closure tests.

Verifies the deterministic ontology layer that solves the
cardmember/customer naming-equivalence problem. All transitive closures
of JOIN ON pairs become equivalence classes; each class's members are
semantically the same business entity.
"""

from __future__ import annotations

from lumi.ontology import (
    compute_equivalence_classes,
    render_equivalence_classes_for_table,
)
from lumi.sql_to_context import parse_sqls


def test_simple_two_table_join_creates_equivalence_class():
    """A.x = B.y in any query → {A.x, B.y} are an equivalence class."""
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer_master b "
        "ON a.cm11 = b.cust_id",
    ])
    eq_map = compute_equivalence_classes(fps)
    equivalents = eq_map.equivalences_for("cardmember", "cm11")
    assert ("customer_master", "cust_id") in equivalents


def test_transitive_closure_across_three_queries():
    """If a.x=b.y in Q1 and b.y=c.z in Q2, then {a.x, b.y, c.z} are
    one equivalence class even though Q1 and Q2 never both touch a.x and c.z."""
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer_master b "
        "ON a.cm11 = b.cust_id",
        "SELECT * FROM customer_master b JOIN risk_history c "
        "ON b.cust_id = c.cust_xref_id",
    ])
    eq_map = compute_equivalence_classes(fps)

    # cardmember.cm11 should now be equivalent to risk_history.cust_xref_id
    # via the transitive customer_master.cust_id intermediary.
    cm11_equivalents = set(eq_map.equivalences_for("cardmember", "cm11"))
    assert ("customer_master", "cust_id") in cm11_equivalents
    assert ("risk_history", "cust_xref_id") in cm11_equivalents

    # And the symmetric direction works too.
    cust_xref_equivalents = set(
        eq_map.equivalences_for("risk_history", "cust_xref_id")
    )
    assert ("cardmember", "cm11") in cust_xref_equivalents


def test_multiple_independent_classes():
    """Different join chains should produce separate classes."""
    fps = parse_sqls([
        "SELECT * FROM customer a JOIN cm_master b ON a.cust_id = b.cm_id",
        "SELECT * FROM acct a JOIN acct_master b ON a.acct_id = b.acct_xref",
    ])
    eq_map = compute_equivalence_classes(fps)
    # Two equivalence classes, no cross-contamination.
    cust_eq = eq_map.equivalences_for("customer", "cust_id")
    assert ("cm_master", "cm_id") in cust_eq
    assert ("acct", "acct_id") not in cust_eq

    acct_eq = eq_map.equivalences_for("acct", "acct_id")
    assert ("acct_master", "acct_xref") in acct_eq
    assert ("cm_master", "cm_id") not in acct_eq


def test_query_count_strength():
    """Equivalence classes should track how many queries supported them."""
    fps = parse_sqls([
        "SELECT * FROM a JOIN b ON a.x = b.y",
        "SELECT * FROM a JOIN b ON a.x = b.y",
        "SELECT * FROM a JOIN b ON a.x = b.y",
    ])
    eq_map = compute_equivalence_classes(fps)
    assert eq_map.classes
    # All three queries contributed; class strength should be 3.
    assert eq_map.classes[0].query_count == 3


def test_no_join_no_class():
    """A table that never joins doesn't create an equivalence class."""
    fps = parse_sqls(["SELECT a, b FROM t WHERE x = 1"])
    eq_map = compute_equivalence_classes(fps)
    assert eq_map.equivalences_for("t", "a") == []
    assert len(eq_map.classes) == 0


def test_render_for_table_includes_equivalences():
    """The Markdown render should show our table's columns + their
    equivalents on other tables."""
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer_master b ON a.cm11 = b.cust_id",
    ])
    eq_map = compute_equivalence_classes(fps)
    md = render_equivalence_classes_for_table("cardmember", eq_map)
    assert "Cross-table semantic equivalences" in md
    assert "cm11" in md
    assert "customer_master.cust_id" in md


def test_render_empty_when_no_relevant_classes():
    """If our table doesn't appear in any equivalence class, render
    returns empty string (so the prompt section is skipped cleanly)."""
    fps = parse_sqls([
        "SELECT * FROM other_a JOIN other_b ON other_a.x = other_b.y",
    ])
    eq_map = compute_equivalence_classes(fps)
    md = render_equivalence_classes_for_table("not_in_any_join", eq_map)
    assert md == ""


def test_compound_join_keys_handled_gracefully():
    """JOIN with compound ON (a.x = b.y AND a.z = b.w) — sqlglot only
    captures one key pair, but we shouldn't crash."""
    fps = parse_sqls([
        "SELECT * FROM a JOIN b ON a.col1 = b.col1 AND a.col2 = b.col2",
    ])
    eq_map = compute_equivalence_classes(fps)
    # Either we get one class (if sqlglot picked one) or none — both fine.
    # The test is just that we don't crash.
    assert eq_map.classes is not None
