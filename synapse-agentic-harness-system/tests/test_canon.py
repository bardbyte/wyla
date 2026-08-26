"""P0 gate: golden fingerprints (E8), twins, determinism, quarantine."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from sahs.canon.authority import Authority, authority_for
from sahs.canon.canonical import (
    CANON_VERSION,
    CanonError,
    c,
    try_canon,
    wrap_case,
    wrap_predicate,
)

from golden_sqls import GOLDEN, TWINS

GOLDENS_PATH = Path(__file__).parent / "fixtures" / "golden_fps.json"


def _compute() -> dict[str, dict[str, str]]:
    out = {}
    for name, sql in sorted(GOLDEN.items()):
        r = c(sql)
        out[name] = {"fp_expr": r.fp_expr, "fp_template": r.fp_template,
                     "canonical_sql": r.canonical_sql}
    return {"canon_version": CANON_VERSION, "goldens": out}


def test_golden_fingerprints_exact():
    computed = _compute()
    if os.environ.get("SAHS_REGEN_GOLDENS") == "1":
        GOLDENS_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDENS_PATH.write_text(json.dumps(computed, indent=1),
                                encoding="utf-8")
    frozen = json.loads(GOLDENS_PATH.read_text(encoding="utf-8"))
    assert frozen["canon_version"] == CANON_VERSION, (
        "canon_version drifted — a ruleset/sqlglot bump is a deliberate "
        "remint migration: rerun with SAHS_REGEN_GOLDENS=1 and review the "
        "golden diff")
    assert computed["goldens"] == frozen["goldens"]


def test_twins_fingerprint_equal():
    for name, twin_sql in TWINS.items():
        want = c(GOLDEN[name])
        got = c(twin_sql)
        assert got.fp_expr == want.fp_expr, (
            f"{name}: twin diverged\n  golden: {want.canonical_sql}"
            f"\n  twin:   {got.canonical_sql}")


def test_determinism_two_runs_identical():
    for sql in list(GOLDEN.values())[:8]:
        assert c(sql).fp_expr == c(sql).fp_expr


def test_template_is_arity_and_literal_blind():
    a = c("SELECT * FROM t WHERE c IN ('a','b')")
    b = c("SELECT * FROM t WHERE c IN ('x','y','z','w')")
    assert a.fp_template == b.fp_template
    assert a.fp_expr != b.fp_expr
    d1 = c("SELECT * FROM t WHERE d > DATE '2026-01-01'")
    d2 = c("SELECT * FROM t WHERE d > DATE '2020-06-15'")
    assert d1.fp_template == d2.fp_template


def test_tables_resolve_through_ctes_and_dots():
    r = c(GOLDEN["g09_cte_inline"])
    assert r.tables == ["gms_transaction"]
    r = c(GOLDEN["g19_dotted_table"])
    assert r.tables == ["axp-lumi.dw.wwcas_authorization"]


def test_self_join_stays_semantically_distinct():
    r = c(GOLDEN["g11_self_join"])
    assert "t1" in r.canonical_sql and "t2" in r.canonical_sql


def test_quarantine_categories():
    _, err = try_canon("SELEKT nope FROM")
    assert err is not None and err.category == "parse_error"
    _, err = try_canon("   ")
    assert err is not None and err.category == "fragment"
    with pytest.raises(CanonError):
        c("")


def test_fragment_wrappers():
    r = c(wrap_predicate("se_typ = 'B'", "gms_merchant_char"))
    assert r.tables == ["gms_merchant_char"]
    r = c(wrap_case("CASE WHEN a=1 THEN 'x' ELSE 'y' END", "t"))
    assert r.kind == "select"


def test_authority_lattice_order_and_unknown():
    assert Authority.CERTIFIED > Authority.PENDING > \
        Authority.SKILL_CONTRACT > Authority.MINED > Authority.SNIPPET
    assert authority_for("metrics_dmp") is Authority.CERTIFIED
    with pytest.raises(ValueError):
        authority_for("mystery_source")


def test_try_canon_never_raises_on_tokenizer_junk():
    """The real corpus killed the first census with an unterminated
    backtick (sqlglot TokenError, a sibling of ParseError). Every
    flavor of junk quarantines; try_canon NEVER raises."""
    from sahs.canon.canonical import try_canon
    junk = [
        "SELECT `ort_dt BETWEEN '2024-06-01' AND '2024-06-30'",  # open `
        "SELECT 'unterminated string FROM t",
        "SELECT \"unterminated dquote",
        "SELECT ((((((",
    ]
    for sql in junk:
        result, err = try_canon(sql)
        assert result is None and err is not None, sql
        assert err.category in ("parse_error", "fragment", "transform")
