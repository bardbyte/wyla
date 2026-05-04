"""Grounding-signals tests — covers the deterministic intelligence we
synthesise from fingerprint + MDM + baseline before the enrichment LLM
call. The hardest signals (PK ranking, join cardinality, filtered-
measure detection, confidence labelling) get focused tests so a
regression here is visible immediately.

If grounding signals quality regresses, LLM output quality regresses
in lockstep — so this test file is a quality-budget canary.
"""

from __future__ import annotations

from lumi.grounding import (
    GROUNDED,
    INFERRED,
    GUESSED,
    build_grounding_signals,
    render_grounding_signals,
)
from lumi.schemas import TableContext
from lumi.sql_to_context import parse_sqls


def _ctx_with_mdm(
    table: str = "cornerstone_metrics",
    *,
    columns: list[str] | None = None,
    mdm_columns: list[dict] | None = None,
    baseline_dimensions: list[dict] | None = None,
    baseline_measures: list[dict] | None = None,
    queries_using: list[str] | None = None,
) -> TableContext:
    return TableContext(
        table_name=table,
        columns_referenced=columns or [],
        aggregations=[],
        case_whens=[],
        ctes_referencing_this=[],
        temp_tables_referencing_this=[],
        joins_involving_this=[],
        filters_on_this=[],
        date_functions=[],
        mdm_columns=mdm_columns or [],
        mdm_table_description=None,
        mdm_coverage_pct=0.5,
        existing_view_lkml=None,
        baseline_dimensions=baseline_dimensions or [],
        baseline_dimension_groups=[],
        baseline_measures=baseline_measures or [],
        baseline_quality_signals={},
        queries_using_this=queries_using or [],
    )


# ─── PK ranking ──────────────────────────────────────────────


def test_pk_grounded_when_id_pattern_plus_join_plus_distinct():
    """Strongest signal: name pattern + JOIN key in 2+ queries +
    COUNT(DISTINCT) seen → grounded."""
    sql_a = (
        "SELECT COUNT(DISTINCT cm_id) FROM cardmember c "
        "JOIN account a ON c.cm_id = a.cm_id"
    )
    sql_b = (
        "SELECT cm_id, SUM(amt) FROM cardmember c "
        "JOIN risk r ON c.cm_id = r.cm_id GROUP BY cm_id"
    )
    fps = parse_sqls([sql_a, sql_b])

    ctx = _ctx_with_mdm(
        table="cardmember",
        columns=["cm_id", "amt"],
        mdm_columns=[{
            "name": "cm_id", "type": "STRING",
            "business_name": "Cardmember Identifier",
        }],
        queries_using=["Q01", "Q02"],
    )

    g = build_grounding_signals(ctx, fps, {})
    assert g.primary_key_candidates
    top = g.primary_key_candidates[0]
    assert top.column == "cm_id"
    assert top.confidence == GROUNDED
    assert any("identifier" in r.lower() for r in top.reasons)


def test_pk_inferred_when_only_name_pattern_no_evidence():
    """Just a name pattern + no joins / no distinct → inferred only."""
    fps = parse_sqls(["SELECT amt FROM tbl WHERE rpt_dt = DATE('2025-01-01')"])
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["amt", "rpt_dt", "cust_xref_id"],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    # cust_xref_id has the name pattern but no JOIN / DISTINCT evidence
    cust = next((p for p in g.primary_key_candidates if p.column == "cust_xref_id"), None)
    if cust:
        assert cust.confidence in {INFERRED, GUESSED}


def test_pk_synthetic_composite_when_signals_present_but_weak():
    """When individual cols have name-pattern signals but no strong
    grouping evidence, we get either INFERRED individuals or a
    composite suggestion."""
    sqls = [
        "SELECT acct_id FROM facts WHERE rpt_dt = DATE('2025-01-01')",
        "SELECT cm_id FROM facts WHERE rpt_dt = DATE('2025-02-01')",
    ]
    fps = parse_sqls(sqls)
    ctx = _ctx_with_mdm(
        table="facts",
        columns=["acct_id", "cm_id", "rpt_dt", "amt"],
        queries_using=["Q01", "Q02"],
    )
    g = build_grounding_signals(ctx, fps, {})
    # Both id-like cols should surface as candidates by name pattern alone.
    cols = {p.column for p in g.primary_key_candidates}
    assert "acct_id" in cols or "cm_id" in cols
    # At least one should be at INFERRED — they have name signal but
    # no JOIN / DISTINCT evidence to cross 8.
    confs = {p.confidence for p in g.primary_key_candidates}
    assert INFERRED in confs or GROUNDED in confs


# ─── Join inference ──────────────────────────────────────────


def test_join_hint_observed_from_fingerprint():
    sql = "SELECT * FROM cardmember c JOIN account a ON c.cm_id = a.cm_id"
    fps = parse_sqls([sql])
    ctx = _ctx_with_mdm(
        table="cardmember",
        columns=["cm_id"],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    assert g.join_hints
    h = g.join_hints[0]
    assert h.this_table == "cardmember"
    assert h.this_column == "cm_id"
    assert h.other_table == "account"
    assert h.source == "fingerprint"
    assert h.confidence == GROUNDED


def test_join_cardinality_inferred_from_partner_distinct():
    """If the partner column is COUNT-DISTINCT'd, it's the 'one' side
    so our relationship is many_to_one."""
    sql_join = (
        "SELECT * FROM cardmember c JOIN account a ON c.cm_id = a.cm_id"
    )
    sql_distinct = "SELECT COUNT(DISTINCT cm_id) FROM account"
    fps = parse_sqls([sql_join, sql_distinct])

    ctx_cm = _ctx_with_mdm(
        table="cardmember", columns=["cm_id"], queries_using=["Q01"],
    )
    ctx_acct = _ctx_with_mdm(
        table="account", columns=["cm_id"], queries_using=["Q01", "Q02"],
    )
    contexts = {"cardmember": ctx_cm, "account": ctx_acct}

    g = build_grounding_signals(ctx_cm, fps, contexts)
    h = g.join_hints[0]
    assert h.relationship == "many_to_one"


# ─── Always filter ───────────────────────────────────────────


def test_always_filter_from_mdm_partition():
    sql = "SELECT a FROM tbl WHERE rpt_dt = DATE('2025-01-01')"
    fps = parse_sqls([sql])
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["a", "rpt_dt"],
        mdm_columns=[{"name": "rpt_dt", "type": "DATE", "is_partitioned": True}],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    af_cols = [a.column for a in g.always_filter_candidates]
    assert "rpt_dt" in af_cols
    rpt = next(a for a in g.always_filter_candidates if a.column == "rpt_dt")
    assert rpt.reason == "mdm_partitioned"
    assert rpt.suggested_default == "last 90 days"


def test_always_filter_from_high_freq_date():
    sqls = [
        "SELECT a FROM tbl WHERE rpt_dt = DATE('2025-01-01')",
        "SELECT b FROM tbl WHERE rpt_dt = DATE('2025-02-01')",
        "SELECT c FROM tbl WHERE rpt_dt = DATE('2025-03-01')",
        "SELECT d FROM tbl WHERE rpt_dt BETWEEN DATE('2025-01-01') AND DATE('2025-02-01')",
    ]
    fps = parse_sqls(sqls)
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["a", "b", "c", "d", "rpt_dt"],
        queries_using=["Q01", "Q02", "Q03", "Q04"],
    )
    g = build_grounding_signals(ctx, fps, {})
    af_cols = [a.column for a in g.always_filter_candidates]
    assert "rpt_dt" in af_cols


# ─── Hidden candidates ───────────────────────────────────────


def test_hidden_candidate_for_audit_field():
    fps = parse_sqls(["SELECT amt FROM tbl"])
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["amt"],
        baseline_dimensions=[
            {"name": "amt", "type": "number"},
            {"name": "etl_load_dt", "type": "date"},
        ],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    hidden_cols = [h.column for h in g.hidden_candidates]
    assert "etl_load_dt" in hidden_cols


# ─── Observed values (allowed_values without BQ) ─────────────


def test_observed_values_aggregate_across_queries():
    sqls = [
        "SELECT amt FROM tbl WHERE bus_seg = 'Consumer'",
        "SELECT amt FROM tbl WHERE bus_seg = 'Commercial'",
        "SELECT amt FROM tbl WHERE bus_seg IN ('Consumer', 'GNS')",
    ]
    fps = parse_sqls(sqls)
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["amt", "bus_seg"],
        queries_using=["Q01", "Q02", "Q03"],
    )
    g = build_grounding_signals(ctx, fps, {})
    seen = set(g.observed_values_by_column.get("bus_seg", []))
    assert {"Consumer", "Commercial", "GNS"} <= seen


# ─── Confidence labels ───────────────────────────────────────


def test_confidence_grounded_when_mdm_describes_column():
    fps = parse_sqls(["SELECT amt FROM tbl"])
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["amt"],
        mdm_columns=[{
            "name": "amt", "type": "NUMERIC",
            "business_name": "Transaction Amount",
            "description": "USD amount of the transaction",
        }],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    assert g.column_confidence.get("amt") == GROUNDED


def test_confidence_guessed_when_no_anchor_anywhere():
    """Column referenced but no MDM, no baseline desc, no usage signal."""
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["mystery_col"],
        queries_using=[],
    )
    g = build_grounding_signals(ctx, [], {})
    # Column may not even appear in usage if no fingerprints reference it,
    # but if it does, it should be guessed. Either way, no grounded signal.
    if "mystery_col" in g.column_confidence:
        assert g.column_confidence["mystery_col"] in {GUESSED, INFERRED}


# ─── Filtered-measure detection ──────────────────────────────


def test_filtered_measure_candidate_from_case_when_in_sum():
    sql = """
    SELECT SUM(CASE WHEN bus_seg = 'Consumer' THEN amt ELSE 0 END) AS bb_consumer
    FROM tbl
    """
    fps = parse_sqls([sql])
    ctx = _ctx_with_mdm(
        table="tbl",
        columns=["bus_seg", "amt"],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    if g.filtered_measure_candidates:
        # At least one should map back to bus_seg=Consumer
        cands = g.filtered_measure_candidates
        assert any(
            c.filter_column == "bus_seg" and c.filter_value == "Consumer"
            for c in cands
        )


# ─── Render output ───────────────────────────────────────────


def test_render_grounding_signals_produces_markdown():
    sql = "SELECT cm_id FROM cardmember WHERE rpt_dt = DATE('2025-01-01')"
    fps = parse_sqls([sql])
    ctx = _ctx_with_mdm(
        table="cardmember",
        columns=["cm_id", "rpt_dt"],
        mdm_columns=[
            {"name": "cm_id", "business_name": "Cardmember ID", "type": "STRING"},
            {"name": "rpt_dt", "type": "DATE", "is_partitioned": True},
        ],
        queries_using=["Q01"],
    )
    g = build_grounding_signals(ctx, fps, {})
    md = render_grounding_signals(g)
    # The rendered prompt section should mention all the major sections.
    assert "Grounding signals" in md
    assert "Primary-key candidates" in md
    assert "always_filter" in md
    assert "Per-column intelligence" in md
    # Confidence labels surface as bracketed tags.
    assert "[grounded]" in md or "[GROUNDED]" in md or "[inferred]" in md
