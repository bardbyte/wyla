"""Phase 0 — sqlglot Layer-3 + Layer-4 extractor tests.

Coverage for the new SQLFingerprint fields. Each extractor has at least
one happy-path test; complex ones (subqueries, HAVING semantics, intent
classification) have multiple scenarios.
"""

from __future__ import annotations

from lumi.sql_to_context import parse_sqls


# ─── Layer 3 — direct extractors ────────────────────────────


def test_order_by_extraction():
    fps = parse_sqls(["SELECT a FROM t ORDER BY a DESC, b ASC"])
    ob = fps[0].order_by
    assert len(ob) == 2
    assert ob[0]["direction"] == "DESC"
    assert ob[1]["direction"] == "ASC"


def test_order_by_positional_reference():
    fps = parse_sqls(["SELECT a, b FROM t ORDER BY 1 DESC, 2 ASC"])
    ob = fps[0].order_by
    assert len(ob) == 2
    assert ob[0]["is_position_ref"] is True
    assert ob[1]["is_position_ref"] is True


def test_having_extracts_threshold_semantics():
    fps = parse_sqls([
        "SELECT bus_seg, SUM(amount) FROM t GROUP BY bus_seg HAVING SUM(amount) > 1000",
    ])
    having = fps[0].having
    assert len(having) == 1
    assert having[0]["aggregation"] == "SUM"
    assert having[0]["source_column"] == "amount"
    assert having[0]["operator"] == ">"
    assert having[0]["semantic_class"] == "threshold"


def test_limit_with_order_by_is_top_n():
    fps = parse_sqls(["SELECT a FROM t ORDER BY a DESC LIMIT 100"])
    assert fps[0].limit["value"] == 100
    assert fps[0].limit["is_top_n"] is True


def test_limit_without_order_by_not_top_n():
    fps = parse_sqls(["SELECT a FROM t LIMIT 10"])
    assert fps[0].limit["value"] == 10
    assert fps[0].limit["is_top_n"] is False


def test_distinct_select_detected():
    fps = parse_sqls(["SELECT DISTINCT a FROM t"])
    assert fps[0].distinct_select is True


def test_distinct_select_absent_by_default():
    fps = parse_sqls(["SELECT a FROM t"])
    assert fps[0].distinct_select is False


def test_subquery_in_where_detected():
    fps = parse_sqls([
        "SELECT a FROM t1 WHERE a IN (SELECT id FROM t2)",
    ])
    sq = fps[0].subqueries
    assert any(s["type"] == "IN_WHERE" for s in sq)
    assert any("t2" in s["tables"] for s in sq)


def test_exists_subquery_detected():
    fps = parse_sqls([
        "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
    ])
    sq = fps[0].subqueries
    assert any(s["type"] == "EXISTS" for s in sq)


def test_set_operations_union_detected():
    fps = parse_sqls([
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
    ])
    so = fps[0].set_operations
    assert len(so) == 1
    assert so[0]["type"] in {"UNION", "UNION_ALL"}
    assert so[0]["branch_count"] == 2


def test_null_handlers_coalesce_detected():
    fps = parse_sqls([
        "SELECT COALESCE(a, b, 0) AS x FROM t",
    ])
    nh = fps[0].null_handlers
    assert any(h["function"] == "COALESCE" for h in nh)


def test_type_casts_detected():
    fps = parse_sqls([
        "SELECT CAST(a AS STRING) AS a_str FROM t",
    ])
    tc = fps[0].type_casts
    assert tc
    assert tc[0]["to_type"] and "STRING" in tc[0]["to_type"]


def test_string_functions_detected():
    fps = parse_sqls([
        "SELECT CONCAT(a, b) AS ab, UPPER(c) AS c_upper FROM t",
    ])
    sf = fps[0].string_functions
    fns = {f["function"] for f in sf}
    assert "CONCAT" in fns
    assert "UPPER" in fns


def test_math_functions_detected():
    fps = parse_sqls([
        "SELECT ROUND(a, 2) AS r, ABS(b) AS abs_b FROM t",
    ])
    mf = fps[0].math_functions
    fns = {f["function"] for f in mf}
    assert "ROUND" in fns
    assert "ABS" in fns


def test_self_joins_detected():
    fps = parse_sqls([
        "SELECT a.x, b.y FROM users a JOIN users b ON a.referrer_id = b.id",
    ])
    sj = fps[0].self_joins
    assert sj
    assert sj[0]["table"] == "users"
    assert len(sj[0]["aliases_used"]) == 2


def test_window_function_detected():
    fps = parse_sqls([
        "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) AS rn FROM t",
    ])
    wf = fps[0].window_functions
    assert wf
    # ROW_NUMBER is detected; partition_by + order_by captured
    assert wf[0]["partition_by"] == ["b"]


def test_partition_pseudocolumn_detected():
    fps = parse_sqls([
        "SELECT a FROM t WHERE _PARTITIONDATE >= '2025-01-01'",
    ])
    pp = fps[0].partition_pseudocolumns
    assert pp
    assert pp[0]["column"] == "_PARTITIONDATE"


# ─── Layer 4 — semantic-feeding signals ─────────────────────


def test_query_fingerprint_hash_stable_across_whitespace():
    fps_a = parse_sqls(["SELECT  a   FROM t"])
    fps_b = parse_sqls(["SELECT a FROM t"])
    assert fps_a[0].query_fingerprint_hash == fps_b[0].query_fingerprint_hash


def test_query_fingerprint_hash_differs_for_different_queries():
    fps_a = parse_sqls(["SELECT a FROM t"])
    fps_b = parse_sqls(["SELECT b FROM t"])
    assert fps_a[0].query_fingerprint_hash != fps_b[0].query_fingerprint_hash


def test_intent_class_single_lookup():
    fps = parse_sqls(["SELECT * FROM customers WHERE id = 42"])
    assert fps[0].inferred_intent_class == "single_lookup"


def test_intent_class_aggregate():
    fps = parse_sqls([
        "SELECT bus_seg, SUM(x) FROM t GROUP BY bus_seg",
    ])
    assert fps[0].inferred_intent_class == "aggregate"


def test_intent_class_trend():
    fps = parse_sqls([
        "SELECT DATE_TRUNC(rpt_dt, MONTH) AS m, SUM(x) FROM t GROUP BY m",
    ])
    # trend = aggregation + date in GROUP BY
    assert fps[0].inferred_intent_class == "trend"


def test_intent_class_top_n():
    fps = parse_sqls([
        "SELECT a, b FROM t ORDER BY b DESC LIMIT 10",
    ])
    assert fps[0].inferred_intent_class == "top_n"


def test_intent_class_comparison_via_union():
    fps = parse_sqls([
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
    ])
    assert fps[0].inferred_intent_class == "comparison"


def test_intent_class_attribution_via_window():
    fps = parse_sqls([
        "SELECT a, ROW_NUMBER() OVER (ORDER BY b) AS rn FROM t",
    ])
    assert fps[0].inferred_intent_class == "attribution"


def test_intent_class_cohort_via_cte_name():
    fps = parse_sqls([
        "WITH active_consumers AS (SELECT id FROM users WHERE status='Active') "
        "SELECT bus_seg, SUM(x) FROM t JOIN active_consumers a ON t.id=a.id GROUP BY bus_seg",
    ])
    assert fps[0].inferred_intent_class == "cohort"


def test_implicit_grain_built_from_group_by():
    fps = parse_sqls([
        "SELECT cm11, SUM(x) FROM cornerstone_metrics GROUP BY cm11",
    ])
    assert "cornerstone_metrics" in fps[0].implicit_grain
    assert "cm11" in fps[0].implicit_grain


def test_derived_dim_proposal_from_case_when():
    fps = parse_sqls([
        "SELECT CASE WHEN fico >= 740 THEN 'Prime' "
        "WHEN fico >= 670 THEN 'Near-Prime' ELSE 'Sub' END AS fico_band FROM t",
    ])
    dd = fps[0].derived_dim_proposals
    assert dd
    assert dd[0]["output_name"] == "fico_band"
    assert dd[0]["source_column"] == "fico"


def test_cohort_scope_detected_from_cte_name():
    fps = parse_sqls([
        "WITH active_consumers AS (SELECT id FROM users WHERE status='Active') "
        "SELECT * FROM active_consumers",
    ])
    cs = fps[0].cohort_scope_signals
    assert cs
    assert cs[0]["cohort_name"] == "active_consumers"


def test_query_shape_summary_populated():
    fps = parse_sqls([
        "WITH x AS (SELECT a FROM t) "
        "SELECT bus_seg, SUM(amount) FROM x "
        "JOIN t2 ON x.a = t2.a WHERE flag = true GROUP BY bus_seg "
        "HAVING SUM(amount) > 100 ORDER BY 2 DESC LIMIT 10",
    ])
    s = fps[0].query_shape_summary
    assert s["n_tables"] >= 1
    assert s["n_joins"] >= 1
    assert s["n_aggregations"] >= 1
    assert s["n_ctes"] >= 1
    assert s["has_having"] is True
    assert s["has_order_by"] is True
    assert s["has_limit"] is True
    assert s["complexity_score"] >= 3


def test_layer3_fields_default_empty_on_simple_query():
    fps = parse_sqls(["SELECT * FROM t"])
    fp = fps[0]
    assert fp.order_by == []
    assert fp.having == []
    assert fp.limit == {}
    assert fp.window_functions == []
    assert fp.subqueries == []
    assert fp.set_operations == []
    # Bare projection now classifies as 'extract' (was 'unknown')
    assert fp.inferred_intent_class == "extract"


def test_intent_class_scalar_aggregate_is_aggregate():
    """SELECT SUM(x) FROM t — no GROUP BY but still an aggregate."""
    fps = parse_sqls(["SELECT SUM(amount) AS total FROM t"])
    assert fps[0].inferred_intent_class == "aggregate"


def test_intent_class_distinct_extract():
    fps = parse_sqls(["SELECT DISTINCT bus_seg FROM t"])
    assert fps[0].inferred_intent_class == "distinct_extract"


def test_intent_class_subquery_filter():
    fps = parse_sqls([
        "SELECT a FROM t1 WHERE a IN (SELECT id FROM t2)",
    ])
    assert fps[0].inferred_intent_class == "subquery_filter"


def test_intent_class_join_exploration():
    """Multi-table JOIN, no agg, no WHERE — exploration."""
    fps = parse_sqls([
        "SELECT a.x, b.y FROM t1 a JOIN t2 b ON a.id = b.id",
    ])
    assert fps[0].inferred_intent_class == "join_exploration"


def test_intent_class_sample():
    """Bare LIMIT, no ORDER BY → sample, not unknown."""
    fps = parse_sqls(["SELECT a FROM t LIMIT 10"])
    assert fps[0].inferred_intent_class == "sample"


def test_intent_class_extract():
    fps = parse_sqls(["SELECT a, b FROM t"])
    assert fps[0].inferred_intent_class == "extract"


def test_parse_error_doesnt_kill_fingerprint():
    fps = parse_sqls(["this is not sql at all"])
    # Whatever happens, parse_error is set OR the fingerprint is at least
    # constructible with empty fields
    fp = fps[0]
    assert fp.raw_sql == "this is not sql at all"
