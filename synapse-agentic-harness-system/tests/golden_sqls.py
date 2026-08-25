"""The 25-statement golden fingerprint set (E8) — BigQuery nasties included.

Frozen fps live in tests/fixtures/golden_fps.json; regenerate deliberately
with SAHS_REGEN_GOLDENS=1 (a ruleset/sqlglot bump is a remint migration,
and the diff of that file IS the migration review).

TWINS pairs are semantically identical statements written differently —
they must fingerprint equal (the whole point of c(sql)).
"""

GOLDEN = {
    "g01_plain": "SELECT a, b FROM orders WHERE x = 1 AND y = 2",
    "g02_qualify":
        "SELECT * EXCEPT(secret_col) FROM t QUALIFY ROW_NUMBER() OVER "
        "(PARTITION BY k ORDER BY d DESC) = 1",
    "g03_safe_divide": "SELECT SAFE_DIVIDE(num, den) AS rate FROM t",
    "g04_unnest":
        "SELECT x FROM t, UNNEST(items) AS item WHERE item.kind = 'A'",
    "g05_date_literals":
        "SELECT DATE '2026-01-01' AS d, TIMESTAMP '2026-01-01 00:00:00' "
        "AS ts, INTERVAL 3 DAY AS iv",
    "g06_backtick_mixed":
        "SELECT `MixedCase`.`Weird Col` FROM `MixedCase`",
    "g07_nested_case":
        "SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN 'x' ELSE 'y' END "
        "ELSE 'z' END FROM t",
    "g08_window":
        "SELECT k, SUM(v) OVER (PARTITION BY k ORDER BY d ROWS BETWEEN 1 "
        "PRECEDING AND CURRENT ROW) FROM t",
    "g09_cte_inline":
        "WITH base AS (SELECT * FROM gms_transaction) "
        "SELECT SUM(x) FROM base",
    "g10_cte_mixed":
        "WITH a AS (SELECT * FROM t1), b AS (SELECT k, SUM(v) s FROM t2 "
        "GROUP BY k) SELECT a.x, b.s FROM a JOIN b ON a.k = b.k",
    "g11_self_join":
        "SELECT a.id FROM emp a JOIN emp b ON a.mgr = b.id WHERE b.lvl = 3",
    "g12_in_list": "SELECT * FROM t WHERE c IN ('b', 'a', 'c')",
    "g13_predicate_wrap":
        "SELECT 1 FROM wwcas_authorization WHERE "
        "trim(business_unit_cd) IN ('1', '2')",
    "g14_case_wrap":
        "SELECT CASE WHEN se_typ = 'B' THEN 'hotel' ELSE 'other' END "
        "FROM gms_merchant_char",
    "g15_union":
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
    "g16_group_having":
        "SELECT k, COUNT(DISTINCT id) n FROM t GROUP BY k "
        "HAVING COUNT(DISTINCT id) > 10",
    "g17_join_three":
        "SELECT o.amt FROM orders o JOIN customers c ON o.cid = c.id "
        "JOIN regions r ON c.rid = r.id WHERE r.code = 'EMEA'",
    "g18_agg_filters":
        "SELECT SUM(CASE WHEN status = 'A' THEN amt ELSE 0 END) FROM txns",
    "g19_dotted_table":
        "SELECT x FROM `axp-lumi`.dw.wwcas_authorization "
        "WHERE part_dt = '2026-08-01'",
    "g20_between":
        "SELECT * FROM t WHERE d BETWEEN '2026-01-01' AND '2026-03-31'",
    "g21_null_logic":
        "SELECT * FROM t WHERE x IS NOT NULL AND COALESCE(y, 0) > 5",
    "g22_numeric_forms": "SELECT * FROM t WHERE x = 1.0 AND y = 0.50",
    "g23_flip_literal": "SELECT * FROM t WHERE 5 = x AND 'b' != c",
    "g24_or_chain":
        "SELECT * FROM t WHERE z = 3 OR y = 2 OR x = 1",
    "g25_subquery":
        "SELECT * FROM t WHERE id IN (SELECT id FROM approved WHERE "
        "region = 'US')",
}

# semantically identical rewrites — fp_expr must match the named golden
TWINS = {
    "g01_plain":
        "select A,   B from ORDERS where Y=2 and 1 = X  -- mangled\n",
    "g09_cte_inline": "SELECT SUM(x) FROM gms_transaction",
    "g11_self_join":
        "SELECT x.id FROM emp x JOIN emp y ON x.mgr = y.id WHERE y.lvl = 3",
    "g12_in_list": "SELECT * FROM t WHERE c IN ('c', 'a', 'b')",
    "g17_join_three":
        "SELECT orders.amt FROM orders JOIN customers ON orders.cid = "
        "customers.id JOIN regions ON customers.rid = regions.id "
        "WHERE regions.code = 'EMEA'",
    "g22_numeric_forms": "SELECT * FROM t WHERE x = 1 AND y = 0.5",
    "g23_flip_literal": "SELECT * FROM t WHERE x = 5 AND c != 'b'",
    "g24_or_chain": "SELECT * FROM t WHERE x = 1 OR y = 2 OR z = 3",
}
