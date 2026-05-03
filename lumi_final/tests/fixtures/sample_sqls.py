"""Sample SQL fixtures for testing all query patterns.

These are representative queries based on the gold query analysis.
Used in tests as inputs to the pipeline stages.
"""

# ─── Easy: single table, simple aggregation ──────────────────

Q1_SQL = """
SELECT SUM(billed_business)
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE bus_seg = 'Consumer'
  AND data_source = 'cornerstone'
  AND rpt_dt = DATE('2025-01-01')
"""

Q2_SQL = """
SELECT SUM(new_accounts_acquired)
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE EXTRACT(YEAR FROM rpt_dt) = 2024
  AND data_source = 'cornerstone'
"""

Q3_SQL = """
SELECT generation, AVG(billed_business) AS avg_bb
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND rpt_dt = DATE('2025-01-01')
GROUP BY generation
"""

Q4_SQL = """
SELECT sub_product_group,
       ROUND(SUM(billed_business) / 1e9, 2) AS bb_billions
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE bus_seg = 'Consumer'
  AND data_source = 'cornerstone'
  AND EXTRACT(YEAR FROM rpt_dt) = 2024
GROUP BY sub_product_group
"""

# ─── Medium: grouping, ordering, complex filters ─────────────

Q5_SQL = """
SELECT fico_band, SUM(accounts_in_force) AS total_aif
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND accounts_in_force > 0
  AND rpt_dt = DATE('2025-01-01')
GROUP BY fico_band
ORDER BY total_aif DESC
"""

Q6_SQL = """
SELECT EXTRACT(MONTH FROM rpt_dt) AS rpt_month,
       SUM(bluebox_discount_revenue) AS total_bbdr
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND EXTRACT(YEAR FROM rpt_dt) = 2024
GROUP BY rpt_month
"""

Q7_SQL = """
SELECT data_source, SUM(billed_business) AS total_bb
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE rpt_dt = DATE('2025-01-01')
  AND data_source IN ('cornerstone', 'oracle')
GROUP BY data_source
"""

Q8_SQL = """
SELECT COUNT(DISTINCT cm11)
FROM `axp-lumi`.dw.acquisitions
WHERE sub_prod_ds = 'gold'
  AND prod_class = 'charge'
  AND glbl_sub_chan = 'digital'
  AND dcsn_dt BETWEEN DATE('2024-01-01') AND DATE('2024-12-31')
  AND cm11 IS NOT NULL
  AND dcsn_cd = 'A'
  AND card_type = 'basic'
  AND dcsn_rsn_cd IS NOT NULL
  AND apm_flag IS NULL
"""

# ─── Hard: CTEs, derived dimensions, multi-table joins ───────

Q9_SQL = """
WITH rpah AS (
  SELECT acct_cust_xref_id, acct_bal_age_mth01_cd,
         acct_bill_bal_mth01_amt, acct_wrt_off_am,
         acct_rcvr_mo_01_am, acct_bus_unit_cd, acct_as_of_dt
  FROM `axp-lumi`.dw.risk_pers_acct_history
  WHERE acct_as_of_dt = DATE('2025-05-01')
    AND acct_srce_sys_cd = 'TRIUMPH'
    AND acct_bus_unit_cd IN (1, 2)
),
rich AS (
  SELECT cust_xref_id, fico_score, as_of_dt
  FROM `axp-lumi`.dw.risk_indv_cust_hist
  WHERE as_of_dt = DATE('2025-05-01')
)
SELECT rpah.*,
       CASE
         WHEN rich.fico_score >= 800 THEN 'Exceptional'
         WHEN rich.fico_score >= 740 THEN 'Very Good'
         WHEN rich.fico_score >= 670 THEN 'Good'
         WHEN rich.fico_score >= 580 THEN 'Fair'
         ELSE 'Poor'
       END AS fico_band,
       CASE
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) IN ('00', '01') THEN 'Current'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '02' THEN '30 DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '03' THEN '60 DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '04' THEN '90 DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '05' THEN '120 DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '06' THEN '150 DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) IN ('07', '08', '09') THEN '180+ DPB'
         WHEN TRIM(rpah.acct_bal_age_mth01_cd) = '99' THEN 'Written Off'
         ELSE 'NA/Other'
       END AS age_bucket,
       SUM(rpah.acct_bill_bal_mth01_amt) AS total_ar,
       SUM(rpah.acct_wrt_off_am) AS total_writeoffs,
       SUM(rpah.acct_rcvr_mo_01_am) AS total_recoveries
FROM rpah
LEFT JOIN rich
  ON rpah.acct_cust_xref_id = rich.cust_xref_id
  AND rpah.acct_as_of_dt = rich.as_of_dt
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""

Q10_SQL = """
WITH drm_prod AS (
  SELECT prod_cd, mbr_nm, prod_card_portfo AS business_unit
  FROM `axp-lumi`.dw.drm_product_member
  WHERE LOWER(prod_geo) = 'us'
    AND LOWER(prod_card_portfo) = 'business'
    AND LOWER(enbl_flag) = 'y'
    AND LOWER(leaf_in) = 'true'
),
drm_hier AS (
  SELECT parnt_nm, hier_level, hier_path
  FROM `axp-lumi`.dw.drm_product_hier
)
SELECT drm_hier.hier_path,
       SUM(rpah.acct_spend_mth01_amt) AS total_spend
FROM `axp-lumi`.dw.risk_pers_acct_history rpah
JOIN drm_prod
  ON rpah.acct_ia_pct_cd = drm_prod.prod_cd
JOIN drm_hier
  ON drm_prod.mbr_nm = drm_hier.parnt_nm
GROUP BY drm_hier.hier_path
"""

# ─── Convenience lists ───────────────────────────────────────

EASY_SQLS = [Q1_SQL, Q2_SQL, Q3_SQL, Q4_SQL]
MEDIUM_SQLS = [Q5_SQL, Q6_SQL, Q7_SQL, Q8_SQL]
HARD_SQLS = [Q9_SQL, Q10_SQL]
ALL_SQLS = EASY_SQLS + MEDIUM_SQLS + HARD_SQLS

QUERY_IDS = {
    Q1_SQL: "Q1", Q2_SQL: "Q2", Q3_SQL: "Q3", Q4_SQL: "Q4",
    Q5_SQL: "Q5", Q6_SQL: "Q6", Q7_SQL: "Q7", Q8_SQL: "Q8",
    Q9_SQL: "Q9", Q10_SQL: "Q10",
}
