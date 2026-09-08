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
