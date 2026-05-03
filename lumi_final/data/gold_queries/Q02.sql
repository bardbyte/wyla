SELECT SUM(new_accounts_acquired)
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE EXTRACT(YEAR FROM rpt_dt) = 2024
  AND data_source = 'cornerstone'
