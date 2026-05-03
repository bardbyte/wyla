SELECT fico_band, SUM(accounts_in_force) AS total_aif
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND accounts_in_force > 0
  AND rpt_dt = DATE('2025-01-01')
GROUP BY fico_band
ORDER BY total_aif DESC
