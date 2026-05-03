SELECT data_source, SUM(billed_business) AS total_bb
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE rpt_dt = DATE('2025-01-01')
  AND data_source IN ('cornerstone', 'oracle')
GROUP BY data_source
