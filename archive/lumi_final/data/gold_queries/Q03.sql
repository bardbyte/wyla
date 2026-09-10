SELECT generation, AVG(billed_business) AS avg_bb
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND rpt_dt = DATE('2025-01-01')
GROUP BY generation
