SELECT SUM(billed_business)
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE bus_seg = 'Consumer'
  AND data_source = 'cornerstone'
  AND rpt_dt = DATE('2025-01-01')
