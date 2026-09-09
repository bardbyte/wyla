SELECT EXTRACT(MONTH FROM rpt_dt) AS rpt_month,
       SUM(bluebox_discount_revenue) AS total_bbdr
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE data_source = 'cornerstone'
  AND EXTRACT(YEAR FROM rpt_dt) = 2024
GROUP BY rpt_month
