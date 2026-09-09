SELECT sub_product_group,
       ROUND(SUM(billed_business) / 1e9, 2) AS bb_billions
FROM `axp-lumi`.dw.cornerstone_metrics
WHERE bus_seg = 'Consumer'
  AND data_source = 'cornerstone'
  AND EXTRACT(YEAR FROM rpt_dt) = 2024
GROUP BY sub_product_group
