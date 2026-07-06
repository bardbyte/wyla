-- C-30 dollar roll rate by month (reference pattern; lags pre-computed)
SELECT
  rpt_month,
  SUM(CASE WHEN dpd_bucket = '30+' THEN bal_lag1 END)
    / SUM(CASE WHEN dpd_bucket_lag1 = 'C' THEN bal_lag1 END) AS c30_dollar_rate
FROM common.roll_rate_calc
WHERE rpt_month = @as_of_month
GROUP BY rpt_month;
