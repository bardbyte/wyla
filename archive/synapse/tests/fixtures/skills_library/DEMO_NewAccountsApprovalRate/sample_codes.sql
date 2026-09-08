-- Gross approval rate by decision month (reference pattern)
SELECT
  DATE_TRUNC(decision_dt, MONTH) AS decision_month,
  COUNT(DISTINCT CASE WHEN decision_cd = 'A' THEN na_pcn_no END)
    / COUNT(DISTINCT CASE WHEN decision_cd IN ('A','D') THEN na_pcn_no END)
    AS gross_approval_rate
FROM sbs_new_accounts
WHERE decision_dt BETWEEN @start_month AND @end_month
GROUP BY decision_month
ORDER BY decision_month;
