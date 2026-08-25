-- SQLite-dialect demo (deliberately NOT BigQuery)
SELECT strftime('%Y-%m', app_dt) m,
       1.0 * SUM(CASE WHEN approval_cd = 'A' THEN 1 ELSE 0 END) / COUNT(1)
FROM sbs_new_accounts GROUP BY m;
