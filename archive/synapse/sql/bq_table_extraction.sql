-- ============================================================================
-- Synapse — BigQuery extraction script for a single table
-- ============================================================================
-- Run this in the BigQuery console as the SVC ID with these roles:
--    roles/bigquery.metadataViewer      (INFORMATION_SCHEMA reads)
--    roles/bigquery.resourceViewer      (JOBS_BY_PROJECT history)
--    roles/bigquery.dataViewer          (SELECT against the target table for profiling)
--    roles/datacatalog.viewer           (policy tag taxonomy reads)
--    roles/dataplex.metadataReader      (Auto-DQ + Catalog API; optional, gated)
--
-- BEFORE RUNNING: edit these three lines, then run sections in order.
-- The script never executes DML; only reads. The profiling section costs
-- a few GB-scanned per table — run with a `bytes_billed` budget if needed.
-- ============================================================================

DECLARE target_project   STRING DEFAULT 'amex-dw-prod';            -- @@PROJECT
DECLARE target_dataset   STRING DEFAULT 'cornerstone_data';        -- @@DATASET
DECLARE target_table     STRING DEFAULT 'custins_customer_insights_cardmember'; -- @@TABLE
DECLARE region           STRING DEFAULT 'region-us';               -- BigQuery region for INFORMATION_SCHEMA scope
DECLARE lookback_days    INT64  DEFAULT 90;                        -- usage window

-- =========================================================================
-- SECTION 1 — Structural metadata (column schema + table options)
-- Source for: Table node, Column nodes, partitioning, clustering, types
-- Cost: free (INFORMATION_SCHEMA reads are not billed)
-- =========================================================================

-- 1.1 Column schema with types, nullability, partition + cluster role,
--      and any embedded description.
SELECT
  table_catalog,
  table_schema,
  table_name,
  column_name,
  ordinal_position,
  is_nullable,
  data_type,
  is_partitioning_column,
  clustering_ordinal_position,
  is_generated,
  generation_expression,
  is_hidden,
  is_updatable,
  is_system_defined,
  collation_name
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'custins_customer_insights_cardmember'
ORDER BY ordinal_position;

-- 1.2 Column-level descriptions, rounding policies, default expressions.
SELECT
  column_name,
  data_type,
  description,
  rounding_mode,
  column_default
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
WHERE table_name = 'custins_customer_insights_cardmember';

-- 1.3 Table-level metadata (type, creation time, options blob).
SELECT
  table_name,
  table_type,                  -- BASE TABLE | VIEW | MATERIALIZED VIEW | EXTERNAL
  creation_time,
  is_typed,
  is_insertable_into,
  ddl                          -- full CREATE TABLE — capture as a fact
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.TABLES
WHERE table_name = 'custins_customer_insights_cardmember';

-- 1.4 Table-level options: description, labels, partition expiration,
--     friendly name, require_partition_filter (governance signal).
SELECT
  option_name,
  option_type,
  option_value
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.TABLE_OPTIONS
WHERE table_name = 'custins_customer_insights_cardmember';

-- 1.5 Declared PK / FK / UNIQUE constraints (BigQuery added these in 2024).
--     Almost no AmEx team uses them yet, but if any do, this is gold.
SELECT
  tc.constraint_name,
  tc.constraint_type,           -- PRIMARY KEY | FOREIGN KEY
  kcu.column_name,
  kcu.ordinal_position,
  ccu.table_name  AS referenced_table,
  ccu.column_name AS referenced_column
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
LEFT JOIN `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON tc.constraint_name = kcu.constraint_name
LEFT JOIN `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
  ON tc.constraint_name = ccu.constraint_name
WHERE tc.table_name = 'custins_customer_insights_cardmember';

-- =========================================================================
-- SECTION 2 — Storage + partition stats
-- Source for: row_count, byte_size, freshness, partition cardinality
-- Cost: free
-- =========================================================================

-- 2.1 Table size + last modified.
SELECT
  table_id,
  row_count,
  size_bytes,
  TIMESTAMP_MILLIS(creation_time)      AS created_at,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_at,
  type                                 -- 1=table, 2=view
FROM `amex-dw-prod.cornerstone_data.__TABLES__`
WHERE table_id = 'custins_customer_insights_cardmember';

-- 2.2 Per-partition stats — partition cardinality, hot/cold partitions,
--     freshness of each partition. Critical for grain understanding.
SELECT
  partition_id,
  total_rows,
  total_logical_bytes,
  total_billable_bytes,
  last_modified_time,
  storage_tier             -- ACTIVE | LONG_TERM
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'custins_customer_insights_cardmember'
ORDER BY partition_id DESC
LIMIT 60;                  -- last 60 partitions; for date-partitioned, 2 months

-- 2.3 Streaming buffer stats (if any). Often surfaces "real-time" tables.
SELECT
  table_id,
  streaming_buffer_oldest_entry_time,
  streaming_buffer_estimated_rows,
  streaming_buffer_estimated_bytes
FROM `amex-dw-prod.cornerstone_data.__STREAMING_BUFFER__`
WHERE table_id = 'custins_customer_insights_cardmember';

-- =========================================================================
-- SECTION 3 — Per-column profiling (cardinality, nulls, distinct values)
-- Source for: cardinality_bucket, null_fraction, distinct_sample, FilterValue nodes
-- Cost: ONE FULL SCAN of the table — budget accordingly.
-- Strategy: one query with all aggregations to amortize the scan.
-- =========================================================================

-- 3.1 Cardinality + null fraction per column. Generated from the schema
--     dynamically. Replace `__COLS__` placeholders by hand or generate the
--     SQL programmatically from section 1.1 output. Example shown for the
--     known cardmember columns.

SELECT
  COUNT(*) AS total_rows,
  -- cm11
  APPROX_COUNT_DISTINCT(cm11)                            AS cm11__distinct,
  COUNTIF(cm11 IS NULL) / NULLIF(COUNT(*), 0)            AS cm11__null_frac,
  -- rpt_dt
  APPROX_COUNT_DISTINCT(rpt_dt)                          AS rpt_dt__distinct,
  COUNTIF(rpt_dt IS NULL) / NULLIF(COUNT(*), 0)          AS rpt_dt__null_frac,
  MIN(rpt_dt)                                            AS rpt_dt__min,
  MAX(rpt_dt)                                            AS rpt_dt__max,
  -- bus_seg (low-card categorical)
  APPROX_COUNT_DISTINCT(bus_seg)                         AS bus_seg__distinct,
  COUNTIF(bus_seg IS NULL) / NULLIF(COUNT(*), 0)         AS bus_seg__null_frac,
  -- data_source (structural filter)
  APPROX_COUNT_DISTINCT(data_source)                     AS data_source__distinct,
  COUNTIF(data_source IS NULL) / NULLIF(COUNT(*), 0)     AS data_source__null_frac,
  -- card_product_id (coded)
  APPROX_COUNT_DISTINCT(card_product_id)                 AS card_product_id__distinct,
  -- billed_business (numeric)
  APPROX_QUANTILES(billed_business, 100)                 AS billed_business__quantiles,
  AVG(billed_business)                                   AS billed_business__avg,
  MIN(billed_business)                                   AS billed_business__min,
  MAX(billed_business)                                   AS billed_business__max,
  COUNTIF(billed_business IS NULL) / NULLIF(COUNT(*), 0) AS billed_business__null_frac,
  -- fico (numeric, bounded 300-850)
  APPROX_QUANTILES(fico, 10)                             AS fico__deciles,
  COUNTIF(fico < 300 OR fico > 850) / NULLIF(COUNT(*),0) AS fico__out_of_range_frac,
  -- generation (low-card categorical)
  APPROX_COUNT_DISTINCT(generation)                      AS generation__distinct
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);  -- bound the scan
-- Cost note: replace with `WHERE 1=1` for a full scan if budget allows.

-- 3.2 Top distinct values + counts for low-cardinality columns.
--     Run one of these per low-card column (distinct ≤ 1000).
SELECT
  data_source                                      AS value,
  COUNT(*)                                         AS row_count,
  COUNT(*) / SUM(COUNT(*)) OVER ()                 AS share
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;

SELECT bus_seg AS value, COUNT(*) AS row_count
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

SELECT card_product_id AS value, COUNT(*) AS row_count
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

SELECT generation AS value, COUNT(*) AS row_count
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

-- 3.3 Pairwise null co-occurrence (cheap signal for "this column only
--     populated when X is set" — gives the LLM a hint at conditional grain).
SELECT
  COUNTIF(billed_business IS NULL AND accounts_in_force = 0) AS billed_null_when_no_accts,
  COUNTIF(billed_business IS NULL AND accounts_in_force > 0) AS billed_null_when_active,
  COUNTIF(fico IS NULL AND accounts_in_force = 0)            AS fico_null_when_no_accts
FROM `amex-dw-prod.cornerstone_data.custins_customer_insights_cardmember`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- =========================================================================
-- SECTION 4 — Usage telemetry from JOBS history
-- Source for: top_users, top_teams, query_count, peak_hours,
--             observed_joins, observed_filters, observed_aggregations
-- Cost: free (INFORMATION_SCHEMA.JOBS_BY_PROJECT)
-- =========================================================================

-- 4.1 Top users (by query count and bytes billed) on this table.
SELECT
  user_email,
  COUNT(*)                                                  AS query_count,
  SUM(total_bytes_billed)                                   AS bytes_billed,
  SUM(total_slot_ms)                                        AS slot_ms,
  AVG(total_slot_ms)                                        AS avg_slot_ms,
  MIN(creation_time)                                        AS first_seen,
  MAX(creation_time)                                        AS last_seen,
  COUNT(DISTINCT EXTRACT(DATE FROM creation_time))          AS active_days
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND EXISTS (
    SELECT 1 FROM UNNEST(referenced_tables) AS rt
    WHERE rt.table_id = 'custins_customer_insights_cardmember'
      AND rt.dataset_id = 'cornerstone_data'
  )
GROUP BY user_email
ORDER BY query_count DESC
LIMIT 30;

-- 4.2 Hourly distribution of queries (peak hours).
SELECT
  EXTRACT(HOUR FROM creation_time AT TIME ZONE 'UTC') AS hour_utc,
  COUNT(*) AS queries
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND EXISTS (
    SELECT 1 FROM UNNEST(referenced_tables) AS rt
    WHERE rt.table_id = 'custins_customer_insights_cardmember'
  )
GROUP BY hour_utc
ORDER BY queries DESC;

-- 4.3 Co-queried tables — what else gets touched in queries that touch this.
--     This is structural lineage that doesn't require LookML or sqlglot.
SELECT
  CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS co_referenced_table,
  COUNT(*)                                                    AS co_query_count
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
     UNNEST(j.referenced_tables) AS rt
WHERE j.job_type = 'QUERY'
  AND j.state = 'DONE'
  AND j.error_result IS NULL
  AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND EXISTS (
    SELECT 1 FROM UNNEST(j.referenced_tables) AS r2
    WHERE r2.table_id = 'custins_customer_insights_cardmember'
      AND r2.dataset_id = 'cornerstone_data'
  )
  AND rt.table_id != 'custins_customer_insights_cardmember'
GROUP BY co_referenced_table
ORDER BY co_query_count DESC
LIMIT 25;

-- 4.4 Recent successful queries that touched this table — sample for
--     corpus mining. Useful when we don't have gold queries yet.
SELECT
  job_id,
  user_email,
  creation_time,
  total_bytes_billed,
  statement_type,
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND statement_type = 'SELECT'
  AND query NOT LIKE '%INFORMATION_SCHEMA%'  -- exclude meta queries
  AND EXISTS (
    SELECT 1 FROM UNNEST(referenced_tables) AS rt
    WHERE rt.table_id = 'custins_customer_insights_cardmember'
  )
ORDER BY creation_time DESC
LIMIT 200;

-- 4.5 Failed queries — surfaces the questions users TRY to ask but can't
--     answer with the current schema. Highest-signal source for "what's
--     the graph still missing?"
SELECT
  user_email,
  creation_time,
  error_result.reason,
  error_result.message,
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NOT NULL
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND query LIKE '%custins_customer_insights_cardmember%'
ORDER BY creation_time DESC
LIMIT 100;

-- =========================================================================
-- SECTION 5 — Governance, PII, access control
-- Source for: pii_taxonomy, policy_tags, access_grants, ownership
-- Cost: free
-- =========================================================================

-- 5.1 Policy tags on columns (column-level security / PII classification).
--     Requires roles/datacatalog.viewer.
SELECT
  column_name,
  policy_tag.name,
  policy_tag.taxonomy_name,
  policy_tag.display_name
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.COLUMNS,
     UNNEST(IF(ARRAY_LENGTH(policy_tags) > 0, policy_tags, [STRUCT('' AS name, '' AS taxonomy_name, '' AS display_name)])) AS policy_tag
WHERE table_name = 'custins_customer_insights_cardmember'
  AND policy_tag.name != '';

-- 5.2 Object-level access grants (who has SELECT on this table).
SELECT
  grantee,
  grantee_type,
  privilege_type,
  is_grantable
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
WHERE object_name = 'custins_customer_insights_cardmember';

-- 5.3 Row-level access policies (data masking, filters).
SELECT
  row_access_policy_name,
  filter_predicate,
  creator,
  creation_time
FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
WHERE table_name = 'custins_customer_insights_cardmember';

-- =========================================================================
-- SECTION 6 — Data Quality (Dataplex Auto-DQ, if accessible)
-- Source for: DataQualityRule nodes, last_run_status, severity
-- Cost: free
-- =========================================================================

-- 6.1 Latest Auto-DQ scan results — requires Dataplex Catalog API + the
--     dataset to be registered in a Dataplex lake/zone.
SELECT
  data_source.table,
  rule.name           AS rule_name,
  rule.dimension      AS rule_dimension,   -- COMPLETENESS | VALIDITY | UNIQUENESS | etc.
  rule.column         AS target_column,
  rule.threshold      AS threshold_value,
  result.passed       AS passed,
  result.evaluated_count,
  result.passed_count,
  result.failed_count,
  result.null_count,
  job_end_time
FROM `amex-dw-prod`.dataplex.auto_data_quality_results
WHERE data_source.table = 'custins_customer_insights_cardmember'
  AND job_end_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY job_end_time DESC;
-- If Dataplex isn't enabled in your project, this query errors silently —
-- safe to skip.

-- =========================================================================
-- SECTION 7 — Cost + slot utilization (operational signal)
-- Source for: cost-tier flag on table, query economics
-- Cost: free
-- =========================================================================

-- 7.1 Last 30 days of cost on this table.
SELECT
  EXTRACT(DATE FROM creation_time) AS query_date,
  COUNT(*)                          AS query_count,
  SUM(total_bytes_billed) / 1e9     AS gb_billed,
  SUM(total_bytes_billed) / 1e12 * 6.25 AS approx_usd  -- on-demand pricing
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND EXISTS (
    SELECT 1 FROM UNNEST(referenced_tables) AS rt
    WHERE rt.table_id = 'custins_customer_insights_cardmember'
  )
GROUP BY query_date
ORDER BY query_date DESC;

-- =========================================================================
-- SECTION 8 — Schema change history (drift detection)
-- Source for: schema_changes feed, "since you last looked" signal
-- Cost: free
-- =========================================================================

-- 8.1 Recent DDL ops on this table.
SELECT
  creation_time,
  user_email,
  statement_type,         -- CREATE_TABLE | ALTER_TABLE_ADD_COLUMN | DROP_TABLE | ...
  query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND statement_type IN (
    'CREATE_TABLE', 'CREATE_TABLE_AS_SELECT', 'CREATE_OR_REPLACE_TABLE',
    'ALTER_TABLE', 'ALTER_TABLE_ADD_COLUMN', 'ALTER_TABLE_DROP_COLUMN',
    'ALTER_TABLE_SET_OPTIONS', 'DROP_TABLE'
  )
  AND query LIKE '%custins_customer_insights_cardmember%'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
ORDER BY creation_time DESC
LIMIT 50;

-- =========================================================================
-- SECTION 9 — Lineage via referenced/referencing relationships
-- Source for: UPSTREAM_OF / DOWNSTREAM_OF edges (empirical, not declared)
-- Cost: free
-- =========================================================================

-- 9.1 What writes to this table? (CREATE_TABLE_AS_SELECT / MERGE / INSERT
--     jobs that referenced upstream tables and wrote to this one.)
SELECT
  CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS upstream_table,
  COUNT(*) AS write_jobs,
  MAX(creation_time) AS last_seen
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
     UNNEST(j.referenced_tables) AS rt
WHERE j.job_type = 'QUERY'
  AND j.state = 'DONE'
  AND j.error_result IS NULL
  AND j.statement_type IN ('CREATE_TABLE_AS_SELECT', 'MERGE', 'INSERT', 'CREATE_OR_REPLACE_TABLE')
  AND j.destination_table.table_id = 'custins_customer_insights_cardmember'
  AND rt.table_id != 'custins_customer_insights_cardmember'
  AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
GROUP BY upstream_table
ORDER BY write_jobs DESC
LIMIT 30;

-- 9.2 What reads from this table and writes elsewhere (downstream).
SELECT
  CONCAT(j.destination_table.project_id, '.',
         j.destination_table.dataset_id, '.',
         j.destination_table.table_id) AS downstream_table,
  COUNT(*) AS write_jobs,
  MAX(creation_time) AS last_seen
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
WHERE j.job_type = 'QUERY'
  AND j.state = 'DONE'
  AND j.error_result IS NULL
  AND j.statement_type IN ('CREATE_TABLE_AS_SELECT', 'MERGE', 'INSERT', 'CREATE_OR_REPLACE_TABLE')
  AND j.destination_table.table_id IS NOT NULL
  AND j.destination_table.table_id != 'custins_customer_insights_cardmember'
  AND EXISTS (
    SELECT 1 FROM UNNEST(j.referenced_tables) AS rt
    WHERE rt.table_id = 'custins_customer_insights_cardmember'
  )
  AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
GROUP BY downstream_table
ORDER BY write_jobs DESC
LIMIT 30;

-- =========================================================================
-- SECTION 10 — Snapshot (single JSON-shaped bundle for the graph builder)
-- One row out. Feed directly into synapse/loaders/bq_loader.py.
-- =========================================================================

WITH cols AS (
  SELECT ARRAY_AGG(STRUCT(
    column_name,
    data_type,
    is_nullable,
    is_partitioning_column,
    clustering_ordinal_position
  )) AS columns
  FROM `amex-dw-prod`.cornerstone_data.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name = 'custins_customer_insights_cardmember'
),
size AS (
  SELECT row_count, size_bytes,
         TIMESTAMP_MILLIS(last_modified_time) AS last_modified_at
  FROM `amex-dw-prod.cornerstone_data.__TABLES__`
  WHERE table_id = 'custins_customer_insights_cardmember'
)
SELECT
  TO_JSON_STRING(STRUCT(
    'custins_customer_insights_cardmember' AS table_name,
    'amex-dw-prod' AS project_id,
    'cornerstone_data' AS dataset_id,
    (SELECT columns FROM cols) AS columns,
    (SELECT row_count FROM size) AS row_count,
    (SELECT size_bytes FROM size) AS size_bytes,
    (SELECT last_modified_at FROM size) AS last_modified_at
  )) AS snapshot;

-- =========================================================================
-- End of script. Save the outputs as JSON files in:
--    synapse/data/real/bq_cache/<table>.json
--    synapse/data/real/usage_history/<table>.json
--    synapse/data/real/dq_rules/<table>.json
--    synapse/data/real/lineage/<table>.json
-- The Python loaders read from these same paths the synthetic generator
-- uses, so no graph-builder changes are needed.
-- =========================================================================
