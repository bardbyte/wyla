# Synapse — BigQuery extraction guide

> Companion to `synapse/sql/bq_table_extraction.sql`. Run this in the BQ
> console on the work laptop as the SVC ID. Send the outputs back in the
> shape described per section. We feed each piece into a specific graph
> node/edge.
>
> **Audience:** you, running this against one real AmEx table to validate
> the loader plumbing before we scale to all 56.
> **Target table for the first run:** `custins_customer_insights_cardmember`
> (or any tier-1 cardmember table you prefer).

---

## Before you run

1. Open `synapse/sql/bq_table_extraction.sql`.
2. Edit the three `DECLARE` lines at the top:
   - `target_project` — your BQ project (e.g. `amex-dw-prod`)
   - `target_dataset` — the dataset that holds the table
   - `target_table` — the table name (no project/dataset prefix)
3. Confirm the SVC ID has these roles (script will silently return empty for any section it lacks the role for):
   - `roles/bigquery.metadataViewer` (always required)
   - `roles/bigquery.resourceViewer` (for sections 4, 7, 8, 9 — JOBS history)
   - `roles/bigquery.dataViewer` (for section 3 — profiling, ~1 GB-scan cost per run with the 30-day bound)
   - `roles/datacatalog.viewer` (for section 5.1 — policy tags / PII classification)
   - `roles/dataplex.metadataReader` (for section 6 — Auto-DQ; optional, skip if Dataplex isn't enabled in your project)
4. Run sections **in order**. Sections 1, 2, 5, 6, 7, 8, 9 are free. Section 3 (profiling) costs real money — keep the 30-day `WHERE` clause unless you want a full scan.

---

## What each section gives us and what to send back

| Section | What we ask BQ for | Why we need it | What graph fact it feeds | Send back as |
|---|---|---|---|---|
| **1.1** | `INFORMATION_SCHEMA.COLUMNS` — name, type, nullable, partition role, cluster ordinal | The literal schema. Ground truth, zero hallucination. | `Column` node properties: `data_type`, `is_nullable`, `is_partitioning`, `cluster_position`. Mints one `Column` node + one `CONTAINS` edge per row. | CSV (use BQ "Save results → CSV") |
| **1.2** | Column descriptions + default expressions | Pre-existing BQ-level column documentation (often from `OPTIONS(description=...)`). | `Column.description` with `source="bq"` — competes with MDM description. | CSV |
| **1.3** | Table-level metadata + DDL string | Asset kind, creation time, full CREATE statement. The DDL is gold — encodes intent. | `Table.asset_kind`, `Table.ddl_snapshot` (new property — see below). | JSON (one row, the DDL field is multi-line) |
| **1.4** | `TABLE_OPTIONS` — labels, friendly_name, partition_expiration, `require_partition_filter` | `require_partition_filter` is a critical governance signal: query without partition predicate → fails. Labels often encode domain/tier/owner. | `Table.tags` (from labels), `Table.require_partition_filter` (new), `Table.partition_expiration_days` (new). | CSV |
| **1.5** | Declared PK / FK constraints | If any teams use them, they're human-asserted ground truth. Promotes `IDENTIFIES` edges to `human_asserted`. | `Column.is_primary` (overrides MDM if conflict), `EQUIVALENT_TO` edge between FK source/target. | CSV |
| **2.1** | `__TABLES__` — `row_count`, `size_bytes`, `last_modified_time` | Freshness + scale. Drives the "FRESH" tile in the Trust Header. | `Table.row_count`, `Table.size_bytes`, `Table.last_modified`. | CSV (one row) |
| **2.2** | `INFORMATION_SCHEMA.PARTITIONS` (last 60 partitions) | Per-partition row counts → reveals hot vs. cold partitions, partition cardinality, freshness per slice. Lets us detect "stale partition" failures. | `Table.partition_grain` (e.g. "daily"), `Table.partition_freshness_hours`, candidate `DataQualityRule` if any partition is unexpectedly small. | JSON or CSV |
| **2.3** | Streaming buffer stats | If a table has a streaming buffer, it's near-real-time. Changes the freshness story entirely. | `Table.is_streaming` (new bool), `Table.streaming_lag_seconds`. | CSV (often empty — that's fine, means no stream) |
| **3.1** | `APPROX_COUNT_DISTINCT` + `COUNTIF(NULL)` + numeric `MIN/MAX/AVG/QUANTILES` per column | Cardinality bucketing (low/medium/high/very_high), null-fraction, value ranges. Drives the "is this a categorical?" / "is this an identifier?" decision. | `Column.approx_distinct`, `Column.null_fraction`, `Column.min_value`, `Column.max_value`, `Column.cardinality_bucket`. | CSV. **One row** with one column per metric. |
| **3.2** | Per-column `APPROX_TOP_COUNT` for low-card columns (run one per column with distinct ≤ 1000) | The actual observed enum values. Mints `FilterValue` nodes — what stewards / agents see when picking filters. | One `FilterValue` node per (table, column, value) + `count_obs`. | One CSV per column. Name the file `topcount__<column>.csv`. |
| **3.3** | Pairwise null co-occurrence (sample) | Detects conditional columns ("FICO is null when accounts_in_force=0"). Reveals grain conditions. | `Column.populated_when` (free-form clause, new property). | CSV |
| **4.1** | Top users (by query count + bytes billed) on this table, last 90 days | The user list. **Top users are the candidate stewards.** | `User` nodes, `QUERIED_BY` edges (Table → User) with `query_count` and `bytes_billed`. | CSV |
| **4.2** | Hourly query distribution | When the table is hot. Useful for scheduling, governance, and finding stewards in your timezone. | `Table.peak_query_hours` (list of integers UTC). | CSV |
| **4.3** | Co-queried tables — what else gets touched in queries that touch this table | **Empirical lineage** — stronger than any declared lineage. The same tables JOIN-ed in real queries. | `EQUIVALENT_TO` edges (via co-query frequency) + `RELATES_TO` edges with `co_query_count`. Also seeds candidate Entity proposals: "these 7 tables share `cm11` → 'Customer' is a candidate entity." | CSV |
| **4.4** | Sample of 200 recent successful queries that touched the table | If we don't have a curated gold-query set for this table, these BECOME the corpus. sqlglot extraction runs on these. | Mints all the corpus-derived facts: `EQUIVALENT_TO` (JOIN), `COMPUTED_FROM` + `SLICEABLE_BY` (aggregations + GROUP BY), `FilterValue` (WHERE literals), `CodeMapping` (CASE WHEN). | JSON — each row has `job_id`, `user_email`, `query` (the SQL string is what we extract from). |
| **4.5** | Recent **failed** queries that mention the table | Failed queries surface "questions the user TRIED to ask but the schema can't answer." Highest-signal gap finder. | New node type proposal: `UnansweredQuestion` (we don't have this yet — adds it as a `review_queue/` entry). Drives entity-proposal review priority. | CSV |
| **5.1** | Policy tags on each column | PII classification authority. If MDM says "is_pii=False" but a policy tag says "Sensitive>Identifier>SSN", the policy tag wins — it's enforcement-level truth. | `Column.pii_taxonomy`, `Column.is_pii` — with `source="bq"` source attribution (rather than MDM). High-weight signal. | CSV |
| **5.2** | Object-level access grants (who has SELECT) | Governance graph. Shows which teams can query this. Useful for the steward-finding workflow. | New edge type `HAS_ACCESS` (User/Group → Table). For v1, store as a Table property `access_groups: list[str]`. | CSV |
| **5.3** | Row-level access policies | Data masking + row filters. Critical because the values you see in profiling may be MASKED for some users. | `Table.has_row_access_policy` (bool) + the predicate text. Warns Radix that result counts may differ by user. | CSV |
| **6.1** | Dataplex Auto-DQ scan results (last 7 days) | Existing DQ infrastructure if your org runs Dataplex. Pass/fail per rule per column. | `DataQualityRule` nodes with `last_run_status`, `VALIDATED_BY` edges. Drives the "DQ" tile in the Trust Header. | CSV. **Skip if Dataplex returns no rows** — we'll generate proposed rules from the BQ profile in section 3 instead. |
| **7.1** | Cost per day, last 30 days, on this table | Operational signal. Expensive tables get prioritized for caching / aggregate_tables. | `Table.monthly_cost_usd_estimate`. Not consumed by Radix; used by the UI's "high-value tables" filter. | CSV |
| **8.1** | DDL ops history (last 180 days) — `CREATE_TABLE`, `ALTER_TABLE_ADD_COLUMN`, etc. | Schema drift detection. Powers the "Changes since you last looked" feed. **Catches columns that exist now but didn't a month ago — directly relevant for descriptions that need to be rewritten.** | New node type proposal: `SchemaChangeEvent`. For v1, store as `Table.recent_schema_changes: list[dict]`. | CSV |
| **9.1** | Tables that write TO this one (destination_table = this) | Real lineage upstream. Not what MDM declares, what the warehouse actually does. | `UPSTREAM_OF` edges with `source="bq"` (overrides MDM's declared lineage on conflict). | CSV |
| **9.2** | Tables that this one writes TO (this in referenced_tables, destination_table elsewhere) | Real lineage downstream. | `UPSTREAM_OF` edges in the reverse direction. | CSV |
| **10** | Single JSON-shaped bundle (table_name, columns, row_count, size, last_modified) | Convenience snapshot for the bq_loader to round-trip easily. Quick sanity check before processing the bigger files. | Feeds `Table` + `Column` core properties — but mostly a "did the queries succeed end-to-end?" smoke test. | JSON (one row, one column called `snapshot`). |

---

## Minimum viable set (if you're short on time)

If you can only run a subset, this is the prioritized order — each tier still produces a graph that builds and renders, just with progressively richer signal:

**Tier 1 — must run (10 minutes, all free):** 1.1, 1.3, 1.4, 2.1, 10.
> Gets us: a Table node with full schema, partition info, asset kind, freshness. Inspector renders.

**Tier 2 — add for usability (5 minutes, all free):** 4.1, 4.2, 4.3, 4.4.
> Gets us: top users (steward candidates), peak hours, empirical lineage, sample queries → the **corpus** for sqlglot extraction.

**Tier 3 — add for governance (5 minutes, all free):** 1.5, 5.1, 5.2, 5.3.
> Gets us: PII classification, access grants, row policies → the Governance tab fills out.

**Tier 4 — add for trust (~$1 of scan, 30 minutes):** 3.1, 3.2, 3.3.
> Gets us: per-column profiling → cardinality buckets, null fractions, FilterValue nodes. The Trust Header lights up.

**Tier 5 — add for lifecycle (5 minutes, free):** 8.1, 9.1, 9.2.
> Gets us: schema change history, real upstream/downstream lineage.

**Tier 6 — add if Dataplex is enabled (5 minutes, free):** 6.1.
> Gets us: existing Auto-DQ rules. If your org doesn't use Dataplex, we generate proposed rules from section 3 profiling instead — zero work for you.

Run Tier 1 first and send back. We loop on whether the loader correctly produces the same graph state our synthetic data produces today, then add Tiers 2-6 in order.

---

## File layout on send-back

Save outputs into one folder per table:

```
~/synapse_bq_outputs/custins_customer_insights_cardmember/
├── 1_1__columns.csv
├── 1_2__column_descriptions.csv
├── 1_3__table_meta.json
├── 1_4__table_options.csv
├── 1_5__constraints.csv
├── 2_1__size_freshness.csv
├── 2_2__partitions.csv
├── 2_3__streaming_buffer.csv         (may be empty — OK)
├── 3_1__cardinality_nulls.csv
├── 3_2__topcount__data_source.csv
├── 3_2__topcount__bus_seg.csv
├── 3_2__topcount__card_product_id.csv
├── 3_2__topcount__generation.csv
├── 3_3__null_cooccurrence.csv
├── 4_1__top_users.csv
├── 4_2__peak_hours.csv
├── 4_3__co_queried_tables.csv
├── 4_4__sample_queries.json
├── 4_5__failed_queries.csv
├── 5_1__policy_tags.csv
├── 5_2__access_grants.csv
├── 5_3__row_policies.csv
├── 6_1__dataplex_dq.csv               (may be empty — OK)
├── 7_1__cost_30d.csv
├── 8_1__ddl_history.csv
├── 9_1__upstream_tables.csv
├── 9_2__downstream_tables.csv
└── 10__snapshot.json
```

Zip it, drop the zip in the repo at `synapse/data/real/<table_name>/`, and tell me it's ready. I'll write the loader to read this exact layout.

---

## What I'll do once I have one table's output

1. Write `synapse/loaders/bq_loader.py` that reads this layout and emits the same JSON shape the synthetic `bq_cache/`, `usage_history/`, `dq_rules/`, `lineage/` files use today.
2. Run `build_graph_from_sources(real_dir)` against just this one table — confirm the Streamlit UI renders correctly with real data.
3. Diff the resulting graph against the synthetic-data baseline — confirm all 10 sources contribute, all per-source view blocks populate, trust header tiles light up correctly.
4. Only after that smoke test passes, scale to the next 2 tables. Then to 5. Then to 56.

This is the "validate on real before adding layers" discipline. No new architecture until this loop closes cleanly on one table.

---

## What I already know (don't need from you)

- **MDM API shape** — confirmed via `scripts/check_mdm_access.py`, 193-col response captured. I'll build `synapse/loaders/mdm_loader.py` against the same array-wrapper response shape.
- **Glossary / acropedia format** — CSV with `Symbol, Definition, BusinessUnit, Region, EntryType`. Already loadable.
- **Metric catalog format** — CSV with `technical_name, business_name, calculation_logic, primary_data_product, business_synonyms, metric_grain, associated_domain`. Already loadable.
- **Table catalog format** — CSV with `table_name, IS IN DMP, company_domain, data_domain`. Already loadable.
- **Baseline LookML** — parsed via the `lkml` library; already proven in the curator agent.
- **GHE PAT access** — confirmed via `scripts/check_github_access.py`.

These six don't need a SQL extraction guide — the loaders just hit their existing APIs / read their existing files.
