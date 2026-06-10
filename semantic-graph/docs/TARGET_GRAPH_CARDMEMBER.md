# Target-State Graph Design: `custins_customer_insights_cardmember`

> **Purpose.** Specify exactly what the graph should look like for this one table when every source is firing at full signal. The gold standard for completeness; reviewers check actual graph state against this.
>
> **Audience.** The engineer who will implement the 30% → 100% lumi extension + the KC loader + the Ingest Agent. Also the steward who reviews entity proposals against this target.
>
> **Scope.** One table — `custins_customer_insights_cardmember` (the AmEx cardmember-day P&L fact view). This spec generalizes to the other 52 tables in §13.

---

## 1. Table identity (the canonical anchor)

| Property | Value | Source contributing |
|---|---|---|
| `table_name` | `custins_customer_insights_cardmember` | mdm, bq, lumi, table_catalog |
| `bq_project.bq_dataset.bq_table` | `axp-lumi.dw.custins_customer_insights_cardmember` | mdm, bq |
| `asset_kind` | `View` (wraps `data.custins_customer_insights_cardmember`) | bq (DDL), mdm |
| `business_name` | `Customer Insights — Cardmember Daily` | mdm |
| `description` | `Cardmember-level daily snapshot fact carrying billed business, active flags, balances, and segment classification. The most-queried analytical table in cornerstone-data.` | mdm + llm_generated (combined) |
| `data_category` | `CP&A` | mdm |
| `data_sub_category` | `Cardmember` | mdm |
| `company_domain` | `Finance` | table_catalog |
| `feed_type` | `LumiFirst` | mdm |
| `table_type` | `DERIVED` (a VIEW) | mdm, bq |
| `partition_field` | `rpt_dt` (on underlying physical table; not on view) | mdm, bq |
| `partition_grain` | `daily` | bq (INFORMATION_SCHEMA.PARTITIONS) |
| `row_count_estimate` | 845,230,197 | bq (__TABLES__) |
| `size_bytes` | ~500 GB | bq |
| `last_modified` | yesterday | bq |
| `is_in_dmp` | True | table_catalog |
| `owner_team` | `cardmember-insights@example.com` | mdm |
| `tags` | `cornerstone`, `pii`, `daily-refresh`, `tier-1`, `gold`, `actively-queried` | mdm + table_catalog + bq labels |
| `lineage_upstream` | `pmdl_fin_business_volume_transaction_detail`, `fin_consumer_business_card_member_status`, `risk_indv_cust_hist` | bq (JOBS), mdm lineage, knowledge_catalog |
| `has_row_access_policy` | True (ONCOP-keyed RLS via `security.user_fin_oncop`) | bq (DDL parse) |
| `access_policy_predicate` | embedded in DDL | bq |
| **Expected confidence tier** | **`grounded`** | 7+ sources fuse → multi-source breadth gate |

---

## 2. The 11-source provenance breakdown for the Table node

What each source independently contributes to this single Table node:

| Source | Weight | What it asserts about the Table |
|---|---|---|
| `mdm` | 3 | business_name, description, owner, partition_field, data_category, sensitivity flags on each column |
| `bq` | 4 | row_count, size_bytes, last_modified, DDL (including RLS predicate), partition_field confirmation, clustering fields, table_type |
| `table_catalog` | 3 | is_in_dmp, company_domain, data_domain, table_name (canonical registry) |
| `baseline_lookml` | 3 | view_label, view_description, sql_table_name, sql_aliases, has_primary_key, primary_key_column |
| `corpus` | 1 / observation | Active use frequency (35 queries observed); table referenced in 90% of cardmember-domain queries |
| `usage` | 1 / observation | Top users, peak query hours (16-18 UTC), 30-day cost (~$25), query count (101/90d) — currently zero on this view due to dataset_id mismatch; populates when run against base table |
| `metric_catalog` | 5 | Metrics defined as sourced from this table (TBB, active_cardmembers, fico_band) |
| `glossary` | 5 | Acronyms used in column names that resolve to known terms (CM, TBB, etc.) |
| `dq_engine` | 4 | DQ rules attached (row_count, freshness, not_null on key cols, enum on coded cols) |
| `llm_generated` | 1 | Disambiguated description (only fills if mdm description sparse) |
| `knowledge_catalog` | 4 | KC entry name, KC aspects (auto-description, glossary links, scorecard), KC lineage |
| **Total breadth** | **11 distinct sources** | Drives tier to `grounded` |

This is the "click to expand into 11 sources" UI panel — each source independently asserting facts about the Table node, all fused via Provenance envelope.

---

## 3. Column nodes (190 total)

Every column gets a `Column` node. Cardmember table has 190 columns, breakdown:

| Category | Count | Examples | Source breadth expected |
|---|---|---|---|
| Identifiers (account-level) | 1 | `cm11` | 5 sources: mdm, bq, lumi (mdm_columns + columns_referenced + joins), corpus, baseline_lookml |
| Identifiers (customer-level) | 1 | `cust_xref_id` | 4 sources: mdm, bq, lumi, corpus |
| Identifiers (other) | ~3 | `acct_id` (if present), `prod_logo_id`, `dl_ind` | 3-4 sources |
| Dimensional (categorical) | ~30 | `product_group`, `sub_product_group`, `business_org`, `bus_seg`, `generation`, `data_source`, `card_type`, `issuer_ctry_id` | 4-5 sources (mdm, bq, corpus FilterValues, baseline_lookml dimensions, knowledge_catalog) |
| Financial measures (FLOAT64) | ~147 | `billed_business`, `gross_provision`, `gross_contribution_margin`, `account_servicing_opex`, `write_offs_*`, `total_revenue`, `lend_margin` | 3-4 sources (mdm, bq, corpus aggregations, baseline_lookml measures) |
| Time (DATE/DATETIME) | 4 | `rpt_dt`, `card_setup_dt`, `lumi_source_creation_time`, `lumi_ingestion_time` | 4 sources (mdm, bq, corpus date_functions, baseline_lookml dimension_groups) |
| Integer measures | 4 | `cust_xref_id`, `fico_score`, `accounts_in_force`, `del_rec_in` | 3-4 sources |
| Lumi pipeline metadata | 3 | `lumi_execution_id`, `lumi_source_creation_time`, `lumi_ingestion_time` | 2 sources (mdm, bq) |

### Per-column properties expected

For each Column node, populate:

| Property | Filled by |
|---|---|
| `name` | mdm + bq (must agree) |
| `data_type` | bq (authoritative) — must NOT be llm-asserted |
| `is_nullable` | bq (authoritative) |
| `is_partitioning` | bq + mdm (rpt_dt only) |
| `clustering_ordinal` | bq |
| `description` | mdm (if present), llm_generated (if mdm < 40 chars) |
| `business_name` | mdm |
| `is_primary` | baseline_lookml + corpus join evidence (when cm11 used as JOIN key in N queries) |
| `is_dedupe_key` | mdm (when populated) |
| `is_pii` | mdm + bq policy_tags + knowledge_catalog (3 independent witnesses for sensitive cols) |
| `pii_taxonomy` | mdm (canonical), policy_tags (BQ-asserted) |
| `is_critical_data_element` | mdm |
| `cardinality_bucket` | bq profiling (when RLS-bypass on base table) OR derived from corpus FilterValue observations |
| `approx_distinct` | bq profiling (when accessible) |
| `null_fraction` | bq profiling (when accessible) |
| `distinct_sample` | bq profiling (FilterValue nodes; derived from corpus when RLS blocks bq) |
| `candidate_role` (NEW — from llm enrichment) | llm_generated: `identifier` / `attribute` / `category` / `measure` / `timestamp` / `filter` / `code` |
| `candidate_entity_name` (NEW) | llm_generated: which Entity this column instantiates (only set when ≥2 corroborating sources) |
| `ai_generated_description` | llm_generated (from Synapse) + knowledge_catalog (from KC's Gemini-in-BQ) |
| `reference_count` | usage (number of queries that touched this column) |
| `is_filter`, `is_group_by`, `is_join_key` | corpus (set when observed in WHERE / GROUP BY / JOIN ON) |
| `is_coded` | mdm + corpus CASE-WHEN evidence |
| `resolved_by_table` | lookup-table resolution (drm_product_hier for card_prod_id) |
| `kc_path_name` | knowledge_catalog |
| `kc_aspects` | knowledge_catalog |

### Confidence tier expected per column

| Column class | Expected tier | Why |
|---|---|---|
| `cm11`, `cust_xref_id`, `rpt_dt`, `card_prod_id`, `product_group`, `bus_seg`, `business_org`, `data_source` | `grounded` | 4+ sources corroborate; multi-source breadth + weight pushes above 0.85 |
| Most FLOAT64 measures (billed_business, gross_provision, etc.) | `inferred` | mdm + bq + corpus aggregation; description from llm_generated if mdm sparse — 3 sources, doesn't quite hit grounded breadth |
| Obscure measures (rarely-used cols) | `guessed` to `inferred` | mdm + bq alone, no corpus observation |
| `fico_score` (PII, sensitive) | `grounded` | mdm + bq + corpus (FICO appears in queries) + policy_tags + lumi |
| Lumi pipeline metadata cols | `inferred` | mdm + bq only |

---

## 4. Entity nodes (the canonical business concepts)

Entities are minted from llm enrichment proposals + steward approval. Cardmember table surfaces these:

| Entity | Identified by columns | Materialized in tables | Relationships |
|---|---|---|---|
| **Cardmember Account** | `cm11` | This table + ~30 other tables sharing cm11 | belongs_to → Customer, holds → Card Product |
| **Customer** | `cust_xref_id` | This table + ~5 other tables | owns → Cardmember Account (1:N), holds → Card Product (M:N) |
| **Card Product** | `card_prod_id`, `product_group`, `sub_product_group` | This table + drm_product_hier (lookup) | belongs_to → Card Family, has_segment → Business Org |
| **Business Org** | `business_org` (Prop Lending, Charge, Cobrand, BIP, Vpay) | This table + transaction tables | classifies → Cardmember Account |
| **Business Segment** | `bus_seg` (CPS, OPEN, Commercial) | This table + customer status tables | classifies → Cardmember Account |
| **Generation Cohort** | `generation` (Boomer, Gen X, Millennial, Gen Z) | This table | classifies → Cardmember Account |
| **Issuer Country** | `issuer_ctry_id` (US, UK, etc.) | This table + many | localizes → Cardmember Account |

Expected confidence tier for Entity nodes: `inferred` (LLM proposed) until steward approves → `human_asserted` post-approval.

**The cardmember-vs-customer distinction is the #1 most important entity-modeling insight on this table.** It's the source of analyst confusion in NL questions. Both Entity nodes must exist; `cm11` IDs the account, `cust_xref_id` IDs the customer, the relationship `Customer → owns → Cardmember Account` is 1:N.

---

## 5. Metric nodes

From the metric catalog + corpus aggregations + baseline_lookml measures:

| Metric | Formula | Grain | Source contributing | Expected tier |
|---|---|---|---|---|
| `total_billed_business` | `SUM(billed_business)` | aggregated | metric_catalog + corpus + baseline_lookml | `grounded` |
| `active_cardmembers` | `COUNT(DISTINCT cm11) FILTER (WHERE accounts_in_force > 0)` | aggregated | metric_catalog + corpus | `inferred` |
| `fico_band` | `CASE WHEN fico_score >= 740 THEN 'Prime' WHEN fico_score >= 670 THEN 'Near-Prime' ELSE 'Sub' END` | row | metric_catalog + corpus case_whens + baseline_lookml | `grounded` |
| `gross_contribution_margin` | `SUM(gross_contribution_margin)` (already an additive measure) | aggregated | metric_catalog + corpus + mdm | `grounded` |
| `total_write_offs` | `SUM(write_offs_fees + write_offs_interest + write_offs_principal)` | aggregated | corpus aggregations | `inferred` |
| `total_revenue` | `SUM(discount_revenue + card_fees + interest_income + commissions)` | aggregated | corpus | `inferred` |
| `account_margin` | `SUM(account_margin)` | aggregated | mdm + corpus | `inferred` |
| `lend_margin` | `SUM(lend_margin)` | aggregated | mdm + corpus | `inferred` |
| `spend_margin` | `SUM(spend_margin)` | aggregated | mdm + corpus | `inferred` |
| `total_servicing_opex` | `SUM(account_servicing_opex + credit_servicing_opex + collection_fees)` | aggregated | corpus | `inferred` |
| `mr_expense_earn` | `SUM(mr_expense_earn)` (Membership Rewards expense) | aggregated | corpus + glossary (MR = Membership Rewards) | `inferred` |
| `cobrand_rewards_cost` | `SUM(cobrand_cashback_rewards + payment_to_partners)` | aggregated | corpus | `inferred` |

**~12+ explicit Metric nodes expected**, each with:
- `formula` (SQL expression)
- `business_name`
- `grain` (row vs aggregated)
- `domain` (Finance / Risk / Loyalty / etc.)
- `synonyms` (from glossary + baseline_lookml aliases)
- `sourced_from_table` = cardmember table
- `symmetric_aggregates_required` (bool — only for non-additive metrics)
- `evidence_count_in_corpus`

### COMPUTED_FROM edges

Every Metric → Column edge for source columns referenced in the formula. ~20-30 edges.

### SLICEABLE_BY edges

Every Metric → Column edge for columns observed as GROUP BY for that metric. ~40-50 edges (12 metrics × ~4 typical group-bys each).

---

## 6. Synonym nodes (context-keyed)

From glossary + baseline_lookml aliases + knowledge_catalog glossary + corpus column aliases + llm_generated:

| Surface form | Canonical entity | Business unit | Region | Evidence source |
|---|---|---|---|---|
| `CM` | `Cardmember Account` | Finance | Global | glossary + mdm |
| `CM` | `Communication Module` | Marketing | US | glossary (ambiguity flag → conflicts) |
| `CMID` | `Cardmember ID` | Finance | Global | glossary |
| `TBB` | `Total Billed Business` | Finance | Global | glossary + metric_catalog + corpus aliases |
| `NAA` | `New Accounts Acquired` | Acquisitions | US | glossary |
| `FICO` | `Fair Isaac Corporation Score` | Risk | US | glossary |
| `AA` | `Account Adjustment` | Finance | US | glossary (3-way ambiguity) |
| `AA` | `Account Acquisition` | Marketing | EU | glossary |
| `AA` | `Adverse Action` | Risk | US | glossary |
| `MCC` | `Merchant Category Code` | Merchant | Global | glossary |
| `MR` | `Membership Rewards` | Loyalty | Global | glossary + corpus |
| `PLAT` | `Platinum Card Product` | Loyalty | Global | glossary + corpus FilterValue |
| `Cardmember` | (canonical Entity) | — | — | mdm + table_catalog |
| `Total BB` | `Total Billed Business` | Finance | Global | baseline_lookml alias + corpus |
| `Billed Business` | `Total Billed Business` | Finance | Global | metric_catalog + mdm |
| `Engaged CMs` | `active_cardmembers` | Finance | Global | metric_catalog |
| `FICO Tier` | `fico_band` | Risk | US | metric_catalog |
| `Cust XRef` | `cust_xref_id` | Finance | Global | corpus alias + mdm |

**~20+ Synonym nodes** expected. The CM and AA ambiguities are the highest-signal items — they're the canonical AmEx disambiguation challenges, and they appear with `Provenance.conflicts` flagged for steward review when corpus context disagrees.

### HAS_SYNONYM edges

Every Synonym → (Entity or Metric or Column) edge. ~30-40 edges.

---

## 7. CodeMapping nodes (value → meaning)

From lookup-table resolution + corpus CASE-WHEN extraction + llm_generated:

| Column | Raw value | Human meaning | Source |
|---|---|---|---|
| `product_group` | `Delta` | Delta Air Lines co-branded card portfolio | corpus + llm_generated |
| `product_group` | `Platinum` | Platinum Card | corpus + glossary (PLAT) |
| `product_group` | `Gold` | Gold Card | corpus |
| `product_group` | `Centurion` | Centurion (Black Card) | corpus + llm_generated |
| `sub_product_group` | `Delta Gold` | Delta Gold tier | corpus |
| `sub_product_group` | `Delta Platinum` | Delta Platinum tier | corpus |
| `business_org` | `Prop Lending` | Proprietary Lending | mdm |
| `business_org` | `Charge` | Charge cards | mdm |
| `business_org` | `Cobrand` | Co-branded cards | mdm |
| `business_org` | `BIP` | (definition needed from steward) | conflict — needs steward |
| `business_org` | `Vpay` | (definition needed) | conflict — needs steward |
| `bus_seg` | `CPS` | Consumer (definition partially derived) | corpus + llm_generated |
| `bus_seg` | `OPEN` | Open Banking / OPEN small biz | corpus + llm_generated |
| `bus_seg` | `Commercial` | Commercial accounts | corpus |
| `data_source` | `cornerstone` | Cornerstone data warehouse (canonical) | corpus (90%+ queries filter on this) |
| `generation` | `Boomer` | Baby Boomer cohort | corpus |
| `generation` | `Gen X`, `Millennial`, `Gen Z` | Generational cohorts | corpus |
| `issuer_ctry_id` | `US`, `UK`, etc. | Issuer country code | mdm |

**~25+ CodeMapping nodes** expected. The `BIP` and `Vpay` definitions are flagged for steward; the rest are corpus-defensible.

### RESOLVED_BY edges

Each CodeMapping → lookup-table edge (or → corpus-event reference). ~15 edges.

---

## 8. FilterValue nodes (observed WHERE-clause literals)

From corpus filter extraction:

| Table | Column | Value | `count_obs` | `is_structural` |
|---|---|---|---|---|
| cardmember | `data_source` | `'cornerstone'` | 32 of 35 queries (91%) | **TRUE** — structural filter |
| cardmember | `bus_seg` | `'CPS'` | 8 | False |
| cardmember | `bus_seg` | `'OPEN'` | 5 | False |
| cardmember | `bus_seg` | `'Commercial'` | 3 | False |
| cardmember | `product_group` | `'Platinum'` | 6 | False |
| cardmember | `product_group` | `'Delta'` | 4 | False |
| cardmember | `card_prod_id` | various 3-digit codes | varies | False |
| cardmember | `generation` | `'Millennial'` | 2 | False |
| cardmember | `issuer_ctry_id` | `'US'` | 15 | False |
| cardmember | `rpt_dt` | partition predicates | all queries | **TRUE** — structural |

**~15+ FilterValue nodes** expected. The `data_source = 'cornerstone'` filter is the canonical safety note (queries omitting it likely double-count rows from legacy backfills).

### ALWAYS_FILTER edges

Two ALWAYS_FILTER edges:
- Explore (cardmember table) → FilterValue (data_source = cornerstone), `is_always_filter: true`
- Explore → FilterValue (rpt_dt partition predicate), `is_always_filter: true`

---

## 9. DataQualityRule nodes

From Auto DQ profile synthesis (when BQ profiling RLS-bypass works) + knowledge_catalog scorecard + llm_generated suggestions:

| Rule | Target | Kind | Threshold | Last status |
|---|---|---|---|---|
| Row count above floor | (table-level) | `row_count` (Volume dim) | `> 800M` | pass |
| Freshness | (table-level) | `freshness` (Freshness dim) | `< 24h` | pass |
| cm11 not null | `cm11` | `not_null` (Completeness dim) | `null_pct < 0.01` | pass |
| cm11 unique within (cm11, rpt_dt) | `cm11` × `rpt_dt` | `uniqueness` (Uniqueness dim) | grain check | pass |
| rpt_dt not null | `rpt_dt` | `not_null` | `null_pct < 0.001` | pass |
| product_group in enum | `product_group` | `enum` / `set` (Validity dim) | values from CodeMapping | pass |
| bus_seg in enum | `bus_seg` | `enum` | CPS/OPEN/Commercial | pass |
| fico_score range | `fico_score` | `range` (Accuracy dim) | 300 ≤ value ≤ 850 | warning (some null) |
| billed_business non-negative | `billed_business` | `range` | `>= 0` (typically) | pass |
| data_source in known set | `data_source` | `set` (Validity dim) | values from FilterValue obs | pass |

**~12-15 DataQualityRule nodes** expected. Each tagged with `dimension` from KC's 7-enum (Freshness, Volume, Completeness, Validity, Consistency, Accuracy, Uniqueness).

### VALIDATED_BY edges

Each Column → DataQualityRule edge. ~10-12 edges.

---

## 10. User nodes (top analysts on this table)

From `JOBS_BY_PROJECT` (currently empty due to view-vs-base-table dataset_id mismatch; fix per stress test §6 item 8):

| User | Team | Query count | Bytes billed |
|---|---|---|---|
| `risk-modeling-1@example.com` | Risk Modeling | 142 | 5 GB |
| `finance-fpa-3@example.com` | Finance FP&A | 109 | 3 GB |
| `risk-modeling-2@example.com` | Risk Modeling | 92 | 2 GB |
| ... | ... | ... | ... |

**~8-15 User nodes** expected once base-table JOBS query lands.

### QUERIED_BY edges

Each Table → User edge. ~10 edges with `query_count` and `bytes_billed` properties.

### Stewards (derived from top users)

Top users are candidate stewards. The graph auto-suggests stewards based on activity:
- `risk-modeling-1@example.com` becomes a candidate steward for the cardmember table (highest activity)
- Steward verdicts get tied to User nodes via a future `APPROVED_BY` / `REJECTED_BY` edge type

---

## 11. Lineage edges (UPSTREAM_OF)

From BQ JOBS (when base-table accessible) + MDM lineage hints + KC lineage + DDL parsing of view:

**Upstream:**
- `pmdl_fin_business_volume_transaction_detail` → cardmember (via transaction aggregation)
- `fin_consumer_business_card_member_status` → cardmember (via status JOIN)
- `risk_indv_cust_hist` → cardmember (via FICO + risk attributes)
- `drm_product_hier` → cardmember (as lookup for card_prod_id, product_group)

**Downstream:**
- Likely → various dashboards / aggregate tables (TBD from BQ JOBS once visible)

Edges carry `source="bq"` or `source="mdm"` or `source="knowledge_catalog"` depending on which observed them. Multi-source agreement on a lineage edge promotes it to `grounded`.

### KC adds: PII flow propagation

When KC lineage is available, each lineage edge carries `propagates_pii: bool`. The cardmember table contains PII (cm11, fico_score, billed_business as Sensitive>FinancialAmount); KC's PII flow tracking propagates this through downstream lineage.

---

## 12. Cross-table relationships (related tables)

From corpus JOIN observations + MDM external_references + KC entry links:

| Related table | Via columns | Cardinality | Source |
|---|---|---|---|
| `drm_product_hier` | `card_prod_id` ↔ `card_prod_id` | many_to_one | corpus + mdm |
| `pmdl_fin_business_volume_transaction_detail` | `cm11` ↔ `cm11` + `rpt_dt` ↔ `txn_dt` (date range) | one_to_many | corpus |
| `fin_consumer_business_card_member_status` | `cm11` ↔ `cm11` | one_to_one | corpus + mdm |
| `risk_indv_cust_hist` | `cust_xref_id` ↔ `cust_xref_id` + `rpt_dt` | one_to_one | corpus |
| `custins_customer_insights_product` | `card_prod_id` ↔ `card_prod_id` | many_to_one | corpus |
| `gms_merchant_full_hier` | (via transaction join — indirect) | — | indirect |
| `loyalty_rc_redemption` | `cm11` ↔ `cm11` | one_to_many | corpus (if any cardmember loyalty queries) |

### RELATES_TO / EQUIVALENT_TO edges

Each shared column → EQUIVALENT_TO edge between Column nodes. ~10-20 edges with `count` reflecting JOIN observation frequency.

---

## 13. Generalizing to the other 52 tables

The same pattern applies. Each table will surface:

- **1 Table node** with 7-11 source provenance breakdown
- **N Column nodes** (N varies; cardmember has 190, dim tables have ~20)
- **K Metric nodes** (K varies by domain; finance tables have most)
- **L Synonym nodes** (most ambiguous acronyms repeat across tables)
- **M CodeMapping nodes** (each coded column gets its enum)
- **P FilterValue nodes** (each table's structural filters)
- **Q DataQualityRule nodes** (~10-12 per table)
- **R User nodes** (top users — overlap with other tables)
- **Lineage edges** to upstream + downstream tables
- **Entity nodes** (Cardmember, Customer, Card Product, etc.) — shared across tables; one entity, many tables materializing it

**Cross-table sharing:**
- Entities are shared (Cardmember entity has `materialized_in_tables: list` of all tables containing cm11)
- Synonyms are shared (the CM acronym applies everywhere)
- Users are shared (one user queries many tables)
- Lineage forms the table DAG

**Estimated scale at 53 tables:**
- ~53 Table nodes
- ~5,000 Column nodes (avg 95 cols × 53 tables)
- ~200 Metric nodes
- ~50 Synonym nodes (most are shared, not per-table)
- ~30 Entity nodes (highly shared)
- ~150 CodeMapping nodes
- ~500 FilterValue nodes
- ~600 DataQualityRule nodes (~12 per table)
- ~40 User nodes (highly shared)
- ~10,000 edges total

**Graph size at 53 tables:** ~7,000 nodes / ~10,000 edges. In-memory dict is fine until ~50k nodes; we have headroom. The stress test's persistence-via-SQLite recommendation kicks in for this scale; not blocking, but nice.

---

## 14. What the inspector returns (the agent's view)

`inspect_table('custins_customer_insights_cardmember')` should return a dict with:

```
{
  "identity": { ... all of §1 above ... },
  "fused_view": {
    "confidence_tier": "grounded",
    "confidence_score": 0.95,
    "n_sources_agree": 7,  # of 11
    "sources_contributed": [
      "mdm", "bq", "table_catalog", "baseline_lookml", "corpus",
      "dq_engine", "knowledge_catalog"  # + others as they fire
    ],
    "evidence_count": ~250,  # cumulative across all sources
    "conflicts": []  # or list of unresolved source disagreements
  },
  "per_source_view": {
    "mdm": { ... }, "bq": { ... }, "corpus": { ... }, ...
    "knowledge_catalog": { ... }   # NEW
  },
  "columns": [ ... 190 column objects per §3 ... ],
  "metrics": [ ... 12+ metric objects per §5 ... ],
  "entities": [ ... 7 entity objects per §4 ... ],   # NEW — currently not in inspector
  "synonyms": [ ... 20+ synonym objects per §6 ... ],   # NEW
  "code_resolutions": [ ... 25+ from §7 ... ],
  "filter_values": [ ... 15+ from §8 ... ],   # currently mixed into columns
  "dq_rules": [ ... 12+ from §9 ... ],
  "usage": { top_users: [...], peak_hours: [...] },   # currently empty due to RLS
  "lineage": { upstream: [...], downstream: [...] },
  "related_tables": [ ... 7 from §12 ... ],
  "governance": { ... },
  "data_quality": { completeness, consistency, freshness, dimensions[] },   # extend dimensions to 7
  "example_queries": [ ... top 5-10 from corpus ... ],   # NEW from KC + corpus
  "rls_predicate": "..." # NEW — the DDL-derived ONCOP predicate text
}
```

**New blocks to add to the inspector** (currently missing):
- `entities[]` — proposed/approved entities materialized by this table
- `synonyms[]` — surfaces all synonyms that resolve to a column/metric/entity in this table
- `example_queries[]` — top corpus queries that touch this table
- `rls_predicate` — DDL-parsed ONCOP predicate

---

## 15. Verification checklist — how do we know the graph is complete?

For the cardmember table specifically:

- [ ] Table node has `n_sources_agree >= 7` and tier = `grounded`
- [ ] 190 Column nodes exist with correct data_types from BQ
- [ ] `cm11` and `cust_xref_id` are distinct Column nodes with different `candidate_entity_name` (Cardmember Account vs Customer)
- [ ] Entity nodes exist for Cardmember Account, Customer, Card Product, Business Org, Business Segment, Generation Cohort, Issuer Country (7+)
- [ ] At least 12 Metric nodes (TBB, active_cardmembers, fico_band, 9+ others)
- [ ] At least 20 Synonym nodes including the CM/AA/DM ambiguities surfaced with conflicts
- [ ] At least 25 CodeMapping nodes (product_group, sub_product_group, business_org, bus_seg, generation, data_source enum members)
- [ ] FilterValue: `data_source = 'cornerstone'` exists with `is_structural=True` and `count_obs >= 30 of 35`
- [ ] 12+ DataQualityRule nodes spanning all 7 KC dimensions
- [ ] At least 4 UPSTREAM_OF lineage edges (to the 4 upstream tables in §11)
- [ ] At least 7 related-tables entries via EQUIVALENT_TO observation evidence
- [ ] When KC loader fires, `knowledge_catalog` appears in Table.provenance.sources
- [ ] Inspector dict has all blocks from §14 populated
- [ ] No Column node has `source='llm_generated'` alone — every column has bq or mdm corroboration
- [ ] No Column has confidence > `inferred` when sources = `['llm_generated']` alone
- [ ] `Provenance.conflicts` correctly populated for: CM ambiguity (Finance vs Marketing), BIP/Vpay undefined business_org codes, any MDM-BQ schema disagreements
- [ ] Failed-query corrections from BQ section 4.5 surface somewhere (as UnansweredQuestion nodes or as `Column.naming_correction` properties)

**When all 15 checks pass, the cardmember graph is production-quality.** This is the verification gate before scaling to 53 tables.

---

## 16. Build sequence to reach this target

In priority order (each step gets us closer to the target above):

| Step | What | Effort | Why first |
|---|---|---|---|
| 1 | Fix LLM-tier calibration bug (stress test #2) | 30 min | Without this, llm_generated facts spuriously promote |
| 2 | Extend `lumi_loader` to consume 100% of session1_output.json fields (per APPENDIX of stress test) | 1 day | Currently using 30%; this adds aggregations / case_whens / joins / filters as first-class facts |
| 3 | Refactor `_ingest_corpus` to drop regex and use lumi-pre-extracted facts directly | 0.5 day | Higher fidelity; sqlglot beats regex |
| 4 | Build `kc_loader.py` + add `knowledge_catalog` source | 3 days | Source #11; adds AI-description corroboration + glossary hierarchy + lineage |
| 5 | Extend DataQualityRule to use KC's 7-dimension enum | 1 hour | Trivial; standards alignment |
| 6 | Add `Entity` nodes to inspector output (currently aggregated into columns) | 0.5 day | Required for §4 |
| 7 | Add `UnansweredQuestion` node type (per stress test §3 item 9) for failed-query signal | 0.5 day | Captures BQ section 4.5 gold signal |
| 8 | Add base-table JOBS query path (RLS bypass) for usage telemetry | 1 day | Currently empty top_users; unblocks §10 |
| 9 | Build Entity proposal review flow (steward UI in Streamlit) | 1 day | Required to mature `inferred` entities to `human_asserted` |
| 10 | Run verification checklist §15 against cardmember graph | 0.5 day | The gate before scaling |

**Total: ~9-10 days to production-quality cardmember graph.** Then ~1 week each to scale to remaining 52 tables (most of that time is debugging per-table data quirks, not new architecture).

---

## 17. Final answer to "what should the graph look like for this table?"

It should be a graph where:

1. **The Table is grounded by 7-11 independent sources.** Every fact about it has provenance.
2. **190 Columns are typed correctly from BQ, described by MDM or LLM (capped at inferred unless multi-source corroborated), with PII classification from MDM + policy tags + KC.**
3. **The two key entities (Cardmember Account, Customer) are distinct and the 1:N relationship between them is explicit.**
4. **12+ canonical metrics are materialized as nodes with formulas, grains, business names, and synonyms — sliceable by the dimensional columns observed in corpus.**
5. **The CM / AA / DM acronym ambiguities are surfaced as Synonym nodes with `Provenance.conflicts` flagging the disambiguation challenge.**
6. **The structural `data_source = 'cornerstone'` filter is a first-class FilterValue node with `is_structural=True` and a safety note about omission causing double-counting.**
7. **DQ rules span all 7 KC quality dimensions, including the new ones (Volume, Validity, Accuracy, Uniqueness) we currently don't model.**
8. **Lineage shows the 4 upstream tables (transaction, status, risk, product) with PII propagation marked.**
9. **Cross-table relationships (Cardmember Account spans 30+ tables) are explicit via the Cardmember Entity node's `materialized_in_tables` list.**
10. **Steward review queue is populated with entity proposals + business_org code resolutions (BIP, Vpay) + naming-correction failed-query items.**

A user asking the Consumer Agent "How many active Platinum cardmembers spent over $5k in Q1?" should get back fully-grounded BigQuery SQL because every fact the agent uses is `grounded` or `inferred`, never `guessed`, and every fact is cited with its source list.

**This is what done looks like.**
