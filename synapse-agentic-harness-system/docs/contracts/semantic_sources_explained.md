# Semantic & Governance Sources — What Each Artifact Contributes (Building to the Ten-Source Map)

> Transcribed verbatim from screenshots, in original order. Agent trace lines (`Updated todo list`, `Read [](...)`, `Ran terminal command: ...`) are preserved where they appeared. The ASCII relationship maps were line-wrapped by the mobile view; they are reflowed here into their intended column layout with all labels, counts, and arrows preserved exactly.

---

Updated todo list

**`std_tech_metadata_summary.csv`** — Extraction status for 46 tables from the Atlas `getStdTechMetadata` API.

| Column | What it tells you |
|---|---|
| `table_name` | The `axp-lumi.dw.*` table name |
| `status` | `success` or `error` |
| `error` | Error message if failed (empty on success) |
| `total_pages` / `total_no_of_elements` / `downloaded_elements` | Pagination counts |

All 46 tables succeeded. Some returned 1 element (single schema version), others returned 2 (e.g. multiple schema versions or dataset registrations). The per-table JSON files under `std_tech_metadata/` contain the full catalog entry (ownership, PII flags, column definitions, business terms per column).

**In short:** "Did we successfully pull the Atlas data catalog entry for every table?" Yes — 46/46.

---

**`business_terms.csv`** — ~4,400 enterprise business term definitions from Atlas `searchBusinessTerms`.

| Column | What it tells you |
|---|---|
| `businessTermId` | Numeric ID in Atlas |
| `businessTermName` | The canonical term (e.g. "Card Member 13 Digit Number (CM13)", "Merchant Category Code") |
| `businessTermStatus` | `Approved`, `Candidate`, `Under Review`, or `Rejected` |

Covers the full enterprise glossary — account terms, card terms, merchant terms, risk terms, regulatory terms, personal checking/savings terms, travel terms, etc. Status distribution is roughly: ~70% Approved, ~25% Candidate, remainder Under Review/Rejected.

**In short:** "What does the business call every data concept, and is the definition officially blessed?" This is the glossary that the `std_tech_metadata` column-level `businessMetadata` links back to.

---

**extracted_gold_queries.json** — 158 human-written prompt→SQL pairs (a "gold set" / evaluation benchmark).

| Field | What it tells you |
|---|---|
| `id` | Sequential identifier |
| `prompt` | Natural language question a business user would ask |
| `sql` | The verified correct BigQuery SQL (empty string = unanswered/pending) |
| `difficulty` | Difficulty tag (currently all null) |
| `source_row` | Row number from the original spreadsheet |

Coverage spans ~20 tables across domains: `custins_*` (card member insights), `risk_*` (risk/AR/writeoffs), `acqdw_*` (acquisitions), `pmdl_*` (merchant financial match), `cpbm_*` (lounge/benefit usage), `gsnmon_*` (servicing calls/chats), `tlsarpt_*` (travel sales), `fin_*` (AR daily balance, card status), `loyalty_rc_*` (offers), `lsc_*` (collections), `bmis_*` (paper comms). ~30 prompts have empty SQL (unanswered — marked for future completion).

**In short:** "Here are real business questions with hand-verified SQL answers, used to evaluate whether a text-to-SQL system produces the correct query."

---

**How all five sources relate:**

```text
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          │                                     35 certified KPIs
          └────────────────────────────────────►(approved SQL + guardrails)
                                                         ↓
                                            extracted_gold_queries.json
                                             158 prompt→SQL eval pairs
                                          (test: does the system produce
                                           the right query for each question?)
```

- **business_terms** = vocabulary
- **std_tech_metadata** = catalog (maps vocab to physical columns)
- **measures_catalog** = discovered usage patterns
- **metrics_dmp** = certified business metrics
- **extracted_gold_queries** = evaluation benchmark

---

Updated todo list

**extended_gmns_semantics.json** — 14 GMNS metric definitions with extended semantic fields, submitted for approval.

Each metric has everything in metrics_dmp.json plus additional fields:

| Field | What it adds beyond metrics_dmp.json |
|---|---|
| `calculation` | Step-by-step plain-English computation logic (grain, sign convention, filters, safe division) |
| `approvedDimensions` | Explicit list of valid slice-by dimensions (e.g. `time_period`, `bank_partner`, `region`, `MCC`) |
| `metricGrain` | The level at which the metric is computed (`transaction`, `distinct merchant/location`, `derived ratio`) |
| `metricScope` | Who should use it (analysts, partner teams, self-service marketplace users) |
| `requestor` | Who requested the metric be formalized |
| `submittedForApproval` | Boolean flag — all 14 are `true`, status = `Submitted` |

All 14 metrics are on `gms_transaction` and cover: spend, txn_count, merchant_count (ALIF), transaction_size, spend_per_merchant, transactions_per_merchant, local spend (2 methods), inbound spend (2 methods), cash spend, purchase spend, card-present spend, card-not-present spend.

**In short:** "The governance-ready, extended version of the GMNS merchant metrics — same SQL as metrics_dmp.json but with calculation logic, approved dimensions, grain, and scope added for formal review and approval."

---

**Updated relationship map across all six sources:**

```text
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          └────────────────────────────────────►35 certified KPIs
                                                (approved SQL + guardrails)
                                                         │
                                                         ↓
                                          extended_gmns_semantics.json
                                          14 GMNS metrics with calculation
                                          logic, approved dimensions, grain,
                                          scope — pending formal approval
                                                         │
                                                         ↓
                                            extracted_gold_queries.json
                                             158 prompt→SQL eval pairs
                                          (test: does the system produce
                                           the right query for each question?)
```

- **business_terms** = vocabulary
- **std_tech_metadata** = catalog (maps vocab to physical columns)
- **measures_catalog** = discovered usage patterns
- **metrics_dmp** = certified business metrics
- **extended_gmns_semantics** = enriched metric specs pending approval (calculation + dimensions + grain)
- **extracted_gold_queries** = evaluation benchmark

---

Updated todo list

The file attachment was summarized (content not shown). Let me read it to understand the structure.

```text
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/blue_business_insights.csv#1-1), lines 1 to 30

Ran terminal command: wc -l /Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/blue_business_insights.csv

Ran terminal command: head -1 blue_business_insights.csv && echo "---" && awk -F',' '{print $3}' blue_business_insights.csv | sort -u | head -30 && echo "---TOTAL DISTINCT TABLES---" && awk -F',' '{print $3}' blue_business_insights.csv | sort -u | wc -l && echo "---SAMPLE INSIGHT NAMES---" && awk -F',' '{print $1}' blue_business_insights.csv | sort -u | head -20

Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/blue_business_insights.csv#28-28), lines 28 to 60
```

**blue_business_insights.csv** — ~35,700 SQL filter/logic snippets extracted from production queries, mapped to table names.

| Column | What it tells you |
|---|---|
| `insight_name` | A short label for the filter or derived concept (e.g. `consumer`, `hotel`, `centurion_product`, `declined`, `north_america_region`) |
| `sql_logic` | The raw SQL predicate or CASE expression (e.g. `trim(business_unit_cd) in ('1')`, `se_typ = 'B'`, `country_cd in ('AU')`) |
| `table_name` | Which table/view the snippet applies to (e.g. `wwcas_authorization`, `risk_pers_acct_history`, `gms_merchant_char`) |

Content includes:
- **Business unit / segment filters** — consumer vs commercial, SBS/OPEN splits
- **Product code mappings** — centurion literals, loan products, plum, platinum, by IA code
- **Geography filters** — country-level (US, GB, AU, CA, ES) and regional groupings (EMEA, APAC, LAC, North America)
- **Merchant type codes** — `se_typ` A–M mappings (shop, hotel, restaurant, travel, gas, ATM, car rental, etc.)
- **Risk/credit filters** — FICO thresholds, approval/decline codes, credit scores
- **Derived CASE expressions** — age bands, vintage year logic, spend sign conventions, tenure calculations
- **Banking/fraud filters** — ACH, Actimize, RDE rule references, zero-value auth indicators

**In short:** "A library of ~35K reusable SQL building blocks — the WHERE clauses, CASE expressions, and code-to-label mappings that analysts embed in real queries against these tables." It's the implicit tribal knowledge of how column values map to business concepts, extracted into a lookup.

---

**Updated relationship map across all seven sources:**

```text
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          └────────────────────────────────────►35 certified KPIs
                                                (approved SQL + guardrails)
                                                         │
                                                         ↓
                                          extended_gmns_semantics.json
                                          14 GMNS metrics w/ calculation,
                                          dimensions, grain — pending approval
                                                         │
                                                         ↓
                                            extracted_gold_queries.json
                                             158 prompt→SQL eval pairs
                                                         │
                                                         ↓
                                            blue_business_insights.csv   ←—— feeds filter/logic
                                            ~35K SQL snippet→label mappings   into all of the above
                                              (the tribal knowledge layer)
```

- **business_terms** = vocabulary
- **std_tech_metadata** = catalog (columns → terms)
- **measures_catalog** = discovered patterns
- **metrics_dmp** = certified KPIs
- **extended_gmns_semantics** = enriched metric specs pending approval
- **extracted_gold_queries** = evaluation benchmark
- **blue_business_insights** = reusable SQL filter/logic library (the "how do analysts actually encode business concepts in SQL" layer)

---

Updated todo list

```text
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/data_cleaned.csv#1-1), lines 1 to 5

Ran terminal command: wc -l data_cleaned.csv && echo "---BU DIST---" && awk -F',' '{print $3}' data_cleaned.csv | sort | uniq -c | sort -rn | head -10 && echo "---REGION DIST---" && awk -F',' '{print $4}' data_cleaned.csv | sort | uniq -c | sort -rn | head -10 && echo "---ENTRY TYPE DIST---" && awk -F',' '{print $5}' data_cleaned.csv | sort | uniq -c | sort -rn | head -10 && echo "---SAMPLE ROWS---" && head -15 data_cleaned.csv
```

**data_cleaned.csv** — ~12,300 internal acronyms and glossary terms used across American Express.

| Column | What it tells you |
|---|---|
| `Symbol` | The abbreviation or term (e.g. `A/R`, `ALIF`, `MCC`, `POT`, `CM11`, `SE10`) |
| `Definition` | What it stands for (e.g. "Accounts Receivable", "Active Locations in Force", "Merchant Category Code") |
| `Business_Unit` | Which org owns the term (`All`, `GMNS`, `GSG`, `Technology`, `Finance`, `TLS`, etc.) |
| `Global_Region` | Geographic scope (`All`, `US`, `EMEA`, `JAPA`, `LACC`, `MX`) |
| `Entry_Type` | `Acronym` (~9,600) or `Glossary Term` (~950) |

Covers every corner of the business: card servicing, merchant services, risk, compliance, travel, technology, banking, finance, marketing. Many symbols have multiple definitions by business unit (e.g. `ABP` = "Abandoned Property" in general, "Automatic Bill Pay" in GMNS).

**In short:** "The company-wide Rosetta Stone — if someone says 'POT' or 'ALIF' or 'CBDS' in a meeting or a column name, this tells you what it means and which business unit uses it that way."

---

**Updated relationship map across all eight sources:**

```text
data_cleaned.csv
~12.3K acronyms & glossary   ——→  decodes jargon everywhere below
          │
          ↓
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          └────────────────────────────────────►35 certified KPIs
                                                         │
                                                         ↓
                                          extended_gmns_semantics.json
                                          14 GMNS metrics w/ calculation,
                                          dimensions, grain — pending approval
                                                         │
                                                         ↓
                                            extracted_gold_queries.json
                                             158 prompt→SQL eval pairs
                                                         │
                                                         ↓
                                            blue_business_insights.csv
                                            ~35K SQL snippet→label mappings
                                              (tribal knowledge layer)
```

- **data_cleaned** = acronym/jargon decoder (the universal translation layer)
- **business_terms** = formal data vocabulary
- **std_tech_metadata** = catalog (columns → terms)
- **measures_catalog** = discovered patterns
- **metrics_dmp** = certified KPIs
- **extended_gmns_semantics** = enriched metric specs pending approval
- **extracted_gold_queries** = evaluation benchmark
- **blue_business_insights** = reusable SQL filter/logic library

---

Updated todo list

Read skills folder files

```text
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/skill.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/knowledge.md)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/sample_codes.sql)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/chart_contract.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/metric_contracts.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/NewAccountsSkills/SBS_NewAccountsApprovalRate/qa_checks.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/SBS_WriteOffs/skill.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/SBS_WriteOffs/knowledge.md)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/SBS_WriteOffs/sample_codes.sql)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/SBS_WriteOffs/data_specs.md)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/CPS_RollRates/skill.yaml)
Read [](file:///Users/sjgandh/Library/CloudStorage/OneDrive-AmericanExpress/Desktop/bq-connect/skills/PortfolioAnalyticsSkills/CPS_RollRates/data_specs.md)
Read [](file:///Users/sjgandh/Library/Application%20Support/Code/User/workspaceStorage/41f1febccbe3454013de35d04bcc4dad/GitHub.copilot-chat/chat-session-resources/2b722b12-9a14-4025-bf23-a81f57e10d72/toolu_vrtx_01W9ztae6WFzf6BqVkYsBZRC__vscode-1787338467744/content.txt#1-1), lines 1 to 150
Read [](file:///Users/sjgandh/Library/Application%20Support/Code/User/workspaceStorage/41f1febccbe3454013de35d04bcc4dad/GitHub.copilot-chat/chat-session-resources/2b722b12-9a14-4025-…
```

```text
skills/
├── NewAccountsSkills/              (2 skills)
│   ├── SBS_NewAccountsApprovalRate/
│   └── SBS_NewAccountsSegmentation/
└── PortfolioAnalyticsSkills/    (8 skills)
    ├── CPS_ContributionAnalysis/
    ├── CPS_PortfolioSegmentation/
    ├── CPS_RollRates/
    ├── SBS_ContributionAnalysis/
    ├── SBS_PortfolioSegmentation/
    ├── SBS_Recoveries/
    ├── SBS_RollRates/
    └── SBS_WriteOffs/
```

**Each skill folder contains a standardized set of files:**

| File | What it provides |
|---|---|
| `skill.yaml` | Skill identity, routing triggers (`when_to_use` / `not_for`), required/optional inputs, tags, which files to load |
| `knowledge.md` | Domain rules: metric definitions, date conventions, segmentation rules, denominator policies, edge cases |
| `metric_contracts.yaml` | Machine-readable metric specs: exact SQL expressions, numerator/denominator contracts, allowed segments with CASE logic, output column names, `do_not_average_ratios` flags |
| `sample_codes.sql` | Ready-to-run SQL examples (SQLite dialect) demonstrating the metric with filters, segments, and safe division |
| `data_specs.md` | Table/column reference: source table, key columns, filter logic, join rules (present in most Portfolio skills) |
| `chart_contract.yaml` | Visualization rules: when to use line vs bar, axis mappings, top-N series policy, percentage formatting (present in approval-rate skill) |
| `qa_checks.yaml` | Automated validation rules: denominator-nonzero checks, distinct-key enforcement, ratio-averaging prohibition, small-base warnings (present in approval-rate skill) |

**Coverage by business segment:**
- **CPS** (Consumer): Portfolio Segmentation, Contribution Analysis, Roll Rates
- **SBS** (Small Business): Portfolio Segmentation, Contribution Analysis, Roll Rates, Write-Offs, Recoveries, New Accounts Approval Rate, New Accounts Segmentation

**In short:** "The skill packs are the instruction manuals for an AI agent — each one says *when* to activate, *what metric to compute*, *how to write the SQL*, *how to chart it*, and *what QA to run* for a specific analytical use case. They're the bridge between a business question and correct, governed output."

---

**Updated relationship map across all nine sources:**

```text
data_cleaned.csv
~12.3K acronyms & glossary   ——→  decodes jargon everywhere below
          │
          ↓
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          └────────────────────────────────────►35 certified KPIs
                                                         │
                                                         ↓
                                          extended_gmns_semantics.json
                                          14 GMNS metrics w/ calculation,
                                          dimensions, grain — pending approval
                                                         │
                                                         ↓
                                            extracted_gold_queries.json   ←—— skills/ provide the
                                             158 prompt→SQL eval pairs        "how to answer" logic
                                                         │                    for specific question types
                                                         ↓
                                            blue_business_insights.csv
                                            ~35K SQL snippet→label mappings
                                                         │
                                                         ↓
                                                      skills/
                                             10 analytical skill packs
                                          (routing + metric contracts +
                                            SQL + charts + QA checks)
```

- **data_cleaned** = acronym/jargon decoder
- **business_terms** = formal data vocabulary
- **std_tech_metadata** = catalog (columns → terms)
- **measures_catalog** = discovered patterns
- **metrics_dmp** = certified KPIs
- **extended_gmns_semantics** = enriched metric specs pending approval
- **extracted_gold_queries** = evaluation benchmark
- **blue_business_insights** = reusable SQL filter/logic library
- **skills/** = packaged agent instruction sets (routing → metric contracts → SQL → charts → QA)

Created 1 todo

---

Updated todo list

**`skills/Booking, Ticket Counts, and Sales — Gross, Cancelled, and Net Semantic Reference.md`** — The authoritative TLS (Travel & Lifestyle Services) metric rulebook for `tlsarpt_travel_sales`.

A 41-section reference covering:

| Section | What it defines |
|---|---|
| **Gross / Cancelled / Net concept** | `Net = Gross - Cancelled` for both counts and sales |
| **Booking Counts** | Always `COUNT(DISTINCT trip_id)` — never row counts |
| **Air Ticket Counts** | `SUM(net_tkt_ct)` — explicitly different from booking counts |
| **Sales** | `trans_usd_am` with sign convention (`ABS()` for cancellations) |
| **Date rules** | Gross → `rpt_dt`, Cancelled → `canc_dt` |
| **Status rules** | Non-Air gross = `ACTIVE + CANCELLED`; Air gross = `ACTIVE` only (cancelled Air has no `rpt_dt`) |
| **Hotel data-quality rule** | Mandatory exclusion of `ABS(trans_usd_am) > 100,000` for all Hotel sales |
| **Air exchange scenarios** | Full refund, refund with penalty, partial refund, even exchange (zero impact), add-collect (incremental only), residual refund |
| **Default metric selection** | "bookings" → Net Booking Count; "tickets" → Net Ticket Count; "sales" → Net Sales |
| **Common pitfalls** | 7 explicit anti-patterns (wrong date for cancellations, conflating booking/ticket counts, treating exchanges as new sales, etc.) |

**In short:** "The complete decision tree for any TLS travel question — given a product type and metric family, this tells you exactly which aggregation, status filter, date field, and product-specific rule to apply. It's the governance layer specifically for the `tlsarpt_travel_sales` table."

---

**Updated relationship map across all ten sources:**

```text
data_cleaned.csv
~12.3K acronyms & glossary   ——→  decodes jargon everywhere below
          │
          ↓
business_terms.csv              std_tech_metadata/            measures_catalog.json
~4,400 glossary terms    ←——→   46 table catalog entries  ←——→  6,223 mined patterns
  (enterprise vocab)             (column→term linkage)         (observed query usage)
          ↓                               ↓                              ↓
          │                               └──────────────┬───────────────┘
          │                                              ↓
          │                                     metrics_dmp.json
          └────────────────────────────────────►35 certified KPIs
                                                         │
                                                         ↓
                                          extended_gmns_semantics.json
                                          14 GMNS metrics w/ calculation,
                                          dimensions, grain — pending approval
                                                         │
                                                         ↓
                                            extracted_gold_queries.json   ←—— skills/ provide the
                                             158 prompt→SQL eval pairs        "how to answer" logic
                                                         │
                                                         ↓
                                            blue_business_insights.csv
                                            ~35K SQL snippet→label mappings
                                                         │
                                                         ↓
                                                      skills/
                                                      ├── 10 analytical skill packs
                                                      │   (routing → contracts → SQL → charts → QA)
                                                      │
                                                      └── TLS Semantic Reference
                                                          (booking/ticket/sales rulebook for
                                                           tlsarpt_travel_sales — Gross/Cancelled/Net
                                                           logic, Air vs non-Air, exchange scenarios,
                                                           Hotel $100K exclusion, date/status rules)
```

- **data_cleaned** = acronym/jargon decoder
- **business_terms** = formal data vocabulary
- **std_tech_metadata** = catalog (columns → terms)
- **measures_catalog** = discovered patterns
- **metrics_dmp** = certified KPIs
- **extended_gmns_semantics** = enriched metric specs pending approval
- **extracted_gold_queries** = evaluation benchmark
- **blue_business_insights** = reusable SQL filter/logic library
- **skills/** = packaged agent instruction sets + TLS semantic reference
