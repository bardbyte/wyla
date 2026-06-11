# Knowledge Catalog — Total Granularity Reference

> **Purpose.** Be the single doc you read once and know everything about Google Cloud Knowledge Catalog (2026): the vocabulary, the architecture options, the access-control model, the human + agent workflows, and exactly how Synapse fits as a confidence-typed reasoning layer on top.
>
> **Audience.** AmEx engineer (you) — visual learner, wants structured ASCII diagrams + tables + worked scenarios.
>
> **Companion to.** `KNOWLEDGE_CATALOG_DEEP_DIVE.md` (35-feature inventory), `KC_VS_KC_PLUS_SYNAPSE.md` (architecture decision).

---

## Table of contents

1. [The big picture (2026 state)](#1-the-big-picture)
2. [Vocabulary](#2-the-vocabulary)
3. [What Synapse maps from KC](#3-what-synapse-maps-from-kc)
4. [Where KC ends and Synapse begins](#4-where-kc-ends-and-synapse-begins)
5. [Three hub-and-spoke patterns](#5-three-hub-and-spoke-patterns)
6. [The worked scenario — 2 projects × 3 tables](#6-the-worked-scenario)
7. [Human access workflow](#7-human-access-workflow)
8. [Agentic access workflow](#8-agentic-access-workflow)
9. [Service identities for agents — full lifecycle](#9-service-identities-for-agents)
10. [Governance + audit](#10-governance--audit)
11. [AmEx-specific recommendations](#11-amex-specific-recommendations)

---

## 1. The big picture

**Apr 22, 2026** — Google rebranded Dataplex Universal Catalog → **Knowledge Catalog**. Positioning shifted from "metadata inventory" to "**dynamic context engine for AI agents**." Three pillars:

```
┌─────────────────────────────────────────────────────────────┐
│              KNOWLEDGE CATALOG  (2026)                      │
│                                                             │
│  AGGREGATION         ENRICHMENT          AGENT SEARCH       │
│  ───────────         ──────────          ────────────       │
│  Native + federated  Gemini-powered      High-precision     │
│  context across      continuous          semantic search    │
│  BQ, AlloyDB,        glossary,           with access-       │
│  Spanner, Looker,    descriptions,       control-aware      │
│  Salesforce, SAP,    relationships,      retrieval (agents  │
│  Workday, Atlan, …   data products       see only what      │
│                                          they're authorized │
│                                          to see)            │
└─────────────────────────────────────────────────────────────┘
```

**Why agents matter for catalog design:** Pre-2026 catalogs were built for humans browsing. Post-2026 they're built for agents querying. The whole IAM + audit + sharing model evolved to handle non-human consumers.

**Where Synapse sits:** KC is the **org-wide context substrate**. Synapse is the **AmEx-specific confidence-typed reasoning layer** that fuses KC (1 source) with 10 others (MDM, corpus, BQ direct, baseline LookML, glossary, metric catalog, table catalog, usage, DQ, AI). KC tells you *what exists*. Synapse tells you *what to trust and why*.

---

## 2. The vocabulary

You'll see these 12 terms everywhere. Internalize them.

| Term | What it is | Example |
|---|---|---|
| **Entry** | A registered data asset in KC | `axp-lumi.dw.custins_customer_insights_cardmember` (BQ view as an entry) |
| **Entry Group** | A container for entries; unit of access control | `axp-lumi-cardmember-entries` |
| **Entry Type** | A typed template defining required Aspects for an entry | `bq_table_entry_type` (system-provided), `cardmember_table_type` (custom) |
| **Aspect** | A bundle of structured metadata attached to an entry or a column path | `pii_classification: {role_id: Sensitive>FinancialAmount}` on `billed_business` |
| **Aspect Type** | Reusable template for an Aspect (defines its fields + cardinality) | `data_quality_scorecard` type — defines `last_run_status`, `pass_rate`, `dimension` |
| **Path** | A column within an entry; column-level metadata target | `axp-lumi.dw.custins_customer_insights_cardmember.fico_score` |
| **Glossary** | Hierarchical taxonomy of business terms | "Cardmember" → "Account Member" → "Active Cardmember" (3-level hierarchy) |
| **Entry Link** | A typed relationship between two entries (or paths) | `cm11` (cardmember table column) → `cm11` (transaction table column), link type `synonym` |
| **Entry Link Type** | Reusable template defining the relationship meaning | `synonym`, `related`, `schema-join`, `definition` (KC's 4 built-in types) |
| **Data Product** | A bundled set of entries with shared governance + SLA + access policy | "Q1 Cardmember P&L Product" = cardmember table + 5 dimensional tables + DQ rules + access policy |
| **Linked Dataset** | A read-only reference dataset that points at a shared dataset in another project (Analytics Hub mechanism) | Project 2 subscribes to "Cardmember Insights Listing" → gets a linked dataset pointing at Project 1's data |
| **Service Agent** | The Google-managed service account KC uses to operate on your project's resources | `service-PROJECT_NUMBER@gcp-sa-dataplex.iam.gserviceaccount.com` (auto-created when KC API enabled) |

**A picture is worth a thousand definitions:**

```
                      ┌──────────────────────────────┐
                      │      ENTRY GROUP             │
                      │  axp-lumi-cardmember-entries │
                      └────────────┬─────────────────┘
                                   │
                  ┌────────────────┼────────────────┐
                  │                │                │
              ┌───▼──┐         ┌───▼──┐         ┌───▼──┐
              │ENTRY │         │ENTRY │         │ENTRY │
              │ table│         │ view │         │ ML   │
              │ A    │         │ B    │         │model │
              └──┬───┘         └──┬───┘         └──┬───┘
                 │                │                │
              ┌──▼──┐          ┌──▼──┐          ┌──▼──┐
              │PATH │          │PATH │          │PATH │
              │col1 │          │col1 │          │feat1│
              │col2 │          │col2 │          │feat2│
              │col3 │          └─────┘          └─────┘
              └──┬──┘
                 │
              ┌──▼─────────────────────────────┐
              │  ASPECTS attached to col1:     │
              │  • pii_classification          │
              │  • data_quality_scorecard      │
              │  • business_glossary_link →    │
              │       Glossary("Cardmember")   │
              └────────────────────────────────┘
```

**Aspects vs. Glossary** — easy confusion point:

- **Aspect** = structured metadata bundle that *describes a single entry/path*. "This column has PII role X." "This column passed DQ check Y at 99.2%."
- **Glossary** = hierarchical *taxonomy of terms* (the AmEx vocabulary). A glossary term can be linked from many entries via `entry_link` of type `definition`.

Aspects are facts *about* an entry. Glossary terms are concepts *referenced by* an entry.

---

## 3. What Synapse maps from KC

Per-fact, where does KC data land in Synapse's graph?

| KC fact | Synapse node/edge | Source tag | Tier impact |
|---|---|---|---|
| Entry for `bq_table_X` | `Table` node — properties update | `source="knowledge_catalog"` | +1 source breadth → likely promotes to `grounded` |
| Entry's `overview` aspect | `Table.description` (only if MDM description sparse) | `source="knowledge_catalog"` | n/a |
| Entry's `contact` aspect | `Table.owner_team` (corroborates MDM) | `source="knowledge_catalog"` | promotes ownership claim |
| Path (column) with `pii_classification` aspect | `Column.is_pii`, `Column.pii_taxonomy` (corroborates MDM) | `source="knowledge_catalog"` | promotes PII claim to `grounded` (triple-witnessed) |
| `data_quality_scorecard` aspect | `DataQualityRule` node | `source="dq_engine"` (KC scorecard sources from Auto DQ) | n/a |
| Glossary term linked to entry | `Synonym` node — `canonical_entity` field; hierarchy preserved | `source="knowledge_catalog"` | adds the missing hierarchy our flat Synonym lacked |
| Entry link of type `synonym` | `HAS_SYNONYM` edge | `source="knowledge_catalog"` | n/a |
| Entry link of type `definition` | New edge type `DEFINED_BY` (Synapse schema add) | `source="knowledge_catalog"` | n/a |
| Entry link of type `schema-join` | `EQUIVALENT_TO` edge | `source="knowledge_catalog"` | corroborates corpus-derived joins |
| KC lineage event with PII flow | `UPSTREAM_OF` edge with `propagates_pii: bool` property | `source="knowledge_catalog"` | sets the PII-flow flag we currently miss |
| KC auto-generated description | `Column.ai_generated_description` (alongside our own enrichment's output) | `source="knowledge_catalog"` | corroborates Synapse enrichment's proposed description |
| Data Product bundle | (Future) New node type `DataProduct` with `member_tables[]` | `source="knowledge_catalog"` | n/a |

**Two key claims this lets Synapse make:**

1. **"This is grounded because BOTH our MDM AND Google's KC say so"** — multi-source breadth becomes evidence-based, not just our own pipeline.
2. **"Our LLM proposed description Y; Google's KC AI proposed description X; they agree → promote to `inferred` with corroboration"** — Gemini's two independent passes count as two sources.

---

## 4. Where KC ends and Synapse begins

Honest division of labor. Each has things the other can never do.

| Capability | KC ✅ | Synapse ✅ | Notes |
|---|---|---|---|
| Discover BQ assets across the org | ✅ | ❌ | KC's reach via BigQuery API integration is broader than ours |
| Auto-generate column descriptions | ✅ (via Gemini in BQ) | ✅ (via our skill.md) | Different prompts; we treat them as separate witnesses |
| PII classification per column | ✅ (policy tags) | ✅ (via MDM) | KC corroborates MDM; both → `grounded` |
| Auto DQ rules + scorecards | ✅ (via Auto DQ) | ⚠ (we synthesize from corpus when Dataplex absent) | KC is authoritative when available |
| Hierarchical business glossary | ✅ | ⚠ (flat today; can extend) | Adopt KC's hierarchy or build our own |
| Lineage with PII propagation | ✅ | ⚠ (BQ JOBS-based, no PII propagation) | KC is better here |
| Native multi-system aggregation (Spanner, AlloyDB, Looker, SAP, Salesforce) | ✅ | ❌ | Not in scope for Synapse |
| Per-fact 7+-source provenance + calibrated confidence | ❌ | ✅ | **Our moat.** KC stores facts; Synapse stores facts + who said + how much we trust |
| Tribal-knowledge corpus mining (sqlglot on 35 cardmember queries) | ❌ | ✅ | **Our moat.** KC ingests job history; not query semantics |
| Context-keyed synonyms (CM = Cardmember in Finance, Communication Module in Marketing) | ❌ | ✅ | **Our moat.** KC glossary is flat per-term |
| Steward arbitration loop with rejection memory | ❌ | ✅ (designed; pending Streamlit UI) | **Our moat.** |
| Custom skill.md for enrichment rules (cap LLM at `inferred`) | ❌ | ✅ | **Our moat.** KC's AI runs at Google's discretion |
| Failed-query corrections (`fico` → `fico_score`) | ❌ | ✅ | **Our moat.** Comes from corpus extraction |
| Honest "we don't know" with low-confidence stamps | ❌ | ✅ | **Our moat.** KC surfaces only what it indexed |
| AmEx-specific column-name conventions | ❌ | ✅ | **Our moat.** |

**The slogan:**
> **KC for discovery and breadth. Synapse for reasoning and depth.**

---

## 5. Three hub-and-spoke patterns

KC supports multiple architectural patterns. Pick based on org constraints.

### Pattern A — Platform-level hub-and-spoke (KC as the org-wide hub)

```
                ┌─────────────────────────────────────┐
                │     KC (in HUB PROJECT)             │
                │     prj-d-knowledge-catalog         │
                │                                     │
                │  Entry Groups:                      │
                │  - global-finance-entries           │
                │  - global-risk-entries              │
                │  - global-loyalty-entries           │
                │  - global-acquisitions-entries      │
                │                                     │
                │  Glossary:                          │
                │  - amex-canonical-glossary          │
                │                                     │
                │  Service Agent:                     │
                │  service-{hub_pn}@gcp-sa-dataplex…  │
                └────────────┬────────────────────────┘
                             │ (KC reads metadata via service agent)
                             ▼
        ┌────────────────────────────────────────────────┐
        │  SPOKE PROJECTS (the data warehouses)          │
        │                                                │
        │  Spoke 1: prj-d-finance-dw                     │
        │   • table_1, table_2, table_3 (Finance dom)    │
        │                                                │
        │  Spoke 2: prj-d-risk-dw                        │
        │   • table_1, table_2, table_3 (Risk dom)       │
        │                                                │
        │  Spoke 3: prj-d-loyalty-dw                     │
        │   • table_1, table_2, table_3 (Loyalty dom)    │
        │                                                │
        │  Each spoke grants KC service agent (hub)      │
        │  READ access to its INFORMATION_SCHEMA + DDL   │
        └────────────────────────────────────────────────┘
```

**Properties:**
- Hub project owns the catalog; spokes own the data.
- One KC instance, one place to search across all spokes.
- IAM enforcement: KC service agent must be granted at each spoke (`roles/bigquery.metadataViewer`).
- Cross-spoke entry links allowed (e.g., Finance.table_2 ↔ Risk.table_3 via `EQUIVALENT_TO`).
- Centralized glossary lives in hub; available to all spokes.

**AmEx fit:** **Strong.** This matches how AmEx already structures projects by data domain.

### Pattern B — Data-warehouse hub-and-spoke (Lumi-style)

```
        ┌───────────────────────────────────────┐
        │       HUB DATA WAREHOUSE              │
        │       (prj-d-lumi)                    │
        │                                       │
        │  dataset 'dw': fact tables (the 53)   │
        │  dataset 'staging': intermediate      │
        │  dataset 'mart': aggregated views     │
        │                                       │
        │  Catalog lives IN this project too:   │
        │  - All entries reside here            │
        │  - One Entry Group: 'dw-entries'      │
        └────────────────┬──────────────────────┘
                         │
                         ▼ (datasets shared OUT via Analytics Hub)
        ┌───────────────────────────────────────┐
        │   CONSUMER PROJECTS                   │
        │                                       │
        │  prj-d-finance-analytics  → linked DS │
        │  prj-d-risk-analytics     → linked DS │
        │  prj-d-loyalty-analytics  → linked DS │
        │                                       │
        │  Each consumes via subscription;      │
        │  reads as if local, but RLS + access  │
        │  follow source.                       │
        └───────────────────────────────────────┘
```

**Properties:**
- Data physically resides in one place (hub warehouse).
- Catalog discovery + data access both flow through the hub.
- Consumers don't host data; they subscribe via Analytics Hub.
- KC indexes the hub's BQ datasets; consumers see linked datasets in their projects' KC too.

**Where Analytics Hub fits:** the *sharing mechanism*. Hub publishes "Cardmember Insights" Listing in an Exchange. Finance project subscribes → gets a Linked Dataset pointing at the hub's data. The linked dataset is **read-only**; queries flow back to the source's IAM + RLS.

**AmEx fit:** **Already partially this.** `axp-lumi.dw` is the hub. Consumer projects pull via existing BQ IAM. Migration to Analytics Hub-based sharing adds publisher metrics + data egress restrictions + structured discoverability.

### Pattern C — Federated catalog (multi-vendor)

```
   Atlan        Collibra        DataHub      KC (Google)
    │              │              │              │
    └──────────────┴──────────────┴──────────────┘
                          │
                          ▼
                  ┌───────────────┐
                  │  KC (federated)│
                  │  imports from  │
                  │  others        │
                  └───────────────┘
                          │
                          ▼
                       AGENTS
```

**Properties:**
- KC has connectors to Atlan, Collibra, DataHub, etc. (per April 2026 GA announcement).
- Catalog facts originate in many systems; KC is the unified discovery surface.
- Useful when org has existing catalog investment that can't be sunset.

**AmEx fit:** **Probably not yet.** AmEx doesn't have a deployed Atlan/Collibra to federate from. Skip Pattern C until that changes.

---

## 6. The worked scenario

You asked for the exact scenario. Let me walk it through carefully.

### Setup

```
SPOKE PROJECT 1 (prj-d-finance-dw)
┌──────────────────────────────────────────────┐
│ Dataset: financial_facts                      │
│   table_1: invoices                           │
│   table_2: cardmember_summary  ← share to t3  │
│   table_3: revenue_rollup      ← receives t2  │
└──────────────────────────────────────────────┘

SPOKE PROJECT 2 (prj-d-risk-dw)
┌──────────────────────────────────────────────┐
│ Dataset: risk_facts                           │
│   table_1: fraud_signals                      │
│   table_2: customer_risk_scores               │
│   table_3: regulatory_reports                 │
└──────────────────────────────────────────────┘
```

User's specific rule: **`table_2` in Project 1 is shareable with `table_3` in Project 1** (intra-project sharing).

### Question 1 — Can Project 1's table_3 use data from table_2?

**Yes, three mechanisms (pick based on grain of access):**

#### Mechanism 1 — Authorized View (within Project 1)

`table_3 = revenue_rollup` is built as an authorized view that queries `table_2 = cardmember_summary`:

```sql
-- In Project 1's `financial_facts` dataset:
CREATE OR REPLACE VIEW financial_facts.revenue_rollup AS
SELECT product_group, SUM(amount) AS revenue
FROM `prj-d-finance-dw.financial_facts.cardmember_summary`
WHERE rpt_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY product_group;
```

Then **authorize** the view on the source dataset:
- Project 1 admin grants `revenue_rollup` view permission to read `cardmember_summary`.
- Anyone with `roles/bigquery.dataViewer` on `revenue_rollup` can query it.
- They do NOT need direct access to `cardmember_summary`.

**Trust chain:**
```
USER (or AGENT)
   │ roles/bigquery.dataViewer on revenue_rollup
   ▼
revenue_rollup (authorized view)
   │ authorized to SELECT from cardmember_summary
   ▼
cardmember_summary (source)
   │ RLS predicate applied (if any)
   ▼
[filtered rows returned]
```

#### Mechanism 2 — Direct IAM grant (same project)

Grant the principal `roles/bigquery.dataViewer` on `financial_facts` dataset. They can query both `table_2` and `table_3` directly. Simpler but less granular.

#### Mechanism 3 — Analytics Hub Listing (overkill for intra-project)

You can publish `table_2` in an Exchange that `table_3`'s owner subscribes to. Works but overengineered for same-project sharing.

**Recommendation:** Use **authorized views** for intra-project column/row subsetting. Use **direct IAM** when entire dataset access is fine.

### Question 2 — Can Project 2's table_3 use data from Project 1's table_2?

**Yes, four mechanisms (cross-project):**

#### Mechanism 1 — Analytics Hub (the modern way)

```
Project 1 (publisher):
  ┌──────────────────────────────────────────┐
  │ Exchange: amex-cardmember-data           │
  │   Listing: "Cardmember Summary Q1 2026"  │
  │     refs → financial_facts.table_2       │
  └──────────────────────────────────────────┘
                    │
                    ▼ (Project 2 subscribes)
Project 2 (subscriber):
  ┌──────────────────────────────────────────┐
  │ Linked dataset: financial_facts_LINKED   │
  │   table_2_linked (read-only pointer)     │
  └──────────────────────────────────────────┘
                    │
                    ▼ (table_3 queries linked dataset)
  CREATE VIEW risk_facts.regulatory_reports AS
  SELECT … FROM `…financial_facts_LINKED.table_2_linked` ...
```

**Properties:**
- Project 2 sees table_2 as if local (no IAM grant needed on Project 1's source dataset for the subscriber to query the linked dataset).
- Publisher tracks usage: queries against shared data, bytes processed, by subscriber project.
- Publisher can set **data egress restriction** → subscribers can't copy/export.
- Source RLS still applies (subscriber's user identity filters rows).
- Storage stays in Project 1; subscriber pays only for queries.

#### Mechanism 2 — Cross-project direct IAM grant

Project 1 admin grants Project 2's user/SA `roles/bigquery.dataViewer` on Project 1's `financial_facts` dataset. Project 2 queries Project 1's table directly using fully-qualified name: `prj-d-finance-dw.financial_facts.table_2`.

Pros: simple. Cons: hard to audit usage at scale; no egress control.

#### Mechanism 3 — Cross-project authorized view

Project 2 creates an authorized view in its own project that queries Project 1's table. Project 1 must authorize the view (cross-project authorization). Project 2's users get permission to query the view; never see source directly.

#### Mechanism 4 — Authorized Dataset (the 2025+ pattern)

Group ALL of Project 2's views/datasets needing access to Project 1's data into one "authorized dataset." Project 1 authorizes once at the dataset level (not per-view). Scales past the 2,500-authorized-resource-per-dataset limit.

**Recommendation matrix:**

| Use case | Mechanism |
|---|---|
| Lightweight ad-hoc cross-project share | Direct IAM grant |
| Cross-project, multiple subscribers, want usage tracking | Analytics Hub |
| Cross-project, want column/row subsetting | Cross-project authorized view |
| Cross-project, many views from one source | Authorized Dataset |

### Question 3 — How does Synapse see all this?

When Synapse's `kc_loader.py` runs against Project 1 + Project 2:

```
KC returns entries:
  - prj-d-finance-dw.financial_facts.table_1, table_2, table_3
  - prj-d-risk-dw.risk_facts.table_1, table_2, table_3

Synapse mints:
  - 6 Table nodes (one per real table)
  - Entry links of type 'schema-join' → EQUIVALENT_TO edges
  - Lineage from KC (Project 1.table_2 → Project 1.table_3 via authorized-view authorship)

If cross-project sharing via Analytics Hub:
  - Project 2's table_2_linked appears as a SEPARATE entry in KC
    (with property `kc_is_linked_dataset = True`,
    `kc_source_entry = prj-d-finance-dw.financial_facts.table_2`)
  - Synapse mints a SEPARATE Table node for the linked dataset,
    with a new edge `LINKED_TO` → the source Table node
  - This means our graph distinguishes "Finance owns it" vs "Risk consumes it"

Agent answering "which project owns table_2?":
  inspect_table('table_2') in Synapse returns 2 results:
    - prj-d-finance-dw (asset_kind: Table, is_source: True)
    - prj-d-risk-dw (asset_kind: LinkedDataset, source: prj-d-finance-dw)
```

This is exactly the kind of disambiguation KC alone can't surface without provenance.

---

## 7. Human access workflow

For a human (analyst, steward, eng) to access metadata + data:

```
┌────────────────────────────────────────────────────────────┐
│  STEP 1 — Identity                                         │
│  User logs in with Google Workspace creds                  │
│  IAM principal: alice@amex.com                             │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  STEP 2 — KC search                                        │
│  Alice opens Knowledge Catalog console                     │
│  Searches: "cardmember spending"                           │
│                                                            │
│  KC returns ONLY entries Alice has at least               │
│  `roles/dataplex.viewer` on the entry's group.            │
│  ↑ this is "access-control-aware search"                  │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  STEP 3 — Drill into entry                                 │
│  Alice clicks "Cardmember Summary"                         │
│  Sees: business_name, description, schema, lineage,        │
│        aspects, glossary links, DQ scorecard               │
│                                                            │
│  Alice can SEE the metadata even if she can't QUERY        │
│  the data — these are separate authorizations.             │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  STEP 4 — Try to query                                     │
│  Alice clicks "Open in BQ"                                 │
│  BQ console: SELECT * FROM `cardmember_summary` LIMIT 10  │
│                                                            │
│  IF Alice has roles/bigquery.dataViewer on dataset:        │
│    Query runs.                                             │
│    RLS predicate filters rows by Alice's identity.         │
│                                                            │
│  ELSE:                                                     │
│    "Permission denied. Request access via" → link to       │
│    a request-access flow (IT ticket OR self-service).     │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  STEP 5 — Audit trail                                      │
│  Cloud Logging records:                                    │
│  - alice@amex.com searched KC for "cardmember spending"   │
│  - alice@amex.com viewed entry "Cardmember Summary"        │
│  - alice@amex.com ran query <hash> against cardmember     │
│  - Bytes scanned, rows returned, latency                  │
│                                                            │
│  Available to security team via Cloud Logging queries.    │
└────────────────────────────────────────────────────────────┘
```

**Key separations of concerns:**

1. **Metadata access** is governed by `roles/dataplex.viewer/editor/admin` on Entry Groups.
2. **Data access** is governed by `roles/bigquery.*` on Datasets/Tables.
3. **Search results filtering** is done by KC at query time using Alice's effective IAM.

**Three-tier permission view:**

| Layer | Role | What Alice can do |
|---|---|---|
| Metadata browse | `roles/dataplex.viewer` | See an entry exists, view its name + description |
| Metadata enrich | `roles/dataplex.editor` | Add aspects, glossary links, update descriptions |
| Data query | `roles/bigquery.dataViewer` | Read rows (subject to RLS) |
| Data own | `roles/bigquery.dataOwner` | Change schema, grant access to others |

---

## 8. Agentic access workflow

Same workflow but the principal isn't a human. Two flavors:

### Flavor 1 — Service Account (legacy / current default)

```
┌─────────────────────────────────────────────────────────────┐
│  AGENT IDENTITY: Service Account                            │
│                                                             │
│  IAM principal: svc-d-synapse-agent@                        │
│                 prj-d-ea-poc.iam.gserviceaccount.com        │
│                                                             │
│  Has been granted:                                          │
│  - roles/dataplex.viewer on prj-d-finance-dw                │
│  - roles/dataplex.viewer on prj-d-risk-dw                   │
│  - roles/bigquery.metadataViewer on both projects           │
│  - roles/aiplatform.user on prj-d-ea-poc                    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Agent (Synapse Consumer Agent, ADK)                        │
│                                                             │
│  Code in apps/semantic_graph_agent/ uses google-auth        │
│  with GOOGLE_APPLICATION_CREDENTIALS=svc-d-synapse-agent.   │
│  json                                                       │
│                                                             │
│  Calls KC API: dataplex.entries.search                      │
│  Cloud Logging records the call attributable to the SA.    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  KC enforces access-control-aware filtering:                │
│  - Returns ONLY entries svc-d-synapse-agent has perms on   │
│  - If the agent asks about an entry it can't see → 404     │
│  - Audit log shows "Permission denied for entry X by       │
│    principal svc-d-synapse-agent"                          │
└─────────────────────────────────────────────────────────────┘
```

**Key properties of this flavor:**
- Agent acts AS the service account.
- IAM grants are explicit and auditable.
- All agent actions in audit logs are attributable to one named SA.
- Service accounts can be granted access via authorized views, in Analytics Hub subscriptions, etc. (same patterns as users).

**Trade-off:** the service account has the same auth surface as a human SA — long-lived JSON keys, key rotation, etc. Vulnerable if key leaked.

### Flavor 2 — Agent Identity (2026 forward-looking)

Google's **Context-Aware Access for AI Agents** introduces a new identity model. As of 2026 this is in preview for Gemini Enterprise Agent Platform:

```
┌─────────────────────────────────────────────────────────────┐
│  AGENT IDENTITY: SPIFFE-shaped, certificate-bound           │
│                                                             │
│  Each agent deployment auto-provisions:                     │
│  - A SPIFFE identity in a Google-managed trust domain      │
│  - An X.509 certificate issued by Google                   │
│  - Workload access tokens bound to that cert (mTLS + DPoP) │
│                                                             │
│  No long-lived JSON key. Tokens are short-lived (1h),      │
│  certificate-bound (proof of possession).                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  IAM enforcement:                                           │
│  - The agent identity is a first-class IAM principal       │
│  - You can grant it the same roles as service accounts     │
│  - But access can be conditioned on: mTLS attestation,     │
│    the agent's parent application, device context          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Audit logs include the AGENT IDENTIFIER, not just the SA  │
│  - Cloud Logging distinguishes "agent X owned by Alice"     │
│    from "Alice herself"                                     │
│  - Forensics: who deployed agent X? when? with what config? │
└─────────────────────────────────────────────────────────────┘
```

**Properties:**
- Higher security floor than SAs.
- Better attribution: audit logs distinguish humans, SAs, and agents.
- Conditional access: agent's grant can require mTLS + IP range + time window simultaneously.
- Aligned with Google's 2026 Gemini Enterprise Agent Platform direction.

**Today's recommendation for AmEx:**
- Use SA for v1 (well-understood, broad tooling support).
- Plan migration to agent identity for v2 when GA stabilizes.

---

## 9. Service identities for agents — full lifecycle

The audit-grade picture from creation to revocation:

### Provisioning

```
Day 1:
  Admin creates SA: svc-d-synapse-agent@prj-d-ea-poc.iam.gserviceaccount.com
  ↓
  Admin grants minimum-privilege roles:
    - roles/dataplex.viewer on hub project
    - roles/dataplex.viewer on each spoke project (or Entry Group)
    - roles/bigquery.metadataViewer on consumed projects
    - roles/aiplatform.user on Vertex project
  ↓
  Admin generates JSON key, distributes via secure channel to agent runtime
    (Cloud Secret Manager, or local file with restricted perms)
  ↓
  Agent code authenticates: google.auth.default() picks up key
```

### Identification

How do we PROVE which agent ran an action? Multiple signals stack:

| Signal | What it tells you |
|---|---|
| Cloud Logging `protoPayload.authenticationInfo.principalEmail` | The SA that authenticated |
| Cloud Logging `protoPayload.requestMetadata.callerIp` | Source IP of the call |
| Cloud Logging `protoPayload.requestMetadata.callerSuppliedUserAgent` | Agent's user-agent string (we set this in code) |
| (Agent Identity flavor) Cloud Logging `protoPayload.authenticationInfo.serviceAccountKeyName` | SA key fingerprint |
| Cloud Logging `protoPayload.authenticationInfo.principalSubject` | SPIFFE identity (for agent identity flavor) |

**Action item for Synapse:** every API call our Consumer Agent makes should set `User-Agent: synapse-consumer-agent/v1.0` so it's distinguishable in audit logs from other workloads using the same SA.

### Control

Three controls available:

1. **IAM Conditions** — limit when/where the SA can be used:
   ```
   binding {
     role = "roles/dataplex.viewer"
     members = ["serviceAccount:svc-d-synapse-agent@…"]
     condition = "request.time < timestamp('2026-12-31T23:59:59Z') &&
                  request.auth.access_levels == 'amex-corporate-network'"
   }
   ```
2. **Service Account Impersonation** — humans can impersonate the SA only with explicit grant (`roles/iam.serviceAccountTokenCreator`).
3. **VPC Service Controls** — perimeter the data; SA can only call APIs from inside.

### Revocation

```
Compromised key:
  1. Revoke key: gcloud iam service-accounts keys delete <KEY_ID>
  2. Generate new key, redistribute
  3. Audit logs: find all calls with old key fingerprint
  4. Report to security team
```

For agent identity flavor: certificates have short TTL (1h) so the blast radius of compromise is much smaller.

### Decommissioning

```
Agent retired:
  1. Remove all role bindings:
     for role in $ROLES; do
       gcloud projects remove-iam-policy-binding <PROJECT> \
         --member="serviceAccount:svc-d-synapse-agent@…" \
         --role="$role"
     done
  2. Delete the SA: gcloud iam service-accounts delete svc-d-synapse-agent@…
  3. Confirm no orphaned resources still reference it
```

---

## 10. Governance + audit

Concrete patterns for compliance.

### Cloud Logging — what's recorded

Every read/write through KC + BQ generates **two log categories**:

| Log type | Records |
|---|---|
| Admin Activity | Resource creation, IAM grants, schema changes (always on, free) |
| Data Access | Queries, metadata reads, entry views (enable per service; you pay storage) |

Enable Data Access logs for `dataplex.googleapis.com` and `bigquery.googleapis.com` to get the full picture.

### Sample audit queries

> Who searched for "cardmember PII" in the last 90 days?

```sql
SELECT
  protoPayload.authenticationInfo.principalEmail AS who,
  TIMESTAMP_TRUNC(timestamp, DAY) AS day,
  protoPayload.metadata.searchQuery AS query
FROM `logs.dataplex_*`
WHERE protoPayload.methodName LIKE '%SearchEntries%'
  AND REGEXP_CONTAINS(protoPayload.metadata.searchQuery, '(?i)cardmember.*pii')
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
ORDER BY day DESC;
```

> Which agents queried the cardmember table last week?

```sql
SELECT
  protoPayload.authenticationInfo.principalEmail AS sa,
  protoPayload.requestMetadata.callerSuppliedUserAgent AS user_agent,
  COUNT(*) AS queries,
  SUM(protoPayload.metadata.tableScanTotalBytes) AS bytes_scanned
FROM `logs.bigquery_jobs_*`
WHERE 'cardmember_summary' IN UNNEST(protoPayload.metadata.referencedTables)
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND ENDS_WITH(protoPayload.authenticationInfo.principalEmail, '.iam.gserviceaccount.com')
GROUP BY sa, user_agent
ORDER BY queries DESC;
```

### Access reviews

Quarterly recommended:
- Run `gcloud projects get-iam-policy` for each spoke
- Diff against last quarter; flag new SA grants or role escalations for review
- For each SA: confirm it's still owned, still needed; document the purpose

### VPC Service Controls

Perimeter the KC + BQ APIs to prevent data exfiltration:

```
Perimeter: amex-data-perimeter
Restricted services:
  - bigquery.googleapis.com
  - dataplex.googleapis.com (= Knowledge Catalog)
  - storage.googleapis.com

Ingress rules:
  - Allow from amex-corporate-network access level
  - Allow specific external customer perimeters (for Analytics Hub sharing)

Egress rules:
  - Allow to: bigquery.googleapis.com in approved external projects
  - Block: everything else
```

**Effect:** even with a compromised SA key, attacker can't exfiltrate data outside the perimeter.

---

## 11. AmEx-specific recommendations

Tied to your specific situation.

### Architectural

- **Adopt Pattern A** (Platform-level hub-and-spoke). Use `prj-d-knowledge-catalog` as the hub. Each business-unit data warehouse is a spoke. The Synapse graph builder consumes from the hub.
- **Keep Synapse as the reasoning layer.** Don't try to replace KC's discovery. Don't try to add provenance calibration to KC.

### Identity

- **v1:** create one SA per Synapse component:
  - `svc-d-synapse-builder@…` — reads metadata to build the graph
  - `svc-d-synapse-consumer-agent@…` — runs the ADK NL→SQL agent
  - `svc-d-synapse-enricher@…` — runs Gemini enrichment
- **v2 (2027+):** migrate to agent identity when GA + tooling matures.

### Access patterns

- **Hub project** owns the catalog; service accounts for Synapse get `roles/dataplex.viewer` (metadata) + `roles/bigquery.metadataViewer` (schema).
- **No data access for the catalog builder.** Metadata only. Profiling that requires data access goes through a separate SA with explicit, narrow grants.
- **Use Authorized Datasets** for the cardmember scenario's intra-project subset sharing (your scenario question 1).
- **Use Analytics Hub** for cross-project sharing if you need usage metrics + egress restrictions.

### Audit

- **Enable Data Access logs** on dataplex.googleapis.com and bigquery.googleapis.com from day 1.
- **Set distinct User-Agent strings** in all Synapse code (consumer agent, enricher, builder) for forensic attribution.
- **Quarterly access reviews** with diffs of IAM policies.

### Governance

- **VPC Service Controls** on the KC + BQ APIs, with explicit ingress allowlists for AmEx network and any approved external integrations.
- **Custom Aspect Types** for AmEx-specific governance metadata (e.g., `business_review_status`, `pii_exception_approval`).
- **One global glossary** in the hub; all spokes link to it.

---

## 12. Reading order for going deeper

1. This doc (you're done) — the granular foundation
2. `KNOWLEDGE_CATALOG_DEEP_DIVE.md` — the 35-feature inventory
3. `KC_VS_KC_PLUS_SYNAPSE.md` — the architecture decision
4. `TARGET_GRAPH_CARDMEMBER.md` — what facts KC contributes to the cardmember graph
5. Official Google docs (for hands-on):
   - [KC IAM and Access Control](https://docs.cloud.google.com/dataplex/docs/iam-and-access-control)
   - [BQ Analytics Hub](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction)
   - [BQ Authorized Views + Authorized Datasets](https://docs.cloud.google.com/bigquery/docs/authorized-views)
   - [Context-Aware Access for AI Agents](https://docs.cloud.google.com/access-context-manager/docs/caa-agent-security)
   - [2026 KC launch announcement](https://cloud.google.com/blog/products/data-analytics/introducing-the-google-cloud-knowledge-catalog)

---

## TL;DR (one paragraph for a busy exec)

**Knowledge Catalog (2026) is Google's AI-grounding context engine** — it aggregates metadata across BQ + AlloyDB + Spanner + Looker + Salesforce + SAP, enriches with Gemini, and exposes access-control-aware search so agents only retrieve assets they're authorized to see. For AmEx, deploy KC as a **platform-level hub-and-spoke**: hub project hosts the catalog + canonical glossary; each business-unit data warehouse is a spoke. Use **Analytics Hub** for cross-project data sharing (publishers track usage; subscribers get linked datasets without IAM overhead). Use **Authorized Datasets** for intra-project subset sharing. **Agents authenticate as service accounts today** (one SA per Synapse component, narrowly scoped) and migrate to **Agent Identity** (SPIFFE-based, mTLS-bound) when Google Gemini Enterprise Agent Platform GA matures. Every metadata + data access is audit-logged with attribution to the specific SA + user-agent string; quarterly access reviews + VPC Service Controls enforce the perimeter. **Synapse sits on top** as the AmEx-specific confidence-typed reasoning layer that fuses KC (one of 11 sources) with corpus + MDM + steward inputs into a graph the Consumer Agent uses to generate grounded BigQuery SQL with citations.
