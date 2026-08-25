# `mdm_extract` — What It Is, What It Produces, and Why It Matters

> Transcribed verbatim from screenshots, in original section order. Where the screenshots skip content, a `[GAP IN SOURCE]` marker appears so nothing is silently invented.

---

Absolutely. The most important way to explain `mdm_extract` to a peer is:

> **`mdm_extract` is not extracting business data from the 46 BigQuery tables. It is extracting the enterprise metadata and relationships around those tables from Lumi MDM — schema, columns, ownership, governance, pipelines, AppFlows, lineage, lifecycle, environments, replication, workflows, versions, and the evidence proving those relationships.**

In other words, it is building the **technical and governance context required to understand and safely reason about each table**.

The Lumi API itself supports this progression: table → dataset/schema → ownership → AppFlow → pipeline → lineage → lifecycle/tracking/security metadata.

---

# 1. What the final run represents

At the end of the 46-table run, we should have something conceptually like:

```text
46 important enterprise tables
        │
        ▼
mdm_extract
        │
        ├── discover each table in Lumi
        ├── find datasets
        ├── find schemas and versions
        ├── find columns
        ├── find ownership/governance
        ├── find AppFlows
        ├── validate pipelines
        ├── find workflows
        ├── retrieve lineage
        ├── retrieve lifecycle/certification
        ├── retrieve environment/promotion
        ├── retrieve replication
        └── preserve every API response as evidence
        │
        ▼
Enterprise metadata archive
        │
        ├── normalized summaries
        ├── raw source evidence
        ├── relationship discovery
        ├── extraction coverage
        ├── failures/unknowns
        └── provenance
        │
        ▼
Knowledge Graph
```

So instead of having just:

```text
Table = gms_transaction
```

we can eventually represent something closer to:

```text
Table
  gms_transaction

HAS_SCHEMA
    └── Schema X

HAS_COLUMN
    ├── se_trans_usd_am
    ├── se_cr_dr_in
    └── ...

BELONGS_TO_DATASET
    └── Dataset Y

OWNED_BY
    └── Owner / business owner / application owner

PRODUCED_BY
    └── Pipeline P

HAS_APPFLOW
    ├── AppFlow A
    └── AppFlow B

UPSTREAM_OF
    └── Other table(s)

DERIVED_FROM
    └── Other columns

LIFECYCLE
    └── certification / promotion state

DEPLOYED_IN
    └── environment

REPLICATED_TO
    └── region

SUPPORTED_BY_EVIDENCE
    └── exact Lumi API response
```

That is the core value.

---

# 2. What the output directory means

The final run should be thought of roughly like this:

```text
output/mdm/mdm_46_patched_v2/
│
├── run_manifest.json
│
├── coverage.json
├── coverage.csv
│
├── table_summaries.json
│
├── logs/
│   └── events.jsonl
│
└── tables/
    │
    ├── gms_transaction/
    │   ├── summary.json
    │   ├── responses/
    │   │   ├── table_exists.json
    │   │   ├── dataset_schema_anchor.json
    │   │   ├── portal_pipeline_by_table.json
    │   │   ├── ...
    │   │   └── hundreds/thousands of metadata responses
    │   │
    │   └── global_matches/
    │
    ├── gms_merchant_char/
    │   └── ...
    │
    ├── wwcas_authorization/
    │   └── ...
    │
    └── ... all 46 tables
```

The extractor explicitly records where individual API results were persisted. For example, the previous run records successful `table_exists`, schema-anchor and pipeline-by-table calls and points to their corresponding files under `tables/gms_transaction/responses/`.

Each layer has a different purpose.

---

# 3. `run_manifest.json` — what exactly did we run?

This is the **identity card for the extraction run**.

It tells us things such as:

```text
Run ID
Extractor version/build
Start time
End time

Lumi API host
API prefix
Storage type = BigQuery

Number of workers
Rate limit
Retry settings
Timeout behavior

Catalog mode = focused
Exhaustive = false

Resume = true/false

Tables requested

Critical failures
Supplemental failures

Overall completion state
```

Think of it as the answer to:

> **"Under what conditions was this metadata collected?"**

This matters enormously later.

If six months from now someone asks:

> "Where did this graph relationship come from?"

you don't want the answer to be:

> "Some Python script ran sometime."

You want:

```text
Extraction run:
    mdm_46_patched_v2

Extractor build:
    patched_v2

Source:
    Lumi MDM dev API

Mode:
    focused

Retrieved:
    2026-08-23

Storage:
    BigQuery

Evidence:
    exact raw API response
```

That creates reproducibility.

---

# 4. `table_summaries.json` — the high-level catalog of all 46

This is one of the most valuable outputs.

Instead of reading thousands of raw JSON files, this file provides a consolidated view across tables.

Conceptually:

```json
{
  "tables": [
    {
      "table_name": "gms_transaction",

      "execution_status": "complete",
      "metadata_status": "complete",

      "answerability": {
        "structure": "strong",
        "semantics": "strong",
        "governance": "strong",
        "lineage": "strong",
        "operations": "strong"
      },

      "discovery": {...},

      "coverage_counts": {...}
    }
  ]
}
```

The earlier `gms_transaction` extraction, for example, rated all five dimensions as strong.

Those dimensions mean roughly:

| Dimension | What it tells us |
| --- | --- |
| **Structure** | Do we understand datasets, schemas, columns, versions, storage? |
| **Semantics** | Do we have descriptions/business/pipeline context explaining what it represents? |
| **Governance** | Do we know owners, sensitivity, CDE, lifecycle, certification, etc.? |
| **Lineage** | Can we trace upstream/downstream table/column dependencies? |
| **Operations** | Do we understand pipeline, AppFlow, promotion, environment, replication, tracking? |

So later the agent can reason:

```text
Can I confidently answer a schema question?
    YES — structure strong.

Can I confidently answer ownership?
    YES — governance strong.

Can I confidently explain lineage?
    YES — lineage strong.
```

rather than assuming every table is equally documented.

---

# 5. `tables/<table>/summary.json` — the dossier for one table

This is the **table-level metadata dossier**.

For each table, it consolidates everything the extraction discovered.

Conceptually:

```text
TABLE SUMMARY
│
├── execution status
├── metadata status
├── answerability
│
├── direct endpoint results
│
├── discovery
│    ├── dataset IDs
│    ├── dataset parent IDs
│    ├── schema IDs
│    ├── schema parent IDs
│    ├── ownership IDs
│    ├── key IDs
│    ├── AppFlow IDs
│    ├── AppFlow parent IDs
│    ├── pipeline IDs
│    ├── workflow IDs
│    ├── operator IDs
│    ├── storage IDs
│    ├── versions
│    ├── regions
│    └── relationships
│
├── validated relationships
│
├── coverage counts
│
└── complete endpoint coverage
```

For example, real summaries contain typed storage-to-region and pipeline-to-version relationships rather than simply throwing all discovered IDs into one bucket.

This is important because:

```text
pipeline_id != appflow_id
appflow_id != appflow_parent_id
schema_id != dataset_id
dataset_id != dataset_parent_id
```

Those are different entity types.

The extractor tries to preserve that distinction.

---

# 6. Structural metadata — "What actually exists?"

The first major knowledge category is structural.

For each table we are trying to determine:

```text
Table
│
├── Dataset
│    ├── dataset ID
│    ├── dataset parent ID
│    └── versions
│
├── Schema
│    ├── schema ID
│    ├── schema parent ID
│    └── versions
│
├── Columns
│    ├── name
│    ├── data type
│    ├── metadata
│    └── sensitivity information
│
├── Storage
├── Region
├── Keys
└── View DDL where applicable
```

The API supports fetching dataset ownership, resolving datasets to AppFlows, getting latest dataset children and obtaining view DDL.

This means the agent can eventually answer things like:

> Does table X exist?

> Which columns are in it?

> What type is column X?

> Which schema version is current?

> What historical schema versions existed?

> Is this a table or view?

> Where is it hosted?

> Which region is associated with the asset?

---

# 7. Schema evolution

We're not only interested in today's schema.

The graph can eventually represent:

```text
Schema Parent
     │
     ├── Version 1.0
     ├── Version 1.1
     ├── Version 1.2
     ├── Version 1.3
     └── Current
```

Then we can ask:

> Has this table changed?

> Which versions exist?

> When did a column appear?

> Was a field removed?

> Could this schema change affect downstream consumers?

This becomes especially valuable once connected to lineage and metrics.

---

# 8. Ownership and governance — "Who is responsible?"

The extraction also attempts to answer:

```text
Table
   ↓
Dataset
   ↓
Ownership / Governance
```

Pipeline metadata can contain information such as:

* pipeline requestor
* additional pipeline owners
* business unit
* application owner
* business owner
* source system
* markets
* regions
* legal entities
* data usage
* access information
* PII flags
* sensitivity flags
* third-party flags

Those are explicitly part of Lumi's pipeline/governance model.

This makes questions possible such as:

> Who owns this data?

> Which business unit does it belong to?

> What source system does it originate from?

> Does the pipeline involve sensitive information?

> Which markets or legal entities does it cover?

---

# 9. AppFlows — "How is the data moving?"

The next level is AppFlow.

Conceptually:

```text
Dataset
   │
   ▼
AppFlow Parent
     │
     ├── AppFlow version A
     ├── AppFlow version B
     └── ...
```

An AppFlow represents an important piece of Lumi's processing topology.

It helps connect:

```text
Dataset
   ↓
AppFlow
   ↓
Pipeline
```

So we are moving from merely knowing:

```text
Table exists
```

to knowing:

```text
how the table participates in the data-processing system
```

---

# 10. Pipelines — "What produces/manages this table?"

The extractor validates pipeline relationships rather than treating every UUID it encounters as a pipeline.

Once validated, pipeline metadata can tell us things like:

```text
Pipeline ID
Pipeline name
Pipeline type
Pipeline version
Description
Business unit
Feed type
Requestor
Owners
Host regions
Governance information
```

Lumi exposes both pipeline details and full portal metadata for those pipeline IDs.

So eventually:

```text
Table: X
    PRODUCED_BY
Pipeline: P
```

becomes a defensible relationship.

---

# 11. Workflows and operators — "How does that pipeline execute?"

Inside or around pipeline execution there can be:

```text
Pipeline
   ↓
AppFlow
   ↓
Workflow
   ↓
Operators
```

The API exposes workflow contracts, workflow versions, operators, executions and operator results.

That enables deeper technical questions:

> Which workflow processes this dataset?

> What datasets does its workflow contract reference?

> What operator components participate?

> Which workflow versions exist?

This is much closer to a technical architecture map than a traditional table catalog.

---

# 12. Table lineage — "Where did this table come from?"

This is one of the highest-value outputs.

Lumi exposes both directions.

```text
            upstream
               │
               ▼
Source Table A
               │
Source Table B ────────► Target Table X
               │
Source Table C
```

And:

```text
Table X
   │
   ├──► Downstream Y
   ├──► Downstream Z
   └──► Downstream Q
```

Lumi explicitly provides table-as-target, table-as-source, pipeline-scoped and AppFlow-scoped lineage.

So we can eventually answer:

> Where does this dataset come from?

> Which other tables depend on it?

> If we deprecate this table, what downstream assets may be affected?

---

# 13. Column-level lineage — even more valuable

Instead of stopping at:

```text
Table A → Table B
```

we may know:

```text
Table A.column_x
      │
      ├── transformation / derivation logic
      ▼
Table B.column_y
```

Lumi's attribute lineage model contains:

* source table
* source identifier
* source column
* target table
* target identifier
* target column
* pipeline ID
* `[one bullet obscured between screenshots]`
* feed name
* derivation logic

This lets the future agent answer:

> Where did this particular field come from?

> What source column ultimately feeds this metric?

> What breaks if we change this field?

That is much more powerful than table-level lineage alone.

---

# 14. Lifecycle and certification — "Can we trust this asset operationally?"

The extractor also gets lifecycle information where available.

Lifecycle can expose:

```text
pipeline
table
AppFlow parent

lifecycle version
activity
status

region

DCU required
DCU complete

data purge flags
breaking-change indicators

comments
errors
timestamps
```

Lumi explicitly exposes these fields in …

`[GAP IN SOURCE — end of this sentence not captured]`

So we can distinguish:

```text
Table exists
```

from:

```text
Table exists
AND
pipeline is certified/promoted
AND
lifecycle status is completed
AND
environment state is known
```

That's a much stronger trust signal.

---

# 15. Promotion/environment

The extractor can learn things like:

```text
Pipeline
   │
   ├── development environment
   ├── promoted environment
   └── E3 / production-related status
```

This helps answer:

> Is this technically registered but not production promoted?

> Is this a production-backed table?

> Which environment is this pipeline running in?

That's important when the agent has to choose between two apparently equivalent datasets.

---

# 16. Replication and regions

```text
Table / Pipeline
       │
       ├── USA
       ├── IND
       └── GLOBAL
```

where Lumi provides it.

The API explicitly offers latest replication information by pipeline and replication statistics.

This helps with questions like:

> Where is this data replicated?

> Is this asset regional or global?

> Which technical geography is associated with it?

---

# 17. `responses/` — the evidence archive

This is arguably the most important piece for enterprise-grade explainability.

We don't only keep:

```text
Pipeline P → Table X
```

We keep the actual API response that caused us to believe that.

For example:

```text
tables/gms_transaction/
    responses/
        portal_pipeline_by_table.json
```

Then the graph edge can eventually carry something like:

`[GAP IN SOURCE — example edge payload not captured]`

That is **provenance**.

---

# 18. Why keeping raw responses matters

Suppose the agent eventually says:

> "`gms_transaction` is produced by pipeline P."

Someone challenges it:

> "How do you know?"

We should be able to trace:

```text
Agent answer
     ↓
Knowledge Graph assertion
     ↓
Graph edge
     ↓
Extraction run
     ↓
Lumi endpoint
     ↓
Raw Lumi JSON response
```

That's the difference between:

> an LLM making a statement

and:

> an LLM explaining a statement supported by enterprise metadata evidence.

---

# 19. `coverage.json` / `coverage.csv` — what did we try?

Coverage is different from metadata.

Coverage tells us **what the extractor attempted and what happened**.

`[GAP IN SOURCE — remainder of section 19 and the start of section 20 not captured]`

---

# 20. (continued) — error taxonomy

```text
HTTP 503
```

Meaning:

> Lumi/service was unavailable or degraded.

Again, we cannot conclude the relationship does not exist.

This distinction is extremely important.

And your current 46-table run demonstrates why. Several tables completed with very strong metadata despite hundreds of retry-exhausted calls, while some tables landed in partial state during broader service degradation.

So in the final KG:

```text
503 != DOES_NOT_EXIST
```

It should become:

```text
UNKNOWN
reason = source unavailable
```

---

# 21. `events.jsonl` — the operational timeline

The event log is essentially the run's flight recorder.

It records things like:

```text
table_start
request_retry
request_retry_capped
table_complete
warnings
errors
```

For example, your live run currently records `table_complete` events with duration, status, answerability and coverage counts. It also shows simultaneous HTTP 503 retry activity across unrelated endpoints, which is evidence of service degradation rather than a single malformed table.

This allows us afterward to ask:

> Which tables were slowest?

> Which endpoints caused the most retries?

> When did Lumi start returning 503?

> Which tables were running during the outage?

> Which failed tables should be rerun?

---

# 22. `discovery` — the bridge between raw responses and the graph

Raw JSON is useful but cumbersome.

`discovery` extracts typed identifiers and relationships from those responses.

For example:

```json
{
  "table_name": "gms_transaction",

  "dataset_ids": [...],
  "dataset_parent_ids": [...],

  "schema_ids": [...],
  "schema_parent_ids": [...],

  "ownership_ids": [...],

  "appflow_ids": [...],
  "appflow_parent_ids": [...],

  "pipeline_ids": [...],
```

`[GAP IN SOURCE — remainder of the discovery JSON, and sections 23–29, not captured]`

---

# 29. (continued) — column-centric traversal

The graph can potentially traverse:

```text
se_trans_usd_am
        │
        ├── belongs to
        │      gms_transaction
        │
        ├── used by
        │      Spend
        │      Transaction Size
        │      CNP Spend
        │      Cash Spend
        │      ...
        │
        ├── used by
        │      observed measures
        │
        ├── originates from
        │      upstream column(s)
        │
        ├── processed by
        │      pipeline
        │
        └── downstream into
               other columns/tables
```

Now we're doing **business-aware technical impact analysis**.

---

# 30. Example: outage impact

Someone asks:

> **"Pipeline P is down. What does that actually affect?"**

Without this metadata:

```text
Pipeline P failed.
```

With the graph:

```text
Pipeline P
   ↓
...
   ↓
/ governed metrics
   ↓
and 85 heavily used observed measures
   ↓
used across multiple domains
```

`[GAP IN SOURCE — middle of this block not captured]`

Now the CTO or product owner sees the business impact, not just the technical failure.

---

# 31. What `mdm_extract` deliberately does NOT give us

This is also important to explain to your colleague.

It does **not** extract actual transaction rows.

So it cannot by itself answer:

> How much Spend occurred yesterday?

> Who were the top merchants?

> What was transaction count last quarter?

> Which customers redeemed offers?

> What percentage of rows are null today?

Those require the **BigQuery data plane**.

Think of it this way:

```text
mdm_extract
    =
    HOW / WHERE / WHAT / WHO / LINEAGE / TRUST

BigQuery
    =
    ACTUAL VALUES
```

Or:

> **MDM teaches the agent how to ask the right data question. BigQuery supplies the numerical answer.**

`[GAP IN SOURCE — section 32 not captured]`

---

# 33. A peer-ready explanation you can use

If somebody asks you **"What exactly is this `mdm_extract` thing doing?"**, I would explain it like this:

> "We have identified 46 strategically important BigQuery tables. `mdm_extract` is a read-only metadata crawler built against Lumi MDM's OpenAPI. Starting with only a table name, it walks the metadata relationships around that table — datasets and versions, schemas and columns, ownership and governance, AppFlows, validated pipelines, workflows, table and column lineage, lifecycle/certification, environments, promotion and replication.
>
> It doesn't query or copy the actual business rows. Its job is to build the technical knowledge envelope around each data asset.
>
> For every API call we retain the raw Lumi response, the endpoint used, HTTP result, attempts and extraction status. We also produce normalized table summaries and typed discovery relationships. That means we're not just building a catalog — we're building a provenance-backed metadata graph where an assertion such as 'Pipeline X produces Table Y' can be traced back to the exact Lumi source response that established it.
>
> We also explicitly distinguish metadata that was successfully observed from metadata that is unknown because Lumi returned a 500, 503 or timeout. That prevents an API outage from becoming a false knowledge-graph statement that something doesn't exist.
>
> Once these 46 table dossiers are loaded into the graph, we can layer the governed DMP metrics on top to tell us what business metrics officially mean, and the query-history measures catalog to tell us how analysts actually calculate things. Then later a BigQuery executor provides the actual numbers.
>
> So the end state is: DMP tells us what SHOULD be calculated, query history tells us what people ACTUALLY calculate, MDM tells us WHAT DATA EXISTS and how it is produced/governed/connected, and BigQuery tells us WHAT ACTUALLY HAPPENED. The knowledge graph ties those together with evidence and provenance."

That's the cleanest description of what this run is buying us.

And one more point I would emphasize to your peer: **the fact that this first 46-table run is taking a long time does not make the output less valuable.** In fact, its coverage/error artifacts are showing us exactly where Lumi metadata is available, missing, or temporarily unavailable. Your current run has examples of tables completing with strong answerability despite hundreds of failed supplemental requests, while other tables became partial during 503 degradation. That uncertainty itself is useful provenance: the graph can know not only facts, but also **what we do not yet know and why**.
