# Card sourcing audit — what reaches the agent, and what doesn't

> **Question asked.** "Are we utilizing each and every thing from the
> sources? Are we getting business unit and user information? And if we
> are, are we *showing* it — enough for the agent to make the right
> judgement call?"
>
> **Method.** Every loader read end to end; then a real fixture build
> run (`build-graph` + `compile` over
> `tests/fixtures/real_extractions_production` +
> `tests/fixtures/mdm_46_patched_v2` + `tests/fixtures/sources`) and the
> produced card diffed against the folded graph node props. Nothing in
> here is from memory — every claim is a file, a prop, or a line that
> is/isn't on a rendered card.
>
> **Scope.** Audit only. No code changed.

---

## 1. The three gates

A fact from a source has to pass three gates before it can change what
the agent does. Think of it as a warehouse:

| gate | the warehouse analogy | today's instrument |
|---|---|---|
| **G1 — file read** | the truck is unloaded at the dock | `UtilizationLedger` (consumed / deferred / inventoried) |
| **G2 — field parsed → graph** | the box is opened and its contents shelved | nothing — no field-level instrument exists |
| **G3 — rendered on a card** | the item is put in the shop window where the customer sees it | nothing — no instrument exists |

**G1 is genuinely solved.** The fixture run accounts for every byte:
**68 consumed, 21 deferred (each with a pinned reason), 0 inventoried.**
That is a real achievement and it's why the "are we reading everything?"
question feels answered.

**G2 and G3 are where the leak is.** A file can be 100% "consumed" while
half its fields are dropped on the floor, and a field can be perfectly
shelved in the graph while never appearing on the page the agent reads.
The ledger cannot see either. Today the leak is roughly:

- **G2**: ~20 documented Atlas/MDM fields parsed-and-dropped or never read.
- **G3**: counted mechanically against the fixture build — of 28 distinct
  table props in the graph, `cards.py` renders **11**; of the semantic
  column props, it prints **name, type, description, and a SENSITIVE
  boolean** and nothing else; and of 22 edge predicates, **10 are read by
  no downstream consumer at all**. This is the big one.

---

## 2. What the agent actually sees today

Real card, straight out of the fixture build (`dw.gms_transaction`):

```
# table dw.gms_transaction
- object: TABLE · rows ≈ 1020 · lifecycle: certified
- owner: own_a@corp · business unit: GMNS · layer: SOR
- line of business: GMNS: Global Merchant & Network Services (steward;
  corroborated by 2 dmp metric(s); 2 gmns spec(s))
- used by: GMNS: Global Merchant & Network Services · 117
- purpose: Global merchant transaction spine.
- partitioned: latest 20260822 · schema v1_c67da1a5
- usage rhythm: 10:00 UTC × 22 · 9:00 UTC × 14 · 14:00 UTC × 8
## columns
- cm13 string (SENSITIVE): Card member number.
- country_cd string: ISO country of the transaction.
- se_no string: Merchant identifier.
- trans_usd_am float64: Transaction amount in USD. | lumi: Signed …
## joined with (observed)          ## common filters
## metrics available               ## access
## conflicts
```

**Business unit and ownership do arrive** — `business unit: GMNS` (from
MDM `pipeline.business_unit`), `owner: own_a@corp` (Atlas
`ownership.business_owner`), the steward `in_lob` line with its
witnesses, and the mined `used by` line. That part of the question is a
yes.

Everything in §3 is the part that is a no.

---

## 3. On the shelf, not in the window — graph facts no card renders

These are **already extracted, already in the graph, already correct**.
They just never appear on the page the agent reads. Dumped live from the
folded graph for `table:dw.gms_transaction`:

### 3.1 Table-level props held but never rendered

**16 of 28** (excluding the internal `stub` marker):

| prop | fixture value | why the agent needs it |
|---|---|---|
| `answerability` | `{governance: strong, lineage: strong, operations: strong, semantics: strong, structure: strong}` | **the single best "can I trust this table" signal we own** — and it is invisible |
| `project` | `axp-lumi` | the card says `dw.gms_transaction`; the agent cannot write a fully-qualified reference from it |
| `business_name_atlas` | `Merchant Transactions` | the human name; the agent only ever sees the physical name |
| `data_category` | `Merchant Services` | domain placement |
| `table_meta_logical.last_modified` | `2026-08-01` | **freshness/staleness — the card shows a partition date but never when the table last actually moved** |
| `table_metrics.table_size_bytes` | `987654321` | scan cost |
| `cost_prior` | `{p50_bytes: 400MB, p95_bytes: 9GB, n_jobs: 9}` | mined, indexed to `cost_priors.json`, **never on a card** — the agent can't judge whether a query is cheap or a 9GB scan |
| `top_users` | `analyst1@corp (48), analyst2@corp (22)` | the *actual* human users. The `used by` line shows a mined org code, not these people |
| `feed_type` / `pipeline_name` / `source_system` | `batch` / `gms_daily_load` / `GMS` | how and how often data arrives |
| `environment` | `E3` | which environment this is |
| `has_pii_atlas` / `has_gdpr_atlas` / `has_oncop_atlas` | `True` / `False` / `False` | **table-level compliance flags never render.** The `## access` section only shows row-access policy + PII *columns* |
| `ownership_atlas.vp`, `.car_id` | `vp_a@corp`, `CAR-1` | escalation path; only `business_owner` renders |
| `n_partitions` | `2` | |

### 3.2 Column-level props held but never rendered

Five are never read by the compiler at all (`approx_distinct`,
`null_count`, `profile_coverage`, `is_partitioning`, `is_primary_key`);
the rest reach `ColumnConsensus` and are then not printed:

| prop | fixture value | why it matters |
|---|---|---|
| `business_name` / `business_name_atlas` | `Card Member 13`, `Service Establishment Number` | **parsed all the way into `ColumnConsensus.business_name` and then never printed.** The agent sees `se_no` and a one-line description; the human name is right there and dropped |
| `is_primary_key` | `se_no: True`, `txn_uid: True` | **grain.** `tables.jsonl` already carries `primary_key: [se_no, txn_uid]`. The card's `grain` section is a *protected, never-dropped* section — and it contains a partition date and a schema hash, not the key |
| `is_partitioning` | per column | the card says "partitioned" but never **which column** — a cost and correctness landmine |
| `approx_distinct` / `null_count` / `profile_coverage` | `42 / 10 / recent_partitions_budgeted` | join safety and NULL handling |
| `pii_role_id` / `sde_group` | `R3` / `SDE1` | collapsed to the word `SENSITIVE`; the actual role and group are lost |
| `ordinal` | `2, 3, 4…` | columns render alphabetically, not in schema order |

### 3.3 Edge families held but never rendered

**10 of 22 edge predicates are consumed by nothing downstream** —
not `cards.py`, not `compile.py`, not `reconcile.py`, not the tools or
loop layer: `has_domain`, `mapped_term`, `owned_by`, `upstream_of`,
`derived_from`, `evidenced_by`, `has_schema`, `described_by`,
`concerns`, `in_domain`. (`fk_references` reaches `joins.jsonl` but not
the card; `has_policy` reaches `build_acl` only.)

| edge | fixture instance | why it matters |
|---|---|---|
| `fk_references` | `gms_transaction.cm13 → wwcas_authorization.card_no` | **a declared FK is the most reliable join fact we have and it is not on the card.** The joins section shows co-queries and CTE-scoped studio joins only. It *is* in `joins.jsonl` (`source: constraints`) so `get_join_paths` finds it — but the card, which is what the agent reads first, does not |
| `has_domain` → `domain:` nodes | `country_cd = US 72% / GB 6.3% / CA 4%` | value domains are extracted and indexed. `sample_values` serves them — **but the card never says which columns have a domain**, so the agent has no reason to call the tool |
| `derived_from` (+ `derivation_logic`) | `trans_usd_am ← data.raw_gms_feed.amt` | column lineage — invisible |
| `upstream_of` | table lineage | invisible |
| `mapped_term` | `trans_usd_am → "Transaction United States Dollar Amount" (Approved)` | **the entire glossary linkage — the whole point of business_terms.csv — never appears on a card** |
| `owned_by` | `own_a@corp` (business), `tech_a@corp` (tech) | MDM's own ownership edges never render; the card's owner line reads the Atlas prop instead |
| `evidenced_by` → `doc:view_sql_*` | full view SQL retained | for a VIEW, its definition is the strongest possible statement of what it means — retained, never shown |

---

## 4. Field-level utilization, source by source

15 source families are registered (`SOURCE_WITNESS`). Verdict per source:

| source | display | G1 file | G2 fields | G3 on card |
|---|---|---|---|---|
| `bq` | BQ warehouse archive | ✅ 9 of 9 semantic artifacts | ✅ good | ⚠️ partial (§3) |
| `jobs_30d` | 30-day query logs | ✅ | ✅ good | ⚠️ rhythm yes, cost/users no |
| `lumi` | Atlas MDM archive | ✅ | ✅ good | ⚠️ answerability/pipeline/lineage no |
| `std_tech_metadata` | Atlas catalog | ✅ | ✅ **closed — every documented field** | ⚠️ partial |
| `business_terms` | Atlas glossary | ✅ | ✅ definitions now land via std_tech | ❌ **never on any card** |
| `glossary` (data_cleaned) | Acropedia acronyms | ✅ | ✅ all 5 cols | ❌ **never on any card** |
| `metrics_dmp` | certified KPIs | ✅ | ✅ all 15 keys | ✅ metric cards |
| `extended_gmns` | pending specs | ✅ | ✅ | ✅ |
| `measures_catalog` | mined patterns | ✅ | ⚠️ `query_count` dropped | ✅ |
| `studio_queries` | studio export | ✅ | ✅ **exemplary** — every column either consumed or named as deliberately ignored in the docstring | ✅ |
| `skill_contract` | skill packs | ⚠️ only `metric_contracts.yaml` | ❌ `knowledge.md`, `data_specs.md`, `qa_checks.yaml` deferred | ❌ |
| `gold_queries` | eval pairs | ✅ | ✅ | n/a (eval) |
| `blue_insights` | analyst snippets | ✅ | ✅ | ✅ filters section |
| `lob_map` / org_map | steward map | ✅ | ✅ | ⚠️ one line on the table card; no card of its own |

### 4.1 `std_tech_metadata` (Atlas catalog) — CLOSED

> **Status: closed.** Every field below now reaches the graph; the
> per-field destinations are pinned in
> `docs/contracts/std_tech_metadata_layout.md` ("Field utilization")
> and fenced by
> `tests/test_p0_census.py::test_std_tech_parses_every_documented_field`
> and
> `tests/test_p2_graph.py::test_std_tech_full_utilization_reaches_the_graph`.
> What follows is the original finding, kept as the record of what was
> missing.

#### The original finding

Enumerated every key in the fixture and diffed against `vocab.py` +
`quads_emit.emit_std_tech`:

**Parsed into `StdTechEntry` and then dropped by the emitter:**
- `data_sub_category` — parsed, never emitted, never rendered.

**Never read at all** (present in the fixture and/or documented in
`docs/contracts/std_tech_metadata_layout.md`):
- `datasetAttribute.pii_columns[]` — a table-level list of
  `{column, pii_role_id}`. **A second, independent PII witness we
  already have and never consult.**
- `datasetAttribute.load_type` (`SNAPSHOT_DEDUPE_MAX`…) — how the table
  is loaded; directly governs whether a naive `SELECT` double-counts.
- `datasource` (GCP project), `datasetGroup` (BQ dataset) — the
  qualified-name pieces we're missing on the card.
- `isActive`, `isLatest`, `isLineageExist`, `datasystem`, `dataserver`,
  `technology`, `appl_id`.
- `businessMetadata.businessTermDescription` — **the actual definition
  of the business term.** We link a column to a term node that carries
  only `{name, status}`. The sentence explaining what the term *means*
  is in the file, on the same object, and we throw it away.
- Documented-but-not-in-fixture per the contract:
  `pdeAttribute.derived_logic` (SQL for computed columns — the contract
  itself flags it as "a future semantic source"), `primary_key_indicator`,
  `partition_indicator`, `nullable_indicator`, `position`,
  `column_length_number`, `businessTermId`, `confidenceScore`,
  `is_partitioned`, `target_system`, `type`.

**Also:** column↔term links are matched **by name** against
`business_terms.csv`, even though `businessMetadata` carries
`businessTermId`. Every name-spelling mismatch becomes
`term_links_unmatched`. Matching on the id first would raise the link
rate for free.

#### What closing it changed

Beyond the props, four things the graph could not previously say:

- **Ownership became edges.** Atlas `ownership` was a dict prop nothing
  read; it now emits `owned_by` per role, so Atlas and Lumi corroborate
  on the same owner node (`own_a@corp` carries both witnesses) and the
  VP is a first-class endpoint. A `car_id` stays a prop — an identifier
  is not a person.
- **Table-level `has_pii` finally emits a policy edge.** Only `oncop`
  and `gdpr` did before, so of the three compliance flags the ACL never
  saw the strongest one.
- **`pii_columns[]` is a second, independent PII witness.** It supplies
  a role where the pde listing carried none, records a
  `pii_role_id_table_declared` where the two Atlas declarations
  disagree, and mints the column where the pde listing missed it
  entirely. In the fixture that surfaced a real edge — a column present
  in *neither* BigQuery nor the catalog's column listing, only in the
  PII declaration. It routes through E1's pinned D1 handler: the fact
  lives in the graph as a policy edge for governance, and the column
  stays off the card, because the agent must never see a column the
  runtime cannot serve.
- **Business terms resolve on `businessTermId` first.** Name matching
  was losing every link whose spelling had drifted; the resolution used
  is now recorded on the edge as `matched_on`, a term Atlas declares by
  id is minted rather than dropped, and `businessTermDescription` lands
  on the term node — `business_terms.csv` is id + name + status only,
  so this is the sole place the *meaning* of a term exists at all.

One drift the new fields make visible: Atlas puts `trans_usd_am` at
ordinal 5, BigQuery at ordinal 1. Both are on the column node now
(`ordinal_atlas` vs `ordinal`), so it is a question someone can ask
rather than a silent disagreement.

#### What Atlas does NOT carry

Asked directly — do we get business unit and table descriptions from
Atlas?

- **Table description: yes.** `description_atlas` ("Global merchant
  transaction spine.") plus `business_name_atlas` ("Merchant
  Transactions"), and a description and business name per column.
- **Business unit: no — Atlas has no such field.** Its business axis is
  `data_category` → `data_sub_category` ("Merchant Services" →
  "Payments"), plus ownership and `appl_id`. The `business_unit: GMNS`
  on a table node comes from **MDM** (`pipeline.business_unit`, the
  `lumi` source), and line-of-business membership comes from the
  steward's `lob_map.jsonl` corroborated by dmp/gmns declarations.
  Three different witnesses answer three different questions — who
  operates the pipeline (MDM), who owns the data domain (steward LOB),
  and what business category it falls in (Atlas). None of them is a
  substitute for the others, and the graph keeps them separate.

### 4.2 Deferrals that are semantic, not operational

Most of the 21 deferrals are genuinely operational (checkpoints, format
twins, physical-layer twins). Four are not — they are exactly the
"context so the agent makes the right judgement call" the question is
about:

| deferred file | current reason | what's actually in it |
|---|---|---|
| `knowledge.md` (10 packs) | "skill prose: doc-evidence concern" | metric definitions, date conventions, denominator policies, **edge cases** |
| `data_specs.md` | "prose: not machine-parsed" | source table, key columns, filter logic, **join rules** |
| `tls_reference.md` | "doc evidence node later, never parsed" | the 41-section TLS rulebook — Gross/Cancelled/Net, Air vs non-Air, **the mandatory Hotel >$100K exclusion**, 7 named anti-patterns |
| `qa_checks.yaml` | "eval-layer concern" | denominator-nonzero, distinct-key enforcement, **do-not-average-ratios** |

The reasons are honest and were correct when written. But "not
machine-parsed" is not the same as "cannot be shown" — these could ride
as doc nodes surfaced on the relevant table card without any parsing at
all. The Hotel $100K rule is a wrong-number-generator if the agent
never sees it.

---

## 5. The missing card types

There are exactly three card templates: `table_card`, `metric_card`,
`concept_card`. Confirmed repo-wide — no other card generator exists.

### 5.1 No business-unit / LOB card

The data is all there — `lob.jsonl` per build:

```json
{"code":"GMNS","name":"Global Merchant & Network Services","kind":"lob",
 "tables":["dw.gms_transaction","dw.wwcas_authorization"],
 "domains":["merchant"],"used_tables":["dw.gms_transaction"],
 "usage_support":117}
{"code":"CRO","name":"Credit Risk Ops","kind":"org_unit","parent":"SBS",
 "used_tables":["dw.wwcas_authorization"],"usage_support":24}
```

…plus per-LOB readiness in `sources.json`
(`GMNS: 2 of 2 tables witnessed, 100%`). None of it is a card, so
`read_card` cannot reach it. An agent asked a GMNS question has no way
to orient on GMNS as a *thing* — only to stumble into GMNS tables one at
a time.

**Related defect:** `list_tables(lob="gmns")` filters on the *metric's*
`line_of_business` field, not on the authoritative `in_lob` edges in
`lob.jsonl`. A steward-declared table with no LOB-tagged metric will not
appear under its own LOB.

### 5.2 No context / vocabulary card

12.3K acronyms and 4.4K business terms land as `acr:` and `term:` nodes
and in `vocab.jsonl`. They are reachable — `search_semantics(kind=vocab)`
finds them — but:

- they are **attached to nothing**. No `acr` → column edge, no `term` →
  table edge beyond the sparse `mapped_term` links.
- the agent reading `se_typ`, `cm13`, or `ALIF` on a card gets **no
  signal that a decoder exists**, so it has no reason to search.
- acronyms are **business-unit- and region-scoped** (`ABP` means
  "Abandoned Property" generally and "Automatic Bill Pay" in GMNS) —
  and we know the table's BU. We have everything needed to resolve the
  right meaning and we never do it.

---

## 6. What I'd fix, in order — status

Ranked by (agent judgement improved) ÷ (work). **Items 1–8 and 10 are
done** (PR #108, second and third commits); every fact family below
now has a card section, a console card, and where relevant a tool —
the full map is `docs/audits/fact_to_surface_map.md`.

1. ~~Put the grain on the grain line.~~ **done** — `## grain`: primary
   key (constraints, with the Atlas view when it differs), partition
   column(s), latest partition, partition count, load type, rows,
   bytes, schema fingerprint.
2. ~~Render `fk_references` in the joins section.~~ **done** — declared
   constraints lead `## joins`; the profile shows them ● first.
3. ~~Column line: business name + value-domain marker.~~ **done** —
   plus PK/PARTITION/NOT NULL markers, sensitivity role/group, term
   with definition, FK, computed logic, lineage, ordinal divergence.
4. ~~`## trust & operations`~~ **done** — answerability, freshness,
   feed/pipeline/source, environment, cost prior, rhythm, top users,
   Atlas active/latest/lineage flags.
5. ~~Complete the access section.~~ **done** — table PII/GDPR/ONCOP,
   policy witnesses, per-column role/group, full owner chain with
   witnesses on the header.
6. ~~`cards/lob/<code>.md` + fix `list_tables(lob=…)`.~~ **done** —
   lob cards for LOBs and org units, `read_card("lob:…")`,
   `list_tables(lob=)` resolves through the steward map, SYNAPSE.md
   `## business units`, Explorer › business units + unit profile.
7. ~~Context card per table.~~ **done as a section** —
   `## vocabulary (scoped to <units>, All)`: exact-token matches of
   acronyms/terms against column and business names, restricted to
   the table's own business units (the ABP disambiguation is the
   scope rule itself). Also the VOCABULARY card on the profile.
8. ~~G2 close-out on `std_tech_metadata`.~~ **done**, see §4.1.
9. Attach the four semantic deferrals as doc nodes — **left open by
   decision** (skip the four deferrals).
10. ~~Field-level utilization instrument.~~ **done** —
    `sahs/compiler/coverage.py` writes `indexes/coverage.json` on
    every compile; CI holds `unaccounted` at empty. 42 table props,
    25 column props and 20 edge predicates rendered; 5 deferred with
    reasons; 0 unaccounted on the fixture build.

Token budget: raised to 3K with drop order vocabulary → column
long-tail → joins → filters → metrics; grain, access, conflicts stay
protected. The fixture card renders at ~1.8K.

## 7. Summary in one paragraph

We read every file (G1: 68/68 accounted for, 0 unexplained) and we do
get business unit, ownership, and user information. The failure is not
extraction — it's **delivery**. Sixteen of twenty-eight table
props, most column detail, and ten of twenty-two edge families are
correctly extracted, correctly reconciled, sitting in the graph, and
never printed on the page the
agent actually reads: grain, declared foreign keys, freshness, cost,
trust scores, value domains, glossary links, lineage, and the full
compliance picture. On top of that, two card *types* the agent needs to
orient itself — a business-unit card and a context/vocabulary card —
don't exist, though every input for both is already compiled into the
build. The token budget is not the constraint: the fixture card renders
well under the 2K ceiling with room to spare.
