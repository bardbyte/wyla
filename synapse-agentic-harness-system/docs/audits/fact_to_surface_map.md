# Fact → surface map — where every graph fact is shown

> The instrument the card sourcing audit asked for (item 10): one row
> per fact family, from the source that produced it to the exact
> place the agent and the human see it. Held current by CI:
> `indexes/coverage.json` is written on every compile, and
> `tests/test_p2_compiler.py::test_coverage_ledger_accounts_for_every_prop_and_edge`
> fails the build if any table/column prop or edge predicate in the
> graph is neither rendered nor deferred-with-reason.

## The one row everything renders from

```
graph (folded nodes + edges)
      │  compiler/facts.py  ── build_table_facts()  ── one row per table
      ▼
indexes/tables.jsonl          meridian.table_facts/1
      ├── cards/tables/<t>.md          compiler/cards.py::table_card   → agent: read_card("table:…")
      ├── /api/meridian/table/{t}      backend/meridian.py::table      → console: Table Profile
      ├── /api/meridian/explorer/tables                                 → console: Explorer › tables
      ├── indexes/graph_map.json (table nodes)                          → console: Cosmos rail
      └── list_tables (loop/tools.py)                                   → agent: the shelf
indexes/lobs.jsonl            build_lob_facts()
      ├── cards/lob/<code>.md          compiler/cards.py::lob_card     → agent: read_card("lob:…")
      ├── /api/meridian/explorer/lobs + /lob/{code}                     → console: Explorer › business units
      └── SYNAPSE.md ## business units (loop/digest.py)                 → agent: the briefing
```

The card and the profile are two renderings of one compiled row.
Neither reaches back to the graph, so a number on screen and the
number the agent reasoned from are the same number by construction.

## Table facts — family by family

| fact family | source(s) | graph | facts row | table card section | Table Profile card | agent tool |
|---|---|---|---|---|---|---|
| business name | Atlas `business_name` | `business_name_atlas` | `identity.business_name` | title line | head title | `list_tables[].business_name` |
| description | Atlas › BQ | `description_atlas` / `description_bq` | `identity.description` (+ `_bq`) | `- purpose:` | head | `list_tables[].purpose` |
| where it lives | BQ `project`, Atlas `datasource`/`datasetGroup`/`technology`/`dataserver`/`appl_id`/`target_system` | `project`, `*_atlas` | `identity.project/dataset/technology/…` | `- lives at:` | head "lives at" | card |
| category | Atlas `data_category` › `data_sub_category` | same | `identity.data_category/_sub_category` | header line 4 | IDENTITY & GRAIN | card |
| object / table / layer / load type | BQ `type`, Atlas `type`, `data_type_name`, `load_type` | `object_type`, `table_type_atlas`, `layer_type`, `load_type_atlas` | `identity.*` | header line 4 + `## grain` | IDENTITY & GRAIN | card |
| **primary key** | BQ `11_logical_constraints`, Atlas `primary_key_indicator` | `is_primary_key`, `is_primary_key_atlas` | `primary_key`, `operations.primary_key_atlas` | `## grain` (protected) | IDENTITY & GRAIN | `table_facts()` (E18 fan-out), card |
| **partition column** | BQ `is_partitioning`, Atlas `partition_indicator` / `is_partitioned` | same | `operations.partition_columns(_atlas)`, `identity.is_partitioned_atlas` | `## grain` | IDENTITY & GRAIN | card |
| rows · bytes · partitions · schema | BQ `13_table_metrics`, `10_partitions`, schema node | `total_rows`, `table_metrics`, `n_partitions`, `partition_latest`, `schema_fingerprint` | `operations.*`, `identity.schema_fingerprint` | `## grain` | IDENTITY & GRAIN | `list_tables[].rows` |
| **business unit** | MDM `pipeline.business_unit` | `business_unit` | `business.business_unit` | header line 4 | IDENTITY & GRAIN, Explorer › tables | `list_tables[].business_unit` |
| line of business (+ witnesses) | steward `lob_map`, dmp/gmns `lineOfBusiness` | `in_lob` edges | `business.lobs[]` | `- line of business:` | WHO (link → unit profile) | `list_tables(lob=…)`, `read_card("lob:…")` |
| who runs queries | mined `business_unit` via `org_map` | `used_by` edges | `business.used_by[]` | `- used by:` | WHO | card, lob card |
| **owners with roles + witnesses** | Atlas `ownership` (per role), MDM `ownership.json` | `owned_by` edges, `ownership_atlas` | `business.owners[]`, `business.ownership_ids` | `- owner:` (chain) | WHO | `list_tables[].owner` |
| top users | BQ `jobs_top_users` | `top_users` | `business.top_users[]` | `## trust & operations` | WHO | card |
| lifecycle · environment | MDM `lifecycle.json` (503 → `unknown_unavailable`) | `lifecycle_status`, `environment` | `lifecycle`, `operations.*` | header line 2 | head chip, Explorer › tables | `list_tables[].readiness` |
| feed · pipeline · source system | MDM `pipeline.json` | same | `operations.*` | `## trust & operations` | TRUST & OPERATIONS | card |
| **freshness** (created / last modified) | BQ `01_logical_table_meta` | `table_meta_logical` | `operations.created/last_modified` | `## trust & operations` | TRUST & OPERATIONS | card |
| **cost prior** | jobs_30d p50/p95 bytes | `cost_prior` | `operations.cost_prior` | `## trust & operations` | TRUST & OPERATIONS | sandbox anomaly gate, card |
| usage rhythm | jobs_30d peak hours | `usage_rhythm` | `operations.usage_rhythm` | `## trust & operations` | TRUST & OPERATIONS | card |
| **answerability** | MDM `table_summaries.answerability` | `answerability` | `trust.answerability` | `## trust & operations` | TRUST & OPERATIONS chips | card |
| active · latest · lineage declared | Atlas `isActive`/`isLatest`/`isLineageExist` | `is_*_atlas` | `trust.*` | `## trust & operations` | TRUST & OPERATIONS | card |
| display tier | derived (purpose + LOB + metrics) | — | `trust.tier` | — | head chip, Explorer, Cosmos | — |
| row-access policy | BQ `16_row_access_policies` (DENIED → unknown) | `has_policy → policy:unknown_denied/row_access` | `access.restricted` | `## access` (protected) | ACCESS (crimson when unknown) | sandbox refuses live |
| **table-level PII / GDPR / ONCOP** | Atlas `has_pii/has_oncop/has_gdpr` | `has_*_atlas` + `has_policy` edges | `access.has_*_atlas`, `access.policies` | `## access` | ACCESS chips | card |
| sensitive columns with role/group | Atlas `pii_role_id`/`sde_group`/`pii_columns[]`, MDM `is_pii` | column props + `has_policy → policy:pii` | `access.sensitive_columns[]`, `column_facts[].pii_role/sde_group` | `## access` + column markers | ACCESS + column chips | `validate_sql` sensitivity |
| **column business names** | Atlas / MDM `business_name` | `business_name(_atlas)` | `column_facts[].business_name` | column line `“…”` | COLUMNS | card |
| column descriptions (both planes) | Atlas › MDM › BQ | `description_*` | `column_facts[].description(+_supplementary)` | column line | COLUMNS | card |
| column profile (distinct / null / length) | BQ `14_column_profile`, Atlas `column_length_number` | `approx_distinct`, `null_count`, `profile_coverage`, `column_length` | `column_facts[].*` | column line texture | COLUMNS right rail | card |
| **value domains** | BQ `15_low_cardinality_values` | `domain:` nodes + `has_domain` | `column_facts[].domain{n_values, top}` | column line `N known values (…) → sample_values` | COLUMNS | `sample_values` |
| **business terms with definitions** | Atlas `businessMetadata` (id-first), business_terms.csv | `mapped_term` edges, `term:` nodes with `description` | `column_facts[].terms[]`, `declared_terms[]` | column line `term: … — definition` | COLUMNS | `search_semantics(kind=vocab)` |
| **declared foreign keys** | BQ `11_logical_constraints` | `fk_references` edges | `joins.declared[]`, `column_facts[].fk_references` | `## joins` first rows | JOINS & LINEAGE (● declared) | `get_join_paths` (constraints tier) |
| observed / scoped joins | jobs co-query digest, studio ON-clauses | `co_queried_with`, `joins_via` | `joins.observed[]`, `joins.scoped[]` | `## joins` | JOINS & LINEAGE | `get_join_paths` |
| **computed-column logic** | Atlas `derived_logic` | `derived_logic` prop + `doc:derived_logic_*` via `described_by` | `column_facts[].derived_logic` | column line `computed:` | COLUMNS | card |
| **column lineage** | MDM `attr_lineage.json` | `derived_from` edges | `column_facts[].derived_from[]`, `lineage.derived_columns` | column line + `## lineage` | COLUMNS + JOINS & LINEAGE | card |
| **table lineage** | MDM `lineage_up.json` | `upstream_of` edges | `lineage.upstream/downstream` | `## lineage` | JOINS & LINEAGE | card |
| view SQL | BQ `05_view_definition` | `doc:view_sql_*` via `described_by` | `lineage.view_sql`, `lineage.docs` | `## lineage` | JOINS & LINEAGE | card |
| ordinals from both planes | BQ `ordinal_position`, Atlas `position` | `ordinal`, `ordinal_atlas` | `column_facts[].ordinal(_atlas)` | card order; divergence noted | COLUMNS right rail | card |
| **vocabulary in scope** (context) | Acropedia `data_cleaned.csv` (BU/region-scoped), Atlas terms | `acr:` / `term:` nodes | `vocabulary[]` (exact token match, BU-scoped) | `## vocabulary (scoped to …)` | VOCABULARY | `search_semantics(kind=vocab)` |
| D1–D5 structural conflicts | reconcile | consensus | `trust.structural`, `column_facts[].flags`, `omitted_catalog_only` | `## conflicts` (protected) | COLUMNS (flags, omitted line) | tickets.jsonl |

Bold rows are the audit's headline misses — every one now has a card
section, a profile card, and (where relevant) a tool.

## Business-unit facts

| fact | source | facts row (`lobs.jsonl`) | lob card | Explorer › business units | agent |
|---|---|---|---|---|---|
| code · name · kind · parent | steward `lob_map` / `org_map` | `code/name/kind/parent` | title + line 1 | list row | `read_card("lob:<code>")`, `list_tables(lob=)` |
| metric domains | dmp `metricDomain` via `in_lob` | `domains` | line 1 | — | card |
| tables as a shelf (business name, description, tier, metrics, lifecycle, PII, MDM unit) | table facts | `tables[]` | `## tables` with `read_card` addresses | unit profile TABLES | card |
| readiness | compiled (witnessed metric per table) | `readiness{pct}` | line 2 | list WITNESSED column | SYNAPSE.md `## business units` |
| usage (who runs queries) | mined `business_unit` via `used_by` | `used_tables`, `usage_support` | `## queries these tables` | list USAGE | card |
| owners across tables | Atlas + MDM `owned_by` | `owners[]` | `## owners` | unit profile OWNERS | card |
| vocabulary scoped to the unit | Acropedia `Business_Unit` | `vocabulary_entries` | line 4 | list VOCAB | `search_semantics(kind=vocab)` |

## Deferred, with reasons (pinned in `sahs/compiler/coverage.py`)

| kind | key | reason |
|---|---|---|
| table prop | `stub` | internal lineage-endpoint marker |
| column prop | `stub`, `nested_path` | internal markers; the dotted name already says nested |
| edge | `has_schema` | schema versioning; the fingerprint prop rides on identity |
| edge | `valid_in` | served when versioned builds land |
| edge | `alias_of` | served by `search_semantics(kind=vocab)`, not a table fact |
| edge | `concerns` | ReviewItem subjects → the Operate steward queue |

## Still open (not in this pass)

- The four **prose deferrals** (`knowledge.md`, `data_specs.md`,
  `qa_checks.yaml`, `tls_reference.md`) — left deferred by decision;
  the Artifacts staging surface is where they will land.
- **Products tab** still binds to the old `semantic-graph` data plane
  (design inventory delta #2); the Build-backed business-unit shelf
  now lives in Semantics › business units, which is the surface to
  re-point Products at.
- `measures_catalog.query_count` (mined) is parsed nowhere — a metric
  fact, outside this table-facts pass.
