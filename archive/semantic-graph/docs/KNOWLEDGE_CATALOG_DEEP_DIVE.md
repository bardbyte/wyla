# Google Cloud Knowledge Catalog — Deep Dive + Synapse Integration Plan

> **Purpose.** Be the single canonical reference for what Knowledge Catalog (KC) offers, how it relates to Synapse, and exactly how we incorporate it. Read this once and be an expert.
>
> **Audience.** Senior AI engineer at AmEx building Synapse. Assumes familiarity with Gemini, ADK, BigQuery, and the Synapse graph store.
>
> **Sources.** [BQ Knowledge Catalog overview](https://docs.cloud.google.com/bigquery/docs/use-knowledge-catalog), [Dataplex Universal Catalog overview](https://docs.cloud.google.com/dataplex/docs/catalog-overview), [Auto Data Quality](https://docs.cloud.google.com/dataplex/docs/auto-data-quality-overview), [Dataplex AI overview](https://docs.cloud.google.com/dataplex/docs/ai-overview). Fetched 2026-06-09.

---

## 1. Naming history (so you're not confused)

Google has rebranded this product three times in five years. As of 2026:

| Era | Name | Notes |
|---|---|---|
| 2019–2022 | **Data Catalog** | Original standalone product. Now legacy; still gets some references in old docs. |
| 2022–April 2026 | **Dataplex Catalog / Dataplex Universal Catalog** | Folded under Dataplex umbrella. |
| April 2026 onward | **Knowledge Catalog** | Current name. Unifies BigQuery Knowledge Catalog (BQ-only AI-grounding surface) and the broader Dataplex catalog into one product. |

When AmEx people say "Knowledge Catalog" they could mean any of these. The current product is one thing across all of BQ, Dataplex, Auto DQ, and Dataplex AI.

---

## 2. The full feature inventory — 35+ capabilities across 4 surfaces

### A. Catalog surface (the metadata index)

| # | Feature | Concrete behavior |
|---|---|---|
| 1 | **Automated metadata ingestion** | Near-real-time discovery of BQ + Dataform + Dataproc + Vertex AI (models, datasets, feature groups) + Looker (instances, dashboards, LookML projects — Preview) + Bigtable + Spanner + AlloyDB (Preview) + Cloud SQL + Pub/Sub + Cloud Storage + Iceberg REST Catalog (Databricks Unity, AWS Glue, Snowflake) |
| 2 | **Dark data discovery** | Scans PDFs in Cloud Storage, extracts entities, exposes them as queryable BQ assets |
| 3 | **Column-level metadata (paths)** | Each column is a "path" with attached metadata: PII markers, DQ scores, custom business metadata |
| 4 | **Aspects + Aspect Types** | Typed metadata fields attached to entries or links. Aspect Types are reusable templates with cardinality rules and nested structures (lists, maps, arrays). Examples: `schema`, `contact info`, `overview`, `data-quality-scorecard` |
| 5 | **Entries + Entry Types** | Entry = a data asset. Entry Type = a typed template enforcing required Aspect Types. Includes system Entry Types (BQ table, Looker dashboard) and custom Entry Types |
| 6 | **Entry Groups** | Containers for entries + links. Unit of access control + organization |
| 7 | **Entry Links + Entry Link Types** | Typed relationships between entries. Symmetric (synonym, related, schema-join) or asymmetric (definition). Can target an entire entry OR a specific column path within an entry |
| 8 | **Business Glossaries** | Hierarchical taxonomy. Terms link bidirectionally to entries + columns. Supports synonyms, definitions, parent/child term relationships. Import/export via JSON or Google Sheets |
| 9 | **Data Products** | Bundle related assets into shared products with unified governance, versioning, quality SLAs, access patterns |
| 10 | **Semantic search** | NL search across all metadata; supports full-text + predicate filters (type, location, owner, created, modified); no policy-tag search; no cross-org search |
| 11 | **Name translation** | BQ fully-qualified SQL name ↔ catalog entry name; resolves user input to canonical asset references |
| 12 | **Metadata change feeds** | Pub/Sub topic emits create/update/delete events in near real-time; enables external workflow triggers |

### B. Lineage surface

| # | Feature | Concrete behavior |
|---|---|---|
| 13 | **Auto-ingested lineage** | Automatic capture from BigQuery, Dataform, Dataproc operations (job history → lineage graph) |
| 14 | **Multi-region lineage search** | Cross-region traversal in one API call |
| 15 | **OpenLineage integration** | REST endpoints for external systems to push lineage events |
| 16 | **PII flow tracking** | Lineage edges carry sensitivity classification; PII propagates through transforms automatically |

### C. Auto Data Quality surface

| # | Feature | Concrete behavior |
|---|---|---|
| 17 | **Row-level rules** | Five types: `RangeExpectation` (min/max), `NonNullExpectation` (non-NULL), `SetExpectation` (allowed values), `RegexExpectation` (pattern match), `Uniqueness` (distinct count) |
| 18 | **Aggregate rules** | `StatisticRangeExpectation` (mean/min/max ranges), row condition (custom WHERE with passing threshold), table condition (boolean aggregate), SQL assertion (returns rows = fail) |
| 19 | **Rule templates** | Reusable rule definitions stored as `data-quality-rule-template` entries; library of common patterns |
| 20 | **7 fixed quality dimensions** | Freshness, Volume, Completeness, Validity, Consistency, Accuracy, Uniqueness. Custom dimension names available via CLI/API |
| 21 | **AI-suggested rules** | Generated from data profile scan results (Knowledge Catalog data profiling) |
| 22 | **Execution models** | Scheduled (specific interval), on-demand, full-table or incremental (Date/Timestamp marker) |
| 23 | **Identity model** | Default service agent, custom SA, or End-User Credentials; supports least-privilege patterns |
| 24 | **Data filters + sampling** | Row filters (time periods, regions), sampling percentages, AIP-160 filter syntax for selective rule eval |
| 25 | **Results publication** | Export to BigQuery tables, `data-quality-scorecard` aspect on catalog entries, Looker dashboards |
| 26 | **Monitoring + alerting** | `data_scan` + `data_quality_scan_rule_result` logs in Cloud Logging; email notifications for low quality scores / job failure / job completion |

### D. Dataplex AI surface

| # | Feature | Concrete behavior |
|---|---|---|
| 27 | **Auto-generated descriptions** | Gemini emits dataset + column descriptions from schema + historical usage patterns |
| 28 | **Auto relationship graphs** | Gemini infers table-to-table relationships from usage |
| 29 | **Example queries from history** | Suggests representative SQL per asset based on past job history |
| 30 | **AI-generated glossary terms** | Gemini drafts glossary entries from existing documentation |
| 31 | **Custom-agent integration via ADK** | Build agents that use Knowledge Catalog CRUD APIs (entries, aspects, links) as tools |
| 32 | **MCP integration framework** | Local MCP Toolbox + remote MCP servers expose catalog metadata to AI agents |
| 33 | **Discovery agent** | Autonomous data exploration via MCP |
| 34 | **Metadata enrichment agent** | Automated tagging from internal docs, code repos, wikis |
| 35 | **Lineage MCP server** | Agents query data provenance directly |

### E. Cross-cutting

- **IAM integration**: Catalog Admin, Editor, Viewer roles; 100+ specific permissions
- **VPC Service Controls**: perimeter isolation
- **CMEK**: customer-managed encryption for metadata
- **Audit logging**: every operation tracked
- **Custom constraints**: org-level policy enforcement
- **Sensitive Data Protection** integration (via Data Catalog bridge)

---

## 3. Capability map — what we have vs what we need

Three buckets:

### Bucket A — Already covered, often deeper than KC

| KC feature | Synapse equivalent | Why ours is comparable or better |
|---|---|---|
| 1. Automated metadata ingestion | `bq_loader.py` reading INFORMATION_SCHEMA + `lumi_loader.py` reading session1_output.json | We span more source types (10 vs KC's 1 — BQ + variants) |
| 3. Column-level metadata | `Column` node with full property bag + `Provenance` envelope | We carry per-fact 10-source attribution; KC carries only the latest write |
| 5. Entries + Entry Types | `Node.node_type` enum (Table, Column, Metric, Entity, Synonym, …) | Same shape; ours strictly typed via Pydantic |
| 7. Entry Links + Entry Link Types | `Edge.edge_type` enum (CONTAINS, EQUIVALENT_TO, RELATES_TO, COMPUTED_FROM, …) | Same shape; isomorphic to KC's symmetric/asymmetric link model |
| 11. Name translation | `canonical_uri` scheme (`synapse://table/...`) | Same intent, simpler resolution |
| 13. Auto-ingested lineage (BQ) | `_ingest_lineage_from_bq` reading JOBS_BY_PROJECT | Equivalent for BQ; KC adds Dataform + Dataproc auto-ingest |
| 21. AI-suggested DQ rules | `bq_loader._build_dq_blob` synthesizes rules from BQ profile | Local equivalent; KC needs Dataplex enabled |
| 27. Auto-generated descriptions | `enrichment/runner.py` with Gemini 3.1 Pro + 4-rule skill.md | Ours has strict tier-capping at `inferred`; KC just emits descriptions without confidence calibration |
| 28. Auto relationship graphs | Graph builder mints `EQUIVALENT_TO` / `RELATES_TO` from corpus + JOBS + LookML | Ours fuses 3+ source types; KC uses BQ jobs alone |
| 30. AI-generated glossary terms | Enrichment proposes `candidate_synonyms` | Ours scoped to corpus evidence; KC's broader |
| 31. Custom-agent integration via ADK | `apps/semantic_graph_agent/` (Consumer Agent) | Built; ours has confidence-aware prompt design |
| 34. Metadata enrichment agent | Our enrichment runner IS this | Confidence-typed, skill-driven, capped at `inferred` |

### Bucket B — Should add by pulling from KC as a source

| KC feature | What we'd add | Cost | Why |
|---|---|---|---|
| 4. Aspects + Aspect Types | Adopt typed-template pattern: `AspectType` registry, validate node properties against it | 0.5 day | Forces metadata standards instead of free-form dict |
| 8. Glossary hierarchy | Extend `Synonym` to support parent/child term relationships | 0.5 day | Cardmember demo benefits immediately (CM → CardmemberAccount → AccountTransaction hierarchy) |
| 16. PII flow tracking | Edge property `propagates_pii: bool` populated from KC lineage | 0.5 day | Plugs the gap our BQ-jobs lineage doesn't reach |
| 17. Row-level DQ rule types | Type `DataQualityRule.rule_kind` as enum: `range`, `not_null`, `set`, `regex`, `uniqueness` instead of free-form string | 1 hour | Future-proofs when Dataplex Auto DQ ramps |
| 20. 7 DQ dimensions | Extend `DataQualityRule.dimension` enum to all 7 | 1 hour | Trivial; aligns with KC convention |
| 25. KC `data-quality-scorecard` aspect | When Dataplex enabled, ingest scorecard verbatim into `DataQualityRule` nodes | 0.5 day | We already have the node type |
| 27. KC auto-descriptions | If KC enabled in project, ingest KC's per-column AI descriptions with `source="knowledge_catalog"` weight 4 | 1 day | Adds another independent witness; multi-source corroboration kicks in |
| 29. Example queries from history | Surface `example_queries[]` on table inspector (we already have the corpus) | 30 min | KC promises this; we have the data; we just don't expose it |

### Bucket C — Explicitly skip (over-engineering for v1)

| KC feature | Why skip |
|---|---|
| 2. Dark data discovery (PDFs in GCS) | We design our own PDF parser in the Ingest Agent. Don't pay the GCS scan cost for one demo table |
| 6. Entry Groups as access-control units | Single tenant for v1. Add when N business units share Synapse |
| 9. Data Products framework | Org-political construct, not a graph-correctness need. Defer indefinitely |
| 10. KC's semantic search UI | We have Streamlit UI. Don't build a duplicate search |
| 12. Pub/Sub metadata change feeds | Over-engineering until we have N concurrent readers |
| 14. Multi-region lineage | We're single-region forever for v1 |
| 15. OpenLineage REST endpoints | We don't have N external lineage producers pushing |
| 18. KC's full aggregate-rule library | Only matters at scale; row-level rules cover the cardmember demo |
| 19. Rule templates | Premature abstraction; revisit at >50 rules |
| 22-24. KC's DQ execution + identity + filter machinery | Operational concern, separate workstream from graph correctness |
| 26. Cloud Logging + email alerting | Operational, Slack-bot territory |
| 32. Local MCP Toolbox via KC | We build our own MCP server (see `MCP_SERVER_SPEC.md`) |
| 33. KC's discovery agent | We design our own deep-research agent (see `DEEP_RESEARCH_AGENT_SPEC.md`) |

**Tally:** ✅ 12 already have · ➕ 8 should add (~4 days work) · ❌ 13 skip

---

## 4. The integration architecture — KC as source #11

Today Synapse fuses 10 sources. KC becomes the 11th:

```
                        ┌──────────────────────────────────┐
KNOWLEDGE CATALOG   ───▶│  source = "knowledge_catalog"    │
(Google-managed)        │  weight = 4 (= BQ)               │
                        │  trust tier on conflict =        │
                        │    just below MDM, above LLM     │
                        └─────────────┬────────────────────┘
                                      │
                                      ▼
                        ┌──────────────────────────────────┐
                        │   SYNAPSE GRAPH                  │
                        │  10 + KC = 11 sources fused via  │
                        │  Provenance envelope             │
                        └─────────────┬────────────────────┘
                                      │
                                      ▼
        ┌─────────────────────────────────────────────────────────┐
        │ Consumer Agent · Ingest Agent · MCP Server · Streamlit │
        └─────────────────────────────────────────────────────────┘
```

### What KC contributes per node type

| KC fact | Synapse target |
|---|---|
| Entry for `bq_table_X` | `Table` node — adds `kc_entry_name`, `kc_aspects[]`, KC's auto-description as `ai_generated_description` (source=KC, not llm_generated) |
| KC column path | `Column` node — adds `kc_path_name`, KC PII markers as a second witness alongside our policy-tag-derived PII |
| `data-quality-scorecard` aspect | `DataQualityRule` nodes (one per rule), with `source="knowledge_catalog"`, dimension from KC's 7-enum |
| KC business glossary term | `Synonym` node with `business_unit` from KC entry group |
| KC entry link of type `synonym` | Synapse `HAS_SYNONYM` edge |
| KC entry link of type `definition` | Synapse new edge type `DEFINED_BY` (new — minor schema addition) |
| KC entry link of type `schema-join` | Synapse `EQUIVALENT_TO` edge |
| KC lineage | Synapse `UPSTREAM_OF` edge with `kc_lineage_id` property |
| KC auto-description for column | `Column.ai_generated_description` with `source="knowledge_catalog"` |

### Source weights — where KC sits

Updated source weights:

| Source | Weight | Rationale |
|---|---|---|
| `human_approval` | 10 | Trump card |
| `metric_catalog` | 5 | Human-curated, single-purpose |
| `glossary` | 5 | Human-curated |
| `bq` | 4 | Ground truth from warehouse |
| `dq_engine` | 4 | System-attested rule eval |
| `knowledge_catalog` | **4 (new)** | Google-managed metadata; equivalent trust to BQ direct |
| `mdm` | 3 | Org canonical metadata (may lag) |
| `baseline_lookml` | 3 | Human-vouched dim model |
| `table_catalog` | 3 | Org canonical registry |
| `corpus` | 1 (per obs) | Empirical evidence from real queries |
| `usage` | 1 (per obs) | Telemetry |
| `llm_generated` | 1 | AI inference, can't promote past `inferred` |

### Why weight=4 for KC, not 5

KC is **automated**, not human-curated. It runs Gemini under the hood for some facts (descriptions) and reads BQ INFORMATION_SCHEMA for others (schema). Its trust profile matches BQ direct ingestion, not human-asserted glossary entries. We don't treat it as a steward; we treat it as a managed metadata service.

### Conflict resolution

| KC says | Synapse says | Winner |
|---|---|---|
| Column is PII | MDM says it's not | MDM wins (weight 3 + human-curated)? No — actually surface as `conflict`. Stewards review. |
| Auto-description X | Our LLM enrichment Y | Both render; if they agree, multi-source corroboration promotes to `inferred`. If they disagree, both show with their respective sources. |
| Auto-description X | MDM business_name Y | MDM wins on `business_name` field (MDM owns it); KC populates `ai_generated_description` separately. No conflict. |
| Lineage edge A→B | Our corpus lineage A→C | Both edges exist (different `to_uri`); no conflict. |

---

## 5. KC loader implementation plan

### Discovery prerequisite

Before any loader code: confirm KC is enabled in `prj-d-ea-poc` for the `axp-lumi.dw` dataset. Probe:

```
gcloud dataplex catalog entries list \
  --project=prj-d-ea-poc --location=global \
  --filter='entry_source.system="BIGQUERY" AND fully_qualified_name~"custins_customer_insights_cardmember"'
```

If empty result: KC not indexed; loader is N/A until KC's BQ ingester runs against the dataset. If non-empty: proceed.

### Loader contract

`synapse/synapse/loaders/kc_loader.py`:

```
def load_kc_for_table(
    table_name: str, *,
    project: str, location: str = "global",
    out_dir: Path,
    fetch_lineage: bool = True,
    fetch_dq_scorecard: bool = True,
    fetch_glossary_links: bool = True,
) -> LoadResult:
    ...
```

Hits KC REST API for:
1. `projects.locations.searchEntries` with filter on the table's FQN → finds the Entry
2. `projects.locations.entryGroups.entries.get?view=FULL` → returns Entry + all attached Aspects
3. If `fetch_lineage`: `projects.locations.lineage:search?target=<entry>` → upstream + downstream events
4. If `fetch_dq_scorecard`: fetch the `data-quality-scorecard` aspect specifically
5. If `fetch_glossary_links`: list Entry Links where this entry is source or target

Writes to:
- `kc_cache/<table>.json` — Entry + Aspects + auto-description per column
- `lineage/<table>__kc.json` — KC-sourced lineage events (merged into `lineage_upstream` / `lineage_downstream`)
- `dq_rules/<table>__kc.json` — KC scorecard rules (merged into `dq_rules/`)
- `glossary_links/<table>.json` — entry links to glossary terms

Existing graph builder ingesters extended to consume these via separate `_ingest_kc_*` functions tagged `source="knowledge_catalog"`.

### Effort

| Step | Effort |
|---|---|
| KC enablement probe + IAM check | 30 min |
| `kc_loader.py` skeleton + REST client | 0.5 day |
| Aspect + Entry parsing | 0.5 day |
| Lineage + scorecard ingestion | 0.5 day |
| `_ingest_kc_*` in graph builder | 0.5 day |
| Test on cardmember table | 0.5 day |
| Documentation update | 0.5 day |

**Total: 3 days.** First day delivers visible signal (KC's auto-descriptions appearing on cardmember columns as a second AI witness next to our enrichment).

---

## 6. What KC explicitly cannot do for AmEx (don't expect)

| Capability | Why KC can't do it |
|---|---|
| Per-fact provenance with calibrated confidence | KC stores facts; not who-said-what-when. No multi-source breadth gating. |
| Tribal knowledge from analyst SQL corpus | KC ingests BQ job history; doesn't extract aggregations / case-whens / joins as first-class facts the way `lumi_final/lumi/sql_to_context.py` does |
| Context-keyed synonyms (CM = Cardmember in Finance / CM = Communication Module in Marketing) | KC's glossary is flat per-term; no business-unit-scoped disambiguation |
| Steward arbitration loop with negative-training memory | KC has no concept of rejections feeding future LLM prompts |
| Custom skill.md governing what the LLM can/can't claim | KC's AI features are managed; can't inject org-specific rules |
| Honest "we don't know" for facts under RLS | KC just shows what it indexed; doesn't surface gaps |
| AmEx-specific column-name corrections (fico vs fico_score) | These come from BQ failed-query logs + corpus; KC doesn't surface them |
| Confidence-typed answers to NL questions | KC's MCP server returns facts; doesn't gate by tier or surface provenance |

These are exactly the things Synapse exists to do. KC is the substrate; Synapse is the AmEx-specific reasoning layer on top.

---

## 7. Reading order if you're new to this

1. This doc — establishes vocabulary
2. `synapse/synapse/graph/store.py` — the typed graph + Provenance envelope
3. `synapse/docs/MCP_SERVER_SPEC.md` — our MCP tool surface (later compares to KC's MCP)
4. `synapse/synapse/loaders/lumi_loader.py` — how we ingest a single source today
5. KC official docs (4 URLs in §1) — for production-grade detail
6. `semantic-graph/docs/KC_VS_KC_PLUS_SYNAPSE.md` — the architecture decision doc (companion to this)

---

## 8. Open questions for AmEx-specific decisions

1. **Is Dataplex (Knowledge Catalog) enabled in `prj-d-ea-poc`?** Needs verification. If yes, what dataset coverage today?
2. **Does AmEx have an existing KC enablement on a different project?** If yes, can we cross-project query?
3. **Does AmEx have an internal glossary or term taxonomy already imported into KC?** If yes, we lean on that for the Acropedia integration story.
4. **Is there a Lineage MCP server already deployed at AmEx?** Per KC §35, this is a service Google ships. If AmEx has it on, we can call it from our Consumer Agent for cross-table lineage queries.

Answer these and we lock the loader spec.

---

## 9. Summary in one sentence

**Knowledge Catalog is the Google-managed metadata substrate; Synapse is the AmEx-specific confidence-typed reasoning graph; the right architecture is Synapse-fuses-KC-as-one-of-eleven-sources, NOT one-replaces-the-other.**
