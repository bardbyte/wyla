# LUMI — Architecture & Design

> **Status:** Production-tracking. All four context-engineering commits landed; pipeline runs end-to-end against real prod data with `python -m lumi plan --with-llm` and `python -m lumi execute`. 142 tests passing.
>
> **Goal:** Highest-accuracy NL→LookML retrieval from Radix by giving Gemini every piece of deterministic evidence we can extract from on-disk metadata sources (MDM API, baseline LookML, gold SQL queries), structured for grounded reasoning rather than pattern-matching.
>
> **Sibling docs:**
> - `DESIGN.md` — original architecture (workflow vs agent classification, 7-stage flow rationale, ADK construct mapping)
> - `LUMI_BUILD_PLAN.md` — session-by-session implementation plan
> - `MORNING_TESTING_PLAN.md` — runbook for the work laptop
> - `.claude/skills/lookml/SKILL.md` — LookML output completeness rules (the spec Gemini must satisfy)

---

## TL;DR

LUMI enriches existing LookML views from three sources of truth (MDM business metadata + baseline view files + gold NL→SQL queries) using Gemini 3.1 Pro. The system runs as a 7-stage workflow with a human-approval gate between Phase 1 (cheap, planning) and Phase 2 (expensive, enrichment). Every Gemini call receives a **densely-structured deterministic context payload** built from sqlglot fingerprints + lkml-parsed baselines + an expanded MDM digest, plus per-column **grounding signals** (PK candidates with evidence chains, join hints with cardinality, observed values, naming patterns, PII roles) and a **table narrative** (holistic semantic summary). Every field the LLM emits carries a confidence label (`grounded` | `inferred` | `guessed`). Plans carry an authoring stamp (`llm` vs `skeleton`). Every replacement of a baseline value is logged. Every fallback path returns the deterministic skeleton so the pipeline never crashes.

This is **context engineering as the primary architectural concern**, not a bolt-on. The bet: for AmEx-internal columns like `cm11` (which exist in zero training corpora), retrieval accuracy depends entirely on what evidence we surface to the model.

---

## 1. Problem statement

### 1.1 What we're building

A semantic layer (LookML views + explores + golden NL questions) over ~30 BigQuery tables in `axp-lumi.dw.*` that drives Radix — an internal NL2SQL retrieval system. Radix's accuracy depends on the semantic layer being:

- **Complete:** every queryable field has a name + description + type + tags
- **Correct:** join cardinality, primary keys, partition columns, value formats are accurate
- **Grounded:** descriptions reflect what columns actually mean, not LLM guesses

The hard constraint: AmEx column names (`cm11`, `pmdl_*`, `acct_xref_id`, `cust_insights_*`) are **opaque** to any LLM trained on public data. Out-of-the-box, Gemini guesses semantics. The pipeline's job is to replace guessing with grounded evidence from on-disk sources.

### 1.2 The cm11 test

If `cm11` has zero description in MDM, zero description in baseline LookML, and never appears in a glossary — but gold queries show it `JOIN`ed to `customer_master.cust_id` and `COUNT(DISTINCT cm11)`-ed across 4 queries, AND MDM tags it with `pii_role_id="NGBD-SDE-CM11"` — then Gemini should write something like:

```lookml
dimension: cm11 {
  type: string
  sql: ${TABLE}.cm11 ;;
  primary_key: yes
  hidden: yes
  label: "Cardmember Identifier (Internal)"
  description: "Internal cardmember-grain identifier (PII role: SDE-CM11). Used as join key to customer_master.cust_id; appears in COUNT(DISTINCT) aggregations as the cardinality measure for cardmembers."
  tags: ["pii", "cardmember", "ngbd-sde", "identifier"]
}
```

…and mark `confidence: grounded` (because every claim above traces to a deterministic signal). Before our context-engineering work, Gemini would have written `description: "Customer Member 11"` and called it grounded — pattern matching on the column name with no actual evidence.

### 1.3 Generalization is the goal

The user's framing repeated throughout the build: **"LookML that answers queries we haven't seen yet."** Pattern-matched output doesn't generalize; type-grounded output does. If Gemini understands that `cm11` IS a cardmember-grain identifier (semantic type), it will correctly include `cm11` in `drill_fields`, propose joining cardmember tables on it, and generate sensible NL questions using "cardmember" as the natural-language term — for queries the gold corpus never contained.

---

## 2. Architecture overview

### 2.1 Layered system, top to bottom

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         LUMI PIPELINE (7 stages, 2 phases)                 │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  PHASE 1 — cheap, deterministic + cheap LLM                                │
│  ┌─────────┐   ┌──────────┐   ┌────────┐   ┌────────────────────┐          │
│  │  Parse  │──▶│ Discover │──▶│ Stage  │──▶│ Plan (LLM-authored │          │
│  │ sqlglot │   │ MDM+lkml │   │ rank   │   │  with full context)│          │
│  └─────────┘   └──────────┘   └────────┘   └─────────┬──────────┘          │
│                                                      │                     │
│  ════════════════════════════════════════════════════▼═════════════════    │
│  HUMAN APPROVAL GATE  ←  review_queue/<table>.plan.md  + approval.json    │
│  (file-system blocker, not an ADK construct)                              │
│  ════════════════════════════════════════════════════▼═════════════════    │
│                                                      │                     │
│  PHASE 2 — expensive, Gemini-driven                  │                     │
│  ┌──────────┐   ┌──────────────────┐   ┌─────────────────────────┐         │
│  │ Enrich   │◀──│ Validate         │──▶│ Publish                 │         │
│  │ (per     │   │ - coverage_check │   │ - additive merge        │         │
│  │  table,  │   │ - sql_recon...   │   │ - quality threshold     │         │
│  │  parallel│   │ - regression     │   │ - 4 catalog JSONs       │         │
│  │  capped) │   │   detection      │   │ - proposed_overwrites.md│         │
│  └──────────┘   └──────────────────┘   │ - uncertain_fields.md   │         │
│                                        └─────────────────────────┘         │
└────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 The context payload — what flows into every Gemini call

```
                ┌──────────────────────────────────────────────┐
                │            ON-DISK SIGNAL SOURCES            │
                ├──────────────────────────────────────────────┤
                │                                              │
                │   data/gold_queries/Q*.sql   (138 queries)   │
                │   data/looker_master/**/*.view.lkml  (29)    │
                │   data/mdm_cache/<table>.json        (29)    │
                │   data/learnings.md          (per-table)     │
                │   .claude/skills/lookml/SKILL.md             │
                │                                              │
                └────────────────┬─────────────────────────────┘
                                 │
            ┌────────────────────┼────────────────────┐
            ▼                    ▼                    ▼
    ┌───────────────┐    ┌──────────────┐    ┌────────────────┐
    │ sql_to_       │    │   mdm.py     │    │  _parse_       │
    │   context.py  │    │   _digest    │    │  baseline_view │
    │  (sqlglot)    │    │  (lkml-free) │    │   (lkml lib)   │
    └──────┬────────┘    └──────┬───────┘    └───────┬────────┘
           │                    │                    │
           ▼                    ▼                    ▼
    ┌─────────────────────────────────────────────────────────┐
    │                     TableContext (Pydantic)             │
    │                                                         │
    │  ─── from sqlglot fingerprint ───                       │
    │  tables / columns_referenced                            │
    │  aggregations / case_whens / filters                    │
    │  ctes_referencing_this / temp_tables_referencing_this   │
    │  joins_involving_this / date_functions                  │
    │  select_aliases  ◄─── analysts' own glossary            │
    │                                                         │
    │  ─── from MDM (~30 fields per col, 17 table-level) ───  │
    │  mdm_columns: [{name, business_name, description, type, │
    │                 is_primary, is_dedupe_key, pii_role_id, │
    │                 partition/cluster info, derived_logic,  │
    │                 attribute_format, attribute_details_    │
    │                 extra (catch-all), ...}]                │
    │  mdm_dataset_details: {table_type, feed_type, retention,│
    │                        is_internal, is_history_required,│
    │                        ..., mdm_dataset_extra}          │
    │  mdm_ownership: {business_contacts, tech_contacts,      │
    │                  imr_queue, aim_id, ...}                │
    │                                                         │
    │  ─── from baseline LookML (lkml-parsed) ───             │
    │  baseline_dimensions/measures/dimension_groups          │
    │  baseline_view_description / baseline_view_label        │
    │  baseline_sql_table_name / baseline_derived_table_sql   │
    │  baseline_primary_key_column  ◄─── PK NAME, not bool    │
    │  baseline_extends_chain / baseline_sets / parameters    │
    │  baseline_access_filter  ◄── never overwrite (security) │
    │  baseline_drill_fields_curated                          │
    │  baseline_filtered_measures  ◄── canonical slicing      │
    │  baseline_sql_aliases  ◄── human-curated synonyms       │
    │  baseline_quality_signals: {dim counts, gap counts}     │
    └────────────────────────┬────────────────────────────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
    ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐
    │ grounding.py  │  │ narrative.py │  │ Baseline gap    │
    │ (per-column   │  │ (holistic    │  │ analysis        │
    │   evidence)   │  │  table view) │  │ (in enrich.py)  │
    └───────┬───────┘  └──────┬───────┘  └────────┬────────┘
            │                 │                   │
            └─────────────────┼───────────────────┘
                              ▼
                ┌──────────────────────────────┐
                │   ASSEMBLED ENRICH PROMPT    │
                ├──────────────────────────────┤
                │ 1. Base template             │
                │ 2. Approved plan (scope)     │
                │ 3. Baseline gap analysis     │
                │ 4. Table narrative ◄─────────┼── HOLISTIC FIRST
                │ 5. Grounding signals ◄───────┼── EVIDENCE SECOND
                │ 6. Confidence rules          │
                │ 7. SKILL.md sections 1-7     │
                └──────────────────────────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ Gemini 3.1 Pro (T=0)   │
                  │ output_schema =        │
                  │   EnrichedOutput       │
                  │ self-repair retry × 2  │
                  └───────────┬────────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │   EnrichedOutput       │
                  │ - view_lkml            │
                  │ - derived_table_views  │
                  │ - explore_lkml         │
                  │ - filter_catalog       │
                  │ - metric_catalog       │
                  │ - nl_questions         │
                  │ - field_confidences ◄──┼── per-field grounded/inferred/guessed
                  │ - uncertain_fields  ◄──┼── what the LLM admitted it guessed
                  │ - proposed_overwrites  │
                  └────────────────────────┘
```

### 2.3 Per-column grounding evidence chain (the grounding.py output)

```
Column: cm11
        │
        ├── name pattern matches *_id / *_xref_id / cmNN  →  +5 GROUNDED
        ├── used as JOIN key in 4 queries                 →  +4 GROUNDED
        ├── COUNT(DISTINCT cm11) seen in 2 queries        →  +3 GROUNDED
        ├── MDM pii_role_id = "NGBD-SDE-CM11"             →  +5 GROUNDED
        ├── MDM is_primary = false                        →   0
        └── No description in MDM, no description in
            baseline                                       →  +0
                                                  ─────────────
                                                   total: 17 → GROUNDED PK candidate

        join_hints:
          cm11 → customer_master.cust_id (many_to_one) [OBSERVED, fingerprint]

        observed_values: (none — high cardinality, expected)

        column_confidence: GROUNDED
          - reason: "MDM business_name + 4 query usages + JOIN evidence"
```

---

## 3. The four-commit context-engineering arc

We arrived at the current architecture through four substantial commits, each driven by direct user observation that some specific signal was being thrown away. Each is summarized below with the "why now" and the tradeoff considered.

### 3.1 Commit 1 — Expanded MDM digest (`feat(mdm): expanded digest captures every signal MDM exposes`)

**Trigger.** User pointed out that `dataset_details.data_desc` was the most concise table summary available and we weren't using it for narrative. On running `scripts/explore_mdm_payload.py` against three real tables (`custins_customer_insights_cardmember`, `custins_customer_insights_product`, `acadw_acquisition_us`), we discovered the previous digest captured **~5 fields per column out of ~22, and ~3 fields per table out of ~15**. The remainder included high-value signals.

**What we found in the real payloads:**

| Signal | Status before | What it unlocks |
|---|---|---|
| `sensitivity_details.is_primary` | Not captured | **Top-tier GROUNDED PK signal.** `acadw_acquisition_us` has `[supp_nbr, pcn_nbr]` declared as composite PK — MDM-certified, no inference needed |
| `sensitivity_details.pii_role_id` | Not captured | The cm11 grounding signal: `cm11 → "NGBD-SDE-CM11"`, `cm15 → "NGBD-SDE-CM15"`, `cm_age → "NGBD-SDE-Age"`. Even with no `attribute_desc`, MDM tells us "this is sensitive cardmember data of role X" |
| `attribute_details.is_partitioned` + `partition_position` + `time_partition_type` | Boolean only | Full partition strategy → `always_filter` precision (not "last 90 days" generic; "last 3 months" matching MONTH partition) |
| `attribute_details.is_clustered` + `cluster_position` | Not captured | BQ clustering position → optimal filter ordering in explore (`data_source` first, `bus_seg` second per `cluster_position`) |
| `attribute_details.derived_logic` | Not captured | When MDM has the formula, copy verbatim into the column description |
| `dataset_details.feed_type` (e.g. `"LumiFirst"`) | Not captured | Different from textbook FULL_REFRESH/INCREMENTAL — drives `sql_always_where` freshness |
| `dataset_details.table_type` (e.g. `"DERIVED"`) | Not captured | Architectural role → drives whether table gets many measures (FACT) or many lookups (DIMENSION) |
| `ownership_details.business_contacts` / `imr_queue` | Not captured | View header comment: owner email + oncall queue for production routing |
| `external_reference_details` | Not captured | Was hoped to be join-hint goldmine; **on AmEx data it's `{"source": "DATA"}` placeholder uniformly** — useful negative finding (rules out a whole architectural direction) |

**Implementation tradeoff: explicit fields vs catch-all dicts.** We could have just dumped the entire MDM payload onto `TableContext.mdm_columns_raw: dict` and let downstream code grep. We chose the dual approach: every documented field gets an explicit hoisted field on the digest (so type-checked code paths work), AND every section has an `*_extra` catch-all dict that captures undocumented keys. This way:

- Today's code uses `is_primary` directly without dictionary lookups
- Tomorrow's MDM addition shows up in `mdm_dataset_extra` automatically without code changes
- The narrative renderer can iterate `attribute_details_extra` when present

**Files:** `lumi/mdm.py` (`_digest` rewrite, ~250 LOC), `lumi/sql_to_context.py` (`_build_mdm_dataset_details`), `lumi/schemas.py` (`mdm_dataset_details`, `mdm_ownership`), `tests/test_mdm_digest.py` (17 tests).

### 3.2 Commit 2 — Baseline LookML deep extraction (`feat(baseline): deep LookML extraction — every human-curated signal`)

**Trigger.** The original `_parse_baseline_view` produced only field-list counts + gap quality signals. But baseline LookML files contain extensive human curation we were treating as opaque text and feeding to Gemini as a string blob.

**Newly extracted signals** (all become first-class fields on `TableContext`):

| Signal | Why it matters |
|---|---|
| `baseline_view_description` | Most concise human-authored table summary when present |
| `baseline_view_label` | Curated display name |
| `baseline_sql_table_name` | Authoritative BQ FQN; overrides LumiConfig default |
| `baseline_derived_table_sql` | When baseline IS a derived_table, the SQL is a style-guide for new DTs |
| `baseline_primary_key_column` | Actual NAME of PK dim (was just `bool`) — critical for preservation |
| `baseline_extends_chain` | Refinement chain — new fields go on the refining view, not the base |
| `baseline_sets` / `baseline_parameters` | Pre-curated structural blocks |
| `baseline_access_filter` | **Security model — must NEVER be touched by enrichment** |
| `baseline_drill_fields_curated` | Match the team's drill ordering convention |
| `baseline_filtered_measures` | Pre-filtered measures (e.g. `revenue_consumer { filters: [bus_seg: "Consumer"] }`) reveal canonical slicing patterns; new filtered measures should follow the same naming |
| `baseline_sql_aliases` | When dim NAME differs from source column (`customer_segment ← bus_seg`) — human-curated synonym mapping for tag preservation |

**Implementation surprise.** lkml's parser uses `__all` suffixes and pluralization for multi-occurrence keys (`extends → extends__all`, `access_filter → access_filters`, measure-level `filters → filters__all`). Our extractor checks all variants so the lookup works regardless of which form lkml emits.

**Files:** `lumi/sql_to_context.py` (`_parse_baseline_view` extension, ~100 LOC), `lumi/schemas.py` (13 new fields with sensible defaults), `tests/test_baseline_deep_extract.py` (12 tests).

### 3.3 Commit 3 — TableNarrative + SQL alias capture (`feat(narrative): TableNarrative + SQL alias capture for holistic context`)

**Trigger.** User's observation: "Currently we send Gemini scattered fragments — table description here, column descriptions there, baseline LookML as raw text. Gemini never gets the holistic 'what IS this table' view that comes from reading 28 column descriptions together." They named it correctly: a TableNarrative.

The accompanying observation: SQL aliases are gold. When a query author writes `SUM(billed_business) AS total_revenue`, the alias name carries domain semantics that the cryptic source column doesn't. We were extracting columns, aggregations, and filters from each query but discarding the aliases analysts chose.

**Two work-streams in this commit:**

1. **`SQLFingerprint.select_aliases`** — extracted via `_extract_select_aliases(tree)` in `sql_to_context.py`. Walks the outer SELECT's projections and pulls `(column, alias, expression)` triples. The new `_peel_to_column` helper handles `COUNT(DISTINCT col) AS unique_customers` correctly by walking through `exp.Distinct` wrappers.

2. **`lumi/narrative.py`** — new module producing `TableNarrative` dataclass + `render_table_narrative()`. Aggregates everything from MDM + baseline + fingerprints into the holistic context block.

**TableNarrative captures:**
- Identity block (table_business_name, table_type, feed_type, retention, BQ FQN, ownership header)
- View-level baseline description + label
- **Description corpus** — per-column, sourced from MDM > baseline > none, scored by source coverage. Reveals which columns have NO description from any source (must be marked `confidence: guessed`)
- **PII role assignments** — the cm11-class grounding signals lifted from MDM
- **Domain aliases** — analysts' glossary, quality-filtered via `is_meaningful_alias` (drops `a`, `t1`, `x`, `tmp`, `count`, `total`)
- **Baseline rename pairs** — `customer_segment ← bus_seg` style synonyms preserved as tags
- **Topical clusters** — Customer / Time / Financial / Status / Codes / Identifiers, derived from MDM `data_category` × naming patterns
- **Named intermediate concepts** — CTE aliases + temp-table names as domain terms
- **Filter-value frequencies** — top-5 most-common WHERE values per column across the corpus = `allowed_values` without BQ access
- **MDM-declared formulas** — when `derived_logic` is populated, surface verbatim
- **Inferred grain** — synthesized from clusters: "Customer × time grain (likely cardmember × day given the cm-prefix columns + reporting-date partition)"

**Render order in the prompt is intentional.** The narrative goes BEFORE the per-column grounding signals. Empirically: Gemini reads "this is a cardmember-grain customer-insights table" first, then reads per-column evidence with that frame already loaded. Inverting the order produces worse output (the model reasons about each column in isolation, then tries to assemble the table-level picture last).

**Alias quality filter design.** Aliases like `a`, `t1`, `tmp_result` are noise; `total_billed_business`, `unique_customers`, `consumer_segment` are signal. Two-tier capture: **all** aliases stored on `fp.select_aliases` for traceability, **only meaningful** aliases reach the narrative. `is_meaningful_alias` requires:
- `len ≥ 4`
- not in `{"a", "b", "c", "x", "t1", ...}` and trivial nouns `{"result", "total", "count", "n"}`
- has `_` separator OR camelCase boundary (looks like a phrase, not a token)

**Files:** `lumi/narrative.py` (~600 LOC), `lumi/sql_to_context.py` (`_extract_select_aliases` + `_peel_to_column`), `lumi/enrich.py` (prompt section wiring), `tests/test_narrative.py` (13 tests).

### 3.4 Commit 4 — Gemini-authored plan stage (`feat(plan): Gemini-authored plans with full grounded context`)

**Trigger.** User observation: "The plan markdown the human reviews is just deterministic tallies. Make Gemini read everything and AUTHOR the plan — substantive reasoning, real risks, real questions. By the time the human reviews, the plan should be a coherent blueprint, not a checkbox list."

**Architecture shift.**

```
BEFORE                                    AFTER
─────                                     ─────
Phase 1 (deterministic):                  Phase 1 (cheap LLM, ~10K tokens/table):
  parse → discover → skeleton tally          parse → discover → skeleton (deterministic)
  → markdown tally                            ↓ pass full context
                                             Gemini authors EnrichmentPlan
                                              with reasoning + grounded
                                              description_summary + real risks
                                            → markdown with Gemini's reasoning

Human reviews tallies, ticks ✅           Human reviews REASONING, ticks ✅
                                          (rejection feedback now meaningful)
```

**Why two LLM calls (plan + enrich) beat one (enrich alone).** Two-pass LLM reasoning > one-pass deep reasoning empirically. The plan call frames + commits to a structure with grounded descriptions per proposal; the enrich call executes within that structure with the same deep context. Less drift, more focused output. The plan stage is also the right place to surface per-column ambiguity to the human BEFORE we burn tokens on full LookML enrichment for a misaligned table.

**Defensive fallbacks at every failure point.**
- `with_llm=False` (default) → deterministic skeleton, same as before
- `all_fingerprints=None` while `with_llm=True` → skeleton with `fallback_reason="all_fingerprints not provided"`
- LlmAgent invocation exception → skeleton with `fallback_reason="<TypeName>: <message>"`
- LLM returns `None` → skeleton with `fallback_reason="LLM returned no usable plan (None)"`
- LLM returns plan with empty proposed dims AND empty proposed measures → skeleton with `fallback_reason="LLM-authored plan had no dimensions/measures"`
- LLM zeroes out token estimates → preserve from skeleton
- LLM drops `fields_to_enrich` → preserve from skeleton

**Plan provenance stamp.** Every plan now carries `authoring: {"mode": "llm" | "skeleton", "reason": str | None}`. The markdown render starts with one of:

```
**Authored by**: 🧠 Gemini (full grounded context)
**Authored by**: ⚠ deterministic skeleton — LLM unavailable: _<reason>_
**Authored by**: deterministic skeleton (re-run with `--with-llm` for richer reasoning)
```

The pipeline summary aggregates `plans_llm_authored` / `plans_skeleton_fallback` counts plus the top-3 fallback reasons so the user knows immediately how many plans actually got Gemini's reasoning vs how many fell back.

**Files:** `lumi/plan_builder.py` (skeleton/LLM split + LlmAgent factory + tolerant JSON parser, ~250 LOC additions), `lumi/schemas.py` (`EnrichmentPlan.authoring`), `lumi/pipeline.py` (provenance counts in `PipelineResult.extra`), `lumi/__main__.py` (`--with-llm` flag), `tests/test_plan_llm.py` (9 tests).

---

## 4. Key design decisions and tradeoffs

### 4.1 Vertex direct, not SafeChain

**Decision:** Service-account JSON + four `GOOGLE_*` env vars. Direct google-genai client. No SafeChain wrapper.

**Tradeoff considered:** SafeChain provides centralized auth + observability. We tried it. Removed it.

**Why removed:**
- Adapter complexity that didn't carry its weight
- Async-path issues that didn't exist with direct ADK ↔ Vertex
- Auth indirection added latency to every call
- ADK's google-genai client picks up the four env vars natively

**Impact:** Simpler call path, fewer failure modes, faster.

### 4.2 sqlglot for SQL parsing, lkml for LookML parsing — never regex, never LLM

**Decision:** All structural parsing goes through proper grammar-aware libraries.

**Why:**
- Regex on SQL fails on edge cases (multi-statement, BQ-specific syntax, escaped strings)
- LLM parsing is non-deterministic AND expensive
- sqlglot understands BQ dialect natively (`backtick.qualified.names`, `EXTRACT(YEAR FROM dt)`, `WITH x AS ...`, CTE chains)
- lkml correctly handles refinements, sets, pluralization quirks (`extends__all`, `access_filters`, `filters__all` on measures)

**Cost:** Two third-party libraries we could in theory replace. Both have been stable enough that the cost is invisible.

### 4.3 Two-phase architecture with human-approval gate

**Decision:** Phase 1 (Parse → Discover → Stage → Plan) stops at a file-system blocker; humans tick `[x] ✅ APPROVED` in `review_queue/<table>.plan.md`; Phase 2 (Enrich → Validate → Publish) only proceeds for approved tables.

**Why a gate?**
- Enrichment is the expensive call (~30K tokens per table, 29 tables). Misaligned plans waste tokens.
- Structural changes (new primary_key, new derived_table, new joins) need human eyes — they shape the semantic layer that downstream Radix queries against.
- Description-only changes can auto-approve; structural ones require explicit ✅.

**Tradeoff:** Adds friction. The friction is intentional. For description-only changes, we have an `auto_approve_descriptions_only` path so the human only sees the structural ones.

**Why a file-system blocker, not an ADK construct?**
- Reviewable in any text editor without running anything
- Resumable across sessions (close the laptop, come back tomorrow, plan files are still there)
- Auditable: git-checkable, diffable
- No special infrastructure

### 4.4 30-character description quality threshold

**Decision:** Baseline descriptions ≥ 30 chars are preserved (assumed human-curated); < 30 chars are replaced by enrichment (assumed Looker auto-generated stub).

**Why this exact rule?**
- Looker auto-generated baselines uniformly produce one-or-two-word descriptions ("Customer ID", "Bus seg")
- 30 chars roughly = a complete short sentence with subject + verb + qualifier
- Human-curated descriptions are almost always much longer than this
- The threshold strikes a balance: protect human signal, fix auto-gen stubs

**Audit trail:** Every replacement lands in `output/proposed_overwrites.md` with baseline vs proposed values + reason. The user reviews before next iteration; if a real human-curated short description got replaced (rare), they push back and we adjust the threshold.

**Tradeoff considered:** Could be 20, 25, 35, 40 chars. We picked 30 as the lower bound where stubs end and curation begins. After running on real data, we can tune. Tags are always cumulative (no threshold needed).

### 4.5 Provenance stamps on every plan

**Decision:** `EnrichmentPlan.authoring = {"mode": "llm" | "skeleton", "reason": ...}`. Every plan, every run.

**Why?** A user observation drove this: "I ran with `--with-llm` and saw no changes — was the LLM actually used?" In their case, every LLM call had failed silently (cm{N} template error + SSL), every plan fell back to the deterministic skeleton, and there was no way to tell from the plan markdown.

**Now:** Every plan markdown opens with an authoring badge. Every run summary reports `plans_llm_authored` / `plans_skeleton_fallback` counts plus top-3 fallback reasons. Failures become visible immediately.

### 4.6 Tolerant LLM-output parsing

**Decision:** Four-strategy parse cascade for LLM JSON output: as-is → strip code fence → extract first balanced object → strip trailing commas. Plus `response_mime_type="application/json"` and `output_schema=EnrichmentPlan`.

**Why all four?** When user ran on real data, four distinct parse failures occurred:
1. `Unterminated string starting at line 194` — output truncated mid-string (4K token cap)
2. `Expecting property name in double quotes line 199` — trailing comma in output
3. Same on lines 192, 129 — same root cause

We bumped `max_output_tokens` to 12K to fix #1, but #2-#4 (trailing commas) is a Gemini quirk that occasionally appears. Belt-and-suspenders parsing handles all observed failure modes without crashing.

### 4.7 Confidence labels and uncertain_fields.md

**Decision:** Every field in `EnrichedOutput.field_confidences` carries `grounded` | `inferred` | `guessed`. Anything `guessed` lands in `EnrichedOutput.uncertain_fields` and surfaces to `output/uncertain_fields.md`.

**Why?** A pipeline that silently emits guesses is dangerous. A pipeline that admits its uncertainty is auditable.

**The contract enforced in the enrich prompt:** "If you would normally write a description by pattern-matching on the column name alone (e.g. `cm11` → 'Customer Member 11'), that's a `guessed` and goes into `uncertain_fields`. Do NOT silently write speculative descriptions as if they were grounded."

**Tradeoff:** More structured output, slightly more tokens. Worth it: trust signal is non-negotiable when LookML lands in production semantics.

### 4.8 Resumable per-table checkpointing

**Decision:** `data/enriched/<table>.json` written per table. On Phase 2 re-run, tables already enriched are skipped unless `--force`.

**Why?** Phase 2 takes ~5-10 minutes for 29 tables × parallel-of-5. If the run crashes at table 17, we don't redo 1-16. If the LookML quality on cornerstone_metrics looks weird, we re-run that one table with `--force --table cornerstone_metrics --dry-run` to iterate on the prompt without burning tokens on the other 28.

### 4.9 Bounded concurrency (semaphore)

**Decision:** Phase 2 enrichment runs `asyncio.gather` with `Semaphore(LumiConfig.max_concurrent_enrichments=5)`.

**Why 5?**
- Lower than 5: each table sits idle waiting for the previous; total wall time = N × per-call time
- Higher than 5: Vertex per-project QPS limits start showing up as 429s + retry storms
- 5 is the empirical knee point for our quota

**Tradeoff vs ADK ParallelAgent.** ParallelAgent provides session-scoped fan-out. Our use case is simpler (one LlmAgent per table, no shared session state) so `asyncio.gather + Semaphore` is more direct. Each enrich call already has its own self-repair retry loop inside the LlmAgent.

### 4.10 Two-SA setup (Vertex + BQ)

**Decision:** Vertex SA in `GOOGLE_APPLICATION_CREDENTIALS`, BQ SA in `LUMI_BQ_KEY_FILE`, billed against `LUMI_BQ_BILLING_PROJECT`. One source command (`setup_lumi_env.sh`) configures both.

**Why?** Different IAM grants, different Google Cloud projects:
- Vertex SA needs `roles/aiplatform.user` on `prj-d-ea-poc`
- BQ SA needs `roles/bigquery.jobUser` on `prj-d-lumi-gpt` + `roles/bigquery.dataViewer` on `axp-lumi`

Trying to use one SA for both is operationally fragile (one SA to manage two grants, easy to over-permission). Two SAs let us isolate: each one carries the minimum role its scope needs.

**Implementation:** `check_bq_access.py` reads `LUMI_BQ_KEY_FILE` first, falls back to `GOOGLE_APPLICATION_CREDENTIALS`. Prints which source it used so mix-ups are visible.

### 4.11 Treat CREATE TEMP TABLE as CTE-equivalent

**Decision:** `CREATE [OR REPLACE] [TEMP] TABLE x AS SELECT ... FROM y, z` — exclude `x` from the real-table list (Looker can't query a session temp table) BUT preserve the inner SELECT structure as `SQLFingerprint.temp_tables[i]` with the same shape as a CTE entry.

**Why?**
- The original fix (just exclude `x`) was correct in spirit but lost the GROUPING of which logic was bundled into which named intermediate
- CTE and temp-table treatment now identical: the named intermediate gets propagated to its source tables' `TableContext`s with structural filters baked
- Reused temp tables become PDT (persistent derived table) candidates — flagged as risks in the plan
- Business-named intermediates (`renewal_fees`, `active_customers`, `q4_2024_revenue`) feed the narrative as domain concepts

### 4.12 Truststore-by-default, comprehensive bypass on demand

**Decision:**
- `lumi/__init__.py` and `apps/lumi/__init__.py` inject truststore on import (works on macOS Keychain where corp root CA is pre-installed)
- `LUMI_INSECURE_TLS=1` env var → comprehensive bypass (stdlib + requests + httpx + google-auth's AuthorizedSession)
- Probe scripts also accept `--insecure` flag

**Why three layers?**
- 95% case: truststore handles corp MITM transparently
- 5% case: corp root CA isn't in Keychain (older laptops, custom configs) — `LUMI_INSECURE_TLS=1` works
- Probe scripts: explicit flag because probes are interactive and the user can decide

**Tradeoff:** Insecure mode disables verification. Mitigated by the env-var requirement (not default) + warnings logged on use.

---

## 5. Module deep dive

```
lumi_final/
├── lumi/
│   ├── __init__.py            # Truststore inject on import
│   ├── __main__.py            # CLI: plan / status / approve / execute
│   │
│   ├── config.py              # LumiConfig — paths, model, project, thresholds
│   ├── schemas.py             # All Pydantic models (TableContext, EnrichmentPlan,
│   │                            EnrichedOutput, etc.)
│   ├── guardrails.py          # check_parse_and_discover, check_planning,
│   │                            check_approvals, check_enrichment, check_evaluation,
│   │                            check_pre_publish, check_sql_reconstruction
│   │
│   ├── sql_to_context.py      # parse_sqls (sqlglot) + discover_tables
│   │                          #   _parse_baseline_view (lkml deep extract)
│   │                          #   _extract_select_aliases (analyst glossary)
│   │                          #   _build_mdm_dataset_details (collapse)
│   │
│   ├── mdm.py                 # CachedMDMClient + HttpMDMClient
│   │                          #   _digest (the 30-field-per-col + 17-field-per-table
│   │                          #            extraction with *_extra catch-alls)
│   │
│   ├── grounding.py           # build_grounding_signals(ctx, all_fps, ctx_by_table)
│   │                          #   per-column ColumnUsageProfile
│   │                          #   primary_key_candidates (ranked, scored, with reasons)
│   │                          #   join_hints (observed + cardinality-inferred)
│   │                          #   always_filter_candidates / hidden_candidates
│   │                          #   filtered_measure_candidates
│   │                          #   observed_values_by_column (allowed_values w/o BQ)
│   │                          #   column_confidence (grounded | inferred | guessed)
│   │                          # render_grounding_signals → dense Markdown
│   │
│   ├── narrative.py           # build_table_narrative(ctx, all_fps)
│   │                          #   identity + ownership header
│   │                          #   description corpus grouped by topical cluster
│   │                          #   PII role assignments (cm11-class signals)
│   │                          #   domain aliases (analysts' glossary, quality-filtered)
│   │                          #   filter-value frequencies
│   │                          #   inferred grain synthesis
│   │                          # is_meaningful_alias (drops a, t1, x, tmp, count, ...)
│   │                          # render_table_narrative → dense Markdown
│   │
│   ├── plan_builder.py        # build_enrichment_plan_skeleton (deterministic)
│   │                          # build_enrichment_plan(with_llm=True) — Gemini-authored
│   │                          # _author_plan_with_llm — ADK LlmAgent + Runner
│   │                          # _parse_plan_response — tolerant 4-strategy parser
│   │                          # _maybe_disable_tls — LUMI_INSECURE_TLS env support
│   │                          # save/load_plan_json — persistence for Phase 2
│   │                          # format_enrichment_plan_markdown — review file render
│   │
│   ├── enrich.py              # enrich_table(ctx, plan, all_fps, contexts_by_table)
│   │                          #   build_enrichment_prompt — assembles all 7 sections
│   │                          #   self-repair retry loop on guardrail failure
│   │                          #   confidence-labeling rules section
│   │
│   ├── validate.py            # coverage_check (deterministic, no LLM)
│   │                          # reconstruct_sql_check (pre-publish safety net)
│   │                          # build_evaluator_loop (LoopAgent with gap_fixer)
│   │
│   ├── publish.py             # additive_merge_view (with quality threshold)
│   │                          # publish_to_disk → output/views, models, catalogs
│   │                          # _render_overwrites_md / _render_uncertain_md
│   │
│   ├── pipeline.py            # run_plan_phase / run_execute_phase (functional)
│   │                          # LumiPipeline (back-compat class wrapper)
│   │                          # PipelineResult — JSON-serializable run summary
│   │                          # _enrich_many — bounded concurrency via Semaphore
│   │
│   ├── approval.py            # collect_approvals — parses [x] ✅/❌ from .plan.md
│   ├── status.py              # 3-zoom lumi_status.md generator
│   ├── planner.py             # Old deterministic planner (kept for back-compat)
│   │
│   └── prompts/
│       └── enrich_view.md     # The enrichment prompt template
│
├── scripts/                   # Probes (run on the work laptop)
│   ├── setup_lumi_env.sh      # Two-SA env setup (Vertex + BQ)
│   ├── check_vertex_gemini.py # Vertex preflight
│   ├── check_bq_access.py     # BQ access probe
│   ├── inspect_sa_json.py     # SA JSON field inspector
│   ├── diag_network.sh        # Corp-network 407/SSL diagnostic
│   ├── no_proxy_shell.sh      # Nuke proxy env vars
│   ├── probe_mdm.py           # MDM cache hydrator
│   ├── explore_mdm_payload.py # MDM schema-inference dumper
│   ├── probe_baseline_lookup.py # Verify baseline match rate
│   ├── probe_pipeline_dry_run.py
│   ├── probe_review_queue.py
│   ├── probe_enrich.py        # Single-table enrich (real Gemini)
│   ├── probe_one_table.py     # Phase 1 + auto-tick + Phase 2 dry-run + diff
│   ├── probe_validate.py
│   ├── probe_publish.py
│   ├── run_session1.py        # Driver for parse + discover
│   ├── run_session1.py        # Phase 1 pure-deterministic driver
│   ├── run_phase1.py          # Standalone Phase 1 driver (bypass for CLI issues)
│   ├── excel_to_queries.py    # Excel → Q*.sql ingestion
│   ├── fetch_lookml_master.py # GHE → data/looker_master/ (network)
│   ├── import_lookml_local.py # Local Looker repo → data/looker_master/
│   ├── fetch_baselines.py     # Per-table baseline fetcher (legacy)
│   └── diagnose_parse_failures.py
│
├── apps/
│   └── lumi/                  # ADK web entry — `adk web apps/`
│       ├── __init__.py        # Truststore inject
│       └── agent.py           # root_agent (LlmAgent for adk web tracing)
│
├── tests/                     # 142 tests, 5 e2e skipped (need prod data)
│   ├── test_sql_to_context.py
│   ├── test_baseline_quality.py
│   ├── test_baseline_deep_extract.py
│   ├── test_mdm_digest.py
│   ├── test_grounding.py
│   ├── test_narrative.py
│   ├── test_plan_llm.py
│   ├── test_pipeline_real.py  # E2E with mocked LLM
│   ├── test_session1_e2e.py   # Skipped — needs prod data
│   ├── test_publish.py
│   ├── test_enrich.py
│   ├── test_validate.py
│   ├── test_approval.py
│   └── fixtures/              # Sample SQLs + LLM response fixtures
│
└── data/                      # Inputs + intermediates + cache
    ├── gold_queries/          # Q*.sql (138 queries from Excel)
    ├── looker_master/         # Looker repo mirror (29 .view.lkml files)
    ├── mdm_cache/             # Per-table MDM digest JSONs (29 files)
    ├── plans/                 # Phase 1 output: <table>.plan.json
    ├── enriched/              # Phase 2 checkpoint: <table>.json (resumable)
    ├── session1_output.json   # Consolidated TableContext dict
    └── learnings.md           # Iteration learnings
```

---

## 6. Failure modes and recovery

The pipeline is designed so that **no single failure crashes the run**. Every stage has graceful degradation.

| Failure | Behavior | User sees |
|---|---|---|
| Vertex unreachable (auth/network) | Per-table fallback to deterministic skeleton | `plans_skeleton_fallback: N`, `fallback_reasons_top3`, plan markdown opens with `⚠ deterministic skeleton — LLM unavailable: <reason>` |
| sqlglot parse error on one query | Quarantined as `parse_error="empty_input"` (real Excel blanks) or `parse_error="<reason>"`. Other queries continue | `Parse: 122/138 parsed, 35 empty cells, 1 real errors` |
| Empty MDM cache (probe didn't run) | All tables get `mdm_coverage_pct=0`, descriptions sparser. Pipeline runs but with weaker grounding | Warning at run start: `"MDM cache empty — run probe_mdm.py --save data/mdm_cache/"` |
| Missing baseline file for one table | `existing_view_lkml=None`, `baseline_quality_signals={}`. Enrichment generates from scratch instead of merging | Probe `probe_baseline_lookup.py` shows `✗ NO MATCH` for that table |
| LLM returns truncated JSON | Parser tries 4 repair strategies; on total failure → skeleton fallback for that one table | Plan-stage warning includes first 200 chars of unparseable output |
| Validation regression (previously-covered query now uncovered) | `check_evaluation` blocks at warn level | Coverage report has `regressions` list; pipeline exits 1 |
| Pending approval (no checkbox ticked) | `check_approvals` blocks Phase 2 | Phase 2 halts with `approval gate FAIL — open review_queue/<table>.plan.md and tick a checkbox` |
| LookML output fails lkml.load | `check_pre_publish` blocks; the bad view doesn't make it to `output/` | Per-view error logged; other views still publish |

The error policy is consistent: **auth/permission/missing-input → halt; per-table runtime errors → log + skip + continue; regression detection → run completes, exit code reflects status.**

---

## 7. The signal hierarchy (what beats what)

When multiple sources disagree, this is the precedence:

```
HIGHEST CONFIDENCE
       ↑
       │  1. MDM sensitivity_details.is_primary = true
       │       → GROUNDED PK, no inference. Other PK candidates ignored.
       │
       │  2. MDM derived_logic (when populated)
       │       → Direct prompt context for the column description.
       │
       │  3. Baseline LookML primary_key: yes
       │       → GROUNDED PK preservation. Enrichment must NOT overwrite.
       │
       │  4. MDM attribute_desc (when populated, length > 0)
       │       → Used as column description; baseline desc only as fallback.
       │
       │  5. Baseline description ≥ 30 chars (assumed human-curated)
       │       → Preserved verbatim; quality threshold prevents overwrite.
       │
       │  6. SQL fingerprint JOIN evidence
       │       → Ground truth for join cardinality; trumps MDM
       │         external_reference_details (which is unreliable on AmEx data).
       │
       │  7. SQL fingerprint COUNT(DISTINCT col)
       │       → Strong PK candidate signal (+3 in PK ranking).
       │
       │  8. MDM business_name + name pattern
       │       → INFERRED, used when no description but pattern matches
       │         (id_like / amount / date / code).
       │
       │  9. SQL alias intelligence (analyst-chosen aliases)
       │       → Domain glossary; passes quality filter then surfaced.
       │
       │ 10. Filter-value frequencies across corpus
       │       → allowed_values without BQ DISTINCT access.
       │
       │ 11. Naming-pattern heuristics (cm_*, *_amt, *_dt, *_flag)
       │       → INFERRED; lowest confidence still surfaced.
       │
       │ 12. LLM training-data prior
       │       → Last resort. Anything not anchored → confidence: guessed.
       ↓
LOWEST CONFIDENCE
```

The bet: by stacking 11 deterministic evidence sources before the LLM's prior, the LLM's reasoning has somewhere to land. Without this stack, Gemini falls back to pattern-matching on column names — which fails on AmEx-internal naming.

---

## 8. What's deferred (and why)

### 8.1 BigQuery `INFORMATION_SCHEMA` + `SELECT DISTINCT`

**Status:** Probe written (`scripts/check_bq_access.py`) and tested. Endpoint correct (`bigquery-dev.p.googleapis.com` PSC route). 404s in our environment due to outstanding IAM grant on the BQ SA.

**What it would give us:** Authoritative column types, allowed_values for low-cardinality fields (50-row LIMIT), partition column verification.

**Workaround:** Fingerprint `observed_values_by_column` + MDM `attribute_format` + MDM `is_partitioned` + `partition_position` covers ~80% of what BQ DISTINCT would. The remaining 20% (low-cardinality columns whose values never appear in WHERE literals) is where we'd see lift when access lands.

**Decision:** Don't block the pipeline on this. Re-run `probe_mdm` is sufficient grounding for Phase 1 enrichment quality. BQ probe runs as a follow-up sprint.

### 8.2 Looker MCP usage statistics

**Status:** Not started.

**What it would give us:** Production-real signal of which fields actually get queried in dashboards (versus which are theoretical drill_fields). Could rank `drill_fields` by real usage instead of fingerprint-corpus frequency.

**Decision:** Defer until post-launch. Requires a Looker API integration we haven't built; the value is incremental over what fingerprint frequency gives us already.

### 8.3 Cross-table description similarity clustering

**Status:** Not started.

**What it would give us:** If two tables have very similar column descriptions, they might be the same entity at different grains (daily vs monthly snapshot of cardmember risk metrics). Could propose unified dimension names across them.

**Decision:** Research-grade. Defer. Current per-table TableNarrative is sufficient for individual-table accuracy; cross-table is post-launch optimization.

### 8.4 LLM-authored Stage / Plan reasoning at the very fine level

**Status:** Plan stage now LLM-authors the `reasoning` field substantively. We don't currently invoke a separate "Plan reasoner" agent that takes the deterministic plan and refines individual proposal descriptions.

**Decision:** The current single-call plan agent does both jobs (read context → emit refined plan with reasoning). Splitting into two LLM passes would double tokens for marginal gain. Revisit if plan output quality plateaus.

### 8.5 Glossary file (Layer 3 from earlier discussion)

**Status:** Decided not to ship. The naming-pattern detection in `_name_pattern_signal` (cm_*, _amt, _dt, _flag, _cd) covers the bulk of what a glossary would. A formal glossary becomes useful when team conventions extend beyond what regex covers; right now it'd be premature.

---

## 9. Future iterations (post-launch)

In rough priority order if this pipeline goes to production:

1. **BQ access** lands → wire `observed_values` from real DISTINCT queries. ~2-day project once IAM is granted.
2. **Looker MCP usage stats** → rank drill_fields and explore design by production reality.
3. **Cross-table description similarity** → unify entity vocabulary across tables.
4. **Glossary file** → formal team-vocabulary capture for column-prefix conventions beyond regex.
5. **PR auto-creation to GHE** → currently `publish_to_github` is a `status: "deferred"` stub that prints manual git commands. Wire the gh CLI for full automation.
6. **Multi-pass enrichment** → if first-pass quality plateaus below 90% coverage, add a second LLM pass that takes coverage gaps as input and patches missing fields.
7. **Looker test harness** → for each enriched view + explore, generate a test that mimics Radix's NL→LookML resolution path and asserts the field selection is correct.

---

## 10. Honesty about what worked vs what didn't

Things we tried that didn't pan out:

- **MDM `external_reference_details` for join hints** — we hoped MDM declared cross-table relationships with cardinality. On AmEx data it's `{"source": "DATA"}` placeholder uniformly. Not useful. We capture it for forward-compat but the join inference stays driven by sqlglot fingerprints.
- **SafeChain wrapper** — added auth indirection without offsetting value. Removed.
- **Hand-coded JSON schema dump** for the MDM probe — the structure varies between tables; we replaced it with a schema-inference pass that auto-deduplicates lists of dicts and shows populate-rate per key. ~50 lines of output for a 193-column table instead of ~5000.
- **Plan stage as deterministic-only** — initially we said "planning should be cheap; the expensive call is enrich." User pushed back: the plan IS the human review surface, so it should be reasoning-rich. We added the LLM authoring path as an opt-in (`--with-llm`) with skeleton fallback at every failure point.

Things that exceeded expectations:

- **`pii_role_id` per column** — we found this by reading the raw MDM payload for `acadw_acquisition_us` and seeing `cm11 → "NGBD-SDE-CM11"`. This single signal solves the cm11 grounding problem we'd worried about for the entire build.
- **`is_primary` per column** — same table had `[supp_nbr, pcn_nbr]` declared as composite PK. When MDM has it, no inference needed.
- **30-char baseline-description threshold** — the 30 number was a guess; on real data it cleanly splits Looker auto-gen stubs from human curation. Worth tuning post-launch but no urgent reason to change.
- **`*_extra` catch-all dicts in MDM digest** — forward-compat without code changes. Already paid off twice when probing surfaced fields we hadn't documented.

---

## Appendix A: Glossary

- **TableContext** — the Pydantic data class consumed by every downstream stage. Carries everything we extracted about one table from sqlglot + MDM + baseline LookML.
- **EnrichmentPlan** — the structured plan output of Phase 1, scope contract for Phase 2.
- **PlanApproval** — the human's decision recorded in `review_queue/<table>.plan.md`.
- **EnrichedOutput** — Phase 2 output: view_lkml, derived_table_views, explore_lkml, catalogs, NL questions, field_confidences, uncertain_fields.
- **Grounding signals** — per-column deterministic evidence (PK candidates, join hints, observed values, etc.) computed from the corpus by `lumi/grounding.py`.
- **Table narrative** — holistic table summary computed by `lumi/narrative.py`. Read by Gemini BEFORE the per-column grounding signals.
- **SKILL.md** — `.claude/skills/lookml/SKILL.md`, the LookML completeness rules. Sections 1-7 are appended to every enrichment prompt.
- **Confidence label** — one of `grounded` | `inferred` | `guessed`, set per field in `EnrichedOutput.field_confidences`. Anything `guessed` lands in `uncertain_fields.md`.
- **Authoring** — `EnrichmentPlan.authoring = {mode, reason}`. Tracks whether each plan was LLM-authored or fell back to the deterministic skeleton.
- **Quality threshold** — the 30-char rule for distinguishing human-curated baseline descriptions from Looker auto-generated stubs.
- **Self-repair retry loop** — when `check_enrichment` flags blocking failures, the failure messages are appended to the prompt and the LLM call retries once. Implemented in `enrich.py`.
- **Resumable checkpoint** — `data/enriched/<table>.json` written per table; subsequent runs skip already-enriched tables unless `--force`.

---

## Appendix B: References

- **Anthropic, "Building Effective Agents"** (Schluntz & Zhang, 2024) — workflow vs. agent classification underlying §1 of `DESIGN.md`.
- **Google ADK documentation** — `LlmAgent`, `SequentialAgent`, `ParallelAgent`, `LoopAgent`, `Runner`, `SessionService`.
- **sqlglot** — BigQuery dialect SQL parsing (https://github.com/tobymao/sqlglot).
- **lkml** — LookML parser/serializer (https://github.com/joshtemple/lkml).
- **`.claude/skills/lookml/SKILL.md`** — the spec our LookML output must satisfy.
- **`MORNING_TESTING_PLAN.md`** — runbook for executing the pipeline on a fresh laptop.
- **`DESIGN.md`** — original architecture document (workflow classification, ADK construct mapping, refinement chain semantics).
- **`LUMI_BUILD_PLAN.md`** — original session-by-session implementation plan.
