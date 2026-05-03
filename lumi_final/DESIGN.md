# LUMI — Design Document

**Status:** Final design, 7-stage flow, pre-implementation
**Author:** Saheb / Claude architecture session
**Last updated:** May 2026 (v2 — added Stage + Plan + human-approval gate)

---

## 1. What LUMI is (and is not)

LUMI is a **workflow**, not an agent.

This distinction matters. Per Anthropic's taxonomy ("Building
Effective Agents," Schluntz & Zhang 2024):

- **Workflow:** LLMs and tools orchestrated through predefined
  code paths. Predictable, debuggable, cheaper.
- **Agent:** LLMs dynamically direct their own processes and
  tool use. More flexible, less predictable, more expensive.

LUMI's flow is fully deterministic: parse → discover → enrich →
validate → publish. The steps don't change based on model output.
The ORDER never varies. This is a workflow.

But WITHIN the workflow, individual nodes use LLM reasoning
(metric extraction, description writing, quality judging). And
the validation stage implements the **evaluator-optimizer** pattern
— a loop that critiques output and retries. This makes LUMI a
**workflow with agentic components**, which Anthropic identifies
as the sweet spot for production systems.

The analogy: LUMI is an assembly line (workflow), not a taxi
(agent). The conveyor belt moves in one direction. But some
stations on the line have skilled workers (LLMs) making judgment
calls. And there's a quality inspector (evaluator) who can send
a piece back for rework.

### Why this classification matters for Google ADK

ADK provides both workflow constructs (`SequentialAgent`,
`ParallelAgent`, `LoopAgent`) and agentic constructs (`LlmAgent`
with tool use). LUMI uses workflow constructs for orchestration
and `LlmAgent` for the reasoning nodes. This is the pattern
Google's own ADK documentation recommends for production pipelines.

---

## 2. Architecture (v2 — 7 stages)

```
                    ┌─────────────────────────────┐
                    │         LUMI Pipeline        │
                    │      (SequentialAgent)        │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 1: Parse              │
                    │         (Tool — no LLM)       │
                    │  sqlglot fingerprint each SQL │
                    │                               │
                    │  IN:  raw SQL strings          │
                    │  OUT: list[SQLFingerprint]     │
                    │                               │
                    │  GUARD: all SQL parses?        │
                    │         CTE counts consistent? │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 2: Discover           │
                    │         (Tool — no LLM + API) │
                    │  group fps by table           │
                    │  fetch MDM per table           │
                    │  load baseline view per table  │
                    │                               │
                    │  OUT: dict[table, TableContext]│
                    │       + ecosystem_brief        │
                    │                               │
                    │  GUARD: ≥50% MDM coverage?     │
                    │         all CTE-internal tbls? │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 3: Stage              │
                    │         (Tool — no LLM)       │
                    │  rank tables by:              │
                    │  - dependencies (CTE first)   │
                    │  - query count (most-used 1st)│
                    │  - complexity (simple first)  │
                    │                               │
                    │  OUT: list[TablePriority]      │
                    │                               │
                    │  GUARD: dep DAG acyclic?       │
                    │         every table ranked?    │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 4: Plan               │
                    │     (ParallelAgent of LlmAgent)│
                    │  CHEAP per-table call (~1K tok)│
                    │  emits EnrichmentPlan          │
                    │  writes review_queue/          │
                    │    <table>.plan.md             │
                    │                               │
                    │  GUARD: each plan has fields?  │
                    │         token budget < 800K?   │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │  ☆ HUMAN APPROVAL GATE ☆     │
                    │    (blocks until decided)     │
                    │                               │
                    │  Human reviews each .plan.md  │
                    │  appends "✅ APPROVED" or     │
                    │    "❌ REJECTED: <reason>"    │
                    │                               │
                    │  PlanApproval per table       │
                    │                               │
                    │  GUARD: all plans decided?     │
                    │         rejected → has feedback│
                    └─────────────┬───────────────┘
                                  │ (only approved tables proceed)
                    ┌─────────────▼───────────────┐
                    │   Stage 5: Enrich             │
                    │     (ParallelAgent)           │
                    │                               │
                    │  For each unique table:       │
                    │  ┌───────────────────────┐    │
                    │  │  LlmAgent (Gemini)    │    │
                    │  │                       │    │
                    │  │  IN:  TableContext     │    │
                    │  │       + ecosystem_brief│    │
                    │  │       + all SQL fps    │    │
                    │  │                       │    │
                    │  │  OUT: EnrichedOutput   │    │
                    │  │       .view_lkml       │    │
                    │  │       .derived_tables  │    │
                    │  │       .explore_lkml    │    │
                    │  │       .filter_catalog  │    │
                    │  │       .metric_catalog  │    │
                    │  └───────────────────────┘    │
                    │                               │
                    │  GUARD: LookML parses?         │
                    │         descriptions 15-200ch? │
                    │         derived tables for CTEs?│
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 6: Validate           │
                    │     (LoopAgent, max_iter=3)   │
                    │                               │
                    │  ┌───────────────────────┐    │
                    │  │  Coverage Checker      │    │
                    │  │  (deterministic)       │    │
                    │  │                       │    │
                    │  │  For each input SQL:   │    │
                    │  │  - measures present?   │    │
                    │  │  - dimensions present? │    │
                    │  │  - filters resolvable? │    │
                    │  │  - explore exists?     │    │
                    │  │  - joins correct?      │    │
                    │  │  - derived tables?     │    │
                    │  └──────────┬────────────┘    │
                    │             │                  │
                    │  ┌──────────▼────────────┐    │
                    │  │  SQL Reconstructor     │    │
                    │  │  (deterministic)       │    │
                    │  │                       │    │
                    │  │  For each gold query:  │    │
                    │  │  1. Read explore def   │    │
                    │  │  2. Trace join chain   │    │
                    │  │  3. Assemble SQL from  │    │
                    │  │     LookML fields      │    │
                    │  │  4. Compare vs gold    │    │
                    │  │     SQL via sqlglot    │    │
                    │  │     AST comparison     │    │
                    │  │  5. Flag mismatches:   │    │
                    │  │     wrong table, wrong │    │
                    │  │     agg, wrong join,   │    │
                    │  │     missing filter     │    │
                    │  └──────────┬────────────┘    │
                    │             │                  │
                    │  ┌──────────▼────────────┐    │
                    │  │  Gap Fixer (LlmAgent)  │    │
                    │  │  Only runs if gaps > 0 │    │
                    │  │  Re-enriches specific  │    │
                    │  │  tables with gap info  │    │
                    │  └───────────────────────┘    │
                    │                               │
                    │  EXIT: coverage ≥ 90%          │
                    │     OR iterations = 3          │
                    │                               │
                    │  GUARD: no regressions         │
                    │         (prev covered stays)   │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Stage 7: Publish            │
                    │         (Tool — no LLM)       │
                    │                               │
                    │  1. Update learnings.md        │
                    │  2. Write output files         │
                    │  3. Push to GitHub branch      │
                    │  4. Create PR                  │
                    │                               │
                    │  GUARD: all LookML lints       │
                    │         diff < 50% per view    │
                    └─────────────────────────────┘
```

### ADK construct mapping (v2 — 7 stages, two phases)

The pipeline splits into TWO phases around the human-approval gate:

```python
from google.adk.agents import (
    SequentialAgent, ParallelAgent, LoopAgent, LlmAgent
)

# Phase 1: cheap, automated planning (Stages 1-4)
lumi_plan_phase = SequentialAgent(
    name="lumi_plan",
    sub_agents=[
        parse_tool,              # Stage 1: sqlglot, no LLM
        discover_tool,           # Stage 2: MDM + baseline, no LLM
        stage_tool,              # Stage 3: prioritize, no LLM
        plan_parallel,           # Stage 4: ParallelAgent of cheap LlmAgent calls
    ],
)

# Stage 4: one cheap LlmAgent per table — emits EnrichmentPlan
plan_parallel = ParallelAgent(
    name="planning",
    sub_agents=[
        LlmAgent(
            name=f"plan_{table}",
            model="gemini-3.1-pro-preview",
            instruction=PLAN_PROMPT,           # ~1K-tok output, schema-bound
            output_schema=EnrichmentPlan,
        )
        for table in prioritized_tables
    ],
)

# ☆ HUMAN APPROVAL GATE — runs between phases ☆
# Not an ADK construct; it's a file-system blocker:
#   review_queue/<table>.plan.md must contain "✅ APPROVED" or
#   "❌ REJECTED: <feedback>" before Phase 2 starts.

# Phase 2: expensive, automated enrichment (Stages 5-7)
lumi_execute_phase = SequentialAgent(
    name="lumi_execute",
    sub_agents=[
        enrich_parallel,         # Stage 5: ParallelAgent of full enrichment
        evaluate_loop,           # Stage 6: LoopAgent (evaluator-optimizer)
        publish_tool,            # Stage 7: git push + PR, no LLM
    ],
)

# Stage 5: full enrichment, ONLY for approved tables
enrich_parallel = ParallelAgent(
    name="enrichment",
    sub_agents=[
        LlmAgent(
            name=f"enrich_{table}",
            model="gemini-3.1-pro-preview",
            instruction=ENRICH_PROMPT,
            output_schema=EnrichedOutput,
        )
        for table in approved_tables          # filtered by PlanApproval
    ],
)

# Stage 6: evaluator-optimizer loop
evaluate_loop = LoopAgent(
    name="evaluator",
    sub_agents=[
        coverage_checker,        # deterministic tool
        gap_fixer,               # LlmAgent, only fires if gaps > 0
    ],
    max_iterations=3,
)
```

### Why split into two phases?

1. **Plans are cheap** (~$0.05 per table); enrichment is expensive (~$0.50).
   A 5-minute review of plans saves a multi-hour rework cycle.
2. **Human catches misalignment cheaply.** A plan markdown reviewer notices
   "this isn't what we want" 100x faster than reviewing 30+ generated
   `.view.lkml` files.
3. **Auditability.** The approval gate creates a paper trail of what
   shipped to Looker — non-negotiable for regulated environments.
4. **CLI maps cleanly:** `lumi plan` runs Phase 1, `lumi execute` runs Phase 2.

### review_queue/ file format

```
review_queue/
├── cornerstone_metrics.plan.md    ← human-readable plan (with frontmatter)
├── cornerstone_metrics.approval.json  ← appears once approved
├── risk_pers_acct_history.plan.md
└── ...
```

**`<table>.plan.md` structure** (auto-generated):
```markdown
---
table_name: cornerstone_metrics
complexity: medium
estimated_input_tokens: 24500
estimated_output_tokens: 9200
queries_using_this: [Q1, Q2, Q3, Q4, Q5, Q6, Q7]
---

# Enrichment plan: cornerstone_metrics

## Reasoning
This table is the primary cornerstone metrics fact table. SQL fingerprints
show 6 aggregations on billed_business + new_accounts_acquired, with
EXTRACT(YEAR/MONTH) on rpt_dt and CASE WHEN on fico_score.

## Proposed dimensions (8)
- bus_seg (string): "Business Segment classification..."
- data_source (string): "Source system identifier..."
[...]

## Proposed dimension_groups (1)
- report (time, datatype=date): timeframes [raw, date, month, quarter, year]

## Proposed measures (4)
- total_billed_business (sum, value_format_name=usd)
- total_new_accounts_acquired (sum)
[...]

## Proposed explore
- base: cornerstone_metrics
- always_filter: report_date last 12 months
- joins: (none — single-table queries)

## Risks
- ⚠ primary_key inference is uncertain — consider compound (bus_seg+rpt_dt+...)
- ⚠ MDM coverage 68% — 3 columns will need LLM-inferred descriptions

## Questions for reviewer
- Should `data_source = 'cornerstone'` become a filtered measure? (filter
  appears in 90% of queries)

---
APPROVAL (append below):
```

**Reviewer appends one of:**
```
✅ APPROVED                        # plain approval
✅ APPROVED — yes to filtered measure on data_source
❌ REJECTED — primary_key should be (bus_seg, rpt_dt, sub_product_group)
```

**`<table>.approval.json` is created by `lumi approve <table>`** parsing the
appended block, validating against `PlanApproval`, and writing structured form
for the execute phase.

### Why ParallelAgent for enrichment

Anthropic's parallelization pattern: "use when subtasks are
independent and latency matters." Each table's enrichment is
independent — table A's descriptions don't depend on table B's
output (cross-table awareness comes from the ecosystem_brief,
which is computed BEFORE enrichment starts). Running 8 tables
in parallel vs. sequential: ~30s vs. ~4 minutes.

ADK's ParallelAgent handles the semaphore internally. Configure
`max_concurrent` in lumi_config.yaml to respect SafeChain rate
limits (default: 5).

### Why LoopAgent for evaluation

Anthropic's evaluator-optimizer pattern: "one LLM produces,
another critiques, the first revises." Here:

- The coverage checker (deterministic, no LLM) identifies gaps
- The gap fixer (LlmAgent) re-enriches only the tables with gaps,
  injecting the specific gap info ("Q9 needs measure X, currently
  missing from your output")
- The loop re-checks coverage after each fix
- Exit when coverage ≥ 90% or after 3 iterations
- Regression guard: any previously-covered query that becomes
  uncovered is a blocking error

This is not an autonomous agent loop — the exit condition is
deterministic and the max iterations are hard-capped. It's a
controlled retry mechanism.

---

## 3. Guardrails

Every stage has entry and exit gates. The pipeline halts on
blocking failures and warns on quality degradation.

### Stages 1-2 (Parse + Discover) guardrails — deterministic

| Check | Type | Action on fail |
|-------|------|----------------|
| Every SQL parses in sqlglot | Blocking | Quarantine failed SQL, continue with rest |
| Every table has MDM response | Warning | Log coverage gap, proceed with empty descriptions |
| CTE count in fingerprint matches WITH clause count | Blocking | Parser bug — halt and fix |
| Join DAG is acyclic | Blocking | Malformed SQL — quarantine |
| All tables from inside CTEs included in discovery | Blocking | Parser bug — halt |

### Stages 3-4 (Stage + Plan) guardrails — see check_staging, check_planning, check_approvals

Implemented in `lumi/guardrails.py`. Highlights:
- Every parsed table has a TablePriority; ranks unique; dependency DAG acyclic
- Every priority has an EnrichmentPlan; plan has at least one dim or measure
- Every plan has a PlanApproval (human or auto); rejected plans carry feedback

### Stage 5 (Enrich) guardrails — per LlmAgent call

| Check | Type | Action on fail |
|-------|------|----------------|
| LLM response parses to EnrichedOutput schema | Retry (2x) | On 3rd fail: log, skip table |
| Generated view_lkml parses with lkml library | Blocking | Schema parse error — retry with error message injected |
| Every description is 15-200 characters | Warning | Log but don't block |
| Derived table exists for each CTE in TableContext | Warning | Will be caught by evaluator |
| Measures include aggregation type in description | Warning | Quality degradation — log |
| Explore joins are in topological order | Blocking | Re-enrich with explicit ordering instruction |

### Stage 6 guardrails (evaluator loop)

| Check | Type | Action on fail |
|-------|------|----------------|
| Coverage ≥ 90% after loop | Warning | Proceed but flag in coverage report |
| No regressions from previous iteration | Blocking | Revert to previous iteration's output |
| All structural filters baked into derived_table or sql_always_where | Blocking | Re-enrich affected explore |
| Join paths connect all tables for each multi-table query | Blocking | Re-enrich with join DAG injected |

### Stage 7 guardrails (deterministic)

| Check | Type | Action on fail |
|-------|------|----------------|
| All output LookML files lint clean | Blocking | Abort publish |
| No view changes > 50% diff from baseline | Warning | Flag for human review |
| metric_catalog.json is valid JSON | Blocking | Serialization bug |
| Git push succeeds | Blocking | Auth/network error — retry |

---

## 4. The learning system

LUMI maintains a persistent learning document that grows with
every pipeline run. This implements Anthropic's "structured
note-taking" pattern from their context engineering guide:

> "Write durable state to disk. The filesystem is a first-class
> memory primitive."

### learnings.md

```markdown
# LUMI Learnings

## Table patterns discovered
- cornerstone_metrics: simple flat table, ~40 columns, high MDM coverage
- risk_pers_acct_history: complex, 150+ columns, used in CTEs with
  structural filters for source system scoping
- drm_product_member: lookup table, always pre-filtered by geo + portfolio

## Prompt patterns that work
- Injecting "the user will never see this SQL" improves descriptions
  (model stops describing the SQL and starts describing the business)
- Providing 2-3 example descriptions in the prompt dramatically
  improves consistency across tables
- Explicit "DO NOT list valid values in descriptions — put them in
  filter_catalog" reduces description bloat

## Prompt patterns that fail
- Asking for descriptions AND filter catalog AND derived tables in
  one shot for tables with >100 columns — quality drops on the last
  items. Split into two calls for large tables.
- Telling the model "be concise" without giving a character range —
  it interprets "concise" as 5 words

## Common LookML mistakes the model makes
- Forgets value_format_name on currency measures
- Puts joins in alphabetical order instead of topological order
- Uses type: string for columns that should be type: number
- Forgets dimension_group for DATE columns, uses plain dimension

## MDM gaps
- drm_product_hier: 0% MDM coverage, all columns need LLM inference
- acquisitions: cm11 column has no description anywhere

## Evaluator insights
- Q8 consistently fails on filter coverage: too many IS NULL / IS NOT
  NULL patterns that the enricher doesn't surface as filter catalog entries
- Q10 join ordering fixed after adding explicit position field to prompt
```

### How it grows

After each pipeline run, the learning system:

1. **Compares** this run's coverage with the previous run's
2. **Identifies** what changed (new gaps, fixed gaps, regressions)
3. **Captures** any new patterns:
   - Tables with unusual characteristics (high column count, low MDM)
   - Prompt adjustments that improved/degraded quality
   - Evaluator findings (common gap categories)
   - LookML validation errors and their fixes
4. **Appends** to learnings.md with a timestamped entry

This is deterministic — no LLM needed. It's diff analysis +
append. The learnings.md is checked into git, so it persists
across sessions and machines.

### How it feeds back

On the NEXT pipeline run, learnings.md is loaded as part of
the enrichment prompt context. The LlmAgent sees:

```
## Known issues with this table (from previous runs):
- This table has 0% MDM coverage on product hierarchy columns
- Previous run missed value_format on currency measures
- Join to drm_product_hier must come AFTER drm_product_member
```

This is the "write" strategy from LangChain's context engineering
taxonomy: persist state outside the context window so future
runs benefit from past experience.

---

## 5. Context engineering strategy

Per Anthropic and LangChain, context engineering has four
operations: **write, select, compress, isolate.** Here's how
LUMI uses each:

### Write
- **learnings.md**: persists cross-run knowledge
- **ecosystem_brief**: persists cross-table relationships
- **metric_catalog.json**: persists canonical metric definitions
- **coverage_report.json**: persists evaluation state

### Select
- Each enrichment call gets ONLY the columns referenced in SQL
  from MDM, not all 200 columns. If a table has 200 columns but
  the input SQLs only touch 40 of them, the LLM sees 40.
- The ecosystem_brief is a SELECTED summary — 200 tokens per
  table, not the full 20K context of every table.
- Learnings relevant to THIS table are selected from learnings.md,
  not the entire file.

### Compress
- SQL fingerprints are a COMPRESSED representation of raw SQL.
  The LLM doesn't need to re-parse the SQL — the fingerprint
  tells it exactly what aggregations, CTEs, joins, and filters
  exist. This saves ~60% of the tokens vs. passing raw SQL.
- The ecosystem brief is compressed table metadata.

### Isolate
- Each table's enrichment runs in its OWN context window
  (separate LlmAgent in ParallelAgent). Table A's enrichment
  cannot pollute table B's context.
- The evaluator loop has its own context — it doesn't carry
  the full enrichment context, just the coverage gaps.

### Token budget per enrichment call

```
Component                  Tokens    Source
─────────────────────────────────────────────
System prompt + examples    ~3,000   Static (prompt file)
Table's MDM columns         ~8,000   Selected from MDM (only referenced columns)
SQL fingerprints (all)      ~5,000   Compressed from raw SQL
Existing LookML view        ~4,000   From baseline
Ecosystem brief             ~1,500   Compressed cross-table summary
Table-specific learnings      ~500   Selected from learnings.md
CTE patterns + CASE WHENs  ~3,000   From fingerprint (variable)
─────────────────────────────────────────────
TOTAL                      ~25,000   2.5% of Gemini's 1M context
```

Headroom: 975,000 tokens unused. This means:
- Tables with 300+ columns still fit comfortably
- We can add more examples to the prompt without worry
- We can include more learnings context as it grows
- The "split into 2 calls" threshold is very far away

---

## 6. Evolution path

The simplified pipeline is designed with explicit extension
points. Each evolution step is triggered by a MEASURABLE problem,
not a theoretical concern.

```
CURRENT (v1)                  TRIGGER                 EVOLUTION (v2+)
───────────────────────────────────────────────────────────────────────

One Gemini call per table     Quality drops on         Split into 2 calls:
                              tables with >150         descriptions + complex
                              referenced columns       patterns (CTEs, CASE WHENs)

                              ↓ if quality still drops

                              Split into 4-step        The full prompt chain from
                              prompt chain             the 12-session plan (5a-5d)

───────────────────────────────────────────────────────────────────────

Metric catalog as side        Need cross-table         Metric catalog becomes
output of enrichment          dedup that can't fit     a SEPARATE stage with its
                              in ecosystem_brief       own LlmAgent (Stage 4 from
                              (~50+ tables)            the 12-session plan)

───────────────────────────────────────────────────────────────────────

Sequential table processing   Processing 100+ tables   Already using ParallelAgent.
(ParallelAgent handles this)  takes > 30 minutes       Increase semaphore, add
                                                       batching, add queue.

───────────────────────────────────────────────────────────────────────

learnings.md as flat file     Learning corpus          Move to structured DB
                              exceeds 50K tokens       (SQLite or pgvector) with
                              (impractical to inject   semantic search over
                              into every prompt)        learnings. Select only
                                                       relevant entries per table.

───────────────────────────────────────────────────────────────────────

Manual prompt tuning for      Tuning cycle takes       Automated prompt mutation:
coverage gaps                 > 2 hours per run        evaluator generates specific
                                                       prompt edits, applies them,
                                                       re-runs, measures. Full
                                                       evaluator-optimizer autonomy.

───────────────────────────────────────────────────────────────────────

One enrichment prompt         Different BUs need       Routing pattern: classifier
                              fundamentally different  picks which prompt variant
                              enrichment strategies    to use per table based on
                                                       BU + table characteristics.
```

### The key principle

> "Start with the simplest thing that works. Add complexity
> only when a measurable problem justifies it." — Anthropic,
> OpenAI, Google ADK, and LangChain all agree on this.

Every row in the evolution table has a TRIGGER column. Don't
build the v2+ column until the trigger fires. The current
design handles 137 queries across ~50 tables comfortably.

---

## 7. The prompt

The enrichment prompt (`lumi/prompts/enrich_view.md`) is the
single most important artifact in the system. Everything else
is plumbing. The prompt determines output quality.

### Structure

```markdown
# Enrich LookML View

You are a LookML expert building a semantic layer for an
enterprise data platform. Your output will be consumed by
an NL-to-SQL system called Radix that needs to unambiguously
select the right dimension, measure, and filter for any
business question.

## Your task

Generate enriched LookML for the table: {table_name}

## Context

### This table
{table_mdm_description}

### Columns referenced in queries (with MDM descriptions)
{selected_mdm_columns}

### SQL patterns found (from sqlglot analysis)
{fingerprint_summary}

### How this table relates to others
{ecosystem_brief}

### Learnings from previous runs
{table_specific_learnings}

### Existing LookML (current state to improve upon)
{existing_view_lkml}

## Output requirements

### View LookML
For each column:
- **Dimensions:** type, label, description (15-30 words,
  front-load business meaning), group_label if related to
  other fields. DO NOT list valid values in descriptions.
- **Dimension groups:** for ALL date/timestamp columns, use
  dimension_group with type: time and appropriate timeframes.
- **Measures:** for each aggregation found in SQL patterns,
  create a measure with type (sum/count/average), label,
  description (include aggregation type: "Sum of..."),
  and value_format_name for currency/percentage fields.

### Derived tables (from CTE patterns)
For each CTE in the SQL patterns:
- Create a separate view with derived_table
- Bake structural filters INTO the derived_table SQL
- These filters are NOT user-selectable — they define scope
- Name the view descriptively (not the CTE alias)

### Derived dimensions (from CASE WHEN patterns)
For each CASE WHEN:
- Create a dimension with the full CASE WHEN SQL
- Description MUST explain what each mapped value means
- Use MDM to interpret the source column's code values
- Include tags with relevant business terms

### Explore definition
- Use derived_table views as from_view when CTEs are present
- Joins MUST be in the order specified by position field
  (later joins may reference columns from earlier joins)
- sql_always_where for structural filters NOT in derived_tables
- Description: list 3-5 example questions this explore answers

### Filter catalog (JSON array)
For each filterable dimension:
- field_key, canonical_name, synonyms (business terms users
  would actually say), known_values (from SQL patterns),
  operators (=, IN, >, IS NULL, etc.), is_structural

### Metric catalog (JSON array)
For each measure:
- canonical_name, description, aggregation, source_column,
  synonyms (all observed names across queries)

## Examples

### Good description (measure)
"Sum of total billed business volume in USD across all
charge card transactions for the reporting period."

### Bad description (measure)
"billed_business" (just the column name)
"The sum of the billed_business column" (describes SQL, not business)

### Good description (derived dimension)
"FICO credit score grouped into standard risk bands.
Exceptional (800+), Very Good (740-799), Good (670-739),
Fair (580-669), Poor (below 580)."

### Bad description (derived dimension)
"A case when expression on fico_score" (describes SQL, not business)
```

### Why the prompt is structured this way

1. **Role + downstream consumer** at the top: Anthropic's
   prompting guide says "tell the model who it is and who
   consumes its output." The model writes better descriptions
   when it knows Radix needs disambiguation.

2. **Context sections with clear headers**: Google ADK's
   "context like source code" principle. Each section has one
   job. The model doesn't have to hunt for information.

3. **Negative examples**: OpenAI's agent guide recommends
   "show what NOT to do." The bad description examples prevent
   the most common failure modes.

4. **Output structure matching the schema**: The output
   requirements mirror the EnrichedOutput Pydantic schema
   exactly. Gemini's output_schema enforces this, but the
   prompt description helps the model understand WHY each
   field exists.

---

## 8. NL question generation (golden dataset for Radix)

Every SQL answers a business question. Usually several. LUMI
generates these questions as a parallel output — zero extra
Gemini calls, just an additional field in EnrichedOutput.

### Why this matters

Radix needs (question → explore + fields + filters) pairs to:
1. Train its routing classifier (which explore handles this question?)
2. Populate k-shot retrieval (find similar past questions)
3. Evaluate accuracy (does Radix produce the same resolution?)

137 queries × ~8 variants = **~1,000 golden pairs** generated
as a free byproduct of enrichment.

### How it works

The enrichment prompt includes:

```markdown
### NL question variants
For each input SQL pattern, generate 5-10 natural language
questions that a business user would ask that this SQL answers.

Vary along these axes:
- Specificity: "total revenue" vs "Q3 2024 consumer revenue"
- Phrasing: "what is" vs "show me" vs "how much"
- Aggregation awareness: "total" vs "average" vs "by month"
- Filter awareness: "for consumer" vs "excluding GNS"
- Comparison: "vs last year" vs "trend over time"

For each question, also output the resolution:
- explore: which explore answers this
- fields: which measures + dimensions
- filters: which filters with what values

Example:
{
  "question": "What was the total billed business for consumer
               segment in January 2025?",
  "explore": "cornerstone_metrics",
  "measures": ["total_billed_business"],
  "dimensions": ["report_date_month"],
  "filters": {"bus_seg": "Consumer", "report_date_month": "2025-01"}
}
```

### Output schema addition

```python
class NLQuestionVariant(BaseModel):
    question: str                  # natural language question
    explore: str                   # which explore answers it
    measures: list[str]            # LookML measure names
    dimensions: list[str]          # LookML dimension names
    filters: dict[str, str]        # {field: value}
    difficulty: str                # easy / medium / hard
    source_sql_id: str             # which input SQL this derives from

class EnrichedOutput(BaseModel):
    # ... existing fields ...
    nl_questions: list[NLQuestionVariant]  # NEW — golden dataset
```

### Output file

```
output/golden_questions.json — all NL variants across all tables
```

This file is what Radix loads at deploy time for k-shot retrieval
and evaluation. It's version-controlled alongside the LookML.

---

## 9. LookML completeness for Looker MCP round-trip

This is the requirement that separates a decorative semantic layer
from a functional one. Looker MCP needs specific LookML key-value
pairs to generate valid SQL. Missing any of them means Radix can
ask the right question but Looker can't execute it.

### The round-trip

```
User question → Radix → selects explore + fields + filters
  → Looker MCP → reads LookML → generates SQL → BigQuery → results
```

If the LookML is incomplete, the chain breaks at "reads LookML."
Looker MCP doesn't guess — it fails.

### Required LookML attributes by level

#### View level (REQUIRED for Looker to find the table)

```lookml
view: cornerstone_metrics {
  sql_table_name: `project.dataset.cornerstone_metrics` ;;
  # Without this, Looker doesn't know which BQ table to query.
  # The agent MUST emit this using the table name from the
  # SQL fingerprint. Format: `project.dataset.table`
}
```

#### Dimension level

| Attribute | Required? | Why Looker MCP needs it |
|-----------|-----------|------------------------|
| `sql` | REQUIRED | The column expression. Without it, Looker can't build SELECT/WHERE |
| `type` | REQUIRED | string/number/date/yesno. Wrong type = wrong filter UI + wrong SQL casting |
| `primary_key: yes` | REQUIRED on PK | Without it, Looker can't detect join fanout. Aggregations may double-count |
| `label` | Recommended | What Radix sees when matching fields to questions |
| `description` | Recommended | What embeddings are built from. Quality here = Radix accuracy |
| `group_label` | Recommended | Groups related dims in Looker Explore UI |
| `tags` | Recommended | Synonyms that help Radix find this field |
| `hidden: yes` | Conditional | For internal/technical fields Radix shouldn't select |

#### Dimension group level (dates)

| Attribute | Required? | Why |
|-----------|-----------|-----|
| `type: time` | REQUIRED | Tells Looker to generate time-grain SQL (DATE_TRUNC, EXTRACT) |
| `timeframes` | REQUIRED | Which grains are available: [date, week, month, quarter, year] |
| `sql` | REQUIRED | The date column |
| `datatype` | REQUIRED | "date" or "datetime" — affects SQL generation |
| `convert_tz: no` | Recommended for BQ | BigQuery dates don't need timezone conversion |

Without `dimension_group`, a date column becomes a plain string
dimension. Users can't filter by month/year. Looker MCP generates
`WHERE rpt_dt = '2025-01'` instead of `WHERE DATE_TRUNC(rpt_dt, MONTH) = '2025-01-01'`.

#### Measure level

| Attribute | Required? | Why |
|-----------|-----------|-----|
| `type` | REQUIRED | sum/count/count_distinct/average/min/max. Determines aggregation SQL |
| `sql` | REQUIRED | The column to aggregate. Can be an expression |
| `value_format_name` | Recommended | usd/decimal_0/percent_2 — affects display but also tells Radix the semantic type |
| `filters` | Conditional | For filtered measures like "revenue where source = cornerstone" |
| `drill_fields` | Optional | What to show when user clicks a number |

#### Explore level

| Attribute | Required? | Why |
|-----------|-----------|-----|
| `from` | Conditional | If base view is a derived_table view, this is REQUIRED |
| `sql_table_name` on the from view | REQUIRED | Looker needs to know the physical table |
| `join: view_name` | REQUIRED for joins | Each joined view |
| `sql_on` | REQUIRED per join | The join condition — Looker generates this verbatim into SQL |
| `relationship` | REQUIRED per join | many_to_one/one_to_many/many_to_many. Wrong = wrong aggregation |
| `type` | REQUIRED per join | left_outer/inner/full_outer. Affects SQL JOIN type |
| `sql_always_where` | Conditional | For structural filters. Looker appends this to every query's WHERE |
| `label` | Recommended | What users/Radix see |
| `description` | Recommended | What this explore is for. Radix uses this for explore selection |

### The primary_key problem

This is the most commonly missed attribute and the most dangerous.
Without `primary_key: yes` on the correct dimension:

- Looker can't detect symmetric aggregation problems
- JOIN + SUM produces double-counted results
- The error is SILENT — no warning, just wrong numbers

The agent must identify primary keys from:
1. The SQL fingerprint (columns in JOIN ON conditions)
2. MDM metadata (if PK info is available)
3. Heuristic: columns named `*_id`, `*_key`, `*_cd` that appear
   in JOIN conditions are likely PKs

### The relationship problem

For each join, the agent must determine:
- `many_to_one`: the joined table has one row per join key (lookup)
- `one_to_many`: the joined table has multiple rows per key
- `many_to_many`: both sides have multiple rows (rare, dangerous)

Wrong relationship = wrong aggregation. If `risk_acct JOIN product_member`
is marked `one_to_many` instead of `many_to_one`, every SUM on
product_member measures gets multiplied by the number of risk accounts.

The agent infers relationship from:
1. The CTE/subquery structure (a CTE that groups by key = one row per key = many_to_one)
2. Column naming patterns (*_id as PK = many_to_one from the referencing side)
3. MDM cardinality info if available

### Append/modify strategy for existing LookML

LUMI does NOT regenerate views from scratch. It MERGES into
existing LookML:

```python
def merge_enrichment(
    existing_lkml: str,          # current .view.lkml from git
    enriched: EnrichedOutput     # what the LLM produced
) -> str:
    """
    Strategy:
    1. Parse existing LookML with lkml library
    2. For each field in enriched output:
       a. If field EXISTS in current view:
          - UPDATE description (if enriched is longer/better)
          - UPDATE label (if enriched provides one and current doesn't)
          - ADD tags (merge, don't replace)
          - ADD group_label (if missing)
          - KEEP sql unchanged (existing is authoritative)
          - KEEP type unchanged (existing is authoritative)
          - KEEP any manually-added attributes (drill_fields, etc.)
       b. If field is NEW (from SQL analysis, not in current view):
          - APPEND as new dimension/measure/dimension_group
          - Include all attributes from enriched output
    3. For sql_table_name:
       - ADD if missing from existing view
       - KEEP if already present
    4. For primary_key:
       - ADD if missing and we can identify it
       - NEVER remove an existing primary_key
    5. Return merged LookML string
    """
```

This means:
- Manually added fields by a Looker developer are PRESERVED
- Existing sql expressions are NOT overwritten (they may have
  been manually tuned)
- New measures discovered from SQL analysis are APPENDED
- Descriptions are UPGRADED (longer/richer replaces shorter)
- The merge is ADDITIVE, never destructive

For explores (model.lkml):
- If an explore already exists for this table combination, UPDATE
  its description and ensure joins are complete
- If it's a new table combination from the SQL analysis, APPEND
  a new explore
- NEVER delete existing explores

### What the enrichment prompt must enforce

Add to `lumi/prompts/enrich_view.md`:

```markdown
## LookML completeness requirements (for Looker MCP)

Your output LookML MUST include these attributes. Without them,
Looker cannot generate valid SQL and the semantic layer is broken.

EVERY view MUST have:
- sql_table_name: `project.dataset.table_name` ;;

EVERY dimension MUST have:
- sql: ${TABLE}.column_name ;;
- type: (infer from MDM data type or column naming)

EVERY date column MUST be a dimension_group, not a dimension:
- type: time
- timeframes: [date, week, month, quarter, year]
- datatype: date
- convert_tz: no

EVERY measure MUST have:
- type: sum/count/count_distinct/average (match the SQL aggregation)
- sql: ${TABLE}.column_name ;;
- value_format_name: (usd for currency, decimal_0 for counts)

EVERY view MUST identify its primary key:
- Look for columns in JOIN ON conditions
- Look for columns ending in _id, _key, _cd
- Mark exactly ONE dimension with: primary_key: yes

EVERY explore join MUST have:
- type: left_outer (or inner if the SQL uses INNER JOIN)
- relationship: many_to_one / one_to_many (infer from context)
- sql_on: using ${view.field} syntax, NOT raw column names

If you are UNSURE about relationship cardinality, default to
many_to_one and add a tag: ["relationship_needs_review"]
```

---

## 10. Data flow for 137 queries

```
137 SQL strings
       │
       ▼
  sqlglot parse (deterministic, ~2 seconds)
       │
       ▼
  ~30-50 unique tables identified
  SQL fingerprints grouped by table
       │
       ▼
  MDM fetch for each table (API calls, ~30 seconds)
  Baseline views loaded from git
       │
       ▼
  Ecosystem brief generated (deterministic, <1 second)
       │
       ▼
  ParallelAgent: 30-50 Gemini calls (semaphore=5, ~3-5 minutes)
  Each call: ~25K tokens in, ~10K tokens out
       │
       ▼
  Coverage checker: 137 queries × generated LookML (~5 seconds)
       │
       ▼
  If coverage < 90%: gap fixer re-enriches 3-8 tables (~1-2 min)
  Loop up to 3 times
       │
       ▼
  Learnings appended to learnings.md
       │
       ▼
  Output written: views/ + models/ + catalogs + golden_questions + coverage
       │
       ▼
  Git push to branch, PR created
       │
       ▼
  Total: ~8-12 minutes, ~1.5M tokens, ~$3-5 per run
```

---

## 11. File inventory

```
lumi/
├── pipeline.py            # SequentialAgent wiring
├── sql_to_context.py      # Stage 1: parse + discover
├── enrich.py              # Stage 2: LlmAgent per table
├── validate.py            # Stage 3: coverage + gap fixing
├── publish.py             # Stage 4: learn + git push
├── prompts/
│   └── enrich_view.md     # THE prompt
├── schemas.py             # All Pydantic models
└── config.py              # SafeChain, semaphore, thresholds
tests/
├── test_sql_to_context.py
├── test_enrich.py
├── test_validate.py
├── test_pipeline.py       # End-to-end
└── conftest.py
scripts/
├── probe_mdm.py           # Helper: real MDM output
└── run_pipeline.py        # Entry point
data/
├── gold_queries/          # Input SQL files
├── baseline_views/        # Current LookML from git
└── learnings.md           # Persistent cross-run knowledge
output/
├── views/*.view.lkml
├── models/*.model.lkml
├── metric_catalog.json
├── filter_catalog.json
├── golden_questions.json      # NL question variants for Radix
├── coverage_report.json
└── pipeline_stats.json
.claude/
├── commands/
│   ├── session.md         # /session N
│   ├── coverage.md        # /coverage
│   ├── fix-gaps.md        # /fix-gaps
│   ├── probe.md           # /probe [api]
│   ├── fingerprint.md     # /fingerprint [sql]
│   └── retro.md           # /retro
└── skills/
    └── lookml-patterns/
        └── SKILL.md
CLAUDE.md
DESIGN.md                  # This document
```

8 production Python files. 4 test files. 1 prompt. 1 learnings doc.

---

## 12. Build sessions (4 sessions, ~13 hours)

### Session 1: Parse + Discover (2-3h)
Build `sql_to_context.py`. Tests for all 10 query patterns.
Success: `prepare_enrichment_context` handles CTEs, CASE WHENs,
3-hop joins, date functions, complex filters. Ecosystem brief
generated. All deterministic, no LLM.

### Session 2: Enrich (3-4h)
Build `enrich.py` + `enrich_view.md` prompt. Tests for LookML
validity, derived tables, derived dimensions, join ordering.
Success: one Gemini call per table produces valid, rich LookML.
Most time spent on prompt engineering.

### Session 3: Validate + Integrate (2-3h)
Build `validate.py` + `pipeline.py`. Wire everything together.
Evaluator loop with coverage checker + gap fixer. Learning
system writes to learnings.md.
Success: full pipeline runs on all 10 queries, coverage report
generated, learnings captured.

### Session 4: Tune + Ship (2-3h)
Fix gaps from coverage report. Tune prompt. Push to GitHub.
Success: ≥90% coverage on 10 queries, all LookML valid,
metric catalog and filter catalog produced, PR created.

Then: feed 137 queries. Measure. Fix. Iterate.

---

## 13. Design decisions log

| Decision | Rationale | Research grounding |
|----------|-----------|-------------------|
| Workflow, not agent | Steps are deterministic and known | Anthropic: "workflows beat agents when the task is predictable" |
| One Gemini call per table | 25K tokens per call = 2.5% of context. No quality degradation at this size | Anthropic: "start simple, add complexity when needed" |
| ParallelAgent for enrichment | Tables are independent. Latency: 3 min parallel vs 20 min sequential | Anthropic: parallelization pattern |
| LoopAgent for evaluation | Evaluator-optimizer with deterministic exit condition | Anthropic: evaluator-optimizer pattern |
| Ecosystem brief | Cross-table awareness without blowing up context | LangChain: "compress" + "select" strategies |
| learnings.md | Persistent note-taking across runs | Anthropic: "structured note-taking" in context engineering |
| Metric catalog as side output (not separate stage) | Fits in single enrichment call for <50 tables | "Add complexity when measurable problem justifies it" |
| sqlglot for all SQL parsing | Deterministic, fast, no LLM tokens | Anthropic: "never send an LLM to do a linter's job" |
| lkml for LookML validation | Deterministic syntax checking | Same principle |
| Git branch + PR (not direct to main) | Human review before Looker deploy | OpenAI: "human-in-the-loop is a design primitive" |
| Probe scripts for API discovery | Claude Code sees real data through human executor | Boris Cherny: "natural language, not rigid commands" |
| NL question generation as side output | Zero extra LLM cost, produces ~1000 golden pairs for Radix eval | "Maximize value per token spent" |
| LookML completeness checklist in prompt | Looker MCP needs specific attributes to generate SQL — missing any breaks the round-trip | Looker documentation: required vs optional attributes |
| Merge into existing LookML (not regenerate) | Preserves manual customizations, additive-only, no destructive overwrites | Production safety: "first, do no harm" |
| Primary key + relationship inference | Without these, Looker silently produces wrong aggregations — the most dangerous failure mode | Looker symmetric aggregation documentation |
| Ecosystem brief for cross-table awareness | Each enrichment call understands the full table landscape for better descriptions and join context | LangChain: "compress" strategy + Anthropic: context engineering |
