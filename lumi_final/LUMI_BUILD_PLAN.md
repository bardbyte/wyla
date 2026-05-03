# LUMI — Build Plan (v2: 7-stage flow, ADK-native)

## The shape

Seven stages, two phases, one human-approval gate between them.

```
PHASE 1 (cheap, automated)              PHASE 2 (expensive, automated)
──────────────────────────              ──────────────────────────────
Parse → Discover → Stage → Plan         Enrich → Validate → Publish
                              │                            │
                              ▼                            ▼
                    HUMAN APPROVAL GATE          GitHub PR for Looker review
                    (review_queue/*.plan.md)
```

Six sessions. Each produces working, tested output. ADK constructs throughout
(`SequentialAgent`, `ParallelAgent`, `LoopAgent`, `LlmAgent`).

---

## Session 1 — Parse + Discover (~3 hours)

### Build
```
lumi/sql_to_context.py
tests/test_sql_to_context.py
scripts/probe_mdm.py        ← Saheb runs on work laptop
scripts/fetch_baselines.py  ← Saheb runs on work laptop
```

### What `sql_to_context.py` does
```python
def parse_sqls(sqls: list[str]) -> list[SQLFingerprint]:
    """Stage 1: pure sqlglot. No I/O. Returns one fingerprint per SQL."""

def discover_tables(
    fingerprints: list[SQLFingerprint],
    mdm_client,
    baseline_views_dir: str,
) -> dict[str, TableContext]:
    """Stage 2: group fps by table, fetch MDM + baseline per table."""

def prepare_enrichment_context(
    sqls: list[str],
    mdm_client,
    baseline_views_dir: str,
) -> dict[str, TableContext]:
    """One-call wrapper used by tests + the pipeline."""
```

### Tests (TDD — write first, watch them fail)
- `test_q1_simple_aggregation` — Q1 yields 1 table, 1 measure (SUM), 3 filters
- `test_q9_cte_with_structural_filters` — Q9's CTEs surface `acct_srce_sys_cd='TRIUMPH'` as is_structural=True
- `test_q10_three_hop_join` — joins_involving_this preserves position order
- `test_date_function_extraction` — Q2's `EXTRACT(YEAR FROM rpt_dt)` lands in date_functions
- `test_case_when_extraction` — Q9's two CASE WHENs land in case_whens with mapped_values
- `test_multi_query_dedup` — Q1+Q4 produce ONE TableContext for cornerstone_metrics with merged aggregations
- `test_all_10_queries` — all 10 SQLs through the pipeline yield a non-error context dict

### Success criteria
- All Session-1 tests pass
- `guardrails.check_parse_and_discover(...)` returns status=pass on the 10 SQLs

---

## Session 2 — Stage + Plan (~3 hours)

### Build
```
lumi/stage.py
lumi/plan.py
lumi/prompts/plan_table.md
tests/test_stage.py
tests/test_plan.py
```

### Stage step (no LLM)
```python
def prioritize_tables(
    table_contexts: dict[str, TableContext],
) -> list[TablePriority]:
    """Topological order of dependencies, then query_count desc, then complexity."""
```

### Plan step (LlmAgent per table, ParallelAgent)
```python
def build_planning_agent(table_name: str, model) -> LlmAgent:
    return LlmAgent(
        name=f"plan_{table_name}",
        model=model,
        instruction=PLAN_PROMPT,
        output_schema=EnrichmentPlan,
    )

def write_plan_markdown(plan: EnrichmentPlan, queue_dir: str) -> Path:
    """Render plan to review_queue/<table>.plan.md."""
```

### Tests
- `test_prioritization_topological` — Q9's CTE-source ranks before consumers
- `test_prioritization_query_count_tiebreak` — most-touched table wins ties
- `test_plan_markdown_renders` — generated .plan.md has all required sections
- `test_plan_passes_guardrails` — `check_planning(plans, contexts)` passes
- `test_low_risk_auto_approval` — simple plans with no risks can be auto-approved

### Success criteria
- All Session-2 tests pass
- Plan markdowns are human-readable AND schema-recoverable

---

## Session 3 — Approval Gate + Enrich (~4 hours)

### Build
```
lumi/approval.py
lumi/enrich.py
lumi/prompts/enrich_view.md  (already exists, may need tuning)
tests/test_approval.py
tests/test_enrich.py
```

### Approval step
```python
def collect_approvals(queue_dir: str) -> list[PlanApproval]:
    """Parse review_queue/<table>.plan.md appended approval lines."""
```

### Enrich step (one LlmAgent per APPROVED table)
```python
def enrich_table(
    table_context: TableContext,
    approved_plan: EnrichmentPlan,
    model,
) -> EnrichedOutput:
    """Single Gemini 3.1 Pro call (Vertex direct — no SafeChain).
    Prompt receives:
      - TableContext + ecosystem_brief
      - The APPROVED plan (acts as scope contract)
      - Sections 1-4 + compressed 6,7 of .claude/skills/lookml/SKILL.md
    """
```

### Tests
- `test_approval_parses_approved` — recognizes `✅ APPROVED` and variants
- `test_approval_parses_rejected_with_feedback` — captures feedback text
- `test_approval_blocks_on_pending` — `check_approvals()` fails when undecided
- `test_enrich_simple_table` — Q1 → valid LookML with primary_key, dim_groups
- `test_enrich_cte_produces_derived_table` — Q9 → derived_table view with structural filters baked
- `test_enrich_skill_injected` — prompt includes patterns from SKILL.md sections 1-4

### Success criteria
- `check_approvals()` blocks until decisions made
- Enriched LookML parses with `lkml` + passes `check_enrichment()` per table

---

## Session 4 — Validate (~3 hours)

### Build
```
lumi/validate.py
tests/test_validate.py
```

### Validate step (LoopAgent, max 3 iterations)
```python
def build_evaluator_loop(model) -> LoopAgent:
    return LoopAgent(
        name="evaluator",
        sub_agents=[
            CoverageCheckerAgent(),  # deterministic
            SqlReconstructorAgent(), # deterministic — wraps guardrails.check_sql_reconstruction
            GapFixerAgent(model),    # LlmAgent — only fires if gaps>0
        ],
        max_iterations=3,
    )
```

### Tests
- `test_coverage_full_when_all_fields_present` — synthetic enriched view → 100%
- `test_coverage_identifies_missing_measure` — drop a measure → flagged
- `test_loop_exits_on_pass` — first-iter coverage=100% → no gap fixer fires
- `test_loop_max_iterations` — perma-failing fixture → exits at 3
- `test_no_regression_blocking` — previously-covered query becomes uncovered → blocking

### Success criteria
- Loop exits cleanly on coverage ≥ 90%
- Regression detection works

---

## Session 5 — Publish + Pipeline Wiring (~3 hours)

### Build
```
lumi/publish.py
lumi/pipeline.py     (rewrite as full ADK SequentialAgent composition)
tests/test_publish.py
tests/test_pipeline_e2e.py
apps/lumi/{__init__.py, agent.py}    # adk web entry
```

### Publish step
```python
def merge_to_baseline(...): ...     # additive merge (rules in DESIGN.md §9)
def update_learnings(...): ...      # append run-specific findings
def publish_to_github(...): ...     # branch + commit + push + gh pr create
```

### Pipeline (ADK composition)
```python
def build_lumi_plan_phase(model) -> SequentialAgent:
    return SequentialAgent(
        name="lumi_plan",
        sub_agents=[parse, discover, stage, plan_parallel],
    )

def build_lumi_execute_phase(model, approvals) -> SequentialAgent:
    return SequentialAgent(
        name="lumi_execute",
        sub_agents=[enrich_parallel(approvals), evaluate_loop, publish],
    )
```

### Tests
- `test_merge_preserves_existing_sql` — manually-tuned sql expression survives
- `test_merge_appends_new_measure` — measure not in baseline gets appended
- `test_pipeline_e2e_phase_1` — Parse→Discover→Stage→Plan completes for all 10 SQLs
- `test_pipeline_e2e_phase_2` — given pre-approved plans, Enrich→Validate→Publish completes
- `test_apps_lumi_imports` — `apps/lumi/agent.py` has module-level `root_agent`

### Success criteria
- Both pipeline phases run end-to-end on the 10 fixtures
- `adk web apps/` shows `lumi` agent and runs without errors

---

## Session 6 — Tune + Ship (~3 hours)

### Build
None. Fix gaps from `output/coverage_report.json`.

### Loop
1. Read coverage report
2. Categorize gaps:
   - `prompt_fix` → tune `lumi/prompts/{plan_table,enrich_view}.md`
   - `mdm_fix` → add fallback in `discover_tables()`
   - `parser_fix` → fix sqlglot extraction
3. Re-run pipeline (`python -m lumi plan && python -m lumi execute`)
4. Repeat until coverage ≥ 90%
5. Open PR

### Success criteria
- Coverage ≥ 90% on the 10 SQLs (9/10)
- All output LookML lints with `lkml`
- PR created against `amex-eng/prj-d-lumi-gpt-semantic`

---

## Total

```
SESSION  FOCUS                                TIME    CUMULATIVE
─────────────────────────────────────────────────────────────────
  1      Parse + Discover                     ~3h     3h
  2      Stage + Plan                         ~3h     6h
  3      Approval gate + Enrich               ~4h     10h
  4      Validate                             ~3h     13h
  5      Publish + pipeline + adk web         ~3h     16h
  6      Tune + ship (coverage ≥ 90%)         ~3h     19h
─────────────────────────────────────────────────────────────────
TOTAL: 6 sessions, ~19 hours, 3-4 days of focused work
```

---

## Final file inventory

```
lumi/
├── sql_to_context.py     # S1: parse + discover
├── stage.py              # S2: prioritize
├── plan.py               # S2: cheap LLM call
├── approval.py           # S3: parse review_queue
├── enrich.py             # S3: full LLM call (Gemini 3.1 Pro via Vertex direct)
├── validate.py           # S4: LoopAgent evaluator
├── publish.py            # S5: merge + git
├── pipeline.py           # S5: SequentialAgent wiring
├── schemas.py            # all Pydantic models (DONE)
├── guardrails.py         # all stage gates (DONE)
├── config.py             # paths + thresholds
├── __main__.py           # CLI: plan / status / execute / approve
└── prompts/
    ├── plan_table.md     # S2 prompt
    └── enrich_view.md    # S3 prompt (DONE — may need tuning)

apps/lumi/                # adk web entry point (S5)
├── __init__.py           # truststore inject
└── agent.py              # re-exports root_agent

scripts/                  # written before sessions that need them
├── probe_mdm.py          # S1: see real MDM shape
└── fetch_baselines.py    # S1: pull .view.lkml from GHE

tests/
├── conftest.py                  # exists
├── fixtures/sample_sqls.py      # exists, 10 queries
├── test_sql_to_context.py       # S1
├── test_stage.py                # S2
├── test_plan.py                 # S2
├── test_approval.py             # S3
├── test_enrich.py               # S3
├── test_validate.py             # S4
├── test_publish.py              # S5
└── test_pipeline_e2e.py         # S5

data/
├── gold_queries/         # populated from sample_sqls.py constants
├── baseline_views/       # populated by fetch_baselines.py probe
└── learnings.md

review_queue/             # generated by Phase 1
└── <table>.plan.md       # human reviews and approves
```
