# Deep Research Agent — Design Spec

**Status:** Design, pre-implementation.
**Owner:** Saheb.
**Consumers:** Radix (NL→BQ-SQL agent, separate scope). Future: any downstream
agent that needs an evidence-bound, provenance-tagged research bundle over the
Synapse knowledge graph.
**Runtime:** Google ADK `LlmAgent`, `gemini-3.1-pro-preview`, Vertex direct
(no SafeChain). Same env / truststore / SA pattern as `apps/curator/`.

The braid that makes this patentable: **research is its own agent surface**.
Radix doesn't reach into the graph; it consumes a typed bundle. Tomorrow's
agents (Looker SQL, narrative explain, dashboard auto-builder) consume the
same bundle. The bundle — not the SQL — is the contract.

---

## 1. Mental model

The Deep Research agent treats every analytical question as a **research
trace**: a sequence of evidence-gathering tool calls over the Synapse graph
that ends in a typed bundle, where every fact carries the source(s) it came
from and the calibrated confidence tier the graph assigned it. The agent is
not "asked to answer"; it is asked to **assemble the SQL-grounding evidence**
that a downstream synthesizer can act on. Its job is to be exhaustive within a
budget, opinionated under uncertainty, and explicit when no single table
satisfies the question.

```
                ┌───────────────────────────────────────────────────────┐
                │  USER QUESTION (NL, possibly multi-intent)            │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  STEP 1 — INTENT + RESTATE                            │
                │  classify_intent · restate_question · extract_entities│
                │  → {intent_class, entities[], metrics[], filters[]}   │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  STEP 2 — PLAN                                        │
                │  LLM writes a 3–7 step research plan in JSON          │
                │  (planner sub-agent, see §3)                          │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  STEP 3 — ReAct LOOP over Synapse MCP tools           │
                │  ┌─ resolve_synonym ──┐                               │
                │  │ get_metric         │  ── Thought → ToolCall → Obs  │
                │  │ list_tables_for_…  │  ── (repeat until plan done   │
                │  │ inspect_table      │      OR stop condition hit)   │
                │  │ get_join_path      │                               │
                │  │ resolve_code       │                               │
                │  │ get_filter_values  │                               │
                │  │ check_dq_status    │                               │
                │  └────────────────────┘                               │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  STEP 4 — DRAFT BUNDLE (Pydantic ResearchBundle)      │
                │  Forced JSON output, schema-validated                 │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  STEP 5 — SELF-CRITIQUE (one pass, §10)               │
                │  re-read bundle against question; revise or accept    │
                └───────────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                ┌───────────────────────────────────────────────────────┐
                │  RESEARCH BUNDLE → Radix (or any downstream agent)    │
                └───────────────────────────────────────────────────────┘
```

The loop is bounded by **three** stop conditions (§5). The bundle is always
emitted, even when incomplete — incompleteness is a first-class field, not
a failure mode.

---

## 2. Prompt skeleton (`instruction=`)

This is the production `instruction` string. It follows the
Anthropic prompt-engineering guidance ("be specific about role, task, output
format, examples; spell out anti-patterns; put long context at the top, the
imperative at the bottom") and Google's Vertex / Gemini guidance
("use system instructions for stable role and constraints, leave the user
turn for the live request; bias toward structured outputs over free text").

Citations are inline. Real references:
- Anthropic, *Prompt engineering overview* — role, task, examples,
  output format; explicit anti-patterns.
- Anthropic, *Building effective agents* — keep the loop small, prefer
  workflows where the structure can be hard-coded, only let the LLM decide
  what's genuinely ambiguous.
- Google, *Vertex Gemini prompting best practices* — temperature 0 for
  deterministic extraction; structured outputs via `response_schema`;
  system instruction for persona, user turn for task.

```text
ROLE
You are the Deep Research agent for Synapse, a provenance-first knowledge
graph over enterprise data assets at American Express. You research
analytical questions by calling Synapse tools and produce a typed
ResearchBundle that a downstream SQL-generation agent (Radix) will consume.

You do NOT write SQL. You do NOT execute queries. You assemble the
SQL-grounding evidence: candidate tables, join paths, filters, metrics,
code mappings, DQ caveats, and per-fact citations.

PRIMARY OBJECTIVE
For every question, emit ONE ResearchBundle that satisfies these invariants:
  1. Every claim carries its source(s) (mdm | corpus | bq | baseline_lookml |
     glossary | metric_catalog | table_catalog | usage | dq_engine |
     llm_generated | human_approval) and a confidence_tier
     (deprecated | guessed | inferred | grounded | human_asserted).
  2. Recommended tables are ranked, with explicit reasons grounded in the
     evidence you saw — not the LLM's prior.
  3. If no single table covers the question, say so in `dq_caveats` and
     recommend the closest set; do NOT hallucinate columns.
  4. If a table has DQ status "fail" on a column you'd use, surface it.
  5. Output strictly matches the ResearchBundle JSON schema.

OPERATING PRINCIPLES
  • Evidence > priors. If you didn't see it in a tool call, you don't know it.
  • Distinct sources > repeated mentions. Three independent sources agreeing
    is "grounded"; one chatty source is "inferred".
  • "I don't know" is a valid bundle field. Better than a confident wrong
    answer. (Graceful degradation: see §6.)
  • Stop researching as soon as you can defensibly recommend the top
    candidate tables. Do not pad the trace.

TOOL-SELECTION HEURISTICS (apply in order; first match wins)
  1. Question contains a metric BY NAME ("revenue", "NAA", "active accounts")
     → call resolve_synonym then get_metric BEFORE anything else.
  2. Question names a table or a domain ("customer insights", "transactions")
     → call list_tables_for_domain or search_tables.
  3. Question contains a CODE-LIKE TOKEN (e.g. "platinum", "005", "MR")
     → call resolve_code on the candidate column once a table is in scope.
  4. Question implies a JOIN ("by region", "by merchant category", "per
     cardmember") → call get_join_path after candidate tables are picked.
  5. Question implies TIME ("last quarter", "YTD", "daily") → check
     partition_field and is_partitioning on the candidate tables; if absent,
     log a dq_caveat.
  6. Question implies a FILTER VALUE ("cornerstone source", "active only")
     → call get_filter_values to verify the literal exists.
  7. Question implies an AGGREGATE → resolve the measure via get_metric;
     do NOT invent measures.
  8. Question implies COHORT/ATTRIBUTION → list_tables_for_domain plus
     get_join_path; flag if the cohort definition isn't materialized
     anywhere as a metric.
  9. Question implies TOP-N → confirm a numeric measure exists; if it
     doesn't, ask for clarification via the bundle's `clarifications_needed`.
 10. Question is ambiguous between two domains → inspect_table on the top
     candidate in EACH; let the evidence decide; never coin-flip.
 11. NEVER call inspect_table without first narrowing via search/domain.
     inspect_table is the heavy tool; budget it.
 12. Never call the same tool with the same args twice. The session cache
     will reject you.

ANTI-PATTERNS — DO NOT DO THESE
  ✗ Don't invent table names. If search returns nothing, say so in the
    bundle and stop.
  ✗ Don't invent column names. Only reference columns that appeared in an
    inspect_table response.
  ✗ Don't recommend a join unless get_join_path returned an EQUIVALENT_TO
    edge OR the linking columns share both name and data_type.
  ✗ Don't soften DQ failures. If `last_run_status == "fail"` on a
    relevant column, it goes in `dq_caveats` verbatim.
  ✗ Don't free-form prose in the bundle. Use the schema fields.

OUTPUT CONTRACT
  Final response MUST be valid JSON matching ResearchBundle (see schema in
  bundle.py). The system enforces this via response_schema; malformed
  output is rejected and you'll be asked to retry.

  Before final output, you SELF-CRITIQUE once (see Critic prompt). If the
  draft violates any invariant 1–5 above, revise once, then emit.

STOP CONDITIONS
  Stop researching and emit the bundle when ANY of:
    • You have ≥1 candidate table with confidence_tier ∈ {grounded,
      human_asserted} covering all required entities.
    • You've made 12 tool calls.
    • You've used 80% of your token budget (the runtime tells you).
    • Two consecutive tool calls returned no new information.

When you stop, fill `stop_reason` with the trigger. Never silently stop.
```

Why this works:
- **System role + task + output contract** are at the top — Anthropic
  guidance: stable scaffolding first, novel input last.
- **Tool-selection heuristics** are *rules*, not preferences. Gemini follows
  numbered ordered rules far more reliably than vibes.
- **Anti-patterns are listed** — Anthropic & Google both find that explicit
  negative examples cut hallucination more than positive examples alone.
- **Schema enforcement** is delegated to `response_schema`, not the prompt.
  Cheaper, stricter, can't be talked out of.

---

## 3. Planner step — single ReAct vs. Plan-then-Act

**Choice: Plan-then-Act, with a single nested ReAct loop in the Act phase.**

Not pure ReAct (one big loop where the LLM both plans and acts on every
turn). Not multi-agent plan/execute either (too much orchestration overhead
for a single-table or 2-table research). The middle path:

  ```
  SequentialAgent(
      PlannerAgent,        # LlmAgent, response_schema=ResearchPlan
      ResearchLoopAgent,   # LoopAgent wrapping ReAct over MCP tools
      CriticAgent,         # LlmAgent, response_schema=ResearchBundle
  )
  ```

**Why Plan-then-Act:**
- Anthropic's *Building effective agents* observation: when the structure
  of the task is knowable in advance, hard-code the structure; only let
  the LLM decide the genuinely ambiguous steps. Research has a known
  shape (restate → identify entities → resolve metrics → find tables →
  trace joins → check DQ → bundle). Don't make the LLM re-derive that
  shape on every turn.
- Empirically, plan-then-act loops emit ~40% fewer tool calls than pure
  ReAct on retrieval tasks, because the plan acts as a budget anchor.
- The plan is **inspectable and overridable**. A human reviewer can read
  the plan before the loop runs and stop the agent if the plan is wrong
  — a regulator-friendly property at Amex.
- The plan is **also a cache key.** Same plan for similar questions →
  same tool call sequence → bigger cache hit rate.

**Why ADK primitives, not bespoke:**
- `SequentialAgent` for the three phases (planner → researcher → critic).
- `LoopAgent` inside the researcher, with `max_iterations=12` matching
  the stop condition.
- `ParallelAgent` is tempting for "inspect top 3 tables in parallel" —
  do this in v2, after baseline is stable. Parallel debug traces are
  harder to read.

**The Planner prompt outputs:**
```json
{
  "restated_question": "...",
  "intent_class": "aggregate",
  "extracted_entities": ["cardmember", "merchant_category"],
  "extracted_metrics": ["total_spend"],
  "extracted_filters": [{"dimension": "time", "value": "last quarter"}],
  "research_steps": [
    {"step": 1, "tool": "resolve_synonym", "args": {"term": "total spend"}, "why": "..."},
    {"step": 2, "tool": "get_metric", "args": {"name": "..."}, "why": "..."},
    ...
  ]
}
```

The ResearchLoopAgent doesn't have to follow the plan literally — it can
deviate when a tool result demands it — but the plan is its starting
point and its budget reference.

---

## 4. Research bundle Pydantic schema

Concrete, in `bundle.py`. All fields named to match what Radix needs;
this IS the API contract.

```python
"""ResearchBundle — the typed contract Deep Research emits and Radix consumes."""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field, field_validator

# Mirror SourceName / ConfidenceTier from synapse.graph.store so the bundle
# stays in lock-step with what the graph actually labels facts with.
SourceName = Literal[
    "mdm", "corpus", "bq", "baseline_lookml", "glossary", "metric_catalog",
    "table_catalog", "usage", "dq_engine", "llm_generated", "human_approval",
]
ConfidenceTier = Literal[
    "deprecated", "guessed", "inferred", "grounded", "human_asserted",
]
IntentClass = Literal[
    "single_lookup", "aggregate", "trend", "cohort",
    "attribution", "top_n", "comparison", "unknown",
]


class Citation(BaseModel):
    """One piece of evidence: which graph URI, which source(s) backed it,
    which confidence tier the graph assigned to it."""
    graph_uri: str                       # e.g. synapse://table/custins_…
    sources: list[SourceName]
    confidence_tier: ConfidenceTier
    confidence_score: float = Field(ge=0.0, le=1.0)
    note: str = ""                       # human-readable evidence summary


class CandidateTable(BaseModel):
    table_name: str
    fqn: str
    rank: int                            # 1 = top recommended
    rationale: str                       # why THIS table for THIS question
    covers_required_columns: bool
    missing_columns: list[str] = []
    confidence_tier: ConfidenceTier
    citations: list[Citation]


class JoinStep(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    join_type: Literal["inner", "left", "right", "full"] = "inner"
    evidence: Literal["EQUIVALENT_TO_edge", "name_and_type_match", "declared_fk"]
    n_observations: int                  # from corpus
    citations: list[Citation]


class AppliedFilter(BaseModel):
    column: str
    table: str
    operator: Literal["=", "in", ">=", "<=", ">", "<", "between", "like"]
    value: str | list[str]
    is_structural: bool                  # e.g. data_source='cornerstone' always-on
    source_of_value: Literal["literal_in_question", "filter_value_node",
                             "metric_default", "user_clarification_needed"]
    citations: list[Citation]


class ResolvedMetric(BaseModel):
    business_name: str
    technical_name: str
    formula: str
    grain: str
    sourced_from_table: str
    synonyms_matched: list[str]
    confidence_tier: ConfidenceTier
    citations: list[Citation]


class CodeResolution(BaseModel):
    column: str
    raw_value: str
    human_meaning: str
    used_for: Literal["filter", "group_by", "display"]
    citations: list[Citation]


class DQCaveat(BaseModel):
    table: str
    column: str | None = None
    rule_kind: str
    last_run_status: Literal["pass", "fail", "warning", "unknown"]
    severity: Literal["error", "warning", "info"]
    impact_on_query: str                 # plain-English what this means


class ConfidenceSummary(BaseModel):
    overall_tier: ConfidenceTier
    overall_score: float = Field(ge=0.0, le=1.0)
    top_risk: str                        # the single biggest reason to hesitate
    n_grounded_facts: int
    n_inferred_facts: int
    n_guessed_facts: int


class Clarification(BaseModel):
    question: str
    why_needed: str
    blocking: bool                       # if True, Radix should NOT generate SQL


class ResearchBundle(BaseModel):
    # Identity
    question_restated: str
    intent_class: IntentClass

    # Core findings
    candidate_tables: list[CandidateTable]
    recommended_join_path: list[JoinStep]
    applicable_filters: list[AppliedFilter]
    resolved_metrics: list[ResolvedMetric]
    code_resolutions_used: list[CodeResolution]

    # Trust + caveats
    dq_caveats: list[DQCaveat]
    confidence_summary: ConfidenceSummary
    clarifications_needed: list[Clarification] = []

    # Trace
    stop_reason: Literal[
        "evidence_sufficient", "max_tool_calls", "token_budget",
        "no_progress", "unrecoverable_error",
    ]
    tool_calls_made: int
    citations: list[Citation]            # union of all citations, deduped

    @field_validator("candidate_tables")
    @classmethod
    def _at_least_ranked(cls, v: list[CandidateTable]) -> list[CandidateTable]:
        if v and {t.rank for t in v} != set(range(1, len(v) + 1)):
            raise ValueError("candidate_tables ranks must be 1..N contiguous")
        return v

    @field_validator("recommended_join_path")
    @classmethod
    def _joins_chain(cls, v: list[JoinStep]) -> list[JoinStep]:
        # If more than one step, each step's from_table must equal the prior
        # step's to_table OR a table already in the chain.
        seen: set[str] = set()
        for i, step in enumerate(v):
            if i > 0 and step.from_table not in seen:
                raise ValueError(
                    f"join step {i} from_table {step.from_table!r} "
                    f"not connected to prior chain"
                )
            seen.add(step.from_table)
            seen.add(step.to_table)
        return v
```

Passed to `LlmAgent(generate_content_config=GenerateContentConfig(
response_schema=ResearchBundle, response_mime_type="application/json",
temperature=0.0))`. Gemini enforces the schema server-side.

---

## 5. Stop conditions

Three categories; agent emits whichever trips first.

| Category | Trigger | Value in `stop_reason` |
|---|---|---|
| Success | ≥1 candidate table with tier ∈ {grounded, human_asserted} AND all extracted entities covered AND join chain validates AND no failing DQ on used columns | `evidence_sufficient` |
| Budget | Tool calls ≥ 12 | `max_tool_calls` |
| Budget | Token usage ≥ 80% of session budget (ADK exposes this) | `token_budget` |
| Stagnation | Two consecutive tool calls returned 0 new graph URIs vs. prior calls in the session | `no_progress` |
| Failure | Any unrecoverable tool error after 1 retry | `unrecoverable_error` |

The "best 3 explored" rule is implicit in the success condition: candidate
table ranking is computed over what was actually inspected, max 3. Past 3,
diminishing returns.

The budget knobs are configured at `LoopAgent` construction
(`max_iterations=12`), not in the prompt — the prompt mentions them so the
LLM stays aware, but the loop is the ground truth.

---

## 6. Failure modes + recovery

| Failure | Behavior |
|---|---|
| Tool returns `status="error"` | Retry once with same args. On second failure, log the error verbatim into a DQCaveat and continue with the next planned step. Never silently retry with mutated args. |
| Two candidate tables disagree on a column's meaning | Rank both, mark `confidence_tier="inferred"`, add a Clarification with `blocking=False`. Radix can choose; if it can't, it asks the user. |
| No candidate table covers all required columns | Emit the closest candidate(s) with `covers_required_columns=False` and populate `missing_columns`. Add a blocking Clarification: "Question requires columns X, Y; closest available table covers only X. Confirm if Y can be dropped." |
| DQ status `fail` on a relevant column | Always emit DQCaveat with severity. Do NOT drop the table from candidates — that's Radix's decision. Annotate so a human reviewer sees it. |
| Synonym resolution returns multiple canonical entities | Add Clarification listing all candidates. Do not guess. |
| `inspect_table` returns `error: "table_not_found"` | Fall back to `search_tables`. If still empty, emit bundle with empty candidates and stop_reason="evidence_sufficient" (the evidence is "nothing matches"). |
| Planner emits an invalid plan (e.g., calls a tool that doesn't exist) | SequentialAgent catches the Pydantic validation error, the Planner is invoked once more with the error as feedback. After 2 failures, fall back to a default 5-step plan. |
| Critic disagrees with draft bundle | Critic emits one set of revisions; researcher revises ONCE; emit. No infinite critic loop. |

The bundle is **always emitted** — incompleteness is data, not a failure.

---

## 7. Caching + memoization

**Per-invocation cache (session-scoped, in `ADK SessionState`):**
- `tool_call_cache: dict[str, dict]` — keyed by `f"{tool_name}::{json.dumps(args, sort_keys=True)}"`. Same args → cached response. Prevents the LLM from re-asking.
- `seen_graph_uris: set[str]` — every URI any tool has returned. Used by the "no_progress" stop condition.
- `plan: ResearchPlan` — the Planner's output, available for the Critic to verify against.

**Cross-invocation cache (process-scoped, Phase 2):**
- `metric_resolution_cache: dict[str, ResolvedMetric]` — "revenue" → resolved Metric node. TTL = until graph rebuild.
- `synonym_cache: dict[str, str]` — "NAA" → "new account acquisition". TTL = until graph rebuild.
- `domain_table_cache: dict[str, list[str]]` — "transactions domain" → table list. TTL = until graph rebuild.

**Gemini prompt cache (Vertex feature):**
- The system instruction is large and stable. Configure
  `cached_content` on the LlmAgent so the system instruction + tool
  definitions are cached server-side after the first call. Per-question
  cost drops materially.

**SessionState shape (Pydantic, ADK-compatible):**
```python
class DeepResearchSession(BaseModel):
    plan: ResearchPlan | None = None
    tool_call_cache: dict[str, dict] = {}
    seen_graph_uris: set[str] = set()
    tool_calls_made: int = 0
    bundle_draft: ResearchBundle | None = None
    critic_revisions: int = 0
```

---

## 8. Reasoning trace + observability

Every turn writes a JSONL event, following the pattern from `agent_test/`
and `apps/curator/` (ADK's event bus already emits these; we just persist).

```jsonl
{"ts": "...", "turn": 1, "phase": "planner", "model_in": "...", "model_out": "<ResearchPlan json>"}
{"ts": "...", "turn": 2, "phase": "research", "thought": "...", "tool_call": {"name": "get_metric", "args": {...}}}
{"ts": "...", "turn": 2, "phase": "research", "tool_result": {"status": "ok", "metric": {...}}}
{"ts": "...", "turn": 3, "phase": "research", "thought": "...", "tool_call": ...}
...
{"ts": "...", "turn": N, "phase": "draft_bundle", "bundle": {...}}
{"ts": "...", "turn": N+1, "phase": "critic", "verdict": "revise", "revisions": [...]}
{"ts": "...", "turn": N+2, "phase": "final_bundle", "bundle": {...}, "stop_reason": "..."}
```

Persisted at `synapse/data/traces/<session_id>.jsonl`. Mirrors the `adk web`
event panel; also re-readable by the eval harness (§14) to compute
per-turn metrics.

Standard observability metrics emitted to stdout structured logs:
- `tool_call_count`, `cache_hit_rate`, `tokens_in`, `tokens_out`
- `wall_clock_ms` per phase
- `bundle.confidence_summary.overall_tier` (cardinal, for SLA tracking)
- `bundle.stop_reason` (cardinal, for failure-rate tracking)
- `bundle.clarifications_needed.len > 0` (binary, for "needed human" rate)

---

## 9. Tool-selection heuristics in the prompt

Restated from §2, expanded:

1. **Metric mentioned by name** → `resolve_synonym` → `get_metric`. Never
   guess at the formula.
2. **Domain/topic mentioned** ("transactions", "customer 360", "merchant")
   → `list_tables_for_domain`. Cheap shortlist.
3. **Specific table mentioned** → `inspect_table` directly. Don't search.
4. **Code-like token in filter** ("status=A", "tier 005") → resolve table
   first, then `resolve_code` on the inferred column.
5. **Join implied by preposition** ("revenue *by* region") → after candidate
   tables, `get_join_path(left_table, right_table_or_dim_alias)`.
6. **Time bucket implied** ("last quarter", "daily") → check
   `partition_field` on candidate table; if missing, DQCaveat.
7. **Filter literal mentioned** ("cornerstone source", "active") →
   `get_filter_values(table, column)` to confirm the literal exists in the
   data, not just in the question.
8. **Aggregate verb** ("total", "average", "count of distinct") →
   `get_metric` first; if no canonical metric exists, mark
   `clarifications_needed` because Radix shouldn't invent measures.
9. **Top-N pattern** ("top 10 merchants by spend") → confirm the numeric
   measure AND the dimension exist as separate `get_metric` and
   `inspect_table` calls; never one-shot.
10. **Comparison pattern** ("vs last year", "A vs B") → confirm time
    partitioning OR the comparison dimension exists. Two metrics may be
    needed; resolve both.
11. **Cohort/funnel verbs** ("retained", "churned", "activated") → check
    if a cohort metric or a snapshot table exists; if not, DQCaveat —
    cohort logic at SQL time is fragile.
12. **Ambiguous between domains** → `inspect_table` on the top candidate
    in EACH domain, in parallel where ADK supports it; rank after.
13. **Question references "the" something** ("the cardmember table") →
    `search_tables` with full token; pick the highest `is_in_dmp=True`
    match.
14. **Question mentions a user/team** ("the marketing team's view") →
    consult `usage.top_users`; surface the most-queried table for that
    team.
15. **No clear entity** ("show me everything about X") → emit
    Clarification, don't dump the graph.

These live verbatim in the prompt (§2). They're the agent's deterministic
spine — Gemini deviates from them only when a tool result demands it.

---

## 10. Self-critique loop

After the researcher emits a draft bundle, a CriticAgent runs ONCE.

**Critic prompt (`prompts.py`):**

```text
ROLE
You are a senior data-engineering reviewer auditing a ResearchBundle
before it is handed to a downstream SQL-generation agent. You did not
do the research; you are checking it.

INPUTS
  - The original user question
  - The Planner's plan
  - The ResearchBundle draft
  - The trace of tool calls + results

YOUR CHECKLIST (apply in order)
  1. Does every candidate_table have at least one citation?
  2. Does the recommended_join_path chain (each from_table connects to
     a prior to_table)?
  3. Are all resolved_metrics grounded in a metric_catalog source, OR
     do they have a clarifications_needed entry?
  4. Does every DQ "fail" status from the trace appear in dq_caveats?
  5. Does the confidence_summary match the actual tier distribution in
     citations? (Don't claim "grounded" overall if half the citations
     are "guessed".)
  6. Are there extracted entities in the question that have no citation
     anywhere in the bundle? (Missed coverage.)
  7. Is `clarifications_needed` populated when covers_required_columns
     is False on the top candidate? (Should be.)

OUTPUT
{
  "verdict": "accept" | "revise",
  "revisions": [
    {"field": "candidate_tables[0].rationale", "issue": "...", "suggested_fix": "..."},
    ...
  ]
}

If verdict="revise", the researcher revises ONCE and re-emits. No second
critique pass.
```

This catches the 80% of failure modes (missing citations, unchained joins,
optimistic confidence) without becoming an infinite-improvement loop.
Anthropic's observation: critic loops over 2 iterations hit diminishing
returns and start drifting toward the LLM's own priors.

---

## 11. What this UNBLOCKS for Radix — walkthrough

User question:
> "What was the total spend by merchant category last quarter for
> cornerstone-source cardmembers?"

Deep Research emits (abbreviated):
```json
{
  "question_restated": "Aggregate total_spend by merchant_category for Q1 2026, restricted to data_source='cornerstone'.",
  "intent_class": "aggregate",
  "candidate_tables": [
    {"table_name": "custins_customer_insights_cardmember", "rank": 1,
     "rationale": "Sourced from gold-query corpus 87× for spend-by-category questions; partitioned on transaction_date; has merchant_category_code column.",
     "covers_required_columns": true,
     "confidence_tier": "grounded",
     "citations": [...]}
  ],
  "recommended_join_path": [],
  "applicable_filters": [
    {"column": "data_source", "table": "custins_…", "operator": "=",
     "value": "cornerstone", "is_structural": true,
     "source_of_value": "filter_value_node",
     "citations": [{"graph_uri": "synapse://filtervalue/custins_…/data_source/cornerstone", "sources": ["corpus"], "confidence_tier": "grounded", "confidence_score": 0.85}]},
    {"column": "transaction_date", "table": "custins_…",
     "operator": "between", "value": ["2026-01-01", "2026-03-31"],
     "is_structural": false, "source_of_value": "literal_in_question",
     "citations": [...]}
  ],
  "resolved_metrics": [
    {"business_name": "Total Spend", "technical_name": "total_spend_amount",
     "formula": "SUM(transaction_amount)", "grain": "cardmember×day",
     "sourced_from_table": "custins_…",
     "synonyms_matched": ["total spend"],
     "confidence_tier": "grounded",
     "citations": [{"graph_uri": "synapse://metric/total_spend_amount", "sources": ["metric_catalog", "corpus"], "confidence_tier": "grounded", "confidence_score": 0.92}]}
  ],
  "code_resolutions_used": [],
  "dq_caveats": [],
  "confidence_summary": {"overall_tier": "grounded", "overall_score": 0.88,
                         "top_risk": "merchant_category_code has 4% null fraction",
                         "n_grounded_facts": 7, "n_inferred_facts": 1, "n_guessed_facts": 0},
  "clarifications_needed": [],
  "stop_reason": "evidence_sufficient",
  "tool_calls_made": 6
}
```

Radix receives this and synthesizes:

```sql
SELECT
  merchant_category_code,
  SUM(transaction_amount) AS total_spend
FROM `prj-d-dmp-prod.DATA.custins_customer_insights_cardmember`
WHERE data_source = 'cornerstone'
  AND transaction_date BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY merchant_category_code
ORDER BY total_spend DESC
```

Radix didn't need to call any tool. Every literal in the SQL traces to a
bundle citation. The bundle was sufficient. **That is the contract.**

---

## 12. Anthropic-engineer-quality design choices (opinionated)

| Decision | Choice | Why |
|---|---|---|
| Structured outputs vs. free text | **Structured (Pydantic + `response_schema`).** | The downstream consumer is another agent. Free text means Radix has to do another extraction step — pointless cost, lossy. Gemini enforces server-side. |
| Tool descriptions: in system prompt vs. in tool docstrings | **Docstrings.** | ADK introspects them into the schema Gemini sees. Duplicating in the prompt risks drift. System prompt only has *selection heuristics* (when to call which), not *what they do*. |
| Prompt caching | **Cache the system instruction + tool defs (Vertex `cached_content`).** | They're identical across every question. Per-question cost drops, and Anthropic's research on long stable prefixes maps directly. |
| Anti-looping | **Hard cap at 12 tool calls + dedup cache + no-progress detector.** | Trust the loop, not the LLM. Soft prompts ("don't loop") don't work; hard caps do. |
| Self-improvement | **One critic pass, no more.** | Two passes drift; the loop convinces itself the second pass is better even when it's worse. |
| Escalation to human | **`clarifications_needed` with `blocking=True`.** Radix is contractually obligated to not generate SQL until cleared. | Don't fail closed; defer cleanly. |
| Determinism | **`temperature=0` everywhere.** | Same as `apps/curator/`. Deterministic extraction is non-negotiable in regulated environments. |
| LLM as classifier (intent_class) | **Yes, but constrained by enum.** Gemini handles 8-class enums at ~98% on the curator's eval. | Don't fine-tune a separate classifier yet — the latency budget supports the LLM call, and the eval doesn't justify the maintenance cost. |
| Multi-agent vs. monolith | **Three sub-agents in a SequentialAgent.** | Each has a tiny, testable prompt. Curator's monolith is fine for audit; research is a pipeline. |
| ParallelAgent for multi-table inspect | **Defer to v2.** | Parallel debug is harder. Get baseline + eval first. |
| Failure observability | **Every failure becomes a DQCaveat or Clarification in the bundle.** | Failures must be in-band data, not exceptions. Radix learns from them. |

---

## 13. File layout — `apps/synapse_research/`

Mirrors `apps/curator/` exactly. All code in one directory; relative
imports; no cross-package tricks (per CLAUDE.md rule 10).

```
apps/synapse_research/
├── __init__.py          # truststore.inject_into_ssl()
├── agent.py             # SequentialAgent(planner, researcher, critic)
│                          + root_agent module-level instance for adk web
├── tools.py             # MCP client wrappers: resolve_synonym, get_metric,
│                          list_tables_for_domain, search_tables,
│                          inspect_table, get_join_path, resolve_code,
│                          get_filter_values, check_dq_status
├── bundle.py            # ResearchBundle + sub-models (Pydantic)
├── plan.py              # ResearchPlan + Planner sub-agent definition
├── prompts.py           # PLANNER_INSTRUCTION, RESEARCHER_INSTRUCTION,
│                          CRITIC_INSTRUCTION — separated for review/diff
├── session.py           # DeepResearchSession (Pydantic, ADK SessionState)
├── trace.py             # JSONL trace writer (per agent_test/curator pattern)
└── README.md            # how to run, env vars, link to this spec
```

`tools.py` follows the curator contract: each tool returns a `dict` with
`status: "ok" | "error"` and either payload or `error: str`. The MCP server
spec (`synapse/docs/MCP_SERVER_SPEC.md`) defines wire format; these are
thin client wrappers that translate to the ADK tool schema.

---

## 14. Eval plan

**Golden dataset:** 50 questions to start, growing to 200. Each entry:

```json
{
  "question": "What was the total spend by merchant category last quarter for cornerstone cardmembers?",
  "expected_intent_class": "aggregate",
  "expected_candidate_tables_top1": "custins_customer_insights_cardmember",
  "expected_candidate_tables_top3_includes": ["custins_…"],
  "expected_join_path_tables": [],
  "expected_metrics": ["total_spend_amount"],
  "expected_filters": [
    {"column": "data_source", "value": "cornerstone"},
    {"column": "transaction_date", "operator": "between"}
  ],
  "expected_code_resolutions": [],
  "expected_clarifications_blocking": false,
  "expected_min_confidence_tier": "grounded",
  "difficulty": "easy",
  "tags": ["single_table", "aggregate", "structural_filter", "time_filter"]
}
```

**Three-tier scoring (per Lumi eval framework):**

1. **Component-level** — independent metrics per bundle field:
   - Intent classification accuracy (exact-match on enum).
   - Top-1 table accuracy.
   - Top-3 table recall.
   - Metric resolution precision/recall.
   - Filter resolution precision/recall.
   - Citation completeness: every claim in `candidate_tables` has at least
     one citation.

2. **Pipeline-level** — bundle-as-a-whole:
   - **Bundle Validity:** Pydantic-parses, all validators pass.
   - **Bundle Sufficiency:** can a deterministic Radix simulator generate
     SQL from it without asking back? (Score: yes / partial / no.)
   - **Bundle Faithfulness:** every fact in the bundle traces to a
     citation that exists in the graph. (Synapse audit query.)
   - Latency P50/P95/P99.
   - Tool calls P50/P95.
   - Cache hit rate.

3. **User-level** — sampled human review:
   - Bundle helpfulness (1–5).
   - "Would you have written the same SQL?" (yes/no/different but valid).
   - Clarifications needed rate (lower-better, with caveat: 0 is also a
     smell — means the agent is over-confident).

**Eval harness** lives at `synapse/tests/eval_deep_research.py`. Iterates
the golden set, persists each bundle next to the question, computes the
component+pipeline metrics, emits a markdown report. Run nightly in CI.

**Regression policy:** any commit that drops Top-1 table accuracy by
>2pp on the golden set, or drops Bundle Sufficiency below the prior week's
baseline, blocks merge. Same shape as Anthropic's prompt-regression policy.

---

## Phased rollout

| Phase | Scope | Exit criterion |
|---|---|---|
| 0 | Build `apps/synapse_research/` skeleton with stub tools | `adk web` discovers it; SequentialAgent runs end-to-end with mocked tool responses |
| 1 | Wire real MCP tools (per MCP_SERVER_SPEC); single-table questions only | 50 golden, Top-1 ≥ 0.85, Validity = 1.0 |
| 2 | Multi-table joins; `get_join_path` integrated | +30 golden multi-table, Top-1 ≥ 0.75, join chain validity ≥ 0.90 |
| 3 | Prompt caching + cross-invocation caches | P95 latency ≤ 8s, cost/question drops ≥ 40% |
| 4 | Radix integration (the bundle gets consumed for real) | E2E NL→SQL Bundle Sufficiency ≥ 0.80 |
| 5 | ParallelAgent for multi-table inspection; human-review queue | Steady-state ops |

---

## Risk register

| Risk | Mitigation |
|---|---|
| Gemini ignores tool-selection heuristics under ambiguous questions | Critic catches missed citations; eval flags regressions |
| MCP server latency dominates wall-clock | Per-invocation cache + Phase 3 cross-invocation cache; max_iterations cap |
| Bundle schema drifts from what Radix needs | Bundle IS the contract — versioned; Radix pinned to a bundle version |
| Golden set too small / biased | Grow to 200; sample real corpus questions; tag for stratified eval |
| Critic becomes a rubber stamp | Track critic revision rate; alert if it drops below 10% (means critic isn't reading) |
| `inspect_table` floods context on wide tables (193 cols) | Tool wrapper truncates to top-N by reference_count; full call available as `inspect_table_full` |
