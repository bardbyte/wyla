# LUMI — Final Design Document + Claude Code Implementation Guide

## The Goal (In One Sentence)

Every view file that our 137 gold queries touch becomes enriched so completely — via gold query vocabulary, MDM business metadata, and LLM reasoning — that the downstream NL2SQL agent can answer ANY question about those views with near-certainty, not just the 137 we trained on.

---

## The Three Sources of Truth (And What Each Provides)

```
SOURCE 1: Gold Queries (137 user_prompt + expected_sql pairs)
├── PROVIDES: User vocabulary ("NAA", "AIF", "spend", "fico band")
├── PROVIDES: Which measures must exist (SUM, COUNT DISTINCT, AVG patterns)
├── PROVIDES: Which join paths are real (multi-table queries)
├── PROVIDES: Common filter values (data_source='cornerstone' in 90%+ queries)
├── PROVIDES: Frequency data (which fields are asked about most)
└── SERVES AS: Validation test suite (can enriched LookML resolve all 137?)

SOURCE 2: MDM API (business entity metadata for every view)
├── PROVIDES: Canonical business names for EVERY column (not just gold query ones)
├── PROVIDES: Business definitions (what does acct_bal_age_mth01_cd actually mean?)
├── PROVIDES: Synonyms and aliases from the business glossary
├── PROVIDES: Allowable values for categorical fields
├── PROVIDES: Entity relationships (which views relate to which)
└── FILLS THE GAP: Columns the 137 queries don't touch

SOURCE 3: LookML View Files (from GitHub, the existing code)
├── PROVIDES: Current field definitions (what already exists)
├── PROVIDES: SQL expressions (the actual column mappings)
├── PROVIDES: Existing type information
├── PROVIDES: Any existing descriptions/labels (preserve, don't overwrite)
└── SERVES AS: The substrate we're enriching
```

### The Analogy

Think of it like training a new analyst:
- The **gold queries** are the analyst shadowing senior colleagues for a month, learning how they phrase questions and what SQL they write. This gives them the practical vocabulary.
- The **MDM** is the company's data dictionary and onboarding docs. This gives them the formal definitions for every field, including the hundreds they haven't encountered yet.
- The **view files** are the codebase they'll be working in. They need to understand what already exists before adding anything.

A great analyst uses ALL THREE. So does LUMI.

---

## Input Configuration

```yaml
# lumi_config.yaml

git:
  repo: "github.com/amex/looker-project"
  branch: "main"
  model_file: "models/analytics.model.lkml"
  view_files:
    # All 30 views touched by the 137 queries
    - "views/custins_customer_insights_cardmember.view.lkml"
    - "views/acqdw_acquisition_us.view.lkml"
    - "views/risk_pers_acct_history.view.lkml"
    - "views/risk_indv_cust_hist.view.lkml"
    - "views/drm_product_member.view.lkml"
    - "views/drm_product_hier.view.lkml"
    # ... remaining 24 views

mdm:
  endpoint: "https://mdm.internal.amex.com/api/v2"
  auth: "bearer_token"
  # For each view, the MDM entity name to query
  view_to_mdm_entity:
    custins_customer_insights_cardmember: "customer_insights_cardmember"
    acqdw_acquisition_us: "acquisition_us_accounts"
    risk_pers_acct_history: "risk_account_history"
    risk_indv_cust_hist: "risk_individual_customer"
    drm_product_member: "product_member_reference"
    drm_product_hier: "product_hierarchy"
    # ... remaining mappings

gold_queries:
  file: "data/gold_queries.xlsx"
  columns:
    question: "user_prompt"
    sql: "expected_query"
    difficulty: "difficulty"

output:
  branch: "lumi/enrichment-v1"
```

---

## Architecture

### The Principle

Anthropic: "Start with the simplest solution. Only add complexity when needed."

For 137 queries across 30 views, the right architecture is:

```
DETERMINISTIC TOOLS (no LLM, no tokens, no hallucination):
  7 tools that handle ALL data plumbing

LLM AGENTS (intelligence only where intelligence is needed):
  1 agent definition instantiated 30 times (ViewEnricher)
  1 agent for explores (ExploreBuilder)
  1 agent for vocabulary consistency (VocabChecker)
```

### The Tools

```python
# ━━━ TOOL 1: parse_excel_to_json ━━━━━━━━━━━━━━━━━━
# Input: Excel file path
# Output: list of structured query dicts
# Method: openpyxl + sqlglot for SQL parsing
# LLM: NONE — pure parsing
# 
# For each query, extracts:
#   user_prompt, expected_sql, difficulty,
#   tables (from SQL), measures (aggregations),
#   dimensions (GROUP BY + non-agg SELECT),
#   filters (WHERE columns + values),
#   joins (JOIN ON conditions)

# ━━━ TOOL 2: clone_and_parse_views ━━━━━━━━━━━━━━━━
# Input: git repo URL, list of view file paths
# Output: dict of view_name → parsed structure
# Method: git clone + lkml.load() per file
# LLM: NONE — pure parsing
#
# Handles 14K+ line files in milliseconds.
# Returns structured dicts, never raw strings.

# ━━━ TOOL 3: query_mdm_api ━━━━━━━━━━━━━━━━━━━━━━━
# Input: entity_domain, MDM endpoint, auth config
# Output: entity metadata (canonical names, definitions,
#         synonyms, allowed values, relationships)
# Method: HTTP GET to MDM REST API
# LLM: NONE — pure API call
#
# Caches responses in session.state with 24h TTL.
# Falls back to column-name heuristics if MDM returns empty.

# ━━━ TOOL 4: group_queries_by_view ━━━━━━━━━━━━━━━━
# Input: list of parsed queries
# Output: dict of view_name → [queries that touch this view]
# Method: defaultdict(list), key = primary table
# LLM: NONE — pure Python grouping
#
# Also computes per-view:
#   - field_frequency: how often each column appears
#   - filter_defaults: values that appear in >80% of queries
#   - user_vocabulary: mapping of user terms → column names

# ━━━ TOOL 5: extract_join_graphs ━━━━━━━━━━━━━━━━━━
# Input: list of parsed queries (only multi-table ones)
# Output: list of distinct join patterns
# Method: sqlglot JOIN extraction + deduplication
# LLM: NONE — pure SQL parsing
#
# Each join pattern:
#   { tables: [A, B, C],
#     joins: [{left: A.col, right: B.col, type: "left_outer"}],
#     query_count: 5 }

# ━━━ TOOL 6: write_lookml_files ━━━━━━━━━━━━━━━━━━━
# Input: view_name, enriched field definitions
# Output: .view.lkml file
# Method: surgical string insertion into original file OR
#         lkml.dump() for new content
# LLM: NONE — deterministic file writing

# ━━━ TOOL 7: validate_coverage ━━━━━━━━━━━━━━━━━━━━
# Input: enriched views, list of gold queries
# Output: coverage report (per-query pass/fail)
# Method: for each query, check:
#   - Does a measure exist for each aggregation?
#   - Does a dimension exist for each GROUP BY / WHERE column?
#   - Does an explore exist with the right joins?
# LLM: NONE — deterministic field matching
#
# This is NOT an LLM evaluator. It's a Python script that
# checks structural coverage. Cheap, fast, deterministic.
```

### The Agents

```python
# ━━━ AGENT 1: ViewEnricher (one instance per view) ━━━

view_enricher = LlmAgent(
    name="ViewEnricher",
    model="gemini-2.5-pro",
    temperature=0,
    instruction="""You are a senior LookML developer enriching
    a view file to create a world-class semantic layer.

    YOUR GOAL: After your enrichment, an NL2SQL agent reading
    this view's metadata via MCP should be able to answer ANY
    question about this data — not just the gold queries, but
    any question a business user could conceivably ask.

    YOU RECEIVE:
    1. The FULL parsed view (every field, not just gold-query ones)
    2. ALL gold queries that touch this view, with:
       - The user's natural language phrasing
       - The expected SQL
       - Field frequency (how often each field is asked about)
       - Filter defaults (values in >80% of queries)
       - User vocabulary map (user terms → column names)
    3. MDM metadata for this view:
       - Canonical business names for every column
       - Business definitions
       - Synonyms and aliases
       - Allowable values for categorical fields

    YOUR OUTPUT — for EVERY field in the view:

    A) FIELDS THAT APPEAR IN GOLD QUERIES (high confidence):
       - description: Use the EXACT language from user prompts.
         If users say "NAA" for new_accounts_acquired, the
         description MUST mention "NAA" and "new accounts acquired"
         and "acquisitions." Include common filter values.
         If data_source='cornerstone' appears in 90%+ of queries,
         say "Default filter: data_source='cornerstone' for most
         business queries."
       - tags: Every synonym from user prompts + MDM glossary
       - label: MDM canonical name (or best human-readable name)
       - If a measure is MISSING (SQL aggregation exists in gold
         queries but no LookML measure), CREATE IT with full
         description.

    B) FIELDS NOT IN GOLD QUERIES (MDM-informed):
       - description: Use MDM business definition as the base.
         Apply the domain vocabulary you learned from the gold
         queries (e.g., if the domain calls things "cardmember"
         not "customer", use "cardmember" consistently).
       - tags: MDM synonyms + domain-consistent terms
       - label: MDM canonical name
       - Add tag "inferred" to mark these for future review.

    C) DERIVED DIMENSIONS (from gold query SQL):
       If a gold query contains a CASE WHEN that creates a
       derived categorization (like fico_band or age_bucket),
       CREATE a LookML dimension with that SQL logic.

    RULES:
    - NEVER delete or overwrite existing descriptions that are
      already good. Only ADD or IMPROVE.
    - Use consistent vocabulary across all fields in this view.
      If MDM says "cardmember", never say "customer" in any field.
    - For every measure, include what aggregation it performs and
      what the typical use case is.
    - For every dimension used as a filter, include common values.
    """,
    tools=[parse_lookml, query_mdm_api, write_lookml_files],
    output_key="enriched_view"
)

# ━━━ AGENT 2: ExploreBuilder ━━━━━━━━━━━━━━━━━━━━━

explore_builder = LlmAgent(
    name="ExploreBuilder",
    model="gemini-2.5-pro",
    temperature=0,
    instruction="""You build explore definitions for a LookML model.

    YOU RECEIVE:
    1. All distinct join patterns from the 137 gold queries
    2. All view names and their enriched descriptions
    3. MDM relationship data between entities

    YOUR GOAL: Create explores that cover EVERY valid join
    combination so the NL2SQL agent can answer any multi-table
    question.

    PRINCIPLES:
    - Every single-table view gets its own explore (for simple
      queries that don't need joins)
    - Multi-table join patterns from gold queries get explicit
      explores with the exact join conditions from the SQL
    - Each explore description MUST list:
      a) What types of questions it answers
      b) Which business domains it covers
      c) Any default filters the NL2SQL agent should apply
      d) The join relationships in plain English
    - If MDM shows relationships between views that DON'T
      appear in the gold queries, create explores for those too
      (these are the "any freaking question" explores)

    For each explore, include:
    - label, description (NL2SQL-optimized)
    - from: (base view)
    - join: blocks with type, relationship, sql_on
    - The description should be LONG and RICH — this is what
      the NL2SQL agent reads to decide which explore to use.
    """,
    tools=[write_model_file],
    output_key="model_file"
)

# ━━━ AGENT 3: VocabChecker ━━━━━━━━━━━━━━━━━━━━━━━

vocab_checker = LlmAgent(
    name="VocabChecker",
    model="gemini-2.5-flash",
    temperature=0,
    instruction="""You check vocabulary consistency across
    all enriched views.

    YOU RECEIVE: All labels and descriptions from all 30 views.

    CHECK FOR:
    1. Same concept, different words across views
       (e.g., "Total Spend" in view A vs "Billed Business" in B)
    2. Same column name, different descriptions in different views
    3. Inconsistent terminology (mixing "customer"/"cardmember")
    4. Missing tags that should be propagated

    For each issue, output:
    { view, field, issue, recommendation, severity }

    If everything is consistent, output: { "consistent": true }
    """,
    output_key="vocab_report"
)
```

### Composition

```python
from google.adk.agents import LlmAgent
from google.adk.agents.sequential_agent import SequentialAgent
from google.adk.agents.parallel_agent import ParallelAgent

# Phase 1: Deterministic data loading (tools, no LLM)
# These run as tool calls within a lightweight orchestrator
# or as a CustomAgent that calls them in sequence

data_loader = CustomAgent(
    name="DataLoader",
    # Calls: parse_excel_to_json, clone_and_parse_views,
    #        query_mdm_api (for all 30 views),
    #        group_queries_by_view, extract_join_graphs
    # Writes everything to session.state
    output_key="loaded_data"
)

# Phase 2: Per-view enrichment (LLM, parallel across 30 views)
# Each instance reads its view's data from session.state
enrichment_team = ParallelAgent(
    name="EnrichmentTeam",
    sub_agents=[view_enricher]  # dynamically instantiated per view
)

# Phase 3: Explores + Vocabulary check (LLM, sequential)
finalization = SequentialAgent(
    name="Finalization",
    sub_agents=[explore_builder, vocab_checker]
)

# Phase 4: Validation (tool, no LLM)
validator = CustomAgent(
    name="Validator",
    # Calls: validate_coverage for all 137 queries
    # Outputs: coverage report
    output_key="coverage_report"
)

# Root
root_agent = SequentialAgent(
    name="LUMI",
    sub_agents=[data_loader, enrichment_team, finalization, validator]
)
```

### The Pipeline Visualized

```
INPUT
  Excel (137 queries) + GitHub (30 views) + MDM API (30 entities)
  │
  ▼
PHASE 1: DATA LOADING [CustomAgent — no LLM]
  parse_excel_to_json()      → 137 structured query dicts
  clone_and_parse_views()    → 30 parsed view structures
  query_mdm_api() × 30      → 30 MDM entity metadata sets
  group_queries_by_view()    → queries grouped by primary table
  extract_join_graphs()      → ~10-15 distinct join patterns
  │
  ▼
PHASE 2: VIEW ENRICHMENT [ParallelAgent — 30 LLM calls]
  ViewEnricher(view_1, queries_for_v1, mdm_for_v1)  ──┐
  ViewEnricher(view_2, queries_for_v2, mdm_for_v2)  ──┤
  ViewEnricher(view_3, queries_for_v3, mdm_for_v3)  ──┤ PARALLEL
  ...                                                  │
  ViewEnricher(view_30, queries_for_v30, mdm_for_v30)──┘
  │
  ▼
PHASE 3: FINALIZATION [SequentialAgent — 2 LLM calls]
  ExploreBuilder(all join patterns, all view descriptions)
  VocabChecker(all labels, all descriptions)
  │
  ▼
PHASE 4: VALIDATION [CustomAgent — no LLM]
  validate_coverage(enriched_views, 137 gold queries)
  → Coverage report: X/137 queries fully resolvable
  → Gap report: which queries failed and why
  │
  ▼
OUTPUT
  30 enriched .view.lkml files (on Git branch)
  1 enriched .model.lkml file (with all explores)
  coverage_report.json
  gap_report.json
  vocab_report.json
```

---

## Cost & Time Estimate

```
Phase 1: ~30 seconds (API calls + parsing, no LLM cost)
Phase 2: 30 parallel LLM calls × ~5K tokens each = ~150K tokens
         Gemini 2.5 Pro: ~$0.19 input + ~$0.75 output = ~$0.94
Phase 3: 2 LLM calls × ~10K tokens each = ~20K tokens
         ~$0.12
Phase 4: ~5 seconds (deterministic validation)

TOTAL COST:  ~$1.10
TOTAL TIME:  ~3-5 minutes
LLM CALLS:   32 (30 views + explore builder + vocab checker)
```

---

## For Views With 14K+ Lines

The ViewEnricher handles this with the same decomposition from v3:

1. `lkml.load()` parses the full file (zero tokens)
2. If field count > 150, batch in dependency + semantic sibling order
3. Extract domain vocabulary from gold queries → inject into every batch
4. Extract MDM metadata → inject into every batch
5. Reassemble with `lkml.dump()` (zero tokens)

For views under 150 fields: one LLM call per view.
For views over 150 fields: ~5-10 batched calls per view.

Most of your 30 views will be under 150 fields. The 2-3 large ones get batched.

---

## The CLAUDE.md for Implementation

```markdown
# LUMI — LookML Enrichment System

## What This Is
Multi-agent system that enriches 30 LookML views using 137 gold queries
+ MDM business metadata. Goal: any NL2SQL question about these views
should be answerable with high confidence.

## Tech Stack
Python 3.11+, google-adk, lkml, sqlglot, openpyxl, pyyaml, pydantic, requests

## Architecture Rules — NEVER VIOLATE
1. LLM NEVER sees raw .lkml file contents. Always lkml.load() first.
2. SQL parsing is sqlglot. Never regex. Never LLM.
3. Excel parsing is openpyxl. Never LLM.
4. MDM API calls are HTTP requests. Never LLM.
5. Query grouping is defaultdict. Never LLM.
6. Coverage validation is field matching. Never LLM.
7. The LLM is ONLY used for: writing descriptions, creating measures,
   building explores, and checking vocabulary. NOTHING ELSE.
8. Temperature=0 for all LlmAgents.
9. Every field in the view gets enriched — not just gold-query fields.
10. MDM canonical terms take precedence over LLM-invented names.

## File Structure
lumi/
├── CLAUDE.md
├── agent.py               # root_agent composition
├── config.py              # config loader + validation
├── tools/
│   ├── __init__.py
│   ├── excel_tools.py     # parse_excel_to_json
│   ├── git_tools.py       # clone_and_parse_views
│   ├── mdm_tools.py       # query_mdm_api (with caching + fallback)
│   ├── grouping_tools.py  # group_queries_by_view, extract_join_graphs
│   ├── lookml_tools.py    # parse_lookml, write_lookml_files
│   └── validation_tools.py # validate_coverage
├── agents/
│   ├── __init__.py
│   ├── data_loader.py     # CustomAgent: calls all tools
│   ├── view_enricher.py   # LlmAgent: per-view enrichment
│   ├── explore_builder.py # LlmAgent: explore definitions
│   ├── vocab_checker.py   # LlmAgent: vocabulary consistency
│   └── validator.py       # CustomAgent: coverage validation
├── schemas/
│   ├── config_schema.py   # Pydantic: lumi_config.yaml
│   ├── query_schema.py    # Pydantic: parsed gold query
│   ├── view_schema.py     # Pydantic: parsed view
│   └── report_schema.py   # Pydantic: coverage report
├── prompts/
│   ├── view_enricher.md   # instruction for ViewEnricher
│   ├── explore_builder.md # instruction for ExploreBuilder
│   └── vocab_checker.md   # instruction for VocabChecker
├── tests/
│   ├── test_tools/
│   │   ├── test_excel_tools.py
│   │   ├── test_git_tools.py
│   │   ├── test_mdm_tools.py
│   │   ├── test_grouping_tools.py
│   │   ├── test_lookml_tools.py
│   │   └── test_validation_tools.py
│   ├── test_agents/
│   │   └── test_integration.py
│   └── fixtures/
│       ├── sample_view.lkml
│       ├── sample_queries.xlsx
│       └── sample_mdm_response.json
├── lumi_config.yaml
└── pyproject.toml

## Commands
adk run lumi/
pytest tests/ -v
pytest tests/test_tools/ -v  # tools only
python -m lumi.agents.validator  # run validation standalone

## Testing Rules
- Every tool has unit tests. Tools are deterministic — tests are exact.
- ViewEnricher has an integration test with mock session.state.
- Coverage validation runs on ALL 137 queries after every change.
- NEVER modify tests to make them pass. Fix implementation.

## Build Order (each is one Claude Code session)
Session 1: Pydantic schemas (config, query, view, report)
Session 2: excel_tools + git_tools + tests
Session 3: mdm_tools + grouping_tools + tests
Session 4: lookml_tools + validation_tools + tests
Session 5: data_loader CustomAgent + test
Session 6: ViewEnricher LlmAgent + prompt + test
Session 7: ExploreBuilder + VocabChecker + prompts + tests
Session 8: Root agent composition + integration test
Session 9: Run on real data. Fix gaps. Ship.
```

---

## Claude Code Session Plan

| Session | Focus | Prompt Pattern | Output |
|---------|-------|----------------|--------|
| 1 | Schemas | "Build Pydantic models for config, parsed query, parsed view, and coverage report. TDD." | schemas/*.py + tests |
| 2 | Excel + Git tools | "Build parse_excel_to_json using openpyxl + sqlglot. Build clone_and_parse_views using lkml. TDD." | excel_tools.py, git_tools.py + tests |
| 3 | MDM + Grouping tools | "Build query_mdm_api with 24h caching and snake_case fallback. Build group_queries_by_view and extract_join_graphs. TDD." | mdm_tools.py, grouping_tools.py + tests |
| 4 | LookML + Validation tools | "Build parse_lookml (with batching for 150+ fields) and validate_coverage. TDD." | lookml_tools.py, validation_tools.py + tests |
| 5 | DataLoader agent | "Build CustomAgent that calls tools 1-5 in sequence, writes to session.state. Test with fixtures." | data_loader.py + test |
| 6 | ViewEnricher agent | "Read the prompt at prompts/view_enricher.md. Build the LlmAgent. Run against one view with mock state. Check output quality." | view_enricher.py + test |
| 7 | ExploreBuilder + VocabChecker | "Build both agents. ExploreBuilder reads join patterns from state. VocabChecker reads all descriptions. Test both." | explore_builder.py, vocab_checker.py + tests |
| 8 | Composition | "Wire all agents into root_agent as SequentialAgent(DataLoader, ParallelAgent(ViewEnricher×30), SequentialAgent(ExploreBuilder, VocabChecker), Validator). Integration test." | agent.py + integration test |
| 9 | Ship | "Run on real data. Review coverage report. Fix gaps in enrichment prompts. Run again. Commit to branch." | enriched .lkml files on Git branch |

**Total: 9 sessions. 3-4 days of focused work.**

---

## What Makes This "Any Freaking Question" Ready

The system achieves full coverage through three reinforcing layers:

**Layer 1 — Gold Query Vocabulary (HIGH confidence, 137 queries):**
Every field touched by a gold query gets descriptions using the exact language
users actually use. "NAA" for new_accounts_acquired. "Spend" for billed_business.
These descriptions are the highest quality because they're grounded in real usage.

**Layer 2 — MDM Business Definitions (MEDIUM-HIGH confidence, every field):**
Every field NOT in the gold queries gets its description from MDM: canonical name,
business definition, synonyms, allowed values. The quality depends on MDM coverage,
but even partial MDM is better than no description.

**Layer 3 — LLM Domain Inference (MEDIUM confidence, gap-filling):**
For fields with no gold query AND no MDM coverage, the LLM infers descriptions
from the column name, neighboring field context, and the domain vocabulary it
learned from Layers 1 and 2. Tagged as "inferred" for future review.

Together: Layer 1 covers ~30% of fields with the best descriptions. Layer 2
covers ~60% with good descriptions. Layer 3 covers the remaining ~10% with
reasonable descriptions. Every field is enriched. Nothing is left blank.

The explores then make every valid join combination queryable. The NL2SQL agent
can route any question to the right explore, find the right fields, and generate
SQL — whether the question was in the training set or not.

That's the system. Build it in 9 sessions. Ship it in a week.