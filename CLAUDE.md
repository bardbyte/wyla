# LUMI — LookML Understanding & Metric Intelligence

## What This Is
Multi-agent LookML enrichment system on Google ADK. Takes 137 gold NL-to-SQL
queries + 30 LookML views from GitHub + internal MDM API metadata. Produces
enriched LookML where every field can answer ANY NL2SQL question.

## Tech Stack
Python 3.11+, google-adk (>=1.31.1), lkml, sqlglot, openpyxl, pyyaml, pydantic, requests
LLM access: SafeChain (LangChain-compatible) — NEVER openai/google.generativeai/vertexai directly
Testing: pytest, ruff, mypy

## LLM Access (read docs/README.md + docs/ADK_INTEGRATION.md)
- Single entry point: `from src.adapters.model_adapter import get_model`
- Model indices (strings, not ints):
  - "1" → google-gemini-2.5-pro (chat, reasoning-heavy)
  - "2" → bge-large-en-v1.5 (embedding, 1024-dim)
  - "3" → google-gemini-2.5-flash (chat, default for latency)
- For ADK: wrap via `make_safechain_llm(model_idx)` from `src/adapters/adk_safechain_llm.py`
  - `LlmAgent(model=make_safechain_llm("1"), ...)` — NEVER `LlmAgent(model="gemini-2.5-pro")`
- Prefer `.with_structured_output(PydanticModel)` for all extraction — NEVER regex-parse
- For >10 concurrent calls use `.abatch(...)` not sequential `.invoke(...)`
- .env must have CIBIS_CONSUMER_INTEGRATION_ID, CIBIS_CONSUMER_SECRET, CONFIG_PATH

## Architecture (6 Phases)
Phase 0: Load — CustomAgent calls tools. NO LLM. Parse Excel, clone Git, query MDM, group queries.
Phase 1: Analyze — ParallelAgent. PatternMiner (sqlglot) + IntentClassifier. 2 LLM calls.
Phase 2: Blueprint — ParallelAgent. One BlueprintNarrator per view. ~30 Flash calls.
Phase 3: Enrich — CustomAgent (MDM transpile, NO LLM) → LoopAgent (batch enrichment, LLM).
Phase 4: Finalize — SequentialAgent. ExploreBuilder + VocabChecker. 2 LLM calls.
Phase 5: Validate — CustomAgent. Deterministic coverage check. NO LLM.

## 12 Rules — NEVER VIOLATE
1. LLM NEVER sees raw .lkml. Always lkml.load() first.
2. SQL parsing = sqlglot. Never regex. Never LLM.
3. LookML parsing = lkml library. Never regex. Never LLM.
4. Deterministic logic = CustomAgent or tool function. Never LlmAgent.
5. LLM is ONLY for: descriptions, tags, labels, explores, vocab check.
6. Temperature=0 for all LlmAgents (set via llm.bind or generation_config — SafeChain-wrapped).
7. Every field in every view gets enriched — not just gold-query fields.
8. All endpoints/paths from lumi_config.yaml. Never hardcode.
9. Type hints on ALL functions. Pydantic for complex types.
10. Tool docstrings must be clear — ADK uses them for tool selection.
11. LLM access goes through SafeChain only (get_model / make_safechain_llm). No openai/google.generativeai/vertexai imports anywhere.
12. Structured LLM output = with_structured_output(PydanticModel). Never regex-parse free text.

## File Structure
lumi/
├── agent.py                # Root agent composition
├── config.py               # Config loader
├── tools/                  # 7 deterministic tools
│   ├── excel_tools.py      # parse_excel_to_json
│   ├── git_tools.py        # clone_and_parse_views
│   ├── mdm_tools.py        # query_mdm_api (no auth, cached)
│   ├── grouping_tools.py   # group_queries_by_view, extract_join_graphs
│   ├── lookml_tools.py     # parse_lookml (lkml wrapper with batching)
│   └── validation_tools.py # validate_coverage (deterministic)
├── agents/                 # 3 LlmAgents + 2 CustomAgents
│   ├── data_loader.py      # CustomAgent: orchestrates tool calls
│   ├── view_enricher.py    # LlmAgent: per-view enrichment
│   ├── explore_builder.py  # LlmAgent: explore definitions
│   └── vocab_checker.py    # LlmAgent: vocabulary consistency
├── schemas/                # Pydantic models
├── prompts/                # Agent instructions as .md files
├── tests/                  # pytest suite
│   ├── test_tools/
│   ├── test_agents/
│   └── fixtures/
└── lumi_config.yaml

## Commands
pytest tests/ -v                              # all tests
pytest tests/test_tools/test_excel_tools.py -v # one tool
ruff check lumi/ --fix                        # lint
adk run lumi/                                 # run pipeline
./scripts/preflight_deps.sh                   # check deps
./scripts/preflight_github.sh                 # check git
./scripts/preflight_mdm.sh                    # check MDM API
./scripts/preflight_llm.sh                    # check Gemini

## Domain Facts
- data_source='cornerstone' is a DEFAULT filter (90%+ of queries)
- "NAA" = new_accounts_acquired, "AIF" = accounts_in_force
- MDM API: no auth. GET {endpoint}/{table_name} → metadata JSON
- BigQuery dialect: sqlglot.parse_one(sql, dialect="bigquery")
- Views can be 14K+ lines. lkml.load() handles any size in milliseconds.

## Three Sources of Truth
1. Gold Queries (137): user vocabulary, missing measures, join paths. ~30% of fields.
2. MDM API: canonical names, definitions, synonyms, allowed values. ~60% of fields.
3. LLM Inference: domain-consistent descriptions for rest. Tag as "inferred". ~10%.

## Code Style
- pathlib for paths. logging not print(). Double quotes user-facing, single internal.
- Line length 99. Imports: stdlib → third-party → local.
- Tests: pytest. TDD always. NEVER modify tests to make them pass.

## Build Order (one session each, /clear between sessions)
Session 0: SafeChain→ADK adapter (adk_safechain_llm.py) + smoke test. Prereq for Sessions 6-8.
Session 1: Pydantic schemas (config, query, view, report)
Session 2: excel_tools + git_tools (TDD)
Session 3: mdm_tools + grouping_tools (TDD) — confirm MDM auth policy first
Session 4: lookml_tools + validation_tools (TDD)
Session 5: DataLoader CustomAgent
Session 6: ViewEnricher LlmAgent (via make_safechain_llm("1")) + prompt
Session 7: ExploreBuilder ("1") + VocabChecker ("3")
Session 8: Root agent.py + integration test
Session 9: Real data run + fix gaps + ship

## Open questions to resolve before coding
- MDM auth: CLAUDE.md said "no auth", design doc config shows bearer_token. Confirm before Session 3.
- ViewEnricher tools: design doc lists [parse_lookml, query_mdm_api, write_lookml_files] as LLM tools.
  Prefer: pre-populate session.state from CustomAgent + use with_structured_output for emission.
  LLM does not need these as callable tools.
- Temperature=0: set via llm.bind(temperature=0) on the SafeChain model OR via generation_config
  passed through LlmRequest. Pick one pattern in Session 0.
- google-adk version: pin >=1.31.1 (stale >=0.3.0 still in some repos).
