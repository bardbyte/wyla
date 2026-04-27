# LUMI — Implementation Plan

_Source docs analyzed: `design-doc-for-lookml-enrichment-pipeline.md`, `docs/README.md` (SafeChain LLM access), `docs/ADK_INTEGRATION.md` (SafeChain→ADK adapter). Last refreshed: 2026-04-23._

---

## Current state of the repo

| Area | State |
|---|---|
| `CLAUDE.md` | Written. Updated to reflect SafeChain LLM access (not raw Gemini/Vertex). |
| `.claude/commands/` | 5 slash commands written: `implement-tool`, `implement-agent`, `validate`, `preflight`, `plan`. |
| `.claude/settings.json` | Hooks (ruff auto-fix on Edit/Write of `.py`) + permissions allowlist. |
| `docs/DESIGN.md` | Does not exist. Slash commands reference it — create or alias to `design-doc-for-lookml-enrichment-pipeline.md`. |
| `lumi/` package | Does not exist yet. |
| `tests/` | Does not exist yet. |
| `config/config.yml` | Does not exist in this repo. SafeChain assumes it lives at `CONFIG_PATH` — may be external to this repo. |
| `src/adapters/model_adapter.py` | Does not exist here. `get_model()` is referenced from another Amex repo/package. Confirm installation path. |
| `.env` | Must be created before any LLM call. `.env.example` should be committed. |
| `pyproject.toml` / `requirements.txt` | Empty or missing. |

---

## What you MUST know before coding (distilled from the three docs)

### 1. Three sources of truth (design doc §9–33)
- **Gold Queries (137)**: Excel at `data/gold_queries.xlsx`. Columns: `user_prompt`, `expected_query`, `difficulty`. Supplies user vocabulary (e.g., "NAA", "AIF"), aggregation patterns, join paths, default filters (`data_source='cornerstone'` in 90%+). Used as the validation test suite.
- **MDM API (30 entities)**: REST endpoint per view. Provides canonical names, definitions, synonyms, allowed values, relationships. Fills gaps the 137 queries don't cover (~60% of fields).
- **LookML Views (30)**: From GitHub. The substrate we enrich. Parsed with `lkml.load()`, never string-scraped.

Target enrichment coverage: Layer 1 (gold-query-informed) ~30% · Layer 2 (MDM-informed) ~60% · Layer 3 (LLM-inferred, tagged `inferred`) ~10%. Every field of every view gets enriched.

### 2. LLM access (docs/README.md)
- **Single entry point:** `from src.adapters.model_adapter import get_model` → returns LangChain-compatible model.
- **Model registry** (strings, not ints):
  - `"1"` = `google-gemini-2.5-pro` (chat) — reasoning-heavy enrichment, explore authoring.
  - `"2"` = `bge-large-en-v1.5` (embedding, 1024-dim) — not used by LUMI's core pipeline (no vector search in enrichment). Could be used for semantic-sibling clustering if we want.
  - `"3"` = `google-gemini-2.5-flash` (chat) — default. Use for VocabChecker and any classification.
- **Traffic path:** SafeChain → CIBIS/IDaaS → Amex-hosted Gemini. No direct openai / google-generativeai / vertexai imports anywhere in this repo.
- **Env vars required:** `CIBIS_CONSUMER_INTEGRATION_ID`, `CIBIS_CONSUMER_SECRET`, `CONFIG_PATH`.
- **Preferred patterns:**
  - Structured extraction: `llm.with_structured_output(PydanticModel)`. Never regex-parse.
  - Concurrency: `.abatch([...])` for >10 calls. Sequential `.invoke` loops are wasteful.
  - BGE queries (not docs) must be prefixed with `BGE_QUERY_PREFIX` from `config.constants`.
- **Antipatterns that will break the build:** hardcoding model names (`"gemini-2.5-pro"`), bypassing SafeChain with raw SDK imports, committing `.env`, caching `get_model()` across threads without a lock.

### 3. ADK integration (docs/ADK_INTEGRATION.md)
- ADK ≥ 1.31.1 (older pins are stale).
- `LlmAgent(model=get_model("1"))` is **wrong** — ADK needs a `BaseLlm`. Wrap with `SafeChainLlm(BaseLlm)` and use `make_safechain_llm("1")`.
- `LiteLlm` does NOT work with SafeChain (LiteLlm wants an HTTP endpoint; SafeChain is a Python object).
- **Session state:** mutate only via `tool_context.state["k"] = v` inside a tool/callback, or via `output_key` auto-write. Direct mutation from outside silently vanishes.
- **State scopes:** `user:` (cross-session), `app:` (global), `temp:` (invocation-only), no prefix = current session.
- **Streaming:** distinguish token streaming (`RunConfig.streaming_mode=SSE`) from event streaming (tool calls, sub-agent transitions — always separate events).
- **Deploy:** Cloud Run / GKE. NOT `adk deploy agent_engine` (bug #4208 breaks custom `BaseLlm`).
- **LlmAgent + `output_schema` is mutually exclusive with `tools`.** For ViewEnricher, prefer `output_schema=EnrichedView` and pre-populate state from a CustomAgent rather than giving the LLM tools to call.

### 4. Key domain facts
- `data_source='cornerstone'` is a default filter in 90%+ of queries — mention in every explore description.
- Acronyms: NAA (new_accounts_acquired), AIF (accounts_in_force), and many more in the gold queries.
- BigQuery dialect: `sqlglot.parse_one(sql, dialect="bigquery")`.
- Some views are 14K+ lines — `lkml.load()` handles them in milliseconds; batching is for LLM calls only.

---

## Contradictions to resolve (discovered during analysis)

| # | Source A | Source B | Action |
|---|---|---|---|
| 1 | CLAUDE.md: "MDM API: no auth" | design doc config: `mdm.auth: "bearer_token"` | **Confirm with data team before Session 3.** If bearer token needed, add token to `.env` and to `lumi_config.yaml.auth`. |
| 2 | design doc ViewEnricher: `tools=[parse_lookml, query_mdm_api, write_lookml_files]` | ADK doc: `output_schema` is mutually exclusive with `tools`; best practice is to pre-populate state | **Prefer the latter.** DataLoader CustomAgent pre-populates `session.state` with parsed view + MDM metadata + grouped queries. ViewEnricher emits `EnrichedView` via `with_structured_output` / `output_schema`. No tools needed during enrichment. |
| 3 | design doc: `LlmAgent(model="gemini-2.5-pro", temperature=0, ...)` | README.md: hardcoding model names is an antipattern; temperature must be bound on the LangChain side OR passed via `LlmRequest.config.generation_config` | **Build Session 0 (adapter) to handle this.** Pattern: `SafeChainLlm(lc_model=get_model("1").bind(temperature=0))`. |
| 4 | design doc: `enrichment_team = ParallelAgent(sub_agents=[view_enricher])` with dynamic instantiation per view | ADK docs: `ParallelAgent` takes a static list of sub-agents at construction time | **Instantiate 30 ViewEnricher instances up front**, one per view, each with an `input_schema` pointing at its slice of `session.state` (e.g., `state["views"][view_name]`). Or use `LoopAgent` over a queue of views. Decide in Session 8. |
| 5 | design doc: `auth: "bearer_token"` in mdm config | README.md / CLAUDE.md focus on CIBIS env vars for LLM — no MDM auth pattern documented | **Document MDM auth separately** in `lumi_config.yaml` (`mdm.auth_env: MDM_BEARER_TOKEN`), load from `.env`. |

---

## Build Order — 9 sessions + Session 0 prerequisite

Each session = one Claude Code session, `/clear` between them, one git commit minimum. Sessions are dependent on predecessors; the whole chain is ~3-4 engineer-days.

### Session 0: SafeChain→ADK adapter (PREREQUISITE for Sessions 6-8)
**Goal:** Make `make_safechain_llm(model_idx)` work end-to-end so `LlmAgent` can actually call Gemini.

**Depends on:** Access to `src.adapters.model_adapter.get_model` (resolve from Amex repo or install). `.env` populated. `config/config.yml` resolvable via `CONFIG_PATH`.

**Deliverables:**
- `src/adapters/adk_safechain_llm.py` — `SafeChainLlm(BaseLlm)` + `make_safechain_llm(model_idx: str)` (copy from `docs/ADK_INTEGRATION.md` Step 1).
- `scripts/preflight_llm.sh` — smoke test: calls `get_model("3")` with `.invoke("ping")`, asserts non-empty `.content`. Also runs `make_safechain_llm("3")` through an `LlmAgent` in a tiny `Runner`.
- `.env.example` — committed template of required env vars.

**Acceptance:** `./scripts/preflight_llm.sh` exits 0. Both raw SafeChain (`get_model("3").invoke(...)`) and ADK-wrapped (`LlmAgent(model=make_safechain_llm("3"))` via `Runner.run_async`) return text responses.

**Estimated time:** 2-3 hours (mostly unblocking env / config issues).

---

### Session 1: Pydantic schemas
**Depends on:** Nothing.

**Deliverables:**
- `lumi/schemas/config_schema.py` — validates `lumi_config.yaml` (git, mdm, gold_queries, llm, output, batching).
- `lumi/schemas/query_schema.py` — `ParsedQuery` (user_prompt, expected_sql, difficulty, tables, measures [{function, column}], dimensions, filters [{column, value}], joins [{left, right, type}]).
- `lumi/schemas/view_schema.py` — `ParsedView` (view_name, sql_table_name, dimensions, measures, dimension_groups, field_count). Plus `EnrichedField`, `EnrichedView` for LLM output.
- `lumi/schemas/report_schema.py` — `CoverageReport`, `GapReport`, `VocabReport`.
- `tests/test_schemas/` — one file per schema: valid input, invalid input, edge case (empty, oversized).

**Acceptance:** `pytest tests/test_schemas/ -v` all green. `mypy --strict lumi/schemas/` clean.

**Notes:** `EnrichedView` will be the `output_schema` for ViewEnricher — design it carefully (enough structure for the LLM to fill; not so rigid it rejects valid emissions).

---

### Session 2: excel_tools + git_tools (TDD)
**Depends on:** Session 1.

**Deliverables:**
- `lumi/tools/excel_tools.py` → `parse_excel_to_json(path) -> list[ParsedQuery]`. Uses `openpyxl` + `sqlglot.parse_one(sql, dialect="bigquery")`. Extracts tables, aggregations (measures), GROUP BY + non-agg SELECT (dimensions), WHERE (filters), JOIN ON (joins).
- `lumi/tools/git_tools.py` → `clone_and_parse_views(repo, branch, model_file, view_files) -> dict[str, ParsedView]`. Uses `git` subprocess + `lkml.load()`. Caches clone; pulls if re-run.
- `tests/test_tools/test_excel_tools.py` + `test_git_tools.py` — happy path, missing-file, malformed SQL (CTE, CASE WHEN), empty, large.
- `tests/fixtures/sample_queries.xlsx`, `tests/fixtures/sample_view.lkml`.

**Acceptance:** Tests pass. Tools return `dict` with `status` key (`"success"` | `"error"`) per CLAUDE.md rule.

**Edge cases flagged:**
- CTEs: `WITH ... SELECT` — `sqlglot` handles, but some queries have nested CTEs.
- CASE WHEN derived columns — preserve the expression for Session 6 (derived dimension creation).
- Non-standard BQ SQL (e.g., `QUALIFY`, array operations) — log warning, continue.

---

### Session 3: mdm_tools + grouping_tools (TDD)
**Depends on:** Sessions 1-2. **Blocked on:** MDM auth policy decision (contradiction #1).

**Deliverables:**
- `lumi/tools/mdm_tools.py` → `query_mdm_api(endpoint, entity_name, auth=None) -> dict`. 24h disk cache at `.mdm_cache/{entity}.json`. Fallback to `snake_case → title-case` label generation when MDM returns empty. Report coverage %.
- `lumi/tools/grouping_tools.py` → `group_queries_by_view(queries) -> dict[str, list[ParsedQuery]]` (by primary table); `extract_join_graphs(queries) -> list[JoinPattern]` (distinct join signatures with counts); plus per-view `field_frequency`, `filter_defaults` (>80% coverage values), `user_vocabulary` (user-term → column).
- `tests/test_tools/test_mdm_tools.py` — happy path (mocked HTTP), empty response (fallback triggered), 401 (auth), 404 (entity missing), cache hit.
- `tests/test_tools/test_grouping_tools.py` — single-table query, multi-table join, deduplication, default filter detection.
- `tests/fixtures/sample_mdm_response.json` — saved from a real pre-flight call.

**Acceptance:** Tests pass. Cache behavior verified (second call skips HTTP).

---

### Session 4: lookml_tools + validation_tools (TDD)
**Depends on:** Sessions 1-3.

**Deliverables:**
- `lumi/tools/lookml_tools.py`:
  - `parse_lookml(path) -> ParsedView` — `lkml.load()` wrapper.
  - `batch_fields(view, max_batch=30) -> list[Batch]` — if `field_count > 150`, split into batches in dependency order (from `${field_name}` references); co-batch semantic siblings (same prefix / same MDM parent) even if batch exceeds 30.
  - `write_lookml_files(enriched_view, out_dir)` — `lkml.dump()` OR surgical string insertion preserving existing formatting. Preserve existing human descriptions that are good.
- `lumi/tools/validation_tools.py`:
  - `validate_coverage(enriched_views, gold_queries) -> CoverageReport` — per-query: does a measure exist for each aggregation? dimension for each WHERE/GROUP BY column? explore with the right joins? Purely structural (no LLM).
- Tests for 14K-line view (generate synthetic), view with cyclic `${}` deps (error), empty view (edge case).

**Acceptance:** Tests pass. Batcher handles the known-large views without OOM (`lkml.load` is fast; batch logic is the concern).

---

### Session 5: DataLoader CustomAgent
**Depends on:** Sessions 1-4.

**Deliverables:**
- `lumi/agents/data_loader.py` — `DataLoader(CustomAgent)`, `_run_async_impl` calls the 5 tools in sequence, writes to `session.state`:
  - `state["gold_queries"]` — list[ParsedQuery]
  - `state["parsed_views"]` — dict[str, ParsedView]
  - `state["mdm_metadata"]` — dict[str, dict]
  - `state["queries_by_view"]` — dict[str, list[ParsedQuery]]
  - `state["join_graphs"]` — list[JoinPattern]
  - `state["field_frequency"]`, `state["filter_defaults"]`, `state["user_vocabulary"]` per view.
- `tests/test_agents/test_data_loader.py` — pre-populate fixture files, run agent, assert all state keys populated.

**Acceptance:** `pytest tests/test_agents/test_data_loader.py -v` green. State is fully populated after run.

---

### Session 6: ViewEnricher LlmAgent
**Depends on:** Session 0 (adapter) + Session 5. **Core session — highest value per hour.**

**Deliverables:**
- `lumi/prompts/view_enricher.md` — long instruction (see design doc §190-258). Must cover:
  - A) Gold-query fields → use EXACT user language, include common filter values.
  - B) MDM-only fields → canonical names, business definitions, synonym tags, `inferred` tag.
  - C) Derived dimensions from CASE WHEN in gold SQL → create LookML dimension with SQL.
  - Vocabulary consistency rule (if MDM says "cardmember", never use "customer").
  - Data source default filter mention.
- `lumi/agents/view_enricher.py`:
  - `build_view_enricher(view_name) -> LlmAgent(model=make_safechain_llm("1"), output_schema=EnrichedView, ...)`.
  - Reads from `state["parsed_views"][view_name]`, `state["queries_by_view"][view_name]`, `state["mdm_metadata"][view_name]`.
  - Writes to `state["enriched_views"][view_name]` via `output_key`.
  - For >150 fields: wrap in `LoopAgent` over batches, or call multiple times per batch and merge.
- `tests/test_agents/test_view_enricher.py` — pre-populate state with fixtures for one small view. Run agent. Assert output structure. Spot-check content quality manually (record first-run output for regression).

**Acceptance:** One real view enriches end-to-end. Manual review of output: descriptions use gold-query vocabulary, MDM canonical names present, `inferred` tag on non-MDM/non-gold fields.

**Edge cases:**
- Output schema rejects mid-generation — log, retry once, then skip the field with an explicit error marker (do not hallucinate).
- Very long view (14K lines) — batching must preserve cross-field vocabulary consistency. Pass the accumulated vocabulary as context on each batch.

---

### Session 7: ExploreBuilder + VocabChecker
**Depends on:** Sessions 0, 5, 6.

**Deliverables:**
- `lumi/prompts/explore_builder.md` + `lumi/agents/explore_builder.py` → `LlmAgent(model=make_safechain_llm("1"), ...)`. Reads `state["join_graphs"]` + `state["enriched_views"]` + `state["mdm_metadata"]`. Emits `state["model_file"]` — all explores with rich descriptions, each mentioning `data_source='cornerstone'` default filter.
- `lumi/prompts/vocab_checker.md` + `lumi/agents/vocab_checker.py` → `LlmAgent(model=make_safechain_llm("3"), output_schema=VocabReport)`. Reads all labels/descriptions. Flags: same concept different words (e.g., "Total Spend" vs "Billed Business"), inconsistent terminology ("customer" vs "cardmember"), missing cross-view tags. Writes `state["vocab_report"]`.
- Tests with mocked `state` fixtures.

**Acceptance:** Both agents produce structured output via `output_schema`. VocabChecker catches planted inconsistencies in test fixtures.

---

### Session 8: Root agent composition + integration test
**Depends on:** Sessions 0-7.

**Deliverables:**
- `lumi/agent.py`:
```python
root_agent = SequentialAgent(
    name="LUMI",
    sub_agents=[
        data_loader,                                      # CustomAgent, no LLM
        ParallelAgent("EnrichmentTeam",
            sub_agents=[build_view_enricher(v) for v in view_names]),  # 30 LLM calls parallel
        SequentialAgent("Finalization",
            sub_agents=[explore_builder, vocab_checker]),  # 2 LLM calls
        validator,                                         # CustomAgent, no LLM
    ],
)
```
- `tests/test_agents/test_integration.py` — run full pipeline on fixture data (small subset: 3 views, 10 gold queries). Assert: all views enriched, coverage ≥ 90%, explores present, vocab report no high-severity issues.

**Acceptance:** Integration test passes. Run time < 2 minutes on fixtures.

**Decision deferred from contradiction #4:** If `ParallelAgent` with static sub_agents is limiting, fall back to `LoopAgent` over a view queue. Benchmark both on 5 views before committing.

---

### Session 9: Real data run + fix gaps + ship
**Depends on:** All prior.

**Steps:**
1. Populate `lumi_config.yaml` with real repo URL, MDM endpoint, view paths, gold Excel path.
2. `/preflight` — all four scripts must pass.
3. `adk run lumi/` — full pipeline on real data.
4. `/validate` — inspect coverage + vocab reports.
5. Fix gaps: tweak prompts, add tool logic for edge cases that surfaced. Re-run.
6. `git checkout -b lumi/enrichment-v1`; commit enriched `.view.lkml` + `.model.lkml`; push.
7. Measure: token cost (expect ~$1.10), wall time (expect 3-5 min), coverage (expect ≥95% of 137 gold queries fully resolvable).

**Acceptance:** Coverage report ≥95% PASS on the 137 gold queries. Vocab report clean. Enriched branch pushed.

---

## Test fixtures to create in Session 1 and reuse everywhere

| Fixture | Session | Used by |
|---|---|---|
| `tests/fixtures/sample_queries.xlsx` (5-10 queries, mix of single/multi-table, CTEs) | 2 | 2, 3, 5, 8 |
| `tests/fixtures/sample_view.lkml` (small, ~20 fields) | 2 | 2, 4, 5, 6, 8 |
| `tests/fixtures/large_view.lkml` (synthetic 500 fields) | 4 | 4, 6 |
| `tests/fixtures/sample_mdm_response.json` (real pre-flight save) | 3 | 3, 5, 6 |
| `tests/fixtures/planted_vocab_issues.json` (for VocabChecker) | 7 | 7 |
| `tests/conftest.py` — shared fixtures + in-memory `session.state` builder | 1 | All |

---

## Cost / time budget (from design doc §422-435, re-checked against SafeChain)

| Phase | LLM calls | Tokens | Cost (Pro+Flash mix) | Wall time |
|---|---|---|---|---|
| 1 Load | 0 | 0 | $0 | ~30s |
| 2 Enrich (30 parallel) | ~30 (Pro) | ~150K | ~$0.94 | ~60-90s (rate-limit dependent) |
| 3 Finalize | 2 (1 Pro + 1 Flash) | ~20K | ~$0.12 | ~15s |
| 4 Validate | 0 | 0 | $0 | ~5s |
| **Total** | **~32** | **~170K** | **~$1.10** | **~3-5 min** |

Large views with batching may push total calls to ~50 and cost to ~$1.50.

---

## Immediate next steps (in order)

1. **Resolve contradictions** (MDM auth, ViewEnricher tools vs output_schema, temperature binding) — 30 min, mostly Slack/email.
2. **Confirm SafeChain install path** — where does `src.adapters.model_adapter` come from in this repo? Is it vendored, or is there a pip-installable `safechain` + internal adapter package we need?
3. **Create `.env.example`** and `pyproject.toml` with `google-adk>=1.31.1`, `lkml`, `sqlglot`, `openpyxl`, `pyyaml`, `pydantic>=2`, `requests`, `pytest`, `ruff`, `mypy`, `python-dotenv`.
4. **Execute Session 0** — the adapter must work before Session 6 is viable.
5. **Then Sessions 1-9 in order**, `/clear` between each, commit per session.
