# LUMI

LookML Understanding & Metric Intelligence — a multi-agent pipeline that enriches LookML views from gold NL→SQL queries, MDM business metadata, and LLM reasoning, so a downstream NL2SQL agent can answer **any** question about those views.

> Status: rebuild in progress. Previous full implementation lives under [`archive/`](./archive) for reference and parts salvage.

---

## What this will do

Take three inputs:

1. **137 gold queries** (Excel) — `user_prompt` + `expected_sql`
2. **30 LookML view files** (internal Git repo)
3. **30 MDM entity records** (internal API) — canonical names, definitions, synonyms

Produce one output:

- **Enriched LookML** where every field of every view has rich descriptions, MDM-backed labels, user-vocabulary tags, generated measures/dimensions for SQL patterns the existing LookML doesn't cover, and a deterministic coverage report proving each gold query is resolvable.

---

## Plan

Build it back step by step, this time with each session a small, commit-sized increment:

1. Project skeleton + `CLAUDE.md` + `pyproject.toml`
2. Pydantic schemas (config, query, view, report) + tests
3. `parse_excel_to_json` (sqlglot) + `parse_lookml_file` (lkml) + tests
4. `clone_and_parse_views` + `query_mdm_api` (cache + fallback) + tests
5. `group_queries_by_view` + `extract_join_graphs` + `validate_coverage` + tests
6. SafeChain → ADK adapter (`SafeChainLlm`)
7. `DataLoader` CustomAgent
8. `ViewEnricher` LlmAgent + prompt
9. `ExploreBuilder` + `VocabChecker` + `Aggregator` + `Validator`
10. Root `agent.py` composition + `python -m lumi` entry point
11. Preflight scripts + `BOOTSTRAP.md` + first real run

Each step: write, test, commit, push.

---

## Reference

- **`archive/`** — the previous end-to-end build (40 tests green, mypy strict clean). Read it for the design decisions; copy from it deliberately when rebuilding.
- **`archive/design-doc-for-lookml-enrichment-pipeline.md`** — original architecture spec.
- **`archive/docs/README.md`** — SafeChain LLM access patterns (Amex-internal).
- **`archive/docs/ADK_INTEGRATION.md`** — wrapping SafeChain inside Google ADK.

---

## Stack

Python 3.11+ · Google ADK 1.31 · SafeChain (Gemini 2.5 Pro + Flash) · `lkml` · `sqlglot` · `pydantic` · `openpyxl`

LLM access goes through SafeChain only — no direct `openai`/`google.generativeai`/`vertexai` imports anywhere.
