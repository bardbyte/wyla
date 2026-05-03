# LUMI — LookML Understanding and Metric Intelligence

Enriches LookML views from SQL queries + MDM metadata + Gemini reasoning.
Output: enriched `.view.lkml`, `.model.lkml`, metric catalog, filter catalog,
and golden NL questions for Radix.

## How it works

Two-phase pipeline with a human review gate in the middle:

```
Phase 1 (autonomous):
  SQL → [Parse+Discover] → [Stage+Plan] → review_queue/REVIEW.md → STOP

Human reviews REVIEW.md. Approves/modifies plans.

Phase 2 (after approval):
  Approved plans → [Enrich] → [Validate+SQL Reconstruct] → [Publish]
```

Description-only changes auto-approve. Structural changes (new PK,
derived tables, type changes) wait for human.

## How to run

```bash
python -m lumi --input data/gold_queries/     # Phase 1: plan
# review review_queue/REVIEW.md
python -m lumi --execute                      # Phase 2: execute

cat lumi_status.md | head -20                 # quick status (phone)
cat review_queue/REVIEW.md                    # review plans

pytest tests/ -v                              # tests (offline)
pytest tests/ -v --run-integration            # tests (work laptop)
```

## Observability

`lumi_status.md` updates after every stage. Three zoom levels:
- **Top**: one status line per stage (5 sec glance, phone-friendly)
- **Middle**: per-table progress table (plan/enrich/SQL check status)
- **Bottom**: gate results with pass/fail per check

## Model

Vertex AI direct. **Not SafeChain.** Model `gemini-3.1-pro-preview`,
project `prj-d-ea-poc`, location `global`. Temperature 0.
`source agent_test/setup_vertex_env.sh ~/Downloads/key.json`

## Rules

1. SQL parsing = sqlglot. LookML parsing = lkml. Never regex. Never LLM.
2. Temperature 0. LLM never sees raw binary. Tools parse first.
3. Type hints everywhere. Docstrings are tool descriptions for Gemini.
4. Tool functions return `dict` with `status` + `error` fields.
5. pathlib for paths. logging not print. Line length 99.
6. Merge into existing LookML — never regenerate. Additive only.
7. LookML completeness rules: see `.claude/skills/lookml/SKILL.md` (single source).
8. Tests first. Implementation second. Full suite after every change.
9. Structural changes need human approval. Description-only auto-approves.

## Don'ts

- SafeChain (removed), `gemini-3-pro-preview` (discontinued), `us-central1` (404s)
- MDM response as dict (array — peel `[0]`), skip truststore (TLS breaks)
- Regenerate views (merge only), valid values in descriptions (filter_catalog)
- Skip primary_key (silent wrong numbers), alphabetical joins (topological only)
- Plain dimension for dates (dimension_group only), skip always_filter on large tables
- Cross-package imports (relative only), file-attach in adk web (path in chat)

## Status

| What | Status |
|------|--------|
| Vertex/Gemini, GitHub PAT, MDM API, ADK | ✓ All verified |
| LUMI pipeline | ✗ Start Session 1 |

## Current session

Session 1: Parse + Discover
