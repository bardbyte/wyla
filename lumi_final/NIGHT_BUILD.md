# Night build — Sessions 2 through 7

**Branch:** `feat/sessions-2-through-7-pipeline`
**Started from:** `fix/create-table-aliases-and-empty-cells` (722f481)
**Goal:** Wake up to a fully wired, fully tested pipeline with a clear morning test plan.

## What's being built

| Session | Stage | Deliverable |
|---|---|---|
| 2 | **Stage** (prioritize tables) | `lumi/agents/stage.py` — LlmAgent ranks tables by query frequency × MDM coverage × business criticality. Guarded by `check_staging`. |
| 3 | **Plan** (per-table enrichment plan) | `lumi/agents/plan.py` — LlmAgent proposes dimensions/measures/risks per table. Guarded by `check_planning`. |
| 4 | **Human gate** | `lumi/review_gate.py` — generates `review_queue/REVIEW.md`, ingests human edits/approvals back into `PlanApproval`s. |
| 5 | **Enrich** (LookML generation) | `lumi/agents/enrich.py` — Gemini per table; merges into existing baseline `.view.lkml`, never regenerates. |
| 6 | **Validate** (coverage + SQL reconstruction) | `lumi/agents/validate.py` — deterministic; for each gold query checks the generated LookML can answer it. |
| 7 | **Publish** | `lumi/agents/publish.py` — writes `output/views/*.view.lkml`, `output/models/*.model.lkml`, `metric_catalog.json`, `golden_questions.json`. |

Plus:
- `lumi/pipeline.py` — `SequentialAgent` wiring the whole thing.
- `lumi/observability.py` — three-zoom `lumi_status.md` updated after every stage.
- `lumi/prompts/*.py` — temperature-0 prompt templates per agent.
- `tests/` — mock-LLM tests for every agent + integration tests.
- `MORNING_TESTING_PLAN.md` — ordered checklist for tomorrow.

## Honest caveats

- **No real Gemini calls verified.** Every agent has a real LLM body, but I cannot run Vertex from this machine. Mock-LLM tests verify the data flow; output quality is unverified until you run with real Gemini against real `session1_output.json` tomorrow.
- **LookML output quality will need iteration.** First Enrich run on one table may need prompt tuning. The pipeline supports `--single-table` for exactly this iteration loop.
- **No real BQ access.** Pipeline treats `data/bq_cache/` as optional — runs without it, just produces less rich `allowed_values` on enums.

## Morning testing plan

See `MORNING_TESTING_PLAN.md` (written after the build completes).
