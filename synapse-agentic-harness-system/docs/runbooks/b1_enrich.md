# B1 — the enrichment loop (E13)

The graph knows WHAT (canonical SQL, tables, support). The certified
catalogs say WHY for only ~41 metrics. B1 asks the Vertex model to
draft the why — mined-metric questions and grains, thin-concept
descriptions — under three non-negotiables:

1. **Certified is never clobbered.** Enrichment writes only the
   `*_enriched` keys, only where the catalog value is blank; compile
   prefers the catalog value forever, and every enriched line renders
   `[prov:llm_enriched·unreviewed]`.
2. **The A5 blind gate runs before any write.** Names are stripped
   from the certified/pending metrics; the model must recover them
   from expression + card context alone. ≥80% → batch-tier review;
   60–80% → item-review tier; <60% → the run HALTS with nothing
   written (iterate the prompt, never the graph).
3. **Everything is witnessed.** `witness: llm_enriched`, the model id
   in evidence, ordinary append-only quads a steward can retract (B2).

## 0. Environment (once) — Vertex is NOT the BQ contract

The Vertex SVC-ID and project are DIFFERENT from the BQ dry-run ones.
Add to `synapse-agentic-harness-system/.env` (alongside the BQ vars —
nothing is borrowed between the two):

```
LUMI_VERTEX_SA_KEY=/path/to/vertex-key.json
VERTEX_PROJECT_ID=<the Vertex project id>
VERTEX_LOCATION=us-central1
VERTEX_MODEL=<the model id your project serves, e.g. gemini-2.5-pro>
# only if your enterprise routes Vertex through a PSC endpoint:
# VERTEX_API_BASE_URL=https://<psc-host>
```

Proxy/TLS knobs are shared with BQ (`BQ_DISABLE_PROXY=1`,
`REQUESTS_CA_BUNDLE=…`, last-resort `BQ_SSL_NO_VERIFY=1`).

```bash
python scripts/vertex_check.py             # config + token
python scripts/vertex_check.py --generate  # one tiny model call
```

Exit 0 = go · 3 = env problem (message names the variable) · 1 = the
model refused (message explains — 404 = wrong model/location, 403 =
SVC-ID lacks aiplatform permissions).

## 1. Plan first — zero tokens

```bash
python scripts/laptop.py enrich \
  --graph graph --builds builds \
  --plan --limit 200 \
  --out graph/runs/b1_plan --plain
# read graph/runs/b1_plan/plan.jsonl — exactly what WOULD be asked
```

## 2. Smoke batch, then scale

```bash
python scripts/laptop.py enrich \
  --graph graph --builds builds \
  --limit 25 \
  --out graph/runs/b1_smoke --plain
# gates: blind_gate_a5 (tier + rate on the certified set) ·
#        enrich_writes (written / collisions→review / invalid_json)
# read graph/runs/b1_smoke/blind_results.jsonl — per-item recovery
```

Interrupted or rate-limited? Re-run the same command — the checkpoint
resumes; `--fresh` restarts the batch deliberately. Then raise
`--limit` (the full backlog is ~3,000 metric items; run it in slices).

## 3. Recompile — enrichment reaches the serving layer

```bash
python scripts/laptop.py compile \
  --graph graph --builds builds \
  --out graph/runs/b1_compile --json
# builds/<id>/DIFF_vs_prev.md is the acceptance record: questions and
# grains filled, concept cards gaining meaning lines — all marked
# unreviewed until B2's steward pass
```

## What the report means

`enrich_report.json`: `blind` (n / recovered / rate / tier) ·
`metrics_enriched` / `concepts_enriched` (writes) · `collisions`
(enriched question duplicated a certified one → a
ReviewItem(kind=metric_conflict), never a write — a variant signal for
a steward) · `invalid_json` (model output that failed the strict
schema — counted, skipped) · `usage` (calls + token counts).
