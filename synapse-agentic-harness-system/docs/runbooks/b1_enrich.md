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

The Vertex SVC-ID and project are DIFFERENT from the BQ dry-run ones,
and the resolution honors the PROVEN laptop contract — the same env
the ADK apps and `check_vertex_gemini.py` already ran with against
`prj-d-ea-poc`. If your `.env` still carries the ADK-era variables,
**nothing new is needed** — they resolve as-is:

```
# proven ADK setup (works unchanged):
GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/prj-d-ea-poc.json
GOOGLE_CLOUD_PROJECT=prj-d-ea-poc
GOOGLE_CLOUD_LOCATION=global            # optional — global is default
GEMINI_MODEL=gemini-3.1-pro-preview     # optional — this is default

# silo-first names (win over the above when both are set):
# LUMI_VERTEX_SA_KEY=… VERTEX_PROJECT_ID=… VERTEX_LOCATION=…
# VERTEX_MODEL=…  VERTEX_API_BASE_URL=<PSC endpoint if applicable>
```

Keep the key file OUTSIDE the repo (e.g. `~/.gcp/`) — never commit it.

⚠ **Two keys on one laptop**: BQ and Vertex BOTH fall back to
`GOOGLE_APPLICATION_CREDENTIALS`, and your two SVC-IDs are different.
When both live in the same `.env`, use the explicit names so neither
borrows the other's key:

```
LUMI_BQ_SA_KEY=~/.gcp/<bq-key>.json
LUMI_VERTEX_SA_KEY=~/.gcp/<vertex-key>.json
```
A regional location (e.g. `us-central1`) derives its own regional host
automatically; `global` uses the globally-routed endpoint (the right
default for the Gemini previews).

TLS on the corporate network (the field-proven order): `pip install
truststore` — the OS keychain, where the corporate root actually
lives, engages automatically; else `GEMINI_CA_BUNDLE=<corporate root
pem>` (or `REQUESTS_CA_BUNDLE`); last resort `GEMINI_TLS_INSECURE=1`
(or `BQ_SSL_NO_VERIFY=1`).

**Proxy — the two planes are OPPOSITE and the knobs are separate.**
BQ's PSC endpoint needs the NO_PROXY injection (goes direct); Vertex
rides the corporate proxy untouched — exactly how
`check_vertex_gemini.py` and the ADK apps proved it. Nothing to set
on the standard corporate laptop. Other topologies:
`VERTEX_DISABLE_PROXY=1` (direct-egress network, drop the proxy) ·
`VERTEX_NO_PROXY_GOOGLE=1` (private-DNS/restricted-VIP network,
re-enable the BQ-style injection). The field symptom that pinned
this: googleapis in NO_PROXY sends the Vertex OAuth call direct and
the network blackholes it — a 120s timeout at the auth step.
Thinking: `GEMINI_THINKING_BUDGET=<n>` sets a budget, `0` disables;
endpoints that reject thinking degrade gracefully for the run.

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
