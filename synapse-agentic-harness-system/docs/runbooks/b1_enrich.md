# B1 — the enrichment loop (E13)

> **Prompt/grader history.** `b1.1` halted at 56% on the real laptop
> (19/34): 8 misses were house-dialect paraphrases (the model answered
> in literal SQL English — "Net Transaction Amount" for "Spend"),
> ~6 dropped a discriminating qualifier, and the token grader PASSED a
> Card Present / Card Not Present swap in both directions. `b1.2` adds
> the HOUSE_STYLE block to both prompts; grader `v1.1` adds a negation
> veto (strictly harder), per-item `share` margins, and per-item
> `context_leak` so a pass can be trusted (a leaky context measures
> leakage, not recovery). Note honestly: iterating the prompt against
> exam feedback adds mild optimistic bias to A5 — the exam names are
> never embedded, and the true audit is B2 steward review.
> Re-enrichment is version-aware: a metric enriched under an older
> prompt version is re-planned; append-only keeps every draft.
>
> `b1.2` reached 20/34 (59% — halt by one case) with the dialect
> bucket converted to exact matches and the veto correctly failing a
> repeated CP/CNP swap at share 1.0. The remaining misses: the
> `- Method` suffix family (base names now right, suffix dropped),
> the invisible-filter family (the page/channel literal lives in
> `common_filters`, which the context never carried — OUR bug), and
> ' / ' compound answer-key names halving the share. `b1.3` feeds
> `common_filters` into both prompts (compile rows now carry them),
> strengthens the suffix rule, and softens the CM clause (it caused
> one overcorrection); grader `v1.2` credits either whole part of a
> spaced-slash compound name (answer-key normalization, not synonym
> credit — tight slashes like 'Local/Foreign' never split).
>
> `b1.3` OPENED THE GATE: 21/34 (62%, tier item) — first real writes
> (23 metrics + 21 concepts, 0 collisions). Churn was high (5 fixed /
> 4 regressed) and a context probe on the misses found three things.
> (1) Only 1/34 blind items carried any filters — `common_filters`
> exists only on the mined plane; the certified metrics' scoping
> WHERE lives in their referenced full SQL, not the canonical
> expression, so five cases (both 'View All Cards', Paid Search,
> campaign redeemers, eligible merchants) were unwinnable from the
> shown context — the model's "wrong" answers were faithful to the
> evidence (one catalog SQL even aliases a Sessions metric
> `monthly_visits` — a B2 steward finding, not a model error).
> (2) The vocab shelf fed poison: SQL keywords (CASE, AS) and
> 2-letter column fragments (cr/dr = credit/debit, not Customer
> Reference / Disaster recovery) matched unrelated acronyms — and
> caused one regression directly (TOT/SBS expansions). (3) The CP/CNP
> twins differ on POS cardholder-present codes where '0' means
> PRESENT — the model assumed 0 = absent. `b1.4` therefore: vocab
> hygiene in `_vocab_for` (keyword blocklist, 3-letter minimum,
> trailing-s stem lookup, Atlas-phrase-first ordering), suffix
> derivation by EXPANDING the discriminating column's name plus a
> column-name dialect table, a register clause (spell out population
> nouns, keep catalog acronyms, never drop a per-X denominator), and
> the POS encoding fact. Honesty note, doubly so now: b1.4 is tuned
> against this exam's residuals — treat the A5 rate as an upper
> bound; B2 steward review is the audit. b1.4 is the LAST prompt
> iteration before scale regardless of tier.

> **Pinned follow-up (not built): observed filters from referenced
> SQL.** The graph already stores each certified metric's full
> referenced query (`doc:referenced_sql_*`, PR #89). Mining its
> top-level WHERE conjuncts with string literals into an
> `observed_filters` row field would make the five
> unwinnable-from-expression exam cases (and every future mined
> metric on those tables) name their page/channel/campaign scope.
> Loader/compile patch, sqlglot in-silo — do it as its own change,
> measured by its own exam delta.

> **B1.2 wave (pinned, not yet built):** aliases + anti-aliases
> (merchant-vs-cardmember-vs-issuer country first), the geography
> concept family, and an edit-distance NAME-collision check against
> certified labels — the question-duplication tripwire complements
> but does not replace it.

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

The run narrates itself live — one line per model call
(`blind 12/34 · ✓ share 0.83 · 8.4s · metric:…`,
`metric 7/25 · ✓ wrote question+grain (conf 0.85) · 11.2s · …`),
`[vertex]` lines for retries/backoffs/MAX_TOKENS self-heals, a
`resuming: N/M already checkpointed` line on resume, and a closing
`usage:` token line. Blind lines show the model's prediction on a
miss but NEVER the withheld true name — the terminal stays as blind
as the exam. Every line is also recorded in `<out>/events.jsonl`.

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
