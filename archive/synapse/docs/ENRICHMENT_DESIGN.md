# Enrichment — design blueprint

The enricher, redesigned for scale. Captures the decisions taken before
implementation so the build has a spec.

## Goal (what enrichment is FOR)

Not "describe every column" — at 3,000+ columns that is neither viable nor
valuable (the grounding gate holds evidence-poor columns anyway). Three
deliverables:

1. **Table identity** — grain, subject, business role. Per table.
2. **The entity layer** — tag identifier columns so entities (Customer,
   Account, …) cluster across tables. The north star: anyone can answer
   "what is this table about" by walking the entity graph.
3. **Decoded salience** — the handful of columns whose meaning isn't in
   their name + type: coded/categorical columns, and the columns analysts
   actually filter/group/join on.

Principle: **spend expensive LLM reasoning only where the cheap sources
(MDM, BQ) are silent AND there is evidence to be grounded.** The LLM's
unique value is synthesis, not restating facts we already hold.

## Two modes, one brain

- **Eager (batch, build time)** — enriches the high-value *head* of each
  table.
- **Lazy (on-demand, question time)** — an agent tool fills the *tail*, one
  column at a time, when something actually needs it. The graph's
  lazy-loading layer: read-through cache with a gated LLM fill on miss.

Both use the SAME evidence, the SAME grounding gate, and the SAME capped
provenance. Only the trigger differs.

## Prioritization (a cheap triage score, no LLM)

Rank every column from signals we already ingest, spend the budget
top-down, stop:

| A column earns priority if it's… | Known from |
|---|---|
| an identifier / key | MDM is_primary, BQ PK/FK, **or inferred** (below) |
| coded / categorical | BQ wrote it a `topcount` file (low-card string) |
| analyst-queried | it appears in `sample_queries` from a **human analyst** |
| MDM-undescribed with evidence | MDM silence + usage / derived_logic |
| bare numeric, no usage, no evidence | BQ type + zero signal → **skip** |

Order: **table identity → identifiers → analyst-salient → coded →
MDM-gap → STOP.** The tail stays **grounded by BQ/MDM** (type, range,
null%, PK/FK) — not LLM-narrated. "Not enriched" ≠ "unknown."

## Signal quality (two filters that make the score trustworthy)

**1. Querier provenance.** The salience signal only counts if the querier
is a human making an analytical decision.
- **Analyst** — human corporate email (`first.last@axp.com`). **Votes.**
- **Operational** — `*.gserviceaccount.com`, `svc-*`, `prj-*`, external /
  unknown domains. **Does not vote**, but is **kept and flagged
  `operational`**: it tells us how the table is loaded (lineage, DQ) and
  which tables are mere plumbing vs. hero tables. Nothing discarded —
  everything *sorted*.

**2. Identifiers are inferred, not just declared** (most real tables
declare no PK). Score key-ness from:
- **uniqueness ratio** (`approx_distinct / row_count`) + **null fraction**
  — the dominant signal; ~unique & non-null ⇒ candidate PK.
- **name priors** (`*_id`, `*_no`, `*_xref`, `*_key`, …).
- **analyst join evidence** — columns humans `JOIN … ON`.
- **cross-table co-occurrence** — same column in ≥2 tables ⇒ an entity key.

An **inferred** key is never laundered into a **declared** one: it lands at
`inferred` with its reasons, distinct from a grounded declared PK.

## Provenance / on-demand write-back

On-demand fills are `llm_generated`, **tier-capped at `inferred`**, through
the same grounding gate, into a **labeled overlay** (folded into the next
compile; the canonical MDM+BQ build stays reproducible; the agent layer is
distinct + reversible).

- **Policy A (now):** auto-persist immediately at the capped tier, and
  record each fill as a **steward proposal** in the backend (the review
  trail exists from day one).
- **Policy B (at scale):** gate the write on human confirmation. Flipping
  A→B is a config change, not a rebuild, because the proposal record
  already exists.

The agent is always a **witness, never an authority** — it cannot mint
`grounded` or `human_asserted`. No evidence ⇒ the gate holds it ⇒ the agent
says "not enough to define this" rather than inventing.

## Roadmap (implementation slices)

1. ✅ **Signal-quality primitives** — querier classification + key inference
   (`enrichment/signals.py`).
2. ✅ **Column priority score** — the composite over §Prioritization
   (`enrichment/prioritize.py`).
3. ✅ **Prioritized batch enricher** — analyst-only corpus + select the
   head via the enricher's `skip_columns`; tail grounded-only
   (`pipeline.py --enrich`; `--enrich-all-columns` restores the sweep).
4. ✅ **On-demand tool + overlay + proposal record** — policy A
   (`enrichment/on_demand.py`, `explain_column` MCP tool + agent roster).
