# Architecture Decision: Knowledge Catalog Alone vs Knowledge Catalog + Synapse

> **Status.** Recommendation locked. KC + Synapse, NOT KC alone. This doc justifies that.
>
> **Audience.** Decision-makers (eng leadership, principal architect) + the engineer who has to defend the architecture in review.
>
> **Companion to.** `KNOWLEDGE_CATALOG_DEEP_DIVE.md` (the capability inventory).

---

## TL;DR

**KC alone is not sufficient for AmEx's NL→BigQuery agent use case.** It's a magnificent metadata substrate; it does not do confidence-typed reasoning over fused tribal knowledge with steward arbitration. Building Synapse purely to consume KC + nothing else would still leave the four most important properties of the AmEx use case on the floor.

**The right architecture is KC + Synapse together** — KC contributes as the 11th source in our 11-source fusion. We gain Google's managed metadata at zero ingestion cost; we keep our reasoning layer that does the things KC was never designed to do.

**The cost of KC-only:** rebuild four AmEx-specific capabilities ourselves anyway (provenance calibration, corpus tribal knowledge, context-keyed synonyms, steward loop) inside KC's data model, which doesn't natively support them.

**The cost of KC + Synapse:** ~3 days of loader work + permanent dependency on KC being healthy for one source out of eleven (fault-tolerant — Synapse degrades gracefully if KC is down).

---

## 1. The decision space

Four possible architectures. We rule out three.

| Architecture | What it means | Why we reject (if we do) |
|---|---|---|
| **(a) KC alone** | Stop building Synapse; use KC's MCP server + AI features as-is | Rejected — see §3 |
| **(b) Synapse alone, no KC** | Build everything in Synapse; ignore KC | Rejected — see §4 |
| **(c) KC + Synapse loose** | KC and Synapse coexist; Consumer Agent calls both | Rejected — see §5 |
| **(d) KC + Synapse fused** | Synapse ingests KC as source #11; one fused graph behind one agent | **CHOSEN — see §6** |

---

## 2. The four AmEx-specific properties the NL→SQL use case requires

These determine which architecture survives. If your architecture can't deliver all four, it's not sufficient.

### Property 1 — Per-fact multi-source provenance with calibrated confidence

> "When the agent says `cm11` is a Cardmember account ID, the user can see all 4 sources that asserted it (MDM, BQ, corpus, baseline LookML), the calibrated confidence tier (`grounded`), and the evidence event IDs in audit logs."

This is the production-trust contract. Without it, every agent answer is "the LLM said so" — unauditable.

### Property 2 — Tribal knowledge from analyst SQL corpus

> "The 35 real queries against the cardmember table reveal: structural filters (`data_source = 'cornerstone'` in 90%+ of queries), JOIN patterns (cm11 ↔ cm11 in 47 queries), code resolutions (CASE WHEN product_group = '005' THEN 'Platinum'). The agent uses these as first-class facts."

This is the AmEx-specific knowledge. KC's auto-relationship-graph reads job history; it does not extract aggregations / case-whens / structural filters as typed facts the way `lumi_final/lumi/sql_to_context.py` does.

### Property 3 — Context-keyed synonyms

> "`CM` means Cardmember when the query is from a Finance project. `CM` means Communication Module when the query is from a Marketing project. The same surface form, two canonical entities, scoped by business unit + region."

KC's glossary is a flat per-term taxonomy. Synonyms are 1-to-1. Disambiguation by context is not in its data model.

### Property 4 — Steward arbitration loop with negative-training memory

> "When a steward rejects an entity proposal, the rejection reason is stored. Next enrichment run, the LLM sees `previously rejected: X (reason: Y)` in context and avoids repeating. Over time, steward acceptance rate measurably improves."

KC has no concept of "rejected proposals." Its AI features emit metadata; they don't learn from feedback.

---

## 3. Why "KC alone" fails

**Score: 0 of 4 properties.**

| Property | KC alone? | Why |
|---|---|---|
| Per-fact multi-source provenance | ❌ | KC stores aspects with a single source-of-truth model. No multi-source breadth gating. No confidence tier. The closest is the `data-quality-scorecard` aspect, which is one-source-at-a-time. |
| Tribal knowledge from SQL corpus | ❌ | KC's auto-relationships come from job history, not from sqlglot extraction of aggregations / case-whens / filters. We'd have to build our own corpus extractor to feed KC as aspects — at which point we're rebuilding Synapse inside KC. |
| Context-keyed synonyms | ❌ | KC glossary term has one definition. Disambiguation by business_unit / region requires a custom Aspect Type that KC's MCP server doesn't natively reason about. |
| Steward arbitration with memory | ❌ | No first-class concept. Could be hacked via a custom Aspect Type tracking "rejection events" but no LLM prompt machinery to consume them in the next enrichment run. |

**Additional gaps KC alone can't fix:**

- Honest "we don't know" when RLS hides data. KC indexes what it can see; it doesn't surface what it can't.
- AmEx-specific column-name corrections (`fico` → `fico_score`, `card_product_id` → `card_prod_id`) from BQ failed-query logs. KC doesn't extract these.
- The strict "LLM facts cap at `inferred`" rule. KC's AI emits descriptions and trusts them at face value.
- Cardmember-vs-Customer entity distinction (`cm11` is account-level; `cust_xref_id` is customer-level). KC could be told this via a custom Aspect, but the reasoning that flags ambiguous user questions (`"how many cardmembers"` — accounts or customers?) is an AmEx-specific skill.md, not a KC feature.

**KC alone would require building all four properties on top of KC's data model** — and KC's data model doesn't natively support them. We'd be fighting the platform.

---

## 4. Why "Synapse alone, no KC" loses high-value low-cost wins

**Score: 4 of 4 properties (we already have them).** But we leave value on the floor.

What we'd miss by ignoring KC:

| KC capability | What we'd lose by skipping |
|---|---|
| Auto-ingested metadata across BQ + Dataform + Dataproc + Vertex + Looker + Bigtable + Spanner | Multi-system reach beyond what our bq_loader covers |
| Auto-generated descriptions from Gemini in BQ | A free second AI-witness alongside our enrichment (multi-source corroboration boost) |
| Auto-ingested lineage with PII flow tracking | Edge property `propagates_pii: bool` that Google computes for us |
| `data-quality-scorecard` aspect — Auto DQ rules + results | Free DQ rules + dimensions we'd otherwise synthesize ourselves from BQ profile |
| Hierarchical business glossary | Our Synonym model is flat; KC has parent/child term relationships built in |
| Lineage MCP server (when AmEx enables it) | Free cross-BQ-project lineage queries from the Consumer Agent |

**Quantified cost of skipping KC:** ~3 weeks of engineering work to replicate. KC delivers all of this for ~3 days of loader work on our side.

**Quantified risk of skipping KC:** when AmEx eventually mandates KC adoption org-wide (likely), Synapse looks like it duplicates KC rather than augmenting it. Political fragility.

---

## 5. Why "KC + Synapse loose" is the worst of both worlds

In this architecture, the Consumer Agent calls BOTH KC's MCP server AND Synapse's MCP server. The agent has to reconcile answers itself, or worse, the user has to pick which one to trust.

**Why this fails:**

| Problem | Detail |
|---|---|
| Two graphs, one user | The agent sees two different views of the same facts. Reconciliation logic lives in the agent prompt — that's a recipe for hallucinated reconciliation. |
| No multi-source confidence calibration | Each system reports its own confidence; the agent doesn't know how to combine them. |
| Latency × 2 | Every question hits two MCP servers serially. |
| Steward loop is fragmented | Rejections go to which system? Verdicts captured by which system? The loop never closes. |
| Failure modes multiply | When KC is down, the agent partially fails. When Synapse is down, the agent partially fails. Users don't know which to trust today. |

This architecture only makes sense if the two systems serve genuinely orthogonal questions (e.g., one for catalog discovery, one for query generation). For our use case, they overlap heavily — both want to answer "what is this column?"

---

## 6. Why "KC + Synapse fused" wins

**Score: 4 of 4 properties + KC value harvested at low cost + single source-of-truth for the agent.**

The architecture:

```
                   ┌──────────────────────────────────────┐
                   │   KNOWLEDGE CATALOG (managed)         │
                   │   - Auto BQ ingestion                 │
                   │   - Auto AI descriptions              │
                   │   - Auto lineage (BQ + Dataform)      │
                   │   - Data-quality-scorecard            │
                   │   - Glossary hierarchy                │
                   └────────────┬──────────────────────────┘
                                │ REST API (read-only)
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  SYNAPSE — 11-SOURCE FUSED GRAPH                         │
   │                                                          │
   │  Sources:                                                │
   │  1. mdm                   7. usage                       │
   │  2. corpus (sqlglot)      8. dq_engine                   │
   │  3. bq (metadata-only)    9. llm_generated               │
   │  4. baseline_lookml      10. acropedia (when ready)      │
   │  5. metric_catalog       11. knowledge_catalog ← NEW    │
   │  6. glossary                                             │
   │  + table_catalog                                         │
   │                                                          │
   │  Per-fact Provenance envelope tracks which sources       │
   │  contributed; calibrated confidence emerges from         │
   │  multi-source breadth + weight                           │
   └────────────┬─────────────────────────────────────────────┘
                │
                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  ONE MCP SERVER (Synapse) — agents call this             │
   │  Consumer Agent · Ingest Agent · UI · Looker · Cursor    │
   └──────────────────────────────────────────────────────────┘
```

**Why this wins:**

| Property | How KC+Synapse delivers |
|---|---|
| 1. Per-fact multi-source provenance | KC's auto-description becomes another tracked source (`source="knowledge_catalog"`) alongside MDM and our llm_generated. Provenance envelope counts KC as one of the breadth gates for `grounded` tier. |
| 2. Tribal knowledge from corpus | Already in Synapse via `_ingest_corpus`. KC adds independent corroboration on facts the corpus also surfaces. |
| 3. Context-keyed synonyms | Already in Synapse via Synonym node's `business_unit` + `region` properties. KC's flat glossary terms become source corroboration on the canonical_entity field, not the (surface_form × scope) tuple. |
| 4. Steward arbitration | Already in Synapse via Provenance.conflicts + steward verdict UI. KC contributes facts; stewards adjudicate via Synapse. |

**Additional benefits:**

- Single source-of-truth for the agent — one MCP server, one schema, one provenance model.
- Fault tolerance — when KC is down, source #11 is missing from this run's fusion; the other 10 sources still produce a valid graph. The Provenance envelope shows KC absent; the confidence tier reflects the missing breadth.
- Migration path — when AmEx adopts KC org-wide, our architecture already consumes it. We don't have to rebuild.
- Honest competitive positioning — Synapse augments KC; doesn't compete with it. AmEx leadership sees us as the org-specific reasoning layer that sits on the org-wide metadata fabric.

---

## 7. Cost / benefit by architecture

| Architecture | Build cost | Annual maintenance | Risk profile | NL→SQL use case fitness |
|---|---|---|---|---|
| (a) KC alone | 0 (Google built it) | 0 | High — can't deliver 4 properties without hacking KC | ❌ Insufficient |
| (b) Synapse alone | ~10 weeks for what we have today | ~1 dev FTE | Medium — political fragility when KC ramps | ⚠ Adequate but leaves KC value on floor |
| (c) KC + Synapse loose | ~12 weeks (Synapse + reconciliation logic) | ~1.2 dev FTE | Highest — two systems to keep in sync, fragmented steward loop | ❌ Anti-pattern |
| (d) **KC + Synapse fused** | ~10 weeks (Synapse) + 3 days (KC loader) | ~1 dev FTE | Lowest — KC absence is graceful degradation, single agent surface | ✅ Best fit |

The fused architecture is **strictly better** on every axis except absolute lowest build cost (where KC-alone wins by being 0 — but KC-alone fails the fitness test).

---

## 8. Migration path if AmEx pivots toward KC

This matters for political durability of the architecture.

**Scenario A — AmEx adopts KC org-wide, mandates it.** Synapse continues to exist as the AmEx-specific reasoning layer on top of KC. We consume KC entries; we still own Synonym disambiguation, steward loops, LLM enrichment skill rules. Roughly 5% of Synapse code would change (the `kc_loader.py` becomes the primary source instead of one of eleven). We're aligned, not displaced.

**Scenario B — AmEx mandates KC and deprecates custom catalogs.** Same as A. Synapse is not a catalog; it's a reasoning + agent surface on top of a catalog. No deprecation pressure.

**Scenario C — Google deprecates KC features we depend on.** Source #11 goes silent in fusion. Synapse falls back to 10-source fusion. Graceful degradation.

**Scenario D — AmEx doesn't adopt KC.** Synapse uses its 10 native sources. KC loader is dormant. No work wasted because the loader is one of N; not load-bearing.

In every scenario, KC + Synapse fused is the most robust.

---

## 9. The honest concession to KC-alone advocates

KC alone IS sufficient for these use cases:

- Org-wide metadata catalog (discoverability).
- Lineage queries for compliance audits.
- Auto-generated descriptions for cold-start cataloging.
- Generic NL search ("find me datasets about Q3 revenue").

For these, building Synapse would be wasteful overhead.

KC alone is NOT sufficient for our specific use case (NL→BQ SQL generation against the cardmember table with grounded SQL + citations + steward arbitration). For us, Synapse's confidence-typed reasoning layer is the value-add.

**The architectural slogan:** _KC for discovery; Synapse for reasoning._

---

## 10. Decision

**Adopt architecture (d) — KC + Synapse fused.**

- Phase 1: Confirm KC is enabled in `prj-d-ea-poc` (30 min probe)
- Phase 2: Build `kc_loader.py` + add `knowledge_catalog` as source #11 (~3 days)
- Phase 3: Update enrichment skill.md to add KC to authoritative-source list (1 hour)
- Phase 4: Update Streamlit UI per-source view to render KC alongside the other 10 (half day)

**Total cost: ~4 days.**

**Value delivered:** auto-descriptions × 2, auto-lineage with PII flow, free DQ scorecards, glossary hierarchy. All four are signals we'd otherwise have to build or do without.

**Risk: low.** KC degradation = source #11 silent = 10-source graph still valid.

---

## 11. Open question still on the table

**Is KC actually enabled in `prj-d-ea-poc` for the `axp-lumi.dw` dataset?** Until verified, the loader is theoretical. The probe is in `KNOWLEDGE_CATALOG_DEEP_DIVE.md §5`. Action: run it on the work laptop, share the output.

If KC is NOT enabled, the architecture choice doesn't change — we still want it fused for the day it IS enabled. The loader is built as a dormant capability; turns on when KC's index covers our table.

---

## Appendix A — references

- `KNOWLEDGE_CATALOG_DEEP_DIVE.md` — capability inventory
- `STRESS_TEST_REARCHITECTURE.md` — the ai-systems-engineer's critique of the broader rearchitecture
- `synapse/synapse/graph/store.py` — the Provenance envelope and SOURCE_WEIGHTS
- `synapse/docs/MCP_SERVER_SPEC.md` — our MCP tool surface
- `lumi_final/lumi/sql_to_context.py` — the sqlglot corpus extractor

---

## Appendix B — explicit anti-patterns to avoid

When implementing the loader, do NOT:

1. **Treat KC as the source of truth.** It's one of 11 witnesses. The Provenance envelope handles the rest.
2. **Bypass Synapse's confidence calibration for KC facts.** KC writes get the same multi-source-breadth gating as any other source.
3. **Build a separate MCP server for KC.** One MCP server (Synapse's) exposes the fused graph including KC's contributions.
4. **Auto-promote KC facts to `grounded` because Google said so.** KC is weight 4 (= BQ). It corroborates; it doesn't dictate.
5. **Let KC's flat glossary override Synapse's context-keyed synonyms.** Our Synonym model is richer; KC terms become source corroboration on the canonical_entity, not replacements for the (surface_form, business_unit, region) tuple.
6. **Forget the conflicts surface.** When KC and MDM disagree on PII classification, both render with their respective sources; the Provenance.conflicts entry is generated for steward review.
