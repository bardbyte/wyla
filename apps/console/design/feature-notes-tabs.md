# Feature notes — new Radix tabs (user-requested, 2026-07-07)

Status: NOTES ONLY — no implementation yet. Captured for the v3 mock /
React IA. Some features ship real, some ship as labeled synthetic
previews ("coming soon") — see the design rule at the bottom.

## Top-level navigation becomes tabs

    Inquiries (current v2 workspace) · Data Products · Metrics · Knowledge Graph

## Tab: Data Products

1. **Search approved data products** — search the org's data products
   (MDM dataset search is the real backend; it already works via the
   crawler's schema/filter endpoint).
2. **Add → re-sync the graph** — adding an approved product triggers a
   graph sync for it (real machinery exists: manifest entry → pipeline
   stages for that table → top-up enrichment → entity refresh). Demo
   ships this as a **labeled preview**: the flow animates the real
   pipeline output (spine matrix → readiness row appearing), the
   trigger button is "coming soon."
3. **My domain's products** — list of data products in the user's
   domain that are proven + functional. The "proven/functional" badge
   IS the context-readiness scorecard row (cols · meaning% · relations
   · governance · tier) — real data, per product.

## Tab: Metrics

1. **Define a metric → viability check** — user drafts a metric (name,
   table, aggregation-ish); the graph answers: exact canonical match /
   near-duplicate (same table + aggregation family) / genuinely new.
   Read-side is real today: 165 Metric nodes from skills, synonyms,
   find_columns_for_concept.
2. **Canonical term + sources** — for any metric/term: what the
   canonical form is, who defines it (skill / steward / corpus), with
   the witness panel showing per-source support. This is
   resolve_synonym + explain_confidence, surfaced.
3. **Submit for canonicalization** — steward-gated write path (same
   pattern as entities.yaml, witness #6). Ships as labeled preview.

## Tab: Knowledge Graph

- Purpose: give everyone the **mental image** of the graph in one
  curated thread — NOT a full graph explorer.
- One storyline view: **Entity (e.g. Account) → IDENTIFIES columns in
  two tables → EQUIVALENT_TO join edge → Metric → DEFINED_BY Skill**,
  with provenance chips on each hop. Two tables max, fixed layout.
- Real backend exists: synapse/graph/viz.py (neighborhood/ego/lineage
  pyvis renderers) + the now-real Entity nodes. Curation of WHICH
  thread matters more than the tech.
- Secondary: tier legend + node/edge counts as ambient credibility.

## Design rule for synthetic features (binding)

Radix is a trust product — the demo must not blur real and fake.
Every synthetic feature wears an explicit **"Preview — ships in v2"**
badge and a designed coming-soon state; every real feature renders
live data. Truth-of-state applies to the roadmap too.
