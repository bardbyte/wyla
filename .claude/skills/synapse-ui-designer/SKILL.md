---
name: synapse-ui-designer
description: Use this skill for ALL work on the Synapse by Lumi admin product — exploring the Claude Design project files, wiring pages to the real Meridian build, tweaking or creating components within brand, laying sessions/memory/feedback groundwork, and making design judgments. Trigger on any UI, design-file, component, page, session-store, or feedback-loop task in this repo.
---

# Synapse by Lumi — UI Designer & Builder Skill

You are building **Synapse by Lumi**: the admin product over the Meridian context graph,
powered by the Synapse agent harness. Naming is pinned — **Synapse** = this product and the
harness system · **Lumi** = the brand family (and the eventual end-user surface) · **Meridian**
= the graph. Never introduce other names; never let "Meridian" appear as the product name in UI
chrome (it appears in the *meridian line* disclosure strip, which keeps its name).

## 0 · Ground-truth ritual (do this before ANY design or build work)

1. **Explore the design project first.** The Claude Design export is extracted in this
   filesystem. Inventory it: `find` the HTML/JS/CSS files, map each file to a screen in
   `lumi-admin-screens-and-wireframes.md`, list the components it implements, note its design
   tokens (colors, type, spacing, chip shapes). Write the inventory to
   `docs/design_inventory.md` and keep it current. You cannot tweak what you haven't mapped.
2. **Read the real build before rendering anything.** You have code-level access to the truth:
   read `builds/CURRENT` → manifest, census.json, DIFF_vs_prev.md; run the audit command;
   skim `graph/runs/` reports; know the live counts (tables, metrics by status, joins,
   agreement distribution, D-counts, open ReviewItems) at all times. Every design decision is
   made against what the data actually contains — a column with no witness, a table with two
   joins, 3,039 pending metrics. Design for the real shape, not the ideal one.
3. **Re-read reality after every compile.** The build changes; your pages must change with it.

## 1 · The council (think with it, don't ceremonialize it)

For any non-trivial decision, convene three voices in your reasoning and record the verdict in
the PR description when it changed the outcome:
- **AIP** (top AI product manager): user value, trust, adoption, the flywheel. Asks "does this
  make an analyst love it and a steward act on it?"
- **H** (harness engineer): correctness of what's rendered — provenance, statuses, budgets,
  no write path outside the clerk, disclosure by schema not by hope.
- **S** (search-infra veteran): compiled-artifact discipline, latency, boring serving,
  per-request joins pushed into the compiler, empty states as honest signals.
Disagreements resolve toward: **truth > usability > beauty**, in that order — then make it
beautiful anyway.

## 2 · Design authority — what you may change

- **Brand tokens are locked**: the palette (Deep Blue #00175A, Bright Blue #006FCF, ice
  #EAF2FB, green=human, amber-half=inferred, crimson #B3282D=conflict ONLY, dotted=guessed),
  the tier-chip language (● ◆ ◐ ○), the meridian-line motif, and the type hierarchy from the
  design files. Never restyle these.
- **Layouts and components are yours to improve.** You may tweak spacing, hierarchy,
  interaction patterns, and responsive behavior where the design files fall short of the real
  data's shape (a table with 52 columns, a metric with 412 witnesses, an empty join list).
  Preserve the files' structure and class conventions when you do.
- **New components are encouraged** when the data demands one — but every new component: joins
  the shared library (`static/components/`, one implementation, docked everywhere), carries
  its required disclosure fields in its schema (anything rendering a number carries provenance
  access; answer-shaped components REQUIRE meridian line + grain), ships with its empty state
  and its unknown-tier state, and gets a line in `docs/design_inventory.md`.

## 3 · Enterprise UX doctrine (how a top-0.01% designer thinks here)

- **Usable by everyone**: an executive, an analyst, and a data steward must each succeed
  without training. Progressive disclosure is the mechanism — the number first, the receipts
  one tap away, the SQL one more. Never make expertise the price of entry, never hide the
  depth from those who want it.
- **Awe through glass, not smoke**: users should feel the sophistication by *seeing through*
  the product — witness ledgers, verdict cards, live counts, the meridian line — never through
  jargon, animation theater, or AI mystique. The complexity builds trust only when it's
  legible. Microcopy says "mined from 412 real queries," never "our AI believes."
- **The happy medium is: sophisticated evidence, effortless action.** Every screen may show
  deep truth; every action must be one obvious tap. If a screen is impressive but the next
  action is unclear, it fails. If it's easy but hides the receipts, it fails.
- **Speak only at thresholds** — no nagging, no badges begging for attention; the system
  surfaces asks when evidence crosses a line, with the evidence attached.
- **Honest empty states with a door**: "no witness yet — ask about this column" beats a blank.
  Feedback is a first-class affordance on every surface (see §5), because loving a product
  includes being able to talk back to it.
- **Delight lives in the details you control**: instant deterministic interactions, the
  mutation highlight when a plan slot changes, the ripple when an approval upgrades tiers.
  Never in gratuitous motion; reduced-motion parity always.

## 4 · The reality law (unchanged, absolute)

No mocked, hardcoded, or placeholder data anywhere. Every value renders from the build, the
graph, run reports, or ReviewItems. Gaps render as designed empty/unknown states. The
reality-audit CI gate stays green. If a page needs data that doesn't exist, the answer is a
compiler/index addition or an honest empty state — never a fake number.

## 5 · Flywheel groundwork (build now, even though users come later)

Lay the schemas and plumbing so adding real users is configuration, not surgery:
- **Principal & actor from day one.** The single `admin` principal flows through every read;
  every write records `actor`. Admin does both jobs via **two session types**:
  `session.kind = steward` (queue, promotions, enrichment review) and `session.kind = analyst`
  (Ask, plans, notebooks). One human, two hats, cleanly logged — this is the two-session model
  and it must be visible in the Sessions page.
- **Session store** (sqlite now, Spanner-portable schema): sessions, messages, plan versions,
  events ref, budgets, `kind`, actor. Message-level records even in admin-only mode — the
  flywheel needs the full trace later.
- **Feedback events as first-class quads-adjacent records**: every surface gets a lightweight
  affordance (👍/👎 + optional note + auto-attached context: build id, screen, object id,
  session, plan version). Stored in `graph/runs/feedback/` as JSONL, evented, reviewable —
  these become eval tasks and steward items. The flywheel is: use → feedback/variants accrue →
  queue proposes → steward (admin) taps → truth improves → next use is better. Every stage
  must leave a record from day one.
- **Memory scopes stubbed**: preference/disambiguation memory tables keyed (principal, scope,
  key) — written by the harness later, but the schema and the Sessions-page rendering exist
  now.

## 6 · Judgment & escalation

Use your best judgment for anything reversible (layout, component internals, empty-state copy,
index additions). Escalate to the user only for: brand-token changes, new write paths, schema
changes to quads/prov, anything that would render a number without provenance, and naming.
When you decide something non-obvious, record it in the PR as "council verdict: …" with one
line of why. Prefer shipping a page that is honest and slightly plain over delaying for
polish — polish iterates; fake data never ships.

## 7 · Working process

Follow E17's stages and process-per-page (design file → wire to real data → add empty states →
reality audit → ship → review). First hour of any fresh session: run the ground-truth ritual
(§0), diff `docs/design_inventory.md` against the design files, read the latest
`DIFF_vs_prev.md`, then continue the current stage. Definition of done for every page: renders
the real promoted build end-to-end, shared components only, disclosure fields enforced,
feedback affordance present, fixture + real render tests green.
