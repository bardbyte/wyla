# Synapse — Information Architecture & UX Spec

**Audience:** the engineer implementing the next pass on `synapse/scripts/synapse_ui.py`.
**Status:** opinionated proposal. Every section ends with a concrete delta against the
current UI.
**North star:** a senior data engineer answers one of six canonical questions in
**under 10 seconds** without scrolling, and reaches per-fact 7-source provenance in
**one click**.

---

## 0. Operating premise

The current UI is a faithful render of `inspect_table()`. That is the bug. The dict
shape is a *data substrate*, not an *information architecture*. Rendering every key
with equal weight is the engineer's instinct ("show the user everything we know") and
the designer's failure ("the user has to triage everything we know").

Tufte, *Visual Display*: ink should be proportional to information value. Today every
section gets a `####` header, an emoji, and equal vertical real estate. PII status
gets the same visual weight as the table's business name. That is not a design — that
is a JSON dump in a Streamlit costume.

We fix this by (a) committing to a dominant metaphor, (b) ordering the page by
question-frequency, not data-shape, and (c) using progressive disclosure aggressively.

---

## 1. The dominant metaphor

> **Synapse is the X-ray for a table.**
> Other tools show you the table's surface (Atlan: a catalog page; Dataplex: a
> properties sheet; dbt docs: a YAML render). Synapse shows you the table's
> **skeleton, organs, and the doctors who diagnosed each one** — fused into one
> view, with every diagnosis traceable back to the doctor who made it.

One sentence: **"Synapse tells you what this table is, how much to trust it, and
which of 10 independent witnesses said so."**

Compare:

| Product           | Dominant metaphor                | What it shows you             |
| ----------------- | -------------------------------- | ----------------------------- |
| Datadog APM       | The patient's vital signs        | Latency, errors, traces       |
| Datadog Logs      | The transcript                   | What happened, line by line   |
| Datadog Infra     | The body                         | Hosts, containers, processes  |
| dbt docs          | The recipe book                  | Models, sources, lineage      |
| Atlan             | The library catalog              | Title, author, Dewey decimal  |
| **Synapse**       | **The X-ray with annotated provenance** | **Structure + diagnosis + which doctor signed off** |

The metaphor must be *visible in the chrome*. Today the chrome says "selector + 12
equally-weighted sections." The chrome should say "diagnosis at top, anatomy below,
witnesses on demand."

**Delta:** rename the page header from "Synapse — 7-source semantic graph" (which is
internal jargon) to **"Synapse · `table_name`"** with a tagline "What is this table,
and how much do we trust it?"

---

## 2. The six questions and their shortest paths

Restating the canonical questions, ranked by frequency (estimate from the user
context — power users in BQ console / Looker / dbt):

| #  | Question                                                            | Frequency | Where on page  | Latency target |
| -- | ------------------------------------------------------------------- | --------- | -------------- | -------------- |
| Q1 | Can I trust this table for my dashboard?                            | Very high | **Hero**       | <2 sec         |
| Q2 | What's the right column to filter by data_source?                   | High      | Columns        | <5 sec         |
| Q3 | What metrics live here and what do they mean?                       | High      | Metrics        | <5 sec         |
| Q4 | How is this column populated — what's upstream?                     | Medium    | Lineage / Col  | <10 sec        |
| Q5 | Who else queries this — is my use case canonical?                   | Medium    | Usage          | <10 sec        |
| Q6 | Why does the graph think `cm11` is a cardmember ID?                 | Low-but-critical | Per-fact drill-down | 1 click |

**Implied reading order, top to bottom:**

```
┌───────────────────────────────────────────────────────────────┐
│ 1. TRUST HEADER      ← Q1 (and provides a "why" link to Q6)   │
│ 2. WHAT IS THIS      ← business name, FQN, asset_kind, owner  │
│ 3. COLUMNS           ← Q2 (filterable, with confidence column)│
│ 4. METRICS           ← Q3                                     │
│ 5. LINEAGE & RELATED ← Q4 + lateral discovery                 │
│ 6. USAGE             ← Q5                                     │
│ 7. GOVERNANCE & DQ   ← collapsed by default; chip in header   │
│ 8. CODE RESOLUTIONS  ← collapsed by default                   │
│ 9. PER-SOURCE PANEL  ← collapsed by default; reachable via    │
│                        any fact's drill-down                  │
│ 10. RAW JSON         ← debug only; off by default             │
└───────────────────────────────────────────────────────────────┘
```

The current UI's order is: identity, per-source-breakdown (huge), columns, metrics,
lineage, related, usage, governance, DQ, code resolutions, raw JSON. The per-source
panel is rendered *second*, before the user has even seen the columns. That panel is
the **drill-down**, not the lead. Burying it is correct; lifting it is wrong.

**Delta:**
1. Move per-source panel from `#2` to `#9` (collapsed).
2. Make per-source panel reachable from any badge click (covered in §5).
3. Replace the order in `main()` accordingly.

---

## 3. Progressive disclosure: L1 / L2 / L3

The current UI is 100% L1. That is the problem.

| Layer | Definition                              | Examples on this page                                                                 |
| ----- | --------------------------------------- | ------------------------------------------------------------------------------------- |
| **L1** | Always visible, above the fold          | Trust header, table name, FQN, owner, column count, key columns, "view all" entrypoints |
| **L2** | One click to reveal                     | Full column list, all metrics, lineage graph, usage detail, DQ rules                  |
| **L3** | Deep dive — drill-down or modal         | Per-fact 7-source provenance, individual DQ rule SQL, individual user's query log     |

**Concrete L1 spec (what stays above the fold on a 1440×900 screen):**

```
┌──────────────────────────────────────────────────────────────┐
│  TRUST HEADER (see §4)                                       │  ~120px
├──────────────────────────────────────────────────────────────┤
│  Business name · FQN · owner · asset_kind · tags             │  ~60px
│  description (truncated at 2 lines, "more" link)             │
├──────────────────────────────────────────────────────────────┤
│  KEY COLUMNS: PK · partition · top-3 most-queried            │  ~140px
│  (small cards, each clickable into the full column drill)    │
├──────────────────────────────────────────────────────────────┤
│  "Show all 193 columns" · "Show 8 metrics" · "Show lineage"  │  ~40px
│  · "Show usage" · "Show 14 DQ rules" — chips, not headers    │
└──────────────────────────────────────────────────────────────┘
```

Everything below those chips is L2, revealed on click via `st.expander` or a tab.

**Delta:** today `_render_columns()` renders all 193 columns immediately. That is the
single biggest IA failure on the page. Replace with: key-columns strip in L1, full
table in L2 expander, with a filter input (name contains, confidence ≥, has PII,
is_join_key, etc.).

**Don Norman, *Design of Everyday Things*, ch. 5:** "When functions are visible but
unused, they impose cognitive load. When functions are hidden but easily summoned,
they impose none." Translate: chips > headers when the section is rarely the user's
target.

---

## 4. The Trust Header (Q1, the hero)

This is the single most important element on the page. A senior data engineer should
read it in **two saccades** and decide whether to keep going.

### 4.1 What goes in it

Five glyphs in a horizontal strip, left-to-right by descending decision-impact:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  custins_customer_insights_cardmember                                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │ TRUST   │  │ FRESH   │  │ DQ      │  │ PII     │  │ USAGE   │            │
│  │  ●●●●○  │  │  4.2 h  │  │  12/14  │  │  3 cols │  │  847/wk │            │
│  │ grounded│  │  green  │  │  passing│  │  amber  │  │  ▁▂▄▆▇▆▄│            │
│  │ 6 of 10 │  │         │  │         │  │         │  │  trend  │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│   why? →       why? →       why? →       why? →       why? →                │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Tile      | Primary glyph                          | Secondary line                | "why?" → opens                |
| --------- | -------------------------------------- | ----------------------------- | ----------------------------- |
| **TRUST** | 5-dot confidence (●●●●○ for grounded)  | "6 of 10 sources agree"       | Per-source panel for table-level facts |
| **FRESH** | Hours since last_modified, traffic-light color | absolute timestamp          | Lineage upstream + last load time |
| **DQ**    | passing / total rules                  | last run timestamp            | DQ rules expander, sorted fail-first |
| **PII**   | column count + taxonomy chip           | owner team                    | Governance section            |
| **USAGE** | queries/week + 12-week sparkline       | top team                      | Usage section                 |

### 4.2 Design rationale

- **Five tiles, no more.** Hick's Law: choice time is logarithmic in option count.
  Anything beyond 5–7 at this level forces sequential reading.
- **Each tile is self-contained but linked.** The "why?" affordance under each tile
  is the drill-down to the relevant L2 section. This is the same pattern as Datadog's
  service-overview tiles (latency / errors / requests / saturation) — glance value +
  drill-down.
- **No metric is bare; each carries a unit and a comparison.** "4.2 h" not "4.2."
  "6 of 10 sources" not "6". "847/wk · ▁▂▄▆▇▆▄" not "847". Tufte: a number without a
  comparison is half a number.
- **Color = ordinal, not decorative.** Green / amber / red ONLY for ordered states
  (freshness, DQ pass rate, PII risk). Confidence uses dot-fill, not color, so it
  reads accessibly and doesn't compete with the DQ traffic light.

### 4.3 Failure mode the hero must prevent

A user lands on this page mid-incident: "is this table broken?" Today they have to
scroll past per-source, past 193 columns, past metrics, past lineage to find
freshness and DQ. That is malpractice. The hero must answer "is it broken?" in two
saccades.

**Delta:** today `_render_identity()` renders the tier badge in the top-right corner
and nothing else. Replace with the five-tile hero. The current `_tier_badge` becomes
the inner glyph of the TRUST tile only.

---

## 5. The provenance drill-down (Q6) — the hero feature

This is the differentiator. Per the brief: **"the click-to-expand-into-10-sources
panel."** Today it lives at page position #2 as a list of 10 expanders. That is wrong
in three ways:

1. It treats the source as the entry point ("here are the 10 sources, expand one to
   see facts"). Users think the opposite way: "I see a fact, I want to know who told
   us."
2. It forces the user to scan 10 expanders to find which source confirmed *the fact
   they're looking at*. That's O(N) cognition for an O(1) answer.
3. It is at page position #2, which means the user has to traverse it to reach the
   data they came for.

### 5.1 The corrected interaction

**Inversion: every fact is the drill-down trigger. The source panel is the
destination, scoped to that fact.**

Concretely: anywhere on the page we render a fact (column description, metric
formula, business_name, partition_field, PII flag, code resolution, JOIN
equivalence), the fact has a small **provenance chip** next to it:

```
custins_customer_insights_cardmember                          [●●●●○ 6/10 ⓘ]
                                                                       ↑
                                                                       click
```

Clicking `ⓘ` opens a **right-side slide-over panel** (not a modal, not an inline
expand) that shows:

```
┌──────────────────────────────────────────┐
│  WHY DO WE BELIEVE THIS?                 │
│  Fact: table.business_name =             │
│    "Customer Insights — Cardmember"      │
│  Confidence: grounded · score 0.82       │
├──────────────────────────────────────────┤
│  Sources that contributed (3 of 10):     │
│                                          │
│  ✓ MDM            weight 3 · 1 event     │
│    "business_name" from MDM table        │
│    schema, last seen 2026-05-30 14:22Z   │
│    [view raw MDM payload →]              │
│                                          │
│  ✓ TABLE_CATALOG  weight 3 · 1 event     │
│    "business_name" matches catalog       │
│    entry id=7842                         │
│    [view catalog entry →]                │
│                                          │
│  ✓ CORPUS         weight 1 · 14 events   │
│    Phrase appears in 14 SQL comments     │
│    [view queries →]                      │
│                                          │
│  Sources that did NOT contribute:        │
│    bq, baseline_lookml, glossary,        │
│    metric_catalog, usage, dq_engine,     │
│    llm_generated                         │
│                                          │
│  Conflicts: none                         │
└──────────────────────────────────────────┘
```

### 5.2 Why a slide-over, not a modal, not inline, not hover

- **Hover** loses the panel on mouse-out. Power users want to scan and reference; we
  must let them keep the panel open while inspecting other facts.
- **Inline expand** pushes content below it down 200+ pixels. Disastrous for a long
  page; the user loses spatial reference.
- **Modal** is the worst — it blocks the rest of the page and forces close-before-
  next-action. Atlan does this and it's painful.
- **Right-side slide-over** (think Linear's issue panel, GitHub's PR file panel, or
  Stripe's payment detail) keeps the page visible on the left, the panel persistent on
  the right, and supports rapid scan: click chip A, read panel, click chip B, panel
  updates in place. This is the only pattern that supports the *comparison* use case
  ("does the corpus agree with MDM on this field?").

### 5.3 Cognitive load math

- Old pattern: 10 sources × 1 expand × ~5 facts per source = 50 cognitive units to
  find "who told us about business_name?"
- New pattern: 1 click on the fact → panel shows the answer scoped to that fact. ~2
  units.

That's a 25× reduction in cognitive load for the signature interaction. This is the
whole UX of the product.

### 5.4 Implementation note

Streamlit doesn't natively support a persistent right slide-over. Options, ranked:

1. **`st.dialog` (Streamlit 1.31+)** — modal, less ideal but available now. Use for
   v1 with a clear plan to migrate.
2. **Custom component** (React + Streamlit Components SDK) for the real slide-over.
   Worth it for the hero feature.
3. **Two-column layout (`st.columns([2, 1])`)** with the right column reserved as a
   "provenance pane" that updates on click. Pragmatic v1.5.

Recommend option 3 for v1.5 — no JS, real persistence, scannable.

**Delta:** remove the existing `_render_per_source_view()` from page position 2.
Replace with: (a) provenance chip on every fact, (b) right-pane provenance
inspector, (c) keep the existing per-source breakdown reachable from a "show all
sources for this table" link at the bottom of the page for audit use.

---

## 6. Confidence tier visualization — critique and redesign

### 6.1 What today's UI does

Colored pill badges:
- `HUMAN_ASSERTED` green
- `GROUNDED` blue
- `INFERRED` amber
- `GUESSED` grey
- `DEPRECATED` red

### 6.2 Why this fails

**Failure 1: Color collision with semantics.** Blue means "grounded" here, but blue
also means "info / hyperlink / selected" in every other UI convention. The user's
visual system has to disambiguate at every glance.

**Failure 2: The five tiers aren't equally important.** `human_asserted` and
`grounded` are functionally "trust it." `guessed` and `deprecated` are "don't trust
it." `inferred` is the middle. A 5-color rainbow flattens this; users should pre-
attentively see three states (trust / caution / avoid).

**Failure 3: No magnitude.** The score is appended as a decimal ("GROUNDED · 0.82").
Decimals require parsing. A glyph with fill encodes magnitude pre-attentively.

**Failure 4: Same shape everywhere.** PII chips, source chips, tag chips, and
confidence chips are all rounded rectangles with similar typography. They blur into
one another.

### 6.3 Reference patterns done right

| Product | Pattern                  | Why it works                                           |
| ------- | ------------------------ | ------------------------------------------------------ |
| **Linear** priority | Icon: blank / line / two-line / dot / urgent | Pure shape encoding, no color dependency, scannable in lists |
| **Datadog** monitor severity | Critical=red dot, warn=yellow dot, ok=green dot, no-data=grey | Three colors only, all ordered, traffic-light convention |
| **Stripe Radar** risk | Score bar 0-100 + "normal/elevated/highest" label | Magnitude + tier together; bar is pre-attentive, label gives precision |
| **GitHub** CI checks | Green check / yellow dot / red x / grey circle | Shape + color redundant — survives color-blindness |

### 6.4 Proposed Synapse glyph

Use a **5-segment dot meter** + a single-character tier code, no color for the
default state:

```
●●●●●  H   human_asserted   (all 5 filled, "H" label)
●●●●○  G   grounded         (4 filled)
●●●○○  I   inferred         (3 filled)
●●○○○  ?   guessed          (2 filled, question mark)
●○○○○  ✕   deprecated       (1 filled, red, ONLY tier with color)
```

Color is reserved for the **outliers**: `deprecated` is red (warning), nothing else
is colored. This solves the color-collision problem and lets DQ/freshness own the
green/amber/red palette.

Score is shown as a tooltip on hover, not in the chip. The chip carries tier; the
tooltip carries precision. Tufte: layered information.

**For inline-list use** (e.g. the columns table), even smaller form:

```
| col_name              | type    | conf | sources       |
| cm11                  | STRING  | ●●●● | mdm·bq·lkml·c |
| account_status_code   | STRING  | ●●●○ | mdm·bq·c      |
| ai_suggested_pii_flag | BOOLEAN | ●●○○ | llm           |
```

Compact, scannable, no color noise.

**Delta:** replace `_tier_badge()` and `_TIER_COLORS` with the dot-meter glyph
component. Strip the score from the visible chip; move to tooltip.

---

## 7. Section-by-section: best-in-class analogies

For each section of the page, name the gold standard and the specific affordance to
steal.

### 7.1 Identity block

**Steal from:** GitHub repo header (the row with name / fork count / star count /
description).
**Specific:** breadcrumb-style FQN (`project › dataset › table`) with each segment
clickable. Description truncated at 2 lines with a "more" affordance. Owner team
shown as an avatar-stack with team name, not a string.

### 7.2 Provenance pane (the 10-source drill-down)

**Steal from:** Linear issue side panel (right-side persistent panel that updates as
you click items in the list).
**Specific:** sticky on scroll, ~360px wide, dismissable with `esc`. Header shows the
fact being inspected; body shows contributing sources with weight and evidence count;
footer shows non-contributing sources and conflicts. Each contributing source has a
"view raw" deep-link.

### 7.3 Columns table

**Steal from:** dbt docs column list, refined by Snowflake's INFORMATION_SCHEMA UI.
**Specific:** dense table (not cards). Columns: name · type · flags (PK/PART/PII as
single-char icons) · cardinality bucket · confidence dot-meter · sources (compact
icon row). Sticky header. Filter bar at top: name contains, confidence ≥, has PII,
is_join_key, is_filter, cardinality bucket. Click a row to drill into that column
(opens provenance pane).

**Today's UI uses `st.container(border=True)` cards per column.** With 193 columns
that's 193 cards. Brutal. Table-first, cards on click only.

### 7.4 Metrics block

**Steal from:** Looker LookML measure browser + dbt metrics page.
**Specific:** each metric is a card with: technical name, business name, formula in
monospace, grain, domain, synonyms. Confidence dot-meter on the right. **Add what's
missing today:** a "used in N dashboards" count and a "last queried N hours ago"
freshness signal. These are the two questions an engineer always asks about a metric.

### 7.5 Lineage

**Steal from:** dbt Cloud lineage graph (not Datadog service map — service maps are
for dynamic call graphs; lineage is a DAG, render it like dbt does).
**Specific:** small horizontal SVG with this table in the center, upstreams to the
left, downstreams to the right, max 2 hops shown. Click any node to navigate to that
table's Synapse page. Today's UI shows lineage as two bulleted lists — that's a data
dump, not a lineage view. **A picture of a DAG is the only correct rendering of a
DAG.**

If a real graph component is too much for v1, fall back to a 2-column "← upstream |
downstream →" with arrows, NOT bullet lists.

### 7.6 Data quality

**Steal from:** Monte Carlo's rule list + Dataplex Auto-DQ.
**Specific:** sort fail/warning first (already done — good). Add a rule-status
sparkline showing the last 30 runs of each rule (red/green dots in a row).
**Critically:** show the *threshold expression* as code, not a string in a tiny gray
font. `null_pct < 0.01` should be `null_pct < 0.01` in monospace, with the *observed*
value next to it (`observed: 0.003 ✓`).

### 7.7 Usage

**Steal from:** GitHub repo Insights → Traffic + Contributors.
**Specific:** queries/week sparkline (12-week window), top users as a list with
team and query count, peak hours as a 24-hour heat strip. **Missing today:** WHO are
the queries from (specific people / teams) and WHAT type of query (BI tool / ad-hoc
SQL / pipeline). That last cut is what tells a user whether their use case is
canonical.

### 7.8 Code resolutions

**Steal from:** Stripe's API reference enum tables.
**Specific:** dense two-column table — `raw → human`. Group by column. Today's
bulleted list is fine but inefficient at scale; switch to `st.dataframe` for >10
mappings.

---

## 8. KILL list — what to remove or demote

Be ruthless. Every section that survives the cut should earn its real estate.

| Kill / demote                                       | Reason                                                                                                  | Action                                                                                  |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `_render_per_source_view` at page position 2        | Wrong entry point (see §5); pushes real content below the fold                                          | Move to bottom, mark "for audit"; primary access via per-fact chip                      |
| Emoji prefixes on every `####` header (`🔎 📑 🎯 🧬 🔗 👥 🔒 ✅ 🆔`) | Decorative; competes with content; ages badly; doesn't survive grayscale                                | Remove all. Use weight + size for hierarchy.                                            |
| `🤖` icon on AI descriptions                         | Stigmatizes a useful signal; users learn to skip purple boxes                                           | Replace with a subtle "AI-suggested · low confidence" inline tag                        |
| Tier color palette (5 colors)                       | See §6; flattens the trust/caution/avoid hierarchy and collides with DQ semantics                       | Replace with dot-meter; red reserved for `deprecated` only                              |
| All 193 columns as `st.container(border=True)` cards | Catastrophic scroll length; cards waste horizontal space; not scannable                                 | Dense table at L1; card pops out as L3 drill                                            |
| `st.divider()` between every section                | Visual noise; the heading already separates                                                              | Remove; use whitespace                                                                  |
| Sidebar "Graph stats" block (nodes/edges/tiers)     | Internal metric; users don't think in graph terms                                                       | Move behind a "Debug" toggle                                                            |
| "Regenerate sources" checkbox in sidebar            | Dev affordance leaking into the product chrome                                                          | Move behind "Debug"                                                                     |
| `🔧 Raw inspection JSON` expander                    | Useful for debug, distracting in product                                                                 | Hide behind a `?debug=1` URL param                                                      |
| Bulleted lineage lists                              | A DAG is not a list                                                                                     | Render as a real graph (or 2-column with arrows as a v1 stopgap)                        |
| `_source_pill` reused for tags AND sources AND synonyms | Same visual = same meaning, per Gestalt; using one chip for three concepts is a category error           | Three distinct chip styles: tag (text), source (icon+text), synonym (italic text)        |
| Per-source panel showing zero-contribution sources expanded | `expanded=contributed` is already correct, but the section header still takes space for empty sources    | Hide non-contributing sources behind a "show 4 sources that didn't contribute" link     |
| `st.metric` for "Has PII: Yes (3 columns)"          | `st.metric` is for trend numbers; using it for a boolean is misuse and looks weird                       | Plain text with a chip                                                                  |

---

## 9. What's MISSING — high-leverage additions

Ranked by ROI. Top 5 are L1 priority; rest are roadmap.

### 9.1 (P0) Inline search / command bar (`⌘K`)

Power users live in palettes. Cmd-K opens a fuzzy search over: table names, column
names, metric names, business glossary terms, top users. Selecting a result either
navigates (table → that table's page) or scrolls + highlights (column → that column
in the current page).

**Reference:** Linear, Notion, Raycast. This is non-negotiable for power users.

### 9.2 (P0) Filterable columns table

The columns block must have a filter input. Today, with 193 columns, finding
"which column has data_source values" requires Cmd-F in the browser, which doesn't
work across collapsed expanders. Filter on: name contains, type, confidence ≥,
has PII, is_join_key, is_filter, is_group_by, cardinality bucket.

### 9.3 (P0) Sparklines on usage and freshness

Number without trend = half a number (Tufte). `queries/wk = 847` is useless;
`queries/wk = 847 ▁▂▄▆▇▆▄` says "stable, slight downturn last week." 30 pixels of
information value.

### 9.4 (P0) "Changes since you last looked" feed

A passive but honest behavioral hook (see §10). Per table: "Schema changed 3 days
ago (added 2 columns, removed 1). 1 DQ rule started failing yesterday. Top user
shifted from team-A to team-B last week."

This is the only legitimate "open daily" hook for these users — actual change
information they need to know.

### 9.5 (P0) Confidence delta vs. comparable tables

"This table's trust score is 0.82. The 12 other `custins_*` tables average 0.68.
You're looking at the highest-trust table in this family." Comparison-to-cohort
turns an absolute number into a decision-grade signal.

### 9.6 (P1) Diff view between two tables

Select a second table from a dropdown; render a side-by-side comparison: schema
diff (added / removed / type-changed columns), trust delta, lineage overlap, shared
metrics. Engineers do this constantly when migrating from one mart to another.

### 9.7 (P1) Per-column "where is this used?" deep-link

Click a column → see the top 10 queries that reference it, deduplicated by query
shape. Solves the recurring "if I rename this, what breaks?" question.

### 9.8 (P1) Keyboard shortcuts

- `?` opens shortcut overlay
- `g t` go to table search
- `g c` jump to columns
- `g l` jump to lineage
- `g p` toggle provenance pane
- `j / k` next / previous column in the table
- `esc` close provenance pane

**Reference:** GitHub, Linear, Gmail. Power users will learn them in a day and
never go back.

### 9.9 (P1) Permalink with anchors and pane state

`?table=foo&col=bar&pane=provenance` should restore exactly what the user was
looking at. Shareability is the cheapest growth lever for an internal tool —
"saw this in Slack, opened the link, landed on the exact pane the sender meant."

### 9.10 (P2) Conflicts surface

`prov.conflicts` exists in the data model but isn't rendered. When MDM says
`is_partitioned=True` and BQ says `is_partitioned=False`, the user MUST see that.
This is the single highest-signal output of a multi-source graph and the current UI
silently swallows it. Add a "conflicts" tile in the trust header if `len(conflicts)
> 0`.

### 9.11 (P2) "Ask the graph" embedded chat

Connected to MCP. "Which columns can I use to filter for North America customers in
the last 30 days?" → graph traversal answer with citations. Don't build this yet,
but design the page to leave room for it (the right-side provenance pane location
becomes the chat location in v2).

---

## 10. Behavioral hooks (without being cute)

These users hate gamification. Streaks, XP, badges are insulting to a senior engineer.
But there are honest hooks rooted in BJ Fogg's model — `Behavior = Motivation ×
Ability × Prompt`:

| Hook                                                              | Motivation                                                       | Ability                          | Prompt                          |
| ----------------------------------------------------------------- | ---------------------------------------------------------------- | -------------------------------- | ------------------------------- |
| **"Tables you depend on had changes since your last visit"**      | Avoiding a broken dashboard tomorrow morning                     | One click to see the diff        | Email digest 9am Monday         |
| **"3 DQ rules started failing on tables you own"**                | Reputational — you don't want to be the one who shipped bad data | Single page with all your rules  | Slack DM when a rule goes red   |
| **"`cm11` was reclassified from `guessed` to `grounded` this week — your downstream Looker measure can be promoted"** | Concrete improvement to ship                                     | One-click PR generator for LookML | Weekly digest                   |
| **"Conflict detected: MDM and corpus disagree on the grain of `revenue_amount`"** | Curiosity + correctness                                          | Side-by-side conflict view       | In-app banner on the table page |
| **"Your team's top 5 most-queried tables are below DQ threshold X"** | Team responsibility                                              | Pre-filtered list                | Monthly team-level report       |

**The pattern:** every hook is a real signal the user needs anyway. None of them
fabricate engagement. The product becomes the place you check when something is wrong
or improvable — not the place you visit to feel rewarded.

**What we explicitly do NOT do:**
- Streaks, XP, points, leaderboards
- Empty notifications ("you have 0 alerts")
- "You've explored 12 tables this week" (we don't care)
- Achievement badges ("Trust Maven — viewed 100 tables")

Don Norman: "Good design is invisible." Bad gamification is the most visible thing in
a product, and these users will uninstall on contact.

---

## 11. Layout sketch — the proposed page top-to-bottom

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│ Synapse  ·  custins_customer_insights_cardmember           [⌘K]  [?]  [debug]    │
├──────────────────────────────────────────────────────────────────────────────────┤
│ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                      │
│ │ TRUST   │ │ FRESH   │ │ DQ      │ │ PII     │ │ USAGE   │                      │  L1: HERO
│ │ ●●●●○ G │ │ 4.2h ●  │ │ 12/14 ● │ │ 3 cols  │ │847 ▂▄▆▇▆│                      │  Q1
│ │ 6 of 10 │ │ 09:14Z  │ │ 2 warn  │ │ amber   │ │ +team-A │                      │
│ │ why?→   │ │ why?→   │ │ why?→   │ │ why?→   │ │ why?→   │                      │
│ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘                      │
├──────────────────────────────────────────────────────────────────────────────────┤
│ Customer Insights — Cardmember              [●●●●○ ⓘ]    Owner: data-platform   │
│ prj-d-ea-prod › custins › cardmember                       Asset: Table          │  L1: IDENTITY
│ The canonical cardmember insights table for cornerstone-data customers...        │
│ more ▸                                                                           │
│ tags: gold · cornerstone · cardmember                                            │
├──────────────────────────────────────────────────────────────────────────────────┤
│ KEY COLUMNS                                                                      │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│ │ 🔑 cm11      │ │ 📅 load_date │ │ data_source  │ │ account_stat │              │  L1: KEY COLS
│ │ STRING ●●●●  │ │ DATE ●●●●●   │ │ STRING ●●●●  │ │ STRING ●●●○  │              │  Q2 entry
│ │ used 412x    │ │ partition    │ │ used 380x    │ │ used 297x    │              │
│ └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘              │
├──────────────────────────────────────────────────────────────────────────────────┤
│ [Show all 193 columns ▾]  [8 metrics ▾]  [Lineage ▾]  [Usage ▾]  [14 DQ ▾]      │  L2 ENTRYPOINTS
│ [3 conflicts ▾]  [12 code resolutions ▾]  [Per-source audit ▾]                  │
└──────────────────────────────────────────────────────────────────────────────────┘
        (expanded sections render below; provenance pane slides in on the right)
```

When the user clicks any `[●●●●○ ⓘ]` chip anywhere on the page, the right pane
appears:

```
                                              ┌────────────────────────────────────┐
                                              │ WHY WE BELIEVE THIS                │
                                              │ fact: business_name                │
                                              │ value: "Customer Insights — ..."   │
                                              │ confidence: grounded · 0.82        │
                                              ├────────────────────────────────────┤
                                              │ Contributed (3 of 10):             │
                                              │  ✓ mdm           w=3  ev=1         │
                                              │  ✓ table_catalog w=3  ev=1         │
                                              │  ✓ corpus        w=1  ev=14        │
                                              ├────────────────────────────────────┤
                                              │ Did not contribute (7):            │
                                              │   bq · baseline_lookml · glossary  │
                                              │   metric_catalog · usage           │
                                              │   dq_engine · llm_generated        │
                                              ├────────────────────────────────────┤
                                              │ Conflicts: none                    │
                                              │ First seen: 2026-03-12  Last: 5-30 │
                                              └────────────────────────────────────┘
```

---

## 12. Accessibility & density check

- **Color-blindness:** dot-meter glyph survives grayscale; DQ traffic light is
  redundantly coded with icon (✓ / ! / ✕). PII chip is not color-only — it has
  the word "PII" and the taxonomy.
- **Keyboard navigation:** every L2 expander must be tab-reachable and openable with
  `enter` / `space`. Provenance pane closable with `esc`.
- **Density:** target ~30% information density (ink-to-pixel) in the columns table
  — well above Streamlit defaults, in line with Bloomberg Terminal / Datadog dashboards
  / dbt docs. These users prefer dense to sparse.
- **Line length:** descriptions capped at ~80ch for readability; longer text behind
  "more" link.
- **Monospace for identifiers:** every column name, FQN, formula, threshold uses
  monospace. Already done correctly in the current UI; preserve.

---

## 13. Implementation order (suggested PR sequence)

1. **PR 1 — Trust header** (replaces top of `main()`). Pure cosmetic; no data
   changes. High visible impact. Two days of work.
2. **PR 2 — Confidence dot-meter component** (replaces `_tier_badge`). Pure UI
   primitive change; touched everywhere. Half a day.
3. **PR 3 — Columns table refactor** (replaces card-per-column with a `st.dataframe`
   + filter bar; key-columns strip in L1). Biggest scroll-length improvement. Two days.
4. **PR 4 — Provenance pane** (right-column layout, per-fact `ⓘ` chips). Hero
   feature. Three to five days including the per-fact wiring.
5. **PR 5 — Section reordering and KILL list** (remove dividers, demote per-source
   panel, hide raw JSON behind debug). One day.
6. **PR 6 — Lineage real graph** (component or SVG). Two days.
7. **PR 7 — Cmd-K search and keyboard shortcuts.** Two to three days.
8. **PR 8 — "Since you last looked" feed.** Requires persistence of last-visit
   timestamp. Three days plus data plumbing.
9. **PR 9 — Conflicts surface and comparison-to-cohort.** Two days.

Stop here for v1. Diff view, embedded chat, and per-column "where used" are v2.

---

## 14. Single-line summary for the next implementer

> Replace the JSON-dump rendering with: a five-tile trust header, an L1/L2/L3
> disclosure hierarchy, a dot-meter confidence glyph, and a per-fact ⓘ chip that
> opens a persistent right-side provenance pane. Kill the dividers, the equal-weight
> sections, and the always-expanded 7-source panel at page position 2. The hero
> feature is per-fact provenance on demand — not "here are all 10 sources, expand
> one."
