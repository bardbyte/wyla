# Synapse — Visual System & Interaction Spec

**Owner:** Visual design layer (companion to `UX_INFORMATION_ARCHITECTURE.md`)
**Scope:** Tokens, components, motion, iconography, code typography, empty states,
Streamlit feasibility, paste-ready CSS, ASCII wireframe.
**Target aesthetic:** Linear (calm density) × Datadog (status legibility) ×
Stripe Dashboard (typographic discipline) × Apple (restraint).
**Audience:** Senior data / ML engineers. Visual sophisticates. They will
notice the 1px difference between a `border-color` change and a layout shift.
They will reject anything that smells of toy or anything that smells of SAP.

The defining gesture of this system is **confidence-as-color, source-as-glyph,
identifier-as-monotype**. Everything else is structure for those three.

---

## 0. Design principles (the rules we won't break)

1. **The graph speaks; the chrome whispers.** Confidence color and source
   glyphs are the loudest things on screen. Borders, dividers, labels —
   reduced to the thinnest, lowest-contrast form that still works.
2. **Identifiers are monotype, prose is proportional.** A column name is
   never the same font as its description. Mixing them is what makes
   enterprise dashboards feel like Excel.
3. **Density is earned, not assumed.** We pack more per screen than a SaaS
   marketing site — but only because users scan vertically and we use
   typographic hierarchy + an 8pt grid to keep it parseable.
4. **No gratuitous motion.** Engineers see animation as cost (latency,
   distraction). Motion only appears at expand/collapse, focus rings, and
   value updates. Total motion budget: 200ms per interaction.
5. **Dark mode is first-class, not a toggle.** Both themes get hand-tuned
   contrast. We do not auto-invert.
6. **The 5 confidence tiers are the spine of the entire color system.**
   They are the only colors that carry semantic weight at the global level.
   Source-family colors are subordinate (used as glyph tints, never fills).

---

## 1. Design tokens

### 1.1 Color palette

#### 1.1.1 Neutrals — the canvas

Inspired by Linear's `#08090A` near-black and Vercel's grayscale ramp.
Slightly cool (hint of blue) because warm grays read as "Notion" and this
is not Notion. 13-step ramp, light and dark mode.

| Token            | Light (hex) | Dark (hex) | Use                                       |
|------------------|-------------|------------|-------------------------------------------|
| `--surface-0`    | `#FFFFFF`   | `#0A0A0B`  | Page background                           |
| `--surface-1`    | `#FAFAFA`   | `#101113`  | Card background (resting)                 |
| `--surface-2`    | `#F4F4F5`   | `#161719`  | Card background (hovered) / sidebar fill  |
| `--surface-3`    | `#EEEEF0`   | `#1C1D20`  | Inset (code blocks, kbd, raw JSON)        |
| `--border-subtle`| `#E8E8EB`   | `#202125`  | Hairlines between rows                    |
| `--border-default`| `#D4D4D8`  | `#2A2B2F`  | Card borders                              |
| `--border-strong`| `#A1A1AA`   | `#3F4046`  | Focus rings, active borders               |
| `--fg-muted`     | `#71717A`   | `#71727A`  | Captions, secondary text                  |
| `--fg-default`   | `#3F3F46`   | `#C9CACE`  | Body text                                 |
| `--fg-strong`    | `#18181B`   | `#F4F4F6`  | Headers, identifier names                 |

**Why this ramp:** the 11-step `surface → fg` ladder gives us 5 stops of
neutral background AND 3 stops of foreground text. Most dashboards use a
3-stop ramp (`bg / card / text`) and end up with grey-on-grey legibility
issues at the row-divider level. This ramp's `border-subtle` is
mathematically distinct from `surface-1` and `surface-2`, so a card-on-card
section break is always visible without needing a shadow.

#### 1.1.2 Accent — the brand voice

We use **electric indigo**, not Amex blue. Amex blue is corporate. This is
internal tooling for engineers. Indigo reads as "developer-first" (Linear,
Vercel, Stripe all live in this range).

| Token            | Light       | Dark        | Use                                  |
|------------------|-------------|-------------|--------------------------------------|
| `--accent-bg`    | `#EEF0FF`   | `#1E1F4A`   | Selected row, focused tab            |
| `--accent-border`| `#A5AEFF`   | `#4F56CF`   | Selected row border, focus ring tint |
| `--accent-fg`    | `#4F46E5`   | `#A5B0FF`   | Selected text, link hover            |
| `--accent-solid` | `#4338CA`   | `#6366F1`   | Primary button bg                    |

Single accent. We do NOT introduce a second hue for "info" or "secondary
action." If something needs to stand out beyond default text, it gets the
indigo. If two things compete for indigo, one of them shouldn't be
prominent in the first place.

#### 1.1.3 Confidence tiers — the spine

The current Streamlit code uses Google's old Material palette (`#0b8043`,
`#1a73e8`, `#e8a317`, `#9aa0a6`, `#d93025`). Those colors are too saturated
and too 2018. We re-cast them in a **monochromatic-saturation ramp** so
they read as ordinal (less confident → more confident is perceptible as a
single axis), and we mute the deprecated color to a clear red-grey so it
never competes with active rows for attention.

| Tier             | Light fg    | Light bg    | Dark fg     | Dark bg     | Dot       | Mental model                |
|------------------|-------------|-------------|-------------|-------------|-----------|-----------------------------|
| `deprecated`     | `#7A1F1F`   | `#FEECEC`   | `#FCA5A5`   | `#3A1414`   | `#DC2626` | Tombstone — do not consume  |
| `guessed`        | `#71717A`   | `#F4F4F5`   | `#A1A1AA`   | `#1C1D20`   | `#A1A1AA` | One source, low weight      |
| `inferred`       | `#92400E`   | `#FEF3C7`   | `#FCD34D`   | `#3A2810`   | `#F59E0B` | Multi-source or weighted    |
| `grounded`       | `#155E75`   | `#CFFAFE`   | `#67E8F9`   | `#0E3A45`   | `#06B6D4` | Catalog-attested, defensible |
| `human_asserted` | `#14532D`   | `#DCFCE7`   | `#86EFAC`   | `#0E2E1A`   | `#22C55E` | Signed by a human           |

**Justification for the swap of blue → cyan for `grounded`:**
- The default page accent is already indigo (`#4338CA`). If `grounded` is
  blue, every grounded badge competes with selected-row state.
- Cyan-teal reads as "system-attested infrastructure" (Datadog uses teal
  for system services). Indigo reads as "primary brand." Decoupling them
  prevents the user from confusing "this is what I clicked" with "this is
  trustworthy."

**Justification for amber on `inferred`:**
- The classic Western traffic-light prior (amber = "be cautious, verify")
  carries over without semantic re-training.
- Amber on dark mode at `#F59E0B` clears WCAG 4.5:1 against `surface-1`.

**Justification for cool-grey on `guessed` (not warm):**
- Warm greys imply "documentation in progress" (Notion). Cool greys imply
  "low signal, system-evaluated" (Linear). The latter matches the meaning.

#### 1.1.4 DQ severity — the CI-status palette

These are deliberately **brighter** than the confidence tiers because DQ
rules are runtime status, not metadata. A failing DQ rule should pop the
same way a red X next to a GitHub commit pops.

| Status      | Fg        | Bg        | Icon glyph (Lucide) |
|-------------|-----------|-----------|---------------------|
| `pass`      | `#15803D` | `#DCFCE7` | `check-circle-2`    |
| `warning`   | `#B45309` | `#FEF3C7` | `alert-triangle`    |
| `fail`      | `#B91C1C` | `#FEE2E2` | `x-circle`          |
| `unknown`   | `#71717A` | `#F4F4F5` | `circle-help`       |

Crucially: the `fail` red here is **brighter** than the `deprecated` red
above. Deprecated says "this used to matter, ignore it." Fail says "this
is on fire, look at it." The hierarchy of attention requires they look
visually distinct.

#### 1.1.5 Source families — glyph tints

The 10 sources in `store.SourceName` group into 4 families. We tint glyphs
by family (not by individual source) — 10 colors is impossible to memorize;
4 is feasible.

| Family   | Members                                                    | Tint (light) | Tint (dark) |
|----------|-------------------------------------------------------------|--------------|-------------|
| Catalog  | `mdm`, `table_catalog`, `metric_catalog`, `glossary`, `baseline_lookml` | `#7C3AED` (violet) | `#A78BFA` |
| Runtime  | `bq`, `dq_engine`                                           | `#0891B2` (teal-cyan) | `#22D3EE` |
| Corpus   | `corpus`, `usage`                                           | `#0D9488` (deep teal) | `#5EEAD4` |
| AI       | `llm_generated`                                             | `#DB2777` (rose) | `#F472B6` |

Family color tints the **glyph stroke only**, never the pill background.
The pill stays neutral (`surface-2`). Glyph color carries the signal;
typography carries the label. This is how Linear distinguishes priority
icons from labels.

**Pink for AI is deliberate.** AI is the lowest-trust source (`SOURCE_WEIGHTS["llm_generated"] == 1`).
We don't hide it (engineers want to see the AI suggestion), but we mark it
with a hue that's *separate from everything else in the system* so it can
never be visually mistaken for a catalog source. Rose / magenta is the
universally-readable "this is a suggestion, not ground truth" color
(Cursor uses it, Linear uses it for AI label suggestions).

---

### 1.2 Typography

#### 1.2.1 Font stack

```css
--font-sans: "Inter", "SF Pro Text", -apple-system, BlinkMacSystemFont,
             "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;

--font-mono: "JetBrains Mono", "SF Mono", "IBM Plex Mono", Menlo,
             Consolas, "Liberation Mono", monospace;

--font-display: "Inter", -apple-system, sans-serif;
             /* same as --font-sans but used with negative tracking */
```

**Why Inter, not SF Pro:** Inter is open, ships variable, and renders
identically on every laptop including the older Amex MacBooks that don't
have SF Pro 18+ glyph updates. The fallback chain still hits SF Pro on
modern macs.

**Why JetBrains Mono over SF Mono:** JetBrains Mono has a wider character
set, distinguishes `0` from `O` and `l` from `1` more aggressively, and
its `=`, `→`, and `≤` ligatures (which we disable globally — see below)
match the typographic weight of the surrounding sans. SF Mono's lowercase
`g` looks like a marketing typeface; JetBrains Mono looks like a
programmer's typeface. We want the latter.

**Ligatures: disabled globally** (`font-variant-ligatures: none`). Data
engineers reading SQL formulas need `<>` to render as two characters, not
≠. Ligatures are cute in your IDE; in a tooltip they obscure literal
operator semantics.

#### 1.2.2 Type scale (6 steps, deliberately tight)

| Token           | Size / line-height | Weight | Tracking | Family   | Use                          |
|-----------------|--------------------|--------|----------|----------|------------------------------|
| `--text-display`| `28px / 36px`      | 620    | `-0.4px` | display  | Hero table name (`HeroHeader`) |
| `--text-h1`     | `20px / 28px`      | 600    | `-0.2px` | sans     | Section dividers ("Columns") |
| `--text-h2`     | `15px / 22px`      | 600    | `0`      | sans     | Card titles, panel labels    |
| `--text-body`   | `13px / 20px`      | 420    | `0`      | sans     | Body prose, descriptions     |
| `--text-caption`| `11px / 16px`      | 480    | `0.2px`  | sans     | Meta labels, counts, "12 columns" |
| `--text-mono`   | `12px / 18px`      | 460    | `0`      | mono     | All identifiers, formulas    |

**Why 13px body, not 14px:** Linear uses 13px for issue rows. At our
density target (8+ data points per card) the extra pixel of vertical
breathing room from 13 vs. 14 lets us drop card padding by 4px without
crowding. The cumulative gain across a scroll is ~150px of visible content.

**Why 12px mono, not 13px:** monospace x-heights run larger than
proportional x-heights at the same nominal size. 12px JetBrains Mono ≈
13px Inter optically. Matching x-heights when sans and mono sit on the
same baseline is what separates a designed page from a developer's bad day.

**Why weight 420 / 460 / 480 for body/mono/caption:** these are between
"regular" (400) and "medium" (500). Variable fonts let us pick any number.
420 reads slightly heavier than 400 on low-DPI screens, which compensates
for Inter rendering thin on Windows. 480 on caption-mono adds enough
weight for small all-caps labels to be readable without going to 500
(which reads as "active state").

**No `--text-largest`, no `--text-tiny`.** Six steps cover everything. If
you want bigger, use the display. If you want smaller, restructure your
data.

---

### 1.3 Spacing — 4pt grid

```css
--space-0: 0;
--space-1: 2px;   /* hairline tweaks only, e.g. icon offset */
--space-2: 4px;   /* between glyph and label inside pill */
--space-3: 8px;   /* between related controls in a row */
--space-4: 12px;  /* card internal padding (top/bottom in dense rows) */
--space-5: 16px;  /* card internal padding (standard) */
--space-6: 24px;  /* between cards */
--space-7: 32px;  /* between major sections */
--space-8: 48px;  /* page top padding */
--space-9: 64px;  /* reserved — currently unused */
```

**Why 4pt not 8pt:** the visible elements (badges, glyphs, hairlines)
operate at sub-8pt scales (12px icons, 2px borders, 16px badges). An 8pt
grid would force half-steps for every glyph, and developers hate seeing
`padding: 4px 8px` as "non-grid." Better to use a 4pt grid and never
violate it.

**Most-used spacing trio:** `--space-3` (8px), `--space-5` (16px),
`--space-6` (24px). Together they handle row gaps, card internal padding,
and section breaks respectively. If you find yourself reaching for
`--space-7`, ask whether a divider would do the work instead.

---

### 1.4 Borders, radii, shadows

```css
--radius-xs: 4px;   /* badges, pills, kbd */
--radius-sm: 6px;   /* small buttons, code chips */
--radius-md: 8px;   /* cards, inputs */
--radius-lg: 12px;  /* modals, sheets */

--border-width-hairline: 0.5px;  /* row dividers on retina */
--border-width-default:  1px;    /* card edges */
--border-width-focus:    2px;    /* keyboard focus rings */

/* Shadows — Linear-restraint, NOT Material elevation */
--shadow-none:  none;
--shadow-rest:  0 0 0 1px var(--border-subtle);
                /* 1px ring, no offset — the "card sits on the page" */
--shadow-hover: 0 0 0 1px var(--border-default),
                0 1px 2px rgba(0,0,0,0.04),
                0 4px 8px rgba(0,0,0,0.02);
--shadow-pop:   0 0 0 1px var(--border-default),
                0 4px 12px rgba(0,0,0,0.08),
                0 16px 32px rgba(0,0,0,0.04);
                /* for popovers / floating menus */
```

**Why ring-shadows over offset-shadows:** offset shadows (Material) imply
physical elevation. We're not Material. Cards in Synapse don't "float" —
they're regions of the same surface. A 1px ring is how Stripe's
Dashboard, Linear, and Vercel all delineate cards. It also removes the
visual stack-up problem when cards sit close to each other (offset
shadows create dark seams between adjacent cards; rings don't).

**Hairlines:** `0.5px` borders look correct on every retina display, render
as `1px` on non-retina. We use them for **row dividers within a card**
(separating one column-row from the next). Card outer edges always use
1px. This 0.5px-internal / 1px-external rule is how Apple's Settings.app
and Stripe's invoice tables handle dense lists.

---

## 2. Component library

Each component lists: **purpose · anatomy · visual spec · states · Streamlit
feasibility tag**. Feasibility legend:

- ✅ native — Streamlit primitive renders this with no markup
- 🎨 CSS-via-markdown — `st.markdown(..., unsafe_allow_html=True)` + injected CSS
- 🧩 components.html — needs `st.components.v1.html` iframe
- ⛔ needs different stack — Streamlit fundamentally cannot do this

---

### 2.1 `ConfidenceBadge` — the spine of the entire UI · 🎨

**Purpose:** encode 3 things in one glanceable token — confidence *tier*,
calibrated *score*, and *count of sources contributing*. Replaces the
current flat colored pill.

**References:** Linear's priority dot + label · GitHub's check-circle +
label + count · Datadog's severity band on the left of a log row.

**Anatomy (left to right):**

```
[ ●  GROUNDED · 0.84  · 4/10 ]
  │       │       │      │
  │       │       │      └─ "agree / contributed-of-10": sources_agree
  │       │       │
  │       │       └─ score (2 decimals, monotype)
  │       │
  │       └─ tier label, all caps, mono, tracking +0.4px
  │
  └─ filled dot (8px), color = tier dot color
```

**Visual spec:**

| Property         | Value                                          |
|------------------|------------------------------------------------|
| Height           | 22px                                           |
| Horizontal pad   | 8px (left/right)                               |
| Internal gap     | 6px between dot/label/score/count              |
| Border           | 1px solid `tier.fg @ 18%` alpha                |
| Background       | `tier.bg` (per tier palette above)             |
| Tier label       | 10px / 14px mono, 540 weight, +0.4px tracking, `tier.fg` |
| Score            | 11px / 14px mono, 460 weight, `tier.fg @ 80%`  |
| Count            | 11px / 14px mono, 460 weight, `tier.fg @ 70%`  |
| Dot              | 8px circle, `tier.dot`, no border              |
| Radius           | `--radius-xs` (4px)                            |

**States:**
- **rest** — as above
- **hover** — background shifts 1 step darker (`tier.bg` → `surface-2`
  blended at 85%); cursor `help`; after 400ms reveal a tooltip showing
  the full `sources_contributed` list with per-source evidence counts.
- **focus (keyboard)** — 2px `--accent-border` outline, 1px offset
- **interactive** (when badge is clickable to filter view) — adds a
  trailing 12px chevron glyph; on hover, the entire badge background
  switches to `surface-2`.

**Compact variant** (`ConfidenceBadgeCompact`) — used in column rows where
horizontal space is tight: dot + tier label only, no score, no count.
14px tall. Used by `ColumnCard` to keep row height at 56px.

**Streamlit:** 🎨 — render as inline HTML span with the dot as a SVG
`<circle>`. CSS class drives tier color via a single `data-tier="grounded"`
attribute selector. No JS needed for hover tooltip — pure `title=` attr
works as fallback; for the richer tooltip use Streamlit's `st.help` or
inline `<details>`.

---

### 2.2 `SourcePill` — one of 10 sources, in a row of many · 🎨

**Purpose:** scannable indicator of which catalog/runtime/corpus/AI
source contributed a fact. Designed to live in groups of 1–10 across the
bottom of a card.

**Anatomy:**

```
[ ⌘ mdm ]   [ ▶ bq ]   [ ⊙ corpus ]   [ ✨ llm ]
   │  │       │  │        │  │            │  │
glyph label  glyph label glyph label    glyph label
```

Glyph is the per-source SF Symbol / Lucide mapping (see §5). Glyph color
inherits the **family tint** (catalog = violet, runtime = teal,
corpus = deep-teal, AI = rose). Label is the bare source name in
monospace.

**Visual spec:**

| Property      | Value                                       |
|---------------|---------------------------------------------|
| Height        | 20px                                        |
| Horizontal pad| 6px                                         |
| Internal gap  | 4px                                         |
| Border        | 1px solid `--border-subtle`                 |
| Background    | `--surface-1`                               |
| Glyph         | 11px Lucide stroke, color = family tint     |
| Label         | 11px / 14px mono, 460 weight, `--fg-default`|
| Radius        | `--radius-xs` (4px)                         |

**States:**
- **rest** — as above
- **muted** (source did NOT contribute) — opacity 0.35, no border, glyph
  desaturated to `--fg-muted`. Used to show "this source could have
  contributed but didn't" — important context for confidence reasoning.
- **hover** — border becomes `--border-default`, background `--surface-2`,
  tooltip: "mdm contributed 3 evidence events: table_business_name,
  description, owner_team".
- **active** (clicked to filter view) — background fills with family tint
  at 12% alpha, border becomes family tint at 60% alpha, label color
  becomes family tint at 100%.

**Streamlit:** 🎨 — HTML span with `data-source="mdm"` driving CSS. A
helper `def source_pill(name, contributed=True): -> str` returns the
markup. Single Lucide SVG sprite injected once at page load.

---

### 2.3 `ColumnCard` — the heart of the table inspection view · 🎨

**Purpose:** show name / type / flags / PII / confidence / sources /
distinct / description with maximum density and zero crowding. Replaces
the current `st.container(border=True)` mess.

**References:** Linear's issue row (single-line scan + expand-to-detail) ·
Stripe API objects in their docs · dbt column documentation rows · GitHub's
file row in a folder listing.

**Anatomy (collapsed, 56px tall):**

```
┌────────────────────────────────────────────────────────────────────────┐
│ ▸ cm_account_token   STRING   🔑 PK  🔒 PII   [● GROUNDED]  ⌘ mdm ▶ bq │
│   Card member account identifier (canonical)                  124,481  │
└────────────────────────────────────────────────────────────────────────┘
```

**Anatomy (expanded, +variable height):**
- everything above
- AI-suggested description (rose left rail, italic, opacity 0.85)
- PII taxonomy chip (if `is_pii`)
- distinct sample chips (up to 5)
- "Referenced in 12 queries" caption with click-to-list
- per-column 7-source breakdown table (collapsed by default within
  expansion, expandable inline)

**Visual spec (collapsed):**

| Region            | Spec                                                            |
|-------------------|-----------------------------------------------------------------|
| Height            | 56px (single line + caption line + 12px vertical padding)       |
| Row 1 (left)      | Triangle disclosure (8px) → column name (mono, 13px, 540, `--fg-strong`) → data type (mono, 11px, 460, `--fg-muted` in `surface-3` chip with 2px radius, 1px hpad) |
| Row 1 (mid)       | Flag pills: PK / PART / JOIN / GROUP / PII / CODED. Each is an icon-only 14×14 glyph in a 16×16 box. Tooltip on hover. |
| Row 1 (right)     | `ConfidenceBadgeCompact` then up to 4 `SourcePill`s (overflow indicator `+3` for more) |
| Row 2 (full width)| Business name italicized 12px `--fg-muted`; if none, fallback to first line of description; if neither, "—" in `--fg-muted @ 0.5`. Distinct count flush right in mono 11px. |
| Border            | None top (handled by parent list divider); `--border-subtle` 0.5px bottom |
| Background        | `--surface-1` rest, `--surface-2` hover                         |
| Cursor            | `pointer` (entire row is the disclosure target)                 |

**States:**
- **rest** — bg `--surface-1`
- **hover** — bg `--surface-2`, triangle rotates 0deg → -90deg by 4deg
  to hint affordance (subtle, 80ms)
- **expanded** — triangle 90deg, row 2 stays, plus expanded panel slides
  down with 120ms ease-out; left edge of expanded panel gets a 2px solid
  `--accent-border` rail to anchor the eye
- **focused** (keyboard) — 2px `--accent-border` ring inset, no offset
- **flagged with failing DQ rule** — left edge becomes 2px solid
  `dq.fail` color; tooltip on hover shows "1 failing DQ rule"

**Density math:** at 56px per row, 12 columns fit in 672px of vertical
space — fits above the fold on a 14" MBP at default zoom. Compared to
current `st.container(border=True)` which renders at ~110px per column,
this is a 49% density gain.

**Streamlit:** 🎨 — HTML table or HTML divs (NOT `st.container` because
its border, padding, and div nesting can't be overridden cleanly). Each
row is an `<details>` element with `<summary>` for the collapsed state,
so disclosure is native HTML (no JS, no Streamlit re-run on expand —
critical to avoid full-page re-render every click).

---

### 2.4 `MetricCard` — formula-prominent, synonyms-as-chips · 🎨

**Purpose:** show a derived metric's technical name, business definition,
SQL formula, grain, synonyms, and provenance. Formula is the dominant
element because that's what engineers verify first.

**References:** dbt Cloud metric definitions · LookML measure docs ·
Stripe API "Request" / "Response" code-prominent blocks.

**Anatomy:**

```
┌──────────────────────────────────────────────────────────────────┐
│ basic_card_count_naa         Basic Card Count NAA                │
│ ─────────────────────────────────────────────────────────────────│
│ │ COUNT(DISTINCT cm_account_token)                              ││
│ │ WHERE basic_supp_indicator = 'BASIC'                          ││
│ │   AND status_code IN ('001', '005')                           ││
│ ─────────────────────────────────────────────────────────────────│
│ Domain: cardmember   Grain: account-month                        │
│ Synonyms: [BCC] [Basic Cards] [Active Basic] [NAA Cards]         │
│                                       [● GROUNDED · 0.91 · 3/10] │
│                                                ⌘ mdm ⌘ metric ▶ bq│
└──────────────────────────────────────────────────────────────────┘
```

**Visual spec:**

| Region        | Spec                                                          |
|---------------|---------------------------------------------------------------|
| Card padding  | 16px                                                          |
| Card bg       | `--surface-1`, border `--shadow-rest`, radius `--radius-md`   |
| Tech name     | mono 13px 540, `--fg-strong`                                  |
| Business name | sans 13px 420, `--fg-default`, flush right of tech name       |
| Formula block | bg `--surface-3`, padding 12px, mono 12px 460, line-height 18px, radius `--radius-sm`, syntax-highlighted via `st.code`-like styling (SQL keywords get `--accent-fg`, strings get `confidence.grounded.fg`, comments get `--fg-muted`) |
| Meta line     | sans 12px 420, `--fg-muted`, labels in mono 11px 540 to make them parseable |
| Synonym chips | 18px tall, `--surface-2` bg, 1px `--border-subtle`, 6px hpad, mono 11px, 4px radius, 4px gap between chips |
| Conf + sources| bottom-right, vertical stack: `ConfidenceBadge` then `SourcePill`s |

**Streamlit:** 🎨 — single HTML block. Formula uses `st.code(language="sql")`
inside an HTML wrapper for native Streamlit syntax highlighting (cheaper
than rolling our own), or use Prism.js via 🧩 components.html for full
control of token colors to match the theme. **Recommend st.code with a
CSS override on `.stCodeBlock` for the bg color**.

---

### 2.5 `SourceBlock` — the 7-source breakdown · 🎨

**Purpose:** show, per source, "what does THIS source independently tell
us about this table?" — the centerpiece of Synapse's "audit-grade" claim.

**Decision: side-panel, NOT accordion, NOT popover.** Reasoning:

| Pattern         | Pros | Cons | Verdict |
|-----------------|------|------|---------|
| Accordion (current) | familiar, easy to implement | only one expanded at a time → comparison across sources requires mental memory; vertical scroll on each expand; loses position | reject |
| Popover         | nice for "drill into one" | can't compare sources side-by-side; modal-y, breaks scroll | reject |
| Side-panel (right rail) | persistent, comparison-friendly, doesn't disrupt main scroll | costs screen width | **adopt** |

The side-panel becomes a **fixed-position right rail** at 360px wide.
The 10 sources stack vertically as **tabs along the rail's left edge**,
each showing the source glyph + a status dot (contributed / not /
conflicting). Clicking a tab swaps the panel's right portion to that
source's contributed facts.

**Anatomy:**

```
┌─ rail ─┬─ panel body ──────────────────────────────────┐
│ ⌘ mdm ●│ MDM contributed 3 evidence events            │
│ ⌘ tbl ●│                                              │
│ ⌘ met ○│  table_business_name: Customer Insights …    │
│ ⌘ glo ─│  description:         The cardmember card…   │
│ ⌘ blk ●│  owner_team:          cs-customer-insights   │
│ ▶ bq  ●│  partition_field:     dt                     │
│ ▶ dq  ●│  row_count_estimate:  124,481                │
│ ⊙ cor ●│                                              │
│ ⊙ usg ●│  ─── conflicts with bq ───                  │
│ ✨ llm ○│  partition_field disagrees: mdm says dt,     │
│        │  bq says event_date                          │
└────────┴──────────────────────────────────────────────┘
```

**Visual spec:**

| Region | Spec |
|--------|------|
| Rail width | 44px (glyph + status dot only) |
| Rail item height | 36px |
| Rail item glyph | 18px Lucide, family tint |
| Rail item status dot | 6px circle, 4px from glyph right edge |
| Status dot color | green = contributed; grey = did not contribute; amber = conflict detected |
| Panel body width | 316px |
| Panel header | mono 12px, source name + " contributed N evidence events", `--fg-muted` |
| Fact row | label (mono 11px `--fg-muted`) + value (mono 12px `--fg-default`), 4px vertical gap, 12px between label and value |
| Conflict section | bg `confidence.deprecated.bg`, left rail 2px `confidence.deprecated.dot`, padding 8px |

**Keyboard interactions:**
- `[` / `]` — previous / next source tab
- `Esc` — collapse the rail to icon-only (toggleable via header button)
- `cmd-click` on a fact row — copies the `key: value` line to clipboard

**Streamlit:** 🎨 + 🧩 — feasible with CSS-positioned divs (`position:fixed`)
inside an HTML block. Tab switching requires either (a) Streamlit
`st.session_state` + button row at top, or (b) `<details>` toggles, or
(c) for full keyboard nav, a small `st.components.v1.html` iframe with
postMessage back to Streamlit. **Recommend (a) for first pass** —
keyboard nav is a v2 enhancement.

---

### 2.6 `LineageRail` — text-as-graph · 🎨

**Purpose:** show upstream / downstream tables in a way that reads as a
graph fragment, not as a Streamlit bullet list. Even though we can't
render an actual D3 graph cheaply in Streamlit, the visual rhythm should
say "graph."

**References:** dbt's lineage column · Datadog APM service dependencies
text view · GitHub Actions' workflow run dependency tree.

**Anatomy:**

```
UPSTREAM (3)                              DOWNSTREAM (5)
  ┌────────────────────────┐                ┌────────────────────────┐
  │ raw_cm_accounts        │                │ vw_active_cards        │
  │   ↳  joined with raw…  │ ──── this ──── │   ↳  3 metrics, 12 q's │
  │                        │     table      │                        │
  │ raw_cm_addresses       │                │ rpt_monthly_acquisitions│
  │   ↳  joined with raw…  │                │   ↳  Tableau dashboard │
  │                        │                │                        │
  │ + 1 more…              │                │ + 3 more…              │
  └────────────────────────┘                └────────────────────────┘
```

The "─── this table ───" connector is a CSS pseudo-element (centered
horizontal rule with the label as its midpoint). Visually it makes the
two columns feel like nodes on either side of an edge, not two unrelated
bulleted lists.

**Visual spec:**

| Region | Spec |
|--------|------|
| Column width | 50% each, 24px gap between |
| Section header | mono 11px 540, +0.4px tracking, `--fg-muted`, with count in parens |
| Item card | bg `--surface-1`, padding 8px 12px, radius `--radius-sm`, border `--shadow-rest`, 4px vertical gap between |
| Item table name | mono 12px 540 `--fg-strong` |
| Item sub-line | mono 10px 460 `--fg-muted`, prefixed with `↳` |
| Center connector | 1px `--border-default`, 24px tall on each side of label; label "this table" in mono 10px `--fg-muted` |
| Empty state (upstream) | "Root table — no upstream sources observed" italic 12px `--fg-muted`; small terminus glyph `■` to the left |
| Empty state (downstream) | "Leaf table — not consumed by other tables" similar treatment |
| "+ N more" link | mono 11px, `--accent-fg`, expands to full list inline |

**Streamlit:** 🎨 — two-column HTML flex with a centered absolutely-
positioned label between them. Pure CSS, no JS.

---

### 2.7 `DQRuleRow` — the CI status list · 🎨

**Purpose:** scan a list of data-quality rules the way you scan a GitHub
PR's check list — one row per rule, status badge dominant, threshold and
last-run-value subordinate.

**References:** GitHub PR checks · Vercel deployment checks · CircleCI
workflow step list.

**Anatomy:**

```
┌──────────────────────────────────────────────────────────────────┐
│ ✓  not_null              cm_account_token       null_pct < 0.01  │ 🤖
│    last run: null_pct = 0.0003 · 2h ago                          │
├──────────────────────────────────────────────────────────────────┤
│ ⚠  freshness             (table)                hours < 24       │
│    last run: hours = 27.4 · 14m ago                              │
├──────────────────────────────────────────────────────────────────┤
│ ✗  enum                  status_code            in {001,005,007} │ 🤖
│    last run: 14 violations · 1h ago                              │
└──────────────────────────────────────────────────────────────────┘
```

**Visual spec:**

| Region | Spec |
|--------|------|
| Row height (collapsed) | 44px |
| Status icon | 16px Lucide, color = dq.severity.fg |
| Rule kind | mono 12px 540 `--fg-strong` |
| Target column | mono 12px 460 `--fg-default`; if table-level, "(table)" in `--fg-muted` |
| Threshold | mono 11px 460 `--fg-muted`, flush right |
| AI marker | rose 🤖 12px Lucide `sparkles` icon, 8px to right of threshold, only if `auto_suggested` |
| Sub-line | mono 11px 420 `--fg-muted`, includes last_run_value + relative time |
| Row left edge | 3px solid dq.severity.dot — this is the dominant scan cue |
| Divider between rows | `--border-subtle` 0.5px |
| Hover | bg shifts to `--surface-2`, sub-line tightens (no animation, just style swap) |

**Sort order (already done by inspector):** `fail → warning → unknown → pass`.
Don't override.

**Streamlit:** 🎨 — single HTML block. Each row is a `<details>` with
expanded view showing rule definition (full SQL if `custom_sql` kind),
history sparkline (a small SVG inline, 80px wide × 16px tall, no library
needed), and "edit / silence / re-run" actions (action buttons are
Streamlit `st.button`s in a row immediately below — Streamlit can't put
buttons inside HTML, so the expanded panel splits into HTML-content + a
trailing button row).

---

### 2.8 `HeroHeader` — the trust-this-table-at-a-glance section · 🎨

**Purpose:** the dominant element on the page. Replaces the current
`st.columns([3, 1])` identity block. The user should be able to answer
in 3 seconds: "what is this, who owns it, can I trust it, when was it
last refreshed, does it have PII."

**References:** Stripe Dashboard's customer header · Linear's issue view
title block · Datadog service overview header.

**Anatomy:**

```
┌────────────────────────────────────────────────────────────────────────┐
│ TABLE · CARDMEMBER DOMAIN                                              │
│                                                                        │
│ custins_customer_insights_cardmember          [● GROUNDED · 0.91 · 7/10]│
│ Customer Insights Cardmember — the daily card-level snapshot           │
│                                                                        │
│ ⌘ mdm  ⌘ catalog  ⌘ lookml  ▶ bq  ▶ dq  ⊙ corpus  ⊙ usage  ✨ llm     │
│                                                                        │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┬───────────┐ │
│ │ FRESHNESS   │ ROW COUNT   │ COLUMNS     │ OWNER       │ PII       │ │
│ │ 2.4h ago    │ 124,481     │ 47          │ cs-cust-ins │ 6 cols    │ │
│ └─────────────┴─────────────┴─────────────┴─────────────┴───────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

**Visual spec:**

| Region | Spec |
|--------|------|
| Eyebrow | mono 11px 540 `--fg-muted`, +0.6px tracking, all-caps: "{ASSET_KIND} · {DATA_DOMAIN}" |
| Table name | `--text-display` (28px / 36px, weight 620, -0.4px tracking), mono, `--fg-strong` |
| Confidence badge | `ConfidenceBadge` at LARGE variant (26px tall, 12px text), flush right of table name, baseline-aligned |
| Description | `--text-body` 13px 420 `--fg-default`, max-width 720px, ellipsis at 2 lines, click to expand |
| Source row | row of all 10 `SourcePill`s, contributed ones in default state, non-contributing ones in `muted` state — this is the "X of 10 sources agree" data made visual |
| Stat grid | 5-column flex, each cell: caption-label (mono 11px 540 +0.4px caps `--fg-muted`) above value (mono 14px 540 `--fg-strong`) |
| Cell dividers | `--border-subtle` 1px vertical between cells |
| Outer card | bg `--surface-1`, padding 24px, radius `--radius-md`, border `--shadow-rest` |

**No icons in the stat grid.** Icons compete with values. Use the label
+ value pattern, and let the typography do the work.

**Streamlit:** 🎨 — single HTML block + injected CSS. The "5 of 10 sources
muted" pattern is what makes this hero visually different from any
generic dashboard header — the row of pills tells the confidence story
better than the badge alone.

---

## 3. Layout grid & responsive behavior

### 3.1 Wide layout (≥1280px viewport — the design target)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ ┌──────────────┐ ┌────────────────────────────────────────┐ ┌──────────────┐ │
│ │ NAV / STATS  │ │  MAIN CONTENT                          │ │  SOURCE      │ │
│ │              │ │                                        │ │  BREAKDOWN   │ │
│ │ logo         │ │  HeroHeader                            │ │  PANEL       │ │
│ │ table picker │ │                                        │ │              │ │
│ │ ─────────    │ │  ───────────────────────────────────── │ │  rail of 10  │ │
│ │ stats:       │ │  Columns (47)                          │ │              │ │
│ │  nodes 1.4K  │ │  ColumnCard × 47                       │ │  selected:   │ │
│ │  edges 6.2K  │ │                                        │ │   mdm        │ │
│ │ ─────────    │ │  Metrics (12)                          │ │   facts...   │ │
│ │ tiers:       │ │  MetricCard × 12                       │ │              │ │
│ │  human   12  │ │                                        │ │              │ │
│ │  grounded 88 │ │  Lineage                               │ │              │ │
│ │  inferred 41 │ │  LineageRail                           │ │              │ │
│ │  guessed  19 │ │                                        │ │              │ │
│ │  deprecated 3│ │  DQ rules (8)                          │ │              │ │
│ │              │ │  DQRuleRow × 8                         │ │              │ │
│ └──────────────┘ └────────────────────────────────────────┘ └──────────────┘ │
│   220px               flex-1 (min 720px, max 960px)              360px       │
└──────────────────────────────────────────────────────────────────────────────┘
```

- **Left sidebar:** 220px fixed. Logo + table selector at top, global
  graph stats below the divider, confidence-tier counts at the bottom.
  This matches Streamlit's native `st.sidebar` — we just restyle its
  contents.
- **Main column:** flex-1 with `min-width: 720px` and `max-width: 960px`.
  Centered if there's extra room. The max-width is the Linear / Vercel
  reading-width convention — beyond 960px, scan-line length harms
  legibility for prose, and we have prose (descriptions, AI suggestions).
- **Right panel (`SourceBlock`):** 360px fixed. Collapsible to 44px
  (icons-only rail) via a header toggle. **This is new** — currently
  the source breakdown lives inline at the top of the page; promoting
  it to a persistent right panel is the single biggest IA win.

### 3.2 Medium layout (960–1279px)

- Right `SourceBlock` collapses to 44px rail by default; clicking a tab
  opens it as an **overlay** (`position: absolute; right: 0; width: 360px`)
  with backdrop blur. Main content underneath does not reflow.
- Left sidebar stays 220px.

### 3.3 Narrow layout (<960px — likely a laptop with a sidebar IDE open)

- Both side panels collapse. Left becomes a hamburger drawer (Streamlit
  default sidebar behavior). Right `SourceBlock` becomes a bottom sheet
  triggered by a "Sources" tab at the bottom of the `HeroHeader`.
- Main column goes full-width with 16px page gutters.

### 3.4 Sticky behavior

- `HeroHeader` is **NOT sticky**. It's tall (~180px) and would steal too
  much vertical space. Instead, a **compact sticky header** appears
  (32px tall) on scroll past the hero, showing: table name (mono) + tier
  badge + a small "back to top" affordance. Linear does this; it works.
- Left sidebar is sticky always.
- Right panel is sticky always.

### 3.5 Sticky compact header spec

```
┌──────────────────────────────────────────────────────────────────────┐
│ custins_customer_insights_cardmember  [● GROUNDED · 0.91]   ↑ Top    │
└──────────────────────────────────────────────────────────────────────┘
```
Height 32px, background `--surface-0 @ 80%` with `backdrop-filter: blur(12px)`,
border-bottom `--border-subtle`.

---

## 4. Motion & micro-interactions

### 4.1 Motion principles for data engineers

Engineers tolerate motion that *informs them about state* and resent
motion that *performs polish*. Every animation in Synapse must answer
one of two questions: "did my interaction register?" or "where did the
content I was looking at go?"

### 4.2 Durations

```css
--motion-instant: 0ms;     /* state swaps that should NOT animate */
--motion-fast:    80ms;    /* hover state changes */
--motion-default: 120ms;   /* expand/collapse, panel slide */
--motion-slow:    200ms;   /* page-level transitions (rare) */
--motion-ease:    cubic-bezier(0.2, 0.0, 0.0, 1.0);  /* fast-out, slow-in */
```

**No springs. No bounces. No keyframe loops.** This isn't a marketing site.

### 4.3 What animates, what doesn't

**Animates:**
- ColumnCard expand/collapse: 120ms ease, height-only (no opacity fade).
- SourceBlock tab switch: cross-fade panel body at 80ms.
- Sticky compact header: 80ms slide-down on scroll past hero.
- ConfidenceBadge tooltip: 0ms in, 80ms out (Linear pattern — instant
  reveal, gentle dismiss).
- Focus ring: 80ms ease in (the eye must catch where focus went).
- Tier-count numbers in left sidebar when graph rebuilds: 200ms count-up
  via `requestAnimationFrame`. This is the ONE place where motion is
  decorative-yet-informative — it tells you "yes, the rebuild finished
  and the numbers updated."

**Does NOT animate:**
- Table selector dropdown options.
- Hover state on rows (instant `background-color` swap).
- DQRuleRow status badge changes after a re-run (instant — the user
  just clicked refresh, motion would feel like fake loading).
- ConfidenceBadge color changes (instant).
- Page navigation between tables (instant; we are not React Router).

### 4.4 Reduce-motion compliance

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

Card expansion stays functional (just no height tween). Sticky header
appears instantly. Engineers who enable reduce-motion in macOS settings
get an immediate, jump-cut experience and we respect that.

### 4.5 Haptic / sound — none

This is a desktop web app. No audio, no haptics. Period.

### 4.6 Micro-interactions worth specifying

**Copy-on-click for identifiers** — every monospace identifier (table
name, column name, URI, formula token) gets a hover state showing a
small `copy` glyph 4px to the right. Clicking the identifier copies the
literal text to the clipboard and shows a 1-second toast in the bottom-
right corner: "Copied: cm_account_token". This is the highest-utility
micro-interaction in the entire app for engineers who paste identifiers
into SQL clients all day.

**Hover-to-reveal "why" on confidence badges** — after 400ms hover,
tooltip shows the calibration breakdown:
```
Sources weighted:
  human_approval × 1   = 10
  mdm            × 3   = 9     (capped at 5 events)
  bq             × 2   = 8
  metric_catalog × 1   = 5
  ─────────────────────────
  weighted total  = 32
  score = min(0.99, 32/15) = 0.91 → GROUNDED
```
This is the **trust-the-system** moment. Senior engineers will not
believe a confidence tier unless they can see why. Show your work.

---

## 5. Iconography

### 5.1 Family: Lucide

**Decision: Lucide** (https://lucide.dev) over Phosphor, SF Symbols, or
custom glyphs.

- **vs SF Symbols:** SFSymbols are Apple-licensed and don't render
  reliably as web fonts on Windows / Linux dev laptops. Inconsistent
  rendering is worse than slightly less expressive icons.
- **vs Phosphor:** Lucide is slightly less stylized (Phosphor leans
  whimsical with its rounded corners), which suits an engineering tool.
- **vs Heroicons:** Lucide has the broader coverage we need
  (`database-zap`, `git-branch`, `sparkles`, `binary`, etc.).
- **vs custom glyphs:** never. Building custom glyphs for a tool with
  a 12-week scope is a luxury we cannot afford and an accessibility
  failure mode we cannot tolerate (custom glyphs need ARIA labels
  everywhere; Lucide ships them).

Stroke weight: 1.5px. Size: 14px default, 16px in `HeroHeader`, 11px
inside `SourcePill`, 18px in `SourceBlock` rail.

### 5.2 Node-type glyph map

| Node type          | Lucide glyph        | Tint family |
|--------------------|---------------------|-------------|
| `Table`            | `table-2`           | neutral     |
| `Column`           | `columns-3`         | neutral     |
| `Entity`           | `box`               | neutral     |
| `Metric`           | `function-square`   | accent      |
| `Synonym`          | `bookmark`          | catalog     |
| `User`             | `user-round`        | corpus      |
| `CodeMapping`      | `binary`            | catalog     |
| `FilterValue`      | `filter`            | corpus      |
| `DataQualityRule`  | `shield-check`      | runtime     |

### 5.3 Edge-type glyph map

Edge glyphs appear in the `LineageRail` and in the per-column "related
tables" sub-section. They are 12px stroke-1.5.

| Edge type        | Lucide glyph         |
|------------------|----------------------|
| `CONTAINS`       | `corner-down-right`  |
| `IDENTIFIES`     | `key-round`          |
| `RELATES_TO`     | `link`               |
| `EQUIVALENT_TO`  | `equal`              |
| `COMPUTED_FROM`  | `calculator`         |
| `SLICEABLE_BY`   | `slice`              |
| `HAS_SYNONYM`    | `arrow-right-left`   |
| `ALWAYS_FILTER`  | `filter`             |
| `QUERIED_BY`     | `terminal`           |
| `RESOLVED_BY`    | `git-pull-request`   |
| `LOOKUP_TO`      | `external-link`      |
| `UPSTREAM_OF`    | `arrow-down-from-line` (rotates 180° for downstream) |
| `VALIDATED_BY`   | `shield-check`       |

### 5.4 Source glyph map (the 10)

| Source            | Family   | Lucide glyph        | Rationale |
|-------------------|----------|---------------------|-----------|
| `mdm`             | catalog  | `database`          | the canonical metadata store |
| `corpus`          | corpus   | `scroll-text`       | "tribal SQL" — scrolls of usage |
| `bq`              | runtime  | `database-zap`      | actual warehouse, live |
| `baseline_lookml` | catalog  | `ruler`             | the dimensional model "rule" |
| `glossary`        | catalog  | `book-marked`       | the canonical vocabulary |
| `metric_catalog`  | catalog  | `target`            | metric definitions |
| `table_catalog`   | catalog  | `library`           | the table directory |
| `usage`           | corpus   | `activity`          | "what people actually query" |
| `dq_engine`       | runtime  | `shield-check`      | runtime rule evaluation |
| `llm_generated`   | AI       | `sparkles`          | universally read as AI |
| `human_approval`  | (catalog)| `user-check`        | the trump card |

Each glyph in a pill: family-tinted stroke at 11px / 1.5 weight, no fill.

---

## 6. Typography of code

### 6.1 Where identifiers appear & how they render

| Location                                | Treatment                                 |
|-----------------------------------------|-------------------------------------------|
| Column name in `ColumnCard` row 1       | mono 13px 540 `--fg-strong`, no background |
| Column name when referenced in prose    | mono 12px 460 `--fg-default` inline, with `--surface-3` background, 2px hpad, 2px radius — a "code chip" |
| Table FQN in `HeroHeader`               | mono 11px 460 `--fg-muted`, inline       |
| Table name in `HeroHeader` title        | mono 28px 620 -0.4px tracking, `--fg-strong` (display variant) |
| SQL formula in `MetricCard`             | mono 12px 460 in code-block (bg `--surface-3`, 12px padding, radius `--radius-sm`) with syntax highlighting |
| DQ threshold expression                 | mono 11px 460 `--fg-muted` inline, no bg |
| URI (e.g. `synapse://table/foo`) in raw JSON | mono 11px 420 `--fg-muted` inside `st.json` (we override Streamlit's default) |
| Code resolution `005` → `Platinum`      | mono 12px 540 for `005` (in a chip), then `→` glyph, then sans 12px 420 for `Platinum` |

### 6.2 The "code chip" rule

Whenever a column or table identifier appears **inline within prose**,
it wears a code chip — a `--surface-3` background, 2px horizontal
padding, 2px radius. This is how dbt docs, GitHub READMEs, and Stripe
API docs handle inline code. It's the single typographic affordance
that prevents identifiers from disappearing into sentences.

```
Good:   The [cm_account_token] column joins to [cm_accounts].
        (chips around identifiers)

Bad:    The cm_account_token column joins to cm_accounts.
        (identifiers indistinguishable from prose)
```

### 6.3 Numbers

All numbers in the UI use **tabular-numerals** so they don't jiggle
when values change:

```css
.tabular { font-variant-numeric: tabular-nums; }
```

Apply to: stat grid values in `HeroHeader`, count-up animations in
sidebar, distinct counts in column rows, DQ rule run values.

### 6.4 SQL syntax highlighting

Limit to 4 token classes:

| Token       | Color (light)     | Color (dark)      |
|-------------|-------------------|-------------------|
| keyword     | `--accent-fg`     | `--accent-fg`     |
| identifier  | `--fg-strong`     | `--fg-strong`     |
| string      | `confidence.grounded.fg` | `confidence.grounded.fg` |
| comment     | `--fg-muted`      | `--fg-muted`      |

That's it. No rainbow. No italic strings. SQL is not Lisp.

---

## 7. Empty states & error states

Absence is information. Every empty state must answer two questions:
"what would normally be here?" and "what could I do to populate it?"

### 7.1 No DQ rules

```
┌──────────────────────────────────────────────────────────────────┐
│       ◇                                                          │
│                                                                  │
│   No data-quality rules attached yet.                            │
│                                                                  │
│   dq_engine has not contributed evidence for this table. Auto-   │
│   DQ runs nightly — if this is unexpected, the table may have    │
│   been added in the last 24h.                                    │
│                                                                  │
│   [ Suggest rules from profile ]  [ View Auto-DQ schedule ]      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

- Outer block: `--surface-1`, dashed `--border-default` 1px, padding
  32px, centered text.
- Icon: 24px Lucide `shield-off` in `--fg-muted`.
- Headline: sans 13px 540 `--fg-default`.
- Body: sans 12px 420 `--fg-muted`, max-width 480px.
- Actions: ghost buttons (transparent bg, `--border-default` border,
  `--accent-fg` text).

### 7.2 No lineage upstream

Inline within the `LineageRail` upstream column:

```
■  Root table
   No upstream sources observed. mdm_lineage has no UPSTREAM_OF edges
   pointing to this table. Either this is genuinely a root, or the
   lineage source hasn't crawled it yet.
```

Black square glyph (■) signals "terminus" — borrowed from graph theory
visual conventions. Body in `--fg-muted` 11px.

### 7.3 No metrics

Skipped silently (not every table has metrics). The current code already
does this with the `if not metrics: return`. Keep that — empty by
omission is correct here because metrics are a sparse facet, not a
required facet.

### 7.4 No related tables

Inline note within the section: "No JOIN observations link this table
to others in the corpus. Either it's a leaf reference table, or the
corpus doesn't yet contain queries that join it." 12px `--fg-muted`,
no surrounding card.

### 7.5 Table not found (error)

The current code's `st.error(f"{inspection['error']}: ...")` is too
terse and red-rectangle. Replace with:

```
┌──────────────────────────────────────────────────────────────────┐
│   ⌀  Table not found                                             │
│                                                                  │
│   The graph store does not contain a Table node with URI         │
│   synapse://table/foo_bar_baz                                    │
│                                                                  │
│   Available tables containing "foo":                             │
│     · foo_accounts                                                │
│     · foo_addresses                                               │
│                                                                  │
│   [ Show all 30 tables ]  [ Rebuild graph from sources ]         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

- Slashed-circle glyph in `confidence.deprecated.dot`.
- Left rail of the block: 3px solid `confidence.deprecated.dot`.
- Background: `--surface-1` (NOT a red fill — red fills look like
  alerts; this is an information failure, not a system alert).
- The fuzzy-match list uses the `inspection["available"]` already
  returned by the inspector — we already have the data, we just need
  to show it.

### 7.6 Rebuild in progress

Streamlit's `show_spinner="Generating sources..."` is fine but too
visually busy. Replace with a skeleton: render the page layout
(HeroHeader, sections) with grey shimmer placeholders. The user sees
the shape of the answer before the answer arrives.

Skeleton CSS:
```css
.skel {
  background: linear-gradient(90deg,
    var(--surface-2) 0%,
    var(--surface-3) 50%,
    var(--surface-2) 100%);
  background-size: 200% 100%;
  animation: skel-shimmer 1.4s linear infinite;
  border-radius: 4px;
  color: transparent;
}
@keyframes skel-shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}
```

### 7.7 Conflicting sources

When `node.provenance.conflicts` is non-empty, the `ConfidenceBadge`
gets a small amber dot in its upper-right corner (a "warning" affordance)
and the `SourceBlock` shows a "Conflicts" tab at the bottom of the rail
in amber. The conflicts list is rendered in a callout with the same
treatment as the DQ warning state (left rail amber, `--surface-1` bg,
12px padding).

---

## 8. Streamlit feasibility matrix

Tagging each spec item. Legend: ✅ native · 🎨 CSS-via-markdown ·
🧩 components.html · ⛔ different stack.

| Spec item                              | Tag | Notes |
|----------------------------------------|-----|-------|
| CSS variables for colors / spacing     | 🎨  | inject `<style>` once via `st.markdown(unsafe_allow_html=True)` at app top |
| Inter / JetBrains Mono fonts           | 🎨  | inject `@import` from Google Fonts; pre-load via `<link rel=preload>` |
| Type scale                             | 🎨  | CSS classes only |
| ConfidenceBadge                        | 🎨  | inline HTML span + CSS |
| ConfidenceBadge tooltip with calibration math | 🎨 | `<details>` or native `title` attr fallback; richer requires 🧩 |
| SourcePill                             | 🎨  | inline HTML span |
| Lucide glyphs                          | 🎨  | inline SVG sprite OR per-icon SVG; CDN fetch is fine |
| ColumnCard collapsed row               | 🎨  | HTML `<details>` element — disclosure WITHOUT Streamlit re-run |
| ColumnCard expanded panel              | 🎨  | continues HTML inside `<details>` body |
| MetricCard with syntax-highlighted SQL | 🎨  | `st.code(language="sql")` styled via CSS; or 🧩 for full token color control |
| SourceBlock (right rail)               | 🎨  | `position: fixed` div; tab state via `st.session_state` |
| SourceBlock keyboard nav `[ ]`         | 🧩  | needs JS keydown handler — only inside an iframe |
| LineageRail two-column with center label | 🎨 | CSS flex + pseudo-element |
| DQRuleRow with status left-rail        | 🎨  | HTML |
| HeroHeader stat grid                   | 🎨  | CSS grid |
| Sticky compact header                  | 🎨  | `position: sticky` + scroll listener — works in Streamlit (sticky element inside main column) |
| Sticky compact header reacts to scroll | 🧩  | scroll detection requires JS; ✅ if "always visible" with no scroll trigger |
| Copy-on-click for identifiers          | 🧩  | clipboard API requires JS; or use Streamlit's `st.code` which has a built-in copy button (✅ but lower-fi) |
| Tier count-up animation in sidebar     | 🧩  | requestAnimationFrame requires JS; ✅ degraded to instant value swap |
| Skeleton shimmer loading states        | 🎨  | pure CSS keyframes |
| Reduce-motion compliance               | 🎨  | pure CSS media query |
| Right SourceBlock as fixed panel       | 🎨  | doable but fights Streamlit's main-column layout; safer is 🧩 (iframe with `height` set) |
| Drag-to-resize panels                  | ⛔  | not in Streamlit's layout model; would need full Next.js or similar |
| Real D3/Cytoscape lineage graph        | ⛔  | (or 🧩 with a vis-network/cytoscape.js iframe — possible but heavy) |
| Real keyboard shortcut system (cmd-k)  | 🧩  | iframe-only; or move to a richer stack later |
| Dark/light mode toggle from a button   | 🎨  | `data-theme` attr on root + CSS variable swap; toggle button is `st.button` that mutates session_state |

**Bottom line:** ~85% of this spec ships in Streamlit via injected CSS
alone. The remaining 15% — keyboard nav, scroll-triggered sticky header,
clipboard, count-up — degrades gracefully (works without JS, better
with). The 3 items tagged ⛔ are not blockers for v1; they're flags for
when we outgrow Streamlit and migrate to Next.js + shadcn/ui (the
natural successor stack for this aesthetic).

---

## 9. Paste-ready CSS block

Drop this at the top of `synapse_ui.py` after `st.set_page_config(...)`:

```python
st.markdown(SYNAPSE_CSS, unsafe_allow_html=True)
```

where `SYNAPSE_CSS` is:

```html
<style>
/* ─── Font loading ──────────────────────────────────────── */
@import url('https://rsms.me/inter/inter.css');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;460;540;620&display=swap');

/* ─── Design tokens — light (default) ───────────────────── */
:root {
  --font-sans:    'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  --font-mono:    'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace;
  --font-display: 'Inter', -apple-system, sans-serif;

  /* Neutrals */
  --surface-0:        #FFFFFF;
  --surface-1:        #FAFAFA;
  --surface-2:        #F4F4F5;
  --surface-3:        #EEEEF0;
  --border-subtle:    #E8E8EB;
  --border-default:   #D4D4D8;
  --border-strong:    #A1A1AA;
  --fg-muted:         #71717A;
  --fg-default:       #3F3F46;
  --fg-strong:        #18181B;

  /* Accent — electric indigo */
  --accent-bg:        #EEF0FF;
  --accent-border:    #A5AEFF;
  --accent-fg:        #4F46E5;
  --accent-solid:     #4338CA;

  /* Confidence tiers */
  --conf-deprecated-fg:  #7A1F1F;
  --conf-deprecated-bg:  #FEECEC;
  --conf-deprecated-dot: #DC2626;
  --conf-guessed-fg:     #71717A;
  --conf-guessed-bg:     #F4F4F5;
  --conf-guessed-dot:    #A1A1AA;
  --conf-inferred-fg:    #92400E;
  --conf-inferred-bg:    #FEF3C7;
  --conf-inferred-dot:   #F59E0B;
  --conf-grounded-fg:    #155E75;
  --conf-grounded-bg:    #CFFAFE;
  --conf-grounded-dot:   #06B6D4;
  --conf-human-fg:       #14532D;
  --conf-human-bg:       #DCFCE7;
  --conf-human-dot:      #22C55E;

  /* DQ severity */
  --dq-pass-fg:    #15803D;  --dq-pass-bg:    #DCFCE7;
  --dq-warn-fg:    #B45309;  --dq-warn-bg:    #FEF3C7;
  --dq-fail-fg:    #B91C1C;  --dq-fail-bg:    #FEE2E2;
  --dq-unknown-fg: #71717A;  --dq-unknown-bg: #F4F4F5;

  /* Source family tints */
  --src-catalog: #7C3AED;
  --src-runtime: #0891B2;
  --src-corpus:  #0D9488;
  --src-ai:      #DB2777;

  /* Spacing — 4pt grid */
  --space-1: 2px;  --space-2: 4px;  --space-3: 8px;  --space-4: 12px;
  --space-5: 16px; --space-6: 24px; --space-7: 32px; --space-8: 48px;

  /* Radii */
  --radius-xs: 4px; --radius-sm: 6px; --radius-md: 8px; --radius-lg: 12px;

  /* Shadows */
  --shadow-rest:  0 0 0 1px var(--border-subtle);
  --shadow-hover: 0 0 0 1px var(--border-default), 0 1px 2px rgba(0,0,0,0.04), 0 4px 8px rgba(0,0,0,0.02);
  --shadow-pop:   0 0 0 1px var(--border-default), 0 4px 12px rgba(0,0,0,0.08), 0 16px 32px rgba(0,0,0,0.04);

  /* Motion */
  --motion-fast:    80ms;
  --motion-default: 120ms;
  --motion-ease:    cubic-bezier(0.2, 0.0, 0.0, 1.0);
}

/* ─── Dark theme — same vars, different values ──────────── */
@media (prefers-color-scheme: dark) {
  :root {
    --surface-0:        #0A0A0B;
    --surface-1:        #101113;
    --surface-2:        #161719;
    --surface-3:        #1C1D20;
    --border-subtle:    #202125;
    --border-default:   #2A2B2F;
    --border-strong:    #3F4046;
    --fg-muted:         #71727A;
    --fg-default:       #C9CACE;
    --fg-strong:        #F4F4F6;

    --accent-bg:        #1E1F4A;
    --accent-border:    #4F56CF;
    --accent-fg:        #A5B0FF;
    --accent-solid:     #6366F1;

    --conf-deprecated-fg:  #FCA5A5; --conf-deprecated-bg: #3A1414;
    --conf-guessed-fg:     #A1A1AA; --conf-guessed-bg:    #1C1D20;
    --conf-inferred-fg:    #FCD34D; --conf-inferred-bg:   #3A2810;
    --conf-grounded-fg:    #67E8F9; --conf-grounded-bg:   #0E3A45;
    --conf-human-fg:       #86EFAC; --conf-human-bg:      #0E2E1A;

    --src-catalog: #A78BFA; --src-runtime: #22D3EE;
    --src-corpus:  #5EEAD4; --src-ai:      #F472B6;
  }
}

/* ─── Global reset / base ───────────────────────────────── */
html, body, [class*="css"], .stApp {
  font-family: var(--font-sans);
  font-feature-settings: 'cv11', 'ss01', 'ss03';
  font-variant-ligatures: none;
  color: var(--fg-default);
  background: var(--surface-0);
  font-size: 13px;
  line-height: 20px;
}
code, pre, .mono, [class*="mono"] {
  font-family: var(--font-mono);
  font-feature-settings: 'calt' 0;
  font-variant-ligatures: none;
  font-size: 12px;
  line-height: 18px;
}
.tabular { font-variant-numeric: tabular-nums; }

/* ─── Streamlit element overrides ───────────────────────── */
.stApp > header { background: transparent; }
section[data-testid="stSidebar"] {
  background: var(--surface-1);
  border-right: 1px solid var(--border-subtle);
}
.stMarkdown h1 { font-size: 20px; line-height: 28px; font-weight: 600; letter-spacing: -0.2px; color: var(--fg-strong); margin: 0; }
.stMarkdown h2 { font-size: 15px; line-height: 22px; font-weight: 600; color: var(--fg-strong); margin-top: var(--space-6); margin-bottom: var(--space-3); }
.stMarkdown h3, .stMarkdown h4 { font-size: 13px; line-height: 20px; font-weight: 540; color: var(--fg-strong); margin-top: var(--space-5); margin-bottom: var(--space-3); }
.stMarkdown p { font-size: 13px; line-height: 20px; }
.stMarkdown code { font-family: var(--font-mono); font-size: 12px; background: var(--surface-3); color: var(--fg-strong); padding: 1px 4px; border-radius: 2px; }
.stCodeBlock, .stCodeBlock pre { background: var(--surface-3) !important; border-radius: var(--radius-sm) !important; border: 1px solid var(--border-subtle) !important; }
hr, .stDivider { border-color: var(--border-subtle) !important; opacity: 1; }
[data-testid="stMetricValue"] { font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--fg-strong); }
[data-testid="stMetricLabel"] { font-family: var(--font-mono); font-size: 10px; letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted); }

/* ─── ConfidenceBadge ───────────────────────────────────── */
.synapse-badge {
  display: inline-flex; align-items: center; gap: 6px;
  height: 22px; padding: 0 8px;
  font-family: var(--font-mono); font-size: 10px; line-height: 14px;
  font-weight: 540; letter-spacing: 0.4px;
  border-radius: var(--radius-xs);
  border: 1px solid;
}
.synapse-badge .dot { width: 8px; height: 8px; border-radius: 50%; }
.synapse-badge .score, .synapse-badge .count { font-weight: 460; opacity: 0.85; }
.synapse-badge[data-tier="deprecated"]    { background: var(--conf-deprecated-bg); color: var(--conf-deprecated-fg); border-color: color-mix(in srgb, var(--conf-deprecated-fg) 18%, transparent); }
.synapse-badge[data-tier="deprecated"] .dot { background: var(--conf-deprecated-dot); }
.synapse-badge[data-tier="guessed"]       { background: var(--conf-guessed-bg);    color: var(--conf-guessed-fg);    border-color: color-mix(in srgb, var(--conf-guessed-fg)    18%, transparent); }
.synapse-badge[data-tier="guessed"] .dot    { background: var(--conf-guessed-dot); }
.synapse-badge[data-tier="inferred"]      { background: var(--conf-inferred-bg);   color: var(--conf-inferred-fg);   border-color: color-mix(in srgb, var(--conf-inferred-fg)   18%, transparent); }
.synapse-badge[data-tier="inferred"] .dot   { background: var(--conf-inferred-dot); }
.synapse-badge[data-tier="grounded"]      { background: var(--conf-grounded-bg);   color: var(--conf-grounded-fg);   border-color: color-mix(in srgb, var(--conf-grounded-fg)   18%, transparent); }
.synapse-badge[data-tier="grounded"] .dot   { background: var(--conf-grounded-dot); }
.synapse-badge[data-tier="human_asserted"]{ background: var(--conf-human-bg);      color: var(--conf-human-fg);      border-color: color-mix(in srgb, var(--conf-human-fg)      18%, transparent); }
.synapse-badge[data-tier="human_asserted"] .dot { background: var(--conf-human-dot); }
.synapse-badge.large { height: 26px; font-size: 12px; padding: 0 10px; }

/* ─── SourcePill ────────────────────────────────────────── */
.synapse-pill {
  display: inline-flex; align-items: center; gap: 4px;
  height: 20px; padding: 0 6px;
  background: var(--surface-1); border: 1px solid var(--border-subtle);
  border-radius: var(--radius-xs);
  font-family: var(--font-mono); font-size: 11px; line-height: 14px;
  color: var(--fg-default);
  transition: background var(--motion-fast) var(--motion-ease),
              border-color var(--motion-fast) var(--motion-ease);
}
.synapse-pill:hover { background: var(--surface-2); border-color: var(--border-default); }
.synapse-pill .glyph { display: inline-flex; width: 11px; height: 11px; }
.synapse-pill[data-family="catalog"] .glyph { color: var(--src-catalog); }
.synapse-pill[data-family="runtime"] .glyph { color: var(--src-runtime); }
.synapse-pill[data-family="corpus"]  .glyph { color: var(--src-corpus); }
.synapse-pill[data-family="ai"]      .glyph { color: var(--src-ai); }
.synapse-pill.muted { opacity: 0.35; border-color: transparent; }
.synapse-pill.muted .glyph { color: var(--fg-muted); }

/* ─── ColumnCard ────────────────────────────────────────── */
.synapse-col {
  display: block; padding: 0;
  border-bottom: 0.5px solid var(--border-subtle);
  background: var(--surface-1);
  cursor: pointer;
}
.synapse-col:hover { background: var(--surface-2); }
.synapse-col[open] { background: var(--surface-1); }
.synapse-col[open] > .body { border-left: 2px solid var(--accent-border); padding-left: 14px; }
.synapse-col > summary {
  list-style: none; display: grid;
  grid-template-columns: 16px minmax(0, 1fr) auto auto;
  align-items: center; gap: var(--space-3);
  padding: 12px var(--space-5); min-height: 56px;
}
.synapse-col > summary::-webkit-details-marker { display: none; }
.synapse-col .triangle {
  width: 8px; height: 8px;
  border-left: 4px solid transparent; border-right: 4px solid transparent;
  border-top: 5px solid var(--fg-muted);
  transition: transform var(--motion-fast) var(--motion-ease);
}
.synapse-col[open] .triangle { transform: rotate(-90deg); }
.synapse-col .name { font-family: var(--font-mono); font-size: 13px; font-weight: 540; color: var(--fg-strong); }
.synapse-col .type-chip {
  display: inline-block; padding: 1px 4px; margin-left: 6px;
  background: var(--surface-3); color: var(--fg-muted);
  font-family: var(--font-mono); font-size: 11px; border-radius: 2px;
}
.synapse-col .biz { color: var(--fg-muted); font-size: 12px; margin-top: 2px; }
.synapse-col .flag {
  display: inline-flex; width: 16px; height: 16px; align-items: center; justify-content: center;
  color: var(--fg-muted);
}
.synapse-col .flag[data-flag="pii"]  { color: var(--conf-deprecated-dot); }
.synapse-col .flag[data-flag="pk"]   { color: var(--accent-fg); }
.synapse-col .body { padding: 0 var(--space-5) var(--space-5) 16px; }
.synapse-col[data-failing-dq="true"] { border-left: 2px solid var(--dq-fail-fg); }

/* AI-suggested description block */
.synapse-ai {
  border-left: 2px solid var(--src-ai); padding: 8px 12px;
  background: color-mix(in srgb, var(--src-ai) 6%, transparent);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  font-style: italic; font-size: 12px; color: var(--fg-default); opacity: 0.9;
}

/* ─── MetricCard ────────────────────────────────────────── */
.synapse-metric {
  background: var(--surface-1); box-shadow: var(--shadow-rest);
  border-radius: var(--radius-md); padding: var(--space-5);
  margin-bottom: var(--space-4);
}
.synapse-metric .head { display: flex; align-items: baseline; gap: var(--space-3); justify-content: space-between; margin-bottom: var(--space-3); }
.synapse-metric .tech { font-family: var(--font-mono); font-size: 13px; font-weight: 540; color: var(--fg-strong); }
.synapse-metric .biz  { font-size: 13px; color: var(--fg-default); }
.synapse-metric .formula {
  background: var(--surface-3); padding: 12px; border-radius: var(--radius-sm);
  font-family: var(--font-mono); font-size: 12px; line-height: 18px;
  color: var(--fg-strong); white-space: pre; overflow-x: auto;
  margin: var(--space-3) 0;
}
.synapse-metric .meta { font-size: 12px; color: var(--fg-muted); margin: var(--space-3) 0; }
.synapse-metric .meta .label { font-family: var(--font-mono); font-size: 11px; font-weight: 540; color: var(--fg-default); margin-right: 4px; }
.synapse-chip {
  display: inline-flex; align-items: center; height: 18px; padding: 0 6px;
  background: var(--surface-2); border: 1px solid var(--border-subtle);
  border-radius: var(--radius-xs);
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-default);
  margin-right: 4px;
}

/* ─── HeroHeader ────────────────────────────────────────── */
.synapse-hero {
  background: var(--surface-1); box-shadow: var(--shadow-rest);
  border-radius: var(--radius-md); padding: var(--space-6);
  margin-bottom: var(--space-6);
}
.synapse-hero .eyebrow {
  font-family: var(--font-mono); font-size: 11px; font-weight: 540;
  letter-spacing: 0.6px; text-transform: uppercase;
  color: var(--fg-muted); margin-bottom: var(--space-4);
}
.synapse-hero .title-row { display: flex; align-items: baseline; justify-content: space-between; gap: var(--space-5); margin-bottom: var(--space-3); }
.synapse-hero .title {
  font-family: var(--font-mono); font-size: 28px; line-height: 36px;
  font-weight: 620; letter-spacing: -0.4px; color: var(--fg-strong);
}
.synapse-hero .desc { font-size: 13px; line-height: 20px; color: var(--fg-default); max-width: 720px; margin-bottom: var(--space-4); }
.synapse-hero .sources-row { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: var(--space-5); }
.synapse-hero .stat-grid {
  display: grid; grid-template-columns: repeat(5, 1fr);
  border-top: 1px solid var(--border-subtle); padding-top: var(--space-4);
}
.synapse-hero .stat { padding: 0 var(--space-4); border-left: 1px solid var(--border-subtle); }
.synapse-hero .stat:first-child { padding-left: 0; border-left: none; }
.synapse-hero .stat-label {
  font-family: var(--font-mono); font-size: 11px; font-weight: 540;
  letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted);
}
.synapse-hero .stat-value {
  font-family: var(--font-mono); font-size: 14px; font-weight: 540;
  color: var(--fg-strong); font-variant-numeric: tabular-nums; margin-top: 2px;
}

/* ─── LineageRail ───────────────────────────────────────── */
.synapse-lineage { display: grid; grid-template-columns: 1fr 1fr; gap: var(--space-6); position: relative; }
.synapse-lineage::before {
  content: 'this table'; position: absolute; left: 50%; top: 50%;
  transform: translate(-50%, -50%);
  font-family: var(--font-mono); font-size: 10px; color: var(--fg-muted);
  background: var(--surface-0); padding: 0 6px;
}
.synapse-lineage .col-head {
  font-family: var(--font-mono); font-size: 11px; font-weight: 540;
  letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted);
  margin-bottom: var(--space-3);
}
.synapse-lineage .item {
  background: var(--surface-1); padding: 8px 12px;
  border-radius: var(--radius-sm); box-shadow: var(--shadow-rest);
  margin-bottom: 4px;
}
.synapse-lineage .item .name { font-family: var(--font-mono); font-size: 12px; font-weight: 540; color: var(--fg-strong); }
.synapse-lineage .item .sub  { font-family: var(--font-mono); font-size: 10px; color: var(--fg-muted); margin-top: 2px; }
.synapse-lineage .item .sub::before { content: '↳ '; }

/* ─── DQRuleRow ─────────────────────────────────────────── */
.synapse-dq { display: block; padding: 10px var(--space-5);
  border-bottom: 0.5px solid var(--border-subtle); background: var(--surface-1); }
.synapse-dq[data-status="fail"]    { border-left: 3px solid var(--dq-fail-fg); }
.synapse-dq[data-status="warning"] { border-left: 3px solid var(--dq-warn-fg); }
.synapse-dq[data-status="pass"]    { border-left: 3px solid var(--dq-pass-fg); }
.synapse-dq[data-status="unknown"] { border-left: 3px solid var(--dq-unknown-fg); }
.synapse-dq .row1 { display: grid; grid-template-columns: 20px auto auto 1fr auto auto; gap: var(--space-3); align-items: center; }
.synapse-dq .kind { font-family: var(--font-mono); font-size: 12px; font-weight: 540; color: var(--fg-strong); }
.synapse-dq .target { font-family: var(--font-mono); font-size: 12px; color: var(--fg-default); }
.synapse-dq .thresh { font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted); justify-self: end; }
.synapse-dq .ai-mark { color: var(--src-ai); width: 14px; height: 14px; }
.synapse-dq .sub { font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted); margin-left: 28px; margin-top: 2px; }
.synapse-dq:hover { background: var(--surface-2); }

/* ─── SourceBlock right panel ───────────────────────────── */
.synapse-source-panel {
  position: sticky; top: var(--space-5); align-self: flex-start;
  width: 360px; background: var(--surface-1); border-radius: var(--radius-md);
  box-shadow: var(--shadow-rest); display: grid; grid-template-columns: 44px 1fr;
  max-height: calc(100vh - 80px); overflow: hidden;
}
.synapse-source-panel .rail { display: flex; flex-direction: column; padding: 8px 0;
  border-right: 1px solid var(--border-subtle); }
.synapse-source-panel .rail-item {
  height: 36px; display: flex; align-items: center; justify-content: center;
  position: relative; cursor: pointer; color: var(--fg-muted);
}
.synapse-source-panel .rail-item.contrib { color: var(--src-catalog); }
.synapse-source-panel .rail-item.active { background: var(--accent-bg); color: var(--accent-fg); }
.synapse-source-panel .rail-item .status-dot {
  position: absolute; right: 8px; top: 12px; width: 6px; height: 6px; border-radius: 50%;
  background: var(--conf-guessed-dot);
}
.synapse-source-panel .rail-item.contrib .status-dot { background: var(--conf-human-dot); }
.synapse-source-panel .rail-item.conflict .status-dot { background: var(--conf-inferred-dot); }
.synapse-source-panel .body { padding: var(--space-4); overflow-y: auto; }
.synapse-source-panel .body .head {
  font-family: var(--font-mono); font-size: 12px; color: var(--fg-muted);
  margin-bottom: var(--space-3);
}
.synapse-source-panel .body .fact-row {
  display: grid; grid-template-columns: 140px 1fr; gap: 12px; padding: 4px 0;
}
.synapse-source-panel .body .fact-row .k {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted);
}
.synapse-source-panel .body .fact-row .v {
  font-family: var(--font-mono); font-size: 12px; color: var(--fg-default);
  word-break: break-word;
}

/* ─── Sticky compact header ─────────────────────────────── */
.synapse-sticky {
  position: sticky; top: 0; z-index: 50;
  height: 32px; display: flex; align-items: center; gap: var(--space-3);
  padding: 0 var(--space-5);
  background: color-mix(in srgb, var(--surface-0) 80%, transparent);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border-subtle);
}

/* ─── Skeleton loading ──────────────────────────────────── */
.skel {
  background: linear-gradient(90deg, var(--surface-2) 0%, var(--surface-3) 50%, var(--surface-2) 100%);
  background-size: 200% 100%;
  animation: skel-shimmer 1.4s linear infinite;
  border-radius: 4px; color: transparent;
}
@keyframes skel-shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

/* ─── Empty states ──────────────────────────────────────── */
.synapse-empty {
  background: var(--surface-1); border: 1px dashed var(--border-default);
  border-radius: var(--radius-md); padding: 32px;
  text-align: center; color: var(--fg-muted);
}
.synapse-empty .icon { width: 24px; height: 24px; color: var(--fg-muted); margin-bottom: var(--space-3); }
.synapse-empty .head { font-size: 13px; font-weight: 540; color: var(--fg-default); margin-bottom: var(--space-2); }
.synapse-empty .body { font-size: 12px; max-width: 480px; margin: 0 auto var(--space-4) auto; line-height: 18px; }

/* ─── Reduce motion ─────────────────────────────────────── */
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}

/* ─── Hide Streamlit chrome we don't want ───────────────── */
#MainMenu, footer, [data-testid="stToolbar"] { visibility: hidden; }
</style>
```

---

## 10. ASCII wireframe — page above the fold

```
┌──────────────┬──────────────────────────────────────────────────────────┬───────────────────────────────┐
│ 🧠 SYNAPSE   │  TABLE · CARDMEMBER DOMAIN                                │  SOURCES (10)                 │
│              │                                                          │  ┌──┬─────────────────────┐   │
│ ▼ table picker│ custins_customer_insights_     [● GROUNDED · 0.91 · 7/10]│  │⌘ │ mdm                 │ ● │
│   custins_…  │ cardmember                                                │  │⌘ │ table_catalog       │ ● │
│              │                                                          │  │⌘ │ metric_catalog      │ ○ │
│ ─────────    │ Customer Insights Cardmember — the daily card-level      │  │⌘ │ glossary            │ ─ │
│              │ snapshot used by the customer-success pod for retention  │  │⌘ │ baseline_lookml     │ ● │
│ STATS        │ and engagement modeling.                                  │  │▶ │ bq                  │ ● │
│ Nodes  1.4K  │                                                          │  │▶ │ dq_engine           │ ● │
│ Edges  6.2K  │ ⌘mdm ⌘tbl ⌘mcat ⌘gloss ⌘lkml ▶bq ▶dq ⊙corpus ⊙usage ✨llm │  │⊙ │ corpus              │ ● │
│              │                                                          │  │⊙ │ usage               │ ● │
│ ─────────    │ ┌──────────┬──────────┬──────────┬──────────┬──────────┐ │  │✨│ llm_generated       │ ○ │
│ TIERS        │ │FRESHNESS │ROW COUNT │COLUMNS   │OWNER     │PII       │ │  └──┴─────────────────────┘   │
│ ● human   12 │ │2.4h ago  │124,481   │47        │cs-cust-i │6 cols    │ │                               │
│ ● ground  88 │ └──────────┴──────────┴──────────┴──────────┴──────────┘ │  ─── mdm · 3 events ───       │
│ ● infer   41 │                                                          │                               │
│ ● guess   19 │ ─────────────────────────────────────────────────────── │  business_name: Customer …    │
│ ● deprec   3 │ COLUMNS (47)                                             │  description:   The daily…    │
│              │                                                          │  owner_team:    cs-customer…  │
│              │ ▸ cm_account_token  STRING   🔑 PK 🔒 PII  [●GROUND] ⌘▶  │  partition:     dt            │
│              │   Card member account identifier (canonical)    124,481  │  row_count:     124,481       │
│              │ ─────────────────────────────────────────────────────── │                               │
│              │ ▸ dt                DATE     📅 PART       [●HUMAN]  ⌘▶  │  ─── conflicts with bq ───   │
│              │   Snapshot date                                  1,460  │  partition_field disagrees:   │
│              │ ─────────────────────────────────────────────────────── │  mdm says dt, bq says         │
│              │ ▸ status_code       STRING   🆔 CODED      [●INFER]  ⌘⊙  │  event_date                   │
│              │   Card status (5 known codes)                       5  │                               │
│              │ ─────────────────────────────────────────────────────── │                               │
│              │ … 44 more rows                                          │                               │
└──────────────┴──────────────────────────────────────────────────────────┴───────────────────────────────┘
   220px                       flex (720–960px)                                       360px
```

Key adjacencies the wireframe makes obvious:

1. **HeroHeader at top of main column** owns the most visual real estate
   on the page (~220px tall card). Confidence badge sits in the upper
   right of the title row — the eye lands on the table name, then
   travels right to the trust signal, then down to the sources row to
   see WHICH sources back it up, then down to the stat grid for the
   operational facts. This is the 3-second answer to "should I trust this?"

2. **SourceBlock right panel** lives adjacent to the hero. The user's
   eye moves from `[GROUNDED · 0.91 · 7/10]` in the hero straight right
   into the source rail to see which 7 of 10 sources contributed. The
   adjacency replaces today's "scroll down to the 7-source breakdown"
   pattern, which buries the trust evidence.

3. **The 10-source row in the hero IS the visual representation of
   "7 of 10 sources agree."** Non-contributing sources render in muted
   state. This is the single most important new visual idea: the
   confidence count becomes a row of glyphs, not a number.

4. **Columns list flows naturally below the hero**, with each row at
   56px tall — significantly denser than today's bordered containers.
   The user can scan ~12 columns above the fold instead of ~3.

5. **Left sidebar stays narrow** (220px) and contains nav + global
   stats + tier counts. Tier counts are color-coded so the user gets
   an at-a-glance read on the overall health of the graph.

---

## Closing rationale

The three visual moves that carry the most weight:

1. **Confidence-as-row-of-glyphs in the hero.** Replacing the numeric
   "7 of 10 sources agree" with a literal row of 10 source pills (3
   muted) makes the abstract concept of multi-source agreement
   immediately legible. Engineers see "WHICH 7" not just "7."

2. **The right-rail SourceBlock panel.** Promoting the 7-source
   breakdown from an inline accordion to a persistent right panel
   makes "click a source to see what it contributed" a 1-action
   gesture instead of a scroll-and-expand dance. It also makes
   side-by-side comparison feasible (switch between mdm and bq with `[`/`]`).

3. **The typography system: 13px Inter body + 12px JetBrains Mono
   identifiers, with code chips on inline references.** This alone
   transforms the page from "engineer-built Streamlit" to "Linear-
   adjacent product." It costs nothing to implement (one CSS block)
   and is what users will subconsciously register first.

The Streamlit-feasibility analysis confirms ~85% of this ships behind a
single injected `<style>` block. The remaining 15% (keyboard nav,
clipboard, scroll-triggered sticky) degrades gracefully and is the
right boundary for v1.
