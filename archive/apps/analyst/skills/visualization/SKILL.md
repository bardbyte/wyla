---
name: visualization
description: Constructs correct render_chart and render_dashboard specs — chart grammar, series limits, number formats, color and labeling rules. Use when building any chart or dashboard spec, after response-design has chosen the form.
---

# Visualization — constructing the spec

`render_chart` / `render_dashboard` take a JSON spec and return a themed
HTML artifact. The renderer enforces the hard rules (≤4 series, one axis,
validated palette); this skill covers the judgment the renderer can't.

## Spec shapes (exact)

```json
{"kind":"stat","title":"C-30 dollar roll rate","value":0.0231,
 "format":"percent","delta":0.0014,"delta_is_good":false,
 "delta_label":"MoM","subtitle":"Consumer · Jun 2026",
 "footnote":"sources: bq · skills · corpus"}

{"kind":"line","title":"C-30 trend","format":"percent",
 "series":[{"name":"Consumer","points":[["Jan",0.0205],["Feb",0.0211]]}],
 "caption":"6-month view · partition-filtered"}

{"kind":"bar","title":"Write-offs by quarter","format":"currency",
 "categories":["Q1","Q2","Q3"],
 "series":[{"name":"gross","points":[["Q1",1200000],["Q2",1400000],["Q3",1100000]]}],
 "stacked":false,"horizontal":false}

{"kind":"table","title":"By segment","columns":["segment","rate"],
 "rows":[["Consumer","2.31%"],["Small Biz","2.9%"]]}

{"kind":"dashboard","title":"Portfolio health — Jun 2026",
 "subtitle":"Consumer · graph v<snapshot>",
 "items":[{"spec":{...stat...},"span":4},{"spec":{...line...},"span":8}],
 "footer":"sources + snapshot version + guardrails honored"}
```

## Judgment rules

**Format follows the metric.** Rates → `percent` (pass fractions: 0.0231,
not 2.31). Money → `currency`. Rate *changes* → `bps` on the delta.
Counts → `number`.

**delta_is_good is about the business, not the sign.** Rising approval
rate → `true`; rising delinquency → `false`; ambiguous (volume) → omit
for neutral styling.

**Series naming:** business names ("Consumer", "Small Biz"), never column
codes. Series order = importance order; the palette assigns color by
position, and position 1 is what the eye reads first.

**Top-N:** `horizontal:true`, exactly one series, categories pre-sorted
descending, N ≤ 12. Longer tails belong in the table.

**Stacked bars** only when parts-of-whole is the actual question and parts
≤ 4; otherwise grouped or fold to "Other".

**Time on x:** keep source order (never re-sort months by value); label
density is handled by the renderer (first/middle/last).

**Dashboard composition (the only layout that works):**
1. Stat row on top — 2-3 tiles, `span 4` each. The scoreboard.
2. One evidence chart row — trend `span 8` + driver bar `span 4`, or a
   single `span 12`.
3. Optional detail table `span 12` (analyst audiences only).
4. `footer` = provenance line: sources, snapshot version, guardrails
   honored. Non-negotiable.

**Missing data:** use `null` points (gaps render honestly) — never
interpolate, never zero-fill a gap.

## Refusals you must respect

The renderer rejects >4 series, category/point mismatches, multi-series
horizontal bars. When it refuses, follow the error's instruction (fold to
Other, small multiples, one measure) — do not retry the same spec.
