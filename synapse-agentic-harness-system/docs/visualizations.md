# Visualizations — how a number reads and which picture answers

The charting and dashboarding rules of the assistant, in one place:
how a column of numbers formats itself from the data, how a chart kind
is chosen when the model does not choose one, every kind with its
encodings and an example, the keys that override, and how to add a
kind. The code is `sahs/assistant/artifacts.py` (`number_format`,
`format_value`, `choose_visual`, `CHART_KINDS`, `validate_artifact`,
`EXAMPLES`) and its JS twin `js/artifacts-render.js`, one file
identical on both surfaces (`apps/synapse_admin`, `apps/synapse`).
The tests are `tests/test_v3_artifacts.py` (the silo) and
`apps/synapse_admin/tests/test_artifacts_render.py` (both surfaces,
through node).

## 1 · Formatting from the data

`number_format(values, hint)` decides, per column, how its values read;
`format_value(value, fmt)` renders one. Every chart axis, tooltip,
table cell, KPI tile and PPTX cell goes through them. The inference
reads, in this order, the column's explicit `format`, its `unit`, its
name, then the values themselves — metadata speaks before the data.

| key | rule |
|---|---|
| `kind` | `percent`, `currency`, `count`, `ratio`, `duration`, `date`, `number`, `identifier`, `text`. From `format.kind`; else the unit (`%`, a currency code or symbol, `ms`/`s`/`min`/`h`/`d`, `x`, `count`); else the name (`_pct`, `_rate`, `share` → percent; `spend`, `revenue`, `amount`, `price` … → currency; `count`, `orders`, `users`, `n` … → count; `ratio`, `per`, `index` → ratio; `latency`, `duration`, `_ms`, `_days` → duration; `_id`, `code`, `zip`, `sku` → identifier; `date`, `month`, `_at` → date); else the values (≥ 80 % ISO dates → date; ≥ 80 % numbers → number; else text). A column named like a date whose values are numbers (`year`: 2024) is a plain, ungrouped number; one whose values are labels (`day`: Mon) is text. |
| `decimals` | the maximum decimals present in the data, capped at 4 — never a fixed default. Currency caps at 2; counts, identifiers, dates at 0. Float noise (`0.30000000000000004`) reads as one decimal. A percent given as a fraction counts its decimals after the ×100: `0.125` → 1 decimal → `12.5%`; given in points, its own: `12.5` → `12.5%`, `12` → `12%`. |
| `scale` | **the percent rule**: a percent column whose every value lies within `[-1, 1]` is a fraction and is multiplied by 100; a single value beyond 1 means the column is already in percentage points and is shown as given. (`[0.5, 1.5]` → `0.5%`, `1.5%`; `[0.5, 1.0]` → `50%`, `100%`.) `format.scale` (1 or 100) overrides. |
| `grouping` | thousands separators (`1,234,567`) for every numeric kind except identifiers, dates and the ungrouped year. |
| `compact` | `K` / `M` / `B` / `T` with one decimal (`1.2M`, `988K`, `12M`) when the median magnitude of the column exceeds 100,000 and the kind is a plain number, a count or a currency — never for percents, ratios, durations or identifiers. A KPI tile shows the compact value with the full one on hover; a table's summary strip likewise. |
| `unit` | `%` for percents; a currency symbol as a prefix (`$1,234.50`, `€1,500.0`) or an unknown code as a suffix (`1,500 CHF`); a duration's unit with a space (`120.5 ms`); any other unit as given (`12.5 kg`). |
| `sign` | `auto` shows the minus only; `always` shows `+` too (a delta). `-0.0` reads as `0.0`. |
| null | `—` for `null`, `undefined`, `""` and `NaN`; text and dates pass through as given. |

The shared cases are `tests/fixtures/number_format_cases.json`: the
Python test asserts them and the surface test runs the JS twin on the
same file, so a change to either side that breaks agreement fails.

## 2 · The selection heuristic

`choose_visual(rows, columns, intent)` returns `{kind, x, y, series,
sort, reason, …}`; `reason` is the one sentence the card shows under
the chart and on hover. The `artifact` tool calls it (through
`choose_for_series`) when a chart spec carries no `kind`, and
`chart_rows` (the "Chart these rows" chip) calls it on the saved rows;
a kind, an `x` or a `y` the person or the model names wins over it.
The branches, in the order they are tried:

| the rows look like | picks | encoding |
|---|---|---|
| no rows, or no numeric column | `table` | — |
| one row, one measure | `kpi` | `y = [measure]` |
| one row, a measure and a comparison (a column named `prev_`, `prior_`, `target`, `plan`, `budget`, `forecast`, `yoy` …, or the ask says *vs*, *against*, *compared*) | `kpi` with delta | `y`, `compare` |
| the ask says *distribution* / *histogram*, or one measure with no dimension and ≥ 20 rows | `histogram` | `y = [measure]` |
| a date column, a category with > 12 values | `small_multiples` | `x = date`, `series = category` |
| a date column and a category | `line` | one line per category |
| a date column, > 12 measures | `small_multiples` | one facet per measure |
| a date column, two measures of the same kind, one named like a plan | `combo` | actual as bars, plan as a line, one axis |
| a date column, > 6 measures | `small_multiples` | — |
| a date column, one measure, the ask says *area* / *volume* / *cumulative* | `area` | — |
| a date column | `line` | `x = date`, `y = measures`, `sort = x` |
| two categories and a measure, the pairs filling a grid ≤ 60 × 60 | `heatmap` | `series = rows' category`, `x = columns' category` |
| a category and a measure, the ask says *waterfall* / *bridge* / *walk* | `waterfall` | — |
| a category with ≤ 6 values and a percent measure, or the ask says *share* / *mix* / *breakdown* | `percent_bar` (percent measure) or `stacked_bar` | `series = category` — never a pie |
| a category with > 40 values | `table` with a top-15 `hbar` | `top_n = 15`, `sort = -y` |
| a category and > 12 measures | `small_multiples` | — |
| a category with > 8 values, or labels longer than 12 characters | `hbar` | `sort = -y` |
| a category and measures | `bar`, sorted by value | `x = category`, `y = measures (≤ 6)`, `sort = -y` |
| two measures and nothing to group by | `scatter` | `x = first measure`, `y = second` |
| anything else | `table` | — |

Rules the table rests on (from the dataviz method): a single number is
a tile, not a one-bar chart; bars start at zero and sort by value unless
the categories have their own order; part-of-whole is a stack, never a
pie; two measures of different scale get two charts or an index, never
two y axes; past six series a chart stops reading, so facet; colour is
assigned in a fixed order and never generated for a seventh series.

## 3 · Every kind

All series kinds share the spec `{kind, series: [{name, points: [[x, y],
…], dashed?, role?, unit?, format?}], unit?, format?, x_format?,
x_label?, y_label?, reference?: {value, label?}, sort?: x|-x|y|-y|none,
reason?, provenance}`; every series rides ONE shared x axis of labels, a
null y is a gap. Limits: 6 series (24 for small multiples), 2,000 points
per series (5,000 for a scatter), a 60 × 60 heatmap. `EXAMPLES` in
`artifacts.py` holds one valid spec per kind (the tests validate,
render and export every one); the shapes below are those examples,
shortened.

| kind | draws | required encoding | example |
|---|---|---|---|
| `line` | change over time; dashed for a forecast/target | series with points on shared x labels | `{"kind":"line","unit":"USD","series":[{"name":"Actuals","points":[["2026-01",802000],["2026-02",850000],["2026-03",900000],["2026-04",null]]},{"name":"Forecast","dashed":true,"points":[["2026-03",900000],["2026-04",930000]]}],"reference":{"value":950000,"label":"Plan"}}` |
| `area` | one series as a volume | one series | `{"kind":"area","unit":"count","series":[{"name":"Sessions","points":[["2026-01",1200],["2026-02",1350]]}]}` |
| `bar` | comparison across categories, grouped for several series | series; `sort: "-y"` for value order | `{"kind":"bar","unit":"USD","sort":"-y","series":[{"name":"Spend","points":[["US",512000],["CA",300000]]}]}` |
| `hbar` | the same, horizontal: long labels, many categories | series | `{"kind":"hbar","sort":"-y","series":[{"name":"Merchants","points":[["Grocery and supermarkets",1200],["Restaurants and bars",980]]}]}` |
| `stacked_bar` | parts of a whole per category, a 2px surface gap between segments | ≥ 1 series | `{"kind":"stacked_bar","series":[{"name":"Card present","points":[["Q1",300],["Q2",320]]},{"name":"Card not present","points":[["Q1",200],["Q2",240]]}]}` |
| `percent_bar` | the stack normalized to 100 % per category; the tooltip shows the share and the raw value | ≥ 1 series | `{"kind":"percent_bar","series":[{"name":"Approved","points":[["US",91],["CA",88]]},{"name":"Declined","points":[["US",9],["CA",12]]}]}` |
| `scatter` | one measure against another, one point per entity | numeric x AND y in every point | `{"kind":"scatter","x_label":"Transactions","y_label":"Spend","format":"currency","series":[{"name":"Merchants","points":[[120,4800.5],[340,12100]]}]}` |
| `histogram` | a distribution over bins | `values: [numbers]` (+ `bins?` 2–60; default √n clamped 5–20), binned by the validator into `series` with `lo–hi` labels; or pre-binned `series` | `{"kind":"histogram","name":"Orders","bins":5,"values":[12,15,15.5,18,22,23,25,31]}` |
| `heatmap` | a matrix, one hue light→dark for magnitude, a scale legend, cell labels when they fit | `x: [labels]`, `y: [labels]`, `values: [[one row per y, one cell per x]]` (null = empty) | `{"kind":"heatmap","name":"Transactions","x":["Mon","Tue"],"y":["US","CA"],"values":[[120,130],[40,42]]}` |
| `small_multiples` | one small chart per series on shared scales (lines over dates, bars otherwise) | 1–24 series | `{"kind":"small_multiples","series":[{"name":"US","points":[["2026-01",100],["2026-02",115]]},{"name":"CA","points":[["2026-01",200],["2026-02",215]]}]}` |
| `combo` | bars plus a line on ONE axis (actual vs plan, same unit) | series with `role: "bar"` and `role: "line"` (unstated: first bar, rest line); at least one of each | `{"kind":"combo","unit":"USD","series":[{"name":"Actual","role":"bar","points":[["2026-01",80],["2026-02",85]]},{"name":"Plan","role":"line","dashed":true,"points":[["2026-01",82],["2026-02",84]]}]}` |
| `waterfall` | a bridge: steps up (green) and down (red) with connectors, named totals as absolute bars | exactly one series of steps; `totals: [labels]` | `{"kind":"waterfall","unit":"USD","totals":["Start","End"],"series":[{"name":"Bridge","points":[["Start",1000],["New",250],["Churn",-120],["End",1130]]}]}` |

Aliases the validator accepts and normalizes: `horizontal_bar`/`barh` →
`hbar`, `stacked` → `stacked_bar`, `100%`/`hundred_percent` →
`percent_bar`, `facets` → `small_multiples`, `bridge` → `waterfall`,
`column` → `bar`, `matrix` → `heatmap`, `hist` → `histogram`. Every
kind renders as hand-drawn SVG with round ticks (steps of 1, 2, 2.5,
5 × 10ⁿ), a recessive hairline grid with a firmer zero line, a legend
for two or more series (none for one), `<title>` tooltips on every
mark, sorted categories when `sort` says so, an optional reference
line, the EXPLORATORY watermark when the validator set one, and the
reason as the SVG's own `<title>` plus a line under the chart. Colours
come from the CSS tokens, so light and dark need no second copy;
`prefers-reduced-motion` stops every draw-in.

Other artifact types that carry numbers:

- **table** — `{columns: [{key, label, unit?, format?, status?}], rows}`.
  Every numeric cell reads through its column's format (the raw value on
  hover); the header names the unit; the summary strip shows each numeric
  column's total (its mean for a percent or a ratio) with min / max / avg
  and a sparkline. The first 50 rows show (20 inside a dashboard); **Show
  all** keeps its state on the wrapper (`data-state`), lifts the box's
  height cap, updates the count line and `aria-expanded`, and scrolls the
  first revealed row into view.
- **kpi** — `{value, unit?, label?, delta?, delta_format?, compare_label?,
  format?}`. The value formats from its label and unit (`$1.2M` with
  `$1,234,567.8` on hover); the delta in the value's unit, or in its own
  `delta_format` (`percent` for a −4.2 % change under a dollar figure),
  with `compare_label` beside it.
- **dashboard** — `{grid?: 1|2|3, panels: [{type, title?, span?, spec}],
  filters?, notes?}`. With `grid` the panels lay out on that many columns,
  each panel spanning `span` (a chart or a table takes two by default,
  capped at the grid); without it, as many 220px columns as fit; one
  column under 640px. Every panel formats and renders through the same
  module, and carries its own provenance strip, which wraps.

## 4 · Override keys

| where | key | effect |
|---|---|---|
| a chart spec | `format` | the y axis, tooltips and every series: a kind word (`percent`, `currency`, `count`, `ratio`, `duration`, `date`, `number`, `identifier`, `text`) or `{kind?, decimals?, unit?, compact?, grouping?, sign?, scale?}` |
| a chart spec | `x_format` | the x axis of a scatter |
| a chart spec | `unit` | the unit every series shares (`USD`, `%`, `ms`, `count`) |
| a series | `unit`, `format` | that series alone (its tooltips) |
| a chart spec | `reference` | `{value, label?}`: a dashed line at the value with its label |
| a chart spec | `sort` | `x` (label order, dates chronological), `-x`, `y`, `-y` (largest first), `none` |
| a chart spec | `reason` | the sentence the card shows; the heuristic writes it when the model leaves `kind` out |
| a table column | `unit`, `format` | that column's cells, header and strip |
| a kpi | `format`, `delta_format` | the value; the delta apart from it |
| a dashboard | `grid`, panel `span` | the layout |

Everything not listed is dropped by the validator; unknown format
kinds, sorts and reference shapes are refused with a teaching message.

## 5 · Adding a kind

1. `artifacts.py`: add the name to `CHART_KINDS` (and any alias to
   `CHART_KIND_ALIASES`); give it a branch in `_validate_chart` — reuse
   `_clean_series` for a series kind, or read its own encoding as the
   heatmap does — with its limits; add a valid spec to `EXAMPLES`
   (the tests validate, render and export it automatically).
2. `js/artifacts-render.js`: add the name to `CHART_KINDS` (the surface
   test pins it to the validator's list) and a drawing function, or a
   branch in `categoryChart`; dispatch it from `chartSVG`. Use
   `numberFormat` / `formatValue` for every label, `niceTicks` for the
   scale, class `grid` for gridlines, `tick` for text, `chart-bar` /
   `line` / `dot` / `cell` for marks so the reduced-motion rules apply,
   and `<title>` on each mark. Copy the file to the other surface
   (`apps/synapse/frontend/js/artifacts-render.js`) — they must be
   identical.
3. `export.py`: map it in `_CHART_TYPES` when PowerPoint has the shape,
   else add it to `_TABLE_KINDS` so the deck lands the values as a table
   under the chart's title.
4. If the heuristic should pick it, add the branch to `choose_visual`
   and its row to the table in §2; if the `chart_rows` chip should build
   it from rows, teach `loop._chart_spec` its encoding.
5. Say so in the `artifact` tool description (`kit.py`) and the charts
   skill (`sahs/assistant/skills/charts.md`, which must stay under the
   4,000-character whole-load ceiling).
