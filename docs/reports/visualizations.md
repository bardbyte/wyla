# Report — the charting and dashboarding uplift

Branch `worktree-agent-a37d389ee34042805`, worktree
`/home/user/wyla/.claude/worktrees/agent-a37d389ee34042805`, started
from `origin/claude/production-ready` at `e3524b6` (the report
`docs/reports/backend-fixes-sensitive-model-skills.md` was in place).
One commit, not pushed, no PR.

The owner's ask: "our charting and dashboarding skills need a better
uplift" — (1) number formatting that follows the data, (2) a
selection strategy for which visualization when, (3) more kinds; plus
the two bugs (Show all, the cut-off disclaimer strip) and dashboards
that use the same rules. All six deliverables are in; what is not done
is listed at the end.

## File by file

### The harness (`synapse-agentic-harness-system`)

- `sahs/assistant/artifacts.py` — rewritten around three things.
  `number_format(values, hint)` and `format_value(value, fmt)`: the
  kind (percent, currency, count, ratio, duration, date, number,
  identifier, text) from the explicit `format`, then the `unit`, then
  the column's name, then the values; the decimals are the maximum the
  data carries, capped at 4 (2 for currency, 0 for counts), so
  `0.125` reads `12.5%` and `12.5` reads `12.5%` — never a fixed
  default; the percent 0..1 vs 0..100 rule from the range (every value
  within [-1, 1] is a fraction, one value beyond it means points),
  documented; thousands grouping; compact K/M/B when the median exceeds
  100,000 and the column is not an identifier; sign handling; `—` for
  nulls. Rounding is half-away-from-zero on the same float arithmetic
  the JS twin uses, so both sides give byte-equal answers.
  `choose_visual(rows, columns, intent)` implements the written
  heuristic (table below) and returns kind, encoding (x, y, series,
  sort, top_n, compare) and a one-sentence reason; `choose_for_series`
  is the form the artifact tool uses. `CHART_KINDS` is now twelve
  (line, area, bar, hbar, stacked_bar, percent_bar, scatter,
  histogram, heatmap, small_multiples, combo, waterfall) with an alias
  map; `validate_artifact` stays strict per kind (heatmap matrix
  shape and 60×60 cap, histogram raw values binned server-side with
  `lo–hi` labels or pre-binned series, numeric x on a scatter, combo
  roles, one waterfall series with named totals, 6 series / 24 facets,
  2,000 points per series / 5,000 on a scatter), plus the shared keys
  `format`, `x_format`, `unit`, `reference`, `sort`, `reason`, per-series
  `unit`/`format`; table columns take `unit`/`format`; kpi takes
  `format`, `delta_format`, `compare_label`; dashboards take `grid`
  (1–3) and per-panel `span`. `EXAMPLES` holds one valid spec per kind
  (docs, tests and the node render check all draw from it). TYPES is
  unchanged, so the Spanner constraint is untouched.
- `sahs/assistant/kit.py` — the `artifact` tool fills a missing chart
  `kind` from the heuristic and writes its reason into the spec (a
  refused kpi-shaped series teaches instead); a kind the model names is
  kept (aliases normalized) with the model's own reason. The tool
  description lists the twelve kinds, the heuristic, the per-kind
  shapes, the formatting rule and every override key.
- `sahs/assistant/loop.py` — `chart_rows_turn` uses `choose_visual`
  on the saved rows (a named kind, x or y still wins), and a new
  `_chart_spec` builds what the pick asked for: series per measure or
  per value of a splitting column (capped, largest first), a heatmap's
  matrix, a histogram's values, a scatter's numeric pairs, the top 15
  for a long list; the reply carries the reason. `VISUAL_STYLE` rides
  in the `<style>` section after the model family's style (a family
  with no style keeps no section — `test_model_catalog` pins that and
  the cached prefix), and IDENTITY carries the one-line rule for every
  model. Nothing else in loop.py was touched.
- `sahs/assistant/export.py` — the PPTX deck maps every kind PowerPoint
  can draw (hbar, stacked, hundred-percent, histogram, combo and small
  multiples included, the notes saying when a kind is approximated);
  heatmap and waterfall land as a formatted table under the chart's
  title; a chart PowerPoint refuses falls back to the same table; a
  broken spec never crashes the export. Table cells and KPI values and
  deltas read through `number_format`.
- `sahs/assistant/skills/charts.md` — a short section naming the twelve
  kinds, the heuristic and the formatting rule, kept under the
  4,000-character whole-load ceiling (a longer first draft pushed the
  pack into library mode and broke `test_the_kit_declares_eleven_tools`).
- `tests/test_v3_artifacts.py` (new, 19 tests) — the shared formatting
  cases, the decimals/percent/metadata/compact rules, every heuristic
  branch, every kind's example, aliases, each kind's refusals and
  limits, format overrides on chart/table/kpi, the dashboard grid and
  span, the artifact tool filling a kind and saying why, `_chart_spec`
  for heatmap/top-N/split/histogram/scatter, and the export drawing
  every kind or landing a table.
- `tests/fixtures/number_format_cases.json` (new) — 26 cases with
  their pinned answers; both sides run them.
- `docs/visualizations.md` (new) — the formatting rules, the heuristic
  as a table, every kind with encodings and an example spec, the
  override keys, how to add a kind.
- `docs/specs/synapse_v3_harness.md` — §6 gains the visualization
  bullet linking the doc.
- `runtime.py`, `spanner_store.py`, `skills_loader.py`, `admin.py`, the
  users page, the search page: untouched (`chart_rows` in runtime
  needed no change: it passes kind/x/y through).

### The surfaces (`apps/synapse_admin`, `apps/synapse`)

- `frontend/js/artifacts-render.js` (new on both, byte-identical, the
  test pins it) — `numberFormat`/`formatValue`, the JS twins; `niceTicks`;
  `createArtifactRenderer(hooks)` returning chartSVG (twelve kinds as
  hand-drawn SVG: round ticks, hairline grid with a firmer zero line,
  legends for two or more series, `<title>` tooltips on every mark,
  sorted categories, a reference line, a one-hue colour scale with a
  legend on heatmaps, a 2px surface gap between stacked segments,
  waterfall connectors and named totals, small multiples on shared
  scales), tableReport/tableHTML/bindTable, animateNumbers (through
  the column's format), kpiTile (value, delta and compare label
  formatted), tileFooter, diagramSVG, panelBody, dashboardHTML (the
  spec's grid and spans), renderArtifactBody, and chartReason (the
  heuristic's sentence under the chart and as the SVG's own title, so
  it shows on hover). The words under a number come in as the
  `meridian` hook.
- `frontend/js/pages/chat.js` (both) — the artifact panel section
  (statusChip … renderArtifactBody, ~480 lines each) replaced by one
  `createArtifactRenderer({...})` call; the import added; the chart
  `PALETTE` removed with the charts. The admin passes the definition
  line in the open, the second surface its "Prepared by Radix…"
  disclaimer with the definition line on hover — both pinned by the
  existing `test_synapse_surface` assertions, which still pass.
  `bindDashboardFilters`, `exportButtons`, the cards and everything
  else in the files are untouched.
- `frontend/styles/app.css` (both) — one delimited block appended
  (`/* ══ artifacts-render … ══ */` … `/* ══ end artifacts-render ══ */`,
  identical on both): the strip under a number wraps (`white-space:
  normal; overflow-wrap: anywhere; flex: 1 1 100%`, the footer
  `flex-wrap: wrap`), grid/zero/reference/connector/facet/cell rules
  on the tokens (dark needs no second copy), horizontal bars growing
  from the left, `.tablev3.all { max-height: none }`, the fixed
  dashboard grid with spans and a one-column fallback under 640px,
  reduced-motion parity for every mark.
- `tests/test_artifacts_render.py` (new, 7 tests) + `tests/
  artifacts_render_check.mjs` — the module exists on both surfaces and
  is identical, chat.js imports it and keeps no drawing, its kind list
  is the validator's, both stylesheets carry the identical block;
  `node --check` on every changed JS file; through node, the JS twin
  answers the 26 shared cases exactly as Python; every kind in
  CHART_KINDS draws with grid, tooltips, legend and reference; tables,
  tiles and dashboards format from the data; the Show all toggle is
  state-driven; the disclaimer strip wraps.
- `tests/test_chat_surface.py` — three tests that looked for render
  strings in chat.js now look in the page plus its module.
- `tests/test_approval_workflow.py` — the shared-axis pins read the
  module.
- `README.md` — a paragraph on the module and a link to the doc.

## The two bugs

- **Show all.** The old binder kept the toggle's state in the button's
  words (`btn.textContent === "Show all"`) and only flipped `hidden`
  on rows inside `.tablev3`, a scroll box capped at 58vh that already
  overflowed with fifty rows, and the count line never changed — so a
  click changed nothing on screen. Now the wrapper carries
  `data-state`, `data-limit` and `data-rows`; one `apply()` derives the
  rows' visibility, the `.all` class (which lifts the height cap so the
  page scrolls the rows), the button's words, `aria-expanded` and the
  count line ("All 1,234 rows") from that state; a click flips it and
  scrolls the first revealed row into view. Pinned by
  `test_show_all_keeps_its_state_on_the_wrapper` on both surfaces.
- **The disclaimer strip.** `.tile-footer .meridian` was `white-space:
  nowrap; overflow: hidden; text-overflow: ellipsis` inside a
  `.dash-panel { overflow: hidden }`, so the "Prepared by Radix…" line
  under a tile was cut to one clipped line. The artifacts CSS block,
  last in both stylesheets, sets it to wrap (`white-space: normal;
  overflow: visible; text-overflow: clip; overflow-wrap: anywhere;
  flex: 1 1 100%`) with the footer wrapping, on every artifact card
  and dashboard panel. Pinned by
  `test_the_strip_under_a_number_wraps_instead_of_clipping`.

## The heuristic

| the rows look like | picks |
|---|---|
| no rows / no numeric column | table |
| one row, one measure | kpi; with a comparison column or a "vs" ask → kpi with delta |
| the ask says distribution, or one measure with no dimension and ≥ 20 rows | histogram |
| time on x, a category with > 12 values | small multiples (one per category) |
| time on x, a category | line, one per category |
| time on x, > 12 measures (or > 6) | small multiples |
| time on x, two measures of one kind, one named plan/target/prior | combo (bars + line, one axis) |
| time on x, one measure, the ask says area/volume/cumulative | area |
| time on x | line, sorted by x |
| two categories and a measure filling a grid ≤ 60×60 | heatmap |
| a category and a measure, the ask says waterfall/bridge | waterfall |
| ≤ 6 parts, a percent measure or a share/mix/breakdown ask | percent_bar / stacked_bar — never a pie |
| a category with > 40 values | table with a top-15 hbar |
| a category, > 12 measures | small multiples |
| a category with > 8 values or labels > 12 chars | hbar, sorted |
| a category and measures | bar, sorted by value |
| two measures, nothing to group by | scatter |
| anything else | table |

## Tests and exit codes

- App suite, from the repo root:
  `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider`
  → **165 passed, 2 skipped, exit 0** (baseline before the change:
  exit 0).
- Harness suite, from inside the silo:
  `python -m pytest -q tests -p no:cacheprovider` → **exit 0** (all
  dots; baseline exit 0). The targeted runs on the way: `test_v3_artifacts.py`
  19 passed; `test_chart_spec test_v3_dashboards test_v3_loop
  test_v3_projects` 56 passed; `test_model_catalog test_loop_prompt
  test_v3_loop` 59 passed.
- `node --check` on `apps/*/frontend/js/artifacts-render.js` and
  `apps/*/frontend/js/pages/chat.js`: all pass (also run by
  `test_every_changed_js_file_parses`).
- The node render check: 26/26 cases equal between Python and JS on
  both surfaces; 12/12 kinds draw with every mark inside the viewBox.

## Not done

- No screenshot or browser walk: the charts were checked structurally
  (markup, geometry in bounds, tooltips, grid, legend) through node,
  not eyeballed in a browser. A visual pass on the hbar label widths,
  the heatmap cell labels and the small-multiples footers is worth a
  laptop minute.
- The chart card in the transcript (`artifactCard` in chat.js) was not
  touched: the heuristic's reason shows under the chart and as the SVG's
  own hover title in the panel/body, not on the card's title attribute.
- The heuristic reads column names in English; a column named in
  another language falls back to the values.
- The PPTX export draws heatmap and waterfall as tables (PowerPoint's
  native waterfall is not in python-pptx); combo lands as clustered
  columns with a note.
- The `chart_rows` endpoint takes no `intent` (the run's title is the
  intent); an ask-worded intent would need a field in
  `apps/synapse_admin/backend/chat.py`, outside this lane.

## Files outside my lane touched

- `apps/synapse_admin/tests/test_approval_workflow.py` (11 lines: the
  shared-axis pins now read the module) and
  `apps/synapse_admin/tests/test_chat_surface.py` (18 lines: three tests
  read the page plus the module) — test files, not the other agent's
  regions of chat.js.
- In both `chat.js`: the import line and the removal of the chart
  `PALETTE` constant at the top of the file (four lines each), beside
  the artifact section that was mine.
- `sahs/assistant/loop.py` outside `chart_rows_turn`: the import, the
  `VISUAL_STYLE` constant, one sentence in IDENTITY, the style section
  in `build_prompt`, and the new `_chart_spec` helper. `runtime.py` was
  not touched.
- `sahs/assistant/skills/charts.md` (11 lines).
