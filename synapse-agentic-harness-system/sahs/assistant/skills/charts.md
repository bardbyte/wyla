# Charts that are right

Which chart for which question, and how to build the spec so the picture
says what the rows say — a forecast after the actuals, one axis, no pie.

## Pick the form from the question, before anything else
1. One number now (a total, a rate, this month) → a `kpi` artifact, not a
   one-bar chart. A handful of headline numbers → a dashboard of kpi tiles.
2. Change over time → `line` (an `area` only for a single series that is
   a volume). The x axis is the period, oldest to newest, every period
   present even when its value is null.
3. Comparison across categories (states, merchants, products) → `bar`,
   sorted by value unless the categories have their own order (months,
   tiers, age bands). More than 12 bars: keep the top 10 and fold the
   rest into "Other" — say so in the title.
4. Part of a whole → a `bar` of the parts (a stacked bar in a dashboard);
   never a pie or a donut: the eye cannot compare slices.
5. Relationship between two measures → `scatter`, one point per entity.
6. Two measures of different scale (spend and count) → two charts, or
   both indexed to 100 at the first period on one axis. Never two y
   axes on one plot.
7. A distribution → bars over bins you name ("0–1k", "1k–5k", …).

## Twelve kinds; leave `kind` out and the heuristic picks
`line area bar hbar stacked_bar percent_bar scatter histogram heatmap
small_multiples combo waterfall`. With no `kind` the artifact tool picks
by the data's shape (time → line; category + measure → sorted bar, hbar
for long labels; two measures → scatter; parts ≤ 6 → stacked/percent bar;
distribution → histogram; two categories → heatmap; > 12 series → small
multiples; one number → kpi) and writes its `reason` into the spec. When
you pick, write `reason` yourself. Numbers format themselves from the
data (0.125 → 12.5%, thousands grouped, K/M/B past 100k): name columns
for what they are, set `unit`, and use `format` only to override.

## Build the spec so the axis tells the truth
1. Every series rides ONE shared x axis of labels: `points: [[x, y], …]`
   with the same kind of x in every series (`2026-01`, `2026-02`, … or
   the category names). Dates as `YYYY-MM` or `YYYY-MM-DD`, never
   "Jan" in one series and "2026-01" in another.
2. A series that has no value for a period carries `null` for it, so it
   is drawn as a gap and never shifted onto the wrong period.
3. A forecast, a projection or a target is a SECOND series on the same
   axis, `dashed: true`, null over the actual periods, and it starts at
   the last actual point (repeat that point) so the two lines join —
   actuals Jan–Jun, forecast Jun–Dec, one axis Jan–Dec.
4. Before the artifact call, read the rows once more: the periods in the
   rows are the periods on the axis; a value on the wrong period is a
   wrong chart, whatever it looks like.
5. `unit` names what the numbers are (USD, count, %, millions); a rate
   is a percent, never a fraction on one series and a percent on another.
6. Bars start at zero. Two to six series at most; past six, split the
   chart or fold the tail.
7. The title says the finding ("US TLS net sales, actuals to June and the
   forecast to December"), and the provenance carries the status: a
   projection you computed is `exploratory` until a check fact backs it.

## Never
- Never a pie, a donut, a 3-D chart, or two y axes.
- Never put a forecast's values on the actuals' periods, or drop the
  periods a series is missing: nulls, not shifts.
- Never a chart of one number, or a line through categories that have
  no order.
- Never colour by rank or invent a seventh series colour; fold the tail.
