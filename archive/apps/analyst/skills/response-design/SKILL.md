---
name: response-design
description: Chooses the right response form (number, chart, table, dashboard, or prose) and depth for an analytical answer based on the question's intent and the audience (analyst, VP, or C-suite). Use before composing any final answer that contains data.
---

# Response design — pick the form before writing a word

The best analysts are distinguished less by what they compute than by
what they choose to show. Decide the form FIRST, then fill it.

## Step 1 — classify the question's data shape

| The question asks… | Shape | Default form |
|---|---|---|
| "what is X?" (one number) | single value | **stat tile** + one sentence |
| "how is X changing?" | trend | **line chart** (single series, endpoint labeled) |
| "X by segment/product/region?" | comparison | **bar chart** (grouped; ≤4 series) |
| "what are the biggest…?" | ranking | **horizontal bar** (top-N, one measure) |
| "what makes up X?" | composition | **stacked bar** (≤4 parts, else fold to Other) |
| "why did X move?" | driver decomposition | stat tile (the move) + bar (the drivers) |
| "give me the numbers" | detail | **table** (≤50 rows, exact values) |
| "how are we doing overall?" | multi-KPI | **dashboard** (stat row + 1-2 evidence charts) |
| definitional / metadata | no data viz | prose with citations — do NOT chart it |

Two shapes at once (e.g. "trend by segment") → the smaller cross-product
wins: one line chart with ≤4 segment series; more segments → pick top 3 +
Other, and say so.

## Step 2 — apply the audience modifier

Detect audience from wording and role context; when unstated, default to
**analyst** in chat, **VP** for anything called a "readout" or "review".

**Analyst** (works the data)
- Lead with the table or chart AND the ready-to-run SQL.
- Show methodology: grain, filters, join evidence, exclusions.
- Full citations block; exact values; never round away precision.

**VP** (owns the number)
- Lead with the stat tile + trend; one driver chart maximum.
- Name the threshold that matters ("still under the 2.5% risk appetite").
- 3 bullets: what moved, why, what we're watching. SQL available on ask,
  not shown.

**C-suite** (allocates attention)
- One headline sentence with the number IN it, then at most one visual
  (stat tile or single-series line).
- The "so what" is mandatory: business impact in dollars or customers,
  and the decision it informs.
- No jargon (say "accounts rolling into early delinquency", not "C-30
  event-basis migration"). Caveats compressed to one line.

## Step 3 — depth budget

- One question → one visual, unless shapes genuinely differ (trend AND
  ranking). A dashboard is for "overall health" questions or an explicit
  ask — never volunteer 6 tiles for a 1-number question.
- If confidence on a load-bearing fact is `guessed`, say so BEFORE the
  visual, or ask instead of showing.
- Every visual footer carries provenance: sources + snapshot version.

## Step 4 — anti-patterns

- ✗ A chart for a single number that isn't changing (use the stat tile).
- ✗ A table for a trend (the shape IS the message).
- ✗ Pie charts — composition uses stacked bars here.
- ✗ Two y-axes — split into two charts.
- ✗ Charting `guessed`-tier data without labeling it as a guess.
- ✗ Burying the answer under the chart: the sentence comes first.
