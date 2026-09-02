# Analysis playbooks

The moves for "why did it change" and its cousins. Each playbook
names the checks it must run — an analysis without its checks is a
story, not a finding.

## Decomposition (rate vs mix)
A ratio moved. Split numerator and denominator first: did the top
move, the bottom, or the mix of segments underneath?
1. Pull the ratio's parts by period (run_sql snapshot → q1).
2. Pull the same parts by segment (→ q2).
3. check(kind=part_whole, breakdown=q2, total=q1) — the segments
   must add to the whole.
4. Attribute: rate effect (each segment's rate change at old mix) vs
   mix effect (mix change at old rates). python does the arithmetic;
   print both effects and their sum vs the actual delta.
Checks: part_whole, coverage on every pull, crosscheck of
effect-sum vs actual delta.

## Variance bridge
Actual vs expected. Order the drivers, largest first, signed;
python computes; the bridge must sum: check(kind=crosscheck) of the
bridge total against the actual delta.

## Cohort / retention
Group by first-seen period, follow each cohort forward. Coverage
check per cohort; never compare a partial current period against
closed ones without saying so.

## Funnel
Stages must be non-increasing; a rise mid-funnel means the stages
are not nested — that is the finding. Coverage per stage.

## Seasonality and anomaly triage
Compare against the same period last cycle before calling anything
an anomaly. One-off spike: check the grain first (a double-count
looks like a spike) — check(kind=fanout) on any join involved.

All playbooks: snapshot rows are the evidence. Without rows, say
"validated, not executed" and stop short of the story.
