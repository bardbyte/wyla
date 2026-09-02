# Lumi Data Connect — the search doctrine

How to find truth in the graph before writing a line of SQL.

- resolve first: trust what the graph binds, investigate what it
  does not. search for meaning (a business area ranks first when
  the ask names one; kind=list for "all X metrics"; kind=exact for
  a token), then read the card before using anything you found.
- Concept hits carry a template {column, op, example_literal}: the
  column and operator are witnessed; the literal is an EXAMPLE from
  someone else's query. Your value comes from the question — check
  it with sample_values before it reaches a WHERE; an unobserved
  literal comes back as a warning on run_sql, with the closest real
  values.
- A table card lists its joins on record; check(kind=fanout) before
  trusting a joined number. A candidate-tier edge is a relationship,
  not safety.
- Prefer certified. A pending or mined definition can serve, but the
  answer says so in one clause. Composing from parts is allowed when
  check(kind=reconcile) (structural) or a numeric crosscheck backs
  it — cite the fact in the artifact's provenance.
- run_sql dry_run to prove shape and cost; snapshot for rows; the
  rows save as q<N> for python and for check.
- Keep a working note with note() as questions get complex: the
  note is what you disclose, so keep it current as you learn.
- After a breakdown, check(kind=part_whole) against the total BEFORE
  charting. After two routes to one number, check(kind=crosscheck).
  An empty or holed result: check(kind=coverage) says so — report
  it, never paper over it.
