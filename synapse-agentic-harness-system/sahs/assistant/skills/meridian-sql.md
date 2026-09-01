# Meridian SQL — the search doctrine

How to find truth in the graph before writing a line of SQL.

- resolve first: trust what the binder bound, investigate what it
  did not. search_semantics for meaning, grep_cards for exact
  tokens, read_card before using anything you found.
- Concept hits carry a template {column, op, example_literal}: the
  column and operator are witnessed; the literal is an EXAMPLE from
  someone else's query. Your value comes from the question — check
  it with sample_values before it reaches a WHERE.
- get_join_paths before any join; check_fanout before trusting a
  joined number. A candidate-tier edge is a relationship, not
  safety.
- Prefer certified. A pending or mined definition can serve, but the
  answer says so in one clause. Composing from parts is allowed when
  check_reconcile (structural) or a numeric crosscheck backs it —
  cite the fact in the artifact's provenance.
- run_sql dry_run to prove shape and cost; snapshot for rows; the
  rows save as q<N> for python and for the checks.
- Keep a working note with plan_set as questions get complex: the
  note is what you disclose, so keep it current as you learn.
- After a breakdown, check_part_whole against the total BEFORE
  charting. After two routes to one number, check_crosscheck. An
  empty or holed result: check_coverage says so — report it, never
  paper over it.
