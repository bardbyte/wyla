# Steward briefings — one markdown file per table

A briefing is the paragraph only a human can write: what one row IS,
the gotchas, and the mistakes people actually make. It becomes a
`human_approval` fact on the table's node and flows automatically into
LLM enrichment and the agent's `inspect_table`.

Name the file after the table: `roll_rate_calc.md`,
`sbs_new_accounts.md` (qualified names fine — matching is normalized).
Apply to the current snapshot without a re-compile:

    python synapse/scripts/apply_curation.py

Keep it under a page. The template that earned its keep:

- **Grain** — one row per what? (key × date?)
- **Type gotchas** — view vs base table, RLS behavior, partitioning
- **Key distinctions** — which identifier is account-level vs
  customer-level; which columns look alike but differ
- **Usage notes** — who queries it, when, roughly how much
- **Known analyst mistakes** — misnamings and misuses seen in the wild
