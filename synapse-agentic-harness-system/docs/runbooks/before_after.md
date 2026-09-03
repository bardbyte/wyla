# Before / after runbook — is the new build better or worse?

The question after any compiler change: **did the agent get a better
world, fact by fact, or did something quietly regress?** This runbook
answers it with `scripts/build_snapshot.py`, which freezes what a
promoted build serves and compares two freezes.

## The one thing to know first

`build_id` is the **graph hash**, not a compiler version. Recompiling
the same graph with new compiler code writes into the SAME
`builds/b_<hash>/` directory, in place. If you check out a branch and
compile before freezing, the "before" is gone. **Snapshot first.**

## The sequence (laptop, real archives)

```bash
cd synapse-agentic-harness-system

# 0. on main, against the build you have TODAY — freeze it
git checkout main
python scripts/graph_state.py                       # sanity: what is promoted
python scripts/build_snapshot.py snapshot --out /tmp/before.json
cp -r builds/$(cat builds/CURRENT) /tmp/build_before   # belt and braces

# 1. the branch
git checkout claude/graph-card-sourcing-audit-10rcdj
pip install -e .                                    # if deps changed

# 2. rebuild the graph (Atlas fields land HERE, not at compile) …
python scripts/laptop.py build-graph \
  --graph graph --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --mdm-archive $DATA/mdm_46_patched_v2 \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --out runs/rebuild --plain

# 3. … then compile
python scripts/laptop.py compile --graph graph --builds builds \
  --out runs/rebuild --plain

# 4. freeze the new build and compare
python scripts/build_snapshot.py snapshot --out /tmp/after.json
python scripts/build_snapshot.py compare /tmp/before.json /tmp/after.json
```

The compare exits `0` when nothing got worse and no table lost a fact
it previously carried, `2` otherwise — read the `⚠ regressions` block
before anything else.

## What to read in the output

- **`card_fact_coverage_pct`** — of the 25 table-level facts the agent
  could see, what share do the cards actually carry, averaged over
  every table. On the fixture this went 21% → 65%; on the real 46
  tables expect a higher "before" (real Atlas/MDM planes are richer
  than the fixture) and a larger absolute gain.
- **per-fact rows** — `primary_key`, `declared_fk`, `answerability`,
  `owner_chain`, `cost_prior`, `vocabulary`: each is "how many cards
  carry it". A fact stuck at `0/46` after the change means the SOURCE
  never had it (e.g. no `11_logical_constraints` for any table), not
  that rendering failed — check `indexes/coverage.json` to tell the two
  apart.
- **`coverage_unaccounted`** — must be `0`. Anything else is a prop the
  graph holds that no surface serves and nobody has explained.
- **`cards_over_budget` / `cards_budget_dropped`** — with real 50-column
  tables some cards WILL budget-drop (vocabulary first, then the column
  long-tail). That is by design; a non-zero `over_budget` is not.
- **`card_tokens_avg`** — will rise. The budget ceiling is 3K; the
  agent's `read_card(section=…)` reads one section at a time, so the
  cost is paid only when the whole card is pulled.
- **`tickets`** — may rise (the Atlas `pii_columns[]` witness surfaces
  new D1/D5 cases). Rising tickets are new findings, not regressions;
  read `tickets.jsonl` for the new rows.

## After the numbers

1. `python scripts/e19_baseline.py --real` — the E19 delta line for the
   PR. The cards and `list_tables` changed, so `answered%` /
   `wrong-when-answered%` CAN move; this is the number that decides.
2. Open the console (`apps/console`), Semantics › tables → pick a real
   table → read IDENTITY & GRAIN and THE SERVED CARD side by side. They
   render one row; if they ever disagree, that is a bug, not a display
   choice.
3. Semantics › business units → open your largest LOB. The tables shelf
   should read like a catalogue page, and every table should have a
   `read_card` address on its lob card.
4. Read three real cards end to end (`builds/CURRENT/cards/tables/`).
   The audit's test is simple: could an analyst who has never seen this
   table write a correct query from the card alone?
