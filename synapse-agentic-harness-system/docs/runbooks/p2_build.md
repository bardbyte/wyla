# P2 runbook — truth store + first real compile (laptop)

Fold the real archives and semantic sources into the quad store, then
compile the first immutable build. **E1 blocks the front door**: no
archive quad enters until its table resolves through the human-verified
crosswalk.

## Prereqs

- The two archives on this laptop, in exactly the documented layouts
  (`docs/contracts/`): `real_extractions_production/` (00–17 per table)
  and `mdm_46_patched_v2/` (run_manifest + responses per table).
- P0/P1 committed.

## 1. Author the crosswalk (human step, once — E1)

`graph/identity/crosswalk.jsonl`, one row per table, all 46:

```json
{"physical": "dw.gms_transaction", "lumi_asset_id": "…",
 "atlas_entity_id": "…", "verified_by": "<you>",
 "verified_on": "YYYY-MM-DD", "notes": ""}
```

The validator refuses archive quads whose table subject is not in this
file — a missing row is a loud stop, not a silent skip.

Two optional sidecars sit beside it, same strictness (a row naming a
non-crosswalk physical refuses to load):

- `aliases.jsonl` — alternative names (data-product display names,
  pack nicknames): `{"alias": "…", "physical": "dw.<t>", …}`;
- `lob_map.jsonl` — line-of-business membership, one row per
  (LOB, table): `{"lob_code": "GMNS", "lob_name": "…", "physical":
  "dw.<t>", "verified_by": "…", "verified_on": "…", "notes": "",
  "aliases": ["Global Merchant & Network Svcs"]}`. `aliases` declares
  the OTHER spellings the catalogs use for the same LOB (DMP declares
  by display name, codes are short) — declared equivalence, so the two
  never fork into parallel lob nodes.
- `exclusions.jsonl` — the 46→45 rule: a crosswalk table that does
  not compile (no columns on record anywhere) must carry a steward
  reason here — `{"physical": "dw.<t>", "reason": "…", "verified_by":
  …, "verified_on": …}` — or the compile gate BLOCKS promotion. The
  build manifest's `table_reconciliation` block accounts for every
  row either way; an unexplained gap may not survive a build.
- `org_map.jsonl` — org units (sub-LOBs): `{"org_code": "CFR",
  "org_name": "Credit & Fraud Risk", "parent_lob": "Finance",
  "aliases": [], …}`. Ownership and usage are DIFFERENT facts: the
  mined `business_unit` names who RUNS the queries, and it resolves
  through the LOB+org codes/aliases into `used_by` edges — never into
  `in_lob` ownership. Unmapped values are counted
  (`usage_unmatched`), never guessed.
  Steward witness; dmp/gmns declarations corroborate with their own
  witnesses; mined `business_unit` values only corroborate an existing
  lob node (`lob_unmatched` counts the rest — mined noise never mints).

## 2. Build the graph

```bash
cd synapse-agentic-harness-system

python scripts/laptop.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive <BQ_ROOT> \
  --mdm-archive <MDM_ROOT> \
  --sources-dir <SRC> \
  --registry <BQ_ROOT>/_batch_summary.csv \
  --out graph/runs/p2_build --json
```

Order inside the run (pinned): crosswalk gate → BQ archive → MDM
archive → LOB map (steward first, so mined values have something to
corroborate) → semantic quads (any `studio_results_*.csv` in sources/
loads here, AFTER the catalogs: rows fuse onto canonical metrics via
`dmp:<metric_catalog_id>` — same SQL corroborates as a `studio` witness
family, different SQL lands as a flagged second class; the full
referenced SQL — dmp's own `referencedSqlQuery` included — rides whole
as doc evidence; observed grain/query-shape/lineage-delta/data-owners
ride as texture, never identity) → **studio join mining** (in-silo,
CTE-aware sqlglot walk over the kept SQL: ON equalities between
resolvable tables become `joins_via` edges that are `scope:
scoped_only` by design — evidence a relationship EXISTS, never that
raw tables join safely; self-join patterns counted, never edges) →
**jobs 30d witness** (raw history mined in-silo; runs AFTER the
catalogs so a jobs sighting of a governed metric is testimony, never a
fresh seed) → utilization ledger → run manifest → **validator gate**
(the 14-check catalog; any error exits 2 and nothing downstream runs). Interrupted? Re-run; checkpoints resume.
DENIED/503 archive responses become explicit `unknown_*` quads —
absence is never silence.

Three E12 gates to read in the summary:

- `jobs_canon_rate` — every table must reach ≥90% of its 30-day jobs
  canonicalized-or-understood (nested counts as understood; parse/
  dialect breakage does not). The per-table accounting is in the run
  manifest under `reports.jobs_30d.tables` — **commit it; it is the
  per-table canonicalization report** the exit criteria require.
- `utilization` — the manifest's `utilization[]` accounts for EVERY
  file under all three input roots as consumed | deferred(reason) |
  inventoried. Read the inventoried list; anything you don't recognize
  is a file we're silently not using — that's a finding, not a detail.
- jobs-vs-audit divergences arrive as
  `ReviewItem(kind=witness_divergence)` quads — they fold into the
  review queue, not into features.

## 3. Compile

```bash
python scripts/laptop.py compile \
  --graph graph --builds builds \
  --out graph/runs/p2_compile --json
```

Produces `builds/b_<hash12>/` — manifest, cards/, indexes/ (sqlite +
JSONL twins), census.json (+ `structural` D1–D5 section), acl.json
(E3: unreadable policies compile to `restricted: unknown_policy`),
schema.json, tickets.jsonl, DIFF_vs_prev.md — and moves
`builds/CURRENT` **only after all gates pass** (E4; atomic write).
Same graph → byte-identical build, same id: run it twice if you want
to see determinism with your own eyes.

## 4. Review the DIFF like a PR (human step)

`builds/<id>/DIFF_vs_prev.md` leads with semantic changes (metric
status/expression moves, census drift, structural D1–D5 drift with
direction). The first real compile says "no previous build" — honest,
not empty. Read the tickets: every D1–D5 instance opened one.

## Governance moves (whenever, not just P2)

```bash
python -m sahs.graph.clerk --graph graph \
  --subject metric:<fp12> --set-status team_candidate --actor <you>
```

Illegal jumps (e.g. mined → certified) are refused BEFORE the write;
every clerk quad carries `prov.actor` (E7). Recompile after; the DIFF
will show the transition.

## Commit

```bash
git add graph builds/*/manifest.json builds/*/census.json \
  builds/*/DIFF_vs_prev.md builds/CURRENT graph/runs/p2_build graph/runs/p2_compile
git commit -m "meridian: P2 first real build — validator 0 errors, D1–D5 census, CURRENT -> <id>"
```

Exit criteria met when: crosswalk 46/46; validator 0 errors on the real
graph; every extracted table has a card; every dmp metric has a card
with an active binding; CURRENT names the build; DIFF reviewed.
