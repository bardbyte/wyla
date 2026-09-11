# Onboarding tables — adding N tables and a business unit that shares them

The concrete case this was written for: **12 new tables** for one
business unit, plus **6 tables that already belong to other LOBs** and
are shared with it — 18 on the unit's shelf, 6 of them owned elsewhere.

Nothing in the loaders is a table list. The archive loader walks every
directory under the archive root; the catalog loader globs every JSON.
New tables are picked up by SHAPE. The only gate is **identity**: a
table that does not resolve through the crosswalk contributes nothing,
silently, and is counted as skipped. So onboarding is three files you
author and one report you read.

## 1. Drop the evidence where the loaders look

| what | where | shape |
|---|---|---|
| warehouse extraction | `$BQ/<table_dir>/` | the same numbered artifacts (`00_…` → `16_…`) as every existing table dir; `_`-prefixed dirs are skipped |
| Atlas catalog entry | `sources/std_tech_metadata/<table>.json` (or appended to `std_tech_metadata_all.json`) | the envelope shape the contract documents; the combined file wins when both exist |
| MDM archive (run 2) | `$MDM/<table>/` | same per-table layout as the existing 46 |
| registry (if used) | `_batch_summary.csv` | one row per table |

**If the delivery arrives as suffixed roots** (`real_extractions_production_patched_12/`,
`std_metadata_12/`): the archive loader walks ONE root per run and the
catalog loader reads ONE directory, so do not point the build at both
roots at once.

- **Warehouse archive:** point `--bq-archive` at the `_12` root for
  this run. The graph is append-only; the 46 already in it stay, the
  12 are appended, and the run report and ledger cover exactly the 12.
  No copying. (Merging the 12 dirs into the original root works too
  and gives the same graph; it just re-walks the 46 for nothing.)
- **Atlas catalog:** copy the 12 JSONs into `sources/std_tech_metadata/`.
  The loader reads that one directory — and if a combined
  `std_tech_metadata_all.json` sits beside it, the combined file WINS
  and the directory is ignored; delete or regenerate the combined file.
- **Registry:** if you pass `--registry <root>/_batch_summary.csv`, use
  the `_12` root's summary for this run.
- **Crosswalk first.** An archive dir with no crosswalk row does not
  skip — it BLOCKS the build (`gate crosswalk_resolution`). Author the
  12 rows before you point the build at the `_12` root.

`scripts/rebuild_and_compare.py` runs the whole sequence — freeze at
graph and build grain, build-graph, compile, diff both, gates — in one
command and leaves every JSON under `graph/runs/<run-id>/`.

Nothing else changes for the semantic sources: the glossary, business
terms, governed metrics, mined measures and the value-synonym index are
warehouse-wide files and already cover a new table the moment its name
appears in them.

## 2. Author identity — three files, in this order

**`graph/identity/crosswalk.jsonl` — 12 new rows.** One per new table.
This is the identity everything else resolves through; author it FIRST.

```json
{"physical": "dw.<table>", "lumi_asset_id": "<mdm id or empty>",
 "atlas_entity_id": "<dataset name Atlas uses>",
 "verified_by": "<you>", "verified_on": "YYYY-MM-DD", "notes": ""}
```

`atlas_entity_id` must equal the `dataset` field in the table's Atlas
JSON; that is how `std_tech_metadata` finds its table. The 6 shared
tables already have rows — do not add a second.

**`graph/identity/lob_map.jsonl` — 18 rows for the unit.** 12 with
`role: home`, 6 with `role: shared`. The `physical` on every row MUST be
a crosswalk row or the file refuses to load.

```json
{"lob_code": "<UNIT>", "lob_name": "<display name>",
 "physical": "dw.<new table>", "role": "home",
 "verified_by": "<you>", "verified_on": "YYYY-MM-DD", "notes": "",
 "aliases": ["<every spelling the catalogs use for this unit>"]}
{"lob_code": "<UNIT>", "lob_name": "<display name>",
 "physical": "dw.<existing table>", "role": "shared",
 "verified_by": "<you>", "verified_on": "YYYY-MM-DD",
 "notes": "owned by <HOME LOB>; used here for <why>"}
```

`role` is the whole point. Two rows without it are two equal ownership
claims; `home` + `shared` is one relationship, and every surface reads
it as such (§4). `role` defaults to `home`, so existing rows are
unaffected. The 6 shared tables keep their existing `home` row under
their owning LOB — a shared row never replaces it.

`aliases` matter more than they look: the governed catalog declares
LOBs by display name (`"Global Merchant & Network Svcs"`) while codes
are short (`GMNS`). Without the alias, the catalog's corroboration
mints a PARALLEL lob node and the unit's witnesses split. Put every
spelling you can find in `metrics_dmp*.json` `lineOfBusiness`,
`measures_catalog.json` `business_unit` and Acropedia `Business_Unit`.

**Is it a LOB or an org unit?** If the unit sits UNDER an existing LOB
(a sub-unit that queries but does not own), it belongs in
`org_map.jsonl` with a `parent_lob`, and its tables reach it as
`used_by` (usage) rather than `in_lob` (ownership). A unit that owns
tables is a LOB and goes in `lob_map.jsonl`. When in doubt: does
anything list this unit as the OWNER of a table? LOB. Otherwise org.

**`graph/identity/aliases.jsonl`** — only if a data product or skill
pack names one of the 12 by a different name.

## 3. Rebuild, then read four numbers

```bash
python scripts/build_snapshot.py snapshot --out /tmp/before.json   # freeze first
python scripts/std_tech_keys.py $SRC/std_tech_metadata              # the new JSONs: 0 UNCONSUMED?
python scripts/laptop.py build-graph --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl --bq-archive $BQ \
  --sources-dir $SRC --registry $BQ/_batch_summary.csv --no-jobs-30d \
  --out graph/runs/onboard_<unit> --plain
python scripts/laptop.py compile --graph graph --builds builds \
  --out graph/runs/onboard_<unit> --plain
python scripts/build_snapshot.py snapshot --out /tmp/after.json
python scripts/build_snapshot.py compare /tmp/before.json /tmp/after.json
```

The build report answers "did every table make it, with everything":

| number | where | must be |
|---|---|---|
| `skipped_unresolvable_table` | std_tech and value_synonyms report lines | **0** — a non-zero here is a crosswalk row missing or an `atlas_entity_id` that does not match the JSON's `dataset` |
| `inventoried` | utilization ledger | **0** — a file under an input root that nothing read |
| `coverage_unaccounted` | compile manifest | **0** — a prop in the graph no surface serves |
| `lob_map.shared_memberships` | build report | **6** |
| `⚠ regressions` | snapshot compare | empty; `tables` up by 12 |

Then `read_card("lob:<unit>")` — the shelf should list 18 tables, 6 of
them marked `shared from <HOME LOB>`.

## 4. What "shared" looks like on every surface

One steward row with `role: shared` reaches every reader:

| surface | what it says |
|---|---|
| table card `- line of business:` | `GMNS: … (home; steward) · <UNIT>: … (shared; steward)` — the role is named only when a table has more than one membership |
| unit card `## tables` | `dw.<t> — <name> · … · shared from GMNS · read_card(...)`, and a `- shared: 6 of 18 tables are shared from another LOB` line above the shelf |
| owning LOB's card | unchanged — sharing does not dilute ownership |
| Semantics › business units › unit profile | a `shared from GMNS` chip on the row; the shelf label counts them |
| Semantics › table profile › WHO | each LOB row reads `line of business · home` / `· shared` |
| Cosmos | the body sits in its **home** well, gold-starred, with a gold tether to each well that shares it; the rail reads `★ home GMNS · shared with <UNIT>`; the `membership` pill toggles the tethers |
| `list_tables(lob="<unit>")` | all 18, `lobs` listing both codes |
| `indexes/lob.jsonl` | `table_roles` and `shared_tables` per unit |

The rule underneath: **the steward's role rides on the steward's edge
only.** A catalog corroborating a membership says "this table serves
this LOB", never which kind; the card shows its witness count without a
role. The map places a body in its home well; without a steward role,
the first mapped LOB is the home, which is the previous behaviour.

## 5. Append-only, remember

The graph is a log. `build-graph` into the existing `graph/` appends;
your enriched metrics survive. A fresh directory loses them. Re-running
after fixing a crosswalk row is safe: identity edges fold last-wins.
