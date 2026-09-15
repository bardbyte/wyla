# Knowledge Catalog enrichment — runbook

The KC Enrichment tab (and `pipeline.py kc`) turns what the graph knows
about one table into the constructs the Knowledge Catalog holds, in the
order a person enters them, with every line carrying its witness and
status. Humans enter the content; Synapse records that they did. The
catalog is never written through its API from here (exports are shaped
for it; that is a transport switch later), and catalog content never
overrides the graph (it comes back as a pending witness).

## Where things live

| piece | path |
|---|---|
| config | `config/kc.yaml` (`KC_CONFIG` overrides the path) |
| module | `sahs/kc/` — `coverage` · `assemble` · `render` · `write` · `verify` · `export` · `bundle` · `witness` · `cli` |
| coverage registry | `sahs/kc/coverage.py`, published to `docs/kc_coverage.md` + `docs/kc_coverage.json` |
| model caches, events, push records | `graph/runs/kc/<table>/<build>/` (`llm_<prompt>.json`, `usage.json`, `events.jsonl`, `push_<stamp>.json`) |
| blind gate | `graph/runs/kc/_gate/<build>/<prompt>.json` |
| admin API | `apps/synapse_admin/backend/kc.py` under `/api/kc/*` |
| tab | `apps/synapse_admin/frontend/js/pages/kc.js` — `#/kc`, `#/kc/dictionary`, `#/kc/<table>` |
| tests | `tests/test_kc.py`, `apps/synapse_admin/tests/test_kc_surface.py` |

## Config

```yaml
kc:
  project: ""            # the project that holds the entries and the glossary
  location: "us"
  glossary: ""           # id or full projects/…/glossaries/… path
  tables: []             # the catalog-enabled tables, dataset.table; empty = every table, and the picker says so
  aspect_types: {…}      # ids the catalog knows the custom aspect types by
  push_record_dir: "runs/kc"
  llm: true
  prompt_version: "kc.1"
  budget: {turn_tokens: 120000, turn_calls: 8}
  gate: {halt_below: 0.60, review_below: 0.80}
```

The model plane is the same `.env` contract as enrichment
(`SYNAPSE_VERTEX_SA_KEY`, `VERTEX_PROJECT_ID`, `VERTEX_MODEL`, default
Gemini 3.1 Pro on Vertex). No credential lives in `kc.yaml`; the
frontend never learns the endpoint.

## Roles to request from the catalog admins

`roles/dataplex.catalogViewer` (search), `roles/dataplex.entryOwner`
(aspects on entries), `roles/dataplex.catalogEditor` (overview,
contacts, queries), `roles/dataplex.metadataEditor` plus
`roles/storage.objectViewer` (glossary and entry-link import jobs, which
stage a file in Cloud Storage), `roles/dataplex.dataScanAdmin` (data
quality scans), and BigQuery data viewer on the tables. Also ask: the
glossary naming convention, whether custom aspect types are allowed
org-wide, the import bucket, and whether Gemini data insights are on
(so the read-back can compare its descriptions with ours).

## Once

1. Compile and promote a build; run `python scripts/pipeline.py kc-coverage --out runs/kc_cov --plain`.
   The gate must be green: every node kind, prop, relation, edge prop,
   index field, card section and report field in the current graph has a
   row. A red gate names the unrowed items; add rows in
   `sahs/kc/coverage.py` (a row may be `excluded` with a reason).
2. Fill `config/kc.yaml`: project, location, glossary, the 15 tables.
3. In the catalog: create the glossary and its category tree (LOB →
   domain → concept family; the bundle's Glossary card lists the
   categories it needs), then create the custom aspect types from the
   tab's "Aspect types to create once" panel (one `metadataTemplate`
   each). Enable a data product for the tables if wanted.

## Per table

1. Open `#/kc`, pick the table. Deterministic cards render at once; the
   model cards stream in (first load runs the blind gate for this build
   and prompt version, then three calls: overview, columns, glossary).
   The gate line reads `kc gate: N/M recovered (x%) → tier`; below 60%
   the model blocks are withheld, 60–80% they open review-first, 80%+
   copy-ready. A second load makes zero model calls (cache keyed by
   table, build id, prompt version); "regenerate" re-runs the model
   sections only.
2. Read the translation ledger: which Meridian objects became which
   catalog constructs, how many were translated, how many wait for a
   human and why.
3. Read "Needs review" first. Decide: hold, or approve (approving files
   a review item through the clerk and moves the item to a copy block on
   the next load).
4. Enter each card into the catalog following the right rail:
   description via the JSON export (`entry_patch.json`, an
   `entries.patch` body: descriptions are API-only); overview, glossary
   terms, related entries, aspects, queries (source User) and contacts
   in the console; column descriptions via the same patch body; DQ
   rules attached to terms or as a scan. Bulk glossary work uses a
   metadata import job with `glossary_import.jsonl` and
   `entry_links.jsonl` from the zip export.
5. Tick the sections you entered, type your name, "Mark pushed". The
   clerk writes a `kc_pushed` quad (table → kc run, actor-signed, with
   the build id and each section's content hash) and a JSON record.

`python scripts/pipeline.py kc --table dw.t --out runs/kc --export zip
--plain` does the same from the terminal (`--no-llm` for deterministic
sections only, `--regenerate` to re-run the model).

## Weekly: the read-back

Export the catalog's glossary, published descriptions and DQ results
and post them to `/api/kc/witness-import` (`kind` = `glossary` |
`descriptions` | `dq`). They land as `doc:` nodes and `described_by` /
`evidenced_by` edges with `witness: kc` and `review_status: pending`.
The next compile's census then shows where the catalog and the graph
disagree; the review surface for those items comes later.

## What the rules pin

Consensus first. Model text only under "Suggested — unreviewed".
Pending metrics carry `pending · <origin> ×support` in the definition
and live in a "Candidate terms" category. Join candidates (co-usage,
catalog "joined with" rows without an ON clause) are proposals, never
joins. UNKNOWN row-access policy is copied only with "treat as
restricted". Anti-aliases (common-word acronyms), user variants, and
gold content beyond attested queries never leave the graph. Every model
sentence must cite fact ids, name nothing absent from the facts, and
match the cited facts' statuses; the verifier drops the rest to "needs
review" with the reason.

## Import shapes

`sahs/kc/export.py` encodes the glossary JSONL entries, the entry-link
records (definition / synonym / related), the `entries.patch` body and
the glossary sheet columns; `tests/fixtures/kc/` holds the checked-in
samples the exports are tested against. The catalog's documentation was
not reachable from the environment this was written in, so verify the
field names against the pages named in the module docstring before the
first import job.
