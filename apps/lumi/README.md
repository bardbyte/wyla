# Synapse by Lumi — the product (apps/lumi)

The freshly-built admin surface over the Meridian context graph,
authored directly from the Lumi design canvas
(`apps/console/design/wireframes/`). One process, **zero build
steps**: FastAPI serves the Meridian read plane and a hand-authored
ES-module frontend (no bundler, no node; three.js vendored locally so
the Cosmos renders offline).

## Run

```bash
# from the repo root; needs fastapi + uvicorn in the venv
uvicorn apps.lumi.backend.app:app --port 8400
# → http://localhost:8400  (opens on Home)
```

The app reads the silo's promoted build. Defaults resolve
`synapse-agentic-harness-system/{builds,graph}` next to it; override
with `MERIDIAN_SILO_DIR` / `MERIDIAN_BUILDS_DIR` /
`MERIDIAN_GRAPH_DIR` / `MERIDIAN_SOURCES_DIR`. **No compiled build →
every surface renders its designed empty state with the server's
reason — nothing is mocked, ever.** Builds compiled before E17-A lack
`indexes/sources.json` and `indexes/graph_map.json`; one
`laptop.py compile` lights up the Sources rail and the Cosmos.

The same `.env` as the pipeline rides along: `/api/lumi/planes`
reports the BQ (PSC) and Vertex (proxy) planes as booleans —
configured or not, never values. The app itself calls neither;
enrichment and dry-runs stay with `laptop.py`.

## Surfaces

| route | screen |
|---|---|
| `#/home` | capabilities: hero, six promises, LIVE PROOF, planes, diff, the Sources rail (ledger as trust centerpiece), exclusions |
| `#/semantics` | Explorer — metrics (search + status filters) and tables |
| `#/metric/<id>` · `#/table/<t>` | the profiles — deep-linkable URLs |
| `#/cosmos` | the graph sky from `graph_map.json` (positions baked at compile) |
| `#/artifacts` | Knowledge Files shelf + the staging door (`sources/artifacts/`) |
| `#/operate` | Builds & Diffs + Enrichment Runs from the real reports |

Ask (the conversational surface) ships with E16 and is shown as a
labeled door — never an unlabeled fake.

## Invariants

- The clerk is the only graph writer. This app reads, stages source
  files, and records feedback (`graph/runs/feedback/*.jsonl`) — it
  never writes quads.
- Every number traces to the promoted build or a run report.
- Status is its own axis; a source's display name never implies
  authority. Tiers render ● ◆ ◐ ○; crimson is definition conflict —
  only, ever.

`apps/console` remains the legacy shell (agent theater, scripted
runner); this app is the product and its copy of the read plane
(`backend/meridian.py`) is the canonical one.

## Tests

```bash
python -m pytest apps/lumi/tests/ -q
```
