# wyla

A semantic layer for a BigQuery warehouse, and an agent that answers data
questions on top of it. The layer is a provenance-typed graph compiled from
the sources a data team already has (catalog exports, warehouse metadata,
query history, gold SQL, playbooks, a glossary). The agent is a thin loop over
a general model with a small tool kit, hooks that verify instead of trust, and
two web surfaces: one for the people who steward the graph, one for the people
who ask questions.

Everything here ran against a real 46-table warehouse. Nothing here needs it:
the test suites run on checked-in fixtures, and every screen renders a designed
empty state when there is no compiled build behind it.

## quick start

```bash
git clone <this repo>
cd wyla
python -m venv .venv && source .venv/bin/activate
pip install -e "synapse-agentic-harness-system[sql,dev,assistant]" fastapi uvicorn httpx
python -m pytest synapse-agentic-harness-system/tests apps/synapse_admin/tests -q
uvicorn apps.synapse_admin.backend.app:app --port 8400
```

Open http://localhost:8400. With no build compiled you get the shell, the
empty states, and the reasons they are empty. The second surface is at
http://localhost:8400/synapse/.

To see it full, you need warehouse exports and credentials this repo does not
ship. The runbook that takes you from exports to a promoted build is
`synapse-agentic-harness-system/docs/runbooks/pipeline_end_to_end.md`; the one
for the chat loop is `e18_ask.md` next to it. Both assume a `.env` in the
silo, copied from `.env.example`.

## what this is not

- Not a general text-to-SQL demo. The model only composes SQL against tables
  and columns the graph has witnessed, and every query is dry-run and cost-
  checked before it can execute.
- Not a hosted service. One process, one machine, a laptop with warehouse
  access was the deployment target throughout.
- Not a package you pip install and point at any warehouse. The loaders read
  specific export shapes (documented under `docs/contracts/`). Adapting them
  is real work.
- The `archive/` tree is the platform this replaced. Nothing runs from it.

## how it works

The pipeline has four stages and one rule: the graph is compiled, never
edited. Sources are read into JSONL quads with provenance on every statement;
a reconciler folds agreeing witnesses and keeps disagreeing ones side by side;
the compiler emits an immutable build (cards, indexes, a census, a diff
against the previous build); the serving tools and the agent read only the
promoted build.

Read the code in this order:

```
synapse-agentic-harness-system/scripts/pipeline.py         the CLI: census, ground, build-graph, compile, promote, enrich
synapse-agentic-harness-system/sahs/canon/canonical.py   c(sql): the canonical form every fingerprint hangs off
synapse-agentic-harness-system/sahs/graph/quads.py        the quad store and the witness vocabulary
synapse-agentic-harness-system/sahs/loaders/               one module per source shape; each emits quads, nothing else
synapse-agentic-harness-system/sahs/compiler/reconcile.py  where witnesses agree, disagree, and how that is recorded
synapse-agentic-harness-system/sahs/compiler/compile.py    quads -> build directory
synapse-agentic-harness-system/sahs/tools/api.py           Build.open(): the read API everything downstream uses
synapse-agentic-harness-system/sahs/assistant/loop.py      the thin agent loop; read this one slowly
synapse-agentic-harness-system/sahs/assistant/kit.py       the eleven tools and their descriptions, verbatim
synapse-agentic-harness-system/sahs/assistant/hooks.py     the checks that run around every tool call
apps/synapse_admin/backend/meridian.py                     the read plane the web surfaces share
apps/synapse_admin/frontend/js/main.js                     hash router, no build step, plain ES modules
```

Two decisions worth knowing before you change anything:

- **Status is its own axis.** A source's display name never implies authority;
  a metric is certified, candidate, or conflicting because of what the
  witnesses said, and the UI renders that with tier marks (● ◆ ◐ ○). Crimson
  means definition conflict and nothing else.
- **Fingerprints pin the SQL dialect.** `canon_version` embeds the sqlglot
  version. Upgrading sqlglot is a deliberate remint (`SAHS_REGEN_GOLDENS=1`),
  never an accidental drift. `tests/test_canon.py` will tell you.

## configuration

All of it lives in `synapse-agentic-harness-system/.env`, loaded by the CLI,
the checks, and the web server alike. Shell-exported variables win over the
file. The ones you will actually touch:

| name | default | what it changes |
|---|---|---|
| `MERIDIAN_SOURCES_DIR` | silo `sources/` | where the source exports live |
| `MERIDIAN_BQ_ARCHIVE`, `MERIDIAN_MDM_ARCHIVE` | unset | the warehouse and metadata archives |
| `MERIDIAN_GRAPH_DIR`, `MERIDIAN_BUILDS_DIR` | silo `graph/`, `builds/` | where quads and builds are written |
| `SYNAPSE_BQ_SA_KEY`, `SYNAPSE_BQ_PROJECT`, `SYNAPSE_BQ_DATA_PROJECT` | unset | the BigQuery plane; without them every query is a dry run |
| `SYNAPSE_VERTEX_SA_KEY`, `VERTEX_PROJECT_ID`, `VERTEX_MODEL` | unset | the model plane for chat and enrichment |
| `SAHS_ALLOW_LIVE`, `SAHS_LIVE_MAX_BYTES` | off, 1 GB | lets the chat run queries, under a scan ceiling |
| `SYNAPSE_USER_NAME` | unset | the name memory is bound to and the account row shows |

`.env.example` documents the rest, including the optional model-gateway plane
(`GATEWAY_*`, `IDP_*`) and its check script.

## tests

```bash
python -m pytest synapse-agentic-harness-system/tests -q     # ~400 tests, a few minutes
python -m pytest apps/synapse_admin/tests -q                 # 65 tests, seconds
```

Everything runs offline on the fixtures under `tests/fixtures/`. Tests that
would need a warehouse or a model stub the plane and assert on what would have
been sent. A failure in `test_canon.py` after a dependency change almost always
means the sqlglot pin moved.

## layout

```
synapse-agentic-harness-system/   the silo: pipeline, graph, compiler, tools, agent, evals
  sahs/                           the package (canon, graph, loaders, compiler, tools, assistant, loop, enrich, evals)
  scripts/                        pipeline.py and the one-off checks (bq, vertex, gateway, planes, transport)
  tests/                          the suite and its fixtures
  docs/                           runbooks, contracts (export shapes), specs (design notes), evals
apps/synapse_admin/               the admin surface: FastAPI read plane + ES-module frontend + the wireframes it came from
apps/synapse/                     the second surface, for people who ask rather than steward (served by the same server)
docs/design_inventory.md          the sweep of the design files
archive/                          the previous platform and the design research, kept for reference only
```

## status

The pipeline, the compiler, the serving tools, and both surfaces work end to
end on the reference warehouse. The chat runs queries under limits. The
enrichment loop (model-written descriptions behind a blind grader) works and
is version-aware. Open threads, in the order they matter: the exploratory
scout lane is a first cut; the PPTX export is basic; the gateway plane is a
validated candidate, not the default.

## license

No license file yet. Add one before publishing.
