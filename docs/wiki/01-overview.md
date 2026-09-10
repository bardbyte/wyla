# 1 · Overview and Repository Layout

**Relevant source files**

- [`README.md`](../../README.md)
- [`synapse-agentic-harness-system/pyproject.toml`](../../synapse-agentic-harness-system/pyproject.toml)
- [`synapse-agentic-harness-system/.env.example`](../../synapse-agentic-harness-system/.env.example)
- [`apps/synapse_admin/backend/app.py`](../../apps/synapse_admin/backend/app.py)
- [`MYSTIFY_REPORT.md`](../../MYSTIFY_REPORT.md)

---

## Purpose and Scope

This page orients you in the tree: what each top-level directory is for,
how to get the system running, and what "running" means when you have no
warehouse credentials.

For the layered architecture see [Page 2](02-architecture.md).

---

## What the system is

Two things that need each other:

1. **A semantic layer.** A provenance-typed graph compiled from the
   artifacts a data team already has — a catalog export, warehouse
   metadata, thirty days of query history, gold SQL, playbooks, a
   glossary — into an immutable build of markdown cards and indexes.
2. **An agent harness.** A thin loop over a general model with a small
   tool kit, hooks that verify rather than trust, and two web surfaces:
   one for the people who steward the graph, one for the people who ask
   questions.

Everything here ran against a real 46-table warehouse. Nothing here
*needs* one: the test suites run on checked-in fixtures, and every screen
renders a designed empty state when no build sits behind it.

---

## Repository layout

```
synapse-agentic-harness-system/     "the silo" — pipeline, graph, compiler, tools, agent, evals
  sahs/
    canon/          c(sql), fingerprints, the authority lattice, the census
    graph/          the quad store, IDs, validator, clerk, crosswalk, LOB, review
    loaders/        one module per source shape + the utilization ledger
    compiler/       reconcile, cards, indexes, census, diff, display, graph_map
    tools/          Build API, resolver, sandbox, SQL validation, MCP server
    loop/           the v1 agent loop, its 14 tools, the world digest, the scout
    assistant/      the v3 thin harness: loop, kit, hooks, artifacts, checks, store
    ask/            the deterministic Ask pipeline and the A2UI answer envelope
    enrich/         the model-written-description loop behind a blind grader
    evals/          harness, graders, capability matrix, navigation, SUTs
    util/           auth (service accounts), console, gateway
  scripts/          pipeline.py + one-off plane checks (bq, vertex, gateway, transport, …)
  tests/            476 tests + 500KB of fixtures
  db/spanner/       DDL for the deployment target (identity, chat, graph)
  docs/             runbooks, contracts (export shapes), specs (design notes), evals
  builds/           compiled builds + CURRENT (gitignored except manifests)
  graph/            the quad store (gitignored on a dev clone)

apps/synapse_admin/  the steward console: FastAPI read plane + ES-module frontend
  backend/           app.py, meridian.py (read plane), chat.py, ask.py
  frontend/          hash router, no build step, three.js vendored
  design/wireframes/ the design canvas the surface came from
  tests/             65 tests

apps/synapse/        the second surface, for people who ask rather than steward
docs/                this wiki, the paper, the deck, the design inventory
archive/             the previous platform. Nothing runs from it.
```

### Why two Python trees

`synapse-agentic-harness-system/` is an installable package (`sahs`) with
its own `pyproject.toml`, extras (`sql`, `dev`, `assistant`) and test
suite. `apps/` are consumers: the admin server imports `sahs` from the
silo through `MERIDIAN_SILO_DIR` and never re-implements a reader.

`apps/synapse_admin/backend/meridian.py` states this explicitly: *one
reader implementation — `sahs.tools.api.Build` — imported from the silo,
never re-parsed here.*

---

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e "synapse-agentic-harness-system[sql,dev,assistant]" fastapi uvicorn httpx
python -m pytest synapse-agentic-harness-system/tests apps/synapse_admin/tests -q
uvicorn apps.synapse_admin.backend.app:app --port 8400
```

- `http://localhost:8400` — the admin console.
- `http://localhost:8400/synapse/` — the ask surface, same server.

With no build compiled you get the shell, the empty states, and the
reasons they are empty. That is deliberate and load-bearing: see
**the reality law** below.

---

## The reality law

> When no compiled build exists every endpoint answers
> `{"available": false, "reason": …}` with HTTP 200 and the UI renders
> its designed empty state. **Nothing is mocked, ever.**
> — [`meridian.py`](../../apps/synapse_admin/backend/meridian.py)

The same law applies to the agent's self-description: every fact in an
answer about the system is read from the promoted build or from a served
capability list. The system never claims a capability it does not have
and names the gated ones plainly
([`sahs/ask/converse.py`](../../synapse-agentic-harness-system/sahs/ask/converse.py)).

And to the model's own briefing: `SYNAPSE.md`, the world digest in the
system prompt, is generated from the build's indexes and nothing else.
*Hand-written prose has no entry point.*

---

## Scale

| dimension | count |
|---|---|
| Python | 45,002 lines · 182 files |
| Frontend JS (excl. vendored three.js) | 10,892 lines |
| Tests | 476 (411 silo · 65 admin), all offline |
| Fixtures | 500 KB checked in |
| Registered relations | 25 |
| Node kinds | 20 |
| Witness families | 15 |
| Governance states | 7 |
| Agent tools (v3 kit) | 12 + `suggest_next` |
| Reference warehouse | 46 tables |

---

## Status

Working end to end on the reference warehouse: the pipeline, the
compiler, the serving tools, both surfaces, the chat under limits, the
enrichment loop behind its blind grader.

Open threads, in the order they matter: the exploratory scout lane is a
first cut; the PPTX export is basic; the model-gateway plane is a
validated candidate rather than the default.

---

## A note on names

This tree has been through a sanitisation pass
([`MYSTIFY_REPORT.md`](../../MYSTIFY_REPORT.md)): employer-specific and
personal references were scrubbed from the living tree while leaving code
logic byte-for-byte unchanged. `archive/` was out of scope.

Consequences you will notice while reading:

- The product is **Synapse** (admin surface: *Synapse by Lumi*).
- The semantic-layer engine is **Meridian** — it is the name in
  `MERIDIAN_*` env vars, `meridian.build/1` schemas and
  `meridian.event/1` envelopes.
- The metadata-service witness family is `lumi`; the catalog witness
  family is `atlas`. Those are source-family names, not vendors.
- Warehouse and project ids in fixtures are `demo-warehouse`,
  `demo-billing`, `demo-vertex`.

---

## Next

→ [Page 2 · Architecture and Data Flow](02-architecture.md)
