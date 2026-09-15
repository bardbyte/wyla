# kc-exploration

A discovery agent for Google Cloud's Knowledge Catalog. You put a question in
plain words; a Gemini model rewrites it into a baseline search and up to three
variations, fires them at the catalog in one batch, merges what comes back,
and tells you which entries matter. It runs on a service-account key for the
catalog and one of two model planes: Gemini 3.1 Pro Preview on Vertex AI, or
Gemini 2.5 Pro through an enterprise gateway.

It is a rewrite of Google's `knowledge-catalog/samples/discovery` sample
without the ADK: the same instruction, the same one tool, but the model
calls, the token handling and the proxy route are this folder's own. The
folder is standalone. Copy it out, push it, run it.

## quick start

```bash
git clone <this repo>
cd kc-exploration
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest -q            # 64 tests, offline, a few seconds
```

The real thing needs a project, a key for the catalog, and a model plane.
Copy `.env.example` to `.env`, fill in `KC_PROJECT_ID` and `KC_SA_KEY`, then
either the `VERTEX_*` block or the gateway block. Prove each plane before
the first question:

```bash
python scripts/kc_check.py                  # the catalog: config, token, one search
python scripts/model_check.py --converse    # the model: token, one tool round trip
python scripts/explore.py "which tables hold card transactions in project demo-warehouse?"
```

`explore.py` streams the answer, then prints every search that ran and
every entry found, with how many of the searches found it. A check that
fails names the variable to set or the role to grant; exit code 3 means
the environment, 1 means the service said no.

## what this is not

- Not the ADK sample. There is no `adk run`; the loop in `kcx/agent.py`
  drives the model directly over REST, so the same code runs on Vertex and
  behind a gateway that speaks Gemini's protocol but not Google's auth.
- Not a hosted service. One process, one question, one answer.
- Not a catalog writer. It only ever calls `searchEntries`.
- The gateway plane was validated against one deployment. Its two URLs are
  not in the code; if yours differs in path form or token shape,
  `GATEWAY_PATH_FORM` and `AUTH_MODE` are the knobs, and `model_check.py`
  is where you find out.
- Without keys nothing runs. The tests run on doubles; every script says
  which variable it is missing and stops.

## how it works

Think of the catalog as a library, the model as a librarian who rewrites
your question three ways before walking the stacks, and each model plane
as a different door into the same building with its own key.

One question is one run of `DiscoveryAgent.run`. The model reads
`SKILL.md` as its instruction and sees one tool. On its first turn it
usually emits several `knowledge_catalog_search` calls at once; the agent
runs them all, answers them all in one user turn (ids and thought
signatures echoed back exactly as they came), and asks again. When a turn
comes back with prose and no calls, that prose is the answer. Along the
way every entry any search returned is merged by its full name and counts
one witness per distinct query that found it, so the entries table under
the answer is ranked by agreement, not by the model's mood.

Read the code in this order:

```
kcx/agent.py        the loop: the batch of calls, the one user turn of answers, the ceilings, the witness merge
kcx/SKILL.md        the instruction the model reads, verbatim from the sample; the predicate grammar lives here
kcx/tools.py        the one tool and its declaration; never raises, keeps its answer under a size
kcx/catalog.py      searchEntries over urllib on the catalog's own key: the request, the row shape, the retries
kcx/vertex.py       the Vertex plane: streamed calls in the native tool protocol; read converse() slowly
kcx/gateway.py      the gateway plane: the signed token request, the token manager, whole calls, thinking as a budget
kcx/planes.py       which plane a question rides, and why
kcx/transport.py    the wire both share: the pinned proxy route, TLS, the service-account token, how a refusal is read
kcx/env.py          the .env files and the variables; nothing here touches a network
```

Two decisions worth knowing before you change anything:

- **One route per connection.** The stdlib proxy handler consults
  `NO_PROXY` on every request, so one connection's bypass list can reroute
  another's calls without anyone noticing. `PinnedProxyHandler` in
  `kcx/transport.py` routes exactly what it was given, and each connection
  carries its own proxies and TLS. `KC_DISABLE_PROXY`,
  `VERTEX_DISABLE_PROXY` and `GATEWAY_ROUTE` are the only switches.
- **The catalog key never borrows the model key.** `KC_SA_KEY` is required
  and has no fallback, not even `GOOGLE_APPLICATION_CREDENTIALS`. The roles
  differ, the projects usually differ, and a silent fallback would bill
  and fail against the wrong one.

## configuration

Everything is read from `.env` in this folder (copy `.env.example`), then
from `KC_EXTRA_ENV_FILE` if set. A shell-exported variable wins over both.
The ones you will touch:

| name | default | what it changes |
|---|---|---|
| `KC_PROJECT_ID` | required | the consumer project every search runs in |
| `KC_SA_KEY` | required | the catalog's key file; needs `roles/dataplex.viewer` and `roles/serviceusage.serviceUsageConsumer` |
| `KC_QUOTA_PROJECT` | `KC_PROJECT_ID` | the project that carries the quota; `none` sends no header |
| `KC_SEARCH_SCOPE` | unset | `projects/<id>` or `organizations/<id>`; unset searches the project's whole organization |
| `KC_PAGE_SIZE` | `50` | rows per search, and the cost dial for what goes back to the model |
| `KC_MODEL_PLANE` | `auto` | `vertex`, `gateway`, or auto (Vertex when configured, else the gateway) |
| `KC_THINKING_LEVEL` | `low` | how hard the model thinks before it searches |
| `KC_MAX_MODEL_CALLS`, `KC_WALL_SECONDS` | `6`, `300` | the ceilings on one question |
| `KC_EXTRA_ENV_FILE` | unset | a second file read after this one, for a model contract kept elsewhere |
| `VERTEX_SA_KEY`, `VERTEX_PROJECT_ID`, `VERTEX_MODEL` | unset, unset, `gemini-3.1-pro-preview` | the Vertex plane |
| `GATEWAY_BASE_URL`, `IDP_TOKEN_URL`, `APP_ID`, `APP_SECRET` | unset | the gateway plane; all four required there |

`.env.example` documents the rest: TLS bundles, proxy switches, the gateway's
path form, scopes and thinking budgets.

## tests

```bash
python -m pytest -q        # 64 tests, a few seconds
```

Everything runs offline on doubles: a scripted model, a fake catalog, a fake
gateway, a scripted SSE stream. A failure in `test_standalone.py` means
something in the folder reached outside it. A failure in `test_agent.py`
after a change to the loop usually means the model turn or the user turn of
answers no longer matches what Gemini expects back.

## layout

```
kcx/            the package: env, transport, catalog, tools, vertex, gateway, planes, agent, SKILL.md
scripts/        kc_check.py, model_check.py, explore.py
tests/          the suite, its doubles, one fixture in the API's shape
.env.example    every variable, with the defaults
```

## status

Works end to end on the doubles. Live runs need the keys. What to read off
the first live run and note here: whether the searches table shows several
queries under one `call` index (the batch happened), whether `projectid=`
predicates appear when a project is named, whether Vertex sends
`functionCall.id`, and whether the gateway batches or serializes the calls.
Searches within one batch run one after another; fanning them out is a
follow-up. Cost per question is roughly four searches of fifty rows going
back into the model; `KC_PAGE_SIZE` is the dial.

## license

No license file yet for this folder. `kcx/SKILL.md` is reproduced from
Google's sample under Apache-2.0; see `THIRD_PARTY_NOTICES.md`.
