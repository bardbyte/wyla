# Synapse by Lumi — the product (apps/synapse_admin)

The freshly-built admin surface over the Meridian context graph,
authored directly from the design canvas
(`apps/synapse_admin/design/wireframes/`). One process, **zero build
steps**: FastAPI serves the Meridian read plane and a hand-authored
ES-module frontend (no bundler, no node; three.js vendored locally so
the Cosmos renders offline).

## Run

```bash
# from the repo root; needs fastapi + uvicorn in the venv
uvicorn apps.synapse_admin.backend.app:app --port 8400
# → http://localhost:8400  (opens on Home)
```

The app reads the silo's promoted build. Defaults resolve
`synapse-agentic-harness-system/{builds,graph}` next to it; override
with `MERIDIAN_SILO_DIR` / `MERIDIAN_BUILDS_DIR` /
`MERIDIAN_GRAPH_DIR` / `MERIDIAN_SOURCES_DIR`. **No compiled build →
every surface renders its designed empty state with the server's
reason — nothing is mocked, ever.** Builds compiled before E17-A lack
`indexes/sources.json` and `indexes/graph_map.json`; one
`pipeline.py compile` lights up the Sources rail and the Cosmos.

**Knowledge Files** (Home card + `#/artifacts`) read from two
places: the skills tree (`MERIDIAN_SKILLS_DIR`, any nesting —
`CFR/<skill>`, `CFR/TLS/<semantics>`; defaults to
`<sources>/skills`) and the sources dir, resolved in order:
`MERIDIAN_SOURCES_DIR` → the silo's own `sources/` if present → the
absolute `--sources-dir` recorded by your latest
`pipeline.py build-graph` run (manifests record their input roots).
Clicking a file opens the pullout reader (copy to clipboard, Esc or
✕ to close); the creator stages typed knowledge or dumped text files
into `sources/artifacts/`, and the SharePoint MCP connector button
is a labeled door until the connector service lands. The easiest way to set it: paste
`MERIDIAN_SOURCES_DIR=/path/to/$DATA/sources` into the silo's `.env`
(see `.env.example`) — the app loads it at startup with the
pipeline's own loader, and shell-exported variables always win over
the file. When the shelf is empty the page says exactly which path
it checked. `python scripts/graph_state.py` (in the silo) prints
where the shelf resolves on this machine, along with the promoted
build, statuses, and every enrichment run's blind-gate line.

The same `.env` as the pipeline rides along: `/api/synapse/planes`
reports the BQ (PSC) and Vertex (proxy) planes as booleans —
configured or not, never values. The app itself calls neither;
enrichment and dry-runs stay with `pipeline.py`.

## Sign-in

Three modes, picked by `SAHS_STORE` in the silo `.env`:

| `SAHS_STORE` | who you are | where the chats go | when |
|---|---|---|---|
| `local` (default) | the local developer, admin, no cookie | `graph/runs/chat/sessions.sqlite3`, one shared runtime | a laptop working on the graph |
| `sqlite` | the people in one local file, real sessions and roles | the chat tables in that same file, one runtime per person | trying the sign-in, the account and People pages, tests |
| `spanner` | the people in Cloud Spanner | the chat tables in that database, one runtime per person | the deployment, and a laptop pointed at a dev database |

With `sqlite` or `spanner`, every `/api/chat/*` call needs the session
cookie and every chat, message, project, memory and artifact row carries
the person who made it. What lands where, table by table, and what still
lives on the filesystem: [`docs/spanner-wiring.md`](../../docs/spanner-wiring.md).

With a store, the shell boots as the signed-in person (`js/session.js`):
nobody signed in means every route is the sign-in page, every API call
carries the CSRF header on anything that changes state, and a 401 goes
back to the door. Roles decide the surfaces: admins open this console,
everyone else is sent to `/synapse/`.

The front door is Okta (`OKTA_*` in `.env.example`). To try the whole
hop on a laptop, use the non-production Okta client and add
`http://localhost:8400/callback` to its Login redirect URIs; never the
production client. The email-and-password form (sign-up and sign-in) is
off unless **one flag** opens it: `AUTH_LOCAL_LOGIN=1`. Okta is never
required for it; the two doors are independent, and the sign-in page
draws whichever is configured (both, when both are).

**Local laptop** (email and password on, the store on Spanner, Okta off).
The minimal `.env` block, names only; the values are yours:

```sh
SAHS_STORE=spanner
SPANNER_PROJECT_ID=
SPANNER_INSTANCE_ID=
SPANNER_DATABASE_ID=
SYNAPSE_SPANNER_SA_KEY=          # or GOOGLE_APPLICATION_CREDENTIALS; unset with gcloud ADC
AUTH_PEPPER=                     # 8 characters at least; the store refuses to start without
AUTH_LOCAL_LOGIN=1
AUTH_COOKIE_SECURE=auto          # plain over http://127.0.0.1, Secure behind TLS
AUTH_BOOTSTRAP_ADMIN_EMAIL=      # the sign-up with this address is the first admin
# no OKTA_* lines
```

Then `uvicorn apps.synapse_admin.backend.app:app --port 8810` and open
`http://127.0.0.1:8810/#/signin`: the card shows the email-and-password
form with "Create an account". No Spanner at hand: `SAHS_STORE=sqlite`
with the same `AUTH_*` lines gives the same form on one local file.

When the card says "Sign-in is not configured on this server" it also
says why, from `GET /api/auth/okta` (`local_login_reason`): the flag is
not `1`, the store is `local`, or a store setting is wrong (a pepper
under eight characters is the usual one under `spanner`). Fix that line
and reload; nothing else is cached.

**Production**: Okta on (`OKTA_ISSUER`, `OKTA_CLIENT_ID`,
`OKTA_CLIENT_SECRET`, `OKTA_REDIRECT_URI=https://<host>/callback`,
`AUTH_GROUP_ROLE_MAP` with one group mapped to `admin`), `AUTH_LOCAL_LOGIN`
unset, `SAHS_STORE=spanner` with the same `SPANNER_*` and `AUTH_PEPPER`
lines. The local routes then answer 403 ("sign in with Okta") and the
card offers Okta alone.

`AUTH_COOKIE_SECURE=auto` follows the request: plain over `http://localhost`,
`Secure` behind TLS. Pages: `#/signin?next=`, `#/account` (roles, the
Google connection when BigQuery is delegated to people, sign out),
`#/users` (admins: roles granted or taken back, accounts disabled or
restored, the access contexts behind each person's sign-ins).
`GET /api/whoami` (the same answer as `/api/auth/me`) says who the cookie
is, for a curl.

## Surfaces

| route | screen |
|---|---|
| `#/home` | capabilities: hero + doors, six promises, LIVE PROOF, the Sources rail (ledger as trust centerpiece), exclusions |
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

`archive/apps/console` is the legacy shell (agent theater, scripted
runner); this app is the product and its copy of the read plane
(`backend/meridian.py`) is the canonical one.

## Tests

```bash
python -m pytest apps/synapse_admin/tests/ -q
```

## Synapse by Lumi, the second surface (apps/synapse)

The same server also serves a second frontend at
`http://localhost:8400/synapse/` from `apps/synapse/frontend`: a copy
of this one, stripped for the people who ask questions rather than
steward the graph. The left header reads Synapse Semantic
Intelligence; New chat and Search chats sit at the top, the recent
chats under them, and Data Products, Metrics Explorer and Artifacts in
their own section at the bottom above the account. Home, Skills,
Cosmos and Operate are not there. Search chats is a page of its own
(`/api/chat/search`: every chat, fuzzy, the matching lines shown).
Data Products and Metrics Explorer are cards read from the compiled
build. Artifacts publish inside the chat where the turn made them:
there is no drawer on this surface. The two frontends share the API
and the build; they do not share files, so a change to one is a
change to one. `SYNAPSE_LOGO=/path/to/logo.png` in the silo `.env`
puts an image in the second surface's left header in place of the
words (`/api/synapse/logo` serves it; `/api/synapse/brand` says whether one
is configured).

On the second surface the library reads deeper: Data Products filters
by line of business, a product page lists its columns as a search
with expandable rows that say what each column is (from the
compiler's `columns.json`, or the served card on an older build) and
where it is used, metric cards open in place with the definition and
the table, and Skills (in place of Artifacts) showcases the doctrine
packs with a Use-in-chat door. Skills also lets a person teach one:
material in, the model's draft in the house format out, saved under
`graph/skills/users/<owner>/` and loaded for that person alone. The
same page lists the knowledge files the graph is built from (the
folders under the skills root, the staged drops, the reference docs)
with their authors and last writes; Browse and Add open one pop-up
(bring a file, write a skill, Draft with Synapse). Memory, under
Customize, is `memory.md`: what Synapse remembers, editable. The
chat's "+" offers Add files: PDF and images ride inline, text as
text, Office files converted here.

On both surfaces the composer's model label is a select over the two
model planes (Gemini 3.1 Pro on Vertex, the Gemini 3.x Flash models through
the gateway; each model's engine map is `synapse-agentic-harness-system/docs/model-playbook.md`),
remembered per chat (`POST /api/chat/sessions/{id}/model`), and the
"?" beside the dials explains Chat/Autopilot, Quick/Standard/Deep and
both models from `GET /api/chat/dials`.

A message that reads like several jobs ("compare churn across the
regions, then explain which definitions differ, and build a
dashboard") becomes a task run on both surfaces: the harness splits it
into at most six tasks, runs the independent ones side by side and the
dependent ones after their inputs, and answers once with a "What was
done" document beside the artifacts. The chat shows a task board under
the message — one row per task with its status and cost, the running
row open, the rest folded — and replays it when the chat is reopened.
The events (`plan_made`, `task_started`, `task_done`, and a `task` field
on every record a task's sub-turn emits) ride the same stream; there is
no new route. The mental model, the gate, the budget split and the
report are in `synapse-agentic-harness-system/docs/multi-task-turns.md`.

