# What shipped: PR #140 to PR #150 (15 to 25 September 2026)

One page for a status update. Each entry says what the change does for a
person using the product or running it, then the mechanism, in that order.
Every PR merged to `main` unless marked otherwise.

## The arc in three lines

- **Knowledge Catalog and observability** (#140 to #143): one table's
  knowledge shaped for the catalog with every fact carrying its witness, a
  Langfuse mirror of every turn.
- **Identity and the front door** (#145, #146): the enterprise identity
  branch landed, Okta sign-in, both surfaces sign in, the sqlite stand-in.
- **The model plane, Spanner everywhere, and the production shape**
  (#147 to #150): several models on the gateway, chats and everything
  beside them in Spanner, one flag for local/dev/prod, skill retrieval for
  packs of any size, multi-task turns.

## PR by PR

### #140 · Knowledge Catalog enrichment module and tab (E23)
One table's knowledge from the promoted build and the graph fold, shaped for
the catalog a person maintains. Every fact carries witness, status and
provenance; an include verdict (copy / review / never) is computed in code;
a model writes prose only from those facts and a deterministic verifier
drops any sentence it cannot trace. New `sahs/kc/` (coverage registry of 312
rows with a completeness gate, assembler, render, model writer with a
per-build cache, verifier, blind gate, exports, read-back loader stub, CLI),
the `kc` witness family and `kc_pushed` relation, the `/api/kc/*` router and
the KC Enrichment tab. 27 new tests.

### #141 · Every table in scope, merged glossary, per-item copy
The KC tab treats every table in the build as first-class, with a readiness
dot per entry section, per-item copy in the shape the catalog console takes,
a merged glossary across tables (one term, many related entries), and
export-all as one zip. Summaries cached per build and graph state.

### #142 · Index the fold once, cache the build and the coverage walk
The tab became fast on a real graph: one fold per graph state with edge and
node indexes, the promoted build parsed once, the coverage payload cached.
Measured on 453 tables: assembling one table 166 ms to 9 ms; the 46-table
list cold 7.8 s to 1.0 s; the tables route first load 36.9 s to 5.8 s.
Fact digests byte-identical before and after.

### #143 · Langfuse as a mirror of the record
Traces, datasets and dataset runs from the event record, one way, off by
default (`SAHS_LANGFUSE=1`). A translator that is a pure function of event
records, the SDK adapter, task files as datasets with graded trials linked
to items, `langfuse_sync.py` (check, datasets, backfill), `run_evals.py
--langfuse`. Nothing on the model's path changes; the trace is asserted
step for step against the record.

### #144 · The Radix Graph story deck, the architecture sheet, the mark
Docs only: an eleven-slide story deck committed with its generator, the
Radix mark (SVG masters and PNGs), and the one-page architecture sheet with
every gate drawn. Reading source for every number turned up two stale
figures in older docs (26 relations, 16 witness families), recorded rather
than silently fixed.

### #145 · Radix on the second surface; scroll, snags; sign-in preflights
The second surface says Radix and Radix Graph (the admin console keeps its
names); the thread stays where you scrolled with a "Latest" pill; failed
steps read as plain snags with the raw error on hover; the Explore shelf is
off that surface for now. Three preflight scripts (Okta, Google, LDAP)
answer per environment whether the provider is reachable, whether our client
and callback are registered, and, only with a local secret, whether it
authenticates; placeholders only in the repo.

### #146 · Enterprise sign-in: their identity branch, Okta on the store, both surfaces sign in
The team's identity branch landed as written (security, auth, admin, access
routers; CORS and CSRF gates; per-person Ask runtimes; user-delegated
BigQuery; builds from Spanner), with the modules it imports written here to
its interfaces (`IdentityStore` on Spanner or the sqlite stand-in). Okta
OIDC sign-in on that store with state parked in `AuthStates` so any pod may
take the callback; both surfaces boot from `/api/auth/me`, send the CSRF
header, gate the nav by permission; sign-in, account and People pages. The
email-and-password routes refuse unless `AUTH_LOCAL_LOGIN=1`; the cookie's
`Secure` flag follows the request scheme. `docs/enterprise-port.md` records
every deviation and how a change carries back. 134 app tests.

### #147 · Composer knobs, the gateway plane with several models, hosts out of source
A Thinking-effort slider with five stops and a Model picker on both
surfaces; the gateway plane serves a catalog of models (`GATEWAY_MODELS`),
each with its own thinking style and cap and derived scopes;
`gateway_check.py --all-models` proves each one. The enterprise hostnames
left the source: `IDP_TOKEN_URL` and `GATEWAY_BASE_URL` are required
settings.

### #148 · Model profiles: one harness, five engine maps
Per-engine thinking levels in one table (`sahs/util/profiles.py`), the
dial's five stops folded onto what each engine accepts; two shipped defects
fixed (the literal "json" thinking level sent to 3.x engines; an unlisted
level sent to 3.5 Flash); 3.7 Flash the gateway default; a `--levels` probe;
Gemini 3 sampling policy (no explicit temperature on thinking models); a
`<style>` prompt section after `<mode>`; `docs/model-playbook.md`.

### #149 · Local email-and-password sign-in on Spanner; the chats in the chat tables
`AUTH_LOCAL_LOGIN=1` alone opens the form; the sign-in status says why the
form is shut (`local_login_reason`) and both cards show it; `/api/whoami`.
Under `SAHS_STORE=spanner|sqlite` the chats leave the per-person sqlite
file: `SpannerAssistantStore` holds sessions, messages, artifacts,
projects, memories, plan versions and feedback in `002_chat.sql`'s tables,
every row owned by the signed-in person, one runtime per person, the cookie
required on every chat route. The Google consent hop parks its state in
`AuthStates`. A real bug fixed: `JsonObject` imported from the wrong
package, so any dict cell would have failed on real Spanner. A fake Spanner
SDK database (`tests/fake_spanner.py`) proves the Spanner code path without
a Spanner. `docs/spanner-wiring.md`: every persistence path, its store
class, its table or its gap.

### #150 · Production-ready (draft at time of writing)
Four workstreams integrated on `claude/production-ready`; both suites green
(158 app, 654 harness); the repository's first CI workflow, green on the PR.

- **One flag for local / dev / prod.** `SAHS_ENV_FILE` picks the profile;
  `env/{local,e1,e2,e3}.env.example`; `make run ENV=e1`, `make check
  ENV=e3` (one readiness table from every existing check), `make test`,
  `make ddl-check`; `docs/deploy.md` with the day-one checklist; tenancy by
  deployment (one database per environment, no `TenantId`).
- **Everything in the store.** `005_build_bundles.sql` (the live bundle
  tables the repo lacked) and `007_content.sql` (file bytes chunked like
  builds; the review board). The turn event stream in `ChatEvents` with
  replay after a pod restart; the Ask lane on the Spanner store; chat
  files, own skills, knowledge files and the review board through
  `SpannerContentStore`; the shelf and the staging door read through the
  store. `.env` parsed once per process; `spanner_check.py --emit-ddl`.
- **Skill retrieval for packs of any size.** A Markdown-structure chunker
  and a sqlite FTS5 index with BM25, keyed by content hash. A skill within
  the engine's whole-load budget loads whole, byte-identical to before;
  over it, it loads as a library (table of contents plus the passages that
  match the ask, `skill_toc` / `skill_search` / `skill_read`). One
  `skills_loaded` record per turn; a lexical routing hint. On a synthetic
  2.5 MB pack: 20/20 first-hit on rephrased asks, 432/432 with one ask per
  section, 3 to 5 ms a search. No refusals: a pack that does not fit is a
  library, never an error (this change is landing on the same PR).
- **Multi-task turns.** A compound ask becomes up to six tasks with
  dependencies; independent tasks run side by side on a bounded pool under
  the turn's budget, dependent ones in order with the finished tasks'
  findings; a synthesis turn and a "What was done" artifact; a task board
  on both chat pages; simple asks pinned byte-identical.
- **No refusals in skill loading.** `SkillTooLarge` and the fail-closed
  path are gone: a pack that fits loads whole, one that does not loads as
  a library with `mode: library · preferred: whole|sectioned` and the
  reason on the record; the v1 navigator gets the same static library
  without tools; the only refusal left is an unreadable file, by name.
  The index falls back to memory or a single chunk rather than failing a
  turn.
- **Langfuse insight, the mirror rebuilt from Spanner.** `langfuse_sync.py
  backfill --from spanner` replays every turn in `ChatEvents` through the
  tracer with deterministic trace, span and score ids (a second run
  creates nothing); a prompt fingerprint per turn (`ASSISTANT_VERSION`
  plus hashes of the prefix and each part) recorded on every generation,
  and the prompt parts registered with labels; three dataset builders
  (precedents from a checked-in JSONL, silver from thumbed-up or clean
  turns, scenarios from `plan_made`); `run_evals.py --sut assistant|planner`
  on item files; `langfuse_sync.py coverage` prints which Langfuse concept
  is built from which Spanner columns. The guide is
  `docs/runbooks/langfuse-insight.md`.
- **Laptop-test fixes.** A 3.x model choice no longer answers 422: the
  chat API's `model` fields take 64 characters, the store writes the
  choice whole, and `008_chat_model.sql` widens `ChatSessions.Model` and
  lists every artifact type (`kpi` included); the DDL lint understands
  the ALTERs. Sensitive columns are readable under the new default
  `SAHS_SENSITIVE_COLUMNS=allow` (the read is noted on the check record;
  `deny` restores the refusal per environment). The chat's skill picker
  reads `MERIDIAN_SKILLS_DIR` as well as `<graph>/skills`, nested
  folders included, with `scripts/skills_check.py` to list what it sees
  and why a file was skipped. The composer's pills are one-line controls,
  the Thinking-effort pill names the chosen stop, the stop is an icon
  button that actually stops the model mid-stream, the model rows say
  in plain words when to pick each (engineer facts on hover), and the
  assistant is Radix on both surfaces.
- **Research and decisions.** `docs/research/resilience-accuracy-latency.md`:
  no Redis for memory (Spanner already holds it), Memorystore later for hot
  state only, a plan cache with governed keys rather than an answer
  shortcut, the model's context cache over the prompt prefix, the
  accuracy loop from the analyst precedents, a fallback for every
  dependency.

### After #151 · the laptop round two (branch `claude/production-ready`, PR #152)
- **Charts and dashboards.** Number formatting follows the data (a
  percent shows the decimals it carries, counts group thousands, big
  columns go compact), one formatter in Python with a byte-equal JS twin;
  a written selection heuristic the tools use and state (time → line,
  category → sorted bar or horizontal bar, part-of-whole → stacked or
  percent bar and never a pie, a grid → heatmap, many series → small
  multiples, one number → KPI); twelve chart kinds hand-drawn as SVG;
  dashboards with a grid; "Show all" on tables fixed (state on the
  wrapper, not the button's words); the disclaimer strip wraps instead of
  clipping. `docs/visualizations.md`.
- **Chat UX.** The whole pane scrolls (the gutters and the chips no longer
  dead or trapping the wheel); thinking folds by default into "Radix is
  thinking… 4s" and "Thought for 12s", the person's choice remembered;
  every turn shows elapsed time and tokens live and in a footer on the
  message; `009_usage.sql` stores tokens, calls, elapsed and turns per
  chat, shown on the shelf, in chat search and as a Tokens column per
  person on the People page; the second surface's wordmark reads
  "Systematic Intelligence by Lumi".
- **Auth proven end to end on fakes, six bugs fixed.** Okta sign-in,
  the Google consent hop, the encrypted connection, the refresh, and a
  BigQuery query carrying the person's own token under the sandbox gates,
  in both lanes and on both stores. Fixed: the chat lane had no per-person
  runner; the dry run demanded a service-account key; a failed refresh was
  a raw exception; a Google Bearer on a cookie session was refused; user
  mode without a project was a 500; disconnect left a cached token alive.
  `docs/reports/auth-e2e.md` names what still needs a laptop.
- **Carry-back and sanitization.** `docs/enterprise-carry-back.md`: the
  full procedure to a new branch of the enterprise repository with the
  final `.env` per environment; `docs/reports/sanitization-scan.md` and the
  edits it called for, so nothing in the code says where it came from.

## Still with the team (not code)

Apply `005`, `006` and `007` to E1 (both sign-in hops need `AuthStates`);
widen two `CHECK` lists in `002_chat.sql`; mark the eight governed skills'
frontmatter; prove context caching on the gateway plane; ship the skills
inside the build bundle.

## Numbers, before and after this stretch

| | PR #140 | PR #150 |
|---|---|---|
| app tests | 84 | 186 |
| harness tests | 445 | 684 |
| Spanner tables in the repo DDL | 34 | 44 (9 DDL files) |
| persistence paths on Spanner under a store | identity only | identity, chats, events, files, skills, knowledge, reviews, builds |
| CI | none | both suites plus the DDL lint on every PR |
