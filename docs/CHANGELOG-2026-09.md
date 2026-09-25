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
(153 app, 620 harness); the repository's first CI workflow, green on the PR.

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
- **Research and decisions.** `docs/research/resilience-accuracy-latency.md`:
  no Redis for memory (Spanner already holds it), Memorystore later for hot
  state only, a plan cache with governed keys rather than an answer
  shortcut, the model's context cache over the prompt prefix, the
  accuracy loop from the analyst precedents, a fallback for every
  dependency.

## Still with the team (not code)

Apply `005`, `006` and `007` to E1 (both sign-in hops need `AuthStates`);
widen two `CHECK` lists in `002_chat.sql`; mark the eight governed skills'
frontmatter; prove context caching on the gateway plane; ship the skills
inside the build bundle.

## Numbers, before and after this stretch

| | PR #140 | PR #150 |
|---|---|---|
| app tests | 84 | 153 |
| harness tests | 445 | 620 |
| Spanner tables in the repo DDL | 34 | 44 |
| persistence paths on Spanner under a store | identity only | identity, chats, events, files, skills, knowledge, reviews, builds |
| CI | none | both suites plus the DDL lint on every PR |
