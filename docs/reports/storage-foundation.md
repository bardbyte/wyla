# Workstream report: storage foundation

Nothing pushed, no PR opened. Nothing here was run with real credentials,
hosts, tokens or
certificates; every check in the tests runs on canned exit codes or the fake
SDK database.

## What changed, file by file

### 1. DDL reconciliation

- `synapse-agentic-harness-system/db/spanner/005_build_bundles.sql` (new):
  `BuildBundles` keyed by `BuildId` (Format, SizeBytes, Sha256, FileCount,
  ChunkBytes, ChunkCount, Complete BOOL DEFAULT false, PublishedBy,
  PublishedAt commit timestamp), interleaved in `Builds` ON DELETE CASCADE;
  `BuildBundleChunks` keyed (`BuildId`, `Seq`) with `Chunk BYTES(MAX)`,
  interleaved in `BuildBundles` ON DELETE CASCADE. The interleave follows
  `sahs/builds/spanner_store.py`: a re-publish does `DELETE FROM Builds`
  first, so the cascade removes the old bundle and its chunks; the header
  and bundle rows land in one transaction, chunks one commit each, `Complete`
  flips last. `scripts/spanner_ddl_check.py` globs `*.sql` and needed no
  change; it reports 39 tables across 6 files.
- `synapse-agentic-harness-system/db/spanner/README.md`: the table now lists
  all six files (and the reserved 007), "applied in file order", not "three".
- `synapse-agentic-harness-system/scripts/spanner_check.py`: `--emit-ddl`
  prints CREATE TABLE statements for live tables the repo lacks, from the
  schema listing `inspect` already fetches plus two extra catalog queries
  (primary keys from `INFORMATION_SCHEMA.INDEX_COLUMNS`, nullability from
  `COLUMNS`); `emit_ddl(report, designed=, keys=, nullable=)` is the pure
  function, parents before children, interleaves from `parent`, `?` for a
  key it was not given.

### 2. The turn event stream into `ChatEvents`

- `sahs/assistant/spanner_store.py`: `CHAT_SQLITE_SCHEMA` gains `ChatEvents`
  (so the sqlite stand-in and `tests/fake_spanner.py` have it); new verbs
  `add_event(session_id, record)` (Seq is the bus's seq, TurnId, Ev, Ts,
  Payload = the whole record as `JsonValue`), `events(session_id, after_seq)`
  (owner-filtered through `ChatSessions`), `last_event_seq`, `last_turn`
  (turn id, first seq, closed).
- `sahs/assistant/runtime.py` (`_SessionRuntime`, `runtime()`, new
  `events_since`, `turn_window`): when `self.store` has `add_event`, each
  session's bus gets a sink that inserts every record; a store error is
  logged (`sahs.assistant.runtime`), counted in `store_errors`, never raised.
  The bus resumes numbering from `last_event_seq` so a rebuilt bus never
  collides with stored rows. `events_since(session_id, after)` serves the
  bus first and fills any gap below the bus's first seq from the store (a
  gap the store cannot fill is remembered per runtime so a 50 ms poll does
  not re-query). `turn_window` falls back to `last_turn` when no thread runs
  and the bus is empty: an unclosed stored turn is reported with `running:
  False`, its id, `after`, and `interrupted: True`. The store is read at
  `runtime()` time, not construction, because `backend/chat.py` assigns the
  chat-table store after building the runtime. The JSONL sink is untouched
  in every mode.
- `sahs/ask/events.py` (outside my list; one small method): `EventBus.resume(seq)`.
- `apps/synapse_admin/backend/chat.py`, the `stream` endpoint only: the pump
  reads `runtime.events_since(session_id, seq)` instead of the bus directly.

### 3. The Ask lane on the store

- `sahs/ask/runtime.py`: `AskRuntime(..., store=)` kwarg (and `runtime.store`
  may be assigned afterwards, as before); `sessions()` lists only kinds
  `analyst`/`steward` (the chat tables also hold `assistant` chats).
- `apps/synapse_admin/backend/ask.py`: under a store (`spanner_is_enabled()`)
  and a real user id, the runtime's store becomes
  `SpannerAssistantStore(_identity().db, owner)`, exactly as `chat.py` does.
  Every `SessionStore` verb the lane and the router call (`create_session`,
  `get_session`, `list_sessions`, `set_skills`, `set_title`, `add_message`,
  `messages`, `add_plan_version`, `plan_versions`, `latest_plan`,
  `add_feedback`, `feedback`) already existed on `SpannerAssistantStore` with
  compatible signatures; nothing was missing.

### 4. Settings resolved cheaply

- `sahs/util/auth.py`: `load_dotenv` parses a file once per process, cached
  by resolved path + (size, mtime_ns) in `_DOTENV_CACHE`; each call still
  applies the pairs against a fresh `os.environ` (shell wins; `override=`
  honoured), an edited file is re-read, `reset_dotenv_cache()` for tests.
  `sahs/spanner.py` did not need a change (its `load_dotenv()` call is now a
  stat plus a dict loop).

### 5. One flag, four profiles

- `synapse-agentic-harness-system/env/{local,e1,e2,e3}.env.example` and
  `env/README.md` (new): the complete variable set per profile, placeholders
  only (`<...>`); local = `SAHS_STORE=sqlite` + `AUTH_LOCAL_LOGIN=1` + Vertex /
  gateway placeholders; e1/e2/e3 = `SAHS_STORE=spanner`, `EPAAS_ENV`,
  `SPANNER_*`, `AUTH_PEPPER`, `OKTA_*`, `GATEWAY_*`, `IDP_TOKEN_URL`,
  `GATEWAY_BASE_URL`, `MERIDIAN_BUILDS_SOURCE=spanner`; e1 keeps
  `AUTH_LOCAL_LOGIN=1`, e2 sets `0`, e3 omits it. No variable was renamed or
  added; every name in the examples is documented in `.env.example`.
- `.gitignore` (outside my list; four lines at the end): the repo's `env/`
  rule ignored that directory, so the examples and README are un-ignored and
  filled-in `env/*.env` files stay ignored.
- `Makefile` (new, repo root): `run ENV=<env>` (exports `SAHS_ENV_FILE`,
  uvicorn on 8810), `test`, `check ENV=<env>` (readiness), `ddl-check`;
  `ENV_FILE=` and `PORT=` overrides.
- `synapse-agentic-harness-system/scripts/readiness.py` (new): loads the
  profile's file, runs `spanner_ddl_check.py`, `spanner_check.py`,
  `okta_check.py --envs <E> --no-matrix`, `gateway_check.py --only
  token,generate`, `vertex_check.py`, `bq_check.py`, `google_auth_check.py`
  as subprocesses (runner injectable), plus an in-process settings row, and
  prints one table: check, verdict (`ok` / `missing setting <NAME>` / `bad
  setting <NAME>` / `unreachable` / `failed` / `skipped`), reason. Exit 1
  when a required check is not ok (settings, ddl always; spanner, okta,
  gateway in e1/e2/e3; anything that ran and did not pass). `--json` too.
- `docs/deploy.md` (new): the flag, the four profiles, what each check
  proves, the DDL apply order 001..007, the tenancy decision, a ten-step
  day-one checklist. Linked from `README.md`, `apps/synapse_admin/README.md`
  and `docs/README.md`.

### 6. Tenancy decision

- `docs/deploy.md` and the top of `docs/spanner-wiring.md`: tenancy by
  deployment (one Spanner database per environment); no `TenantId`, none
  added; per-person isolation by `OwnerUserId`/`UserId`; `UserRoles.Scope`
  and `KnowledgeFiles.BusinessUnit` are the seams for business-unit scoping.
  The wiring table: events and the Ask lane moved to "In Spanner", builds
  row names the bundle tables, the Ask lane's own JSONL event log is the
  remaining gap, the parsed `.env` is listed under process memory.

### 7. CI

- `.github/workflows/tests.yml` (new): pull_request and push to main,
  Python 3.11, `pip install -e 'synapse-agentic-harness-system[identity,dev,assistant,sql]'
  fastapi uvicorn httpx langfuse`, both suites, `python scripts/spanner_ddl_check.py`
  from inside the silo. No secrets, no network beyond pip.

## Tests added and exit codes

New files in `synapse-agentic-harness-system/tests/`:

- `test_spanner_emit_ddl.py` (3 tests): `--emit-ddl` on a canned listing;
  the repo DDL's bundle columns equal the writer's INSERT columns.
- `test_chat_events_store.py` (4 tests, sqlite and fake-SDK backends): rows
  land in `ChatEvents` in order for the owner only; the runtime's bus sinks
  every record and the JSONL still exists; a fresh runtime with an empty bus
  replays from the table, numbers on from its head, and a third pod reports
  the interrupted turn; a store that raises never fails the emit; a gap the
  table cannot fill is asked once.
- `test_readiness.py` (7 tests): the table for e1 with every check ok, named
  missing and bad settings, exit codes to verdicts, local's required set,
  `main` end to end on a temp file (text and `--json`), the four examples
  carry the complete set with placeholders only, and each e* example is
  refused until filled in.
- `test_dotenv_cache.py` (2 tests): parsed once, environment read fresh,
  override still wins, an edited file re-read, reset.
- `test_spanner_ddl.py` (+1 test): 005's shape and every DDL file named in
  the README.

In `apps/synapse_admin/tests/test_local_login.py` (+2 tests, on the fake SDK
database through the app): a chat's events land in `ChatEvents` and the
stream, the session's `head`/`turn_after` and a resumed `after=` all replay
after `_RUNTIMES` is cleared; the Ask lane's session (kind analyst), messages,
plan version and feedback land in `ChatSessions`/`ChatMessages`/
`ChatPlanVersions`/`ChatFeedback` with the owner, the two shelves do not mix,
the steward hat is refused to an analyst, the sqlite Ask file stays empty and
another person sees nothing.

Runs (from the worktree):

- `python -m pytest -q synapse-agentic-harness-system/tests -p no:cacheprovider`
  from inside the silo: 577 passed, exit 0 (baseline before my changes: 557
  passed, exit 0).
- `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider`
  from the repo root: 147 passed, 2 skipped, exit 0 (baseline: 145 passed,
  2 skipped). Note: in this container `sahs` is not pip-installed, so the
  app suite fails at collection from the repo root without `PYTHONPATH`
  (three test modules import `backend/app.py`, which imports `sahs` at
  module level); the same was true before my changes. CI installs the
  package with `pip install -e`, and `make test` sets `PYTHONPATH`.
- `python scripts/spanner_ddl_check.py`: ok, 39 tables across 6 files, exit 0.
- `python scripts/readiness.py --env local --env-file env/local.env.example`:
  the table renders; verdict `bad setting AUTH_PEPPER` (the placeholder),
  exit 1, as designed; the vertex and bigquery rows ran their scripts, which
  stopped at "key not found on disk" without any network.

## Not finished, and why

- The Ask lane's own event bus (`sahs/ask/runtime.py` `_SessionRuntime`)
  still writes only its JSONL; the sink and replay were built for the chat
  lane's runtime as the task specified. The same `_keep`/`resume` pattern
  applies verbatim if the Ask lane should follow; listed as the remaining
  gap in `docs/spanner-wiring.md`.
- The chat page (another workstream) replays only when `running` is true;
  `turn_window` now also reports an interrupted stored turn (`interrupted:
  True`, `running: False`, its `after`), and the stream endpoint serves any
  `after` from the table, so the page can pick that up without a backend
  change.
- `readiness.py` does not run `okta_check.py`'s cross-environment matrix or
  either provider's `--inventory` (both need a browser); it reports the
  registration probe only.

## Files outside my ownership that I touched

- `synapse-agentic-harness-system/sahs/ask/events.py`: one method,
  `EventBus.resume(seq)` (nine lines), so a rebuilt bus can continue the
  stored numbering without reaching into a private attribute.
- `.gitignore`: four rules appended at the end (the existing `env/` rule
  would have ignored the new example files).
- `README.md`, `apps/synapse_admin/README.md`, `docs/README.md`: a link and a
  paragraph each to `docs/deploy.md`.
- `apps/synapse_admin/tests/test_local_login.py` and
  `synapse-agentic-harness-system/tests/test_spanner_ddl.py`: tests (mine to
  extend).

Untouched, as required: `sahs/util/spanner/settings.py`,
`sahs/identity/authorization.py`, `db/spanner/001_identity.sql`; the
`/api/chat/reviews*` routes; every environment variable name.
