# Spanner wiring: what the app persists, and where it lands under `SAHS_STORE=spanner`

`SAHS_STORE` picks the book the app writes in. `local` is one laptop, one
developer, no accounts: everything goes to files under the silo's `graph/`.
`spanner` is the deployment: the people, their sessions, their chats and
what they file beside a chat go to one Cloud Spanner database
(`db/spanner/001_identity.sql`, `002_chat.sql`, `004_google_oauth.sql`,
`006_external_identities.sql`, `007_content.sql`). `sqlite` is the same
tables in one local file, so a laptop can rehearse the deployment without
a Spanner.

Tenancy is by deployment: one Spanner database per environment (e1,
e2, e3), selected by the one `.env` that `SAHS_ENV_FILE` names
(`docs/deploy.md`). There is no `TenantId` column and none is added.
Per-person isolation is by owner — `OwnerUserId` on every chat row,
`UserId` on memories and feedback, and every store read filters on it.
`UserRoles.Scope` and `KnowledgeFiles.BusinessUnit` are the seams for
business-unit scoping later; both stay `''` until a decision needs them.

Think of it as a hotel: the identity tables are the front desk's register
(who is here, which key opens what), the chat tables are each guest's
room, and the filesystem is the shared lobby, the same for everyone. Under
`spanner` the register and the rooms are in the database, and the last of
the lobby's filing has moved: a guest's files and own skills into their
room, the review board and the knowledge files into the front desk's
shared register (they are shared by design). What is listed below as a
gap is still in the lobby.

Three classes carry every database write. `IdentityStore`
(`sahs/identity/store.py`) owns the identity tables;
`SpannerAssistantStore` (`sahs/assistant/spanner_store.py`) owns the chat
tables; `SpannerContentStore` (`sahs/assistant/content_store.py`) owns the
content tables — `ChatFiles` and `ChatFileChunks`, `UserSkills`,
`KnowledgeFiles`, and the review board's four. All three speak portable
SQL to one `Database` object (`sahs/identity/database.py`):
`SpannerDatabase` on the Cloud Spanner SDK, `SqliteDatabase` on the
stand-in. The app opens that object once (`backend/auth.py`, `_identity()`)
and hands it to every store, so one connection serves them all.

## In Spanner

| what | route or module | store class | Spanner table |
|---|---|---|---|
| sign-up (email and password) | `POST /api/auth/signup` (`backend/auth.py`) | `IdentityStore.signup` | `Users`, `UserCredentials`, `UserRoles`, `AuthSessions`, `AuditEvents` |
| login, lockout | `POST /api/auth/login` | `IdentityStore.login` | `Users`, `UserCredentials`, `LoginAttempts`, `AuthSessions`, `AuditEvents` |
| Okta sign-in | `GET /api/auth/okta/start`, `GET /callback` (`backend/okta.py`) | `IdentityStore.put_state`, `pop_state`, `find_or_create_external_user`, `set_roles`, `issue_session` | `AuthStates`, `Users`, `ExternalIdentities`, `UserRoles`, `AuthSessions`, `AuditEvents` |
| the session cookie, every request | `bind_request_user` middleware (`backend/app.py`), `current_user` | `IdentityStore.session_user` | `AuthSessions` (read, `LastSeenAt` touched), `Users`, `UserRoles` |
| sign-out, sign-out everywhere | `POST /api/auth/logout`, `/logout-all` | `IdentityStore.logout`, `logout_all` | `AuthSessions` (`RevokedAt`), `AuditEvents` |
| password reset (break-glass) | `POST /api/auth/reset-password` | `IdentityStore.direct_reset_password` | `UserCredentials`, `AuditEvents` |
| roles, permissions | seeded on first use; `POST /api/admin/users/{id}/roles` (`backend/admin.py`) | `IdentityStore._ensure_roles`, `grant_role`, `revoke_role` | `Roles`, `Permissions`, `RolePermissions`, `UserRoles`, `AuditEvents` |
| people (admin) | `GET`/`PATCH`/`DELETE /api/admin/users` | `IdentityStore.list_users`, `update_user`, `delete_user` | `Users` (soft delete: `Status`, `DeletedAt`), `AuditEvents` |
| access contexts (admin) | `GET /api/admin/access` (`backend/access.py`) | `IdentityStore._query` | `AuditEvents`, `Users` (read) |
| Google connection (user-delegated BigQuery) | `GET /api/auth/google/start`, `/callback`, `/connection` | `IdentityStore.put_state`, `pop_state`, `save_google_connection`, `revoke_google_connection` | `AuthStates` (the consent hop's state), `GoogleOAuthConnections` |
| chats | `POST`/`GET /api/chat/sessions*` (`backend/chat.py`) | `SpannerAssistantStore.create_session`, `list_sessions`, `set_title`, `set_flag`, `set_model`, `set_skills`, `set_project`, `set_handoff`, `set_notes` | `ChatSessions` |
| messages (the person's and the assistant's) | `POST /api/chat/sessions/{id}/messages`, `/run`, `/chart`; the loop (`sahs/assistant/loop.py`) | `SpannerAssistantStore.add_message`, `messages` | `ChatMessages` (`Seq` from `ChatSessions.MessageCount`) |
| search chats | `GET /api/chat/search` | `SpannerAssistantStore.list_sessions`, `messages` (read) | `ChatSessions`, `ChatMessages` |
| artifacts, versions, the PPTX export | the loop; `GET /api/chat/artifacts/{id}*` | `SpannerAssistantStore.add_artifact`, `update_artifact`, `get_artifact`, `list_artifacts`, `artifact_versions` | `ChatArtifacts` |
| projects | `GET`/`POST /api/chat/projects*` | `SpannerAssistantStore.create_project`, `update_project`, `list_projects` | `ChatProjects` |
| memory (the memory pass; `memory.md`) | the loop; `GET`/`PUT /api/chat/memory.md`, `/memories*` | `SpannerAssistantStore.add_memory`, `retire_memory`, `list_memories` | `ChatMemories` (retire sets `Status`, `RetiredAt`) |
| plan versions, feedback (the assistant lane) | the loop | `SpannerAssistantStore.add_plan_version`, `add_feedback` | `ChatPlanVersions`, `ChatFeedback` |
| the event stream the page replays | `sahs/assistant/runtime.py` (`_SessionRuntime`: a bus sink); `GET /api/chat/sessions/{id}/stream`, `turn_window` (`GET /api/chat/sessions/{id}`) | `SpannerAssistantStore.add_event`, `events`, `last_event_seq`, `last_turn` | `ChatEvents` (`Seq` is the bus's own; the whole record is `Payload`). The in-memory bus answers first; a pod that restarted serves the stream from the table and numbers new events on from its head. A store write that fails is logged, never fails the turn. The JSONL under `graph/runs/chat/events/` is still written in every mode |
| Ask sessions, messages, plans, feedback (the E18 lane) | `/api/sessions*` (`backend/ask.py`, `sahs/ask/runtime.py`) | `SpannerAssistantStore.create_session` (kind `analyst` or `steward`), `add_message`, `messages`, `add_plan_version`, `plan_versions`, `add_feedback`, `set_skills`, `set_title` | `ChatSessions`, `ChatMessages`, `ChatPlanVersions`, `ChatFeedback`, bound to the person as the chat lane is; the lane's shelf lists the two hats, the chat shelf the `assistant` kind |
| compiled builds (when `MERIDIAN_BUILDS_SOURCE=spanner`) | `sahs/builds/spanner_store.py` | `SpannerBuildStore` | `Builds` (`003_graph.sql`), `BuildBundles`, `BuildBundleChunks` (`005_build_bundles.sql`) |
| the files on a chat: the manifest, the converted text, the bytes | `GET`/`POST /api/chat/sessions/{id}/files`, `DELETE …/files/{file_id}`; the message that carries them (`AssistantRuntime.start_turn` → `_file_parts`, `_mark_files_sent`) | `SpannerContentStore.add_file`, `files`, `remove_file`, `file_bytes`, `file_text`, `parts_for`, `mark_sent` | `ChatFiles` (`002_chat.sql`; `ObjectPath` null), `ChatFileChunks` (`007_content.sql`: the bytes in slices of at most 8 MiB, interleaved in the file) |
| a person's own skills | `POST`/`DELETE /api/chat/skills/mine` (`sahs/assistant/authoring.py`); the shelf, the loop's skill index and the `load_skill` tool (`sahs/assistant/skills_loader.py`, `bind_own_skills`) | `SpannerContentStore.save_skill`, `delete_skill`, `my_skills` | `UserSkills` (`002_chat.sql`) |
| the review board (skills and knowledge files awaiting a manager) | `/api/chat/reviews*` (`AssistantRuntime.reviews` is the store when one is on) | `SpannerContentStore.submit`, `resubmit`, `start_ai`, `record_ai`, `decide`, `withdraw`, `get`, `list`, `find`, `text_of`, `notices`, `mark_seen` — the events folded by `sahs/assistant/reviews.py`'s `fold_records`, the same fold as the ledger's | `ReviewSubmissions`, `ReviewVersions`, `ReviewEvents`, `ReviewSeen` (`007_content.sql`); one board per deployment |
| approved knowledge files | `POST /api/chat/reviews/{id}/decision` (publish: `AssistantRuntime._publish_submission`) | `SpannerContentStore.stage_knowledge` (an approved resubmission replaces its earlier version) | `KnowledgeFiles` (`002_chat.sql`) |
| the staging door and the Knowledge Files shelf | `POST /api/meridian/artifacts`, `GET /api/meridian/artifacts`, `/artifact_file` (`backend/meridian.py`, `_content_store`) | `SpannerContentStore.stage_knowledge`, `knowledge_files` | `KnowledgeFiles`; the door needs a signed-in person (`StagedBy` is a foreign key to `Users`) |

Under a store every `/api/chat/*` route needs the session cookie, and each
signed-in person gets their own `AssistantRuntime` whose store is bound to
their user id: every chat row carries `OwnerUserId`, every memory `UserId`,
every file's chat is checked against the owner, every own skill is keyed
by `UserId`, and a read never crosses owners. The board and the knowledge
files are the exceptions by design: everyone on the deployment reads the
same submissions and the same staged files, and only the seen-mark
(`ReviewSeen`) is the person's. Under `local` the one shared runtime, the
sqlite file `graph/runs/chat/sessions.sqlite3`, the workspaces, the
skills tree, the reviews ledger and `sources/artifacts/` stay as they
were: `AssistantRuntime.content_store` is `None` and every path below the
graph is byte-for-byte the one before.

Two things the content store does not change: the built-in packs
(`sahs/assistant/skills/*.md`) and the shared user packs
(`<graph>/skills/*.md`) are still read from disk under every store mode —
only the person's own packs moved — and the build-graph run still reads
`sources/` from the filesystem, so a `KnowledgeFiles` row reaches the
graph when the ingest step exports the active rows before it runs
(`IngestedRun` stays null until one does; `docs/spanner_schema.md` §4.10).

## Not yet in Spanner

These still write to files, in every store mode. Each names the table it
would land in, or the table it would need.

| what | route or module | writes today | table it would use | why not yet |
|---|---|---|---|---|
| the Ask lane's event log | `sahs/ask/runtime.py` (`_SessionRuntime`) | `graph/runs/ask/[users/<id>/]events/<session>.jsonl` | `ChatEvents` | the chat lane's bus sink is in `sahs/assistant/runtime.py`; the Ask runtime keeps its own `_SessionRuntime` and has not taken the sink yet |
| the build-graph run's read of the knowledge files | `pipeline.py build-graph` (`--sources-dir`) | reads `<sources>/artifacts/` | `KnowledgeFiles` (read) | the graph build is a filesystem job in the first rollout; its ingest step is to export the active rows to `sources/artifacts/` before it runs and write `IngestedRun` after (`docs/spanner_schema.md` §4.10). Until then a staged row is on the shelf, and the app says so, never pretended into the graph |
| Knowledge Catalog: the push record, the model cache, the gate | `POST /api/kc/push-record/{table}` (`sahs/kc/write.py`) | `graph/<push_record_dir>/…`, `llm_<version>.json` | none | a run report on the graph's filesystem, by design (`db/spanner/README.md`: the graph stays on the filesystem in the first rollout) |
| Knowledge Catalog: the read-back import (pending quads) | `POST /api/kc/witness-import` (`sahs/kc/witness.py`) | `graph/nodes/*.jsonl`, `graph/edges/*.jsonl` | `GraphNodeAssertions`, `GraphEdgeAssertions`, `GraphRuns` (`003_graph.sql`) | the graph is the clerk's, on the filesystem, until the graph half moves (`docs/spanner_schema.md` §5) |
| feedback on the admin surfaces | `graph/runs/feedback/*.jsonl` (`backend/meridian.py`) | JSONL | none | a laptop log, never read back by the app |
| the sandbox workspace (a turn's rows, cells, snapshots) | `sahs/assistant/sandbox.py` | `graph/runs/chat/workspaces/<session>/` | none | scratch for one turn, not a record; a turn still prepares the folder under every store mode, but no file of the person's is written there (`_file_parts` reads the store) |

One honest seam in the wiring above: `AssistantRuntime.owner` — the key
the loader binds a person's store-backed shelf under, and the
`submitter_slug` a submission carries — is still the slug of the
person's display name, as it was on the laptop, not their user id. The
rows themselves are keyed by `UserId`; two people with the same display
name would share the slug for those two lookups. Moving `owner` to the
user id under a store is a one-line change in `runtime.py` outside this
lane's ownership.

## Process memory, by design

These are caches or per-request state, not records; nothing is lost when a
pod restarts.

| what | where | note |
|---|---|---|
| the five-second session cache | `backend/auth.py`, `_session_cache` | a positive cache over `AuthSessions`; sign-out evicts it |
| the CSRF token | the `synapse_csrf` cookie, compared to the `X-CSRF-Token` header | stateless: the browser holds it, no table |
| the budget meter, the turn thread, the event bus | `sahs/assistant/runtime.py` (`_SessionRuntime`) | per running turn; the transcript it produced is in `ChatMessages`, the events in `ChatEvents` (the bus is a cache over that table under a store) |
| the parsed `.env` | `sahs/util/auth.load_dotenv` | read once per process, re-read when the file changes; `os.environ` is consulted fresh on every call |
| the runtimes themselves | `backend/chat.py` (`_RUNTIMES`), `backend/ask.py` | one per signed-in person, built on first use from the store |

## Schema notes for the first rollout

Two `CHECK` constraints in `002_chat.sql` are narrower than what the code
can write today; the database, not the app, refuses the row:

* `ChatArtifacts.Type` allows `chart | table | document | dashboard |
  diagram | query`; `sahs/assistant/artifacts.py` also knows `kpi`.
* `ChatSessions.Model` allows `'' | vertex | gateway` (16 characters); the
  composer's model picker may record a longer choice.

Widen both `CHECK` lists in the DDL before the chat lane goes live on E1.
`get_artifact` reaches an artifact by its id alone (an interleaved table
keyed by chat): add an index on `ChatArtifacts (ArtifactId)` when the
tables grow past a laptop's worth.

## How this is tested without a Spanner

`synapse-agentic-harness-system/tests/fake_spanner.py` is a stand-in for
the SDK's `Database` object: `snapshot()` and `run_in_transaction()` over a
sqlite file that carries the same tables. `SpannerDatabase` runs unchanged
against it (the typed parameters, the `JsonObject` cells, the `BYTES`
cells, the commit timestamps), so `tests/test_assistant_spanner_store.py`,
`tests/test_content_store.py`, `apps/synapse_admin/tests/test_local_login.py`
and `apps/synapse_admin/tests/test_content_store_app.py` prove the whole
path from the route to the table — a 9 MiB upload lands as two chunks and
comes back byte-identical, and a submission folds to the same shape from
the rows as from the ledger. What the double does not do: enforce
interleaving, foreign keys or `CHECK` constraints. `scripts/spanner_check.py`
diffs a live database against the DDL for that.
