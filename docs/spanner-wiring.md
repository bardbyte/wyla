# Spanner wiring: what the app persists, and where it lands under `SAHS_STORE=spanner`

`SAHS_STORE` picks the book the app writes in. `local` is one laptop, one
developer, no accounts: everything goes to files under the silo's `graph/`.
`spanner` is the deployment: the people, their sessions and their chats go
to one Cloud Spanner database (`db/spanner/001_identity.sql`,
`002_chat.sql`, `004_google_oauth.sql`, `006_external_identities.sql`).
`sqlite` is the same tables in one local file, so a laptop can rehearse
the deployment without a Spanner.

Think of it as a hotel: the identity tables are the front desk's register
(who is here, which key opens what), the chat tables are each guest's
room, and the filesystem is the shared lobby, the same for everyone. Under
`spanner` the register and the rooms are in the database; what is listed
below as a gap is still in the lobby.

Two classes carry every database write. `IdentityStore`
(`sahs/identity/store.py`) owns the identity tables; `SpannerAssistantStore`
(`sahs/assistant/spanner_store.py`) owns the chat tables. Both speak
portable SQL to one `Database` object (`sahs/identity/database.py`):
`SpannerDatabase` on the Cloud Spanner SDK, `SqliteDatabase` on the
stand-in. The app opens that object once (`backend/auth.py`, `_identity()`)
and hands it to both stores, so one connection serves both.

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
| compiled builds (when `MERIDIAN_BUILDS_SOURCE=spanner`) | `sahs/builds/spanner_store.py` | `SpannerBuildStore` | `Builds` (`003_graph.sql`) |

Under a store every `/api/chat/*` route needs the session cookie, and each
signed-in person gets their own `AssistantRuntime` whose store is bound to
their user id: every chat row carries `OwnerUserId`, every memory `UserId`,
and a read never crosses owners. Under `local` the one shared runtime and
the sqlite file `graph/runs/chat/sessions.sqlite3` stay as they were.

## Not yet in Spanner

These still write to files, in every store mode. Each names the table it
would land in, or the table it would need.

| what | route or module | writes today | table it would use | why not yet |
|---|---|---|---|---|
| the files on a chat (bytes, manifest, converted text) | `POST /api/chat/sessions/{id}/files` (`sahs/assistant/files.py`) | `graph/runs/chat/workspaces/<session>/files/` | `ChatFiles` (`002_chat.sql`), plus an object store for the bytes (`ObjectPath`) | the table keeps the manifest and the text; the bytes need a bucket. `SAHS_FILES_DIR` is named in `.env.example` for that but nothing reads it yet |
| the event log the page replays | `sahs/assistant/runtime.py` (`_SessionRuntime`) | `graph/runs/chat/events/<session>.jsonl` (per person under `users/<id>/` with a store) | `ChatEvents` | the SSE bus replays from memory and the JSONL; the runtime is being reworked by another change, so the sink stays a file for now |
| the review board (skills and knowledge files awaiting a manager) | `/api/chat/reviews*` (`sahs/assistant/reviews.py`) | `graph/runs/reviews/ledger.jsonl`, `files/<id>/v<n>.md`, `seen.json` | none: a `ReviewSubmissions` / `ReviewEvents` pair | no table in the DDL yet; the board is shared across people by design (one ledger), so it needs its own tables, not the per-person chat ones |
| a person's own skills | `POST`/`DELETE /api/chat/skills/mine` (`sahs/assistant/authoring.py`) | `graph/skills/users/<owner>/<name>.md` | `UserSkills` | the loader (`sahs/assistant/skills_loader.py`, `sahs/loop/skills.py`) reads packs off the skills tree; a Spanner read path for the loader has to come with the write |
| approved knowledge files | `POST /api/chat/reviews/{id}/decision` (publish) | `<sources>/artifacts/` | `KnowledgeFiles` | the shelf and the build-graph run read the sources directory; same as above, the readers move with the writer |
| Ask sessions, messages, plans, feedback (the E18 lane) | `/api/sessions*` (`backend/ask.py`, `sahs/ask/store.py`) | `graph/runs/ask/[users/<id>/]sessions.sqlite3` | `ChatSessions` (`Kind` analyst or steward), `ChatMessages`, `ChatPlanVersions`, `ChatFeedback` | the Ask runtime builds its own `SessionStore`; `SpannerAssistantStore` already covers the verbs it uses, so this is the next seam to wire |
| Knowledge Catalog: the push record, the model cache, the gate | `POST /api/kc/push-record/{table}` (`sahs/kc/write.py`) | `graph/<push_record_dir>/…`, `llm_<version>.json` | none | a run report on the graph's filesystem, by design (`db/spanner/README.md`: the graph stays on the filesystem in the first rollout) |
| Knowledge Catalog: the read-back import (pending quads) | `POST /api/kc/witness-import` (`sahs/kc/witness.py`) | `graph/nodes/*.jsonl`, `graph/edges/*.jsonl` | `GraphNodeAssertions`, `GraphEdgeAssertions`, `GraphRuns` (`003_graph.sql`) | the graph is the clerk's, on the filesystem, until the graph half moves (`docs/spanner_schema.md` §5) |
| feedback on the admin surfaces | `graph/runs/feedback/*.jsonl` (`backend/meridian.py`) | JSONL | none | a laptop log, never read back by the app |
| the staging door (Knowledge Files creator) | `POST /api/meridian/artifacts` (`backend/meridian.py`) | `<sources>/artifacts/` | `KnowledgeFiles` | same readers as the approved files above |
| the sandbox workspace (a turn's rows, cells, snapshots) | `sahs/assistant/sandbox.py` | `graph/runs/chat/workspaces/<session>/` | none | scratch for one turn, not a record |

## Process memory, by design

These are caches or per-request state, not records; nothing is lost when a
pod restarts.

| what | where | note |
|---|---|---|
| the five-second session cache | `backend/auth.py`, `_session_cache` | a positive cache over `AuthSessions`; sign-out evicts it |
| the CSRF token | the `synapse_csrf` cookie, compared to the `X-CSRF-Token` header | stateless: the browser holds it, no table |
| the budget meter, the turn thread, the event bus | `sahs/assistant/runtime.py` (`_SessionRuntime`) | per running turn; the transcript it produced is in `ChatMessages` |
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
against it (the typed parameters, the `JsonObject` cells, the commit
timestamps), so `tests/test_assistant_spanner_store.py` and
`apps/synapse_admin/tests/test_local_login.py` prove the whole path from
the route to the table. What the double does not do: enforce interleaving,
foreign keys or `CHECK` constraints. `scripts/spanner_check.py` diffs a live
database against the DDL for that.
