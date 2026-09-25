# Synapse on Spanner

The schema the laptop's stores become when Synapse leaves the laptop:
GoogleSQL DDL files, applied in file order (001, 002, 003, 004, 005,
006, 007, 008), one database per environment.

| file | holds | replaces |
|---|---|---|
| `001_identity.sql` | users, credentials, roles and permissions, login sessions, refresh tokens, MFA, one-shot tokens, invitations, preferences, the audit | nothing yet: the laptop has one configured person (`SYNAPSE_USER_NAME`) |
| `002_chat.sql` | chats, messages, artifacts, plans, feedback, files, events, memory, a person's own skills, staged knowledge files | `graph/runs/chat/sessions.sqlite3`, the events JSONL, the files under each workspace, `graph/skills/users/`, `sources/artifacts/` |
| `003_graph.sql` | the graph's nodes and edges with their provenance, as append-only assertions plus the folded current state, the crosswalk, the clerk's transitions, the builds, and a property graph over the fold | `graph/nodes/*.jsonl`, `graph/edges/*.jsonl`, `graph/identity/crosswalk.jsonl`, `graph/runs/` |
| `004_google_oauth.sql` | a person's connected Google account (the encrypted refresh token) for user-delegated BigQuery | nothing: the laptop runs BigQuery as the service account |
| `005_build_bundles.sql` | the promoted build's bytes: one bundle row per build and its chunks, interleaved under `Builds` (`sahs/builds/spanner_store.py`, `MERIDIAN_BUILDS_SOURCE=spanner`) | `builds/<id>/` on a shared disk |
| `006_external_identities.sql` | identity-provider links (Okta) and the one-time authorization states both sign-in hops park | nothing: the laptop has no sign-in |
| `007_content.sql` | a chat file's bytes (`ChatFileChunks`, at most 8 MiB a row, interleaved in `ChatFiles`) and the review board (`ReviewSubmissions`, `ReviewVersions`, `ReviewEvents`, `ReviewSeen`) | the bytes under each workspace's `files/`; `graph/runs/reviews/ledger.jsonl`, `files/<id>/v<n>.md`, `seen.json` |
| `008_chat_model.sql` | four `ALTER TABLE` statements, no new table: `ChatSessions.Model` becomes `STRING(64)` and loses the plane `CHECK` (the composer records a catalog choice, `plane:model`, and the catalog in the `.env` says which exist), and `ChatArtifacts.Type`'s `CHECK` is replaced by the list in `sahs/assistant/artifacts.py` (`kpi` included) | the first rollout's narrower `002` constraints (`docs/spanner-wiring.md`, "Schema notes") |

The stores that write them: `IdentityStore` (`sahs/identity/store.py`)
for `001`, `004` and `006`; `SpannerBuildStore`
(`sahs/builds/spanner_store.py`) for `Builds` of `003` and `005`; `SpannerAssistantStore`
(`sahs/assistant/spanner_store.py`) for the chat tables of `002`;
`SpannerContentStore` (`sahs/assistant/content_store.py`) for
`ChatFiles`, `UserSkills` and `KnowledgeFiles` of `002` and all of
`007`. Which route lands in which table: `docs/spanner-wiring.md` at
the repository root.

The reasoning behind every table is in `docs/spanner_schema.md`.
This file is the how.

## The first rollout

Every file is applied; the first rollout writes to the identity
tables that email-and-password sign-in needs (`Users`,
`UserCredentials`, `Roles`, `Permissions`, `RolePermissions`,
`UserRoles`, `AuthSessions`, `LoginAttempts`, `UserPreferences`,
`AuditEvents`), to every chat table, and to the content tables of
`007` (a file's bytes go to `ChatFileChunks`, not to a bucket:
`ChatFiles.ObjectPath` stays null). Five identity tables wait for
phase 2 and stay empty — `RefreshTokens`, `MfaFactors`,
`MfaRecoveryCodes`, `ActionTokens`, `Invitations`, marked in the file
— and the graph file's tables stay empty too: the graph stays on the
filesystem (`docs/spanner_schema.md` §3.8 and §5). The `.env` block
the deployment fills is in `.env.example` (`SAHS_STORE=spanner`,
`SPANNER_*`, `AUTH_*`).

## Check the files without a Spanner

```bash
python scripts/spanner_ddl_check.py
```

reads every file as statements and holds them to what the
database will: every table keyed, every interleave on a parent
defined earlier with a key that extends the parent's, every foreign
key and index on columns that exist, every row deletion policy on a
timestamp, no reserved word as a column name, the property graph
keyed on primary keys, every `ALTER TABLE` (drop constraint, alter
column, add constraint) on a table and a name that exist, applied to
the model in file order — and the CHECK lists equal to the Python
registries: the graph half's (`sahs.graph.quads.RELATIONS`,
`WITNESSES`, `sahs.graph.ids.ID_PATTERNS`) and the chat half's
(`sahs.assistant.artifacts.TYPES` for `ChatArtifacts.Type`;
`ChatSessions.Model` as wide as `sahs.assistant.spanner_store` writes,
with no plane list left on it), so a relation or an artifact type added
in code fails the check until the DDL learns it.
`tests/test_spanner_ddl.py` runs it.

## Apply

Against the emulator first, on any laptop with the gcloud SDK:

```bash
gcloud emulators spanner start &
export SPANNER_EMULATOR_HOST=localhost:9010
gcloud config set auth/disable_credentials true
gcloud config set project synapse-local
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud spanner instances create synapse --config=emulator-config \
  --description="Synapse" --nodes=1
gcloud spanner databases create synapse --instance=synapse
for f in db/spanner/00*.sql; do
  gcloud spanner databases ddl update synapse --instance=synapse \
    --ddl-file="$f"
done
```

Then the same commands against the real instance. Each file is one
DDL batch; Spanner applies a batch atomically, so a file that fails
leaves nothing half-made. On a database that already carries `001`
to `006`, apply `007_content.sql` and then `008_chat_model.sql` alone
the same way; on one that carries `007`, `008` alone. `008` is the
first file made of `ALTER TABLE` statements: it changes the tables
`002` created, so a fresh database applied `001` … `008` and the live
E1 database with `008` on top end up the same, and
`scripts/spanner_ddl_check.py` applies the ALTERs to its model in file
order before it holds the schema to the code's registries.

Two things to know about the target:

* **Dynamic labels.** `003_graph.sql` defines the property graph with
  `DYNAMIC LABEL (Kind)` / `DYNAMIC LABEL (Relation)` and
  `DYNAMIC PROPERTIES (Props)`: one node table, one edge table, the
  label read from the row. That is the 2025 Spanner Graph surface. On
  an instance that predates it, the fallback is the same two tables
  with static labels — replace the two element definitions with
  `LABEL Node PROPERTIES ALL COLUMNS` and `LABEL Edge PROPERTIES ALL
  COLUMNS` and match on `n.Kind = 'table'` / `e.Relation =
  'has_column'` instead of on the label.
* **Search indexes.** The two `CREATE SEARCH INDEX` statements need
  the full-text search feature; the emulator accepts the DDL, the
  index becomes useful on a real instance.

## Seed

`001_identity.sql` ends with the seed rows the app expects, as
commented INSERTs: the three roles with the surfaces each may open
(`admin` → the admin console at `/` and Synapse; `analyst` → Synapse
at `/synapse/`; `steward` → Synapse, its permission set still to be
decided) and the permission names the app checks. The bootstrap job
applies them once, idempotent on `Name`, and grants the first admin
from an environment variable — never from a default password.

## Move the laptop's data

One-way, run once per store, in this order (the foreign keys want
users before chats and runs before assertions):

1. users: one row per distinct `SYNAPSE_USER_NAME` / `actor` seen in the
   SQLite stores, status `active`, a temporary password set by the
   admin with `MustChangePassword`;
2. `sessions.sqlite3` → `ChatProjects`, `ChatSessions`,
   `ChatMessages` (Seq from rowid order), `ChatArtifacts`,
   `ChatPlanVersions`, `ChatFeedback`, `ChatMemories`;
3. each workspace's `files/manifest.json` → `ChatFiles`, and each
   file's bytes → `ChatFileChunks` in 8 MiB slices (the same split
   `SpannerContentStore.add_file` makes);
4. `graph/skills/users/<owner>/*.md` → `UserSkills`;
   `sources/artifacts/*` → `KnowledgeFiles`;
   `graph/runs/reviews/ledger.jsonl` → `ReviewSubmissions` (one head
   row per `submitted` record, its `Status` and `Version` from the
   fold), `ReviewEvents` (one row per record, `Seq` per submission,
   the record's other fields in `Payload`), `files/<id>/v<n>.md` →
   `ReviewVersions`, `seen.json` → `ReviewSeen` (the count becomes the
   instant of that record);
5. not in the first rollout (the graph stays on the filesystem):
   `graph/runs/*/manifest.json` → `GraphRuns`, then every JSONL line
   → `GraphNodeAssertions` / `GraphEdgeAssertions` in file order
   (Seq per identity), then one fold pass → `GraphNodes` /
   `GraphEdges`; `identity/crosswalk.jsonl` → `GraphCrosswalk`;
   `builds/*/manifest.json` → `Builds`.

The fold is the same function the compiler runs today
(`GraphDir.fold`): last record per identity wins, retracted drops.
