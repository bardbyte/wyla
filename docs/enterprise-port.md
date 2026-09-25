# The enterprise port: what came across, what changed, and who owns which path

This repository and the enterprise repository share one codebase but not
one history. The team's `feature/unified-changes` branch added an identity
service (people, sessions, roles, audit on Cloud Spanner), user-delegated
Google BigQuery, CORS and CSRF gates, per-person runtimes, and compiled
builds published to Spanner. This document records how that branch was
brought here, so that changes made here can be carried back with a
cherry-pick and a known, short list of differences.

## The rule

Their code is the baseline and lands as they wrote it. Our commits change
only the paths we deliberately touch, and in those paths our version wins.
The landing commit is theirs; every commit after it is ours.

## What landed as written

Backend routers `security.py`, `auth.py`, `admin.py`, `access.py`; the
harness modules `sahs/util/google_auth/{oauth,token}.py`,
`sahs/util/network.py`, `sahs/builds/{bundle,resolve,spanner_store}.py`;
the schema file `db/spanner/004_google_oauth.sql`; and their changes to
`app.py`, `ask.py`, `meridian.py`, `sahs/tools/sandbox.py`,
`sahs/ask/generate.py`, `sahs/compiler/compile.py`, `sahs/util/auth.py`.

## Deviations, each deliberate

| where | theirs | here | why |
|---|---|---|---|
| `backend/chat.py` | one runtime per signed-in person; the `/api/chat/reviews*` routes removed | unchanged: one process-wide runtime, the review board kept | the second surface's skill submission flow and its tests depend on the review routes; the per-person change is pending (see below) |
| `sahs/util/tls.py` | imports the enterprise certificate package by name; probes a fixed secret mount | package named by `SAHS_CA_PACKAGE`; mount named by `SAHS_SECRETS_DIR`; neither consulted when unset | this repository is public |
| `sahs/util/auth.py` | fixed secret-mount paths for the BigQuery key | `SAHS_SECRETS_DIR` | same |
| `app.py` | a second health path with the deployment's name | `SYNAPSE_HEALTH_ALIAS` | same |
| `sahs/constants.py` | per-environment endpoint literals | maps filled from `SAHS_{VERTEX,OAUTH_TOKEN,SPANNER}_ENDPOINT_E{1,2,3}` | same |
| `helm/`, `config/*.yml`, `Dockerfile`, `config/settings.py` | present | absent; the code tolerates the missing `config.settings` and reads the environment | deployment stays with the team; `.env.example` documents every variable |

## Written here to their interfaces

These modules were not available when the port was made. They were written
to the call signatures their code uses, so their callers run unchanged and
either implementation can replace the other file for file:

| module | interface it satisfies |
|---|---|
| `sahs/spanner.py` | `SpannerSettings`, `AuthSettings`, `GoogleOAuthSettings` with `from_env()`, both `*ConfigurationError` classes, `spanner_is_enabled()`, `grpc_endpoint()` |
| `sahs/identity/store.py`, `sahs/identity_store.py` | `IdentityStore(settings, auth)` with `session_user`, `signup` (both arities), `login`, `logout`, `logout_all`, `record_audit`, `update_user`, `delete_user`, `grant_role`, `revoke_role`, `direct_reset_password`, `google_connection`, `save_google_connection`, `revoke_google_connection`, `_query` |
| `sahs/identity/authorization.py` | `ROLE_PERMISSIONS`, `permissions_for_roles` |
| `sahs/util/bigquery_errors.py` | `bigquery_http_error_message`, `is_bigquery_auth_error` |
| `sahs/constants.py` | the three endpoint maps `network.py` imports |
| `AskRuntime`, `AssistantRuntime` | `owner_user_id=`, `runner=`; `start_turn(runner=)`; `BQConnection.from_env(require_key=)` |

Additions with no counterpart on their side, all ours: `sahs/identity/database.py`
(one interface, Spanner and a sqlite stand-in selected by `SAHS_STORE=sqlite`),
`db/spanner/006_external_identities.sql` (identity-provider links and
one-time authorization states), `sahs/util/paths.py` (per-person runtime
paths), the `FirstName`/`LastName` columns on `Users` that their admin
queries read, and the `identity` dependency group in `pyproject.toml`.

Their schema (`001_identity.sql`) and their settings loader were requested
and, once received, decide whether the store reads their column names or
ours. Until then the store targets the schema in this repository.

## Carrying a change back

In the enterprise clone, with this repository as the remote `wyla`:

```sh
git checkout -b ours/<topic> origin/feature/unified-changes
git cherry-pick <one commit from here>
```

Conflicts, when they come, are in the rows of the deviation table above.
For each conflicted path take one side whole: theirs for deployment files,
ours for the paths our commit set out to change.

## Path ownership at reconciliation

| path | owner |
|---|---|
| `helm/`, `config/`, `Dockerfile` | theirs, always |
| `apps/synapse/frontend/**`, `apps/synapse_admin/frontend/**` | ours |
| `backend/auth.py` (Okta sign-in), `sahs/identity/**`, `sahs/spanner.py` | ours once the Okta commits land; theirs for the Google connection routes |
| `backend/chat.py` review routes | ours |
| everything else they added | theirs unless a commit here names the file |
