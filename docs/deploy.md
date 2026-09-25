# Deploying Synapse: one flag, four profiles, one table of checks

This is the whole mental model on one page. The details each row points
at live in `synapse-agentic-harness-system/.env.example` (every
variable), `db/spanner/README.md` (the schema and how to apply it),
`docs/spanner-wiring.md` (what lands in which table) and
`docs/enterprise-port.md` (what differs from the enterprise branch).

## The flag

`SAHS_ENV_FILE` names the `.env` the loader reads
(`sahs.util.auth.load_dotenv`). That is the only switch: the app, the
pipeline and every check read that one file, and shell-exported
variables win over it. The file is parsed once per process and
re-read only when it changes on disk.

```sh
make run ENV=e1      # SAHS_ENV_FILE=synapse-agentic-harness-system/env/e1.env, uvicorn on 8810
make check ENV=e1    # the readiness table for that file
make test            # both suites
make ddl-check       # the DDL under db/spanner, without a Spanner
```

`make` takes `ENV_FILE=/elsewhere/e1.env` when the file is not beside
the examples, and `PORT=` for another port.

## The four profiles

One placeholder example per profile in
`synapse-agentic-harness-system/env/`; copy it to `<profile>.env` beside
it (git ignores everything there but the examples) and fill it in.

| profile | store | sign-in | model plane | build | what it is for |
|---|---|---|---|---|---|
| `local` | `SAHS_STORE=sqlite`: people and chats in one local file | `AUTH_LOCAL_LOGIN=1`: the email-and-password door | Vertex with a service-account key, or the gateway | this disk (`MERIDIAN_BUILDS_SOURCE=local`) | a laptop; the tests rehearse it |
| `e1` | `SAHS_STORE=spanner`, `EPAAS_ENV=e1`, the dev database | Okta (`OKTA_*`); the local door may stay open for a laptop pointed at the dev database | the gateway (`GATEWAY_*`, `IDP_TOKEN_URL`, `GATEWAY_BASE_URL`) | the promoted bundle in Spanner (`MERIDIAN_BUILDS_SOURCE=spanner`) | dev |
| `e2` | as e1, the qa database | Okta only (`AUTH_LOCAL_LOGIN=0`) | the gateway | Spanner | qa |
| `e3` | as e1, the prod database | Okta only; `AUTH_LOCAL_LOGIN` must be unset, and `make check` refuses a file that sets it | the gateway | Spanner | production |

`local` is not a fourth deployment: it is what every developer runs, and
what CI runs (`.github/workflows/tests.yml`: both suites and the DDL
lint, nothing on the network).

## The checks, and what each proves

`make check ENV=<profile>` runs `scripts/readiness.py --env <profile>`:
it loads the profile's file, runs the check scripts that already exist
(each a subprocess reading the same file), and prints one table:
check, verdict, one-line reason. Exit 0 means every required check is
`ok`.

| check | runs | proves | required in |
|---|---|---|---|
| env file | — | the profile's `.env` exists (or `--env-file` / `SAHS_ENV_FILE` named one) | all |
| settings | in-process | every variable the profile needs is set, none is still the example's `<placeholder>`, `SAHS_STORE` and `EPAAS_ENV` match the profile, the pepper is long enough, `AUTH_LOCAL_LOGIN` is shut where it must be | all |
| ddl | `scripts/spanner_ddl_check.py` | the files under `db/spanner` are what the database will accept: keys, interleaves, foreign keys, indexes, the graph registries | all |
| spanner | `scripts/spanner_check.py` | the credential mints a token, the database answers, and every designed table is applied (a live table the repo lacks shows as `undesigned`; `--emit-ddl` prints its CREATE TABLE) | e1, e2, e3 |
| okta | `apps/synapse_admin/scripts/okta_check.py --envs <E> --no-matrix` | discovery and JWKS answer, the client is registered and accepts `OKTA_REDIRECT_URI` — without signing anyone in | e1, e2, e3 |
| gateway | `scripts/gateway_check.py --only token,generate` | the identity-service token and one model call through the gateway | e1, e2, e3 |
| vertex | `scripts/vertex_check.py` | the key, the endpoint and a token, when `SYNAPSE_VERTEX_SA_KEY` is set | when configured |
| bigquery | `scripts/bq_check.py` | a token and one dry run as the service account (skipped under `SAHS_BQ_AUTH_MODE=user`) | when configured |
| google | `apps/synapse_admin/scripts/google_auth_check.py` | the Google OAuth client exists and accepts the callback, when a client is configured | when configured |

Verdicts: `ok` · `missing setting <NAME>` · `bad setting <NAME>` (set to
something the profile does not allow) · `unreachable` (the host did not
answer or refused the credential) · `failed` (the check ran and found
something wrong) · `skipped` (nothing configured for it). A check that
ran and did not pass fails the table in every profile; a skipped one
fails it only where it is required.

## The DDL, applied in order

One Spanner database per environment. Apply the files under
`synapse-agentic-harness-system/db/spanner/` in number order, each as
one batch (`gcloud spanner databases ddl update … --ddl-file=<file>`;
the emulator recipe is in `db/spanner/README.md`):

1. `001_identity.sql` — people, credentials, roles, sessions, the audit
2. `002_chat.sql` — chats, messages, artifacts, plans, feedback, events, memory
3. `003_graph.sql` — the graph tables (empty in the first rollout) and `Builds`
4. `004_google_oauth.sql` — a person's connected Google account
5. `005_build_bundles.sql` — the promoted build's bytes under `Builds`
6. `006_external_identities.sql` — Okta links and the one-time authorization states
7. `007_*.sql` — the review board and the content store, when that change lands

`make ddl-check` before applying; `make check` after, to see the
`spanner` row say every designed table is present.

## Tenancy, decided

Tenancy is by deployment: one Spanner database per environment (e1,
e2, e3), one `.env` per environment, and nothing in a row says which
tenant it belongs to. There is no `TenantId` column and none is added.
Isolation between people is by owner: every chat row carries
`OwnerUserId`, every memory and feedback row `UserId`, and every store
read filters on it. Two seams exist for business-unit scoping later,
without a schema change: `UserRoles.Scope` (a role granted within a
scope) and `KnowledgeFiles.BusinessUnit` (a knowledge file shelved for
one). Until a decision needs them, both stay `''`.

## Day one on a new environment

1. `cp synapse-agentic-harness-system/env/e1.env.example synapse-agentic-harness-system/env/e1.env` and fill every `<placeholder>`; secrets come from the secret store, never from a file in the repository.
2. Register `https://<host>/callback` as a Login redirect URI on the environment's Okta client (and on the Google client if BigQuery runs as the person).
3. Create the Spanner database; apply `001` … `006` in order.
4. `make ddl-check`.
5. `make check ENV=e1` — fix every row that is not `ok` or an allowed `skipped`.
6. Publish a build: `python scripts/pipeline.py publish-build` from a host that compiled one (the `spanner` row stays `ok`; the app's `/api/synapse/planes` will name the build).
7. `make run ENV=e1` on a laptop pointed at the environment (the ingress must send `X-Forwarded-Proto: https`, or set `AUTH_COOKIE_SECURE=true`).
8. Sign in once through Okta; confirm `AUTH_GROUP_ROLE_MAP` puts you in `admin` (without it nobody opens the console).
9. Open a chat, send one message, reload the page: the transcript and the turn's events come back from `ChatMessages` and `ChatEvents`, not from the pod's memory.
10. Hand the filled `.env` to the deployment's secret store as environment variables; the container never carries the file.
