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
| `backend/auth.py` sign-up, login, reset | always on | refuse with 403 unless `AUTH_LOCAL_LOGIN=1` | the enterprise front door is Okta; the email-and-password path is a laptop and break-glass path, to be removed once Okta is live everywhere |
| `backend/auth.py` `google_callback` | owned `GET /callback` | no route of its own; `backend/okta.py` serves `/callback` and hands any state that is not an Okta sign-in to `google_callback` unchanged | one registered callback URL for both providers |
| `backend/auth.py` session cookie | `AUTH_COOKIE_SECURE=auto` always set `Secure` | `auto` follows the request: `Secure` over https or behind `x-forwarded-proto: https`, plain over http | a browser drops a `Secure` cookie set over `http://localhost`, so nobody could sign in on a laptop |
| `backend/admin.py` `users()` | a hand-written SQL join that did not parse | `store.list_users(limit)` | the query had a syntax error; the store already knows how to list people |
| `sahs/util/spanner/settings.py` | re-reads the silo `.env` with `override=True` on every `SpannerSettings.from_env()` (the file beats the shell) | the facade resolves the environment once, shell first, and hands their readers the mapping; `load_dotenv` gained the `override` flag their code calls | the harness convention is that the shell wins; their module stays as written |
| `sahs/util/spanner/settings.py` `spanner_is_enabled` | `SAHS_STORE=spanner` only | the facade's answer is `SAHS_STORE != local`, so `sqlite` counts | the sqlite stand-in is how the sign-in runs on a laptop and in the tests |
| `sahs/util/spanner/settings.py` `AuthSettings.from_env` | demands a pepper of eight characters always | the facade demands it whenever a store runs and reads leniently under `SAHS_STORE=local` | with no store nothing here guards anything, and `/api/auth/okta` must answer on a bare laptop |
| `backend/okta.py` `/callback` | (new) | a refusal (expired state, bad token, no email, Okta's own error) redirects to the sign-in page of the surface the person was heading for, with the reason in the hash; the audit keeps the status | a browser is on that URL, not a script; a JSON error page is a dead end |

## Written here to their interfaces

These modules were not available when the port was made. They were written
to the call signatures their code uses, so their callers run unchanged and
either implementation can replace the other file for file:

| module | interface it satisfies |
|---|---|
| `sahs/spanner.py` | now a facade over their `sahs/util/spanner/settings.py` (landed as written): the same names, their readers and rules, plus `SAHS_STORE=sqlite`, `store_mode()`, `AUTH_LOCK_AFTER`, `AUTH_LOCK_MINUTES`, `AUTH_LOCAL_LOGIN`, defaults on every field, and a lenient `AuthSettings.from_env` under `SAHS_STORE=local` where no store runs |
| `sahs/identity/store.py`, `sahs/identity_store.py` | `IdentityStore(settings, auth)` with `session_user`, `signup` (both arities), `login`, `logout`, `logout_all`, `record_audit`, `update_user`, `delete_user`, `grant_role`, `revoke_role`, `direct_reset_password`, `google_connection`, `save_google_connection`, `revoke_google_connection`, `_query` |
| `sahs/identity/authorization.py` | their file as written (the steward holds `skills.share` too), with `ROLES`, `surfaces_for_roles` and `is_known_role` appended for the store |
| `sahs/util/bigquery_errors.py` | `bigquery_http_error_message`, `is_bigquery_auth_error` |
| `sahs/constants.py` | the three endpoint maps `network.py` imports |
| `AskRuntime`, `AssistantRuntime` | `owner_user_id=`, `runner=`; `start_turn(runner=)`; `BQConnection.from_env(require_key=)` |
| `sahs/identity/oidc.py` | new here, not theirs: `OidcSettings.from_env()` (the `OKTA_*` variables), `OidcClient` (discovery, JWKS, PKCE authorize URL, code exchange, RS256 ID-token verification), `roles_for_groups` |
| `backend/okta.py` | new here, not theirs: `GET /api/auth/okta` (status for the sign-in page), `GET /api/auth/okta/start?next=`, `GET /callback` for both providers; state, nonce and PKCE verifier live in the store's `AuthStates` table so any pod may take the callback |
| `sahs/identity/store.py` additions | `find_or_create_external_user`, `set_roles`, `list_users`, `put_state`, `pop_state`; `db/spanner/006_external_identities.sql` adds `ExternalIdentities` and `AuthStates` |
| both frontends: `js/session.js`, `pages/signin.js`, `pages/account.js`; admin `pages/users.js` | new here, not theirs: every `api.js` call goes through `apiFetch` (the `X-CSRF-Token` header from the `synapse_csrf` cookie on state-changing calls, the sign-in page on a 401); the shell boots from `/api/auth/me` and draws the account row from it; Okta first on the sign-in page, the email-and-password form only under `AUTH_LOCAL_LOGIN=1`; the account page holds their Google connect popup (`google-connected` message) and disconnect; People uses their `/api/admin/users` and `/api/admin/access` routes |

Additions with no counterpart on their side, all ours: `sahs/identity/database.py`
(one interface, Spanner and a sqlite stand-in selected by `SAHS_STORE=sqlite`),
`db/spanner/006_external_identities.sql` (identity-provider links and
one-time authorization states), `sahs/util/paths.py` (per-person runtime
paths), and the `identity` dependency group in `pyproject.toml`. Their
`db/spanner/001_identity.sql` replaced ours once it arrived (the same
columns; theirs carries `FirstName`/`LastName` and the steward's seed).

Their schema, settings loader and authorization module arrived after the
first landing and were reconciled the same way: `001_identity.sql` and
`sahs/util/spanner/settings.py` verbatim, `sahs/identity/authorization.py`
verbatim with our helpers appended, and the deltas in the `sahs.spanner`
facade (table above). Their `settings.py` needs `sahs/util/spanner` to be a
package, so the REST Spanner plane that lived in `sahs/util/spanner.py`
became that package's `__init__.py`; every import of it reads as before.

## Carrying a change back

The two repositories share no history, so never merge a wyla branch into
the enterprise repository (or the reverse): every file would conflict.
Commits cross one at a time, and the first commit here never crosses at
all: it is their own code coming home, plus our modules, which are
checked out by path instead.

The commits on the sign-in branch (`git log a87fd28..`), and what to do
with each in the enterprise clone (wyla as the remote `wyla`):

| commit | subject | carry it by |
|---|---|---|
| `8a87402` | Land the enterprise identity branch | not cherry-picked; `git checkout` of the ours-owned paths below |
| `998aa24` | Sandbox: the live gate behind the cost gates; no crash without BigQuery | cherry-pick |
| `561a0ef` | Okta sign-in on the identity store | cherry-pick |
| `1857044` | Both surfaces sign in | cherry-pick |
| `16e69af` | Their schema, settings loader and authorization module, reconciled | cherry-pick; their three files are byte-identical on both sides, so only `sahs/spanner.py`, `sahs/identity/authorization.py` and the `sahs/util/spanner.py` move can conflict |

```sh
git fetch wyla
git checkout -b feature/okta-signin origin/feature/unified-changes
git cherry-pick 998aa24 561a0ef 1857044 16e69af
```

During a cherry-pick, git's `--ours` is the enterprise branch and
`--theirs` is the wyla commit being applied. For each conflicted path take
one side whole by the ownership table below: `git checkout --theirs -- <path>`
for a path we own, `git checkout --ours -- <path>` for one they own, then
`git add` and `git cherry-pick --continue`.

Then the modules from the first commit that the Okta commits depend on
(the store methods `find_or_create_external_user`, `set_roles`, `list_users`,
`put_state`, `pop_state` exist only in ours):

```sh
git checkout wyla/main -- \
  synapse-agentic-harness-system/sahs/identity \
  synapse-agentic-harness-system/sahs/identity_store.py \
  synapse-agentic-harness-system/db/spanner/006_external_identities.sql \
  synapse-agentic-harness-system/.env.example \
  docs/enterprise-port.md
git commit -m "Identity store, database and states from wyla; the port doc"
```

(`wyla/main` once PR #146 has merged; the branch name before that. Merge
that PR with a merge commit, not a squash, so the five commits keep their
identities for the table above.) Add the `identity` dependency group
(`google-cloud-spanner`, `argon2-cffi`, `cryptography`) to whatever pins
their image installs; `pyproject.toml` here names it.

### Before the enterprise branch is opened for review

1. Both suites green in that clone: `python -m pytest apps/synapse_admin/tests -q`
   and `python -m pytest synapse-agentic-harness-system/tests -q`.
2. The DDL: `db/spanner/006_external_identities.sql` applied to each
   environment's database (`ExternalIdentities`, `AuthStates`), and
   `004_google_oauth.sql` where BigQuery runs as the person.
   `scripts/spanner_check.py` diffs the live database against `db/spanner`.
3. The environment (helm and config, theirs): `SAHS_STORE=spanner`,
   `AUTH_PEPPER` (eight characters at least, a secret), `OKTA_ISSUER`,
   `OKTA_CLIENT_ID`, `OKTA_CLIENT_SECRET` (a secret),
   `OKTA_REDIRECT_URI=https://<host>/callback`, `AUTH_GROUP_ROLE_MAP` with
   at least one group mapped to `admin` (without it nobody opens the
   console), `AUTH_LOCAL_LOGIN` unset in e1, e2 and e3.
4. The cookie: `AUTH_COOKIE_SECURE=auto` needs the ingress to send
   `X-Forwarded-Proto: https`; when it does not, set `true`.
5. Okta: `https://<host>/callback` registered as a Login redirect URI on
   each environment's client, and the ID token carrying the group claim
   (`OKTA_GROUP_CLAIM`, default `groups`) and an email claim.
   `apps/synapse_admin/scripts/okta_check.py` verifies both without
   signing anyone in.
6. Routing: `/callback` must reach the app at the root; a prefix in front
   of the app changes the redirect URI the app must be told.
7. A laptop run first: `SAHS_STORE=sqlite` and the non-production client
   with `http://localhost:8400/callback` added to its redirect URIs, never
   the production client.

## Path ownership at reconciliation

| path | owner |
|---|---|
| `helm/`, `config/`, `Dockerfile` | theirs, always |
| `apps/synapse/frontend/**`, `apps/synapse_admin/frontend/**` | ours |
| `backend/auth.py` (Okta sign-in), `sahs/identity/**`, `sahs/spanner.py` | ours once the Okta commits land; theirs for the Google connection routes |
| `backend/chat.py` review routes | ours |
| everything else they added | theirs unless a commit here names the file |
