# The Okta-to-Google auth chain and the query as the person: what is proven, what is not

Branched from the integration branch at `e3524b6`.
Nothing pushed, no PR opened.

The owner asked: *have we tested the full Okta-to-Google auth flow, and would the
user-impersonated query work?* The short answer, before this branch: **no, and no.**
The sign-in hop was tested on its own and the Google consent hop had one test that
stopped at the token exchange; nothing drove a query through the person's token,
and doing so now exposes six bugs, two of which meant a live query as the person
could not have worked on any deployment without a service-account key, and one of
which meant the chat lane never had the person's runner at all. After this branch:
every hop that can be proven on fakes is, in both lanes, on both stores, and the
six bugs are fixed. What still needs a laptop and a real Google account is listed
by name at the end.

## The tests

- `apps/synapse_admin/tests/test_auth_e2e.py`: eight scenarios, each run under
  `SAHS_STORE=sqlite` and `SAHS_STORE=spanner` (the SDK double
  `tests/fake_spanner.py`), 16 tests. The fakes and the seams they enter through:

  | host | fake | seam |
  |---|---|---|
  | Okta | `SigningOkta` (`test_signin_flow.py`, over `FakeOkta` from the harness's `test_oidc.py`): discovery, JWKS, the token endpoint signing RS256 ID tokens with the nonce and groups the test names | `OidcClient(settings, http=...)`, patched in as `okta._client` |
  | Google | `FakeGoogle`: the token endpoint (code exchange with PKCE form, refresh grant), userinfo, tokeninfo; every access token it mints is remembered | one fake `urlopen` patched into `backend.auth`, `sahs.util.google_auth.oauth` and `sahs.util.google_auth.token`; the real `urllib.request.urlopen` and `plane_opener` are patched to raise |
  | BigQuery | `FakeBigQuery`: `jobs` (dry run) and `jobs.query` (rows), recording the URL, body and `Authorization` header of every trip; refuses any bearer Google did not mint | `BQConnection.opener()` |
  | Spanner | `FakeSpannerDatabase` | `SpannerDatabase.from_settings` |
  | the model | `ScriptedModel` from `test_ask_loop.py` | `AskRuntime.model_for` |

  Placeholders only: the Okta client is the harness fixture's, the Google client id and
  secret are literal strings, the Fernet key is generated in the fixture, the BigQuery
  project is `test-project` at `https://bigquery.test`. No credentials, no new
  environment variables, no model identifiers.
- `synapse-agentic-harness-system/tests/test_p3_tools.py::test_sandbox_live_as_the_person_dry_runs_on_the_runners_connection`:
  the sandbox alone, with a user-scoped `BQJobRunner` and a fake transport.

Both suites exit 0: the app suite (`PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider`)
and the harness suite (`cd synapse-agentic-harness-system && python -m pytest -q tests -p no:cacheprovider`).
Run against the unfixed code, 8 of the 16 new app tests fail (the ask lane, the chat
lane, the no-project case and the Bearer path, on both stores); each failure names
one of the bugs below.

## Every hop

"Proven on fakes" means the app's own code ran the hop end to end with only the
outside host replaced. "After a fix" names the fix from the next section.

| # | hop | verdict | where |
|---|---|---|---|
| 1 | Okta discovery, JWKS | proven on fakes (the client fetches both from the fake issuer; another issuer is refused: `test_oidc.py`) | `test_auth_e2e.py::_sign_in`, `test_oidc.py` |
| 2 | Okta authorize hop: state `okta.…`, nonce and PKCE verifier parked in `AuthStates`, redirect to `/v1/authorize` with S256 | proven on fakes | `_sign_in` |
| 3 | Okta code exchange (Basic client auth, then the POST-body fallback) | proven on fakes | `test_oidc.py`, `_sign_in` |
| 4 | ID-token verification: signature against the JWKS, nonce, aud, iss, exp, alg, kid, key rotation | proven on fakes (every refusal enumerated) | `test_oidc.py` |
| 5 | The person found or created, roles from groups, `ExternalIdentities` row, `login.ok` audit, session issued, `synapse_session` (HttpOnly, SameSite strict) and `synapse_csrf` cookies, redirect to the surface | proven on fakes, both stores | `test_okta_sign_in_then_google_connect_end_to_end`, `test_signin_flow.py` |
| 6 | `GET /api/auth/google/connection` before connecting: not connected, `requires_user_oauth` | proven on fakes | same |
| 7 | `GET /api/auth/google/start?popup=1`: 307 to `accounts.google.com/o/oauth2/v2/auth` with the client id, the registered callback, the four scopes, `access_type=offline`, `prompt=consent`, S256 challenge; state of kind `google_connect` parked in `AuthStates` under the person | proven on fakes | same |
| 8 | The consent screen itself | **not provable here, needs a laptop run** | `google_auth_check.py --inventory` |
| 9 | `GET /callback?code&state` (the one shared callback): the state popped once, the cookie session must be the one that started it | proven on fakes: a replay is 400, no cookie is 401, another person's cookie is 401 | `test_the_callback_refuses_what_it_must` |
| 10 | Google token exchange: `grant_type=authorization_code`, the code, `code_verifier` whose S256 hash is the challenge sent at start, the client secret, the redirect URI | proven on fakes | `test_okta_sign_in_then_google_connect_end_to_end` |
| 11 | userinfo with the exchanged access token: sub, email, `email_verified`; email must equal the Okta person's | proven on fakes: mismatch is 403, unverified is 401, both without a row | `test_the_callback_refuses_what_it_must` |
| 12 | The scopes Google actually grants | **not provable here** (the fake grants what it is told; a missing BigQuery scope is proven to be refused with 400 and no row) | inventory |
| 13 | Google's refresh-token issuance rules (a second consent without `prompt=consent`, a Workspace policy, a testing-mode client's seven-day expiry) | **not provable here** (a response without a refresh token is proven to be refused with 400 and no row; the app sends `prompt=consent` and `access_type=offline`, which is what makes Google issue one) | inventory |
| 14 | The connection saved: `GoogleOAuthConnections` row with subject, email, scopes, and the refresh token Fernet-encrypted with `GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY`; the plaintext is not in the row; the key decrypts it | proven on fakes, both stores | same |
| 15 | The popup answer: an HTML page that posts `google-connected` to the opener and closes; the non-popup answer: 303 to the chat (analyst) | proven on fakes (the page's text; the browser side of `postMessage` is not) | same |
| 16 | `GET /api/auth/google/connection` after: connected, the Google email | proven on fakes | same |
| 17 | The ask lane, `POST /api/sessions/{id}/messages`: the runtime carries `_google_runner(owner)`; the scripted model composes SQL; the sandbox dry-runs first, then runs | proven on fakes **after fixes 2 and 3** | `test_the_ask_lane_runs_the_query_as_the_person` |
| 18 | Refresh: the provider decrypts the stored refresh token and posts `grant_type=refresh_token` with the client id and secret; the minted access token is cached for its lifetime (a second turn does not refresh again) | proven on fakes **after fix 6** | same |
| 19 | BigQuery as the person: both the dry run (`…/projects/test-project/jobs`, `dryRun`) and `jobs.query` carry `Authorization: Bearer <the refreshed token>`, never the consent-time token and never a service account; the row cap rides as `LIMIT`; the answer carries rows and the `executes` criterion passes | proven on fakes after fixes 2 and 3 | same |
| 20 | The gates around it: ACL from the compiled build, `SAHS_ALLOW_LIVE`, the cost gate between the dry run and the run (a dry run priced at 10^12 bytes is `cost_gate_budget`, and `jobs.query` is never sent) | proven on fakes | `test_the_chat_lane_runs_the_query_as_the_person`, `test_p3_tools.py` |
| 21 | Disconnected, or never connected: refused as `google_oauth_required` with the hint ("Open Account, connect Google BigQuery, and retry"), nothing sent to BigQuery, nothing refreshed; a chat runtime's cached access token does not outlive the connection | proven on fakes **after fixes 3 and 6** (ask lane: the answer's limits; chat lane: the tool result and the assistant's sentence) | both lane tests |
| 22 | The chat lane, `POST /api/chat/sessions/{id}/run` on a proposed query: the same runner, dry run, run, rows as a table artifact | proven on fakes **after fix 1** | `test_the_chat_lane_runs_the_query_as_the_person` |
| 23 | The Bearer path: a Google access token as `Authorization: Bearer` on a cookie session, validated through tokeninfo (email, `email_verified`, the two required scopes), refused on email mismatch (403) and missing scope (401), and `_google_request_runner(token)` runs BigQuery with exactly that bearer | proven on fakes **after fix 4**; note that no route depends on `google_bigquery_user` today (the test mounts one) | `test_the_bearer_path_validates_the_google_token_for_the_cookie_user` |
| 24 | `google_bigquery_user_if_live`: silent when `SAHS_ALLOW_LIVE` is off or `ASK_EXECUTE=snapshot` | proven on fakes | `test_the_soft_bearer_dependency_is_silent_when_live_is_off` |
| 25 | Disconnect: `DELETE /api/auth/google/connection` sets `RevokedAt`, the row is kept, the connection reads as absent, the cached provider is invalidated | proven on fakes, both stores | `test_okta_sign_in_then_google_connect_end_to_end` |
| 26 | The one shared callback: an `okta.` state goes to the Okta handler (an unknown one lands on the sign-in page with the reason), any other state goes to Google (an unknown one is 400 "Google OAuth state is invalid or expired", no state is 400 "Google callback is missing code or state"), and `/api/auth/okta/callback` is the same handler | proven on fakes | `test_the_one_callback_dispatches_on_the_state_prefix` |
| 27 | `SAHS_BQ_AUTH_MODE=user` with no BigQuery project: the lanes still open, the runner is absent | proven on fakes **after fix 5** | `test_a_workspace_without_a_bigquery_project_still_opens_the_lanes` |
| 28 | The enterprise route: the proxy pinned on `BQConnection.opener()`, the corporate CA bundle, the private BigQuery endpoint | **not provable here** (the opener is the fake) | a laptop on the corporate network, `scripts/bq_check.py` |
| 29 | A real Spanner (interleaving, `JsonObject` cells, commit timestamps) | **not provable here** (the SDK double) | `scripts/spanner_check.py` |
| 30 | Google's IAM answer on the real project (the person has `bigquery.jobs.create` and read on the tables) | **not provable here** | inventory with `GOOGLE_BQ_PROJECT` |

## Bugs found and fixed

Each one has a test that fails on `e3524b6` and passes now.

1. **The chat lane had no per-person runner.** `backend/chat.py::_make_runtime` built
   the `AssistantRuntime` without `runner=`, while `ask.py::_ask` attached
   `_google_runner(owner)`. Under `SAHS_BQ_AUTH_MODE=user` every Run on the chat was
   refused `google_oauth_required` even for a person with a connection (the sandbox
   sees `runner=None` and user mode). Fixed the way `ask.py` does it: the runner is
   attached when a signed-in person's runtime is built; `None` (no BigQuery
   configured) keeps live execution denied as before. The provider re-reads the
   connection whenever it has no cached token, so connecting after the runtime was
   built works, and disconnecting invalidates the cached token.
2. **The dry run demanded a service-account key.** `sahs/tools/sandbox.py` built the
   user-scoped dry-run substrate as `BQDryRun(token_provider=…)`, whose default
   connection is `BQConnection.from_env()` with `require_key=True`. On a deployment
   that runs BigQuery as the person, which is exactly the one without a key, every
   live attempt raised `AuthError: no SA key configured` before any trip. The ask
   lane swallowed it in `run_query` and answered "validated by dry run, not executed"
   with an error envelope; the chat lane said "the run failed before the warehouse
   answered". No refresh was ever attempted, so nothing in the Google chain past the
   consent ran in practice. Fixed: the substrate rides the runner's own connection
   (`BQDryRun(connection=runner.connection, token_provider=…)`).
3. **A token provider with nothing to mint from was an exception, not the refusal.**
   `GoogleOAuthCredentialProvider` raises `GoogleOAuthTokenError` when the connection
   is gone or Google refuses the refresh; `BQDryRun.dry_run` calls the provider
   outside its `try`, so the error escaped `execute_sandboxed`. The ask lane reported
   `GoogleOAuthTokenError: connect Google BigQuery…` as a generic error, the chat "the
   run failed before the warehouse answered", neither with the hint the account page
   needs. Fixed: the sandbox catches it around the dry run and the run and returns the
   one `google_oauth_required` denial (`_google_oauth_required`), the same words as
   the no-runner case, `kind: access`, `yours_to_fix: false`, the hint. The ledger
   records it as denied.
4. **The Bearer path could not work with a cookie.** `backend/auth.py::current_user`
   resolved `Bearer` first and *replaced* the cookie with it, so a Google access token
   in the header was looked up as a session token, found nothing, and the request was
   401 "sign in required" before `google_bigquery_user` could validate it. Fixed: a
   Bearer that is not a session token falls back to the cookie; a Bearer that is a
   session token still signs in on its own; a stray Bearer with no cookie is still
   nobody.
5. **`SAHS_BQ_AUTH_MODE=user` without a BigQuery project was a 500.**
   `_google_runner` caught `AuthError` only on the service-account branch;
   `BQJobRunner(token_provider=…)` also calls `BQConnection.from_env`, which raises
   when `BQ_PROJECT_ID` (or its aliases) is unset, and `_ask()` let it out of
   `POST /api/sessions`. After fix 1 the same would have broken every chat route.
   Fixed: caught and logged at warning level, the runner absent; the sandbox then
   refuses live runs with `google_oauth_required`, which is the closest existing
   refusal but not the precise cause: the log line names it, and `scripts/bq_check.py`
   (or `make check`) is the place that reports a missing project.
6. **Disconnect left a cached access token alive, and every Ask message refreshed.**
   `_google_runner` built a new `GoogleOAuthCredentialProvider` on every call and
   kept only the latest in `_google_providers`, so disconnect invalidated the latest
   one: the chat runtime's provider (built once per runtime) kept its cached access
   token, up to an hour, after the person had disconnected, whenever the ask lane had
   built a provider since; and the ask lane, which builds a runner per message,
   made one refresh trip per message. Fixed: one provider per person, shared by every
   runner built for them; one refresh per token lifetime; disconnect invalidates the
   one cache. Proven on both lanes (two refreshes for one turn, and a run that
   succeeded after disconnect, on the old registry).

## Findings, not fixed

- `google_bigquery_user`, `google_bigquery_user_if_live` and `_google_request_runner`
  are defined and now proven, but no router depends on them; every live run today goes
  through the stored connection, not a browser-held token. Either wire them or remove
  them when the enterprise branch is reconciled.
- Disconnect revokes the row and the cache, not the grant at Google: the app never
  calls Google's revoke endpoint, so the refresh token stays valid at Google until
  the person removes the app from their account. A reconnect overwrites the row.
- The store's `Scopes` column reads back as a list under the fake SDK and as JSON text
  under sqlite `_query`; `google_connection()` normalizes it, raw reads do not. The
  test handles both.

## How to prove it on a laptop

The preflights first, neither signs anyone in:

```sh
# Okta: discovery, JWKS, the client accepts the callback, the group claim is published
python apps/synapse_admin/scripts/okta_check.py --env-file .env.authcheck.local --envs E1 --no-matrix
# Google: the OAuth client exists and accepts the callback; with GOOGLE_CLIENT_SECRET_E1 the id+secret pair
python apps/synapse_admin/scripts/google_auth_check.py --env-file .env.authcheck.local --envs E1 --no-matrix
```

Then the inventories, each through a browser once on the non-production client with
`http://localhost:8400/callback` added to that client's redirect URIs (never the
production client). These answer hops 8, 12, 13 and 30 above:

```sh
python apps/synapse_admin/scripts/okta_check.py --env-file .env.authcheck.local --inventory E1 --redact --out okta.json
# every claim of the ID token and /userinfo, the signature against the JWKS, a verdict on the email, name, groups, subject, lifetimes

GOOGLE_BQ_PROJECT=<the project> GOOGLE_EXPECTED_HD=<the Workspace domain> \
python apps/synapse_admin/scripts/google_auth_check.py --env-file .env.authcheck.local --inventory E1 --redact --out google.json
# the real consent, the scopes actually granted, whether a refresh token came back, the hd claim,
# and a BigQuery dry run of SELECT 1 in that project with the person's token
```

Then one manual pass through the app, `SAHS_STORE=sqlite`, in the silo `.env`:
`AUTH_PEPPER`, the `OKTA_*` block with `OKTA_REDIRECT_URI=http://localhost:8400/callback`,
`AUTH_GROUP_ROLE_MAP`, `SAHS_BQ_AUTH_MODE=user`, the `GOOGLE_OAUTH_*` block with
`GOOGLE_OAUTH_REDIRECT_URI=http://localhost:8400/callback` and a Fernet key,
`BQ_PROJECT_ID`, `SAHS_ALLOW_LIVE=1`, and a compiled build under
`MERIDIAN_BUILDS_DIR`. No service-account key.

1. `uvicorn apps.synapse_admin.backend.app:app --port 8400`; open
   `http://localhost:8400/synapse/`; sign in with Okta. `GET /api/auth/me` names you
   with the roles your groups map to.
2. Account → connect Google: the popup shows Google's consent for the four scopes and
   closes itself; `GET /api/auth/google/connection` says connected with your Google
   email. In the identity sqlite file, `GoogleOAuthConnections` holds ciphertext, not
   the refresh token.
3. Ask a data question in the chat and press Run on the proposed query. The rows
   arrive as a table; `sandbox_ledger.jsonl` beside the builds directory has a
   `decision: ok` line for it; and BigQuery's job history for the project
   (`INFORMATION_SCHEMA.JOBS_BY_USER`, or the console) shows the job under **your**
   email, not a service account. That last line is the impersonation proven.
4. Account → disconnect; press Run again: the chat says it could not run it,
   `google_oauth_required`, "connect Google BigQuery, and retry", and no job appears
   in BigQuery.
