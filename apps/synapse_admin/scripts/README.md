# Sign-in preflights: Okta, Google, LDAP

Three standalone scripts that answer, per environment (E1 dev, E2 qa, E3 prod):

1. Can we reach the identity provider and read its details?
2. Is our client registered there, and does it accept our callback URL?
3. Optionally, does the configured client or service account authenticate?

They never sign anyone in, and they only ever talk to the provider itself.

## Setup

```sh
pip install ldap3            # LDAP only; Okta and Google use the standard library
pip install truststore       # optional: trust the OS keychain (corporate root CA)
cp .env.authcheck.example .env.authcheck.local   # git-ignored; fill it in
```

## Run

```sh
python okta_check.py        --env-file .env.authcheck.local
python google_auth_check.py --env-file .env.authcheck.local
python ldap_check.py        --env-file .env.authcheck.local
```

Flags: `--envs E1,E3` to narrow, `--json` for machine-readable output,
`--no-matrix` (Okta, Google) to skip the cross-environment probes, `--timeout N`.
Exit codes: 0 everything passed, 1 something FAILED, 2 configuration problem.

## How the callback check works without logging in

Okta and Google both validate `client_id` and `redirect_uri` before doing
anything else on their authorize endpoint. The scripts call it with
`prompt=none` and no browser session, and read the answer from the `Location`
header without following it:

| answer | meaning |
|---|---|
| 302 back to our callback with `error=login_required` | client and callback are registered |
| Okta 400 "The 'redirect_uri' parameter must be a Login redirect URI" | callback NOT registered on this client |
| Okta 400 "Invalid value for 'client_id'" | wrong client id for this authorization server |
| Google 302 to `/signin/oauth/error` decoding to `redirect_uri_mismatch` | callback NOT registered on this client |
| Google 302 to `/signin/oauth/error` decoding to `invalid_client` | client id does not exist |
| 302 back to our callback with `error=invalid_scope` (Okta) | callback fine, a requested scope is not defined on the server |

## Reading the table

- **PASS / FAIL** are decisive. **SKIP** means a value was not configured.
- **WARN** on `scopes published` / `claim ...`: custom scopes and claims appear
  in Okta's discovery document only when "Include in public metadata" is on;
  the live `callback` probe requests the scopes for real, so trust that row.
- **WARN** on `same callback as Google/Okta`: the two providers are configured
  with different callback URLs for the same environment.
- **cross-env** rows probe each client against the *other* environments'
  callbacks. `rejects (expected)` is the healthy answer; `ACCEPTS` is a WARN
  because a prod client should not accept a non-prod callback.
- LDAP `bind` reports which identity form the directory accepted (plain name,
  `user@domain`, or `DOMAIN\user`); the app must send that same form.

## Where to run

LDAP hosts resolve only on the corporate network or VPN. Okta and Google are
public, but a locked-down outbound proxy may refuse `*.okta.com`; the
`discovery` row says so when that happens.
