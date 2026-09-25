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
`--no-matrix` (Okta, Google) to skip the cross-environment probes, `--timeout N`,
`--inventory` (below), `--redact`, `--out FILE`.
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

## Step two: what does each provider actually give us

The checks above prove reachability and registration. The inventories
answer the design question: for one real person, what does each provider
hand the app? Run all three for the same person, then join them.

```sh
python okta_check.py        --env-file .env.authcheck.local --inventory E1 --out okta.json
python google_auth_check.py --env-file .env.authcheck.local --inventory E1 --out google.json
python ldap_check.py        --env-file .env.authcheck.local --envs E1 --inventory --out ldap.json
python identity_map.py --okta okta.json --google google.json --ldap ldap.json
```

- **Okta** opens a browser and signs you in on that environment's client
  with a local callback (`OKTA_LOCAL_REDIRECT_URI`, default
  `http://localhost:8400/callback`; add it to the non-production client,
  never the production one). It prints every claim of the ID token, the
  access token when it is a JWT, and `/userinfo`, verifies the signature
  against the JWKS, and gives one row per thing the app needs:
  `need: email`, `need: name`, `need: groups`, `need: subject`, plus the
  factors used (`amr`) and the token lifetimes.
- **Google** runs the consent with offline access and the BigQuery scope,
  prints the ID token and `/userinfo` claims, the scopes actually granted,
  whether a refresh token came back (the stored connection needs one),
  whether the account is a Workspace account (`hd`), the projects the
  token can see, and a `SELECT 1` dry run in `GOOGLE_BQ_PROJECT`.
- **LDAP** reads the looked-up person's whole entry: the identity
  attributes with values, the rest by name, the account flags decoded,
  direct and nested groups, the manager's entry, a scan for a band or
  level attribute, and the join-key verdicts (does `mail` equal
  `userPrincipalName`; is there an `employeeID`).
- **identity_map** puts the three side by side and says what links them:
  whether the Okta e-mail is the directory's `mail` or its UPN and the
  Google account's e-mail, whether Okta's groups are directory groups,
  which identifiers are stable, what LDAP adds beyond Okta, and ends with
  recommendations in the app's own setting names (`AUTH_EMAIL_CLAIMS`,
  `OKTA_GROUP_CLAIM`, whether the LDAP lookup is needed at all).

`--redact` masks personal values (an e-mail keeps its domain, a name its
shape; group names and claim names stay) so the output can be pasted into
a ticket or a chat. The JSON written by `--out` is masked the same way.
Tokens are never printed, only their decoded claims.

## Where to run

LDAP hosts resolve only on the corporate network or VPN. Okta and Google are
public, but a locked-down outbound proxy may refuse `*.okta.com`; the
`discovery` row says so when that happens.
