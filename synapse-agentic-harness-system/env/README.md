# One `.env` per profile

`SAHS_ENV_FILE` is the single switch: it names the `.env` the loader
reads (`sahs.util.auth.load_dotenv`), and everything — the app, the
checks, the pipeline — reads that one file. Shell-exported variables
always win over it.

Four profiles, one example each, placeholders only (`<...>`):

| profile | file | what it is |
|---|---|---|
| `local` | `local.env.example` | a laptop: the sqlite stand-in, the email-and-password door, the Vertex or gateway plane |
| `e1` | `e1.env.example` | dev: Spanner, Okta, the gateway; the local door may stay open for a laptop pointed at the dev database |
| `e2` | `e2.env.example` | qa: as e1 with the local door shut |
| `e3` | `e3.env.example` | prod: as e2; `AUTH_LOCAL_LOGIN` must be unset |

Copy the example to `<profile>.env` beside it (git ignores everything in
this folder but the examples and this file), fill it in, then from the
repo root:

```sh
make check ENV=e1     # the readiness table: settings, DDL, Spanner, Okta, the model plane
make run ENV=e1       # SAHS_ENV_FILE=env/e1.env, uvicorn on 8810
```

Every variable is documented in `../.env.example`; `docs/deploy.md` at
the repo root is the mental model. Never commit a filled-in file, and
never put a real host, key, secret or token in an example.
