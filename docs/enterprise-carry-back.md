# Carrying everything back: `apps/` and `synapse-agentic-harness-system/` from wyla into the enterprise repository, run, and taken to E3

This is the one document a person on the enterprise team follows. It
assumes `docs/enterprise-port.md` (how the identity branch was landed
here, the deviations table, the first cherry-pick recipe) and extends
it with everything that happened after that landing: wyla `main` at
the merge of PR #151 (`0edaa0b`), which is the `claude/production-ready`
branch fast-forwarded onto `main`.

Every command below is pasteable. Every path named exists in this tree
at that commit. Placeholders are in angle brackets; never a real host,
key, project id or person's name appears here, and none should be
added when the document crosses.

## Contents

1. [The mental model, in ten lines](#1-the-mental-model-in-ten-lines)
   - [The landing commit](#the-landing-commit)
   - [Path ownership, restated and extended](#path-ownership-restated-and-extended)
2. [The procedure](#2-the-procedure)
   - [2.1 The remote and the branch](#21-the-remote-and-the-branch)
   - [2.2 What never crosses](#22-what-never-crosses)
   - [2.3 Ours-owned paths: checkout by path](#23-ours-owned-paths-checkout-by-path)
   - [2.4 Theirs-owned files we touched: one three-way patch](#24-theirs-owned-files-we-touched-one-three-way-patch)
   - [2.5 The conflict rule](#25-the-conflict-rule)
   - [2.6 The same work one commit at a time: the cherry-pick list](#26-the-same-work-one-commit-at-a-time-the-cherry-pick-list)
   - [2.7 The commit](#27-the-commit)
3. [The DDL](#3-the-ddl)
4. [The `.env` for each environment](#4-the-env-for-each-environment)
5. [Dependencies for the image](#5-dependencies-for-the-image)
6. [The verification ladder](#6-the-verification-ladder)
7. [Known differences to decide](#7-known-differences-to-decide)
8. [Checklist](#8-checklist)

## 1. The mental model, in ten lines

1. Their code is the baseline and lands as they wrote it; ours overrides only the paths we deliberately touch, and in those paths our version wins.
2. The two repositories share one codebase and no history: never `git merge` a wyla branch into theirs or the reverse; paths cross by checkout, hunks cross by patch or cherry-pick.
3. The landing commit `8a87402` is their own code coming home plus the modules written here to their interfaces; it never crosses as a commit.
4. Everything after it on wyla `main` is ours: six pull requests (#146 to #151), 39 commits under `apps/` and `synapse-agentic-harness-system/`, 172 files, one CI workflow.
5. A path is either *ours* (checked out whole from `wyla/main`) or *theirs* (left as they have it, with our hunks applied on top where a commit here touched it); the table below says which.
6. Three files are theirs verbatim and must stay byte-identical on both sides: `db/spanner/001_identity.sql`, `sahs/util/spanner/settings.py`, and the top block of `sahs/identity/authorization.py`.
7. The database is the deployment's: one Spanner database per environment, DDL files applied in number order, `scripts/spanner_check.py` says what the live database is missing.
8. The environment is the deployment's: one `.env` per environment, every variable documented in `synapse-agentic-harness-system/.env.example`, secrets from the secret store, and `make check ENV=<env>` refuses a file that is not ready.
9. The tests are the proof: `make test` runs both suites (158 app, 654 harness at PR #151) with no network; `make ddl-check` lints the DDL without a Spanner.
10. Nothing under `docs/` that names wyla, the port, a PR number, a worktree or an agent crosses; the three reports under `synapse-agentic-harness-system/docs/reports/` cross only after the sanitization pass (`docs/reports/sanitization-scan.md`).

### The landing commit

`8a87402` — "Land the enterprise identity branch: Spanner identity
service, delegated BigQuery, CSRF and CORS gates, builds from Spanner;
the missing modules written to its interfaces" — is the first commit of
PR #146 (merge `b2a15d7`). It is their `feature/unified-changes` branch
as written, with the deviations `docs/enterprise-port.md` tables (the
`AUTH_LOCAL_LOGIN` gate, the cookie's `auto`, the env-named certificate
package and secret mount, the endpoint maps from `SAHS_*_ENDPOINT_E{1,2,3}`,
the `users()` fix in `admin.py`), plus the modules that did not exist
here: `sahs/identity/{store,database,authorization}.py`, `sahs/spanner.py`,
`sahs/constants.py`, `sahs/util/bigquery_errors.py`, `sahs/util/paths.py`,
`db/spanner/006_external_identities.sql`, the `identity` extra in
`pyproject.toml`.

### Path ownership, restated and extended

The port doc's table, restated, then every path touched since the
landing (`git diff --stat a87fd28..0edaa0b -- apps synapse-agentic-harness-system`),
each marked **ours** (checkout whole from `wyla/main`) or **theirs**
(their file; our hunks, if any, cross by patch). Paths are relative to
the repository root; `sahs/` and `db/`, `scripts/`, `tests/`, `env/`,
`docs/` without a prefix mean `synapse-agentic-harness-system/…`.

| path | owner | what crossed or changed since the landing |
|---|---|---|
| `helm/`, `config/`, `Dockerfile`, `config/settings.py` | **theirs, always** | absent here; the code tolerates the missing `config.settings` and reads the environment |
| `.github/workflows/tests.yml` | ours, **crosses only if they have no CI of their own** | both suites plus the DDL lint on every PR and push to main |
| `Makefile` (repository root) | ours | `make run ENV=`, `make test`, `make check ENV=`, `make ddl-check` |
| `apps/synapse/frontend/**` | ours | `js/session.js`, `js/pages/signin.js`, `js/pages/account.js`, `js/knobs.js`, `js/pages/chat.js`, `js/api.js`, `js/main.js`, `index.html`, `styles/app.css`, `styles/synapse.css` |
| `apps/synapse_admin/frontend/**` | ours | the same files plus `js/pages/users.js`, `js/pages/skills.js` |
| `apps/synapse_admin/backend/security.py`, `access.py` | theirs, as landed | untouched since |
| `apps/synapse_admin/backend/auth.py` | theirs, with our hunks | the `AUTH_LOCAL_LOGIN` 403 gate, cookie `auto` by request scheme, the Google consent state in `AuthStates`, `/api/whoami`, `_identity()` shared with every store (commits `998aa24`, `561a0ef`, `16e69af`, `5bc879d`) |
| `apps/synapse_admin/backend/admin.py` | theirs, with our hunks | `users()` on `store.list_users(limit)` (`561a0ef`) |
| `apps/synapse_admin/backend/app.py` | theirs, with our hunks | the Okta router mounted, `SYNAPSE_HEALTH_ALIAS` (`561a0ef`, `68efacf`) |
| `apps/synapse_admin/backend/ask.py` | theirs, with our hunks | the Ask lane on `SpannerAssistantStore` under a store (`dec5102`) |
| `apps/synapse_admin/backend/meridian.py` | theirs, with our hunks | the staging door and the shelf through `SpannerContentStore` (`d0ba30b`) |
| `apps/synapse_admin/backend/okta.py` | ours | Okta OIDC sign-in; `/callback` for both providers |
| `apps/synapse_admin/backend/chat.py` | ours | one runtime per person on the chat tables, the review routes kept, the event stream from the store, `model` fields at 64 |
| `apps/synapse_admin/scripts/**` | ours | `okta_check.py`, `google_auth_check.py`, `ldap_check.py`, `identity_map.py`, `.env.authcheck.example`, `README.md` |
| `apps/synapse_admin/tests/**` | ours | every test file |
| `apps/synapse_admin/README.md` | ours | the run modes, the store table, the skills paragraph |
| `sahs/util/google_auth/{__init__,oauth,token}.py`, `sahs/util/network.py`, `sahs/builds/{__init__,bundle,resolve,spanner_store}.py`, `sahs/ask/{generate,verify}.py`, `sahs/compiler/compile.py` | theirs, as landed | untouched since |
| `sahs/tools/sandbox.py` | theirs, with our hunk | the delegated-BigQuery live gate behind the cost gates; no runner without BigQuery instead of a crash (`998aa24`) |
| `sahs/util/auth.py` | theirs, with our hunks | `load_dotenv(override=)` and the once-per-process parse (`16e69af`, `2a9d44e`); `SAHS_SECRETS_DIR` (landing deviation) |
| `sahs/util/tls.py`, `sahs/constants.py` | theirs, with the landing deviations | `SAHS_CA_PACKAGE`, `SAHS_SECRETS_DIR`, the three endpoint maps from `SAHS_{VERTEX,OAUTH_TOKEN,SPANNER}_ENDPOINT_E{1,2,3}`; take the deviation or keep their literals (section 7) |
| `sahs/util/spanner/settings.py` | **theirs, verbatim** | byte-identical on both sides; never edit |
| `sahs/util/spanner/__init__.py` | ours | was `sahs/util/spanner.py` (the REST plane); moved so their `settings.py` has a package |
| `sahs/identity/authorization.py` | theirs verbatim, ours appended | their block as written; `ROLES`, `surfaces_for_roles`, `is_known_role` appended after the marker comment |
| `sahs/identity/{__init__,store,database,oidc}.py`, `sahs/identity_store.py` | ours | the store on Spanner or sqlite, the `Database` protocol, OIDC |
| `sahs/spanner.py` | ours | the facade over their `settings.py` |
| `sahs/ask/loop.py`, `sahs/ask/runtime.py` | theirs, with our hunks | `store=` kwarg, `sessions()` by kind, the library loading of skills (`dec5102`, `e500e74`) |
| `sahs/ask/events.py`, `sahs/ask/model.py` | ours | `EventBus.resume`, `should_stop` on the stream |
| `sahs/assistant/runtime.py` | ours (the landing added their `owner_user_id=` / `runner=` kwargs) | the bus sink into `ChatEvents`, replay, `owner` by user id, the task turns, Radix strings |
| `sahs/assistant/spanner_store.py` | ours | the chat tables of `002`: sessions, messages, artifacts, projects, memory, plans, feedback, events; `MODEL_CHOICE_CHARS = 64` |
| `sahs/assistant/content_store.py` | ours | `ChatFiles`/`ChatFileChunks`, `UserSkills`, `KnowledgeFiles`, the review board |
| `sahs/assistant/planner.py`, `prompt_version.py` | ours | multi-task turns; the prompt fingerprint |
| `sahs/assistant/{agent,authoring,events,files,kit,loop,reviews,skills_loader}.py` | ours | the model catalog and `facts`, the skills tree, the library mode, the stop path, the store-backed shelf |
| `sahs/loop/skill_index.py` | ours | the Markdown chunker, the FTS5 index, BM25 |
| `sahs/loop/{loop,prompt,skills}.py` | ours | frontmatter policy, the loader record, the routing hint |
| `sahs/observe/*` (`coverage`, `datasets`, `experiments`, `langfuse_emitter`, `prompts`, `record`, `setup`, `tracer`) | ours | the Langfuse mirror rebuilt from Spanner |
| `sahs/evals/{assistant_sut,grading,schema,suts}.py` | ours | the `decompose` kind, the assistant and planner SUTs |
| `sahs/enrich/{client,gateway_client}.py`, `sahs/util/gateway.py`, `sahs/util/profiles.py` | ours | the gateway model catalog, the per-engine thinking maps, `should_stop`; the hosts from the environment |
| `sahs/tools/validate_sql.py` | ours | `SAHS_SENSITIVE_COLUMNS` |
| `db/spanner/001_identity.sql` | **theirs, verbatim** | byte-identical on both sides |
| `db/spanner/004_google_oauth.sql` | theirs, as landed | untouched |
| `db/spanner/002_chat.sql`, `003_graph.sql` | ours | unchanged since the landing; `008` alters `002` |
| `db/spanner/005_build_bundles.sql` | ours (describes their writer's tables) | see section 3 |
| `db/spanner/006_external_identities.sql`, `007_content.sql`, `008_chat_model.sql`, `db/spanner/README.md` | ours | Okta links and auth states; file bytes and the review board; the two ALTERs |
| `scripts/{readiness,skills_check,skill_index_check,spanner_check,spanner_ddl_check,gateway_check,langfuse_sync,run_evals}.py` | ours | the readiness table, the skills tree check, `--emit-ddl`, the ALTER-aware lint, `--all-models`, `coverage`, `backfill --from spanner` |
| `env/{README.md,local,e1,e2,e3}.env.example` | ours | one placeholder profile per environment |
| `.env.example` | ours | the documented superset of every variable |
| `pyproject.toml` | ours | the `identity` extra |
| `tests/**` (harness) | ours | `fake_spanner.py` and every test file |
| `docs/` (harness: `model-playbook.md`, `skill-retrieval.md`, `multi-task-turns.md`, `spanner_schema.md`, `specs/synapse_v3_harness.md`, `runbooks/*`) | ours | cross after the sanitization pass |
| `docs/reports/*` (harness) | ours | cross only after the sanitization pass, or not at all |
| everything else they added | theirs | unless a commit here names the file |

## 2. The procedure

### 2.1 The remote and the branch

In a clone of the enterprise repository, on the branch you develop
from (`<their-base>` below; the port doc used `feature/unified-changes`):

```sh
git remote add wyla <wyla-clone-url>
git fetch wyla
git rev-parse --short wyla/main            # 0edaa0b or a descendant
git checkout -b feature/wyla-carry-back <their-base>
```

If `wyla/main` has moved past `0edaa0b`, pin the commit this document
was written against so the paths and hunks below are the ones it
describes:

```sh
git tag -f wyla-carry-back-base 0edaa0b
```

and read `wyla-carry-back-base` wherever `wyla/main` appears.

### 2.2 What never crosses

| path | why |
|---|---|
| `.github/workflows/tests.yml` | only if they have CI of their own; otherwise it crosses, and its `pip install` line is the dependency list of section 5 |
| `docs/enterprise-port.md`, `docs/enterprise-carry-back.md` (this file), `docs/deploy.md`, `docs/spanner-wiring.md`, `docs/CHANGELOG-2026-09.md`, `docs/README.md`, `docs/reports/*`, `docs/research/*`, `docs/wiki/*`, `docs/paper/*`, `docs/architecture/*`, `docs/presentation/*`, `docs/brand/*`, `docs/design_inventory.md` | root docs that name wyla, the port, the PR numbers or the worktrees; `deploy.md` and `spanner-wiring.md` are the two worth rewriting for their wiki (the content is accurate; only the cross-references to the port doc and the PR numbers name this side) |
| `README.md`, `MYSTIFY_REPORT.md` | name the repository |
| `.claude/` | the agent's skill files |
| `archive/`, `kc-exploration/` | exploration and the Knowledge Catalog spike, outside the product |
| `.gitignore`, `.mystify/` | the sanitizer's scan state (untracked at `0edaa0b`) and the ignore rules for it; take the four `env/` lines of `.gitignore` by hand (section 2.3) |
| `synapse-agentic-harness-system/docs/reports/{langfuse-insight,multi-task-turns,skill-retrieval}.md` | name worktrees, branches and commit hashes; cross after the sanitization pass or not at all |

### 2.3 Ours-owned paths: checkout by path

Take every ours-owned path whole from `wyla/main`. This is a superset
of the port doc's list (which stops at the identity modules). Directory
paths take the whole directory; a deleted file inside one is deleted.

```sh
git checkout wyla/main -- \
  Makefile \
  apps/synapse/frontend \
  apps/synapse_admin/frontend \
  apps/synapse_admin/backend/okta.py \
  apps/synapse_admin/backend/chat.py \
  apps/synapse_admin/scripts \
  apps/synapse_admin/tests \
  apps/synapse_admin/README.md \
  synapse-agentic-harness-system/.env.example \
  synapse-agentic-harness-system/env \
  synapse-agentic-harness-system/pyproject.toml \
  synapse-agentic-harness-system/db/spanner/002_chat.sql \
  synapse-agentic-harness-system/db/spanner/003_graph.sql \
  synapse-agentic-harness-system/db/spanner/005_build_bundles.sql \
  synapse-agentic-harness-system/db/spanner/006_external_identities.sql \
  synapse-agentic-harness-system/db/spanner/007_content.sql \
  synapse-agentic-harness-system/db/spanner/008_chat_model.sql \
  synapse-agentic-harness-system/db/spanner/README.md \
  synapse-agentic-harness-system/sahs/identity \
  synapse-agentic-harness-system/sahs/identity_store.py \
  synapse-agentic-harness-system/sahs/spanner.py \
  synapse-agentic-harness-system/sahs/util/spanner/__init__.py \
  synapse-agentic-harness-system/sahs/util/gateway.py \
  synapse-agentic-harness-system/sahs/util/profiles.py \
  synapse-agentic-harness-system/sahs/ask/events.py \
  synapse-agentic-harness-system/sahs/ask/model.py \
  synapse-agentic-harness-system/sahs/assistant \
  synapse-agentic-harness-system/sahs/loop \
  synapse-agentic-harness-system/sahs/observe \
  synapse-agentic-harness-system/sahs/evals \
  synapse-agentic-harness-system/sahs/enrich/client.py \
  synapse-agentic-harness-system/sahs/enrich/gateway_client.py \
  synapse-agentic-harness-system/sahs/tools/validate_sql.py \
  synapse-agentic-harness-system/scripts/readiness.py \
  synapse-agentic-harness-system/scripts/skills_check.py \
  synapse-agentic-harness-system/scripts/skill_index_check.py \
  synapse-agentic-harness-system/scripts/spanner_check.py \
  synapse-agentic-harness-system/scripts/spanner_ddl_check.py \
  synapse-agentic-harness-system/scripts/gateway_check.py \
  synapse-agentic-harness-system/scripts/langfuse_sync.py \
  synapse-agentic-harness-system/scripts/run_evals.py \
  synapse-agentic-harness-system/tests
```

Two things to check right after:

- `synapse-agentic-harness-system/sahs/util/spanner.py` must not exist
  beside the package (`sahs/util/spanner/__init__.py`); if their tree
  still has the module file, `git rm synapse-agentic-harness-system/sahs/util/spanner.py`.
  Their `settings.py` then goes into the package:
  `git checkout wyla/main -- synapse-agentic-harness-system/sahs/util/spanner/settings.py`
  and `git diff --stat <their-base> -- synapse-agentic-harness-system/sahs/util/spanner/settings.py`
  must print nothing (byte-identical), or their copy wins.
- `synapse-agentic-harness-system/sahs/identity/authorization.py` came
  across whole in the checkout above; the block above the comment
  `# (the block above is the enterprise branch's file as written; …` must
  equal their file. Verify with
  `diff <(sed -n '1,55p' synapse-agentic-harness-system/sahs/identity/authorization.py) <(git show <their-base>:synapse-agentic-harness-system/sahs/identity/authorization.py | sed -n '1,55p')`
  and take their block if it differs; ours is only what follows the
  marker (line 56 on, at `0edaa0b`).

The four `.gitignore` lines that keep the filled-in profiles out of the
repository (the tracked examples in, `env/*.env` out) go in by hand:

```sh
cat >> .gitignore <<'EOF'
!synapse-agentic-harness-system/env/
synapse-agentic-harness-system/env/*
!synapse-agentic-harness-system/env/*.env.example
!synapse-agentic-harness-system/env/README.md
EOF
```

Then the harness docs that are pure product documentation (after the
sanitization pass has been applied to them on the wyla side, or with
the lines the scan names edited by hand):

```sh
git checkout wyla/main -- \
  synapse-agentic-harness-system/docs/model-playbook.md \
  synapse-agentic-harness-system/docs/skill-retrieval.md \
  synapse-agentic-harness-system/docs/multi-task-turns.md \
  synapse-agentic-harness-system/docs/spanner_schema.md \
  synapse-agentic-harness-system/docs/specs/synapse_v3_harness.md \
  synapse-agentic-harness-system/docs/runbooks/langfuse.md \
  synapse-agentic-harness-system/docs/runbooks/langfuse-insight.md \
  synapse-agentic-harness-system/docs/runbooks/pipeline_end_to_end.md
```

### 2.4 Theirs-owned files we touched: one three-way patch

Fifteen theirs-owned files carry hunks of ours. Because the landing
commit holds their code exactly as it was on `<their-base>` (plus the
deviations), the difference between `<their-base>` and `wyla/main` on
those files is precisely what we changed, landing deviations included.
Apply it as one patch with three-way fallback so a file they have since
edited conflicts rather than mis-applies:

```sh
git diff <their-base> wyla/main -- \
  apps/synapse_admin/backend/auth.py \
  apps/synapse_admin/backend/admin.py \
  apps/synapse_admin/backend/app.py \
  apps/synapse_admin/backend/ask.py \
  apps/synapse_admin/backend/meridian.py \
  synapse-agentic-harness-system/sahs/tools/sandbox.py \
  synapse-agentic-harness-system/sahs/util/auth.py \
  synapse-agentic-harness-system/sahs/util/tls.py \
  synapse-agentic-harness-system/sahs/constants.py \
  synapse-agentic-harness-system/sahs/ask/loop.py \
  synapse-agentic-harness-system/sahs/ask/runtime.py \
  synapse-agentic-harness-system/sahs/ask/generate.py \
  synapse-agentic-harness-system/sahs/ask/verify.py \
  synapse-agentic-harness-system/sahs/compiler/compile.py \
  synapse-agentic-harness-system/db/spanner/001_identity.sql \
  > /tmp/wyla-theirs-owned.patch
git apply -3 --index /tmp/wyla-theirs-owned.patch
```

A file the patch reports as "already applied" or empty is one where
both sides agree (expected for `001_identity.sql`, `generate.py`,
`verify.py`, `compile.py`, `access.py`, `security.py`: theirs verbatim).
A conflict is resolved by section 2.5.

If `<their-base>` has moved a long way, generate the patch from the
landing commit's parent instead — `git diff 8a87402 wyla/main -- <the same files>`
— which carries everything since the landing but **not** the landing
deviations (the `AUTH_LOCAL_LOGIN` gate, the cookie `auto`, the
`users()` fix, `SAHS_SECRETS_DIR`, `SAHS_CA_PACKAGE`, the endpoint
maps, `SYNAPSE_HEALTH_ALIAS`); those you then apply by reading the
deviations table in `docs/enterprise-port.md` hunk by hunk.

### 2.5 The conflict rule

For a conflicted path take one side whole by the ownership table:

- an **ours** path: our side (`git checkout --theirs -- <path>` during a
  cherry-pick, where git's `--theirs` is the wyla commit; after
  `git apply -3`, the `>>>>>>>` side);
- a **theirs** path: their side for every hunk the deviations table
  does not name, ours for the hunks it does (`docs/enterprise-port.md`,
  "Deviations, each deliberate"), and ours for the hunks the ownership
  table above lists under "what crossed or changed since";
- `sahs/util/spanner/settings.py`, `db/spanner/001_identity.sql`: theirs,
  always, then confirm byte-identical;
- `sahs/identity/authorization.py`: theirs above the marker comment,
  ours below it.

Then `git add <path>` and continue.

### 2.6 The same work one commit at a time: the cherry-pick list

For a team that wants the commit identities, the same content crosses
as cherry-picks. The merge commits on wyla `main` (`git log --first-parent a87fd28..0edaa0b`),
so the numbers can be copied:

| merge | PR | subject |
|---|---|---|
| `b2a15d7` | #146 | Enterprise sign-in: their identity branch, Okta on the store, both surfaces sign in |
| `6084ac6` | #147 | Composer knobs, the gateway plane with several models, hosts out of source |
| `a661202` | #148 | Model profiles: one harness, five engine maps |
| `c62cfc6` | #149 | Local email-and-password sign-in on Spanner; the chats in the chat tables |
| `8b07837` | #150 | Production-ready: storage foundation, content to Spanner, multi-task turns, skill retrieval, Langfuse insight |
| `0edaa0b` | #151 | The laptop-test fixes: the UI fixes, the backend fixes, the reports |

Inside PR #150 the four workstreams were merged in this order (first
parent of `c62cfc6..8b07837`): `07672a5` storage foundation, `4b3fa80`
content to Spanner, `62e75f2` multi-task turns, `7d39d54` skill
retrieval, `271645b` no-refusal skill loading, `fd240e6` Langfuse
insight; inside PR #151: `6a0a95f` UI fixes, `97b72f2` backend fixes.

The commits to cherry-pick, in order, after the checkout of section 2.3
has **not** been done (the two routes are alternatives; pick one). The
first commit of PR #146 never crosses (it is their code); the report-only
commits are skipped; the commits marked *drop* touch a path from 2.2 that
must be removed from the pick before `--continue`.

```sh
# PR #146 (merge b2a15d7): the modules the Okta commits depend on, by path first
git checkout wyla/main -- \
  synapse-agentic-harness-system/sahs/identity \
  synapse-agentic-harness-system/sahs/identity_store.py \
  synapse-agentic-harness-system/sahs/spanner.py \
  synapse-agentic-harness-system/sahs/constants.py \
  synapse-agentic-harness-system/sahs/util/bigquery_errors.py \
  synapse-agentic-harness-system/sahs/util/paths.py \
  synapse-agentic-harness-system/db/spanner/006_external_identities.sql \
  synapse-agentic-harness-system/.env.example \
  synapse-agentic-harness-system/pyproject.toml
git commit -m "Identity store, database, facade and states"
git cherry-pick 998aa24 561a0ef 1857044 16e69af 3e69d18
# PR #147 (merge 6084ac6)
git cherry-pick 0654971 2c7b41d ebf63d0 6404a84 124c776 4c6076e dec4683
#   ebf63d0 also touches .gitignore and .mystify/*: drop those paths from the pick
# PR #148 (merge a661202)
git cherry-pick 68efacf
# PR #149 (merge c62cfc6)
git cherry-pick 5bc879d
#   5bc879d also touches docs/spanner-wiring.md and docs/enterprise-port.md: drop
# PR #150 (merge 8b07837), workstream by workstream
git cherry-pick de90edd dec5102 2a9d44e 7cfc063
#   7cfc063 also touches .github/workflows/tests.yml, .gitignore, Makefile, README.md,
#   docs/deploy.md, docs/README.md, docs/spanner-wiring.md: keep Makefile, decide CI, drop the rest
git cherry-pick d0ba30b
#   d0ba30b also touches docs/spanner-wiring.md: drop
git cherry-pick d256f1f
git cherry-pick 231218a b74ad72
git cherry-pick 14e7af6 e500e74 f97fba5
# PR #151 (merge 0edaa0b)
git cherry-pick 762ac2b
#   762ac2b also touches .claude/skills/synapse-ui-designer/SKILL.md: drop
git cherry-pick c26dfb6
#   c26dfb6 also touches docs/deploy.md, docs/spanner-wiring.md, docs/wiki/07-serving.md,
#   docs/wiki/13-configuration.md: drop
```

Skipped as report- or doc-only: `6c819f6`, `39663ba`, `afbe27f`,
`de870a1`, `331dccf`, `44a268e`, `33fade0`, `87b33da` (CI only),
`070d6e0`, `a75a6fe`, `6aa5e58`, `a33db96`, `bec3f6e`, `29c11a2`,
`4115ae7`.

To drop a path from a pick: `git rm --cached -q <path> && rm -f <path>`
(or `git checkout HEAD -- <path>` for a file that existed before), then
`git cherry-pick --continue`.

**The commit messages never cross as written.** Every commit on this
side ends in two trailer lines (`Co-Authored-By:` and `Claude-Session:`),
and the subjects and bodies name this repository, its PR numbers and its
branches. None of that belongs in their history. Take each pick without
its message and write one from the subject:

```sh
git cherry-pick --no-commit <sha>       # the path drops above go here
git commit -m "<the subject, reworded if it names a PR or a branch>"
```

or, after a plain `git cherry-pick`, `git commit --amend` and delete the
trailers and the references before moving on. `git cherry-pick -x` is
not used: the `(cherry picked from commit …)` line would name our
hashes.

The commits that touch a theirs-owned file, and therefore may conflict,
are: `998aa24` (`sandbox.py`, `auth.py`), `561a0ef` (`auth.py`,
`admin.py`, `app.py`), `16e69af` (`auth.py`, `sahs/util/auth.py`,
`001_identity.sql`, `authorization.py`, `sahs/spanner.py`), `68efacf`
(`app.py`), `5bc879d` (`auth.py`, `sahs/identity/database.py`),
`dec5102` (`ask.py`, `sahs/ask/runtime.py`), `2a9d44e` (`sahs/util/auth.py`),
`d0ba30b` (`meridian.py`), `e500e74` (`sahs/ask/loop.py`, `sahs/ask/runtime.py`).
Every other pick touches ours-owned paths only.

### 2.7 The commit

Whichever route, end with the tree equal to `wyla/main` on every
ours-owned path:

```sh
git diff --stat wyla/main -- \
  apps/synapse/frontend apps/synapse_admin/frontend \
  apps/synapse_admin/backend/okta.py apps/synapse_admin/backend/chat.py \
  apps/synapse_admin/scripts apps/synapse_admin/tests \
  synapse-agentic-harness-system/sahs/identity \
  synapse-agentic-harness-system/sahs/assistant \
  synapse-agentic-harness-system/sahs/loop \
  synapse-agentic-harness-system/sahs/observe \
  synapse-agentic-harness-system/db/spanner \
  synapse-agentic-harness-system/env \
  synapse-agentic-harness-system/scripts \
  synapse-agentic-harness-system/tests
```

must print nothing. On the checkout route, commit with a message that
names the source commit by hash only (`0edaa0b`), not this repository or
its branch; on the cherry-pick route every message was rewritten in 2.6.
Before the branch is opened for review, check the range that crosses:

```sh
git log --format=%B <their-base>..HEAD | grep -i -E "claude|anthropic|session_|co-authored|generated with|wyla|bardbyte"
```

must print nothing (a `-i` hit on a word such as "included" is not one;
read the line). Then run section 6.

## 3. The DDL

One database per environment; the files under
`synapse-agentic-harness-system/db/spanner/` apply in number order,
each as one batch. `db/spanner/README.md` is the authority; this is the
part for a database that already exists.

### What the live database has

E1 has `001` to `004` from the first rollout, and possibly the two
bundle tables of `005`: `BuildBundles` and `BuildBundleChunks` are the
tables their own `sahs/builds/spanner_store.py` writes when
`MERIDIAN_BUILDS_SOURCE=spanner`, and `005_build_bundles.sql` was
written here from that writer's INSERT columns because the repository
lacked a DDL file for tables the database already had. So `005`
describes tables E1 may already carry. How to tell, before applying
anything:

```sh
cd synapse-agentic-harness-system
SAHS_ENV_FILE="$PWD/env/e1.env" python scripts/spanner_check.py
```

The `designed` block lists every table under `db/spanner` as `present`
or `missing`, plus `undesigned` for a live table the repository has no
file for, and a `drift` line per present table whose columns differ.
Read it as:

| `spanner_check.py` says | do |
|---|---|
| `BuildBundles`, `BuildBundleChunks` under `missing` | apply `005` |
| both under `present`, no `drift` line for them | skip `005`; the file matches the tables |
| both under `present` with a `drift` line | do not apply `005`; run `--emit-ddl` (below), compare with the file, and reconcile the file to the live table (the live table is the one their writer fills) |
| any table under `undesigned` | `--emit-ddl` prints its `CREATE TABLE` so it can be checked in; nothing here writes it |

```sh
SAHS_ENV_FILE="$PWD/env/e1.env" python scripts/spanner_check.py --emit-ddl
```

prints `CREATE TABLE` statements (columns, `NOT NULL`, the primary key,
the interleave) for every live table the repository lacks — a starting
point to check in under `db/spanner`, not a substitute for reading it:
defaults, constraints, indexes and commit-timestamp options are not in
the listing.

### What to apply, in order

On a database with `001` to `004` (and `005` by the rule above):

```sh
cd synapse-agentic-harness-system
for f in db/spanner/006_external_identities.sql db/spanner/007_content.sql db/spanner/008_chat_model.sql; do
  gcloud spanner databases ddl update <spanner-database-e1> --instance=<spanner-instance> --ddl-file="$f"
done
```

with `db/spanner/005_build_bundles.sql` first in the list when the
check said `missing`, and `009_*.sql` last if one exists by the time
this runs (`ls db/spanner/` and `db/spanner/README.md` say). `008` is
`ALTER TABLE` only: it widens `ChatSessions.Model` to `STRING(64)`,
drops the plane `CHECK`, and replaces `ChatArtifacts`'s type list with
the registry's (`kpi` included); a fresh database applied `001` … `008`
and E1 with `008` on top end up the same.

Before applying, lint the files without a Spanner:

```sh
cd synapse-agentic-harness-system && python scripts/spanner_ddl_check.py
```

(`ok: 44 tables across 8 files; …` at PR #151). After applying:

```sh
SAHS_ENV_FILE="$PWD/env/e1.env" python scripts/spanner_check.py
```

must show `missing 0`. `008` adds no table, so the listing cannot show
it; check the width directly:

```sh
SAHS_ENV_FILE="$PWD/env/e1.env" python scripts/spanner_check.py \
  --sql "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ChatSessions' AND COLUMN_NAME = 'Model'"
```

must print `STRING(64)`.

### The `MessageCount` backfill

`SpannerAssistantStore.add_message` numbers a message by reading
`ChatSessions.MessageCount`, writing `Seq = MessageCount + 1` and
updating the count in the same transaction. A chat written by an older
writer that inserted `ChatMessages` rows without maintaining
`MessageCount` would have the next message land on a `Seq` already
taken. Find those chats first:

```sql
SELECT s.SessionId, s.MessageCount, m.Rows, m.MaxSeq
FROM ChatSessions AS s
JOIN (SELECT SessionId, COUNT(*) AS Rows, MAX(Seq) AS MaxSeq
      FROM ChatMessages GROUP BY SessionId) AS m
  ON m.SessionId = s.SessionId
WHERE s.MessageCount < m.MaxSeq OR s.MessageCount < m.Rows
```

then bring the count up to the head of what is there (`UpdatedAt` is
a commit-timestamp column, so `PENDING_COMMIT_TIMESTAMP()` is the value
to write):

```sql
UPDATE ChatSessions AS s
SET MessageCount = (SELECT GREATEST(COUNT(*), IFNULL(MAX(Seq), 0))
                    FROM ChatMessages AS m WHERE m.SessionId = s.SessionId),
    UpdatedAt = PENDING_COMMIT_TIMESTAMP()
WHERE s.MessageCount < (SELECT GREATEST(COUNT(*), IFNULL(MAX(Seq), 0))
                        FROM ChatMessages AS m WHERE m.SessionId = s.SessionId)
```

Both run through `gcloud spanner databases execute-sql <spanner-database-e1> --instance=<spanner-instance> --sql="…"`
(a DML statement runs in its own read-write transaction). If the old
writer gave every message the same `Seq` (duplicates within a chat:
`SELECT SessionId, Seq, COUNT(*) FROM ChatMessages GROUP BY 1, 2 HAVING COUNT(*) > 1`),
renumber those rows by `CreatedAt` before the backfill; the store reads
messages `ORDER BY Seq`.

## 4. The `.env` for each environment

One file per environment, derived from `synapse-agentic-harness-system/env/e1.env.example`
(`e2`, `e3` differ only where marked) and documented line by line in
`synapse-agentic-harness-system/.env.example`. The deployment hands
these to the container as environment variables from the secret store;
the file itself is for `make check` and a laptop pointed at the
environment. Markers: **[secret]** from the secret store, never in a
file in the repository; **[endpoint]** per-environment host or id;
**[policy]** a switch the deployment decides; **[absent in e3]** must
not be set there (`make check ENV=e3` refuses the file if it is).
`readiness.py` requires, in e1/e2/e3: `SAHS_STORE`, `EPAAS_ENV`,
`AUTH_PEPPER`, `MERIDIAN_BUILDS_SOURCE`, the three `SPANNER_*_ID`s, the
five Okta names, `IDP_TOKEN_URL`, `GATEWAY_BASE_URL`, `APP_ID`,
`APP_SECRET`; none may still hold a `<placeholder>`.

```sh
# ── the environment ──
EPAAS_ENV=e1                                     # e1 | e2 | e3: selects the SAHS_*_ENDPOINT_E<n> maps [endpoint]

# ── the store: one Spanner database for this environment ──
SAHS_STORE=spanner                               # spanner in every deployment; sqlite and local are the laptop
SPANNER_PROJECT_ID=<gcp-project>                 # the project the database lives in [endpoint]
SPANNER_INSTANCE_ID=<spanner-instance>           # [endpoint]
SPANNER_DATABASE_ID=<spanner-database-e1>        # one database per environment [endpoint]
SPANNER_URL=https://<private-spanner-endpoint>   # the private Spanner endpoint; direct route, pinned on the connection [endpoint]
SAHS_SPANNER_ENDPOINT_E1=https://<private-spanner-endpoint>   # the same host in the per-environment map (their network.py reads it) [endpoint]
SYNAPSE_SPANNER_SA_KEY=<path-to>/spanner-sa-key.json          # roles/spanner.databaseUser: a path, inline JSON or base64 JSON; unset with workload identity [secret]
SPANNER_FORCE_PROXY=0                            # 1 rides HTTPS_PROXY instead of the direct route

# ── sign-in ──
AUTH_PEPPER=<32-or-more-random-characters>       # the password pepper; 8 characters minimum, never changed once anyone has a password [secret]
AUTH_SESSION_HOURS=12                            # the absolute life of a sign-in
AUTH_IDLE_MINUTES=30                             # the idle expiry, pushed forward on use
AUTH_COOKIE_SECURE=auto                          # auto: Secure over https or behind X-Forwarded-Proto: https; set true if the ingress does not send the header
AUTH_BOOTSTRAP_ADMIN_EMAIL=<first-admin@example.com>   # the sign-up with this address is the first admin (the local door only)
AUTH_OPEN_SIGNUP=0                               # 0: only the bootstrap address may sign up; everyone else is created by an admin or arrives through Okta
AUTH_DEFAULT_ROLE=analyst                        # the role a new person gets: analyst opens /synapse/, admin opens both surfaces
AUTH_LOCAL_LOGIN=1                               # [policy] 1 opens the email-and-password routes; e1 may keep 1 for a laptop on the dev database; e2 sets 0; [absent in e3]
AUTH_ALLOW_INSECURE_DIRECT_RESET=0               # 1 resets a password without an email check; never 1 in e2 or e3
OKTA_ISSUER=https://<org>.okta.com/oauth2/<authorization-server-id>   # the OIDC issuer [endpoint]
OKTA_CLIENT_ID=<okta-client-id-e1>               # this environment's client [endpoint]
OKTA_CLIENT_SECRET=<okta-client-secret-e1>       # [secret]
OKTA_REDIRECT_URI=https://<e1-host>/callback     # must be registered on the client; the app's root, no prefix [endpoint]
OKTA_SCOPES=openid profile                       # add the custom scope the authorization server grants
OKTA_GROUP_CLAIM=groups                          # the ID-token claim carrying group names
AUTH_GROUP_ROLE_MAP=<admin-group>=admin,<steward-group>=steward,*=analyst   # [policy] without a group mapped to admin nobody opens the console
AUTH_EMAIL_CLAIMS=email,preferred_username       # where the email is read from, in order
SYNAPSE_ALLOWED_ORIGINS=                         # browser origins allowed cross-origin; empty is same-origin only
SYNAPSE_HEALTH_ALIAS=                            # a second health path the platform probes, e.g. /svc/health

# ── the model plane: the gateway behind an identity-service token ──
SAHS_MODEL_PLANE=gateway                         # gateway in every deployment; vertex is the laptop
AUTH_MODE=generated                              # generated mints the bearer from APP_ID/APP_SECRET; env reads GEMINI_BEARER_TOKEN
APP_ID=<app-id-e1>                               # the application id the token service knows [endpoint]
APP_SECRET=<base64-secret-e1>                    # [secret]
AUTH_VERSION=2                                   # the token service's protocol version
GATEWAY_MODEL=<gateway-model-name>               # the model a new chat starts on
GATEWAY_MODELS=<gateway-model-name>              # every model the gateway serves, space-separated; the composer lists them
GATEWAY_ROUTE=auto                               # direct | proxy | auto (direct first)
IDP_TOKEN_URL=https://<identity-service-host>/security/digital/v1/application/token   # the token service [endpoint]
GATEWAY_BASE_URL=https://<gateway-host>/genai/google/v1                               # the gateway's Gemini root [endpoint]
SAHS_OAUTH_TOKEN_ENDPOINT_E1=https://<oauth-token-endpoint>   # the per-environment OAuth token endpoint (their network.py) [endpoint]
SAHS_VERTEX_ENDPOINT_E1=https://<private-vertex-endpoint>     # the per-environment private Vertex endpoint (their network.py) [endpoint]

# ── the promoted build from Spanner, not a disk ──
MERIDIAN_BUILDS_SOURCE=spanner                   # [policy] spanner: the promoted bundle in BuildBundles; local: a directory baked into the image
MERIDIAN_BUILD_CACHE_DIR=<writable-cache-dir>    # where a pod unpacks the bundle once
MERIDIAN_BUILDS_REFRESH_SECONDS=300              # how often a pod re-checks which build is promoted

# ── BigQuery as the person (their connected Google account) ──
SAHS_BQ_AUTH_MODE=user                           # user: each person's connected Google account; service_account: the key
GOOGLE_OAUTH_CLIENT_ID=<google-oauth-client-id>  # [endpoint]
GOOGLE_OAUTH_CLIENT_SECRET=<google-oauth-client-secret>   # [secret]
GOOGLE_OAUTH_REDIRECT_URI=https://<e1-host>/callback      # the same /callback Okta uses; registered on the Google client [endpoint]
GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY=<fernet-key>   # encrypts the stored refresh tokens [secret]
SYNAPSE_BQ_PROJECT=<bq-query-project>            # runs and bills the query [endpoint]
SYNAPSE_BQ_DATA_PROJECT=<bq-data-project>        # hosts the tables when it is not the query project [endpoint]
SAHS_ALLOW_LIVE=1                                # [policy] 1 lets the chat run a query for rows; 0 is dry runs only
SAHS_LIVE_MAX_BYTES=1000000000                   # the scan ceiling a live query may not exceed
SAHS_SENSITIVE_COLUMNS=allow                     # [policy] allow (the default: a flagged column is readable, the read noted on the record) | deny (the query is refused)

# ── the network names (the landing deviations in tls.py, auth.py, constants.py) ──
SAHS_CA_PACKAGE=<importable-certificate-package> # the package exposing certificate_path(); unset if their tls.py keeps the import [endpoint]
SAHS_SECRETS_DIR=<secret-mount-path>             # the mount holding key.json / ca-bundle.crt; unset if their auth.py keeps the fixed path [endpoint]

# ── the mirror of the record ──
SAHS_LANGFUSE=0                                  # [policy] 1 mirrors every turn to Langfuse; the product never reads it back
SAHS_LANGFUSE_ENV=e1                             # the env tag on every trace
# LANGFUSE_PUBLIC_KEY=<pk-lf>                    # when SAHS_LANGFUSE=1 [secret]
# LANGFUSE_SECRET_KEY=<sk-lf>                    # when SAHS_LANGFUSE=1 [secret]
# LANGFUSE_BASE_URL=https://<langfuse-host>      # when SAHS_LANGFUSE=1 [endpoint]

# ── optional, the same in every environment ──
# MERIDIAN_SKILLS_DIR=<path-to>/skills           # the governed skills tree the picker walks (nested folders allowed); default <sources>/skills
# SAHS_MAX_SKILL_CHARS=220000                    # the longest skill loaded whole; over it a pack loads as a library, never refused
# SAHS_MAX_LOADED_SKILLS=4                       # how many packs one chat loads
# GATEWAY_THINKING_LEVELS=minimal:low,max:high   # the dial's last word per stop; the engine table in sahs/util/profiles.py decides otherwise
# GATEWAY_JSON_MODEL=<lighter-gateway-model>     # the judge, title, memory and review one-shots on a lighter model
# SPANNER_OWNER_USER_ID=                         # the person the builds-from-Spanner store writes as (PublishedBy)
# GATEWAY_CA_BUNDLE=                             # the root bundle if truststore is not installed
```

For **e2**: `EPAAS_ENV=e2`, every `_E1` name becomes `_E2`, the e2
ids and hosts, `AUTH_LOCAL_LOGIN=0`, `SAHS_LANGFUSE_ENV=e2`. For
**e3**: `EPAAS_ENV=e3`, `_E3`, the e3 ids and hosts, `SAHS_LANGFUSE_ENV=e3`,
and these lines **absent** (not `0`: absent): `AUTH_LOCAL_LOGIN`,
`SPANNER_EMULATOR_HOST`, `SAHS_IDENTITY_SQLITE`, `SYNAPSE_VERTEX_SA_KEY`,
`VERTEX_PROJECT_ID`, `VERTEX_MODEL`, `MERIDIAN_BUILDS_DIR`,
`GEMINI_BEARER_TOKEN`; `AUTH_ALLOW_INSECURE_DIRECT_RESET` stays `0`,
`AUTH_COOKIE_SECURE` is `auto` or `true`, never `false`;
`SAHS_SENSITIVE_COLUMNS` is `deny` if the policy requires it (section 7).
`make check ENV=e3` refuses a file that sets `AUTH_LOCAL_LOGIN` at all.

## 5. Dependencies for the image

`synapse-agentic-harness-system/pyproject.toml` declares the harness
(`sahs`) with these extras; the image needs the four the CI workflow
installs plus the four web and mirror packages that are not in the
`pyproject` (they belong to the app, which has no package of its own):

```sh
pip install -e 'synapse-agentic-harness-system[identity,assistant,sql,dev]' fastapi uvicorn httpx langfuse
```

| extra or package | pins in `pyproject.toml` | why |
|---|---|---|
| base | `pydantic>=2.7`, `pyyaml>=6` | the harness |
| `identity` | `google-cloud-spanner>=3.40`, `argon2-cffi>=23`, `cryptography>=42` | the identity store on Spanner, Argon2id passwords, Fernet-protected Google refresh tokens, RS256 verification of the Okta ID token |
| `assistant` | `python-pptx>=0.6.23`, `numpy>=1.26` | the PPTX export; the numpy the sandbox advertises |
| `sql` | `sqlglot==30.15.*` | the SQL validator; a tight pin because fingerprints embed the version |
| `dev` | `pytest>=8` | the suites; leave out of a production image if the suites run elsewhere |
| `fastapi`, `uvicorn` | unpinned here; pin to what the image already runs | the app |
| `httpx` | unpinned | the test client for the app suite (`fastapi.testclient`); `requests` is what the gateway and Vertex clients use and is already in their image |
| `langfuse` | unpinned | imported only when `SAHS_LANGFUSE=1`; the tracer's pinned ids use `opentelemetry.sdk`, which `langfuse` brings |
| `enrich` (optional) | `google-auth`, `truststore` | their image has both already; `truststore` is what makes the corporate root work without `GATEWAY_CA_BUNDLE` |

`sqlite3` with FTS5 is required by the skill index (`sahs/loop/skill_index.py`);
every CPython 3.11 build ships it, and the index falls back to memory
or a single chunk rather than failing a turn when the file cannot be
used. Python is `>=3.11`.

## 6. The verification ladder

Run in this order; each rung assumes the ones before it. Every command
is from the repository root unless it says `cd`.

1. **The DDL lint, no network.**
   `make ddl-check` → `ok: 44 tables across 8 files; keys, interleaves, foreign keys, indexes, policies, the property graph, the ALTERs and the registries agree`, exit 0.
2. **Both suites, no network.**
   `make test` → the app suite (`apps/synapse_admin/tests`: 158 passed, 2 skipped at PR #151) and the harness suite (`synapse-agentic-harness-system/tests`: 654 passed), both exit 0. (`make test` sets `PYTHONPATH=synapse-agentic-harness-system` for the app suite; without it the app suite fails at collection on `import sahs`.)
3. **The readiness table for e1.**
   `cp synapse-agentic-harness-system/env/e1.env.example synapse-agentic-harness-system/env/e1.env`, fill every `<placeholder>`, then `make check ENV=e1`. Every row `ok` or an allowed `skipped`: `settings` (no placeholder left, the pepper long enough, `EPAAS_ENV` matches), `ddl`, `spanner` (a token minted, the database answers, `missing 0`), `okta` (discovery and JWKS answer, the client accepts `OKTA_REDIRECT_URI`, without signing anyone in), `gateway` (the identity-service token and one model call). Exit 0.
4. **Publish a build.** From a host that compiled one (`python scripts/pipeline.py compile …` leaves `builds/<build-id>/`):
   ```sh
   cd synapse-agentic-harness-system
   SAHS_ENV_FILE="$PWD/env/e1.env" python - <<'EOF'
   from pathlib import Path
   from sahs.builds.spanner_store import SpannerBuildStore
   print(SpannerBuildStore.from_env().publish(Path("<builds-dir>/<build-id>"), actor="<your-email>"))
   EOF
   ```
   (`docs/deploy.md` names a `pipeline.py publish-build` subcommand; `scripts/pipeline.py` has no such subcommand at `0edaa0b`, and `SpannerBuildStore.publish` is what the message in `sahs/builds/spanner_store.py` means. Adding the subcommand is a one-screen change in `pipeline.py`'s `main`.) Then `make check ENV=e1` again: the `spanner` row stays `ok`, and the running app's `GET /api/synapse/planes` names the build.
5. **A laptop pointed at e1, signing in through Okta.** On the non-production Okta client add `http://localhost:8810/callback` to its Login redirect URIs (never on the production client), set `OKTA_REDIRECT_URI=http://localhost:8810/callback` and `AUTH_COOKIE_SECURE=auto` in `env/e1.env`, then `make run ENV=e1` and open `http://localhost:8810/`. Sign in through Okta; `GET /api/whoami` names you; `AUTH_GROUP_ROLE_MAP` must put you in `admin` or the console stays shut. Both surfaces (`/` and `/synapse/`) boot from `/api/auth/me`.
6. **The Google connection.** Account page → Connect Google; the consent hop returns to `/callback`, the connection shows on the account page, and `GoogleOAuthConnections` has your row. `python apps/synapse_admin/scripts/google_auth_check.py --env-file synapse-agentic-harness-system/env/e1.env --envs E1 --no-matrix` proves the client and the callback without a browser.
7. **One live query.** With `SAHS_ALLOW_LIVE=1` and `SAHS_BQ_AUTH_MODE=user`, ask a question the build answers and run the proposal for rows: the sandbox dry-runs, prices it under `SAHS_LIVE_MAX_BYTES`, executes as your Google account, and the rows come back. If a flagged column is projected, the check result carries `sensitive_column` as a note with `policy: allow` (or the refusal under `deny`).
8. **One chat with a file and a skill.** Upload a file on the chat (it lands in `ChatFiles` / `ChatFileChunks`, not on the pod's disk), pin a skill from the tree (`MERIDIAN_SKILLS_DIR`; `cd synapse-agentic-harness-system && python scripts/skills_check.py` lists what the picker sees and why a file was skipped), send one message, reload: the transcript and the turn's events replay from `ChatMessages` and `ChatEvents`; the `skills_loaded` record on the turn says `whole` or `library` per skill.
9. **The mirror's coverage.**
   `cd synapse-agentic-harness-system && SAHS_ENV_FILE="$PWD/env/e1.env" python scripts/langfuse_sync.py coverage` prints which Langfuse concept is built from which Spanner columns and how many rows each has; it needs the store, not Langfuse. With `SAHS_LANGFUSE=1` and the three `LANGFUSE_*` names, `python scripts/langfuse_sync.py backfill --from spanner --since <iso-date>` replays the turns (a second run creates nothing).
10. **The same ladder on e2**, with `env/e2.env`: `AUTH_LOCAL_LOGIN=0`, the e2 client, `https://<e2-host>/callback`; rungs 3 to 9 with the deployed host in place of the laptop for rungs 5 to 8.
11. **The same ladder on e3**, with `env/e3.env`: `AUTH_LOCAL_LOGIN` unset, `SAHS_SENSITIVE_COLUMNS=deny` if the policy requires it, the production Okta client with only `https://<e3-host>/callback` registered; `make check ENV=e3` first (it refuses a file that sets `AUTH_LOCAL_LOGIN`), then rungs 4 to 9 against the production host; the ingress must send `X-Forwarded-Proto: https` or `AUTH_COOKIE_SECURE=true`.

## 7. Known differences to decide

| difference | what wyla does | what to decide |
|---|---|---|
| the sensitive-column default | `SAHS_SENSITIVE_COLUMNS` unset or `allow`: a query that projects a column the build flags sensitive runs, and `sensitive_column` / `select_star_over_sensitive` come back as notes with `policy: allow` on the check result, so the record says the column was read; `deny` restores the refusal (`sahs/tools/validate_sql.py`) | whether e2 and e3 set `deny`; the row-access policy, the statement class, cost and the live switch are untouched either way |
| tenancy by deployment | one Spanner database per environment, one `.env` per environment, no `TenantId` column and none added; per-person isolation by `OwnerUserId` on every chat row and `UserId` on memories and feedback; `UserRoles.Scope` and `KnowledgeFiles.BusinessUnit` are the seams for business-unit scoping, both `''` | whether a business unit needs scoping before E3, and if so through those two seams (no schema change) rather than a tenant column |
| the 90-day `ChatEvents` policy | `002_chat.sql` gives `ChatEvents` `ROW DELETION POLICY (OLDER_THAN(Ts, INTERVAL 90 DAY))`; the page replays a turn from that table after a pod restart, and the Langfuse backfill rebuilds traces from it; a turn older than the window is messages and votes with no trace | keep 90 days, or extend the policy in `002` (an `ALTER TABLE ChatEvents REPLACE ROW DELETION POLICY (OLDER_THAN(Ts, INTERVAL <n> DAY))`, to be added as a numbered file; `scripts/spanner_ddl_check.py` reads only `DROP CONSTRAINT`, `ALTER COLUMN` and `ADD CONSTRAINT` today and reports any other `ALTER` form, so the lint learns this form first); the backfill does not extend it |
| the skills' frontmatter | the loader reads `runtime_loading: sectioned \| full_file_required`, `truncation_allowed`, `description`, `aliases` from the leading `---` block as a preference, never a gate: a pack loads whole when it fits the engine's whole-load budget and `SAHS_MAX_SKILL_CHARS`, and as a searchable library with the reason recorded when it does not; nothing is refused for size | mark the eight governed packs: `full_file_required` on the contract skills, `sectioned` on the frameworks, `aliases:` for the routing hint; `scripts/skills_check.py` reports what each file is read as and every warning; and decide whether the packs ship inside the build bundle (the build store already chunks a bundle) or as a per-pod directory |
| gateway context caching | on Vertex the prompt prefix through the skills block is kept byte-identical so the provider's implicit cache hits; on the gateway plane caching is unproven (`docs/research/resilience-accuracy-latency.md`, §3) | send the same prefix twice through `scripts/gateway_check.py` and read the cached-token count before assuming the discount; until then a whole-loaded large skill is paid in full on every turn on the gateway |
| the landing deviations that name the environment | `sahs/util/tls.py` imports the certificate package named by `SAHS_CA_PACKAGE`; `sahs/util/auth.py` reads the secret mount from `SAHS_SECRETS_DIR`; `sahs/constants.py` fills the three endpoint maps from `SAHS_*_ENDPOINT_E{1,2,3}`; `app.py` reads `SYNAPSE_HEALTH_ALIAS` | keep their literals (drop those hunks in section 2.4 and leave the four names unset) or take the environment-named form (set them in every profile) |
| the local door | the email-and-password routes answer only under `AUTH_LOCAL_LOGIN=1` | leave e1's door open for laptops on the dev database, or shut it there too once Okta is live everywhere, and remove the routes afterwards |
| the build's publish command | `docs/deploy.md` names `pipeline.py publish-build`; the code exposes `SpannerBuildStore.publish` only | add the subcommand or keep the Python one-liner of section 6, rung 4, in the runbook |

## 8. Checklist

- [ ] `wyla` added as a remote; the branch cut from `<their-base>`; `wyla/main` pinned at `0edaa0b` (or the descendant this run used)
- [ ] every ours-owned path checked out whole (section 2.3); `git diff --stat wyla/main -- <the ours paths>` prints nothing
- [ ] `sahs/util/spanner.py` gone; `sahs/util/spanner/settings.py` and `db/spanner/001_identity.sql` byte-identical to theirs; `authorization.py` theirs above the marker
- [ ] the fifteen theirs-owned files patched (section 2.4) and every conflict resolved by section 2.5; the landing deviations decided (section 7)
- [ ] nothing from section 2.2 in the tree: no root `docs/` file, no `MYSTIFY_REPORT.md`, `.claude/`, `archive/`, `kc-exploration/`, no harness report that names a worktree
- [ ] the four `.gitignore` lines for `env/`
- [ ] the image installs `identity`, `assistant`, `sql` (and `dev` where the suites run) plus `fastapi`, `uvicorn`, `httpx`, `langfuse`
- [ ] `make ddl-check` exit 0
- [ ] `make test` exit 0, both suites
- [ ] `env/e1.env`, `env/e2.env`, `env/e3.env` filled from the examples; secrets from the secret store; `AUTH_LOCAL_LOGIN` absent in e3
- [ ] `scripts/spanner_check.py` run on each database before any DDL; `005` applied only where `BuildBundles` is `missing`; `006`, `007`, `008` (and `009` if it exists) applied in order; `missing 0` after; `ChatSessions.Model` is `STRING(64)`
- [ ] the `MessageCount` backfill run where the first query returned rows
- [ ] `https://<host>/callback` registered on each environment's Okta client and Google client; `AUTH_GROUP_ROLE_MAP` maps at least one group to `admin`
- [ ] `make check ENV=e1`, `e2`, `e3` exit 0
- [ ] a build published and named by `/api/synapse/planes`
- [ ] a laptop signed in through Okta on e1; the Google connection made; one live query; one chat with a file and a skill replayed after a reload
- [ ] `langfuse_sync.py coverage` prints the table against e1
- [ ] the ladder repeated on e2 and e3
- [ ] the eight governed packs' frontmatter marked; `skills_check.py` exit 0 on the tree
- [ ] the sensitive-column policy, the `ChatEvents` window and gateway caching decided and written in their runbook
