# Sanitization scan: what the code since the landing would tell the enterprise reader about where it came from

A report only; nothing was edited. The scan covers every line **added**
under `apps/` and `synapse-agentic-harness-system/` between the landing
base `a87fd28` (the merge of PR #145) and `0edaa0b` (the merge of
PR #151, wyla `main` = `claude/production-ready`):
`git diff a87fd28..0edaa0b -- apps synapse-agentic-harness-system`,
27,865 added lines across 172 files. Each added line was searched for
the words and shapes the brief names; the findings are grouped by file,
with the line's text and a suggested replacement. Line numbers are the
file's at `0edaa0b`.

The companion is `docs/enterprise-carry-back.md`; the person carrying
the code back applies this list (a separate pass) before section 2.3 of
that document runs, or right after, in the enterprise branch.

## Counts

| category | added lines | files | verdict |
|---|---|---|---|
| `wyla` as a name (dataset, prompt and queue prefixes; worktree paths; the prose) | 32 | 8 | must change |
| "this repository is public" / "not in this repository" / "kept out of this public repository" | 10 | 5 | must change |
| "the enterprise branch's", "as written", "Theirs", "the enterprise settings loader" (provenance in docstrings and comments) | 8 | 5 | must change |
| worktree names, agent ids, `claude/…` branch names, commit hashes, "no PR", "the enterprise baseline files" in the three harness reports | 18 | 3 | must change (or the reports do not cross) |
| a PR number in a test docstring | 2 | 1 | must change |
| a person's name (`Saheb Singh`, negative assertions) | 2 | 2 | must change |
| **must change, total** | **72** | **17** (some files in several rows) | |
| "the owner" meaning the repository's owner | 3 | 2 | judgement call |
| `enterprise` as a plain adjective ("the enterprise front door", "the enterprise network names", "the enterprise hosts are configuration") | 21 | 12 | judgement call: keep, or "the deployment's" |
| `laptop` as a run mode (`SAHS_STORE=sqlite`, the local door, the `laptop` fixture) | 90 | 32 | keep; 8 are possessive ("the laptop's") and still mean the run mode; 0 say "the owner's laptop" or "the laptop test" |
| `corp` / `corporate` (`corporate_ca_bundle`, "corporate proxy", `DC=corp`, `@corp.example`) | 26 | 5 | keep: technical terms and `.example` fixtures |
| `Lumi` ("powered by Lumi", "Synapse by Lumi") | 3 | 3 | keep: the product's own brand word |
| `their team`, `the team's`, `carry back`, `port`/`ported` (in the repository sense), `PR #` in code, `session_`, `Co-Authored-By` inside a file, `bardbyte`, a real hostname, project id, tenant id or email domain other than `example`/`.example` | 0 | | none found |

Plus one block at the end: lines **outside the diff** (pre-existing at
`a87fd28`) in files that cross whole, which carry the same names and
must change in the same pass (a further 26 lines in 8 files), and the
list of root `docs/` files that must not cross.

Search terms used, case-insensitive where it matters: `wyla`,
`enterprise`, `their team`, `the team's`, `corp`, `corporate`, `laptop`
(and the possessive and "owner's laptop" forms), `public repository`,
`this repository is public`, `port`/`ported`/`carry back`, `PR #` and
`#1nn`, `claude`, `session_`, `Co-Authored`, `worktree`, `agent-<hex>`,
the branch names, seven-character hashes in prose, `Saheb`, `Singh`,
`Jane Doe`, `bardbyte`, `github.com`, `mystify`, every `https://` host,
every `@domain` email, every `<word>-<env>` id shape, `okta.com`
subdomains, `the owner`, `theirs`/`ours`/`as written`/`landed`.

## Must change before carry-back

### `synapse-agentic-harness-system/sahs/observe/datasets.py` (8 lines)

The dataset schema ids and Langfuse dataset names carry the repository
name. Rename the prefix once, product-wide (the same prefix is
pre-existing in `prompts.py`, `experiments.py`, `annotations.py` and
`tests/test_observe.py`, listed at the end); the suggested replacement
uses `synapse`, the product name the rest of the code already uses.

| line | text | replacement |
|---|---|---|
| 11 | `{"schema": "wyla.precedent/1" \| "wyla.silver/1" \| "wyla.scenario/1",` | `{"schema": "synapse.precedent/1" \| "synapse.silver/1" \| "synapse.scenario/1",` |
| 38 | `PRECEDENT_SCHEMA = "wyla.precedent/1"` | `PRECEDENT_SCHEMA = "synapse.precedent/1"` |
| 39 | `SILVER_SCHEMA = "wyla.silver/1"` | `SILVER_SCHEMA = "synapse.silver/1"` |
| 40 | `SCENARIO_SCHEMA = "wyla.scenario/1"` | `SCENARIO_SCHEMA = "synapse.scenario/1"` |
| 42 | `DATASET_NAMES = {PRECEDENT_SCHEMA: "wyla-precedents",` | `DATASET_NAMES = {PRECEDENT_SCHEMA: "synapse-precedents",` |
| 43 | `                 SILVER_SCHEMA: "wyla-silver",` | `                 SILVER_SCHEMA: "synapse-silver",` |
| 44 | `                 SCENARIO_SCHEMA: "wyla-scenarios"}` | `                 SCENARIO_SCHEMA: "synapse-scenarios"}` |
| 210 | `    name = name or DATASET_NAMES.get(str(items[0].get("schema")), "wyla-items")` | `… "synapse-items")` |

Note for whoever renames: the names are also rows in a Langfuse
instance (datasets, prompts, the annotation queue). A backfill after
the rename creates new objects under the new names; the old ones stay
until deleted. `tests/fixtures/precedents/precedents.jsonl` carries the
schema id in its three rows and changes with it.

### `synapse-agentic-harness-system/sahs/observe/prompts.py` (4 lines)

| line | text | replacement |
|---|---|---|
| 31 | `PART_PREFIX = "wyla-assistant"` | `PART_PREFIX = "synapse-assistant"` |
| 89 | `    for name, text in (("wyla-planner-system", PLAN_SYSTEM),` | `("synapse-planner-system", PLAN_SYSTEM),` |
| 90 | `                       ("wyla-judge-system", JUDGE_SYSTEM),` | `("synapse-judge-system", JUDGE_SYSTEM),` |
| 91 | `                       ("wyla-review-system", REVIEW_SYSTEM)):` | `("synapse-review-system", REVIEW_SYSTEM)):` |

### `synapse-agentic-harness-system/scripts/run_evals.py` (2 lines)

| line | text | replacement |
|---|---|---|
| 10 | `dataset builders wrote (wyla.precedent/1, wyla.silver/1,` | `dataset builders wrote (synapse.precedent/1, synapse.silver/1,` |
| 11 | `wyla.scenario/1: langfuse_sync.py datasets --build ...). The assistant` | `synapse.scenario/1: langfuse_sync.py datasets --build ...). The assistant` |

### `synapse-agentic-harness-system/tests/test_langfuse_insight.py` (11 lines)

Every one follows the rename above.

| line | text | replacement |
|---|---|---|
| 462 | `    assert dataset_name(path) == "wyla-precedents"` | `"synapse-precedents"` |
| 468 | `    assert set(client.datasets) == {"wyla-precedents"}` | `{"synapse-precedents"}` |
| 469 | `    assert client.items["wyla-precedents"]["prec_declines_per_day"][` | `client.items["synapse-precedents"]…` |
| 571 | `    assert {"wyla-assistant-identity", "wyla-assistant-chain",` | `{"synapse-assistant-identity", "synapse-assistant-chain",` |
| 572 | `            "wyla-assistant-mode-chat", "wyla-assistant-mode-autopilot",` | `"synapse-assistant-mode-chat", "synapse-assistant-mode-autopilot",` |
| 573 | `            "wyla-assistant-style-gemini-3", "wyla-planner-system",` | `"synapse-assistant-style-gemini-3", "synapse-planner-system",` |
| 574 | `            "wyla-judge-system", "wyla-review-system"} <= set(names)` | `"synapse-judge-system", "synapse-review-system"} <= set(names)` |
| 575 | `    assert names["wyla-assistant-style-gemini-3"]["labels"] == \` | `names["synapse-assistant-style-gemini-3"]…` |
| 577 | `    assert names["wyla-planner-system"]["version"].startswith("text-")` | `names["synapse-planner-system"]…` |
| 578 | `    assert "{{style}}" in names["wyla-assistant-system"]["prompt"]` | `names["synapse-assistant-system"]…` |
| 583 | `    stored = client.store["wyla-assistant-style-gemini-3"][0]["labels"]` | `client.store["synapse-assistant-style-gemini-3"]…` |

### `synapse-agentic-harness-system/docs/runbooks/langfuse-insight.md` (2 lines)

| line | text | replacement |
|---|---|---|
| 110 | `├─ model call 1   (generation)       input: system (once) + contents · usage: the budget-tick delta · prompt: wyla-assistant-system vN` | `… prompt: synapse-assistant-system vN` |
| 271 | ``   `ambiguous` and lands on the `wyla-ambiguous` annotation queue`` | ``… the `synapse-ambiguous` annotation queue`` |

### `synapse-agentic-harness-system/.env.example` (3 lines)

| line | text | replacement |
|---|---|---|
| 215 | `# The two enterprise hosts are not in this repository: both are required` | `# The two hosts are configuration, never source: both are required` |
| 285 | `# ── Enterprise network names, kept out of this public repository ──` | `# ── Network names, from the environment ──` |
| 286 | `# The enterprise build hard-codes these; here each comes from the environment.` | `# Each comes from the environment; nothing here is a literal.` |

(Line 291, `# SAHS_OAUTH_TOKEN_ENDPOINT_E1=           # (and _E2, _E3) the enterprise OAuth token endpoint`, is a plain adjective: judgement, below.)

### `synapse-agentic-harness-system/sahs/constants.py` (2 lines)

| line | text | replacement |
|---|---|---|
| 3 | `The enterprise build keeps its private endpoints in this module as` | `The private endpoints are not literals in this module: the three` |
| 4 | `literals. This repository is public, so the same three maps are filled` | `maps are filled` |

(so the docstring reads "…the three maps are filled from the environment instead, one variable per plane and environment:").

### `synapse-agentic-harness-system/sahs/util/auth.py` (3 lines)

| line | text | replacement |
|---|---|---|
| 107 | ``    caller says ``override=True`` (the enterprise settings loader does,`` | ``    caller says ``override=True`` (``sahs/util/spanner/settings.py`` does,`` |
| 373 | `    SAHS_SECRETS_DIR. The enterprise build hard-codes its mount path;` | `    SAHS_SECRETS_DIR, the secret mount holding key.json and ca-bundle.crt;` |
| 374 | `    this repository is public and does not."""` | `    unset means no mount is consulted."""` |

### `synapse-agentic-harness-system/sahs/util/tls.py` (2 lines)

| line | text | replacement |
|---|---|---|
| 3 | `The enterprise build knows its certificate package and secret mount by` | `The certificate package and the secret mount are named by the` |
| 4 | `name. This repository is public, so both are named by the environment:` | `environment, and neither is consulted when unset:` |

### `synapse-agentic-harness-system/sahs/enrich/gateway_client.py` (1 line)

| line | text | replacement |
|---|---|---|
| 95 | `                           + " in the silo .env: the enterprise hosts are not in this repository")` | `+ " in the silo .env: the hosts are configuration, never source")` |

This string reaches a person as an error message.

### `synapse-agentic-harness-system/sahs/identity/authorization.py` (1 line)

| line | text | replacement |
|---|---|---|
| 57 | `# (the block above is the enterprise branch's file as written; the` | `# (the block above is the authorization module as the identity service defines it; the` |

### `synapse-agentic-harness-system/sahs/spanner.py` (3 lines)

| line | text | replacement |
|---|---|---|
| 3 | ``The enterprise branch's module, ``sahs/util/spanner/settings.py``, is`` | ``The settings module, ``sahs/util/spanner/settings.py``, is`` |
| 4 | `the base and lands in this repository as written. This facade is what` | `the base and is never edited here. This facade is what` |
| 110 | ``    a bare ``host:port`` (the emulator) passes through unchanged. Theirs`` | ``    a bare ``host:port`` (the emulator) passes through unchanged. ``settings.py``'s`` |

### `synapse-agentic-harness-system/sahs/util/spanner/__init__.py` (2 lines)

| line | text | replacement |
|---|---|---|
| 25 | ``This module became the package's ``__init__`` when the enterprise`` | ``This module became the package's ``__init__`` so that`` |
| 26 | ``branch's ``sahs/util/spanner/settings.py`` arrived: the same names,`` | ``    ``sahs/util/spanner/settings.py`` has a package: the same names,`` |

### `synapse-agentic-harness-system/tests/test_spanner_ddl.py` (1 line)

| line | text | replacement |
|---|---|---|
| 33 | `    # the enterprise branch's, whose editor pads columns differently` | `    # hand-aligned, and an editor may pad columns differently` |

### `apps/synapse_admin/tests/test_synapse_surface.py` (2 lines)

| line | text | replacement |
|---|---|---|
| 620 | `    pages; the assistant is Radix on both (the owner's call after PR` | `    pages; the assistant is Radix on both (a naming decision: the admin` |
| 621 | `    #145, which had named it only here); the disclaimer stays on this` | `    console had said Synapse); the disclaimer stays on this` |

### `apps/synapse_admin/tests/test_signin_surface.py` (1 line)

| line | text | replacement |
|---|---|---|
| 31 | `        assert "Saheb Singh" not in index                      # no shipped identity` | `        assert "<Your Name>" not in index                      # no shipped identity` (the placeholder `local.env.example` uses; the assertion keeps its meaning once `.env.example` line 77 is changed the same way, below) |

### `apps/synapse_admin/tests/test_synapse_admin_app.py` (1 line)

| line | text | replacement |
|---|---|---|
| 64 | `    assert "Saheb Singh" not in page.text` | `    assert "<Your Name>" not in page.text` |

### `synapse-agentic-harness-system/docs/reports/langfuse-insight.md` (5 lines)

| line | text | replacement |
|---|---|---|
| 3 | ``Branch `worktree-agent-a23cc6b840cde6438`, worktree`` | drop lines 3 to 8; open with "Commits: the mirror change and the insight change, plus this report." or drop the report |
| 4 | ``/home/user/wyla/.claude/worktrees/agent-a23cc6b840cde6438`, fast-forwarded`` | drop |
| 5 | ``onto `claude/production-ready` (`33fade0`) before the first change, since the`` | drop |
| 8 | ``Commits: `14e7af6`, `f97fba5`, plus this report. Nothing pushed, no PR.`` | drop |
| 41 | ``No new environment variable; `.env.example` untouched. No model identifier in code or commits. The enterprise baseline files were not touched. `sahs/loop/skills.py`, …`` | ``No new environment variable; `.env.example` untouched. No model identifier in code. `sahs/loop/skills.py`, …`` |
| 25 | ``… `dataset_name()` names an item file by its schema (`wyla-precedents`, `wyla-silver`, `wyla-scenarios`); …`` | follows the rename: `synapse-precedents`, `synapse-silver`, `synapse-scenarios` |

### `synapse-agentic-harness-system/docs/reports/multi-task-turns.md` (4 lines)

| line | text | replacement |
|---|---|---|
| 3 | ``Branch: `worktree-agent-a866e6dd37820f3a4` `` | drop lines 3 to 7; open with "One commit: multi-task turns." |
| 4 | ``Worktree: `/home/user/wyla/.claude/worktrees/agent-a866e6dd37820f3a4` `` | drop |
| 5 | ``Feature commit: `d256f1f` ("Multi-task turns: a compound ask becomes`` | drop |
| 7 | `report"). Not pushed; no PR.` | drop |

### `synapse-agentic-harness-system/docs/reports/skill-retrieval.md` (9 lines)

| line | text | replacement |
|---|---|---|
| 3 | ``Branch: `worktree-agent-a088816e83c8240ea` `` | drop lines 3 to 9; open with "Two commits: the library mode, then the loading policy." |
| 4 | ``Worktree: `/home/user/wyla/.claude/worktrees/agent-a088816e83c8240ea` `` | drop |
| 5 | `Commits on the branch (nothing pushed, no PR):` | drop |
| 7 | ``- `231218a` — Skill retrieval: a pack over the ceiling loads as a`` | `- Skill retrieval: a pack over the ceiling loads as a` |
| 9 | ``- `b74ad72` — Skill loading policy: whole-load per engine window,`` | `- Skill loading policy: whole-load per engine window,` |
| 262 | ``Branch: `worktree-agent-a67c6daaf6a665657` `` | drop lines 262 to 265; open the section with "One commit, plus this section." |
| 263 | ``Worktree: `/home/user/wyla/.claude/worktrees/agent-a67c6daaf6a665657` `` | drop |
| 264 | ``(branched from `claude/production-ready` at `44a268e`; nothing pushed,`` | drop |
| 265 | ``no PR). Commits: `e500e74` — the change; this section as a second`` | `The change, and this section as a second` |

Line 268, `The repo owner's decision: there is no refusal anywhere in skill`,
is a judgement call (below). The simplest course for all three reports
is not to cross them (`docs/enterprise-carry-back.md`, section 2.2);
the product documentation they duplicate (`docs/skill-retrieval.md`,
`docs/multi-task-turns.md`, `docs/runbooks/langfuse-insight.md`) is
clean apart from the two runbook lines above.

## Judgement calls

### "the owner" meaning the repository's owner (3 lines)

| file | line | text | suggestion |
|---|---|---|---|
| `synapse-agentic-harness-system/tests/test_skills_check.py` | 46 | `    """A skills tree the way the owner keeps one (nested folders, odd` | `"""A skills tree the way a team keeps one (nested folders, odd` |
| `synapse-agentic-harness-system/tests/test_skills_check.py` | 262 | `    # and as a subprocess, the way the owner runs it` | `# and as a subprocess, the way it is run from a shell` |
| `synapse-agentic-harness-system/docs/reports/skill-retrieval.md` | 268 | `The repo owner's decision: there is no refusal anywhere in skill` | `The decision: there is no refusal anywhere in skill` |

Every other "the owner" / "the owner's" in the diff (29 lines) means the
row's owner (`OwnerUserId`) and stays.

### `enterprise` as a plain adjective (21 lines, 12 files)

In the enterprise repository these read as a description of their own
deployment and can stay; "the deployment's" is the neutral form if the
word is to go entirely. None of them says the code came from elsewhere.

| file | line | text | suggestion |
|---|---|---|---|
| `apps/synapse_admin/backend/auth.py` | 249 | `    break-glass path; the enterprise front door is Okta, so they refuse` | keep, or "the deployment's front door is Okta" |
| `apps/synapse_admin/backend/okta.py` | 1 | `"""Okta sign-in: the front door of the enterprise deployment.` | keep, or "the front door of the deployment." |
| `synapse-agentic-harness-system/sahs/spanner.py` | 175 | `    # the email-and-password path (sign-up, login, reset): the enterprise` | keep, or "the deployment's" |
| `synapse-agentic-harness-system/.env.example` | 291 | `# SAHS_OAUTH_TOKEN_ENDPOINT_E1=           # (and _E2, _E3) the enterprise OAuth token endpoint` | keep, or "the per-environment OAuth token endpoint" |
| `synapse-agentic-harness-system/env/e1.env.example` | 52, 73 | `SAHS_OAUTH_TOKEN_ENDPOINT_E1=https://<enterprise-oauth-token-endpoint>`; `# ── the enterprise network names ──` | keep, or `<oauth-token-endpoint>` and "the network names" |
| `synapse-agentic-harness-system/env/e2.env.example` | 50, 71 | the same two lines for e2 | the same |
| `synapse-agentic-harness-system/env/e3.env.example` | 51, 74 | the same two lines for e3 | the same |
| `synapse-agentic-harness-system/sahs/util/gateway.py` | 44 | `# the two enterprise hosts are configuration, never source: the token` | keep, or "the two hosts" |
| `synapse-agentic-harness-system/sahs/util/network.py` | 37, 42, 47 | `"""The enterprise Vertex endpoint selected by EPAAS_ENV/e1-e3."""` and the OAuth and Spanner twins | theirs, as landed: keep |
| `synapse-agentic-harness-system/sahs/util/profiles.py` | 18 | `here names an enterprise host: models, levels and caps only.` | keep, or "names a host" |
| `synapse-agentic-harness-system/tests/test_gateway_check.py` | 18 | `# the enterprise hosts are configuration, never source: the tests name` | keep, or "the hosts" |
| `synapse-agentic-harness-system/tests/test_gateway_plane.py` | 25, 370 | the same comment, and `# (the enterprise hosts are configuration: the .env names them)` | keep, or "the hosts" |
| `synapse-agentic-harness-system/docs/runbooks/pipeline_end_to_end.md` | 375 | ``behind the enterprise gateway host (`GATEWAY_BASE_URL`). Before any of it enters the program, prove`` | keep |

### `laptop` as a run mode (90 lines, 32 files): keep

Every occurrence describes the `local`/`sqlite` run mode, the local
sign-in door, a test fixture named `laptop`, or where a command is run
from. None says "the owner's laptop", "their laptop", "my laptop" or
"the laptop test". The eight possessive forms, listed so the reader can
judge, all still mean the run mode:

| file | line | text | verdict |
|---|---|---|---|
| `apps/synapse_admin/backend/chat.py` | 94 | `        # None keeps the laptop's SYNAPSE_USER_NAME` | keep |
| `apps/synapse_admin/backend/okta.py` | 96 | `    under Spanner). Each refusal names its variable, so the laptop's` | keep |
| `apps/synapse_admin/tests/test_local_login.py` | 1 | `"""The laptop's front door and where its rows land.` | keep |
| `synapse-agentic-harness-system/docs/model-playbook.md` | 16 | `… The laptop's only engine. \|` | keep |
| `synapse-agentic-harness-system/docs/model-playbook.md` | 34 | ``the laptop's `gateway_check.py --all-models` run. The `--levels` probe`` | keep, or "a laptop's" |
| `synapse-agentic-harness-system/docs/spanner_schema.md` | 668 | `laptop's directory seam assigns them, `Status`, `Version`),` | keep |
| `synapse-agentic-harness-system/sahs/builds/resolve.py` | 6 | ``    ``MERIDIAN_BUILDS_DIR`` — the laptop's own ``pipeline.py compile`` `` | theirs, as landed: keep |
| `synapse-agentic-harness-system/sahs/util/profiles.py` | 15 | ``what the laptop's ``gateway_check.py --all-models`` proved; the`` | keep, or "what a laptop's" |

Counts per file: `apps/synapse_admin/tests/test_local_login.py` 19,
`apps/synapse_admin/tests/test_content_store_app.py` 15,
`tests/test_readiness.py` 5, `env/local.env.example` 5,
`docs/model-playbook.md` 4, `apps/synapse_admin/README.md` 4,
`docs/runbooks/langfuse-insight.md` 3, `apps/synapse_admin/backend/auth.py` 3,
and 2 or 1 in each of 24 other files.

### `corp` / `corporate` (26 lines, 5 files): keep

`sahs/util/tls.py` (`corporate_ca_bundle`, "Corporate TLS trust
helpers"; theirs as landed, with the env-named deviation),
`sahs/util/auth.py` (the import and call), `sahs/util/network.py`
("the corporate proxy"; theirs), `apps/synapse_admin/scripts/identity_map.py`
("a managed corporate account"), `apps/synapse_admin/tests/test_authcheck_scripts.py`
(`DC=corp`, `@corp.example` fixtures) and `tests/test_identity_settings.py`
(`@corp.org` as an allowed-domain fixture). All technical or `.example`.

### `Lumi` (3 lines): keep

`apps/synapse/frontend/js/pages/signin.js` 59 and
`apps/synapse_admin/frontend/js/pages/signin.js` 59 (`powered by Lumi`),
`apps/synapse_admin/tests/test_synapse_surface.py` 647 (`Synapse by Lumi`).
The product's own brand word, kept by decision in the earlier
sanitization record.

### Hostnames, emails, ids: none real

Every `https://` host in the diff is `localhost`, `127.0.0.1`,
`testserver`, a `<placeholder>`, an `.example` name
(`identity.example`, `app.example`, `okta.example`, `bq.example`,
`org.okta.com/oauth2/as1` in a fixture) or a public documentation URL.
Every email is `@example.com`, `@corp.example`, `@other.org`, `a@b.co`
or `bo@x.io` in a test fixture. Every id of the shape `<word>-e1` is a
`<placeholder>` in an env example. No project id, tenant id, Okta org
or email domain of a real organisation appears.

## Outside the diff, in files that cross whole (26 lines, 8 files)

These lines pre-date `a87fd28` and are not in the scan's scope, but the
files cross by checkout and carry the same names; the same pass fixes
them.

| file | lines | what | replacement |
|---|---|---|---|
| `synapse-agentic-harness-system/.env.example` | 76, 77 | `# Saheb Singh"); the account block in the app shows the same name:` / `# SYNAPSE_USER_NAME=Saheb Singh` | `# <Your Name>"); …` / `# SYNAPSE_USER_NAME=<Your Name>` (the two tests above follow) |
| `synapse-agentic-harness-system/sahs/observe/prompts.py` | 26, 27 | `ASSISTANT_PROMPT = "wyla-assistant-system"`, `LOOP_PROMPT = "wyla-loop-system"` | `synapse-…` |
| `synapse-agentic-harness-system/sahs/observe/experiments.py` | 28 | `DATASET_PREFIX = "wyla"` | `"synapse"` |
| `synapse-agentic-harness-system/sahs/observe/annotations.py` | 5, 19 | ``on the ``wyla-ambiguous`` queue``, `QUEUE_NAME = "wyla-ambiguous"` | `synapse-ambiguous` |
| `synapse-agentic-harness-system/tests/test_observe.py` | 296, 298, 379, 380, 461, 462, 475, 484, 486, 489, 495, 501, 508, 515 | `wyla-curated`, `wyla-capability-matrix`, `wyla-assistant-system`, `wyla-loop-system` | follow the rename |
| `synapse-agentic-harness-system/docs/runbooks/langfuse.md` | 114 | ``every ambiguous trial's trace goes on the `wyla-ambiguous` annotation`` | `synapse-ambiguous` |
| `synapse-agentic-harness-system/docs/runbooks/pipeline_end_to_end.md` | 96 | `git clone <repo> && cd wyla` | `git clone <repo> && cd <repo>` |
| `apps/synapse_admin/design/wireframes/github.md` | 1 | `repo: bardbyte/wyla` | drop the line, or the file (a wireframe note; not under the checkout list) |

## Files under `docs/` that must not cross

Root `docs/` files that name wyla, the port, the PR numbers, the
worktrees or the agent, and therefore stay on this side (the carry-back
document's section 2.2 repeats this):

| file | names |
|---|---|
| `docs/enterprise-port.md` | wyla (8), the port, PR #146, the enterprise branch |
| `docs/enterprise-carry-back.md` | this pass's companion: wyla, the PRs, the merge commits |
| `docs/deploy.md` | links `enterprise-port.md` (line 7); otherwise product content worth rewriting for their wiki |
| `docs/spanner-wiring.md` | clean of the names; crosses only if rewritten without the link from `deploy.md` and `db/spanner/README.md`'s "at the repository root" reference resolves there |
| `docs/CHANGELOG-2026-09.md` | PR #140 to #151, the branch names, "the team" |
| `docs/README.md` | links the port doc, names the enterprise port |
| `docs/reports/production-ready.md`, `storage-foundation.md`, `content-to-spanner.md`, `ui-fixes-stop-radix.md`, `backend-fixes-sensitive-model-skills.md`, `sanitization-scan.md` | worktree paths, branch names, PR numbers, wyla, "the owner" |
| `docs/research/resilience-accuracy-latency.md` | PR numbers |
| `docs/wiki/README.md`, `docs/wiki/*` | the repository name; the wiki is this repository's |
| `docs/paper/compiling-a-semantic-layer.md`, `docs/architecture/README.md`, `docs/architecture/end-to-end.svg` | the repository name |
| `docs/presentation/*`, `docs/brand/*`, `docs/design_inventory.md` | this repository's story deck and mark |
| `README.md`, `MYSTIFY_REPORT.md` | the repository name and the sanitizer's record |

And the three harness reports (`synapse-agentic-harness-system/docs/reports/*`),
unless the lines above are applied first.

## Applied

The must-change list above was applied on the integration branch at
`9a242e0` (three workstreams had merged after the scan: the searches
were re-run over the whole of `apps/` and
`synapse-agentic-harness-system/`, not only the scan's diff, and the
new root reports were included). Logic is unchanged everywhere: only
comments, docstrings, docs, example files, test strings and the
Langfuse name constants moved. Both suites exit 0 after the edits
(`apps/synapse_admin/tests`: 186 passed, 2 skipped;
`synapse-agentic-harness-system/tests`: all passed).

### Files changed

The rename, `wyla-` → `synapse-` and `wyla.*/1` → `synapse.*/1`, product-wide:

- `synapse-agentic-harness-system/sahs/observe/datasets.py` (the three schema ids, `DATASET_NAMES`, the `synapse-items` fallback)
- `synapse-agentic-harness-system/sahs/observe/prompts.py` (`ASSISTANT_PROMPT`, `LOOP_PROMPT`, `PART_PREFIX`, the planner/judge/review names)
- `synapse-agentic-harness-system/sahs/observe/experiments.py` (`DATASET_PREFIX`)
- `synapse-agentic-harness-system/sahs/observe/annotations.py` (`QUEUE_NAME` and its docstring)
- `synapse-agentic-harness-system/sahs/observe/__init__.py` ("Wyla already writes" → "Synapse already writes")
- `synapse-agentic-harness-system/scripts/run_evals.py`
- `synapse-agentic-harness-system/tests/test_observe.py`, `tests/test_langfuse_insight.py`
- `synapse-agentic-harness-system/docs/runbooks/langfuse.md` (the queue name; "Wyla writes" → "Synapse writes"), `docs/runbooks/langfuse-insight.md`
- `synapse-agentic-harness-system/docs/runbooks/pipeline_end_to_end.md` (`cd wyla` → `cd <repo>`)
- `apps/synapse_admin/design/wireframes/github.md` (the `repo: bardbyte/wyla` line dropped)

Provenance in comments, docstrings and strings, reworded as configuration statements:

- `synapse-agentic-harness-system/sahs/constants.py`
- `synapse-agentic-harness-system/sahs/util/auth.py` (the `load_dotenv` and `_secret_mount_candidates` docstrings)
- `synapse-agentic-harness-system/sahs/util/tls.py`
- `synapse-agentic-harness-system/sahs/enrich/gateway_client.py` (the error string: "the hosts are configuration, never source")
- `synapse-agentic-harness-system/sahs/identity/authorization.py` (the comment below the marker only)
- `synapse-agentic-harness-system/sahs/spanner.py` (the module docstring and the `grpc_endpoint` docstring: "the settings module", never a branch)
- `synapse-agentic-harness-system/sahs/util/spanner/__init__.py`
- `synapse-agentic-harness-system/tests/test_spanner_ddl.py`
- `synapse-agentic-harness-system/.env.example` (the three flagged lines, and the example `SYNAPSE_USER_NAME` → `John Doe`; `SYNAPSE_USER_MANAGER` was already `Jane Doe`)

The person's name, the PR numbers and "the owner's call":

- `apps/synapse_admin/tests/test_synapse_surface.py` (the docstring states the behaviour)
- `apps/synapse_admin/tests/test_signin_surface.py`, `tests/test_synapse_admin_app.py` (each defines `EXAMPLE_NAME = "John Doe"`, the `.env.example` placeholder, and asserts it is not shipped)
- `synapse-agentic-harness-system/tests/test_authoring.py`, `tests/test_reviews.py`, `tests/test_v3_projects.py` (the fixture user is `John Doe` / `john-doe`; the manager fixture stays `Jane Doe`)
- `synapse-agentic-harness-system/docs/specs/synapse_v3_harness.md`, `docs/spanner_schema.md` (the example person)
- `synapse-agentic-harness-system/docs/runbooks/b1_enrich.md` (a PR number dropped)

The reports: worktree paths, agent ids and the branch name removed, commit hashes kept:

- `synapse-agentic-harness-system/docs/reports/langfuse-insight.md` (also "the enterprise baseline files were not touched" dropped, and the dataset names follow the rename), `multi-task-turns.md`, `skill-retrieval.md`
- `docs/reports/auth-e2e.md`, `visualizations.md`, `chat-ux-usage.md`, `backend-fixes-sensitive-model-skills.md`, `content-to-spanner.md`, `ui-fixes-stop-radix.md`, `storage-foundation.md`, `production-ready.md` (`claude/production-ready` → "the integration branch"; content otherwise as it was)

Not touched, by the rules: `sahs/util/spanner/settings.py`, `authorization.py` above its marker, `db/spanner/001_identity.sql`, every judgement-call line, and the body of this report.

### The searches after, over `apps/` and `synapse-agentic-harness-system/`

| pattern | hits |
|---|---|
| `wyla` (any case) | 0 |
| "this repository is public" | 0 |
| "the enterprise branch" | 0 |
| "as written" as provenance | 0 (the 13 remaining lines are ordinary English: "stored as written", "the file as written", "a query not accepted as written") |
| "Theirs" as provenance | 0 (`sahs/spanner.py` keeps its `_theirs` import alias, which is code, not a comment; the lowercase pronoun remains in tests and docs about a person's own rows) |
| "the enterprise settings loader" | 0 |
| "owner's call" | 0 |
| "PR #" | 0 |
| the example person's name (`Saheb`, `Singh`) | 0 |
| `worktrees/agent-`, `worktree-agent`, `agent-<hex>` | 0 |
| `claude/production-ready` | 0 |
| "public repository", "not in this repository", "enterprise build", "enterprise baseline" | 0 |
| `bardbyte` | 0 |

The same searches over the root `docs/reports/*.md` (which do not cross;
rule 5 only removed worktree paths, agent ids and the branch name there):
`worktrees/agent-` 0, `agent-<hex>` 0, `claude/production-ready` 0; the PR
numbers, "nothing pushed, no PR", one "the enterprise branch is
reconciled" (`auth-e2e.md`) and one "the worktree's" (`backend-fixes-…`,
a word, not a path) remain as report content. This report's own body
keeps its findings verbatim.

Judgement-call counts remaining, whole trees (`apps/` and the silo, every
file, not only the scan's diff): `enterprise` as an adjective 53 lines in
27 files; "the owner" in the repository sense 3 lines
(`tests/test_skills_check.py` 46 and 262, `docs/reports/skill-retrieval.md`
"The repo owner's decision"); `laptop` 249 lines; `corp`/`corporate` 157
lines; `Lumi` 69 lines. All left as the scan judged them.

### Left for the owner

A scope addition arrived after the must-change list was applied: no
"paste it back"-style notes and no AI or agent mention anywhere under the
two trees, plus a paragraph in `docs/enterprise-carry-back.md` on how
the commit-message trailers are kept from crossing. The permission
system refused the edit, so none of it was applied. Its grep
(`claude|anthropic|paste (it )?back|co-authored|generated with|session_0|worktrees?/agent|agent-[0-9a-f]{6,}|bardbyte|wyla|saheb|singh|zilla`)
finds exactly one line after this pass:
`synapse-agentic-harness-system/scripts/gateway_check.py:130`, the
printed string `(paste it back)`. The same style, not matched by that
grep, is in `docs/runbooks/e21_intelligence.md` (lines 33–36, 43, 96:
"travels by PASTE", "copy the output into the session", "Paste the
report back", "arrives by paste") and `docs/runbooks/pipeline_end_to_end.md`
(lines 188, 193, 370: "PASTE the transcript back", "PASTE … back into
the session", "Paste the doctor's output"). These, and the carry-back
paragraph, are left for the owner to apply.
