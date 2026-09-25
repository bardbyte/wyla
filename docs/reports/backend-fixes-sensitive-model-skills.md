# Backend fixes from the laptop test: sensitive columns, the model choice, the skills tree

Branch `worktree-agent-a8f40679367274121`, worktree
`/home/user/wyla/.claude/worktrees/agent-a8f40679367274121`. The
worktree was created at `c62cfc6` (PR #149) while the local
`claude/production-ready` ref was 25 commits ahead at `a33db96` and held
every file the brief names (`env/*.env.example`, `docs/deploy.md`,
`docs/skill-retrieval.md`, `007_content.sql`); the branch was
fast-forwarded onto `a33db96` first (`git merge --ff-only`, no merge
commit, nothing of the worktree's lost), then two commits on top, not
pushed, no PR: `c26dfb6` (the fixes) and the report commit.

The task: three fixes from the owner's laptop test — the harness may
read every column the build knows, a 3.x model choice is no longer a
422, and the eight governed packs under `MERIDIAN_SKILLS_DIR` show in
the chat's skill picker — with the DDL, the docs and the tests to match.
All three are done; both suites exit 0; the DDL lint is ok.

## What changed, file by file

### 1. The sensitive-column policy (`SAHS_SENSITIVE_COLUMNS`)

* `synapse-agentic-harness-system/sahs/tools/validate_sql.py` — the
  switch is read here, where the verdict is made: `SENSITIVE_SWITCH`,
  `sensitive_policy(env)` (`allow` unless the value is the word `deny`,
  case-insensitive; unset is `allow`), `sensitive_policy_note(env)` (one
  line for a doctor or a report), `SENSITIVE_CODES`. `validate_sql`
  takes a keyword `env` (the process environment when `None`). Under
  `allow` the `sensitive_column` and `select_star_over_sensitive`
  entries go to `warnings` instead of `violations`, keeping their code,
  the column's name and the `union_most_restrictive` detail, with
  ` — read allowed (SAHS_SENSITIVE_COLUMNS=allow), noted on the record`
  appended, a hint that says the read stands, and a `policy` key
  (`allow` or `deny`) on the entry either way. Under `deny` they are the
  violations they were, word for word. `sensitive_column_in_filter`,
  `policy_unknown`, `restricted_table` and every other check are
  untouched. The module docstring names the policy. The sandbox
  (`sahs/tools/sandbox.py`) was not changed: its gates are tables, cost
  and the live switch, none of them a column, so there was nothing for
  the switch to decide there.
* How the note reaches the turn: `sahs/loop/tools.py` `run_sql` and
  `sahs/assistant/kit.py` `_run_live` already return
  `verdict["warnings"]` on a success, so the tool result the model sees
  and the trace records carry the code, the column and `policy: allow`.
  Neither file needed a change.
* `synapse-agentic-harness-system/.env.example` — the variable with the
  two values explained, under the Ask block beside `SAHS_ALLOW_LIVE`.
* `synapse-agentic-harness-system/env/local.env.example`,
  `e1.env.example`, `e2.env.example` — `SAHS_SENSITIVE_COLUMNS=allow`
  after `SAHS_LIVE_MAX_BYTES`, one comment line; `e3.env.example` — the
  same with the comment that the deployment may set `deny`.
  `tests/test_readiness.py` holds every profile variable to be
  documented in `.env.example`, which it is.
* `docs/deploy.md` — a section "The first launch's policy switch:
  sensitive columns" (the two values, where it is read, what it does
  not touch).
* `docs/wiki/07-serving.md`, `docs/wiki/13-configuration.md` — the
  violation table's two codes explained as policy-dependent; a row in
  the execution-limits table.

### 2. The model choice: 64 characters end to end, and `008`

* `apps/synapse_admin/backend/chat.py` — `NewMessage.model`,
  `DraftRequest.model` and `SessionModel.model` are `max_length=64`
  (the brief also named `RunProposal`; it has no `model` field, and
  none was added). The comments say what the field holds. Nothing else
  in the file changed.
* `synapse-agentic-harness-system/sahs/assistant/spanner_store.py` —
  `MODEL_CHOICE_CHARS = 64`; `set_model` no longer cuts to 16: it
  strips and lowers, stores the choice whole, and raises `ValueError`
  by name for anything over 64 (the runtime's `choice_for` validates
  the choice against the catalog before it gets here, so the store
  needs no catalog of its own). The sqlite stand-in schema
  (`CHAT_SQLITE_SCHEMA`) carries `Model TEXT NOT NULL DEFAULT '' CHECK
  (length(Model) <= 64)`.
* `synapse-agentic-harness-system/db/spanner/008_chat_model.sql` (new)
  — four `ALTER TABLE` statements: `ChatSessions DROP CONSTRAINT
  ck_sessions_model`; `ChatSessions ALTER COLUMN Model STRING(64) NOT
  NULL DEFAULT ('')`; `ChatArtifacts DROP CONSTRAINT ck_artifacts_type`;
  `ChatArtifacts ADD CONSTRAINT ck_artifacts_type CHECK (Type IN
  ('chart', 'table', 'document', 'kpi', 'dashboard', 'diagram'))` —
  exactly `sahs/assistant/artifacts.py` `TYPES`. `query`, which `002`
  listed and nothing writes (`TYPES` never had it), is gone from the
  list. The header says why and what it applies on top of.
* `synapse-agentic-harness-system/scripts/spanner_ddl_check.py` —
  rewritten around a `load(files) -> (tables, findings)` that applies
  every statement in file order. `Table` now carries named
  `constraints` (parsed from the `CONSTRAINT name …` parts of a CREATE
  TABLE). `_apply_alter` reads the three forms — `DROP CONSTRAINT`
  (must exist), `ALTER COLUMN` (must exist; the type is re-parsed with
  the column regex), `ADD CONSTRAINT` (name must be free; a FOREIGN KEY
  is checked like a CREATE's) — on a table defined earlier, and any
  other `ALTER` form is a finding, never silently skipped (before this
  change an ALTER statement fell through unread). `check_list_in`
  replaces the body regex, so the graph registry checks read the
  constraints as they stand after the ALTERs. Two chat-half checks were
  added beside the graph ones: `ChatArtifacts.ck_artifacts_type` equals
  `sahs.assistant.artifacts.TYPES`, and `ChatSessions.Model` is
  `STRING(MODEL_CHOICE_CHARS)` with no constraint listing `Model IN`.
  `ddl_files()` is the shared file list. Output:
  `ok: 44 tables across 8 files; keys, interleaves, foreign keys,
  indexes, policies, the property graph, the ALTERs and the registries
  agree`.
* `synapse-agentic-harness-system/db/spanner/README.md` — `008` in the
  order line, the table (a row saying it is ALTERs only and what it
  changes), the apply paragraph (on a database with `001`…`006`: `007`
  then `008`; with `007`: `008` alone; a fresh database and E1 end up
  the same) and the lint paragraph (the ALTERs, the chat registries).
* `docs/spanner-wiring.md` — "Schema notes for the first rollout" now
  says both caveats are closed by `008` and how (the widened column, the
  dropped plane CHECK, the runtime validating against the catalog, the
  store writing whole, the API's 64, the sqlite CHECK, the artifact
  types held equal by the lint).
* `docs/deploy.md` — the DDL list names `007_content.sql` and
  `008_chat_model.sql`; day-one step 3 says `001` … `008` and what to
  apply on a database that already has some (and that `008` adds no
  table, so `spanner_check.py`'s table listing does not show it).

### 3. One skills tree for both shelves

* `synapse-agentic-harness-system/sahs/assistant/skills_loader.py` —
  `Pack.path` (the file a pack came from; `''` for a store row).
  `SKILLS_DIR_VAR = "MERIDIAN_SKILLS_DIR"`, `skills_dir(env)`,
  `pack_name(rel)` (the relative path as a slug: parts lowered,
  anything but letters and digits folded to one dash, joined by dashes,
  cut to 64 — `ChatSessions.Skills` is `ARRAY<STRING(64)>`).
  `_files(root, recursive)` walks a tree in relative-path order
  regardless of case, skipping hidden folders and the top-level
  `users/` folder (the own-packs folder when the tree is the graph's;
  never shared). `tree_packs(root)` names each file by `pack_name`,
  first wins within the tree, and returns `(packs, skipped)`.
  `shelf_roots(graph_root, env)` is the precedence: the tree when the
  variable is set, then `<graph>/skills`. `collect_skills(graph_root,
  owner, env)` merges built-ins → own → tree → graph shelf with first
  wins and returns the skipped rows (`path`, `name`, `shelf`, `reason`
  naming the winner); `all_skills(graph_root, owner, env=None)` is its
  first half, so every existing caller (`get_skill`, `load_packs`, the
  runtime's `skills()`, the loop's index, `slash_skill`, the
  `load_skill` tool) sees the tree with no call-site change. The parse
  cache: `_PARSED` keyed by path holding `((size, mtime_ns), Skill,
  updated)`, `_parsed(path)`, `clear_cache()`; `_pack` builds the
  `Pack` from the cached skill, titling a heading-less file by its
  pack name rather than its stem. Built-in and own folders use the same
  cache. The module docstring has the new shelf.
* `synapse-agentic-harness-system/sahs/loop/skills.py` —
  `frontmatter_error(text)` (an opening `---` with no closing `---` or
  `...`), `frontmatter_warnings(text)` (a `runtime_loading` outside
  `sectioned | full_file_required`, a `truncation_allowed` that is not
  true/false, an `aliases` that is not a list — each saying what it is
  read as). `parse_skill` returns a skill with `error` set and
  `description: unreadable: frontmatter: …` when the block never
  closes, so it lists and refuses to load by name through the existing
  `SkillUnreadable`, like a non-UTF-8 file, instead of loading as a file
  of keys. `policy_of`'s folding of unknown values is unchanged (a
  pinned test).
* `synapse-agentic-harness-system/scripts/skills_check.py` (new) —
  `report(graph_root, owner, env, model)` and `render(data)`: the
  roots in the picker's order with each one's path, whether it exists,
  how many packs it holds and how many it skipped; the whole-load
  limit and what bounded it (`min(SAHS_MAX_SKILL_CHARS, the engine's
  window)`, `--model` names the engine); every listed pack with origin,
  size, `whole` or `library`, `runtime_loading`, `truncation_allowed`,
  the preference, aliases and path; the packs that refuse to load with
  why; the skipped files with why; the frontmatter warnings. Flags:
  `--graph`, `--skills-dir` (stands in for `MERIDIAN_SKILLS_DIR`),
  `--owner`, `--model`, `--no-dotenv`, `--json`. Reads the silo `.env`
  first unless told not to. Exit 1 while a file is skipped or refuses.
  Stdlib only; nothing is written; no model is called.
* `apps/synapse_admin/README.md` — "Where skill files go": the two
  variables and who reads each, the precedence and the first-wins rule,
  how a tree file is named, the four frontmatter keys with what each
  does, and the check script.
* `synapse-agentic-harness-system/docs/skill-retrieval.md` — "Where
  skill files go": the same, from the loader's side, with the skipped
  rows, the cache and the check script's flags.

### Untouched, by the brief

`sahs/util/spanner/settings.py`, `sahs/identity/authorization.py`,
`db/spanner/001_identity.sql`; the frontends, `sahs/assistant/loop.py`,
`runtime.py`, the model clients (`agent.py`, `sahs/util/gateway.py`,
`profiles.py`), the surface tests (`test_*_surface.py`),
`apps/synapse_admin/backend/meridian.py` (its `_skills_dir` is the same
variable the loader now reads), `sandbox.py`.

## Tests

New or changed:

* `synapse-agentic-harness-system/tests/test_p3_tools.py` —
  `test_violations_sensitivity_and_star` passes `env={"SAHS_SENSITIVE_COLUMNS":
  "deny"}` (the refusals as before); new
  `test_sensitive_columns_allow_by_default_and_deny_on_request`: the
  same two queries refused under `deny` (code in `violations`, `policy:
  deny`) and passing under `allow` with the same code in `warnings`,
  `policy: allow`, the switch's name and "record" in the detail; the
  default with the variable unset is `allow` (`monkeypatch.delenv`);
  `sensitive_policy` reads `Allow`, `DENY`, `no`; `sensitive_policy_note`
  for the three cases; `unknown_column` still refuses under `allow`.
* `synapse-agentic-harness-system/tests/test_spanner_ddl.py` — new
  `test_008_widens_the_model_choice_and_lists_every_artifact_type`:
  the four statements are in the file; `001`+`002` through `lint.load`
  give `Model STRING(16)`, `ck_sessions_model` and the old six types;
  `001`+`002`+`008` give `STRING(64)`, no `ck_sessions_model`, no
  constraint with `Model IN`, and `ck_artifacts_type == set(TYPES)`
  (`kpi` in it); the full model is 44 tables across 8 files with the
  widened column; a temp file with a missing constraint, a missing
  column, a duplicate name, a `RENAME` and an unknown table yields
  exactly those five findings. `test_the_lint_finds_nothing` and the
  README-names-every-file test cover the rest.
* `synapse-agentic-harness-system/tests/test_assistant_spanner_store.py`
  — new `test_a_model_choice_round_trips_whole` on both backends
  (sqlite, and `SpannerDatabase` over the fake SDK database): a 24-
  character `gateway:…` choice stored and read back whole, the raw
  `Model` cell equal, a 64-character choice accepted, 65 refused with
  `ValueError` and the row untouched, `''` forgets.
* `apps/synapse_admin/tests/test_model_choice.py` (new; the `laptop`
  and `spanner` fixtures imported from `test_local_login.py`) —
  parametrized over the two stores: with the gateway configured (no
  call is made) and two 3.x engines listed, the 24-character choice
  appears in `GET /api/chat/dials`, `POST …/model` answers 200 with the
  choice and the label, `GET …/sessions/{id}` reads it back, the
  runtime's store holds it, the `ChatSessions.Model` row on the fake
  Spanner equals it; the three pydantic fields are `max_length=64` and
  accept it; the plane's default model still folds to the plane; an
  unserved model is a reason, not a 422; 64 characters pass validation
  and 65 is a 422.
* `synapse-agentic-harness-system/tests/test_skills_check.py` (new) —
  `pack_name`; the picker's order and winners on a tree with nested
  folders, odd names, a `users/` folder, a hidden folder, a non-UTF-8
  file, an unterminated frontmatter, an unknown `runtime_loading`, a
  file named like a built-in, and a graph shelf sharing a name (the
  tree wins the graph, the built-in wins the tree, the first file in
  path order wins within the tree, `users/` and hidden folders never
  list, the two broken files list with the reason and raise
  `SkillUnreadable`, the skipped rows say which shelf, which name and
  which file won; without the variable the picker is what it was; the
  process environment is the default source); a tree pack pins through
  `load_packs` and a broken one refuses by name; the cache — every file
  parsed once, two more listings parse nothing, an edited file (size
  and mtime changed) is the only file parsed again; `frontmatter_error`
  and `frontmatter_warnings`; the check script's `report`, `render`,
  `main` (exit 1 with a skipped or refusing file, 0 on a clean tree,
  `--json`) and a subprocess run with `--model` and a raised ceiling.

Runs, from the worktree (absolute paths, `set -o pipefail`,
`${PIPESTATUS[0]}`):

| suite | command | result | exit |
|---|---|---|---|
| harness | `cd synapse-agentic-harness-system && python -m pytest -q tests -p no:cacheprovider` | 652 tests collected, all passed (the skill-retrieval line prints `20/20`, `432/432`) | 0 |
| app | `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider` | 155 passed, 2 skipped | 0 |
| DDL lint | `python scripts/spanner_ddl_check.py` | `ok: 44 tables across 8 files; …` | 0 |

## Not finished, and what to know

* `008` on the live E1 database has not been applied from here (no
  Spanner in this environment); the file is what to apply, alone, on a
  database that already has `001` … `007`. `scripts/spanner_check.py`
  lists tables, not column widths, so after applying it the check to
  make is `ChatSessions.Model`'s type in the console.
* `RunProposal` has no `model` field, so no width was raised there; if
  the frontend starts sending one on `POST …/run`, the field would be
  added at 64 like the others (the frontend and `runtime.py` are the
  other agent's).
* The check script reads the own-packs folder, not the store's
  `UserSkills` rows (the runtime binds those under a store); it says
  so in its docstring.
* The v1 navigator's own shelf (`sahs/loop/skills.py` `list_skills`,
  `<graph>/skills` only) was left as it was; the brief's target was the
  chat's picker (`all_skills`).
* The tree's file order (which file wins a name) is relative-path
  order regardless of case, so `a-b.md` beats `a/b.md`; documented in
  `_files` and the test.

## Files outside the lane

None edited. `docs/wiki/07-serving.md` and `docs/wiki/13-configuration.md`
(documentation, not named in the brief) got a paragraph and a table row
so the wiki's violation table stops saying the two codes always refuse.
The fast-forward of the worktree branch onto the local
`claude/production-ready` ref (`a33db96`) is the one thing done to the
branch beyond the two commits; it brought no change of this agent's.
