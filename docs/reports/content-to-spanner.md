# Content to Spanner: the report

Branch `worktree-agent-a7384f21b8a951f30`, worktree
`/home/user/wyla/.claude/worktrees/agent-a7384f21b8a951f30`, on top of
`c62cfc6` (PR #149). One commit, not pushed, no PR.

The task: everything the app still persisted on the filesystem beside
the chat — a chat's files, a person's own skills, the review board, the
knowledge files — goes into the store under `SAHS_STORE=spanner|sqlite`;
under `SAHS_STORE=local` every path stays byte-for-byte the one before.
It did, with the exceptions named under "not finished".

## What changed, file by file

### New

* `synapse-agentic-harness-system/db/spanner/007_content.sql` —
  `ChatFileChunks` (`SessionId`, `FileId`, `Seq`, `Chunk BYTES(MAX)`;
  interleaved in `ChatFiles ON DELETE CASCADE`; a file's bytes in slices
  of at most 8 MiB, the build bundle's pattern) and the review board:
  `ReviewSubmissions` (the head row with the fields the person gave,
  `SubmitterUserId` as a foreign key to `Users`, `SubmitterName`,
  `ApproverName`, `ApproverBand`, `Status`, `Version`; CHECK lists on
  `Kind`, `Status`, `Ext`, a regex on `Name`; indexes by status and by
  submitter), `ReviewVersions` (interleaved; the text of every version),
  `ReviewEvents` (interleaved; one row per ledger record, `Seq` per
  submission, `Payload JSON` for the record's other fields, CHECK on
  `Event`) and `ReviewSeen` (`UserId`, `SeenAt`; interleaved in `Users`).
  The ledger's `by` and `at` are the columns `Actor` and `OccurredAt`:
  `By` and `At` are GoogleSQL reserved words and `scripts/spanner_ddl_check.py`
  refuses them. The lint passes (42 tables across 6 files).
* `synapse-agentic-harness-system/sahs/assistant/content_store.py` —
  `SpannerContentStore(database, owner_user_id)` over the `Database`
  protocol with `CONTENT_SQLITE_SCHEMA` and `database.ensure()`, four
  verb groups: (a) files `add_file`, `files`, `pending`, `remove_file`,
  `file_bytes`, `file_text`, `mark_sent`, `parts_for` over `ChatFiles` +
  `ChatFileChunks` (the classification and conversion are
  `files.prepare`, the turn's parts are `files.build_parts`; the owner
  is checked against the chat's `OwnerUserId`); (b) own skills
  `save_skill`, `delete_skill`, `my_skills` over `UserSkills` (the name
  is held to the DDL regex; `user_id=` lets an approver publish onto the
  submitter's shelf); (c) knowledge `stage_knowledge` (`replace=` for an
  approved resubmission; a retired row under the same name comes back),
  `knowledge_files`, `knowledge_file`, `retire_knowledge` over
  `KnowledgeFiles`; (d) the board: every verb `Reviews` exposes —
  `submit`, `resubmit`, `record_ai`, `start_ai`, `wait`, `decide`,
  `withdraw`, `get`, `list`, `find`, `text_of`, `published_names`,
  `notices`, `mark_seen` — over the four review tables, folded by
  `reviews.fold_records` (the same function the ledger folds through),
  the door and decision checks shared too. The board ignores the owner
  except as the actor: `mark_seen` and the unread count are the owner's
  `ReviewSeen` row (the `me` argument is accepted for the `Reviews`
  signature). The folded dict carries one extra key beyond the ledger's,
  `submitter_user_id`, so an approval can write the submitter's row.
* `synapse-agentic-harness-system/tests/test_content_store.py` — the
  four groups on both backends (see "tests").
* `apps/synapse_admin/tests/test_content_store_app.py` — the app-level
  test on the fake SDK and on the sqlite stand-in (see "tests").
* `docs/reports/content-to-spanner.md` — this file.

### Changed, inside the lane's ownership

* `sahs/assistant/files.py` — `store()` split into `prepare(name, data)
  → (Stored, text | None)` (check, convert, decode, describe: the one
  classification for both homes) plus the filesystem write, unchanged
  in effect; `parts_for(workspace, ids)` now calls `build_parts(rows,
  ids, read_bytes=, read_text=)`, the same loop over two readers. The
  workspace path writes and reads exactly what it did.
* `sahs/assistant/reviews.py` — the fold (`fold_records`), the board
  row (`board_row`), the door's checks (`check_submission`), the
  decision's checks (`check_decision`) and the notices (`notice_rows`)
  factored out of the `Reviews` class as module functions; `Reviews`
  calls them and behaves as before (`tests/test_reviews.py` and
  `test_approval_workflow.py` pass unchanged).
* `sahs/assistant/authoring.py` — `save_skill(..., store=None,
  user_id="")` and `delete_skill(..., store=None)`: the same checks,
  then a `UserSkills` row when a store is given (path
  `UserSkills/<user id>/<name>`), the file otherwise.
* `sahs/assistant/skills_loader.py` — `bind_own_skills(owner, source)`,
  `unbind_own_skills(owner)`, `own_skills(graph_root, owner)`:
  `all_skills` reads the owner's own packs from the bound source (the
  store's `my_skills`) when one is bound for them and from
  `<graph>/skills/users/<owner>/` otherwise. Because `get_skill`,
  `load_packs`, the loop's skill index and the `load_skill` tool all go
  through `all_skills(graph_root, owner)`, no call site in
  `sahs/assistant/loop.py` or `kit.py` changed — the hook is bound by
  the runtime, keyed by the owner slug. Built-in and shared packs still
  come from disk.
* `sahs/assistant/runtime.py` — a `content_store` property whose setter
  binds/unbinds the own-skills shelf and resets the board; `files`,
  `add_file`, `remove_file` go to the store when one is on; two private
  helpers `_file_parts` and `_mark_files_sent` pick the home, and
  `start_turn` calls them in place of `files_mod.parts_for` /
  `files_mod.mark_sent` (the one edit outside the listed methods: two
  lines in `start_turn`, the loop's read of a message's files);
  `reviews` returns the store when one is on; `save_my_skill` /
  `delete_my_skill` pass the store; `_publish_submission` writes the
  submitter's `UserSkills` row or a `KnowledgeFiles` row (`replace=True`)
  under a store and the folder otherwise.
* `apps/synapse_admin/backend/chat.py` — `_make_runtime` attaches
  `SpannerContentStore(_identity().db, owner)` beside the chat store;
  docstring. The `stream` endpoint is untouched.
* `apps/synapse_admin/backend/meridian.py` — `_content_store()` (the
  store when `SAHS_STORE` names one, bound to the signed-in person or to
  a reader's placeholder for a listing) and `_StoredFile` (a
  `KnowledgeFiles` row wearing the bit of `Path` the shelf reads);
  `_knowledge_index` lists the rows as the staged files under a store
  (the same `artifacts/<unit>_<name>.<ext>` rel keys, the business unit
  as the author) and `artifact_file` reads them back; `artifacts()`
  reports `staging_dir` / `staging_store`; `POST /api/meridian/artifacts`
  stages a row (a signed-in person required: `StagedBy` is a foreign
  key) and refuses a duplicate name with the same words as the folder.
  Under local the folder path is unchanged. Only the artifacts/staging
  routes were touched.
* `sahs/identity/database.py` — `_KEY_WIDTH` gains `ChatFileChunks: 3`,
  `ReviewVersions: 2`, `ReviewEvents: 2`.
* `tests/fake_spanner.py` — ensures `CONTENT_SQLITE_SCHEMA` on the
  double; `SeenAt` typed as a timestamp.
* `tests/test_spanner_ddl.py` — `test_the_content_tables_hold_the_bytes_and_the_board`.
* `db/spanner/README.md` — the `007` row (and a row for `004`/`006`,
  which the table lacked), the three stores, applying `007` alone on a
  database that has `001`–`006`, the migration steps for the chunks and
  the ledger.
* `docs/spanner-wiring.md` — six rows moved from "Not yet in Spanner"
  into "In Spanner" with their store verbs and tables; the hotel
  paragraph and the three-stores paragraph; a new "not yet" row for the
  build-graph run's read of the knowledge files; the owner-slug seam
  noted; the testing section names the new tests.
* `docs/spanner_schema.md` §4.11 — an "as built" paragraph naming the
  actual tables and columns (the sketch's `ApproverUserId` and the
  directory columns stay a design note).
* `.env.example` — the `SAHS_FILES_DIR` comment now says nothing reads
  it and the bytes go to `ChatFileChunks`. No variable renamed, none
  added.

### Files outside the lane's ownership that were touched

None. `sahs/loop/skills.py`, `sahs/assistant/kit.py`, `loop.py`,
`events.py`, `spanner_store.py`, the frontends and the enterprise
baseline files were not edited. The `spanner_store.py` docstring still
lists the files, own skills, knowledge files and the ledger as "what
stays on the filesystem"; that paragraph is now stale and belongs to
the workstream that owns the file.

## Tests

Run as instructed, absolute paths, `-p no:cacheprovider`:

* `synapse-agentic-harness-system/tests` (from inside the silo): 572
  passed, exit 0. Of these, new: `tests/test_content_store.py` — 14
  tests, each parametrized over `SqliteDatabase(":memory:")` and
  `SpannerDatabase(FakeSpannerDatabase())`: the file rows and the parts
  on a turn equal to the workspace's for the same files (a PDF, a CSV,
  a workbook), mark-sent / pending / remove, another owner sees nothing;
  a 9 MiB upload lands as two chunks (8 MiB + the rest) and comes back
  byte-identical, base64 on the turn included; own skills per person,
  the DDL regex, the approver writing onto the submitter's shelf,
  `authoring.save_skill` with the store, the loader reading the bound
  shelf for the owner and not for another; knowledge files shared,
  duplicate refused, replace, retire and re-stage; the board run through
  submit → ai_review → reject → resubmit → submit-as-version → approve →
  withdraw on the ledger and on the store, the seven folded submissions
  compared key by key (the store's keys a superset, values equal apart
  from ids and instants), the head row, the event rows, the versions,
  the door's refusals, the publish door refusing, the band floor; the
  notices for two people with their own seen-marks. Plus one test in
  `tests/test_spanner_ddl.py`.
* `apps/synapse_admin/tests` (from the repo root): 148 passed, 2 skipped,
  exit 0 — **with `PYTHONPATH=synapse-agentic-harness-system`**. Without
  it the run exits 2 at collection, before any test, on the pre-existing
  `test_approval_workflow.py` → `test_synapse_surface.py` → `app.py` →
  `import sahs`, which nothing on that path puts on `sys.path` (the
  README's combined command collects the silo's tests first, which do).
  Not a regression of this change: the three files on that import path
  (`test_approval_workflow.py`, `test_synapse_surface.py`, `app.py`) are
  untouched here (`git diff --quiet HEAD` on them), and `sahs` is not
  installed in this environment. New here:
  `test_content_store_app.py::test_every_piece_of_content_lands_in_the_store`
  on both the sqlite stand-in and the fake SDK (sign up; upload a CSV to
  a chat and read the `ChatFiles` / `ChatFileChunks` rows and the parts
  the loop would send; a refused upload leaves nothing; delete removes
  row and chunks; save an own skill and see it on the shelf as mine, in
  `UserSkills`, pinnable on the chat; submit a knowledge file, wait for
  the read, approve it: `ReviewSubmissions` / `ReviewVersions` /
  `ReviewEvents` rows, a `KnowledgeFiles` row, the Knowledge Files shelf
  listing and reading it; a skill through the board onto `UserSkills`,
  deleting it withdraws the record; the notices and `ReviewSeen`; the
  staging door writing a row and refusing a duplicate; nothing under
  `graph/runs/chat/workspaces`, `graph/runs/reviews`, `graph/skills`
  or `sources/`; a second person's own shelf and files apart, the board
  and the knowledge files shared) and
  `test_the_staging_door_needs_a_person_under_a_store`.
* The directly affected suites (`test_reviews`, `test_chat_files`,
  `test_authoring`, `test_assistant_spanner_store`, `test_spanner_ddl`,
  `test_approval_workflow`, `test_local_login`) pass unchanged: the local
  path is the one before.

## Not finished, and why

* **The build-graph run does not read `KnowledgeFiles`.** `pipeline.py
  build-graph` reads `<sources>/artifacts/` from the filesystem; under a
  store a staged row is on the shelf and the app says so, but no ingest
  step exports the rows before a build or writes `IngestedRun` after.
  That is the graph half, on the filesystem in the first rollout by
  design; listed in "Not yet in Spanner".
* **`AssistantRuntime.owner` is still the display-name slug** under a
  store — the key the loader binds the store-backed shelf under, and
  the `submitter_slug` a submission carries. The rows are keyed by user
  id; two people with the same display name would share those two
  lookups. The fix is one line in `runtime.py`'s `owner` property
  (prefer `owner_user_id` when set), outside this lane's ownership, so
  it is noted in `docs/spanner-wiring.md` rather than made.
* **`ObjectPath` stays null.** The bytes are in `ChatFileChunks`;
  `SAHS_FILES_DIR` remains named in `.env.example` (not renamed, not
  removed) and nothing reads it.
* **The board's approver is still the laptop's directory seam**
  (`SYNAPSE_USER_MANAGER`, `SYNAPSE_USER_MANAGER_BAND`), stored as
  `ApproverName` / `ApproverBand`; `ApproverUserId` and the directory
  columns on `Users` wait for the directory, as `docs/spanner_schema.md`
  §4.11 says.
* **The docstring of `sahs/assistant/spanner_store.py`** still names
  these things as on the filesystem; not this lane's file.
