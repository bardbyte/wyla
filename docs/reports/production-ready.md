# Production-ready: the consolidated report

Branch `claude/production-ready`, PR #150. Four workstreams ran in parallel
worktrees off `main` at PR #149 and were merged in this order: storage
foundation, content to Spanner, multi-task turns, skill retrieval. Each has
its own report; this page is the map.

| workstream | report | what it delivers |
|---|---|---|
| storage foundation | [`storage-foundation.md`](storage-foundation.md) | the missing build-bundle DDL (`005`), `spanner_check --emit-ddl`, the turn event stream in `ChatEvents` with replay after a pod restart, the Ask lane on the Spanner store, `.env` parsed once per process, the four env profiles, `Makefile`, `scripts/readiness.py`, `docs/deploy.md`, the tenancy decision, the CI workflow |
| content to Spanner | [`content-to-spanner.md`](content-to-spanner.md) | `007_content.sql` (file bytes in chunks, the review board), `SpannerContentStore` for chat files, own skills, knowledge files and reviews; the shelf and the staging door through the store |
| multi-task turns | [`../../synapse-agentic-harness-system/docs/reports/multi-task-turns.md`](../../synapse-agentic-harness-system/docs/reports/multi-task-turns.md) | `planner.py`, tasks in dependency waves on a bounded pool under one budget, a synthesis turn and a "What was done" artifact, the task board on both chat pages |
| skill retrieval | [`../../synapse-agentic-harness-system/docs/reports/skill-retrieval.md`](../../synapse-agentic-harness-system/docs/reports/skill-retrieval.md) | `skill_index.py` (chunker, FTS5 index, BM25), per-engine whole-load budgets, frontmatter-controlled sectioning that fails closed, `skill_toc` / `skill_search` / `skill_read`, the `skills_loaded` record, the routing hint |

## The one flag

`SAHS_ENV_FILE` names the `.env` the loader reads; everything else follows
from that file. `synapse-agentic-harness-system/env/` holds one placeholder
example per profile (`local`, `e1`, `e2`, `e3`), and the root `Makefile`
wraps it:

```
make run ENV=local      # sqlite stand-in, email-and-password, no accounts elsewhere
make run ENV=e1         # a laptop whose every row goes to the dev Spanner
make check ENV=e3       # the readiness table before anyone deploys
make test               # both suites
make ddl-check          # the DDL under db/spanner, without a Spanner
```

`docs/deploy.md` is the mental model and the day-one checklist. Tenancy is
by deployment: one Spanner database per environment, no `TenantId`,
per-person isolation by `OwnerUserId` / `UserId`.

## Where everything lands now

`docs/spanner-wiring.md` is the authority. In short: identity, sessions,
audit, Google connections, chats, messages, artifacts, projects, memories,
plan versions, feedback, the turn event stream, the Ask lane, chat files
(manifest, text and bytes), own skills, knowledge files, the review board
and the promoted build are all in the store under `SAHS_STORE=spanner` or
`sqlite`. Still on the filesystem, by decision or pending: the graph and
its Knowledge Catalog writes (the graph stays a compiler input; the build
is what ships), the Ask lane's own event log, the build-graph run's read
of the knowledge files, and derived data such as the skill search index
and sandbox scratch.

## Test runs on the merged head

| suite | result | exit |
|---|---|---|
| `apps/synapse_admin/tests` | 153 passed, 2 skipped | 0 |
| `synapse-agentic-harness-system/tests` | 620 passed | 0 |
| `scripts/spanner_ddl_check.py` | 44 tables across 7 files, ok | 0 |
| CI `tests` job on PR #150 | success on the pre-merge head; re-runs on this push | |

Retrieval accuracy on the synthetic 2.5 MB pack (577 sections, 1,022
chunks): 20/20 first-hit on the rephrased asks, 432/432 with one ask per
section, 10/10 on the routing hint; a search costs 3 to 5 ms.

## Integration edits made during the merges

- `docs/spanner-wiring.md` and `db/spanner/README.md` keep both sides' rows.
- `AssistantRuntime.owner` prefers the signed-in user id under a store, so
  two people with one display name never share an own-skills shelf (flagged
  by the content workstream, outside its lane).
- Both chat pages carry the `skills_loaded` arm beside the task-board arms.
- Inside a task sub-turn, a skill refusal leaves the closing to the foreman.
- The simple-ask byte-identical pin lists the loader record.

## Not done here, and who owns it

1. **Apply the DDL to E1**: `005_build_bundles.sql` and `006_external_identities.sql`
   are new to the live database, `007_content.sql` is new everywhere. The
   Okta hop and the Google consent hop both 503 on a database without
   `AuthStates` (006). `make check ENV=e1` reports the schema diff.
2. **Widen two `CHECK` lists in `002_chat.sql`** before real chats hit E1:
   `ChatArtifacts.Type` lacks `kpi`; `ChatSessions.Model` allows only the
   two plane names at 16 characters while the composer records choices
   like `gateway:gemini-3.5-flash`.
3. **Mark the eight governed skills' frontmatter**: `runtime_loading:
   full_file_required` on the contract skills (Portfolio Analytics,
   Current-to-60, New Accounts, TLS), `sectioned` on the frameworks; and
   `aliases:` for the routing hint. Without frontmatter a skill defaults to
   `sectioned`.
4. **Prove context caching on the gateway plane** if the 502K skill is to
   load whole there; the whole skill rides in the cached prefix on Vertex.
5. **Ship the eight skills inside the build bundle** rather than a per-pod
   directory (the build store already chunks a bundle into Spanner); the
   index is derived per pod.
6. **The Ask lane's event log** into `ChatEvents` (the chat lane's sink
   pattern applies verbatim); **the build-graph ingest** of `KnowledgeFiles`.
7. **Per-task token attribution** in multi-task turns (tasks share one
   budget; cost is reported as calls, steps and seconds).
8. **An evaluation from the analyst question-to-SQL precedents** scoring
   skill selection and SQL semantics separately (the loader record and the
   routing hint make both measurable).
