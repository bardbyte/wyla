# Synapse on Spanner

The schema the laptop's stores become when Synapse leaves the laptop:
three GoogleSQL DDL files, applied in order, one database.

| file | holds | replaces |
|---|---|---|
| `001_identity.sql` | users, credentials, roles and permissions, login sessions, refresh tokens, MFA, one-shot tokens, invitations, preferences, the audit | nothing yet: the laptop has one configured person (`LUMI_USER_NAME`) |
| `002_chat.sql` | chats, messages, artifacts, plans, feedback, files, events, memory, a person's own skills, staged knowledge files | `graph/runs/chat/sessions.sqlite3`, the events JSONL, the files under each workspace, `graph/skills/users/`, `sources/artifacts/` |
| `003_graph.sql` | the graph's nodes and edges with their provenance, as append-only assertions plus the folded current state, the crosswalk, the clerk's transitions, the builds, and a property graph over the fold | `graph/nodes/*.jsonl`, `graph/edges/*.jsonl`, `graph/identity/crosswalk.jsonl`, `graph/runs/` |

The reasoning behind every table is in
`docs/specs/spanner_schema.md`. This file is the how.

## Check the files without a Spanner

```bash
python scripts/spanner_ddl_check.py
```

reads the three files as statements and holds them to what the
database will: every table keyed, every interleave on a parent
defined earlier with a key that extends the parent's, every foreign
key and index on columns that exist, every row deletion policy on a
timestamp, no reserved word as a column name, the property graph
keyed on primary keys — and the graph half's CHECK lists equal to the
Python registries (`sahs.graph.quads.RELATIONS`, `WITNESSES`,
`sahs.graph.ids.ID_PATTERNS`), so a relation added in code fails the
check until the DDL learns it. `tests/test_spanner_ddl.py` runs it.

## Apply

Against the emulator first, on any laptop with the gcloud SDK:

```bash
gcloud emulators spanner start &
export SPANNER_EMULATOR_HOST=localhost:9010
gcloud config set auth/disable_credentials true
gcloud config set project synapse-local
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud spanner instances create synapse --config=emulator-config \
  --description="Synapse" --nodes=1
gcloud spanner databases create synapse --instance=synapse
for f in db/spanner/00*.sql; do
  gcloud spanner databases ddl update synapse --instance=synapse \
    --ddl-file="$f"
done
```

Then the same three commands against the real instance. Each file is
one DDL batch; Spanner applies a batch atomically, so a file that
fails leaves nothing half-made.

Two things to know about the target:

* **Dynamic labels.** `003_graph.sql` defines the property graph with
  `DYNAMIC LABEL (Kind)` / `DYNAMIC LABEL (Relation)` and
  `DYNAMIC PROPERTIES (Props)`: one node table, one edge table, the
  label read from the row. That is the 2025 Spanner Graph surface. On
  an instance that predates it, the fallback is the same two tables
  with static labels — replace the two element definitions with
  `LABEL Node PROPERTIES ALL COLUMNS` and `LABEL Edge PROPERTIES ALL
  COLUMNS` and match on `n.Kind = 'table'` / `e.Relation =
  'has_column'` instead of on the label.
* **Search indexes.** The two `CREATE SEARCH INDEX` statements need
  the full-text search feature; the emulator accepts the DDL, the
  index becomes useful on a real instance.

## Seed

`001_identity.sql` ends with the seed rows the app expects, as
commented INSERTs: the three roles with the surfaces each may open
(`admin` → the Lumi console at `/` and Synapse; `analyst` → Synapse
at `/synapse/`; `steward` → Synapse, its permission set still to be
decided) and the permission names the app checks. The bootstrap job
applies them once, idempotent on `Name`, and grants the first admin
from an environment variable — never from a default password.

## Move the laptop's data

One-way, run once per store, in this order (the foreign keys want
users before chats and runs before assertions):

1. users: one row per distinct `LUMI_USER_NAME` / `actor` seen in the
   SQLite stores, status `pending_verification`, a password set on
   first login through the reset flow;
2. `sessions.sqlite3` → `ChatProjects`, `ChatSessions`,
   `ChatMessages` (Seq from rowid order), `ChatArtifacts`,
   `ChatPlanVersions`, `ChatFeedback`, `ChatMemories`;
3. each workspace's `files/manifest.json` → `ChatFiles` (the bytes to
   a bucket, `ObjectPath` the object);
4. `graph/skills/users/<owner>/*.md` → `UserSkills`;
   `sources/artifacts/*` → `KnowledgeFiles`;
5. `graph/runs/*/manifest.json` → `GraphRuns`, then every JSONL line
   → `GraphNodeAssertions` / `GraphEdgeAssertions` in file order
   (Seq per identity), then one fold pass → `GraphNodes` /
   `GraphEdges`; `identity/crosswalk.jsonl` → `GraphCrosswalk`;
   `builds/*/manifest.json` → `Builds`.

The fold is the same function the compiler runs today
(`GraphDir.fold`): last record per identity wins, retracted drops.
