# Synapse on Spanner: identity, the chat store, the graph

The schema for the day Synapse leaves the laptop: who may open which
surface, where a person's chats and messages live, and the graph as
a graph. The DDL is under `db/spanner/` (three files, applied in
order); `scripts/spanner_ddl_check.py` holds the files to the
database's rules and to the Python registries; this document is the
reasoning.

## 1 · What the laptop has, and what changes

Today one configured person (`LUMI_USER_NAME`) uses two surfaces off
one process: the Lumi console at `/` and Synapse at `/synapse/`. The
chat store is SQLite (`sahs/ask/store.py`, `sahs/assistant/store.py`),
the events are JSONL per session, files live in a workspace folder,
a person's own skills in `graph/skills/users/<owner>/`, staged
knowledge in `sources/artifacts/`. The graph is append-only JSONL
(`sahs/graph/quads.py`) folded into current state at compile time.

What changes with Spanner: the person becomes a row, every chat and
memory and skill is owned by one, a role decides which surface opens,
and the graph's fold is a table the property graph reads. What does
not change: the append-only discipline (assertions are never edited;
the fold is derived), the provenance on every record, the one-writer
rule for the graph, and the E14 door for user packs.

## 2 · Identity (`001_identity.sql`)

**Keys.** Every id is a random UUID (`GENERATE_UUID()`), never a
sequence or a timestamp prefix: Spanner splits by key range, and a
monotonic key writes every new row to the same split.

**Uniqueness that means something.** `Email` and `Username` are
unique on their case-folded, trimmed forms (generated columns +
unique indexes), so `Saheb@…` and `saheb@…` are one person and the
app never compares strings itself.

**Passwords.** One row per password in `UserCredentials`, interleaved
in the user. The column holds the Argon2id encoded string
(`$argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>`): the salt and the
parameters ride inside it, so the algorithm can be tightened per row
and old rows still verify. A server-side pepper is applied before
hashing and lives in Secret Manager, referenced by `PepperVersion`,
never in the database: a copy of the table alone cannot be cracked
offline. Retired rows stay for the reuse check (the last five) and
are pruned after. The policy the app enforces (NIST 800-63B): at
least 12 characters, no composition rules, checked against the
breached-password corpus by k-anonymity at set time, changed only
through the reset flow or by the person with their current password.

**Login.** A session is 32 random bytes in an `HttpOnly; Secure;
SameSite=Lax` cookie; the table holds their SHA-256 (`TokenHash`),
so a database read cannot mint a session. Two expiries: idle
(`ExpiresAt`, pushed forward on use, 30 minutes) and absolute
(`AbsoluteExpiresAt`, never pushed, 12 hours). Rows die a week after
the absolute expiry, which is long enough for an investigation to
see them. Refresh tokens rotate in families: each use issues a child
and marks the parent used; a used token presented again is reuse,
the family is revoked, the audit says so. Every attempt lands in
`LoginAttempts` (a month) for rate limiting per address and per
account; five failures lock the account with exponential backoff
(`FailedLoginCount`, `LockedUntil`), reset on success.

**Second factor.** `MfaFactors` holds a TOTP secret as Cloud KMS
ciphertext with the key version, or a WebAuthn credential's public
key; recovery codes are hashed and single-use. `MfaRequired` on the
user is set for every admin by policy.

**One-shot tokens.** Email verification, password reset and
invitations share `ActionTokens`: hashed, single-purpose, short-lived,
used once, gone a day after expiry. An invitation names the address
and the role it will carry (`Invitations`); accepting it creates the
user with that role and nothing else.

**Roles are rows.** `Roles.Surfaces` says which app a role may open,
so the mapping the product wants — admin sees Lumi, analyst sees
Synapse, steward to be decided — is data reviewed with the schema
(the seed at the foot of the file) and changed without a deploy.
Permissions (`metrics.certify`, `users.manage`, `skills.share`, …)
hang off roles; the app checks permissions, never role names, so the
steward's set can be decided later by filling `RolePermissions`. A
grant (`UserRoles`) records who granted it, when, until when, and an
optional business-unit scope for the day a steward is a steward of
one line of business.

**Audit.** `AuditEvents` is append-only by convention (the app has no
update path), keyed at random, indexed by subject, actor and action,
kept 400 days, and published on `AuditStream` for the security
tooling. Every login, failure, lockout, password change, role grant,
session revocation and invitation lands there with the address and
the user agent.

**Deletion.** A person is never hard-deleted while audit rows
reference them: `Status = 'deleted'`, `DeletedAt` set, and the
anonymization job replaces email, username and display name with
tombstones; interleaved children (credentials, factors, preferences,
roles, memories, skills) cascade when the row finally goes.

### The API the pages need

| route | does |
|---|---|
| `POST /api/auth/signup` | email, username, password → user in `pending_verification`, a verify token mailed; the default role is analyst (or the invitation's) |
| `POST /api/auth/verify` | token → `EmailVerifiedAt`, status active |
| `POST /api/auth/login` | email or username + password (+ TOTP when required) → session cookie, refresh token; every outcome in `LoginAttempts` and the audit |
| `POST /api/auth/logout` | revokes the session and its refresh family |
| `POST /api/auth/refresh` | rotates the refresh token, extends the session |
| `POST /api/auth/password/forgot` · `/reset` | token by mail, then a new password; all sessions revoked |
| `GET /api/auth/me` | the person, their roles, the surfaces they may open |
| `GET/POST /api/admin/users`, `…/{id}/roles`, `…/{id}/lock` | `users.manage` |
| `GET /api/admin/audit` | `audit.read` |

The pages: `/login`, `/signup`, `/verify`, `/reset` on both surfaces'
shells; a request without a valid session is sent to `/login`; a
session whose roles carry no `lumi` surface cannot open `/`, no
`synapse` surface cannot open `/synapse/`. The owner seam already in
the runtime (`AssistantRuntime.owner`) becomes the session's
`UserId`.

## 3 · The chat store (`002_chat.sql`)

A one-to-one map of the SQLite tables with the person added:

* `ChatSessions` gains `OwnerUserId`, keeps `Model` (the plane the
  chat rides) and the JSON the store kept; the shelf reads one index
  (`OwnerUserId, Archived, UpdatedAt DESC`).
* `ChatMessages`, `ChatArtifacts`, `ChatPlanVersions`, `ChatFeedback`,
  `ChatFiles`, `ChatEvents` are **interleaved** in their session: a
  chat is one key range, so opening it is one read and deleting it is
  one cascade. Messages carry `Seq` for order and a full-text token
  column for the fuzzy finder.
* `ChatEvents` replaces the JSONL: ninety days, and a change stream
  so the surface can tail a live turn without polling once the app is
  off the laptop.
* `ChatFiles` is the manifest; the bytes go to a bucket
  (`ObjectPath`), the converted text stays because the model reads it
  as text anyway.
* `ChatMemories` is interleaved in the **user**, not the chat: memory
  is bound to the person (spec §7), scoped and statused, never
  deleted.
* `UserSkills` is `graph/skills/users/<owner>/` as rows, with
  `Shared` for the day a steward promotes one to the shelf;
  `KnowledgeFiles` is `sources/artifacts/` with who staged it and
  which run ingested it — empty until one does, never pretended in.

## 4 · The graph (`003_graph.sql`)

**Two tables per family, one discipline.** `GraphNodeAssertions` and
`GraphEdgeAssertions` are the JSONL: one row per record, keyed by
identity plus `Seq`, provenance flattened into typed columns
(`Source`, `RunId`, `Witness`, `Status`, `Support`, `Evidence`,
`Actor`, `Retrieved`, `ValidFor`), append-only. `GraphNodes` and
`GraphEdges` are the fold — the last active assertion per identity —
maintained by the single writer in the same transaction as the
append, so a reader of the fold never sees a state the history does
not explain. Edge identity is `(SubjectId, Relation, ObjectId,
Witness)`: one quad per witness family, so independent testimony
never collapses (E12/A1) and `support_by_witness` is a GROUP BY.

**The registries are the constraints.** The CHECK lists on
`GraphNodes.Kind`, `GraphEdges.Relation` and `GraphEdges.Witness` are
the Python registries (`ID_PATTERNS`, `RELATIONS`, `WITNESSES`) and
the lint fails when they drift. The relation registry's endpoint
typing (a `has_column` edge goes table → col) stays in the writer,
where it is today.

**The property graph.** `SynapseGraph` is defined over the fold with
dynamic labels (the node's `Kind`, the edge's `Relation`) and dynamic
properties (the record's `Props`), so GQL reads the way the compiler
thinks:

```sql
GRAPH SynapseGraph
MATCH (t:table {NodeId: 'table:dw.gms_transaction'})-[:has_column]->(c:col)
RETURN c.NodeId, c.Props.data_type;

GRAPH SynapseGraph
MATCH (m:metric)-[e:measured_on]->(t:table)
WHERE e.Status = 'active' AND e.Witness IN ('jobs_30d', 'studio')
RETURN m.Label, t.NodeId, e.Support;

GRAPH SynapseGraph
MATCH (a:table)-[j:joins_via]->(b:table)
WHERE j.Props.scope = 'scoped_only'
RETURN a.NodeId, b.NodeId, j.Witness;
```

History stays SQL over the assertion tables: "what did we assert
about this metric in run X" is a range read on `(NodeId, Seq)`.

**Around the graph.** `GraphRuns` anchors provenance; `GraphCrosswalk`
is the identity layer (a source's name → the graph's id);
`GraphStatusTransitions` is the clerk's ledger (every move through
mined → team_candidate → pending → certified → deprecated with the
actor, a user now); `Builds` records which compiled snapshot is
promoted, so "which build served this answer" resolves against a
row.

**Loading.** Keys are the graph's own ids, which cluster by kind
prefix (`table:`, `metric:`); a bulk load writes many prefixes at
once and the assertion tables carry `Seq` last, so the hot-range
risk is the prefix, not the time — acceptable for a graph of this
size (tens of thousands of nodes), noted for the day it is not
(a hash shard column in front of the key).

## 5 · Decisions still open

* the steward's permission set (the role exists; `RolePermissions`
  empty until decided);
* whether sign-up is open with a default analyst role or invitation
  only (the schema supports both; the seed assumes invitation for
  admin and steward, open for analyst);
* single tenancy: no organisation column; the day a second company
  shares an instance, `OrgId` leads every key;
* the cards: files on the serving host today, a `BuildCards` table
  (`BuildId, Kind, Name, Text`) when the serving host stops being a
  laptop.
