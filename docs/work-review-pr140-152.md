# Work review: PR #140 to PR #152

Written 26 September 2026. It covers everything merged to `main` from
PR #140 (15 September) to PR #152 (26 September). It is meant for two
readers:

- **Leadership:** read §1 (summary), §11 (decision log) and §12 (open
  items).
- **The team:** read the rest. Each section says what was built, how it
  works, and where the code lives.

The detailed reports this page draws on are listed in §14. Where a fact
here and a report disagree, the report is right. It was written next to
the code.

---

## 1. The summary

Two weeks ago Synapse (the product runs as **Radix** on the second
surface) was a single-laptop tool. It kept its files on disk, had no
sign-in, used one model, and had no deployment story. Now it is a
multi-user service ready to deploy:

| area | before #140 | after #152 |
|---|---|---|
| **Who can use it** | one developer, no accounts | Okta sign-in, three roles (admin, analyst, steward), CSRF/CORS gates, per-person isolation |
| **Where data lives** | files under `graph/` on one disk | Cloud Spanner: 44 tables across 9 DDL files; a sqlite stand-in for laptops and tests |
| **Whose credentials query BigQuery** | a service account | optionally the person's own Google token, proven end to end on fakes |
| **Models** | one engine | a catalog of Gemini engines on the gateway, a Model picker, a five-stop Thinking-effort slider |
| **Skills (governed knowledge)** | pasted into the prompt, or refused when too large | loaded whole when they fit, loaded as a searchable library when they do not, **never refused** |
| **Compound questions** | one long turn | split into up to six tasks that run in parallel or in order, ending in a "What was done" report |
| **Observability** | none outside the app | a Langfuse mirror of every turn, rebuilt from Spanner, with datasets and eval runs |
| **Environments** | none | one flag (`SAHS_ENV_FILE`), four profiles (local, e1, e2, e3), `make check` readiness table, CI on every PR |
| **Tests** | 84 app / 445 harness | about 186 app / 684 harness (#150 figures, grown since), plus a DDL lint in CI |

An analogy for the whole stretch: we had a workshop in a garage, and we
turned it into a building. It now has a front desk (identity), locked
rooms for each person (per-owner rows in Spanner), a shared library
(skills and knowledge files), a logbook of everything that happened
(`ChatEvents` feeding Langfuse), and one blueprint per site (the env
profiles).

---

## 2. Timeline, PR by PR

| PR | date | theme | one line |
|---|---|---|---|
| #140 | 15 Sep | Knowledge Catalog | KC enrichment module and tab. Every fact carries its witness. The model writes prose only from those facts, and a verifier drops any sentence it cannot trace |
| #141 | | Knowledge Catalog | Every table in scope, a merged glossary, copy per item, export all as one zip |
| #142 | | Knowledge Catalog perf | The graph is folded once and the build and coverage are cached. Assembling one table went from 166 ms to 9 ms, and the tables route from 36.9 s to 5.8 s |
| #143 | | Observability | Langfuse as a one-way mirror of the event record (off by default). A Spanner connectivity check over REST |
| #144 | | Docs | The Radix Graph story deck, the end-to-end architecture sheet, the mark |
| #145 | 24 Sep | UI + sign-in prep | Radix on the second surface, the thread stays where you scrolled, errors read in plain words. Okta, Google and LDAP preflight scripts |
| #146 | 24–25 Sep | **Identity** | The enterprise identity branch landed, Okta OIDC sign-in on the identity store, both surfaces sign in, People and account pages |
| #147 | 25 Sep | **Model plane** | The gateway serves a catalog of models. Model picker and Thinking-effort slider. Enterprise hostnames moved out of the source |
| #148 | 25 Sep | Model profiles | One table of per-engine thinking levels. Gemini 3.7 Flash is the gateway default. Two shipped defects fixed |
| #149 | 25 Sep | **Spanner for chats** | Local email-and-password sign-in on Spanner. Chats move into the chat tables. A fake Spanner for tests |
| #150 | 25–26 Sep | **Production-ready** | Storage foundation, content to Spanner, multi-task turns, skill retrieval, no-refusal skill loading, Langfuse insight, research paper, first CI |
| #151 | 26 Sep | Laptop-test fixes | Sensitive-column policy, a 64-character model choice (`008`), one skills tree for both shelves, composer pills, a stop button that actually stops |
| #152 | 26 Sep | Laptop round two | Charts and dashboards (12 kinds, formatting from the data), chat UX and usage tracking (`009`), the Okta → Google → BigQuery chain proven (six bugs fixed), the carry-back guide, the sanitization scan |

---

## 3. Features added, by area

### 3.1 Knowledge Catalog enrichment (#140–#142)

- **What:** For each table in the promoted build, it assembles the
  knowledge from the build and the graph into the shape the catalog
  expects.
- **How:** Every fact carries a witness, a status and its provenance.
  An include verdict (`copy` / `review` / `never`) is computed in code,
  not by the model. The model writes prose only from those facts. A
  deterministic verifier drops any sentence it cannot trace back to a
  fact. There is a completeness gate over a 312-row coverage registry.
- **Where:** `sahs/kc/`, the `/api/kc/*` router, the KC Enrichment tab.
- **Performance (#142):** measured on 453 tables. One table assembles in
  9 ms (was 166 ms). The 46-table list loads in 1.0 s cold (was 7.8 s).
  The fact digests are byte-identical before and after.
- **Analogy:** a journalist who may only write sentences they can
  footnote, with an editor who deletes any sentence that has no
  footnote.

### 3.2 Identity and sign-in (#145, #146, #149, #152)

- Okta OIDC sign-in with PKCE, a nonce, and RS256 ID-token verification
  against the JWKS (`sahs/identity/oidc.py`, `backend/okta.py`).
- One shared `/callback` for Okta and Google. The prefix of the `state`
  value decides which provider handles it.
- The state, nonce and PKCE verifier are parked in the `AuthStates`
  table, so **any pod can take the callback**.
- Both surfaces boot from `/api/auth/me`. Every state-changing call
  sends the `X-CSRF-Token` header. The nav is gated by permission.
- Pages: sign-in, account (with Google connect and disconnect), and
  People (admin).
- The email-and-password form stays shut unless `AUTH_LOCAL_LOGIN=1`.
  It is for laptops and break-glass access only, and `e3` must never
  set it. The sign-in page shows why the form is shut.
- Preflight scripts for Okta, Google and LDAP check that the provider is
  reachable and that our client and callback are registered, without
  signing anyone in. Inventory modes join the providers into an
  identity map.

### 3.3 Querying BigQuery as the person (#146, #152)

- `SAHS_BQ_AUTH_MODE=user`: the person connects their Google account
  once. The refresh token is stored Fernet-encrypted in
  `GoogleOAuthConnections`. Each query runs with an access token minted
  from it.
- **Proven end to end on fakes (#152):** Okta sign-in, Google consent,
  the encrypted connection, the refresh, and a BigQuery dry run plus
  query carrying the person's bearer token. This holds in both the chat
  and Ask lanes, and on both stores.
- **Six bugs fixed along the way:**
  1. The chat lane had no per-person runner.
  2. The dry run demanded a service-account key.
  3. A failed refresh surfaced as a raw exception.
  4. A Google bearer token on a cookie session was refused.
  5. User mode without a BigQuery project returned a 500.
  6. Disconnecting left a cached token alive.
- **Still needs a real laptop run:** the consent screen itself, the
  scopes Google actually grants, the refresh-token issuance rules, the
  corporate proxy and CA, a real Spanner, and real IAM on the project.
  See `docs/reports/auth-e2e.md`.

### 3.4 The model plane (#147, #148, #151)

- The gateway serves a **catalog** (`GATEWAY_MODELS`). Each model has
  its own thinking style and output cap.
- `gateway_check.py --all-models` proves each model, and `--levels`
  probes which thinking levels an engine accepts.
- `sahs/util/profiles.py` holds one row per engine. It maps the
  five-stop Thinking-effort dial onto what each engine accepts, and
  sets that engine's context window and skill budgets.
- The gateway default is Gemini 3.7 Flash. Gemini 2.5 Pro is retiring.
- Two shipped defects were fixed in #148: the literal `"json"` thinking
  level was sent to 3.x engines, and an unlisted level was sent to
  3.5 Flash.
- The choice is stored whole as `plane:model` in up to 64 characters
  (`008_chat_model.sql`). Before this, a 3.x choice returned a 422.
- In the model picker, each row says in plain words when to pick it.
  The engineer-facing facts appear on hover.
- Hostnames (`IDP_TOKEN_URL`, `GATEWAY_BASE_URL`) are required settings,
  never literals in the source.

### 3.5 Multi-task turns (#150)

- A deterministic gate (`planner.should_plan`) scores the ask for
  compound signals: joiners such as "then" and "for each", enumerated
  items, several question marks, several ask verbs. No plan is made for
  asks under 8 words or at Minimal depth.
- If the ask scores high enough, one JSON call makes a plan of 2 to 6
  tasks with dependencies. The plan is validated: ids renumbered, cycles
  broken, unknown dependencies dropped. An invalid plan falls back to
  the plain turn.
- Independent tasks run side by side (a pool of 2). Dependent tasks run
  in dependency waves and receive the findings of the tasks they depend
  on. All tasks share the turn's budget.
- A synthesis sub-turn closes the turn, plus a **"What was done"**
  artifact with a table per task: goal, status, what was checked or
  refused, what was left behind, and cost.
- Both chat pages show a task board. Simple asks are pinned
  byte-identical to before.
- **Analogy:** a foreman who splits a job among two crews, makes the
  plumber wait for the framer, and hands over one completion report.

### 3.6 Skill loading and retrieval (#150, #151): see §6

### 3.7 Charts and dashboards (#152)

- Number formatting follows the data: percentages keep the decimals they
  carry, counts group thousands, and large columns go compact. One
  formatter in Python has a byte-equal JavaScript twin.
- The tools follow a written chart-selection heuristic and state it:
  - time → line
  - category → sorted bar
  - part-of-whole → stacked or percent bar, never a pie
  - a grid → heatmap
  - many series → small multiples
  - one number → KPI
- Twelve chart kinds, drawn as SVG by hand. Dashboards lay out on a
  grid. "Show all" on tables works again, and the disclaimer strip
  wraps instead of clipping.

### 3.8 Chat UX and usage (#145, #151, #152)

- The whole pane scrolls. The thread stays where you scrolled, with a
  "Latest" pill. Failed steps read as plain snags, with the raw error on
  hover.
- Thinking folds by default ("Radix is thinking… 4s", then "Thought for
  12s"), and the person's choice is remembered.
- Each turn shows elapsed time and tokens live, and again in a footer on
  the message.
- The stop button is an icon that actually stops the model mid-stream.
  The composer controls are one-line pills.
- **Usage tracking** (`009_usage.sql`) stores tokens in and out, model
  calls, elapsed time and turns per chat. The totals show on the shelf,
  in chat search, and as a Tokens column per person on the People page.
- The second surface's wordmark reads "Systematic Intelligence by Lumi".

### 3.9 Sensitive-column policy (#151)

`SAHS_SENSITIVE_COLUMNS` controls what happens when a query projects a
column the build flags as sensitive.

- `allow` (the default): the query runs, and the read is recorded as a
  note on the check result.
- `deny`: the query is refused.

Only this one gate changes. The statement-class, schema, row-access,
live-switch and cost gates are untouched.

### 3.10 Observability and evals (#143, #150): see §8

### 3.11 Deployment shape (#150): see §9

---

## 4. Database interactions (Cloud Spanner)

### 4.1 The mental model

`SAHS_STORE` picks where the app writes:

| mode | where data goes | used for |
|---|---|---|
| `local` | files under the silo's `graph/` (the old behaviour, byte-for-byte) | a single developer, no accounts |
| `sqlite` | the same tables as Spanner, in one local file | laptops rehearsing a deployment, tests, CI |
| `spanner` | Cloud Spanner, one database per environment | e1 / e2 / e3 |

**Hotel analogy** (from `docs/spanner-wiring.md`):

- The identity tables are the **front desk register**: who is here, and
  which key opens what.
- The chat tables are each **guest's room**.
- The filesystem is the **shared lobby**.

In this stretch almost everything moved out of the lobby. A person's
files and own skills went into their room. The review board and the
knowledge files went to the front desk, because they are shared by
design.

### 4.2 Three store classes, one connection

| class | owns | file |
|---|---|---|
| `IdentityStore` | people, credentials, roles, sessions, audit, Okta links, auth states, Google connections | `sahs/identity/store.py` |
| `SpannerAssistantStore` | chats, messages, artifacts, projects, memories, plan versions, feedback, events, usage | `sahs/assistant/spanner_store.py` |
| `SpannerContentStore` | chat files and their bytes, own skills, knowledge files, the review board | `sahs/assistant/content_store.py` |
| `SpannerBuildStore` | promoted builds and their bundle bytes | `sahs/builds/spanner_store.py` |

All the stores speak portable SQL to one `Database` object
(`sahs/identity/database.py`). It is backed by `SpannerDatabase` (the
Cloud Spanner SDK) or `SqliteDatabase` (the stand-in). The app opens it
once and hands it to every store.

### 4.3 The DDL, file by file (apply in order)

| file | adds | tables |
|---|---|---|
| `001_identity.sql` | people and access | `Users`, `UserCredentials`, `Roles`, `Permissions`, `RolePermissions`, `UserRoles`, `AuthSessions`, `RefreshTokens`, `LoginAttempts`, `MfaFactors`, `MfaRecoveryCodes`, `ActionTokens`, `Invitations`, `UserPreferences`, `AuditEvents` |
| `002_chat.sql` | chats and what sits beside them | `ChatProjects`, `ChatSessions`, `ChatMessages`, `ChatArtifacts`, `ChatPlanVersions`, `ChatFeedback`, `ChatFiles`, `ChatEvents`, `ChatMemories`, `UserSkills`, `KnowledgeFiles` |
| `003_graph.sql` | graph tables (empty in the first rollout) and builds | `GraphRuns`, `GraphNodeAssertions`, `GraphNodes`, `GraphEdgeAssertions`, `GraphEdges`, `GraphCrosswalk`, `GraphStatusTransitions`, `Builds` |
| `004_google_oauth.sql` | a person's connected Google account | `GoogleOAuthConnections` |
| `005_build_bundles.sql` | the promoted build's bytes, in chunks | `BuildBundles`, `BuildBundleChunks` |
| `006_external_identities.sql` | Okta links, one-time auth states | `ExternalIdentities`, `AuthStates` |
| `007_content.sql` | file bytes, the review board | `ChatFileChunks`, `ReviewSubmissions`, `ReviewVersions`, `ReviewEvents`, `ReviewSeen` |
| `008_chat_model.sql` | ALTER only | `ChatSessions.Model` widened to 64 characters, `ChatArtifacts.Type` lists every artifact type (`kpi` included) |
| `009_usage.sql` | ALTER only | `ChatSessions.TokensIn`, `TokensOut`, `ModelCalls`, `ElapsedMs`, `Turns` |

That is 44 tables in total. `make ddl-check` lints the DDL offline, and
CI runs it on every PR. `scripts/spanner_check.py` diffs a live
database against the DDL, and `--emit-ddl` prints any undesigned
table.

### 4.4 What reads and writes which table

| feature | tables written or read |
|---|---|
| email-and-password sign-up and login, lockout | `Users`, `UserCredentials`, `UserRoles`, `LoginAttempts`, `AuthSessions`, `AuditEvents` |
| Okta sign-in | `AuthStates`, `Users`, `ExternalIdentities`, `UserRoles`, `AuthSessions`, `AuditEvents` |
| the session cookie, on every request | `AuthSessions` (with `LastSeenAt` touched), `Users`, `UserRoles`, behind a 5-second process cache |
| sign-out, sign-out everywhere | `AuthSessions.RevokedAt`, `AuditEvents` |
| roles and People (admin) | `Roles`, `Permissions`, `RolePermissions`, `UserRoles`, `Users` (soft delete), `AuditEvents` |
| Google connection | `AuthStates` (the consent hop), `GoogleOAuthConnections` (refresh token encrypted) |
| chats, titles, flags, model, pinned skills, project | `ChatSessions` |
| messages | `ChatMessages` (`Seq` taken from `ChatSessions.MessageCount`) |
| turn events (live stream and replay) | `ChatEvents`: the in-memory bus answers first, and a pod that restarted replays the stream from this table |
| artifacts and their versions | `ChatArtifacts` |
| projects | `ChatProjects` |
| long-term memory (`memory.md`) | `ChatMemories` (retired, never deleted) |
| plan versions, feedback | `ChatPlanVersions`, `ChatFeedback` |
| usage per chat and per person | `ChatSessions` usage columns; `ChatMessages.Payload.usage` on the final message |
| files on a chat | `ChatFiles` (manifest), `ChatFileChunks` (bytes in slices of at most 8 MiB) |
| a person's own skills | `UserSkills` |
| the review board | `ReviewSubmissions`, `ReviewVersions`, `ReviewEvents`, `ReviewSeen` |
| approved knowledge files | `KnowledgeFiles` |
| the Ask lane (analyst and steward) | the same chat tables, with `kind` set to `analyst` or `steward` |
| compiled builds (`MERIDIAN_BUILDS_SOURCE=spanner`) | `Builds`, `BuildBundles`, `BuildBundleChunks` |

### 4.5 Still on the filesystem (by decision or pending)

| what | why |
|---|---|
| the graph and the Knowledge Catalog writes | The graph stays a compiler input. The build is what ships. |
| the Ask lane's own event log | Pending. The chat lane's sink pattern applies verbatim. |
| the build-graph run's read of `KnowledgeFiles` | Pending: an ingest step will export the active rows before the run. |
| the skill search index, sandbox scratch | Derived or temporary data, not records. |
| built-in and shared skill packs | Read from disk on every pod. The plan is to ship them in the build bundle (§6.6). |

### 4.6 Testing without a Spanner

`tests/fake_spanner.py` stands in for the SDK's `Database`: `snapshot()`
and `run_in_transaction()` over sqlite. The real `SpannerDatabase` code
runs unchanged against it, including typed parameters, `JsonObject` and
`BYTES` cells, and commit timestamps.

It already caught a real bug: `JsonObject` was imported from the wrong
package, so every dict cell would have failed on a real Spanner.

What the fake does **not** enforce: interleaving, foreign keys and
`CHECK` constraints. `spanner_check.py` covers those against a live
database.

---

## 5. Multi-tenancy and isolation

### 5.1 The decision: tenancy by deployment

- **One Spanner database per environment** (e1, e2, e3), selected by the
  one `.env` that `SAHS_ENV_FILE` names.
- **There is no `TenantId` column, and none is added.** Nothing in a row
  says which tenant it belongs to. The database it lives in says that.
- **Analogy:** separate buildings, not shared floors. There is no badge
  on each file saying which company owns it, because each company has
  its own building.

### 5.2 Isolation between people: by owner

| layer | mechanism |
|---|---|
| rows | every chat row carries `OwnerUserId`. Memories and feedback carry `UserId`. Own skills are keyed by `UserId`. |
| reads | every store read filters on the owner, so a read never crosses owners. A chat file's chat is checked against the owner. |
| runtime | each signed-in person gets their own `AssistantRuntime` (and Ask runtime), built on first use. Its store is bound to their user id. |
| routes | under a store, every `/api/chat/*` route requires the session cookie |
| query credentials | in user mode, BigQuery runs with the person's own Google token, so row-level access is Google's own answer for that person |
| observability | Langfuse traces are filed under `ChatSessions.OwnerUserId`, both live and in the backfill |

In #150 `AssistantRuntime.owner` changed to prefer the user id over a
display-name slug. Before that, two people with the same display name
could have shared an own-skills shelf.

### 5.3 Shared by design

These are shared by everyone on a deployment:

- the review board
- the approved knowledge files
- the built-in and governed skill packs
- the promoted build

Only the "seen" mark on the review board (`ReviewSeen`) belongs to the
person. Think of these as the building's shared library, not anyone's
room.

### 5.4 Roles and permissions (`sahs/identity/authorization.py`)

| role | surfaces | permissions |
|---|---|---|
| **admin** | admin console and Synapse | all: `chat.use`, `chat.autopilot`, `skills.own`, `skills.share`, `knowledge.stage`, `metrics.certify`, `graph.build`, `sources.manage`, `users.manage`, `audit.read` |
| **analyst** | Synapse | `chat.use`, `chat.autopilot`, `skills.own`, `knowledge.stage` |
| **steward** | Synapse | analyst permissions plus `metrics.certify` and `skills.share` |

Okta groups map to roles through `AUTH_GROUP_ROLE_MAP`. Without it,
nobody opens the admin console.

### 5.5 Seams left for business-unit scoping

`UserRoles.Scope` (a role granted within a scope) and
`KnowledgeFiles.BusinessUnit` exist today and stay `''`. Scoping by
business unit later needs **no schema change**. We add the rule when a
decision calls for it.

---

## 6. Skill files: how they work

### 6.1 What a skill is

A skill is a Markdown file of governed knowledge: metric definitions,
rules, sources, and the SQL shape for a domain. Portfolio Analytics,
Current-to-60, New Accounts and TLS are among the eight governed
skills. The model reads a skill to answer in the house's terms.

### 6.2 Where skills come from (one shelf, first match wins)

`collect_skills` merges these in order:

1. **Built-in packs**: `sahs/assistant/skills/*.md`, which ship with the
   code.
2. **The person's own skills**: `UserSkills` rows under a store, or the
   graph's `users/` folder in `local` mode. They are private to the
   person.
3. **The skills tree**: `MERIDIAN_SKILLS_DIR`, where the eight governed
   packs live, nested folders included. A file's name is its relative
   path as a slug, at most 64 characters.
4. **The graph shelf**: `<graph>/skills`.

When the same name appears twice, the first one wins. The skipped copy
is reported with its reason. `scripts/skills_check.py` lists every root,
every pack, its size, and whether it will load whole or as a library,
and says why any file was skipped.

### 6.3 From private to shared: the review board

A person saves an own skill (permission `skills.own`). A steward or
admin can promote it to the shared shelf (`skills.share`). Knowledge
files follow the same path: they are submitted, reviewed (optionally
with an AI pre-review), then approved, and on approval they are staged
into `KnowledgeFiles`. In `spanner` mode the board lives in the four
`Review*` tables.

### 6.4 Loading policy: whole when it fits, a library when it does not, never a refusal

**Analogy:** a short skill is a *briefing* handed over in full. A
multi-megabyte skill is a *library*. The model gets the card catalogue
(the full table of contents) and the pages that match the question, and
it can walk to any shelf to read more.

- **The whole-load limit per turn** is the smaller of two numbers:
  - `SAHS_MAX_SKILL_CHARS`: the global ceiling, default 4,000
    characters. Set it to 650,000 to let the big engines take a bundle
    whole.
  - The engine's whole-load budget: context window × 4 characters per
    token × 0.5. For Gemini 3.x (1M tokens) that is 2,097,152
    characters. An unknown engine is assumed small: 262,144 characters.
- **Under the limit:** the skill is pasted whole and verbatim,
  byte-identical to before (a golden-string test pins this).
- **Over the limit:** the skill loads as a **library**. It carries the
  table of contents (at most 35% of the budget) plus the top matching
  passages under the engine's library budget. That budget is 120K
  characters for 3.1 Pro, 80K for 3.7/3.5 Flash and 40K for Flash Lite,
  scaled by the thinking level from 2 to 8 passages. The model gets
  three tools:
  - `skill_toc(name, under?)`
  - `skill_search(query, skill?, k?)`
  - `skill_read(name, section, max_chars, offset?)`
- **The Ask navigator (v1)** has no tools, so it gets the same library
  statically at turn start.
- **No refusals.** `SkillTooLarge` and the fail-closed path were removed
  in #150. The only refusal left is a file that cannot be read (bad
  UTF-8, or frontmatter that never closes), and it is refused by name.
  If the index fails, it falls back to memory, or to a single chunk,
  rather than failing the turn.

### 6.5 Frontmatter: a preference, not a gate

```markdown
---
description: Settlement windows and the reconciliation runs
aliases: [settle, recon, late close]
runtime_loading: sectioned        # or full_file_required
truncation_allowed: true          # or false
---
```

- `full_file_required` means "whole whenever it fits". When it does not
  fit, the skill still loads as a library, and the record says
  `mode: library · preferred: whole` with the reason.
- `aliases` and `description` feed the **routing hint**: a lexical rank
  of the shelf that lists the likely skills first, marked "(likely)".

### 6.6 The index and its accuracy

- `sahs/loop/skill_index.py` chunks Markdown by structure:
  - sections follow the headings, and every chunk carries its heading
    breadcrumb
  - chunks are about 1,200 tokens with about 150 tokens of overlap
  - code fences and table rows are never split
- Indexing uses sqlite FTS5 with BM25, keyed by (skill name, content
  sha256, chunker version), so an unchanged skill costs zero work.
- **Measured on a synthetic 2.5 MB pack** (577 sections, 1,022 chunks):
  - 20/20 right section first on rephrased asks
  - 432/432 with one ask per section
  - 10/10 on the routing hint
  - 3 to 5 ms per search
- An embedding rerank has a seam (`Embedder`) but is not wired, on
  purpose.
- Every turn emits one **`skills_loaded`** record: each skill's mode,
  its characters, the limit, and the chunks served. This is what makes
  skill selection measurable in Langfuse.

### 6.7 Skills still to do (team, not code)

- Mark the eight governed skills' frontmatter: `full_file_required` on
  the contract skills, `sectioned` on the frameworks, and `aliases`.
- Ship the skills **inside the build bundle** rather than as a per-pod
  directory. That gives one promotion, identical pods, and a content
  hash per skill.
- Prove context caching on the gateway plane before loading the
  502K-character skill whole there.

---

## 7. How a turn works end to end (after #152)

1. The browser sends a message with the session cookie and the CSRF
   header.
2. The middleware resolves the person from `AuthSessions` (5-second
   cache). The person's own runtime is fetched or built.
3. The runtime reads the transcript, memories and project from Spanner.
4. **Planner gate:** if the ask is compound, it is planned into tasks
   (§3.5).
5. **Skill context:** each pinned skill is loaded whole or as a
   library. The `skills_loaded` record is emitted and the routing hint
   is computed.
6. The prompt is assembled. The prefix before `<skills>` is kept
   byte-stable (for context caching). A prompt fingerprint is recorded.
7. The model runs on the chosen `plane:model` at the chosen
   thinking-effort stop.
8. Tools run: SQL validation (including the sensitive-column policy),
   then the sandbox. The sandbox does a dry run, applies the cost gate,
   then runs, using the person's Google token in user mode.
9. Every event goes to the in-memory bus and to `ChatEvents`. Messages
   and artifacts go to their tables. Usage is settled onto
   `ChatSessions`.
10. Optionally, Langfuse mirrors the turn live, or later by backfill
    from `ChatEvents`.

---

## 8. Observability and evaluation (#143, #150)

- **Langfuse is a mirror, never on the model's path.** It is off by
  default (`SAHS_LANGFUSE=1` turns it on). The translator is a pure
  function of the event records.
- **Backfill from Spanner:** `langfuse_sync.py backfill --from spanner`
  replays `ChatEvents` with deterministic trace, span and score ids, so
  a second run creates nothing new. Compound turns appear as one trace
  with a `task:<id>` span per task.
- **Prompt fingerprints:** `ASSISTANT_VERSION` plus a hash of the prompt
  prefix and of each part, recorded on every generation. The parts are
  registered in Langfuse with labels.
- **Three datasets:**
  - **precedents:** analyst question-to-SQL pairs from a JSONL file
  - **silver:** turns that were thumbed up or finished clean
  - **scenarios:** built from `plan_made`
- `run_evals.py --sut assistant|planner` runs real turns against item
  files. It scores `verdict`, `pass` and `skill_hit`.
- `langfuse_sync.py coverage` prints which Langfuse concept is built
  from which Spanner columns.
- The guide is `synapse-agentic-harness-system/docs/runbooks/langfuse-insight.md`.

---

## 9. Environments and deployment (#150, #151)

- **One flag:** `SAHS_ENV_FILE` names the `.env`. The app, the pipeline
  and every check read that one file, and the shell's variables win.
- **Four profiles** (`synapse-agentic-harness-system/env/*.env.example`):

| profile | store | sign-in | model plane | builds from |
|---|---|---|---|---|
| `local` | sqlite | email and password | Vertex or the gateway | disk |
| `e1` (dev) | Spanner | Okta (the local door may stay open) | gateway | Spanner |
| `e2` (qa) | Spanner | Okta only | gateway | Spanner |
| `e3` (prod) | Spanner | Okta only (`make check` refuses `AUTH_LOCAL_LOGIN`) | gateway | Spanner |

- **Commands:** `make run ENV=e1`, `make check ENV=e3` (one readiness
  table covering settings, DDL, Spanner, Okta, gateway, Vertex, BigQuery
  and Google), `make test`, `make ddl-check`.
- **CI** (`.github/workflows/tests.yml`) runs both suites and the DDL
  lint. It skips pushes to a draft PR, and runs once when the PR is
  marked ready and on `main`.
- **Day-one checklist:** `docs/deploy.md`.
- **Carrying the work back to the enterprise repository:** the two
  repositories share no history, so commits cross one at a time.
  `docs/enterprise-port.md` lists every deliberate deviation.
  `docs/enterprise-carry-back.md` is the full procedure, with the final
  `.env` per environment. The sanitization scan
  (`docs/reports/sanitization-scan.md`) confirmed that nothing in the
  code says where it came from.

---

## 10. Quality and testing approach

- Every workstream ran in its own worktree and was merged in a known
  order. Each has a file-by-file report with exit codes.
- Anything a change must not alter is pinned by a test: simple turns
  byte-identical, skill rendering byte-identical, the prompt prefix
  identical across depths, fact digests identical across the KC perf
  change.
- Outside hosts are replaced by fakes at the seam: Okta, Google,
  BigQuery, Spanner and the model. Every check is placeholder-only, with
  no credentials in the repository.
- Laptop tests found real bugs, and those bugs became tests: the 422 on
  model choice, the stop that did not stop, the six auth bugs.

---

## 11. Decision log

| # | decision | alternatives considered | why we chose it |
|---|---|---|---|
| D1 | **Tenancy by deployment:** one Spanner database per environment, no `TenantId` | a shared database with a `TenantId` on every row | Simpler, and it cannot leak across tenants by a missed `WHERE`. Business-unit seams (`UserRoles.Scope`, `KnowledgeFiles.BusinessUnit`) exist without a schema change |
| D2 | **Isolation between people by owner column**, with one runtime per person | a sqlite file per person (the enterprise branch's shape) | One shared database that every pod sees. A pod restart loses nothing |
| D3 | **One `Database` interface, backed by Spanner or a sqlite stand-in** | Spanner only, or the emulator | Laptops and CI rehearse the deployment exactly. The fake Spanner caught a real bug |
| D4 | **The enterprise code lands as written.** Our deviations are listed and justified | rewriting it to our style | So changes can be cherry-picked back with a short, known list of differences |
| D5 | **Okta is the front door.** Email and password only behind `AUTH_LOCAL_LOGIN=1`, never in e3 | email and password everywhere | Enterprise SSO. The local door is for laptops and break-glass only |
| D6 | **One `/callback` for Okta and Google, with state in `AuthStates`** | two callbacks, state in process memory | Any pod can take the callback, and there is one URL to register per environment |
| D7 | **BigQuery as the person** (user mode), with the refresh token encrypted at rest | a service account for everyone | Row-level access is Google's own answer for that person. Audit shows who ran what |
| D8 | **Skills: whole when they fit, a library when they do not, never a refusal** | refusing large skills; truncating silently; plain RAG always | The research (long-context vs RAG) favours whole-load when it fits. A library with a catalogue keeps every verbatim reachable. Refusing blocked people; truncating silently was wrong |
| D9 | **Lexical FTS5/BM25 index, with an embedding seam but no embedding service** | a vector database now | 100% first-hit on the synthetic pack, deterministic, 3–5 ms, and no new infrastructure |
| D10 | **Skill frontmatter is a preference, not a gate** | fail-closed when `full_file_required` does not fit | The fail-closed path produced refusals, which D8 rules out. The record states the preference and why it was not met |
| D11 | **Multi-task turns gated in code; the model only plans when the gate fires** | the model decides every time | Simple asks stay byte-identical and pay no extra call. A bounded pool and a shared budget cap the cost |
| D12 | **No Redis for memory** | Redis or Memorystore as the memory store | Memory is already durable in Spanner, shared by every pod. Memorystore comes later, for hot state only, when there is more than one pod |
| D13 | **Cache the plan, re-run the query** (proposed) | a semantic cache of answers | A number computed under one person's credentials is never served to another. The key covers the build, skill hashes, model and plane |
| D14 | **The model's context cache over a byte-stable prompt prefix** | no caching | A 90% discount on cached input on Vertex. The prefix is already pinned |
| D15 | **Langfuse as a one-way mirror, rebuildable from Spanner** | Langfuse as the system of record | Spanner stays the truth, and Langfuse can be rebuilt at any time with deterministic ids |
| D16 | **Sensitive columns allowed by default, the read recorded; `deny` per environment** | always deny | The first launch needs these reads. Each read is on the record, and any environment can switch to deny |
| D17 | **One flag, four profiles, one readiness table** | per-environment code paths | What differs between environments is only the `.env`. `make check` proves each one |
| D18 | **Hostnames and endpoints come from the environment, never the source** | literals | This repository is public. The sanitization scan enforces it |
| D19 | **The graph stays on the filesystem in the first rollout.** The build is what ships (in Spanner) | moving the graph into Spanner now | The graph is a compiler input. The `003_graph.sql` tables exist, empty, for later |
| D20 | **Charts: a written selection heuristic, no pies, formatting from the data** | the model picks freely | Consistent, readable charts, with the reason stated on each one |

---

## 12. Open items and owners

The code for these is done. What remains is team action or a laptop
run.

1. **Apply DDL to E1:** `005`, `006`, `007`, `008`, `009` in order.
   Both sign-in hops return 503 without `AuthStates` (`006`). Run
   `make check ENV=e1` to see the diff.
2. **A laptop run of the real auth chain:** the Google consent screen,
   the scopes granted, the refresh-token rules, the corporate proxy and
   CA, IAM on the project (`docs/reports/auth-e2e.md`).
3. **Mark the eight governed skills' frontmatter**
   (`runtime_loading`, `aliases`).
4. **Prove context caching on the gateway plane** before loading the
   502K-character skill whole there.
5. **Ship the skills inside the build bundle.**
6. **Move the Ask lane's event log into `ChatEvents`.** Add the
   build-graph ingest of `KnowledgeFiles`.
7. **Attribute tokens per task** in multi-task turns. Today tasks share
   one budget.
8. **An evaluation from the analyst precedents** that scores skill
   selection and SQL separately.
9. **The roadmap from the research paper, in order:** retries and
   backoff, a BigQuery circuit breaker, plane fallback, the plan cache
   (`AnswerPlans`), warm-up in the readiness probe, then Memorystore
   when multi-pod.
10. **Add an index on `ChatArtifacts (ArtifactId)`** once tables grow
    past a laptop's worth.

---

## 13. Numbers

| | PR #140 | PR #150 | note |
|---|---|---|---|
| app tests | 84 | 186 | more added in #151 and #152 |
| harness tests | 445 | 684 | |
| Spanner tables in the DDL | 34 | 44 | 9 DDL files after #152 (`009` adds columns only) |
| persistence on Spanner | identity only | identity, chats, events, files, skills, knowledge, reviews, builds, usage | |
| CI | none | both suites plus the DDL lint | |
| KC: one table assembled | 166 ms | 9 ms | #142 |
| skill search | n/a | 3–5 ms, 100% first-hit on the synthetic pack | #150 |

---

## 14. Where to read more

| topic | document |
|---|---|
| PR-by-PR changelog | `docs/CHANGELOG-2026-09.md` |
| the map of #150 to #152 | `docs/reports/production-ready.md` |
| every table and the route that writes it | `docs/spanner-wiring.md` |
| deployment, profiles, tenancy | `docs/deploy.md` |
| the enterprise port and carry-back | `docs/enterprise-port.md`, `docs/enterprise-carry-back.md` |
| storage foundation, content to Spanner | `docs/reports/storage-foundation.md`, `docs/reports/content-to-spanner.md` |
| skill retrieval (design and accuracy) | `synapse-agentic-harness-system/docs/skill-retrieval.md`, `synapse-agentic-harness-system/docs/reports/skill-retrieval.md` |
| multi-task turns | `synapse-agentic-harness-system/docs/reports/multi-task-turns.md` |
| Langfuse | `synapse-agentic-harness-system/docs/reports/langfuse-insight.md`, `synapse-agentic-harness-system/docs/runbooks/langfuse-insight.md` |
| auth end to end | `docs/reports/auth-e2e.md` |
| laptop fixes | `docs/reports/backend-fixes-sensitive-model-skills.md`, `docs/reports/ui-fixes-stop-radix.md` |
| charts, chat UX | `docs/reports/visualizations.md`, `docs/reports/chat-ux-usage.md` |
| caches, memory, resilience decisions | `docs/research/resilience-accuracy-latency.md` |
| model profiles | `synapse-agentic-harness-system/docs/model-playbook.md` |
