# 10 · Web Surfaces and the Event Stream

**Relevant source files**

- [`apps/synapse_admin/backend/app.py`](../../apps/synapse_admin/backend/app.py)
- [`apps/synapse_admin/backend/meridian.py`](../../apps/synapse_admin/backend/meridian.py) — the read plane
- [`apps/synapse_admin/backend/chat.py`](../../apps/synapse_admin/backend/chat.py) · [`ask.py`](../../apps/synapse_admin/backend/ask.py)
- [`apps/synapse_admin/frontend/js/main.js`](../../apps/synapse_admin/frontend/js/main.js) · [`pages/`](../../apps/synapse_admin/frontend/js/pages/)
- [`apps/synapse/frontend/js/main.js`](../../apps/synapse/frontend/js/main.js)
- [`apps/synapse_admin/design/wireframes/`](../../apps/synapse_admin/design/wireframes/)

---

## Purpose and Scope

Two web surfaces, one server, one build. How the read plane projects the
build, how a turn streams, and what each page does.

---

## One process, zero build steps

```python
"""Synapse by Lumi — the product server.

One process, zero build steps: FastAPI serves the Meridian read plane
under ``/api/meridian/*`` and the hand-authored frontend (ES modules,
no bundler, three.js vendored locally) from ``frontend/``."""
```

```bash
uvicorn apps.synapse_admin.backend.app:app --port 8400
```

| mount | surface |
|---|---|
| `/` | **Synapse by Lumi** — the steward console |
| `/synapse/` | **Synapse Semantic Intelligence** — the ask surface |
| `/api/meridian/*` | the read plane |
| `/api/synapse/{planes,brand,logo}` | machine capability + branding |
| `/health` | liveness |

> CORS is open for local dev — **lock it down before any non-localhost
> deployment.**

### No bundler, on purpose

Plain ES modules, a hash router, `three.js` vendored locally so the
cosmos works offline. There is no build step for the frontend at all:
edit a file, reload the page. For a tool whose whole thesis is "read the
code and the context window," a toolchain between you and the source is a
tax.

---

## The read plane

```python
"""Meridian read plane — the canonical copy.

Read-only projections of the compiled Meridian build … One reader
implementation — ``sahs.tools.api.Build`` — imported from the silo,
never re-parsed here."""
```

| endpoint | returns |
|---|---|
| `GET /api/meridian/home` | build id, counts, planes, freshness |
| `GET /api/meridian/sources` | the Sources shelf: every ingestion source with its chip, blurb and contribution |
| `GET /api/meridian/explorer/metrics?q&status&lob` | the metric explorer |
| `GET /api/meridian/explorer/tables` | the table explorer with texture |
| `GET /api/meridian/metric/{metric_id}` | the metric profile |
| `GET /api/meridian/table/{physical}` | the table profile + column detail |
| `GET /api/meridian/graph_map` | the cosmos projection |
| `GET /api/meridian/builds` | build history |
| `GET /api/meridian/enrich_runs` | enrichment runs and their grades |
| `GET /api/meridian/artifacts` · `artifact_file?rel=` | the knowledge/skills shelf |
| `POST /api/meridian/artifacts` | stage an artifact |
| `POST /api/meridian/feedback` | **the only write** |

### The only write, and its boundary

> The ONLY write here is the feedback affordance: append-only JSONL under
> `graph/runs/feedback/` — quads-adjacent records a steward can review,
> **never graph writes (those stay with the clerk).**

"Quads-adjacent" is a precise choice of words. Feedback lands next to the
graph, in the same run tree, in the same append-only shape — so a steward
can read it with the same tools — but it is not a quad and it does not
fold into truth.

### The reality law, again

> when no compiled build exists every endpoint answers
> `{"available": false, "reason": …}` with **HTTP 200** and the UI renders
> its designed empty state. **Nothing is mocked, ever.**

HTTP 200 is deliberate: "there is no build" is a *successful* answer to
"what is in the build." A 404 would make an ordinary state look like a
bug.

### Planes reported as booleans

```python
@app.get("/api/synapse/planes")
```

Which capabilities this machine actually carries — BigQuery configured or
not, the model plane configured or not, live execution allowed or not —
**as booleans, never secrets.** The Home page uses it to say honestly
what this install can do. See [Page 13](13-configuration.md).

---

## Surface 1 · Synapse by Lumi (the steward console)

Routes: `#/home` `#/semantics` `#/tables` `#/cosmos` `#/artifacts`
`#/skills` `#/operate` `#/ask` `#/ask/<session>` `#/chat`
`#/metric/<id>` `#/table/<physical>`

> Deep links work: **a metric profile is a URL you can send someone.**

| page | what it is for |
|---|---|
| **Home** | the build, the counts, the planes, what changed |
| **Semantics Explorer** | every metric, filtered by status and line of business, with tier marks |
| **Metric Profile** | the definition line, canonical SQL, witnesses, variants, group memberships, usage texture |
| **Tables** / **Table Profile** | the warehouse with its meaning: columns, sensitivity, joins by family, ownership vs usage, partitions |
| **Cosmos** | the graph as a 3-D sky |
| **Artifacts / Skills** | the knowledge shelf: packs and knowledge files with authors and last writes, search, browse, add, a reader |
| **Operate** | per-turn instrumentation and transcripts — *the weekly reading ritual* |
| **Ask** | the deterministic lane ([Page 9](09-ask-lane-a2ui.md)) |
| **Chat** | the assistant lane ([Page 8](08-agent-harness.md)) |

### The cosmos

```javascript
/** Cosmos: the sky, from the compiler's graph_map.json (positions
 * baked at compile; this page only renders). three.js is vendored
 * locally: no CDN, works offline. Encoding: size = usage, glow =
 * trust tier, gold star = held by multiple domains. */
```

| visual | encodes |
|---|---|
| size | usage |
| glow | trust tier (`ha` / `gr` / `in` / `gu`) |
| gold star | held by multiple domains |
| wells | domains |
| edge kind | joins · membership · computed-from |

The layout is **baked at compile time**, seeded from sha256 of stable
identifiers — no RNG — so the same build always renders the same sky and
*a diff in the map is a diff in the data.* Click a body → the rail, with
a real link to its profile.

Hue stays in one family; **trust reads as glow plus the rail's tier
words**, so the encoding survives colour-blindness and a monochrome
screenshot.

---

## Surface 2 · Synapse Semantic Intelligence (the ask surface)

```javascript
/** Synapse Semantic Intelligence: the chat first, the library under it.
 * Routes: #/chat #/chat/<session> #/search #/products
 *         #/product/<physical> #/metrics #/metric/<id> #/skills #/memory */
```

Same server, same API, same build; a stripped nav and a different centre
of gravity. Tables are **data products**; the chat is the front door;
artifacts publish inside the conversation.

The vocabulary difference is the product difference: a steward browses
*tables*, an analyst asks about *products*.

---

## The chat surface as a pure consumer

```javascript
/** A pure consumer of the assistant event stream: nothing here calls
 * a model, holds a key, or invents a value. Artifacts render exactly
 * what the validator stored — including the EXPLORATORY watermark —
 * and exports carry the provenance footer. No harness words reach
 * the user: no transcript dump, no JSON, no tool ids. */
```

### The turn API

| route | purpose |
|---|---|
| `POST /sessions` · `GET /sessions` · `GET /sessions/{id}` | session lifecycle; the GET carries `turn_after` for mid-turn reattach |
| `POST /sessions/{id}/messages` | start a turn (202) |
| `POST /sessions/{id}/run` | **execute a proposal — no model call** |
| `POST /sessions/{id}/chart` | **chart saved rows — no model call** |
| `POST /sessions/{id}/stop` | the stop button (shares the breaker's abort path) |
| `POST /sessions/{id}/model` · `GET /dials` | model and depth |
| `GET/POST/DELETE /sessions/{id}/files` · `GET /files/support` | attachments and what is honestly supported |
| `GET /search` | fuzzy search across every chat |
| `GET /skills` · `POST /skills/draft` · `POST /skills/mine` · `DELETE /skills/mine/{name}` | the skill shelf, including *draft one with the assistant* |

### The event family, in the order a healthy turn emits it

```python
ASSISTANT_EVENTS = (
    "turn_started",
    "model_prompt",   # what the model saw: system once, then steps
    "thinking",       # a thought summary delta, the model's own
    "tool_call",      # a call announced, before it runs
    "tool_step",      # one look: tool, args, the compact summary
    "tool_result",    # the full result behind the summary
    "say_token",      # streamed assistant prose
    "artifact",       # created/updated: the full spec rides
    "proposal",       # a query handed over: the card with Run on it
    "chips",          # follow-up suggestions when the turn ends
    "budget_tick",
    "turn_done",
    "error",
)
```

> There is no classify, no resolve_started, and no contract gate — **the
> model drives; the harness streams and records.**

### Four surface behaviours that were bugs first

**A turn belongs to the server, not the tab.**

> Leaving the page closes only the listener; coming back to a session
> mid-turn reattaches from the turn's first event (`turn_after` on the
> session GET) and replays it whole, so switching chats or tabs never
> stops or loses a turn.

**The thinking block folds, and the trace is kept.** One block streams
the model's own thought summaries interleaved with steps ("Searching the
graph for enrolments — 16 results"); its header is the live line with the
seconds ticking. When the answer starts it folds to *"Thought for 34s ·
searched the graph, read the cards"* and stays expandable. A turn that
used no tools and thought nothing shows **no block at all**.

Verbs deduplicated; **no tool names, no ids, no raw output.** The prompts
and full results live in Operate → Transcripts, not in the chat.

**The live line has a heartbeat, and each model call restarts the
clock** — so a long think never wears a tool's name. ("Checking the
query…" while the *model* was the one working was the first stall
anybody noticed.)

**`[hidden] { display: none !important }` globally, and the browser walk
asserts visibility, not the property.** That single CSS specificity bug
is why the thinking line and the artifact panel once never hid.

### The artifact panel

Model-invoked, and a drawer: it opens on an artifact **in this
interaction**, never on reopening an old chat; a card in the transcript
reopens it; closing reflows the chat to full width.

A table artifact is treated as a report: a summary strip of the data pool
first — the rows and their date span, each numeric column's total, range
and mean with its shape — then the rows with a sticky header. **Every
number in the strip is computed from the artifact's own rows.**

The masthead carries the title and Share, never tokens or a build id:
*the build travels on every artifact's footer.*

---

## The design provenance

`apps/synapse_admin/design/wireframes/` holds the design canvas the
surface came from — `.dc.html` artboards for Admin Home, Table Profile,
Metric Profile, Semantics Explorer, Graph Cosmos, Agent Theater, Builds
and Diffs, Enrichment Runs, Evals Dashboard, Sessions, plus nine numbered
flows (F1 Analyst Conversation … F9 Budget Grace).

Keeping them in-tree means a UI question has a source of truth that is
not a screenshot in a chat thread.

---

## Next

→ [Page 11 · Governance, Review and Enrichment](11-governance-enrichment.md)
