# Radix × ChatGPT — Engineering Implementation Brief

**Audience:** the engineer building the Radix ChatGPT connector
**Status:** build spec · v0.1 · read top-to-bottom, it's self-contained
**Companion file:** `radix_pipeline_sequence_mockup.html` — open it, click "Run the sequence," watch the phases. That's the experience we're building. This doc tells you how.

---

## TL;DR (the whole thing in one paragraph)

We control the *entire* user experience by streaming pipeline events from the Radix API directly into a ChatGPT widget over SSE. The model's only jobs are (1) call `radix.start_query` to kick us off and (2) call `radix.render_pipeline` to mount our widget. After that, the **widget talks to the Radix API directly** — we emit `intent → fields → explore → filters → partition → sql`, the widget renders each phase as it lands, then *pauses* at a human-in-the-loop confirm gate. User clicks **Run**, we execute through the governed MCP tool path, the number lands. Every user interaction (confirm, filter edit, explore swap, thumbs, golden) is emitted back, logged to Postgres keyed by `trace_id`, and routed to eval / golden / re-weighting. The model never sees the intermediate phases — it just narrates "here's your answer" after we're done. **We own the pixels and the latency; the model owns discovery and conversation.**

---

## 1. The one architectural decision everything hangs on

**Widget-driven SSE, not model-driven re-renders.**

The naive way to do a multi-phase reveal in Apps SDK is one tool call per phase, each re-rendering the widget. That's wrong: every tool call is a model round-trip (latency + tokens + nondeterminism), and the iframe remounts each time, losing state. OpenAI explicitly warns against attaching a widget to every tool call.

The right way: the widget mounts **once**, then opens its own `EventSource` connection back to the Radix API and streams the phases itself. This is allowed — confirmed in OpenAI's security docs and the CSP spec:

> `connectDomains` in `_meta.ui.csp` maps to the CSP `connect-src` directive, which governs `fetch()`, `XMLHttpRequest`, `WebSocket`, **and `EventSource`**. ([OpenAI Security & Privacy](https://developers.openai.com/apps-sdk/guides/security-privacy), [CSP field guide](https://dev.to/cptrodgers/mcp-app-csp-explained-why-your-widget-wont-render-9n1))

So the widget can hold a live SSE pipe to `radix-mcp.aexp.com` for the whole interaction. The Cards Against AI example in OpenAI's own repo uses exactly this pattern ("SSE state streaming and model-mediated actions").

**Why this is the unlock:** the heavy, beautiful, phase-by-phase UX runs at network speed between widget and our API — zero model involvement. The model is only in the loop at the two ends (start, narrate). The pipeline's existing SSE events (you already emit `entities_extracted`, `explore_scored`, `sql_generated`, etc. to the React app) get reused almost verbatim. We're not building a new streaming system; we're pointing the existing one at a new client.

**⚠ Risk to verify:** there was a reported bug (Oct 2025, [examples issue #85](https://github.com/openai/openai-apps-sdk-examples/issues/85)) where ChatGPT ignored custom `connect_domains` and applied a hardcoded CSP, blocking external API calls. The spec says it works; verify it *actually* works in our enterprise ChatGPT tenant. If it's still broken, fall back to **Pattern B** (§9). This verification is the scope of milestone **M0** (§10).

---

## 2. Tool surface (what the MCP server exposes)

Five tools. Three data (no widget), two render (widget attached). Keep render tools *thin* — they shape pre-validated data for the UI, nothing else.

| Tool | Type | Returns | Notes |
|---|---|---|---|
| `radix.start_query(question, conversation_id?)` | data | `{trace_id, stream_token}` | Kicks the pipeline off **async**. Returns immediately. `stream_token` = short-lived HMAC capability token scoped to this `trace_id` (5-min TTL). Not a credential. |
| `radix.render_pipeline(trace_id, stream_token)` | render | widget shell | The only tool with `_meta["openai/outputTemplate"]`. Widget mounts and opens the SSE stream. |
| `radix.execute_query(trace_id)` | data | `{rows, sql, bytes_scanned, duration_ms}` | Called by the widget via `window.openai.callTool` on confirm. **Privileged** — goes through MCP with server-side bearer token. This is the governed path. |
| `radix.log_event(trace_id, event_type, payload)` | data | `{ok}` | Fire-and-forget UI telemetry. Widget calls on every interaction. |
| `radix.save_golden(trace_id, tag?, notes?)` | data | `{golden_id}` | The ⭐ button. Routes to the golden dataset. |

The MCP server also returns **server-wide instructions** at init telling the model the playbook: *"For any data question, call `start_query` then `render_pipeline`. Do not call `execute_query` — the widget handles that. After the widget signals completion via update-model-context, give a one-line summary of the result."*

---

## 3. The emit sequence — Radix API → widget (SSE)

This is the heart of it. The widget opens `GET https://radix-mcp.aexp.com/stream/{trace_id}?t={stream_token}` and renders each event as it arrives. **You already emit most of these** — we're standardizing the schema for the ChatGPT client.

Every event is `{phase, status, ts, payload}`. The widget keeps a phase list and advances a checklist UI. The narration column below is what the user reads next to each row.

| # | Event `phase` | Emitted when | Payload (key fields) | Widget renders | User reads |
|---|---|---|---|---|---|
| 1 | `intent` | Phase-1 LLM returns `IntentSchema` | `{intent, metrics, dimensions, confidence}` | Checkmark + parsed intent | "Understanding your question" |
| 2 | `fields` | pgvector HNSW top-k done | `{matched_fields:[{name, similarity}]}` | Field chips appear | "Finding the right columns" *(semantic search)* |
| 3 | `explore` | Graph validation + multiplicative scoring done | `{explore, score, confidence, runner_up}` | Explore pill + confidence | "Picking the right table" |
| 4 | `filters` | Filter resolution (catalog enum) done | `{filters:[{col, val, op}]}` | Filter chips | "Applying your filters" |
| 5 | `partition` | Partition filter injected | `{field, default:"last 90 days"}` | Partition badge (cost-fence) | "Limiting the scan to keep it fast" |
| 6 | `sql` | Looker MCP generated SQL **(do NOT execute yet)** | `{sql, model, explore, fields, filters}` | SQL preview + **Run button** | "Here's the query. Run it?" |
| — | **CONFIRM GATE** | widget waits for user | — | Run / Edit / Cancel | *(user decides)* |
| 7 | `executing` | `execute_query` called, BQ running | `{estimated_bytes}` | Skeleton in result hero | "Running against BigQuery" |
| 8 | `result` | BQ returns | `{rows, row_count, shape}` | The **$12.4B** hero | *(the answer)* |
| 9 | `followups` | Phase-3 LLM (concurrent) | `{suggestions:[...]}` | Follow-up chips | "Try next…" |
| 10 | `done` | stream closes | `{trace_id}` | finalize + `setWidgetState` | — |

**Branch events** (replace the happy path mid-stream):

| Event `phase` | Emitted when | Widget renders |
|---|---|---|
| `clarify` | all candidates < `CONFIDENCE_FLOOR` (0.70) | Clarify card — 2-3 options + "rephrase" |
| `disambiguate` | `runner_up/top > NEAR_MISS_RATIO` (0.85) | Two-explore comparison |
| `filter_dropped` | off-catalog value post-validation | Strikethrough chip + fix-it |
| `error` | Looker/BQ/firewall failure | Inline error + plain-English cause |

**Confirm gate detail (the most important behavior in the whole flow):** the pipeline runs phases 1-6 *speculatively* — intent through SQL generation are cheap and read-only, so we do them before asking. We stop dead at `sql`. The expensive, billable BigQuery scan only fires after the user clicks Run. This is the trust contract: *we show you exactly what we're about to do, then you authorize it.* For power users, expose a "Run automatically when confidence ≥ 0.9" toggle in `widgetState` so a trusted user can skip the gate once they've built confidence in the system.

---

## 4. The inbound events — widget → backend (user interactions)

Everything the user does is emitted back. Two transports:

- **Privileged actions** (mutate / spend / govern) → `window.openai.callTool` → MCP tool → bearer-token'd server call. (Just `execute_query` and `save_golden`.)
- **Telemetry** (everything else) → `radix.log_event` via `callTool`, or `navigator.sendBeacon` on teardown for abandon detection.

| Event `type` | Trigger | Payload | Transport |
|---|---|---|---|
| `confirm_run` | clicked **Run** | `{trace_id}` | callTool → `execute_query` |
| `filter_edited` | changed a chip value | `{field, from, to}` | log_event |
| `filter_removed` | ✕ on a chip | `{field}` | log_event |
| `explore_swapped` | picked a different explore | `{from, to}` | log_event (then re-stream) |
| `disambiguation_resolved` | chose an explore on the disambiguate card | `{chosen, rejected, candidates}` | log_event |
| `clarify_resolved` | picked / rephrased on clarify card | `{chosen_option \| "rephrase"}` | log_event |
| `filter_fixed` | resolved a dropped filter | `{field, picked_value}` | log_event |
| `thumb` | ▲ / ▽ | `{direction}` | log_event |
| `golden_saved` | ⭐ Golden | `{trace_id, tag?, notes?}` | callTool → `save_golden` |
| `sql_inspected` | opened SQL fullscreen | `{}` | log_event |
| `expanded` | opened result canvas | `{}` | log_event |
| `followup_clicked` | clicked a suggestion chip | `{suggestion}` | sendFollowUpMessage + log_event |
| `followup_ignored` | new turn started, chips untouched | `{suggestions}` | log_event |
| `shared` | clicked share | `{format}` | log_event |
| `abandoned` | iframe teardown before `confirm_run` | `{last_phase}` | sendBeacon |
| `reask` | same/similar question within 60s | `{prev_trace_id}` | server-derived |

---

## 5. How inbound events get *used* — the evaluation loop

This is the reason we collect any of it. Three consumers, by signal grade.

### 5.1 Eval harness (regression set)
Corrections are the most valuable test cases because they're labeled by a real analyst. Build a nightly job that pulls:
- `explore_swapped` → `(question, wrong_explore, right_explore)` triples
- `filter_edited` → `(question, wrong_value, right_value)` pairs
- `disambiguation_resolved` → `(question, chosen, rejected)` triples

Replay each `question` through the current `resolve` pipeline. Assert the new output matches the *corrected* answer. This is your guardrail against scoring regressions when anyone touches `filters.py`, the embedding model, or the multiplicative weights. Surface pass-rate in CI.

### 5.2 Golden dataset (training / few-shot) → offline training pipeline
Three sources, in descending purity:
1. `golden_saved` — analyst explicitly vouched. Highest signal.
2. `disambiguation_resolved` — explicit choice under known competing options. High signal.
3. `confirm_run` + `thumb:up` within the same trace — confirmed *and* praised. Good signal.

Insert `(question, explore, fields, filters, result_hash)` into `golden_dataset`. Dedup on `(question_normalized, explore)`. These become few-shot examples for the Phase-1 LLM and seeds for the field-embedding catalog.

### 5.3 Re-weighting candidate queue
`explore_swapped` and persistent `filter_edited` patterns feed a queue a human reviews weekly. If a question shape *consistently* resolves to the wrong explore, that's a signal to adjust the multiplicative scoring (e.g., bump `base_view_bonus`, or add a description-similarity term) or to enrich that explore's LookML description. Don't auto-apply; queue for review.

### 5.4 Drift dashboard (for platform owners)
Thumbs are noisy and skew negative — **treat them as a drift gauge, not labels** ([IrisAgent](https://irisagent.com/blog/the-power-of-feedback-loops-in-ai-learning-from-mistakes/)). Pair with implicit signals that are harder to game:
- `sql_inspected` rate ↓ over time = rising trust
- `abandoned`-at-`sql` rate = the confirm gate is scaring people or the SQL looks wrong
- `followup_clicked` rate = Phase-3 quality
- `reask` rate = answer dissatisfaction
- per-phase p50/p95 latency, `bytes_scanned` distribution

---

## 6. Storage — the schema

Four tables in Postgres (you already have `trace_store` in-process; persist it). All keyed by `trace_id`. `user_sub` = the OAuth `sub` claim from ChatGPT, mapped to the Amex internal identity server-side.

```sql
-- One row per query attempt. The full resolution + execution record.
CREATE TABLE radix_trace (
  trace_id        TEXT PRIMARY KEY,
  user_sub        TEXT NOT NULL,
  conversation_id TEXT,
  question        TEXT NOT NULL,
  intent          JSONB,             -- IntentSchema output
  resolution      JSONB,             -- {explore, fields, filters, partition, confidence}
  candidates      JSONB,             -- scored explore candidates (for eval)
  sql             TEXT,              -- LookML-generated SQL
  bytes_scanned   BIGINT,
  durations_ms    JSONB,             -- {intent: 612, fields: 15, ...}
  result_summary  JSONB,            -- {row_count, shape, hero_value}
  status          TEXT,              -- resolved | confirmed | clarified | disambiguated | error
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- One row per user interaction. The event firehose.
CREATE TABLE radix_ui_event (
  event_id   BIGSERIAL PRIMARY KEY,
  trace_id   TEXT REFERENCES radix_trace(trace_id),
  user_sub   TEXT NOT NULL,
  event_type TEXT NOT NULL,          -- confirm_run | filter_edited | thumb | ...
  payload    JSONB,
  ts         TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON radix_ui_event (trace_id);
CREATE INDEX ON radix_ui_event (event_type, ts);

-- The curated Q->A pairs that feed evaluation + few-shot.
CREATE TABLE radix_golden (
  golden_id   BIGSERIAL PRIMARY KEY,
  trace_id    TEXT REFERENCES radix_trace(trace_id),
  question    TEXT NOT NULL,
  explore     TEXT NOT NULL,
  fields      JSONB NOT NULL,
  filters     JSONB NOT NULL,
  result_hash TEXT,                  -- detect when underlying data drifts
  source      TEXT,                  -- golden_button | disambiguation | confirmed_thumbsup
  tag         TEXT,
  notes       TEXT,
  created_by  TEXT NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- Human-reviewed queue for scoring adjustments.
CREATE TABLE radix_reweight_queue (
  id          BIGSERIAL PRIMARY KEY,
  trace_id    TEXT,
  question    TEXT,
  kind        TEXT,                  -- explore_swap | filter_correction
  before      JSONB,
  after       JSONB,
  status      TEXT DEFAULT 'pending',-- pending | reviewed | applied | rejected
  created_at  TIMESTAMPTZ DEFAULT now()
);
```

Retention: `radix_ui_event` is high-volume — partition by month, keep 13 months. `radix_trace` and `radix_golden` keep indefinitely (governance + training value). Scrub `question` text through the same PII firewall the LLM path uses before persisting.

---

## 7. Auth & CSP — the enterprise-critical config

Amex governance means this section is non-negotiable. Two rules:

1. **No credentials in the iframe, ever.** OpenAI's guidance is explicit: privileged operations go through MCP tool calls with bearer tokens held server-side; the iframe never sees them. So `execute_query` and `save_golden` route through `callTool`, where ChatGPT attaches the OAuth context and our server resolves the bearer token. The iframe only ever holds the `stream_token` (a read-only capability scoped to one `trace_id`, 5-min TTL, HMAC-signed — useless for anything else).

2. **Declare exactly one connect domain.** The widget's resource `_meta`:

```jsonc
"_meta": {
  "openai/outputTemplate": "ui://radix/pipeline.html",
  "ui": {
    "csp": {
      "connectDomains": ["https://radix-mcp.aexp.com"],   // fetch + EventSource live here
      "resourceDomains": ["https://radix-cdn.aexp.com"]    // fonts, JS bundle
    }
  }
}
```

`connectDomains` is what permits the SSE stream. If a domain isn't listed, the browser blocks the request before it leaves the iframe. Get `radix-mcp.aexp.com` allow-listed with our OpenAI enterprise partner contact if the default CSP fights us (see §1 risk).

User identity: on first connect, ChatGPT runs OAuth against our IdP; we store the `sub` → Amex-employee mapping. Every `trace_id` is stamped with `user_sub` so row-level security (which Looker enforces anyway) and audit both work.

---

## 8. Sequence diagram

```
USER            CHATGPT (model)        WIDGET (iframe)         RADIX API              LOOKER / BQ
 │                    │                      │                     │                       │
 │ "plat billed Q4"   │                      │                     │                       │
 ├───────────────────>│                      │                     │                       │
 │                    │ start_query(q)       │                     │                       │
 │                    ├─────────────────────────────────────────>  │ kick pipeline (async) │
 │                    │ <{trace_id, token}── ─────────────────────  │                       │
 │                    │ render_pipeline(id,t)│                     │                       │
 │                    ├────────────────────> │ mount               │                       │
 │                    │                      │ EventSource(/stream)│                       │
 │                    │                      ├───────────────────> │                       │
 │                    │                      │ <══ intent ════════ │                       │
 │                    │                      │ <══ fields ════════ │ (pgvector)            │
 │                    │                      │ <══ explore ═══════ │ (graph + score)       │
 │                    │                      │ <══ filters ═══════ │                       │
 │                    │                      │ <══ partition ═════ │                       │
 │                    │                      │ <══ sql ═══════════ │ ──gen SQL──>          │
 │                    │                      │  [CONFIRM GATE]     │                       │
 │ click "Run" ───────────────────────────> │                     │                       │
 │                    │  callTool(execute)   │ confirm_run         │                       │
 │                    │ <────────────────────┤ ───────────────────>│ ──run──> │            │
 │                    │                      │ <══ executing ═════ │          │ scan       │
 │                    │                      │ <══ result ════════ │ <─rows───┤            │
 │                    │                      │ <══ followups ═════ │                       │
 │                    │ update-model-context │ $12.4B rendered     │                       │
 │                    │ <────────────────────┤                     │                       │
 │ <─ "Here's your    │                      │                     │                       │
 │    answer: $12.4B" │                      │                     │                       │
```

Note the model is idle during the entire phase stream. It wakes up only to start us and to narrate the close.

---

## 9. Pattern B — the fallback (only if SSE is blocked)

If the §1 spike shows the CSP genuinely blocks our `EventSource`, degrade gracefully: collapse phases into **two** tool calls.
- `radix.resolve_and_render(question)` → runs phases 1-6, returns a widget showing the full resolution + SQL + Run button in one shot (no live reveal, just the finished plan).
- User clicks Run → `radix.execute_query` → second render with the result.

You lose the cinematic phase-by-phase reveal but keep the confirm gate, the transparency, and the feedback loop. Ship Pattern A; keep Pattern B coded as a feature flag.

---

## 10. Work breakdown (M0–M4)

The connector is divided into five milestones. All five are in scope; each names a coherent slice of the work and the gate that proves it's done.

| Milestone | Scope | Gate |
|---|---|---|
| **M0 — SSE spike** | Prove a widget can hold an `EventSource` to `radix-mcp.aexp.com` in our ChatGPT tenant. Nothing else. | If green → Pattern A. If red → Pattern B + escalate to OpenAI partner. |
| **M1 — Happy path** | `start_query`, `render_pipeline`, the 10 phase events, confirm gate, `execute_query`, result hero. | An analyst gets a number end-to-end in chat. |
| **M2 — Feedback wiring** | All inbound events → `radix_ui_event`. `save_golden`. Filter-chip edits re-stream. | Events land in Postgres, golden button works. |
| **M3 — Branches** | `clarify`, `disambiguate`, `filter_dropped`, `error` cards. | Failure modes don't dead-end the user. |
| **M4 — Eval loop** | Nightly regression job from corrections; drift dashboard. | Scoring changes are guarded by analyst-labeled tests. |

---

## 11. Things I need you to verify (don't assume)

1. **The CSP/SSE spike** (§1). Everything depends on it.
2. Whether `window.openai.callTool` round-trips fast enough that the confirm→execute→result feels instant (it adds a model hop). If laggy, consider letting the widget POST `execute` directly to the API and only using `update-model-context` to inform the model after.
3. Whether `ui/update-model-context` reliably gives the model the final number so its narration isn't hallucinated. Test the narration accuracy explicitly.
4. `widgetSessionId` behavior across the fullscreen→inline transition (there's a known Android remount bug — [community thread](https://community.openai.com/t/android-native-chatgpt-app-requestdisplaymode-remounts-widget-from-scratch-internal-navigation-state-is-lost-on-fullscreen-to-inline-transition/1378315)). Persist all widget state to `setWidgetState` so a remount is survivable.
5. Enterprise tenant: confirm developer-mode / connector install is even enabled for our Amex ChatGPT org, and who owns the OpenAI partner relationship for domain allow-listing.

---

*Open the companion mockup (`radix_pipeline_sequence_mockup.html`) alongside this doc. Click through the sequence. That's the spec made visible.*
