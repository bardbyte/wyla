# Synapse Console — the agentic experience

The VP-grade surface for the semantic graph. One chat, two audiences,
full transparency: a Gemini 3.1 Pro agent that consumes the same tools
the MCP server exposes, and streams every step of its reasoning to a
React frontend so the room *watches it work*.

## Why this exists

The graph + tools are the substrate; `adk web` proves the agent works
but is a developer surface. This is the consumer experience — designed
for the bimodal audience (analysts want depth, VPs want the answer +
trust) on one principle:

> **The answer leads; the evidence is one gesture away.**

The clean conversation is what a VP reads. The trace rail — every tool
call as a human verb, every fact with its provenance — is what an
analyst opens. Same surface, both served, via progressive disclosure.

## Architecture

```
React SPA ──SSE──► FastAPI console-server ──► Runner ──► Gemini 3.1 Pro (Vertex)
   │  event stream                    │                  + build_analyst_tools():
   │  (§ Event protocol)              │                    · 15 GraphService tools (provenance)
   └── POST /approve ◄────────────────┘                    · gated warehouse (dry_run/execute + ledger)
          (HITL SQL gate)                                   · render_chart / render_dashboard
                                                            · run_python_analysis (sandbox)
```

The browser talks **only** to the console-server. Vertex creds, the MCP
tools, and the warehouse gate all live server-side. The frontend is a
**pure function of the event stream** — which is exactly why the stream
is a typed contract (`backend/events.py`) and why the whole UI can be
built and demoed against the offline `ScriptedRunner` before Vertex is
even wired.

Same tools as the MCP server, in-process: `build_analyst_tools()` wraps
`GraphService` directly, so the console and Claude-Desktop-over-MCP give
identical answers with identical provenance — the MCP server stays the
external contract; the console reuses the tool functions with one less
hop.

## Event protocol — the contract the React app renders

Every event is `data: {json}\n\n` over SSE. Type-tagged (`backend/events.py`):

| event | rendered as | pane |
|---|---|---|
| `turn_start` / `turn_end` | turn boundary + usage | — |
| `thinking` | model reasoning (latency-masking) | trace rail |
| `text` | streamed answer tokens | conversation |
| `tool_call` | a **human verb** ("Reading the metadata spine for sbs_new_accounts") | trace rail |
| `tool_result` | one-line summary + **provenance chip** (tier · score · sources) | trace rail |
| `sql_gate` | approve/deny beat: SQL + cost + guardrail checks | **modal / inline gate** |
| `sandbox` | code + output cell (it *computed*, didn't guess) | trace rail / inline |
| `artifact` | self-contained chart/dashboard HTML in a sandboxed iframe | conversation |
| `answer` | the contract: Answer / How I got there / Citations / Governance / Status | conversation |
| `error` | legible failure, never silent | conversation |

Legibility is a property of the protocol: `tool_call` carries the verb,
`tool_result` carries the rendered provenance — the frontend never
reverse-engineers raw tool JSON.

## Runners

- **`ScriptedRunner`** (default, offline) — deterministic golden
  transcripts for three intents: fused-witness governance answer, the
  guardrail refusal ("ask for `cm11_encrypted` on purpose"), and the
  live-SQL flow (gate → cost → approve → sandbox → chart). This is the
  frontend's dev fixture, the behavioral spec, and the tests. No creds.
- **`ADKRunner`** (laptop) — real Gemini 3.1 Pro via `google-adk` over
  the analyst tools; maps ADK's event stream onto `ConsoleEvent`s.
  Select with `SYNAPSE_CONSOLE_RUNNER=adk`.

The frontend cannot tell them apart — build the whole UI on scripted,
flip to real when Vertex is live.

## Run

```bash
# 1. build the SPA once (or `npm run dev` for hot reload on :5173)
cd apps/console/frontend && npm install && npm run build && cd -

# 2a. offline demo — scripted transcripts, sample-labeled reads, no creds
uvicorn apps.console.backend.app:app --port 8080
# open http://localhost:8080

# 2b. laptop, real graph + real agent — the SAME env contract as the
#     pipeline; nothing console-specific to learn.
#     Dependencies: use the certified pins (see requirements.txt) —
#     adk/genai are a matched pair, never upgraded one at a time:
#         pip install -r apps/console/requirements.txt
#     and always launch via `python -m uvicorn` from that venv (a bare
#     `uvicorn` can resolve to another interpreter). Confirm what the
#     server loaded at /api/config → sdk.
export GOOGLE_GENAI_USE_VERTEXAI=1
export GOOGLE_CLOUD_PROJECT=<project>
export GOOGLE_CLOUD_LOCATION=<region>
export GOOGLE_APPLICATION_CREDENTIALS=~/keys/svc.json   # never in-repo
export GEMINI_MODEL=gemini-3.1-pro-preview
export GEMINI_THINKING_BUDGET=-1          # dynamic thinking
export GEMINI_TLS_INSECURE=1              # intranet proxy (or GEMINI_CA_BUNDLE)
export SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json
# optional: SYNAPSE_AGENT_TOOLSET=classic (default) bounds the chat to
# the original single-graph agent's 12 capabilities + dry_run/execute;
# =full restores all 24 tools (charts, sandbox, craft skills)
SYNAPSE_CONSOLE_RUNNER=adk python -m uvicorn apps.console.backend.app:app --port 8080

# smoke it
curl -s localhost:8080/api/config          # non-secret env echo + graph liveness
curl -N localhost:8080/chat -H 'content-type: application/json' \
  -d '{"message":"who owns sbs_new_accounts?"}'
```

TLS is applied at server startup through the same `_apply_tls` path the
enrichment pipeline uses — the ADK runner never re-learns the
corporate-proxy lesson. Failures stream as `error` events whose message
names the fix (TLS → the env to set; 403 → the credential to check;
429 → retry), with the raw error preserved in brackets.

## Read-side API (the tabs)

Every payload carries `live: true|false` — graph-backed when
`SYNAPSE_GRAPH_PATH` resolves to a compiled snapshot, coherent labeled
samples otherwise. The UI shows which world it is in; the demo never
blurs real and illustrative.

| endpoint | serves |
|---|---|
| `/api/products` | Table nodes + context-readiness scorecards |
| `/api/metrics`, `POST /api/metrics/viability` | the metric canon + the copilot's canon-first check |
| `/api/terms/resolve` | canonical term + witnesses |
| `/api/graph/summary`, `/api/graph/thread` | counts by type/tier/witness + the curated storyline (`?table=` anchors it) |
| `/api/questions` | verified-answerable questions from the enrichment demo pack |
| `/api/witness?ref=` | the evidence panel behind any chip or citation |
| `/api/config` | the environment contract, booleans for anything credential-shaped |

## The intricacies (why craft matters here)

1. **Latency masking** — Gemini + thinking is slow; the trace makes the
   wait feel like watching an expert, not a spinner. The trace *is* the
   latency UX.
2. **HITL without breaking flow** — the `sql_gate` is a natural "shall I
   run this? (~1.2 GB)" beat, not a jarring modal.
3. **Tool legibility** — verbs + provenance chips, never raw JSON
   (`backend/verbs.py`).
4. **Grounding the model to actually use tools first** — agent-prompt
   work on `analyst_workflow`, invisible in the UI but decisive.
5. **Graceful failure** — same ethos as the enrichment failure digest.
6. **Sandbox as trust** — show the code *and* the output.

## Phase plan

- **Phase 0** ✅ — event protocol + Runner abstraction + ScriptedRunner
  golden transcripts + ADKRunner + FastAPI SSE + /approve + tests.
- **Phase 1** ✅ — the Radix SPA (`frontend/`): five tabs (Inquiries ·
  Data products · Metrics · Knowledge graph · Bring your knowledge),
  design tokens with light + dark themes, the brief document with
  Brief/Analysis modes and Thread/Ledger toggles, hold-to-run SQL gate,
  witness drawer, read-side API with graph-backed/sample-labeled reads,
  conversation memory in the ADK runner, thinking-config parity with
  the pipeline, legible failure mapping.
- **Phase 2** — resumable ADK loop (the gate genuinely pauses the
  agent), durable brief store, steward write-paths (metric signature,
  entity approval) surfaced in-app.
- **Phase 3** — reader/mobile projections of a brief, cost framing on
  the gate, connector previews go live.
- **Graduation** — ambient briefings: the verified demo questions
  surface proactively on a schedule.
