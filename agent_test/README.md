# agent_test/ — Vertex AI → ADK → Gemini smoke test

Minimal end-to-end check that ADK can run an `Agent` against Gemini 3.1 Pro on
Vertex AI in the certified-access GCP project (`prj-d-ea-poc`). Two modes:

| Mode | When | Command |
|---|---|---|
| **CLI** | Quick smoke test, scriptable | `python agent_test/run.py --key-file ~/Downloads/key.json` |
| **Web UI** | Learn ADK features, inspect events live | `adk web apps/` (after sourcing env) |

## Layout

```
agent_test/                  # the agent + CLI runner
├── __init__.py              # injects truststore for corporate TLS
├── agent.py                 # Agent definition + tools + module-level root_agent
├── run.py                   # CLI runner (sets env, prints event trace)
├── setup_vertex_env.sh      # source me before `adk web`
└── README.md                # this file

apps/                        # AGENTS_DIR for `adk web`
└── vertex_smoke/
    ├── __init__.py
    └── agent.py             # re-exports agent_test.agent.root_agent
```

The single source of truth for the agent definition is `agent_test/agent.py`.
`apps/vertex_smoke/agent.py` is just a 3-line re-export so `adk web apps/`
discovers exactly one agent.

## Run mode 1 — CLI

```bash
# Easy path (truststore auto-handles corporate TLS if installed)
python agent_test/run.py --key-file ~/Downloads/key.json

# Bypass corporate TLS without truststore (intranet only)
python agent_test/run.py --key-file ~/Downloads/key.json --insecure

# Different model
python agent_test/run.py --key-file ~/Downloads/key.json --model gemini-2.5-pro

# Custom prompt
python agent_test/run.py --key-file ~/Downloads/key.json \
    --query "Roll a 20-sided die three times and tell me which results are prime."

# Machine-readable
python agent_test/run.py --key-file ~/Downloads/key.json --json
```

Output is the full event trace ending in `PASS — Vertex AI → ADK → Gemini smoke test succeeded.`

## Run mode 2 — `adk web` (Web UI)

`adk web` is ADK's React-based dev console. You send messages, watch events
stream in, inspect session state, see tool calls expand into JSON, replay
runs, and toggle settings — all without writing any UI code.

### One-time setup

```bash
pip install truststore   # so adk web can talk to Vertex through the corporate proxy
```

### Every-session setup

```bash
# 1. Set the four env vars ADK needs (via the helper script).
#    Pass the path to your service-account JSON; defaults to ~/Downloads/key.json.
source agent_test/setup_vertex_env.sh ~/Downloads/key.json

# 2. Start the web UI.
adk web apps/
```

Open `http://localhost:8000` (or whatever port adk prints).

### What you'll see — left-to-right tour

```
┌────────────────────────┬─────────────────────────────────┬──────────────────────────────┐
│  AGENTS                │  CHAT                           │  EVENTS / STATE / TRACE      │
│  ─────                 │  ────                           │  ──────────────────────────  │
│  > vertex_smoke   ✓    │  user: "Roll an 8-sided die..." │  Event 1  vertex_smoke_agent │
│                        │                                 │    type: tool_call           │
│  Sessions              │  agent:                         │    name: roll_die            │
│  ────────              │   [calls roll_die...]           │    args: {sides: 8}          │
│  > smoke-session  •    │   [calls check_prime...]        │                              │
│                        │   "I rolled a 5 — it's prime."  │  Event 2  ...                │
│                        │                                 │                              │
│                        │  [send box]                     │  [State] [Tools] [Trace]     │
└────────────────────────┴─────────────────────────────────┴──────────────────────────────┘
```

### Step-by-step learning walkthrough

1. **Pick the agent.** Sidebar shows `vertex_smoke` (registered via `apps/`). Click it.

2. **Start a new session.** ADK creates an in-memory session — every event gets logged here.

3. **Send a message:**
   > Roll an 8-sided die for me, then tell me whether the result is prime.

4. **Watch the events panel** stream in real time:
   - `user_message` — your prompt.
   - `model_response` with a `function_call` part — the LLM decided to call `roll_die`. You see the tool name and arguments (e.g., `{"sides": 8}`).
   - `function_response` — your Python `roll_die()` ran and returned, e.g., `5`.
   - `model_response` with another `function_call` — Gemini chains: it now calls `check_prime` with `{"nums": [5]}`.
   - `function_response` — `check_prime` returned `"5 are prime numbers."`.
   - `model_response` (final, no tool calls) — the natural-language answer: `"I rolled a 5 — it is a prime number."`.

5. **Open the State panel.** You'll see `rolls: [5]` — written by the `tool_context.state["rolls"]` line in `roll_die()`. Roll again and the list grows: `[5, 7]`.

6. **Open the Tools panel.** ADK introspected the Python function signatures + docstrings and generated structured tool schemas Gemini can call. You can see the auto-derived JSON Schema for each parameter.

7. **Open the Trace panel.** Tree view of the run: `user → model_call_1 → tool_call(roll_die) → tool_response → model_call_2 → tool_call(check_prime) → tool_response → model_call_3 → final_response`. The ReAct loop made visual.

8. **Inspect token cost.** Hover any model_response — usage metadata shows `prompt_tokens`, `response_tokens`, `total_tokens`. Useful for budgeting.

9. **Replay / fork.** ADK lets you fork a session at any point, edit the user message, and re-run from there. Quick A/B for prompt changes.

10. **Settings (gear icon).** Toggle streaming mode (token-by-token vs one-shot), change `max_llm_calls`, swap models.

### What ADK features this exercises

| Feature | Where you see it |
|---|---|
| `LlmAgent` | The agent itself — pure declarative setup in `agent.py` |
| Tool auto-registration | Tools panel shows `roll_die` + `check_prime` with auto-derived schemas |
| `ToolContext` (session state) | State panel shows `rolls: [...]` accumulating across calls |
| Reason→Act loop (ReAct) | Trace panel visualizes the back-and-forth |
| Function calling | Events panel shows `function_call` + `function_response` parts |
| Generation config | `temperature=0`, safety_settings=OFF set in `agent.py` |
| Streaming events | Toggleable in Settings — events flow live, not batched at end |
| Vertex AI backend | Model `gemini-3.1-pro-preview` resolved via `GOOGLE_GENAI_USE_VERTEXAI=true` + project env |

### Failure modes (web UI)

| Symptom | Likely cause |
|---|---|
| `SSLCertVerificationError` on first message | `pip install truststore` (one-time setup) |
| `403 Permission denied` | Service account missing `roles/aiplatform.user` |
| `404 model not found` | Try `--location us-central1` or model `gemini-2.5-pro` |
| UI shows no agent | Running `adk web` from the wrong dir — must point at `apps/` (the AGENTS_DIR), not `agent_test/` (the agent itself) |
| `ModuleNotFoundError: agent_test` | CWD doesn't contain `agent_test/` — `cd` to repo root first |

## Defaults

| | Default | Override |
|---|---|---|
| Project | `prj-d-ea-poc` | `--project` (CLI), `GOOGLE_CLOUD_PROJECT` env (web) |
| Location | `global` | `--location us-central1` etc. |
| Model | `gemini-3.1-pro-preview` | `--model gemini-2.5-pro` |
| TLS | truststore if installed | `--ca-bundle PATH` or `--insecure` (CLI only) |

## Where this goes next

Once both modes work, the real LUMI pipeline reuses this exact pattern — same
`Agent(model="gemini-3.1-pro-preview", ...)` construction, same env-var
backend selection. No SafeChain layer, no adapter, just direct Vertex AI.
