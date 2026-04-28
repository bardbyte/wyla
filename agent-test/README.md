# agent-test/ — Vertex AI → ADK → Gemini smoke test

Minimal end-to-end check that ADK can run an `Agent` against Gemini 3.1 Pro on
Vertex AI in your certified-access GCP project (`prj-d-ea-poc`). If
`python agent-test/run.py` produces a final answer, ADK + Vertex + service-
account auth + tool calling all work and we can move on.

## What's in here

| File | Purpose |
|---|---|
| `agent.py` | One `Agent` (LlmAgent) with two trivial tools — `roll_die` and `check_prime`. Default model `gemini-3.1-pro-preview`. |
| `run.py` | Sets the four env vars ADK needs (`GOOGLE_GENAI_USE_VERTEXAI`, `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION`, `GOOGLE_APPLICATION_CREDENTIALS`), handles corporate-network TLS, then runs the agent and prints the full event trace. |

## What this proves on PASS

1. ✓ Service-account auth to `prj-d-ea-poc` works
2. ✓ Vertex AI Gemini 3.1 Pro is reachable
3. ✓ Corporate-MITM TLS is handled
4. ✓ ADK builds an agent against the Vertex backend
5. ✓ The agent emits tool calls in the right format
6. ✓ ADK invokes tools and feeds responses back
7. ✓ The reason → act → reason loop terminates with a final answer
8. ✓ Session state persists across tool invocations

## Run it

Same flags that worked for `scripts/check_vertex_gemini.py` (since the
underlying transport is identical).

```bash
# Simplest — assumes truststore is installed (recommended for corporate Mac)
python agent-test/run.py --key-file ~/Downloads/key.json

# Quick bypass for corporate-MITM SSL (intranet only)
python agent-test/run.py --key-file ~/Downloads/key.json --insecure

# Different model
python agent-test/run.py --key-file ~/Downloads/key.json --model gemini-2.5-pro

# Custom prompt
python agent-test/run.py --key-file ~/Downloads/key.json \
    --query "Roll a 20-sided die and tell me if it's prime."

# Machine-readable
python agent-test/run.py --key-file ~/Downloads/key.json --json
```

You can also set `GOOGLE_APPLICATION_CREDENTIALS` once and skip `--key-file`:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/key.json
python agent-test/run.py
```

## Defaults

| | Default | Override |
|---|---|---|
| Project | `prj-d-ea-poc` | `--project` |
| Location | `global` | `--location us-central1` (or any region) |
| Model | `gemini-3.1-pro-preview` | `--model gemini-2.5-pro` |
| TLS | truststore if installed, else `default certifi` | `--ca-bundle PATH` or `--insecure` |

## Expected output (truncated)

```
TLS:                truststore (OS Keychain)
Service account:    svc-d-lumigct-hyd@prj-d-ea-poc.iam.gserviceaccount.com
Key project_id:     prj-d-ea-poc
Vertex project:     prj-d-ea-poc
Vertex location:    global
Model:              gemini-3.1-pro-preview
Elapsed:            2.8s, 5 events

Event trace:
  [0] vertex_smoke_agent
      tool_call: roll_die({'sides': 8})
  [1] vertex_smoke_agent
      tool_resp: roll_die → 5
  [2] vertex_smoke_agent
      tool_call: check_prime({'nums': [5]})
  [3] vertex_smoke_agent
      tool_resp: check_prime → "5 are prime numbers."
  [4] vertex_smoke_agent (final)
      text: I rolled a 5 — it is prime.

PASS — Vertex AI → ADK → Gemini smoke test succeeded.
```

## Failure modes

| Symptom | Likely cause |
|---|---|
| `SSLCertVerificationError` | Corporate MITM proxy — install `truststore` or pass `--insecure` |
| `403 ... Vertex AI API has not been used` | Enable `aiplatform.googleapis.com` on the project |
| `403 ... caller does not have permission` | Service account missing `roles/aiplatform.user` |
| `404 ... model is not found` | Try `--location us-central1` or fall back to `--model gemini-2.5-pro` |
| `refusing to load credentials from inside the repo` | The key path resolves under the repo — move it (e.g. to `~/Downloads/`) |
| `error_code: ...` in event trace | Read the trace — it carries the full Python traceback |
| Agent finishes with no final text | Model only emitted tool calls; bump `max_llm_calls` in `run.py` |

## Where this goes next

Once it passes, the real LUMI pipeline (`lumi/agents/...`) uses this same
direct-Vertex pattern — no SafeChain layer in the agent runtime. The
`make_safechain_llm` adapter is gone; agents just construct `Agent(model="...")`
with the env vars set.
