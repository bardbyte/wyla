# agent-test/ — SafeChain → Google ADK → Gemini smoke test

Minimal end-to-end check that you can run a Google ADK `LlmAgent` powered by
Gemini 2.5 Pro through Amex SafeChain. If `python agent-test/run.py` produces
a final answer, the entire chain is wired correctly and we can move forward.

## What's in here

| File | Purpose |
|---|---|
| `safechain_adk.py` | `SafeChainLlm(BaseLlm)` adapter + `make_safechain_llm(idx)` factory. Translates between ADK's Gemini-shaped requests and SafeChain's LangChain chat client. |
| `agent.py` | One `Agent` (LlmAgent) with two trivial tools — `roll_die` and `check_prime`. Mirrors Google's canonical `hello_world` sample so it's easy to compare against the docs. |
| `run.py` | `Runner` setup + asyncio main. Sends one query, prints the full event trace (tool calls, tool responses, final answer), exits 0 on success. |

## What this proves

When `python agent-test/run.py` succeeds:

1. ✓ SafeChain auth works (CIBIS creds + `config.yml` resolved)
2. ✓ The adapter correctly bridges ADK ↔ LangChain message shapes
3. ✓ Gemini 2.5 Pro receives the prompt and reasons about it
4. ✓ The model emits tool calls in Gemini-native format
5. ✓ ADK invokes the Python tool functions and feeds responses back
6. ✓ The reason→act→reason loop terminates with a final response
7. ✓ Session state survives across tool invocations (roll history persists)

If any of these break, the run prints the event trace showing exactly which
step failed.

## Prerequisites

```bash
pip install 'google-adk>=1.31.1' langchain-core python-dotenv
# Plus the Amex-internal packages (already on your work laptop):
#   safechain
#   ee_config
```

`.env` at the repo root with:

```
CIBIS_CONSUMER_INTEGRATION_ID=...
CIBIS_CONSUMER_SECRET=...
CONFIG_PATH=./config/config.yml
```

`config/config.yml` per the SafeChain bundle's standard layout — must define
model entries for `"1"` (Gemini 2.5 Pro) and `"3"` (Gemini 2.5 Flash) at minimum.

## Run it

From the repo root:

```bash
# Default: Gemini 2.5 Pro, asks for a die roll + prime check
python agent-test/run.py

# Faster / cheaper iteration — Flash
python agent-test/run.py --model 3

# Custom prompt
python agent-test/run.py --query "Roll a 20-sided die three times and tell me which results are prime."

# Machine-readable JSON output
python agent-test/run.py --json
```

## Expected output (Pro, default query)

```
Model:    safechain/1
Query:    Roll an 8-sided die for me, then tell me whether the result is a prime number.
Elapsed:  3.4s, 5 events

Event trace:
  [0] safechain_smoke_agent
      tool_call: roll_die({'sides': 8})
  [1] safechain_smoke_agent
      tool_resp: roll_die → 5
  [2] safechain_smoke_agent
      tool_call: check_prime({'nums': [5]})
  [3] safechain_smoke_agent
      tool_resp: check_prime → "5 are prime numbers."
  [4] safechain_smoke_agent (final)
      text: I rolled a 5 on the 8-sided die — 5 is a prime number.

Final answer:  I rolled a 5 on the 8-sided die — 5 is a prime number.

PASS — SafeChain → ADK → Gemini smoke test succeeded.
```

The exact die roll varies, but the event ordering should match: model → tool
call → tool response → (model decides next step) → final text.

## Failure modes and what to do

| Symptom | Cause | Fix |
|---|---|---|
| `ImportError: safechain` | Amex-internal package not installed | Install via your team's onboarding bundle |
| `ImportError: ee_config` | Same as above | Same |
| `KeyError: CIBIS_CONSUMER_INTEGRATION_ID` | `.env` not loaded | Confirm `.env` is at the repo root and CIBIS vars are populated |
| `FileNotFoundError: config.yml` | `CONFIG_PATH` unset / wrong | `export CONFIG_PATH=$(pwd)/config/config.yml` |
| `KeyError: '1'` (or `'3'`) | The model index isn't defined in `config.yml` | Add the corresponding model entry per SafeChain's docs |
| `error_code: SAFECHAIN_ERROR` in event trace | Underlying SafeChain call raised | The `error_message` field has the inner exception — usually 401 (creds expired), 403 (scope), or 429 (rate-limited) |
| Agent finishes with no final text | Model returned only tool calls and ADK's max_llm_calls hit | Bump `max_llm_calls` in `run.py`, or simplify the prompt |
| `LiteLlm…` errors | You're trying to use ADK's default Gemini loader | Don't — the whole point is `make_safechain_llm("1")` returns a `BaseLlm` directly |

## Where this goes next

`agent-test/` is the proof-of-life. Once it passes, the same pattern
(`make_safechain_llm` factory + `SafeChainLlm` adapter) is what we'll use in
the real LUMI pipeline — `lumi/agents/view_enricher.py` will get its model via
`make_safechain_llm("1")` and behave identically.
