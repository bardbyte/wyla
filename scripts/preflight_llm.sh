#!/usr/bin/env bash
# Smoke-tests SafeChain + the ADK adapter end-to-end.
set -euo pipefail

# Load .env so CIBIS_* + CONFIG_PATH are available.
if [ -f .env ]; then
    # shellcheck disable=SC2046
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

python - <<'PY'
import os, sys

missing = [k for k in ("CIBIS_CONSUMER_INTEGRATION_ID", "CIBIS_CONSUMER_SECRET", "CONFIG_PATH") if not os.environ.get(k)]
if missing:
    print("ERROR: missing env vars:", missing)
    sys.exit(2)

try:
    from src.adapters.model_adapter import get_model
except ImportError as e:
    print("ERROR: SafeChain not installed:", e)
    sys.exit(2)

print("==> Raw SafeChain call (Gemini 2.5 Flash)")
resp = get_model("3").invoke("Reply with the single word 'pong'.")
print("  content:", (resp.content or "")[:120])
assert resp.content, "empty chat response"

print("==> ADK adapter call")
import asyncio
from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from src.adapters.adk_safechain_llm import make_safechain_llm

agent = LlmAgent(
    name="smoke",
    description="smoke test agent",
    model=make_safechain_llm("3"),
    instruction="Reply with 'OK'.",
)
ss = InMemorySessionService()
runner = Runner(app_name="smoke", agent=agent, session_service=ss)

async def main():
    await ss.create_session(app_name="smoke", user_id="u", session_id="s")
    msg = types.Content(role="user", parts=[types.Part(text="ping")])
    async for ev in runner.run_async(user_id="u", session_id="s", new_message=msg,
                                      run_config=RunConfig(streaming_mode=StreamingMode.NONE, max_llm_calls=3)):
        if ev.content and ev.content.parts:
            for p in ev.content.parts:
                if p.text:
                    print("  ", p.text[:120])
        if ev.error_code:
            print("  ERROR:", ev.error_code, ev.error_message)
            sys.exit(1)

asyncio.run(main())
print("OK — LLM + ADK adapter work.")
PY
