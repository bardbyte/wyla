# Using SafeChain LLMs inside Google ADK

Companion to `README.md` in this directory. That doc covers raw LLM access via
`get_model(model_idx)`. This doc covers wrapping those LLMs inside a
`google.adk.agents.LlmAgent` so the agent handles tool calling, session
state, multi-turn memory, and event streaming.

This doc is self-contained: a fresh LLM reading only this file (plus the code
blocks it contains) can produce a working ADK agent backed by SafeChain.

---

## Current state of this repo (April 2026)

- `google-adk>=0.3.0` is declared in `pyproject.toml` but is **stale** — the
  current release line is `1.31.1`. Bump the pin when you integrate.
- ADR `adr/001-adk-over-langgraph.md` is Accepted but the migration has NOT
  landed. Production code uses `safechain.tools.mcp.MCPToolAgent` directly via
  `class ReactAgent` in `src/api/server.py`.
- Nothing imports `google.adk.*` anywhere in this codebase yet.
- This doc is the blueprint for the migration.

---

## Prerequisites (30 seconds)

1. SafeChain set up per `access_llm/README.md`. Recap:
   - `.env` has `CIBIS_CONSUMER_INTEGRATION_ID`, `CIBIS_CONSUMER_SECRET`, `CONFIG_PATH`.
   - `config/config.yml` defines models `"1"`, `"2"`, `"3"`.
   - `from src.adapters.model_adapter import get_model` works.
2. Install ADK: `pip install 'google-adk>=1.31.1'`

---

## 30-second mental model

ADK gives you four things that raw SafeChain does not:

1. **Tool-calling orchestration** — function/MCP tools invoked by the LLM in a loop, automatically.
2. **Session state** — per-conversation key/value bag, mutated via structured events (not direct writes).
3. **Event stream** — every model chunk, tool call, sub-agent transition is an `Event` you can map to SSE.
4. **Multi-agent composition** — sequential, parallel, loop, or LLM-routed hand-offs.

ADK is Gemini-native by default. To use SafeChain's LangChain-wrapped Gemini you write one adapter class (`SafeChainLlm(BaseLlm)`). LiteLlm does NOT fit — LiteLlm needs an HTTP endpoint; SafeChain returns a Python object.

---

## Step 1 — The SafeChain → ADK adapter

Create `src/adapters/adk_safechain_llm.py`:

```python
"""ADK BaseLlm wrapper around a SafeChain LangChain chat model.

Usage:
    from src.adapters.adk_safechain_llm import make_safechain_llm
    llm = make_safechain_llm("1")    # wraps get_model("1") -> ADK BaseLlm

    from google.adk.agents import LlmAgent
    agent = LlmAgent(name="x", model=llm, ...)
"""
from __future__ import annotations

import json
from typing import Any, AsyncGenerator

from google.adk.models import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types as genai_types
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from pydantic import ConfigDict

from src.adapters.model_adapter import get_model


class SafeChainLlm(BaseLlm):
    """Wrap a SafeChain LangChain chat model as an ADK BaseLlm.

    One instance per agent. `lc_model` is shared, never mutated.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    lc_model: BaseChatModel

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"safechain/.*"]

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False,
    ) -> AsyncGenerator[LlmResponse, None]:
        messages = self._to_lc_messages(llm_request)
        lc_tools = self._to_lc_tools(llm_request)
        bound = self.lc_model.bind_tools(lc_tools) if lc_tools else self.lc_model

        if stream:
            buf = ""
            async for chunk in bound.astream(messages):
                text = getattr(chunk, "content", "") or ""
                if text:
                    buf += text
                    yield LlmResponse(
                        content=genai_types.Content(
                            role="model", parts=[genai_types.Part(text=text)],
                        ),
                        partial=True,
                    )
            yield LlmResponse(
                content=genai_types.Content(
                    role="model", parts=[genai_types.Part(text=buf)],
                ),
                partial=False, turn_complete=True,
            )
            return

        ai: AIMessage = await bound.ainvoke(messages)
        parts: list[genai_types.Part] = []
        if ai.content:
            parts.append(genai_types.Part(text=str(ai.content)))
        for tc in (ai.tool_calls or []):
            parts.append(genai_types.Part(
                function_call=genai_types.FunctionCall(
                    name=tc["name"], args=tc.get("args", {}) or {},
                ),
            ))
        yield LlmResponse(
            content=genai_types.Content(role="model", parts=parts),
            partial=False, turn_complete=True,
        )

    def _to_lc_messages(self, req: LlmRequest) -> list[Any]:
        out: list[Any] = []
        sys = getattr(req.config, "system_instruction", None) if req.config else None
        if sys:
            out.append(SystemMessage(content=self._render_sys(sys)))
        for content in req.contents:
            role = content.role
            texts = [p.text for p in content.parts if p.text]
            fcalls = [p.function_call for p in content.parts if p.function_call]
            fresps = [p.function_response for p in content.parts if p.function_response]
            if role == "user" and texts and not fresps:
                out.append(HumanMessage(content="\n".join(texts)))
            elif role == "model":
                tool_calls = [
                    {"name": fc.name, "args": dict(fc.args or {}), "id": fc.name}
                    for fc in fcalls
                ]
                out.append(AIMessage(content="\n".join(texts), tool_calls=tool_calls))
            elif fresps:
                for fr in fresps:
                    out.append(ToolMessage(
                        content=json.dumps(dict(fr.response or {}), default=str),
                        tool_call_id=fr.name,
                    ))
        return out

    def _to_lc_tools(self, req: LlmRequest) -> list[dict]:
        tools = (req.config.tools or []) if req.config else []
        out = []
        for t in tools:
            for fd in (t.function_declarations or []):
                out.append({
                    "type": "function",
                    "function": {
                        "name": fd.name,
                        "description": fd.description or "",
                        "parameters": (
                            fd.parameters.model_dump()
                            if fd.parameters
                            else {"type": "object"}
                        ),
                    },
                })
        return out

    @staticmethod
    def _render_sys(sys: Any) -> str:
        if isinstance(sys, str):
            return sys
        if hasattr(sys, "parts"):
            return "\n".join(p.text for p in sys.parts if getattr(p, "text", None))
        return str(sys)


def make_safechain_llm(model_idx: str = "1") -> SafeChainLlm:
    """model_idx: '1' = Gemini 2.5 Pro, '3' = Gemini 2.5 Flash."""
    return SafeChainLlm(model=f"safechain/{model_idx}", lc_model=get_model(model_idx))
```

**Contract this adapter implements:**
- Input: ADK `LlmRequest` (Gemini-shaped contents + function declarations).
- Output: `AsyncGenerator[LlmResponse, None]`. Always yields at least one item — even on error, yield an `LlmResponse(error_code=..., error_message=...)`. Silent failures hang the runner.
- Streaming: N `LlmResponse(partial=True)` then one `LlmResponse(partial=False, turn_complete=True)`.
- Tool-call translation: LangChain `tool_calls` ↔ Gemini `function_call` parts.
- System prompts: resolved from `req.config.system_instruction` (string or `Content`).

---

## Step 2 — Build an `LlmAgent`

```python
from google.adk.agents import LlmAgent
from src.adapters.adk_safechain_llm import make_safechain_llm

agent = LlmAgent(
    name="cortex_sql_author",
    model=make_safechain_llm("1"),     # Gemini 2.5 Pro via SafeChain
    description="Authors BigQuery SQL via the Looker semantic layer.",
    instruction=(
        "You are a data analyst. Use tools to answer. "
        "Pre-selected explore: {explore_name}. Measures: {measures}."
    ),
    tools=[],                          # see Step 3
    output_key="final_answer",         # auto-saves final text to session.state
)
```

`{explore_name}` and `{measures}` resolve at runtime from `session.state`. This is how you inject the output of Phase 1 (retrieval) into the agent's prompt.

`LlmAgent` constructor reference:

| Param                  | Notes                                                                                 |
|------------------------|---------------------------------------------------------------------------------------|
| `model`                | `BaseLlm` instance OR a string the registry resolves. Pass `make_safechain_llm(...)`. |
| `name`                 | Required. Used in multi-agent routing and as event `author`.                          |
| `instruction`          | str or callable. `{key}` placeholders resolve from session state at runtime.          |
| `description`          | Short. Used by parent LLMs to route into sub-agents.                                   |
| `tools`                | Callables, `FunctionTool`, `McpToolset`, or `AgentTool`.                              |
| `sub_agents`           | `BaseAgent` list — enables LLM-routed hand-offs.                                       |
| `output_key`           | If set, final text response auto-persists to `session.state[output_key]`.             |
| `input_schema` / `output_schema` | Pydantic. `output_schema` is mutually exclusive with `tools` on most providers. |
| `before_model_callback`, `after_model_callback` | Hook model calls before / after.                         |
| `before_tool_callback`, `after_tool_callback`   | Hook tool calls before / after.                          |

---

## Step 3 — Tools

### Regular Python functions (simplest)

```python
def get_exchange_rate(from_currency: str, to_currency: str) -> dict:
    """Get FX rate. Use ISO codes (USD, EUR)."""
    return {"rate": 1.07, "from": from_currency, "to": to_currency}

agent = LlmAgent(..., tools=[get_exchange_rate])
```

ADK introspects the signature + docstring and auto-generates a `FunctionDeclaration`. The docstring IS the tool description the LLM sees — write it well.

### Tools with session access

```python
from google.adk.tools import ToolContext

def save_preference(key: str, value: str, tool_context: ToolContext) -> dict:
    """Save a user preference."""
    tool_context.state[f"user:{key}"] = value    # "user:" prefix = cross-session
    return {"saved": True}
```

The final parameter MUST be named `tool_context` with type `ToolContext` for ADK to inject it. It gives access to `state`, `actions`, `save_artifact`, `search_memory`.

### MCP tool servers (Looker, etc.)

```python
import os
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

looker_mcp = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="http://localhost:5000/mcp",                       # from config.yml → servers.mcp
        headers={"Authorization": f"Bearer {os.environ['LOOKER_TOKEN']}"},
    ),
    tool_filter=["run_inline_query", "get_explore", "get_model"],
)

agent = LlmAgent(..., tools=[looker_mcp])
```

Three MCP connection types:
- `StreamableHTTPConnectionParams` — current MCP spec. Use this for Looker MCP.
- `SseConnectionParams` — legacy SSE. Avoid for new work.
- `StdioConnectionParams` — subprocess server over stdio.

Tools are discovered lazily on first agent invocation. Use `tool_filter=[...]` to whitelist the subset you want exposed.

---

## Step 4 — Runner + SSE streaming

```python
from fastapi.responses import StreamingResponse
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
import json

session_service = InMemorySessionService()
runner = Runner(app_name="cortex", agent=agent, session_service=session_service)

async def stream_query(user_id: str, session_id: str, query: str):
    # Create session if new
    await session_service.create_session(
        app_name="cortex", user_id=user_id, session_id=session_id,
    )
    new_msg = types.Content(role="user", parts=[types.Part(text=query)])
    cfg = RunConfig(streaming_mode=StreamingMode.SSE, max_llm_calls=20)

    async def gen():
        async for ev in runner.run_async(
            user_id=user_id, session_id=session_id,
            new_message=new_msg, run_config=cfg,
        ):
            payload = {
                "author": ev.author,
                "partial": ev.partial,
                "final": ev.is_final_response(),
                "parts": [_part_to_dict(p) for p in (ev.content.parts if ev.content else [])],
                "state_delta": dict(ev.actions.state_delta) if ev.actions and ev.actions.state_delta else {},
            }
            yield f"event: agent\ndata: {json.dumps(payload, default=str)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


def _part_to_dict(p):
    if p.text:
        return {"text": p.text}
    if p.function_call:
        return {"function_call": {
            "name": p.function_call.name,
            "args": dict(p.function_call.args or {}),
        }}
    if p.function_response:
        return {"function_response": {"name": p.function_response.name}}
    return {}
```

**Two kinds of streaming — do NOT conflate them:**
- **Token streaming**: inside one model call, multiple `Event(partial=True)` chunks. Controlled by `RunConfig.streaming_mode=StreamingMode.SSE`.
- **Event streaming**: tool calls, sub-agent transitions, and final responses are separate events regardless of streaming mode. `runner.run_async()` yields all of them in order.

---

## Step 5 — Session state (get this right or data silently disappears)

**Rule: never mutate `session.state` directly.** Three sanctioned paths:

| Path | When |
|------|------|
| `LlmAgent(output_key="final_answer")` | Auto-writes final text response. |
| `tool_context.state["key"] = value` (inside tool or callback) | Most common. ADK batches into `EventActions.state_delta`. |
| `session_service.append_event(session, Event(actions=EventActions(state_delta={...})))` | Manual, for offline seeding. |

**State scopes** (prefix-based):

| Prefix | Scope |
|--------|-------|
| (none) | current session |
| `user:` | persisted per-user across sessions |
| `app:` | global across all users and sessions |
| `temp:` | current invocation only, never persisted |

**State services** (pick one at Runner construction):
- `InMemorySessionService()` — dev default. Lost on restart.
- `DatabaseSessionService(db_url="postgresql+asyncpg://...")` — persists to Postgres/MySQL/SQLite (with async drivers).
- `VertexAiSessionService(project=..., location=...)` — managed on Vertex AI Agent Engine.

**Related services** (optional, attached at Runner):
- `MemoryService` — long-term cross-session recall. `InMemoryMemoryService()` or `VertexAiRagMemoryService(...)`. Access via `tool_context.search_memory(query)`.
- `ArtifactService` — binary blobs (files, images). `InMemoryArtifactService()` or `GcsArtifactService(bucket_name=...)`. Access via `tool_context.save_artifact(name, part)` and `load_artifact(name)`.

---

## Step 6 — Callbacks (mutate-or-replace)

Six hooks, all with the same pattern: return `None` to keep the input (possibly mutated), return a new object to **replace** it.

```python
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse

def redact_pii(callback_context: CallbackContext, llm_request: LlmRequest):
    """Strip SSN-like patterns before sending to the model."""
    for content in llm_request.contents:
        for part in content.parts:
            if part.text:
                part.text = _strip_ssn(part.text)
    return None    # None = proceed with (mutated) request

def short_circuit_if_cached(callback_context, llm_request):
    cached = _cache_lookup(llm_request)
    if cached is not None:
        return cached    # replace — skips model call entirely
    return None

agent = LlmAgent(
    ...,
    before_model_callback=redact_pii,
)
```

| Hook                       | Signature                                                        | Return to bypass                 |
|----------------------------|------------------------------------------------------------------|----------------------------------|
| `before_model_callback`    | `(CallbackContext, LlmRequest) -> Optional[LlmResponse]`         | `LlmResponse` (skips model call) |
| `after_model_callback`     | `(CallbackContext, LlmResponse) -> Optional[LlmResponse]`        | replacement `LlmResponse`        |
| `before_tool_callback`     | `(tool, args, ToolContext) -> Optional[dict]`                    | `dict` (skips tool call)         |
| `after_tool_callback`      | `(tool, args, ToolContext, tool_response) -> Optional[dict]`     | replacement `dict`                |
| `before_agent_callback`    | `(CallbackContext) -> Optional[Content]`                         | `Content` (skips agent)          |
| `after_agent_callback`     | `(CallbackContext) -> Optional[Content]`                         | replacement `Content`             |

---

## Step 7 — Multi-agent patterns

Three deterministic workflow agents + one LLM-routed pattern.

| Pattern       | Class                                                   | Use when                                                           |
|---------------|---------------------------------------------------------|--------------------------------------------------------------------|
| Sequential    | `SequentialAgent(name, sub_agents=[a, b, c])`           | Fixed pipeline. Children share state via `output_key`.             |
| Parallel      | `ParallelAgent(name, sub_agents=[a, b])`                | Independent concurrent work. Shared state, separate invocation branches. |
| Loop          | `LoopAgent(name, sub_agents=[a], max_iterations=N)`     | Iterate until sub-agent emits `EventActions(escalate=True)`.       |
| LLM-routed    | `LlmAgent(name, sub_agents=[a, b], ...)`                | Parent LLM chooses via `transfer_to_agent`. Every sub-agent's `description` is what the parent LLM reads. |

**For the Radix pipeline specifically:**
- Phase 1 (classify + retrieve): plain Python. NOT agentic.
- Phase 2 (SQL generation via Looker MCP): ONE `LlmAgent`. This is the only truly agentic step.
- Phase 3 (format + follow-ups): plain Python. NOT agentic.

Don't wrap the whole orchestrator in a `SequentialAgent`. You'd lose the deterministic guarantees of Phases 1 and 3 and gain nothing. The ADR's "right-sized complexity" principle applies.

---

## Step 8 — Deployment

| Target                                  | Works with `SafeChainLlm`? | Notes                                                      |
|-----------------------------------------|----------------------------|------------------------------------------------------------|
| `adk run path.to.agent_module` (CLI)    | Yes                        | REPL for dev.                                              |
| `adk web`                               | Yes                        | React dashboard on :8000.                                  |
| `adk api_server`                        | Yes                        | FastAPI only, for integration tests.                       |
| Cloud Run                               | Yes                        | **Recommended.** Same container pattern as Cortex today.   |
| GKE                                     | Yes                        | Same container, use your existing manifests.               |
| Vertex AI Agent Engine (`adk deploy agent_engine`) | **NO**             | Open bug: custom `BaseLlm` query methods don't register. See issue [#4208](https://github.com/google/adk-python/issues/4208). |

Stick to Cloud Run / GKE until `adk-python` ships a fix for custom `BaseLlm` deployment.

---

## Migration path for this repo (concrete)

Scoped, reversible, ~2-3 engineer-days.

1. Bump `google-adk>=1.31.1` in `pyproject.toml`.
2. Add `src/adapters/adk_safechain_llm.py` (Step 1 content above).
3. Replace `class ReactAgent` in `src/api/server.py` (approx lines 86-148) with an `LlmAgent` wired to `make_safechain_llm("1")` + `McpToolset(StreamableHTTPConnectionParams(url=config.servers.mcp.url))`.
4. In `RadixOrchestrator.run_query` (`src/pipeline/orchestrator.py` ~line 556), swap `await self.react_agent.run(messages)` for `Runner.run_async(...)`. Translate ADK `Event` → existing `SSEEvent` inline; do NOT leak ADK types into the FastAPI layer or the frontend.
5. Keep Phase 1 (classifier + retrieval) and Phase 3 (formatting + follow-ups) unchanged — they are not agentic.
6. Do NOT deploy to Agent Engine. Stay on Cloud Run / GKE.

**Quality gates:**
- Existing golden-query suite (~30/BU) must pass with no regression.
- Existing SSE wire format must be unchanged from the frontend's perspective (i.e., the React app should need zero changes).
- Feature flag the switch so you can flip back if prod traffic reveals issues.

---

## Failure schema

| Symptom                                           | Cause                                           | Fix                                                                                  |
|---------------------------------------------------|-------------------------------------------------|--------------------------------------------------------------------------------------|
| Runner hangs, no events yielded                    | Custom `BaseLlm` raised before yielding         | Wrap `generate_content_async` body in try/except; yield `LlmResponse(error_code=...)` on any error |
| `ValidationError: lc_model`                        | `SafeChainLlm` missing `arbitrary_types_allowed` | Keep `ConfigDict(arbitrary_types_allowed=True)` in the adapter                       |
| Tool calls missing from streamed output            | Only yielded text chunks, not `function_call`   | Even in streaming mode, emit tool calls in the final non-partial event               |
| `session.state` changes vanish between turns       | Direct mutation instead of `state_delta`         | Use `tool_context.state["k"] = v` inside a tool/callback. Never mutate from outside. |
| `output_schema` appears ignored                    | Agent has both `tools` AND `output_schema`       | Pick one per agent. Providers silently drop tools when `output_schema` is set.       |
| MCP tools not discovered                           | `McpToolset` not yet initialized                | First `run_async` call triggers lazy discovery. Ensure an initial user message.      |
| `adk deploy agent_engine` fails silently           | Issue #4208 with custom `BaseLlm`               | Deploy to Cloud Run/GKE until fixed.                                                 |
| `transfer_to_agent` has no effect                  | Sub-agent `description` missing/empty           | Every sub-agent needs a descriptive `description` — parent LLM reads it.             |
| `KeyError: '{explore_name}'` in prompt rendering   | State key referenced in `instruction` not set    | Ensure upstream sets `session.state["explore_name"]` before agent runs.              |
| Callback changes don't stick                       | Returned a new object when mutating in place    | Return `None` to keep (mutated) input; return new object only to replace.            |

---

## Antipatterns — do NOT do these

| Antipattern                                                     | Why it's wrong                                                                  |
|-----------------------------------------------------------------|---------------------------------------------------------------------------------|
| `LlmAgent(model=get_model("1"))`                                | `LlmAgent` needs `BaseLlm` or a registry-resolvable string. Wrap with `SafeChainLlm`. |
| `LiteLlm(model="openai/...")` aimed at SafeChain                | LiteLlm wants an HTTP endpoint; SafeChain is a Python object. Use `SafeChainLlm`. |
| `session.state["k"] = v` from outside a tool/callback            | Lost on next turn. Only `EventActions.state_delta` persists.                    |
| `LlmAgent` with both `output_schema` and `tools`                | Tools silently dropped on most providers.                                       |
| Returning both a mutated object AND a new object from a callback | Ambiguous. Return `None` to keep mutated input, return new object only to replace. |
| One `Runner` per request                                        | `Runner` is reusable. Construct at startup, reuse across requests.               |
| Concurrent `run_async` with same `(user_id, session_id)`         | Races on state deltas. Serialize per session.                                   |
| Tool returning 1 MB of JSON                                      | Blows model context. Use `tool_context.save_artifact(...)` and return a handle.  |
| `SafeChainLlm` that doesn't yield on error                       | Runner hangs. Always yield at least one `LlmResponse`.                          |
| Deploying custom `BaseLlm` to Agent Engine                       | Broken. Use Cloud Run/GKE. See failure schema above.                             |
| Wrapping non-agentic work in an `LlmAgent`                        | Agents add cost, latency, and non-determinism. Keep classifier + retrieval as plain code. |

---

## Where everything lives (when migration is complete)

| Path                                    | Purpose                                                                  |
|-----------------------------------------|--------------------------------------------------------------------------|
| `src/adapters/model_adapter.py`         | `get_model(model_idx)` — SafeChain entry point (unchanged).              |
| `src/adapters/adk_safechain_llm.py`     | **NEW.** `SafeChainLlm(BaseLlm)` adapter + `make_safechain_llm()`.        |
| `src/pipeline/adk_agent.py`             | **NEW.** `LlmAgent` definitions + `McpToolset` wiring.                    |
| `src/api/server.py`                     | Runner construction; `ReactAgent` class removed; SSE translation inline.  |
| `src/pipeline/orchestrator.py`          | Uses `Runner.run_async` for Phase 2; Phases 1 and 3 untouched.           |
| `config/config.yml`                     | Unchanged. Same SafeChain model definitions + MCP server URL.             |
| `pyproject.toml`                        | `google-adk>=1.31.1`, plus existing `safechain`, `langchain-core`, `pydantic`. |

---

## Full runnable example (post-migration)

A fresh LLM should be able to execute this after Steps 1-3 are in place:

```python
#!/usr/bin/env python3
"""Smoke test for SafeChain + ADK integration. Run: python adk_smoke_test.py"""
import asyncio
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from src.adapters.adk_safechain_llm import make_safechain_llm


def get_time(city: str) -> dict:
    """Return a mocked current time for a city."""
    return {"city": city, "time": "12:34 PM"}


agent = LlmAgent(
    name="smoke_agent",
    model=make_safechain_llm("3"),      # Gemini 2.5 Flash via SafeChain
    description="Tests SafeChain+ADK end-to-end.",
    instruction="Use the get_time tool to answer time questions concisely.",
    tools=[get_time],
    output_key="last_answer",
)

session_service = InMemorySessionService()
runner = Runner(app_name="smoke", agent=agent, session_service=session_service)


async def main():
    await session_service.create_session(
        app_name="smoke", user_id="u1", session_id="s1",
    )
    msg = types.Content(role="user", parts=[types.Part(text="What time is it in Phoenix?")])
    cfg = RunConfig(streaming_mode=StreamingMode.SSE, max_llm_calls=5)

    async for ev in runner.run_async(
        user_id="u1", session_id="s1", new_message=msg, run_config=cfg,
    ):
        print(f"[{ev.author}] partial={ev.partial} final={ev.is_final_response()}")
        if ev.content:
            for p in ev.content.parts:
                if p.text:
                    print(f"  text: {p.text[:120]}")
                if p.function_call:
                    print(f"  fn_call: {p.function_call.name}({dict(p.function_call.args or {})})")
                if p.function_response:
                    print(f"  fn_resp: {p.function_response.name}")


if __name__ == "__main__":
    asyncio.run(main())
```

Expected output: one model call that emits a `function_call` for `get_time`, ADK invokes the function, a second model call that consumes the result and produces a final text response like "It's 12:34 PM in Phoenix."

---

## Further reading (within this repo)

| Path                                         | What it covers                                      |
|----------------------------------------------|------------------------------------------------------|
| `access_llm/README.md`                        | Raw LLM access via SafeChain. Start here.            |
| `adr/001-adk-over-langgraph.md`               | Why ADK over LangGraph for orchestration.            |
| `src/pipeline/orchestrator.py`                | Current (non-ADK) orchestrator to be partially replaced. |
| `src/api/server.py`                           | `class ReactAgent` — to be deleted during migration. |
| `config/config.yml`                           | SafeChain model registry + MCP server URL.           |
