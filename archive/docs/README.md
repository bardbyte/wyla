# Accessing LLMs in Cortex via SafeChain

This doc is self-contained. Reading only this file, you should be able to:
1. Understand what `config/config.yml` must look like.
2. Understand what environment variables must be set.
3. Make a working call to Gemini 2.5 Pro, Gemini 2.5 Flash, or the BGE-large embedding model.
4. Know how to fail less often.

For wiring these LLMs into a Google ADK `LlmAgent`, read `ADK_INTEGRATION.md` next.

---

## 30-second invocation

```python
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())          # loads .env

from src.adapters.model_adapter import get_model

# Chat — Gemini 2.5 Pro
llm = get_model("1")
resp = llm.invoke("Summarize: total billed business grew 12% YoY.")
print(resp.content)

# Embedding — BGE-large-en-v1.5 (1024-dim)
embed = get_model("2")
vec = embed.embed_query("active cardmembers")   # list[float], len == 1024
```

Returned objects are **LangChain-compatible**:
- Chat models expose `.invoke`, `.ainvoke`, `.stream`, `.astream`, `.batch`, `.abatch`, `.bind_tools`, `.with_structured_output`.
- Embedding models expose `.embed_query(str) -> list[float]` and `.embed_documents(list[str]) -> list[list[float]]`.

There is NO direct use of `openai`, `google.generativeai`, or `vertexai` clients in this repo. Do not add any — all LLM traffic goes through SafeChain → CIBIS/IDaaS → Amex-hosted Gemini.

---

## Model registry

Defined in `config/config.yml` (authoritative). `config/models.yaml` is a stale artifact that does NOT drive runtime behavior — ignore it.

| `model_idx` | Model                      | Type      | Dim  | Typical use                                          |
|-------------|----------------------------|-----------|------|------------------------------------------------------|
| `"1"`       | `google-gemini-2.5-pro`    | chat      | —    | Reasoning-heavy: extraction, LookML authoring, description synthesis. |
| `"2"`       | `bge-large-en-v1.5`        | embedding | 1024 | Field/query embeddings for pgvector.                 |
| `"3"`       | `google-gemini-2.5-flash`  | chat      | —    | Latency-sensitive: intent classification, follow-ups. |

Default to `"3"` (Flash) unless empirically required otherwise. Pro is ~10× cost and ~3× latency.

---

## What `config/config.yml` looks like

This is the authoritative runtime config. If this file is missing or the shape changes, every `get_model()` call breaks.

```yaml
# config/config.yml
idaas:
  url: https://oneidentityapi-dev.aexp.com/security/digital/v1/application/token
  scope:
    - /genai/google/v1/models/gemini-2.5-pro/***:post
    - /genai/google/v1/models/gemini-2.5-flash/***:post
    - /genai/google/v1/models/bge-large-en/embeddings/::post
  originator_source: digital-payments

models:
  "1":                                          # Gemini 2.5 Pro (chat)
    idaas:
      id_key: CIBIS_CONSUMER_INTEGRATION_ID     # env var NAME to read
      secret_key: CIBIS_CONSUMER_SECRET         # env var NAME to read
      scope:
        - /genai/google/v1/models/gemini-2.5-pro/***:post
    provider: openai                            # SafeChain uses openai-compat wire format
    type: chat
    model_name: google-gemini-2.5-pro
    api_version: "2024-06-01"
    api_base: https://epsg-dev.aexp.com/genai/google/v1/models/gemini-2.5-pro/openapi

  "2":                                          # BGE-large-en-v1.5 (embedding)
    idaas:
      id_key: CIBIS_CONSUMER_INTEGRATION_ID
      secret_key: CIBIS_CONSUMER_SECRET
      scope:
        - /genai/google/v1/models/bge-large-en/embeddings/::post
    provider: vertex
    type: embedding
    model_name: bge-large-en
    model_url: https://epsg-dev.aexp.com/genai/google/v1/models/bge-large-en/embeddings

  "3":                                          # Gemini 2.5 Flash (chat)
    idaas:
      id_key: CIBIS_CONSUMER_INTEGRATION_ID
      secret_key: CIBIS_CONSUMER_SECRET
      scope:
        - /genai/google/v1/models/gemini-2.5-flash/***:post
    provider: openai
    type: chat
    model_name: google-gemini-2.5-flash
    api_version: "2024-06-01"
    api_base: https://epsg-dev.aexp.com/genai/google/v1/models/gemini-2.5-flash/openapi

servers:
  mcp:
    url: http://localhost:5000/mcp              # Looker MCP sidecar
    transport: streamable_http
```

**Key facts a fresh LLM needs to know:**
- Keys under `models:` are **strings** (`"1"`, `"2"`, `"3"`), not integers. `get_model("1")` works; `get_model(1)` will raise.
- `provider: openai` + `provider: vertex` refer to SafeChain's internal wire format, NOT which vendor hosts the model. Both actually call Gemini/BGE on Amex infrastructure.
- `id_key` / `secret_key` are the **names of environment variables** SafeChain reads at runtime, not literal secret values.
- Switching from dev to prod means changing `idaas.url` and `api_base` hosts (s/dev/prod/).

---

## What `.env` looks like

Minimum set for LLM access (three vars):

```bash
# .env — NEVER commit this. Add .env to .gitignore.

# CIBIS credentials from the Amex CIBIS portal (ask your team lead for access)
CIBIS_CONSUMER_INTEGRATION_ID=abc123-def4-5678-90ab-cdef12345678
CIBIS_CONSUMER_SECRET=your-secret-here

# Absolute or repo-relative path to config/config.yml
CONFIG_PATH=./config/config.yml
```

**How to load it in Python:**
```python
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())         # searches parent directories for .env
```

**Fallback behavior:**
If `CONFIG_PATH` is unset, `src/retrieval/vector.py` auto-sets it to `<repo_root>/config/config.yml`. New code should set it explicitly in its own bootstrap.

**End-to-end env vars for the full Radix stack** (not needed for raw LLM access, included for reference):
```bash
# Looker (for MCP server)
LOOKER_INSTANCE_URL=https://your-instance.looker.com
LOOKER_CLIENT_ID=your-looker-client-id
LOOKER_CLIENT_SECRET=your-looker-client-secret

# PostgreSQL + pgvector + AGE
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_DBNAME=postgres
POSTGRES_GRAPH_PATH=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

---

## The one function you need

`src/adapters/model_adapter.py` is ~34 lines and contains exactly one function:

```python
from ee_config.config import Config
from safechain.lcel import model

_config_initialized = False

def get_model(model_idx: str):
    """Get a SafeChain model client by config.yml index.

    model_idx must be a string that matches a top-level key under `models:`
    in config.yml. Raises KeyError if not present.
    """
    global _config_initialized
    if not _config_initialized:
        Config.from_env()                 # reads CONFIG_PATH, CIBIS_* env vars
        _config_initialized = True
    return model(model_idx)
```

`get_model()` is safe to call repeatedly — `Config.from_env()` runs at most once per process.

---

## Chat patterns

### Simple single-turn

```python
llm = get_model("1")
resp = llm.invoke("What is the capital of France?")
print(resp.content)   # "Paris"
```

### Multi-turn (manage messages yourself)

```python
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

llm = get_model("1")
history = [
    SystemMessage(content="You are a terse assistant."),
    HumanMessage(content="Summarize this filing in 3 bullets."),
]
resp = llm.invoke(history)
history.append(AIMessage(content=resp.content))
history.append(HumanMessage(content="Now translate to French."))
resp = llm.invoke(history)
```

### Structured output (preferred over regex parsing)

```python
from pydantic import BaseModel, Field
from src.adapters.model_adapter import get_model

class Intent(BaseModel):
    intent: str = Field(description="one of: query | follow_up | greeting")
    metrics: list[str]
    dimensions: list[str]
    confidence: float = Field(ge=0.0, le=1.0)

llm = get_model("3")                              # Flash is fine for extraction
structured = llm.with_structured_output(Intent)
result: Intent = structured.invoke("Total billed business last quarter")
# result.intent == "query", result.metrics == ["total_billed_business"], ...
```

Under the hood this uses Gemini's function-calling mode with Pydantic validation. Do NOT parse free-text output with regex — use structured output instead.

### Streaming

```python
llm = get_model("1")
for chunk in llm.stream("Write a haiku about pgvector."):
    print(chunk.content, end="", flush=True)
```

### Async & batch

```python
import asyncio

async def main():
    llm = get_model("3")

    # Async single call
    resp = await llm.ainvoke("Hello")

    # Concurrent batch — 5-10× faster than a sequential loop
    results = await llm.abatch([f"Summarize doc {i}" for i in range(50)])

asyncio.run(main())
```

Use `.batch` / `.abatch` for >10 concurrent calls. Sequential `.invoke` loops waste wall time and don't amortize rate-limit buckets.

### Tool calling

```python
from langchain_core.tools import tool
from src.adapters.model_adapter import get_model

@tool
def get_weather(city: str) -> str:
    """Get current weather for a city."""
    return f"Weather in {city}: sunny"

llm = get_model("1").bind_tools([get_weather])
resp = llm.invoke("What's the weather in Phoenix?")
# resp.tool_calls == [{"name": "get_weather", "args": {"city": "Phoenix"}, "id": "..."}]
```

Tool calls return in `resp.tool_calls`. You execute them yourself and append a `ToolMessage` to continue the conversation. For automatic tool-calling loops, use ADK (`ADK_INTEGRATION.md`).

---

## BGE embedding — one critical gotcha

BGE-large was trained asymmetrically. Prepend an instruction prefix to **queries** but NOT to documents. Skipping this degrades top-k recall by 8-15%.

```python
from src.adapters.model_adapter import get_model
from config.constants import BGE_QUERY_PREFIX
# BGE_QUERY_PREFIX == "Represent this sentence for searching relevant passages: "

embed = get_model("2")

# QUERY (prefix required)
query_vec = embed.embed_query(BGE_QUERY_PREFIX + user_query)

# DOCUMENTS (no prefix)
doc_vecs = embed.embed_documents(field_descriptions)
```

Use `BGE_QUERY_PREFIX` from `config.constants` — do not hardcode it. If the prefix changes upstream, your code should inherit that change.

---

## Failure schema

Keep this table handy when debugging.

| Symptom                                           | Cause                                   | Fix                                                                 |
|---------------------------------------------------|-----------------------------------------|---------------------------------------------------------------------|
| `KeyError: 'CIBIS_CONSUMER_INTEGRATION_ID'`       | `.env` not loaded                       | `load_dotenv(find_dotenv())` before first `get_model()` call        |
| `FileNotFoundError: config.yml`                   | `CONFIG_PATH` unset or pointing wrong   | `export CONFIG_PATH=$(pwd)/config/config.yml`                       |
| `KeyError: '4'` (or any unknown index)            | `model_idx` not in `config.yml` models: | Check `config/config.yml` — only `"1"`, `"2"`, `"3"` defined        |
| HTTP 401 from IDaaS                               | CIBIS secret rotated or typo            | Re-fetch from CIBIS portal, update `.env`, restart process          |
| HTTP 403 "scope not authorized"                   | Requested model not in `idaas.scope`    | Add model path to `config.yml` → `idaas.scope` AND model's scope    |
| HTTP 429                                          | Rate limit hit                          | Switch to `.batch()`; add exponential backoff on `invoke()`         |
| "Input rejected … Company policy"                 | AI Firewall: decrypt_sde()/PII pattern  | Scrub input; redact identifiers before sending                      |
| Embedding vector length != 1024                   | Wrong model index                       | Must be `get_model("2")` — `"1"` and `"3"` are chat, not embedding  |
| LLM returns English when JSON expected            | Didn't use `with_structured_output`     | Always use structured output with a Pydantic schema; never regex-parse |
| `AttributeError: 'dict' object has no attribute 'content'` | Treated raw dict as AIMessage     | `.invoke()` returns an `AIMessage`; access `.content`, not `["content"]` |

---

## Antipatterns — do NOT do these

| Antipattern                                            | Why it's wrong                                                                |
|--------------------------------------------------------|-------------------------------------------------------------------------------|
| `import openai; openai.ChatCompletion.create(...)`     | Bypasses SafeChain — fails auth, no audit trail                               |
| Hardcoding model names: `"gemini-2.5-pro"`             | Model versions change; `model_idx` is stable                                  |
| Caching `get_model()` in a module-global without a lock | Not thread-safe on first call; use lazy init or module-level immediately      |
| Calling `get_model()` inside a tight loop               | Allocates a new client each iteration; hoist out of the loop                  |
| Parsing LLM output with regex                          | Brittle. Always use `with_structured_output(PydanticSchema)`                  |
| Skipping the BGE query prefix                           | -8 to -15% recall on retrieval. Always prefix queries via `BGE_QUERY_PREFIX` |
| Trusting tool_call args without validation              | LLMs occasionally emit off-schema args. Validate before acting                |
| Committing `.env`                                       | Secrets in git history are painful to purge. `.env` must be in `.gitignore`   |

---

## Where everything lives

| Path                                       | Purpose                                                               |
|--------------------------------------------|------------------------------------------------------------------------|
| `src/adapters/model_adapter.py`            | `get_model(model_idx)` — the ONE entry point                          |
| `config/config.yml`                        | Authoritative model registry + IDaaS scopes                           |
| `config/constants.py`                      | `LLM_MODEL_IDX`, `EMBED_MODEL_IDX`, `BGE_QUERY_PREFIX`, other constants |
| `config/models.yaml`                       | **Stale.** Do not consult. Kept for historical reference only.        |
| `src/retrieval/vector.py`                  | Reference usage for embedding + `_bootstrap_environment()` pattern    |
| `src/pipeline/orchestrator.py`             | Reference usage for chat model in a streaming pipeline                |
| `.env`                                     | Secrets. Not committed. Loaded via `python-dotenv`.                   |
| `.env.example`                             | Template for new developers. Committed.                               |
| `access_llm/ADK_INTEGRATION.md`            | How to wrap these models in a Google ADK `LlmAgent`                   |

---

## Full runnable example

This is what a fresh LLM should be able to execute verbatim after setting `.env`:

```python
#!/usr/bin/env python3
"""Smoke test for SafeChain LLM access. Run: python smoke_test.py"""
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from src.adapters.model_adapter import get_model
from config.constants import BGE_QUERY_PREFIX

# 1) Chat — Gemini 2.5 Flash
llm = get_model("3")
resp = llm.invoke("One sentence: what is a semantic layer?")
assert resp.content, "chat returned empty"
print("CHAT OK:", resp.content[:80])

# 2) Embedding — BGE-large
embed = get_model("2")
vec = embed.embed_query(BGE_QUERY_PREFIX + "active cardmembers")
assert len(vec) == 1024, f"expected 1024-dim, got {len(vec)}"
print(f"EMBED OK: {len(vec)} dims, first 3: {vec[:3]}")

# 3) Structured output
from pydantic import BaseModel
class Answer(BaseModel):
    capital: str
    country: str

structured = get_model("1").with_structured_output(Answer)
result = structured.invoke("What is the capital of Japan?")
assert result.country.lower() == "japan"
print(f"STRUCTURED OK: capital={result.capital}, country={result.country}")
```
