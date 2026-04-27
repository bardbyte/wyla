"""SafeChain → ADK BaseLlm adapter.

Wraps the LangChain-compatible chat client returned by `safechain.lcel.model(idx)`
as a `google.adk.models.BaseLlm`, so an ADK `LlmAgent` can use a SafeChain-routed
Gemini model the same way it would use any native ADK model.

Design notes (lessons from getting the ReAct loop to actually work):

  - The reference test_safechain_access.py uses `client.invoke(messages)` (sync).
    SafeChatOpenAI's async path is not battle-tested — calling `ainvoke` here
    surfaced 'SafeChatOpenAI has no attribute get'. We sidestep by running the
    sync call in `asyncio.to_thread`.

  - We do NOT call `.bind(temperature=...)` on the chat model. That returns a
    RunnableBinding whose `bind_tools` semantics are different from the raw
    BaseChatModel's. The reference works without binding; we follow suit.

  - genai's Schema.model_dump() emits JSON-Schema-like dicts but with uppercase
    types ('STRING', 'NUMBER'). OpenAI tool-function specs expect lowercase
    JSON Schema. We walk the schema and normalize before handing to bind_tools.

  - On error inside generate_content_async we yield an LlmResponse(error_code,
    error_message) carrying the full traceback. Raising would hang the runner.

Usage:
    from safechain_adk import make_safechain_llm
    from google.adk.agents import LlmAgent

    llm = make_safechain_llm("1")     # Gemini 2.5 Pro via SafeChain
    agent = LlmAgent(name="x", model=llm, instruction="...", tools=[my_tool])
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from collections.abc import AsyncGenerator
from typing import Any

from dotenv import find_dotenv, load_dotenv
from google.adk.models import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types as genai_types
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from pydantic import ConfigDict
from safechain.lcel import model as safechain_model

# Load .env so SafeChain reads CIBIS_* and CONFIG_PATH on first model() call.
load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)


class SafeChainLlm(BaseLlm):
    """Wrap a SafeChain LangChain chat model as an ADK BaseLlm.

    `lc_model` is the raw `safechain_model("1")` return value — never bind on it
    eagerly; bind only at use time inside `generate_content_async`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    lc_model: BaseChatModel

    @classmethod
    def supported_models(cls) -> list[str]:
        return [r"safechain/.*"]

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        try:
            messages = self._to_lc_messages(llm_request)
            lc_tools = self._to_lc_tools(llm_request)
            bound = (
                self.lc_model.bind_tools(lc_tools) if lc_tools else self.lc_model
            )

            if stream:
                # Sync stream wrapped in to_thread per chunk.
                buf = ""

                def _drain_stream() -> list[Any]:
                    return list(bound.stream(messages))

                chunks = await asyncio.to_thread(_drain_stream)
                for chunk in chunks:
                    text = getattr(chunk, "content", "") or ""
                    if text:
                        buf += text
                        yield LlmResponse(
                            content=genai_types.Content(
                                role="model",
                                parts=[genai_types.Part(text=text)],
                            ),
                            partial=True,
                        )
                yield LlmResponse(
                    content=genai_types.Content(
                        role="model", parts=[genai_types.Part(text=buf)]
                    ),
                    partial=False,
                    turn_complete=True,
                )
                return

            # Non-streaming path: sync invoke off the event loop.
            ai: AIMessage = await asyncio.to_thread(bound.invoke, messages)

            parts: list[genai_types.Part] = []
            if ai.content:
                parts.append(genai_types.Part(text=str(ai.content)))
            for tc in ai.tool_calls or []:
                parts.append(
                    genai_types.Part(
                        function_call=genai_types.FunctionCall(
                            name=tc["name"],
                            args=tc.get("args", {}) or {},
                        )
                    )
                )
            yield LlmResponse(
                content=genai_types.Content(role="model", parts=parts),
                partial=False,
                turn_complete=True,
            )
        except Exception as e:
            tb = traceback.format_exc()
            logger.exception("SafeChainLlm.generate_content_async failed")
            yield LlmResponse(
                error_code="SAFECHAIN_ERROR",
                error_message=f"{type(e).__name__}: {e}\n\n{tb}",
            )

    # ------------------------------------------------------------------ #
    # Translation helpers — Gemini-shaped LlmRequest ↔ LangChain messages #
    # ------------------------------------------------------------------ #

    def _to_lc_messages(self, req: LlmRequest) -> list[Any]:
        out: list[Any] = []
        sys = (
            getattr(req.config, "system_instruction", None) if req.config else None
        )
        if sys:
            out.append(SystemMessage(content=self._render_system_instruction(sys)))

        for content in req.contents:
            role = content.role
            texts = [p.text for p in content.parts if p.text]
            fcalls = [p.function_call for p in content.parts if p.function_call]
            fresps = [
                p.function_response for p in content.parts if p.function_response
            ]

            if role == "user" and texts and not fresps:
                out.append(HumanMessage(content="\n".join(texts)))
            elif role == "model":
                tool_calls = [
                    {
                        "name": fc.name,
                        "args": dict(fc.args or {}),
                        "id": fc.name,
                    }
                    for fc in fcalls
                ]
                out.append(
                    AIMessage(content="\n".join(texts), tool_calls=tool_calls)
                )
            elif fresps:
                for fr in fresps:
                    out.append(
                        ToolMessage(
                            content=json.dumps(
                                dict(fr.response or {}), default=str
                            ),
                            tool_call_id=fr.name,
                            name=fr.name,
                        )
                    )
        return out

    def _to_lc_tools(self, req: LlmRequest) -> list[dict[str, Any]]:
        """Convert ADK genai Tools into OpenAI-compatible function-tool dicts."""
        tools = (req.config.tools or []) if req.config else []
        out: list[dict[str, Any]] = []
        for t in tools:
            for fd in getattr(t, "function_declarations", None) or []:
                params = (
                    _normalize_schema(fd.parameters.model_dump())
                    if fd.parameters
                    else {"type": "object"}
                )
                out.append(
                    {
                        "type": "function",
                        "function": {
                            "name": fd.name,
                            "description": fd.description or "",
                            "parameters": params,
                        },
                    }
                )
        return out

    @staticmethod
    def _render_system_instruction(sys: Any) -> str:
        if isinstance(sys, str):
            return sys
        if hasattr(sys, "parts"):
            return "\n".join(
                p.text for p in sys.parts if getattr(p, "text", None)
            )
        return str(sys)


# ---------------------------------------------------------------------- #
# Schema normalization — genai → JSON Schema (lowercase types)            #
# ---------------------------------------------------------------------- #

# genai's Type enum dumps as uppercase strings; OpenAI tool params expect
# JSON Schema's lowercase. Map both forms safely.
_TYPE_MAP = {
    "TYPE_UNSPECIFIED": "string",
    "STRING": "string",
    "NUMBER": "number",
    "INTEGER": "integer",
    "BOOLEAN": "boolean",
    "ARRAY": "array",
    "OBJECT": "object",
    "NULL": "null",
}


def _normalize_schema(schema: Any) -> Any:
    """Recursively lowercase 'type' values and recurse into nested schemas.

    Pass-through for non-dict / non-list nodes.
    """
    if isinstance(schema, dict):
        out: dict[str, Any] = {}
        for k, v in schema.items():
            if k == "type" and isinstance(v, str):
                out[k] = _TYPE_MAP.get(v, v.lower())
            elif k in {"properties", "patternProperties"} and isinstance(v, dict):
                out[k] = {pk: _normalize_schema(pv) for pk, pv in v.items()}
            elif k == "items":
                out[k] = _normalize_schema(v)
            elif k in {"anyOf", "oneOf", "allOf", "enum"} and isinstance(v, list):
                out[k] = [_normalize_schema(x) for x in v]
            else:
                out[k] = _normalize_schema(v) if isinstance(v, dict | list) else v
        # Drop empty 'required' list that some schemas emit — confuses validators.
        if out.get("required") == []:
            out.pop("required")
        return out
    if isinstance(schema, list):
        return [_normalize_schema(x) for x in schema]
    return schema


# ---------------------------------------------------------------------- #
# Public factory                                                         #
# ---------------------------------------------------------------------- #


def make_safechain_llm(model_idx: str = "1") -> SafeChainLlm:
    """Wrap `safechain.lcel.model(idx)` as an ADK BaseLlm.

    Mirrors the reference test_safechain_access.py exactly: just call
    `safechain_model(idx)`, no `.bind(...)` wrapper. Temperature, if needed,
    can be set via the LlmAgent's `generate_content_config` and translated by
    a future enhancement of this adapter.
    """
    return SafeChainLlm(
        model=f"safechain/{model_idx}",
        lc_model=safechain_model(model_idx),
    )
