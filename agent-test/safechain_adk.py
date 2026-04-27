"""SafeChain → ADK BaseLlm adapter.

Wraps the LangChain-compatible chat client returned by `safechain.lcel.model(idx)`
as a `google.adk.models.BaseLlm`, so an ADK `LlmAgent` can use a SafeChain-routed
Gemini model the same way it would use any native ADK model.

Usage:
    from safechain_adk import make_safechain_llm
    from google.adk.agents import LlmAgent

    llm = make_safechain_llm("1")     # Gemini 2.5 Pro via SafeChain
    agent = LlmAgent(name="x", model=llm, instruction="...", tools=[my_tool])
"""

from __future__ import annotations

import json
import logging
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

# Load .env so SafeChain can read CIBIS_CONSUMER_INTEGRATION_ID,
# CIBIS_CONSUMER_SECRET, and CONFIG_PATH on first model() call.
load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)


class SafeChainLlm(BaseLlm):
    """Wrap a SafeChain LangChain chat model as an ADK BaseLlm.

    Errors are converted into a single error-shaped `LlmResponse` rather than
    raising, because raising inside `generate_content_async` hangs the runner.
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
            bound = self.lc_model.bind_tools(lc_tools) if lc_tools else self.lc_model

            if stream:
                buf = ""
                async for chunk in bound.astream(messages):
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

            ai: AIMessage = await bound.ainvoke(messages)
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
            logger.exception("SafeChainLlm.generate_content_async failed")
            yield LlmResponse(
                error_code="SAFECHAIN_ERROR",
                error_message=f"{type(e).__name__}: {e}",
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
            fresps = [p.function_response for p in content.parts if p.function_response]

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
        tools = (req.config.tools or []) if req.config else []
        out: list[dict[str, Any]] = []
        for t in tools:
            for fd in getattr(t, "function_declarations", None) or []:
                out.append(
                    {
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


def make_safechain_llm(model_idx: str = "1", temperature: float = 0.0) -> SafeChainLlm:
    """Wrap `safechain.lcel.model(idx)` as an ADK BaseLlm.

    model_idx: SafeChain model index — "1" = Gemini 2.5 Pro, "3" = Flash.
    temperature: Bound on the LangChain side via `.bind(temperature=...)`.
    """
    lc_client = safechain_model(model_idx).bind(temperature=temperature)
    return SafeChainLlm(model=f"safechain/{model_idx}", lc_model=lc_client)
