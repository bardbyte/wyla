"""Minimal Vertex AI ADK agent — Gemini 3.1 Pro via prj-d-ea-poc.

This module is imported by run.py *after* the necessary env vars are set
(GOOGLE_GENAI_USE_VERTEXAI, GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION,
GOOGLE_APPLICATION_CREDENTIALS). ADK's google-genai client reads them on first
use, so as long as run.py sets them before `from agent import build_agent`,
the agent talks to Vertex correctly.

Two trivial tools (roll_die + check_prime) so we can verify the full
reason → tool → reason → answer loop, not just LLM connectivity.
"""

from __future__ import annotations

import logging
import random

from google.adk import Agent
from google.adk.tools.tool_context import ToolContext
from google.genai import types

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-3.1-pro-preview"


def roll_die(sides: int, tool_context: ToolContext) -> int:
    """Roll a die with the given number of sides and return the result.

    Args:
        sides: Number of sides on the die. Must be a positive integer.

    Returns:
        Integer between 1 and `sides`, inclusive.
    """
    if sides < 1:
        raise ValueError(f"sides must be ≥ 1, got {sides}")
    result = random.randint(1, sides)
    rolls = list(tool_context.state.get("rolls", []))
    rolls.append(result)
    tool_context.state["rolls"] = rolls
    logger.info("roll_die(sides=%d) → %d", sides, result)
    return result


async def check_prime(nums: list[int]) -> str:
    """Check which numbers in the list are prime.

    Args:
        nums: List of integers to test.

    Returns:
        Human-readable string identifying prime numbers in the list.
    """
    primes: set[int] = set()
    for n in nums:
        n = int(n)
        if n <= 1:
            continue
        is_prime = True
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                is_prime = False
                break
        if is_prime:
            primes.add(n)
    return (
        "No prime numbers found."
        if not primes
        else f"{', '.join(str(n) for n in sorted(primes))} are prime numbers."
    )


def build_agent(model: str = DEFAULT_MODEL) -> Agent:
    return Agent(
        model=model,
        name="vertex_smoke_agent",
        description=(
            "Smoke-test agent that rolls dice and checks prime numbers via "
            "Gemini on Vertex AI. If this responds, the full ADK + Vertex "
            "stack is wired correctly."
        ),
        instruction=(
            "You answer questions about dice rolls and prime numbers.\n"
            "\n"
            "When the user asks for a die roll, call `roll_die` with the\n"
            "integer number of sides — never invent a roll yourself.\n"
            "\n"
            "When the user asks whether numbers are prime, call `check_prime`\n"
            "with a list of integers.\n"
            "\n"
            "When the user asks for both ('roll a die and check if it's prime'),\n"
            "you must:\n"
            "  1. Call roll_die first.\n"
            "  2. Wait for the response.\n"
            "  3. Then call check_prime with the rolled value.\n"
            "  4. Include the rolled value in your final reply.\n"
            "\n"
            "Be concise. One short sentence as the final answer."
        ),
        tools=[roll_die, check_prime],
        generate_content_config=types.GenerateContentConfig(
            temperature=0.0,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
        ),
    )


# Module-level instance — required for `adk web` discovery.
# Construction is cheap (no API calls); the actual model client is lazily
# initialized on first use, by which time run.py / `adk web` has set the
# GOOGLE_* env vars.
root_agent = build_agent()
