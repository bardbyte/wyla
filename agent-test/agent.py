"""Minimal ADK agent backed by SafeChain Gemini 2.5 Pro.

This is the canonical "hello world" ADK pattern (modeled on Google's
hello_world sample with dice + prime tools), wired to SafeChain instead of the
default Gemini model loader.

If `python -m agent_test.run` succeeds, it proves end-to-end:
  1. SafeChain auth works (CIBIS creds + config.yml)
  2. SafeChainLlm adapter correctly translates ADK ↔ LangChain
  3. Gemini 2.5 Pro reasons over the prompt
  4. The model emits tool calls
  5. ADK invokes the tools and feeds responses back
  6. Multi-step reason→act→reason loop completes cleanly
"""

from __future__ import annotations

import logging
import random
import sys
from pathlib import Path

# This directory ('agent-test/') has a hyphen, so it can't be a Python package.
# Add it to sys.path so we can import sibling modules directly.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from google.adk import Agent  # noqa: E402
from google.adk.tools.tool_context import ToolContext  # noqa: E402
from google.genai import types  # noqa: E402

from safechain_adk import make_safechain_llm  # noqa: E402

logger = logging.getLogger(__name__)


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
    # Persist roll history in session state — proves tool_context wiring works.
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


# Build the agent. `make_safechain_llm("1")` returns a SafeChainLlm wrapping
# Gemini 2.5 Pro via SafeChain. Swap to "3" for Flash if you want a faster /
# cheaper run during iteration.
def build_agent(model_idx: str = "1") -> Agent:
    return Agent(
        model=make_safechain_llm(model_idx),
        name="safechain_smoke_agent",
        description=(
            "Smoke-test agent that rolls dice and checks prime numbers. "
            "If this responds, SafeChain → ADK → Gemini is wired correctly."
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
            # Safety settings off so trivial test prompts don't get blocked.
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
        ),
    )


# Module-level instance — `adk run agent_test/` discovers `root_agent`.
root_agent = build_agent()
