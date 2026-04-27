"""End-to-end runner for the SafeChain → ADK smoke test.

Usage (from the repo root):
    python agent-test/run.py
    python agent-test/run.py --model 3                # use Flash
    python agent-test/run.py --query 'your prompt'
    python agent-test/run.py --json                    # machine-readable

Exit codes:
    0  the agent produced a final response
    1  the agent finished without a final response (something's off)
    2  setup error (SafeChain not installed, .env missing, config bad)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

# This directory has a hyphen, so it isn't a Python package — add it to sys.path
# so we can import the sibling modules directly.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

DEFAULT_QUERY = (
    "Roll an 8-sided die for me, then tell me whether the result is a prime number."
)


async def _run(model_idx: str, query: str, as_json: bool, verbose: bool) -> int:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    try:
        from google.adk.agents.run_config import RunConfig, StreamingMode
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types

        from agent import build_agent
    except ImportError as e:
        print(f"ERROR: missing dependency — {e}", file=sys.stderr)
        print(
            "       run:  pip install 'google-adk>=1.31.1' langchain-core python-dotenv",
            file=sys.stderr,
        )
        return 2

    try:
        agent = build_agent(model_idx=model_idx)
    except ImportError as e:
        # SafeChain / ee_config not installed — typical on non-Amex laptops.
        print(f"ERROR: SafeChain bootstrap failed — {e}", file=sys.stderr)
        return 2

    session_service: InMemorySessionService = (
        InMemorySessionService()  # type: ignore[no-untyped-call]
    )
    runner = Runner(
        app_name="safechain_smoke", agent=agent, session_service=session_service
    )

    user_id = "smoke-user"
    session_id = "smoke-session"
    await session_service.create_session(
        app_name="safechain_smoke", user_id=user_id, session_id=session_id
    )

    msg = types.Content(role="user", parts=[types.Part(text=query)])
    cfg = RunConfig(streaming_mode=StreamingMode.NONE, max_llm_calls=10)

    events: list[dict[str, Any]] = []
    final_text: str | None = None
    started = time.perf_counter()

    async for ev in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=msg,
        run_config=cfg,
    ):
        ev_record: dict[str, Any] = {
            "author": ev.author,
            "partial": ev.partial,
            "is_final": ev.is_final_response(),
            "parts": [],
        }
        if ev.error_code:
            ev_record["error_code"] = ev.error_code
            ev_record["error_message"] = ev.error_message
        if ev.content and ev.content.parts:
            for p in ev.content.parts:
                if p.text:
                    ev_record["parts"].append({"text": p.text})
                elif p.function_call:
                    ev_record["parts"].append(
                        {
                            "function_call": {
                                "name": p.function_call.name,
                                "args": dict(p.function_call.args or {}),
                            }
                        }
                    )
                elif p.function_response:
                    ev_record["parts"].append(
                        {
                            "function_response": {
                                "name": p.function_response.name,
                                "response": dict(p.function_response.response or {}),
                            }
                        }
                    )
        events.append(ev_record)

        if ev.is_final_response() and ev.content and ev.content.parts:
            for p in ev.content.parts:
                if p.text:
                    final_text = p.text

    elapsed = time.perf_counter() - started

    if as_json:
        print(
            json.dumps(
                {
                    "model_idx": model_idx,
                    "query": query,
                    "final_text": final_text,
                    "events": events,
                    "elapsed_secs": round(elapsed, 2),
                },
                indent=2,
                default=str,
            )
        )
    else:
        _print_human(query, model_idx, events, final_text, elapsed)

    return 0 if final_text else 1


def _print_human(
    query: str,
    model_idx: str,
    events: list[dict[str, Any]],
    final_text: str | None,
    elapsed: float,
) -> None:
    print()
    print(f"Model:    safechain/{model_idx}")
    print(f"Query:    {query}")
    print(f"Elapsed:  {elapsed:.1f}s, {len(events)} events")
    print()
    print("Event trace:")
    for i, ev in enumerate(events):
        marker = "(final)" if ev.get("is_final") else ""
        print(f"  [{i}] {ev['author']} {marker}")
        if "error_code" in ev:
            print(f"      ERROR {ev['error_code']}: {ev['error_message']}")
        for part in ev.get("parts", []):
            if "text" in part:
                preview = part["text"][:140].replace("\n", " ")
                print(f"      text: {preview}")
            elif "function_call" in part:
                fc = part["function_call"]
                print(f"      tool_call: {fc['name']}({fc['args']})")
            elif "function_response" in part:
                fr = part["function_response"]
                resp_preview = json.dumps(fr["response"], default=str)[:140]
                print(f"      tool_resp: {fr['name']} → {resp_preview}")
    print()
    if final_text:
        print(f"Final answer:  {final_text}")
        print()
        print("PASS — SafeChain → ADK → Gemini smoke test succeeded.")
    else:
        print(
            "FAIL — agent finished without a final response. "
            "Check the event trace above for errors."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="agent_test.run",
        description="Run the SafeChain → ADK smoke-test agent.",
    )
    parser.add_argument(
        "--model",
        default="1",
        help="SafeChain model idx. '1'=Gemini 2.5 Pro (default), '3'=Flash.",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"Prompt to send. Default: {DEFAULT_QUERY!r}",
    )
    parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON instead of human text."
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args.model, args.query, args.json, args.verbose)))


if __name__ == "__main__":
    main()
