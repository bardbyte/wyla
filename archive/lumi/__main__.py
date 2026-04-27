"""Run the LUMI pipeline end-to-end.

    python -m lumi [--config lumi_config.yaml]

Loads config, builds the agent tree, creates a Session, runs to completion,
and prints summary metrics. Relies on Amex SafeChain (`src.adapters.model_adapter`)
being importable — fails fast with a readable error if not.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from lumi.config import ConfigError, load_config

logger = logging.getLogger("lumi")


async def _run(config_path: str, verbose: bool) -> int:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    load_dotenv(find_dotenv())

    try:
        cfg = load_config(config_path)
    except ConfigError as e:
        logger.error("Config error: %s", e)
        return 2

    try:
        from google.adk.agents.run_config import RunConfig, StreamingMode
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types

        from lumi.agent import build_root_agent
    except ImportError as e:
        logger.error(
            "Import failure: %s. On a fresh machine run `pip install -e .` first.", e
        )
        return 2

    try:
        agent = build_root_agent(cfg)
    except ImportError as e:
        logger.error(
            "LLM adapter unavailable (%s). Ensure SafeChain + .env + CONFIG_PATH "
            "are set up per docs/README.md before running.", e
        )
        return 2

    session_service: InMemorySessionService = InMemorySessionService()  # type: ignore[no-untyped-call]
    runner = Runner(app_name="lumi", agent=agent, session_service=session_service)

    user_id = "lumi"
    session_id = "lumi-run"
    session = await session_service.create_session(
        app_name="lumi", user_id=user_id, session_id=session_id
    )
    session.state["lumi_config"] = cfg.model_dump()

    new_msg = types.Content(
        role="user",
        parts=[
            types.Part(
                text="Run the LUMI enrichment pipeline on the configured inputs."
            )
        ],
    )
    run_cfg = RunConfig(streaming_mode=StreamingMode.NONE, max_llm_calls=200)

    async for ev in runner.run_async(
        user_id=user_id, session_id=session_id, new_message=new_msg, run_config=run_cfg
    ):
        if ev.content and ev.content.parts:
            for p in ev.content.parts:
                if p.text:
                    logger.info("[%s] %s", ev.author, p.text[:200])
        if ev.error_code:
            logger.error("[%s] %s: %s", ev.author, ev.error_code, ev.error_message)

    report = session.state.get("coverage_report") or {}
    out_dir = Path(cfg.output.directory).resolve()
    logger.info("=== LUMI DONE ===")
    logger.info("Output directory: %s", out_dir)
    if report:
        logger.info(
            "Coverage: %s/%s (%.1f%%), %s partial, %s failed",
            report.get("passed"),
            report.get("total_queries"),
            report.get("coverage_pct", 0.0),
            report.get("partial"),
            report.get("failed"),
        )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="lumi", description="Run the LUMI pipeline.")
    parser.add_argument("--config", default="lumi_config.yaml", help="Path to lumi_config.yaml")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args.config, args.verbose)))


if __name__ == "__main__":
    main()
