"""The switch. ``SAHS_LANGFUSE=1`` in the silo .env turns the mirror
on; the SDK reads its own keys (LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY,
LANGFUSE_BASE_URL — LANGFUSE_HOST on older SDKs). Off, or misconfigured,
means None: the runtime has no observer and nothing else changes.
"""

from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path
from typing import Any, Callable

from .tracer import TurnTracer

_ON = ("1", "true", "yes", "on")


def enabled(env: Any = None) -> bool:
    env = os.environ if env is None else env
    return str(env.get("SAHS_LANGFUSE", "")).strip().lower() in _ON


def full_results(env: Any = None) -> bool:
    env = os.environ if env is None else env
    return str(env.get("SAHS_LANGFUSE_FULL_RESULTS", "")
               ).strip().lower() in _ON


def langfuse_client() -> Any:
    """The SDK client, or a typed error naming the contract."""
    try:
        from langfuse import Langfuse
    except ImportError as e:                          # pragma: no cover
        raise RuntimeError(
            "SAHS_LANGFUSE is on but the SDK is not installed: "
            "pip install langfuse") from e
    # observation ids are pinned by the emitter (a hash of the trace
    # id and the tracer's key) so a backfill updates in place
    from .langfuse_emitter import PINNED
    return Langfuse(id_generator=PINNED)


def langfuse_observer(*, user_id: str = "",
                      model_of: Callable[[str], str] | None = None,
                      prompt_links: Path | None = None,
                      env: Any = None) -> TurnTracer | None:
    if not enabled(env):
        return None
    prompt_of = None
    if prompt_links is not None:
        from .prompts import PromptLinks
        prompt_of = PromptLinks(prompt_links)
    try:
        from .langfuse_emitter import LangfuseEmitter
        emitter = LangfuseEmitter(langfuse_client())
    except Exception as e:                            # noqa: BLE001
        print(f"langfuse: observer not attached: {e}", file=sys.stderr)
        return None
    tracer = TurnTracer(emitter, user_id=user_id, model_of=model_of,
                        prompt_of=prompt_of,
                        full_results=full_results(env),
                        environment=str((os.environ if env is None
                                         else env).get(
                            "SAHS_LANGFUSE_ENV", "")).strip())
    atexit.register(tracer.flush)
    return tracer


__all__ = ["enabled", "full_results", "langfuse_client",
           "langfuse_observer"]
