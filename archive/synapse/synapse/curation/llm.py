"""Vertex Gemini caller for entity curation.

Wraps `google.genai` (the new SDK that talks Vertex) with three modes:

    dry_run=True             — never call the model; return the prompt
                               as the 'response' (so the caller can dump
                               it and you can paste it into a chat).
    GOOGLE_APPLICATION_CREDENTIALS unset → forced dry-run.
    real call                — Vertex Gemini 3.1 Pro, temperature 0,
                               structured-output disabled (we want the
                               YAML the prompt asks for).

The function is intentionally thin — error handling stays close to the
SDK so failures are diagnosable. We do NOT silently fall back; if the
LLM fails, the caller decides what to do.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger("synapse.curation.llm")

DEFAULT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_PROJECT = os.environ.get("LUMI_VERTEX_PROJECT", "your-vertex-project")
DEFAULT_LOCATION = os.environ.get("LUMI_VERTEX_LOCATION", "global")


@dataclass
class LLMResult:
    response_text: str
    model: str
    dry_run: bool
    error: str = ""


def _have_credentials() -> bool:
    # Checks the resolved Vertex key path (LUMI_VERTEX_SA_KEY first,
    # then GOOGLE_APPLICATION_CREDENTIALS fallback).
    try:
        from synapse.utils.auth import resolve_vertex_key_path
    except ImportError:
        p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return bool(p) and os.path.exists(p)
    return resolve_vertex_key_path() is not None


def call_gemini(
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    project: str = DEFAULT_PROJECT,
    location: str = DEFAULT_LOCATION,
    temperature: float = 0.0,
    dry_run: bool = False,
) -> LLMResult:
    """Call Vertex Gemini. Returns LLMResult.

    dry_run is auto-forced if credentials aren't available."""
    if dry_run or not _have_credentials():
        return LLMResult(
            response_text=prompt,
            model=model,
            dry_run=True,
            error=(
                "" if dry_run
                else "no Vertex SA key resolvable "
                     "(LUMI_VERTEX_SA_KEY or GOOGLE_APPLICATION_CREDENTIALS)"
            ),
        )

    try:
        from google.genai import types as genai_types  # type: ignore[import-not-found]
        from synapse.utils.auth import build_vertex_genai_client
    except ImportError as e:
        return LLMResult(
            response_text="",
            model=model,
            dry_run=False,
            error=f"google-genai or synapse.utils.auth not available: {e}. "
                  f"Install with: pip install google-genai",
        )

    try:
        # Use the shared auth helper so corporate-TLS + LUMI_VERTEX_SA_KEY
        # get the same treatment as everywhere else.
        client = build_vertex_genai_client(project=project, location=location)
        cfg = genai_types.GenerateContentConfig(
            temperature=temperature,
        )
        result = client.models.generate_content(
            model=model,
            contents=prompt,
            config=cfg,
        )
        text = getattr(result, "text", None) or ""
        if not text:
            return LLMResult(
                response_text="",
                model=model,
                dry_run=False,
                error="Vertex returned empty response",
            )
        return LLMResult(response_text=text, model=model, dry_run=False)
    except Exception as e:  # noqa: BLE001
        logger.exception("Vertex call failed")
        return LLMResult(
            response_text="",
            model=model,
            dry_run=False,
            error=f"{type(e).__name__}: {e}",
        )
