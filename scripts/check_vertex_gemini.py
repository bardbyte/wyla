#!/usr/bin/env python3
"""Preflight: hit Gemini 3.1 Pro on Vertex AI directly using a service-account
key (independent path from SafeChain — proves whether your GCP-certified
project can reach the model).

Standalone usage:

    # Put the JSON key file SOMEWHERE OUTSIDE this repo, e.g. ~/.gcp/
    # NEVER commit it. The script refuses to load a key from inside the repo.
    export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/prj-d-ea-poc.json

    python scripts/check_vertex_gemini.py
    python scripts/check_vertex_gemini.py --model gemini-3.1-pro-preview
    python scripts/check_vertex_gemini.py --location us-central1 --prompt 'list 3 prime numbers'
    python scripts/check_vertex_gemini.py --json

Defaults (for the prj-d-ea-poc setup you described):
    project   prj-d-ea-poc
    location  us-central1
    model     gemini-3.1-pro-preview
    key file  $GOOGLE_APPLICATION_CREDENTIALS  (or --key-file)

Tool-ready: the `call_vertex_gemini(...)` function returns the standard
{status, ..., error} dict and can be lifted into a tool wrapper later.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types as genai_types
from google.oauth2 import service_account

DEFAULT_PROJECT = "prj-d-ea-poc"
DEFAULT_LOCATION = "us-central1"
DEFAULT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_PROMPT = (
    "Reply in one short sentence. What is the smallest prime number greater than 100?"
)
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


# --------------------------------------------------------------------------- #
# Tool-shaped core                                                            #
# --------------------------------------------------------------------------- #


def call_vertex_gemini(
    key_file: str,
    project: str = DEFAULT_PROJECT,
    location: str = DEFAULT_LOCATION,
    model: str = DEFAULT_MODEL,
    prompt: str = DEFAULT_PROMPT,
) -> dict[str, Any]:
    """Authenticate via service account and call Gemini on Vertex AI.

    Args:
        key_file: Absolute path to the service-account JSON key. MUST be
            outside this repo — the function refuses paths under the repo root.
        project: GCP project ID hosting the Vertex AI Gemini grant.
        location: Vertex AI region (e.g., us-central1, asia-south1).
        model: Vertex Gemini model ID. Default `gemini-3.1-pro-preview`.
        prompt: Test prompt to send.

    Returns:
        dict with:
          status: "success" | "error"
          project, location, model, key_email, key_project_id
          response_text: model output (or empty on error)
          latency_ms, prompt_tokens, response_tokens
          finish_reason: model's stop reason
          error: str | None
    """
    key_path = Path(key_file).expanduser().resolve()
    if not key_path.exists():
        return _err("not_found", f"key file not found: {key_path}")

    repo_root = Path(__file__).resolve().parent.parent
    try:
        key_path.relative_to(repo_root)
        return _err(
            "unsafe_key_location",
            f"refusing to load credentials from inside the repo ({key_path}). "
            "Move the key outside the repo (e.g. ~/.gcp/) and re-run.",
        )
    except ValueError:
        # Good — key is outside the repo.
        pass

    # Surface non-secret fields from the JSON for the human running this.
    try:
        key_data = json.loads(key_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return _err("bad_key_file", f"could not parse key file: {e}")

    key_project_id = key_data.get("project_id", "(unknown)")
    key_email = key_data.get("client_email", "(unknown)")
    key_type = key_data.get("type", "(unknown)")

    if key_type != "service_account":
        return _err(
            "wrong_key_type",
            f"key file 'type' is {key_type!r}, expected 'service_account'",
        )

    try:
        creds = service_account.Credentials.from_service_account_file(
            str(key_path), scopes=SCOPES
        )
    except Exception as e:
        return _err("auth_load_failed", f"failed to load service account: {e}")

    try:
        client = genai.Client(
            vertexai=True,
            project=project,
            location=location,
            credentials=creds,
        )
    except Exception as e:
        return _err("client_init_failed", f"genai.Client init failed: {e}")

    started = time.perf_counter()
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(temperature=0.0),
        )
    except Exception as e:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return {
            "status": "error",
            "project": project,
            "location": location,
            "model": model,
            "key_email": key_email,
            "key_project_id": key_project_id,
            "response_text": "",
            "latency_ms": latency_ms,
            "prompt_tokens": 0,
            "response_tokens": 0,
            "finish_reason": None,
            "error": f"{type(e).__name__}: {e}",
        }
    latency_ms = int((time.perf_counter() - started) * 1000)

    # Pull text + token counts safely; some preview models don't populate every field.
    response_text = (response.text or "").strip()
    finish_reason = None
    try:
        cand = (response.candidates or [None])[0]
        if cand and cand.finish_reason is not None:
            finish_reason = str(cand.finish_reason)
    except Exception:
        pass

    usage = response.usage_metadata
    prompt_tokens = getattr(usage, "prompt_token_count", 0) if usage else 0
    response_tokens = getattr(usage, "candidates_token_count", 0) if usage else 0

    return {
        "status": "success" if response_text else "error",
        "project": project,
        "location": location,
        "model": model,
        "key_email": key_email,
        "key_project_id": key_project_id,
        "response_text": response_text,
        "latency_ms": latency_ms,
        "prompt_tokens": prompt_tokens,
        "response_tokens": response_tokens,
        "finish_reason": finish_reason,
        "error": None if response_text else "model returned empty text",
    }


def _err(code: str, msg: str) -> dict[str, Any]:
    return {
        "status": "error",
        "project": None,
        "location": None,
        "model": None,
        "key_email": None,
        "key_project_id": None,
        "response_text": "",
        "latency_ms": 0,
        "prompt_tokens": 0,
        "response_tokens": 0,
        "finish_reason": None,
        "error": f"{code}: {msg}",
    }


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #


def _format_summary(result: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("Auth:")
    lines.append(f"  Service account:    {result.get('key_email') or '(none)'}")
    lines.append(f"  Key project_id:     {result.get('key_project_id') or '(none)'}")
    lines.append("")
    lines.append("Vertex AI call:")
    lines.append(f"  Project:            {result.get('project') or '(none)'}")
    lines.append(f"  Location:           {result.get('location') or '(none)'}")
    lines.append(f"  Model:              {result.get('model') or '(none)'}")
    lines.append(f"  Latency:            {result.get('latency_ms', 0)} ms")
    if result.get("status") == "success":
        lines.append(
            f"  Tokens:             prompt={result['prompt_tokens']}, "
            f"response={result['response_tokens']}"
        )
        lines.append(f"  Finish reason:      {result.get('finish_reason') or '(unset)'}")
        lines.append("")
        lines.append("Response:")
        for line in result["response_text"].splitlines():
            lines.append(f"  {line}")
        lines.append("")
        lines.append("PASS — Vertex AI Gemini call succeeded.")
    else:
        lines.append("")
        lines.append(f"FAIL: {result.get('error')}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_vertex_gemini",
        description="Call Gemini on Vertex AI via a service-account key.",
    )
    parser.add_argument(
        "--key-file",
        help=(
            "Path to service-account JSON. Default: $GOOGLE_APPLICATION_CREDENTIALS. "
            "Must be OUTSIDE this repo."
        ),
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=(
            f"Default: {DEFAULT_MODEL}. Other options: gemini-3.1-flash-lite, "
            "gemini-2.5-pro, etc."
        ),
    )
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human text.")
    args = parser.parse_args(argv)

    import os

    key_file = args.key_file or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_file:
        print(
            "ERROR: no key file. Pass --key-file PATH or set "
            "$GOOGLE_APPLICATION_CREDENTIALS to the service-account JSON.",
            file=sys.stderr,
        )
        return 2

    result = call_vertex_gemini(
        key_file=key_file,
        project=args.project,
        location=args.location,
        model=args.model,
        prompt=args.prompt,
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(_format_summary(result))

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
