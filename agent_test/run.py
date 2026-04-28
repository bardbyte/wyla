"""End-to-end runner for the Vertex AI → ADK smoke test.

Same path that scripts/check_vertex_gemini.py proved out, plus an actual ADK
LlmAgent with two tools so the reason → tool → reason → answer loop is
exercised.

Usage (from the repo root):
    python agent_test/run.py --key-file ~/Downloads/key.json
    python agent_test/run.py --key-file ~/Downloads/key.json --insecure
    python agent_test/run.py --key-file ~/Downloads/key.json --model gemini-2.5-pro
    python agent_test/run.py --query 'roll a 20-sided die' --json

Defaults match the certified-access GCP project:
    project   prj-d-ea-poc
    location  global
    model     gemini-3.1-pro-preview

Exit codes:
    0  agent produced a final answer
    1  finished without a final answer (something's off in the trace)
    2  setup error (missing key file, in-repo key, bad CA bundle)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import ssl
import sys
import time
import warnings
from pathlib import Path
from typing import Any

DEFAULT_PROJECT = "prj-d-ea-poc"
DEFAULT_LOCATION = "global"
DEFAULT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_QUERY = (
    "Roll an 8-sided die for me, then tell me whether the result is a prime number."
)


# --------------------------------------------------------------------------- #
# TLS — must run BEFORE any google.* import on a corporate-MITM network        #
# --------------------------------------------------------------------------- #


def _setup_tls(insecure: bool, ca_bundle: str | None) -> str:
    """Make Python trust the corporate root CA. Returns a label for the human."""
    # 1. truststore — the right answer on macOS (uses Keychain).
    try:
        import truststore

        truststore.inject_into_ssl()
        return "truststore (OS Keychain)"
    except ImportError:
        pass

    # 2. Explicit CA bundle.
    if ca_bundle:
        p = Path(ca_bundle).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"CA bundle not found: {p}")
        bundle = str(p)
        for var in (
            "REQUESTS_CA_BUNDLE",
            "SSL_CERT_FILE",
            "CURL_CA_BUNDLE",
            "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
        ):
            os.environ[var] = bundle
        return f"CA bundle: {p}"

    # 3. Hard bypass.
    if insecure:
        ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
        os.environ["PYTHONHTTPSVERIFY"] = "0"
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except ImportError:
            pass
        try:
            import httpx

            _orig_client = httpx.Client
            _orig_async = httpx.AsyncClient

            def _client_no_verify(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("verify", False)
                return _orig_client(*args, **kwargs)

            def _async_no_verify(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("verify", False)
                return _orig_async(*args, **kwargs)

            httpx.Client = _client_no_verify  # type: ignore[misc]
            httpx.AsyncClient = _async_no_verify  # type: ignore[misc]
        except ImportError:
            pass
        try:
            import google.auth.transport.requests as gat

            _orig_session = gat.AuthorizedSession

            class _NoVerifyAuthorizedSession(_orig_session):  # type: ignore[misc, valid-type]
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.verify = False

            gat.AuthorizedSession = _NoVerifyAuthorizedSession  # type: ignore[misc]
        except ImportError:
            pass
        warnings.warn(
            "SSL verification disabled — only safe on networks you already trust.",
            stacklevel=2,
        )
        return "DISABLED (--insecure)"

    return "default certifi"


# --------------------------------------------------------------------------- #
# Vertex env — sets the four vars ADK needs, then we can import ADK            #
# --------------------------------------------------------------------------- #


def _setup_vertex_env(key_file: str, project: str, location: str) -> tuple[str, str]:
    """Returns (key_email, key_project_id) as a sanity check."""
    key_path = Path(key_file).expanduser().resolve()
    if not key_path.exists():
        raise FileNotFoundError(f"key file not found: {key_path}")

    # Refuse to load from inside the repo — credentials must live elsewhere.
    repo_root = Path(__file__).resolve().parent.parent
    try:
        key_path.relative_to(repo_root)
        raise PermissionError(
            f"refusing to load credentials from inside the repo ({key_path}). "
            "Move the key outside the repo (e.g. ~/.gcp/) and re-run."
        )
    except ValueError:
        pass  # outside the repo — good

    try:
        key_data = json.loads(key_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise RuntimeError(f"could not parse key file: {e}") from e

    if key_data.get("type") != "service_account":
        raise RuntimeError(
            f"key file 'type' is {key_data.get('type')!r}, expected 'service_account'"
        )

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(key_path)
    os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"
    os.environ["GOOGLE_CLOUD_PROJECT"] = project
    os.environ["GOOGLE_CLOUD_LOCATION"] = location

    return key_data.get("client_email", "(unknown)"), key_data.get(
        "project_id", "(unknown)"
    )


# --------------------------------------------------------------------------- #
# Run                                                                          #
# --------------------------------------------------------------------------- #


async def _run(
    model: str,
    query: str,
    project: str,
    location: str,
    tls_strategy: str,
    key_email: str,
    key_project_id: str,
    as_json: bool,
    verbose: bool,
) -> int:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Now safe to import ADK — env is fully configured.
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

    from agent import build_agent
    from google.adk.agents.run_config import RunConfig, StreamingMode
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types

    agent = build_agent(model=model)
    session_service: InMemorySessionService = (
        InMemorySessionService()  # type: ignore[no-untyped-call]
    )
    runner = Runner(
        app_name="vertex_smoke", agent=agent, session_service=session_service
    )

    user_id = "smoke-user"
    session_id = "smoke-session"
    await session_service.create_session(
        app_name="vertex_smoke", user_id=user_id, session_id=session_id
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
                    "tls_strategy": tls_strategy,
                    "project": project,
                    "location": location,
                    "model": model,
                    "key_email": key_email,
                    "key_project_id": key_project_id,
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
        _print_human(
            tls_strategy,
            project,
            location,
            model,
            key_email,
            key_project_id,
            query,
            events,
            final_text,
            elapsed,
        )

    return 0 if final_text else 1


def _print_human(
    tls_strategy: str,
    project: str,
    location: str,
    model: str,
    key_email: str,
    key_project_id: str,
    query: str,
    events: list[dict[str, Any]],
    final_text: str | None,
    elapsed: float,
) -> None:
    print()
    print(f"TLS:                {tls_strategy}")
    print(f"Service account:    {key_email}")
    print(f"Key project_id:     {key_project_id}")
    print(f"Vertex project:     {project}")
    print(f"Vertex location:    {location}")
    print(f"Model:              {model}")
    print(f"Query:              {query}")
    print(f"Elapsed:            {elapsed:.1f}s, {len(events)} events")
    print()
    print("Event trace:")
    for i, ev in enumerate(events):
        marker = "(final)" if ev.get("is_final") else ""
        print(f"  [{i}] {ev['author']} {marker}")
        if "error_code" in ev:
            print(f"      ERROR {ev['error_code']}:")
            for line in (ev.get("error_message") or "").splitlines():
                print(f"        {line}")
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
        print("PASS — Vertex AI → ADK → Gemini smoke test succeeded.")
    else:
        print(
            "FAIL — agent finished without a final response. "
            "Check the event trace above for errors."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python agent_test/run.py",
        description="Run the Vertex AI → ADK smoke-test agent.",
    )
    parser.add_argument(
        "--key-file",
        help="Path to service-account JSON. Default: $GOOGLE_APPLICATION_CREDENTIALS.",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Default: {DEFAULT_MODEL}. Try gemini-2.5-pro if 3.1 isn't allowlisted.",
    )
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--ca-bundle", help="Corporate root CA bundle (.pem) path.")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL verification (corporate-MITM bypass — see check_vertex_gemini docs).",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    key_file = args.key_file or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_file:
        print(
            "ERROR: pass --key-file PATH or set $GOOGLE_APPLICATION_CREDENTIALS.",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        tls_strategy = _setup_tls(args.insecure, args.ca_bundle)
        key_email, key_project_id = _setup_vertex_env(
            key_file, args.project, args.location
        )
    except (FileNotFoundError, PermissionError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    sys.exit(
        asyncio.run(
            _run(
                args.model,
                args.query,
                args.project,
                args.location,
                tls_strategy,
                key_email,
                key_project_id,
                args.json,
                args.verbose,
            )
        )
    )


if __name__ == "__main__":
    main()
