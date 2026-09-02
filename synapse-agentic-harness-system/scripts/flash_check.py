#!/usr/bin/env python3
"""flash_check.py — which Flash-tier models can THIS laptop actually use?

    python scripts/flash_check.py                 # probe the default list
    python scripts/flash_check.py --also-pro      # include the Pro control
    python scripts/flash_check.py --models gemini-3.5-flash,gemini-2.5-flash
    cat docs/evals/flash_access.md                # then PASTE it

Runs the SAME bootstrap as vertex_check.py (.env → key → endpoint →
proxy → TLS → OAuth token), then asks two separate questions per
candidate model id, because they fail differently:

  listed   GET  …/publishers/google/models/{id}      — visible to this
                                                       project+location
  answers  POST …/models/{id}:generateContent        — actually usable:
                                                       enabled, permitted,
                                                       within quota

Each probe is ONE request with no retry ladder, so a 404 costs a
second, not six minutes. The verdict per model is one of:
  ok · not_found (404) · forbidden (403) · quota (429) · org_policy
  (400 naming the organization's allowed-models constraint: the model
  exists, the project may not use it) · rejected (other 400) · error. Latency and token usage ride along for the ones that answer.

Why: Synapse v3 wants a cheap, fast model for the memory pass and the
judge (design §7/§8). The recommendation line at the end names the
fastest answering Flash as the value for VERTEX_FLASH_MODEL.

Exit 0 = at least one Flash answers · 1 = none do · 3 = env/auth.
The laptop cannot push — the report travels by PASTE.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import AuthError, VertexConnection      # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH                 # noqa: E402

# newest first; the research names 3.5/3.6/3.7 Flash through 2026 and
# 3.1/3 as the earlier generation — ids are guesses to be CONFIRMED,
# which is exactly what this probe is for
DEFAULT_CANDIDATES = [
    "gemini-3.7-flash", "gemini-3.6-flash", "gemini-3.5-flash",
    "gemini-3.5-flash-lite", "gemini-3.1-flash",
    "gemini-3.1-flash-preview", "gemini-3-flash-preview",
    "gemini-2.5-flash", "gemini-2.5-flash-lite",
]
PROBE_PROMPT = "Reply with the single word OK."


def classify(exc: Exception) -> tuple[str, str]:
    """(verdict, detail) for a failed probe — the codes that matter
    are the ones that mean different things for the laptop."""
    if isinstance(exc, urllib.error.HTTPError):
        body = ""
        try:
            body = exc.read().decode("utf-8", "ignore")[:300]
        except Exception:
            pass
        code = exc.code
        verdict = {404: "not_found", 403: "forbidden",
                   429: "quota", 400: "rejected"}.get(code, "error")
        if code == 400 and "organization policy" in body.lower():
            # the model EXISTS; the org's allowed-models constraint
            # refuses it for this project — an admin ask, not a typo
            verdict = "org_policy"
        return verdict, f"HTTP {code} " + " ".join(body.split())[:160]
    return "error", f"{type(exc).__name__}: {exc}"


def probe_listed(client: Any) -> tuple[str, str]:
    """Is the publisher model visible to this project + location?"""
    c = client.connection
    url = (f"{c.endpoint}/v1/projects/{c.project}/locations/"
           f"{c.location}/publishers/google/models/{c.model}")
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {client._token()}"})
    try:
        with urllib.request.urlopen(
                request, timeout=30,
                context=c.ssl_context()) as response:
            meta = json.loads(response.read().decode("utf-8"))
        return "ok", str(meta.get("versionId") or meta.get("name")
                         or "listed")
    except Exception as exc:                       # noqa: BLE001
        return classify(exc)


def probe_answers(client: Any, max_output_tokens: int = 512
                  ) -> dict[str, Any]:
    """One generateContent, no retries: does the model answer, how
    fast, and at what token cost? A reasoning model thinks before it
    speaks, so the cap is not token-pinched (the vertex_check lesson)."""
    body = {"contents": [{"role": "user",
                          "parts": [{"text": PROBE_PROMPT}]}],
            "generationConfig": {"maxOutputTokens": max_output_tokens}}
    started = time.perf_counter()
    try:
        raw = client._post(body)
    except Exception as exc:                       # noqa: BLE001
        verdict, detail = classify(exc)
        return {"verdict": verdict, "detail": detail,
                "latency_ms": round((time.perf_counter() - started)
                                    * 1000)}
    latency = round((time.perf_counter() - started) * 1000)
    meta = raw.get("usageMetadata") or {}
    parts = (((raw.get("candidates") or [{}])[0].get("content")
              or {}).get("parts") or [])
    text = " ".join(str(p.get("text", "")) for p in parts
                    if isinstance(p, dict) and not p.get("thought")
                    ).strip()
    finish = (raw.get("candidates") or [{}])[0].get("finishReason", "")
    return {"verdict": "ok" if text else "empty",
            "detail": (text[:60] if text
                       else f"no text (finishReason {finish})"),
            "latency_ms": latency,
            "prompt_tokens": int(meta.get("promptTokenCount") or 0),
            "output_tokens": int(meta.get("candidatesTokenCount") or 0),
            "thought_tokens": int(meta.get("thoughtsTokenCount") or 0)}


def probe_all(connection: VertexConnection, models: list[str], *,
              client_factory: Any = None,
              token: str | None = None) -> list[dict[str, Any]]:
    from sahs.enrich.client import VertexClient
    rows = []
    for model in models:
        conn = dataclasses.replace(connection, model=model)
        client = (client_factory(conn) if client_factory
                  else VertexClient(conn,
                                    token_provider=(lambda t=token: t)
                                    if token else None))
        listed, listed_detail = probe_listed(client)
        answered = probe_answers(client)
        rows.append({"model": model, "listed": listed,
                     "listed_detail": listed_detail, **answered})
        mark = "✓" if answered["verdict"] == "ok" else "✗"
        print(f"  {mark} {model:<28} listed {listed:<10} "
              f"answers {answered['verdict']:<10} "
              f"{answered['latency_ms']:>6} ms  "
              f"{answered.get('detail', '')[:50]}")
    return rows


def render_markdown(rows: list[dict[str, Any]], *, label: str) -> str:
    lines = ["# Flash access — " + label, "",
             "| model | listed | answers | latency | tokens "
             "(prompt / output / thought) | detail |",
             "| --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        tokens = (f"{r.get('prompt_tokens', '')} / "
                  f"{r.get('output_tokens', '')} / "
                  f"{r.get('thought_tokens', '')}"
                  if r["verdict"] in ("ok", "empty") else "")
        lines.append(f"| {r['model']} | {r['listed']} | {r['verdict']} "
                     f"| {r['latency_ms']} ms | {tokens} | "
                     f"{str(r.get('detail', ''))[:80]} |")
    usable = [r for r in rows if r["verdict"] == "ok"
              and "flash" in r["model"]]
    blocked = [r["model"] for r in rows if r["verdict"] == "org_policy"]
    lines.append("")
    if blocked:
        lines.append("**blocked by organization policy** (the models "
                     "exist; the project's allowed-models constraint "
                     "refuses them): " + ", ".join(blocked))
        lines.append("→ ask the org admin to allow one of them for "
                     "this project under "
                     "`constraints/vertexai.allowedModels` — "
                     f"{blocked[0]} first; until then the memory pass "
                     "and judge run on the Pro model at thinking low")
        lines.append("")
    if usable:
        best = min(usable, key=lambda r: r["latency_ms"])
        lines.append(f"**recommended VERTEX_FLASH_MODEL={best['model']}** "
                     f"(fastest answering Flash, {best['latency_ms']} ms)")
    else:
        lines.append("**no Flash model answered** — the memory pass "
                     "and judge fall back to the Pro model until one "
                     "is enabled (Model Garden → enable, or the "
                     "SVC-ID needs aiplatform.endpoints.predict)")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flash_check.py")
    parser.add_argument("--models", default="",
                        help="comma-separated model ids to probe "
                             "(default: the Flash candidate list)")
    parser.add_argument("--also-pro", action="store_true",
                        help="include the configured Pro model as a "
                             "control that proves the probe itself")
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    args = parser.parse_args(argv)

    try:
        connection = VertexConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  run python scripts/vertex_check.py first — this probe "
              "rides the same .env contract", file=sys.stderr)
        return EXIT_ENV_AUTH

    models = [m.strip() for m in args.models.split(",") if m.strip()] \
        or list(DEFAULT_CANDIDATES)
    if args.also_pro and connection.model not in models:
        models.insert(0, connection.model)

    from sahs.enrich.client import EnrichTransportError, VertexClient
    print(f"project {connection.project} · location "
          f"{connection.location} · endpoint {connection.endpoint}")
    print("fetching OAuth token…")
    try:
        token = VertexClient(connection)._token()
    except (EnrichTransportError, Exception) as e:     # noqa: BLE001
        print(f"✗ token refresh failed: {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    print(f"✓ token acquired · probing {len(models)} model ids "
          "(one GET + one tiny generate each, no retries)\n")

    rows = probe_all(connection, models, token=token)
    label = f"{connection.project} · {connection.location}"
    markdown = render_markdown(rows, label=label)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "flash_access.json").write_text(
        json.dumps(rows, indent=1), encoding="utf-8")
    (out / "flash_access.md").write_text(markdown, encoding="utf-8")
    print("\n" + markdown)
    print("→ PASTE docs/evals/flash_access.md back into the session.")
    return 0 if any(r["verdict"] == "ok" and "flash" in r["model"]
                    for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
