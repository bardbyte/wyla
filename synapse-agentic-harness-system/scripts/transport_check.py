#!/usr/bin/env python3
"""transport_check.py — decision 1 of the v3 harness, answered by the
laptop: can OUR REST client carry a full native-tool round trip on
Gemini 3.1 Pro, or do we need the SDK and the Interactions API?

    python scripts/transport_check.py             # the four probes
    cat docs/evals/transport_access.md            # then PASTE it

Probes, each one request with no retry ladder:

  round_trip   generateContent with a function declaration →
               functionCall (with its thoughtSignature) → we echo the
               model's content verbatim plus a functionResponse →
               final text. This is Stage 1's whole contract in one
               exchange. ok = the model used the tool result to
               answer; signature_echo says whether the second call
               accepted the echoed thoughtSignature.
  stream_tools streamGenerateContent (SSE) with the same tool: does a
               functionCall part arrive over the stream, and with a
               signature? (streaming function-call args is a Gemini 3
               feature the activity line wants)
  sdk          is google-genai importable here? (the SDK path would
               need it, plus a second pass at the corporate TLS trust)
  interactions best-effort GETs against candidate Interactions API
               paths — REST path shapes are unverified, so the codes
               are evidence, not a verdict

Verdict at the end: round_trip ok → decision 1 = the REST client
(recommended); otherwise the detail says what failed. Exit 0 when the
round trip works, 1 when it does not, 3 for env/auth.
The laptop cannot push — the report travels by PASTE.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))

from sahs.util.auth import AuthError, VertexConnection      # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH                 # noqa: E402

TOOL = {"functionDeclarations": [{
    "name": "lookup_metric",
    "description": "Look up a governed metric by name and return its "
                   "certified definition.",
    "parameters": {"type": "OBJECT", "properties": {
        "name": {"type": "STRING"}}, "required": ["name"]}}]}
ASK = ("Use lookup_metric to find the metric called 'Acquirer Net "
       "Spend', then tell me in one sentence what its definition says.")
CANNED = {"metric": "Acquirer Net Spend", "status": "certified",
          "definition": "sum(trans_usd_am) on dw.gms_transaction "
                        "where the merchant settled in USD"}


def _classify(exc: Exception) -> tuple[str, str]:
    import flash_check
    return flash_check.classify(exc)


def _parts(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return (((raw.get("candidates") or [{}])[0].get("content") or {})
            .get("parts") or [])


def probe_round_trip(client: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "contents": [{"role": "user", "parts": [{"text": ASK}]}],
        "tools": [TOOL],
        "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}},
        "generationConfig": {"maxOutputTokens": 1024}}
    started = time.perf_counter()
    try:
        first = client._post(body)
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict, "stage": "call_1", "detail": detail}
    parts = _parts(first)
    calls = [p for p in parts if "functionCall" in p]
    if not calls:
        return {"verdict": "no_call", "stage": "call_1",
                "detail": json.dumps(parts)[:200]}
    call = calls[0]["functionCall"]
    signed = any("thoughtSignature" in p for p in parts)
    # echo the model's content VERBATIM (signatures included), then
    # answer the call — exactly what the v3 loop will do
    body["contents"].append({"role": "model", "parts": parts})
    body["contents"].append({"role": "user", "parts": [{
        "functionResponse": {"name": call.get("name", "lookup_metric"),
                             **({"id": call["id"]} if call.get("id")
                                else {}),
                             "response": CANNED}}]})
    try:
        second = client._post(body)
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict, "stage": "call_2",
                "signature_returned": signed, "detail": detail,
                "hint": "a 400 naming thought_signature on call 2 "
                        "means the echo dropped it — the loop must "
                        "send parts back untouched"}
    text = " ".join(str(p.get("text", "")) for p in _parts(second)
                    if not p.get("thought")).strip()
    grounded = "usd" in text.lower() or "gms_transaction" in text.lower()
    return {"verdict": "ok" if text and grounded else
            ("ungrounded" if text else "empty"),
            "signature_returned": signed, "signature_echo": "accepted",
            "call": {"name": call.get("name"), "args": call.get("args"),
                     "id": call.get("id")},
            "answer": text[:160],
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "thought_tokens": int((second.get("usageMetadata") or {})
                                  .get("thoughtsTokenCount") or 0)}


def probe_stream_tools(client: Any) -> dict[str, Any]:
    body = {"contents": [{"role": "user", "parts": [{"text": ASK}]}],
            "tools": [TOOL],
            "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}},
            "generationConfig": {"maxOutputTokens": 1024}}
    request = urllib.request.Request(
        client._url("streamGenerateContent") + "?alt=sse",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {client._token()}",
                 "Content-Type": "application/json"}, method="POST")
    started = time.perf_counter()
    chunks, calls, signed, text_chunks = 0, [], False, 0
    try:
        with urllib.request.urlopen(
                request, timeout=120,
                context=client.connection.ssl_context()) as response:
            for raw in response:
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                try:
                    chunk = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue
                chunks += 1
                for part in _parts(chunk):
                    if "functionCall" in part:
                        calls.append(part["functionCall"])
                    if "thoughtSignature" in part:
                        signed = True
                    if part.get("text") and not part.get("thought"):
                        text_chunks += 1
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict, "detail": detail}
    return {"verdict": "ok" if calls else "no_call", "chunks": chunks,
            "calls": len(calls), "signature_in_stream": signed,
            "text_chunks": text_chunks,
            "latency_ms": round((time.perf_counter() - started) * 1000)}


def probe_sdk() -> dict[str, Any]:
    try:
        import google.genai as genai                 # type: ignore
        return {"installed": True,
                "version": getattr(genai, "__version__", "?")}
    except ImportError:
        return {"installed": False,
                "note": "the SDK path would add google-genai (httpx) "
                        "and re-prove the corporate TLS trust"}


def probe_interactions(client: Any) -> dict[str, Any]:
    c = client.connection
    out: dict[str, Any] = {}
    for version in ("v1beta1", "v1"):
        url = (f"{c.endpoint}/{version}/projects/{c.project}/locations/"
               f"{c.location}/interactions")
        request = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {client._token()}"})
        try:
            with urllib.request.urlopen(
                    request, timeout=30,
                    context=c.ssl_context()) as response:
                out[version] = f"HTTP {response.status}"
        except urllib.error.HTTPError as exc:
            out[version] = f"HTTP {exc.code}"
        except Exception as exc:                    # noqa: BLE001
            out[version] = f"{type(exc).__name__}"
    out["note"] = ("path shapes unverified: 404 on both says nothing "
                   "certain; 401/403 says the surface exists but this "
                   "SVC-ID may not use it")
    return out


def recommend(report: dict[str, Any]) -> str:
    rt = report["round_trip"]
    if rt.get("verdict") == "ok":
        line = ("decision 1 → the REST client: a full native-tool round "
                "trip works on the proven transport"
                + (" and the echoed thought signature was accepted"
                   if rt.get("signature_returned") else
                   " (no thought signature was returned — fine, the "
                   "loop echoes parts verbatim either way)")
                + ("; streaming delivers function calls too"
                   if report["stream_tools"].get("verdict") == "ok"
                   else "; streaming did NOT deliver a function call — "
                        "the loop can fall back to non-streamed calls "
                        "for tool steps"))
        if not report["sdk"]["installed"]:
            line += ". The SDK is not installed and is not needed."
        return line
    return ("decision 1 → unresolved: the round trip failed at "
            f"{rt.get('stage', '?')} ({rt.get('verdict')}): "
            f"{rt.get('detail', '')[:160]}. Fix that before choosing; "
            "the SDK would hit the same endpoint")


def render_markdown(report: dict[str, Any], *, label: str) -> str:
    lines = ["# Transport check — " + label, ""]
    for key in ("round_trip", "stream_tools", "sdk", "interactions"):
        lines.append(f"- **{key}**: `{json.dumps(report[key])[:400]}`")
    lines += ["", "**" + recommend(report) + "**"]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transport_check.py")
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    args = parser.parse_args(argv)
    try:
        connection = VertexConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    from sahs.enrich.client import VertexClient
    print(f"project {connection.project} · location "
          f"{connection.location} · model {connection.model}")
    try:
        token = VertexClient(connection)._token()
    except Exception as e:                          # noqa: BLE001
        print(f"✗ token refresh failed: {e}", file=sys.stderr)
        return EXIT_ENV_AUTH
    client = VertexClient(connection, token_provider=lambda: token)
    report: dict[str, Any] = {}
    for name, fn in (("round_trip", lambda: probe_round_trip(client)),
                     ("stream_tools", lambda: probe_stream_tools(client)),
                     ("sdk", probe_sdk),
                     ("interactions", lambda: probe_interactions(client))):
        print(f"  {name}…")
        try:
            report[name] = fn()
        except Exception as exc:                    # noqa: BLE001
            report[name] = {"verdict": "error",
                            "detail": f"{type(exc).__name__}: {exc}"}
        print(f"    {json.dumps(report[name])[:200]}")
    label = f"{connection.project} · {connection.model}"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "transport_access.json").write_text(
        json.dumps(report, indent=1), encoding="utf-8")
    markdown = render_markdown(report, label=label)
    (out / "transport_access.md").write_text(markdown, encoding="utf-8")
    print("\n" + markdown)
    print("→ PASTE docs/evals/transport_access.md back into the session.")
    return 0 if report["round_trip"].get("verdict") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
