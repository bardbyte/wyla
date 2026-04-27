#!/usr/bin/env python3
"""Preflight: hit the LUMI MDM API and digest the response shape.

Two jobs:
  1. Verify connectivity + auth to the MDM endpoint for a given table.
  2. Pretty-print a *structural* summary of the JSON — top-level keys,
     array lengths, per-column key set, sample values — so we can design
     the agent's MDM tool against real data without dumping the whole
     payload.

Standalone usage (zero pip-install — stdlib urllib only):

    # full URL form (easiest)
    python scripts/check_mdm_access.py \\
        'https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas?tableName=custins_customer_insights_cardmember'

    # base + --table form
    python scripts/check_mdm_access.py \\
        --base 'https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas' \\
        --table custins_customer_insights_cardmember

    # with auth (when Amex requires it)
    export MDM_TOKEN='Bearer eyJ...'   # full header value, or just the token
    python scripts/check_mdm_access.py URL --auth-env MDM_TOKEN

Saves the raw JSON to tests/fixtures/sample_mdm_response.json so we have
ground truth to design tools/tests against. Pass --no-save to skip.

Tool-ready: query_mdm_schema(base_url, table_name, ...) returns the
{status, ..., error} dict shape we'll lift into lumi/tools/mdm_tools.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_TIMEOUT_SECS = 30
DEFAULT_FIXTURE_PATH = Path("tests/fixtures/sample_mdm_response.json")


# --------------------------------------------------------------------------- #
# Tool-shaped core — we'll lift this into lumi/tools/mdm_tools.py later.      #
# --------------------------------------------------------------------------- #

def query_mdm_schema(
    base_url: str,
    table_name: str | None = None,
    auth_env: str | None = None,
    timeout_secs: int = DEFAULT_TIMEOUT_SECS,
) -> dict[str, Any]:
    """Call the MDM schema endpoint for one table.

    Args:
        base_url: Either the full URL with `?tableName=...` already embedded,
            or the schemas endpoint base. If `?tableName=` is present in the
            URL, it overrides `table_name`.
        table_name: Required if `base_url` doesn't include `?tableName=...`.
        auth_env: Name of env var holding either the raw token or a full
            `Authorization` header value. None = no auth.
        timeout_secs: HTTP timeout per request.

    Returns:
        {
          "status": "success" | "error",
          "url": the URL we actually hit,
          "table_name": resolved table name or None,
          "http_status": int or None,
          "content_type": str or None,
          "size_bytes": int,
          "latency_ms": int,
          "json": parsed body or None,
          "raw_text": str (raw body if non-JSON),
          "error": str or None,
        }
    """
    url, resolved_table = _resolve_url(base_url, table_name)
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "lumi-preflight/0.1",
    }
    if auth_env:
        token = os.environ.get(auth_env, "").strip()
        if token:
            # Accept either a raw token or a full "Bearer xxx" / "Basic xxx" value.
            if token.lower().startswith(("bearer ", "basic ", "token ")):
                headers["Authorization"] = token
            else:
                headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers, method="GET")
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            raw = resp.read()
            latency_ms = int((time.perf_counter() - started) * 1000)
            content_type = resp.headers.get("Content-Type", "")
            text = raw.decode("utf-8", errors="replace")
            parsed: Any = None
            parse_err: str | None = None
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as e:
                parse_err = f"non-JSON response: {e}"
            return {
                "status": "success" if parsed is not None else "error",
                "url": url,
                "table_name": resolved_table,
                "http_status": resp.status,
                "content_type": content_type,
                "size_bytes": len(raw),
                "latency_ms": latency_ms,
                "json": parsed,
                "raw_text": text if parsed is None else "",
                "error": parse_err,
            }
    except urllib.error.HTTPError as e:
        latency_ms = int((time.perf_counter() - started) * 1000)
        body = b""
        try:
            body = e.read()
        except Exception:
            pass
        return {
            "status": "error",
            "url": url,
            "table_name": resolved_table,
            "http_status": e.code,
            "content_type": e.headers.get("Content-Type", "") if e.headers else "",
            "size_bytes": len(body),
            "latency_ms": latency_ms,
            "json": None,
            "raw_text": body.decode("utf-8", errors="replace"),
            "error": f"HTTP {e.code}: {e.reason}",
        }
    except urllib.error.URLError as e:
        return _err(url, resolved_table, f"connection failed: {e.reason}")
    except (TimeoutError, OSError) as e:
        return _err(url, resolved_table, f"{type(e).__name__}: {e}")


def _resolve_url(base_url: str, table_name: str | None) -> tuple[str, str | None]:
    parsed = urllib.parse.urlparse(base_url)
    qs = dict(urllib.parse.parse_qsl(parsed.query))
    if "tableName" in qs:
        return base_url, qs["tableName"]
    if not table_name:
        raise ValueError(
            "table_name is required when base_url has no ?tableName=... query string"
        )
    new_query = urllib.parse.urlencode({**qs, "tableName": table_name})
    rebuilt = parsed._replace(query=new_query).geturl()
    return rebuilt, table_name


def _err(url: str, table_name: str | None, error: str) -> dict[str, Any]:
    return {
        "status": "error",
        "url": url,
        "table_name": table_name,
        "http_status": None,
        "content_type": None,
        "size_bytes": 0,
        "latency_ms": 0,
        "json": None,
        "raw_text": "",
        "error": error,
    }


# --------------------------------------------------------------------------- #
# Structural digest — what makes this script useful for *understanding* the   #
# API, not just verifying connectivity.                                       #
# --------------------------------------------------------------------------- #

def digest_response(payload: Any, sample_size: int = 3) -> dict[str, Any]:
    """Walk the response and surface its shape: top-level keys, list lengths,
    representative element keys, distinct key sets across collections.
    """
    if not isinstance(payload, dict | list):
        return {
            "kind": "scalar",
            "type": _typename(payload),
            "value_preview": _preview(payload),
        }

    if isinstance(payload, list):
        return {
            "kind": "array",
            "length": len(payload),
            "first_n_samples": [
                digest_response(e, sample_size) for e in payload[:sample_size]
            ],
            "distinct_keys_across_elements": _distinct_keys(payload),
        }

    digest: dict[str, Any] = {"kind": "object", "keys": {}}
    for k, v in payload.items():
        if isinstance(v, list):
            digest["keys"][k] = {
                "type": "array",
                "length": len(v),
                "element_kind": _classify_list(v),
                "distinct_keys": _distinct_keys(v) if v and isinstance(v[0], dict) else None,
                "sample": (
                    digest_response(v[0], sample_size) if v else None
                ),
            }
        elif isinstance(v, dict):
            digest["keys"][k] = {
                "type": "object",
                "subkeys": list(v.keys()),
                "subkey_count": len(v),
                "sample": _preview(v),
            }
        else:
            digest["keys"][k] = {
                "type": _typename(v),
                "value_preview": _preview(v),
            }
    return digest


def _typename(v: Any) -> str:
    if v is None:
        return "null"
    return type(v).__name__


def _preview(v: Any, max_chars: int = 80) -> Any:
    if v is None or isinstance(v, bool | int | float):
        return v
    if isinstance(v, str):
        return v if len(v) <= max_chars else v[: max_chars - 1] + "…"
    if isinstance(v, list | dict):
        s = json.dumps(v, default=str)
        return s if len(s) <= max_chars else s[: max_chars - 1] + "…"
    return str(v)[:max_chars]


def _distinct_keys(items: list[Any]) -> list[str] | None:
    if not items or not all(isinstance(e, dict) for e in items):
        return None
    seen: set[str] = set()
    for e in items:
        seen.update(e.keys())
    return sorted(seen)


def _classify_list(items: list[Any]) -> str:
    if not items:
        return "empty"
    types = {type(e).__name__ for e in items}
    if len(types) == 1:
        return f"homogeneous-{types.pop()}"
    return f"heterogeneous({','.join(sorted(types))})"


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def _format_summary(result: dict[str, Any], digest: dict[str, Any] | None) -> str:
    lines: list[str] = []
    lines.append("Request:")
    lines.append(f"  URL:           {result['url']}")
    lines.append(f"  Table:         {result.get('table_name') or '(in URL)'}")
    lines.append("")
    lines.append("Response:")
    if result.get("http_status") is not None:
        lines.append(f"  HTTP status:   {result['http_status']}")
    lines.append(f"  Content-Type:  {result.get('content_type') or '(none)'}")
    lines.append(f"  Size:          {_human_bytes(result.get('size_bytes', 0))}")
    lines.append(f"  Latency:       {result.get('latency_ms', 0)} ms")
    lines.append("")

    if result["status"] != "success":
        lines.append(f"FAILED: {result['error']}")
        if result.get("raw_text"):
            preview = result["raw_text"][:500]
            lines.append("")
            lines.append(f"Body preview ({len(result['raw_text'])} chars total):")
            lines.append(_indent(preview, "  "))
        return "\n".join(lines)

    lines.append("JSON structure:")
    lines.append(_render_digest(digest, indent="  ") if digest else "  (none)")
    return "\n".join(lines)


def _render_digest(d: dict[str, Any] | None, indent: str = "  ") -> str:
    if d is None:
        return f"{indent}(empty)"
    out: list[str] = []
    kind = d.get("kind")

    if kind == "scalar":
        out.append(f"{indent}{d['type']}: {d['value_preview']}")
        return "\n".join(out)

    if kind == "array":
        out.append(f"{indent}array, length={d['length']}")
        if d.get("distinct_keys_across_elements"):
            keys = d["distinct_keys_across_elements"]
            out.append(f"{indent}  element keys ({len(keys)}): {', '.join(keys)}")
        for i, sample in enumerate(d.get("first_n_samples") or []):
            out.append(f"{indent}  [{i}]:")
            out.append(_render_digest(sample, indent + "    "))
        return "\n".join(out)

    # object
    keys: dict[str, Any] = d.get("keys", {})
    out.append(f"{indent}object with {len(keys)} keys:")
    for k, info in keys.items():
        t = info.get("type", "?")
        if t == "array":
            line = (
                f"{indent}  {k}: array(len={info['length']}, "
                f"{info.get('element_kind', '?')})"
            )
            out.append(line)
            if info.get("distinct_keys"):
                kd = info["distinct_keys"]
                out.append(
                    f"{indent}    element keys ({len(kd)}): "
                    f"{', '.join(kd[:20])}{' …' if len(kd) > 20 else ''}"
                )
            if info.get("sample"):
                out.append(f"{indent}    sample[0]:")
                out.append(_render_digest(info["sample"], indent + "      "))
        elif t == "object":
            sub = info.get("subkeys") or []
            out.append(
                f"{indent}  {k}: object("
                f"{info.get('subkey_count', len(sub))} keys: {', '.join(sub[:10])}"
                f"{' …' if len(sub) > 10 else ''})"
            )
        else:
            out.append(f"{indent}  {k}: {t} = {info.get('value_preview')}")
    return "\n".join(out)


def _human_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def _indent(s: str, prefix: str) -> str:
    return "\n".join(prefix + ln for ln in s.splitlines())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_mdm_access",
        description="Verify MDM API connectivity and digest the response shape.",
    )
    parser.add_argument(
        "url",
        nargs="?",
        help="Full URL (with ?tableName=... already embedded), or omit and use --base/--table.",
    )
    parser.add_argument("--base", help="Base URL (without query string).")
    parser.add_argument("--table", help="Table name (used with --base).")
    parser.add_argument(
        "--auth-env",
        default=None,
        help="Env var holding token or full Authorization header. Default: no auth.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECS,
        help=f"HTTP timeout (sec). Default: {DEFAULT_TIMEOUT_SECS}",
    )
    parser.add_argument(
        "--save",
        default=str(DEFAULT_FIXTURE_PATH),
        help="Where to save the raw response JSON. Default: tests/fixtures/sample_mdm_response.json",
    )
    parser.add_argument("--no-save", action="store_true", help="Don't save raw response.")
    parser.add_argument("--json", action="store_true", help="Print full result + digest as JSON.")
    args = parser.parse_args(argv)

    base_url = args.url or args.base
    if not base_url:
        print("ERROR: provide a URL positionally, or use --base + --table.", file=sys.stderr)
        return 2

    try:
        result = query_mdm_schema(
            base_url=base_url,
            table_name=args.table,
            auth_env=args.auth_env,
            timeout_secs=args.timeout,
        )
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    digest: dict[str, Any] | None = None
    if result["status"] == "success" and result["json"] is not None:
        digest = digest_response(result["json"])

    if args.json:
        print(json.dumps({"result": result, "digest": digest}, indent=2, default=str))
    else:
        print(_format_summary(result, digest))

    if not args.no_save and result["status"] == "success" and result["json"] is not None:
        target = Path(args.save)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result["json"], indent=2, default=str), encoding="utf-8")
        print()
        print(f"Saved raw response → {target} ({_human_bytes(target.stat().st_size)})")

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
