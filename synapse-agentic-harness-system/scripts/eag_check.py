#!/usr/bin/env python3
"""EAG check — can Gemini 2.5 Pro through EAG, behind a OneIdentity
token, do what the harness asks of Vertex today? Run it on the laptop
BEFORE any of it enters the program.

    python scripts/eag_check.py                  # token, generate, stream,
                                                 # tools, the thoughts flag
                                                 # both ways, system instruction
    python scripts/eag_check.py --probe-ttl 7    # then watch the token die
                                                 # (up to 7 minutes)
    python scripts/eag_check.py --only token,generate
    python scripts/eag_check.py --json eag_report.json

Reads the silo .env (never overriding the shell): APP_ID, APP_SECRET,
AUTH_MODE (generated | env), GEMINI_BEARER_TOKEN, EAG_MODEL (not
GEMINI_MODEL: the Vertex plane reads that one too), AUTH_VERSION, THINKING_BUDGET, SHOW_THOUGHTS, GEMINI_PROMPT,
ONEID_TOKEN_URL, EAG_BASE_URL, EAG_SCOPES, ONEID_TIMESTAMP_UNIT (ms |
s; both are tried), EAG_ROUTE (auto | direct | proxy), EAG_CA_BUNDLE.
Secrets never print: the report shows lengths and hashes only.
Exit 0 = the model answered · 3 = no token · 1 = generate refused.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs.util.auth import load_dotenv                    # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH               # noqa: E402
from sahs.util.eag import (Config, RouteChooser,  # noqa: E402
                           candidate_routes, env_warnings, render_report,
                           run_checks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="eag_check.py")
    parser.add_argument("--probe-ttl", type=float, default=0.0,
                        metavar="MINUTES",
                        help="after the checks, probe the token every 20 s "
                             "until it dies or this many minutes pass")
    parser.add_argument("--only", default="",
                        help="comma list of token,generate,stream,tools,"
                             "thinking,system,probe")
    parser.add_argument("--json", default="", metavar="FILE",
                        help="also write the full report (secrets redacted)")
    args = parser.parse_args(argv)

    load_dotenv()
    env = dict(os.environ)
    cfg = Config.from_env(env)
    only = {s.strip() for s in args.only.split(",") if s.strip()} or None
    if args.probe_ttl > 0 and only is not None:
        only.add("probe")

    # the route is decided by the first real request (the token POST):
    # direct first, then the corporate proxy — no GET to a POST endpoint
    chooser = RouteChooser(candidate_routes(env))
    for line in env_warnings(env):
        print(f"! {line}")
    mode = (env.get("GEMINI_MODE") or "").strip()
    if mode:
        print(f"note: GEMINI_MODE={mode} in the environment; this check "
              "runs both request modes regardless")
    print("running the checks (a few model calls; the probe, if asked, "
          "waits up to the minutes you gave)…", flush=True)
    report = run_checks(cfg, chooser.http, chooser.stream,
                        probe_minutes=args.probe_ttl, only=only)
    report["route"] = chooser.label
    report["warnings"] = env_warnings(env)
    if chooser.failures:
        report["route_failures"] = chooser.failures
    print()
    print(render_report(report))
    if chooser.chosen is None:
        print("  (on the corporate network try EAG_ROUTE=proxy, or name the "
              "root cert with EAG_CA_BUNDLE; pip install truststore is the "
              "clean fix for TLS interception)", file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=1, default=str)
                                   + "\n", encoding="utf-8")
        print(f"\nfull report written to {args.json} (paste it back)")
    by_name = {c["name"]: c for c in report["checks"]}
    if not by_name.get("token", {}).get("ok"):
        return EXIT_ENV_AUTH
    if "generate" in by_name and not by_name["generate"]["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
