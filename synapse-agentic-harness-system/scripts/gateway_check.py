#!/usr/bin/env python3
"""Gateway check — can a Gemini model through the gateway, behind an
identity-service token, do what the harness asks of Vertex today? Run
it on the laptop BEFORE any of it enters the program.

    python scripts/gateway_check.py                  # token, generate, stream,
                                                 # tools, the thoughts flag
                                                 # both ways, system instruction
    python scripts/gateway_check.py --all-models     # every model GATEWAY_MODELS names
    python scripts/gateway_check.py --all-models --levels
                                                 # …and which thinkingLevels each
                                                 # accepts (one tiny call per level);
                                                 # prints the GATEWAY_MODEL_LEVELS
                                                 # line to paste when the engine
                                                 # map in sahs.util.profiles is wrong
    python scripts/gateway_check.py --probe-ttl 7    # then watch the token die
                                                 # (up to 7 minutes)
    python scripts/gateway_check.py --only token,generate
    python scripts/gateway_check.py --json gateway_report.json

Reads the silo .env (never overriding the shell): APP_ID, APP_SECRET,
AUTH_MODE (generated | env), GEMINI_BEARER_TOKEN, GATEWAY_MODEL (not
GEMINI_MODEL: the Vertex plane reads that one too), AUTH_VERSION, THINKING_BUDGET, SHOW_THOUGHTS, GEMINI_PROMPT,
IDP_TOKEN_URL, GATEWAY_BASE_URL, GATEWAY_SCOPES, IDP_TIMESTAMP_UNIT (ms |
s; both are tried), GATEWAY_ROUTE (auto | direct | proxy), GATEWAY_CA_BUNDLE.
Secrets never print: the report shows lengths and hashes only.
Exit 0 = the model answered · 3 = no token · 1 = generate refused.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs.util.auth import load_dotenv                    # noqa: E402
from sahs.util.console import EXIT_ENV_AUTH               # noqa: E402
from sahs.util.gateway import (DEFAULT_CHECKS, Config,  # noqa: E402
                           RouteChooser, candidate_routes, env_warnings,
                           render_report, run_checks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gateway_check.py")
    parser.add_argument("--probe-ttl", type=float, default=0.0,
                        metavar="MINUTES",
                        help="after the checks, probe the token every 20 s "
                             "until it dies or this many minutes pass")
    parser.add_argument("--only", default="",
                        help="comma list of token,generate,stream,tools,"
                             "thinking,system,cache,probe")
    parser.add_argument("--model", default="",
                        help="check this model instead of GATEWAY_MODEL (one of GATEWAY_MODELS)")
    parser.add_argument("--all-models", action="store_true",
                        help="check every model GATEWAY_MODELS names, one report each")
    parser.add_argument("--levels", action="store_true",
                        help="also ask which thinkingLevels the model accepts "
                             "(minimal, low, medium, high, max: one tiny call each)")
    parser.add_argument("--json", default="", metavar="FILE",
                        help="also write the full report (secrets redacted)")
    args = parser.parse_args(argv)

    load_dotenv()
    env = dict(os.environ)
    cfg = Config.from_env(env)
    only = {s.strip() for s in args.only.split(",") if s.strip()} or None
    if args.probe_ttl > 0 and only is not None:
        only.add("probe")
    if args.levels:
        only = (only if only is not None else set(DEFAULT_CHECKS)) | {"levels"}

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
    # one model, or every model the gateway serves: the token is the
    # same, the path, the scopes and the thinking dialect differ per model
    if args.all_models:
        models = list(cfg.models)
    elif args.model.strip():
        wanted = args.model.strip()
        if wanted not in cfg.models:
            print(f"no gateway model called {wanted!r}: GATEWAY_MODELS names "
                  + ", ".join(cfg.models), file=sys.stderr)
            return 2
        models = [wanted]
    else:
        models = [cfg.model]
    reports = []
    worst = 0
    for model in models:
        model_cfg = replace(cfg, model=model)
        if len(models) > 1:
            print(f"\n── {model} ──", flush=True)
        report = run_checks(model_cfg, chooser.http, chooser.stream,
                            probe_minutes=args.probe_ttl, only=only)
        report["model"] = model
        report["route"] = chooser.label
        report["warnings"] = env_warnings(env)
        if chooser.failures:
            report["route_failures"] = chooser.failures
        print()
        print(render_report(report))
        reports.append(report)
        by_name = {c["name"]: c for c in report["checks"]}
        if not by_name.get("token", {}).get("ok"):
            worst = max(worst, EXIT_ENV_AUTH)
        elif "generate" in by_name and not by_name["generate"]["ok"]:
            worst = max(worst, 1)
    if chooser.chosen is None:
        print("  (on the corporate network try GATEWAY_ROUTE=proxy, or name the "
              "root cert with GATEWAY_CA_BUNDLE; pip install truststore is the "
              "clean fix for TLS interception)", file=sys.stderr)
    if args.json:
        payload = reports[0] if len(reports) == 1 else {"models": reports}
        Path(args.json).write_text(json.dumps(payload, indent=1, default=str)
                                   + "\n", encoding="utf-8")
        print(f"\nfull report written to {args.json}")
    return worst


if __name__ == "__main__":
    raise SystemExit(main())
