#!/usr/bin/env python3
"""Readiness for one profile: is this .env complete, and does each
plane it names answer?

    python scripts/readiness.py --env local|e1|e2|e3
    python scripts/readiness.py --env e1 --env-file /path/to/e1.env
    python scripts/readiness.py --env e1 --json

Loads the profile's .env (``env/<env>.env`` beside the examples, or
``--env-file``, or ``SAHS_ENV_FILE``; shell exports win, as everywhere),
then runs the checks that already exist, each as a subprocess with that
same file in ``SAHS_ENV_FILE``, and prints ONE table:

    check      verdict                    reason
    env file   ok                         env/e1.env
    settings   missing setting OKTA_ISSUER  and 2 more: ...
    ddl        ok                         39 tables across 6 files
    spanner    unreachable                token: ...
    okta       skipped                    OKTA_ISSUER unset
    ...

Verdicts: ``ok`` · ``missing setting <NAME>`` (a variable the profile
needs is unset) · ``bad setting <NAME>`` (set, but not what the profile
allows: AUTH_LOCAL_LOGIN=1 in e3, EPAAS_ENV not matching --env) ·
``unreachable`` (the check ran and the host did not answer or refused
the credential) · ``failed`` (the check ran and found something wrong)
· ``skipped`` (nothing configured for it). Exit 0 when every required
check is ok; 1 when one is not. Required: settings and ddl always;
spanner, okta and gateway in e1, e2, e3; a check that ran and did not
pass, in any profile.

Nothing here reaches a host itself: the subprocess runner is a
parameter, so the tests hand in canned exit codes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

SILO = Path(__file__).resolve().parents[1]
REPO = SILO.parent
sys.path.insert(0, str(SILO))

from sahs.util.auth import load_dotenv                    # noqa: E402

PROFILES = ("local", "e1", "e2", "e3")
SCRIPTS = SILO / "scripts"
ADMIN_SCRIPTS = REPO / "apps" / "synapse_admin" / "scripts"

# what each profile must name (the settings row); a value rule is a
# (name, predicate, reason) checked only when the name is set
_SPANNER = ("SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID")
_OKTA = ("OKTA_ISSUER", "OKTA_CLIENT_ID", "OKTA_CLIENT_SECRET", "OKTA_REDIRECT_URI",
         "AUTH_GROUP_ROLE_MAP")
_GATEWAY = ("IDP_TOKEN_URL", "GATEWAY_BASE_URL", "APP_ID", "APP_SECRET")
REQUIRED: dict[str, tuple[str, ...]] = {
    "local": ("SAHS_STORE", "AUTH_PEPPER"),
    "e1": ("SAHS_STORE", "EPAAS_ENV", "AUTH_PEPPER", "MERIDIAN_BUILDS_SOURCE")
    + _SPANNER + _OKTA + _GATEWAY,
}
REQUIRED["e2"] = REQUIRED["e1"]
REQUIRED["e3"] = REQUIRED["e1"]

# the same runner shape for every check: argv, env → (code, out, err)
Runner = Callable[[list[str], dict[str, str]], tuple[int, str, str]]


def subprocess_runner(argv: list[str], env: dict[str, str],
                      timeout: float = 180.0) -> tuple[int, str, str]:
    try:
        done = subprocess.run(argv, env=env, capture_output=True, text=True,
                              timeout=timeout, cwd=str(SILO))
    except subprocess.TimeoutExpired:
        return 124, "", f"no answer in {timeout:.0f}s"
    except OSError as exc:
        return 127, "", str(exc)
    return done.returncode, done.stdout, done.stderr


def _flag(env: dict[str, str], name: str) -> bool:
    return (env.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _set(env: dict[str, str], name: str) -> bool:
    return bool((env.get(name) or "").strip())


def _named(text: str) -> list[str]:
    """The variable names a check's message points at ("set A, B in
    the silo .env"), in order, without duplicates."""
    out: list[str] = []
    for name in re.findall(r"\b([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\b", text):
        if name not in out:
            out.append(name)
    return out


def _one_line(text: str) -> str:
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    if not lines:
        return ""
    line = lines[-1] if lines[-1].startswith(("✗", "!")) else lines[0]
    for candidate in lines:
        if candidate.startswith("✗"):
            line = candidate
            break
    return line.lstrip("✗! ").strip()[:160]


# ── the checks ────────────────────────────────────────────────
def check_settings(profile: str, env: dict[str, str]) -> dict[str, str]:
    missing = [n for n in REQUIRED[profile] if not _set(env, n)]
    if missing:
        rest = f"; and {len(missing) - 1} more: {', '.join(missing[1:])}" \
            if len(missing) > 1 else ""
        return {"check": "settings", "verdict": f"missing setting {missing[0]}",
                "reason": f"the {profile} profile needs it{rest}"}
    store = (env.get("SAHS_STORE") or "").strip().lower()
    if profile == "local" and store not in ("sqlite", "local"):
        return {"check": "settings", "verdict": "bad setting SAHS_STORE",
                "reason": f"local is sqlite (or local), not {store!r}"}
    if profile != "local":
        if store != "spanner":
            return {"check": "settings", "verdict": "bad setting SAHS_STORE",
                    "reason": f"{profile} is spanner, not {store!r}"}
        epaas = (env.get("EPAAS_ENV") or "").strip().lower()
        if epaas != profile:
            return {"check": "settings", "verdict": "bad setting EPAAS_ENV",
                    "reason": f"--env {profile} but EPAAS_ENV={epaas!r}"}
        if (env.get("MERIDIAN_BUILDS_SOURCE") or "").strip().lower() != "spanner":
            return {"check": "settings", "verdict": "bad setting MERIDIAN_BUILDS_SOURCE",
                    "reason": f"{profile} serves the promoted build from Spanner"}
    if profile == "e3" and _set(env, "AUTH_LOCAL_LOGIN"):
        return {"check": "settings", "verdict": "bad setting AUTH_LOCAL_LOGIN",
                "reason": "production has one door, Okta: unset it"}
    if profile in ("e2", "e3") and _flag(env, "AUTH_LOCAL_LOGIN"):
        return {"check": "settings", "verdict": "bad setting AUTH_LOCAL_LOGIN",
                "reason": f"{profile} keeps the email-and-password door shut"}
    if len((env.get("AUTH_PEPPER") or "").strip()) < 8:
        return {"check": "settings", "verdict": "bad setting AUTH_PEPPER",
                "reason": "eight characters at least (32+ random ones deployed)"}
    if "<" in "".join(env.get(n, "") for n in REQUIRED[profile]):
        holders = [n for n in REQUIRED[profile] if "<" in env.get(n, "")]
        return {"check": "settings", "verdict": f"bad setting {holders[0]}",
                "reason": "still the example's placeholder"
                          + (f"; also {', '.join(holders[1:])}" if holders[1:] else "")}
    return {"check": "settings", "verdict": "ok",
            "reason": f"every variable the {profile} profile needs is set"}


def _by_exit(name: str, code: int, line: str, *,
             unreachable_on: tuple[int, ...] = (1,),
             config_on: tuple[int, ...] = (2, 3)) -> dict[str, str]:
    if code == 0:
        return {"check": name, "verdict": "ok", "reason": line}
    if code in config_on:
        names = _named(line)
        if names:
            return {"check": name, "verdict": f"missing setting {names[0]}",
                    "reason": line}
        return {"check": name, "verdict": "unreachable", "reason": line}
    if code in unreachable_on or code in (124, 127):
        return {"check": name, "verdict": "unreachable", "reason": line}
    return {"check": name, "verdict": "failed", "reason": line}


def check_ddl(env: dict[str, str], run: Runner) -> dict[str, str]:
    code, out, err = run([sys.executable, str(SCRIPTS / "spanner_ddl_check.py")], env)
    line = _one_line(out) or _one_line(err)
    if code == 0:
        return {"check": "ddl", "verdict": "ok", "reason": line.removeprefix("ok: ")}
    findings = [l for l in out.splitlines() if l.strip()]
    return {"check": "ddl", "verdict": "failed",
            "reason": findings[0][:160] if findings else line}


def check_spanner(profile: str, env: dict[str, str], run: Runner) -> dict[str, str]:
    if (env.get("SAHS_STORE") or "").strip().lower() != "spanner":
        return {"check": "spanner", "verdict": "skipped",
                "reason": f"SAHS_STORE={env.get('SAHS_STORE', '') or 'local'}: no Spanner here"}
    code, out, err = run([sys.executable, str(SCRIPTS / "spanner_check.py")], env)
    if code == 0:
        missing = re.search(r"missing\s+(\d+)(?::\s*(.*))?", out)
        if missing and int(missing.group(1)):
            return {"check": "spanner", "verdict": "failed",
                    "reason": f"tables not applied: {(missing.group(2) or '').strip()}"}
        tables = re.search(r"tables: \d+", out)
        return {"check": "spanner", "verdict": "ok",
                "reason": tables.group(0) if tables else _one_line(out)}
    line = _one_line(err) or _one_line(out)
    if code == 3:
        names = _named(line)
        if names and "set" in line:
            return {"check": "spanner", "verdict": f"missing setting {names[0]}",
                    "reason": line}
        return {"check": "spanner", "verdict": "unreachable", "reason": line}
    return _by_exit("spanner", code, line)


def check_okta(profile: str, env: dict[str, str], run: Runner) -> dict[str, str]:
    if not _set(env, "OKTA_ISSUER"):
        return {"check": "okta", "verdict": "skipped", "reason": "OKTA_ISSUER unset"}
    tag = profile.upper()
    # the preflight reads NAME_<ENV>; hand it the app's own names
    probe = dict(env)
    probe.setdefault(f"OKTA_DISCOVERY_URL_{tag}", env["OKTA_ISSUER"])
    for name in ("OKTA_CLIENT_ID", "OKTA_REDIRECT_URI", "OKTA_CLIENT_SECRET"):
        if _set(env, name):
            probe.setdefault(f"{name}_{tag}", env[name])
    code, out, err = run([sys.executable, str(ADMIN_SCRIPTS / "okta_check.py"),
                          "--envs", tag, "--no-matrix"], probe)
    line = _one_line(err) or _one_line(out)
    if code == 0:
        return {"check": "okta", "verdict": "ok",
                "reason": "discovery, jwks and the callback registration answered"}
    if code == 2:
        names = _named(line)
        return {"check": "okta", "verdict": f"missing setting {names[0]}" if names
                else "failed", "reason": line}
    failed = [l.strip() for l in out.splitlines() if "FAIL" in l]
    if any("UNREACHABLE" in l or "discovery" in l for l in failed):
        return {"check": "okta", "verdict": "unreachable",
                "reason": failed[0][:160] if failed else line}
    return {"check": "okta", "verdict": "failed",
            "reason": failed[0][:160] if failed else line}


def check_gateway(env: dict[str, str], run: Runner) -> dict[str, str]:
    if not (_set(env, "GATEWAY_BASE_URL") and _set(env, "IDP_TOKEN_URL")):
        return {"check": "gateway", "verdict": "skipped",
                "reason": "GATEWAY_BASE_URL / IDP_TOKEN_URL unset"}
    if not (_set(env, "APP_ID") and _set(env, "APP_SECRET")) and not _set(
            env, "GEMINI_BEARER_TOKEN"):
        return {"check": "gateway", "verdict": "missing setting APP_ID",
                "reason": "APP_ID and APP_SECRET (or GEMINI_BEARER_TOKEN) mint the token"}
    code, out, err = run([sys.executable, str(SCRIPTS / "gateway_check.py"),
                          "--only", "token,generate"], env)
    line = _one_line(err) or _one_line(out)
    if code == 0:
        return {"check": "gateway", "verdict": "ok", "reason": "a token and one model call"}
    if code == 3:
        return {"check": "gateway", "verdict": "unreachable", "reason": line or "no token"}
    return _by_exit("gateway", code, line, unreachable_on=(), config_on=(2,))


def check_vertex(env: dict[str, str], run: Runner) -> dict[str, str]:
    if not (_set(env, "SYNAPSE_VERTEX_SA_KEY") and _set(env, "VERTEX_PROJECT_ID")):
        return {"check": "vertex", "verdict": "skipped",
                "reason": "SYNAPSE_VERTEX_SA_KEY / VERTEX_PROJECT_ID unset"}
    code, out, err = run([sys.executable, str(SCRIPTS / "vertex_check.py")], env)
    line = _one_line(err) or _one_line(out)
    if code == 0:
        return {"check": "vertex", "verdict": "ok", "reason": "the key, the endpoint and a token"}
    return _by_exit("vertex", code, line)


def check_bigquery(env: dict[str, str], run: Runner) -> dict[str, str]:
    if (env.get("SAHS_BQ_AUTH_MODE") or "").strip().lower() == "user":
        return {"check": "bigquery", "verdict": "skipped",
                "reason": "SAHS_BQ_AUTH_MODE=user: each person's own Google account"}
    if not _set(env, "SYNAPSE_BQ_SA_KEY"):
        return {"check": "bigquery", "verdict": "skipped", "reason": "SYNAPSE_BQ_SA_KEY unset"}
    code, out, err = run([sys.executable, str(SCRIPTS / "bq_check.py")], env)
    line = _one_line(err) or _one_line(out)
    if code == 0:
        return {"check": "bigquery", "verdict": "ok", "reason": "a token and one dry run"}
    return _by_exit("bigquery", code, line)


def check_google(profile: str, env: dict[str, str], run: Runner) -> dict[str, str]:
    if not _set(env, "GOOGLE_OAUTH_CLIENT_ID"):
        return {"check": "google", "verdict": "skipped", "reason": "GOOGLE_OAUTH_CLIENT_ID unset"}
    tag = profile.upper()
    probe = dict(env)
    probe.setdefault(f"GOOGLE_CLIENT_ID_{tag}", env["GOOGLE_OAUTH_CLIENT_ID"])
    if _set(env, "GOOGLE_OAUTH_REDIRECT_URI"):
        probe.setdefault(f"GOOGLE_REDIRECT_URI_{tag}", env["GOOGLE_OAUTH_REDIRECT_URI"])
    if _set(env, "GOOGLE_OAUTH_CLIENT_SECRET"):
        probe.setdefault(f"GOOGLE_CLIENT_SECRET_{tag}", env["GOOGLE_OAUTH_CLIENT_SECRET"])
    code, out, err = run([sys.executable, str(ADMIN_SCRIPTS / "google_auth_check.py"),
                          "--envs", tag, "--no-matrix"], probe)
    line = _one_line(err) or _one_line(out)
    if code == 0:
        return {"check": "google", "verdict": "ok",
                "reason": "discovery, jwks and the callback registration answered"}
    if code == 2:
        names = _named(line)
        return {"check": "google", "verdict": f"missing setting {names[0]}" if names
                else "failed", "reason": line}
    failed = [l.strip() for l in out.splitlines() if "FAIL" in l]
    return {"check": "google", "verdict": "failed",
            "reason": failed[0][:160] if failed else line}


# ── the table ─────────────────────────────────────────────────
REQUIRED_CHECKS = {"local": ("settings", "ddl"),
                   "e1": ("settings", "ddl", "spanner", "okta", "gateway")}
REQUIRED_CHECKS["e2"] = REQUIRED_CHECKS["e1"]
REQUIRED_CHECKS["e3"] = REQUIRED_CHECKS["e1"]


def resolve_env_file(profile: str, given: str = "") -> Path | None:
    for candidate in (given, os.environ.get("SAHS_ENV_FILE", ""),
                      str(SILO / "env" / f"{profile}.env")):
        if candidate and Path(candidate).is_file():
            return Path(candidate).resolve()
    return None


def readiness(profile: str, env: dict[str, str], run: Runner,
              env_file: Path | None) -> list[dict[str, str]]:
    """Every row of the table, in order, for one profile and one
    resolved environment. ``env`` is what the checks see."""
    rows: list[dict[str, str]] = []
    if env_file is None:
        rows.append({"check": "env file", "verdict": "missing setting SAHS_ENV_FILE",
                     "reason": f"no env/{profile}.env: copy env/{profile}.env.example "
                               "to it, or pass --env-file"})
    else:
        rows.append({"check": "env file", "verdict": "ok", "reason": str(env_file)})
    rows.append(check_settings(profile, env))
    rows.append(check_ddl(env, run))
    rows.append(check_spanner(profile, env, run))
    rows.append(check_okta(profile, env, run))
    rows.append(check_gateway(env, run))
    rows.append(check_vertex(env, run))
    rows.append(check_bigquery(env, run))
    rows.append(check_google(profile, env, run))
    return rows


def verdict(profile: str, rows: list[dict[str, str]]) -> bool:
    """True when every required check is ok and nothing that ran failed."""
    required = set(REQUIRED_CHECKS[profile]) | {"env file"}
    for row in rows:
        if row["verdict"] == "ok":
            continue
        if row["verdict"] == "skipped" and row["check"] not in required:
            continue
        return False
    return True


def format_table(profile: str, rows: list[dict[str, str]], ready: bool) -> str:
    width_check = max(len(r["check"]) for r in rows)
    width_verdict = max(len(r["verdict"]) for r in rows)
    lines = [f"readiness · {profile}",
             f"{'check':<{width_check}}  {'verdict':<{width_verdict}}  reason"]
    for r in rows:
        lines.append(f"{r['check']:<{width_check}}  {r['verdict']:<{width_verdict}}  "
                     f"{r['reason']}")
    lines.append("ready" if ready else "not ready: fix the rows above, then run again")
    return "\n".join(lines)


def main(argv: list[str] | None = None, run: Runner = subprocess_runner) -> int:
    parser = argparse.ArgumentParser(prog="readiness.py")
    parser.add_argument("--env", required=True, choices=PROFILES,
                        help="the profile: local, e1, e2 or e3")
    parser.add_argument("--env-file", default="",
                        help="the .env to load (default: env/<env>.env, or SAHS_ENV_FILE)")
    parser.add_argument("--json", action="store_true", dest="json_out")
    args = parser.parse_args(argv)

    env_file = resolve_env_file(args.env, args.env_file)
    if env_file is not None:
        load_dotenv(env_file)
    env = dict(os.environ)
    if env_file is not None:
        # every check reads the same file, whatever its own default is
        env["SAHS_ENV_FILE"] = str(env_file)
    rows = readiness(args.env, env, run, env_file)
    ready = verdict(args.env, rows)
    if args.json_out:
        print(json.dumps({"profile": args.env, "ready": ready, "checks": rows}, indent=1))
    else:
        print(format_table(args.env, rows, ready))
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
