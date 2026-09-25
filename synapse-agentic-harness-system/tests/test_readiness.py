"""scripts/readiness.py: one table per profile, from the checks that
exist, with the subprocess runner replaced by canned exit codes —
nothing here reaches a host."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "readiness", SILO / "scripts" / "readiness.py")
readiness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(readiness)

E1 = {
    "SAHS_STORE": "spanner", "EPAAS_ENV": "e1", "AUTH_PEPPER": "p" * 32,
    "MERIDIAN_BUILDS_SOURCE": "spanner",
    "SPANNER_PROJECT_ID": "proj", "SPANNER_INSTANCE_ID": "inst", "SPANNER_DATABASE_ID": "db",
    "SYNAPSE_SPANNER_SA_KEY": "/keys/spanner.json",
    "OKTA_ISSUER": "https://issuer.example/oauth2/x", "OKTA_CLIENT_ID": "cid",
    "OKTA_CLIENT_SECRET": "sec", "OKTA_REDIRECT_URI": "https://e1.example/callback",
    "AUTH_GROUP_ROLE_MAP": "Admins=admin,*=analyst",
    "IDP_TOKEN_URL": "https://idp.example/token", "GATEWAY_BASE_URL": "https://gw.example/v1",
    "APP_ID": "app", "APP_SECRET": "c2VjcmV0",
}


class Runner:
    """Canned exit codes by script name; records what was asked."""

    def __init__(self, **outcomes):
        self.outcomes = outcomes
        self.calls: list[tuple[str, list[str], dict[str, str]]] = []

    def __call__(self, argv, env):
        name = Path(argv[1]).stem
        self.calls.append((name, argv[2:], env))
        code, out, err = self.outcomes.get(name, (0, "ok", ""))
        return code, out, err


def _rows(profile, env, runner, env_file=Path("/tmp/x.env")):
    return {r["check"]: r for r in readiness.readiness(profile, env, runner, env_file)}


def test_e1_ready_when_every_check_answers():
    runner = Runner(spanner_ddl_check=(0, "ok: 39 tables across 6 files; keys agree", ""),
                    spanner_check=(0, "tables: 39\nagainst db/spanner (the design):\n"
                                      "  present     39\n  missing     0\n", ""),
                    okta_check=(0, "PASS discovery", ""),
                    gateway_check=(0, "token ok", ""))
    rows = _rows("e1", E1, runner)
    assert rows["settings"]["verdict"] == "ok"
    assert rows["ddl"] == {"check": "ddl", "verdict": "ok",
                           "reason": "39 tables across 6 files; keys agree"}
    assert rows["spanner"]["verdict"] == "ok" and rows["spanner"]["reason"] == "tables: 39"
    assert rows["okta"]["verdict"] == "ok" and rows["gateway"]["verdict"] == "ok"
    assert rows["vertex"]["verdict"] == "skipped" and rows["bigquery"]["verdict"] == "skipped"
    assert rows["google"]["verdict"] == "skipped"
    assert readiness.verdict("e1", list(rows.values())) is True
    # the okta preflight was handed the app's names under its own
    okta = next(c for c in runner.calls if c[0] == "okta_check")
    assert okta[1] == ["--envs", "E1", "--no-matrix"]
    assert okta[2]["OKTA_DISCOVERY_URL_E1"] == E1["OKTA_ISSUER"]
    assert okta[2]["OKTA_CLIENT_ID_E1"] == "cid" and okta[2]["OKTA_REDIRECT_URI_E1"].endswith("/callback")
    gateway = next(c for c in runner.calls if c[0] == "gateway_check")
    assert gateway[1] == ["--only", "token,generate"]
    # nothing else was asked of the network
    assert {c[0] for c in runner.calls} == {"spanner_ddl_check", "spanner_check",
                                            "okta_check", "gateway_check"}


def test_missing_and_bad_settings_are_named():
    env = dict(E1)
    del env["OKTA_ISSUER"], env["APP_SECRET"]
    row = readiness.check_settings("e1", env)
    assert row["verdict"] == "missing setting OKTA_ISSUER" and "APP_SECRET" in row["reason"]
    assert readiness.check_settings("e2", {**E1, "EPAAS_ENV": "e2", "AUTH_LOCAL_LOGIN": "1"})[
        "verdict"] == "bad setting AUTH_LOCAL_LOGIN"
    assert readiness.check_settings("e3", {**E1, "EPAAS_ENV": "e3", "AUTH_LOCAL_LOGIN": "0"})[
        "verdict"] == "bad setting AUTH_LOCAL_LOGIN"            # unset, not 0, in prod
    assert readiness.check_settings("e3", {**E1, "EPAAS_ENV": "e3"})["verdict"] == "ok"
    assert readiness.check_settings("e1", {**E1, "EPAAS_ENV": "e2"})["verdict"] == "bad setting EPAAS_ENV"
    assert readiness.check_settings("e1", {**E1, "SAHS_STORE": "sqlite"})["verdict"] == "bad setting SAHS_STORE"
    assert readiness.check_settings("e1", {**E1, "AUTH_PEPPER": "short"})["verdict"] == "bad setting AUTH_PEPPER"
    assert readiness.check_settings("e1", {**E1, "OKTA_CLIENT_ID": "<okta-client-id-e1>"})[
        "verdict"] == "bad setting OKTA_CLIENT_ID"              # the example's placeholder
    assert readiness.check_settings("e1", {**E1, "MERIDIAN_BUILDS_SOURCE": "local"})[
        "verdict"] == "bad setting MERIDIAN_BUILDS_SOURCE"
    local = readiness.check_settings("local", {"SAHS_STORE": "sqlite", "AUTH_PEPPER": "laptop-pepper"})
    assert local["verdict"] == "ok"
    assert readiness.check_settings("local", {"SAHS_STORE": "spanner", "AUTH_PEPPER": "laptop-pepper"})[
        "verdict"] == "bad setting SAHS_STORE"


def test_exit_codes_become_verdicts_and_required_checks_decide():
    runner = Runner(
        spanner_ddl_check=(1, "003_graph.sql: CREATE TABLE X: no PRIMARY KEY\n\n1 finding(s)", ""),
        spanner_check=(3, "", "✗ no Spanner SA key configured: set SYNAPSE_SPANNER_SA_KEY "
                              "to the key-file path in the silo .env"),
        okta_check=(1, "FAIL  E1  discovery  UNREACHABLE: tunnel failed", ""),
        gateway_check=(3, "", "✗ no token: HTTP 407"))
    rows = _rows("e1", E1, runner)
    assert rows["ddl"]["verdict"] == "failed" and "no PRIMARY KEY" in rows["ddl"]["reason"]
    assert rows["spanner"]["verdict"] == "missing setting SYNAPSE_SPANNER_SA_KEY"
    assert rows["okta"]["verdict"] == "unreachable" and "tunnel" in rows["okta"]["reason"]
    assert rows["gateway"]["verdict"] == "unreachable"
    assert readiness.verdict("e1", list(rows.values())) is False

    runner = Runner(spanner_check=(1, "", "✗ SpannerError 403: permission denied"),
                    okta_check=(1, "FAIL  E1  callback  NOT_REGISTERED", ""),
                    gateway_check=(1, "", "✗ generate refused"))
    rows = _rows("e1", E1, runner)
    assert rows["spanner"]["verdict"] == "unreachable"
    assert rows["okta"]["verdict"] == "failed" and "NOT_REGISTERED" in rows["okta"]["reason"]
    assert rows["gateway"]["verdict"] == "failed"

    # a live database missing designed tables is not ready
    runner = Runner(spanner_check=(0, "tables: 30\nagainst db/spanner (the design):\n"
                                      "  present     30\n  missing     9: BuildBundles, ChatEvents\n", ""))
    rows = _rows("e1", E1, runner)
    assert rows["spanner"]["verdict"] == "failed" and "BuildBundles" in rows["spanner"]["reason"]


def test_local_needs_only_settings_and_ddl_and_skips_the_rest():
    runner = Runner()
    env = {"SAHS_STORE": "sqlite", "AUTH_LOCAL_LOGIN": "1", "AUTH_PEPPER": "laptop-pepper"}
    rows = _rows("local", env, runner)
    assert rows["settings"]["verdict"] == "ok" and rows["ddl"]["verdict"] == "ok"
    for name in ("spanner", "okta", "gateway", "vertex", "bigquery", "google"):
        assert rows[name]["verdict"] == "skipped", name
    assert readiness.verdict("local", list(rows.values())) is True
    assert [c[0] for c in runner.calls] == ["spanner_ddl_check"]
    # a configured plane that fails is a failure even on a laptop
    runner = Runner(vertex_check=(3, "", "✗ Vertex SA key not found on disk: /k.json"))
    rows = _rows("local", {**env, "SYNAPSE_VERTEX_SA_KEY": "/k.json", "VERTEX_PROJECT_ID": "p"},
                 runner)
    assert rows["vertex"]["verdict"] == "unreachable"
    assert readiness.verdict("local", list(rows.values())) is False
    # e1 with a skipped required check is not ready
    rows = _rows("e1", {**E1, "GATEWAY_BASE_URL": ""}, Runner())
    assert rows["settings"]["verdict"] == "missing setting GATEWAY_BASE_URL"
    assert rows["gateway"]["verdict"] == "skipped"
    assert readiness.verdict("e1", list(rows.values())) is False


def test_main_loads_the_profile_file_prints_one_table_and_exits_by_the_verdict(
        tmp_path, monkeypatch, capsys):
    from sahs.util.auth import reset_dotenv_cache
    env_file = tmp_path / "local.env"
    env_file.write_text("SAHS_STORE=sqlite\nAUTH_LOCAL_LOGIN=1\nAUTH_PEPPER=laptop-pepper\n")
    for name in ("SAHS_STORE", "AUTH_LOCAL_LOGIN", "AUTH_PEPPER", "SAHS_ENV_FILE",
                 "SYNAPSE_VERTEX_SA_KEY", "VERTEX_PROJECT_ID", "SYNAPSE_BQ_SA_KEY",
                 "GOOGLE_OAUTH_CLIENT_ID", "OKTA_ISSUER", "GATEWAY_BASE_URL"):
        monkeypatch.delenv(name, raising=False)
    reset_dotenv_cache()
    runner = Runner()
    assert readiness.main(["--env", "local", "--env-file", str(env_file)], run=runner) == 0
    out = capsys.readouterr().out
    assert out.startswith("readiness · local\n")
    header, *rows = out.splitlines()[1:]
    assert header.split()[:2] == ["check", "verdict"]
    assert rows[0].startswith("env file") and str(env_file) in rows[0]
    assert rows[1].startswith("settings") and "  ok  " in rows[1]
    assert rows[-1] == "ready"
    # the checks were handed the same file
    assert all(c[2]["SAHS_ENV_FILE"] == str(env_file) for c in runner.calls)
    # a missing file is the first row, and not ready
    assert readiness.main(["--env", "e3", "--env-file", str(tmp_path / "nope.env")],
                          run=runner) == 1
    text = capsys.readouterr().out
    assert "missing setting SAHS_ENV_FILE" in text and text.rstrip().endswith("run again")
    # --json carries the same rows
    assert readiness.main(["--env", "local", "--env-file", str(env_file), "--json"],
                          run=runner) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["ready"] is True and report["checks"][1]["check"] == "settings"


def test_the_examples_carry_the_complete_variable_set_with_placeholders_only():
    env_dir = SILO / "env"
    for profile in readiness.PROFILES:
        text = (env_dir / f"{profile}.env.example").read_text(encoding="utf-8")
        pairs = dict(line.split("=", 1) for line in text.splitlines()
                     if line and not line.startswith("#") and "=" in line)
        for name in readiness.REQUIRED[profile]:
            assert name in pairs, (profile, name)
        # never a real host: every URL names a <placeholder>
        for name, value in pairs.items():
            if "://" in value:
                assert "<" in value, (profile, name, value)
        if profile == "local":
            assert pairs["SAHS_STORE"] == "sqlite" and pairs["AUTH_LOCAL_LOGIN"] == "1"
        else:
            assert pairs["SAHS_STORE"] == "spanner" and pairs["EPAAS_ENV"] == profile
            assert pairs["MERIDIAN_BUILDS_SOURCE"] == "spanner"
        if profile == "e3":
            assert "AUTH_LOCAL_LOGIN" not in pairs
        if profile == "e2":
            assert pairs["AUTH_LOCAL_LOGIN"] == "0"
    # every variable the examples name is documented in .env.example
    documented = (SILO / ".env.example").read_text(encoding="utf-8")
    for profile in readiness.PROFILES:
        text = (env_dir / f"{profile}.env.example").read_text(encoding="utf-8")
        for line in text.splitlines():
            if line and not line.startswith("#") and "=" in line:
                name = line.split("=", 1)[0]
                # the per-environment endpoints are documented once, as _E1
                name = re.sub(r"_E[23]$", "_E1", name)
                assert name in documented, (profile, name)


@pytest.mark.parametrize("profile", ["e1", "e2", "e3"])
def test_the_example_itself_is_refused_until_filled_in(profile):
    text = (SILO / "env" / f"{profile}.env.example").read_text(encoding="utf-8")
    pairs = dict(line.split("=", 1) for line in text.splitlines()
                 if line and not line.startswith("#") and "=" in line)
    row = readiness.check_settings(profile, pairs)
    assert row["verdict"].startswith("bad setting") and "placeholder" in row["reason"]
