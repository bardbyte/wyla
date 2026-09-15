"""The scripts: each refuses by name, prints no secret, and runs with
nothing but this package on the path."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

from tests.doubles import FakeCatalog, ScriptedModel, entry, google_error

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(
        f"scripts_{name}", ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kc_check_refuses_by_name_without_the_catalog_contract(capsys):
    assert _load("kc_check").main([]) == 3
    err = capsys.readouterr().err
    assert "KC_PROJECT_ID" in err and "KC_SA_KEY" in err


def test_kc_check_connects_prints_the_configuration_and_never_the_token(
        capsys, catalog_env):
    fake = FakeCatalog(default=[entry("orders", description="d")])
    assert _load("kc_check").main(["--query", "type=table"], http=fake.http,
                                  token=fake.token) == 0
    out = capsys.readouterr().out
    assert "resolved configuration" in out and "demo-project" in out
    assert "CONNECTED" in out and "orders" in out
    assert "catalog-token" not in out
    assert fake.requests[0]["body"] == {"query": "type=table", "pageSize": 3,
                                        "semanticSearch": True}


def test_kc_check_names_the_remedy_for_a_refusal(capsys, catalog_env):
    fake = FakeCatalog()
    fake.fail_next = [(403, google_error(403, "USER_PROJECT_DENIED",
                                         "quota project not allowed"))]
    assert _load("kc_check").main([], http=fake.http, token=fake.token) == 1
    err = capsys.readouterr().err
    assert "serviceusage.serviceUsageConsumer" in err
    assert "KC_QUOTA_PROJECT=none" in err


def test_kc_check_json_is_machine_readable(capsys, catalog_env):
    fake = FakeCatalog(default=[entry("orders")])
    assert _load("kc_check").main(["--json"], http=fake.http,
                                  token=fake.token) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["config"]["project"] == "demo-project"
    assert outcome["results"][0]["display_name"] == "orders"


def test_model_check_refuses_by_name_and_proves_a_round_trip(capsys,
                                                             gateway_env,
                                                             monkeypatch):
    monkeypatch.delenv("GATEWAY_BASE_URL")
    assert _load("model_check").main(["--plane", "gateway"]) == 3
    assert "GATEWAY_BASE_URL" in capsys.readouterr().err
    model = ScriptedModel([
        [{"call": {"name": "knowledge_catalog_search",
                   "args": {"query": "customer tables"}}}],
        [{"text": "customer_account and customer_contact hold customers."}]])
    assert _load("model_check").main(["--plane", "vertex", "--converse"],
                                     model=model) == 0
    out = capsys.readouterr().out
    assert "planes:" in out and "called knowledge_catalog_search 1×" in out
    assert "grounded answer" in out


def test_explore_json_and_table_outputs(capsys, catalog_env):
    fake = FakeCatalog({"q1": [entry("orders"), entry("lines")],
                        "q2": [entry("orders")]})
    steps = [[{"call": {"name": "knowledge_catalog_search",
                        "args": {"query": "q1"}}},
              {"call": {"name": "knowledge_catalog_search",
                        "args": {"query": "q2"}}}],
             [{"text": "orders, then lines."}]]
    explore = _load("explore")
    assert explore.main(["which?", "--json"], model=ScriptedModel(list(steps)),
                        http=fake.http, token=fake.token) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["answer"] == "orders, then lines."
    assert [e["witnesses"] for e in result["entries"]] == [2, 1]
    assert [s["query"] for s in result["searches"]] == ["q1", "q2"]

    fake = FakeCatalog({"q1": [entry("orders"), entry("lines")],
                        "q2": [entry("orders")]})
    assert explore.main(["which?", "--quiet"], model=ScriptedModel(list(steps)),
                        http=fake.http, token=fake.token) == 0
    out = capsys.readouterr().out
    assert "orders, then lines." in out and "── searches" in out
    assert "wit" in out and "── entries (2)" in out
    assert "model calls 2" in out


def test_explore_refuses_a_plane_it_does_not_have(capsys, catalog_env):
    assert _load("explore").main(["q", "--plane", "gpt"]) == 3
    assert "vertex and gateway" in capsys.readouterr().err
    assert _load("explore").main(["q"]) == 3
    assert "VERTEX_SA_KEY" in capsys.readouterr().err


def test_every_script_runs_with_nothing_but_the_package():
    for name in ("kc_check", "model_check", "explore"):
        done = subprocess.run([sys.executable, str(ROOT / "scripts" / f"{name}.py"),
                               "--help"], capture_output=True, text=True,
                              cwd=ROOT)
        assert done.returncode == 0, done.stderr
        assert name.replace("_", "_") in done.stdout
