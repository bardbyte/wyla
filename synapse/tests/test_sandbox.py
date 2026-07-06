"""Python analysis sandbox — containment behaviors an agent relies on."""

from __future__ import annotations

import os

from synapse.utils.sandbox import run_python


def test_runs_and_returns_stdout_and_artifacts():
    result = run_python(
        "import json\n"
        "print(json.dumps({'mean': 0.6}))\n"
        "open('out.json', 'w').write('{\"k\": 1}')\n"
    )
    assert result["status"] == "ok"
    assert '"mean": 0.6' in result["stdout"]
    assert result["artifacts"] == {"out.json": '{"k": 1}'}


def test_environment_is_scrubbed():
    os.environ["SBX_TEST_SECRET"] = "leakme"
    try:
        result = run_python("import os; print(sorted(os.environ))")
        assert "SBX_TEST_SECRET" not in result["stdout"]
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in result["stdout"]
    finally:
        del os.environ["SBX_TEST_SECRET"]


def test_timeout_is_enforced():
    result = run_python("while True: pass", timeout_seconds=2)
    assert result["status"] == "timeout"


def test_crash_is_reported_not_raised():
    result = run_python("raise ValueError('boom')")
    assert result["status"] == "error"
    assert "boom" in result["stderr"]


def test_input_files_are_mounted_by_basename(tmp_path):
    data = tmp_path / "rates.csv"
    data.write_text("m,v\njan,0.62\n", encoding="utf-8")
    result = run_python(
        "print(open('rates.csv').read().strip().splitlines()[-1])",
        input_files={"../../evil/rates.csv": data},  # traversal neutralized
    )
    assert result["status"] == "ok"
    assert "jan,0.62" in result["stdout"]


def test_empty_code_rejected():
    assert run_python("   ")["status"] == "error"
