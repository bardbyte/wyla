"""Gate chain behaviors the agent (and auditors) rely on."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from synapse.graph.builder import build_graph_from_sources
from synapse.loaders.skills_loader import load_skills_library
from synapse.mcp.service import GraphService
from synapse.warehouse.runner import GateConfig, WarehouseRunner

FIXTURE_LIBRARY = Path(__file__).parent / "fixtures" / "skills_library"


class FakeClient:
    def __init__(self, *, total_bytes: int = 1024, valid: bool = True,
                 rows: list[dict[str, Any]] | None = None,
                 total_rows: int | None = None) -> None:
        self.total_bytes = total_bytes
        self.valid = valid
        self.rows = rows if rows is not None else [{"m": "2026-06", "rate": 0.0231}]
        self.total_rows = total_rows if total_rows is not None else len(self.rows)
        self.calls: list[str] = []

    def dry_run(self, sql: str) -> dict[str, Any]:
        self.calls.append("dry_run")
        return {"valid": self.valid, "total_bytes": self.total_bytes,
                "error": None if self.valid else "column FOO not found"}

    def execute(self, sql: str, *, max_rows: int, timeout_s: float) -> dict[str, Any]:
        self.calls.append("execute")
        return {"rows": self.rows[:max_rows], "total_rows": self.total_rows,
                "bytes_billed": self.total_bytes, "job_id": "job_test"}


@pytest.fixture
def guarded_service(tmp_path) -> GraphService:
    # Guardrails are enforced from the SkillsRegistry (files), NOT graph
    # nodes — the data graph carries no skill nodes. GATE 2 must still bite.
    from synapse.mcp.skills_registry import SkillsRegistry
    load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    return GraphService(build_graph_from_sources(tmp_path),
                        skills=SkillsRegistry.from_dir(tmp_path / "skills"))


GOOD_SQL = ("SELECT rpt_month, SUM(bal_lag1) AS bal "
            "FROM common.roll_rate_calc GROUP BY rpt_month")


def test_happy_path_runs_all_gates_then_executes(guarded_service):
    client = FakeClient(total_bytes=100_000)
    runner = WarehouseRunner(client, graph_service=guarded_service)
    out = runner.execute(GOOD_SQL)
    assert out["status"] == "ok"
    assert out["data"]["gates"] == {
        "shape": "pass", "guardrails": "pass", "guardrail_violations": [],
        "dry_run": "pass", "budget": "pass",
    }
    assert client.calls == ["dry_run", "execute"]
    assert out["data"]["rows"][0]["rate"] == 0.0231


def test_dml_refused_before_any_network_call(guarded_service):
    client = FakeClient()
    runner = WarehouseRunner(client, graph_service=guarded_service)
    out = runner.execute("DELETE FROM common.roll_rate_calc WHERE 1=1")
    assert out["status"] == "refused" and out["code"] == "bad_statement"
    assert client.calls == []  # never reached the warehouse


def test_multi_statement_refused():
    runner = WarehouseRunner(FakeClient())
    out = runner.dry_run("SELECT 1; SELECT 2")
    assert out["status"] == "refused" and out["code"] == "bad_statement"


def test_guardrail_violation_refuses_execution(guarded_service):
    client = FakeClient()
    runner = WarehouseRunner(client, graph_service=guarded_service)
    out = runner.execute(
        "SELECT cm11_encrypted FROM common.roll_rate_calc")
    assert out["status"] == "refused" and out["code"] == "guardrail_violation"
    assert "cm11_encrypted" in out["reason"]
    assert client.calls == []  # refused before touching the warehouse


def test_over_budget_refused_with_actionable_number(guarded_service):
    client = FakeClient(total_bytes=50 * 2**30)  # 50 GiB
    runner = WarehouseRunner(client, graph_service=guarded_service,
                             gate=GateConfig(max_bytes_scanned=5 * 2**30))
    out = runner.execute(GOOD_SQL)
    assert out["status"] == "refused" and out["code"] == "over_budget"
    assert "50.0 GB" in out["reason"] and "5.0 GB" in out["reason"]
    assert client.calls == ["dry_run"]  # priced, never executed


def test_invalid_sql_surfaces_bq_error(guarded_service):
    runner = WarehouseRunner(FakeClient(valid=False),
                             graph_service=guarded_service)
    out = runner.dry_run(GOOD_SQL)
    assert out["status"] == "refused" and out["code"] == "invalid_sql"
    assert "FOO" in out["reason"]


def test_dry_run_reports_cost_estimate(guarded_service):
    runner = WarehouseRunner(FakeClient(total_bytes=2**40),  # 1 TiB
                             graph_service=guarded_service,
                             gate=GateConfig(max_bytes_scanned=2 * 2**40))
    out = runner.dry_run(GOOD_SQL)
    assert out["status"] == "ok"
    assert out["data"]["est_cost_usd"] == 6.25
    assert out["data"]["within_budget"] is True


def test_row_cap_and_truncation_flag(guarded_service):
    rows = [{"i": i} for i in range(50)]
    runner = WarehouseRunner(
        FakeClient(rows=rows, total_rows=5_000),
        graph_service=guarded_service,
        gate=GateConfig(max_rows_returned=10),
    )
    out = runner.execute(GOOD_SQL, max_rows=999)  # caller cannot exceed gate
    assert out["data"]["n_rows_returned"] == 10
    assert out["data"]["truncated"] is True


def test_no_client_is_a_structured_refusal(guarded_service):
    runner = WarehouseRunner(None, graph_service=guarded_service)
    out = runner.dry_run(GOOD_SQL)
    assert out["status"] == "refused" and out["code"] == "no_client"
    assert "work laptop" in out["reason"]


def test_audit_ledger_records_every_attempt(tmp_path, guarded_service):
    audit = tmp_path / "audit" / "ledger.jsonl"
    runner = WarehouseRunner(FakeClient(), graph_service=guarded_service,
                             gate=GateConfig(audit_path=audit))
    runner.execute(GOOD_SQL)                                  # ok
    runner.execute("SELECT cm11_encrypted FROM common.roll_rate_calc")  # refused
    lines = [json.loads(l) for l in audit.read_text().splitlines()]
    assert len(lines) == 2
    assert lines[0]["status"] == "ok" and lines[0]["action"] == "execute"
    assert lines[1]["status"] == "refused"
    assert all(len(l["sql_sha256"]) == 16 for l in lines)
    assert runner.audit_log[0]["gates"]["dry_run"] == "pass"
