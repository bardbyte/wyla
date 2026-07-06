"""WarehouseRunner — read-only BigQuery execution behind a fixed gate chain.

The user confirmed the SVC ID can execute queries and return data. This
module makes that power safe enough to hand to an agent by encoding the
checks as a deterministic workflow (never model judgment):

    GATE 1  shape      single statement, SELECT/WITH only (no DML/DDL/scripts)
    GATE 2  guardrails GraphService.validate_sql_plan — skill guardrail
                       violations refuse execution outright
    GATE 3  dry run    BigQuery validates against LIVE schema and prices the
                       scan — nothing executes, nothing is billed
    GATE 4  budget     bytes-scanned cap (default 5 GiB) — over-budget
                       queries refuse with the number and how to narrow
    GATE 5  execute    row-capped, timeout-bounded; results truncated, never
                       streamed unbounded into the model's context

Every attempt — pass or refuse — appends one JSONL record to the audit
ledger: timestamp, sql sha256, gates passed, bytes, rows, outcome. That
ledger is the "full traceability" surface the demo UI renders.

The BigQuery client is injected (any object with ``dry_run(sql)`` and
``execute(sql, max_rows, timeout_s)``), so the gate chain is fully
testable offline; ``BigQueryClient`` is the real adapter, imported lazily
so the module works where google-cloud-bigquery isn't installed.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

# BigQuery on-demand US list price per TiB scanned.
_USD_PER_TIB = 6.25
_TIB = float(2**40)


class WarehouseClient(Protocol):
    def dry_run(self, sql: str) -> dict[str, Any]:
        """→ {"valid": bool, "total_bytes": int, "error": str|None}"""
        ...

    def execute(self, sql: str, *, max_rows: int,
                timeout_s: float) -> dict[str, Any]:
        """→ {"rows": list[dict], "total_rows": int|None,
              "bytes_billed": int|None, "job_id": str|None}"""
        ...


@dataclass
class GateConfig:
    max_bytes_scanned: int = 5 * 2**30          # 5 GiB
    max_rows_returned: int = 500
    timeout_seconds: float = 60.0
    require_guardrail_pass: bool = True
    audit_path: Path | None = None               # None → in-memory only
    dialect: str = "bigquery"
    extra: dict[str, Any] = field(default_factory=dict)


class WarehouseRunner:
    def __init__(
        self,
        client: WarehouseClient | None = None,
        *,
        graph_service: Any = None,               # GraphService, optional
        gate: GateConfig | None = None,
    ) -> None:
        self.client = client
        self.graph_service = graph_service
        self.gate = gate or GateConfig()
        self.audit_log: list[dict[str, Any]] = []  # in-memory mirror

    # ── public tools ─────────────────────────────────────────

    def dry_run(self, sql: str) -> dict[str, Any]:
        """Gates 1-4 without execution: shape → guardrails → live dry-run →
        budget verdict. Free, touches no rows."""
        started = time.monotonic()
        record = self._begin("dry_run", sql)
        outcome = self._run_gates(sql, record)
        outcome["latency_ms"] = int((time.monotonic() - started) * 1000)
        self._commit(record, outcome)
        return outcome

    def execute(self, sql: str, max_rows: int | None = None) -> dict[str, Any]:
        """Full gate chain, then a row-capped, timeout-bounded read.
        Refusals return structured reasons, never exceptions."""
        started = time.monotonic()
        record = self._begin("execute", sql)
        outcome = self._run_gates(sql, record)
        if outcome["status"] != "ok" or not outcome["data"]["within_budget"]:
            if outcome["status"] == "ok":
                outcome = self._refuse(
                    "over_budget",
                    f"query would scan {outcome['data']['gb_scanned']} GB — "
                    f"cap is {round(self.gate.max_bytes_scanned / 2**30, 1)} GB. "
                    "Narrow the partition window or select fewer columns.",
                    gates=outcome["data"],
                )
            outcome["latency_ms"] = int((time.monotonic() - started) * 1000)
            self._commit(record, outcome)
            return outcome

        row_cap = min(max_rows or self.gate.max_rows_returned,
                      self.gate.max_rows_returned)
        try:
            result = self.client.execute(  # type: ignore[union-attr]
                sql, max_rows=row_cap, timeout_s=self.gate.timeout_seconds)
        except Exception as exc:
            outcome = self._refuse("execution_error", str(exc)[:400],
                                   gates=outcome["data"])
            outcome["latency_ms"] = int((time.monotonic() - started) * 1000)
            self._commit(record, outcome)
            return outcome

        rows = result.get("rows") or []
        total = result.get("total_rows")
        outcome = {
            "status": "ok",
            "action": "execute",
            "data": {
                **outcome["data"],
                "rows": rows[:row_cap],
                "n_rows_returned": min(len(rows), row_cap),
                "total_rows": total,
                "truncated": bool(total is not None and total > row_cap)
                or len(rows) > row_cap,
                "bytes_billed": result.get("bytes_billed"),
                "job_id": result.get("job_id"),
            },
        }
        outcome["latency_ms"] = int((time.monotonic() - started) * 1000)
        self._commit(record, outcome)
        return outcome

    # ── the gate chain (shared by dry_run and execute) ───────

    def _run_gates(self, sql: str, record: dict[str, Any]) -> dict[str, Any]:
        gates: dict[str, Any] = {}

        # GATE 1 — statement shape
        shape_error = self._check_shape(sql)
        gates["shape"] = "pass" if shape_error is None else "fail"
        if shape_error is not None:
            return self._refuse("bad_statement", shape_error, gates=gates)

        # GATE 2 — graph guardrails (skill rules are law)
        if self.graph_service is not None and self.gate.require_guardrail_pass:
            check = self.graph_service.validate_sql_plan(
                sql, dialect=self.gate.dialect)
            violations = ((check.get("data") or {}).get("violations")) or []
            gates["guardrails"] = "fail" if violations else "pass"
            gates["guardrail_violations"] = violations
            if violations:
                return self._refuse(
                    "guardrail_violation",
                    "; ".join(v.get("reason", "") for v in violations)[:400],
                    gates=gates,
                )
        else:
            gates["guardrails"] = "skipped"

        # GATE 3 — live dry-run
        if self.client is None:
            return self._refuse(
                "no_client",
                "no warehouse client configured — set GOOGLE_APPLICATION_"
                "CREDENTIALS and BQ project env on a network with BigQuery "
                "access (work laptop), or inject a client.",
                gates=gates,
            )
        try:
            dry = self.client.dry_run(sql)
        except Exception as exc:
            return self._refuse("dry_run_error", str(exc)[:400], gates=gates)
        if not dry.get("valid"):
            gates["dry_run"] = "fail"
            return self._refuse(
                "invalid_sql",
                str(dry.get("error") or "dry run rejected the query")[:400],
                gates=gates,
            )
        gates["dry_run"] = "pass"

        # GATE 4 — budget
        total_bytes = int(dry.get("total_bytes") or 0)
        within = total_bytes <= self.gate.max_bytes_scanned
        gates["budget"] = "pass" if within else "fail"
        return {
            "status": "ok",
            "action": "dry_run",
            "data": {
                "valid": True,
                "gates": gates,
                "total_bytes": total_bytes,
                "gb_scanned": round(total_bytes / 2**30, 3),
                "est_cost_usd": round(total_bytes / _TIB * _USD_PER_TIB, 4),
                "within_budget": within,
                "budget_gb": round(self.gate.max_bytes_scanned / 2**30, 1),
            },
        }

    @staticmethod
    def _check_shape(sql: str) -> str | None:
        """Single read-only statement or a reason string."""
        if not sql or not sql.strip():
            return "empty SQL"
        try:
            import sqlglot
            from sqlglot import expressions as exp
        except ImportError:
            return "sqlglot is required for the statement-shape gate"
        try:
            statements = [s for s in sqlglot.parse(sql, read="bigquery") if s]
        except Exception as exc:
            return f"unparseable SQL: {str(exc)[:200]}"
        if len(statements) != 1:
            return f"exactly one statement allowed, got {len(statements)}"
        stmt = statements[0]
        # unwrap WITH — sqlglot represents CTEs inside the Select
        if not isinstance(stmt, (exp.Select, exp.Union)):
            return (f"read-only SELECT required, got {stmt.key.upper()} — "
                    "writes are never allowed through the agent")
        forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Merge,
                     exp.Create, exp.Drop, exp.Alter)
        for node_type in forbidden:
            if stmt.find(node_type) is not None:
                return f"forbidden operation inside query: {node_type.__name__}"
        return None

    # ── refusals + audit ledger ──────────────────────────────

    @staticmethod
    def _refuse(code: str, reason: str, *, gates: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "refused",
            "code": code,
            "reason": reason,
            "data": {"gates": gates},
        }

    def _begin(self, action: str, sql: str) -> dict[str, Any]:
        return {
            "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "action": action,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16],
            "sql_prefix": " ".join(sql.split())[:120],
        }

    def _commit(self, record: dict[str, Any], outcome: dict[str, Any]) -> None:
        record["status"] = outcome.get("status")
        record["code"] = outcome.get("code")
        data = outcome.get("data") or {}
        record["gates"] = data.get("gates")
        record["total_bytes"] = data.get("total_bytes")
        record["n_rows_returned"] = data.get("n_rows_returned")
        self.audit_log.append(record)
        if self.gate.audit_path is not None:
            self.gate.audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self.gate.audit_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, default=str) + "\n")


# ── the real adapter (laptop path) ───────────────────────────


class BigQueryClient:
    """google-cloud-bigquery adapter. Lazy import; standard env auth
    (GOOGLE_APPLICATION_CREDENTIALS + billing project)."""

    def __init__(self, project: str | None = None,
                 location: str | None = None) -> None:
        from google.cloud import bigquery  # deferred — laptop dependency

        self._bq = bigquery
        self._client = bigquery.Client(project=project, location=location)

    def dry_run(self, sql: str) -> dict[str, Any]:
        config = self._bq.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            job = self._client.query(sql, job_config=config)
        except Exception as exc:
            return {"valid": False, "total_bytes": 0, "error": str(exc)}
        return {"valid": True,
                "total_bytes": int(job.total_bytes_processed or 0),
                "error": None}

    def execute(self, sql: str, *, max_rows: int,
                timeout_s: float) -> dict[str, Any]:
        job = self._client.query(sql)
        iterator = job.result(timeout=timeout_s, max_results=max_rows)
        rows = [dict(row) for row in iterator]
        return {
            "rows": rows,
            "total_rows": getattr(iterator, "total_rows", None),
            "bytes_billed": job.total_bytes_billed,
            "job_id": job.job_id,
        }
