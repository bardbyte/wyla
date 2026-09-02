"""execute_sandboxed — the ONLY door to the warehouse, and the model is
never the lock (E3).

Order is the security property, pinned: parse → resolve tables → **ACL
verdict BEFORE any execution object is constructed** → then, and only
then, a substrate (snapshot = dry-run) or runner (live) may exist.

Modes:
- ``snapshot`` — the dry-run substrate: validity + result schema +
  bytes, zero rows ever. A table with UNKNOWN row-access policy is
  permitted here WITH ``meta.policy_unknown = true`` (the disclosure
  travels with the answer).
- ``live`` — default DENY. Requires ``SAHS_ALLOW_LIVE=1`` in the
  environment, refuses any policy-unknown or restricted table
  (fail-closed), passes a dry-run **cost gate**
  (``SAHS_LIVE_MAX_BYTES``, default 1e9) and a **row cap** (LIMIT
  injected/tightened via the AST) before a single row moves.

Every decision — allowed, denied, errored — is appended to an
append-only JSONL ledger beside the builds directory. The ledger is the
audit trail; silence is not an outcome.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path
from typing import Any, Protocol

from sahs.canon.canonical import try_canon
from sahs.tools.api import Build
from sahs.tools.qualify import qualify_tables
from sahs.tools.validate_sql import _DML_DDL, _QUERY_KINDS

DEFAULT_MAX_BYTES = 1_000_000_000
DEFAULT_ROW_CAP = 1000
MIN_PRIOR_JOBS = 20        # A6: thinner priors are anecdotes, not gates


class LiveRunner(Protocol):
    """The only shape a live executor may take."""

    name: str

    def run(self, sql: str, limit: int) -> dict[str, Any]: ...


class BQJobRunner:
    """Laptop live runner: synchronous jobs.query via the SVC-ID
    contract. Constructed ONLY after the ACL verdict allowed live."""

    name = "bq_jobs_query"

    def __init__(self, connection=None, token_provider=None) -> None:
        from sahs.util.auth import BQConnection
        self.connection = connection or BQConnection.from_env()
        self._token_provider = token_provider

    def _token(self) -> str:
        if self._token_provider is not None:
            return str(self._token_provider())
        from google.auth.transport.requests import Request     # type: ignore
        from google.oauth2 import service_account              # type: ignore
        creds = service_account.Credentials.from_service_account_file(
            str(self.connection.key_path),
            scopes=["https://www.googleapis.com/auth/bigquery"])
        creds.refresh(Request(session=self.connection.token_session()))
        return creds.token

    def run(self, sql: str, limit: int) -> dict[str, Any]:
        import urllib.request
        url = (f"{self.connection.endpoint}/bigquery/v2/projects/"
               f"{self.connection.project}/queries")
        body = {"query": sql, "useLegacySql": False,
                "maxResults": limit, "timeoutMs": 60000}
        if getattr(self.connection, "location", ""):
            body["location"] = self.connection.location
        request = urllib.request.Request(
            url, data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(
                request, timeout=90,
                context=self.connection.ssl_context()) as response:
            payload = json.loads(response.read().decode("utf-8"))
        fields = (payload.get("schema") or {}).get("fields") or []
        names = [f.get("name", "") for f in fields]
        rows = [[cell.get("v") for cell in row.get("f", [])]
                for row in payload.get("rows", [])][:limit]
        return {"rows": rows,
                "schema": [{"name": f.get("name", "").lower(),
                            "type": f.get("type", "")} for f in fields],
                "columns": names,
                "bytes_processed": int(
                    payload.get("totalBytesProcessed", 0) or 0)}


def _ledger_write(path: Path | None, entry: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True)
                + "\n")


def _cap_limit(sql: str, limit: int) -> str:
    """Inject or tighten LIMIT via the AST — never trust string games."""
    import sqlglot
    from sqlglot import expressions as exp
    tree = sqlglot.parse_one(sql, read="bigquery")
    existing = tree.args.get("limit")
    if existing is not None:
        try:
            current = int(existing.expression.name)
        except (AttributeError, ValueError):
            current = limit + 1
        if current <= limit:
            return sql
    return tree.limit(limit).sql(dialect="bigquery")


def execute_sandboxed(build: Build, sql: str, mode: str = "snapshot",
                      limit: int = DEFAULT_ROW_CAP, *,
                      substrate: Any = None, runner: LiveRunner | None
                      = None, ledger_path: Path | None = None,
                      env: dict[str, str] | None = None) -> dict:
    """Run ``sql`` against the warehouse under the compiled ACL.
    ``mode``: snapshot (dry-run; default) | live (default-deny)."""
    env = os.environ if env is None else env
    if ledger_path is None:
        ledger_path = build.root.parent / "sandbox_ledger.jsonl"
    meta: dict[str, Any] = {"mode": mode, "build_version": build.version,
                            "policy_unknown": False}

    def _finish(status: str, data: Any = None, error: str = "",
                **extra: Any) -> dict:
        meta.update(extra)
        _ledger_write(ledger_path, {
            "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(
                timespec="seconds"),
            "mode": mode, "decision": status, "error": error,
            "tables": meta.get("tables", []),
            "fp": meta.get("fp", ""), "limit": limit,
            "bytes": meta.get("bytes_scanned")})
        return {"status": status, "data": data,
                "error": error or None, "meta": meta}

    if mode not in ("snapshot", "live"):
        return _finish("error", error=f"unknown mode {mode!r}: "
                       "snapshot | live")

    # 1. parse (static — no execution object exists yet)
    result, err = try_canon(sql)
    if err is not None:
        return _finish("error", error=f"parse_error: {str(err)[:160]}")
    if result.kind in _DML_DDL or result.kind not in _QUERY_KINDS:
        return _finish("denied",
                       error=f"statement_not_allowed: {result.kind}: "
                             "read-only SELECT only")
    meta["fp"] = result.fp_expr

    # 2. resolve tables against the build
    tables: list[str] = []
    for raw in result.tables:
        physical = build.physical_of(raw)
        if physical is None:
            return _finish("error",
                           error=f"unknown_table: {raw!r}: validate_sql "
                                 "first; describe_table lists tables")
        tables.append(physical)
    meta["tables"] = sorted(set(tables))

    # 3. ACL verdict — BEFORE any substrate/runner is constructed (E3)
    unknown = [t for t in meta["tables"]
               if build.acl.get(t, {}).get("restricted")
               == "unknown_policy"]
    restricted = [t for t in meta["tables"]
                  if build.acl.get(t, {}).get("restricted")
                  not in (None, "", "unknown_policy")]
    if unknown:
        meta["policy_unknown"] = True
    if mode == "live":
        if unknown:
            return _finish(
                "denied",
                error="policy_unknown: live execution DENIED for "
                      + ", ".join(unknown)
                      + ": the row-access policy could not be read at "
                        "extraction; resolve the sensitivity_conflict "
                        "ticket, recompile, then retry. Snapshot mode "
                        "(dry-run) remains available.")
        if restricted:
            return _finish(
                "denied",
                error="restricted_table: live execution requires "
                      "clearance for " + ", ".join(restricted))
        if env.get("SAHS_ALLOW_LIVE") != "1":
            return _finish(
                "denied",
                error="live_disabled: live mode is default-deny: set "
                      "SAHS_ALLOW_LIVE=1 on the laptop to enable; "
                      "snapshot mode answers most questions")

    # 4. only now may an execution object exist
    if substrate is None:
        from sahs.evals.substrate import BQDryRun
        substrate = BQDryRun()

    # the project that HOSTS the tables may not be the one that runs
    # the query: qualify every table the build knows before the trip
    # (qualify.py) — the model keeps writing the cards' dataset.table
    data_project = (
        env.get("BQ_DATA_PROJECT") or env.get("LUMI_BQ_DATA_PROJECT")
        or getattr(getattr(substrate, "connection", None),
                   "data_project", "")
        or getattr(getattr(runner, "connection", None),
                   "data_project", ""))
    sent, qualified = qualify_tables(sql, build, data_project)
    if qualified:
        meta["sql_sent"] = sent
        meta["qualified"] = qualified

    outcome = substrate.dry_run(sent)
    if not outcome.valid:
        return _finish("error", error=f"invalid_sql: {outcome.error}")
    meta["bytes_scanned"] = outcome.bytes_processed

    if mode == "snapshot":
        return _finish("ok", data={
            "valid": True,
            "result_schema": outcome.result_schema,
            "bytes_processed": outcome.bytes_processed,
            "rows": None,
            "note": "snapshot mode = dry-run: shape and cost, no rows"})

    # 5. live: TWO cost gates answering two different questions,
    # never substitutable (E12/A3 amended). The budget ceiling asks
    # "can any query be worth this much?"; the anomaly gate asks "is
    # this query normal for THIS table?" (3× its observed p95, jobs
    # witness). A prior may TIGHTEN the cap below global, never loosen
    # it above — effective cap = min(both). Thin priors are no priors:
    # <20 canonicalized jobs and the p95 is an anecdote in a
    # percentile's clothes, so global alone applies. Constants are
    # assumption A6, revisited from denied-query triage.
    observed = outcome.bytes_processed or 0
    max_bytes = int(env.get("SAHS_LIVE_MAX_BYTES", DEFAULT_MAX_BYTES))
    if observed > max_bytes:
        return _finish(
            "denied",
            error=f"cost_gate_budget: dry-run predicts {observed:,} "
                  f"bytes > the live-mode ceiling {max_bytes:,} — add "
                  "a partition filter (see the table card's grain "
                  "line)")
    for physical in meta["tables"]:
        prior = build.cost_priors.get(physical) or {}
        if int(prior.get("n_jobs") or 0) < MIN_PRIOR_JOBS:
            continue
        anomaly_cap = 3 * int(prior.get("p95_bytes") or 0)
        if anomaly_cap and observed > anomaly_cap:
            meta["anomaly_cap_bytes"] = anomaly_cap
            return _finish(
                "denied",
                error=f"cost_gate_anomaly: dry-run predicts "
                      f"{observed:,} bytes — over 3× {physical}'s "
                      f"observed p95 of {prior['p95_bytes']:,} bytes "
                      f"({prior['n_jobs']} jobs, 30d). Normal usage "
                      "bounds normal queries; narrow the scan or "
                      "justify the outlier to a steward.")
    if runner is None:
        runner = BQJobRunner()
    capped = _cap_limit(sent, limit)
    data = runner.run(capped, limit)
    meta["bytes_scanned"] = data.get("bytes_processed",
                                     meta["bytes_scanned"])
    meta["row_cap"] = limit
    return _finish("ok", data={"rows": data["rows"],
                               "schema": data.get("schema"),
                               "row_count": len(data["rows"])})
