"""Execution substrates — where a grader's SQL actually goes.

Decision locked (assumption A1): **dry-run only for now**. BigQuery's
dry-run costs nothing and returns the query's RESULT SCHEMA, so the
ground can grade validity + output-schema equivalence against gold
without touching a single data row. The Protocol keeps the upgrade slot
open: a governed sample or synthetic warehouse slots in behind the same
interface with zero grader rework.

``BQDryRun`` talks REST directly through the SVC-ID contract
(util.auth) — no google-cloud-bigquery dependency; the silo owns its
surface area. It only ever sets ``dryRun: true``; this module cannot
execute anything by construction.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol

from sahs.util.auth import BQConnection


@dataclass
class DryRunOutcome:
    valid: bool
    error: str = ""
    result_schema: list[dict[str, str]] | None = None   # [{name, type}]
    bytes_processed: int | None = None


class ExecutionSubstrate(Protocol):
    """The eval harness's only view of an execution backend."""

    name: str

    def dry_run(self, sql: str) -> DryRunOutcome: ...


class BQDryRun:
    """Laptop substrate: POST jobs?dryRun=true via the enterprise endpoint."""

    name = "bq_dry_run"

    def __init__(self, connection: BQConnection | None = None,
                 token_provider: Any = None) -> None:
        self.connection = connection or BQConnection.from_env()
        self._token_provider = token_provider

    def _token(self) -> str:
        if self._token_provider is not None:
            return str(self._token_provider())
        # google-auth is present wherever the laptop already runs BQ; kept
        # as a runtime import so fixture CI never needs it. The refresh
        # session honors the connection's verify/CA settings and the
        # NO_PROXY injection done at from_env (the bq_connect contract).
        # cached per key file (sahs.util.auth): one token trip per
        # session, refreshed only when it expires
        return self.connection.token()

    def dry_run(self, sql: str) -> DryRunOutcome:
        url = (f"{self.connection.endpoint}/bigquery/v2/projects/"
               f"{self.connection.project}/jobs")
        body: dict = {"configuration": {
            "dryRun": True,
            "query": {"query": sql, "useLegacySql": False}}}
        if getattr(self.connection, "location", ""):
            body["jobReference"] = {"location": self.connection.location}
        request = urllib.request.Request(
            url, data=json.dumps(body).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token()}",
                     "Content-Type": "application/json"},
            method="POST")
        try:
            # the connection's opener: this plane's route and TLS,
            # never the environment's NO_PROXY
            with self.connection.opener().open(
                    request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                detail = json.loads(e.read().decode("utf-8"))
                message = detail.get("error", {}).get("message", str(e))
            except Exception:
                message = str(e)
            return DryRunOutcome(valid=False, error=message)
        except Exception as e:
            return DryRunOutcome(valid=False, error=f"transport: {e}")
        stats = payload.get("statistics", {}).get("query", {})
        fields = (stats.get("schema") or {}).get("fields") or []
        return DryRunOutcome(
            valid=True,
            result_schema=[{"name": f.get("name", "").lower(),
                            "type": f.get("type", "")} for f in fields],
            bytes_processed=int(stats.get("totalBytesProcessed", 0) or 0))


class StaticSubstrate:
    """CI substrate: canned outcomes keyed by canonical fingerprint —
    lets grader logic be tested without any warehouse at all."""

    name = "static"

    def __init__(self, outcomes: dict[str, DryRunOutcome]) -> None:
        self.outcomes = outcomes

    def dry_run(self, sql: str) -> DryRunOutcome:
        from sahs.canon.canonical import try_canon
        result, err = try_canon(sql)
        if err is not None:
            return DryRunOutcome(valid=False, error=err.category)
        hit = self.outcomes.get(result.fp_expr)
        if hit is None:
            return DryRunOutcome(valid=True, result_schema=None)
        return hit


class FaultySubstrate:
    """Fault injection around any substrate: the first dry runs fail
    with the given messages, in order; the rest go through to the
    inner substrate (the laptop's BigQuery when ``inner`` is None).
    This is how a recovery eval asks the same question offline and
    against the real warehouse."""

    name = "faulty"

    def __init__(self, faults: list[str], inner: Any = None) -> None:
        self.faults = list(faults)
        self.inner = inner
        self.seen: list[str] = []

    @property
    def connection(self) -> Any:
        return getattr(self.inner, "connection", None)

    def dry_run(self, sql: str) -> DryRunOutcome:
        self.seen.append(sql)
        if self.faults:
            return DryRunOutcome(valid=False, error=self.faults.pop(0))
        if self.inner is None:
            import sahs.evals.substrate as me       # late: patchable
            self.inner = me.BQDryRun()
        return self.inner.dry_run(sql)
