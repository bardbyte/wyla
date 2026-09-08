"""BigQuery REST on the proven laptop contract — no google-cloud-bigquery.

Rides ``sahs.util.auth.BQConnection``: the same .env bootstrap, PSC
endpoint, pinned direct route, TLS knobs and cached OAuth token that
``scripts/bq_check.py`` already proves. Everything here is urllib +
json; ``google-auth`` is touched only inside ``connection.token()``.

What this client adds over a raw POST:

    typed outcomes   every HTTP/job failure becomes a BQError carrying a
                     STATUS (DENIED / NOT_FOUND / INVALID / QUOTA /
                     TRANSPORT / ERROR) — the archive's status vocabulary,
                     decided in one place from the error reason
    retries          429 / 5xx / transport errors back off (jittered,
                     capped); 4xx never retries
    parameters       named query parameters, so table names and dates
                     are never spliced into SQL text
    paging           getQueryResults pages until the job is complete and
                     every row is read; typed row decoding incl. nested
                     RECORD / REPEATED cells
    the ledger       bytes processed / billed per job, with the scope and
                     operation that asked — the run's cost record
    the guard        ``maximumBytesBilled`` on every real job (server-side
                     cap) plus a client-side Budget that dry runs check
                     before spending
"""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

from sahs.util.auth import BQConnection

# ── status vocabulary (shared with the archive contract) ──
FETCHED = "FETCHED"
CACHED = "CACHED"
EMPTY = "EMPTY"
DENIED = "DENIED"
NOT_FOUND = "NOT_FOUND"
BUDGET_SKIPPED = "BUDGET_SKIPPED"
ERROR = "ERROR"
INVALID = "INVALID"          # BQ rejected the statement (our bug/unsupported)
QUOTA = "QUOTA"              # rate limited after retries
TRANSPORT = "TRANSPORT"      # network / TLS / proxy after retries
RUNNING = "RUNNING"
STATUSES = (FETCHED, CACHED, EMPTY, DENIED, NOT_FOUND, BUDGET_SKIPPED,
            ERROR, INVALID, QUOTA, TRANSPORT)

_RETRYABLE_REASONS = {"rateLimitExceeded", "backendError", "internalError",
                      "quotaExceeded", "jobRateLimitExceeded",
                      "jobBackendError", "jobInternalError"}
_DENIED_REASONS = {"accessDenied", "forbidden", "insufficientPermissions",
                   "authError"}
_NOT_FOUND_REASONS = {"notFound"}
_INVALID_REASONS = {"invalidQuery", "invalid", "badRequest", "invalidParameter",
                    "resourcesExceeded"}


class BQError(RuntimeError):
    """A BigQuery refusal with its archive status already decided."""

    def __init__(self, status: str, message: str, *, code: int = 0,
                 reason: str = "", job_id: str = "", operation: str = ""):
        super().__init__(message)
        self.status = status
        self.message = message
        self.code = code
        self.reason = reason
        self.job_id = job_id
        self.operation = operation

    def __str__(self) -> str:                      # pragma: no cover
        head = f"{self.status}"
        if self.code:
            head += f" http={self.code}"
        if self.reason:
            head += f" reason={self.reason}"
        return f"{head}: {self.message}"


def classify(code: int, reason: str, message: str) -> str:
    """HTTP code + BigQuery error reason + message → archive status."""
    reason = (reason or "").strip()
    text = (message or "")
    lower = text.lower()
    if reason in _DENIED_REASONS or code in (401, 403):
        return DENIED
    if reason in _NOT_FOUND_REASONS or code == 404 \
            or lower.startswith("not found:"):
        return NOT_FOUND
    if code == 429 or reason in ("rateLimitExceeded", "quotaExceeded",
                                 "jobRateLimitExceeded"):
        return QUOTA
    if "access denied" in lower or "permission denied" in lower \
            or "does not have" in lower and "permission" in lower:
        return DENIED
    if "bytes billed" in lower or "maximumbytesbilled" in lower:
        return BUDGET_SKIPPED
    if reason in _INVALID_REASONS or code == 400:
        return INVALID
    if code >= 500:
        return TRANSPORT
    return ERROR


def _retryable(code: int, reason: str) -> bool:
    return code == 429 or code >= 500 or reason in _RETRYABLE_REASONS


# ── query parameters ──

def p_str(name: str, value: str) -> dict:
    return {"name": name, "parameterType": {"type": "STRING"},
            "parameterValue": {"value": str(value)}}


def p_int(name: str, value: int) -> dict:
    return {"name": name, "parameterType": {"type": "INT64"},
            "parameterValue": {"value": str(int(value))}}


def p_ts(name: str, iso_utc: str) -> dict:
    """TIMESTAMP parameter from an ISO-8601 UTC string."""
    return {"name": name, "parameterType": {"type": "TIMESTAMP"},
            "parameterValue": {"value": iso_utc}}


def p_array_str(name: str, values: list[str]) -> dict:
    return {"name": name,
            "parameterType": {"type": "ARRAY",
                              "arrayType": {"type": "STRING"}},
            "parameterValue": {"arrayValues": [{"value": str(v)}
                                               for v in values]}}


# ── typed row decoding ──

def _decode_cell(field_schema: dict, cell: Any) -> Any:
    value = cell.get("v") if isinstance(cell, dict) else cell
    if field_schema.get("mode") == "REPEATED":
        items = value or []
        inner = dict(field_schema, mode="NULLABLE")
        return [_decode_cell(inner, item) for item in items]
    if value is None:
        return None
    ftype = (field_schema.get("type") or "STRING").upper()
    if ftype in ("RECORD", "STRUCT"):
        subfields = field_schema.get("fields") or []
        cells = value.get("f", []) if isinstance(value, dict) else []
        return {sf.get("name", f"f{i}"): _decode_cell(sf, cells[i]
                                                        if i < len(cells)
                                                        else {"v": None})
                for i, sf in enumerate(subfields)}
    if ftype in ("INTEGER", "INT64"):
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if ftype in ("FLOAT", "FLOAT64"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if ftype in ("BOOLEAN", "BOOL"):
        return str(value).lower() == "true"
    if ftype == "TIMESTAMP":
        # epoch seconds as a decimal string → ISO-8601 UTC
        try:
            import datetime as _dt
            secs = float(value)
            return _dt.datetime.fromtimestamp(
                secs, tz=_dt.timezone.utc).isoformat(
                    timespec="microseconds").replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError):
            return value
    return value


def decode_rows(schema: dict | None, rows: list) -> list[dict]:
    fields = (schema or {}).get("fields") or []
    out: list[dict] = []
    for row in rows or []:
        cells = row.get("f", []) if isinstance(row, dict) else []
        out.append({f.get("name", f"f{i}"):
                    _decode_cell(f, cells[i] if i < len(cells)
                                 else {"v": None})
                    for i, f in enumerate(fields)})
    return out


# ── outcomes ──

@dataclass
class QueryResult:
    rows: list[dict]
    schema: dict
    job_id: str = ""
    bytes_processed: int = 0
    bytes_billed: int = 0
    cache_hit: bool = False
    total_rows: int = 0
    elapsed_s: float = 0.0
    dry_run: bool = False
    referenced_tables: list[dict] = field(default_factory=list)
    statement_type: str = ""

    @property
    def status(self) -> str:
        return FETCHED if self.rows else EMPTY


@dataclass
class LedgerEntry:
    job_id: str
    scope: str
    operation: str
    bytes_processed: int
    bytes_billed: int
    cache_hit: bool
    elapsed_s: float
    dry_run: bool = False


class Ledger:
    """Thread-safe byte accounting for the run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.entries: list[LedgerEntry] = []

    def add(self, entry: LedgerEntry) -> None:
        with self._lock:
            self.entries.append(entry)

    @property
    def bytes_processed(self) -> int:
        with self._lock:
            return sum(e.bytes_processed for e in self.entries
                       if not e.dry_run)

    @property
    def bytes_billed(self) -> int:
        with self._lock:
            return sum(e.bytes_billed for e in self.entries
                       if not e.dry_run)

    def by_scope(self) -> dict[str, dict[str, int]]:
        with self._lock:
            out: dict[str, dict[str, int]] = {}
            for e in self.entries:
                if e.dry_run:
                    continue
                d = out.setdefault(e.scope, {"jobs": 0, "bytes_processed": 0,
                                             "bytes_billed": 0})
                d["jobs"] += 1
                d["bytes_processed"] += e.bytes_processed
                d["bytes_billed"] += e.bytes_billed
            return out

    def snapshot(self) -> list[dict]:
        with self._lock:
            return [e.__dict__.copy() for e in self.entries]


class Budget:
    """A scan allowance that dry-run estimates reserve against BEFORE a
    job runs. Reservations settle to the actual bytes billed after."""

    def __init__(self, total_bytes: int) -> None:
        self.total = int(total_bytes)
        self._lock = threading.Lock()
        self._reserved = 0
        self._spent = 0

    @property
    def spent(self) -> int:
        with self._lock:
            return self._spent

    @property
    def remaining(self) -> int:
        with self._lock:
            return max(0, self.total - self._spent - self._reserved)

    def try_reserve(self, estimate: int) -> bool:
        with self._lock:
            if self.total and self._spent + self._reserved + estimate \
                    > self.total:
                return False
            self._reserved += estimate
            return True

    def settle(self, estimate: int, actual: int) -> None:
        with self._lock:
            self._reserved = max(0, self._reserved - estimate)
            self._spent += max(0, int(actual))

    def release(self, estimate: int) -> None:
        with self._lock:
            self._reserved = max(0, self._reserved - estimate)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {"total_bytes": self.total, "spent_bytes": self._spent,
                    "reserved_bytes": self._reserved,
                    "remaining_bytes": max(0, self.total - self._spent
                                           - self._reserved)}


# ── the client ──

class BQRest:
    """Sync client; safe to share across worker threads."""

    def __init__(self, connection: BQConnection, *,
                 billing_project: str = "",
                 token_provider: Callable[[], str] | None = None,
                 opener: Any = None,
                 sleep: Callable[[float], None] = time.sleep,
                 max_retries: int = 5,
                 ledger: Ledger | None = None,
                 labels: dict[str, str] | None = None) -> None:
        self.connection = connection
        self.project = billing_project or connection.project
        self.location = (connection.location or "US").upper()
        self.endpoint = connection.endpoint.rstrip("/")
        self._token_provider = token_provider
        self._opener = opener
        self._sleep = sleep
        self.max_retries = max_retries
        self.ledger = ledger or Ledger()
        self.labels = dict(labels or {})
        self._token_lock = threading.Lock()

    # -- plumbing --
    def _token(self) -> str:
        with self._token_lock:
            if self._token_provider is not None:
                return str(self._token_provider())
            return self.connection.token()

    def _open(self, request: urllib.request.Request, timeout: int):
        opener = self._opener or self.connection.opener()
        return opener.open(request, timeout=timeout)

    def _call(self, method: str, path: str, *, body: dict | None = None,
              params: dict | None = None, timeout: int = 120,
              operation: str = "") -> dict:
        url = f"{self.endpoint}{path}"
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        attempt = 0
        while True:
            attempt += 1
            headers = {"Authorization": f"Bearer {self._token()}",
                       "Accept": "application/json"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url, data=data, headers=headers,
                                             method=method.upper())
            try:
                with self._open(request, timeout) as response:
                    raw = response.read().decode("utf-8")
                    return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as e:
                code = e.code
                try:
                    detail = json.loads(e.read().decode("utf-8"))
                    err = detail.get("error", {}) or {}
                    message = err.get("message") or str(e)
                    errors = err.get("errors") or []
                    reason = (errors[0].get("reason") if errors
                              else err.get("status", "")) or ""
                except Exception:                       # noqa: BLE001
                    message, reason = str(e), ""
                if _retryable(code, reason) and attempt <= self.max_retries:
                    self._sleep(self._backoff(attempt))
                    continue
                raise BQError(classify(code, reason, message), message,
                              code=code, reason=reason,
                              operation=operation) from None
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                if attempt <= self.max_retries:
                    self._sleep(self._backoff(attempt))
                    continue
                raise BQError(TRANSPORT, f"transport: {e}",
                              operation=operation) from None

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(30.0, (2 ** (attempt - 1)) + random.uniform(0, 0.5))

    # -- jobs --
    def _job_body(self, sql: str, params: list[dict] | None, *,
                  dry_run: bool, max_bytes_billed: int | None,
                  use_cache: bool, labels: dict[str, str] | None) -> dict:
        query: dict[str, Any] = {"query": sql, "useLegacySql": False,
                                 "useQueryCache": use_cache}
        if params:
            query["parameterMode"] = "NAMED"
            query["queryParameters"] = params
        if max_bytes_billed and not dry_run:
            query["maximumBytesBilled"] = str(int(max_bytes_billed))
        config: dict[str, Any] = {"query": query, "dryRun": dry_run}
        all_labels = {**self.labels, **(labels or {})}
        if all_labels:
            config["labels"] = {k[:63]: str(v)[:63]
                                for k, v in all_labels.items()}
        return {"jobReference": {"projectId": self.project,
                                 "location": self.location},
                "configuration": config}

    def dry_run(self, sql: str, params: list[dict] | None = None, *,
                scope: str = "", operation: str = "") -> QueryResult:
        """Bytes and result schema without spending — and the physical
        tables the statement would read (``referencedTables``), which is
        how a view's base table is discovered for free."""
        t0 = time.monotonic()
        payload = self._call(
            "POST", f"/bigquery/v2/projects/{self.project}/jobs",
            body=self._job_body(sql, params, dry_run=True,
                                max_bytes_billed=None, use_cache=False,
                                labels=None),
            timeout=60, operation=operation or "dryRun")
        status = payload.get("status", {}) or {}
        if status.get("errorResult"):
            er = status["errorResult"]
            raise BQError(classify(0, er.get("reason", ""),
                                   er.get("message", "")),
                          er.get("message", "dry run failed"),
                          reason=er.get("reason", ""),
                          operation=operation or "dryRun")
        stats = (payload.get("statistics") or {}).get("query") or {}
        result = QueryResult(
            rows=[], schema=stats.get("schema") or {},
            bytes_processed=int(stats.get("totalBytesProcessed", 0) or 0),
            bytes_billed=0, dry_run=True,
            elapsed_s=time.monotonic() - t0,
            referenced_tables=list(stats.get("referencedTables") or []),
            statement_type=stats.get("statementType", ""))
        self.ledger.add(LedgerEntry(
            job_id="dry-run", scope=scope, operation=operation,
            bytes_processed=result.bytes_processed, bytes_billed=0,
            cache_hit=False, elapsed_s=result.elapsed_s, dry_run=True))
        return result

    def query(self, sql: str, params: list[dict] | None = None, *,
              scope: str = "", operation: str = "",
              timeout_s: int = 900, max_bytes_billed: int | None = None,
              use_cache: bool = True, labels: dict[str, str] | None = None,
              page_size: int = 10000) -> QueryResult:
        """jobs.insert → poll getQueryResults → page every row."""
        t0 = time.monotonic()
        insert = self._call(
            "POST", f"/bigquery/v2/projects/{self.project}/jobs",
            body=self._job_body(sql, params, dry_run=False,
                                max_bytes_billed=max_bytes_billed,
                                use_cache=use_cache, labels=labels),
            timeout=120, operation=operation or "jobs.insert")
        job_ref = insert.get("jobReference") or {}
        job_id = job_ref.get("jobId", "")
        location = job_ref.get("location") or self.location
        if not job_id:
            raise BQError(ERROR, f"no job id in insert response: "
                          f"{json.dumps(insert)[:300]}",
                          operation=operation)
        status = insert.get("status") or {}
        if status.get("errorResult"):
            raise self._job_error(status["errorResult"], job_id, operation)

        deadline = time.monotonic() + timeout_s
        rows: list = []
        schema: dict = {}
        page_token: str | None = None
        bytes_processed = 0
        cache_hit = False
        total_rows = 0
        poll_wait = 1.0
        while True:
            if time.monotonic() > deadline:
                raise BQError(ERROR, f"job {job_id} did not finish within "
                              f"{timeout_s}s", job_id=job_id,
                              operation=operation)
            page = self._call(
                "GET", f"/bigquery/v2/projects/{self.project}/queries/"
                       f"{urllib.parse.quote(job_id)}",
                params={"location": location, "maxResults": page_size,
                        "pageToken": page_token,
                        "timeoutMs": 30000},
                timeout=90, operation=operation or "getQueryResults")
            if page.get("errors") and not page.get("jobComplete"):
                pass                             # transient job errors
            if not page.get("jobComplete"):
                self._sleep(poll_wait)
                poll_wait = min(5.0, poll_wait * 1.5)
                continue
            if page.get("schema"):
                schema = page["schema"]
            bytes_processed = int(page.get("totalBytesProcessed",
                                           bytes_processed) or 0)
            cache_hit = bool(page.get("cacheHit", cache_hit))
            total_rows = int(page.get("totalRows", total_rows) or 0)
            rows.extend(page.get("rows") or [])
            page_token = page.get("pageToken")
            if not page_token:
                break
        # the final statistics carry bytes billed (getQueryResults does not)
        billed = bytes_processed
        error_result = None
        try:
            job = self._call(
                "GET", f"/bigquery/v2/projects/{self.project}/jobs/"
                       f"{urllib.parse.quote(job_id)}",
                params={"location": location}, timeout=60,
                operation="jobs.get")
            qstats = (job.get("statistics") or {}).get("query") or {}
            billed = int(qstats.get("totalBytesBilled", billed) or 0)
            bytes_processed = int(qstats.get("totalBytesProcessed",
                                             bytes_processed) or 0)
            cache_hit = bool(qstats.get("cacheHit", cache_hit))
            error_result = (job.get("status") or {}).get("errorResult")
        except BQError:
            pass                                 # statistics are optional
        if error_result:
            raise self._job_error(error_result, job_id, operation)
        elapsed = time.monotonic() - t0
        self.ledger.add(LedgerEntry(
            job_id=job_id, scope=scope, operation=operation,
            bytes_processed=bytes_processed, bytes_billed=billed,
            cache_hit=cache_hit, elapsed_s=elapsed))
        return QueryResult(rows=decode_rows(schema, rows), schema=schema,
                           job_id=job_id, bytes_processed=bytes_processed,
                           bytes_billed=billed, cache_hit=cache_hit,
                           total_rows=total_rows or len(rows),
                           elapsed_s=elapsed)

    @staticmethod
    def _job_error(error_result: dict, job_id: str, operation: str
                   ) -> BQError:
        reason = error_result.get("reason", "") or ""
        message = error_result.get("message", "") or "job failed"
        return BQError(classify(0, reason, message), message,
                       reason=reason, job_id=job_id, operation=operation)

    # -- resources --
    def get_table(self, project: str, dataset: str, table: str) -> dict:
        return self._call(
            "GET", f"/bigquery/v2/projects/{project}/datasets/{dataset}/"
                   f"tables/{urllib.parse.quote(table)}",
            timeout=60, operation="tables.get")

    def get_dataset(self, project: str, dataset: str) -> dict:
        return self._call(
            "GET", f"/bigquery/v2/projects/{project}/datasets/{dataset}",
            timeout=60, operation="datasets.get")

    def list_row_access_policies(self, project: str, dataset: str,
                                 table: str) -> list[dict]:
        policies: list[dict] = []
        token: str | None = None
        while True:
            page = self._call(
                "GET", f"/bigquery/v2/projects/{project}/datasets/{dataset}/"
                       f"tables/{urllib.parse.quote(table)}/rowAccessPolicies",
                params={"pageToken": token, "pageSize": 100}, timeout=60,
                operation="rowAccessPolicies.list")
            policies.extend(page.get("rowAccessPolicies") or [])
            token = page.get("nextPageToken")
            if not token:
                return policies

    def test_iam_permissions(self, project: str, dataset: str, table: str,
                             permissions: list[str]) -> list[str]:
        payload = self._call(
            "POST", f"/bigquery/v2/projects/{project}/datasets/{dataset}/"
                    f"tables/{urllib.parse.quote(table)}:testIamPermissions",
            body={"permissions": permissions}, timeout=60,
            operation="tables.testIamPermissions")
        return list(payload.get("permissions") or [])
