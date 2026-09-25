"""The Spanner plane: connection, REST client, and a schema inspection.

The same shape as the BigQuery plane in ``sahs.util.auth``: the
process environment is read, never written; the route is pinned on
the connection (DIRECT by default, the private-endpoint contract;
``SPANNER_FORCE_PROXY=1`` rides the corporate proxy); TLS follows the
same knobs. Everything goes over Spanner's REST API through urllib,
so there is no gRPC stack to route and no SDK to configure.

Contract (the silo .env):

    SPANNER_PROJECT_ID       the project that owns the instance
    SPANNER_INSTANCE_ID      the instance
    SPANNER_DATABASE_ID      the database
    SPANNER_URL              the API endpoint; default the public one,
                             a private endpoint on the corporate network
    SYNAPSE_SPANNER_SA_KEY   the service-account key file
    SPANNER_EMULATOR_HOST    development only: host:port of the emulator
                             (its REST port, 9020, is used)

``inspect()`` is what ``scripts/spanner_check.py`` prints: the
database's state, its tables with columns and indexes, optional row
counts, and the diff against the designed DDL under ``db/spanner``.

This module became the package's ``__init__`` so that
``sahs/util/spanner/settings.py`` has a package: the same names,
importable as before; the settings live in the submodule.
"""

from __future__ import annotations

import json
import re
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.util.auth import (AuthError, _first_env, describe_route, env_proxies,
                   load_dotenv, plane_opener, resolve_ssl)

DEFAULT_ENDPOINT = "https://spanner.googleapis.com"
SCOPES = ("https://www.googleapis.com/auth/spanner.data",
          "https://www.googleapis.com/auth/spanner.admin")
_CREDENTIALS: dict[str, Any] = {}

# (method, url, headers, body) → (status, json body)
Transport = Callable[[str, str, dict[str, str], bytes | None],
                     tuple[int, dict[str, Any]]]


def spanner_proxies() -> dict[str, str]:
    import os
    if os.environ.get("SPANNER_FORCE_PROXY") == "1":
        return env_proxies()
    return {}


@dataclass(frozen=True)
class SpannerConnection:
    project: str
    instance: str
    database: str
    endpoint: str
    key_path: Path | None
    emulator: bool = False
    ssl_verify: bool = True
    ca_bundle: str | None = None
    proxies: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "SpannerConnection":
        load_dotenv()
        project = _first_env("SPANNER_PROJECT_ID")
        instance = _first_env("SPANNER_INSTANCE_ID")
        database = _first_env("SPANNER_DATABASE_ID")
        missing = [name for name, value in (
            ("SPANNER_PROJECT_ID", project), ("SPANNER_INSTANCE_ID", instance),
            ("SPANNER_DATABASE_ID", database)) if not value]
        if missing:
            raise AuthError("Spanner is not configured: set "
                            + ", ".join(missing) + " in the silo .env")
        emulator_host = _first_env("SPANNER_EMULATOR_HOST")
        url = _first_env("SPANNER_URL")
        if url:
            endpoint, emulator = url.rstrip("/"), False
        elif emulator_host:
            host = emulator_host.split(":", 1)[0]
            endpoint, emulator = f"http://{host}:9020", True
        else:
            endpoint, emulator = DEFAULT_ENDPOINT, False
        key: Path | None = None
        if not emulator:
            raw = _first_env("SYNAPSE_SPANNER_SA_KEY",
                             "GOOGLE_APPLICATION_CREDENTIALS")
            if not raw:
                raise AuthError("no Spanner SA key configured: set "
                                "SYNAPSE_SPANNER_SA_KEY to the key-file "
                                "path in the silo .env")
            key = Path(raw).expanduser()
            if not key.exists():
                raise AuthError(f"Spanner SA key not found on disk: {key}")
        verify, bundle = resolve_ssl()
        return cls(project=str(project), instance=str(instance),
                   database=str(database), endpoint=endpoint, key_path=key,
                   emulator=emulator, ssl_verify=verify, ca_bundle=bundle,
                   proxies=spanner_proxies())

    @property
    def database_path(self) -> str:
        return (f"projects/{self.project}/instances/{self.instance}"
                f"/databases/{self.database}")

    def route(self) -> str:
        return describe_route(self.proxies)

    def ssl_context(self) -> ssl.SSLContext:
        if not self.ssl_verify:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        return ssl.create_default_context(cafile=self.ca_bundle)

    def opener(self) -> urllib.request.OpenerDirector:
        return plane_opener(self.proxies, self.ssl_context())

    def token_session(self):
        import requests                          # ships with google-auth
        session = requests.Session()
        session.trust_env = False
        session.proxies = dict(self.proxies)
        session.verify = (self.ca_bundle or True) if self.ssl_verify \
            else False
        return session

    def token(self, *, make_credentials: Any = None,
              refresh: Any = None) -> str | None:
        """The OAuth access token, cached per key file; None on the
        emulator, which takes no auth."""
        if self.emulator or self.key_path is None:
            return None
        key = str(self.key_path)
        creds = _CREDENTIALS.get(key)
        if creds is None:
            if make_credentials is not None:
                creds = make_credentials()
            else:
                from google.oauth2 import service_account   # type: ignore
                creds = service_account.Credentials.from_service_account_file(
                    key, scopes=list(SCOPES))
            _CREDENTIALS[key] = creds
        if not getattr(creds, "valid", False):
            if refresh is not None:
                refresh(creds)
            else:
                from google.auth.transport.requests import Request  # type: ignore
                creds.refresh(Request(session=self.token_session()))
        return str(creds.token)


class SpannerError(RuntimeError):
    def __init__(self, status: int, message: str, path: str) -> None:
        super().__init__(f"{status} on {path}: {message}")
        self.status = status
        self.path = path


def urllib_transport(opener: urllib.request.OpenerDirector,
                     timeout: float = 60.0) -> Transport:
    def send(method: str, url: str, headers: dict[str, str],
             body: bytes | None) -> tuple[int, dict[str, Any]]:
        req = urllib.request.Request(url, data=body, method=method,
                                     headers=headers)
        try:
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw) if raw else {}
            except ValueError:
                payload = {"error": {"message": raw.decode(errors="replace")}}
            return e.code, payload
    return send


class SpannerClient:
    """Just enough of the REST API: database metadata, DDL, and SQL
    through a short-lived session."""

    def __init__(self, connection: SpannerConnection,
                 transport: Transport | None = None) -> None:
        self.connection = connection
        self._transport = transport or urllib_transport(connection.opener())
        self._token: str | None = None

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token is None:
            self._token = self.connection.token() or ""
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def call(self, method: str, path: str,
             body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.connection.endpoint}/v1/{path}"
        data = json.dumps(body).encode() if body is not None else None
        status, payload = self._transport(method, url, self._headers(), data)
        if status >= 300:
            message = (payload.get("error") or {}).get("message") \
                if isinstance(payload.get("error"), dict) \
                else str(payload.get("error") or payload)
            raise SpannerError(status, str(message), path)
        return payload

    # ── admin ────────────────────────────────────────────────
    def database(self) -> dict[str, Any]:
        return self.call("GET", self.connection.database_path)

    def ddl(self) -> list[str]:
        return list(self.call("GET", self.connection.database_path + "/ddl")
                    .get("statements") or [])

    # ── data ─────────────────────────────────────────────────
    def query(self, sql: str) -> tuple[list[str], list[list[Any]]]:
        """→ (column names, rows). Spanner returns INT64 as strings;
        they are left as returned."""
        session = self.call("POST", self.connection.database_path
                            + "/sessions", {})
        name = session["name"]
        try:
            result = self.call("POST", f"{name}:executeSql", {"sql": sql})
        finally:
            try:
                self.call("DELETE", name)
            except SpannerError:
                pass
        fields = ((result.get("metadata") or {}).get("rowType") or {}
                  ).get("fields") or []
        return [f.get("name", "") for f in fields], list(result.get("rows")
                                                         or [])


# ── the inspection ───────────────────────────────────────────
_TABLES_SQL = ("SELECT TABLE_NAME, PARENT_TABLE_NAME FROM "
               "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' "
               "AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
_COLUMNS_SQL = ("SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE FROM "
                "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' "
                "ORDER BY TABLE_NAME, ORDINAL_POSITION")
_INDEXES_SQL = ("SELECT TABLE_NAME, INDEX_NAME FROM "
                "INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = '' "
                "AND INDEX_TYPE != 'PRIMARY_KEY' ORDER BY TABLE_NAME, "
                "INDEX_NAME")

_CREATE_RE = re.compile(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?`?(\w+)`?",
                        re.I)
_COLUMN_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"(?P<type>ARRAY<[^>]+>|[A-Z]+(?:\([^)]*\))?)", re.M)


def designed_tables(ddl_dir: Path) -> dict[str, list[str]]:
    """table → columns, from the checked-in DDL files, in file order."""
    out: dict[str, list[str]] = {}
    for path in sorted(Path(ddl_dir).glob("*.sql")):
        text = "\n".join(line.split("--", 1)[0] for line in
                         path.read_text(encoding="utf-8").splitlines())
        for stmt in text.split(";"):
            m = _CREATE_RE.search(stmt)
            if not m:
                continue
            body = stmt[m.end():]
            body = body[body.find("(") + 1:] if "(" in body else ""
            depth, cols = 1, []
            for ch in body:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
                cols.append(ch)
            names = []
            for part in "".join(cols).split(","):
                part = part.strip()
                if not part or part.upper().startswith("CONSTRAINT"):
                    continue
                cm = _COLUMN_RE.match(part)
                if cm:
                    names.append(cm.group("name"))
            out[m.group(1)] = names
    return out


def inspect(client: SpannerClient, *, ddl_dir: Path | None = None,
            counts: bool = False) -> dict[str, Any]:
    """The report: what is there, and how it compares to the design."""
    conn = client.connection
    report: dict[str, Any] = {
        "config": {"project": conn.project, "instance": conn.instance,
                   "database": conn.database, "endpoint": conn.endpoint,
                   "route": conn.route(), "emulator": conn.emulator,
                   "key": str(conn.key_path) if conn.key_path else ""},
        "database": {}, "ddl_statements": None, "tables": [],
        "designed": {}, "column_drift": {}, "errors": [],
    }
    try:
        db = client.database()
        report["database"] = {k: db.get(k) for k in
                              ("state", "databaseDialect", "createTime")}
    except SpannerError as e:
        report["errors"].append(f"database: {e}")
    try:
        report["ddl_statements"] = len(client.ddl())
    except SpannerError as e:
        report["errors"].append(f"ddl: {e}")

    _, table_rows = client.query(_TABLES_SQL)
    _, column_rows = client.query(_COLUMNS_SQL)
    try:
        _, index_rows = client.query(_INDEXES_SQL)
    except SpannerError as e:
        index_rows = []
        report["errors"].append(f"indexes: {e}")
    columns: dict[str, list[list[str]]] = {}
    for table, column, kind in column_rows:
        columns.setdefault(table, []).append([column, kind])
    indexes: dict[str, list[str]] = {}
    for table, index in index_rows:
        indexes.setdefault(table, []).append(index)
    for name, parent in table_rows:
        entry: dict[str, Any] = {"name": name, "parent": parent,
                                 "columns": columns.get(name, []),
                                 "indexes": indexes.get(name, []),
                                 "rows": None}
        if counts:
            try:
                _, rows = client.query(f"SELECT COUNT(*) FROM `{name}`")
                entry["rows"] = int(rows[0][0]) if rows else 0
            except SpannerError as e:
                report["errors"].append(f"count {name}: {e}")
        report["tables"].append(entry)

    if ddl_dir is not None:
        designed = designed_tables(ddl_dir)
        live = {t["name"]: [c[0] for c in t["columns"]]
                for t in report["tables"]}
        report["designed"] = {
            "present": sorted(t for t in designed if t in live),
            "missing": sorted(t for t in designed if t not in live),
            "undesigned": sorted(t for t in live if t not in designed)}
        for table in report["designed"]["present"]:
            want, have = set(designed[table]), set(live[table])
            if want != have:
                report["column_drift"][table] = {
                    "missing": sorted(want - have),
                    "extra": sorted(have - want)}
    return report


def format_report(report: dict[str, Any]) -> str:
    cfg = report["config"]
    lines = ["resolved configuration:",
             f"  project    {cfg['project']}",
             f"  instance   {cfg['instance']}",
             f"  database   {cfg['database']}",
             f"  endpoint   {cfg['endpoint']}"
             + ("   (emulator)" if cfg["emulator"] else ""),
             f"  route      {cfg['route']}",
             f"  key        {'present' if cfg['key'] else 'none'}"]
    db = report["database"]
    if db:
        lines.append(f"database: {db.get('state')} · "
                     f"{db.get('databaseDialect')} · created "
                     f"{db.get('createTime')}")
    if report["ddl_statements"] is not None:
        lines.append(f"ddl: {report['ddl_statements']} statements")
    lines.append(f"tables: {len(report['tables'])}")
    for t in report["tables"]:
        rows = "" if t["rows"] is None else f"  rows={t['rows']}"
        parent = f"  (in {t['parent']})" if t["parent"] else ""
        lines.append(f"  {t['name']:28} {len(t['columns']):>3} cols  "
                     f"{len(t['indexes']):>2} idx{rows}{parent}")
    designed = report["designed"]
    if designed:
        lines.append("against db/spanner (the design):")
        lines.append(f"  present     {len(designed['present'])}")
        lines.append(f"  missing     {len(designed['missing'])}"
                     + (": " + ", ".join(designed["missing"])
                        if designed["missing"] else ""))
        lines.append(f"  undesigned  {len(designed['undesigned'])}"
                     + (": " + ", ".join(designed["undesigned"])
                        if designed["undesigned"] else ""))
        for table, drift in report["column_drift"].items():
            lines.append(f"  drift {table}: missing {drift['missing']} "
                         f"extra {drift['extra']}")
    for err in report["errors"]:
        lines.append(f"  ! {err}")
    return "\n".join(lines)


__all__ = ["DEFAULT_ENDPOINT", "SCOPES", "SpannerClient",
           "SpannerConnection", "SpannerError", "designed_tables",
           "format_report", "inspect", "urllib_transport"]
