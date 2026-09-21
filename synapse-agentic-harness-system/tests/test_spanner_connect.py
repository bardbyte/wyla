"""The Spanner plane: the .env contract, the pinned route, the REST
client, and the schema inspection — on a fake transport, no network."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import AuthError                       # noqa: E402
from sahs.util.spanner import (DEFAULT_ENDPOINT, SpannerClient,   # noqa: E402
                               SpannerConnection, SpannerError,
                               designed_tables, format_report, inspect)

_VARS = ("SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID",
         "SPANNER_URL", "SYNAPSE_SPANNER_SA_KEY",
         "GOOGLE_APPLICATION_CREDENTIALS", "SPANNER_EMULATOR_HOST",
         "SPANNER_FORCE_PROXY", "HTTPS_PROXY", "https_proxy", "NO_PROXY",
         "BQ_SSL_NO_VERIFY", "SAHS_ENV_FILE")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for name in _VARS:
        monkeypatch.delenv(name, raising=False)
    empty = tmp_path / "empty.env"
    empty.write_text("")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty))


def _configure(monkeypatch, tmp_path, **extra):
    key = tmp_path / "sa.json"
    key.write_text("{}")
    monkeypatch.setenv("SPANNER_PROJECT_ID", "proj")
    monkeypatch.setenv("SPANNER_INSTANCE_ID", "inst")
    monkeypatch.setenv("SPANNER_DATABASE_ID", "db")
    monkeypatch.setenv("SYNAPSE_SPANNER_SA_KEY", str(key))
    for k, v in extra.items():
        monkeypatch.setenv(k, v)


def test_contract_names_every_missing_variable():
    with pytest.raises(AuthError) as e:
        SpannerConnection.from_env()
    assert "SPANNER_PROJECT_ID" in str(e.value)
    assert "SPANNER_DATABASE_ID" in str(e.value)


def test_missing_key_is_a_typed_error(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("SYNAPSE_SPANNER_SA_KEY", str(tmp_path / "nope.json"))
    with pytest.raises(AuthError, match="not found on disk"):
        SpannerConnection.from_env()


def test_endpoint_defaults_and_private_url_wins(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    c = SpannerConnection.from_env()
    assert c.endpoint == DEFAULT_ENDPOINT
    assert c.database_path == "projects/proj/instances/inst/databases/db"
    monkeypatch.setenv("SPANNER_URL", "https://spanner-private.example/")
    c = SpannerConnection.from_env()
    assert c.endpoint == "https://spanner-private.example"


def test_route_is_direct_unless_forced(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path, HTTPS_PROXY="http://proxy:3128")
    assert SpannerConnection.from_env().route() == "direct"
    monkeypatch.setenv("SPANNER_FORCE_PROXY", "1")
    assert "proxy:3128" in SpannerConnection.from_env().route()


def test_emulator_needs_no_key_and_uses_the_rest_port(monkeypatch):
    monkeypatch.setenv("SPANNER_PROJECT_ID", "proj")
    monkeypatch.setenv("SPANNER_INSTANCE_ID", "inst")
    monkeypatch.setenv("SPANNER_DATABASE_ID", "db")
    monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
    c = SpannerConnection.from_env()
    assert c.emulator and c.key_path is None
    assert c.endpoint == "http://localhost:9020"
    assert c.token() is None


# ── the REST client on a fake transport ──────────────────────
def _rows(*rows):
    return {"metadata": {"rowType": {"fields": [
        {"name": f"c{i}"} for i in range(len(rows[0]))]}} if rows
        else {"metadata": {}}, "rows": [list(r) for r in rows]}


class _Fake:
    """Canned responses keyed on (method, url suffix); records calls."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, headers,
                           json.loads(body) if body else None))
        for (m, suffix), payload in self.responses.items():
            if m == method and url.endswith(suffix):
                status = 200
                if isinstance(payload, tuple):
                    status, payload = payload
                return status, payload
        raise AssertionError(f"unexpected {method} {url}")


def _client(monkeypatch, tmp_path, responses):
    _configure(monkeypatch, tmp_path, SPANNER_URL="https://sp.example")
    conn = SpannerConnection.from_env()
    client = SpannerClient(conn, transport=_Fake(responses))
    client._token = "tok"                       # never mint a real one
    return client


def test_query_runs_in_a_short_lived_session(monkeypatch, tmp_path):
    fake = {("POST", "/databases/db/sessions"): {"name": "projects/proj/"
            "instances/inst/databases/db/sessions/s1"},
            ("POST", "/sessions/s1:executeSql"): _rows(["1"]),
            ("DELETE", "/sessions/s1"): {}}
    client = _client(monkeypatch, tmp_path, fake)
    columns, rows = client.query("SELECT 1")
    assert columns == ["c0"] and rows == [["1"]]
    methods = [(m, u.rsplit("/", 1)[-1]) for m, u, _h, _b in
               client._transport.calls]
    assert methods == [("POST", "sessions"), ("POST", "s1:executeSql"),
                       ("DELETE", "s1")]
    assert client._transport.calls[0][2]["Authorization"] == "Bearer tok"
    assert client._transport.calls[1][3] == {"sql": "SELECT 1"}


def test_errors_carry_status_and_path(monkeypatch, tmp_path):
    fake = {("GET", "/databases/db"): (403, {"error": {"message":
                                              "permission denied"}})}
    client = _client(monkeypatch, tmp_path, fake)
    with pytest.raises(SpannerError, match="403 .*permission denied") as e:
        client.database()
    assert e.value.status == 403


def test_inspect_reports_tables_and_the_diff_against_the_design(
        monkeypatch, tmp_path):
    ddl = tmp_path / "ddl"
    ddl.mkdir()
    (ddl / "001_a.sql").write_text(
        "-- the design\n"
        "CREATE TABLE Users (\n  UserId STRING(36) NOT NULL,\n"
        "  Email STRING(320) NOT NULL,\n  CONSTRAINT x CHECK (1=1)\n"
        ") PRIMARY KEY (UserId);\n"
        "CREATE TABLE ChatSessions (\n  SessionId STRING(36) NOT NULL,\n"
        "  OwnerId STRING(36) NOT NULL\n) PRIMARY KEY (SessionId);\n")
    assert designed_tables(ddl) == {"Users": ["UserId", "Email"],
                                    "ChatSessions": ["SessionId", "OwnerId"]}
    session = "projects/proj/instances/inst/databases/db/sessions/s1"
    fake = {
        ("GET", "/databases/db"): {"state": "READY",
                                   "databaseDialect": "GOOGLE_STANDARD_SQL",
                                   "createTime": "2026-01-01T00:00:00Z"},
        ("GET", "/databases/db/ddl"): {"statements": ["CREATE TABLE Users"]},
        ("POST", "/databases/db/sessions"): {"name": session},
        ("DELETE", "/sessions/s1"): {},
    }
    answers = iter([
        _rows(["Users", None], ["Legacy", None]),                # tables
        _rows(["Users", "UserId", "STRING(36)"],                  # columns
              ["Users", "Nick", "STRING(64)"],
              ["Legacy", "Id", "INT64"]),
        _rows(["Users", "UsersByEmail"]),                          # indexes
        _rows(["2"]), _rows(["0"]),                                 # counts
    ])
    fake[("POST", "/sessions/s1:executeSql")] = None
    client = _client(monkeypatch, tmp_path, fake)
    transport = client._transport

    def send(method, url, headers, body):
        if url.endswith(":executeSql"):
            return 200, next(answers)
        return transport(method, url, headers, body)
    client._transport = send

    report = inspect(client, ddl_dir=ddl, counts=True)
    assert report["database"]["state"] == "READY"
    assert report["ddl_statements"] == 1
    assert [t["name"] for t in report["tables"]] == ["Users", "Legacy"]
    users = report["tables"][0]
    assert users["columns"] == [["UserId", "STRING(36)"],
                                ["Nick", "STRING(64)"]]
    assert users["indexes"] == ["UsersByEmail"] and users["rows"] == 2
    assert report["designed"] == {"present": ["Users"],
                                  "missing": ["ChatSessions"],
                                  "undesigned": ["Legacy"]}
    assert report["column_drift"] == {"Users": {"missing": ["Email"],
                                                "extra": ["Nick"]}}
    text = format_report(report)
    assert "missing     1: ChatSessions" in text
    assert "drift Users" in text and "key        present" in text
    assert not report["errors"]


def test_designed_tables_reads_the_checked_in_ddl():
    designed = designed_tables(SILO / "db" / "spanner")
    assert {"Users", "ChatSessions", "ChatEvents", "Builds",
            "GraphStatusTransitions"} <= set(designed)
    assert "UserId" in designed["Users"]
