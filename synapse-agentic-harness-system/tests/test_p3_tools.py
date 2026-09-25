"""P3 gate: the eight serving tools.

Pins: the ≥12-violation validate_sql catalog with clean twins; E3
sandbox fail-closed (ACL verdict BEFORE any execution object — proven
with a booby-trapped substrate); E5 floor semantics (curated floor
1.000, excluded count printed, abstention floor hard); E6 resolver
feature traces + constants_version; primary-identity binding for fused
metrics; the MCP envelope contract; TTL cache; p50 latency.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.evals.harness import run_suite                  # noqa: E402
from sahs.evals.schema import read_tasks                  # noqa: E402
from sahs.evals.substrate import DryRunOutcome, StaticSubstrate  # noqa: E402
from sahs.tools.api import Build                          # noqa: E402
from sahs.tools.constants import RESOLVER_CONSTANTS       # noqa: E402
from sahs.tools.mcp_server import (                       # noqa: E402
    TTLCache,
    build_handlers,
)
from sahs.tools.resolver import resolve, resolver_sut     # noqa: E402
from sahs.tools.sandbox import _cap_limit, execute_sandboxed  # noqa: E402
from sahs.tools.validate_sql import validate_sql          # noqa: E402

FX = SILO / "tests" / "fixtures"
CURATED = SILO / "tests" / "tasks" / "curated" / "curated.jsonl"

GMS = "dw.gms_transaction"
WWCAS = "dw.wwcas_authorization"
DATED = "WHERE part_dt = '2026-01-01'"


@pytest.fixture(scope="module")
def build(tmp_path_factory) -> Build:
    """One compiled fixture build for the whole module."""
    tmp = tmp_path_factory.mktemp("p3")
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"), "build-graph",
         "--graph", str(tmp / "graph"),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "p3_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    # through the CLI on purpose: every pipeline.py subcommand must run
    # end-to-end on fixtures in CI (the runbook-drift guard)
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"), "compile",
         "--graph", str(tmp / "graph"), "--builds", str(tmp / "builds"),
         "--out", str(tmp / "run_compile"), "--plain"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    return Build.open(tmp / "builds")


@pytest.fixture()
def mini() -> Build:
    """Hand-rolled build for surgical metric-contract cases."""
    return Build(
        root=Path("."), manifest={"build_id": "b_mini"},
        metrics=[{
            "id": "metric:aaa111aaa111", "fp": "aaa111aaa111",
            "mgroup": "mgroup:dmp:900", "mgroups": ["mgroup:dmp:900"],
            "label": "Net Spend", "table": GMS, "grain": "transaction",
            "status": "certified", "source": "metrics_dmp",
            "authority": 5, "support": 3, "question": "",
            "canonical_sql": "sum(trans_usd_am)",
            "approved_dimensions": ["region", "time_period"],
            "sign_convention": ""}],
        bindings=[], vocab=[], joins=[],
        acl={GMS: {"restricted": None, "pii_columns": ["cm13"]},
             WWCAS: {"restricted": "unknown_policy",
                     "pii_columns": ["card_no"]}},
        schema={GMS: {"trans_usd_am": "FLOAT64", "country_cd": "STRING",
                      "part_dt": "DATE", "cm13": "STRING",
                      "se_cr_dr_in": "STRING"},
                WWCAS: {"approval_cd": "STRING", "card_no": "STRING",
                        "part_dt": "DATE", "trans_usd_am": "FLOAT64"}})


def _codes(report: dict) -> set[str]:
    return {v["code"] for v in report["violations"]}


def _warning_codes(report: dict) -> set[str]:
    return {w["code"] for w in report["warnings"]}


# ── validate_sql: the 12-violation catalog, each with a clean twin ──

def test_violations_parse_and_statement_class(build):
    assert "parse_error" in _codes(
        validate_sql(build, "SELECT sum( FROM x"))                    # 1
    assert "not_a_select" in _codes(
        validate_sql(build, "trans_usd_am > 100"))                    # 2
    assert "statement_not_allowed" in _codes(
        validate_sql(build, f"DROP TABLE {GMS}"))                     # 3
    assert "statement_not_allowed" in _codes(
        validate_sql(build, f"DELETE FROM {GMS} WHERE 1=1"))
    clean = validate_sql(build,
                         f"SELECT country_cd FROM {GMS} {DATED}")
    assert clean["ok"], clean["violations"]


def test_violations_tables_and_columns(build):
    assert "unknown_table" in _codes(
        validate_sql(build, "SELECT 1 FROM dw.no_such_table"))        # 4
    report = validate_sql(build, f"SELECT wrong_col FROM {GMS}")
    assert "unknown_column" in _codes(report)                         # 5
    hint = next(v for v in report["violations"]
                if v["code"] == "unknown_column")["hint"]
    assert "describe_table" in hint          # errors teach the next call
    joined = (f"SELECT part_dt FROM {GMS} t JOIN {WWCAS} w "
              "ON t.part_dt = w.part_dt")
    assert "ambiguous_column" in _codes(validate_sql(build, joined))  # 6
    twin = (f"SELECT t.part_dt FROM {GMS} t JOIN {WWCAS} w "
            "ON t.part_dt = w.part_dt")
    assert "ambiguous_column" not in _codes(validate_sql(build, twin))
    assert validate_sql(build, twin)["ok"]


def test_violations_sensitivity_and_star(build):
    DENY = {"SAHS_SENSITIVE_COLUMNS": "deny"}
    assert "sensitive_column" in _codes(
        validate_sql(build, f"SELECT cm13 FROM {GMS} {DATED}",
                     env=DENY))                                       # 7
    assert "select_star_over_sensitive" in _codes(
        validate_sql(build, f"SELECT * FROM {WWCAS} {DATED}",
                     env=DENY))                                       # 8
    twin = validate_sql(
        build, f"SELECT approval_cd, part_dt FROM {WWCAS} {DATED}",
        env=DENY)
    assert twin["ok"], twin["violations"]
    # E3 surfaces here as a WARNING; the sandbox is where it denies
    assert "policy_unknown" in _warning_codes(twin)
    filt = validate_sql(
        build, f"SELECT country_cd FROM {GMS} WHERE cm13 = 'x'",
        env=DENY)
    assert filt["ok"]
    assert "sensitive_column_in_filter" in _warning_codes(filt)


def test_sensitive_columns_allow_by_default_and_deny_on_request(
        build, monkeypatch):
    """SAHS_SENSITIVE_COLUMNS: the same query is refused under deny and
    passes under allow with the SAME code as a note — the record still
    says a sensitive column was read. allow is the default: unset, or
    any word but deny."""
    from sahs.tools.validate_sql import (SENSITIVE_SWITCH, sensitive_policy,
                                         sensitive_policy_note)
    column_sql = f"SELECT cm13 FROM {GMS} {DATED}"
    star_sql = f"SELECT * FROM {WWCAS} {DATED}"
    # the default is allow, read from the process environment
    monkeypatch.delenv(SENSITIVE_SWITCH, raising=False)
    assert sensitive_policy() == "allow"
    assert sensitive_policy({}) == "allow"
    assert sensitive_policy({SENSITIVE_SWITCH: "Allow"}) == "allow"
    assert sensitive_policy({SENSITIVE_SWITCH: "DENY"}) == "deny"
    assert sensitive_policy({SENSITIVE_SWITCH: "no"}) == "allow"    # only deny denies
    assert "unset" in sensitive_policy_note({})
    assert "deny" in sensitive_policy_note({SENSITIVE_SWITCH: "deny"})
    assert "reads as allow" in sensitive_policy_note({SENSITIVE_SWITCH: "no"})

    for sql, code in ((column_sql, "sensitive_column"),
                      (star_sql, "select_star_over_sensitive")):
        refused = validate_sql(build, sql, env={SENSITIVE_SWITCH: "deny"})
        assert not refused["ok"] and code in _codes(refused)
        assert code not in _warning_codes(refused)
        denied = next(v for v in refused["violations"] if v["code"] == code)
        assert denied["policy"] == "deny"
        # the same query under allow: not refused, the note carries the
        # same code, the column's name and the policy that allowed it
        allowed = validate_sql(build, sql, env={SENSITIVE_SWITCH: "allow"})
        assert allowed["ok"], allowed["violations"]
        assert code not in _codes(allowed)
        assert code in _warning_codes(allowed)
        note = next(w for w in allowed["warnings"] if w["code"] == code)
        assert note["policy"] == "allow"
        assert f"{SENSITIVE_SWITCH}=allow" in note["detail"]
        assert "record" in note["detail"]
        assert "cm13" in note["detail"] or "sensitive" in note["detail"]
        # unset in the environment is allow too
        default = validate_sql(build, sql)
        assert default["ok"] and code in _warning_codes(default)
    # the other checks are untouched by the policy: a real violation
    # still refuses under allow
    still = validate_sql(build, f"SELECT wrong_col FROM {GMS}",
                         env={SENSITIVE_SWITCH: "allow"})
    assert not still["ok"] and "unknown_column" in _codes(still)


def test_violation_cross_join(build):
    report = validate_sql(
        build, f"SELECT count(*) FROM {GMS}, {WWCAS}")
    assert "cross_join_unconstrained" in _codes(report)               # 9
    twin = (f"SELECT count(*) FROM {GMS} t JOIN {WWCAS} w "
            "ON t.part_dt = w.part_dt")
    assert "cross_join_unconstrained" not in _codes(
        validate_sql(build, twin))


def test_violations_metric_contract(mini):
    ok_sql = f"SELECT sum(trans_usd_am) FROM {GMS} {DATED}"
    assert "unknown_metric" in _codes(
        validate_sql(mini, ok_sql, metric_id="metric:nope"))          # 10
    off = validate_sql(mini, f"SELECT count(*) FROM {GMS} {DATED}",
                       metric_id="mgroup:dmp:900")
    assert "metric_expression_missing" in _codes(off)                 # 11
    grouped = (f"SELECT country_cd, sum(trans_usd_am) FROM {GMS} "
               f"{DATED} GROUP BY country_cd")
    dims = validate_sql(mini, grouped, metric_id="mgroup:dmp:900")
    assert "dim_not_approved" in _codes(dims)                         # 12
    twin = (f"SELECT part_dt, sum(trans_usd_am) FROM {GMS} "
            f"{DATED} GROUP BY part_dt")
    ok = validate_sql(mini, twin, metric_id="mgroup:dmp:900")
    assert ok["ok"], ok["violations"]        # time dim conforms
    contained = validate_sql(mini, ok_sql, metric_id="mgroup:dmp:900")
    assert contained["ok"], contained["violations"]


def test_full_scan_warning(build):
    report = validate_sql(build, f"SELECT country_cd FROM {GMS}")
    assert report["ok"]
    assert "no_where_filter" in _warning_codes(report)


# ── sandbox (E3): fail-closed, and the order IS the property ──

class _BoobyTrap:
    """A substrate that must never be touched."""

    name = "boobytrap"

    def dry_run(self, sql):
        raise AssertionError("execution object used before ACL verdict")


def test_sandbox_denies_live_on_unknown_policy_before_any_execution(
        build, tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    out = execute_sandboxed(
        build, f"SELECT approval_cd FROM {WWCAS} {DATED}", mode="live",
        substrate=_BoobyTrap(), ledger_path=ledger,
        env={"SAHS_ALLOW_LIVE": "1"})       # even explicitly enabled
    assert out["status"] == "denied"
    assert "policy_unknown" in out["error"]
    assert out["meta"]["policy_unknown"] is True
    entry = json.loads(ledger.read_text().splitlines()[-1])
    assert entry["decision"] == "denied"
    assert WWCAS in entry["tables"]


def test_sandbox_snapshot_permitted_with_disclosure(build, tmp_path):
    substrate = StaticSubstrate({})
    out = execute_sandboxed(
        build, f"SELECT approval_cd FROM {WWCAS} {DATED}",
        mode="snapshot", substrate=substrate,
        ledger_path=tmp_path / "ledger.jsonl")
    assert out["status"] == "ok"
    assert out["meta"]["policy_unknown"] is True
    assert out["data"]["rows"] is None       # dry-run never moves rows


class _Recorder:
    """A substrate that remembers exactly what it was asked to run."""

    def __init__(self):
        self.seen = []

    def dry_run(self, sql):
        self.seen.append(sql)
        return DryRunOutcome(valid=True, result_schema=None)


def test_sandbox_qualifies_tables_with_the_data_project(build, tmp_path):
    sql = f"SELECT approval_cd FROM {WWCAS} {DATED}"
    rec = _Recorder()
    out = execute_sandboxed(build, sql, mode="snapshot", substrate=rec,
                            ledger_path=tmp_path / "l.jsonl",
                            env={"BQ_DATA_PROJECT": "demo-warehouse"})
    assert out["status"] == "ok"
    assert "`demo-warehouse`.dw.wwcas_authorization" in rec.seen[0]
    assert out["meta"]["sql_sent"] == rec.seen[0]
    assert out["meta"]["qualified"] == [
        {"from": WWCAS, "to": f"demo-warehouse.{WWCAS}"}]
    assert out["meta"]["tables"] == [WWCAS]      # the graph's name stays
    # a query already written the warehouse's way resolves and passes
    rec = _Recorder()
    out = execute_sandboxed(
        build, f"SELECT approval_cd FROM `demo-warehouse`.{WWCAS} {DATED}",
        mode="snapshot", substrate=rec, ledger_path=tmp_path / "l.jsonl",
        env={"BQ_DATA_PROJECT": "demo-warehouse"})
    assert out["status"] == "ok" and "sql_sent" not in out["meta"]
    assert out["meta"]["tables"] == [WWCAS]
    # the connection's data project is the default source of truth
    class _Conn:
        data_project = "demo-warehouse"
    rec = _Recorder()
    rec.connection = _Conn()
    out = execute_sandboxed(build, sql, mode="snapshot", substrate=rec,
                            ledger_path=tmp_path / "l.jsonl", env={})
    assert "`demo-warehouse`.dw.wwcas_authorization" in rec.seen[0]
    # no data project anywhere: the SQL travels untouched
    rec = _Recorder()
    execute_sandboxed(build, sql, mode="snapshot", substrate=rec,
                      ledger_path=tmp_path / "l.jsonl", env={})
    assert rec.seen[0] == sql


class _Refuses:
    def __init__(self, message):
        self.message = message

    def dry_run(self, sql):
        return DryRunOutcome(valid=False, error=self.message)


def test_sandbox_teaches_a_failed_dry_run(build, tmp_path):
    sql = f"SELECT approval_cd FROM {WWCAS} {DATED}"
    out = execute_sandboxed(
        build, sql, mode="snapshot",
        substrate=_Refuses("Unrecognized name: approval_cdx at [1:8]"),
        ledger_path=tmp_path / "l.jsonl", env={})
    assert out["status"] == "error" and "invalid_sql" in out["error"]
    taught = out["meta"]["taught"]
    assert taught["kind"] == "sql" and taught["yours_to_fix"] is True
    assert "approval_cd" in taught["closest"]
    out = execute_sandboxed(
        build, sql, mode="snapshot",
        substrate=_Refuses("Not found: Table demo-billing:"
                           "dw.wwcas_authorization was not found in "
                           "location US"),
        ledger_path=tmp_path / "l.jsonl",
        env={"BQ_DATA_PROJECT": "", "BQ_LOCATION": "US"})
    taught = out["meta"]["taught"]
    assert taught["kind"] == "environment"
    assert taught["yours_to_fix"] is False
    assert "SYNAPSE_BQ_DATA_PROJECT" in taught["hint"]
    # a live execution that blows up is taught the same way
    class _Explodes:
        connection = None

        def run(self, sql, limit):
            raise RuntimeError("transport: Connection reset by peer")

    out = execute_sandboxed(build, f"SELECT country_cd FROM {GMS} {DATED}",
                            mode="live", substrate=StaticSubstrate({}),
                            runner=_Explodes(),
                            ledger_path=tmp_path / "l.jsonl",
                            env={"SAHS_ALLOW_LIVE": "1"})
    assert out["status"] == "error" and "execution_failed" in out["error"]
    assert out["meta"]["taught"]["kind"] == "environment"
    # the gates say whose fix it is, too
    out = execute_sandboxed(build, f"SELECT country_cd FROM {GMS} {DATED}",
                            mode="live", substrate=StaticSubstrate({}),
                            ledger_path=tmp_path / "l.jsonl", env={})
    assert out["meta"]["taught"]["kind"] == "access"


def test_sandbox_live_default_deny_and_cost_gate(build, tmp_path):
    sql = f"SELECT country_cd FROM {GMS} {DATED}"
    out = execute_sandboxed(build, sql, mode="live",
                            substrate=StaticSubstrate({}),
                            ledger_path=tmp_path / "l.jsonl", env={})
    assert out["status"] == "denied"
    assert "live_disabled" in out["error"]
    from sahs.canon.canonical import try_canon
    fp = try_canon(sql)[0].fp_expr
    pricey = StaticSubstrate({fp: DryRunOutcome(
        valid=True, bytes_processed=10**12)})
    out = execute_sandboxed(build, sql, mode="live", substrate=pricey,
                            ledger_path=tmp_path / "l.jsonl",
                            env={"SAHS_ALLOW_LIVE": "1"})
    assert out["status"] == "denied"
    assert "cost_gate" in out["error"]


def test_sandbox_live_allowed_path_row_cap_and_ledger(build, tmp_path):
    class _Runner:
        name = "stub"

        def __init__(self):
            self.seen_sql = ""

        def run(self, sql, limit):
            self.seen_sql = sql
            return {"rows": [["US", "1.0"]], "schema": [
                {"name": "country_cd", "type": "STRING"}],
                "bytes_processed": 1234}

    runner = _Runner()
    ledger = tmp_path / "ledger.jsonl"
    out = execute_sandboxed(
        build, f"SELECT country_cd FROM {GMS} {DATED}", mode="live",
        limit=50, substrate=StaticSubstrate({}), runner=runner,
        ledger_path=ledger, env={"SAHS_ALLOW_LIVE": "1"})
    assert out["status"] == "ok"
    assert out["data"]["row_count"] == 1
    assert "LIMIT 50" in runner.seen_sql     # row cap injected via AST
    assert json.loads(ledger.read_text().splitlines()[-1])[
        "decision"] == "ok"


def test_sandbox_live_as_the_person_dry_runs_on_the_runners_connection(
        build, tmp_path, monkeypatch):
    """BigQuery as the person (SAHS_BQ_AUTH_MODE=user): the runner
    brings a token provider and no service-account key. The dry run
    the sandbox makes first must ride the runner's connection and
    token, never demand a key; and a token provider that has no
    connection to mint from is the google_oauth_required refusal, by
    name, with the hint — from the dry run and from the run alike."""
    import json as _json
    from sahs.tools.sandbox import BQJobRunner
    from sahs.util.google_auth.oauth import GoogleOAuthTokenError
    from sahs.util.auth import BQConnection
    for name in ("GOOGLE_APPLICATION_CREDENTIALS", "SYNAPSE_BQ_SA_KEY",
                 "SAHS_SECRETS_DIR", "BQ_DATA_PROJECT",
                 "SYNAPSE_BQ_DATA_PROJECT", "SAHS_LIVE_MAX_BYTES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SAHS_BQ_AUTH_MODE", "user")
    monkeypatch.setenv("BQ_PROJECT_ID", "person-project")
    monkeypatch.setenv("BIGQUERY_API_BASE_URL", "https://bigquery.test")

    class _Response:
        def __init__(self, payload):
            self._body = _json.dumps(payload).encode()

        def read(self):
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    class _Transport:
        """BQConnection.opener(): records the bearer on every trip."""
        calls: list[dict] = []

        def open(self, request, timeout=None):
            body = _json.loads(request.data.decode())
            self.calls.append({"url": request.full_url, "body": body,
                               "bearer": request.get_header("Authorization")})
            if request.full_url.endswith("/jobs"):
                return _Response({"statistics": {"query": {
                    "totalBytesProcessed": "10",
                    "schema": {"fields": [{"name": "country_cd",
                                           "type": "STRING"}]}}}})
            return _Response({"schema": {"fields": [
                {"name": "country_cd", "type": "STRING"}]},
                "rows": [{"f": [{"v": "CA"}]}], "totalBytesProcessed": "10"})

    monkeypatch.setattr(BQConnection, "opener", lambda self: _Transport())
    sql = f"SELECT country_cd FROM {GMS} {DATED}"
    env = {"SAHS_ALLOW_LIVE": "1"}

    # the person's token rides the dry run and the run
    runner = BQJobRunner(token_provider=lambda: "ya29.the-person")
    assert runner.connection.key_path is None
    out = execute_sandboxed(build, sql, mode="live", runner=runner,
                            ledger_path=tmp_path / "l.jsonl", env=env)
    assert out["status"] == "ok", out
    assert out["data"]["rows"] == [["CA"]]
    assert [c["url"].rsplit("/", 1)[1] for c in _Transport.calls] == ["jobs", "queries"]
    assert {c["bearer"] for c in _Transport.calls} == {"Bearer ya29.the-person"}
    assert all("person-project" in c["url"] for c in _Transport.calls)

    # no connection to mint from: refused by name before any trip
    _Transport.calls.clear()

    def no_connection():
        raise GoogleOAuthTokenError("connect Google BigQuery before running live queries")

    out = execute_sandboxed(build, sql, mode="live",
                            runner=BQJobRunner(token_provider=no_connection),
                            ledger_path=tmp_path / "l.jsonl", env=env)
    assert out["status"] == "denied"
    assert out["error"].startswith("google_oauth_required: connect Google")
    assert out["meta"]["taught"]["kind"] == "access"
    assert "connect Google BigQuery" in out["meta"]["taught"]["hint"]
    assert _Transport.calls == []
    # the same refusal when the provider fails only at run time
    class _RefusesAtRun:
        name = "refuses"
        connection = runner.connection
        token_provider = lambda self: "ya29.the-person"  # noqa: E731

        def run(self, sql, limit):
            raise GoogleOAuthTokenError("Google token refresh failed; reconnect Google BigQuery")

    out = execute_sandboxed(build, sql, mode="live", runner=_RefusesAtRun(),
                            ledger_path=tmp_path / "l.jsonl", env=env)
    assert out["status"] == "denied" and "google_oauth_required" in out["error"]
    assert "reconnect" in out["error"]
    # and with no runner at all, the refusal the app relies on
    out = execute_sandboxed(build, sql, mode="live",
                            substrate=StaticSubstrate({}),
                            ledger_path=tmp_path / "l.jsonl", env=env)
    assert out["status"] == "denied" and "google_oauth_required" in out["error"]


def test_sandbox_refuses_ddl(build, tmp_path):
    out = execute_sandboxed(build, f"DROP TABLE {GMS}", mode="snapshot",
                            substrate=_BoobyTrap(),
                            ledger_path=tmp_path / "l.jsonl")
    assert out["status"] in ("denied", "error")
    assert "statement_not_allowed" in (out["error"] or "") \
        or "parse_error" in (out["error"] or "")


def test_cap_limit_tightens_never_loosens():
    sql = f"SELECT country_cd FROM {GMS}"
    assert "LIMIT 50" in _cap_limit(sql, 50)
    assert _cap_limit(sql + " LIMIT 10", 50).endswith("LIMIT 10")
    assert "LIMIT 50" in _cap_limit(sql + " LIMIT 99999", 50)


# ── resolver: E5 floor + E6 traces + primary identity ──

def test_resolver_floor_curated_all_kinds(build):
    tasks = read_tasks(CURATED)
    report = run_suite(tasks, resolver_sut(build))
    assert report["overall"]["pass@1"] == 1.0, report["failures"]
    # abstention floor is HARD: named options on every disambiguation,
    # zero false binds on should-not-answer
    assert report["by_kind"]["disambiguate"]["pass@1"] == 1.0
    assert report["by_kind"]["abstain"]["pass@1"] == 1.0
    assert not report["determinism_alarms"]


def test_run_evals_prints_excluded_count_and_gates(build):
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "run_evals.py"),
         "--tasks", str(CURATED),
         "--sut", f"resolver:{build.root.parent}",
         "--fail-under", "0.9", "--plain", "--json"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-500:]
    assert "excluded (coverage=external): 0 of 10" in result.stderr
    summary = json.loads(result.stdout.splitlines()[-1])
    assert summary["excluded_out_of_coverage"] == 0


def test_resolver_excludes_kinds_it_cannot_answer(build, tmp_path):
    """The deterministic resolver BINDS — grading it on nl2sql
    generation tasks measures a category error, not the floor. Those
    tasks exclude LOUDLY and the binding floor still reads clean."""
    row = json.loads(
        CURATED.read_text(encoding="utf-8").split("\n")[0])
    row["id"] = "nl_synthetic_0001"
    row["kind"] = "nl2sql"
    nl = tmp_path / "nl.jsonl"
    nl.write_text(json.dumps(row) + "\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "run_evals.py"),
         "--tasks", str(CURATED), "--tasks", str(nl),
         "--sut", f"resolver:{build.root.parent}",
         "--fail-under", "0.9", "--plain", "--json"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-500:]
    assert "excluded (kind outside sut capability): 1" in result.stderr
    summary = json.loads(result.stdout.splitlines()[-1])
    assert summary["excluded_kind_not_answerable"] == 1


def test_floor_failures_emit_triage_table(tmp_path):
    """E5: a failing floor run writes the pending-triage table."""
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "run_evals.py"),
         "--tasks", str(CURATED), "--sut", "null",
         "--out", str(tmp_path), "--plain"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0        # no gate flags → report only
    rows = [json.loads(x) for x in
            (tmp_path / "triage" / "floor_failures.jsonl")
            .read_text().splitlines()]
    assert rows and all(r["triage"] == "pending" and r["category"] is None
                        for r in rows)


def test_resolve_features_and_constants_version(build):
    out = resolve(build, "How much volume did our merchants process?")
    assert out["constants_version"] == RESOLVER_CONSTANTS["version"]
    features = out["features_by_slot"]["metric"]
    for key in ("tier", "support_score", "recency", "context_fit",
                "margin", "rest"):
        assert key in features, key
    assert out["metrics"] and out["confidence"] > 0


def test_fused_metric_binds_primary_identity(build):
    spend = next(m for m in build.metrics
                 if m["label"] == "GMNS Merchant Spend")
    assert len(spend["mgroups"]) >= 2        # fused across catalogs
    assert spend["mgroup"] == "mgroup:dmp:101"   # authority, not alphabet
    tasks = [t for t in read_tasks(CURATED) if t.id == "bind_001"]
    answer = resolver_sut(build)(tasks[0])
    assert answer.kind == "bindings"
    assert answer.bindings["metrics"] == ["dmp:101"]


def test_resolve_ambiguity_names_options(build):
    out = resolve(build, "How many consumer authorizations were declined?")
    assert out["ambiguities"], "conflicted concepts must ASK"
    for ambiguity in out["ambiguities"]:
        assert ambiguity["options"]
        assert all(o["label"] for o in ambiguity["options"])
    assert out["confidence"] == 0.0


# ── envelope + cache + latency ──

def test_envelope_contract_all_eight_tools(build):
    handlers = build_handlers(build.root.parent)
    assert set(handlers) == {
        "search_metrics", "search_concepts", "describe_table",
        "sample_values", "resolve", "validate_sql",
        "execute_sandboxed", "get_definition_line"}
    calls = {
        "search_metrics": {"intent": "merchant spend"},
        "search_concepts": {"phrase": "declined"},
        "describe_table": {"name": "gms_transaction"},
        "sample_values": {"table": "gms_transaction",
                          "column": "country_cd"},
        "resolve": {"question": "total merchant spend"},
        "validate_sql": {"sql": f"SELECT country_cd FROM {GMS} {DATED}"},
        "get_definition_line": {"metric_id": "mgroup:dmp:101"},
        "execute_sandboxed": {"sql": f"SELECT 1 FROM {GMS} {DATED}",
                              "substrate": StaticSubstrate({})},
    }
    for name, kwargs in calls.items():
        out = handlers[name](**kwargs)
        assert set(out) == {"status", "data", "error", "meta"}, name
        assert out["meta"]["tool"] == name
        assert out["meta"]["build_version"].startswith("b_")
        assert isinstance(out["meta"]["latency_ms"], float)
        assert out["status"] == "ok", (name, out["error"])
    bad = handlers["describe_table"](name="definitely_not_a_table")
    assert bad["status"] == "error"
    assert bad["data"]["suggestions"] is not None


def test_definition_line_speaks_meridian(build):
    handlers = build_handlers(build.root.parent)
    line = handlers["get_definition_line"](
        metric_id="mgroup:dmp:101")["data"]["definition_line"]
    assert "meridian line" in line
    assert "certified" in line


def test_ttl_cache_and_envelope_caching(build):
    cache = TTLCache(ttl_s=0.05)
    cache.put("k", 1)
    assert cache.get("k") == 1
    time.sleep(0.06)
    assert cache.get("k") is None
    handlers = build_handlers(build.root.parent)
    first = handlers["search_metrics"](intent="merchant spend")
    second = handlers["search_metrics"](intent="merchant spend")
    assert first is second                   # served from cache


def test_p50_latency_under_100ms(build):
    handlers = build_handlers(build.root.parent)
    samples = []
    for _ in range(20):
        out = handlers["resolve"](
            question="How much volume did our merchants process?")
        samples.append(out["meta"]["latency_ms"])
    assert statistics.median(samples) < 100, samples
