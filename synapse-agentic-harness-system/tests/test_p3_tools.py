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
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "build-graph",
         "--graph", str(tmp / "graph"),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "p3_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    # through the CLI on purpose: every laptop.py subcommand must run
    # end-to-end on fixtures in CI (the runbook-drift guard)
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "compile",
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
    assert "sensitive_column" in _codes(
        validate_sql(build, f"SELECT cm13 FROM {GMS} {DATED}"))       # 7
    assert "select_star_over_sensitive" in _codes(
        validate_sql(build, f"SELECT * FROM {WWCAS} {DATED}"))        # 8
    twin = validate_sql(
        build, f"SELECT approval_cd, part_dt FROM {WWCAS} {DATED}")
    assert twin["ok"], twin["violations"]
    # E3 surfaces here as a WARNING; the sandbox is where it denies
    assert "policy_unknown" in _warning_codes(twin)
    filt = validate_sql(
        build, f"SELECT country_cd FROM {GMS} WHERE cm13 = 'x'")
    assert filt["ok"]
    assert "sensitive_column_in_filter" in _warning_codes(filt)


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
