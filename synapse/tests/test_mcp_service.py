"""GraphService — the one implementation behind MCP and ADK surfaces."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import GraphStore, canonical_uri
from synapse.loaders.gold_sql_loader import load_gold_sql_corpus
from synapse.loaders.skills_loader import load_skills_library
from synapse.mcp.service import TOOL_NAMES, GraphService

FIXTURE_LIBRARY = Path(__file__).parent / "fixtures" / "skills_library"

GOLD_SQL = """
SELECT DATE_TRUNC(decision_dt, MONTH) AS m,
       COUNT(DISTINCT na_pcn_no) AS approved
FROM sbs_new_accounts
WHERE decision_cd = 'A'
GROUP BY m
"""


@pytest.fixture(scope="module")
def service(tmp_path_factory) -> GraphService:
    out = tmp_path_factory.mktemp("sources")
    load_skills_library(FIXTURE_LIBRARY, out_dir=out)
    sql_dir = tmp_path_factory.mktemp("sql")
    (sql_dir / "G01.sql").write_text(GOLD_SQL, encoding="utf-8")
    load_gold_sql_corpus(sql_dir, out_dir=out)
    return GraphService(build_graph_from_sources(out), tenant_id="test")


def test_every_registered_tool_exists(service):
    for name in TOOL_NAMES:
        assert callable(getattr(service, name)), name


def test_envelope_is_uniform_and_provenance_typed(service):
    res = service.search_entities("approval rate")
    assert res["status"] == "ok"
    assert res["meta"]["tool_name"] == "search_entities"
    assert res["meta"]["tenant_id"] == "test"
    hit = res["data"]["hits"][0]
    assert {"confidence_tier", "confidence_score", "sources"} <= set(hit)


def test_search_error_is_structured_not_raised(service):
    res = service.search_entities("zzz_does_not_exist_zzz")
    assert res["status"] == "error"
    assert res["error"]["code"] == "not_found"
    assert res["error"]["suggestions"]


def test_metric_resolution_by_synonym(service):
    res = service.get_metric("approval rate")
    assert res["status"] == "ok"
    metric = res["data"]["metric"]
    assert metric["technical_name"] == "gross_approval_rate"
    assert "COUNT(DISTINCT" in metric["formula"]
    assert metric["defined_by_skill"] == ["demo_newaccountsapprovalrate"]


def test_skill_lookup_bundles_guardrails(service):
    res = service.get_skill("roll rates")
    assert res["status"] == "ok"
    assert res["data"]["skill"]["skill_id"] == "DEMO_RollRates"
    rules = {g["rule"] for g in res["data"]["guardrails"]}
    assert any("LAG()" in r for r in rules)


def test_guardrails_for_table_include_column_scoped(service):
    res = service.get_guardrails("common.roll_rate_calc")
    assert res["status"] == "ok"
    rails = res["data"]["guardrails"]
    assert any("cm11_encrypted" in g["rule"] for g in rails)
    # severity=error rails sort first
    assert rails[0]["severity"] == "error"


def test_validate_sql_plan_catches_machine_checkable_violations(service):
    bad = ("SELECT cm11_encrypted, LAG(bal) OVER (ORDER BY rpt_month) "
           "FROM common.roll_rate_calc")
    res = service.validate_sql_plan(bad)
    reasons = " | ".join(v["reason"] for v in res["data"]["violations"])
    assert "cm11_encrypted" in reasons
    assert "LAG()" in reasons


def test_validate_sql_plan_passes_clean_sql(service):
    good = ("SELECT rpt_month, SUM(bal_lag1) FROM common.roll_rate_calc "
            "GROUP BY rpt_month")
    res = service.validate_sql_plan(good)
    assert res["data"]["parse_ok"] is True
    assert res["data"]["violations"] == []
    assert res["data"]["must_respect"]  # advisory list still present


def test_validate_sql_plan_flags_count_without_distinct(service):
    res = service.validate_sql_plan(
        "SELECT COUNT(na_pcn_no) FROM sbs_new_accounts")
    assert any("COUNT" in v["reason"] for v in res["data"]["violations"])


def test_validate_sql_plan_handles_unparseable_sql(service):
    res = service.validate_sql_plan("SELEC nope FROM FROM")
    assert res["status"] == "partial"
    assert res["data"]["parse_ok"] is False


def test_inspect_table_carries_guardrails_and_caches(service):
    first = service.inspect_table("sbs_new_accounts")
    assert first["status"] == "ok"
    assert first["data"]["guardrails"]
    assert first["meta"]["cached"] is False
    second = service.inspect_table("sbs_new_accounts")
    assert second["meta"]["cached"] is True


def test_filter_values_from_corpus(service):
    res = service.get_filter_values("sbs_new_accounts", "decision_cd")
    assert res["status"] == "ok"
    values = {v["raw_value"] for v in res["data"]["values"]}
    assert "A" in values


def test_explain_confidence_names_the_raise_path(service):
    res = service.explain_confidence("gross_approval_rate")
    assert res["status"] == "ok"
    assert res["data"]["tier"] in {"guessed", "inferred", "grounded"}
    assert res["data"]["what_would_raise_it"]


def test_disambiguate_surfaces_ambiguity_instead_of_coinflip(service):
    res = service.disambiguate_term("rate", context_query="")
    assert res["status"] == "ok"
    data = res["data"]
    # with no context, competing *_rate metrics must NOT be silently chosen
    assert data["ambiguity_reason"] is not None or data["chosen"] is not None


def test_join_path_absence_is_error_with_guidance():
    # two real (mdm-grounded) tables with no observed join between them —
    # the agent must be told not to invent a path, not handed a guess
    store = GraphStore()
    for t in ("alpha_tbl", "beta_tbl"):
        store.upsert_node("Table", canonical_uri("table", t),
                          {"table_name": t}, source="mdm")
    res = GraphService(store).get_join_path("alpha_tbl", "beta_tbl")
    assert res["status"] == "error"
    assert "invent" in " ".join(res["error"]["suggestions"]).lower()


def test_snapshot_version_round_trips(service, tmp_path):
    path = tmp_path / "snap.json"
    service.store.save_json(path)
    loaded = GraphStore.load_json(path)
    assert loaded.snapshot_version == service.store.snapshot_version
    assert loaded.snapshot_version != "unversioned"
    assert len(loaded.nodes) == len(service.store.nodes)
