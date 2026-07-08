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
    """A realistic two-source service: a DATA graph (MDM tables/columns +
    the metric REGISTRY + corpus) and a SkillsRegistry (business logic +
    guardrails) — the post-refactor architecture. Skills feed the registry,
    never the graph."""
    import json

    from synapse.mcp.skills_registry import SkillsRegistry
    out = tmp_path_factory.mktemp("sources")
    load_skills_library(FIXTURE_LIBRARY, out_dir=out)   # → registry, not graph

    # tables + columns from MDM (the data spine)
    mdm = out / "mdm_cache"
    mdm.mkdir(parents=True, exist_ok=True)
    (mdm / "sbs_new_accounts.json").write_text(json.dumps({
        "table_name": "sbs_new_accounts",
        "table_description": "New-account decisions.",
        "columns": [{"name": c, "type": t} for c, t in [
            ("decision_cd", "STRING"), ("na_pcn_no", "STRING"),
            ("fraud_decline_in", "STRING")]]}))
    (mdm / "roll_rate_calc.json").write_text(json.dumps({
        "table_name": "common.roll_rate_calc",
        "table_description": "Monthly roll-rate calc.",
        "columns": [{"name": c, "type": t} for c, t in [
            ("rpt_month", "DATE"), ("bal_lag1", "FLOAT64"),
            ("cm11_encrypted", "STRING")]]}))

    # metrics from the REGISTRY (data-defined; this stays in the graph)
    reg = out / "registries" / "raw"
    reg.mkdir(parents=True, exist_ok=True)
    (reg / "metric_catalog.csv").write_text(
        "technical_name,primary_data_product,business_name,calculation_logic,"
        "metric_grain,associated_domain,business_synonyms\n"
        "gross_approval_rate,sbs_new_accounts,Gross Approval Rate,"
        "COUNT(DISTINCT CASE WHEN decision_cd = 'A' THEN na_pcn_no END)/"
        "COUNT(DISTINCT na_pcn_no),decision_month,New Accounts,"
        "approval rate;gross approval\n"
        "credit_approval_rate,sbs_new_accounts,Credit Approval Rate,"
        "COUNT(DISTINCT CASE WHEN decision_cd = 'A' THEN na_pcn_no END)/"
        "COUNT(DISTINCT na_pcn_no),decision_month,New Accounts,credit approval\n")

    # corpus (gold SQL) for observed filter values
    sql_dir = tmp_path_factory.mktemp("sql")
    (sql_dir / "G01.sql").write_text(GOLD_SQL, encoding="utf-8")
    load_gold_sql_corpus(sql_dir, out_dir=out)
    return GraphService(build_graph_from_sources(out), tenant_id="test",
                        skills=SkillsRegistry.from_dir(out / "skills"))


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
    # metrics are data-defined (the registry), not skill-derived, so there is
    # no DEFINED_BY-skill edge in the graph anymore
    assert metric["defined_by_skill"] == []


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


def test_explain_column_fills_on_demand_and_caches(tmp_path):
    """The on-demand tool through the service: envelope-wrapped, fills a
    gap once, then serves it read-through."""
    from synapse.enrichment.on_demand import OverlayStore
    from synapse.enrichment.schemas import (
        ColumnObservation, EnrichmentBundle, SelfAssessment)

    store = GraphStore()
    t = canonical_uri("table", "t")
    store.upsert_node("Table", t, {"table_name": "t",
                                   "description": "T"}, source="mdm")
    c = canonical_uri("column", "t", "x")
    store.upsert_node("Column", c, {"table_name": "t",
                                    "data_type": "STRING"}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")

    class _Client:
        calls = 0

        def enrich(self, *, skill_md, context, table_name):
            _Client.calls += 1
            name = context["inspection"]["columns"][0]["name"]
            return EnrichmentBundle(
                table_name=table_name, column_observations=[ColumnObservation(
                    column_name=name, proposed_description="what x means",
                    candidate_role="attribute", self_confidence=0.8,
                    evidence_used=["bq"])],
                self_assessment=SelfAssessment(
                    tables_skipped_for_lack_of_signal=[],
                    columns_marked_ambiguous=0,
                    proposed_entities_with_low_evidence=[],
                    requires_steward_attention=[]))

    svc = GraphService(store, llm_client=_Client(),
                       overlay=OverlayStore(tmp_path / "ov.json"))
    res = svc.explain_column("t", "x")
    assert res["status"] == "ok"
    assert res["meta"]["tool_name"] == "explain_column"
    assert "what x means" in res["data"]["description"]
    # re-ask is a read-through cache hit — no second model call
    again = svc.explain_column("t", "x")
    assert again["data"]["cached"] is True and _Client.calls == 1


def test_explain_column_without_client_serves_grounded_profile():
    store = GraphStore()
    t = canonical_uri("table", "t")
    store.upsert_node("Table", t, {"table_name": "t"}, source="mdm")
    c = canonical_uri("column", "t", "x")
    store.upsert_node("Column", c, {"table_name": "t", "data_type": "STRING",
                                    "max_value": "9"}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    res = GraphService(store).explain_column("t", "x")
    assert res["status"] == "partial"
    assert res["data"]["grounded_facts"]["data_type"] == "STRING"


def test_snapshot_version_round_trips(service, tmp_path):
    path = tmp_path / "snap.json"
    service.store.save_json(path)
    loaded = GraphStore.load_json(path)
    assert loaded.snapshot_version == service.store.snapshot_version
    assert loaded.snapshot_version != "unversioned"
    assert len(loaded.nodes) == len(service.store.nodes)
