"""Domain layer + retrieval evals: the graph as a searchable machine.

Pins four contracts:

1. The domain ROLLUP builds the layer ON TOP of the graph: Domain nodes
   with edge-based membership derived from the labels tables carry —
   descriptions assembled only from provable counts, witnesses inherited
   (never invented), idempotent across re-runs. Overlap is first-class.
2. The steward DOMAIN-TAGS map COEXISTS with machine labels: membership
   edges at human_approval, never a property overwrite — what MDM said
   stays as MDM said it. Steward facts (edges + prose) survive rollup
   recomputes; a steward+machine agreement fuses into one edge with two
   witness families.
3. SEARCH is domain-aware: plural-fold stemming, behavioral vocabulary
   haystacks, a ``domain`` filter resolved through the layer's edges,
   uris on every hit, and route_question — domain first, honest flat
   fallback when no domain carries signal.
4. The retrieval EVAL extracts its gold set from the graph's own
   evidence and scores hit@k/MRR deterministically.
"""

from __future__ import annotations

import json
from pathlib import Path

from synapse.evals.retrieval import (
    evaluate_retrieval,
    extract_gold_set,
    format_report,
)
from synapse.graph.rollup import rollup_domains
from synapse.graph.store import GraphStore, canonical_uri
from synapse.loaders.domain_tags_loader import load_domain_tags
from synapse.mcp.service import GraphService
from synapse.mcp.skills_registry import SkillsRegistry


# ── fixture world: two segments, metrics, joins, a skill ─────

def _world() -> GraphStore:
    store = GraphStore()

    def table(name, bu, source, **props):
        return store.upsert_node(
            "Table", canonical_uri("table", name),
            {"table_name": name, "business_unit": bu, **props},
            source=source)

    table("gms_transaction", "GCS", "mdm",
          business_name="Merchant Transactions", data_domain="Merchant")
    table("gms_merchant_char", "GCS", "usage_mined",
          data_domain="Merchant")
    table("risk_pers_acct", "Credit and Risk", "mdm",
          business_name="Personal Account Risk", data_domain="Risk")
    table("pmdl_stmt_fact", "", "bq")          # no domain evidence

    m1 = canonical_uri("metric", "gms_transaction", "total_merchant_spend")
    store.upsert_node("Metric", m1, {
        "business_name": "Total Merchant Spend",
        "sourced_from_table": "gms_transaction",
        "question_answered": "How much volume did our merchants process?",
        "execution_count": 900, "user_count": 40,
        "group_by_patterns": ["merchant_region"],
    }, source="dmp")
    store.upsert_edge("COMPUTED_FROM", m1,
                      canonical_uri("table", "gms_transaction"), {},
                      source="dmp")
    m2 = canonical_uri("metric", "risk_pers_acct", "delinquency_rate")
    store.upsert_node("Metric", m2, {
        "business_name": "Delinquency Rate",
        "sourced_from_table": "risk_pers_acct",
        "execution_count": 120, "user_count": 8,
    }, source="usage_mined")
    store.upsert_edge("COMPUTED_FROM", m2,
                      canonical_uri("table", "risk_pers_acct"), {},
                      source="usage_mined")

    store.upsert_edge(
        "JOINS_WITH", canonical_uri("table", "gms_transaction"),
        canonical_uri("table", "gms_merchant_char"),
        {"observed_count": 5}, source="usage_mined")
    store.upsert_edge(
        "JOINS_WITH", canonical_uri("table", "gms_transaction"),
        canonical_uri("table", "risk_pers_acct"),
        {"observed_count": 2}, source="usage_mined")
    return store


# ── 1. rollup ────────────────────────────────────────────────

def test_rollup_builds_layer_with_derived_profile_and_edges():
    store = _world()
    report = rollup_domains(store)
    assert report["domains"] == 2
    assert report["tables_in_no_domain"] == 1
    gcs = store.get(canonical_uri("domain", "GCS"))
    assert gcs is not None and gcs.node_type == "Domain"
    p = gcs.properties
    assert p["table_count"] == 2
    assert p["metric_count"] == 1
    assert p["execution_count"] == 900
    assert "GCS — 2 table(s)" in p["description"]
    assert "Merchant (2)" in p["description"]
    assert p["question_bank"] == [
        "How much volume did our merchants process?"]
    assert p["internal_join_edges"] == 1     # gms→gms
    assert p["external_join_edges"] == 1     # gms→risk
    assert p["external_join_partners"] == ["Credit and Risk"]
    assert p["membership"] == {"gms_merchant_char": "derived",
                               "gms_transaction": "derived"}
    members = {e.to_uri for e in store.outgoing(
        gcs.canonical_uri, "CONTAINS")}
    assert members == {canonical_uri("table", "gms_transaction"),
                       canonical_uri("table", "gms_merchant_char")}


def test_rollup_inherits_witnesses_from_members_only():
    store = _world()
    rollup_domains(store)
    gcs = store.get(canonical_uri("domain", "GCS"))
    assert set(gcs.provenance.sources) == {"mdm", "usage_mined"}
    # bq alone can't assert an org label — but a table whose ONLY
    # witness is bq still contributes its strongest source as fallback
    assert "bq" not in gcs.provenance.sources


def test_rollup_is_idempotent_and_reflects_current_state():
    store = _world()
    rollup_domains(store)
    first = len(store.nodes_by_type("Domain"))
    # a table changes segment; re-rollup must move it, not duplicate
    t = store.get(canonical_uri("table", "gms_merchant_char"))
    t.properties["business_unit"] = "Credit and Risk"
    rollup_domains(store)
    assert len(store.nodes_by_type("Domain")) == first
    gcs = store.get(canonical_uri("domain", "GCS"))
    assert gcs.properties["table_count"] == 1


# ── 2. steward domain tags: coexistence + overlap ────────────

def test_domain_tags_coexist_never_overwrite_and_overlap(tmp_path: Path):
    store = _world()
    tags = tmp_path / "tags.json"
    tags.write_text(json.dumps([
        {"company_domain": "Merchant Services",
         "description": "Steward-authored merchant segment.",
         "tables": ["gms_transaction", "gms_merchant_charact",
                    "brand_new_tbl"]},
        {"company_domain": "Credit Risk",
         "tables": ["gms_transaction", "risk_pers_acct"]},
    ]))
    report = load_domain_tags(
        store, tags, aliases={"gms_merchant_charact": "gms_merchant_char"})
    assert report["memberships"] == 5
    assert report["stubs_minted"] == ["brand_new_tbl"]
    assert report["overlapping_tables"] == {
        "gms_transaction": ["Credit Risk", "Merchant Services"]}
    # COEXISTENCE: the MDM label on the table is untouched
    t = store.get(canonical_uri("table", "gms_transaction"))
    assert t.properties["business_unit"] == "GCS"
    assert "company_domain" not in t.properties

    rollup_domains(store)
    # the table now belongs to THREE domains: GCS (derived from its
    # label) + the steward's two — overlap the property never allowed
    doms = {str(d.properties["name"])
            for d in store.nodes_by_type("Domain")
            if canonical_uri("table", "gms_transaction") in {
                e.to_uri for e in store.outgoing(
                    d.canonical_uri, "CONTAINS")}}
    assert doms == {"GCS", "Merchant Services", "Credit Risk"}
    ms = store.get(canonical_uri("domain", "Merchant Services"))
    assert ms.provenance.confidence_tier == "human_asserted"
    assert ms.properties["description"] == (
        "Steward-authored merchant segment.")
    assert ms.properties["description_by"] == "steward"
    assert ms.properties["table_count"] == 3
    # membership kinds are explicit
    assert ms.properties["membership"]["gms_transaction"] == "steward"


def test_steward_facts_survive_rollup_recompute(tmp_path: Path):
    store = _world()
    tags = tmp_path / "tags.json"
    tags.write_text(json.dumps(
        {"Merchant Services": ["gms_transaction"]}))
    load_domain_tags(store, tags)
    rollup_domains(store)
    rollup_domains(store)                     # and again
    ms = store.get(canonical_uri("domain", "Merchant Services"))
    assert ms is not None
    assert ms.provenance.confidence_tier == "human_asserted"
    edge = store.outgoing(ms.canonical_uri, "CONTAINS")
    assert len(edge) == 1
    assert "human_approval" in edge[0].provenance.sources


def test_steward_and_machine_agreement_fuses_one_edge(tmp_path: Path):
    store = _world()
    tags = tmp_path / "tags.json"
    tags.write_text(json.dumps({"GCS": ["gms_transaction"]}))
    load_domain_tags(store, tags)
    rollup_domains(store)
    gcs = store.get(canonical_uri("domain", "GCS"))
    assert gcs.properties["membership"]["gms_transaction"] == "both"
    edges = [e for e in store.outgoing(gcs.canonical_uri, "CONTAINS")
             if e.to_uri == canonical_uri("table", "gms_transaction")]
    assert len(edges) == 1                    # ONE membership…
    assert "human_approval" in edges[0].provenance.sources
    assert "mdm" in edges[0].provenance.sources   # …two witness families


def test_domain_tags_dict_shape_and_empty_rows(tmp_path: Path):
    store = _world()
    tags = tmp_path / "tags.json"
    tags.write_text(json.dumps({"Credit Risk": ["risk_pers_acct"],
                                "Empty Domain": []}))
    report = load_domain_tags(store, tags)
    assert report["memberships"] == 1
    assert report["skipped"] == [
        "Empty Domain: no tables listed "
        "(domain node minted, no memberships)"]
    # the empty domain still exists as a steward-minted node
    rollup_domains(store)
    assert store.get(canonical_uri("domain", "Empty Domain")) is not None


# ── 3. domain-aware search + routing ─────────────────────────

def _service(store: GraphStore | None = None,
             skills: SkillsRegistry | None = None) -> GraphService:
    store = store or _world()
    rollup_domains(store)
    return GraphService(store, skills=skills)


def test_stemming_folds_plurals_both_sides():
    svc = _service()
    hits = svc.search_entities("merchants")["data"]["hits"]
    assert any(h["kind"] == "Table" and "merchant" in h["name"]
               for h in hits)


def test_search_hits_carry_uri_and_label():
    svc = _service()
    top = svc.search_entities("total merchant spend")["data"]["hits"][0]
    assert top["uri"] == canonical_uri(
        "metric", "gms_transaction", "total_merchant_spend")
    t = svc.search_entities("gms_transaction")["data"]["hits"][0]
    assert t["business_unit"] == "GCS"


def test_domain_filter_resolves_through_layer_edges(tmp_path: Path):
    store = _world()
    tags = tmp_path / "tags.json"
    # steward adds risk_pers_acct to a NEW domain — no prop carries it
    tags.write_text(json.dumps(
        {"Fraud Analytics": ["risk_pers_acct"]}))
    load_domain_tags(store, tags)
    svc = _service(store)
    hits = svc.search_entities(
        "delinquency", domain="Fraud Analytics")["data"]["hits"]
    assert any("risk_pers_acct" in h["uri"] for h in hits)
    # scoped OUT: gms tables are not Fraud Analytics members
    scoped = svc.search_entities("spend", domain="GCS")
    for h in scoped["data"]["hits"]:
        assert "risk_pers_acct" not in h["uri"]
    missing = svc.search_entities("delinquency", domain="GCS")
    assert missing["status"] == "error"
    assert "GCS" in missing["error"]["message"]


def test_route_question_picks_domain_via_question_bank():
    svc = _service()
    data = svc.route_question(
        "how much volume did our merchants process last month")["data"]
    assert data["mode"] == "domain_routing"
    top = data["domains"][0]
    assert top["domain"] == "GCS"
    assert top["score"] > 0
    assert top["top_tables"][0]["table"] in {"gms_transaction",
                                             "gms_merchant_char"}
    assert any(m["metric"] == "Total Merchant Spend"
               for m in top["top_metrics"])


def test_route_question_flat_fallback_when_no_signal_or_no_layer():
    svc = _service()
    off_topic = svc.route_question("weather in tokyo tomorrow")["data"]
    assert off_topic["mode"] == "flat_search"
    assert "no domain matches" in off_topic["note"]
    bare = GraphService(_world())          # rollup never ran
    resp = bare.route_question("merchant volume")["data"]
    assert resp["mode"] == "flat_search"
    assert "no domain layer" in resp["note"]


def test_route_question_attaches_skills_by_domain_and_by_tables():
    registry = SkillsRegistry([
        {"skill_id": "MerchantHealth", "domain": "merchant_analytics",
         "company_domain": "GCS", "description": "Merchant KPIs.",
         "tables_used": [], "guardrails": []},
        {"skill_id": "RollRates", "domain": "portfolio_analytics",
         "description": "Roll rates.", "guardrails": [],
         "tables_used": ["risk_pers_acct"]},
        {"skill_id": "Unrelated", "domain": "x", "description": "",
         "tables_used": ["elsewhere_tbl"], "guardrails": []},
    ])
    svc = _service(skills=registry)
    units = {u["domain"]: u for u in svc.route_question(
        "merchant delinquency spend volume")["data"]["domains"]}
    gcs_skills = {s["skill_id"] for s in units["GCS"]["skills"]}
    assert gcs_skills == {"MerchantHealth"}          # via company_domain
    risk_skills = {s["skill_id"]
                   for s in units["Credit and Risk"]["skills"]}
    assert risk_skills == {"RollRates"}              # via tables_used


# ── 4. retrieval eval ────────────────────────────────────────

def test_gold_set_extraction_kinds_and_dedupe():
    store = _world()
    rollup_domains(store)
    gold = extract_gold_set(store)
    kinds = {g.kind for g in gold}
    assert {"dmp_question", "dmp_question_heldout", "metric_name",
            "mined_measure_name", "table_business_name",
            "domain_route"} <= kinds
    heldout = next(g for g in gold if g.kind == "dmp_question_heldout")
    for banned in ("total", "spend"):     # name tokens must be stripped
        assert banned not in heldout.query.lower().split()
    assert len({(g.query.lower(), tuple(sorted(g.expect)))
                for g in gold}) == len(gold)


def test_evaluate_retrieval_math_is_exact():
    class Fake:
        store = None

        def search_entities(self, query, top_k=10):
            hits = {"first": [{"uri": "A", "name": "a", "score": 1}],
                    "third": [{"uri": "x", "name": "x", "score": 3},
                              {"uri": "y", "name": "y", "score": 2},
                              {"uri": "B", "name": "b", "score": 1}],
                    "never": []}[query]
            return {"data": {"hits": hits}}

    from synapse.evals.retrieval import GoldExample
    gold = [GoldExample("first", ["A"], "k1"),
            GoldExample("third", ["B"], "k1"),
            GoldExample("never", ["Z"], "k2")]
    report = evaluate_retrieval(Fake(), gold)
    assert report["n_examples"] == 3
    # MRR = (1 + 1/3 + 0) / 3
    assert report["overall"]["mrr"] == round((1 + 1 / 3) / 3, 3)
    assert report["overall"]["hit@1"] == round(1 / 3, 3)
    assert report["overall"]["hit@3"] == round(2 / 3, 3)
    assert report["by_kind"]["k2"]["mrr"] == 0.0
    fails = {f["query"] for f in report["failures"]}
    assert fails == {"never"}
    assert "OVERALL" in format_report(report)


def test_eval_on_the_fixture_world_finds_everything_by_rank_3():
    svc = _service()
    gold = extract_gold_set(svc.store)
    report = evaluate_retrieval(svc, gold)
    assert report["overall"]["hit@3"] == 1.0


# ── 5. domain sub-agent specs (pure composition) ─────────────

def test_domain_agent_specs_compose_from_graph_evidence():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from apps.analyst.domains import build_domain_agent_specs
    registry = SkillsRegistry([
        {"skill_id": "MerchantHealth", "domain": "merchant_analytics",
         "company_domain": "GCS", "description": "Merchant KPIs.",
         "tables_used": [], "guardrails": []},
    ])
    svc = _service(skills=registry)
    specs = build_domain_agent_specs(svc)
    assert [s["name"] for s in specs][:1] == ["gcs_specialist"]
    gcs = specs[0]
    assert gcs["domain"] == "GCS"
    assert gcs["n_tables"] == 2 and gcs["n_skills"] == 1
    assert "gms_transaction" in gcs["instruction"]
    assert "MerchantHealth" in gcs["instruction"]
    assert "domain='GCS'" in gcs["instruction"]
    # largest domain first, deterministic order
    assert specs == sorted(
        specs, key=lambda s: (-s["n_tables"], s["name"]))
