"""Capability parity — everything the original single-table agent could
do, the analyst agent can do too.

The first agent (semantic-graph, one graph over one table) carried 13
tools. This suite maps each of those capabilities onto the current
toolset and exercises them end-to-end against a snapshot, so parity is
pinned, not asserted. The one deliberate non-port is
get_failed_query_corrections: the current corpus witness ingests gold
SQL only — shipping the tool without its data would be a dead surface.
It returns when the corpus loader ingests failed→fixed pairs.

    old tool                      current tool
    ────────────────────────────  ─────────────────────────────────────
    inspect_table                 inspect_table
    list_tables                   list_tables_for_domain + search_entities
    search_columns                search_entities / find_columns_for_concept
    get_metric                    get_metric
    get_join_path                 get_join_path
    find_columns_for_concept      find_columns_for_concept
    get_lineage                   get_lineage
    get_entity                    get_entity                (added here)
    resolve_synonym               search_entities + disambiguate_term
    get_failed_query_corrections  — data gap, documented above
    get_dq_status                 get_dq_status
    validate_sql                  validate_sql_plan (+ live dry_run_sql)
    get_steward_review_queue      get_steward_review_queue  (added here)
"""

from __future__ import annotations

from synapse.graph.store import GraphStore, canonical_uri
from synapse.mcp.adk_tools import build_adk_tools
from synapse.mcp.service import TOOL_NAMES, GraphService


def _world() -> GraphService:
    """A miniature of the real compile: two tables sharing an entity,
    a metric, a synonym, a join, lineage, and one llm-only fact."""
    store = GraphStore()
    for table, cols in [("accounts", ["acct_id", "open_dt"]),
                        ("history", ["acct_id", "bal"])]:
        t = canonical_uri("table", table)
        store.upsert_node("Table", t, {"table_name": table,
                                       "description": f"{table} data"},
                          source="mdm")
        for col in cols:
            c = canonical_uri("column", table, col)
            store.upsert_node("Column", c,
                              {"table_name": table, "name": col,
                               "description": f"the account {col} field"},
                              source="mdm")
            store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    a, h = (canonical_uri("column", "accounts", "acct_id"),
            canonical_uri("column", "history", "acct_id"))
    store.upsert_edge("EQUIVALENT_TO", a, h,
                      {"observation_count": 12}, source="corpus")
    store.upsert_edge("UPSTREAM_OF", canonical_uri("table", "accounts"),
                      canonical_uri("table", "history"), {}, source="mdm")
    m = canonical_uri("metric", "approval rate")
    store.upsert_node("Metric", m,
                      {"name": "Approval rate",
                       "business_name": "approval rate",
                       "formula": "approved / decisioned",
                       "synonyms": ["appr rate"]},
                      source="skills")
    ent = canonical_uri("entity", "Account")
    store.upsert_node("Entity", ent,
                      {"name": "Account",
                       "description": "a credit account"},
                      source="human_approval")
    store.upsert_edge("IDENTIFIES", a, ent, {}, source="human_approval")
    store.upsert_edge("IDENTIFIES", h, ent, {}, source="human_approval")
    guessed = canonical_uri("column", "history", "bal")
    store.nodes[guessed].provenance.sources = ["llm"]
    store.nodes[guessed].provenance.confidence_tier = "guessed"
    store.nodes[guessed].provenance.confidence_score = 0.2
    return GraphService(store)


def test_every_old_capability_answers():
    svc = _world()
    checks = {
        "inspect_table": svc.inspect_table("accounts"),
        "list_tables": svc.list_tables_for_domain(),
        "search_columns": svc.search_entities("acct"),
        "get_metric": svc.get_metric("appr rate"),
        "get_join_path": svc.get_join_path("accounts", "history"),
        "find_columns": svc.find_columns_for_concept("account"),
        "get_lineage": svc.get_lineage("accounts"),
        "get_entity": svc.get_entity("Account"),
        "resolve_synonym": svc.disambiguate_term("approval rate"),
        "get_dq_status": svc.get_dq_status("accounts"),
        "validate_sql": svc.validate_sql_plan(
            "SELECT acct_id FROM accounts"),
        "review_queue": svc.get_steward_review_queue(),
    }
    for capability, resp in checks.items():
        assert resp.get("status") == "ok", (capability, resp)


def test_get_entity_returns_identifying_columns_at_top_tier():
    resp = _world().get_entity("account")          # case-insensitive
    data = resp["data"]
    assert data["name"] == "Account"
    assert data["provenance"]["confidence_tier"] == "human_asserted"
    assert data["n_supporting_tables"] == 2
    assert {c["table"] for c in data["identified_by"]} \
        == {"accounts", "history"}


def test_get_entity_is_honest_when_none_are_minted():
    svc = GraphService(GraphStore())
    resp = svc.get_entity("Account")
    assert resp["status"] == "error"
    assert "propose" in resp["error"]["message"]   # names the remedy


def test_review_queue_surfaces_weakest_facts_first():
    resp = _world().get_steward_review_queue(limit=50)
    items = resp["data"]["items"]
    assert items and items[0]["tier"] == "guessed"   # weakest first
    llm_only = [i for i in items if i["sources"] == ["llm"]]
    assert llm_only, "the llm-only fact must be queued"
    assert "witness" in llm_only[0]["reason"]
    # the steward-signed entity must NOT be in the queue
    assert not [i for i in items if i["kind"] == "Entity"]


def test_registry_and_adk_roster_carry_the_new_tools():
    assert "get_entity" in TOOL_NAMES
    assert "get_steward_review_queue" in TOOL_NAMES
    names = {t.__name__ for t in build_adk_tools(_world())}
    assert {"get_entity", "get_steward_review_queue", "explain_column",
            "check_data_trust", "capture_knowledge"} <= names
    assert len(names) == 20                        # 18 + the 2 ports
