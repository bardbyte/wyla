"""End-to-end proof of the focused-build recipe, offline.

Stage a small sources_dir (MDM + BQ profile for two of the five tables,
plus a junk gold query that references a CTE placeholder), build with the
5-table allowlist, and assert the whole recipe holds at once:

  • the two real tables are grounded, their profiled columns grounded
    (mdm + bq + synthesized dq_engine),
  • PII populates from the MDM sensitivity shape,
  • the CTE/placeholder junk from the query is pruned,
  • DQ rules were synthesized.

This is the offline stand-in for the laptop run: same builder, same
allowlist, same calibrator — only the data is synthetic.
"""

from __future__ import annotations

import json
from pathlib import Path

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import canonical_uri

FIVE = {
    "custins_customer_insights_cardmember",
    "fin_consumer_business_card_member_status",
    "risk_pers_acct",
    "risk_pers_acct_history",
    "risk_indv_cust_hist",
}


def _stage(root: Path) -> Path:
    src = root / "sources"
    (src / "mdm_cache").mkdir(parents=True)
    (src / "bq_cache").mkdir(parents=True)
    (src / "gold_queries").mkdir(parents=True)

    for tbl, cols in [
        ("risk_pers_acct",
         [("acct_id", "STRING", "Sensitive>Identifier>MemberID"),
          ("acct_bal", "FLOAT64", "Sensitive>FinancialAmount"),
          ("bus_seg", "STRING", "Internal")]),
        ("custins_customer_insights_cardmember",
         [("cust_xref_id", "INT64", "Sensitive>Identifier"),
          ("billed_business", "FLOAT64", "Sensitive>FinancialAmount"),
          ("product_group", "STRING", "Internal")]),
    ]:
        # MDM: names + PII via sensitivity_details (the nested shape)
        (src / "mdm_cache" / f"{tbl}.json").write_text(json.dumps({
            "table_name": tbl,
            "table_business_name": tbl.replace("_", " ").title(),
            "row_count_estimate": 845_000,
            "columns": [
                {"name": n, "type": t,
                 "sensitivity_details": {"is_pii": role != "Internal",
                                         "pii_role_id": role}}
                for n, t, role in cols
            ],
        }))
        # BQ: profile stats so dq_engine can attest (→ grounded)
        (src / "bq_cache" / f"{tbl}.json").write_text(json.dumps({
            "table_name": tbl, "row_count": 845_000,
            "columns": [{"name": n, "data_type": t, "is_nullable": False}
                        for n, t, _ in cols],
            "column_stats": {n: {"approx_distinct": 900, "null_fraction": 0.0}
                             for n, _, _ in cols},
            "distinct_values": {}, "policy_tags_by_column": {},
        }))

    # a gold query that references a real table AND a CTE placeholder
    (src / "gold_queries" / "Q1.sql").write_text(
        "WITH base AS (SELECT * FROM `axp-lumi.dw.risk_pers_acct`)\n"
        "SELECT acct_id FROM `your_project.your_dataset.source_table`\n"
        "JOIN `axp-lumi.dw.risk_pers_acct` a ON a.acct_id = a.acct_id")
    return src


def test_focused_build_is_scoped_grounded_and_pii(tmp_path):
    src = _stage(tmp_path)
    store = build_graph_from_sources(src, allowlist=FIVE)

    tables = {n.properties.get("table_name"): n
              for n in store.nodes_by_type("Table")}
    # only the two staged (in-scope) tables — the CTE placeholder is pruned
    assert set(tables) == {"risk_pers_acct",
                           "custins_customer_insights_cardmember"}
    assert "your_project.your_dataset.source_table" not in tables
    assert "base" not in tables

    # both tables grounded (mdm + bq + dq_engine + corpus on risk_pers_acct)
    for t in tables.values():
        assert t.provenance.confidence_tier in ("grounded", "human_asserted")

    # profiled columns are grounded, and PII populated from MDM sensitivity
    acct_bal = store.get(canonical_uri("column", "risk_pers_acct", "acct_bal"))
    assert acct_bal.provenance.confidence_tier == "grounded"
    assert acct_bal.properties["is_pii"] is True

    # dq rules were synthesized and attest via VALIDATED_BY
    assert store.nodes_by_type("DataQualityRule")
    assert any(e.edge_type == "VALIDATED_BY" for e in store.edges.values())

    # grounded share is high — the whole point of the focused build
    tiers = [n.provenance.confidence_tier for n in store.nodes_by_type("Column")]
    grounded = sum(t in ("grounded", "human_asserted") for t in tiers)
    assert grounded / len(tiers) >= 0.8
