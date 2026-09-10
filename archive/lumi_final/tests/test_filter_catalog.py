"""Tier 1: Radix-shaped filter catalog tests."""

from __future__ import annotations

from pathlib import Path

from lumi.filter_catalog import build_filter_catalog, write_filter_catalog
from lumi.schemas import DomainOntology, OntologyEntity, TableContext
from lumi.sql_to_context import parse_sqls


def _ctx(name: str = "cardmember_dim", **kw) -> TableContext:
    return TableContext(
        table_name=name,
        columns_referenced=kw.get("columns_referenced", []),
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=kw.get("filters_on_this", []),
        date_functions=[],
        mdm_columns=kw.get("mdm_columns", []),
        mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml=None,
        baseline_sql_aliases=kw.get("baseline_sql_aliases", {}),
        queries_using_this=[],
    )


def test_partition_columns_marked_partition_and_mandatory():
    ctx = _ctx(mdm_columns=[
        {"name": "trans_dt", "type": "DATE", "is_partitioned": True,
         "business_name": "Transaction Date"},
    ])
    catalog = build_filter_catalog({"finance_fact": ctx}, [])
    # _ctx default name is 'cardmember_dim', but we passed finance_fact in dict
    entry = catalog["finance_fact.trans_dt"]
    assert entry["partition"] is True
    assert entry["mandatory"] is True
    assert entry["type"] == "date"
    assert entry["namespace"] == "finance_fact"


def test_business_name_becomes_synonym():
    ctx = _ctx(mdm_columns=[
        {"name": "bu", "type": "STRING", "business_name": "Business Unit"},
    ])
    catalog = build_filter_catalog({"finance_fact": ctx}, [])
    entry = catalog["finance_fact.bu"]
    assert entry["synonyms"] == {"Business Unit": "bu"}


def test_baseline_sql_aliases_become_synonyms():
    ctx = _ctx(
        mdm_columns=[{"name": "bus_seg", "type": "STRING"}],
        baseline_sql_aliases={"customer_segment": "bus_seg"},
    )
    catalog = build_filter_catalog({"cornerstone": ctx}, [])
    assert (
        catalog["cornerstone.bus_seg"]["synonyms"].get("customer_segment")
        == "bus_seg"
    )


def test_observed_query_filter_values_collected():
    """WHERE-clause literals across queries become catalog values."""
    fps = parse_sqls([
        "SELECT * FROM finance_fact WHERE bu = 'GCS'",
        "SELECT * FROM finance_fact WHERE bu = 'GMNS'",
        "SELECT * FROM finance_fact WHERE bu = 'GCS'",
    ])
    ctx = _ctx(
        mdm_columns=[{"name": "bu", "type": "STRING"}],
    )
    catalog = build_filter_catalog({"finance_fact": ctx}, fps)
    entry = catalog["finance_fact.bu"]
    assert "GCS" in entry["values"]
    assert "GMNS" in entry["values"]
    assert len(entry["values"]) == 2  # dedup


def test_structural_filter_marked_mandatory():
    ctx = _ctx(
        mdm_columns=[{"name": "data_source", "type": "STRING"}],
        filters_on_this=[{
            "column": "data_source", "operator": "=", "value": "cornerstone",
            "is_structural": True,
        }],
    )
    catalog = build_filter_catalog({"cornerstone": ctx}, [])
    assert catalog["cornerstone.data_source"]["mandatory"] is True


def test_ontology_synonyms_attached_to_entity_columns():
    ctx = _ctx(mdm_columns=[{"name": "cm11", "type": "STRING"}])
    ontology = DomainOntology(
        entities=[OntologyEntity(
            name="cardmember",
            synonyms=["cardmember", "card member", "cm"],
            grain_columns={"cardmember_dim": ["cm11"]},
        )],
        table_to_primary_entity={"cardmember_dim": "cardmember"},
    )
    catalog = build_filter_catalog(
        {"cardmember_dim": ctx}, [], ontology=ontology,
    )
    syns = catalog["cardmember_dim.cm11"]["synonyms"]
    # Each synonym maps to canonical column name.
    assert "card member" in syns
    assert syns["card member"] == "cm11"


def test_select_aliases_become_synonyms():
    """SELECT col AS alias surfaces alias as a synonym for col."""
    fps = parse_sqls([
        "SELECT cm11 AS card_member_id, billed_amount AS gross_billings "
        "FROM cornerstone",
    ])
    ctx = _ctx(mdm_columns=[
        {"name": "cm11", "type": "STRING"},
        {"name": "billed_amount", "type": "NUMERIC"},
    ])
    catalog = build_filter_catalog({"cornerstone": ctx}, fps)
    cm_syns = catalog["cornerstone.cm11"]["synonyms"]
    assert "card_member_id" in cm_syns
    bb_syns = catalog["cornerstone.billed_amount"]["synonyms"]
    assert "gross_billings" in bb_syns


def test_write_filter_catalog_writes_sorted_json(tmp_path: Path):
    catalog = {
        "z_table.col": {"synonyms": {}, "values": [], "type": "string",
                       "namespace": "z_table", "partition": False, "mandatory": False},
        "a_table.col": {"synonyms": {}, "values": [], "type": "string",
                       "namespace": "a_table", "partition": False, "mandatory": False},
    }
    path = tmp_path / "filter_catalog.json"
    write_filter_catalog(catalog, path)
    assert path.exists()
    text = path.read_text()
    assert text.index("a_table.col") < text.index("z_table.col")


def test_type_inference_from_mdm_types():
    """Verify type mapping: NUMERIC→number, BOOL→yesno, DATE→date, else string."""
    ctx = _ctx(mdm_columns=[
        {"name": "amount", "type": "NUMERIC"},
        {"name": "is_active", "type": "BOOLEAN"},
        {"name": "trans_dt", "type": "DATE"},
        {"name": "name", "type": "STRING"},
    ])
    catalog = build_filter_catalog({"t1": ctx}, [])
    assert catalog["t1.amount"]["type"] == "number"
    assert catalog["t1.is_active"]["type"] == "yesno"
    assert catalog["t1.trans_dt"]["type"] == "date"
    assert catalog["t1.name"]["type"] == "string"


def test_namespace_is_table_name():
    """Radix expects view_name.column_name with namespace = view_name."""
    ctx = _ctx(mdm_columns=[{"name": "c", "type": "STRING"}])
    catalog = build_filter_catalog({"my_table": ctx}, [])
    entry = catalog["my_table.c"]
    assert entry["namespace"] == "my_table"


def test_values_capped_at_50():
    """Don't blow up on high-cardinality dims."""
    ctx = _ctx(mdm_columns=[{"name": "id", "type": "STRING"}])
    fps = parse_sqls([
        f"SELECT * FROM t1 WHERE id = '{i}'" for i in range(100)
    ])
    catalog = build_filter_catalog({"t1": ctx}, fps)
    assert len(catalog["t1.id"]["values"]) <= 50
