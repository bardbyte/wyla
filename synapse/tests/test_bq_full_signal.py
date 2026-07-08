"""Every BQ extract signal becomes a graph fact — nothing dropped.

The extract emits ~20 files per table (schema, constraints, profile,
usage, governance). This pins that the loader parses the ones we used to
skip — constraints (PK/FK), min/max/avg, failed queries, row policies —
and that the builder promotes them: a declared primary key flags the
grain, a declared foreign key becomes a walkable EQUIVALENT_TO join, the
DDL/footprint/governance ride on the table, and the agent-facing inspector
surfaces all of it.
"""

from __future__ import annotations

import json
from pathlib import Path

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.inspector import inspect_table
from synapse.graph.store import canonical_uri
from synapse.loaders.bq_loader import load_bq_for_table


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _make_extract(root: Path) -> None:
    """A parent (risk_pers_acct, PK acct_id) and a child
    (risk_pers_acct_history, FK acct_id → parent) — a real join."""
    parent = root / "risk_pers_acct"
    _write(parent / "1_1__columns.csv",
           "column_name,data_type,is_nullable,is_partitioning_column,"
           "clustering_ordinal_position\n"
           "acct_id,STRING,NO,NO,\n"
           "bal,NUMERIC,YES,NO,\n"
           "status_cd,STRING,YES,NO,\n")
    _write(parent / "1_2__column_descriptions.csv",
           "column_name,description\nacct_id,Account identifier\n")
    _write(parent / "1_3__table_meta.json", json.dumps({
        "table_name": "risk_pers_acct", "table_type": "BASE TABLE",
        "creation_time": "2020-01-01T00:00:00Z",
        "ddl": "CREATE TABLE dw.risk_pers_acct (acct_id STRING, bal NUMERIC, "
               "status_cd STRING) PARTITION BY DATE(_PARTITIONTIME)"}))
    _write(parent / "1_4__table_options.csv",
           "option_name,option_value\nlabels,[(\"domain\",\"risk\")]\n")
    # declared PRIMARY KEY on acct_id
    _write(parent / "1_5__constraints.csv",
           "constraint_type,column_name,constraint_name,referenced_table,"
           "referenced_column\n"
           "PRIMARY KEY,acct_id,pk_risk_pers_acct,,\n")
    _write(parent / "2_1__size_freshness.csv",
           "row_count,size_bytes,last_modified_at\n"
           "1000000,20480000,2026-07-01T00:00:00Z\n")
    # wide cardinality/nulls row incl. min/max/avg for bal
    _write(parent / "3_1__cardinality_nulls.csv",
           "acct_id__distinct,acct_id__null_frac,bal__distinct,"
           "bal__null_frac,bal__min,bal__max,bal__avg,status_cd__distinct,"
           "status_cd__null_frac\n"
           "1000000,0.0,50000,0.02,-500.00,98000.50,1234.75,3,0.0\n")
    _write(parent / "3_2__topcount__status_cd.csv",
           "value,row_count\nA,900000\nC,80000\nX,20000\n")
    # a row-access policy exists → governance flag
    _write(parent / "5_3__row_policies.csv",
           "policy_name,filter\nrap_region,region = 'US'\n")
    # two queries that ERRORED against the table (anti-patterns)
    _write(parent / "4_5__failed_queries.csv",
           "job_id,user_email,error\n"
           "job_a,x@e.com,Syntax error\njob_b,y@e.com,Not found: column\n")

    child = root / "risk_pers_acct_history"
    _write(child / "1_1__columns.csv",
           "column_name,data_type,is_nullable,is_partitioning_column,"
           "clustering_ordinal_position\n"
           "acct_id,STRING,NO,NO,\n"
           "rpt_month,DATE,NO,YES,\n"
           "bal_lag1,NUMERIC,YES,NO,\n")
    _write(child / "1_3__table_meta.json", json.dumps({
        "table_name": "risk_pers_acct_history", "table_type": "BASE TABLE",
        "ddl": "CREATE TABLE dw.risk_pers_acct_history (...)"}))
    # declared FOREIGN KEY acct_id → risk_pers_acct.acct_id (FQN target)
    _write(child / "1_5__constraints.csv",
           "constraint_type,column_name,constraint_name,referenced_table,"
           "referenced_column\n"
           "FOREIGN KEY,acct_id,fk_hist_acct,axp-lumi.dw.risk_pers_acct,"
           "acct_id\n")


def _build(tmp_path: Path):
    root = tmp_path / "bq_extract"
    _make_extract(root)
    sources = tmp_path / "sources"
    for t in ("risk_pers_acct", "risk_pers_acct_history"):
        load_bq_for_table(t, source_dir=root, out_dir=sources)
    return sources, build_graph_from_sources(sources)


# ─── loader: the previously-skipped sections are parsed ──────────


def test_loader_parses_constraints_and_governance(tmp_path):
    root = tmp_path / "bq_extract"
    _make_extract(root)
    out = tmp_path / "out"
    load_bq_for_table("risk_pers_acct", source_dir=root, out_dir=out)
    blob = json.loads(
        (out / "bq_cache" / "risk_pers_acct.json").read_text())
    # 1.5 constraints — PK captured, and folded onto the column
    assert blob["constraints"]["primary_key"] == ["acct_id"]
    acct = next(c for c in blob["columns"] if c["name"] == "acct_id")
    assert acct["is_primary"] is True
    # 3.1 min/max/avg survive (kept raw, not float-nulled)
    assert blob["column_stats"]["bal"]["min"] == "-500.00"
    assert blob["column_stats"]["bal"]["max"] == "98000.50"
    assert float(blob["column_stats"]["bal"]["avg"]) == 1234.75
    # 5.3 row policy → governance flag
    assert blob["has_row_access_policy"] is True
    # DDL + footprint carried
    assert "CREATE TABLE" in blob["ddl_snapshot"]
    assert blob["size_bytes"] == 20480000


def test_loader_captures_failed_queries_as_usage(tmp_path):
    root = tmp_path / "bq_extract"
    _make_extract(root)
    out = tmp_path / "out"
    load_bq_for_table("risk_pers_acct", source_dir=root, out_dir=out)
    usage = json.loads(
        (out / "usage_history" / "risk_pers_acct.json").read_text())
    assert usage["failed_query_count"] == 2
    assert any("Syntax" in (q.get("error") or "")
               for q in usage["failed_queries"])


def test_loader_parses_foreign_key_reference(tmp_path):
    root = tmp_path / "bq_extract"
    _make_extract(root)
    out = tmp_path / "out"
    load_bq_for_table("risk_pers_acct_history", source_dir=root, out_dir=out)
    blob = json.loads(
        (out / "bq_cache" / "risk_pers_acct_history.json").read_text())
    fks = blob["constraints"]["foreign_keys"]
    assert len(fks) == 1
    assert fks[0]["column"] == "acct_id"
    # FQN target shortened to the bare table name (join matching is bare)
    assert fks[0]["references_table"] == "risk_pers_acct"
    assert fks[0]["references_column"] == "acct_id"


# ─── builder: signals promoted to graph facts ───────────────────


def test_primary_key_flags_the_grain(tmp_path):
    _, store = _build(tmp_path)
    acct = store.get(canonical_uri("column", "risk_pers_acct", "acct_id"))
    assert acct.properties["is_primary"] is True


def test_min_max_avg_land_on_the_column(tmp_path):
    _, store = _build(tmp_path)
    bal = store.get(canonical_uri("column", "risk_pers_acct", "bal"))
    assert bal.properties["min_value"] == "-500.00"
    assert bal.properties["max_value"] == "98000.50"
    assert float(bal.properties["avg_value"]) == 1234.75


def test_ddl_footprint_and_governance_ride_on_the_table(tmp_path):
    _, store = _build(tmp_path)
    t = store.get(canonical_uri("table", "risk_pers_acct"))
    p = t.properties
    assert "CREATE TABLE" in p["ddl"]
    assert p["size_bytes"] == 20480000
    assert p["created_at"] == "2020-01-01T00:00:00Z"
    assert p["has_row_access_policy"] is True
    assert p["row_count"] == 1000000


def test_foreign_key_becomes_a_walkable_join(tmp_path):
    _, store = _build(tmp_path)
    child_acct = canonical_uri("column", "risk_pers_acct_history", "acct_id")
    parent_acct = canonical_uri("column", "risk_pers_acct", "acct_id")
    # the FK column is flagged, and the EQUIVALENT_TO edge get_join_path +
    # related_tables traverse exists, sourced from bq
    col = store.get(child_acct)
    assert col.properties["is_foreign_key"] is True
    assert col.properties["is_join_key"] is True
    eqs = store.outgoing(child_acct, "EQUIVALENT_TO")
    assert any(e.to_uri == parent_acct and "bq" in e.provenance.sources
               for e in eqs)


def test_related_tables_sees_the_declared_join(tmp_path):
    _, store = _build(tmp_path)
    view = inspect_table(store, "risk_pers_acct_history")
    related = {r["table"] for r in view["related_tables"]}
    assert "risk_pers_acct" in related


# ─── inspector: the agent sees the new signal ───────────────────


def test_inspector_surfaces_physical_and_ranges(tmp_path):
    _, store = _build(tmp_path)
    view = inspect_table(store, "risk_pers_acct")
    phys = view["physical"]
    assert phys["has_ddl"] is True
    assert "CREATE TABLE" in phys["ddl_excerpt"]
    assert phys["has_row_access_policy"] is True
    assert phys["size_bytes"] == 20480000
    bal = next(c for c in view["columns"] if c["name"] == "bal")
    assert bal["min_value"] == "-500.00" and bal["max_value"] == "98000.50"
    acct = next(c for c in view["columns"] if c["name"] == "acct_id")
    assert acct["is_primary"] is True
