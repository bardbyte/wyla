"""std_tech_metadata — every documented field reaches a record.

Full utilization at field grain: the catalog feed's own contract
(docs/contracts/std_tech_metadata_layout.md) says what it sends, and
these hold the loader to it. A regression here is a field going
quietly dark again.

They live apart from the general census suite on purpose: this is the
one source with a pinned field contract, and keeping its tests in
their own file means a change to the catalog and a change to any other
loader never land in the same place."""

from __future__ import annotations

import json
from pathlib import Path

from sahs.loaders.sources.vocab import load_std_tech_metadata

FX = Path(__file__).resolve().parent / "fixtures" / "sources"


def test_std_tech_parses_every_documented_field():
    """Full utilization at field grain (docs/contracts/
    std_tech_metadata_layout.md): every documented key reaches a
    record. A regression here is a field going quietly dark again."""
    entries, quarantined = load_std_tech_metadata(FX / "std_tech_metadata")
    assert not quarantined
    gms = next(e for e in entries if e.table == "gms_transaction")
    # Layer 1 envelope + Layer 2 tech entry
    assert gms.appl_id == "600001868"
    assert gms.datasource == "axp-lumi" and gms.dataset_group == "data"
    assert gms.data_server == "Lumi" and gms.technology == "BigQuery"
    assert gms.data_system.startswith("NGBD")
    assert gms.is_active is True and gms.is_latest is True
    assert gms.is_lineage_exist is True          # the feed sends "Y"
    # Layer 3 datasetAttribute
    assert gms.table_name == "gms_transaction"
    assert gms.table_type == "DERIVED"
    assert gms.load_type == "LOAD_APPEND"
    assert gms.is_partitioned is True
    assert gms.target_system == "Lumi BigQuery"
    assert gms.data_sub_category == "Payments"
    # the feed states a type and a mandatory flag beside each
    # sensitive column, and both ride along
    assert gms.pii_columns == [
        {"column": "cm13", "pii_role_id": "R3",
         "data_type": "STRING", "mandatory": True},
        {"column": "cm15_hash", "pii_role_id": "R4",
         "data_type": "STRING", "mandatory": False}]
    # Layer 4 pdeAttribute
    amount = next(c for c in gms.columns if c.name == "trans_usd_am")
    assert amount.column_name == "trans_usd_am" and amount.position == 5
    assert amount.column_length == 18
    assert amount.nullable is True and amount.primary_key is False
    assert amount.partition_key is False
    assert amount.derived_logic.startswith("CASE WHEN se_cr_dr_in")
    assert amount.linked_terms[0]["businessTermId"] == "8"
    # ABSENT IS NOT FALSE: wwcas ships a thin Layer 2, and an unsent
    # flag must stay unknown rather than be flattened into a denial
    wwcas = next(e for e in entries if e.table == "wwcas_authorization")
    assert wwcas.is_active is None and wwcas.is_latest is None
    assert wwcas.is_partitioned is False          # "N", genuinely false
    assert wwcas.table_type == "VIEW"
    assert next(c for c in wwcas.columns).primary_key is True


def test_std_tech_unmatchable_terms_keep_their_text(tmp_path: Path):
    """A term with neither an id nor a glossary spelling cannot mint an
    identity — slugging its name would fork the node the moment the id
    arrives. The TEXT is still evidence, so it rides on the column, and
    EVERY such declaration survives (a fold is last-wins per key, so
    appending them one at a time would leave only the last)."""
    from sahs.graph.crosswalk import Crosswalk
    from sahs.graph.quads import GraphDir
    from sahs.loaders.quads_emit import emit_std_tech

    src = tmp_path / "src"
    src.mkdir()
    (src / "t.json").write_text(json.dumps({
        "dataset": "gms_transaction",
        "tech_metadata_list": [{
            "datasetAttribute": {"description": "x"},
            "pde": [{"pdeRelPath": "cm13",
                     "pdeAttribute": {"data_type_name": "STRING"},
                     "businessMetadata": [
                         {"businessTermName": "Unknown Term A",
                          "businessTermDescription": "definition A"},
                         {"businessTermName": "Unknown Term B",
                          "businessTermDescription": "definition B"}]}]}]}),
        encoding="utf-8")
    cw = tmp_path / "crosswalk.jsonl"
    cw.write_text(json.dumps({"physical": "dw.gms_transaction",
                              "verified_by": "t",
                              "verified_on": "2026-09-02"}) + "\n",
                  encoding="utf-8")
    entries, _ = load_std_tech_metadata(src)
    graph = GraphDir(tmp_path / "graph")
    report = emit_std_tech(entries, [], graph, Crosswalk.load(cw), "r1")
    assert report["term_links_unmatched"] == 2
    props = graph.fold_nodes()["col:dw.gms_transaction.cm13"].props
    assert props["declared_terms"] == [
        {"name": "Unknown Term A", "description": "definition A"},
        {"name": "Unknown Term B", "description": "definition B"}]


def test_std_tech_compliance_flags_absent_is_unknown(tmp_path: Path):
    """ABSENT IS NOT FALSE holds for has_pii / has_oncop / has_gdpr too:
    an entry that never sent the flag has not denied PII. The record
    keeps None, the graph gets neither a `has_*_atlas` prop nor a
    policy edge fabricated from silence — but a flag sent as false IS
    a denial and stays."""
    from sahs.graph.crosswalk import Crosswalk
    from sahs.graph.quads import GraphDir
    from sahs.loaders.quads_emit import emit_std_tech

    src = json.loads((FX / "std_tech_metadata"
                      / "gms_transaction.json").read_text())
    # the fixture registers gms TWICE in one envelope (the real feed
    # does) — silence has to be silence in EVERY registration, or the
    # fold rightly takes the other one's word
    for item in src["tech_metadata_list"]:
        attr = item["datasetAttribute"]
        attr.pop("has_pii", None)
        attr.pop("has_oncop", None)
        attr["has_gdpr"] = "N"
    d = tmp_path / "std"
    d.mkdir()
    (d / "gms.json").write_text(json.dumps(src), encoding="utf-8")
    entries, quarantined = load_std_tech_metadata(d)
    assert not quarantined
    e = entries[0]
    assert e.has_pii is None and e.has_oncop is None
    assert e.has_gdpr is False
    cw = tmp_path / "crosswalk.jsonl"
    cw.write_text(json.dumps({"physical": "dw.gms_transaction",
                              "verified_by": "t",
                              "verified_on": "2026-09-02"}) + "\n",
                  encoding="utf-8")
    graph = GraphDir(tmp_path / "graph")
    emit_std_tech(entries, [], graph, Crosswalk.load(cw), "r1")
    table = graph.fold_nodes()["table:dw.gms_transaction"].props
    assert "has_pii_atlas" not in table
    assert "has_oncop_atlas" not in table
    assert table["has_gdpr_atlas"] is False
    assert not [q for q in graph.iter_edges("has_policy")
                if q.s == "table:dw.gms_transaction"]


def test_the_real_feed_spells_the_sensitive_column_key_differently(
        tmp_path: Path):
    """The column key inside the table-level sensitivity lists has two
    spellings: `column` in the documented contract, `column_name` in
    the feed itself. The loader read one, so on the real export every
    row of every list was skipped and the second sensitivity witness —
    a shipped feature — produced nothing at all. Both spellings parse,
    and the other two compliance regimes are read beside PII: the feed
    sends three parallel lists of the same shape and a column named by
    any of them is sensitive."""
    from sahs.loaders.sources.vocab import load_std_tech_metadata
    src = tmp_path / "std"
    src.mkdir()
    (src / "t.json").write_text(json.dumps({
        "dataset": "gms_transaction",
        "tech_metadata_list": [{"datasetAttribute": {
            "description": "x",
            "pii_columns": [{"column_name": "cm13", "pii_role_id": "R3",
                             "data_type_name": "STRING",
                             "is_mandatory": "Y"}],
            "gdpr_columns": [{"column": "cm15", "pii_role_id": "R4"}],
            "oncop_columns": [{"column_name": "se_no"}]}, "pde": []}]}),
        encoding="utf-8")
    e = load_std_tech_metadata(src)[0][0]
    assert e.pii_columns == [{"column": "cm13", "pii_role_id": "R3",
                              "data_type": "STRING", "mandatory": True}]
    assert e.gdpr_columns == [{"column": "cm15", "pii_role_id": "R4"}]
    assert e.oncop_columns == [{"column": "se_no"}]


def test_the_catalog_carries_its_own_business_unit(tmp_path: Path):
    """The contract said the catalog had no business-unit axis and only
    the MDM plane did. The real feed carries one on every entry. It is
    read under its OWN name so the two planes can agree, disagree, or
    fill each other's gaps — one plane silently overwriting the other's
    answer is the failure this avoids."""
    from sahs.loaders.sources.vocab import load_std_tech_metadata
    src = tmp_path / "std"
    src.mkdir()
    (src / "t.json").write_text(json.dumps({
        "dataset": "gms_transaction",
        "tech_metadata_list": [{"datasetAttribute": {
            "description": "x", "business_unit": "Finance",
            "data_classification": "Confidential", "host_region": "US",
            "decommissioned": "N",
            "partitioned_columns": ["part_dt"]}, "pde": []}]}),
        encoding="utf-8")
    e = load_std_tech_metadata(src)[0][0]
    assert e.business_unit == "Finance"
    assert e.data_classification == "Confidential"
    assert e.host_region == "US"
    assert e.decommissioned is False        # "N" is a denial, not absence
    assert e.partitioned_columns == ["part_dt"]


def test_column_clustering_reaches_the_record(tmp_path: Path):
    """Which columns a query should filter on to stay affordable: the
    feed states clustering and partition ordinals per column and the
    loader read neither."""
    from sahs.loaders.sources.vocab import load_std_tech_metadata
    src = tmp_path / "std"
    src.mkdir()
    (src / "t.json").write_text(json.dumps({
        "dataset": "gms_transaction",
        "tech_metadata_list": [{"datasetAttribute": {"description": "x"},
                                "pde": [{"pdeRelPath": "part_dt",
                                         "pdeAttribute": {
                                             "is_clustered": "Y",
                                             "cluster_position": 1,
                                             "partition_position": 2,
                                             "attribute_scale": 0,
                                             "publish_code": "P"}}]}]}),
        encoding="utf-8")
    c = load_std_tech_metadata(src)[0][0].columns[0]
    assert c.is_clustered is True
    assert c.cluster_position == 1 and c.partition_position == 2
    assert c.attribute_scale == 0 and c.publish_code == "P"
