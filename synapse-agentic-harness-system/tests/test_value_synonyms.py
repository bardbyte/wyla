"""Mined value readings — `1` means "KYC done".

The warehouse profile says which values a column holds; this index
says what they mean. It is MINED, so it never outranks an authored
description and never creates schema."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.graph.crosswalk import Crosswalk                # noqa: E402
from sahs.graph.quads import GraphDir, NodeRecord, Prov   # noqa: E402
from sahs.loaders.quads_emit import emit_value_synonyms   # noqa: E402
from sahs.loaders.sources.value_synonyms import (         # noqa: E402
    load_value_synonyms,
)

FX = SILO / "tests" / "fixtures" / "sources"


def _crosswalk(tmp_path: Path) -> Crosswalk:
    cw = tmp_path / "crosswalk.jsonl"
    cw.write_text(json.dumps({"physical": "dw.gms_transaction",
                              "verified_by": "t",
                              "verified_on": "2026-09-02"}) + "\n",
                  encoding="utf-8")
    return Crosswalk.load(cw)


def test_stored_values_keep_their_exact_spelling(tmp_path: Path):
    """"0", "001", "01", "Y" and "None" are five distinct stored
    values. Coercing any to a number or to null destroys the mapping,
    and "None" is a value a column HOLDS, never an absence."""
    src = tmp_path / "idx.json"
    src.write_text(json.dumps({"value lookup": {"dw.t": {
        "0": [{"column": "c", "synonym": "zero"}],
        "001": [{"column": "c", "synonym": "oh-oh-one"}],
        "01": [{"column": "c", "synonym": "oh-one"}],
        "Y": [{"column": "c", "synonym": "yes"}],
        "None": [{"column": "c", "synonym": "explicitly none"}],
    }}}), encoding="utf-8")
    records, quarantined = load_value_synonyms(src)
    assert not quarantined and len(records) == 1
    synonyms = records[0]["synonyms"]
    assert set(synonyms) == {"0", "001", "01", "Y", "None"}
    assert all(isinstance(k, str) for k in synonyms)
    assert synonyms["None"] == ["explicitly none"]


def test_one_value_may_carry_several_readings(tmp_path: Path):
    records, _ = load_value_synonyms(FX
                                     / "low_cardinality_synonyms_index"
                                       ".json")
    by_col = {(r["table"], r["column"]): r for r in records}
    country = by_col[("dw.gms_transaction", "country_cd")]
    assert country["synonyms"]["US"] == ["United States"]
    # both readings survive, in document order
    assert country["synonyms"]["0"] == ["Unclassified", "Not Provided"]


def test_mined_readings_never_create_schema(tmp_path: Path):
    """The BQ profile mints a column it did not expect, because
    OBSERVING a value proves the column exists. A mined reading proves
    nothing of the kind: a column nobody declared is counted, not
    minted, so a phantom never reaches a card."""
    graph = GraphDir(tmp_path / "graph")
    graph.append_node(NodeRecord(
        id="col:dw.gms_transaction.country_cd", props={},
        prov=Prov(source="bq", run="r0", evidence="e")))
    records, _ = load_value_synonyms(
        FX / "low_cardinality_synonyms_index.json")
    report = emit_value_synonyms(records, graph, _crosswalk(tmp_path),
                                 "r1")
    assert report["columns"] == 1
    assert report["skipped_unknown_column"] == 1      # phantom column
    assert report["skipped_unresolvable_table"] == 1  # unknown table
    ids = {n.id for n in graph.iter_nodes()}
    assert "col:dw.gms_transaction.never_declared_col" not in ids

    # the reading rides ON the profile's domain node, as its own prop
    folded = graph.fold_nodes()["domain:dw.gms_transaction.country_cd"]
    assert folded.props["synonyms"]["US"] == ["United States"]
    # and it is marked mined, so nothing ranks it as authored
    witnesses = {q.prov.witness for q in graph.iter_edges("has_domain")}
    assert witnesses == {"catalog_mined"}


def test_a_shape_it_cannot_read_quarantines_loudly(tmp_path: Path):
    src = tmp_path / "idx.json"
    src.write_text(json.dumps({"something_else": [1, 2, 3]}),
                   encoding="utf-8")
    records, quarantined = load_value_synonyms(src)
    assert not records and len(quarantined) == 1
    assert "value-lookup object" in quarantined[0].detail
