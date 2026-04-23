from __future__ import annotations

from pathlib import Path

from lumi.schemas import EnrichedField, EnrichedView
from lumi.tools.lookml_tools import batch_fields, parse_lookml_file, write_lookml_files


def test_parse_sample_view(sample_view_lkml: Path) -> None:
    r = parse_lookml_file(sample_view_lkml)
    assert r["status"] == "success"
    v = r["parsed_view"]
    assert v.view_name == "acqdw_acquisition_us"
    assert v.field_count >= 7
    names = {f.name for f in v.fields}
    assert {"account_id", "fico_score", "fico_band", "new_accounts_acquired"} <= names


def test_batch_fields_single_batch_when_under_threshold(sample_view_lkml: Path) -> None:
    v = parse_lookml_file(sample_view_lkml)["parsed_view"]
    batches = batch_fields(v, field_threshold=150, batch_size=30)
    assert len(batches) == 1
    assert len(batches[0]) == v.field_count


def test_batch_fields_splits_when_over_threshold(sample_view_lkml: Path) -> None:
    v = parse_lookml_file(sample_view_lkml)["parsed_view"]
    batches = batch_fields(v, field_threshold=1, batch_size=3)
    assert len(batches) >= 2
    total = sum(len(b) for b in batches)
    assert total == v.field_count


def test_batch_fields_respects_dependency_order(sample_view_lkml: Path) -> None:
    v = parse_lookml_file(sample_view_lkml)["parsed_view"]
    # fico_band references ${TABLE}.fico_score in sql — not a field ref, so no dep.
    # account_id is referenced by new_accounts_acquired via ${account_id}.
    batches = batch_fields(v, field_threshold=1, batch_size=2)
    flat = [n for b in batches for n in b]
    assert flat.index("account_id") < flat.index("new_accounts_acquired")
    assert flat.index("billed_business") < flat.index("total_billed_business")


def test_missing_file() -> None:
    r = parse_lookml_file(Path("/tmp/definitely_not_here.view.lkml"))
    assert r["status"] == "error"


def test_write_enriched(tmp_path: Path) -> None:
    ev = EnrichedView(
        view_name="v",
        view_label="V",
        view_description="A test view.",
        fields=[
            EnrichedField(
                name="x",
                kind="dimension",
                type="string",
                sql="${TABLE}.x",
                label="X",
                description="A field.",
                origin="mdm",
                tags=["x_tag"],
            ),
            EnrichedField(
                name="c",
                kind="measure",
                type="count",
                sql="${x}",
                label="Count",
                description="Row count.",
                origin="gold_query",
            ),
        ],
    )
    r = write_lookml_files({"v": ev}, tmp_path)
    assert r["status"] == "success"
    out = (tmp_path / "v.view.lkml").read_text()
    assert "view: v" in out
    assert "dimension: x" in out
    assert "measure: c" in out
    assert 'description: "A field."' in out
