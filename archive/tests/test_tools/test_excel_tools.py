from __future__ import annotations

from pathlib import Path

from lumi.tools.excel_tools import parse_excel_to_json


def test_parse_fixture_ok(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    assert result["status"] == "success"
    queries = result["queries"]
    assert len(queries) == 5
    assert result["parse_errors"] == 0

    ids = [q.query_id for q in queries]
    assert all(i.startswith("q_") for i in ids)


def test_first_query_is_single_table_count_distinct(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    q = result["queries"][0]
    assert q.primary_table == "acqdw_acquisition_us"
    assert any(m.function == "COUNT" and m.distinct for m in q.measures)
    assert any(f.column == "data_source" and f.value.strip("'") == "cornerstone" for f in q.filters)


def test_case_when_dim_and_group_by(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    q = result["queries"][1]  # fico_band CASE WHEN
    assert any(m.function == "SUM" and m.column == "billed_business" for m in q.measures)
    # fico_band is derived; we still pick it up as a dimension alias from the GROUP BY.
    assert "fico_band" in q.dimensions


def test_join_detected(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    q = result["queries"][3]
    assert len(q.joins) == 1
    j = q.joins[0]
    assert j.left_table == "a"
    assert j.right_table == "c"
    assert j.join_type.lower() == "left"


def test_missing_file() -> None:
    r = parse_excel_to_json("nope.xlsx")
    assert r["status"] == "error"
    assert "not found" in r["error"].lower()


def test_missing_column(tmp_path: Path) -> None:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.append(["q", "sql"])
    ws.append(["x", "SELECT 1"])
    p = tmp_path / "bad.xlsx"
    wb.save(p)

    r = parse_excel_to_json(str(p))
    assert r["status"] == "error"
    assert "user_prompt" in r["error"]
