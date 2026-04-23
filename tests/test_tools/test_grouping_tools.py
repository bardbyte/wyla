from __future__ import annotations

from pathlib import Path

from lumi.tools.excel_tools import parse_excel_to_json
from lumi.tools.grouping_tools import extract_join_graphs, group_queries_by_view


def test_group_by_primary_table(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    g = group_queries_by_view(result["queries"])
    assert g["status"] == "success"
    by_view = g["queries_by_view"]
    # All 5 fixture queries have primary table acqdw_acquisition_us (query 4 uses alias 'a').
    assert "acqdw_acquisition_us" in by_view
    assert len(by_view["acqdw_acquisition_us"]) == 5


def test_filter_defaults_detects_cornerstone(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    g = group_queries_by_view(result["queries"])
    defaults = g["filter_defaults"]["acqdw_acquisition_us"]
    # All 5 fixture queries filter data_source='cornerstone'
    assert defaults.get("data_source", "").strip("'") == "cornerstone"


def test_field_frequency_counts_right(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    g = group_queries_by_view(result["queries"])
    freq = g["field_frequency"]["acqdw_acquisition_us"]
    # data_source appears in every query
    assert freq.get("data_source", 0) == 5


def test_grouping_by_view_name(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    mapping = {"acquisition_us": "acqdw_acquisition_us"}
    g = group_queries_by_view(result["queries"], view_name_to_table=mapping)
    assert "acquisition_us" in g["queries_by_view"]


def test_join_graph_dedup(sample_queries_xlsx: Path) -> None:
    result = parse_excel_to_json(str(sample_queries_xlsx))
    jg = extract_join_graphs(result["queries"])
    assert jg["status"] == "success"
    # Only one of the 5 queries has a join
    patterns = jg["patterns"]
    assert len(patterns) == 1
    assert set(patterns[0].tables) >= {"a", "c"}
