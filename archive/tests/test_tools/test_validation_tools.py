from __future__ import annotations

from pathlib import Path

from lumi.schemas import EnrichedField, EnrichedView
from lumi.tools.excel_tools import parse_excel_to_json
from lumi.tools.grouping_tools import extract_join_graphs
from lumi.tools.validation_tools import validate_coverage


def _make_enriched_view(
    dims: list[str], measures: list[tuple[str, str, str]]
) -> EnrichedView:
    """measures: list of (name, lkml_type, sql_body)."""
    fields = [
        EnrichedField(
            name=n,
            kind="dimension",
            label=n.replace("_", " ").title(),
            description=f"desc for {n}",
            origin="gold_query",
        )
        for n in dims
    ] + [
        EnrichedField(
            name=n,
            kind="measure",
            type=lkml_type,
            sql=sql,
            label=n.replace("_", " ").title(),
            description=f"measure {n}",
            origin="gold_query",
        )
        for n, lkml_type, sql in measures
    ]
    return EnrichedView(
        view_name="acquisition_us",
        view_label="Acquisition US",
        view_description="Accounts acquired; default filter data_source='cornerstone'.",
        fields=fields,
    )


def test_full_coverage_when_all_fields_present(sample_queries_xlsx: Path) -> None:
    queries = parse_excel_to_json(str(sample_queries_xlsx))["queries"]
    jg = extract_join_graphs(queries)

    full = _make_enriched_view(
        dims=[
            "account_id",
            "data_source",
            "acquisition_date",
            "fico_score",
            "fico_band",
            "billed_business",
        ],
        measures=[
            ("new_accounts_acquired", "count_distinct", "${account_id}"),
            ("total_billed_business", "sum", "${billed_business}"),
            ("avg_fico", "average", "${fico_score}"),
            ("max_billed_business", "max", "${billed_business}"),
            ("accounts", "count_distinct", "${account_id}"),
        ],
    )

    r = validate_coverage(
        gold_queries=queries,
        enriched_views={"acquisition_us": full},
        view_name_to_table={"acquisition_us": "acqdw_acquisition_us"},
        explore_patterns=jg["patterns"],
    )
    report = r["report"]
    # Four single-table queries should pass outright.
    assert report.passed >= 4
    # The joined query may be partial (missing explore covers 'a'/'c' aliases).
    assert report.passed + report.partial + report.failed == report.total_queries


def test_missing_measure_is_flagged(sample_queries_xlsx: Path) -> None:
    queries = parse_excel_to_json(str(sample_queries_xlsx))["queries"]
    # Remove billed_business so SUM(billed_business) fails.
    ev = _make_enriched_view(
        dims=["account_id", "data_source", "fico_band", "acquisition_date", "fico_score"],
        measures=[("new_accounts_acquired", "count_distinct", "${account_id}")],
    )
    r = validate_coverage(
        gold_queries=queries,
        enriched_views={"acquisition_us": ev},
        view_name_to_table={"acquisition_us": "acqdw_acquisition_us"},
    )
    failures = r["report"].failures
    reasons = {f.reason for f in failures}
    assert "missing_measure" in reasons or "missing_dimension" in reasons


def test_parse_error_counted() -> None:
    from lumi.schemas import ParsedQuery

    bad = ParsedQuery(
        query_id="q_9999",
        user_prompt="bad",
        expected_sql="NOT SQL",
        parse_error="sqlglot boom",
    )
    r = validate_coverage(
        gold_queries=[bad],
        enriched_views={},
        view_name_to_table={},
    )
    assert r["report"].failed == 1
    assert r["report"].failures[0].reason == "parse_error"
