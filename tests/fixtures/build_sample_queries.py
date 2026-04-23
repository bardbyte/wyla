"""Generates sample_queries.xlsx used by tests. Run once:
    python tests/fixtures/build_sample_queries.py
The generated file is committed under tests/fixtures/ so tests don't depend on
openpyxl availability in CI (the generator does; the consumer just reads).
"""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

ROWS = [
    (
        "How many new accounts did we acquire from cornerstone last month?",
        """
        SELECT COUNT(DISTINCT account_id) AS naa
        FROM acqdw_acquisition_us
        WHERE data_source = 'cornerstone'
          AND acquisition_date >= '2026-03-01'
        """,
        "easy",
    ),
    (
        "What is the total billed business by FICO band from cornerstone?",
        """
        SELECT
          CASE
            WHEN fico_score >= 800 THEN 'Super Prime'
            WHEN fico_score >= 740 THEN 'Prime'
            WHEN fico_score >= 670 THEN 'Near Prime'
            ELSE 'Subprime'
          END AS fico_band,
          SUM(billed_business) AS total_billed_business
        FROM acqdw_acquisition_us
        WHERE data_source = 'cornerstone'
        GROUP BY fico_band
        """,
        "medium",
    ),
    (
        "Average FICO score of cardmembers acquired in Q1 2026",
        """
        SELECT AVG(fico_score) AS avg_fico
        FROM acqdw_acquisition_us
        WHERE data_source = 'cornerstone'
          AND acquisition_date BETWEEN '2026-01-01' AND '2026-03-31'
        """,
        "easy",
    ),
    (
        "Accounts acquired joined with customer insights — count by FICO band",
        """
        SELECT
          a.fico_band,
          COUNT(DISTINCT a.account_id) AS accounts
        FROM acqdw_acquisition_us AS a
        LEFT JOIN custins_customer_insights_cardmember AS c
          ON a.account_id = c.account_id
        WHERE a.data_source = 'cornerstone'
        GROUP BY a.fico_band
        """,
        "hard",
    ),
    (
        "Max billed business",
        "SELECT MAX(billed_business) FROM acqdw_acquisition_us WHERE data_source = 'cornerstone'",
        "easy",
    ),
]


def build(target: Path) -> None:
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "queries"
    ws.append(["user_prompt", "expected_query", "difficulty"])
    for prompt, sql, difficulty in ROWS:
        ws.append([prompt.strip(), sql.strip(), difficulty])
    target.parent.mkdir(parents=True, exist_ok=True)
    wb.save(target)


if __name__ == "__main__":
    out = Path(__file__).parent / "sample_queries.xlsx"
    build(out)
    print(f"Wrote {out}")
