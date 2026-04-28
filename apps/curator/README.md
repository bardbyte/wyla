# apps/curator/

A Gemini 3.1 Pro agent that audits gold-query Excel files for the LUMI
NL-to-SQL → LookML pipeline. Replaces the old heuristic-based
`scripts/inspect_excel.py` — instead of regex-scoring columns, this agent
inspects the file using tools and reasons about it like a senior engineer.

The whole agent (system prompt, tools, TLS bootstrap) lives in this
directory — no cross-package imports, no `sys.path` tricks. ADK loads
`agent.py` directly and grabs `root_agent`.

## What it answers

You can ask it any of these in natural language:

- **Is this Golden dataset ready?** — runs structure + SQL spot-checks, gives a verdict.
- **What can be extracted, and how will the extract look?** — describes the schema and shows a sample.
- **Run all the validation checks.** — SQL parse rate, LookML mappability, missing columns.
- **Will these queries map to a LookML view?** — decomposes a sample of SQL into tables/dims/measures/joins.
- **Extract the queries to JSON.** — calls `extract_gold_queries` with the right column names and saves the file.

It calls tools, reasons between calls, and ends every audit with `VERDICT: READY / READY WITH NOTES / NOT READY` plus the supporting evidence.

## How to run

`adk web` is the right entry point — interactive chat, full event trace, can fork.

```bash
# One-time
pip install truststore     # corporate-MITM TLS

# Every session
source agent_test/setup_vertex_env.sh ~/Downloads/key.json
adk web apps/
```

Open `http://localhost:8000`. The sidebar will show **two** apps now:

- `vertex_smoke` — the dice/prime canonical smoke test (kept for reference)
- `curator` — the gold-curator agent (this directory)

Pick `curator`, then start with a message like:

> Audit the Excel at `/Users/me/Downloads/gold_queries.xlsx`. Is it ready?

The agent will list_excel_sheets, preview a few of them, validate sample SQL, and give you a verdict.

### ⚠ Don't use the attach-file button

Type the **file path** in the chat — don't drag/upload the .xlsx through the
paperclip icon. Reasons:

1. Gemini doesn't natively accept `.xlsx` as multimodal input. Attaching
   gets you a `400 Invalid argument: mime type ... not supported` error
   from the model.
2. Our whole tool kit is path-based. The Excel binary stays on your disk;
   only the structured tool outputs (sheet names, markdown previews, SQL
   parse results) ever travel to Gemini. That's faster, smaller, and
   keeps the source data local.

If you do attach by mistake, the agent will respond with a one-line nudge
asking for the path. Just paste the path and continue.

## Tools (what the LLM has access to)

| Tool | Use when |
|---|---|
| `list_excel_sheets(path)` | Always first — discovers what sheets exist. |
| `preview_excel_sheet(path, sheet, n)` | See what the data actually looks like; header names alone are unreliable. |
| `read_excel_rows(path, sheet, start, end)` | Spot-check deeper than a preview lets you. |
| `summarize_excel_columns(path, sheet)` | Per-column stats (distinct counts, samples). |
| `validate_sql(sql, dialect)` | Parse SQL with sqlglot — assess syntactic health. |
| `analyze_for_lookml(sql, dialect)` | Decompose SQL into LookML primitives (tables, dims, measures, joins). |
| `extract_gold_queries(path, sheet, prompt_col, sql_col, ...)` | Final extraction — only after column meanings are confirmed. |

Each tool returns a `dict` with `status: "ok" | "error"` and either payload or an `error` field. The LLM reads docstrings to decide which to call.

## Example conversation

**You:** "Look at `~/Downloads/gold_queries.xlsx` — is it ready to feed into the LUMI pipeline?"

**Agent (after several tool calls):**
> File: ~/Downloads/gold_queries.xlsx (78.4 KB), 1 sheet:
> - `Queries` — 138 rows × 4 cols
>
> Columns identified (from previewing rows 2–6):
> - **A** "ID" — auto-increment integers
> - **B** "User Question" — natural-language prompts; all end with '?'
> - **C** "Expected SQL" — BigQuery SQL; SELECT…FROM patterns confirmed
> - **D** "Difficulty Tier" — small enum: easy / medium / hard
>
> SQL sample validation (5 queries spot-checked, rows 2, 30, 70, 100, 137):
> - 5/5 parse cleanly with sqlglot (BigQuery dialect)
> - Tables touched: `acqdw_acquisition_us`, `custins_customer_insights_cardmember`, `risk_pers_acct_history`
> - 1 query uses CTE + window function (row 100); flag for prompt iteration
>
> LookML readiness (sample):
> - All sample queries decompose cleanly: 1 primary table, 1-3 measures (SUM/COUNT_DISTINCT/AVG), 2-5 dimensions
> - Filter `data_source = 'cornerstone'` appears in 4/5 — likely a default filter for the LookML view
>
> **VERDICT: READY**
> - Schema is clean, SQL parses, LookML decomposition works
> - One row with window function will need extra prompt care during enrichment
> - Recommend extracting now: `extract_gold_queries(..., prompt_column="User Question", sql_column="Expected SQL", difficulty_column="Difficulty Tier", id_column="ID")`

## Why this beats the old heuristic script

The old `scripts/inspect_excel.py` scored columns with regex. It failed on:
- Headers in non-English
- SQL columns mixed with NL columns (e.g., commented SQL)
- Trick cases where header lies about content

The agent doesn't have those failure modes — it reads the actual data and reasons. It also produces explanations + verdicts, which the old tool couldn't.

## Where this fits in LUMI

`gold_curator` is the entry-point auditor for the gold-query input. Once it
produces a clean JSON extract via `extract_gold_queries(..., output_json_path=...)`,
that JSON becomes the input to the downstream enrichment agents (the ones
that combine gold queries + MDM + BigQuery + existing LookML).
