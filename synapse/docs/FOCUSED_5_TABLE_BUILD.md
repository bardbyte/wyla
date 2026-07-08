# Focused 5-table build — the runbook

Regress the graph to five join-connected tables and make them fully
grounded — at par with, or better than, the original single-table
cardmember build. Same builder, same calibrator; the difference is scope
(a manifest allowlist) and witness density (every witness fired for the
five, including the two — DQ + lineage — that fired nowhere before).

## The five

`config/tables_focus5.yaml`:

- `custins_customer_insights_cardmember` — cardmember-day P&L fact
- `fin_consumer_business_card_member_status` — account status / lifecycle
- `risk_pers_acct` — account-level risk (current)
- `risk_pers_acct_history` — account-level risk (history)
- `risk_indv_cust_hist` — customer-level risk (history)

They share the account key and the customer key, so the build demonstrates
shared entities, cross-table join paths, and lineage a single table can't.

## Why this grounds (the arithmetic)

**MDM is the top data authority** (weight 8 — it carries the business
names, ownership, and sensitivity for every column), **BQ second**
(weight 6 — the physical truth). With the new weighting the two sources
that "have all the data" ground a fact together:

| witnesses on a fact | tier |
|---|---|
| `mdm` alone | inferred |
| **`mdm` + `bq`** | **grounded (0.93)** |
| `bq` + `corpus` | inferred → + `dq_engine` = grounded |
| `llm_generated` alone | guessed (capped — never grounds on its own) |

Four mechanical changes do the work, all in the builder:

1. **MDM-first weighting** — MDM highest, BQ second; a column both
   witness means grounds. This mirrors the original cardmember build,
   where MDM + BQ + corpus fused to grounded.
2. **Skills are out of the data graph** — a skill "applying to" a table
   no longer witnesses that table's tier (it used to, at weight 7,
   *outranking* MDM). Skills remain as **guardrails** (security) and
   **agent knowledge** (`get_skill`); guardrail enforcement is name-based
   so it needs no Table node.
3. **Allowlist** (`build_graph_from_sources(..., allowlist=…)`, wired to
   `--manifest`) prunes everything outside the five — including the CTE
   aliases / placeholders (`base`, `the`,
   `your_project.your_dataset.source_table`) SQL parsing would mint as
   tables. The 5-table graph is exactly the five.
4. **Auto-DQ** (`_synthesize_dq_from_profile`) derives DQ rules from the
   BQ profile and records `dq_engine` as a witness — it lifts any column
   BQ profiled but MDM missed, and adds the DQ-quality signal. No BQ
   profile, no rule; nothing invented.

Plus the **PII fix**: BQ policy tags now only *confirm* PII, never
overwrite MDM's flag with `False` — which is why the manifest build used
to report zero PII.

## Run it (laptop, with creds)

```bash
MANIFEST=synapse/config/tables_focus5.yaml

# 1. BQ extraction for the five (risk_indv_cust_hist is the one that
#    was still missing a profile — the profile is what Auto-DQ needs)
python semantic-graph/scripts/bq_batch_extract.py --tables "$MANIFEST"

# 2. Build scoped: manifest is BOTH the extraction scope AND the builder
#    allowlist, so only these five (and their real columns) are minted.
python synapse/scripts/pipeline.py \
    --manifest "$MANIFEST" \
    --mdm-crawl \
    --enrich            # Gemini enrichment for descriptions (optional)

# 3. Verify — expect ~5/5 tables grounded, high % of columns grounded,
#    "5/5 real tables profiled", PII > 0, zero junk suspects
python synapse/scripts/graph_probe.py
```

## What "done" looks like

Re-run the probe and check:

- **`% grounded+`** and **`% of columns`** climb far past the broad build's
  1.6% — toward the old cardmember build's 50%+, across all five.
- **`bq coverage: 5/5 real tables profiled`** (the lever — no table left
  at `inferred` for want of a profile).
- **`junk?`** line absent (allowlist did its job).
- **PII > 0** on cardmember / risk tables (the clobber fix).
- **`--table` gold-standard check** shows entities and metrics on each.

Paste the probe's `PASTE THIS BACK` JSON into the chat and I'll confirm
the five are at or beyond cardmember-grade, table by table.

## If a table still lags

The probe names, per thin table, the **exact missing witnesses** and the
tier it *would* reach once staged. Usually it's a missing BQ profile
(re-run step 1 for that table) or an absent `usage`/`corpus` signal.
Curated `entities.yaml` / `metric_catalog.csv` / `glossary.csv` entries
for the five add further witnesses and lift borderline columns.
