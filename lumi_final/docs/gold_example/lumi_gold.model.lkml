################################################################
# Gold-standard LUMI model — what one fully-enriched model file
# should look like. Built per the rubric in docs/GOLD_STANDARD_LOOKML.md.
#
# Three things this file does that auto-gen would not:
#   1) Access grants for column-level PII gating
#   2) Datagroup for cache invalidation tied to the daily ETL
#   3) Clustered explores authored at corpus level, not table level
################################################################

connection: "amex_lumi_bq"
include: "/views/refinements/*.view.lkml"

# ─── Access grants for PII column gating ────────────────────────

access_grant: pii_access {
  user_attribute: has_pii_access
  allowed_values: ["yes"]
}

access_grant: gdpr_access {
  user_attribute: has_gdpr_access
  allowed_values: ["yes"]
}

# ─── Datagroup for cache invalidation ──────────────────────────

datagroup: lumi_daily_refresh {
  sql_trigger: SELECT MAX(rpt_dt) FROM `axp-lumi.dw.custins_customer_insights_cardmember` ;;
  max_cache_age: "24 hours"
  description: "Daily ETL trigger — invalidates cache when cornerstone refresh lands."
}

persist_with: lumi_daily_refresh

# ─── EXPLORE: cardmember_metrics_by_segment ──────────────────
# Corpus cluster: questions like "total BB by business segment last quarter"
# This explore is designed to:
#   1) Win Radix retrieval on spend-by-segment queries (150-250 word description,
#      5 example questions verbatim, synonyms section)
#   2) Generate correct SQL through Looker MCP (every relationship corpus-validated,
#      every join anchored, every filter resolvable)
#   3) Be safe for AmEx governance (access_filter on business_unit, sql_always_where
#      for soft-delete + data source, partition floor)

explore: cardmember_metrics_by_segment {
  label: "Cardmember Metrics by Segment"
  group_label: "Cardmember 360"
  view_name: cornerstone_metrics

  description: "Cardmember-grain spend, accounts-in-force, and tenure metrics by business segment
and product. Use this explore to answer questions about cardmember activity at month-end snapshots,
including:
- What was the total billed business by business segment last quarter?
- How many active cardmembers are in the Consumer segment with FICO score above 740?
- What's the average account tenure by card product for cornerstone cardmembers?
- Show me the trend of unique cardmembers month-over-month for the past year.
- Total billed business by FICO band and card product for the last 6 months.

Grain: one row per cardmember per reporting month (composite PK on cm11 + rpt_dt).
Default filter: data_source = 'cornerstone'; last 90 days unless user overrides.
Joins available: cardmember_dim (many-to-one).
Synonyms: AIF = accounts in force = unique cardmembers; BB = billed business = billings = spend volume;
Cornerstone = the analytical default data source; NAA = new account acquisition (separate explore)."

  # Structural invariants — hidden, immutable, anchored to base view columns only.
  sql_always_where:
    ${cornerstone_metrics.data_source} = 'cornerstone'
    AND ${cornerstone_metrics.rpt_dt} >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)  -- partition floor: cost guardrail
  ;;

  # User-changeable default — date window.
  always_filter: {
    filters: [cornerstone_metrics.report_date: "last 90 days"]
  }

  # Row-level security mapped to user attribute.
  access_filter: {
    field: cornerstone_metrics.business_segment
    user_attribute: allowed_business_segments
  }

  # ── Join: cardmember_dim ──
  # Cardmember is the dim side; cornerstone_metrics is the fact side
  # (many cornerstone rows per cardmember across months).
  # relationship: many_to_one is corpus-validated (every JOIN in 138 gold queries observed).
  join: cardmember_dim {
    view_label: "Cardmember"
    relationship: many_to_one
    type: left_outer
    sql_on: ${cornerstone_metrics.cm11} = ${cardmember_dim.cm11} ;;
    fields: [
      cardmember_dim.cardmember_status,
      cardmember_dim.country,
      cardmember_dim.region,
      cardmember_dim.enrolled_in_membership_rewards
    ]
    # cm11 + cust_xref_id intentionally excluded — already on base view.
  }

  # ── Aggregate table: hot GROUP BY pattern ──
  # Observed in 14+ gold queries: total_billed_business by report_month + business_segment.
  # Materialized rollup answers these in 5% of base-table query time.
  aggregate_table: monthly_bb_by_segment {
    query: {
      dimensions: [cornerstone_metrics.report_month, cornerstone_metrics.business_segment]
      measures: [cornerstone_metrics.total_billed_business, cornerstone_metrics.unique_cardmembers]
      filters: [cornerstone_metrics.data_source: "cornerstone"]
      timezone: "America/New_York"
    }
    materialization: {
      datagroup_trigger: lumi_daily_refresh  # PREFER datagroup over sql_trigger_value
    }
  }
}

# ─── EXPLORE: cardmember_risk_profile (sibling explore) ──────
# Different question cluster: questions about credit risk distribution,
# FICO band trends, account age cohorts. Same base view, different lens.

explore: cardmember_risk_profile {
  label: "Cardmember Risk Profile"
  group_label: "Risk & Fraud"
  view_name: cornerstone_metrics

  description: "Credit risk and tenure profile of cardmembers at month-end snapshots.
Use this explore to answer questions about FICO distributions, risk-band shifts, and
account-age cohorts, including:
- How many cardmembers in each FICO band as of last month?
- What's the average account tenure by FICO band?
- Trend of cardmembers in the Exceptional FICO band over the past year.
- Distribution of cardmembers by FICO band within the Consumer segment.
- Total billed business by FICO band for cornerstone cardmembers.

Grain: one row per cardmember per reporting month.
Default filter: data_source = 'cornerstone'; current month.
Joins available: cardmember_dim (many-to-one) for status + geography.
Synonyms: risk band = FICO band = credit tier; tenure = account age = MOB (months on book)."

  sql_always_where:
    ${cornerstone_metrics.data_source} = 'cornerstone'
    AND ${cornerstone_metrics.rpt_dt} >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
  ;;

  always_filter: {
    filters: [cornerstone_metrics.report_date: "this month"]
  }

  access_filter: {
    field: cornerstone_metrics.business_segment
    user_attribute: allowed_business_segments
  }

  join: cardmember_dim {
    view_label: "Cardmember"
    relationship: many_to_one
    type: left_outer
    sql_on: ${cornerstone_metrics.cm11} = ${cardmember_dim.cm11} ;;
    fields: [
      cardmember_dim.cardmember_status,
      cardmember_dim.country,
      cardmember_dim.region
    ]
  }
}
