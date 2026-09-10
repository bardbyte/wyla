################################################################
# Refinement file. Auto-generated `cornerstone_metrics.view.lkml`
# lives in views/auto/ and is NEVER edited — it regenerates from
# BQ INFORMATION_SCHEMA. Enrichment lives here so schema regen
# can't wipe it.
#
# Embed string Radix builds per field:
#   "{view.label}: {field.label} — {field.description} — {tags}"
#
# NOTE on `hint:` — not a real LookML parameter. Use `tags` for
# synonym surfaces and `# RADIX_HINT:` comments for AI-only
# context that should not reach the Looker UI.
################################################################

include: "/views/auto/cornerstone_metrics.view.lkml"

view: +cornerstone_metrics {
  label: "Cornerstone Customer Insights"
  # description: shown on hover in the Explore picker. Three sentences:
  # 1) grain, 2) canonical use, 3) scope.
  description: "One row per cardmember per reporting month — point-in-time snapshot.
Supports billed-business, accounts-in-force, spend, tenure, and segment analysis.
Cornerstone source only; for other feeds filter on data_source."

  # ── PRIMARY KEY ─────────────────────────────────────────────
  # Composite PK synthesized into one dim — required for Looker's
  # symmetric_aggregates to dedup correctly under fan-out joins.
  dimension: pk {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${TABLE}.cm11, '|', CAST(${TABLE}.rpt_dt AS STRING)) ;;
  }

  # ── IDENTIFIERS ─────────────────────────────────────────────
  dimension: cm11 {
    # RADIX_HINT: also called cardmember number, CM number, customer ID, cust xref;
    # used as the cardmember-grain join key across every fact in the dw schema.
    type: string
    sql: ${TABLE}.cm11 ;;
    label: "Cardmember ID (CM11)"
    description: "11-digit cardmember identifier — the canonical customer key across all AmEx US Consumer systems. PII role NGBD-SDE-CM11."
    tags: ["cm11", "cardmember", "customer id", "card holder", "account holder"]
    group_label: "Identifiers"
    required_access_grants: [pii_access]
  }

  dimension: customer_xref_id {
    # RADIX_HINT: equivalent to cm11 via proven JOIN closure on customer_master.cust_xref_id
    type: string
    sql: ${TABLE}.cust_xref_id ;;
    label: "Customer Cross-Reference ID"
    description: "Customer cross-reference identifier equivalent to CM11; used by legacy systems."
    tags: ["cust_xref_id", "customer xref", "cardmember", "cm11_alt"]
    group_label: "Identifiers"
    required_access_grants: [pii_access]
  }

  # ── DATE (always dimension_group for dates, never plain dimension) ────────
  dimension_group: report {
    type: time
    timeframes: [raw, date, day_of_week, week, month, month_name, quarter, year, fiscal_year, fiscal_quarter]
    datatype: date
    convert_tz: no  # MANDATORY: BQ stores UTC; default would double-shift.
    sql: ${TABLE}.rpt_dt ;;
    label: "Report Period"
    description: "Month-end snapshot date — each row represents the cardmember's state as of this date. UTC."
    group_label: "Date"
  }

  # ── BUSINESS SEGMENTATION ───────────────────────────────────
  dimension: business_segment {
    # RADIX_HINT: canonical analyst term for bus_seg column; values include Consumer, Commercial, GCS, GNS
    type: string
    sql: ${TABLE}.bus_seg ;;
    label: "Business Segment"
    description: "Business unit segmentation: Consumer, Commercial, Global Commercial Services (GCS), Global Network Services (GNS)."
    tags: ["bus_seg", "business segment", "BU", "segment", "business unit"]
    group_label: "Customer Demographics"
  }

  dimension: data_source {
    type: string
    sql: ${TABLE}.data_source ;;
    label: "Data Source"
    description: "Source feed for the cardmember row — typically 'cornerstone' for the analytical default."
    tags: ["data_source", "feed", "source system"]
    group_label: "Source / Audit"
    hidden: yes  # Structural filter is baked into sql_always_where on the explore; analysts shouldn't query directly.
  }

  # ── CREDIT RISK ─────────────────────────────────────────────
  dimension: fico_band {
    # RADIX_HINT: also called credit tier, credit grade, FICO bucket in NAA reports
    type: string
    sql: CASE
           WHEN ${TABLE}.fico_score >= 800 THEN 'Exceptional (800+)'
           WHEN ${TABLE}.fico_score >= 740 THEN 'Very Good (740-799)'
           WHEN ${TABLE}.fico_score >= 670 THEN 'Good (670-739)'
           WHEN ${TABLE}.fico_score >= 580 THEN 'Fair (580-669)'
           WHEN ${TABLE}.fico_score IS NULL THEN 'Unscored'
           ELSE 'Poor (<580)'
         END ;;
    label: "FICO Band"
    description: "FICO credit-score band: Exceptional, Very Good, Good, Fair, Poor, or Unscored."
    tags: ["fico", "credit score", "credit band", "credit tier", "credit grade", "risk band"]
    group_label: "Credit Risk"
    order_by_field: fico_band_sort  # Without this, alphabetical sort gives wrong order.
  }

  dimension: fico_band_sort {
    hidden: yes
    type: number
    sql: CASE
           WHEN ${TABLE}.fico_score >= 800 THEN 1
           WHEN ${TABLE}.fico_score >= 740 THEN 2
           WHEN ${TABLE}.fico_score >= 670 THEN 3
           WHEN ${TABLE}.fico_score >= 580 THEN 4
           WHEN ${TABLE}.fico_score IS NULL THEN 6
           ELSE 5
         END ;;
  }

  dimension: fico_score {
    type: number
    sql: ${TABLE}.fico_score ;;
    label: "FICO Score"
    description: "Raw FICO credit score numeric value (300-850). Use FICO Band for grouping."
    tags: ["fico", "fico score", "credit score numeric"]
    group_label: "Credit Risk"
    value_format_name: decimal_0
  }

  # ── PRODUCT ─────────────────────────────────────────────────
  dimension: card_product {
    type: string
    sql: ${TABLE}.card_product ;;
    label: "Card Product"
    description: "Card product code (e.g. Platinum, Gold, Green, Blue Cash) — the specific product the cardmember holds."
    tags: ["product", "card product", "card type", "card name"]
    group_label: "Product"
  }

  dimension: account_age_months {
    type: number
    sql: ${TABLE}.account_age_mth ;;
    label: "Account Age (Months)"
    description: "Tenure of the cardmember's account in months as of the reporting date."
    tags: ["tenure", "account age", "months on book", "MOB"]
    group_label: "Customer Demographics"
    value_format_name: decimal_0
  }

  # ── MEASURES ────────────────────────────────────────────────
  measure: count {
    type: count
    label: "Number of Records"
    description: "Count of cardmember-month rows in the result set."
    value_format_name: decimal_0
    drill_fields: [cornerstone_metrics_detail*]
  }

  measure: unique_cardmembers {
    type: count_distinct
    sql: ${TABLE}.cm11 ;;
    label: "Unique Cardmembers"
    description: "Count of distinct cardmember identifiers (CM11) in the result set — Accounts In Force (AIF)."
    tags: ["AIF", "accounts in force", "unique customers", "active cardmembers"]
    value_format_name: decimal_0
    drill_fields: [cornerstone_metrics_detail*]
  }

  measure: total_billed_business {
    type: sum
    sql: ${TABLE}.billed_business ;;
    label: "Total Billed Business"
    description: "Sum of cardmember spend volume in USD for the period. Also known as billings or BB."
    tags: ["billed_business", "billings", "BB", "volume", "spend", "gross billings", "card spend"]
    value_format_name: usd_0
    group_label: "Spend"
    drill_fields: [cornerstone_metrics_detail*]
  }

  measure: total_billed_business_cornerstone {
    # Filtered measure — bakes the >80%-frequency filter into the measure itself.
    type: sum
    sql: ${TABLE}.billed_business ;;
    filters: [data_source: "cornerstone"]
    label: "Total Billed Business (Cornerstone)"
    description: "Sum of billed business volume in USD, restricted to the Cornerstone data source — the default analytical slice."
    tags: ["billings cornerstone", "BB cornerstone", "cornerstone spend"]
    value_format_name: usd_0
    group_label: "Spend"
  }

  measure: avg_billed_business_per_cardmember {
    type: number
    sql: ${total_billed_business} / NULLIF(${unique_cardmembers}, 0) ;;
    label: "Average Billed Business per Cardmember"
    description: "Ratio of total billings to unique cardmembers — average spend per card holder for the period."
    tags: ["avg spend", "spend per cardmember", "average billings", "ASB"]
    value_format_name: usd_0
    group_label: "Spend"
  }

  measure: total_transactions {
    type: sum
    sql: ${TABLE}.txn_count ;;
    label: "Total Transactions"
    description: "Sum of transaction counts across cardmember-months in the result set."
    tags: ["transactions", "txn count", "charges", "swipes"]
    value_format_name: decimal_0
    group_label: "Spend"
  }

  measure: avg_account_age_months {
    type: average
    sql: ${TABLE}.account_age_mth ;;
    label: "Average Account Age (Months)"
    description: "Average tenure of cardmembers in months as of the reporting date."
    tags: ["avg tenure", "average MOB", "average account age"]
    value_format_name: decimal_1
    group_label: "Customer Demographics"
  }

  # ── SETS (drill-through targets, PII grouping) ───────────────
  set: cornerstone_metrics_detail {
    fields: [cm11, business_segment, card_product, fico_band, report_month, total_billed_business, unique_cardmembers]
  }

  set: pii_fields {
    fields: [cm11, customer_xref_id]
  }
}
