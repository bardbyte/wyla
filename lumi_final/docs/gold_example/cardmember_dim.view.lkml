################################################################
# Cardmember dimension — joined into multiple explores as a dim.
# Refinement file pattern.
################################################################

include: "/views/auto/cardmember_dim.view.lkml"

view: +cardmember_dim {
  label: "Cardmember"
  description: "Point-in-time cardmember attributes — demographics, contact info, status.
One row per cardmember. Use this view when you need cardmember-level attributes
not present on a fact view; never use for spend or transaction questions."

  dimension: cm11 {
    primary_key: yes
    type: string
    sql: ${TABLE}.cm11 ;;
    label: "Cardmember ID (CM11)"
    description: "11-digit cardmember identifier — the canonical customer key. PII role NGBD-SDE-CM11."
    tags: ["cm11", "cardmember", "customer id", "card holder"]
    group_label: "Identifiers"
    required_access_grants: [pii_access]
  }

  dimension: cust_xref_id {
    type: string
    sql: ${TABLE}.cust_xref_id ;;
    label: "Customer Cross-Reference ID"
    description: "Customer cross-reference identifier equivalent to CM11; used by legacy systems."
    tags: ["cust_xref_id", "customer xref"]
    group_label: "Identifiers"
    required_access_grants: [pii_access]
    hidden: yes  # Equivalent to cm11; surface cm11 to analysts.
  }

  dimension: cardmember_status {
    type: string
    sql: ${TABLE}.cm_status ;;
    label: "Cardmember Status"
    description: "Current status: Active, Closed, Cancelled, Suspended, Probationary."
    tags: ["status", "cardmember status", "account status"]
    group_label: "Customer Demographics"
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country_cd ;;
    label: "Country"
    description: "Cardmember's country of residence (ISO 2-letter code)."
    tags: ["country", "country code", "geo"]
    group_label: "Geography"
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region_cd ;;
    label: "Region"
    description: "Cardmember's region (US-East, US-West, Europe, APAC, etc.)."
    tags: ["region", "region code"]
    group_label: "Geography"
  }

  dimension: business_unit {
    type: string
    sql: ${TABLE}.business_unit_cd ;;
    label: "Business Unit"
    description: "Cardmember's home business unit — used for row-level access control."
    tags: ["business unit", "BU"]
    group_label: "Customer Demographics"
  }

  dimension: enrolled_in_membership_rewards {
    type: yesno
    sql: ${TABLE}.mr_enrolled_flg = 'Y' ;;
    label: "Enrolled In Membership Rewards"
    description: "Yes if the cardmember is enrolled in Membership Rewards, No otherwise."
    tags: ["MR", "membership rewards", "rewards enrolled"]
    group_label: "Engagement"
  }

  measure: unique_cardmembers {
    type: count_distinct
    sql: ${cm11} ;;
    label: "Unique Cardmembers"
    description: "Count of distinct cardmembers (AIF)."
    tags: ["AIF", "accounts in force", "unique cardmembers"]
    value_format_name: decimal_0
  }
}
