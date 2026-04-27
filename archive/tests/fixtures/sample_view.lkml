view: acqdw_acquisition_us {
  sql_table_name: `prj.dataset.acqdw_acquisition_us` ;;

  dimension: account_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.account_id ;;
  }

  dimension: data_source {
    type: string
    sql: ${TABLE}.data_source ;;
  }

  dimension: acquisition_date {
    type: date
    sql: ${TABLE}.acquisition_date ;;
  }

  dimension: fico_score {
    type: number
    sql: ${TABLE}.fico_score ;;
  }

  dimension: fico_band {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.fico_score >= 800 THEN 'Super Prime'
        WHEN ${TABLE}.fico_score >= 740 THEN 'Prime'
        WHEN ${TABLE}.fico_score >= 670 THEN 'Near Prime'
        ELSE 'Subprime'
      END ;;
  }

  dimension: billed_business {
    type: number
    sql: ${TABLE}.billed_business ;;
  }

  measure: new_accounts_acquired {
    type: count_distinct
    sql: ${account_id} ;;
    description: "NAA — distinct accounts acquired."
  }

  measure: total_billed_business {
    type: sum
    sql: ${billed_business} ;;
  }
}
