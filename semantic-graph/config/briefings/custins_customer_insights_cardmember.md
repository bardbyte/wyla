# custins_customer_insights_cardmember — steward briefing

Ported from the original single-table build's curated capsule; keep it
current as the table evolves.

- **Type**: `VIEW`, not a base table. Wraps
  `data.custins_customer_insights_cardmember` with row-level security
  via ONCOP keys (`security.user_fin_oncop`, `security.s_users_map`).
- **Grain**: one row per `cm11` (11-digit cardmember account ID) ×
  `rpt_dt` (report date / model execution month).
- **Width**: ~190 columns. ~147 are FLOAT64 financial P&L metrics; ~35
  are STRING dimensional/identifier columns; the rest are integer keys,
  dates, timestamps, and `lumi_*` pipeline metadata.
- **Account vs customer**: `cm11` is ACCOUNT-level. `cust_xref_id` is
  the 12-digit CUSTOMER-level key — one customer can hold multiple
  accounts. Any "how many customers …" question must count
  `cust_xref_id`, never `cm11`.
- **Security**: RLS via ONCOP — service accounts without ONCOP keys see
  zero rows. The schema is queryable; the data is not.
- **Usage**: ~100 queries / 90 days, primarily US business hours.
- **Known analyst mistakes**:
  - saying `fico` when the column is `fico_score`
  - saying `card_product_id` when the column is `card_prod_id`
  - treating the view as a base table (partition/DDL assumptions fail)
