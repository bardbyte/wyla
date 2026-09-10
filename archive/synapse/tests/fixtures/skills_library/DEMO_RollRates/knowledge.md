# Portfolio Analytics — Roll Rates

## Grain

`common.roll_rate_calc` is account-month grain with pre-computed lag
columns (`bal_lag1`, `bal_lag2`, `status_lag1`, ...). The lags are
materialized upstream by the ETL.

## Definitions

- **C-30 rate**: share of current balances that roll to 30+ DPD next month.
- **C-60 rate**: current → 60+ over two months; uses the t-2 profile.
- Dollar rates weight by balance; event (AIF) rates count accounts.

## Rules

- Never use SQL LAG() over this table — the lag columns are already there;
  applying LAG() again double-shifts the window and silently corrupts rates.
- Never mix dollar and event numerators/denominators in one ratio.
- Point-in-time KPIs (60+ share) use same-month segments; transition KPIs
  use lagged segments.
