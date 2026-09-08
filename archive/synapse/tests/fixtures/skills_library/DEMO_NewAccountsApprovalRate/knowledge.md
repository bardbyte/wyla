# New Accounts — Approval Rate

## Grain

One row per application. The application key is `na_pcn_no`; a single
customer can hold multiple applications, so application-level counts and
customer-level counts are different questions.

## Definitions

- **Gross approval rate** = distinct approved applications / distinct
  decisioned applications in the window.
- **Credit approval rate** excludes fraud declines from the denominator —
  it measures the credit policy, not the fraud screen.
- Decision mix = share of approve / decline / pend outcomes per month.

## Edge cases

- Applications with `decision_cd = 'P'` (pended) are NOT decisioned;
  exclude them from both numerator and denominator until resolved.
- A re-decisioned application keeps its original `na_pcn_no`; use the
  latest decision per key.

## Rules

- Never count rows for approval metrics — always `COUNT(DISTINCT na_pcn_no)`.
- Never average monthly rates to get a quarterly rate; recompute from the
  summed numerator and denominator.
- Do not expose `cm11_encrypted` in any output, ever.
