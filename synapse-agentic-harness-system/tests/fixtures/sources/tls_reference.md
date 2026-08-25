# TLS Booking, Ticket Counts, and Sales — Semantic Reference (trimmed fixture)
## 1. Gross / Cancelled / Net
Net = Gross - Cancelled for both counts and sales.
## 2. Booking Counts
Always COUNT(DISTINCT trip_id) — never row counts.
## 3. Sales
trans_usd_am with sign convention (ABS() for cancellations).
## 4. Date rules
Gross → rpt_dt; Cancelled → canc_dt.
## 5. Hotel data-quality rule
Mandatory exclusion of ABS(trans_usd_am) > 100000 for all Hotel sales.
