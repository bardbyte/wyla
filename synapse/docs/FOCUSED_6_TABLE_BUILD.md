# Focused 6-table build — the laptop runbook

Build the graph for six join-connected tables the cardmember way:
**session1 (lumi) + MDM (the spine) + BQ (enhance, every signal) →
grounded, scoped to the six**, with entities auto-understood and skills
kept in a file registry (not the graph).

## The six (`synapse/config/tables_focus6.yaml`)

`custins_customer_insights_cardmember` · `fin_consumer_business_card_member_status`
· `risk_new_acct` · `risk_pers_acct` · `risk_pers_acct_history` ·
`risk_indv_cust_hist`

They share the account key (cm11-class) and the customer key
(cust_xref_id-class), so the build shows shared entities, join paths, and
lineage a single table can't.

## What the build produces

- **Graph = warehouse truth.** MDM (authority) + BQ (enhances) + corpus +
  auto-DQ + the **metric registry** + lineage. MDM+BQ agreeing grounds a
  fact. Only the six survive (the manifest is the builder allowlist — CTE
  and placeholder junk is pruned).
- **BQ contributes every signal it extracts, not just columns.** The
  builder now promotes the full extract: declared **primary keys** (the
  grain) and **foreign keys** (which become walkable `EQUIVALENT_TO` join
  edges — the strongest join signal there is), per-column **min/max/avg**
  ranges, the exact **DDL**, physical footprint (row count, size,
  created/modified, partition grain), BQ **labels**, and governance flags
  (row-access policy, streaming buffer). Failed queries + null
  co-occurrence are carried as enricher evidence.
- **Entities auto-understood** at `inferred` (one table is enough; steward
  *upgrades* to `human_asserted`, never required to create). Needs `--enrich`.
- **PII on the graph**, correct — the `"Y"/"N"` + clobber bugs are fixed.
- **Skills → a file registry** (`sources/skills`), never the graph.
  Guardrails enforced from it; `get_skill` serves the full business logic.

## Steps

### 0 · Pull main

```bash
git checkout main && git pull origin main
```

### 1 · Credentials (env)

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
export GOOGLE_CLOUD_PROJECT=<vertex-project>
export BQ_BILLING_PROJECT=prj-p-lumi-gpt
export SYNAPSE_MDM_BASE=<mdm-api-base-url>     # for the live MDM crawl
# export GEMINI_TLS_INSECURE=1                 # only if on the corp intranet, as before
```

### 2 · BQ extraction for the six (the profile BQ enhances with)

```bash
python semantic-graph/scripts/bq_batch_extract.py \
  --config synapse/config/tables_focus6.yaml \
  --output-dir ~/bq_extract_focus6
```

Every section the extractor writes is now used — constraints (`1_5`),
min/max/avg (`3_1`), row policies (`5_3`), failed queries (`4_5`), DDL and
footprint. Sections your run couldn't reach (jobs/policy REST perms) are
skipped gracefully; whatever lands is ingested.

### 3 · Build the graph (scoped to the six)

```bash
python synapse/scripts/pipeline.py \
  --mdm-manifest synapse/config/tables_focus6.yaml \
  --lumi-session /path/to/session1_output.json \   # if you have it — adds LookML + the gold queries
  --bq-extract-dir ~/bq_extract_focus6 \
  --skills-dir /path/to/your/skills/library \      # staged to sources/skills for the registry
  --enrich \                                        # Gemini pass → descriptions + auto-entities
  --out synapse/data/cache/graph_snapshot.json
```

- **`--mdm-manifest tables_focus6.yaml`** does double duty: it live-crawls
  the six tables' MDM *and* scopes every stage (it's the allowlist).
- **`--lumi-session`** is optional. With it you get baseline LookML +
  the gold-query corpus (more witnesses, more grounding). Without it, MDM +
  BQ still ground the six — just drop the flag.
- **`--skills-dir`** stages your skills so the registry (and thus guardrails
  + `get_skill`) can load them at runtime. Skills never enter the graph.
- **`--enrich`** produces the auto-entities and runs **verbose**: it prints
  the call plan up front and a live `[k/~M] … ~Nm left` countdown, and
  checkpoints memory after each table.

### 4 · Verify

```bash
python synapse/scripts/graph_probe.py
```

Expect: **6/6 tables grounded**, a high % of columns grounded, `bq coverage:
6/6`, **PII > 0** on the risk/cardmember tables, **2–3 entities**
auto-materialized across the join, declared **PK/FK** reflected as grain +
join edges, and **no skill/junk nodes**.

Paste the probe's `PASTE THIS BACK` JSON into the chat — I'll confirm each of
the six is at cardmember-grade, table by table.

### 5 · At runtime (console / agent)

The agent auto-loads the skills registry from `sources/skills` next to the
snapshot (or set `SYNAPSE_SKILLS_DIR`). Guardrails enforce from there; the
graph answers "what is this table about" from MDM + entities + metrics +
lineage + the full BQ profile.

## If a table still lags

The probe names, per thin table, the exact missing witnesses and the tier it
*would* reach once staged — usually a missing BQ profile (re-run step 2 for
that table) or an absent gold-query corpus (pass `--lumi-session` or
`--gold-sql-dir`).
