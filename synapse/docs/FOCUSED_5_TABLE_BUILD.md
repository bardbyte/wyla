# Focused 5-table build — the laptop runbook

Build the graph for five join-connected tables the cardmember way:
**session1 (lumi) + MDM (the spine) + BQ (enhance) → grounded, scoped to the
five**, with entities auto-understood and skills kept in a file registry
(not the graph).

## The five (`synapse/config/tables_focus5.yaml`)

`custins_customer_insights_cardmember` · `fin_consumer_business_card_member_status`
· `risk_pers_acct` · `risk_pers_acct_history` · `risk_indv_cust_hist`

They share the account and customer keys, so the build shows shared
entities, join paths, and lineage a single table can't.

## What the build produces

- **Graph = warehouse truth.** MDM (authority) + BQ (enhances) + corpus +
  auto-DQ + the **metric registry** + lineage. MDM+BQ agreeing grounds a
  fact. Only the five survive (the manifest is the builder allowlist — CTE
  and placeholder junk is pruned).
- **Entities auto-understood** at `inferred` (one table is enough; steward
  *upgrades* to `human_asserted`, never required to create). Needs `--enrich`.
- **PII on the graph**, correct — the `"Y"/"N"` + clobber bugs are fixed.
- **Skills → a file registry** (`sources/skills`), never the graph.
  Guardrails enforced from it; `get_skill` serves the full business logic.

## Steps

### 0 · Pull the branch

```bash
git fetch origin claude/graph-quality-learnings
git checkout claude/graph-quality-learnings && git pull
```

### 1 · Credentials (env)

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
export GOOGLE_CLOUD_PROJECT=<vertex-project>
export BQ_BILLING_PROJECT=<billing-project>
export SYNAPSE_MDM_BASE=<mdm-api-base-url>     # for the live MDM crawl
# export GEMINI_TLS_INSECURE=1                 # only if on the corp intranet, as before
```

### 2 · BQ extraction for the five (the profile BQ enhances with)

```bash
python semantic-graph/scripts/bq_batch_extract.py \
  --config synapse/config/tables_focus5.yaml \
  --output-dir ~/bq_extract_focus5
```

`risk_indv_cust_hist` is the one that was missing a profile last time — this
covers all five, so every column BQ profiles can reach grounded.

### 3 · Build the graph (scoped to the five)

```bash
python synapse/scripts/pipeline.py \
  --mdm-manifest synapse/config/tables_focus5.yaml \
  --lumi-session /path/to/session1_output.json \   # if you have it — adds LookML + the ~35 gold queries
  --bq-extract-dir ~/bq_extract_focus5 \
  --skills-dir /path/to/your/skills/library \      # staged to sources/skills for the registry
  --enrich \                                        # Gemini pass → descriptions + auto-entities
  --out synapse/data/cache/graph_snapshot.json
```

- **`--mdm-manifest tables_focus5.yaml`** does double duty: it live-crawls
  the five tables' MDM *and* scopes every stage (it's the allowlist).
- **`--lumi-session`** is optional. With it you get baseline LookML +
  the gold-query corpus (more witnesses, more grounding). Without it, MDM +
  BQ still ground the five — just drop the flag.
- **`--skills-dir`** stages your skills so the registry (and thus guardrails
  + `get_skill`) can load them at runtime. Skills never enter the graph.
- **`--enrich`** is what produces the auto-entities (it tags identifier
  columns, which `propose_entities` reduces). Skip it and you get a grounded
  graph but no entity layer.

### 4 · Verify

```bash
python synapse/scripts/graph_probe.py
```

Expect: **5/5 tables grounded**, a high % of columns grounded, `bq coverage:
5/5`, **PII > 0** on the risk/cardmember tables, **2–3 entities**
auto-materialized across the join, and **no skill/junk nodes**.

Paste the probe's `PASTE THIS BACK` JSON into the chat — I'll confirm each of
the five is at cardmember-grade, table by table.

### 5 · At runtime (console / agent)

The agent auto-loads the skills registry from `sources/skills` next to the
snapshot (or set `SYNAPSE_SKILLS_DIR`). Guardrails enforce from there; the
graph answers "what is this table about" from MDM + entities + metrics +
lineage.

## If a table still lags

The probe names, per thin table, the exact missing witnesses and the tier it
*would* reach once staged — usually a missing BQ profile (re-run step 2 for
that table) or an absent gold-query corpus (pass `--lumi-session` or
`--gold-sql-dir`).
