# Retrieval & routing: business units, gold-set evals, domain agents

The graph stopped being only *rich* and became *findable, measurably*.
Three moving parts, in dependency order.

## 1. The segment level: BusinessUnit nodes

Tables carry a `business_unit` string (MDM ownership authoritative;
mined/DMP catalogs gap-fill). The **rollup stage** (always on, cheap,
idempotent) lifts that axis into first-class nodes:

    synapse://business_unit/<slug>
        name, table_count, member_tables, data_domains,
        metric_count, execution_count, user_count, top_metrics,
        vocabulary,            # group-by/filter terms analysts used
        question_bank,         # DMP questions this unit answers
        internal/external_join_edges, external_join_partners,
        description            # assembled ONLY from those counts

Honesty rules: the description can't say anything the evidence doesn't
pay for, and witnesses are **inherited** from member tables (filtered to
sources that can plausibly assert an org label — bq profiling never
did), so a unit asserted by MDM across 20 tables outranks one gap-filled
by usage mining on 2. Re-runs recompute from current tables — append
builds stay consistent.

### The steward's map: `--domain-tags` (the human ceiling)

When a person owns the segmentation, hand the pipeline their map:

```bash
python synapse/scripts/pipeline.py --append-to snap.json \
    --domain-tags domain_tags.json [--table-aliases aliases.json]
```

Either JSON shape works:

```json
{"Credit Risk": ["risk_pers_acct", "cstonedb3_gcp"],
 "Merchant Services": ["gms_transaction"]}
```

```json
[{"company_domain": "Credit Risk",
  "description": "Underwriting, exposure and delinquency analytics.",
  "tables": ["risk_pers_acct", "cstonedb3_gcp"]}]
```

Semantics: stamped as **human_approval** — overrides every mined or
crawled label; `description` (list form) replaces the derived unit
description; tagged-but-uncrawled tables are minted as stubs and
reported; names run through the shared alias map. Tables the map doesn't
mention keep their machine labels, so partial maps compose.

## 2. Segment-aware search + `route_question`

`search_entities` upgrades (all deterministic, no embeddings):

- **behavioral vocabulary haystacks** — DMP `question_answered` (×1.5),
  mined `group_by_patterns`/`common_filters`, unit `vocabulary`/
  `question_bank`: how analysts *phrase* things, not just what things
  are named;
- **light plural fold** ("merchants" meets "merchant") applied to both
  query and haystack — consistency is what makes bag-of-words survive;
- **`business_unit=` filter** to stay inside a segment (columns belong
  through their table; unaffiliated nodes are excluded — that's what a
  filter means);
- hits now carry `uri` (chainable) and `business_unit`;
- units are **dampened ×0.5 in flat search** (a container matches
  everything about its members; the precise fact must win) — unless the
  query IS the unit's name.

`route_question(question)` — the new first call for broad questions:
ranks units (undamped) against the question, then scores tables/metrics
*within* the winners and attaches the skill playbooks covering them.
Zero lexical signal on every unit → honest flat-search fallback, never a
guessed segment. Also on `list_tables_for_domain(business_unit=...)`.

## 3. Grading it: the retrieval eval

The ingested catalogs double as labeled retrieval data — the same rows
that grew the graph grade it:

```bash
python synapse/scripts/eval_retrieval.py --graph snap.json \
    [--report eval.json] [--gold-out gold.json] [--fail-under-mrr 0.8]
```

Gold kinds and what each proves: `dmp_question` (curated question
verbatim — indexed, so it's the regression floor), `dmp_question_heldout`
(question **minus the metric's own name tokens** — the generalization
probe), `metric_name` / `mined_measure_name`, `table_business_name`,
`business_unit_route`. Scores hit@1/3/5 + MRR overall and per kind;
failures list what outranked the expected node. Deterministic: same
snapshot, same numbers. `--fail-under-mrr` gates CI.

Demo delta (13-table snapshot, 19 gold examples): overall MRR
0.803 → 0.939, hit@1 0.684 → 0.895; DMP natural-language questions
0.375 → 1.000 on both verbatim and held-out forms.

## 4. Domain specialists (opt-in)

`SYNAPSE_DOMAIN_AGENTS=1` gives the ADK analyst one sub-agent per
BusinessUnit node: instruction = the unit's derived profile + member
tables with tiers + most-used metrics + its skill playbooks; same tool
roster (scoping lives in instructions + `business_unit` filters, so a
proven cross-segment join is still followable). The root stays the
generalist front door and self-identifies the company domain before
transferring — `route_question` is the tool-shaped version of the same
decision. No units in the snapshot → no sub-agents, nothing changes.

### Arranging skills per domain (for stewards)

- **explicit**: `company_domain: Credit Risk` in a `skill.yaml` pins the
  bundle to that unit regardless of tables;
- **implicit**: leave it out — the skill attaches to any unit whose
  member tables intersect its `tables_used`.
