# Retrieval & routing: the domain layer, gold-set evals, domain agents

The graph stopped being only *rich* and became *findable, measurably*.
Three moving parts, in dependency order.

## 1. The domain layer: Domain nodes, edge-based membership

Tables carry org labels (`business_unit` from MDM ownership —
authoritative; `company_domain`; catalog gap-fill). Those labels stay
exactly where the witnesses put them. The **rollup stage** (always on,
cheap, idempotent) builds the LAYER on top:

    synapse://domain/<slug>            node_type "Domain"
        name, table_count, member_tables, membership (per-table kind),
        data_domains, metric_count, execution/user counts, top_metrics,
        vocabulary,            # group-by/filter terms analysts used
        question_bank,         # DMP questions this domain answers
        internal/external_join_edges, external_join_partners,
        shared_tables,         # OVERLAP — tables also in other domains
        description            # steward prose, or derived from counts
    Domain —CONTAINS→ Table            one edge PER MEMBERSHIP

Membership is **edges, not a property**, so one table can belong to
several domains — each membership independently witnessed. Two witness
families coexist on the layer:

- **derived** — minted from the labels a table carries, provenance
  inherited from its BU-capable witnesses; recomputed every build;
- **steward** — minted by `--domain-tags` at `human_approval`; survives
  recomputes verbatim. A membership asserted by both fuses into ONE
  edge with both witness families (`membership: "both"`).

Honesty rules: the derived description can't say anything the evidence
doesn't pay for; steward prose (`description_by: "steward"`) outranks
it and is preserved across recomputes; witnesses are inherited, never
invented.

### The steward's map: `--domain-tags` (the coexisting human witness)

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
  "tables": ["risk_pers_acct", "gms_transaction"]}]
```

Semantics: **coexists, never overwrites** — the map becomes Domain
nodes + membership edges at `human_approval`; the `business_unit` MDM
put on a table is untouched (it keeps feeding the derived family).
List the same table under two domains and it gets two memberships —
overlap is the point. Tagged-but-uncrawled tables are minted as stubs
and reported; names run through the shared alias map; partial maps
compose.

## 2. Segment-aware search + `route_question`

`search_entities` upgrades (all deterministic, no embeddings):

- **behavioral vocabulary haystacks** — DMP `question_answered` (×1.5),
  mined `group_by_patterns`/`common_filters`, unit `vocabulary`/
  `question_bank`: how analysts *phrase* things, not just what things
  are named;
- **light plural fold** ("merchants" meets "merchant") applied to both
  query and haystack — consistency is what makes bag-of-words survive;
- **`domain=` filter** to stay inside a company domain — membership
  resolves through the layer's edges (overlap-aware), falling back to
  table labels when the layer wasn't built; columns belong through
  their table; unaffiliated nodes are excluded — that's what a filter
  means;
- hits now carry `uri` (chainable) and the table's `business_unit`
  label;
- Domain nodes are **dampened ×0.5 in flat search** (a container
  matches everything about its members; the precise fact must win) —
  unless the query IS the domain's name.

`route_question(question)` — the new first call for broad questions:
ranks domains (undamped) against the question, then scores
tables/metrics *within* the winners, surfaces each winner's
`shared_tables` overlap, and attaches the skill playbooks covering
them. Zero lexical signal on every domain → honest flat-search
fallback, never a guessed domain. Also on
`list_tables_for_domain(domain=...)`.

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
`domain_route`. Scores hit@1/3/5 + MRR overall and per kind;
failures list what outranked the expected node. Deterministic: same
snapshot, same numbers. `--fail-under-mrr` gates CI.

Demo delta (13-table snapshot, 19 gold examples): overall MRR
0.803 → 0.939, hit@1 0.684 → 0.895; DMP natural-language questions
0.375 → 1.000 on both verbatim and held-out forms.

## 4. Domain specialists (opt-in)

`SYNAPSE_DOMAIN_AGENTS=1` gives the ADK analyst one sub-agent per
Domain node: instruction = the domain's profile (steward-described or
derived) + member tables with tiers + most-used metrics + its skill
playbooks; same tool roster (scoping lives in instructions + `domain`
filters, so a proven cross-domain join is still followable). The root
stays the generalist front door and self-identifies the company domain
before transferring — `route_question` is the tool-shaped version of
the same decision. No domains in the snapshot → no sub-agents, nothing
changes.

### Arranging skills per domain (for stewards)

- **explicit**: `company_domain: Credit Risk` in a `skill.yaml` pins the
  bundle to that unit regardless of tables;
- **implicit**: leave it out — the skill attaches to any unit whose
  member tables intersect its `tables_used`.
