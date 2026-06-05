# Semantic Graph — confidence-typed knowledge graph + NL→BigQuery ADK agent

> **One table demo.** Fuses 3 real sources (BigQuery extraction, MDM API,
> SQL corpus) for `custins_customer_insights_cardmember` into a
> confidence-typed semantic graph, runs an LLM enrichment pass with
> Gemini 3.1 Pro, and exposes the graph as a tool to a Google ADK agent
> that generates grounded BigQuery SQL with citations.
>
> Designed as the proof-of-concept for the production semantic layer
> that powers downstream consumers: NL→BigQuery agents, Looker /
> Gemini-LookML, governance dashboards, lineage tools, MCP clients.

---

## Architecture

```
          INGEST                            GRAPH                            CONSUMERS
   ┌────────────────────┐                                              ┌──────────────────────┐
   │ MDM API            │──┐                                       ┌──▶│ NL→BigQuery Agent    │
   │ BQ INFO_SCHEMA     │──┤                                       │   │ (Google ADK, Gemini) │
   │ BQ Profiling       │──┤            ┌──────────────────┐       ├──▶│ Looker (Gemini       │
   │ SQL Corpus (~35 q) │──┤            │                  │       │   │  LookML / NL chat)   │
   │ Glossary.md        │──┼──────────▶│  SEMANTIC GRAPH   │──────┼──▶│ Claude Desktop       │
   │ Knowledge Catalog  │──┤            │ confidence-typed │       │   │ (via MCP server)     │
   │ Baseline LookML    │──┤            │ per-fact prov.   │       ├──▶│ Governance dashbd.   │
   │ Metric Catalog     │──┤            └────────┬─────────┘       │   │                      │
   │ Auto-DQ rules      │──┤                     │                  ├──▶│ Lineage / impact     │
   │ Steward annot.     │──┤                     ▼                  │   │ analysis             │
   │ AI-generated desc. │──┘            [LLM ENRICHMENT]            └──▶│ Quality monitors     │
   └────────────────────┘             disambiguate · propose            │                      │
                                      entities · review                 └──────────────────────┘

   3 sources wired for this demo: ▮▮▮▮▮▮▮▮▮▮ BQ · MDM · SQL Corpus
   7 more architecturally planned: ▒▒▒▒▒▒▒▒▒▒ Glossary · Knowledge Catalog · Baseline LookML
                                              Metric Catalog · Auto-DQ · Steward · AI-Generated
```

**The graph is the product.** LookML, SQL agents, governance, lineage —
they're all consumers of the same shared semantic substrate. New
consumers cost zero new ingest work; new sources improve every consumer
simultaneously.

---

## What's in this folder

```
semantic-graph/
├── .env.example                 # template — copy to .env and fill in
├── README.md                    # this file
├── requirements.txt
├── pyproject.toml
│
├── skills/
│   ├── enrichment_skill.md      # rules the LLM enrichment pass follows
│   └── agent_skill.md           # rules the NL→BQ agent follows
│
├── src/semantic_graph/
│   ├── __init__.py              # truststore inject (corp TLS)
│   ├── config.py                # .env loader
│   ├── loaders/                 # BQ + MDM + corpus, single-table-focused
│   ├── enrichment/              # real Vertex Gemini client + skill-driven runner
│   ├── graph/                   # builds + persists the graph
│   ├── tools/                   # inspect_table — the agent's one tool
│   └── agent/                   # root_agent for adk web
│
├── apps/semantic_graph_agent/   # adk web discovery shim
│   ├── __init__.py
│   └── agent.py
│
├── scripts/
│   ├── build_graph.py           # one-button: load → build → enrich
│   └── inspect.py               # debug — dump graph state for the table
│
└── tests/
    └── test_smoke.py            # synthetic end-to-end (no real Vertex needed)
```

---

## Run guide (work laptop, ~5 min)

### 1. Pull + install

```bash
git pull origin feat/real-pipeline-orchestration
cd semantic-graph
pip install -r requirements.txt
```

### 2. Configure `.env`

```bash
cp .env.example .env
$EDITOR .env
```

Fill in the **three input paths** for the table you've extracted:

```
BQ_EXTRACTION_DIR=/Users/you/.../custins_customer_insights_cardmember
MDM_JSON_PATH=/Users/you/.../mdm.json
SQL_QUERIES_DIR=/Users/you/.../queries
```

And the **Vertex AI credentials**:

```
GOOGLE_APPLICATION_CREDENTIALS=/Users/you/keys/vertex-sa.json
GOOGLE_CLOUD_PROJECT=prj-d-ea-poc
GEMINI_MODEL=gemini-3.1-pro-preview
```

### 3. Build the graph + run enrichment

```bash
python scripts/build_graph.py
```

What this does:
1. **Loads** BQ extraction CSVs, MDM JSON, copies the SQL corpus.
2. **Builds** the graph using the existing synapse graph code (Table + 190 Column nodes, JOIN edges from the corpus, governance + lineage + DQ rule nodes).
3. **Enriches** every column with Gemini 3.1 Pro:
   - Disambiguated descriptions tagged `source=llm_generated`, tier capped at `inferred`.
   - Candidate synonyms (TBB, etc.) from corpus column-alias evidence.
   - Code resolutions (`product_group='Delta'` → meaning).
   - Filter rationale (which WHERE clauses are structural).
4. **Proposes entities** — `Cardmember Account (cm11)`, `Customer (cust_xref_id)`, `Card Product (card_prod_id)` — for steward review.
5. **Saves** snapshot + memory + proposals to `data/cache/`.

Approx Gemini cost: ~$0.50 for the full 190-column enrichment (10 batches of 20 columns × 8K-token avg context). Runtime ~3 min.

### 4. Run the ADK agent

```bash
adk web apps/
```

Open `http://localhost:8000`, select `semantic_graph_agent`, and ask questions like:

```
What's the total billed business for Platinum cardmembers last quarter?

How many distinct customers had any FICO score recorded in May 2026?

Show me the top 10 cardmember accounts by gross provision over the last 3 months.

Which columns can I use to filter for the cobrand portfolio?
```

For each, the agent:
1. Calls `inspect_table('custins_customer_insights_cardmember')` once.
2. Composes a BigQuery SQL using ONLY column names from the graph.
3. Returns the SQL + **Citations** (source + confidence tier per fact used) + **Governance** notes + `✅ READY TO RUN` stamp.

You paste the SQL into BQ console as your normal user (RLS depends on your ONCOP keys, not the SVC ID).

### 5. (Optional) Debug

```bash
python scripts/inspect.py | jq .
```

Dumps the full inspector dict for the table — what the agent's tool returns.

### 6. (Optional) Verify wiring without real Vertex / BQ access

```bash
pytest tests/test_smoke.py -v
```

Runs the full pipeline end-to-end against synthetic data, dry-run enrichment, no network calls. ~5 sec. Use this when you've changed wiring and want to confirm imports + flow before touching real data.

---

## What's in the skills

### `skills/enrichment_skill.md` — for the LLM enrichment pass

The full operating manual for the Gemini disambiguation pass. Four
non-negotiable rules:
1. LLM output is always tagged `source="llm_generated"` (no impersonating other sources).
2. LLM facts cap at confidence tier `inferred` regardless of corroboration.
3. MDM, BQ, and corpus outrank LLM on facts they own.
4. Disagreements surface as `Provenance.conflicts`, never silently resolved.

Plus concrete decision rules for when to propose a description, an entity,
a code resolution, or a filter rationale — and the anti-patterns that
get the output rejected.

### `skills/agent_skill.md` — for the NL→BQ agent

The operating manual for the ADK agent. Seven non-negotiable rules:
1. Ground every column reference in the tool response — never invent names.
2. Never `SELECT *` from this 190-column view.
3. Always include the `rpt_dt` partition filter (cost discipline).
4. Resolve customer (`cust_xref_id`) vs account (`cm11`) correctly.
5. Cite every fact used (source + tier).
6. Surface governance + RLS warnings.
7. End every response with `✅ READY TO RUN` or `⚠ NEEDS CLARIFICATION`.

Includes a fully worked example showing the exact output format.

---

## Why this is interesting (beyond the demo)

**For the human analyst:**
- Stops the "Slack a senior to find the right column" cycle. The graph IS the answer.
- Surfaces governance (PII, RLS, owner) on every column without manual lookup.
- Catches the canonical analyst mistakes (`fico` vs `fico_score`, `card_product_id` vs `card_prod_id`) before they leave the agent's response.

**For the NL→BQ agent (and any future agent):**
- Single tool surface. The agent doesn't reason about catalogs, MDM, or SQL grammar — it reads what the graph already knows.
- Confidence-typed responses. The agent can defer uncertain claims to the user instead of confidently hallucinating.
- Per-fact provenance lets the agent cite its sources, which is the difference between "looks correct" and "I'll bet my dashboard on it."

**For the org:**
- The graph is the moat. Atlan / Dataplex give you metadata catalogs;
  none give you per-fact multi-source provenance + calibrated
  confidence + tribal-knowledge corpus mining + steward arbitration.
- MCP is the distribution. Wrap the graph as an MCP server and every
  agent in the org (Claude Desktop, Cursor, Looker Gemini, custom ADK)
  calls one API for the AmEx semantic layer.

---

## What's NOT in this demo (and where it goes)

| Capability | Status | Where it lands |
|---|---|---|
| Multi-table graph | this demo: 1 table | extend `TABLE_NAME` to a list; same loaders + enrichment scale |
| BQ dry-run validation tool | skipped for v1 | add `validate_sql` FunctionTool with BQ SA creds |
| Steward review UI for entity proposals | skipped for v1 | a Streamlit / Next.js panel reading `entity_proposals.json` |
| Right-rail per-fact provenance panel | skipped for v1 | the Streamlit UI in `../synapse/scripts/synapse_ui.py` has the IA design + visual system already in place |
| MCP server exposing the graph | designed, not built | `synapse/docs/MCP_SERVER_SPEC.md` has the full spec |
| TAO-style read API | designed, not built | `synapse/docs/TAO_API_MAPPING.md` has the contract |
| Glossary.md ingestion | template ready | `synapse/templates/glossary_template.md`; loader to be written |
| Knowledge Catalog source | architecturally planned | requires Dataplex enabled in the project |
| Steward feedback → graph memory loop | designed | proposal rejections become negative-training memory for next enrichment run |

The graph is the spine. Everything above plugs into it without
re-deriving anything.

---

## Files of interest if you want to read deeper

- `skills/enrichment_skill.md` — the LLM's rules of engagement
- `skills/agent_skill.md` — the agent's rules of engagement
- `../synapse/synapse/graph/store.py` — the typed graph + Provenance + confidence calibration
- `../synapse/synapse/graph/inspector.py` — the fused-view API the tool wraps
- `../synapse/docs/UX_INFORMATION_ARCHITECTURE.md` — IA for the UI consumer
- `../synapse/docs/UX_VISUAL_SYSTEM.md` — design tokens + components
- `../synapse/docs/MCP_SERVER_SPEC.md` — MCP tool surface design
- `../synapse/docs/TAO_API_MAPPING.md` — Facebook-TAO-shaped read API
- `../synapse/docs/DEEP_RESEARCH_AGENT_SPEC.md` — deep-research agent design
