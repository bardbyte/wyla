# wyla — enterprise semantic layer PoC

Five signal sources ("witnesses") fused into one provenance-typed knowledge
graph, compiled into a versioned snapshot, served to agents over MCP and
Google ADK, and consumed by a guardrail-enforcing analyst agent with a
python sandbox.

**Read first:** [`docs/SEMANTIC_LAYER_BLUEPRINT.md`](docs/SEMANTIC_LAYER_BLUEPRINT.md)
(system design) · [`docs/SECURITY_AUDIT.md`](docs/SECURITY_AUDIT.md)
(risk register) · [`synapse/docs/MCP_SERVER_SPEC.md`](synapse/docs/MCP_SERVER_SPEC.md)
· [`synapse/docs/DEEP_RESEARCH_AGENT_SPEC.md`](synapse/docs/DEEP_RESEARCH_AGENT_SPEC.md)

## Five-minute offline demo (no credentials, no network)

```bash
pip install -e synapse pydantic pyyaml sqlglot pytest
python synapse/scripts/pipeline.py --demo         # witnesses → compiled graph snapshot
pip install "mcp"                                 # optional: the MCP surface
SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json \
  python -m synapse.mcp.server                    # 15 tools for any MCP host
pytest synapse/tests/ -q                          # 188 tests
```

Real runs point `pipeline.py` at the real witnesses instead:
`--skills-dir ~/Downloads/skills --gold-sql-dir … --bq-extract-dir …
--lumi-session … --mdm-cache-dir …` (any subset works).

## Layout

```
synapse/            THE PLATFORM — provenance-typed graph + loaders + MCP
├── synapse/graph/      GraphStore (confidence fusion), builder, inspector, viz
├── synapse/loaders/    mdm · bq · lumi · skills (NEW) · gold_sql (NEW)
├── synapse/mcp/        GraphService + MCP server + ADK adapter (NEW)
├── synapse/utils/      auth, dotenv, sandbox (NEW)
├── scripts/pipeline.py one-trigger compile: sources → snapshot (NEW)
└── docs/               MCP server / deep-research / UX specs

apps/               ADK agents (adk web apps/)
├── analyst/            the Data/Business Analyst agent + sandbox (NEW)
└── curator/            gold-query Excel auditor

semantic-graph/     BQ extraction scripts (capabilities probe, 12-probe batch
                    extractor), 13-tool consumer agent, Obsidian viz, KC docs

lumi_final/         LookML enrichment pipeline + the richest SQL extractor
                    (sql_to_context) + event-sourced ontology store (AGE path)

docs/               blueprint + security audit (repo-wide)
```

## The five witnesses

| Witness | Kind | Adapter |
|---|---|---|
| MDM API | declared metadata | `synapse/loaders/mdm_loader.py` |
| BigQuery (SVC ID) | observed behavior | `semantic-graph/scripts/bq_batch_extract.py` → `bq_loader.py` |
| Skills library | expert playbooks + guardrails | `synapse/loaders/skills_loader.py` |
| Gold-SQL corpus | proven analyst usage | `synapse/loaders/gold_sql_loader.py` |
| LLM enrichment (Vertex) | labeled inference | `synapse/curation/` · `semantic-graph/enrichment/` |

Everything lands as provenance-stamped assertions; the graph is **compiled,
not written** — delete the snapshot and recompile any time.

## The agent

One implementation (`synapse/mcp/service.py`), two hosts:

- **MCP** — Claude Desktop / Claude Code / any MCP client: `python -m synapse.mcp.server`
- **ADK** — Gemini on Vertex: `adk web apps/` → `warehouse_analyst`

The analyst fetches the covering **skill** before designing a query, treats
graph **guardrails** as law, statically **validates SQL** before a human sees
it, computes in a scrubbed **sandbox**, and answers with citations +
confidence tiers + a status stamp. It does not execute warehouse SQL.

## lumi_final pipeline (unchanged usage)

```bash
pip install -e lumi_final
cd lumi_final
LUMI_AGE_ENABLED=1 python scripts/probe_corpus_phase012.py \
    --from-excel ~/path/to/gold_queries.xlsx --refresh-mdm --fresh
pytest lumi_final/tests/ -v
```

Env vars: see `lumi_final/lumi/config.py` (`LUMI_VERTEX_PROJECT`,
`LUMI_BQ_PROJECT`, `LUMI_MDM_API_BASE`, `GOOGLE_APPLICATION_CREDENTIALS`, …).
