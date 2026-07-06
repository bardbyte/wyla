# analyst — the warehouse Data/Business Analyst agent

One agent, two hosts, one implementation:

- **ADK host (in-house, Gemini/Vertex)** — this app. `adk web apps/` →
  `warehouse_analyst`. 15 graph tools + `run_python_analysis` sandbox.
- **MCP host (Claude Desktop / Claude Code / any MCP client)** — the same
  tools served by `python -m synapse.mcp.server`. Zero agent code to
  maintain; bring-your-own frontier model for demos.

Both call `synapse.mcp.service.GraphService` — identical answers,
identical provenance envelopes.

## Run (offline demo)

```bash
pip install -e synapse pyyaml sqlglot          # + google-adk truststore on the work laptop
python synapse/scripts/pipeline.py --demo      # compile fixture graph
adk web apps/                                  # ADK surface, or:
SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json \
  python -m synapse.mcp.server                 # MCP surface (stdio)
```

Point at real extractions instead with `pipeline.py --skills-dir/--gold-sql-dir/
--bq-extract-dir/--lumi-session/--mdm-cache-dir`.

## What the agent will and won't do

- Answers with the fixed contract: **Answer / How I got there / Citations /
  Governance / Status stamp**.
- Fetches the covering **skill** before designing a query; guardrails from
  the graph are hard constraints; every SQL draft goes through
  `validate_sql_plan` before a human sees it.
- **Does not execute warehouse SQL** in this deployment — it emits
  validated, ready-to-run SQL. Execution stays with the human / a
  sanctioned executor with its own audit trail.
- The sandbox is stdlib-python, credential-scrubbed, time/memory-limited —
  for math on in-conversation numbers, not for I/O (see
  `synapse/synapse/utils/sandbox.py` for the honest isolation scope).

## Env

| Var | Default | Meaning |
|---|---|---|
| `SYNAPSE_GRAPH_PATH` | `synapse/data/cache/graph_snapshot.json` | compiled snapshot to serve |
| `SYNAPSE_TENANT_ID` | `default` | echoed in every tool response |
| `GEMINI_MODEL` | `gemini-3.1-pro-preview` | ADK model id |
| `GOOGLE_APPLICATION_CREDENTIALS` etc. | — | standard Vertex auth (ADK host only) |
