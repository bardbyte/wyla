# Synapse MCP Server — Specification

**Status:** v1 draft, pre-implementation
**Owner:** Synapse platform team
**Substrate:** `synapse.graph.store.GraphStore` (in-process) wrapped by the TAO-style
read API (see companion doc `TAO_API_MAPPING.md` once published).
**Surface:** Model Context Protocol (MCP) — stdio for local agents, Streamable HTTP for remote.
**Primary consumer:** Radix (NL→BigQuery-SQL ADK agent). Secondary: Streamlit UI,
governance dashboards, external partner agents.

---

## 0. Design tenets

1. **Agent-friendly verbs, not graph primitives.** TAO gives us `assoc_get` /
   `obj_get`. Those are not tools an LLM should call directly — they are too
   low-level, the LLM will loop. MCP tools are *task-shaped*: "find columns for
   this concept", "get me a join path", "resolve this code". Each tool answers a
   question Radix actually asks during NL→SQL.
2. **Confidence is a first-class output.** Every tool returns
   `confidence_tier` + `sources` for every fact. Radix uses this to decide
   whether to (a) act, (b) ask the user, or (c) refuse with a "low-confidence"
   explanation. No tool ever returns bare facts.
3. **Read-only in v1.** No tool mutates the graph. Writes happen through the
   curation pipeline and the human review queue, not MCP. This keeps the surface
   safe for any external agent.
4. **Latency budget: p95 < 200ms per tool.** Radix calls 3–8 tools per NL
   question. If we breach 200ms we cap the agent's wall-clock budget and the UX
   collapses.
5. **Idempotent.** Same input → same output (modulo graph refresh). Enables
   client-side caching and replay-based eval.
6. **Self-documenting.** The docstring IS the prompt the LLM sees. Includes
   "when to call" AND "when NOT to call" (anti-pattern), with example
   inputs/outputs in the description.
7. **Structured output, never free text.** Pydantic-modeled JSON. No
   markdown, no bulleted prose. The agent does its own rendering.
8. **Hide the graph.** Don't expose `canonical_uri`, edge types, or node types
   in the tool surface unless the agent is supposed to feed them back. URIs
   leak the substrate; if we swap GraphStore for Neo4j tomorrow, tool
   contracts shouldn't move.

---

## 1. Tool surface

12 tools, grouped by intent. All read-only. All return
`SynapseResponse[T]` (see §3).

### 1.1 Discovery & search

#### `search_entities`

```
Input:  SearchEntitiesInput { query: str, top_k: int=10,
                              node_kinds: list[Literal["Entity","Synonym","Metric","Table","Column"]] | None = None }
Output: SearchEntitiesOutput { hits: list[EntityHit] }
        EntityHit { kind, name, canonical_uri, business_name, score: float,
                    confidence_tier, sources: list[str], one_line_summary: str }
```
**When to call.** First-pass resolution of any user term whose binding to the
schema isn't obvious. "active cardmembers" → maps to an Entity; "Platinum" →
maps to a CodeMapping or Synonym; "NAA" → Synonym. Always call this BEFORE
guessing a table or column name yourself.

**When NOT to call.** If the user already gave you a fully-qualified
`project.dataset.table.column` or a metric name you've previously resolved in
this session — go straight to `inspect_table` / `get_metric`.

**Caching.** Memoize `(query, top_k, node_kinds)` for the lifetime of one
graph snapshot (`snapshot_version` in response header). 15-minute TTL.

---

#### `find_columns_for_concept`

```
Input:  FindColumnsInput { concept: str, table_hint: str | None = None,
                           max_results: int = 25 }
Output: FindColumnsOutput { columns: list[ConceptColumn] }
        ConceptColumn { table, column, role: Literal["identifier","dimension","measure","filter"],
                        why: str,  # human-readable provenance trace
                        confidence_tier, sources, distinct_sample: list[str] }
```
**When to call.** User asks for a concept ("spend", "tenure", "active flag")
and you need the physical column(s) it materializes as. Traverses Entity →
`IDENTIFIES` → Column, plus `EQUIVALENT_TO` columns across tables.

**When NOT to call.** Not a synonym-resolver — use `search_entities` for
ambiguous tokens first. Not a metric lookup — use `get_metric` if the user
asked for an aggregate ("total spend", "average tenure").

**Caching.** Same as `search_entities`. Key on `(concept, table_hint)`.

---

#### `disambiguate_term`

```
Input:  DisambiguateInput { term: str, context_query: str,
                            candidate_uris: list[str] | None = None }
Output: DisambiguateOutput { chosen: Candidate | None,
                              alternatives: list[Candidate],
                              ambiguity_reason: str | None }
        Candidate { canonical_uri, kind, business_name, score: float,
                    confidence_tier, supporting_terms: list[str] }
```
**When to call.** When `search_entities` returned ≥2 hits with comparable
scores OR when a term is a known acronym with multiple expansions
("CM" = Cardmember | Communication Module). Pass the full NL question as
`context_query` — disambiguation uses surrounding tokens (e.g. "card" → CM=Cardmember).

**When NOT to call.** Don't call if `search_entities` returned exactly one
high-confidence hit. Don't call recursively — if disambiguation fails twice,
SURFACE THE AMBIGUITY TO THE USER. Looping here is the #1 agent failure mode.

**Caching.** Do NOT cache. Context-dependent.

---

#### `list_tables_for_domain`

```
Input:  ListTablesInput { data_domain: str | None = None,
                          company_domain: str | None = None,
                          tag: str | None = None,
                          in_dmp_only: bool = True }
Output: ListTablesOutput { tables: list[TableSummary] }
        TableSummary { table, fqn, business_name, asset_kind, n_columns,
                       n_queries_observed, confidence_tier, last_modified }
```
**When to call.** User asks "what tables do we have for X?" or you need to
narrow a search before calling `inspect_table`. Domains follow MDM
taxonomy (`Customer`, `Acquisition`, `Spend`, ...).

**When NOT to call.** Not a free-text search — use `search_entities` for that.
Don't iterate this to enumerate the whole catalog; if you need everything,
use the `synapse://catalog` MCP **resource** (see §4) instead.

**Caching.** 1h TTL, key on full input.

---

### 1.2 Table-centric inspection

#### `inspect_table`

```
Input:  InspectTableInput { table: str,
                            include: list[Literal["columns","metrics","related",
                              "lineage","governance","dq","code_resolutions",
                              "per_source"]] | None = None,
                            column_limit: int = 50 }
Output: InspectTableOutput { ...inspector dict, trimmed by `include` ... }
```
**When to call.** Once you've identified a candidate table. This is the
"give me everything you know about this table" call — returns the inspector
payload (identity, columns, metrics, lineage, governance, DQ, code
resolutions). Use `include` to keep the payload small; default is `["identity",
"columns", "governance"]`.

**When NOT to call.** Don't call for every table in a join chain — use
`get_join_path` instead. Don't call before `search_entities` resolves the
table name.

**Caching.** 5-minute TTL keyed on `(table, frozenset(include), column_limit)`.

**Critical:** `per_source` is OPT-IN. It is a heavy payload (each source's
view), and an agent rarely needs the 7-source breakdown — only humans
clicking through the UI do. Don't tempt the LLM with it by default.

---

#### `get_filter_values`

```
Input:  GetFilterValuesInput { table: str, column: str, limit: int = 20,
                               include_structural_only: bool = False }
Output: GetFilterValuesOutput { values: list[FilterValueEntry],
                                 cardinality_bucket: str,
                                 column_is_coded: bool,
                                 resolver_hint: str | None }
        FilterValueEntry { raw_value, observation_count, is_structural: bool,
                           human_meaning: str | None, confidence_tier, sources }
```
**When to call.** Before emitting any SQL `WHERE col = 'X'` predicate.
Returns observed values from the corpus (with frequencies) and from BQ
profiling (`distinct_sample`). `is_structural=True` flags values that appear in
90%+ of observed queries (e.g. `data_source='cornerstone'`) — usually a hidden
default the user expects.

**When NOT to call.** For unbounded columns (timestamps, free-text). The
tool returns `cardinality_bucket=very_high` and `values=[]` in that case;
respect that.

**Caching.** 10-minute TTL.

---

#### `resolve_code`

```
Input:  ResolveCodeInput { column: str, raw_value: str, table_hint: str | None = None }
Output: ResolveCodeOutput { resolved: CodeResolution | None,
                            alternates: list[CodeResolution] }
        CodeResolution { raw_value, human_meaning, source: Literal["lookup_table",
                         "case_when","llm_inferred"], confidence_tier, sources }
```
**When to call.** User says "Platinum" but the column is `product_code`
with integer values. This walks `Column —RESOLVED_BY→ CodeMapping` to invert
the mapping ("Platinum" → `005`) OR forward-resolve (`005` → "Platinum").

**When NOT to call.** Not a generic lookup — only for columns where
`get_filter_values` returned `column_is_coded=True` or
`resolver_hint != None`. If `confidence_tier == "guessed"`, do NOT silently
substitute — surface the mapping to the user for confirmation.

**Caching.** 1h TTL — code mappings are stable.

---

### 1.3 Relationships & lineage

#### `get_join_path`

```
Input:  GetJoinPathInput { from_table: str, to_table: str, max_hops: int = 3 }
Output: GetJoinPathOutput { paths: list[JoinPath] }
        JoinPath { hops: list[JoinHop], total_observations: int,
                   confidence_tier, sources }
        JoinHop { left_table, left_column, right_table, right_column,
                  n_corpus_observations: int, join_type_observed: str }
```
**When to call.** Building a multi-table SQL query. Returns up to 5 ranked
paths (most-observed first) with the actual ON clauses extracted from gold
corpus queries.

**When NOT to call.** Don't call for same-table queries (you'll get an empty
result). Don't trust `total_observations < 3` paths blindly — surface those
as "uncertain" to the user.

**Caching.** 1h TTL.

---

#### `get_lineage`

```
Input:  GetLineageInput { table: str, direction: Literal["upstream","downstream","both"] = "both",
                          depth: int = 2 }
Output: GetLineageOutput { upstream: list[LineageNode], downstream: list[LineageNode] }
        LineageNode { table, hops_from_origin: int, edge_source: str,
                      confidence_tier, sources }
```
**When to call.** "Where does this table come from?" / "What breaks if I
change this column?" Used by governance dashboards and impact analysis;
Radix calls this rarely.

**When NOT to call.** Not for join paths — those are `EQUIVALENT_TO`/observed,
lineage is `UPSTREAM_OF`/declared. Don't conflate.

**Caching.** 1h TTL.

---

### 1.4 Metrics

#### `get_metric`

```
Input:  GetMetricInput { name_or_synonym: str }
Output: GetMetricOutput { metric: MetricDetail | None,
                           candidates_if_ambiguous: list[MetricSummary] }
        MetricDetail { technical_name, business_name, formula, grain,
                       sourced_from_table, synonyms, slice_by: list[str],
                       confidence_tier, sources }
```
**When to call.** User asks for any aggregate ("total spend", "NAA",
"approval rate"). Resolves via `Synonym —HAS_SYNONYM→ Metric` and returns the
canonical formula + grain.

**When NOT to call.** Not for raw column lookups — use `find_columns_for_concept`.
Not for filters — use `resolve_code` / `get_filter_values`.

**Caching.** 15-min TTL.

---

### 1.5 Quality & trust

#### `get_dq_status`

```
Input:  GetDqStatusInput { table: str,
                           min_severity: Literal["info","warning","error"] = "warning" }
Output: GetDqStatusOutput { rules: list[DqRule], summary: DqSummary }
        DqRule { rule_id, target_column, rule_kind, threshold,
                 last_run_status, last_run_value, severity, auto_suggested }
        DqSummary { n_pass, n_fail, n_warning, n_unknown, freshness_hours }
```
**When to call.** Before returning numbers to the user, check whether the
underlying table has failing DQ rules. If `n_fail > 0` on a column you're
about to aggregate, add a disclaimer in your answer.

**When NOT to call.** Don't call for every table touched in a 10-table join;
restrict to the fact table being aggregated.

**Caching.** 5-min TTL (DQ status is freshness-sensitive).

---

### 1.6 Reasoning aid

#### `explain_confidence`

```
Input:  ExplainConfidenceInput { canonical_uri: str }
Output: ExplainConfidenceOutput { tier, score, contributing_sources: list[SourceContribution],
                                   evidence_event_ids: list[str], conflicts: list[str],
                                   what_would_raise_it: list[str] }
```
**When to call.** When an upstream tool returned a `guessed` or `inferred`
tier and you (or the user) need to know WHY. Returns the calibration trace.

**When NOT to call.** Don't call routinely; only when explaining low
confidence to the user OR when the previous tool's `ambiguity_reason` was
non-null.

**Caching.** 5-min TTL.

---

### What I removed / merged from the proposed minimum-viable set

- **No standalone `get_synonyms` tool.** Folded into `search_entities` (with
  `node_kinds=["Synonym"]`). Avoids tool-count bloat; agents already loop on
  too many similar tools.
- **No `get_columns_for_table` tool.** It's `inspect_table` with
  `include=["columns"]`. Same query, same cache. Two tools doing the same
  thing is an anti-pattern (see §10).
- **`disambiguate_term` is new and load-bearing.** The original list under-
  weighted the CM = Cardmember vs Communication Module problem. Without this
  tool, the agent will either pick wrong silently or loop on `search_entities`.
- **`explain_confidence` is new.** Without a way to introspect why a tier was
  assigned, the agent's "ask the user vs. proceed" decision is uncalibrated.

---

## 2. What stays internal (NOT MCP tools)

Deliberately excluded from the v1 surface:

| Internal capability | Why not exposed |
|---|---|
| `store.upsert_node` / `upsert_edge` | Writes flow through curation pipeline + human review queue. Never via MCP in v1. |
| Raw `outgoing` / `incoming` edge iteration | Too primitive — LLMs will build inefficient traversals. Wrap in task-shaped tools. |
| Per-source view in `inspect_table` default | Heavy payload, rarely useful to an agent. Opt-in via `include=["per_source"]`. |
| Visualization HTML (`neighborhood_html`, `lineage_dag_html`) | Render artifacts belong to the UI, not the MCP tool surface. Possibly expose as MCP **resources** (PNG) in v2. |
| `consumption_flow_html` | Static documentation; ship as a resource not a tool. |
| Source weights / `confidence_from_sources` | Calibration internals. Agent gets the *output* (tier + score) via `explain_confidence`; doesn't need the weights. |
| `top_users`, `peak_query_hours` | PII-adjacent + low decision value. Keep in `inspect_table` but only when `include=["governance"]` explicitly requested, and even then trim emails. |
| `evidence_event_ids` (in `Provenance`) | Audit trail for governance dashboards. Exposed only via `explain_confidence`, not on every tool's default response. |

---

## 3. Response envelope (uniform across all tools)

```python
class SynapseResponse[T](BaseModel):
    status: Literal["ok", "error", "partial"]
    data: T | None
    error: ErrorDetail | None = None
    meta: ResponseMeta

class ResponseMeta(BaseModel):
    tool_name: str
    tool_version: str               # "1.0.0" — semver, see §6
    snapshot_version: str           # graph snapshot the answer was computed against
    latency_ms: int
    cached: bool
    tenant_id: str = "amex_us_consumer"   # see §5

class ErrorDetail(BaseModel):
    code: Literal[
        "not_found", "ambiguous", "low_confidence", "invalid_input",
        "rate_limited", "internal_error", "stale_snapshot",
    ]
    message: str                    # one-sentence, agent-readable
    suggestions: list[str] = []     # e.g. ["try search_entities('cardmember')"]
```

**Every payload `T` includes:**
- `confidence_tier: ConfidenceTier`
- `sources: list[SourceName]` (deduplicated, sorted)
- `evidence_count: int` when relevant

This is non-negotiable. An agent that gets a fact with no provenance has no
way to gate downstream actions on trust.

**Why `partial` matters.** When `get_join_path` finds 1 of 2 requested paths,
or `inspect_table` skips a column whose node was missing, we return `partial`
with the data we have AND a `meta.warnings: list[str]`. Never silently drop.

---

## 4. MCP resources (read-only, addressable)

In addition to tools, expose:

- `synapse://catalog` — paginated listing of all tables (replaces "iterate
  `list_tables_for_domain` to enumerate everything")
- `synapse://glossary` — full Synonym ↔ Entity table
- `synapse://metrics` — full Metric catalog (formulas + grains)
- `synapse://dq_failures` — current failing rules, table-scoped

Resources are for *bulk reference data the agent might browse*, not *answers
to a question*. Anything you'd cache for an hour is a candidate.

---

## 5. MCP server skeleton

```python
# synapse/mcp/server.py
"""Synapse MCP server.

Boot:
    # local / Claude Desktop
    uv run synapse-mcp                          # stdio transport

    # remote / HTTP
    uv run synapse-mcp --transport streamable-http --port 8765

Env:
    SYNAPSE_GRAPH_PATH   path to serialized GraphStore (.json or .parquet)
    SYNAPSE_TENANT_ID    default tenant id (v1: amex_us_consumer)
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from synapse.graph.store import GraphStore
from synapse.graph.inspector import inspect_table as _inspect_table_impl
from synapse.mcp.cache import TTLCache
from synapse.mcp.envelope import SynapseResponse, ResponseMeta, ErrorDetail
from synapse.mcp.tao import TaoView           # the substrate from companion doc

logger = logging.getLogger("synapse.mcp")

# ── bootstrap ──
_TOOL_VERSION = "1.0.0"
_TENANT = os.environ.get("SYNAPSE_TENANT_ID", "amex_us_consumer")
_store = GraphStore.parse_file(os.environ["SYNAPSE_GRAPH_PATH"])
_tao = TaoView(_store)
_cache = TTLCache(default_ttl_seconds=600)

mcp = FastMCP(
    name="synapse",
    instructions=(
        "Confidence-typed semantic knowledge graph for AmEx analytics. "
        "Use these tools to map natural-language terms to schema, find join "
        "paths, resolve coded values, and get data-quality signals BEFORE "
        "generating SQL. Every result includes a confidence_tier — gate "
        "your actions on it."
    ),
)


def _envelope(tool: str, data, *, cached: bool = False,
              snapshot: str | None = None) -> dict:
    return SynapseResponse(
        status="ok",
        data=data,
        meta=ResponseMeta(
            tool_name=tool,
            tool_version=_TOOL_VERSION,
            snapshot_version=snapshot or _store.snapshot_version,
            latency_ms=0,           # filled by middleware
            cached=cached,
            tenant_id=_TENANT,
        ),
    ).model_dump()


@mcp.tool(
    name="inspect_table",
    description=(
        "Return the full structured view of a single table from the Synapse "
        "graph: identity, columns (with profiling), metrics sourced from it, "
        "related tables via observed JOINs, lineage, governance, DQ rules, "
        "and code resolutions. Each fact carries its confidence_tier and "
        "contributing sources.\n\n"
        "Use this AFTER resolving the table name via search_entities. "
        "Use `include` to keep payload small; default is "
        "['identity','columns','governance']. Do NOT call with include="
        "['per_source'] unless a human asked for the 7-source breakdown — "
        "it is heavy and rarely useful to an agent."
    ),
)
def inspect_table(
    table: Annotated[str, Field(description="Table name (not FQN). "
                                  "E.g. 'custins_customer_insights_cardmember'.")],
    include: Annotated[list[str] | None, Field(
        default=None,
        description="Sections to include. Default ['identity','columns','governance']."
                    " Options: identity, columns, metrics, related, lineage, "
                    "governance, dq, code_resolutions, per_source.",
    )] = None,
    column_limit: Annotated[int, Field(default=50, ge=1, le=500)] = 50,
) -> dict:
    cache_key = ("inspect_table", table, tuple(sorted(include or [])), column_limit)
    cached = _cache.get(cache_key)
    if cached is not None:
        return _envelope("inspect_table", cached, cached=True)
    raw = _inspect_table_impl(_store, table)
    if "error" in raw:
        return SynapseResponse(
            status="error", data=None,
            error=ErrorDetail(
                code="not_found", message=f"Table '{table}' not in graph.",
                suggestions=[f"search_entities(query='{table}', node_kinds=['Table'])"],
            ),
            meta=ResponseMeta(tool_name="inspect_table", tool_version=_TOOL_VERSION,
                              snapshot_version=_store.snapshot_version,
                              latency_ms=0, cached=False, tenant_id=_TENANT),
        ).model_dump()
    sections = set(include or ["identity", "columns", "governance"])
    trimmed = {k: v for k, v in raw.items() if k in sections or k == "fused_view"}
    if "columns" in trimmed:
        trimmed["columns"] = trimmed["columns"][:column_limit]
    _cache.set(cache_key, trimmed, ttl_seconds=300)
    return _envelope("inspect_table", trimmed)


# ... 11 more @mcp.tool registrations following the same pattern ...


# ── resources ──
@mcp.resource("synapse://catalog")
def catalog_resource() -> str:
    """Paginated catalog of all tables (JSON)."""
    return _tao.dump_catalog()  # implemented over TAO substrate


@mcp.resource("synapse://glossary")
def glossary_resource() -> str:
    return _tao.dump_glossary()


# ── prompts ──
@mcp.prompt("nl_to_sql_workflow")
def nl_to_sql_workflow() -> str:
    """Canonical agent workflow: resolve → inspect → join → validate → emit SQL."""
    return open("synapse/mcp/prompts/nl_to_sql_workflow.md").read()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["stdio", "streamable-http"],
                        default="stdio")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if args.transport == "stdio":
        mcp.run()                                  # default
    else:
        mcp.run(transport="streamable-http", port=args.port)


if __name__ == "__main__":
    main()
```

**Notes:**
- We use **FastMCP** (the official Python SDK's high-level API) — strictly
  better DX than the low-level `Server` class. Tool registration is one
  decorator, schemas are derived from `Annotated[..., Field(...)]`.
- The cache lives **above the TAO substrate** so that swapping the graph
  backend doesn't invalidate the LLM-facing cache key namespace.
- **Middleware** (latency stamping, structured logging, error normalization)
  lives in `synapse/mcp/middleware.py` and wraps every tool call. Logs include
  `tenant_id`, `tool_name`, `cache_hit`, `confidence_tier_returned` so we can
  measure agent behavior in prod.
- **stdio for local** (Claude Desktop, ADK in-process), **streamable-http for
  remote** (deployed Radix in GKE, partner agents). Both supported on day 1.

---

## 6. Authentication + multi-tenancy

**v1 reality:** single tenant (AmEx US Consumer). Single shared GraphStore.

**Forward-compatible envelope (NO breaking changes when we add tenants):**

1. **Tenant ID propagated everywhere.**
   - HTTP transport: `X-Synapse-Tenant-Id` header (required). Missing →
     falls back to `SYNAPSE_TENANT_ID` env default.
   - stdio transport: passed via MCP `initialize` request's `clientInfo.metadata`.
   - Every tool response echoes `meta.tenant_id`. Logs/metrics tagged.
2. **Auth (HTTP transport only):**
   - v1: bearer token validated against an allowlist (env var
     `SYNAPSE_API_KEYS`, comma-separated).
   - v2: OAuth2 with per-tenant scopes (`synapse:read:amex_us_consumer`,
     `synapse:read:amex_international`).
   - stdio = trust the OS process boundary (Claude Desktop / local dev). No
     auth.
3. **No tenant_id in tool inputs.** Carried in transport metadata, not in
   each tool's schema. This is the key non-breaking move — adding more
   tenants later doesn't change a single tool signature.
4. **Per-tenant graph storage:** `SYNAPSE_GRAPH_PATH` becomes a directory in
   v2, with one GraphStore per tenant; routing happens at request entry.

---

## 7. Versioning

**Tool naming is stable.** We do NOT version into the tool name
(`inspect_table_v2`) — that's how Stripe's old APIs ended up with 14
variants of the same endpoint and every agent calls a different one.

**Three additive evolution moves (all non-breaking):**

1. **Add optional input args** (with sensible defaults). Old agents work
   unchanged.
2. **Add fields to outputs.** Pydantic-tolerant clients (the official MCP
   SDK is one) ignore unknown fields.
3. **Bump `meta.tool_version`** (semver) so agents can branch on it if they
   need new behavior. `1.x` minor = additive. `2.0` major = breaking, and
   only happens with a dual-tool overlap period:
   - Register both `inspect_table` (v1, deprecated) AND `inspect_table_v2`
     for 90 days.
   - v1's description gets a `[DEPRECATED — use inspect_table_v2]` prefix so
     the LLM steers itself.
   - Telemetry on `tool_name@tool_version` calls tells us when v1 traffic
     hits zero.
   - Remove v1.

**Breaking change examples we'd version-major for:** renaming a tool,
removing a required field from output, changing the meaning of
`confidence_tier`, changing the response envelope shape.

---

## 8. Radix end-to-end example

**User asks:** *"How many active cardmembers in the Platinum product spent over
$5k in Q1?"*

### Step 1 — Decompose terms

Radix identifies four uncertain bindings:
1. "active cardmembers" → entity + filter
2. "Platinum product" → coded value
3. "spent" → metric or column?
4. "Q1" → date filter (Radix handles dates internally, no tool call)

### Step 2 — Resolve each via MCP

```
→ search_entities(query="active cardmember", top_k=5)
← hits=[
    {kind:"Entity", name:"active_cardmember", confidence_tier:"grounded",
     sources:["mdm","corpus","metric_catalog"], score:0.94},
    {kind:"Synonym", name:"ACM", canonical:"active_cardmember", score:0.81},
  ]
# OK — top hit is grounded, single best.

→ search_entities(query="Platinum product", top_k=5)
← hits=[
    {kind:"CodeMapping", name:"product_code=005", human:"Platinum",
     confidence_tier:"grounded", sources:["glossary","corpus"]},
    {kind:"Entity", name:"product_line", score:0.62},
  ]
# Ambiguous on what to do — is "Platinum" a value or a dimension? Disambiguate.

→ disambiguate_term(term="Platinum", context_query="active cardmembers in the Platinum product spent over $5k in Q1",
                    candidate_uris=[<code_mapping_uri>, <entity_uri>])
← chosen={canonical_uri:<code_mapping_uri>, kind:"CodeMapping",
          business_name:"Platinum (product_code=005)",
          supporting_terms:["product"]}
# Resolved: "Platinum" is a value of column product_code.

→ get_metric(name_or_synonym="spend")
← metric={technical_name:"total_spend", formula:"SUM(amt_billed_usd)",
          grain:"cardmember_month",
          sourced_from_table:"spend_fact_monthly",
          synonyms:["spend","billed","charges"],
          slice_by:["product_code","region","tenure_bucket"],
          confidence_tier:"grounded",
          sources:["metric_catalog","corpus","baseline_lookml"]}

→ find_columns_for_concept(concept="active cardmember", table_hint="spend_fact_monthly")
← columns=[
    {table:"custins_customer_insights_cardmember", column:"active_flag",
     role:"filter", why:"active_cardmember Entity IDENTIFIES this column",
     confidence_tier:"grounded", distinct_sample:["Y","N"]},
  ]
```

### Step 3 — Plan the join

```
→ get_join_path(from_table="spend_fact_monthly",
                to_table="custins_customer_insights_cardmember", max_hops=2)
← paths=[{
    hops:[{left_table:"spend_fact_monthly", left_column:"cardmember_id",
           right_table:"custins_customer_insights_cardmember", right_column:"cardmember_id",
           n_corpus_observations:142, join_type_observed:"INNER"}],
    total_observations:142,
    confidence_tier:"grounded", sources:["corpus","baseline_lookml"],
  }]
```

### Step 4 — Verify the filter value

```
→ get_filter_values(table="custins_customer_insights_cardmember",
                    column="active_flag", limit=5)
← values=[
    {raw_value:"Y", observation_count:1184, is_structural:False,
     confidence_tier:"grounded"},
    {raw_value:"N", observation_count:412, is_structural:False},
  ]
# Confirms 'Y' / 'N' encoding. Radix uses active_flag='Y'.

→ resolve_code(column="product_code", raw_value="Platinum")
← resolved={raw_value:"005", human_meaning:"Platinum", source:"lookup_table",
            confidence_tier:"grounded", sources:["glossary","corpus"]}
# Now Radix knows to write product_code='005'.
```

### Step 5 — Quality gate

```
→ get_dq_status(table="spend_fact_monthly", min_severity="warning")
← rules=[
    {rule_id:"freshness_24h", last_run_status:"pass", ...},
    {rule_id:"row_count_min_1m", last_run_status:"pass", ...},
  ]
  summary={n_pass:8, n_fail:0, freshness_hours:3.2}
# Green. Proceed.
```

### Step 6 — Emit SQL

Radix synthesizes:
```sql
SELECT COUNT(DISTINCT s.cardmember_id) AS n_cardmembers
FROM spend_fact_monthly s
JOIN custins_customer_insights_cardmember c
  ON s.cardmember_id = c.cardmember_id
WHERE c.active_flag = 'Y'
  AND s.product_code = '005'       -- Platinum
  AND s.month BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY s.cardmember_id
HAVING SUM(s.amt_billed_usd) > 5000;
```

### Failure / retry path

If `search_entities("Platinum product")` had returned **two grounded hits with
near-equal score**, `disambiguate_term` is called; if THAT also returns
`ambiguity_reason != None`, Radix surfaces a clarifying question to the user
and HALTS. The agent must not loop.

If `get_join_path` returns `paths=[]`, Radix tries `get_lineage` to see if the
tables share an upstream, and if not, surfaces "I can't connect these tables
with observed evidence" to the user. No SQL emitted.

**Total tool calls for the happy path:** 7. Each ~50–150ms. Wall-clock under 1s.

---

## 9. What we steal from peer MCP servers

| Peer | What's right | What we adopt |
|---|---|---|
| **GitHub MCP** (`github/github-mcp-server`) | Tools are task-shaped (`create_pull_request`, `search_code`), not REST-shaped. Each tool's description tells the LLM *when* to use it, with examples. | Verb-noun naming. "When to / when NOT to" in every docstring. |
| **Sentry MCP** | Returns issue summaries with severity + frequency + affected users — agent has enough to triage without a follow-up call. Bundles related facts. | Confidence + sources on every fact. `inspect_table` returns the bundle, not 12 separate calls. |
| **Notion MCP** | `search` returns objects with their canonical IDs, so subsequent tools can use them as handles. | `canonical_uri` on every hit so the agent can chain calls without re-resolving. |
| **Google Drive MCP** | Resources (`gdrive:///<file>`) for browse-style access vs. tools for action-style access. Clean separation. | `synapse://catalog`, `synapse://glossary` as resources; tools are for queries. |
| **Stripe Agent Toolkit** | Toolkit ships with an `instructions` block + `prompts` for canonical workflows. LLMs use the prompts as templates instead of re-deriving the flow. | `nl_to_sql_workflow` prompt resource (§5) for Radix's canonical sequence. |
| **Linear MCP** | Soft errors (404 → "not found, did you mean X?") never throw; always return structured error with suggestions. | Our `ErrorDetail.suggestions` field; never throw from tool handlers. |

---

## 10. Anti-patterns to avoid

These are mistakes the user has explicitly called out on other teams, plus
patterns documented in MCP failure post-mortems.

1. **Too many similar tools.** `get_table`, `get_table_columns`,
   `get_table_metadata`, `describe_table` — the LLM picks one at random and
   loops. **We have ONE `inspect_table` with an `include` parameter.**
2. **Tools that mirror REST endpoints.** "We have GET, POST, PUT, DELETE for
   each resource" → 40 tools, all useless. Tools are *verbs the agent
   actually does in its workflow*, not your API surface.
3. **Free-text outputs.** Markdown-formatted responses force the LLM to
   re-parse. Structured JSON only.
4. **Unbounded result sets.** A tool returning 5,000 columns blows the
   context window. Every tool has `limit` / `top_k` with sensible defaults.
5. **No confidence in outputs.** Agents will trust any fact returned. Then
   they fabricate downstream. **Confidence tier is mandatory on every output.**
6. **Throwing exceptions from tool handlers.** Crashes the MCP server and
   the agent has no recourse. **Return `SynapseResponse[status="error"]`.**
7. **Tools that loop on each other.** If `disambiguate_term` fails twice,
   STOP. Surface ambiguity to the user. The Claude-Code team documented
   3-tool-deep loops as the most common ADK failure mode in Q1 2026.
8. **Hidden mutations.** A read-named tool that writes telemetry is fine; a
   read-named tool that mutates the graph is forbidden. **All MCP writes
   require explicit opt-in and a separate tool group, deferred to v2.**
9. **Echoing back internal IDs the LLM doesn't need.** `evidence_event_ids`
   blow up the context for zero agent benefit. Surface them only via
   `explain_confidence`.
10. **Per-source view by default.** Tempting because it's "more info";
    disastrous because it's 5× the payload and the LLM doesn't know what to
    do with it. **Opt-in only.**
11. **Encoding state in tool args.** No `session_id`, no `transaction_id`
    threading through tool calls. MCP tools are stateless. State lives in the
    agent.
12. **Verbose docstrings that hide the use case.** A 400-word docstring will
    get truncated or skimmed by the LLM. **Lead with one sentence describing
    output. Then "when to call" / "when NOT to call". Then examples.**

---

## 11. Open questions (for v1.1)

- **Streaming** — should `inspect_table` stream column-by-column for large
  tables? MCP supports it via `streamable-http`; gating on whether real-world
  responses exceed 50KB.
- **Caching key** — currently per-tenant + tool args. Should also include
  the agent's identity for usage analytics? Comes with privacy review.
- **Write tools (v2)** — `propose_synonym`, `propose_dq_rule`,
  `flag_low_confidence` would let agents contribute back to the graph,
  routed through the human review queue. Out of scope for v1.
- **Embedding-based `search_entities`** — current plan is BM25 + alias index.
  Embedding rerank is a v1.5 swap that doesn't change the tool contract.
