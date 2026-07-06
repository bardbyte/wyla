"""Synapse MCP server — the compiled graph as Model Context Protocol tools.

Boot (after `pipeline.py` has produced a snapshot):

    export SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json
    python -m synapse.mcp.server                       # stdio (Claude Desktop/Code)
    python -m synapse.mcp.server --transport streamable-http --port 8765

Claude Desktop / Claude Code config:

    {
      "mcpServers": {
        "synapse": {
          "command": "python",
          "args": ["-m", "synapse.mcp.server"],
          "env": {"SYNAPSE_GRAPH_PATH": "/abs/path/graph_snapshot.json"}
        }
      }
    }

Requires the `mcp` package (`pip install "mcp[cli]"`). The tool logic
lives in GraphService — this module is transport only.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from synapse.graph.store import GraphStore
from synapse.mcp.service import GraphService

_INSTRUCTIONS = (
    "Confidence-typed semantic knowledge graph over an enterprise BigQuery "
    "warehouse. Use these tools to map business terms to schema, find join "
    "paths, resolve coded values, fetch metric contracts and skill "
    "playbooks, and check guardrails BEFORE generating SQL. Every fact "
    "carries confidence_tier + sources — gate your actions on them: "
    "grounded/human_asserted → proceed; inferred → proceed and cite; "
    "guessed → tell the user you are guessing or ask. Workflow for an "
    "analytical question: search_entities → get_skill (if one covers the "
    "topic) → inspect_table → get_metric / get_join_path / "
    "get_filter_values as needed → get_guardrails → draft SQL → "
    "validate_sql_plan. If validate_sql_plan returns violations, fix and "
    "re-validate before showing SQL to the user."
)


def build_server(graph_path: str | Path, *, tenant_id: str = "default"):
    """Create the FastMCP app over a snapshot. Import-guarded so the rest
    of synapse works without the `mcp` package installed."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "The 'mcp' package is required for the MCP server: "
            "pip install 'mcp[cli]'"
        ) from exc

    store = GraphStore.load_json(graph_path)
    service = GraphService(store, tenant_id=tenant_id)
    mcp = FastMCP(name="synapse", instructions=_INSTRUCTIONS)

    # ── tools (thin, typed shims over GraphService) ──────────

    @mcp.tool()
    def search_entities(query: str, top_k: int = 10,
                        node_kinds: list[str] | None = None) -> dict:
        """Resolve a business term to graph objects (Table, Column, Metric,
        Synonym, Skill, Guardrail). CALL FIRST for any term whose schema
        binding isn't obvious ("active cardmembers", "NAA", "Platinum").
        Do NOT call when you already hold an exact table/metric name —
        go straight to inspect_table / get_metric."""
        return service.search_entities(query, top_k, node_kinds)

    @mcp.tool()
    def list_tables_for_domain(data_domain: str = "", company_domain: str = "",
                               tag: str = "") -> dict:
        """Browse tables by governance domain or tag. Use for "what tables
        do we have for X?" — NOT free-text search (use search_entities)."""
        return service.list_tables_for_domain(data_domain, company_domain, tag)

    @mcp.tool()
    def inspect_table(table: str, include: list[str] | None = None,
                      column_limit: int = 50) -> dict:
        """Everything known about one table: identity, columns, governance
        (default) — plus metrics/related/lineage/dq/usage/per_source via
        `include`. Guardrails ALWAYS ride along; respect them. Resolve
        ambiguous names via search_entities first; this is the heavy call."""
        return service.inspect_table(table, include, column_limit)

    @mcp.tool()
    def find_columns_for_concept(concept: str, table_hint: str = "",
                                 max_results: int = 25) -> dict:
        """Physical columns that materialize a business concept ("spend",
        "delinquency bucket"). Not a synonym resolver (search_entities) and
        not for aggregates (get_metric)."""
        return service.find_columns_for_concept(concept, table_hint, max_results)

    @mcp.tool()
    def get_filter_values(table: str, column: str, limit: int = 20) -> dict:
        """Observed values for a column with frequencies (corpus +
        profiling). Call BEFORE emitting any WHERE col = 'X' literal —
        confirms the literal exists and flags structural always-on filters."""
        return service.get_filter_values(table, column, limit)

    @mcp.tool()
    def resolve_code(column: str, raw_value: str, table_hint: str = "") -> dict:
        """Decode coded values both directions ('005' ↔ 'Platinum') using
        CASE-WHEN and lookup-table evidence. Only for columns flagged
        is_coded / where get_filter_values shows opaque codes."""
        return service.resolve_code(column, raw_value, table_hint)

    @mcp.tool()
    def get_join_path(from_table: str, to_table: str, max_hops: int = 3) -> dict:
        """Ranked join paths from OBSERVED analyst joins, with the real ON
        columns and observation counts. If empty: say so — NEVER invent a
        join. Distinct from get_lineage (declared upstream/downstream)."""
        return service.get_join_path(from_table, to_table, max_hops)

    @mcp.tool()
    def get_lineage(table: str, direction: str = "both", depth: int = 2) -> dict:
        """Declared data lineage (where a table comes from / feeds into).
        For impact analysis — not for join planning (use get_join_path)."""
        return service.get_lineage(table, direction, depth)

    @mcp.tool()
    def get_metric(name_or_synonym: str) -> dict:
        """Canonical metric contract: formula, grain, source table,
        synonyms, defining skill. Use for ANY aggregate the user names
        ("total spend", "approval rate", "C-30"). Never invent formulas."""
        return service.get_metric(name_or_synonym)

    @mcp.tool()
    def get_skill(topic: str) -> dict:
        """The curated skill package covering a topic/table/metric — the
        expert playbook (definitions, contracts, guardrails) for HOW to
        answer a class of question. Check for one before improvising."""
        return service.get_skill(topic)

    @mcp.tool()
    def get_guardrails(target: str) -> dict:
        """All guardrails constraining a table/column/metric, most severe
        first. Call before generating SQL that touches the target; treat
        severity=error rules as hard constraints."""
        return service.get_guardrails(target)

    @mcp.tool()
    def get_dq_status(table: str, min_severity: str = "warning") -> dict:
        """Data-quality rules + pass/fail summary for a table. Check before
        presenting an aggregate; disclose failing rules in your answer."""
        return service.get_dq_status(table, min_severity)

    @mcp.tool()
    def explain_confidence(name_or_uri: str) -> dict:
        """Why a fact has its confidence tier: contributing sources,
        evidence counts, conflicts, and what would raise it. Use when you
        must justify or escalate a guessed/inferred fact — not routinely."""
        return service.explain_confidence(name_or_uri)

    @mcp.tool()
    def disambiguate_term(term: str, context_query: str = "") -> dict:
        """Choose between competing meanings using the full question as
        context. If ambiguity_reason is non-null, STOP and ask the user —
        do not loop or coin-flip."""
        return service.disambiguate_term(term, context_query)

    @mcp.tool()
    def validate_sql_plan(sql: str, dialect: str = "bigquery") -> dict:
        """Static pre-flight for drafted SQL: parse check + machine-checkable
        guardrail enforcement + the full must-respect list. ALWAYS run this
        before showing SQL to the user; any violation means fix and
        re-validate first."""
        return service.validate_sql_plan(sql, dialect)

    # ── resources (bulk reference data, browse-style) ────────

    import json as _json

    @mcp.resource("synapse://catalog")
    def catalog() -> str:
        """All tables with identity + provenance."""
        return _json.dumps(
            service.list_tables_for_domain()["data"], indent=2)

    @mcp.resource("synapse://metrics")
    def metrics() -> str:
        """Full metric catalog (formulas + grains + provenance)."""
        rows = [{
            "technical_name": n.canonical_uri.rsplit("/", 1)[-1],
            "business_name": n.properties.get("business_name", ""),
            "formula": n.properties.get("formula", ""),
            "grain": n.properties.get("grain", ""),
            "sourced_from_table": n.properties.get("sourced_from_table", ""),
            "confidence_tier": n.provenance.confidence_tier,
        } for n in store.nodes_by_type("Metric")]
        return _json.dumps({"metrics": rows}, indent=2)

    @mcp.resource("synapse://guardrails")
    def guardrails() -> str:
        """Every guardrail in the graph, with targets and severity."""
        rows = [{**n.properties,
                 "confidence_tier": n.provenance.confidence_tier}
                for n in store.nodes_by_type("Guardrail")]
        return _json.dumps({"guardrails": rows}, indent=2)

    @mcp.resource("synapse://skills")
    def skills() -> str:
        """Every skill package: id, domain, coverage, metrics defined."""
        rows = [{
            "skill_id": n.properties.get("skill_id", ""),
            "domain": n.properties.get("domain", ""),
            "description": n.properties.get("description", ""),
            "tables_used": n.properties.get("tables_used", []),
            "metrics_defined": n.properties.get("metrics_defined", []),
        } for n in store.nodes_by_type("Skill")]
        return _json.dumps({"skills": rows}, indent=2)

    # ── canonical workflow prompt ────────────────────────────

    @mcp.prompt()
    def analyst_workflow() -> str:
        """Canonical research→SQL workflow over the Synapse graph."""
        prompt_path = Path(__file__).parent / "prompts" / "analyst_workflow.md"
        return prompt_path.read_text(encoding="utf-8")

    return mcp


def main() -> None:
    parser = argparse.ArgumentParser(description="Synapse MCP server")
    parser.add_argument(
        "--graph",
        default=os.environ.get("SYNAPSE_GRAPH_PATH", ""),
        help="Path to graph_snapshot.json (or set SYNAPSE_GRAPH_PATH)",
    )
    parser.add_argument("--tenant", default=os.environ.get(
        "SYNAPSE_TENANT_ID", "default"))
    parser.add_argument("--transport", choices=["stdio", "streamable-http"],
                        default="stdio")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if not args.graph:
        raise SystemExit(
            "no graph snapshot: pass --graph or set SYNAPSE_GRAPH_PATH "
            "(produce one with: python synapse/scripts/pipeline.py)"
        )
    mcp = build_server(args.graph, tenant_id=args.tenant)
    if args.transport == "stdio":
        mcp.run()
    else:  # pragma: no cover — needs a socket
        mcp.settings.port = args.port
        mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
