"""Google-ADK adapter — the same GraphService as ADK FunctionTools.

ADK introspects plain functions (name, signature, docstring) into the
schema Gemini sees, so this module just manufactures named closures over
one GraphService instance. The in-house analyst agent and any MCP host
therefore call the exact same implementation.

Usage:
    from synapse.graph.store import GraphStore
    from synapse.mcp.adk_tools import build_adk_tools
    from synapse.mcp.service import GraphService

    service = GraphService(GraphStore.load_json(snapshot_path))
    agent = Agent(model=..., tools=build_adk_tools(service), ...)
"""

from __future__ import annotations

from typing import Any, Callable

from synapse.mcp.service import GraphService


def build_adk_tools(service: GraphService) -> list[Callable[..., dict[str, Any]]]:
    """Named, docstring-carrying closures for every graph tool."""

    def search_entities(query: str, top_k: int = 10,
                        business_unit: str = "") -> dict:
        """Resolve a business term to graph objects (tables, columns,
        metrics, synonyms, skills, business units). Call FIRST for any
        term whose schema binding isn't obvious. Pass business_unit to
        stay inside the segment route_question picked. Returns hits with
        uri, confidence_tier + sources."""
        return service.search_entities(query, top_k,
                                       business_unit=business_unit)

    def route_question(question: str, top_units: int = 3) -> dict:
        """Which business unit (company domain) is this question about?
        Ranks the graph's business units against the question; returns
        each unit's evidence-derived profile, best-matching tables and
        metrics inside it, and the skill playbooks covering it. Call
        FIRST for broad or ambiguous questions, then work inside the
        winning unit."""
        return service.route_question(question, top_units)

    def list_tables_for_domain(data_domain: str = "",
                               company_domain: str = "",
                               business_unit: str = "") -> dict:
        """Browse tables by governance domain or business unit. For "what
        tables exist for X?" — not free-text search."""
        return service.list_tables_for_domain(
            data_domain, company_domain, business_unit=business_unit)

    def inspect_table(table: str, column_limit: int = 50) -> dict:
        """Everything known about one table (identity, columns, governance,
        guardrails). The heavy call — resolve the name first."""
        return service.inspect_table(table, None, column_limit)

    def find_columns_for_concept(concept: str, table_hint: str = "") -> dict:
        """Physical columns materializing a business concept, with roles
        (identifier/dimension/measure/filter) and provenance."""
        return service.find_columns_for_concept(concept, table_hint)

    def get_filter_values(table: str, column: str, limit: int = 20) -> dict:
        """Observed values for a column with frequencies. Call BEFORE
        emitting any WHERE col='X' literal."""
        return service.get_filter_values(table, column, limit)

    def resolve_code(column: str, raw_value: str) -> dict:
        """Decode coded values both directions ('005' ↔ 'Platinum')."""
        return service.resolve_code(column, raw_value)

    def get_join_path(from_table: str, to_table: str, max_hops: int = 3) -> dict:
        """Ranked join paths from OBSERVED joins with real ON columns.
        Empty result means: tell the user — never invent a join."""
        return service.get_join_path(from_table, to_table, max_hops)

    def get_lineage(table: str, direction: str = "both") -> dict:
        """Declared upstream/downstream lineage for impact analysis."""
        return service.get_lineage(table, direction)

    def get_metric(name_or_synonym: str) -> dict:
        """Canonical metric contract (formula, grain, source table,
        defining skill). Use for ANY named aggregate; never invent
        formulas."""
        return service.get_metric(name_or_synonym)

    def get_skill(topic: str) -> dict:
        """The curated skill package (expert playbook + guardrails) for a
        topic. Check for one before improvising an approach."""
        return service.get_skill(topic)

    def get_guardrails(target: str) -> dict:
        """All guardrails constraining a table/column/metric. Treat
        severity=error as hard constraints on any SQL you write."""
        return service.get_guardrails(target)

    def get_dq_status(table: str, min_severity: str = "warning") -> dict:
        """Data-quality rules + summary; disclose failures with answers."""
        return service.get_dq_status(table, min_severity)

    def explain_confidence(name_or_uri: str) -> dict:
        """Why a fact has its tier (sources, evidence, conflicts, and what
        would raise it). For justifying or escalating shaky facts."""
        return service.explain_confidence(name_or_uri)

    def disambiguate_term(term: str, context_query: str = "") -> dict:
        """Choose between competing meanings using question context. On
        non-null ambiguity_reason: stop and ask the user."""
        return service.disambiguate_term(term, context_query)

    def validate_sql_plan(sql: str, dialect: str = "bigquery") -> dict:
        """Static pre-flight of drafted SQL against machine-checkable
        guardrails. Run before showing SQL; fix violations and re-validate."""
        return service.validate_sql_plan(sql, dialect)

    def get_entity(name: str) -> dict:
        """One business entity (Account, Card Product…): its steward-
        approved definition, the columns that identify it, and their
        tables. Use for "what is X?" questions about business objects —
        the strongest-tier facts in the graph."""
        return service.get_entity(name)

    def get_steward_review_queue(limit: int = 20) -> dict:
        """The facts most in need of human review — lowest-confidence,
        fewest-witness assertions, weakest first. Use for "what needs
        review/curation?" or to qualify how settled an area is."""
        return service.get_steward_review_queue(limit)

    def explain_column(table: str, column: str, question: str = "") -> dict:
        """Explain what a specific column MEANS when inspect_table doesn't
        already make it clear. Read-through: returns the grounded
        description if the graph has one; otherwise fills it on demand with
        one gated LLM call at capped provenance and remembers it. Always
        returns the grounded profile (type, range, nulls); says so honestly
        when there isn't enough evidence to define the meaning."""
        return service.explain_column(table, column, question or None)

    def check_data_trust(table: str) -> dict:
        """Before you commit a number the user will rely on, check whether
        the feeding table has a red flag — a recent breaking change, a
        passed recertification, deprecated columns, or failing data-quality
        rules — plus PII context. Surface a warning to the user only if one
        fires; stay quiet when the table is clean."""
        return service.check_data_trust(table)

    def capture_knowledge(subject_type: str, subject_ref: str,
                          statement: str, actor: str = "analyst") -> dict:
        """When the user tells you what something MEANS in their world (a
        definition or a correction), record it as authoritative and credited
        to them — it outranks the machine's guess for everyone, immediately.
        subject_type is table|column|entity; for a column, subject_ref is
        'table.column'. Thank them; their knowledge is now part of the graph."""
        return service.capture_knowledge(subject_type, subject_ref,
                                         statement, actor)

    return [
        search_entities, route_question, list_tables_for_domain,
        inspect_table,
        find_columns_for_concept, get_filter_values, resolve_code,
        get_join_path, get_lineage, get_metric, get_skill, get_guardrails,
        get_dq_status, explain_confidence, disambiguate_term,
        validate_sql_plan, get_entity, get_steward_review_queue,
        explain_column, check_data_trust, capture_knowledge,
    ]
