"""Tool → human verb. The legibility layer.

`inspect_table({"table_name": "sbs_new_accounts"})` is not what a VP
should read; "Reading the metadata spine for sbs_new_accounts" is. This
map turns every tool call into a present-tense verb phrase. When a tool
isn't mapped, we degrade to a humanized tool name — never raw JSON.
"""

from __future__ import annotations

from typing import Any, Callable

# tool_name → (args) → verb phrase
_VERBS: dict[str, Callable[[dict[str, Any]], str]] = {
    "inspect_table": lambda a: (
        f"Reading the metadata spine for {a.get('table_name', 'the table')}"),
    "find_columns_for_concept": lambda a: (
        f"Searching the graph for “{a.get('concept', a.get('query', '…'))}”"),
    "get_lineage": lambda a: (
        f"Tracing lineage for {a.get('table_name', 'the table')}"),
    "explain_confidence": lambda a: (
        f"Checking how well-evidenced {a.get('target', 'this fact')} is"),
    "resolve_synonym": lambda a: (
        f"Resolving the term “{a.get('term', a.get('surface_form', '…'))}”"),
    "resolve_code": lambda a: (
        f"Decoding value “{a.get('value', a.get('raw_value', '…'))}”"),
    "list_skills": lambda a: "Looking up the governing skill packages",
    "get_skill": lambda a: (
        f"Loading the {a.get('skill_id', 'relevant')} skill"),
    "validate_sql_plan": lambda a: "Statically checking the SQL against guardrails",
    "dry_run_sql": lambda a: "Dry-running the query to estimate cost",
    "execute_sql": lambda a: "Running the approved query (row-capped)",
    "render_chart": lambda a: "Rendering a chart",
    "render_dashboard": lambda a: "Composing a dashboard",
    "run_python_analysis": lambda a: "Computing in the sandbox",
    "list_agent_skills": lambda a: "Choosing how to present the answer",
    "load_agent_skill": lambda a: (
        f"Applying the {a.get('skill_name', 'presentation')} craft skill"),
}


def verb_for(tool: str, args: dict[str, Any] | None = None) -> str:
    args = args or {}
    fn = _VERBS.get(tool)
    if fn is not None:
        try:
            return fn(args)
        except Exception:
            pass
    return tool.replace("_", " ").capitalize()
