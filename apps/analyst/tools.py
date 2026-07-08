"""Analyst agent tools = shared graph tools + the python sandbox.

The 15 graph tools come from `synapse.mcp.adk_tools` — the same
GraphService implementation the MCP server exposes, so the ADK agent and
any MCP host behave identically. This module only adds process wiring
(snapshot discovery, lazy singleton) and the sandbox tool.
"""

from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SYNAPSE_ROOT = _REPO_ROOT / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:  # until synapse ships as a package
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.graph.store import GraphStore  # noqa: E402
from synapse.mcp.adk_tools import build_adk_tools  # noqa: E402
from synapse.mcp.service import GraphService  # noqa: E402
from synapse.utils.sandbox import run_python  # noqa: E402
from synapse.viz.chartspec import Dashboard  # noqa: E402
from synapse.viz.render import render_page  # noqa: E402
from synapse.viz import parse_spec  # noqa: E402
from synapse.warehouse.runner import GateConfig, WarehouseRunner  # noqa: E402

from .agent_skills import list_agent_skills, load_agent_skill  # noqa: E402,F401

_DEFAULT_SNAPSHOT = _SYNAPSE_ROOT / "data" / "cache" / "graph_snapshot.json"


def _artifacts_dir() -> Path:
    """Read at use time, not import time — the env must win regardless
    of which module imported us first."""
    return Path(os.environ.get(
        "SYNAPSE_ARTIFACTS_DIR", str(_SYNAPSE_ROOT / "data" / "artifacts")))


def snapshot_path() -> Path:
    return Path(os.environ.get("SYNAPSE_GRAPH_PATH", str(_DEFAULT_SNAPSHOT)))


def _enrichment_client():
    """A Vertex client for on-demand column enrichment attaches only when
    credentials exist (work laptop); otherwise explain_column serves the
    grounded profile + read-through cache without an LLM."""
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return None
    try:
        from synapse.enrichment.vertex_client import VertexLLMClient
        return VertexLLMClient()
    except Exception:      # noqa: BLE001 — no creds/network → graceful None
        return None


@lru_cache(maxsize=1)
def _service() -> GraphService:
    path = snapshot_path()
    if not path.exists():
        raise FileNotFoundError(
            f"no graph snapshot at {path} — build one first:\n"
            "  python synapse/scripts/pipeline.py --demo\n"
            "or point SYNAPSE_GRAPH_PATH at an existing snapshot."
        )
    from synapse.enrichment.on_demand import OverlayStore
    from synapse.mcp.skills_registry import load_registry
    store = GraphStore.load_json(path)
    # the durable agent-fill layer — replay prior on-demand fills so the
    # graph the agent sees already carries them (the flywheel across
    # restarts); the canonical snapshot on disk is never mutated
    overlay = OverlayStore(os.environ.get(
        "SYNAPSE_ENRICHMENT_OVERLAY",
        str(path.with_name("enrichment_overlay.json"))))
    overlay.apply(store)
    return GraphService(
        store,
        tenant_id=os.environ.get("SYNAPSE_TENANT_ID", "default"),
        skills=load_registry(path),   # guardrails enforced from files, not the graph
        llm_client=_enrichment_client(),
        overlay=overlay,
    )


@lru_cache(maxsize=1)
def _runner() -> WarehouseRunner:
    """Gated warehouse runner. A real BigQuery client attaches only when
    credentials exist (work laptop); otherwise every call returns a
    structured no_client refusal that explains where it CAN run."""
    client = None
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            from synapse.warehouse.runner import BigQueryClient
            client = BigQueryClient(
                project=os.environ.get("BQ_BILLING_PROJECT") or None)
        except Exception:
            client = None  # runner reports no_client with guidance
    audit = _artifacts_dir() / "audit" / "warehouse_ledger.jsonl"
    return WarehouseRunner(
        client,
        graph_service=_service(),
        gate=GateConfig(audit_path=audit),
    )


def dry_run_sql(sql: str) -> dict[str, Any]:
    """Pre-flight a SELECT against the LIVE warehouse without executing:
    shape check → graph guardrails → BigQuery dry-run (validates against
    real schema) → bytes/cost estimate + budget verdict. Free, touches no
    rows. ALWAYS call before execute_sql; show the user the cost."""
    return _runner().dry_run(sql)


def execute_sql(sql: str, max_rows: int = 100) -> dict[str, Any]:
    """Run a read-only SELECT through the full gate chain (shape →
    guardrails → dry-run → byte budget) and return capped rows. Refusals
    come back structured (guardrail_violation / over_budget / invalid_sql)
    — fix and retry, or surface to the user. Every attempt is written to
    the audit ledger. Never use for DML/DDL; it will refuse."""
    return _runner().execute(sql, max_rows=max_rows)


def _slug(text: str) -> str:
    keep = "".join(c if c.isalnum() else "-" for c in text.lower())
    return "-".join(filter(None, keep.split("-")))[:48] or "chart"


def render_chart(spec: dict[str, Any]) -> dict[str, Any]:
    """Render one chart spec (kind: stat | line | bar | table) to a themed,
    self-contained HTML artifact; returns the file path to share. Load the
    `visualization` agent skill first for exact spec shapes and judgment
    rules. Invalid specs return instructive errors — follow them, don't
    guess."""
    import datetime as _dt
    try:
        parsed = parse_spec(spec)
    except Exception as exc:
        return {"status": "error", "error": str(exc)[:600]}
    if isinstance(parsed, Dashboard):
        return {"status": "error",
                "error": "use render_dashboard for kind='dashboard'"}
    stamp = _dt.datetime.now().strftime("%H%M%S")
    path = render_page(
        parsed, _artifacts_dir() / f"{_slug(parsed.title)}-{stamp}.html")
    return {"status": "ok", "path": str(path), "kind": parsed.kind,
            "title": parsed.title}


def render_dashboard(spec: dict[str, Any]) -> dict[str, Any]:
    """Render a composed dashboard (stat row + evidence charts + provenance
    footer) to one self-contained HTML artifact. Use for 'overall health'
    questions or an explicit dashboard ask — not for single-number
    answers. Footer must carry sources + snapshot version."""
    import datetime as _dt
    try:
        parsed = parse_spec(spec)
    except Exception as exc:
        return {"status": "error", "error": str(exc)[:600]}
    if not isinstance(parsed, Dashboard):
        return {"status": "error",
                "error": "spec.kind must be 'dashboard'; use render_chart "
                         "for single visuals"}
    stamp = _dt.datetime.now().strftime("%H%M%S")
    path = render_page(
        parsed, _artifacts_dir() / f"{_slug(parsed.title)}-{stamp}.html")
    return {"status": "ok", "path": str(path), "kind": "dashboard",
            "title": parsed.title, "n_items": len(parsed.items)}


def run_python_analysis(code: str, timeout_seconds: int = 30) -> dict[str, Any]:
    """Run python analysis code in an isolated sandbox and return
    {status, stdout, stderr, artifacts}.

    Use for math on numbers already in the conversation: rate
    recomputation, contribution decomposition, simple forecasting,
    scenario what-ifs. stdlib only (json, statistics, math, csv,
    datetime...). print() everything you want to see; files written to
    the working directory come back in `artifacts`. No network, no
    credentials, no warehouse access — never try."""
    return run_python(code, timeout_seconds=timeout_seconds)


def build_analyst_tools() -> list[Callable[..., dict[str, Any]]]:
    """Everything the analyst can do, grouped by trust level:
    graph tools (read snapshot) + gated warehouse (dry_run/execute) +
    presentation (charts/dashboards) + craft skills + sandbox."""
    return [
        *build_adk_tools(_service()),
        dry_run_sql, execute_sql,
        render_chart, render_dashboard,
        list_agent_skills, load_agent_skill,
        run_python_analysis,
    ]


# The original single-graph agent's capability set, plus the gated
# warehouse pair and the skills library — the user-selected bound for
# the chat experience. get_skill is in the bound because the curated
# playbooks are how the agent UNDERSTANDS a question (definitions,
# metric contracts, guardrails) before designing any answer.
_CLASSIC_GRAPH_TOOLS = (
    "search_entities", "list_tables_for_domain", "inspect_table",
    "find_columns_for_concept", "get_join_path", "get_lineage",
    "get_metric", "get_skill", "get_dq_status", "disambiguate_term",
    "validate_sql_plan", "get_entity", "get_steward_review_queue",
    "explain_column",
)


def build_classic_tools() -> list[Callable[..., dict[str, Any]]]:
    """The bounded chat roster: the original agent's 12 capabilities
    (under their current names) + dry_run_sql + execute_sql. No charts,
    no sandbox, no skill loader — guardrails still bind because they
    are enforced INSIDE validate_sql_plan and the execute gate chain,
    not by tool availability."""
    by_name = {t.__name__: t for t in build_adk_tools(_service())}
    return [
        *[by_name[n] for n in _CLASSIC_GRAPH_TOOLS],
        dry_run_sql, execute_sql,
    ]
