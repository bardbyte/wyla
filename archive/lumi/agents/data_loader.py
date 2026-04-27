"""DataLoader — deterministic ADK CustomAgent that orchestrates all tools.

Runs as Phase 0 of the pipeline. No LLM. Populates session.state with everything
downstream agents need. If any tool errors, we fail fast and surface the error.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types as genai_types

from lumi.schemas import LumiConfig
from lumi.tools.excel_tools import parse_excel_to_json
from lumi.tools.git_tools import clone_and_parse_views
from lumi.tools.grouping_tools import extract_join_graphs, group_queries_by_view
from lumi.tools.mdm_tools import query_mdm_api
from lumi.util import safe_key

logger = logging.getLogger(__name__)


class DataLoader(BaseAgent):
    """ADK CustomAgent. Reads the LumiConfig from state["lumi_config"] and populates:

      state["gold_queries"]        : list[ParsedQuery]
      state["parsed_views"]        : dict[str, ParsedView]
      state["model_file_text"]     : str (raw model file content, for context)
      state["mdm_metadata"]        : dict[str, dict]   # per view_name
      state["queries_by_view"]     : dict[str, list[ParsedQuery]]
      state["field_frequency"]     : dict[str, dict[str, int]]
      state["filter_defaults"]     : dict[str, dict[str, str]]
      state["user_vocabulary"]     : dict[str, dict[str, str]]
      state["join_graphs"]         : list[JoinPattern]

    On any tool error, raises RuntimeError — the SequentialAgent parent will
    terminate the run cleanly.
    """

    def __init__(self, name: str = "DataLoader") -> None:
        super().__init__(name=name)

    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        cfg = _require_config(ctx)

        yield self._progress("Parsing gold queries…")
        queries_res = parse_excel_to_json(
            cfg.gold_queries.file,
            prompt_column=cfg.gold_queries.prompt_column,
            sql_column=cfg.gold_queries.sql_column,
            difficulty_column=cfg.gold_queries.difficulty_column,
            sheet=cfg.gold_queries.sheet,
        )
        _require_success(queries_res, "parse_excel_to_json")

        yield self._progress(
            f"Parsed {len(queries_res['queries'])} queries "
            f"({queries_res['parse_errors']} parse errors). Cloning repo…"
        )
        views_res = clone_and_parse_views(
            repo=cfg.git.repo,
            branch=cfg.git.branch,
            model_file=cfg.git.model_file,
            view_files=cfg.git.view_files,
            clone_dir=cfg.git.clone_dir,
        )
        _require_success(views_res, "clone_and_parse_views")

        yield self._progress(f"Parsed {len(views_res['parsed_views'])} views. Querying MDM…")
        mdm_metadata: dict[str, dict[str, Any]] = {}
        for view_name, entity_name in cfg.mdm.view_to_mdm_entity.items():
            res = query_mdm_api(
                endpoint=cfg.mdm.endpoint,
                entity_name=entity_name,
                auth_env=cfg.mdm.auth_env,
                cache_dir=cfg.mdm.cache_dir,
                cache_ttl_hours=cfg.mdm.cache_ttl_hours,
            )
            mdm_metadata[view_name] = res

        yield self._progress("Grouping queries and extracting joins…")
        # Build view_name_to_table from the parsed views so we can group by view_name.
        view_name_to_table = {
            v.view_name: (v.sql_table_name or v.view_name)
            for v in views_res["parsed_views"].values()
        }
        grouping_res = group_queries_by_view(
            queries_res["queries"], view_name_to_table=view_name_to_table
        )
        _require_success(grouping_res, "group_queries_by_view")

        joins_res = extract_join_graphs(queries_res["queries"])
        _require_success(joins_res, "extract_join_graphs")

        # Flat, per-view state keys so each ViewEnricher in a ParallelAgent
        # sees its own slice without racing with siblings on shared keys.
        state_delta: dict[str, Any] = {
            "gold_queries": [q.model_dump() for q in queries_res["queries"]],
            "parsed_views": {
                name: v.model_dump() for name, v in views_res["parsed_views"].items()
            },
            "model_file_text": views_res["model_file_text"],
            "mdm_metadata_by_view": mdm_metadata,
            "queries_by_view": {
                view: [q.model_dump() for q in qs]
                for view, qs in grouping_res["queries_by_view"].items()
            },
            "field_frequency_by_view": grouping_res["field_frequency"],
            "filter_defaults_by_view": grouping_res["filter_defaults"],
            "user_vocabulary_by_view": grouping_res["user_vocabulary"],
            "join_graphs": [p.model_dump() for p in joins_res["patterns"]],
            "view_name_to_table": view_name_to_table,
            "view_names": list(views_res["parsed_views"].keys()),
        }

        # Per-view flat slices for ParallelAgent instruction templating.
        # The key shape MUST match what build_view_enricher reads — both go
        # through lumi.util.safe_key to guarantee they agree.
        for view_name, parsed_view in views_res["parsed_views"].items():
            safe = safe_key(view_name)
            state_delta[f"parsed_view__{safe}"] = parsed_view.model_dump()
            state_delta[f"queries_for_view__{safe}"] = [
                q.model_dump() for q in grouping_res["queries_by_view"].get(view_name, [])
            ]
            state_delta[f"mdm_metadata_for_view__{safe}"] = mdm_metadata.get(view_name, {})
            state_delta[f"field_frequency_for_view__{safe}"] = (
                grouping_res["field_frequency"].get(view_name, {})
            )
            state_delta[f"filter_defaults_for_view__{safe}"] = (
                grouping_res["filter_defaults"].get(view_name, {})
            )
            state_delta[f"user_vocabulary_for_view__{safe}"] = (
                grouping_res["user_vocabulary"].get(view_name, {})
            )

        yield Event(
            author=self.name,
            actions=EventActions(state_delta=state_delta),
            content=genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part(
                        text=(
                            f"DataLoader complete: "
                            f"{len(queries_res['queries'])} queries, "
                            f"{len(views_res['parsed_views'])} views, "
                            f"{len(joins_res['patterns'])} join patterns."
                        )
                    )
                ],
            ),
        )

    def _progress(self, msg: str) -> Event:
        logger.info("%s: %s", self.name, msg)
        return Event(
            author=self.name,
            content=genai_types.Content(
                role="model", parts=[genai_types.Part(text=msg)]
            ),
        )


def _require_config(ctx: InvocationContext) -> LumiConfig:
    raw = ctx.session.state.get("lumi_config")
    if raw is None:
        raise RuntimeError(
            "session.state['lumi_config'] missing. "
            "Populate it with a LumiConfig model_dump() before invoking DataLoader."
        )
    return LumiConfig.model_validate(raw)


def _require_success(result: dict[str, Any], tool_name: str) -> None:
    if result.get("status") != "success":
        raise RuntimeError(f"{tool_name} failed: {result.get('error')}")
