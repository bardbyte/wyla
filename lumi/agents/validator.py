"""Validator — deterministic CustomAgent that runs validate_coverage and writes
enriched LookML files + reports to disk.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types as genai_types

from lumi.schemas import (
    EnrichedView,
    JoinPattern,
    LumiConfig,
    ParsedQuery,
    VocabReport,
)
from lumi.tools.lookml_tools import write_lookml_files
from lumi.tools.validation_tools import validate_coverage

logger = logging.getLogger(__name__)


class Validator(BaseAgent):
    def __init__(self, name: str = "Validator") -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        state = ctx.session.state
        cfg = LumiConfig.model_validate(state["lumi_config"])

        enriched_views: dict[str, EnrichedView] = {
            view_name: EnrichedView.model_validate(raw)
            for view_name, raw in _collect_enriched_views(state).items()
        }

        gold_queries = [ParsedQuery.model_validate(q) for q in state["gold_queries"]]
        join_patterns = [JoinPattern.model_validate(p) for p in state.get("join_graphs", [])]
        view_name_to_table: dict[str, str] = state.get("view_name_to_table", {})

        out_root = Path(cfg.output.directory)
        views_dir = out_root / cfg.output.views_subdir
        model_dir = out_root / cfg.output.model_subdir
        reports_dir = out_root / cfg.output.reports_subdir
        for d in (views_dir, model_dir, reports_dir):
            d.mkdir(parents=True, exist_ok=True)

        write_res = write_lookml_files(enriched_views, views_dir)
        if write_res["status"] != "success":
            raise RuntimeError(f"write_lookml_files failed: {write_res['error']}")

        model_text = state.get("model_file_text_enriched", "")
        if model_text:
            model_filename = Path(cfg.git.model_file).name or "analytics.model.lkml"
            (model_dir / model_filename).write_text(model_text, encoding="utf-8")

        cov_res = validate_coverage(
            gold_queries=gold_queries,
            enriched_views=enriched_views,
            view_name_to_table=view_name_to_table,
            explore_patterns=join_patterns,
        )
        report = cov_res["report"]
        (reports_dir / "coverage_report.json").write_text(
            report.model_dump_json(indent=2), encoding="utf-8"
        )

        vocab_raw = state.get("vocab_report")
        if vocab_raw:
            vr = VocabReport.model_validate(vocab_raw)
            (reports_dir / "vocab_report.json").write_text(
                vr.model_dump_json(indent=2), encoding="utf-8"
            )

        gap_summary = {
            "coverage_pct": report.coverage_pct,
            "passed": report.passed,
            "partial": report.partial,
            "failed": report.failed,
            "by_source": report.coverage_by_source,
            "failure_reasons": _count_reasons(report),
        }
        (reports_dir / "gap_report.json").write_text(
            json.dumps(gap_summary, indent=2), encoding="utf-8"
        )

        msg = (
            f"Validator complete: {report.passed}/{report.total_queries} pass "
            f"({report.coverage_pct}%), {report.partial} partial, {report.failed} fail. "
            f"Output at {out_root}"
        )
        logger.info(msg)
        yield Event(
            author=self.name,
            actions=EventActions(state_delta={"coverage_report": report.model_dump()}),
            content=genai_types.Content(role="model", parts=[genai_types.Part(text=msg)]),
        )


def _collect_enriched_views(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Gather state keys shaped like `enriched_view__{view_name}` into a flat map."""
    prefix = "enriched_view__"
    collected: dict[str, dict[str, Any]] = {}
    for key, val in state.items():
        if key.startswith(prefix) and isinstance(val, dict):
            view_name = key[len(prefix):]
            collected[view_name] = val
    return collected


def _count_reasons(report: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for f in report.failures:
        counts[f.reason] = counts.get(f.reason, 0) + 1
    return counts
