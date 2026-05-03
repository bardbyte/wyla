"""LUMI pipeline — two phases of ADK SequentialAgents around the human gate.

Phase 1 (cheap): Parse → Discover → Stage → Plan        (Stages 1-4)
                 [HUMAN APPROVAL GATE — review_queue/]
Phase 2 (expensive): Enrich → Validate → Publish        (Stages 5-7)

Sessions 1-5 fill in the run_plan_phase / run_execute_phase methods.
Session 5 wires the actual ADK SequentialAgent / ParallelAgent / LoopAgent
constructions per DESIGN.md §2.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from lumi.config import LumiConfig

logger = logging.getLogger("lumi.pipeline")


class PipelineHaltError(Exception):
    """Raised when a blocking guardrail fails."""


class LumiPipeline:
    """Two-phase pipeline. Stage states are tracked on disk under output/."""

    STAGES = (
        "Parse",
        "Discover",
        "Stage",
        "Plan",
        "Enrich",
        "Validate",
        "Publish",
    )

    def __init__(self, config: LumiConfig | None = None) -> None:
        self.config = config or LumiConfig()
        self.gate_results: list[Any] = []

    # ─── Phase 1 ─────────────────────────────────────────────

    def run_plan_phase(self, sql_inputs: list[str]) -> None:
        """Stages 1-4. Writes review_queue/<table>.plan.md per table.

        Sessions filling this in:
          S1: parse + discover (sql_to_context.prepare_enrichment_context)
          S2: stage + plan     (stage.prioritize_tables, plan.* + LlmAgent)
          S5: ADK wiring       (SequentialAgent of all four)
        """
        # TODO S5: wire SequentialAgent(parse, discover, stage, plan_parallel)
        raise NotImplementedError("Session 5 wires this together")

    # ─── Human-approval gate (file-system blocker) ───────────

    def collect_approvals(self) -> list[Any]:
        """Scan review_queue/ for approval markers. Built in Session 3."""
        # from lumi.approval import collect_approvals
        # return collect_approvals(queue_dir="review_queue/")
        raise NotImplementedError("Session 3 builds this")

    # ─── Phase 2 ─────────────────────────────────────────────

    def run_execute_phase(self, dry_run: bool = False) -> None:
        """Stages 5-7. Only runs for tables with PlanApproval(approved=True)."""
        # TODO S5: wire SequentialAgent(enrich_parallel, evaluate_loop, publish)
        raise NotImplementedError("Session 5 wires this together")

    # ─── Status display ──────────────────────────────────────

    def print_status(self) -> None:
        """Print 7-stage progress matching the sketch in CLAUDE.md / DESIGN.md.

        Reads markers from output/ + review_queue/ to determine completion:
          ✓  stage complete and gates passed
          ●  stage in progress (e.g. "Plan: 3/6 plans written")
          ○  stage not started
        """
        states = self._read_stage_states()
        for stage in self.STAGES:
            marker, detail = states.get(stage, ("○", ""))
            line = f"{marker} {stage}:".ljust(15) + detail
            print(line)

    def _read_stage_states(self) -> dict[str, tuple[str, str]]:
        """Inspect disk for stage completion markers.

        Sessions 1-5 will enrich this with real markers; the file-system
        artifacts they each write are what print_status keys off of.
        """
        out_dir = Path(self.config.output_dir)
        queue_dir = Path("review_queue")
        states: dict[str, tuple[str, str]] = {s: ("○", "") for s in self.STAGES}

        if queue_dir.exists():
            plans = list(queue_dir.glob("*.plan.md"))
            approvals = list(queue_dir.glob("*.approval.json"))
            if plans:
                states["Parse"] = ("✓", "")
                states["Discover"] = ("✓", "")
                states["Stage"] = ("✓", "")
                states["Plan"] = (
                    "●",
                    f"{len(plans)} plans written, {len(approvals)} approved",
                )

        if (out_dir / "coverage_report.json").exists():
            states["Enrich"] = ("✓", "")
            states["Validate"] = ("✓", "")

        if (out_dir / "publish_log.json").exists():
            states["Publish"] = ("✓", "")

        return states
