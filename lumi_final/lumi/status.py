"""LUMI observability — three zoom levels in one file.

Writes lumi_status.md after every stage completion.

Zoom 1 (5 sec, phone): status line per stage at top of file
Zoom 2 (30 sec, table detail): per-table plan + progress
Zoom 3 (2 min, debug): gate results + token counts + timing

Usage:
    status = LumiStatus()
    status.start_stage("parse")
    ...
    status.complete_stage("parse", summary="10 queries → 6 tables")
    status.write("lumi_status.md")
"""

from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from lumi.schemas import GateResult, CoverageReport

logger = logging.getLogger("lumi.status")

STAGE_ORDER = ["parse", "discover", "stage", "plan", "review",
               "enrich", "validate", "publish"]
STAGE_LABELS = {
    "parse":    "Parse SQL",
    "discover": "Discover tables",
    "stage":    "Priority staging",
    "plan":     "Expert assessment",
    "review":   "Human review",
    "enrich":   "Enrich LookML",
    "validate": "Validate + reconstruct SQL",
    "publish":  "Learn + publish",
}


@dataclass
class StageStatus:
    name: str
    label: str
    status: str = "pending"  # pending, running, done, failed, waiting
    summary: str = ""
    started_at: float | None = None
    completed_at: float | None = None
    gate_result: GateResult | None = None
    details: dict = field(default_factory=dict)

    @property
    def duration_str(self) -> str:
        if self.started_at and self.completed_at:
            d = self.completed_at - self.started_at
            if d < 60:
                return f"{d:.0f}s"
            return f"{d / 60:.1f}m"
        return ""

    @property
    def icon(self) -> str:
        return {
            "pending": "○",
            "running": "●",
            "done": "✓",
            "failed": "✗",
            "waiting": "◉",
        }[self.status]


@dataclass
class TableStatus:
    table_name: str
    priority_rank: int
    query_count: int
    plan_status: str = "pending"   # pending, planned, approved, enriching, done, failed
    enrichment_status: str = ""
    coverage_pct: float | None = None
    sql_reconstruction: str = ""   # pass, fail, pending


class LumiStatus:
    """Pipeline status tracker. Call after each stage to update."""

    def __init__(self):
        self.stages: dict[str, StageStatus] = {
            name: StageStatus(name=name, label=label)
            for name, label in STAGE_LABELS.items()
        }
        self.tables: dict[str, TableStatus] = {}
        self.coverage: CoverageReport | None = None
        self.run_started_at = time.time()
        self.total_tokens_in = 0
        self.total_tokens_out = 0
        self.total_llm_calls = 0

    def start_stage(self, stage: str, summary: str = ""):
        s = self.stages[stage]
        s.status = "running"
        s.started_at = time.time()
        s.summary = summary
        logger.info(f"Stage {stage}: started — {summary}")

    def complete_stage(
        self,
        stage: str,
        summary: str = "",
        gate: GateResult | None = None,
        details: dict | None = None,
    ):
        s = self.stages[stage]
        s.status = "done" if (not gate or gate.status != "fail") else "failed"
        s.completed_at = time.time()
        s.summary = summary
        s.gate_result = gate
        s.details = details or {}
        logger.info(f"Stage {stage}: {s.status} — {summary} ({s.duration_str})")

    def wait_stage(self, stage: str, summary: str = ""):
        s = self.stages[stage]
        s.status = "waiting"
        s.summary = summary

    def register_table(self, table_name: str, priority_rank: int, query_count: int):
        self.tables[table_name] = TableStatus(
            table_name=table_name,
            priority_rank=priority_rank,
            query_count=query_count,
        )

    def update_table(self, table_name: str, **kwargs):
        if table_name in self.tables:
            for k, v in kwargs.items():
                setattr(self.tables[table_name], k, v)

    def add_llm_call(self, tokens_in: int = 0, tokens_out: int = 0):
        self.total_llm_calls += 1
        self.total_tokens_in += tokens_in
        self.total_tokens_out += tokens_out

    def write(self, path: str = "lumi_status.md"):
        """Write the full status document."""
        Path(path).write_text(self._render())
        logger.info(f"Status written to {path}")

    def _render(self) -> str:
        lines = []

        # ─── ZOOM 1: Pipeline overview (phone view) ──────
        lines.append("# LUMI pipeline status")
        lines.append(f"*Updated: {datetime.now().strftime('%H:%M:%S')}*\n")

        for name in STAGE_ORDER:
            s = self.stages[name]
            duration = f" ({s.duration_str})" if s.duration_str else ""
            lines.append(f"{s.icon} **{s.label}**{duration}  {s.summary}")

        # Quick stats
        elapsed = time.time() - self.run_started_at
        lines.append(f"\n*Elapsed: {elapsed/60:.1f}m | "
                      f"LLM calls: {self.total_llm_calls} | "
                      f"Tokens: {self.total_tokens_in + self.total_tokens_out:,}*")

        if self.coverage:
            lines.append(f"\n**Coverage: {self.coverage.coverage_pct:.0f}%** "
                          f"({self.coverage.covered}/{self.coverage.total_queries} queries)")

        lines.append("\n---\n")

        # ─── ZOOM 2: Per-table status ─────────────────────
        lines.append("## Table progress\n")

        sorted_tables = sorted(
            self.tables.values(),
            key=lambda t: t.priority_rank
        )

        lines.append("| # | Table | Queries | Plan | Enrich | SQL check |")
        lines.append("|---|-------|---------|------|--------|-----------|")

        for t in sorted_tables:
            plan_icon = {
                "pending": "○", "planned": "◐",
                "approved": "✓", "enriching": "●",
                "done": "✓", "failed": "✗"
            }.get(t.plan_status, "?")

            enrich_icon = "✓" if t.enrichment_status == "done" \
                else "●" if t.enrichment_status == "running" \
                else "○"

            sql_icon = {"pass": "✓", "fail": "✗", "pending": "○"}.get(
                t.sql_reconstruction, "○"
            )

            lines.append(
                f"| {t.priority_rank} | {t.table_name} | {t.query_count} | "
                f"{plan_icon} {t.plan_status} | {enrich_icon} | {sql_icon} |"
            )

        lines.append("\n---\n")

        # ─── ZOOM 3: Gate details ─────────────────────────
        lines.append("## Gate results\n")

        for name in STAGE_ORDER:
            s = self.stages[name]
            if s.gate_result:
                g = s.gate_result
                lines.append(f"### {s.label} — {g.status.upper()}\n")

                for check in g.checks:
                    icon = "✓" if check["passed"] else "✗"
                    lines.append(f"- {icon} {check['name']}: {check.get('message', '')}")

                if g.blocking_failures:
                    lines.append("\n**Blocking:**")
                    for bf in g.blocking_failures:
                        lines.append(f"- ✗ {bf}")

                if g.warnings:
                    lines.append("\n**Warnings:**")
                    for w in g.warnings:
                        lines.append(f"- ⚠ {w}")

                lines.append("")

        # Details section
        if any(s.details for s in self.stages.values()):
            lines.append("## Details\n")
            for name in STAGE_ORDER:
                s = self.stages[name]
                if s.details:
                    lines.append(f"### {s.label}")
                    lines.append(f"```json\n{json.dumps(s.details, indent=2)}\n```\n")

        return "\n".join(lines)
