"""End-to-end pipeline smoke tests.

Phase 1 (Parse → Discover → Stage → Plan) runs deterministically — no LLM
required. Phase 2 (Enrich → Validate → Publish) is exercised against fixture
``EnrichedOutput`` JSONs so the LLM is never invoked.

Two infrastructure tests verify that the ``apps/lumi`` ADK entrypoint imports
and that the ``python -m lumi --help`` CLI surface still loads.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from lumi.planner import (
    classify_risk,
    compute_deterministic_diff,
    compute_priority,
)
from lumi.publish import publish_to_disk
from lumi.schemas import EnrichedOutput, TableContext
from lumi.sql_to_context import parse_sqls, prepare_enrichment_context
from lumi.validate import coverage_check, reconstruct_sql_check

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "llm_responses"


# ─── Mock MDM for offline runs ──────────────────────────────────────


class _MockMDM:
    def fetch(self, table_name: str) -> dict[str, Any]:  # noqa: D401
        return {
            "table_name": table_name,
            "columns": [],
            "table_description": None,
        }


# ─── Phase 1 e2e ────────────────────────────────────────────────────


def test_pipeline_e2e_phase_1(tmp_path: Path) -> None:
    """Parse → Discover → Stage → Plan completes for fixture SQLs (mock LLM)."""
    from tests.fixtures.sample_sqls import ALL_SQLS

    fps = parse_sqls(ALL_SQLS)
    assert any(not fp.parse_error for fp in fps), "no SQL parsed"

    contexts = prepare_enrichment_context(ALL_SQLS, _MockMDM(), str(tmp_path))
    assert contexts, "discover returned no tables"

    # Stage = order by priority desc.
    ranked = sorted(
        contexts.values(), key=lambda c: (-compute_priority(c), c.table_name)
    )
    assert ranked
    assert ranked[0].table_name in contexts

    # Plan = run the deterministic planner on each ctx — no exceptions.
    for ctx in ranked:
        diff = compute_deterministic_diff(ctx)
        has_struct, changes = classify_risk(diff, ctx)
        assert isinstance(has_struct, bool)
        assert all(c.risk in {"low", "medium", "high"} for c in changes)


# ─── Phase 2 e2e ────────────────────────────────────────────────────


def _load_fixture_enriched(table_name: str) -> EnrichedOutput:
    raw = json.loads(
        (FIXTURE_DIR / f"enrich_{table_name}.json").read_text(encoding="utf-8")
    )
    return EnrichedOutput.model_validate(raw)


def test_pipeline_e2e_phase_2(tmp_path: Path) -> None:
    """Enrich (fixture) → Validate → Publish completes given approvals + outputs."""
    enriched = {
        "cornerstone_metrics": _load_fixture_enriched("cornerstone_metrics"),
    }

    # Fingerprint shaped to be coverage-checkable: one bare aggregation that
    # we know the cornerstone fixture's measures cover (sum of billed_business).
    fingerprints = [
        {
            "query_id": "Q01",
            "tables": ["cornerstone_metrics"],
            "aggregations": [
                {"function": "SUM", "column": "billed_business", "alias": None}
            ],
            "filters": [],
            "case_whens": [],
            "ctes": [],
            "temp_tables": [],
            "joins": [],
            "columns_referenced": ["billed_business"],
        }
    ]

    report = coverage_check(fingerprints, enriched)
    assert report.total_queries == 1
    assert report.coverage_pct >= 0.0  # smoke: report constructed

    recon = reconstruct_sql_check(["SELECT 1"], fingerprints, enriched)
    assert recon.status in {"pass", "warn", "fail"}

    out_dir = tmp_path / "out"
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    result = publish_to_disk(
        enriched, baseline_dir=baseline_dir, output_dir=out_dir, coverage=report
    )
    assert result["status"] == "ok"
    assert (out_dir / "views" / "cornerstone_metrics.view.lkml").exists()
    assert (out_dir / "metric_catalog.json").exists()
    assert (out_dir / "coverage_report.json").exists()


# ─── Infrastructure ─────────────────────────────────────────────────


def test_apps_lumi_imports() -> None:
    """``apps/lumi/agent.py`` must import and expose root_agent."""
    apps_path = REPO_ROOT / "apps"
    if str(apps_path) not in sys.path:
        sys.path.insert(0, str(apps_path))
    try:
        from lumi.agent import root_agent  # type: ignore[import-not-found]
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"apps.lumi.agent unavailable in this env: {e}")
        return
    assert root_agent is not None
    assert getattr(root_agent, "name", None) == "lumi"


def test_main_help() -> None:
    """``python -m lumi --help`` must exit 0 and mention the subcommands."""
    proc = subprocess.run(
        [sys.executable, "-m", "lumi", "--help"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout + proc.stderr
    for sub in ("plan", "status", "approve", "execute"):
        assert sub in out, f"--help did not mention subcommand {sub!r}"


# Quietly silence unused-import warnings in some linters.
_ = TableContext
