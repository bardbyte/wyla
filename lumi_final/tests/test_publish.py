"""Tests for :mod:`lumi.publish` — additive merge, catalogs, on-disk emission.

All tests use ``tmp_path`` — none touch the real ``output/`` directory.
"""

from __future__ import annotations

import json
from pathlib import Path

import lkml
import pytest

from lumi.guardrails import check_pre_publish
from lumi.publish import (
    additive_merge_view,
    build_metric_catalog,
    publish_to_disk,
)
from lumi.schemas import EnrichedOutput

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "llm_responses"


# ─── Helpers ────────────────────────────────────────────────────────


def _load_fixture(table_name: str) -> EnrichedOutput:
    raw = json.loads(
        (FIXTURE_DIR / f"enrich_{table_name}.json").read_text(encoding="utf-8")
    )
    return EnrichedOutput.model_validate(raw)


def _baseline_view(table_name: str, sql: str = "old_sql_expr") -> str:
    """Build a tiny baseline view used to prove the merge preserves baseline SQL."""
    return f"""view: {table_name} {{
  sql_table_name: `axp-lumi.dw.{table_name}` ;;

  dimension: business_segment {{
    type: string
    sql: ${{TABLE}}.{sql} ;;
  }}

  measure: total_billed_business {{
    type: sum
    sql: ${{TABLE}}.billed_business ;;
  }}
}}
"""


# ─── Merge: preserve sql, append measures ───────────────────────────


def test_merge_preserves_existing_sql() -> None:
    """Enriched proposes a different sql expression — baseline wins."""
    table = "cornerstone_metrics"
    baseline = _baseline_view(table, sql="bus_seg")  # original baseline sql

    enriched = _load_fixture(table)
    merged = additive_merge_view(baseline, enriched.view_lkml)

    parsed = lkml.load(merged)
    view = parsed["views"][0]
    by_name = {d["name"]: d for d in view.get("dimensions", [])}
    assert "business_segment" in by_name
    # The sql expression from baseline must survive.
    assert "bus_seg" in by_name["business_segment"]["sql"]


def test_merge_appends_new_measure() -> None:
    """Baseline measure is preserved; new enriched measures are appended."""
    table = "cornerstone_metrics"
    baseline = _baseline_view(table)

    enriched = _load_fixture(table)
    merged = additive_merge_view(baseline, enriched.view_lkml)

    parsed = lkml.load(merged)
    view = parsed["views"][0]
    measure_names = {m["name"] for m in view.get("measures", [])}
    # Baseline measure preserved.
    assert "total_billed_business" in measure_names
    # And at least one enriched-only measure was appended.
    assert measure_names - {"total_billed_business"}, (
        f"Expected new measures appended; only got {measure_names!r}"
    )


# ─── Catalogs ───────────────────────────────────────────────────────


def test_metric_catalog_complete() -> None:
    """Every measure across every enriched view appears in the catalog."""
    enriched = {
        "cornerstone_metrics": _load_fixture("cornerstone_metrics"),
        "risk_pers_acct_history": _load_fixture("risk_pers_acct_history"),
    }
    catalog = build_metric_catalog(enriched)

    # Walk fixtures by hand to count expected measures per view.
    expected_field_keys: set[str] = set()
    for table_name, eo in enriched.items():
        for src in [eo.view_lkml, *(eo.derived_table_views or [])]:
            try:
                parsed = lkml.load(src)
            except Exception:  # noqa: BLE001 — guarded path
                continue
            for v in parsed.get("views") or []:
                vname = v.get("name", "")
                for m in v.get("measures") or []:
                    if m.get("name"):
                        expected_field_keys.add(f"{vname}.{m['name']}")
        _ = table_name  # keep unused-var lint quiet

    catalog_keys = {row["field_key"] for row in catalog}
    missing = expected_field_keys - catalog_keys
    assert not missing, f"metric catalog missing entries: {missing}"


# ─── Disk emission ──────────────────────────────────────────────────


def test_publish_to_disk_writes_all_files(tmp_path: Path) -> None:
    enriched = {
        "cornerstone_metrics": _load_fixture("cornerstone_metrics"),
        "risk_pers_acct_history": _load_fixture("risk_pers_acct_history"),
    }
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    (baseline_dir / "cornerstone_metrics.view.lkml").write_text(
        _baseline_view("cornerstone_metrics"), encoding="utf-8"
    )
    out_dir = tmp_path / "out"

    result = publish_to_disk(enriched, baseline_dir=baseline_dir, output_dir=out_dir)

    assert result["status"] == "ok"
    files = result["files_written"]
    file_strs = [Path(f).name for f in files]

    # Two view files (one per enriched output) + at least one derived view for risk
    assert "cornerstone_metrics.view.lkml" in file_strs
    assert "risk_pers_acct_history.view.lkml" in file_strs

    # Single combined model file.
    assert "lumi_enriched.model.lkml" in file_strs

    # 3 catalogs always (coverage_report only when coverage passed).
    for catalog in ("metric_catalog.json", "filter_catalog.json", "golden_questions.json"):
        assert catalog in file_strs

    # filter_catalog.json must round-trip.
    fc_path = out_dir / "filter_catalog.json"
    assert isinstance(json.loads(fc_path.read_text(encoding="utf-8")), list)


def test_publish_passes_pre_publish_guardrail(tmp_path: Path) -> None:
    """End-to-end: publish then run check_pre_publish — should not block."""
    enriched = {
        "cornerstone_metrics": _load_fixture("cornerstone_metrics"),
    }
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    out_dir = tmp_path / "out"

    publish_to_disk(enriched, baseline_dir=baseline_dir, output_dir=out_dir)

    gate = check_pre_publish(str(out_dir), str(baseline_dir))
    # With no coverage_report.json on disk the guardrail emits warnings, but
    # blocking failures must be empty (everything that IS on disk lints + valid).
    assert not gate.blocking_failures, gate.blocking_failures
    assert gate.status in ("pass", "warn")


# Ensure unused-import linters stay quiet on imported pytest (some envs strip
# unused imports otherwise).
_ = pytest
