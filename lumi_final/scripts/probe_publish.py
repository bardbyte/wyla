#!/usr/bin/env python3
"""Run :func:`lumi.publish.publish_to_disk` against ``data/enriched/`` outputs.

Default behaviour writes to ``output_dryrun/`` so the real ``output/`` tree is
left alone. Pass ``--apply`` to publish to the canonical output dir.

After writing, every emitted ``.view.lkml`` is round-tripped through
``lkml.load`` to confirm it lints cleanly.

Usage:
    python scripts/probe_publish.py
    python scripts/probe_publish.py --enriched-dir data/enriched/ --output output_dryrun/
    python scripts/probe_publish.py --apply              # writes to ./output/

Exit codes:
    0  publish succeeded and pre-publish guardrail passed
    1  publish wrote files but a guardrail flagged blocking failures
    2  required input files missing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import lkml

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.config import LumiConfig  # noqa: E402
from lumi.guardrails import check_pre_publish  # noqa: E402
from lumi.publish import publish_to_disk  # noqa: E402
from lumi.schemas import CoverageReport, EnrichedOutput  # noqa: E402

logger = logging.getLogger("probe.publish")


def _refuse_in_repo_sa_key() -> None:
    import os

    val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not val:
        return
    p = Path(val).resolve()
    try:
        p.relative_to(REPO_ROOT.parent)
        print(
            f"ERROR: GOOGLE_APPLICATION_CREDENTIALS points inside the repo: {p}\n"
            "Move the SA JSON outside the repo.",
            file=sys.stderr,
        )
        sys.exit(2)
    except ValueError:
        return


def _load_enriched(enriched_dir: Path) -> dict[str, EnrichedOutput]:
    out: dict[str, EnrichedOutput] = {}
    for f in sorted(enriched_dir.glob("*.json")):
        try:
            raw = json.loads(f.read_text(encoding="utf-8"))
            out[f.stem] = EnrichedOutput.model_validate(raw)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not load %s: %s", f, e)
    return out


def _load_coverage(path: Path) -> CoverageReport | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        # probe_validate.py wraps coverage under "coverage"; accept both shapes.
        cov = raw.get("coverage", raw)
        return CoverageReport.model_validate(cov)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not load coverage report %s: %s", path, e)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_publish")
    parser.add_argument("--enriched-dir", default="data/enriched/")
    parser.add_argument("--baseline", default=None)
    parser.add_argument(
        "--coverage",
        default="data/coverage_report.json",
        help="Path to a saved coverage report (probe_validate --save).",
    )
    parser.add_argument(
        "--output",
        default="output_dryrun/",
        help="Output directory (default: output_dryrun/, won't touch real output/)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to LumiConfig.output_dir (real ./output/) instead of dryrun",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    _refuse_in_repo_sa_key()

    cfg = LumiConfig()
    enriched_dir = Path(args.enriched_dir)
    baseline_dir = Path(args.baseline) if args.baseline else Path(cfg.baseline_views_dir)
    output_dir = Path(cfg.output_dir) if args.apply else Path(args.output)

    if not enriched_dir.exists() or not list(enriched_dir.glob("*.json")):
        print(
            f"ERROR: no enriched outputs in {enriched_dir}. "
            "Run `python -m lumi execute` (or drop fixtures here).",
            file=sys.stderr,
        )
        return 2

    enriched = _load_enriched(enriched_dir)
    print(f"Loaded {len(enriched)} enriched output(s) from {enriched_dir}")
    coverage = _load_coverage(Path(args.coverage))
    if coverage:
        print(
            f"Loaded coverage: {coverage.covered}/{coverage.total_queries} "
            f"({coverage.coverage_pct:.1f}%)"
        )

    result = publish_to_disk(
        enriched, baseline_dir=baseline_dir, output_dir=output_dir, coverage=coverage
    )
    print(f"\nPublished status={result['status']} to {output_dir}")
    for f in result["files_written"]:
        print(f"   wrote {f}")

    # Re-lint every view we wrote.
    bad: list[str] = []
    for f in result["files_written"]:
        if not f.endswith(".view.lkml"):
            continue
        try:
            lkml.load(Path(f).read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            bad.append(f"{f}: {e}")
    if bad:
        print("\n✗ lkml round-trip FAILED on:")
        for b in bad:
            print(f"   ✗ {b}")
    else:
        print("\n✓ every emitted .view.lkml round-trips through lkml.load")

    gate = check_pre_publish(str(output_dir), str(baseline_dir))
    icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}[gate.status]
    print(f"\n{icon} guardrail [{gate.stage}] — {gate.status.upper()}")
    for c in gate.checks:
        ci = "✓" if c["passed"] else "✗"
        print(f"   {ci} {c['name']:<22} {c.get('message', '')}")
    for w in gate.warnings:
        print(f"   ⚠ {w}")
    for b in gate.blocking_failures:
        print(f"   ✗ BLOCKING: {b}")

    return 0 if (gate.status != "fail" and not bad) else 1


if __name__ == "__main__":
    raise SystemExit(main())
