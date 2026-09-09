#!/usr/bin/env python3
"""Run coverage_check + reconstruct_sql_check on enriched outputs.

Reads:
  - ``data/session1_output.json``    consolidated TableContexts (from Session 1)
  - ``data/enriched/<table>.json``   one EnrichedOutput per enriched table

Runs both deterministic gates from :mod:`lumi.validate`:
  - ``coverage_check`` over the SQL fingerprints reconstructed from
    ``session1_output.json`` columns_referenced/aggregations/etc.
  - ``reconstruct_sql_check`` against the original gold SQL strings.

Prints a per-query coverage summary plus the SQL-reconstruction gate. With
``--save`` writes the full ``CoverageReport`` to disk for downstream reads.

Usage:
    python scripts/probe_validate.py
    python scripts/probe_validate.py --enriched-dir data/enriched/
    python scripts/probe_validate.py --save data/coverage_report.json

Exit codes:
    0  coverage gate passed (status=pass)
    1  coverage or sql_recon gate failed
    2  required input files missing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.config import LumiConfig  # noqa: E402
from lumi.schemas import EnrichedOutput  # noqa: E402
from lumi.sql_to_context import parse_sqls  # noqa: E402
from lumi.validate import coverage_check, reconstruct_sql_check  # noqa: E402

logger = logging.getLogger("probe.validate")


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


def _fingerprints_from_session1(session1_path: Path) -> list[dict[str, Any]]:
    """Reconstruct fingerprint dicts from the Session 1 dump.

    The session1_output.json is keyed by table name with TableContext payloads.
    We synthesise one fingerprint per query_id mentioned across all tables —
    enough for coverage_check / reconstruct_sql_check which mostly care about
    tables/aggregations/filters/case_whens/joins.
    """
    raw = json.loads(session1_path.read_text(encoding="utf-8"))
    by_qid: dict[str, dict[str, Any]] = {}
    for table_name, ctx in raw.items():
        for qid in ctx.get("queries_using_this") or []:
            fp = by_qid.setdefault(
                qid,
                {
                    "query_id": qid,
                    "tables": [],
                    "aggregations": [],
                    "filters": [],
                    "case_whens": [],
                    "ctes": [],
                    "temp_tables": [],
                    "joins": [],
                    "columns_referenced": [],
                },
            )
            if table_name not in fp["tables"]:
                fp["tables"].append(table_name)
            for k_src, k_dst in (
                ("aggregations", "aggregations"),
                ("filters_on_this", "filters"),
                ("case_whens", "case_whens"),
                ("ctes_referencing_this", "ctes"),
                ("temp_tables_referencing_this", "temp_tables"),
                ("joins_involving_this", "joins"),
                ("columns_referenced", "columns_referenced"),
            ):
                for entry in ctx.get(k_src) or []:
                    if entry not in fp[k_dst]:
                        fp[k_dst].append(entry)
    return [by_qid[q] for q in sorted(by_qid)]


def _print_report(report, recon_gate) -> None:
    print(f"\nCoverage: {report.covered}/{report.total_queries} "
          f"({report.coverage_pct:.1f}%)  all_lookml_valid={report.all_lookml_valid}")
    if report.top_gaps:
        print("\nTop gaps:")
        for g in report.top_gaps[:10]:
            print(f"   - {g}")
    print(f"\nPer query ({len(report.per_query)}):")
    for q in report.per_query:
        icon = "✓" if q.covered else "✗"
        bits = []
        if q.measures_missing:
            bits.append(f"miss_measures={len(q.measures_missing)}")
        if q.dimensions_missing:
            bits.append(f"miss_dims={len(q.dimensions_missing)}")
        if q.filters_missing:
            bits.append(f"miss_filters={len(q.filters_missing)}")
        if not q.derived_tables_exist:
            bits.append("no_derived_table")
        if not q.structural_filters_baked:
            bits.append("structural_filter_unbaked")
        suffix = (" — " + ", ".join(bits)) if bits else ""
        print(f"   {icon} {q.query_id}{suffix}")

    print(
        f"\nSQL reconstruction: status={recon_gate.status} "
        f"blocking={len(recon_gate.blocking_failures)} "
        f"warnings={len(recon_gate.warnings)}"
    )
    for b in recon_gate.blocking_failures[:10]:
        print(f"   ✗ {b}")
    for w in recon_gate.warnings[:5]:
        print(f"   ⚠ {w}")


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_validate")
    parser.add_argument("--session1", default="data/session1_output.json")
    parser.add_argument("--enriched-dir", default="data/enriched/")
    parser.add_argument(
        "--gold-queries",
        default=None,
        help="Directory of original gold SQLs (default: from LumiConfig)",
    )
    parser.add_argument(
        "--save",
        default=None,
        help="Write the full CoverageReport JSON to this path",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    _refuse_in_repo_sa_key()

    cfg = LumiConfig()
    session1_path = Path(args.session1)
    enriched_dir = Path(args.enriched_dir)
    gold_dir = Path(args.gold_queries) if args.gold_queries else Path(cfg.gold_queries_dir)

    missing: list[str] = []
    if not session1_path.exists():
        missing.append(f"  - {session1_path} (run scripts/run_session1.py)")
    if not enriched_dir.exists() or not list(enriched_dir.glob("*.json")):
        missing.append(
            f"  - {enriched_dir}/*.json "
            "(run python -m lumi execute, or drop fixtures here)"
        )
    if missing:
        print("ERROR: missing inputs:", file=sys.stderr)
        for m in missing:
            print(m, file=sys.stderr)
        return 2

    enriched = _load_enriched(enriched_dir)
    print(f"Loaded {len(enriched)} enriched output(s) from {enriched_dir}")

    fingerprints = _fingerprints_from_session1(session1_path)
    print(f"Reconstructed {len(fingerprints)} fingerprint(s) from {session1_path}")

    report = coverage_check(fingerprints, enriched)

    gold_sqls: list[str] = []
    if gold_dir.exists():
        gold_sqls = [f.read_text(encoding="utf-8") for f in sorted(gold_dir.glob("*.sql"))]
        # parse_sqls just to surface any obvious parser errors before recon
        _ = parse_sqls(gold_sqls)
    recon_gate = reconstruct_sql_check(gold_sqls, fingerprints, enriched)

    _print_report(report, recon_gate)

    if args.save:
        save_path = Path(args.save)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(
            json.dumps(
                {
                    "coverage": report.model_dump(),
                    "sql_reconstruction": recon_gate.model_dump(),
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print(f"\nWrote coverage report → {save_path}")

    coverage_pass = report.coverage_pct >= cfg.coverage_target_pct
    recon_pass = recon_gate.status != "fail"
    return 0 if coverage_pass and recon_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
