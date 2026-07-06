#!/usr/bin/env python3
"""BQ extraction readiness probe — is everything the graph needs on disk?

Run AFTER `bq_batch_extract.py` (no network needed — this validates the
extraction OUTPUT, not the connection; use bq_capabilities_probe.py for
the SA/network side):

    python scripts/probe_bq_readiness.py \
        --extract-dir ./data/real_extractions \
        --tables-yaml semantic-graph/config/tables.yaml

For every table folder it (1) checks which extraction artifacts exist and
how much signal each carries, (2) runs the REAL synapse bq_loader
conversion into a temp dir to prove the graph builder can consume it,
and (3) buckets tables the same way the batch extractor does:

    fully_usable      schema + meta + profiling + usage present
    partially_usable  schema present; some optional signal missing
                      (expected on views — non-blocking)
    genuinely_failed  no schema → the graph cannot use this table

Prints a per-table matrix + a paste-back SUMMARY; exit code = number of
genuinely_failed tables.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "synapse"))

GREEN, YELLOW, RED, DIM, END = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m"

# artifact → (label, what the graph gets from it)
CORE = {
    "1_1__columns.csv": "schema columns",
    "1_3__table_meta.json": "DDL + table_type",
}
VALUED = {
    "1_2__col_descriptions.csv": "column descriptions",
    "1_4__table_options.csv": "labels/options",
    "1_5__constraints.csv": "declared PK/FK",
    "2_1__size_freshness.csv": "row count + last_modified",
    "2_2__partitions.csv": "partition stats",
    "3_1__cardinality_nulls.csv": "profiling (distinct/nulls)",
    "4_1__top_users.csv": "top users (90d)",
    "4_3__co_queried_tables.csv": "co-query neighbors",
    "4_4__sample_queries.json": "sample queries (corpus)",
    "4_5__failed_queries.csv": "failed queries (naming gold)",
    "7_1__cost_30d.csv": "cost telemetry",
    "9_1__upstream_tables.csv": "empirical upstream lineage",
    "9_2__downstream_tables.csv": "empirical downstream lineage",
}


def _csv_rows(path: Path) -> int:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            return max(0, sum(1 for _ in csv.reader(fh)) - 1)  # minus header
    except OSError:
        return 0


def _probe_table(table_dir: Path) -> dict:
    name = table_dir.name
    found: dict[str, int | str] = {}
    missing_core, missing_valued = [], []

    for fname, label in CORE.items():
        path = table_dir / fname
        if not path.exists():
            missing_core.append(label)
        elif fname.endswith(".csv"):
            found[label] = _csv_rows(path)
        else:
            try:
                meta = json.loads(path.read_text(encoding="utf-8"))
                found[label] = (f"{meta.get('table_type', '?')}, "
                                f"ddl {len(str(meta.get('ddl') or ''))}ch")
            except (OSError, json.JSONDecodeError):
                found[label] = "unreadable"

    for fname, label in VALUED.items():
        path = table_dir / fname
        if not path.exists():
            missing_valued.append(label)
        elif fname.endswith(".csv"):
            found[label] = _csv_rows(path)
        else:
            found[label] = "present"
    topcounts = list(table_dir.glob("3_2__topcount__*.csv"))
    found["low-card value profiles"] = len(topcounts)

    # the decisive check: does the REAL loader convert this folder?
    loader_status, loader_note, n_artifacts = "skipped", "", 0
    try:
        from synapse.loaders.bq_loader import load_bq_for_table
        with tempfile.TemporaryDirectory() as tmp:
            # source_dir is the PARENT that contains <table>/ (loader contract)
            result = load_bq_for_table(
                name, source_dir=table_dir.parent, out_dir=Path(tmp))
            loader_status = result.status
            loader_note = (result.error or "; ".join(result.warnings[:2]))[:80]
            n_artifacts = len(result.artifacts_written)
    except Exception as exc:  # loader crash = genuinely broken folder
        loader_status, loader_note = "crash", str(exc)[:80]

    n_cols = found.get("schema columns", 0)
    if missing_core or loader_status in ("error", "crash") or not n_cols:
        bucket = "genuinely_failed"
    elif missing_valued:
        bucket = "partially_usable"
    else:
        bucket = "fully_usable"
    return {
        "table": name, "bucket": bucket, "n_columns": n_cols,
        "found": found, "missing_core": missing_core,
        "missing_valued": missing_valued,
        "loader": {"status": loader_status, "note": loader_note,
                   "canonical_artifacts": n_artifacts},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--extract-dir", default="./data/real_extractions")
    ap.add_argument("--tables-yaml", default="",
                    help="flag manifest tables with no extraction folder")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    root = Path(args.extract_dir).expanduser()
    if not root.is_dir():
        print(f"{RED}extract dir not found: {root}{END}")
        return 2
    table_dirs = sorted(d for d in root.iterdir()
                        if d.is_dir() and not d.name.startswith("_"))
    reports = [_probe_table(d) for d in table_dirs]

    not_extracted: list[str] = []
    if args.tables_yaml:
        from synapse.utils.manifest import read_tables_manifest
        expected = {t["name"] for t in read_tables_manifest(args.tables_yaml)}
        not_extracted = sorted(expected - {r["table"] for r in reports})

    print(f"\n{'table':38} {'bucket':18} {'cols':>5} {'prof':>5} "
          f"{'users':>5} {'loader':10}")
    print("─" * 92)
    for r in reports:
        color = (GREEN if r["bucket"] == "fully_usable"
                 else YELLOW if r["bucket"] == "partially_usable" else RED)
        print(f"{r['table'][:38]:38} {color}{r['bucket']:18}{END} "
              f"{r['n_columns']:>5} "
              f"{r['found'].get('profiling (distinct/nulls)', 0):>5} "
              f"{r['found'].get('top users (90d)', 0):>5} "
              f"{r['loader']['status']:10}"
              + (f" {DIM}{r['loader']['note']}{END}"
                 if r["loader"]["note"] else ""))
    for name in not_extracted:
        print(f"{name[:38]:38} {RED}{'not_extracted':18}{END} "
              f"{'–':>5} {'–':>5} {'–':>5} {'–':10}")

    buckets = {b: sum(1 for r in reports if r["bucket"] == b)
               for b in ("fully_usable", "partially_usable",
                         "genuinely_failed")}
    summary = {"extract_dir": str(root), "buckets": buckets,
               "not_extracted": not_extracted,
               "tables": [{k: r[k] for k in
                           ("table", "bucket", "n_columns", "missing_core",
                            "missing_valued", "loader")} for r in reports]}
    print(f"\n{'═'*28} SUMMARY (paste this back) {'═'*28}")
    print(json.dumps(summary if args.json else {
        "buckets": buckets, "not_extracted": not_extracted,
        "per_table": {r["table"]: {
            "bucket": r["bucket"], "cols": r["n_columns"],
            "loader": r["loader"]["status"],
            "missing": r["missing_valued"][:4] + r["missing_core"],
        } for r in reports},
    }, indent=2))
    (root / "_readiness_report.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    print(f"{DIM}full report → {root / '_readiness_report.json'}{END}")
    return buckets["genuinely_failed"]


if __name__ == "__main__":
    sys.exit(main())
