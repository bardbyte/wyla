#!/usr/bin/env python3
"""BigQuery warehouse extraction — the production pull behind the graph.

    python scripts/bq_warehouse_extract.py probe   [--table gms_transaction]
    python scripts/bq_warehouse_extract.py plan    [--tables a,b,c]
    python scripts/bq_warehouse_extract.py run     [--tables a,b,c] [--fresh]
    python scripts/bq_warehouse_extract.py index
    python scripts/bq_warehouse_extract.py status

    probe    what this identity can see, one table, every surface, and
             WHERE the 30-day query history actually is — before spending
    plan     every phase without a byte billed: dry-run cost plan per
             table, written to <output_dir>/_plan/<run_id>.json
    run      the extraction (resumable; --fresh forgets the checkpoint,
             --force a,b re-extracts named tables)
    index    re-derive every table's 17_queries_30d/ from the corpus in
             _history/ — offline, no network
    status   what is on disk: the last run report, per-table completeness

Connection: the SAME contract as scripts/bq_check.py — .env →
GOOGLE_APPLICATION_CREDENTIALS + BQ_PROJECT_ID + BIGQUERY_URL,
LUMI_BQ_DATA_PROJECT for the project that hosts the tables, the pinned
direct route to the PSC endpoint, the cached OAuth token.

Tables come from config/warehouse_tables.yaml; ``--tables`` restricts to
names already there, ``--add`` appends bare names with every default (the
"new list of tables, instantly" path). Exit codes: 0 complete ·
1 completed with ERROR statuses · 2 config · 3 env/auth · 4 interrupted
(checkpoint kept).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.extract.config import ConfigError, load_config, fmt_bytes  # noqa: E402
from sahs.extract.warehouse import (                              # noqa: E402
    EXIT_ENV_AUTH, EXIT_GATE_FAILURE, EXIT_INTERRUPTED, EXIT_OK,
    EXIT_VALIDATION_ERROR, WarehouseExtractor, utc_now)

DEFAULT_CONFIG = SILO / "config" / "warehouse_tables.yaml"


def _connect(cfg):
    from sahs.util.auth import AuthError, BQConnection
    from sahs.extract.bq_rest import BQRest
    try:
        connection = BQConnection.from_env()
    except AuthError as e:
        print(f"✗ {e}", file=sys.stderr)
        print("  the .env needs GOOGLE_APPLICATION_CREDENTIALS, "
              "BQ_PROJECT_ID (billing) and BIGQUERY_URL; "
              "LUMI_BQ_DATA_PROJECT names the project hosting the tables",
              file=sys.stderr)
        raise SystemExit(EXIT_ENV_AUTH)
    print(f"connection: billing {cfg.billing_project or connection.project}"
          f" · data {cfg.data_project} · endpoint {connection.endpoint} · "
          f"location {connection.location} · route {connection.route()}",
          file=sys.stderr)
    return BQRest(connection, billing_project=cfg.billing_project,
                  labels=cfg.labels)


def _load(args) -> "RunConfig":                                  # noqa: F821
    only = [t for t in (args.tables or "").split(",") if t.strip()]
    add = [t for t in (getattr(args, "add", "") or "").split(",")
           if t.strip()]
    try:
        return load_config(Path(args.config), only=only or None,
                           extra_tables=add or None,
                           output_dir=Path(args.output_dir)
                           if args.output_dir else None)
    except ConfigError as e:
        print(f"✗ config: {e}", file=sys.stderr)
        raise SystemExit(EXIT_VALIDATION_ERROR)


def cmd_probe(args) -> int:
    from sahs.extract.probe import render_probe, run_probe
    cfg = _load(args)
    spec = cfg.table(args.table) if args.table else cfg.tables[0]
    if spec is None:
        print(f"✗ --table {args.table} is not in the config (use --add to "
              "append it)", file=sys.stderr)
        return EXIT_VALIDATION_ERROR
    client = _connect(cfg)
    self_email = ""
    key = client.connection.key_path
    if key is not None:
        try:
            self_email = json.loads(Path(key).read_text()).get(
                "client_email", "")
        except (OSError, ValueError):
            pass
    print(f"probing {spec.logical_fqn} → {spec.physical_fqn} as "
          f"{self_email or '(unknown identity)'}", file=sys.stderr)
    rows = run_probe(cfg, client, spec, self_email=self_email,
                     history_days=args.history_days)
    print(render_probe(rows))
    out = Path(cfg.output_dir) / "_probe"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{spec.name}.json"
    path.write_text(json.dumps({
        "table": spec.name, "identity": self_email,
        "billing_project": client.project, "data_project": cfg.data_project,
        "rows": [r.to_json() for r in rows],
        "ledger": client.ledger.snapshot()}, indent=1, default=str))
    print(f"\nprobe report: {path}", file=sys.stderr)
    return EXIT_OK


def _extractor(cfg, client, args, *, plan_only: bool) -> WarehouseExtractor:
    force = [t for t in (getattr(args, "force", "") or "").split(",")
             if t.strip()]
    return WarehouseExtractor(
        cfg, client, fresh=getattr(args, "fresh", False),
        force_tables=force, skip_profile=getattr(args, "no_profile", False),
        skip_history=getattr(args, "no_history", False),
        plan_only=plan_only, quiet=getattr(args, "quiet", False))


def cmd_run(args, *, plan_only: bool = False) -> int:
    cfg = _load(args)
    if args.budget:
        from sahs.extract.config import _bytes_of
        cfg.run_budget_bytes = _bytes_of(args.budget, "--budget")
    print(f"{'planning' if plan_only else 'extracting'} {len(cfg.tables)} "
          f"tables → {cfg.output_dir} · history {cfg.history_days}d from "
          f"{', '.join(cfg.job_projects)}"
          + (f" + audit {cfg.audit_log_table}" if cfg.audit_log_table
             else "")
          + f" · run budget {fmt_bytes(cfg.run_budget_bytes)}",
          file=sys.stderr)
    client = _connect(cfg)
    extractor = _extractor(cfg, client, args, plan_only=plan_only)
    report = extractor.run()
    if plan_only:
        plan_dir = Path(cfg.output_dir) / "_plan"
        plan_dir.mkdir(parents=True, exist_ok=True)
        (plan_dir / f"{extractor.run_id}.json").write_text(
            json.dumps(report, indent=1, default=str))
    print(f"\nrun report: {Path(cfg.output_dir) / '_run_report.md'}",
          file=sys.stderr)
    if report["status"] == "interrupted":
        return EXIT_INTERRUPTED
    counts = report["status_counts"]
    if any(counts.get(k) for k in ("ERROR", "INVALID", "TRANSPORT",
                                   "QUOTA")):
        return EXIT_GATE_FAILURE
    return EXIT_OK


def cmd_index(args) -> int:
    from sahs.extract.history import DigestLimits, index_history
    cfg = _load(args)
    root = Path(cfg.output_dir)
    history = root / "_history"
    if not history.exists():
        print(f"✗ no corpus at {history} — run the extraction first",
              file=sys.stderr)
        return EXIT_VALIDATION_ERROR
    excluded = list(cfg.exclude_users)
    report_path = root / "_run_report.json"
    if report_path.exists():
        try:
            identity = json.loads(report_path.read_text()).get("identity")
            if identity:
                excluded.append(str(identity))
        except ValueError:
            pass
    report = index_history(
        history, cfg.tables, root, data_project=cfg.data_project,
        exclude_users=excluded, history_days=cfg.history_days,
        limits=DigestLimits(top_users=cfg.top_users_retained,
                            co_queried=cfg.co_queried_retained,
                            failed_queries=cfg.failed_queries_retained,
                            templates=cfg.templates_retained))
    for name, s in report["tables"].items():
        print(f"{name:<48} jobs {s['jobs_rows']:>6}  audit "
              f"{s['audit_rows']:>6}  users {s['distinct_users']:>4}")
    print(f"corpus: {json.dumps(report['corpus'], default=str)}")
    return EXIT_OK


def cmd_status(args) -> int:
    cfg = _load(args)
    root = Path(cfg.output_dir)
    md = root / "_run_report.md"
    if md.exists():
        print(md.read_text(encoding="utf-8"))
    else:
        print(f"no run report at {root}")
    state = root / "_state.json"
    if state.exists():
        data = json.loads(state.read_text(encoding="utf-8"))
        print(f"checkpoint: phase {data.get('phase')} · "
              f"{len(data.get('tasks', {}))} tasks · "
              f"{len(data.get('completed', []))} tables complete")
    missing = [t.name for t in cfg.tables
               if not (root / t.name / "02_logical_columns.csv").exists()]
    if missing:
        print(f"tables without a schema on disk: {', '.join(missing)}")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bq_warehouse_extract.py",
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", default=str(DEFAULT_CONFIG))
        p.add_argument("--tables", default="",
                       help="comma-separated subset of configured tables")
        p.add_argument("--add", default="",
                       help="comma-separated bare table names to append "
                            "with every default")
        p.add_argument("--output-dir", default="")

    p = sub.add_parser("probe", help="capability matrix for one table")
    common(p)
    p.add_argument("--table", default="")
    p.add_argument("--history-days", type=int, default=7)
    p.set_defaults(fn=cmd_probe)

    for name, plan in (("run", False), ("plan", True)):
        p = sub.add_parser(name)
        common(p)
        p.add_argument("--fresh", action="store_true",
                       help="forget the checkpoint and re-extract everything")
        p.add_argument("--force", default="",
                       help="comma-separated tables to re-extract")
        p.add_argument("--no-profile", action="store_true")
        p.add_argument("--no-history", action="store_true")
        p.add_argument("--budget", default="",
                       help="run scan budget, e.g. '2 TiB' (overrides yaml)")
        p.add_argument("--quiet", action="store_true")
        p.set_defaults(fn=lambda a, plan=plan: cmd_run(a, plan_only=plan))

    p = sub.add_parser("index", help="re-index _history/ offline")
    common(p)
    p.set_defaults(fn=cmd_index)

    p = sub.add_parser("status")
    common(p)
    p.set_defaults(fn=cmd_status)

    args = parser.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
