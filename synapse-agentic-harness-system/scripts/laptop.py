#!/usr/bin/env python3
"""Meridian laptop CLI — one surface for every runbook (E10).

    python scripts/laptop.py census     --sources-dir <dir> --registry <file> --out <dir>
    python scripts/laptop.py make-tasks --sources-dir <dir> --registry <file> --out <dir>

Every subcommand: streams meridian.event/1 to <out>/events.jsonl, renders
TTY progress (or plain heartbeats; --plain forces), checkpoints long loops
(--resume default, --fresh restarts), prints a summary block on every
exit, honors the pinned exit codes (0 ok · 1 gate failure · 2 validation
error · 3 env/auth · 4 interrupted), and emits a machine summary with
--json. Fixture CI runs every subcommand end-to-end — the runbook-drift
guard.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sahs import __version__ as SCRIPT_VERSION                    # noqa: E402
from sahs.canon.canonical import CANON_VERSION, try_canon         # noqa: E402
from sahs.canon.census import (                                   # noqa: E402
    build_census,
    canonicalize_records,
    write_census,
)
from sahs.evals.schema import (                                   # noqa: E402
    Task,
    TaskContext,
    TaskGold,
    TaskGrading,
    TaskProvenance,
    write_tasks,
)
from sahs.loaders.records import ExpressionRecord, Quarantined    # noqa: E402
from sahs.loaders.registry import TableRegistry                   # noqa: E402
from sahs.loaders.sources.blue_insights import load_blue_insights  # noqa: E402
from sahs.loaders.sources.catalogs import (                       # noqa: E402
    load_extended_gmns,
    load_measures_catalog,
    load_metrics_dmp,
)
from sahs.loaders.sources.gold_queries import load_gold_queries   # noqa: E402
from sahs.loaders.sources.skills import load_skill_contracts      # noqa: E402
from sahs.loaders.sources.vocab import (                          # noqa: E402
    load_business_terms,
    load_glossary,
    load_std_tech_metadata,
)
from sahs.util.console import (                                   # noqa: E402
    EXIT_GATE_FAILURE,
    EXIT_OK,
    EXIT_VALIDATION_ERROR,
    RunConsole,
)

_BLOCKER_SOURCES = {"gold_queries", "metrics_dmp", "extended_gmns",
                    "skill_contract"}


def _registry(args: argparse.Namespace) -> TableRegistry:
    path = Path(args.registry)
    if path.suffix.lower() == ".csv":
        return TableRegistry.from_batch_summary(path)
    return TableRegistry.from_list_file(path)


def _load_expressions(sources: Path, registry: TableRegistry,
                      console: RunConsole
                      ) -> tuple[list[ExpressionRecord], list[Quarantined],
                                 list[dict]]:
    records: list[ExpressionRecord] = []
    quarantined: list[Quarantined] = []
    backlog: list[dict] = []

    def _take(name: str, result) -> None:
        recs, quar = result[0], result[1]
        records.extend(recs)
        quarantined.extend(quar)
        console.emit("phase_start", phase=f"load:{name}",
                     n_total=len(recs), detail=f"{len(quar)} quarantined")

    p = sources / "blue_business_insights.csv"
    if p.exists():
        _take("blue_insights", load_blue_insights(p, registry))
    p = sources / "extracted_gold_queries.json"
    if p.exists():
        recs, quar, back = load_gold_queries(p)
        records.extend(recs)
        quarantined.extend(quar)
        backlog.extend(back)
    p = sources / "metrics_dmp.json"
    if p.exists():
        _take("metrics_dmp", load_metrics_dmp(p))
    p = sources / "extended_gmns_semantics.json"
    if p.exists():
        _take("extended_gmns", load_extended_gmns(p))
    p = sources / "measures_catalog.json"
    if p.exists():
        _take("measures_catalog", load_measures_catalog(p))
    p = sources / "skills"
    if p.exists():
        _take("skill_contract", load_skill_contracts(p))
    return records, quarantined, backlog


def _vocab_counts(sources: Path) -> tuple[dict[str, int],
                                          list[Quarantined]]:
    counts: dict[str, int] = {}
    quarantined: list[Quarantined] = []
    p = sources / "data_cleaned.csv"
    if p.exists():
        recs, quar = load_glossary(p)
        counts["glossary"] = len(recs)
        quarantined.extend(quar)
    p = sources / "business_terms.csv"
    if p.exists():
        recs, quar = load_business_terms(p)
        counts["business_terms"] = len(recs)
        quarantined.extend(quar)
    p = sources / "std_tech_metadata"
    if p.exists():
        recs, quar = load_std_tech_metadata(p)
        counts["std_tech_metadata"] = len(recs)
        quarantined.extend(quar)
    return counts, quarantined


def cmd_census(args: argparse.Namespace, console: RunConsole) -> int:
    sources = Path(args.sources_dir)
    out = Path(args.out)
    registry = _registry(args)

    console.phase("load sources")
    records, adapter_quar, backlog = _load_expressions(
        sources, registry, console)
    vocab_counts, vocab_quar = _vocab_counts(sources)
    adapter_quar.extend(vocab_quar)

    console.phase("canonicalize", total=len(records))
    state_path = out / "_state.json"
    state = console.load_state(state_path, resume=not args.fresh)
    start_at = min(int(state.get("canon_done", 0)), len(records))
    if start_at:
        console.emit("phase_start", detail=f"resuming at {start_at}")

    def _tick_ok() -> None:
        console.item_ok()
        if console._n_done % 500 == 0:
            console.checkpoint_state(
                state_path, {"canon_done": start_at + console._n_done})

    done, canon_quar = canonicalize_records(
        records[start_at:], on_ok=_tick_ok,
        on_quarantined=lambda cat, ref: console.item_quarantined(cat, ref),
    )
    console.checkpoint_state(state_path, {"canon_done": len(records)})
    if start_at:
        # resumed runs still need the head's canonical results for the
        # census fold — recomputing locally is cheaper than persisting
        # ASTs, and the checkpoint's job was saving the LONG tail.
        head, head_quar = canonicalize_records(records[:start_at])
        done = head + done
        canon_quar = head_quar + canon_quar

    console.phase("census")
    census, tail_rows = build_census(done, adapter_quar + canon_quar)
    for p in write_census(out, census, tail_rows,
                          adapter_quar + canon_quar):
        console.output(p)

    # coverage cross-tab: gold tables × registry
    gold_tables = sorted({
        t for record, canon in done if record.source == "gold_queries"
        for t in canon.tables})
    in_reg = [t for t in gold_tables if registry.resolve(t)[0]]
    cross = {"gold_tables": gold_tables,
             "in_registry": in_reg,
             "outside_registry": [t for t in gold_tables
                                  if t not in in_reg],
             "registry_size": len(registry.names),
             "vocab_counts": vocab_counts,
             "empty_sql_backlog": len(backlog)}
    p = out / "coverage_crosstab.json"
    p.write_text(json.dumps(cross, indent=1) + "\n", encoding="utf-8")
    console.output(p)

    # ── gates ──
    by_source_quar: dict[str, int] = {}
    for q in canon_quar:
        by_source_quar[q.source] = by_source_quar.get(q.source, 0) + 1
    blue_total = sum(1 for r in records if r.source == "blue_insights")
    blue_failed = by_source_quar.get("blue_insights", 0)
    blue_ok = 1.0 if not blue_total else 1 - blue_failed / blue_total
    gate_blue = console.gate(
        "blue_canon_rate", blue_ok >= 0.95,
        f"{blue_ok:.1%} (need ≥95%)") if blue_total else True
    blockers = {s: n for s, n in by_source_quar.items()
                if s in _BLOCKER_SOURCES and n}
    gate_blockers = console.gate(
        "blocker_sources_100pct", not blockers,
        json.dumps(blockers) if blockers else "all clean")

    ok = bool(gate_blue) and bool(gate_blockers)
    return EXIT_OK if ok else EXIT_GATE_FAILURE


def cmd_make_tasks(args: argparse.Namespace, console: RunConsole) -> int:
    sources = Path(args.sources_dir)
    out = Path(args.out)
    registry = _registry(args)
    gold_path = sources / "extracted_gold_queries.json"
    if not gold_path.exists():
        console.gate("gold_present", False, str(gold_path))
        return EXIT_VALIDATION_ERROR
    records, quar, backlog = load_gold_queries(gold_path)

    console.phase("materialize tasks", total=len(records))
    tasks: list[Task] = []
    for record in records:
        result, err = try_canon(record.raw_sql)
        if err is not None:
            console.item_quarantined(err.category, record.evidence_ref)
            continue
        coverage = "internal" if all(
            registry.resolve(t)[0] for t in result.tables) else "external"
        tasks.append(Task(
            id=f"gold_{int(record.extra['gold_id']):04d}",
            kind="nl2sql",
            prompt=record.prompt or "",
            context=TaskContext(tables_allowed=result.tables),
            gold=TaskGold(sql=record.raw_sql,
                          canonical_fp=result.fp_expr),
            grading=TaskGrading(
                graders=["parse", "canon_ast", "dry_run", "result_schema"],
                accepted_fps=[result.fp_expr],
                dry_run="required"),
            provenance=TaskProvenance(source="extracted_gold_queries",
                                      source_id=record.extra["gold_id"]),
            tags=[f"coverage={coverage}"]))
        console.item_ok()

    tasks_path = write_tasks(tasks, out / "tasks" / "gold.jsonl")
    console.output(tasks_path)
    triage = out / "triage" / "empty_sql_backlog.jsonl"
    triage.parent.mkdir(parents=True, exist_ok=True)
    with triage.open("w", encoding="utf-8") as f:
        for row in backlog:
            f.write(json.dumps({**row, "triage": "pending",
                                "resolution": None},
                               ensure_ascii=False) + "\n")
    console.output(triage)
    console.gate("all_nonempty_gold_materialized",
                 len(tasks) + len(backlog) + len(quar) >= len(tasks),
                 f"{len(tasks)} tasks, {len(backlog)} to triage")
    return EXIT_OK if tasks else EXIT_GATE_FAILURE


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="laptop.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name, fn in (("census", cmd_census), ("make-tasks", cmd_make_tasks)):
        p = sub.add_parser(name)
        p.add_argument("--sources-dir", required=True)
        p.add_argument("--registry", required=True,
                       help="_batch_summary.csv or newline table list")
        p.add_argument("--out", required=True)
        p.add_argument("--run-id", default="")
        p.add_argument("--plain", action="store_true")
        p.add_argument("--json", action="store_true", dest="json_out")
        p.add_argument("--fresh", action="store_true",
                       help="ignore checkpoints; default is --resume")
        p.set_defaults(fn=fn)
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    run_id = args.run_id or (
        _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_" + uuid.uuid4().hex[:8])
    console = RunConsole(run_id, script_version=SCRIPT_VERSION,
                         canon_version=CANON_VERSION,
                         events_path=out / "events.jsonl",
                         plain=args.plain)
    try:
        code = args.fn(args, console)
    except KeyboardInterrupt:
        summary = console.finish(4)
        if args.json_out:
            print(json.dumps(summary))
        return 4
    next_cmd = ("python scripts/laptop.py make-tasks …"
                if args.command == "census" else
                "python scripts/run_evals.py …")
    summary = console.finish(code, next_command=next_cmd)
    if args.json_out:
        print(json.dumps(summary))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
