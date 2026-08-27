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
from sahs.loaders.ledger import UtilizationLedger                 # noqa: E402
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


def _std_tech_path(sources: Path) -> Path | None:
    """The Atlas std-tech feed ships as either a per-table directory or
    one combined export; the combined file wins when both exist (a
    partially-populated directory must not shadow the full export)."""
    combined = sources / "std_tech_metadata_all.json"
    if combined.exists():
        return combined
    directory = sources / "std_tech_metadata"
    return directory if directory.is_dir() else None


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
    p = _std_tech_path(sources)
    if p is not None:
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


def cmd_build_graph(args: argparse.Namespace, console: RunConsole) -> int:
    from sahs.canon.census import canonicalize_records
    from sahs.graph.crosswalk import Crosswalk
    from sahs.graph.quads import GraphDir
    from sahs.graph.validate import validate_graph
    from sahs.loaders.archives.bq_extraction import load_bq_archive
    from sahs.loaders.archives.mdm46 import load_mdm_archive
    from sahs.loaders.quads_emit import (
        emit_expressions,
        emit_std_tech,
        emit_vocab,
    )

    graph_root = Path(args.graph)
    crosswalk = Crosswalk.load(Path(args.crosswalk))
    graph = GraphDir(graph_root)
    run_id = console.run_id
    blocking: list[str] = []
    reports: dict[str, dict] = {}

    ledger = UtilizationLedger()
    no_jobs = bool(getattr(args, "no_jobs_30d", False))
    if no_jobs:
        # A8: the 30-day query history is judged incorrect for this run —
        # nothing derived from it may witness the graph, and the ledger
        # says so instead of leaving the files unaccounted for
        ledger.defer_dir(
            "17_queries_30d",
            "jobs witness disabled this run (A8) — 30-day query history "
            "judged incorrect; re-enable when a corrected extract lands")
    jobs_gate_failures: list[str] = []
    if args.bq_archive:
        console.phase("bq archive")
        reports["bq"], blocked = load_bq_archive(
            Path(args.bq_archive), graph, crosswalk, run_id,
            ledger=ledger, include_jobs_digests=not no_jobs)
        blocking += blocked
    if args.mdm_archive:
        console.phase("mdm archive")
        reports["mdm"], blocked = load_mdm_archive(
            Path(args.mdm_archive), graph, crosswalk, run_id,
            ledger=ledger)
        blocking += blocked
    if not console.gate("crosswalk_resolution", not blocking,
                        "; ".join(blocking[:5]) if blocking
                        else "all archive tables resolved"):
        console.finish(EXIT_VALIDATION_ERROR)
        return EXIT_VALIDATION_ERROR

    # steward LOB map — sidecar beside the crosswalk (same contract as
    # aliases.jsonl: strict, local, never guessed). Emitted BEFORE the
    # semantic catalogs so mined business_unit values can corroborate
    # steward-declared lob nodes.
    known_lobs: set[str] = set()
    lob_path = Path(args.crosswalk).parent / "lob_map.jsonl"
    if lob_path.exists():
        from sahs.graph.ids import lob_id
        from sahs.graph.lob import emit_lob_map, load_lob_map
        console.phase("lob map")
        lob_rows = load_lob_map(lob_path, crosswalk)
        reports["lob_map"] = emit_lob_map(lob_rows, graph, run_id)
        known_lobs = {lob_id(r.lob_code) for r in lob_rows}

    if args.sources_dir:
        sources = Path(args.sources_dir)
        registry = _registry(args)
        console.phase("semantic sources → quads")
        records, _quar, _backlog = _load_expressions(
            sources, registry, console)
        pairs, canon_quar = canonicalize_records(records)
        for q in canon_quar:
            console.item_quarantined(q.category, q.evidence_ref)
        reports["expressions"] = emit_expressions(
            pairs, graph, crosswalk, run_id, known_lobs=known_lobs)
        glossary_path = sources / "data_cleaned.csv"
        terms_path = sources / "business_terms.csv"
        glossary = (load_glossary(glossary_path)[0]
                    if glossary_path.exists() else [])
        terms = (load_business_terms(terms_path)[0]
                 if terms_path.exists() else [])
        reports["vocab"] = emit_vocab(glossary, terms, graph, run_id)
        std_path = _std_tech_path(sources)
        if std_path is not None:
            console.emit("phase_start", phase="load:std_tech",
                         detail=f"reading {std_path.name}")
            entries = load_std_tech_metadata(std_path)[0]
            reports["std_tech"] = emit_std_tech(
                entries, terms, graph, crosswalk, run_id)

    # jobs witness AFTER the semantic catalogs: a jobs sighting of an
    # already-governed metric is testimony, never a fresh seed (E7)
    jobs_gate = True
    if args.bq_archive and no_jobs:
        console.emit("phase_start", phase="jobs 30d witness",
                     detail="disabled (--no-jobs-30d, A8)")
    elif args.bq_archive:
        console.phase("jobs 30d witness")
        from sahs.loaders.archives.jobs_30d import load_jobs_30d
        reports["jobs_30d"], jobs_gate_failures = load_jobs_30d(
            Path(args.bq_archive), graph, crosswalk, run_id,
            ledger=ledger)
        jobs_gate = console.gate(
            "jobs_canon_rate", not jobs_gate_failures,
            "; ".join(jobs_gate_failures[:5]) if jobs_gate_failures
            else "every table ≥90% canonicalized-or-understood")

    # E12/A2 — the utilization ledger: every file under every input
    # root accounted for (consumed | deferred(reason) | inventoried)
    ledger_roots = [Path(p) for p in (args.bq_archive, args.mdm_archive,
                                      args.sources_dir) if p]
    if args.registry:
        ledger.consumed(Path(args.registry))
    if args.sources_dir:
        sources = Path(args.sources_dir)
        for name in ("blue_business_insights.csv",
                     "extracted_gold_queries.json", "metrics_dmp.json",
                     "extended_gmns_semantics.json",
                     "measures_catalog.json", "data_cleaned.csv",
                     "business_terms.csv"):
            ledger.consumed(sources / name)
        for pack_file in sorted(sources.glob("skills/**/skill.yaml")) + \
                sorted(sources.glob("skills/**/metric_contracts.yaml")):
            ledger.consumed(pack_file)
        std_path = _std_tech_path(sources)
        if std_path is not None:
            for p in ([std_path] if std_path.is_file()
                      else sorted(std_path.glob("*.json"))):
                ledger.consumed(p)
    utilization = ledger.build(ledger_roots)
    reports["utilization"] = UtilizationLedger.summary(utilization)
    console.emit("phase_start", phase="utilization ledger",
                 detail=json.dumps(reports["utilization"]))

    (graph_root / "runs" / run_id).mkdir(parents=True, exist_ok=True)
    manifest = graph_root / "runs" / run_id / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": run_id, "archived": False, "reports": reports,
        "utilization": utilization},
        indent=1) + "\n", encoding="utf-8")
    console.output(manifest)

    console.phase("validate")
    report = validate_graph(graph_root)
    (graph_root / "runs" / run_id / "validation.json").write_text(
        report.to_json() + "\n", encoding="utf-8")
    console.output(graph_root / "runs" / run_id / "validation.json")
    ok = console.gate("graph_valid", report.ok,
                      f"{len(report.errors)} error(s), "
                      f"{len(report.warnings)} warning(s)")
    if not jobs_gate:
        return EXIT_GATE_FAILURE
    return EXIT_OK if ok else EXIT_VALIDATION_ERROR


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
    def cmd_compile(args: argparse.Namespace, console: RunConsole) -> int:
        from sahs.compiler.compile import compile_build
        console.phase("compile")
        build_dir, manifest, failures = compile_build(
            Path(args.graph), Path(args.builds))
        console.output(build_dir / "manifest.json")
        console.output(build_dir / "DIFF_vs_prev.md")
        console.emit("gate_result",
                     detail=f"build {manifest['build_id']}")
        ok = console.gate("compile_gates", not failures,
                          "; ".join(failures[:5]) if failures
                          else f"{manifest['build_id']} promoted "
                               "(CURRENT moved)")
        return EXIT_OK if ok else EXIT_GATE_FAILURE

    p = sub.add_parser("compile")
    p.add_argument("--graph", required=True)
    p.add_argument("--builds", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--plain", action="store_true")
    p.add_argument("--json", action="store_true", dest="json_out")
    p.add_argument("--fresh", action="store_true")
    p.set_defaults(fn=cmd_compile)

    p = sub.add_parser("build-graph")
    p.add_argument("--graph", required=True, help="graph/ root (L2)")
    p.add_argument("--crosswalk", required=True,
                   help="identity/crosswalk.jsonl (E1)")
    p.add_argument("--bq-archive", default="")
    p.add_argument("--mdm-archive", default="")
    p.add_argument("--sources-dir", default="")
    p.add_argument("--registry", default="")
    p.add_argument("--no-jobs-30d", action="store_true",
                   help="A8: exclude every fact derived from the 30-day "
                        "query history (jobs witness, cost priors, "
                        "top_users, co_queried, templates) — files are "
                        "ledgered deferred, never unaccounted")
    p.add_argument("--out", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--plain", action="store_true")
    p.add_argument("--json", action="store_true", dest="json_out")
    p.add_argument("--fresh", action="store_true")
    p.set_defaults(fn=cmd_build_graph)
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
