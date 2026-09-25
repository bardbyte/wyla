#!/usr/bin/env python3
"""langfuse_sync.py — the one writer of Langfuse objects that are not
live traces (docs/runbooks/langfuse.md, docs/runbooks/langfuse-insight.md).

    python scripts/langfuse_sync.py check
        proves the keys: constructs the client and authenticates
    python scripts/langfuse_sync.py datasets [--tasks <jsonl> ...]
        upserts one dataset per task file (default: every file under
        tests/tasks); items keyed by task id, the whole task in metadata
    python scripts/langfuse_sync.py datasets --build precedents|silver|scenarios
            [--out <jsonl>] [--push] [--precedents <jsonl>] [--since <iso>]
            [--owner <user id>] [--session <id>]
        builds one of the three item sets — precedents from a checked-in
        JSONL, silver and scenarios from the record in Spanner — writes
        it to --out (default graph/langfuse/<name>.jsonl) for review,
        and uploads it only with --push
    python scripts/langfuse_sync.py backfill <events-file-or-dir> ...
    python scripts/langfuse_sync.py backfill --from spanner [--since <iso>]
            [--session <id>] [--owner <user id>]
        replays assistant records into traces — the same translator the
        live turn uses, so the trace is the same trace, and the same
        ids, so a second run creates nothing new. From files: the JSONL
        under graph/runs/chat/events. From spanner: ChatEvents through
        the store (SAHS_STORE=spanner, or sqlite for the stand-in), the
        user the session's owner, the votes in ChatFeedback as scores
        [--full-results] keeps tool results on the spans (real rows)
        [--user <name>] the user file-replayed traces are filed under
    python scripts/langfuse_sync.py prompts
        registers each system prompt's template and its static parts
        under its version string as a label (plus production and the
        engine family), and writes <graph>/langfuse/prompts.json — the
        file the tracer reads to link generations to the prompt
    python scripts/langfuse_sync.py coverage
        prints the coverage table: each Langfuse concept, the Spanner
        table and columns it is built from, present or missing in the
        live rows. Needs the store, not Langfuse.
    python scripts/langfuse_sync.py pull-annotations [--tasks <jsonl> ...]
        reads the stewards' accept / fail resolutions off the
        annotation queue and writes accepted fingerprints into the
        task files (default: tests/tasks/curated). Then commit.

Needs SAHS_LANGFUSE=1 and the SDK keys in the silo .env for anything
that writes to Langfuse; the store block (SAHS_STORE) for anything
that reads the record.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import load_dotenv                          # noqa: E402

TASKS_ROOT = SILO / "tests" / "tasks"
PRECEDENTS_DEFAULT = SILO / "tests" / "fixtures" / "precedents" / "precedents.jsonl"


def _client():
    from sahs.observe.setup import enabled, langfuse_client
    if not enabled():
        print("SAHS_LANGFUSE is not on in the silo .env — nothing sent",
              file=sys.stderr)
        return None
    return langfuse_client()


def _graph_root() -> Path:
    import os
    return Path(os.environ.get("MERIDIAN_GRAPH_DIR", SILO / "graph"))


def _record_reader():
    """The record reader over the store the .env names, or None with
    the reason printed (SAHS_STORE=local has no chat tables)."""
    from sahs.identity.database import open_database
    from sahs.observe.record import RecordReader
    from sahs.spanner import SpannerConfigurationError, SpannerSettings
    try:
        settings = SpannerSettings.from_env()
    except SpannerConfigurationError as exc:
        print(f"the record is not in a store: {exc}", file=sys.stderr)
        return None
    return RecordReader(open_database(settings))


def cmd_check(_args: argparse.Namespace) -> int:
    client = _client()
    if client is None:
        return 2
    ok = client.auth_check()
    print("langfuse: authenticated" if ok else
          "langfuse: auth_check failed — check the keys and the base url")
    return 0 if ok else 1


def cmd_datasets(args: argparse.Namespace) -> int:
    if args.build:
        return _build_dataset(args)
    from sahs.observe.experiments import push_datasets
    client = _client()
    if client is None:
        return 2
    paths = ([Path(p) for p in args.tasks] if args.tasks
             else sorted(TASKS_ROOT.glob("*/*.jsonl")))
    pushed = push_datasets(client, paths)
    client.flush()
    for name, n in pushed.items():
        print(f"{name:40} {n:>5} items")
    return 0


def _build_dataset(args: argparse.Namespace) -> int:
    from sahs.observe import datasets as D
    name = args.build
    if name == "precedents":
        path = Path(args.precedents) if args.precedents else PRECEDENTS_DEFAULT
        if not path.exists():
            print(f"no precedents file at {path}; the shape is documented in "
                  "sahs/observe/datasets.py", file=sys.stderr)
            return 2
        items = D.precedent_items(D.read_precedents(path))
        source = str(path)
    else:
        reader = _record_reader()
        if reader is None:
            return 2
        turns = []
        for session in reader.sessions(since=args.since or None,
                                       session_id=args.session,
                                       owner=args.owner):
            turns += reader.turns(session)
        items = (D.silver_items(turns) if name == "silver"
                 else D.scenario_items(turns))
        source = f"the record ({len(turns)} turns read)"
    out = Path(args.out) if args.out else _graph_root() / "langfuse" / f"{name}.jsonl"
    D.write_jsonl(items, out)
    print(f"{name}: {len(items)} items from {source} → {out}")
    if not args.push:
        print("review the file, then re-run with --push to upload")
        return 0
    client = _client()
    if client is None:
        return 2
    dataset, n = D.push_items(client, items, description=f"built from {source}")
    client.flush()
    print(f"{dataset:40} {n:>5} items pushed")
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    from sahs.observe.langfuse_emitter import LangfuseEmitter
    from sahs.observe.tracer import TurnTracer, replay_file
    client = _client()
    if client is None:
        return 2
    tracer = TurnTracer(LangfuseEmitter(client), user_id=args.user,
                        full_results=args.full_results)
    if args.source == "spanner":
        from sahs.observe.record import backfill
        reader = _record_reader()
        if reader is None:
            return 2
        stats = backfill(reader, tracer, since=args.since or None,
                         session_id=args.session, owner=args.owner,
                         log=print)
        print(f"{stats['sessions']} sessions, {stats['turns']} turns, "
              f"{stats['records']} records replayed, {stats['feedback']} "
              f"votes scored, {stats['skipped_turns']} turns before --since")
        return 0
    if not args.paths:
        print("backfill needs events files, or --from spanner", file=sys.stderr)
        return 2
    files: list[Path] = []
    for raw in args.paths:
        path = Path(raw)
        files += sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    total = 0
    for path in files:
        n = replay_file(path, tracer)
        total += n
        print(f"{path.name:48} {n:>6} records")
    tracer.flush()
    print(f"{len(files)} files, {total} records replayed")
    return 0


def cmd_prompts(args: argparse.Namespace) -> int:
    from sahs.observe.prompts import links_path, register_prompts
    client = _client()
    if client is None:
        return 2
    out = Path(args.out) if args.out else links_path(_graph_root())
    rows = register_prompts(client, out, root=SILO)
    client.flush()
    for row in rows:
        state = ("created" if row["created"] else "already registered")
        if row["drift"]:
            state += " — TEXT CHANGED under the same version string: bump it"
        print(f"{row['name']:28} {row['version']:14} → v{row['langfuse_version']}"
              f"  {state}")
    print(f"links: {out}")
    return 1 if any(r["drift"] for r in rows) else 0


def cmd_coverage(_args: argparse.Namespace) -> int:
    from sahs.observe.coverage import coverage, format_coverage
    reader = _record_reader()
    if reader is None:
        return 2
    rows = coverage(reader)
    print(format_coverage(rows))
    missing = [r["concept"] for r in rows if r["present"] is False]
    print(f"\n{len(missing)} missing: {', '.join(missing) or 'none'}")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    import json
    from sahs.observe.annotations import pull_resolutions
    client = _client()
    if client is None:
        return 2
    paths = ([Path(p) for p in args.tasks] if args.tasks
             else sorted((TASKS_ROOT / "curated").glob("*.jsonl")))
    report = pull_resolutions(client, paths)
    print(json.dumps(report, indent=1))
    if report["files_written"]:
        print("→ review the diff and commit: the suite learns through git")
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv(SILO / ".env")
    parser = argparse.ArgumentParser(prog="langfuse_sync.py")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check").set_defaults(fn=cmd_check)
    p = sub.add_parser("datasets")
    p.add_argument("--tasks", action="append", default=[])
    p.add_argument("--build", choices=["precedents", "silver", "scenarios"],
                   default="")
    p.add_argument("--out", default="")
    p.add_argument("--push", action="store_true")
    p.add_argument("--precedents", default="")
    p.add_argument("--since", default="")
    p.add_argument("--session", default="")
    p.add_argument("--owner", default="")
    p.set_defaults(fn=cmd_datasets)
    p = sub.add_parser("backfill")
    p.add_argument("paths", nargs="*")
    p.add_argument("--from", dest="source", choices=["files", "spanner"],
                   default="files")
    p.add_argument("--since", default="")
    p.add_argument("--session", default="")
    p.add_argument("--owner", default="")
    p.add_argument("--full-results", action="store_true")
    p.add_argument("--user", default="")
    p.set_defaults(fn=cmd_backfill)
    p = sub.add_parser("prompts")
    p.add_argument("--out", default="")
    p.set_defaults(fn=cmd_prompts)
    sub.add_parser("coverage").set_defaults(fn=cmd_coverage)
    p = sub.add_parser("pull-annotations")
    p.add_argument("--tasks", action="append", default=[])
    p.set_defaults(fn=cmd_pull)
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
