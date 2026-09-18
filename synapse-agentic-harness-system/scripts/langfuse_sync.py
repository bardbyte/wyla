#!/usr/bin/env python3
"""langfuse_sync.py — the one writer of Langfuse objects that are not
live traces (docs/runbooks/langfuse.md).

    python scripts/langfuse_sync.py check
        proves the keys: constructs the client and authenticates
    python scripts/langfuse_sync.py datasets [--tasks <jsonl> ...]
        upserts one dataset per task file (default: every file under
        tests/tasks); items keyed by task id, the whole task in metadata
    python scripts/langfuse_sync.py backfill <events-file-or-dir> ...
        replays assistant events files into traces — the same
        translator the live turn uses, so the trace is the same trace
        [--full-results] keeps tool results on the spans (real rows)
        [--user <name>] the user the traces are filed under
    python scripts/langfuse_sync.py prompts
        registers each system prompt's template under its version
        string as a label, and writes <graph>/langfuse/prompts.json —
        the file the tracer reads to link generations to the prompt
    python scripts/langfuse_sync.py pull-annotations [--tasks <jsonl> ...]
        reads the stewards' accept / fail resolutions off the
        annotation queue and writes accepted fingerprints into the
        task files (default: tests/tasks/curated). Then commit.

Needs SAHS_LANGFUSE=1 and the SDK keys in the silo .env.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import load_dotenv                          # noqa: E402

TASKS_ROOT = SILO / "tests" / "tasks"


def _client():
    from sahs.observe.setup import enabled, langfuse_client
    if not enabled():
        print("SAHS_LANGFUSE is not on in the silo .env — nothing sent",
              file=sys.stderr)
        return None
    return langfuse_client()


def cmd_check(_args: argparse.Namespace) -> int:
    client = _client()
    if client is None:
        return 2
    ok = client.auth_check()
    print("langfuse: authenticated" if ok else
          "langfuse: auth_check failed — check the keys and the base url")
    return 0 if ok else 1


def cmd_datasets(args: argparse.Namespace) -> int:
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


def cmd_backfill(args: argparse.Namespace) -> int:
    from sahs.observe.langfuse_emitter import LangfuseEmitter
    from sahs.observe.tracer import TurnTracer, replay_file
    client = _client()
    if client is None:
        return 2
    tracer = TurnTracer(LangfuseEmitter(client), user_id=args.user,
                        full_results=args.full_results)
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


def _graph_root() -> Path:
    import os
    return Path(os.environ.get("MERIDIAN_GRAPH_DIR", SILO / "graph"))


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
        print(f"{row['name']:24} {row['version']:14} → v{row['langfuse_version']}"
              f"  {state}")
    print(f"links: {out}")
    return 1 if any(r["drift"] for r in rows) else 0


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
    p.set_defaults(fn=cmd_datasets)
    p = sub.add_parser("backfill")
    p.add_argument("paths", nargs="+")
    p.add_argument("--full-results", action="store_true")
    p.add_argument("--user", default="")
    p.set_defaults(fn=cmd_backfill)
    p = sub.add_parser("prompts")
    p.add_argument("--out", default="")
    p.set_defaults(fn=cmd_prompts)
    p = sub.add_parser("pull-annotations")
    p.add_argument("--tasks", action="append", default=[])
    p.set_defaults(fn=cmd_pull)
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
