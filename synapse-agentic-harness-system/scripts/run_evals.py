#!/usr/bin/env python3
"""Run the Meridian eval suite.

    python scripts/run_evals.py --tasks <jsonl> [--tasks <jsonl> ...]
        --sut oracle|null [--out <dir>] [--fail-under 0.9]
        [--max-ambiguous 0.1] [--json] [--plain]

Exit codes: 0 ok · 1 gate failure · 2 validation error.
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
from sahs.canon.canonical import CANON_VERSION                    # noqa: E402
from sahs.evals.harness import format_report, run_suite, write_report  # noqa: E402
from sahs.evals.schema import read_tasks                          # noqa: E402
from sahs.evals.suts import BUILTIN_SUTS                          # noqa: E402
from sahs.util.console import (                                   # noqa: E402
    EXIT_GATE_FAILURE,
    EXIT_OK,
    EXIT_VALIDATION_ERROR,
    RunConsole,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_evals.py")
    parser.add_argument("--tasks", action="append", required=True)
    parser.add_argument("--sut", default="oracle",
                        help="oracle | null | resolver:<builds-dir>")
    parser.add_argument("--out", default="")
    parser.add_argument("--fail-under", type=float, default=None)
    parser.add_argument("--max-ambiguous", type=float, default=None)
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--stochastic", action="store_true",
                        help="SUT is non-deterministic; use --samples")
    parser.add_argument("--include-external", action="store_true",
                        help="score coverage=external tasks too (they are "
                             "excluded from the floor by default)")
    parser.add_argument("--plain", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_out")
    args = parser.parse_args(argv)

    tasks = []
    for path in args.tasks:
        tasks.extend(read_tasks(Path(path)))
    loaded = len(tasks)
    excluded = 0
    if not args.include_external:
        in_scope = [t for t in tasks if "coverage=external" not in t.tags]
        excluded = loaded - len(in_scope)
        tasks = in_scope
    # E5: the denominator never shrinks silently — print even when 0
    print(f"excluded (coverage=external): {excluded} of {loaded}",
          file=sys.stderr)
    if not tasks:
        print("no tasks loaded", file=sys.stderr)
        return EXIT_VALIDATION_ERROR

    if args.sut.startswith("resolver:"):
        from sahs.tools.api import Build
        from sahs.tools.resolver import resolver_sut
        build = Build.open(Path(args.sut.split(":", 1)[1]))
        sut = resolver_sut(build)
    elif args.sut in BUILTIN_SUTS:
        sut = BUILTIN_SUTS[args.sut]
    else:
        print(f"unknown sut {args.sut!r}", file=sys.stderr)
        return EXIT_VALIDATION_ERROR

    # a SUT that declares its answerable kinds is measured on THOSE —
    # grading a binding resolver on generation tasks measures a
    # category error, not the floor. Excluded loudly, never silently;
    # the nl2sql ground stays intact for generation-capable SUTs.
    answerable = getattr(sut, "answerable_kinds", None)
    excluded_kind = 0
    if answerable:
        in_kind = [t for t in tasks if t.kind in answerable]
        excluded_kind = len(tasks) - len(in_kind)
        tasks = in_kind
    print(f"excluded (kind outside sut capability): {excluded_kind}",
          file=sys.stderr)
    if not tasks:
        print("no tasks answerable by this sut", file=sys.stderr)
        return EXIT_VALIDATION_ERROR

    out = Path(args.out) if args.out else None
    run_id = (_dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
              + "_" + uuid.uuid4().hex[:8])
    console = RunConsole(
        run_id, script_version=SCRIPT_VERSION, canon_version=CANON_VERSION,
        events_path=(out / "events.jsonl") if out else None,
        plain=args.plain)
    console.phase("evaluate", total=len(tasks))

    report = run_suite(
        tasks, sut,
        n=args.samples, deterministic=not args.stochastic,
        triage_path=(out / "triage" / "ambiguous.jsonl") if out else None,
        on_trial=lambda t: (console.item_ok() if t.verdict == "pass"
                            else console.item_quarantined(t.verdict,
                                                          t.reason)))
    report["excluded_out_of_coverage"] = excluded
    report["excluded_kind_not_answerable"] = excluded_kind
    print(format_report(report), file=sys.stderr)
    if out:
        console.output(write_report(report, out / "eval_report.json"))
        if report["failures"]:
            # E5: every floor failure gets triaged — this file is the
            # table a human fills in and commits; open resolver_bug
            # items block P3 exit
            triage = out / "triage" / "floor_failures.jsonl"
            triage.parent.mkdir(parents=True, exist_ok=True)
            with triage.open("w", encoding="utf-8") as f:
                for failure in report["failures"]:
                    f.write(json.dumps({
                        **failure, "triage": "pending",
                        "category": None,   # resolver_bug | gold_defect
                                            # | coverage_gap
                    }, ensure_ascii=False) + "\n")
            console.output(triage)

    ok = True
    if args.fail_under is not None:
        ok &= console.gate("pass_at_1_floor",
                           report["overall"]["pass@1"] >= args.fail_under,
                           f"{report['overall']['pass@1']:.3f} "
                           f"(need ≥{args.fail_under})")
    if args.max_ambiguous is not None:
        ok &= console.gate(
            "ambiguous_ceiling",
            report["overall"]["ambiguous_rate"] <= args.max_ambiguous,
            f"{report['overall']['ambiguous_rate']:.3f}")
    if report["determinism_alarms"]:
        ok = console.gate("deterministic_sut_stable", False,
                          ",".join(report["determinism_alarms"])) and ok

    code = EXIT_OK if ok else EXIT_GATE_FAILURE
    summary = console.finish(code, extra={
        "overall": report["overall"],
        "excluded_out_of_coverage": excluded,
        "excluded_kind_not_answerable": excluded_kind})
    if args.json_out:
        print(json.dumps(summary))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
