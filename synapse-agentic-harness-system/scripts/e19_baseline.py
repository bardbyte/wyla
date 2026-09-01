#!/usr/bin/env python3
"""e19_baseline.py — run the E19 capability matrix (E21 Step 0b).

    python scripts/e19_baseline.py --builds <builds> --graph <graph> \
           --out docs/evals
    python scripts/e19_baseline.py ... --real     # Vertex, on the laptop

Runs the matrix under the three configurations (pinned/looser/strict
resolver margin, via a copied manifest) and writes e19_baseline.md +
.json. With --real the loop's transport is Vertex and the outputs are
suffixed _vertex, so the scripted and real baselines sit side by side
and never overwrite each other.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.evals.capability import (CONFIGS, MatrixResult,   # noqa: E402
                                   ScriptedTransport, load_tasks,
                                   render_report, run_matrix)
from sahs.tools.api import Build                            # noqa: E402
from sahs.util.auth import load_dotenv                      # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--builds", default="")
    parser.add_argument("--graph", default="")
    parser.add_argument("--tasks", default=str(
        SILO / "tests" / "tasks" / "capability" / "matrix.jsonl"))
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    parser.add_argument("--real", action="store_true",
                        help="use Vertex for the loop transport "
                             "(the laptop baseline)")
    args = parser.parse_args(argv)

    load_dotenv()
    builds = Path(args.builds or os.environ.get("MERIDIAN_BUILDS_DIR")
                  or SILO / "builds")
    build = Build.open(builds)
    tasks = load_tasks(Path(args.tasks))
    # curated paths in the task file are silo-relative
    for task in tasks:
        if "curated_path" in task:
            task["curated_path"] = str(SILO / task["curated_path"])

    factory = None
    transport = "scripted"
    if args.real:
        from sahs.ask.budget import Budget
        from sahs.ask.model import VertexModel
        transport = "vertex"

        def factory(task):                       # noqa: E731 (scoped)
            return VertexModel.from_env(Budget())

    runs: list[MatrixResult] = []
    with tempfile.TemporaryDirectory() as tmp:
        for config, margin in CONFIGS.items():
            print(f"config {config} (margin "
                  f"{margin if margin is not None else 'shipped'}) ...")
            run = run_matrix(build, Path(tmp), tasks, config=config,
                             margin=margin, model_factory=factory)
            for r in run.results:
                mark = "ok " if r.passed else "FAIL"
                print(f"  [{mark}] {r.tier:4} {r.task_id:28} {r.detail}")
            runs.append(run)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    suffix = "_vertex" if args.real else ""
    report = render_report(runs, build_id=build.version,
                           transport=transport)
    (out / f"e19_baseline{suffix}.md").write_text(report,
                                                  encoding="utf-8")
    (out / f"e19_baseline{suffix}.json").write_text(json.dumps({
        "schema": "meridian.e19_baseline/1",
        "build_id": build.version, "transport": transport,
        "runs": [{"config": r.config, "margin": r.margin,
                  "line": r.line(), "tiers": r.tier_scores(),
                  "results": [vars(x) for x in r.results]}
                 for r in runs]}, indent=1) + "\n", encoding="utf-8")
    print(f"\nwrote {out / ('e19_baseline' + suffix + '.md')}")
    failures = [r for r in runs[0].results if not r.passed]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
