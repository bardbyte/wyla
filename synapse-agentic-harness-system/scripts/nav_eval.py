#!/usr/bin/env python3
"""nav_eval.py — the navigation task set (Agent Loop v1 §9.4).

    python scripts/nav_eval.py --real                # Vertex (laptop)
    python scripts/nav_eval.py --real --limit 5      # a short pull
    cat docs/evals/navigation_baseline_vertex.md     # then PASTE it

Drives all 30 navigation tasks through the real turn engine with the
navigation lane on, grades outcomes (found / wrong-when-found /
precision) and trajectory hygiene, and writes
docs/evals/navigation_baseline_vertex.{md,json}.

There is no scripted baseline on purpose: navigation is the model's
judgement, and a scripted navigator would grade the script, not the
system. The harness math is pinned by tests/test_navigation_eval.py
instead. Without --real this prints the task list and exits: an
honest nothing rather than a pretend number.

The laptop cannot push — the report travels by PASTE.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.evals.navigation import (load_tasks, render_markdown,  # noqa: E402
                                   run_navigation)
from sahs.tools.api import Build                                 # noqa: E402
from sahs.util.auth import load_dotenv                           # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--builds", default="")
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    parser.add_argument("--limit", type=int, default=0,
                        help="run only the first N tasks")
    parser.add_argument("--real", action="store_true",
                        help="use Vertex (the only meaningful run)")
    args = parser.parse_args(argv)

    tasks = load_tasks()
    if not args.real:
        print(f"{len(tasks)} navigation tasks on file. This eval "
              "grades the MODEL's navigation, so it only runs with "
              "--real (Vertex on the laptop). The grader math is "
              "test-pinned; there is no scripted number to print.")
        for task in tasks:
            print(f"  {task['id']:>26} · {task['route']:<9} · "
                  f"{task['question']}")
        return 0

    load_dotenv()
    builds = Path(args.builds or os.environ.get("MERIDIAN_BUILDS_DIR")
                  or SILO / "builds")
    build = Build.open(builds)

    def factory(budget):
        from sahs.ask.model import VertexModel
        return VertexModel.from_env(budget)

    report = run_navigation(build, factory, tasks, limit=args.limit)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "navigation_baseline_vertex.json").write_text(
        json.dumps(report, indent=1, sort_keys=True) + "\n",
        encoding="utf-8")
    md = render_markdown(report, label=f"build {build.version}, Vertex")
    (out / "navigation_baseline_vertex.md").write_text(
        md, encoding="utf-8")
    print(md)
    print("wrote docs/evals/navigation_baseline_vertex.{md,json} — "
          "the laptop cannot push, so paste the .md back.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
