#!/usr/bin/env python3
"""chat_eval.py — the assistant eval suites (Synapse v2 §13.6).

    python scripts/chat_eval.py --real                # Vertex (laptop)
    python scripts/chat_eval.py --real --kind playbook --limit 2
    cat docs/evals/assistant_baseline_vertex.md       # then PASTE it

Drives the artifact / reasoning / playbook task sets through the
REAL assistant loop, grades outcomes from the event record, and
writes docs/evals/assistant_baseline_vertex.{md,json}: one
two-number line per suite, trajectory hygiene beside them, and the
per-task rows (an aggregate without its rows is a mood).

There is no scripted baseline on purpose: the suites measure the
model's judgement, and a scripted assistant would grade the script.
The harness math is pinned by tests/test_v3_evals.py instead.
Without --real this prints the task list and exits — an honest
nothing rather than a pretend number.

The reasoning suite's calibrated judge is a fresh-context model
call; with --no-judge the keyword floor grades alone (cheaper,
stricter to read). The laptop cannot push — the report travels by
PASTE.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.assistant.evals import (load_tasks, render_markdown,  # noqa: E402
                                  run_suite)
from sahs.tools.api import Build                                # noqa: E402
from sahs.util.auth import load_dotenv                          # noqa: E402

JUDGE_SYSTEM = """You are a skeptical reviewer of an analyst's \
reasoning. Given a question and an answer, decide whether the answer \
is a genuinely useful, correct frame for the question — not padding, \
not evasion, no invented facts. Reply with STRICT JSON: \
{"useful": true|false, "because": "<one sentence>"}"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--builds", default="")
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    parser.add_argument("--kind", default="",
                        choices=["", "artifact", "reasoning", "recovery",
                                 "playbook"])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--real", action="store_true",
                        help="use Vertex (the only meaningful run)")
    parser.add_argument("--no-judge", action="store_true",
                        help="grade reasoning by keyword floor only")
    args = parser.parse_args(argv)

    tasks = load_tasks(args.kind or None)
    if not args.real:
        print(f"{len(tasks)} assistant tasks on file across "
              "artifact / reasoning / playbook. This eval only "
              "means something against a real model:")
        for task in tasks:
            print(f"  {task['id']:>28}  {task['kind']:<9} "
                  f"{task['question'][:60]}")
        print("\nrun with --real on the laptop (Vertex creds in "
              "the silo .env).")
        return 0

    load_dotenv(SILO / ".env")
    from sahs.ask.budget import Budget
    from sahs.ask.model import VertexModel
    from sahs.assistant.agent import VertexAgent
    builds_root = Path(args.builds) if args.builds \
        else SILO / "graph" / "builds"
    build = Build.open(builds_root)

    def model_factory(budget: Budget):
        return VertexAgent.from_env(budget)

    judge = None
    if not args.no_judge:
        judge_model = VertexModel.from_env(Budget())

        def judge(question: str, prose: str) -> bool:
            verdict = judge_model.json(
                f"QUESTION:\n{question}\n\nANSWER:\n{prose}",
                system=JUDGE_SYSTEM, temperature=0.0, max_tokens=200)
            return bool(isinstance(verdict, dict)
                        and verdict.get("useful") is True)

    report = run_suite(build, model_factory, tasks,
                       limit=args.limit, judge=judge)
    label = f"Vertex · build {build.version}"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "assistant_baseline_vertex.json").write_text(
        json.dumps(report, indent=1), encoding="utf-8")
    markdown = render_markdown(report, label=label)
    (out / "assistant_baseline_vertex.md").write_text(
        markdown, encoding="utf-8")
    print(markdown)
    print("→ PASTE docs/evals/assistant_baseline_vertex.md back "
          "into the session.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
