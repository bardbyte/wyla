"""The harness — pass@1 and pass^3, honestly computed (pinned).

n samples per task, c passes:
    pass@1 = mean(c/n)
    pass^3 = mean(C(c,3)/C(n,3))     — unbiased all-3-pass estimator
Deterministic SUTs run n=1 with ``deterministic=true``; pass^3 ≡ pass@1
and is never simulated. A deterministic SUT with 0 < c < n across
repeated trials is itself a bug alarm (the harness raises it).

AMBIGUOUS trials count in neither pass nor fail — their own rate, their
own triage file. ``--fail-under`` gates on pass; ``--max-ambiguous``
optionally gates the ambiguous rate.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from math import comb
from pathlib import Path
from typing import Any, Callable

from sahs.evals.grading import (
    SutAnswer,
    TrialResult,
    append_ambiguous,
    grade,
)
from sahs.evals.schema import Task
from sahs.evals.substrate import ExecutionSubstrate

Sut = Callable[[Task], SutAnswer]


@dataclass
class KindStats:
    n_tasks: int = 0
    pass_sum: float = 0.0
    pass3_sum: float = 0.0
    ambiguous_sum: float = 0.0
    fail_sum: float = 0.0

    def credit(self, n: int, c: int, a: int, f: int) -> None:
        self.n_tasks += 1
        self.pass_sum += c / n
        self.ambiguous_sum += a / n
        self.fail_sum += f / n
        self.pass3_sum += (comb(c, 3) / comb(n, 3)) if n >= 3 else c / n

    def report(self) -> dict[str, Any]:
        d = max(self.n_tasks, 1)
        return {"n_tasks": self.n_tasks,
                "pass@1": round(self.pass_sum / d, 4),
                "pass^3": round(self.pass3_sum / d, 4),
                "ambiguous_rate": round(self.ambiguous_sum / d, 4),
                "fail_rate": round(self.fail_sum / d, 4)}


def run_suite(tasks: list[Task], sut: Sut, *,
              substrate: ExecutionSubstrate | None = None,
              n: int = 1, deterministic: bool = True,
              triage_path: Path | None = None,
              on_trial: Callable[[TrialResult], None] | None = None
              ) -> dict[str, Any]:
    overall = KindStats()
    by_kind: dict[str, KindStats] = defaultdict(KindStats)
    failures: list[dict[str, Any]] = []
    warnings: list[str] = []
    determinism_alarms: list[str] = []

    effective_n = 1 if deterministic else max(n, 1)
    for task in tasks:
        verdicts: list[TrialResult] = []
        for _ in range(effective_n):
            answer = sut(task)
            trial = grade(task, answer, substrate)
            verdicts.append(trial)
            if on_trial:
                on_trial(trial)
            if trial.verdict == "ambiguous" and triage_path is not None:
                append_ambiguous(triage_path, task, trial)
            warnings.extend(trial.warnings)
        c = sum(1 for t in verdicts if t.verdict == "pass")
        a = sum(1 for t in verdicts if t.verdict == "ambiguous")
        f = effective_n - c - a
        if deterministic and 0 < c < effective_n:
            determinism_alarms.append(task.id)
        overall.credit(effective_n, c, a, f)
        by_kind[task.kind].credit(effective_n, c, a, f)
        if f or a:
            worst = next((t for t in verdicts if t.verdict != "pass"),
                         verdicts[0])
            failures.append({"task_id": task.id, "kind": task.kind,
                             "verdict": worst.verdict,
                             "reason": worst.reason,
                             "tags": task.tags})

    return {
        "schema": "meridian.evalreport/1",
        "n_tasks": overall.n_tasks,
        "samples_per_task": effective_n,
        "deterministic": deterministic,
        "overall": overall.report(),
        "by_kind": {k: s.report() for k, s in sorted(by_kind.items())},
        "failures": failures,
        "warnings": warnings[:50],
        "determinism_alarms": determinism_alarms,
    }


def format_report(report: dict[str, Any]) -> str:
    lines = [
        f"meridian eval — {report['n_tasks']} tasks · "
        f"n={report['samples_per_task']}"
        + (" · deterministic" if report["deterministic"] else ""),
        f"{'kind':16} {'n':>4} {'pass@1':>8} {'pass^3':>8} "
        f"{'ambig':>7} {'fail':>7}",
    ]

    def row(label: str, s: dict[str, Any]) -> str:
        return (f"{label:16} {s['n_tasks']:>4} {s['pass@1']:>8.3f} "
                f"{s['pass^3']:>8.3f} {s['ambiguous_rate']:>7.3f} "
                f"{s['fail_rate']:>7.3f}")

    lines.append(row("OVERALL", report["overall"]))
    for kind, stats in report["by_kind"].items():
        lines.append(row(kind, stats))
    if report["determinism_alarms"]:
        lines.append("⚠ DETERMINISM ALARMS (deterministic SUT flapped): "
                     + ", ".join(report["determinism_alarms"]))
    for f in report["failures"][:12]:
        lines.append(f"  ✗ [{f['kind']}] {f['task_id']} — "
                     f"{f['verdict']}: {f['reason']}")
    return "\n".join(lines)


def write_report(report: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=1, ensure_ascii=False) + "\n",
                    encoding="utf-8")
    return path
