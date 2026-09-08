#!/usr/bin/env python3
"""Walk ``review_queue/*.plan.md`` and print each table's approval state.

Wraps :func:`lumi.approval.collect_approvals` and prints a fixed-width table:

    table                                | approved? | approver       | feedback
    ------------------------------------ | --------- | -------------- | --------

Exits non-zero (2) if any table is still pending so a CI / wake-up loop can
detect "the human has not finished reviewing yet".

Usage:
    python scripts/probe_review_queue.py
    python scripts/probe_review_queue.py --queue-dir review_queue/
    python scripts/probe_review_queue.py --json    # machine-readable

Exit codes:
    0  every plan has a decision (approved or rejected)
    2  at least one plan is still pending
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.approval import collect_approvals  # noqa: E402

logger = logging.getLogger("probe.review_queue")


def _refuse_in_repo_sa_key() -> None:
    import os

    val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not val:
        return
    p = Path(val).resolve()
    try:
        p.relative_to(REPO_ROOT.parent)
        print(
            f"ERROR: GOOGLE_APPLICATION_CREDENTIALS points inside the repo: {p}\n"
            "Move the SA JSON outside the repo (e.g. ~/Downloads/).",
            file=sys.stderr,
        )
        sys.exit(2)
    except ValueError:
        return


def _truncate(s: str | None, n: int) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_review_queue")
    parser.add_argument("--queue-dir", default="review_queue/")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    _refuse_in_repo_sa_key()

    queue = Path(args.queue_dir)
    if not queue.exists():
        print(f"ERROR: queue dir {queue} does not exist", file=sys.stderr)
        return 2

    approvals = collect_approvals(str(queue))
    if not approvals:
        print(f"(no .plan.md files in {queue})", file=sys.stderr)
        return 0

    if args.json:
        out = [
            {
                "table": a.table_name,
                "approved": a.approved,
                "approver": a.approver,
                "feedback": a.feedback,
                "pending": a.approver == "pending",
            }
            for a in approvals
        ]
        print(json.dumps(out, indent=2))
    else:
        header = f"{'table':<45} | {'approved?':<9} | {'approver':<14} | feedback"
        print(header)
        print("-" * len(header))
        for a in approvals:
            mark = "yes" if a.approved else ("no" if a.approver != "pending" else "—")
            print(
                f"{a.table_name[:44]:<45} | "
                f"{mark:<9} | "
                f"{str(a.approver)[:14]:<14} | "
                f"{_truncate(a.feedback, 80)}"
            )

    pending = [a for a in approvals if a.approver == "pending"]
    if pending:
        print(
            f"\n{len(pending)} table(s) still pending: "
            f"{', '.join(a.table_name for a in pending[:5])}"
            f"{' …' if len(pending) > 5 else ''}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
