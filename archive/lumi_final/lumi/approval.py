"""Stage 4: Human-Approval Gate.

Walks ``review_queue/<table>.plan.md`` files written by ``lumi.plan`` and
parses the human's decision (or its absence) into ``PlanApproval`` records.

The decision lives at the bottom of each plan. We accept several markdown
variants because reviewers will hand-edit these files in random editors:

  - ``- [x] ✅ APPROVED``
  - ``- [X] ✅ APPROVED — yes to filtered measure``
  - ``[x] ✅ APPROVED``
  - ``✅ APPROVED`` on its own line
  - ``- [x] ❌ REJECTED`` plus a ``**Feedback:**`` / ``Notes:`` block

If neither box is ticked, the table is PENDING — surfaced as
``PlanApproval(approved=False, approver="pending", feedback="<pending …>")``.
``guardrails.check_approvals`` will then block the pipeline cleanly with a
named offender rather than silently skipping the table.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from lumi.schemas import ApprovalSource, PlanApproval

logger = logging.getLogger("lumi.approval")

# ─── Regexes for the three decision shapes ──────────────────────────
#
# Tolerant on:
#   - leading list marker ("- ", "* ", or none)
#   - checkbox state ("[ ]", "[x]", "[X]", or no checkbox at all)
#   - optional emoji (some terminals strip it)
#   - trailing free text after "APPROVED"/"REJECTED" (used as feedback)

_APPROVED_RE = re.compile(
    r"""
    ^[\s>*\-]*                                  # list marker / blockquote
    (?:\[\s*[xX]\s*\]\s*)?                      # optional ticked checkbox
    (?:✅\s*)?                              # optional ✅
    APPROVED
    (?:\s*[—\-:]\s*(?P<note>.+?))?         # optional " — note" / "- note"
    \s*$
    """,
    re.VERBOSE | re.MULTILINE,
)

_REJECTED_RE = re.compile(
    r"""
    ^[\s>*\-]*
    (?:\[\s*[xX]\s*\]\s*)?
    (?:❌\s*)?                              # optional ❌
    REJECTED
    (?:\s*[—\-:]\s*(?P<note>.+?))?
    \s*$
    """,
    re.VERBOSE | re.MULTILINE,
)

# Untickled boxes — used to detect "the template is here but nobody decided".
_PENDING_BOX_RE = re.compile(
    r"^[\s>*\-]*\[\s\]\s*(?:✅|❌)?\s*(APPROVED|REJECTED)",
    re.MULTILINE,
)

# Optional **Feedback:** / Notes: block. Captures everything from the heading
# until the next blank-line-then-heading or end-of-file.
_FEEDBACK_BLOCK_RE = re.compile(
    r"""
    (?:^|\n)
    (?:\*{0,2}|\#{0,3}\s*)
    (?:Feedback|Notes|Reviewer\s+notes)
    \s*[:：]?\s*\*{0,2}
    \s*\n
    (?P<body>.+?)
    (?=\n\s*\n\#|\n\s*\n\*\*|\n\s*\n[-=]{3,}|\Z)
    """,
    re.VERBOSE | re.IGNORECASE | re.DOTALL,
)

# Fenced code block fences — strip them from feedback bodies.
_FENCE_RE = re.compile(r"^```\w*\s*$|^```\s*$", re.MULTILINE)


# ─── Public API ─────────────────────────────────────────────────────


def collect_approvals(queue_dir: str) -> list[PlanApproval]:
    """Walk ``queue_dir`` for ``*.plan.md`` files and parse the human decision.

    One :class:`PlanApproval` per file. Files where neither the APPROVED nor
    REJECTED checkbox is ticked are returned as PENDING — that is intentional;
    :func:`lumi.guardrails.check_approvals` will fail the gate so the human
    sees exactly which tables still need attention.

    Args:
        queue_dir: Directory containing ``<table>.plan.md`` files.

    Returns:
        One ``PlanApproval`` per discovered ``*.plan.md`` file, sorted by
        table name for stable output. Returns an empty list if the directory
        does not exist.
    """
    queue = Path(queue_dir)
    if not queue.exists():
        logger.warning("Approval queue dir %s does not exist — no approvals collected", queue)
        return []

    approvals: list[PlanApproval] = []
    for plan_file in sorted(queue.glob("*.plan.md")):
        approvals.append(parse_approval_file(plan_file))
    return approvals


def parse_approval_file(plan_path: Path) -> PlanApproval:
    """Parse a single ``<table>.plan.md`` into a ``PlanApproval``.

    Reads the table name from the filename stem (everything before
    ``.plan.md``). The body is scanned for an APPROVED or REJECTED marker;
    rejections carry the optional ``Feedback:`` / ``Notes:`` block as
    ``feedback``.
    """
    table_name = plan_path.name.removesuffix(".plan.md")
    text = plan_path.read_text(encoding="utf-8")
    return _decide(table_name, text)


def parse_approval_text(table_name: str, text: str) -> PlanApproval:
    """Same as :func:`parse_approval_file` but takes already-loaded text.

    Useful for tests that build the markdown in-memory and don't want the
    round-trip through the filesystem.
    """
    return _decide(table_name, text)


# ─── Internal: decision logic ───────────────────────────────────────


def _decide(table_name: str, text: str) -> PlanApproval:
    approved_match = _find_decision(text, _APPROVED_RE)
    rejected_match = _find_decision(text, _REJECTED_RE)

    # If both somehow exist (template malformed or human mid-edit), the LATEST
    # one in the file wins. Reviewers append at the bottom; later == final.
    if approved_match and rejected_match:
        if approved_match.start() > rejected_match.start():
            rejected_match = None
        else:
            approved_match = None

    if approved_match:
        note = (approved_match.group("note") or "").strip() or None
        return PlanApproval(
            table_name=table_name,
            approved=True,
            approver=_infer_source(text, approved=True),
            feedback=note,
        )

    if rejected_match:
        feedback = _extract_feedback(text) or (
            (rejected_match.group("note") or "").strip() or None
        )
        if not feedback:
            # Reviewer ticked REJECTED but wrote nothing. We still record that
            # — the guardrail will block on missing feedback, which is the
            # right behavior (rejection without reason is not actionable).
            feedback = None
        return PlanApproval(
            table_name=table_name,
            approved=False,
            approver=_infer_source(text, approved=False),
            feedback=feedback,
        )

    # Pending — neither box ticked. Return a structured "still pending" record
    # so the guardrail can name the offending table cleanly.
    return PlanApproval(
        table_name=table_name,
        approved=False,
        approver="pending",  # type: ignore[arg-type]
        feedback="<pending — no decision marked>",
    )


def _find_decision(text: str, pattern: re.Pattern[str]) -> re.Match[str] | None:
    """Return the LAST match of ``pattern`` in ``text``, or None.

    We walk every match because reviewers occasionally leave the original
    template's "✅ APPROVED" sample line above their own decision; the latest
    one always wins.
    """
    last: re.Match[str] | None = None
    for m in pattern.finditer(text):
        # Skip if this match is actually an empty-checkbox template line.
        # The pending regex catches "[ ] APPROVED"; we exclude those here.
        line = _line_containing(text, m.start())
        if _PENDING_BOX_RE.match(line):
            continue
        last = m
    return last


def _line_containing(text: str, pos: int) -> str:
    start = text.rfind("\n", 0, pos) + 1
    end = text.find("\n", pos)
    if end == -1:
        end = len(text)
    return text[start:end]


def _extract_feedback(text: str) -> str | None:
    """Pull the body of a ``**Feedback:**`` / ``Notes:`` section, if present.

    Strips fenced-code markers and normalises whitespace. Returns None when
    the section is absent or contains only the placeholder text the planner
    writes by default ("(write any modifications here)").
    """
    m = _FEEDBACK_BLOCK_RE.search(text)
    if not m:
        return None
    body = m.group("body").strip()
    body = _FENCE_RE.sub("", body).strip()
    if not body:
        return None
    if body.lower().startswith("(write any modifications") or body == "(none)":
        return None
    # Collapse trailing horizontal-rule lines reviewers sometimes leave.
    body = re.sub(r"\n[-=]{3,}\s*$", "", body).strip()
    return body or None


def _infer_source(text: str, approved: bool) -> ApprovalSource:
    """Best-effort guess at WHO made the call.

    The plan markdown carries no explicit signature, so we sniff for hints:
      - "(auto-approved" / "auto_low_risk" → ``auto_low_risk``
      - "(auto skip)" → ``auto_skip``
      - default → ``human``

    Misclassification here only affects a guardrail WARNING ("no human in
    the loop"), not pipeline correctness, so heuristics are acceptable.
    """
    lowered = text.lower()
    if "auto_low_risk" in lowered or "auto-approved" in lowered:
        return "auto_low_risk"
    if "auto_skip" in lowered or "(auto skip)" in lowered:
        return "auto_skip"
    return "human" if approved or "rejected" in lowered else "human"
