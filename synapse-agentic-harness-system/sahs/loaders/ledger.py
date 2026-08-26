"""Utilization ledger (E12/A2) — every archive file accounted for.

"Are we utilizing everything we're getting?" stops being a question you
answer from memory: every file under the archive (and sources) roots is
checksummed and marked

    consumed              a loader actually read it this run
    deferred(reason)      deliberately unread, with the pinned reason
    inventoried           present, unread, NOT deliberately deferred —
                          the honest "we have this and do nothing yet"

No archive artifact may be absent from the ledger — the walk guarantees
presence; the CI completeness test guarantees the `inventoried` set only
ever contains files we KNOW about. The ledger lands in the run manifest
as ``utilization[]``.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

# pinned deliberate deferrals (filename-suffix → reason)
DEFERRALS: tuple[tuple[str, str], ...] = (
    ("audit_30d.jsonl.gz",
     "corroboration digests only — raw audit gz unread by design "
     "(two witnesses of the same events don't vote twice)"),
    ("tls_reference.md",
     "TLS rulebook: doc evidence node later, never parsed"),
    ("sample_codes.sql",
     "SQLite-dialect demo material — deliberately not canonicalized "
     "as BigQuery (dialect trap)"),
    ("knowledge.md",
     "skill prose — doc-evidence concern, not machine-parsed"),
)
# pinned deliberate deferrals (path-segment → reason)
DEFERRED_DIRS: tuple[tuple[str, str], ...] = (
    ("_history", "run-level history — index validation only"),
    ("_run_logs", "execution logs — operational, not semantic"),
)


def _sha12(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()[:12]


class UtilizationLedger:
    """Loaders call ``consumed(path)`` at every read; ``build(roots)``
    walks the trees and renders the full accounting."""

    def __init__(self) -> None:
        self._consumed: set[Path] = set()
        self._run_deferred_dirs: list[tuple[str, str]] = []

    def consumed(self, path: Path) -> None:
        path = Path(path)
        if path.exists():
            self._consumed.add(path.resolve())

    def defer_dir(self, segment: str, reason: str) -> None:
        """Run-scoped deliberate deferral (e.g. a witness disabled for
        this run by assumption) — same semantics as the pinned
        DEFERRED_DIRS, decided per invocation instead of forever."""
        self._run_deferred_dirs.append((segment, reason))

    def _status(self, path: Path) -> tuple[str, str]:
        if path.resolve() in self._consumed:
            return "consumed", ""
        for segment, reason in (tuple(self._run_deferred_dirs)
                                + DEFERRED_DIRS):
            if segment in path.parts:
                return "deferred", reason
        for suffix, reason in DEFERRALS:
            if path.name == suffix:
                return "deferred", reason
        return "inventoried", ""

    def build(self, roots: list[Path]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for root in roots:
            root = Path(root)
            if not root.exists():
                continue
            for path in sorted(p for p in root.rglob("*") if p.is_file()):
                status, reason = self._status(path)
                row = {"root": root.name,
                       "path": str(path.relative_to(root)),
                       "sha256_12": _sha12(path),
                       "status": status}
                if reason:
                    row["reason"] = reason
                rows.append(row)
        return rows

    @staticmethod
    def summary(rows: list[dict[str, Any]]) -> dict[str, int]:
        out = {"consumed": 0, "deferred": 0, "inventoried": 0}
        for row in rows:
            out[row["status"]] += 1
        return out
