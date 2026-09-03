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

# pinned deliberate deferrals (exact filename → reason)
DEFERRALS: tuple[tuple[str, str], ...] = (
    ("audit_30d.jsonl.gz",
     "corroboration digests only: raw audit gz unread by design "
     "(two witnesses of the same events don't vote twice)"),
    ("tls_reference.md",
     "TLS rulebook: doc evidence node later, never parsed"),
    ("glossary_terms.csv",
     "generated view of data_cleaned.csv (Entry Type = Glossary Term): "
     "drift against the corpus is counted in the vocab report, rows "
     "are never loaded twice"),
    ("sample_codes.sql",
     "SQLite-dialect demo material, deliberately not canonicalized "
     "as BigQuery (dialect trap)"),
    ("knowledge.md",
     "skill prose: doc-evidence concern, not machine-parsed"),
    ("data_specs.md",
     "skill-pack data-spec prose: doc-evidence concern, "
     "not machine-parsed"),
    ("chart_contract.yaml",
     "skill-pack chart/presentation contract: rendering concern, "
     "not semantics"),
    ("qa_checks.yaml",
     "skill-pack QA checks: eval-layer concern, deferred until the "
     "harness consumes them"),
    ("_summary.json",
     "per-table extraction summary: operational metadata, "
     "not semantic"),
    ("summary.json",
     "extraction summary metadata: operational (consumed rows win "
     "where a loader reads one)"),
    ("run_manifest.json",
     "MDM extraction run manifest: operational metadata"),
    ("_profile_summary.json",
     "profiling run summary: operational metadata"),
    ("_state.json",
     "extractor checkpoint state: operational, not semantic"),
    (".DS_Store", "OS metadata"),
)
# pinned deliberate deferrals (filename-prefix → reason)
DEFERRAL_PREFIXES: tuple[tuple[str, str], ...] = (
    ("00_physical_table_resource",
     "physical-layer twin of the consumed 00 logical resource"),
    ("04_logical_table_options",
     "table options/labels: deferred until a card section "
     "consumes them"),
    ("06_physical_table_meta",
     "physical-layer twin of the logical artifact (01 is consumed): "
     "wire only if a divergence question arises"),
    ("07_physical_columns",
     "physical-layer twin of the logical artifact (02 is consumed): "
     "wire only if a divergence question arises"),
    ("08_physical_column_field_paths",
     "physical-layer twin of the logical artifact (03 is consumed): "
     "wire only if a divergence question arises"),
    ("09_physical_table_options",
     "physical-layer twin of the table options artifact"),
    ("12_physical_constraints",
     "physical-layer twin of the logical constraints (11 is "
     "consumed): same declarations at physical grain"),
    ("14_profile", "column-profiling plan/coverage metadata: "
     "operational, not semantic"),
    ("15_low_cardinality_manifest",
     "value-profile manifest: profiling coverage metadata, "
     "operational"),
    ("_batch_summary",
     "extraction batch summary: the registry input when used as "
     "--registry; operational otherwise"),
)
# pinned deliberate deferrals (filename-extension → reason)
DEFERRAL_EXTENSIONS: tuple[tuple[str, str], ...] = (
    (".bak", "editor backup: the live file is the source of record"),
)
# pinned deliberate deferrals (path-segment → reason)
DEFERRED_DIRS: tuple[tuple[str, str], ...] = (
    ("_history", "run-level history: index validation only"),
    ("_run_logs", "execution logs: operational, not semantic"),
    ("_shared", "shared extraction scratch: operational"),
    ("_profile_chunks", "profiling chunk scratch: operational"),
    ("14_profile_plan", "column-profiling plan metadata: "
     "operational, not semantic"),
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

    def _status(self, path: Path,
                consumed_stems: set[tuple[Path, str]]) -> tuple[str, str]:
        if path.resolve() in self._consumed:
            return "consumed", ""
        for segment, reason in (tuple(self._run_deferred_dirs)
                                + DEFERRED_DIRS):
            if segment in path.parts:
                return "deferred", reason
        # format twin: the SAME artifact in another serialization —
        # a sibling sharing the stem was consumed, so this file's facts
        # are already in the graph (13_table_metrics.json beside the
        # consumed .csv, _run_report.md beside the consumed .json, the
        # paired 15_ value-profile jsons, …)
        parent = path.resolve().parent
        if (parent, path.stem) in consumed_stems:
            return "deferred", ("format twin: the same artifact was "
                                "consumed in another serialization")
        for name, reason in DEFERRALS:
            if path.name == name:
                return "deferred", reason
        for prefix, reason in DEFERRAL_PREFIXES:
            if path.name.startswith(prefix):
                return "deferred", reason
        for ext, reason in DEFERRAL_EXTENSIONS:
            if path.name.endswith(ext):
                return "deferred", reason
        if path.suffix == ".json" \
                and "15_low_cardinality_values" in path.parts:
            return "deferred", ("value profile in JSON form: the "
                                "loader consumes the CSV form; "
                                "JSON-only profiles await a reader")
        return "inventoried", ""

    def build(self, roots: list[Path]) -> list[dict[str, Any]]:
        consumed_stems = {(p.parent, p.stem) for p in self._consumed}
        rows: list[dict[str, Any]] = []
        for root in roots:
            root = Path(root)
            if not root.exists():
                continue
            for path in sorted(p for p in root.rglob("*") if p.is_file()):
                status, reason = self._status(path, consumed_stems)
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
