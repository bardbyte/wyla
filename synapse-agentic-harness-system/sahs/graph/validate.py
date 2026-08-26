"""The validator — foreign keys, by discipline (pinned check list).

A build that fails validation never compiles. Checks (E-numbered where an
amendment pinned them):

     1. every line parses as its record model
     2. required keys present (model-enforced) + source non-empty
     3. IDs match the grammar regexes
     4. every edge endpoint resolves to a node (2-pass)
     5. relation registered; subject/object kinds legal
     6. valid_for references a real schema-version node or "unversioned"
        (warning)
     7. evidence paths exist when the run manifest marks the source
        archived
     8. no duplicate (s, r, o, run)
     9. E7: certified_as transitions legal per the lattice
    10. prov.status in the quad-status enum
    11. support ≥ 1 where present
    12. post-fold: every certified metric retains ≥ 1 active binding
        (measured_on) — a certified metric with no home is a lie
    13. E12/A1: measure-plane quads carry prov.witness ∈ WITNESSES;
        an unknown witness anywhere is an error
    14. E12/A5: review nodes — kind/status enums, numeric priority,
        a resolving `concerns` edge
    E7. prov.actor REQUIRED when source == "clerk"

Warnings (never block): orphan nodes, unversioned valid_for.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from sahs.graph.ids import (
    ID_PATTERNS,
    LEGAL_TRANSITIONS,
    STATUS_STATES,
    kind_of,
)
from sahs.graph.quads import (
    QUAD_STATUSES,
    RELATIONS,
    REVIEW_KINDS,
    REVIEW_STATUSES,
    WITNESSES,
    GraphDir,
)

# the relations where "who saw it" is load-bearing (E12/A1)
_MEASURE_PLANE = {"bound_to", "member_of", "measured_on",
                  "defines_metric", "variant_of", "certified_as"}


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_json(self) -> str:
        return json.dumps({"ok": self.ok, "n_errors": len(self.errors),
                           "n_warnings": len(self.warnings),
                           "errors": self.errors[:200],
                           "warnings": self.warnings[:200]}, indent=1)


def validate_graph(root: Path) -> ValidationReport:
    report = ValidationReport()
    graph = GraphDir(root)

    # pass 1 — nodes
    node_ids: set[str] = set()
    schema_ids: set[str] = set()
    for path in sorted((root / "nodes").glob("*.jsonl")):
        for line_no, line in enumerate(
                path.read_text(encoding="utf-8").split("\n"), start=1):
            if not line.strip():
                continue
            where = f"{path.name}:{line_no}"
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                report.errors.append(f"[1] {where}: unparsable line ({e})")
                continue
            node_id = str(record.get("id", ""))
            kind = kind_of(node_id)
            if kind is None:
                report.errors.append(
                    f"[3] {where}: id fails grammar: {node_id!r}")
                continue
            if kind != path.stem:
                report.errors.append(
                    f"[3] {where}: {node_id} filed under {path.stem}.jsonl")
            prov = record.get("prov") or {}
            if not prov.get("source"):
                report.errors.append(f"[2] {where}: empty prov.source")
            node_ids.add(node_id)
            if kind == "schema":
                schema_ids.add(node_id)

    # pass 2 — edges
    seen: set[tuple[str, str, str, str, str]] = set()
    manifests = _archived_runs(root)
    for path in sorted((root / "edges").glob("*.jsonl")):
        relation = path.stem
        signature = RELATIONS.get(relation)
        if signature is None:
            report.errors.append(f"[5] {path.name}: unregistered relation")
            continue
        allowed_s, allowed_o = signature
        for line_no, line in enumerate(
                path.read_text(encoding="utf-8").split("\n"), start=1):
            if not line.strip():
                continue
            where = f"{path.name}:{line_no}"
            try:
                quad = json.loads(line)
            except json.JSONDecodeError as e:
                report.errors.append(f"[1] {where}: unparsable line ({e})")
                continue
            s, r, o = (str(quad.get(k, "")) for k in ("s", "r", "o"))
            prov = quad.get("prov") or {}
            if r != relation:
                report.errors.append(
                    f"[5] {where}: r={r!r} filed under {relation}.jsonl")
            for endpoint, allowed, label in ((s, allowed_s, "s"),
                                             (o, allowed_o, "o")):
                kind = kind_of(endpoint)
                if kind is None:
                    report.errors.append(
                        f"[3] {where}: {label} fails grammar: {endpoint!r}")
                elif kind not in allowed:
                    report.errors.append(
                        f"[5] {where}: {label} kind {kind} illegal for "
                        f"{relation}")
                elif kind not in ("run", "status", "owner", "policy",
                                  "doc", "domain") \
                        and endpoint not in node_ids:
                    report.errors.append(
                        f"[4] {where}: {label} endpoint unresolved: "
                        f"{endpoint}")
            status = prov.get("status", "active")
            if status not in QUAD_STATUSES:
                report.errors.append(f"[10] {where}: bad status {status!r}")
            support = prov.get("support")
            if support is not None and (not isinstance(support, int)
                                        or support < 1):
                report.errors.append(f"[11] {where}: support {support!r}")
            for v in prov.get("valid_for") or []:
                if v != "unversioned" and f"schema:{v}" not in schema_ids \
                        and v not in schema_ids:
                    report.errors.append(
                        f"[6] {where}: valid_for {v!r} not a schema node")
            if (prov.get("valid_for") or ["unversioned"]) == ["unversioned"]:
                report.warnings.append(f"[6w] {where}: unversioned")
            run = prov.get("run", "")
            # dedup per (s,r,o,run,source,WITNESS): one witness family
            # may not repeat itself within a run; independent witnesses
            # each testify once (E12/A1)
            key = (s, r, o, run, prov.get("source", ""),
                   prov.get("witness", ""))
            if key in seen:
                report.errors.append(
                    f"[8] {where}: duplicate (s,r,o,run,source,witness)")
            seen.add(key)
            if prov.get("source") == "clerk" and not prov.get("actor"):
                report.errors.append(
                    f"[E7] {where}: clerk edge without prov.actor")
            # [13] E12/A1 — measure-plane quads carry a legal witness
            witness = prov.get("witness", "")
            if r in _MEASURE_PLANE:
                if not witness:
                    report.errors.append(
                        f"[13] {where}: measure-plane quad without "
                        "prov.witness")
                elif witness not in WITNESSES:
                    report.errors.append(
                        f"[13] {where}: unknown witness {witness!r}")
            elif witness and witness not in WITNESSES:
                report.errors.append(
                    f"[13] {where}: unknown witness {witness!r}")
            evidence = prov.get("evidence", "")
            if evidence and run in manifests and manifests[run]:
                if not (root / evidence).exists() \
                        and not Path(evidence).exists():
                    report.errors.append(
                        f"[7] {where}: evidence path missing: {evidence}")

    # E7 — governance transition legality
    for subject, states in graph.governance_history().items():
        previous = None
        for state in states:
            if state not in STATUS_STATES:
                report.errors.append(
                    f"[9] {subject}: unknown state {state!r}")
            elif previous is not None and state != previous \
                    and state not in LEGAL_TRANSITIONS.get(previous, set()):
                report.errors.append(
                    f"[9] {subject}: illegal transition "
                    f"{previous} → {state}")
            previous = state

    # 12 — every certified metric keeps an active home
    edges = graph.fold_edges()
    certified = {s for (s, r, o, _w), q in edges.items()
                 if r == "certified_as" and o == "status:certified"
                 and q.prov.status == "active" and s.startswith("metric:")}
    homed = {s for (s, r, o, _w), q in edges.items()
             if r == "measured_on" and q.prov.status == "active"}
    for metric in sorted(certified - homed):
        report.errors.append(
            f"[12] certified metric without active measured_on: {metric}")

    # [14] E12/A5 — ReviewItem nodes: kind/status enums, numeric
    # priority, and a resolving `concerns` subject
    concerned = {s for (s, r, o, _w), q in edges.items()
                 if r == "concerns"}
    for node_id, record in graph.fold_nodes().items():
        if not node_id.startswith("review:"):
            continue
        props = record.props
        if props.get("kind") not in REVIEW_KINDS:
            report.errors.append(
                f"[14] {node_id}: unknown review kind "
                f"{props.get('kind')!r}")
        if props.get("status") not in REVIEW_STATUSES:
            report.errors.append(
                f"[14] {node_id}: bad review status "
                f"{props.get('status')!r}")
        if not isinstance(props.get("priority"), (int, float)):
            report.errors.append(f"[14] {node_id}: non-numeric priority")
        if node_id not in concerned:
            report.errors.append(
                f"[14] {node_id}: no `concerns` edge to its subject")

    # warnings — orphan nodes (no edge touches them)
    touched = {e for key in edges for e in (key[0], key[2])}
    for orphan in sorted(node_ids - touched):
        report.warnings.append(f"[orphan] {orphan}")
    return report


def _archived_runs(root: Path) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for manifest in (root / "runs").glob("*/manifest.json"):
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            out[str(payload.get("run_id", manifest.parent.name))] = bool(
                payload.get("archived"))
        except (OSError, json.JSONDecodeError):
            continue
    return out
