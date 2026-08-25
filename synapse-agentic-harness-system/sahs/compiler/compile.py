"""compile(graph, crosswalk) → builds/b_<graph_hash12>/ — deterministic.

The compiler is a pure function of the truth graph: same graph, byte-
identical build (no timestamps inside build artifacts — run metadata
lives in the event stream). Stages: fold → reconcile (E1) → acl (E3) →
index rows → cards (budgeted) → census (+structural) → manifest
(resolver constants embedded, E6) → DIFF_vs_prev → gates →
**atomic CURRENT cutover (E4)** only when every gate passes.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from sahs.canon.authority import SOURCE_AUTHORITY, Authority
from sahs.compiler.cards import concept_card, metric_card, table_card
from sahs.compiler.diff import build_diff
from sahs.compiler.indexes import build_indexes
from sahs.compiler.reconcile import (
    build_acl,
    reconcile,
    structural_census,
)
from sahs.graph.quads import GraphDir
from sahs.tools.constants import RESOLVER_CONSTANTS

SCHEMA = "meridian.build/1"

_EXPR_AUTHORITY = {
    "metrics_dmp": Authority.CERTIFIED,
    "extended_gmns": Authority.PENDING,
    "skill_contract": Authority.SKILL_CONTRACT,
    "gold_queries": Authority.SKILL_CONTRACT,
    "measures_catalog": Authority.MINED,
    "blue_insights": Authority.SNIPPET,
}


def graph_hash(graph_root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(Path(graph_root).glob("nodes/*.jsonl")) + \
            sorted(Path(graph_root).glob("edges/*.jsonl")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def compile_build(graph_root: Path, builds_root: Path
                  ) -> tuple[Path, dict[str, Any], list[str]]:
    """→ (build_dir, manifest, gate_failures). CURRENT moves only when
    gate_failures is empty."""
    graph = GraphDir(graph_root)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    build_id = f"b_{graph_hash(graph_root)}"
    build_dir = Path(builds_root) / build_id
    (build_dir / "cards" / "tables").mkdir(parents=True, exist_ok=True)
    (build_dir / "cards" / "metrics").mkdir(parents=True, exist_ok=True)
    (build_dir / "cards" / "concepts").mkdir(parents=True, exist_ok=True)

    consensus = reconcile(graph)
    acl = build_acl(graph, consensus)

    # ── metric rows ──
    status_by_metric: dict[str, str] = {}
    for (s, r, o), quad in edges.items():
        if r == "certified_as" and quad.prov.status == "active":
            status_by_metric[s] = o.split(":", 1)[1]
    metric_rows: list[dict[str, Any]] = []
    members_by_group: dict[str, list[str]] = defaultdict(list)
    for (s, r, o), quad in sorted(edges.items()):
        if r != "member_of":
            continue
        record = nodes.get(s)
        if record is None:
            continue
        table = next((obj.split(":", 1)[1]
                      for (s2, r2, obj), q2 in edges.items()
                      if s2 == s and r2 == "measured_on"), "")
        source = record.prov.source
        metric_rows.append({
            "id": s, "mgroup": o,
            "label": record.props.get("label", ""),
            "table": table,
            "grain": record.props.get("grain", ""),
            "status": status_by_metric.get(s, "mined"),
            "question": record.props.get("question_answered", ""),
            "canonical_sql": record.props.get("canonical_sql", ""),
            "fp": s.split(":", 1)[1],
            "authority": int(_EXPR_AUTHORITY.get(
                source, Authority.SNIPPET)),
            "support": quad.prov.support or 1,
            "source": source,
            "approved_dimensions":
                record.props.get("approved_dimensions") or [],
            "sign_convention": record.props.get("sign_convention", ""),
        })
        members_by_group[o].append(s)
    # collapse per metric id: a metric fused across catalogs (same fp)
    # holds SEVERAL mgroup memberships — one row, groups merged, max
    # status/authority, support summed
    _STATUS_RANK = {"mined": 1, "team_candidate": 2, "pending": 3,
                    "certified": 4, "rejected": 0, "deprecated": 0}
    collapsed: dict[str, dict[str, Any]] = {}
    for row in metric_rows:
        held = collapsed.get(row["id"])
        if held is None:
            row["mgroups"] = [row["mgroup"]]
            collapsed[row["id"]] = row
            continue
        held["mgroups"].append(row["mgroup"])
        held["support"] += row["support"]
        if row["authority"] > held["authority"]:
            held.update(authority=row["authority"], source=row["source"])
        if _STATUS_RANK.get(row["status"], 0) > _STATUS_RANK.get(
                held["status"], 0):
            held["status"] = row["status"]
        for key in ("question", "grain", "label", "sign_convention"):
            held[key] = held[key] or row[key]
        if row["approved_dimensions"] and not held["approved_dimensions"]:
            held["approved_dimensions"] = row["approved_dimensions"]
    metric_rows = sorted(collapsed.values(),
                         key=lambda r: (-r["authority"], -r["support"],
                                        r["id"]))
    for row in metric_rows:
        row["mgroup"] = sorted(row["mgroups"])[0]

    # ── binding rows ──
    binding_rows: list[dict[str, Any]] = []
    for (s, r, o), quad in sorted(edges.items()):
        if r != "bound_to":
            continue
        concept = nodes.get(s)
        pred = nodes.get(o)
        if concept is None or pred is None:
            continue
        label = concept.props.get("label", "").strip().lower()
        table = s.split("@table:", 1)[1]
        source = quad.prov.source
        binding_rows.append({
            "label": label, "table": table,
            "fp": o.split(":", 1)[1],
            "canonical_sql": pred.props.get("canonical_sql", ""),
            "authority": int(_EXPR_AUTHORITY.get(
                source, Authority.SNIPPET)),
            "support": quad.prov.support or 1,
            "source": source, "agreement": 1,
        })
    binding_rows.sort(key=lambda r: (r["label"], r["table"],
                                     -r["authority"], -r["support"],
                                     r["fp"]))

    # ── vocab rows ──
    vocab_rows: list[dict[str, Any]] = []
    for node_id, record in sorted(nodes.items()):
        kind = node_id.split(":", 1)[0]
        if kind == "acr":
            vocab_rows.append({
                "text": record.props.get("symbol", ""),
                "kind": "acronym", "ref": node_id,
                "bu": record.props.get("business_unit", "All").lower(),
                "region": record.props.get("region", "All").lower(),
                "definition": record.props.get("definition", "")})
        elif kind == "term":
            vocab_rows.append({
                "text": record.props.get("name", ""), "kind": "term",
                "ref": node_id,
                "definition": record.props.get("status", "")})
        elif kind == "mgroup":
            label = record.props.get("label", "")
            if label:
                vocab_rows.append({"text": label, "kind": "metric",
                                   "ref": node_id, "definition": ""})
        elif kind == "concept":
            vocab_rows.append({
                "text": record.props.get("label", ""), "kind": "concept",
                "ref": node_id, "definition": ""})
        elif kind == "table":
            physical = node_id.split(":", 1)[1]
            vocab_rows.append({
                "text": physical.split(".")[-1], "kind": "table",
                "ref": node_id,
                "definition": record.props.get(
                    "business_name_atlas", "")})

    index_report = build_indexes(build_dir / "indexes", vocab_rows,
                                 binding_rows, metric_rows)

    # ── cards ──
    budget: dict[str, Any] = {"over_budget": 0, "dropped": {}}
    co_by_table: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (s, r, o), quad in sorted(edges.items()):
        if r == "co_queried_with":
            co_by_table[s.split(":", 1)[1]].append(
                (o.split(":", 1)[1], quad.prov.support or 1))
    for tid, table_consensus in sorted(consensus.items()):
        physical = table_consensus.physical
        record = nodes.get(tid)
        metrics_here = [
            {"id": m["id"], "label": m["label"], "status": m["status"],
             "source": m["source"]}
            for m in metric_rows if m["table"] == physical]
        filters_here = sorted(
            ({"label": b["label"], "sql": b["canonical_sql"],
              "support": b["support"], "source": b["source"],
              "authority": b["authority"]}
             for b in binding_rows if b["table"] == physical),
            key=lambda f: (-f["authority"], -f["support"], f["label"]))
        text, card_budget = table_card(
            table_consensus, record.props if record else {},
            metrics_here, filters_here,
            sorted(co_by_table.get(physical, []),
                   key=lambda x: -x[1]),
            acl.get(physical, {"restricted": None, "pii_columns": []}))
        (build_dir / "cards" / "tables"
         / f"{physical.replace('.', '__')}.md").write_text(
            text + "\n", encoding="utf-8")
        budget["over_budget"] += int(card_budget["over_budget"])
        if card_budget["dropped"]:
            budget["dropped"][physical] = card_budget["dropped"]

    children_of: dict[str, list[str]] = defaultdict(list)
    parent_of: dict[str, str] = {}
    for (s, r, o), quad in sorted(edges.items()):
        if r == "variant_of" and quad.prov.status == "active":
            children_of[o].append(s)
            parent_of[s] = o
    rows_by_id = {m["id"]: m for m in metric_rows}
    for row in metric_rows:
        variants = [rows_by_id[child]
                    for child in children_of.get(row["id"], [])
                    if child in rows_by_id]
        row["parent_fp"] = (parent_of.get(row["id"], "")
                            .split(":", 1)[-1]
                            if row["id"] in parent_of else "")
        (build_dir / "cards" / "metrics"
         / f"{row['fp']}.md").write_text(
            metric_card(row, variants) + "\n", encoding="utf-8")

    bindings_by_label: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in binding_rows:
        bindings_by_label[row["label"]].append(row)
    for label, rows in sorted(bindings_by_label.items()):
        slug = label.replace(" ", "_").replace("/", "_")[:60]
        (build_dir / "cards" / "concepts" / f"{slug}.md").write_text(
            concept_card(label, rows) + "\n", encoding="utf-8")

    # ── census (compiled view + E1 structural) ──
    concept_conflicts = sum(
        1 for rows in bindings_by_label.values()
        if len({r["fp"] for r in rows}) > 1)
    group_conflicts = sum(
        1 for members in members_by_group.values()
        if len(set(members)) > 1)
    census = {
        "schema": "meridian.census/1",
        "summary": {
            "concept_cells": len(bindings_by_label),
            "concept_conflicts": concept_conflicts,
            "metric_groups": len(members_by_group),
            "metric_conflicts": group_conflicts,
        },
        "structural": structural_census(consensus),
    }
    (build_dir / "census.json").write_text(
        json.dumps(census, indent=1) + "\n", encoding="utf-8")
    tickets = [t for c in consensus.values() for t in c.tickets]
    (build_dir / "tickets.jsonl").write_text(
        "".join(json.dumps(t, sort_keys=True) + "\n" for t in tickets),
        encoding="utf-8")
    (build_dir / "acl.json").write_text(
        json.dumps(acl, indent=1, sort_keys=True) + "\n", encoding="utf-8")

    # ── manifest (no wall-clock — determinism) ──
    manifest = {
        "schema": SCHEMA, "build_id": build_id,
        "graph_hash": graph_hash(graph_root),
        "counts": {"tables": len(consensus),
                   "metrics": len(metric_rows),
                   "bindings": len(binding_rows),
                   "vocab": len(vocab_rows),
                   "tickets": len(tickets)},
        "index": index_report,
        "budget": budget,
        "resolver_constants": RESOLVER_CONSTANTS,
        "census_summary": census["summary"],
        "structural_totals": census["structural"]["totals"],
    }
    (build_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=1, sort_keys=True) + "\n",
        encoding="utf-8")

    # ── DIFF vs the currently promoted build ──
    current_path = Path(builds_root) / "CURRENT"
    previous = None
    if current_path.exists():
        candidate = Path(builds_root) / current_path.read_text(
            encoding="utf-8").strip()
        if candidate.exists() and candidate != build_dir:
            previous = candidate
    (build_dir / "DIFF_vs_prev.md").write_text(
        build_diff(build_dir, previous) + "\n", encoding="utf-8")

    # ── gates → atomic CURRENT (E4) ──
    failures: list[str] = []
    for table_consensus in consensus.values():
        card = (build_dir / "cards" / "tables"
                / f"{table_consensus.physical.replace('.', '__')}.md")
        if not card.exists():
            failures.append(f"missing table card: "
                            f"{table_consensus.physical}")
    for row in metric_rows:
        if row["status"] == "certified" and not row["table"]:
            failures.append(f"certified metric without table: {row['id']}")
    if not failures:
        tmp = current_path.with_suffix(".tmp")
        tmp.write_text(build_id + "\n", encoding="utf-8")
        os.replace(tmp, current_path)
    return build_dir, manifest, failures
