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
from sahs.graph.quads import RANKING_WITNESSES
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


def _merge_witness_maps(held: dict[str, Any],
                        incoming: dict[str, Any]) -> None:
    """Cross-quad merge is MAX within a family (E12/A1) — the same
    witness corroborating from two directions is one witness, not two."""
    for family, value in incoming.items():
        if family not in held or value > held[family]:
            held[family] = value


# Gold contamination guard (pinned): the gold pairs are the eval answer
# key — a full graph citizen (census, cards, evidence) that must never
# feed a feature the resolver ranks on. audit_30d corroborates, never
# votes. The SUT must not contain its own test set.
assert "gold_attested" not in RANKING_WITNESSES
assert "audit_30d" not in RANKING_WITNESSES


def _finish_witness_features(row: dict[str, Any]) -> None:
    """support_effective = max over RANKING witnesses (never a sum —
    the catalog was mined from a superset of the same history);
    agreement = ranking families attesting; recency from the jobs
    witness alone when present (the only true timestamps), else the
    catalog-provided dates, flagged."""
    ranking = {family: value
               for family, value in row["support_by_witness"].items()
               if family in RANKING_WITNESSES}
    row["support"] = max(ranking.values(), default=0)
    row["witness_agreement"] = len(ranking)
    seen = row.get("seen_by_witness") or {}
    if seen.get("jobs_30d"):
        row["last_seen"] = seen["jobs_30d"]
        row["recency_source"] = "jobs_30d"
    else:
        catalog_dates = [d for family, d in seen.items()
                         if family in RANKING_WITNESSES and d]
        row["last_seen"] = max(catalog_dates, default="")
        row["recency_source"] = "catalog" if catalog_dates else ""


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
    table_of_metric: dict[str, str] = {}
    for (s, r, o, _w), quad in edges.items():
        if r == "certified_as" and quad.prov.status == "active":
            status_by_metric[s] = o.split(":", 1)[1]
        elif r == "measured_on" and s not in table_of_metric:
            table_of_metric[s] = o.split(":", 1)[1]
    metric_rows: list[dict[str, Any]] = []
    members_by_group: dict[str, list[str]] = defaultdict(list)
    # per-witness member_of quads for the same (metric, mgroup) fuse
    # here — support_by_witness is COMPILER OUTPUT, never store state
    membership: dict[tuple[str, str], dict[str, Any]] = {}
    for (s, r, o, witness), quad in sorted(edges.items()):
        if r != "member_of" or nodes.get(s) is None:
            continue
        entry = membership.setdefault((s, o), {
            "support_by_witness": {}, "seen_by_witness": {}})
        family = witness or "unknown"
        entry["support_by_witness"][family] = (
            entry["support_by_witness"].get(family, 0)
            + (quad.prov.support or 1))
        seen = str(quad.props.get("last_seen") or "")
        if seen > entry["seen_by_witness"].get(family, ""):
            entry["seen_by_witness"][family] = seen
    for (s, o), entry in sorted(membership.items()):
        record = nodes[s]
        source = record.prov.source
        metric_rows.append({
            "id": s, "mgroup": o,
            "label": record.props.get("label", ""),
            "table": table_of_metric.get(s, ""),
            "grain": record.props.get("grain", ""),
            "status": status_by_metric.get(s, "mined"),
            "question": record.props.get("question_answered", ""),
            "canonical_sql": record.props.get("canonical_sql", ""),
            "fp": s.split(":", 1)[1],
            "authority": int(_EXPR_AUTHORITY.get(
                source, Authority.SNIPPET)),
            "support_by_witness": entry["support_by_witness"],
            "seen_by_witness": entry["seen_by_witness"],
            "source": source,
            "approved_dimensions":
                record.props.get("approved_dimensions") or [],
            "sign_convention": record.props.get("sign_convention", ""),
        })
        members_by_group[o].append(s)
    # collapse per metric id: a metric fused across catalogs (same fp)
    # holds SEVERAL mgroup memberships — one row, groups merged, max
    # status/authority, per-witness support merged (max within family)
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
        _merge_witness_maps(held["support_by_witness"],
                            row["support_by_witness"])
        _merge_witness_maps(held["seen_by_witness"],
                            row["seen_by_witness"])
        if row["authority"] > held["authority"]:
            held.update(authority=row["authority"], source=row["source"])
        if _STATUS_RANK.get(row["status"], 0) > _STATUS_RANK.get(
                held["status"], 0):
            held["status"] = row["status"]
        for key in ("question", "grain", "label", "sign_convention"):
            held[key] = held[key] or row[key]
        if row["approved_dimensions"] and not held["approved_dimensions"]:
            held["approved_dimensions"] = row["approved_dimensions"]
    for row in collapsed.values():
        _finish_witness_features(row)
    metric_rows = sorted(collapsed.values(),
                         key=lambda r: (-r["authority"], -r["support"],
                                        r["id"]))
    # primary group identity: a fused metric ANSWERS as its highest-
    # authority catalog name (dmp ≻ gmns ≻ skill ≻ mined ≻ label@table);
    # the other memberships stay in mgroups as corroboration
    _CATALOG_RANK = {"dmp": 0, "gmns": 1, "skill": 2, "mined": 3}

    def _primacy(group: str) -> tuple[int, str]:
        catalog = group.split(":", 2)[1] if group.count(":") >= 2 else ""
        return (_CATALOG_RANK.get(catalog, 4), group)

    for row in metric_rows:
        row["mgroups"] = sorted(row["mgroups"])
        row["mgroup"] = min(row["mgroups"], key=_primacy)

    # ── binding rows: per-witness quads collapse per (label,table,fp) ──
    bound: dict[tuple[str, str, str], dict[str, Any]] = {}
    for (s, r, o, witness), quad in sorted(edges.items()):
        if r != "bound_to":
            continue
        concept = nodes.get(s)
        pred = nodes.get(o)
        if concept is None or pred is None:
            continue
        label = concept.props.get("label", "").strip().lower()
        table = s.split("@table:", 1)[1]
        source = quad.prov.source
        authority = int(_EXPR_AUTHORITY.get(source, Authority.SNIPPET))
        row = bound.setdefault((label, table, o.split(":", 1)[1]), {
            "label": label, "table": table, "fp": o.split(":", 1)[1],
            "canonical_sql": pred.props.get("canonical_sql", ""),
            "authority": authority, "source": source,
            "support_by_witness": {}, "seen_by_witness": {},
            "agreement": 1,
        })
        family = witness or "unknown"
        row["support_by_witness"][family] = (
            row["support_by_witness"].get(family, 0)
            + (quad.prov.support or 1))
        seen = str(quad.props.get("last_seen") or "")
        if seen > row["seen_by_witness"].get(family, ""):
            row["seen_by_witness"][family] = seen
        if authority > row["authority"]:
            row.update(authority=authority, source=source)
    binding_rows: list[dict[str, Any]] = list(bound.values())
    for row in binding_rows:
        _finish_witness_features(row)
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
    for (s, r, o, _w), quad in sorted(edges.items()):
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
    for (s, r, o, _w), quad in sorted(edges.items()):
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

    # snapshot schema (serving-facing: only columns BigQuery can serve —
    # D1 omissions fall out naturally) + observed join pairs
    schema = {
        c.physical: {name: column.data_type
                     for name, column in c.columns.items()}
        for c in consensus.values()}
    (build_dir / "schema.json").write_text(
        json.dumps(schema, indent=1, sort_keys=True) + "\n",
        encoding="utf-8")
    join_rows = [
        {"a": s.split(":", 1)[1], "b": o.split(":", 1)[1],
         "support": quad.prov.support or 1}
        for (s, r, o, _w), quad in sorted(edges.items())
        if r == "co_queried_with"]
    (build_dir / "indexes" / "joins.jsonl").write_text(
        "".join(json.dumps(j, sort_keys=True) + "\n" for j in join_rows),
        encoding="utf-8")
    domain_rows = [
        {"key": node_id.split(":", 1)[1],
         "values": record.props.get("values", [])}
        for node_id, record in sorted(nodes.items())
        if node_id.startswith("domain:")]
    (build_dir / "indexes" / "domains.jsonl").write_text(
        "".join(json.dumps(d, sort_keys=True) + "\n"
                for d in domain_rows), encoding="utf-8")

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
