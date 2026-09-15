"""The read-back loader (stub): what the Knowledge Catalog says, filed
as a pending witness — ``witness: kc`` — so the next compile can show
where the catalog and the graph disagree. Never a write to KC, never an
override of the graph.

Accepted exports: ``glossary`` (a list of terms with display name,
description, category, related entries), ``descriptions`` (entry or
column descriptions, with the entry's physical name), ``dq`` (data
quality scan results per table and rule). Each becomes ``doc:`` nodes
plus ``described_by`` / ``evidenced_by`` edges from the table (or
column) they are about, with ``props.review_status = "pending"``. Ids
follow the grammar in ``sahs.graph.ids`` (``doc:<slug>``), so nothing
mints a new kind."""

from __future__ import annotations

import datetime as _dt
import re
from pathlib import Path
from typing import Any

from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad

SOURCE = "kc_export"
KINDS = ("glossary", "descriptions", "dq")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")[:80] or "x"


def _prov(run_id: str, evidence: str, actor: str) -> Prov:
    return Prov(source=SOURCE, run=run_id, witness="kc",
                retrieved=_dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
                evidence=evidence, actor=actor or None)


def import_kc_export(graph_root: Path, kind: str, payload: dict[str, Any], *,
                     run_id: str, actor: str = "", known_tables: set[str] | None = None
                     ) -> dict[str, Any]:
    """→ report {kind, nodes, edges, skipped: [reason]}. Items about a
    table the graph does not know are skipped by name, never guessed."""
    if kind not in KINDS:
        return {"kind": kind, "nodes": 0, "edges": 0,
                "skipped": [f"unknown export kind {kind!r}: {', '.join(KINDS)}"]}
    graph = GraphDir(graph_root)
    report: dict[str, Any] = {"kind": kind, "nodes": 0, "edges": 0, "skipped": []}
    items = payload.get("items") if isinstance(payload, dict) else payload
    for i, item in enumerate(items or []):
        if not isinstance(item, dict):
            report["skipped"].append(f"item {i}: not an object")
            continue
        table = str(item.get("table") or item.get("physical") or "").lower()
        column = str(item.get("column") or "")
        if known_tables is not None and table and table not in known_tables:
            report["skipped"].append(f"item {i}: unknown table {table}")
            continue
        evidence = str(item.get("source") or f"kc_export:{kind}#{i}")
        if kind == "glossary":
            name = str(item.get("term") or item.get("displayName") or "")
            if not name:
                report["skipped"].append(f"item {i}: term without a name")
                continue
            doc_id = f"doc:kc_term_{_slug(name)}"
            graph.append_node(NodeRecord(id=doc_id, props={
                "kind": "kc_glossary_term", "text": str(item.get("description") or ""),
                "name": name, "category": str(item.get("category") or ""),
                "review_status": "pending"}, prov=_prov(run_id, evidence, actor)))
            report["nodes"] += 1
            targets = [table] if table else []
            targets += [str(t).lower() for t in item.get("related_entries") or []]
            for target in targets:
                if not target or (known_tables is not None
                                  and target.split(".")[0:2] and
                                  ".".join(target.split(".")[:2]) not in known_tables):
                    if target:
                        report["skipped"].append(f"item {i}: unknown entry {target}")
                    continue
                subject = (f"col:{target}" if target.count(".") >= 2 else f"table:{target}")
                graph.append_edge(Quad(s=subject, r="described_by", o=doc_id,
                                       props={"review_status": "pending",
                                              "link": "definition"},
                                       prov=_prov(run_id, evidence, actor)))
                report["edges"] += 1
        elif kind == "descriptions":
            text = str(item.get("description") or "")
            if not table or not text:
                report["skipped"].append(f"item {i}: needs table and description")
                continue
            target = f"{table}.{column}" if column else table
            doc_id = f"doc:kc_description_{_slug(target)}"
            graph.append_node(NodeRecord(id=doc_id, props={
                "kind": "kc_description", "text": text,
                "generated_by": str(item.get("source_kind") or ""),
                "review_status": "pending"}, prov=_prov(run_id, evidence, actor)))
            subject = f"col:{target}" if column else f"table:{table}"
            graph.append_edge(Quad(s=subject, r="described_by", o=doc_id,
                                   props={"review_status": "pending"},
                                   prov=_prov(run_id, evidence, actor)))
            report["nodes"] += 1
            report["edges"] += 1
        else:
            rule = str(item.get("rule") or "")
            if not table or not rule:
                report["skipped"].append(f"item {i}: needs table and rule")
                continue
            doc_id = f"doc:kc_dq_{_slug(table)}_{_slug(rule)}_{_slug(column or 'table')}"
            graph.append_node(NodeRecord(id=doc_id, props={
                "kind": "kc_dq_result", "rule": rule, "column": column,
                "passed": item.get("passed"), "score": item.get("score"),
                "text": str(item.get("detail") or ""), "review_status": "pending"},
                prov=_prov(run_id, evidence, actor)))
            subject = f"col:{table}.{column}" if column else f"table:{table}"
            graph.append_edge(Quad(s=subject, r="evidenced_by", o=doc_id,
                                   props={"review_status": "pending"},
                                   prov=_prov(run_id, evidence, actor)))
            report["nodes"] += 1
            report["edges"] += 1
    return report
