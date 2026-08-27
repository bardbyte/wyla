"""E1 — structural reconciliation: Atlas ↔ Lumi ↔ BQ consensus.

Runs inside compile, BEFORE cards. Per (table, column): one consensus
record with per-field {value, source} and an ``agreement_count`` (how
many witnesses concur on the winning value — exposed as a ranking
feature). Disagreements route through the pinned D1–D5 handlers:

  D1 catalog-only column (absent in BQ)  → omitted from cards; ticket
     `catalog_stale`. The agent must never see a column the runtime
     can't serve.
  D2 BQ-only column (undocumented)       → on card, flagged
     `ungoverned`; ticket `coverage_gap`; resolver binds only with a
     disclosure flag.
  D3 type/DDL mismatch                   → BQ wins everywhere
     execution-facing; ticket `catalog_mismatch`.
  D4 description divergence Atlas↔Lumi   → both kept with attribution,
     Atlas display-first; no ticket (usually complementary).
  D5 sensitivity flags disagree          → most-restrictive applied
     immediately; ticket `sensitivity_conflict`; restrictive holds
     while open.

The census gains a `structural` section (D1–D5 counts per table);
tickets land in the build for the governance queue.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import yaml

from sahs.graph.quads import GraphDir, NodeRecord


@dataclass
class ColumnConsensus:
    name: str
    present_bq: bool
    present_catalog: bool
    data_type: str = ""
    type_source: str = ""
    description: str = ""
    description_source: str = ""
    description_supplementary: str = ""
    business_name: str = ""
    sensitive: bool = False
    sensitivity_sources: list[str] = field(default_factory=list)
    ungoverned: bool = False               # D2
    agreement_count: int = 1
    flags: list[str] = field(default_factory=list)   # D-codes hit


@dataclass
class TableConsensus:
    physical: str
    columns: dict[str, ColumnConsensus] = field(default_factory=dict)
    omitted_catalog_only: list[str] = field(default_factory=list)  # D1
    structural: dict[str, int] = field(
        default_factory=lambda: {f"D{i}": 0 for i in range(1, 6)})
    tickets: list[dict[str, Any]] = field(default_factory=list)


def load_merge_policy(path) -> dict:
    return yaml.safe_load(open(path, encoding="utf-8"))


def reconcile(graph: GraphDir) -> dict[str, TableConsensus]:
    """Fold the graph and produce per-table consensus. Deterministic:
    same graph, same output (iteration is over sorted folds)."""
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()

    columns_by_table: dict[str, list[str]] = defaultdict(list)
    for (s, r, o, _w), quad in edges.items():
        if r == "has_column":
            columns_by_table[s].append(o)

    out: dict[str, TableConsensus] = {}
    for tid in sorted(columns_by_table):
        physical = tid.split(":", 1)[1]
        consensus = TableConsensus(physical=physical)
        for cid in sorted(set(columns_by_table[tid])):
            record = nodes.get(cid)
            if record is None:
                continue
            props = record.props
            # everything after "dataset.table." — a nested field path
            # (col:dw.t.a.b.c) keeps its full dotted name on the card
            name = cid.split(":", 1)[1][len(physical) + 1:]
            bq_type = str(props.get("data_type") or "")
            mdm_type = str(props.get("data_type_mdm") or "")
            atlas_type = str(props.get("data_type_atlas") or "")
            present_bq = bool(bq_type) or props.get("ordinal") is not None
            present_catalog = bool(mdm_type or atlas_type
                                   or props.get("description_mdm")
                                   or props.get("description_atlas"))
            column = ColumnConsensus(
                name=name, present_bq=present_bq,
                present_catalog=present_catalog)

            if not present_bq and present_catalog:            # D1
                consensus.structural["D1"] += 1
                consensus.omitted_catalog_only.append(name)
                consensus.tickets.append({
                    "ticket": "catalog_stale", "table": physical,
                    "column": name,
                    "detail": "documented in catalog, absent in BigQuery"})
                continue                # never reaches a card
            if present_bq and not present_catalog:            # D2
                consensus.structural["D2"] += 1
                column.ungoverned = True
                column.flags.append("D2")
                consensus.tickets.append({
                    "ticket": "coverage_gap", "table": physical,
                    "column": name,
                    "detail": "physical column with no business meaning "
                              "on record"})

            # column type: BQ wins execution-facing (D3 on disagreement)
            column.data_type = bq_type or mdm_type or atlas_type
            column.type_source = ("bq" if bq_type else
                                  "lumi" if mdm_type else "atlas")
            catalog_types = {t for t in (mdm_type, atlas_type) if t}
            if bq_type and catalog_types and \
                    any(t != bq_type for t in catalog_types):
                consensus.structural["D3"] += 1
                column.flags.append("D3")
                consensus.tickets.append({
                    "ticket": "catalog_mismatch", "table": physical,
                    "column": name,
                    "detail": f"bq={bq_type} vs catalog="
                              f"{sorted(catalog_types)}"})
            column.agreement_count = 1 + sum(
                1 for t in (mdm_type, atlas_type) if t == bq_type)

            # description: Atlas display-first, Lumi kept (D4 divergence)
            atlas_desc = str(props.get("description_atlas") or "")
            mdm_desc = str(props.get("description_mdm") or "")
            column.description = atlas_desc or mdm_desc
            column.description_source = ("atlas" if atlas_desc else
                                         "lumi" if mdm_desc else "")
            if atlas_desc and mdm_desc and atlas_desc != mdm_desc:
                consensus.structural["D4"] += 1
                column.flags.append("D4")
                column.description_supplementary = mdm_desc
            column.business_name = str(
                props.get("business_name")
                or props.get("business_name_atlas") or "")

            # sensitivity: union-most-restrictive (D5 on disagreement)
            signals = {
                "lumi": bool(props.get("is_pii_mdm")),
                "atlas": bool(props.get("pii_role_id")
                              or props.get("sde_group")),
            }
            flagged = [src for src, hit in signals.items() if hit]
            column.sensitive = bool(flagged)
            column.sensitivity_sources = flagged
            if len(flagged) == 1:
                # one witness flags while the other is silent → D5:
                # most-restrictive applies immediately, ticket stays open
                consensus.structural["D5"] += 1
                column.flags.append("D5")
                consensus.tickets.append({
                    "ticket": "sensitivity_conflict",
                    "table": physical, "column": name,
                    "detail": f"flagged by {flagged[0]} only — "
                              "most-restrictive applied while open"})
            consensus.columns[name] = column
        out[tid] = consensus
    return out


def structural_census(consensus: dict[str, TableConsensus]
                      ) -> dict[str, Any]:
    per_table = {c.physical: dict(c.structural)
                 for c in consensus.values()}
    totals = {f"D{i}": sum(t[f"D{i}"] for t in per_table.values())
              for i in range(1, 6)}
    return {"per_table": per_table, "totals": totals,
            "handlers": {"D1": "catalog_stale (column omitted)",
                         "D2": "coverage_gap (ungoverned, disclosed)",
                         "D3": "catalog_mismatch (bq wins)",
                         "D4": "description divergence (atlas first)",
                         "D5": "sensitivity_conflict (most restrictive)"}}


def build_acl(graph: GraphDir,
              consensus: dict[str, TableConsensus]) -> dict[str, Any]:
    """E3 — fail closed on UNKNOWN. Any table whose row-access policy is
    UNKNOWN (DENIED/503) is marked restricted; PII columns listed from
    the most-restrictive consensus."""
    acl: dict[str, Any] = {}
    edges = graph.fold_edges()
    for (s, r, o, _w), quad in edges.items():
        if r != "has_policy" or not s.startswith("table:"):
            continue
        physical = s.split(":", 1)[1]
        entry = acl.setdefault(physical, {"restricted": None,
                                          "pii_columns": []})
        if o == "policy:unknown_denied":
            entry["restricted"] = "unknown_policy"
        elif o.startswith("policy:row_access"):
            entry["restricted"] = entry["restricted"] or "row_access"
    for tid, table in consensus.items():
        physical = table.physical
        entry = acl.setdefault(physical, {"restricted": None,
                                          "pii_columns": []})
        entry["pii_columns"] = sorted(
            c.name for c in table.columns.values() if c.sensitive)
    return acl
