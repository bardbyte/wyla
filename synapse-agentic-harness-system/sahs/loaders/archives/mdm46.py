"""MDM archive loader — the real `mdm_46_patched_v2/` layout.

Reads run_manifest / coverage / table_summaries plus per-table
`tables/<t>/summary.json` and `responses/*.json` (schema, ownership,
pipeline, lifecycle, lineage_up, attr_lineage), and emits quads under the
`lumi` source.

Status semantics carried through: a coverage entry of HTTP_503 / DENIED
means UNKNOWN, never absence — lifecycle unknowable becomes an explicit
prop (`lifecycle_status: "unknown_unavailable"`), and downstream trust
logic fails closed on it rather than assuming health. Table subjects
resolve through the E1 crosswalk (lumi asset id first, unique short name
second) or the load BLOCKS.
"""

from __future__ import annotations

import json
from pathlib import Path

from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import col_id, table_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad

SOURCE = "lumi"


def _json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_mdm_archive(root: Path, graph: GraphDir, crosswalk: Crosswalk,
                     run_id: str, ledger=None) -> tuple[dict, list[str]]:
    root = Path(root)

    def track(path: Path):
        if ledger is not None:
            ledger.consumed(path)
        return path

    blocking: list[str] = []
    report = {"tables": 0, "columns": 0, "lineage_edges": 0,
              "attr_lineage_edges": 0, "lifecycle_unknown": 0}
    coverage = _json(track(root / "coverage.json")) or {}
    summaries = {t.get("table_name"): t for t in
                 (_json(track(root / "table_summaries.json")) or {}).get(
                     "tables", [])}

    def prov(**kw) -> Prov:
        return Prov(source=SOURCE, run=run_id, **kw)

    for d in sorted((root / "tables").iterdir()):
        if not d.is_dir():
            continue
        name = d.name
        summary = _json(track(d / "summary.json")) or {}
        asset_ids = (summary.get("discovery") or {}).get("dataset_ids", [])
        physical = crosswalk.physical_for_lumi(
            name, asset_ids[0] if asset_ids else "")
        if physical is None:
            blocking.append(f"crosswalk: no row for lumi table {name}")
            continue
        tid = table_id(physical)
        responses = d / "responses"
        rel_ev = f"tables/{name}/responses"

        ownership = _json(track(responses / "ownership.json")) or {}
        pipeline = _json(track(responses / "pipeline.json")) or {}
        lifecycle = _json(track(responses / "lifecycle.json"))
        table_coverage = coverage.get(name, {})
        lifecycle_status = None
        if lifecycle is not None:
            lifecycle_status = str(lifecycle.get("status") or "")
        elif str(table_coverage.get("lifecycle", "")).upper() in (
                "HTTP_503", "DENIED", "ERROR"):
            lifecycle_status = "unknown_unavailable"
            report["lifecycle_unknown"] += 1

        graph.append_node(NodeRecord(
            id=tid,
            props={
                "business_unit": pipeline.get("business_unit", ""),
                "pipeline_name": pipeline.get("pipeline_name", ""),
                "feed_type": pipeline.get("feed_type", ""),
                "source_system": pipeline.get("source_system", ""),
                "lifecycle_status": lifecycle_status,
                "environment": (lifecycle or {}).get("environment", ""),
                "answerability": summaries.get(name, {}).get(
                    "answerability", {}),
            },
            prov=prov(evidence=f"tables/{name}/summary.json")))
        report["tables"] += 1

        for key in ("business_owner", "tech_owner"):
            owner = str(ownership.get(key) or "").strip().lower()
            if owner:
                graph.append_node(NodeRecord(
                    id=f"owner:{owner}", props={"role": key},
                    prov=prov(evidence=f"{rel_ev}/ownership.json")))
                graph.append_edge(Quad(
                    s=tid, r="owned_by", o=f"owner:{owner}",
                    prov=prov(evidence=f"{rel_ev}/ownership.json")))

        schema = _json(track(responses / "schema.json")) or {}
        for column in schema.get("columns", []):
            cname = str(column.get("name") or "").strip()
            if not cname:
                continue
            cid = col_id(physical, cname)
            graph.append_node(NodeRecord(
                id=cid,
                props={
                    "data_type_mdm": column.get("data_type", ""),
                    "business_name": column.get("business_name", ""),
                    "description_mdm": column.get("description", ""),
                    "is_pii_mdm": bool(column.get("is_pii")),
                },
                prov=prov(evidence=f"{rel_ev}/schema.json")))
            graph.append_edge(Quad(
                s=tid, r="has_column", o=cid,
                prov=prov(evidence=f"{rel_ev}/schema.json")))
            report["columns"] += 1
            if column.get("is_pii"):
                graph.append_edge(Quad(
                    s=cid, r="has_policy", o="policy:pii",
                    prov=prov(evidence=f"{rel_ev}/schema.json")))

        def _stub_table(stub_physical: str, evidence: str) -> str:
            stub = table_id(stub_physical)
            graph.append_node(NodeRecord(
                id=stub, props={"stub": True},
                prov=prov(evidence=evidence)))
            return stub

        for up in _json(track(responses / "lineage_up.json")) or []:
            up_physical = crosswalk.physical_for_lumi(
                str(up.get("source_table") or ""))
            if up_physical is None:
                continue                # upstream outside the 46-table scope
            graph.append_edge(Quad(
                s=_stub_table(up_physical, f"{rel_ev}/lineage_up.json"),
                r="upstream_of", o=tid,
                prov=prov(evidence=f"{rel_ev}/lineage_up.json")))
            report["lineage_edges"] += 1

        for row in _json(track(responses / "attr_lineage.json")) or []:
            src_physical = crosswalk.physical_for_lumi(
                str(row.get("source_table") or ""))
            target_col = str(row.get("target_column") or "")
            if src_physical is None or not target_col:
                continue
            source_col = col_id(src_physical,
                                str(row.get("source_column") or "?"))
            graph.append_node(NodeRecord(       # lineage endpoint stub
                id=source_col, props={"stub": True},
                prov=prov(evidence=f"{rel_ev}/attr_lineage.json")))
            graph.append_edge(Quad(
                s=col_id(physical, target_col), r="derived_from",
                o=source_col,
                props={"derivation_logic":
                       row.get("derivation_logic", "")},
                prov=prov(evidence=f"{rel_ev}/attr_lineage.json")))
            report["attr_lineage_edges"] += 1
    return report, blocking
