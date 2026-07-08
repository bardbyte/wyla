#!/usr/bin/env python3
"""Graph probe — read-only build-quality report for a compiled snapshot.

Run this on the work laptop (where the REAL enriched graph lives, with the
MDM / BigQuery / usage / DQ witnesses fired) to ground a build-quality
assessment and compare the current synapse build against the original
semantic-graph build.

It reads *compiled snapshots* only — no BigQuery, no MDM, no creds, no
network, no writes. Pure standard library; it will optionally borrow the
real calibrator from ``synapse.graph.store`` when importable, and falls
back to a vendored copy so it also runs from a bare checkout.

    # auto-discover snapshots under the repo and report each (+ compare)
    python synapse/scripts/graph_probe.py

    # explicit snapshot(s) — 2+ triggers side-by-side comparison
    python synapse/scripts/graph_probe.py path/to/synapse_snapshot.json \
                                          path/to/semantic_graph_snapshot.json

    # deep-dive one table against the cardmember gold standard
    python synapse/scripts/graph_probe.py --table custins_customer_insights_cardmember

    # also drop the machine summary to a file
    python synapse/scripts/graph_probe.py --json /tmp/graph_probe.json

At the end it prints a compact ``PASTE THIS BACK`` JSON block — copy that
into the chat and I'll ground the comparison from real numbers.
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path
from typing import Any

# ── the real calibrator if we can reach it, else a vendored copy ──
# The `synapse` package sits one level up from this scripts/ dir; put it on
# the path so we stamp tiers with the SAME calibrator the build used.
_PKG_PARENT = Path(__file__).resolve().parents[1]
if str(_PKG_PARENT) not in sys.path:
    sys.path.insert(0, str(_PKG_PARENT))
try:  # keep the probe runnable even from a bare checkout
    from synapse.graph.store import SOURCE_WEIGHTS, confidence_from_sources
    _CALIBRATOR = "synapse.graph.store (real)"
except Exception:  # pragma: no cover - fallback path
    _CALIBRATOR = "vendored fallback"
    SOURCE_WEIGHTS = {
        "human_approval": 10, "skills": 7, "metric_catalog": 5, "glossary": 5,
        "bq": 4, "dq_engine": 4, "mdm": 3, "baseline_lookml": 3,
        "table_catalog": 3, "corpus": 1, "usage": 1, "llm_generated": 1,
    }

    def confidence_from_sources(sources, evidence_counts=None):
        counts = evidence_counts or {s: 1 for s in sources}
        distinct = set(sources)
        weighted = sum(
            SOURCE_WEIGHTS.get(s, 0) * min(max(counts.get(s, 1), 1), 5)
            for s in distinct
        )
        score = min(0.99, weighted / 15.0)
        n = len(distinct)
        if "human_approval" in distinct:
            return 0.99, "human_asserted"
        if n >= 4 or (n >= 3 and score >= 0.70) or score >= 0.90:
            return score, "grounded"
        if n >= 2 or score >= 0.45:
            return score, "inferred"
        return score, "guessed"

TIER_ORDER = ["human_asserted", "grounded", "inferred", "guessed", "deprecated"]
TIER_RANK = {t: i for i, t in enumerate(TIER_ORDER)}
# witnesses the real extraction stages that push a table toward grounded
HIGH_VALUE = ["mdm", "bq", "usage", "dq_engine", "glossary", "metric_catalog"]
# CTE aliases / template placeholders that SQL parsing mints as "tables"
_JUNK_NAMES = {
    "base", "the", "totals", "pivoted", "lagged", "segmented", "yoy",
    "rates", "with_shares", "components", "final_calc", "full_result",
    "base_data", "contribution_output", "contribution_a", "rate_mix_calc",
    "segment_rates", "recovery_cohort", "seg_step1", "seg_step2",
    "approval_pcn_lvl", "source_table", "prepared_table", "sbs_new_accts",
}


def _col_is_pii(col: dict) -> bool:
    """PII if the flag is set OR the taxonomy is anything but Internal —
    so it catches PII carried only under pii_taxonomy."""
    p = col.get("properties", {})
    if p.get("is_pii"):
        return True
    tax = (p.get("pii_taxonomy") or "").strip().lower()
    return bool(tax) and tax not in ("internal", "public", "none")


def _looks_junk(name: str, n_cols: int, witnesses: list) -> bool:
    """Heuristic: a CTE/placeholder minted as a table. Conservative — used
    to flag suspects on unscoped builds, never to delete."""
    n = name.lower()
    tail = n.rsplit(".", 1)[-1]
    if "your_project" in n or "your_dataset" in n:
        return True
    if tail in _JUNK_NAMES:
        return True
    # a bare single-word alias (no dots, no underscores → CTE-shaped, unlike
    # a real risk_pers_acct / axp-lumi.dw.x) with no columns and only soft
    # SQL witnesses. Conservative: named tables never match.
    return (n_cols == 0 and "." not in n and "_" not in n
            and set(witnesses) <= {"skills", "corpus", "llm_generated"})


# ── discovery ────────────────────────────────────────────────


def discover(repo: Path) -> list[Path]:
    """Find compiled snapshots under the repo (best-effort, deduped)."""
    seen: dict[Path, None] = {}
    likely = [
        repo / "synapse" / "data" / "cache" / "graph_snapshot.json",
        repo / "semantic-graph" / "data" / "cache" / "graph_snapshot.json",
    ]
    for p in likely:
        if p.exists():
            seen[p.resolve()] = None
    for p in repo.rglob("graph_snapshot*.json"):
        s = str(p)
        if "__pycache__" in s or "/sources/" in s or "node_modules" in s:
            continue
        seen[p.resolve()] = None
    return list(seen)


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


# ── analysis ─────────────────────────────────────────────────


def _prov(node: dict) -> dict:
    return node.get("provenance", {}) or {}


def analyze(snap: dict, path: Path) -> dict:
    """Reduce a snapshot to a structured build-quality summary."""
    nodes = snap.get("nodes", {})
    edges = snap.get("edges", {})

    by_type = collections.Counter(n["node_type"] for n in nodes.values())
    by_edge = collections.Counter(e["edge_type"] for e in edges.values())
    by_tier = collections.Counter(
        _prov(n).get("confidence_tier", "guessed") for n in nodes.values()
    )
    witness_footprint = collections.Counter()
    conflicts = 0
    for n in nodes.values():
        p = _prov(n)
        for s in p.get("sources", []):
            witness_footprint[s] += 1
        conflicts += len(p.get("conflicts", []))

    # columns grouped by owning table
    cols_by_table: dict[str, list] = collections.defaultdict(list)
    for n in nodes.values():
        if n["node_type"] == "Column":
            cols_by_table[n["properties"].get("table_name", "?")].append(n)

    # metrics / entities / dq attached per table (richness signals)
    metrics_by_table: dict[str, int] = collections.Counter()
    for n in nodes.values():
        if n["node_type"] == "Metric":
            t = n["properties"].get("sourced_from_table", "")
            if t:
                metrics_by_table[t] += 1

    tables = []
    for n in nodes.values():
        if n["node_type"] != "Table":
            continue
        name = n["properties"].get("table_name") or n["canonical_uri"].split("/")[-1]
        p = _prov(n)
        srcs = list(p.get("sources", []))
        tier = p.get("confidence_tier", "guessed")
        score = round(float(p.get("confidence_score", 0.0)), 2)
        cols = cols_by_table.get(name, [])
        col_tiers = collections.Counter(
            _prov(c).get("confidence_tier", "guessed") for c in cols
        )
        pii = sum(1 for c in cols if _col_is_pii(c))
        # projection: what tier if the high-value witnesses were also present?
        projected_srcs = sorted(set(srcs) | set(HIGH_VALUE))
        proj_score, proj_tier = confidence_from_sources(projected_srcs)
        missing = [w for w in HIGH_VALUE if w not in srcs]
        tables.append({
            "name": name,
            "witnesses": srcs,
            "n_witnesses": len(set(srcs)),
            "tier": tier,
            "score": score,
            "n_cols": len(cols),
            "col_tiers": {k: col_tiers[k] for k in TIER_ORDER if col_tiers.get(k)},
            "pii_cols": pii,
            "metrics": metrics_by_table.get(name, 0),
            "missing_high_value": missing,
            "to_grounded": max(0, 4 - len(set(srcs))) if tier not in ("grounded", "human_asserted") else 0,
            "projected_tier_if_extracted": proj_tier,
            "junk": _looks_junk(name, len(cols), srcs),
        })
    tables.sort(key=lambda t: (TIER_RANK.get(t["tier"], 9), -t["n_witnesses"]))

    # coverage + quality rollups across the real (non-junk) tables
    real = [t for t in tables if not t["junk"]]
    bq_tables = [t for t in real if "bq" in t["witnesses"]]
    all_cols = [c for n_ in nodes.values() if n_["node_type"] == "Column"
                for c in [n_]]
    col_grounded = sum(
        _prov(c).get("confidence_tier") in ("grounded", "human_asserted")
        for c in all_cols)
    junk_suspects = [t["name"] for t in tables if t["junk"]]

    n = len(nodes) or 1
    return {
        "path": str(path),
        "snapshot_version": snap.get("snapshot_version", "unversioned"),
        "n_nodes": len(nodes),
        "n_edges": len(edges),
        "node_types": dict(by_type),
        "edge_types": dict(by_edge),
        "tier_dist": {t: by_tier.get(t, 0) for t in TIER_ORDER if by_tier.get(t)},
        "pct_grounded_or_better": round(
            100 * (by_tier.get("grounded", 0) + by_tier.get("human_asserted", 0)) / n, 1
        ),
        "pct_columns_grounded": round(100 * col_grounded / (len(all_cols) or 1), 1),
        "bq_coverage": f"{len(bq_tables)}/{len(real)} real tables profiled",
        "junk_suspects": junk_suspects,
        "witness_footprint": dict(witness_footprint.most_common()),
        "conflicts": conflicts,
        "richness": {
            "entities": by_type.get("Entity", 0),
            "metrics": by_type.get("Metric", 0),
            "synonyms": by_type.get("Synonym", 0),
            "code_mappings": by_type.get("CodeMapping", 0),
            "filter_values": by_type.get("FilterValue", 0),
            "dq_rules": by_type.get("DataQualityRule", 0),
            "guardrails": by_type.get("Guardrail", 0),
            "skills": by_type.get("Skill", 0),
            "lineage_edges": by_edge.get("UPSTREAM_OF", 0),
        },
        "tables": tables,
    }


# ── cardmember gold-standard check (TARGET_GRAPH §15 essentials) ──


def cardmember_check(snap: dict, table: str) -> dict | None:
    nodes = snap.get("nodes", {})
    tnode = next(
        (n for n in nodes.values()
         if n["node_type"] == "Table"
         and (n["properties"].get("table_name") == table
              or n["canonical_uri"].endswith("/" + table))),
        None,
    )
    if tnode is None:
        return None
    tname = tnode["properties"].get("table_name", table)
    cols = [n for n in nodes.values()
            if n["node_type"] == "Column" and n["properties"].get("table_name") == tname]
    colnames = {c["properties"].get("name", "") for c in cols}
    metrics = [n for n in nodes.values()
               if n["node_type"] == "Metric" and n["properties"].get("sourced_from_table") == tname]
    entities = [n for n in nodes.values()
                if n["node_type"] == "Entity"
                and tname in n["properties"].get("materialized_in_tables", [])]
    tier = _prov(tnode).get("confidence_tier", "guessed")
    checks = {
        "table_tier": tier,
        "table_grounded": tier in ("grounded", "human_asserted"),
        "n_columns": len(cols),
        "has_account_key_cm11": any("cm11" in c for c in colnames),
        "has_customer_key_cust_xref_id": any("cust_xref" in c for c in colnames),
        "n_entities": len(entities),
        "n_metrics": len(metrics),
        "n_pii_cols": sum(1 for c in cols if c["properties"].get("is_pii")),
        "witnesses": list(_prov(tnode).get("sources", [])),
    }
    return checks


# ── rendering ────────────────────────────────────────────────


def bar(tier: str) -> str:
    return {"human_asserted": "██", "grounded": "█",
            "inferred": "▍", "guessed": "▏", "deprecated": "·"}.get(tier, "")


def render(a: dict) -> None:
    print("\n" + "=" * 78)
    print(f"SNAPSHOT  {a['path']}")
    print(f"          version={a['snapshot_version']}   "
          f"{a['n_nodes']} nodes · {a['n_edges']} edges   "
          f"calibrator={_CALIBRATOR}")
    print("=" * 78)
    print(f"  node types : {a['node_types']}")
    print(f"  tiers      : {a['tier_dist']}   "
          f"→ {a['pct_grounded_or_better']}% grounded+  "
          f"({a['pct_columns_grounded']}% of columns)")
    print(f"  bq coverage: {a['bq_coverage']}")
    if a["junk_suspects"]:
        shown = ", ".join(a["junk_suspects"][:8])
        more = f" (+{len(a['junk_suspects']) - 8} more)" if len(a["junk_suspects"]) > 8 else ""
        print(f"  junk?      : {len(a['junk_suspects'])} CTE/placeholder "
              f"suspects — {shown}{more}  [scope with a manifest to drop]")
    print(f"  witnesses  : " + ", ".join(
        f"{s}×{c}" for s, c in a["witness_footprint"].items()) or "(none)")
    r = a["richness"]
    print(f"  richness   : {r['entities']} entities · {r['metrics']} metrics · "
          f"{r['synonyms']} synonyms · {r['code_mappings']} code-maps · "
          f"{r['filter_values']} filters · {r['dq_rules']} DQ · "
          f"{r['guardrails']} guardrails · {r['lineage_edges']} lineage edges")
    if a["conflicts"]:
        print(f"  conflicts  : {a['conflicts']} unresolved source disagreements")

    print(f"\n  {'TABLE':<42} {'TIER':<14} SC   WIT  COLS  →grounded")
    print("  " + "-" * 74)
    for t in a["tables"]:
        proj = ("" if t["tier"] in ("grounded", "human_asserted")
                else f"  add {','.join(t['missing_high_value']) or '—'} → {t['projected_tier_if_extracted']}")
        print(f"  {bar(t['tier'])} {t['name'][:40]:<40} {t['tier']:<14} "
              f"{t['score']:<4} {t['n_witnesses']:<4} {t['n_cols']:<4}{proj}")


def render_compare(reports: list[dict]) -> None:
    print("\n" + "#" * 78)
    print("# SIDE BY SIDE")
    print("#" * 78)
    keys = [("nodes", "n_nodes"), ("edges", "n_edges"),
            ("% grounded+", "pct_grounded_or_better")]
    labels = [Path(r["path"]).parts[-4] if len(Path(r["path"]).parts) >= 4
              else r["path"] for r in reports]
    w = 24
    print(f"  {'metric':<18}" + "".join(f"{l[:w]:<{w}}" for l in labels))
    for name, k in keys:
        print(f"  {name:<18}" + "".join(f"{str(r[k]):<{w}}" for r in reports))
    # witness sets
    print(f"  {'witnesses':<18}" + "".join(
        f"{','.join(sorted(r['witness_footprint']))[:w-1]:<{w}}" for r in reports))
    # tables in common → tier delta
    tmaps = [{t["name"]: t for t in r["tables"]} for r in reports]
    shared = set(tmaps[0]) & set(tmaps[1]) if len(tmaps) >= 2 else set()
    if shared:
        print(f"\n  shared tables — tier in each build:")
        for name in sorted(shared):
            tiers = " | ".join(f"{tm[name]['tier']}" for tm in tmaps[:2])
            print(f"    {name[:44]:<44} {tiers}")


# ── main ─────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("snapshots", nargs="*", type=Path,
                    help="snapshot json path(s); omit to auto-discover")
    ap.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2],
                    help="repo root for auto-discovery (default: two levels up)")
    ap.add_argument("--table", default="custins_customer_insights_cardmember",
                    help="table to gold-standard-check (default: cardmember)")
    ap.add_argument("--json", type=Path, help="also write the machine summary here")
    args = ap.parse_args()

    paths = args.snapshots or discover(args.repo)
    if not paths:
        print("No snapshots found. Build one (synapse/scripts/pipeline.py) or pass "
              "a path. Looked under:", args.repo)
        return

    reports, machine = [], []
    for path in paths:
        if not path.exists():
            print(f"  ! missing: {path}")
            continue
        snap = load(path)
        a = analyze(snap, path)
        render(a)
        cm = cardmember_check(snap, args.table)
        if cm:
            ok = "GROUNDED" if cm["table_grounded"] else f"thin ({cm['table_tier']})"
            print(f"\n  ── gold-standard: {args.table} → {ok} ──")
            print(f"     {cm['n_columns']} cols · cm11={cm['has_account_key_cm11']} "
                  f"cust_xref_id={cm['has_customer_key_cust_xref_id']} · "
                  f"{cm['n_entities']} entities · {cm['n_metrics']} metrics · "
                  f"{cm['n_pii_cols']} PII · witnesses={cm['witnesses']}")
        reports.append(a)
        # compact per-snapshot machine record
        machine.append({
            "path": a["path"], "version": a["snapshot_version"],
            "n_nodes": a["n_nodes"], "n_edges": a["n_edges"],
            "node_types": a["node_types"], "tier_dist": a["tier_dist"],
            "pct_grounded_or_better": a["pct_grounded_or_better"],
            "pct_columns_grounded": a["pct_columns_grounded"],
            "bq_coverage": a["bq_coverage"], "junk_suspects": a["junk_suspects"],
            "witness_footprint": a["witness_footprint"],
            "richness": a["richness"], "conflicts": a["conflicts"],
            "tables": [{k: t[k] for k in ("name", "witnesses", "tier", "score",
                                          "n_cols", "col_tiers", "pii_cols",
                                          "metrics", "missing_high_value",
                                          "projected_tier_if_extracted")}
                       for t in a["tables"]],
            "cardmember_check": cm,
        })

    if len(reports) >= 2:
        render_compare(reports)

    summary = {"calibrator": _CALIBRATOR, "snapshots": machine}
    if args.json:
        args.json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"\n  machine summary → {args.json}")

    print("\n" + "=" * 78)
    print("PASTE THIS BACK  (compact JSON — copy from the line below to the end)")
    print("=" * 78)
    print(json.dumps(summary, separators=(",", ":")))


if __name__ == "__main__":
    main()
