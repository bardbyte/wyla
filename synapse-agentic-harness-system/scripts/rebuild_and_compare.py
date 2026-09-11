#!/usr/bin/env python
"""rebuild_and_compare — one command: freeze, rebuild, compile, diff.

The graph is an append-only log and the build id is the graph hash, so
"what did this run change?" is only answerable if you froze the state
BEFORE the run. This script does that at two grains and then runs the
sequence the runbook prescribes:

    graph state  (nodes by kind · edges by relation and witness ·
                  tables · steward memberships with roles)   → before
    build state  (scripts/build_snapshot.py)                  → before
    build-graph  (append into the existing graph)
    compile
    graph state · build state                                 → after
    diff both, print the verdict, exit 2 on any regression or
    any gate counter that must read zero and does not

Every JSON it writes lands under --out-dir so the run is reviewable
later. Typical use, onboarding a delivery that arrived as suffixed
roots (the archive loader walks ONE root; the catalog loader reads ONE
directory — see docs/runbooks/onboarding_tables.md §1):

    python scripts/rebuild_and_compare.py \\
      --graph graph --builds builds \\
      --crosswalk graph/identity/crosswalk.jsonl \\
      --bq-archive $DATA/real_extractions_production_patched_12 \\
      --sources-dir sources --registry $BQ/_batch_summary.csv \\
      --no-jobs-30d --run-id onboard_cfr_12
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.graph.quads import GraphDir                     # noqa: E402
from scripts.build_snapshot import (                      # noqa: E402
    compare as compare_builds,
    resolve_build,
    snapshot as snapshot_build,
)

SCHEMA = "meridian.graph_state/1"


# ── graph grain ──────────────────────────────────────────────────
def graph_state(graph_dir: Path) -> dict[str, Any]:
    """What the folded graph holds, counted. Deterministic; reads the
    log only, never a build."""
    graph_dir = Path(graph_dir)
    if not (graph_dir / "nodes").exists():
        return {"schema": SCHEMA, "present": False}
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    by_kind: Counter = Counter(n.split(":", 1)[0] for n in nodes)
    by_relation: Counter = Counter(r for (_s, r, _o, _w) in edges)
    by_rel_witness: Counter = Counter(
        f"{r}@{w or 'unknown'}" for (_s, r, _o, w) in edges)
    tables = sorted(n.split(":", 1)[1] for n in nodes
                    if n.startswith("table:"))
    # steward memberships with roles, per unit
    memberships: dict[str, dict[str, str]] = defaultdict(dict)
    for (s, r, o, w), quad in edges.items():
        if r == "in_lob" and s.startswith("table:") and w == "steward":
            memberships[o.split(":", 1)[1]][s.split(":", 1)[1]] = str(
                quad.props.get("role") or "home")
    return {
        "schema": SCHEMA, "present": True,
        "nodes": dict(sorted(by_kind.items())),
        "edges": dict(sorted(by_relation.items())),
        "edges_by_witness": dict(sorted(by_rel_witness.items())),
        "n_nodes": len(nodes), "n_edges": len(edges),
        "tables": tables,
        "memberships": {k: dict(sorted(v.items()))
                        for k, v in sorted(memberships.items())},
    }


def diff_graph(before: dict[str, Any], after: dict[str, Any]
               ) -> dict[str, Any]:
    def delta(key: str) -> dict[str, int]:
        b, a = before.get(key, {}) or {}, after.get(key, {}) or {}
        return {k: a.get(k, 0) - b.get(k, 0)
                for k in sorted(set(b) | set(a))
                if a.get(k, 0) != b.get(k, 0)}
    bt, at = set(before.get("tables", [])), set(after.get("tables", []))
    bm, am = before.get("memberships", {}), after.get("memberships", {})
    new_memberships = {
        unit: {t: role for t, role in tables.items()
               if bm.get(unit, {}).get(t) != role}
        for unit, tables in am.items()}
    new_memberships = {u: m for u, m in new_memberships.items() if m}
    return {
        "tables_added": sorted(at - bt),
        "tables_removed": sorted(bt - at),
        "nodes": delta("nodes"), "edges": delta("edges"),
        "edges_by_witness": delta("edges_by_witness"),
        "memberships_added": new_memberships,
    }


# ── the run ──────────────────────────────────────────────────────
def _run(cmd: list[str], label: str) -> int:
    print(f"\n▶ {label}\n  $ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=SILO)
    return result.returncode


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _gate_counters(run_manifest: Any, build_manifest: Any
                   ) -> dict[str, Any]:
    """The numbers that must read zero (or a known value) after an
    onboarding run — pulled defensively from whichever report carries
    them, absent when a report is absent."""
    out: dict[str, Any] = {}
    reports = (run_manifest or {}).get("reports", {}) or {}
    for source in ("std_tech", "value_synonyms", "bq"):
        rep = reports.get(source)
        if isinstance(rep, dict):
            # a loader's report counts only what happened; a counter
            # never incremented is absent. Absent here means ZERO
            # skips, and the reader should see the zero
            out[f"{source}.skipped_unresolvable_table"] = int(
                rep.get("skipped_unresolvable_table", 0) or 0)
    lob = reports.get("lob_map") or {}
    for key in ("memberships", "shared_memberships",
                "shared_without_home", "multiple_homes"):
        if key in lob:
            out[f"lob_map.{key}"] = lob[key]
    util = (run_manifest or {}).get("utilization")
    if isinstance(util, list):
        out["ledger.inventoried"] = sum(
            1 for r in util if r.get("status") == "inventoried")
    elif isinstance(util, dict):
        for key in ("inventoried", "n_inventoried"):
            if key in util:
                out["ledger.inventoried"] = util[key]
    counts = (build_manifest or {}).get("counts", {}) or {}
    if "coverage_unaccounted" in counts:
        out["compile.coverage_unaccounted"] = counts["coverage_unaccounted"]
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--graph", required=True, type=Path)
    ap.add_argument("--builds", required=True, type=Path)
    ap.add_argument("--crosswalk", required=True, type=Path)
    ap.add_argument("--bq-archive", required=True, type=Path)
    ap.add_argument("--mdm-archive", type=Path)
    ap.add_argument("--sources-dir", required=True, type=Path)
    ap.add_argument("--registry", type=Path)
    ap.add_argument("--no-jobs-30d", action="store_true")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", type=Path,
                    help="where the before/after JSON and the verdict "
                         "land (default graph/runs/<run-id>)")
    args = ap.parse_args(argv)
    out_dir = args.out_dir or (args.graph / "runs" / args.run_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 0. freeze
    before_graph = graph_state(args.graph)
    before_build = None
    if (args.builds / "CURRENT").exists():
        before_build = snapshot_build(resolve_build(args.builds))
    (out_dir / "before_graph.json").write_text(
        json.dumps(before_graph, indent=1), encoding="utf-8")
    if before_build:
        (out_dir / "before_build.json").write_text(
            json.dumps(before_build, indent=1), encoding="utf-8")
    print(f"frozen: {before_graph.get('n_nodes', 0)} nodes · "
          f"{before_graph.get('n_edges', 0)} edges · "
          f"{len(before_graph.get('tables', []))} tables · build "
          f"{(before_build or {}).get('build_id', 'none')}")

    # 1. build-graph (append)
    cmd = [sys.executable, str(SILO / "scripts" / "laptop.py"),
           "build-graph", "--graph", str(args.graph),
           "--crosswalk", str(args.crosswalk),
           "--bq-archive", str(args.bq_archive),
           "--sources-dir", str(args.sources_dir),
           "--out", str(out_dir / "build_graph"), "--plain",
           "--run-id", args.run_id]
    if args.mdm_archive:
        cmd += ["--mdm-archive", str(args.mdm_archive)]
    if args.registry:
        cmd += ["--registry", str(args.registry)]
    if args.no_jobs_30d:
        cmd.append("--no-jobs-30d")
    rc = _run(cmd, "build-graph (append into the existing graph)")
    if rc != 0:
        print(f"\nbuild-graph exited {rc} — read "
              f"{out_dir / 'build_graph'} before compiling. Nothing "
              "was compiled.")
        return rc

    # 2. compile
    rc = _run([sys.executable, str(SILO / "scripts" / "laptop.py"),
               "compile", "--graph", str(args.graph),
               "--builds", str(args.builds),
               "--out", str(out_dir / "compile"), "--plain"], "compile")
    if rc != 0:
        print(f"\ncompile exited {rc} — the graph was appended to; "
              "the build was not promoted.")
        return rc

    # 3. after + diff
    after_graph = graph_state(args.graph)
    after_build = snapshot_build(resolve_build(args.builds))
    (out_dir / "after_graph.json").write_text(
        json.dumps(after_graph, indent=1), encoding="utf-8")
    (out_dir / "after_build.json").write_text(
        json.dumps(after_build, indent=1), encoding="utf-8")
    gdiff = diff_graph(before_graph, after_graph)
    (out_dir / "graph_diff.json").write_text(
        json.dumps(gdiff, indent=1), encoding="utf-8")

    print("\n═══ GRAPH: what this run added ═══")
    print(f"tables added ({len(gdiff['tables_added'])}): "
          + (", ".join(gdiff["tables_added"]) or "none"))
    if gdiff["tables_removed"]:
        print("tables REMOVED (a log cannot remove — investigate): "
              + ", ".join(gdiff["tables_removed"]))
    print("nodes by kind: " + (", ".join(
        f"{k} {v:+d}" for k, v in gdiff["nodes"].items()) or "no change"))
    print("edges by relation: " + (", ".join(
        f"{k} {v:+d}" for k, v in gdiff["edges"].items()) or "no change"))
    for unit, members in gdiff["memberships_added"].items():
        print(f"memberships {unit}: " + ", ".join(
            f"{t} ({role})" for t, role in sorted(members.items())))

    run_manifest = _read_json(args.graph / "runs" / args.run_id
                              / "manifest.json")
    build_manifest = _read_json(resolve_build(args.builds)
                                / "manifest.json")
    gates = _gate_counters(run_manifest, build_manifest)
    print("\n═══ GATES ═══")
    bad = []
    for key, value in gates.items():
        # a catalog entry or an archive dir for a table that does not
        # resolve is an onboarding failure. The synonym index is a
        # warehouse-wide file that names tables the graph may not
        # hold; its skips are reported, never gated
        must_zero = (key in ("std_tech.skipped_unresolvable_table",
                             "bq.skipped_unresolvable_table")
                     or key.endswith(("inventoried",
                                      "coverage_unaccounted")))
        named = key.endswith(("shared_without_home", "multiple_homes"))
        flag = ""
        if must_zero and value:
            flag = "  ← must be 0"
            bad.append(key)
        elif named and value:
            flag = "  ← the steward decides"
            bad.append(key)
        print(f"  {key:<42} {value}{flag}")
    (out_dir / "gates.json").write_text(
        json.dumps(gates, indent=1), encoding="utf-8")

    rc = 0
    if before_build:
        print("\n═══ BUILD: before → after ═══")
        rc = compare_builds(before_build, after_build)
    else:
        print("\n(no promoted build before this run — nothing to compare "
              "the build against; the graph diff above is the record)")
    if bad:
        print("\n⚠ gates: " + ", ".join(bad))
        rc = 2
    print(f"\nverdict: {'CLEAN' if rc == 0 else 'READ THE FLAGS ABOVE'} · "
          f"artifacts in {out_dir}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
