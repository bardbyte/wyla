#!/usr/bin/env python3
"""One-trigger pipeline: sources → assertions → compiled graph snapshot.

The consolidation point the repo has been converging on — every witness
lands in one canonical sources dir, one builder compiles the graph, one
snapshot serves every consumer (MCP server, ADK agent, Streamlit UI).

    graph is COMPILED, not written:
      loaders emit canonical artifacts (per-source witness statements)
      → build_graph_from_sources() fuses them with per-fact provenance
      → snapshot JSON + assertion log for audit/replay

Offline demo (no network, no credentials — uses committed fixtures):

    python synapse/scripts/pipeline.py --demo

Real run (any subset; missing sources are skipped, not fatal):

    python synapse/scripts/pipeline.py \
        --skills-dir ~/Downloads/skills \
        --gold-sql-dir lumi_final/data/gold_queries \
        --bq-extract-dir ~/synapse_bq_outputs \
        --lumi-session lumi_final/data/session1_output.json \
        --mdm-cache-dir lumi_final/data/mdm_cache

Then serve it:

    SYNAPSE_GRAPH_PATH=synapse/data/cache/graph_snapshot.json \
        python -m synapse.mcp.server
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.graph.builder import build_graph_from_sources  # noqa: E402
from synapse.loaders.gold_sql_loader import load_gold_sql_corpus  # noqa: E402
from synapse.loaders.skills_loader import load_skills_library  # noqa: E402


def _stage(msg: str) -> None:
    print(f"\n\033[1;36m═══ {msg} ═══\033[0m")


def _note(msg: str) -> None:
    print(f"  {msg}")


def run_pipeline(args: argparse.Namespace) -> Path:
    sources_dir = Path(args.sources_dir).expanduser()
    sources_dir.mkdir(parents=True, exist_ok=True)
    run_report: dict[str, dict] = {}

    # ── 1. Skills library (semantic witness) ─────────────────
    skills_dir = args.skills_dir
    if args.demo and not skills_dir:
        skills_dir = str(SYNAPSE_ROOT / "tests" / "fixtures" / "skills_library")
    if skills_dir:
        _stage("Skills library → canonical artifacts")
        result = load_skills_library(Path(skills_dir), out_dir=sources_dir)
        run_report["skills"] = result.model_dump(mode="json")
        _note(f"{result.status}: {result.records_count} skill package(s)")
        for warning in result.warnings:
            _note(f"⚠ {warning}")

    # ── 2. Gold SQL corpus (behavioral witness) ──────────────
    gold_dir = args.gold_sql_dir
    if args.demo and not gold_dir:
        candidate = REPO_ROOT / "lumi_final" / "data" / "gold_queries"
        gold_dir = str(candidate) if candidate.is_dir() else ""
    if gold_dir:
        _stage("Gold SQL corpus → sqlglot signals")
        result = load_gold_sql_corpus(Path(gold_dir), out_dir=sources_dir)
        run_report["gold_sql"] = result.model_dump(mode="json")
        _note(f"{result.status}: {result.records_count} queries parsed, "
              f"{len(result.metadata.get('tables_discovered', []))} tables")
        for warning in result.warnings[:5]:
            _note(f"⚠ {warning}")

    # ── 3. BQ batch-extraction output (physical witness) ─────
    if args.bq_extract_dir:
        _stage("BigQuery extraction → canonical artifacts")
        from synapse.loaders.bq_loader import load_bq_for_table
        bq_root = Path(args.bq_extract_dir).expanduser()
        table_dirs = [d for d in sorted(bq_root.iterdir())
                      if d.is_dir() and not d.name.startswith("_")]
        outcomes = []
        for tdir in table_dirs:
            result = load_bq_for_table(
                tdir.name, source_dir=tdir, out_dir=sources_dir)
            outcomes.append({"table": tdir.name, "status": result.status})
        run_report["bq"] = {"tables": outcomes}
        _note(f"{len(outcomes)} table extraction folder(s) staged")

    # ── 4. Lumi fused output (governance + corpus witness) ───
    if args.lumi_session:
        _stage("Lumi session output → canonical artifacts")
        from synapse.loaders.lumi_loader import load_lumi_for_table
        session_path = Path(args.lumi_session).expanduser()
        blob = json.loads(session_path.read_text(encoding="utf-8"))
        outcomes = []
        for table_name in sorted(blob):
            result = load_lumi_for_table(
                table_name, lumi_path=session_path, out_dir=sources_dir)
            outcomes.append({"table": table_name, "status": result.status})
        run_report["lumi"] = {"tables": outcomes}
        _note(f"{len(outcomes)} table(s) split from session output")

    # ── 5. MDM cache (declared-metadata witness) ─────────────
    if args.mdm_cache_dir:
        _stage("MDM cache → canonical artifacts")
        from synapse.loaders.mdm_loader import load_mdm_for_table
        mdm_root = Path(args.mdm_cache_dir).expanduser()
        outcomes = []
        for cached in sorted(mdm_root.glob("*.json")):
            result = load_mdm_for_table(
                cached.stem.replace("__mdm_raw", ""),
                source_dir=mdm_root, out_dir=sources_dir, dry_run=False)
            outcomes.append({"table": cached.stem, "status": result.status})
        run_report["mdm"] = {"tables": outcomes}
        _note(f"{len(outcomes)} MDM digest(s) staged")

    # ── 6. Compile ───────────────────────────────────────────
    _stage("Compile: assertions → typed graph")
    store = build_graph_from_sources(sources_dir)
    stats = store.stats()
    _note(f"nodes: {stats['n_nodes']}  edges: {stats['n_edges']}")
    _note(f"by type: {stats['nodes_by_type']}")
    _note(f"by tier: {stats['nodes_by_confidence_tier']}")

    # ── 7. Snapshot + run manifest ───────────────────────────
    snapshot_path = Path(args.out).expanduser()
    store.save_json(snapshot_path)
    manifest_path = snapshot_path.with_name("run_manifest.json")
    manifest_path.write_text(json.dumps({
        "snapshot_version": store.snapshot_version,
        "sources_dir": str(sources_dir),
        "stats": stats,
        "loaders": run_report,
    }, indent=2, default=str), encoding="utf-8")
    _stage("Done")
    _note(f"snapshot: {snapshot_path}  (version {store.snapshot_version})")
    _note(f"manifest: {manifest_path}")
    _note("serve it:  SYNAPSE_GRAPH_PATH="
          f"{snapshot_path} python -m synapse.mcp.server")
    return snapshot_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compile the semantic graph from all available sources")
    parser.add_argument("--demo", action="store_true",
                        help="use committed fixtures (offline, no creds)")
    parser.add_argument("--skills-dir", default="",
                        help="skills library root (dirs with skill.yaml)")
    parser.add_argument("--gold-sql-dir", default="",
                        help="directory of gold *.sql files")
    parser.add_argument("--bq-extract-dir", default="",
                        help="bq_batch_extract.py output root")
    parser.add_argument("--lumi-session", default="",
                        help="lumi_final session1_output.json")
    parser.add_argument("--mdm-cache-dir", default="",
                        help="directory of cached raw MDM JSON")
    parser.add_argument("--sources-dir",
                        default=str(SYNAPSE_ROOT / "data" / "cache" / "sources"),
                        help="staging dir for canonical artifacts")
    parser.add_argument("--out",
                        default=str(SYNAPSE_ROOT / "data" / "cache"
                                    / "graph_snapshot.json"))
    args = parser.parse_args()
    if not any([args.demo, args.skills_dir, args.gold_sql_dir,
                args.bq_extract_dir, args.lumi_session, args.mdm_cache_dir]):
        parser.error("nothing to load — pass --demo or at least one source")
    run_pipeline(args)


if __name__ == "__main__":
    main()
