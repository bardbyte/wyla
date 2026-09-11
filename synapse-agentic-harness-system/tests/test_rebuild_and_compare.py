"""rebuild_and_compare.py — freeze, rebuild, compile, diff, in one go.

Proven the way it will be used: a graph built from an archive missing
one table, then the script run with the full archive. The diff must
name exactly that table as added, the gates must read clean, and the
build compare must not call the addition a regression."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from scripts.rebuild_and_compare import diff_graph, graph_state  # noqa: E402

FX = SILO / "tests" / "fixtures"


def _without(src: Path, dst: Path, needle: str) -> Path:
    """Copy a tree, dropping every file/dir whose name carries the
    needle and every JSONL row whose text does — the state of the
    world BEFORE a table is onboarded: no archive dir, no catalog
    entry, no crosswalk row, no steward row."""
    shutil.copytree(src, dst)
    for path in sorted(dst.rglob("*"), reverse=True):
        if needle in path.name:
            shutil.rmtree(path) if path.is_dir() else path.unlink()
        elif path.suffix == ".jsonl":
            lines = [x for x in path.read_text(encoding="utf-8")
                     .splitlines() if x.strip() and needle not in x]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return dst


def test_adding_a_table_shows_up_as_exactly_that(tmp_path: Path):
    # the world before: the auth table is nowhere — not in the
    # archive, not in the catalog, not in the crosswalk or steward map
    archive45 = _without(FX / "real_extractions_production",
                         tmp_path / "archive_45", "wwcas")
    identity45 = _without(FX / "identity", tmp_path / "identity_45",
                          "wwcas")
    sources45 = _without(FX / "sources", tmp_path / "sources_45", "wwcas")
    # the MDM archive too: an archive dir with no crosswalk row does
    # not skip, it BLOCKS the build (gate crosswalk_resolution) —
    # which is the right answer for a real delivery, and why the
    # crosswalk rows come first in the onboarding runbook
    mdm45 = _without(FX / "mdm_46_patched_v2", tmp_path / "mdm_45",
                     "wwcas")
    graph, builds = tmp_path / "graph", tmp_path / "builds"
    r1 = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "rebuild_and_compare.py"),
         "--graph", str(graph), "--builds", str(builds),
         "--crosswalk", str(identity45 / "crosswalk.jsonl"),
         "--sources-dir", str(sources45),
         # the registry INSIDE the sources root: passing the original
         # path left the copy unread, and the ledger gate flagged it —
         # one file on the dock with no label, as designed
         "--registry", str(sources45 / "tables_registry.txt"),
         "--mdm-archive", str(mdm45),
         "--bq-archive", str(archive45), "--run-id", "r45"],
        capture_output=True, text=True, cwd=SILO)
    assert r1.returncode == 0, r1.stdout[-2500:] + r1.stderr[-800:]
    assert "no promoted build before this run" in r1.stdout
    state1 = graph_state(graph)
    assert "dw.wwcas_authorization" not in state1["tables"]

    # the delivery: rows authored, files dropped, the FULL set appended
    # into the SAME graph
    r2 = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "rebuild_and_compare.py"),
         "--graph", str(graph), "--builds", str(builds),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--run-id", "r46"],
        capture_output=True, text=True, cwd=SILO)
    assert r2.returncode == 0, r2.stdout[-2500:] + r2.stderr[-800:]
    out = graph / "runs" / "r46"
    gdiff = json.loads((out / "graph_diff.json").read_text())
    assert gdiff["tables_added"] == ["dw.wwcas_authorization"]
    assert gdiff["tables_removed"] == []
    assert gdiff["nodes"].get("table") == 1
    assert gdiff["nodes"].get("col", 0) > 0
    # its steward membership is new too, with its role
    assert gdiff["memberships_added"] == {
        "gmns": {"dw.wwcas_authorization": "home"}}
    gates = json.loads((out / "gates.json").read_text())
    assert gates["std_tech.skipped_unresolvable_table"] == 0
    assert gates["compile.coverage_unaccounted"] == 0
    # the synonym index names a table outside the crosswalk on purpose
    # (fixture) — reported, never gated
    assert gates["value_synonyms.skipped_unresolvable_table"] == 1
    assert "lob_map.shared_without_home" not in gates
    assert "tables added (1): dw.wwcas_authorization" in r2.stdout
    assert "verdict: CLEAN" in r2.stdout
    for name in ("before_graph.json", "before_build.json",
                 "after_graph.json", "after_build.json"):
        assert (out / name).exists(), name
    # the graph-grain diff is a pure function of two states
    assert diff_graph(state1, state1)["tables_added"] == []
