"""build_snapshot.py — the before/after instrument over a real build.

The snapshot reads only the build directory and must work on ANY build
shape. A build that carries no compiled facts row and no coverage
ledger reports them ABSENT, never zero — "we do not serve this" and
"nobody measured" are different answers. The compare must never call a
regression an improvement: a lost fact on any table exits non-zero."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from scripts.build_snapshot import (            # noqa: E402
    CARD_FACTS,
    compare,
    resolve_build,
    snapshot,
)

FX = SILO / "tests" / "fixtures"


def _compiled(tmp_path: Path) -> Path:
    graph_dir = tmp_path / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp_path / "run"), "--plain",
         "--run-id", "snap_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    build_dir, _m, failures = compile_build(graph_dir, tmp_path / "builds")
    assert not failures
    return Path(build_dir)


def test_snapshot_reads_the_real_build_and_reports_absence_honestly(
        tmp_path: Path):
    build_dir = _compiled(tmp_path)
    snap = snapshot(resolve_build(tmp_path / "builds"))
    assert snap["schema"] == "meridian.build_snapshot/1"
    assert snap["build_id"] == build_dir.name
    s = snap["scalars"]
    # what this build DOES serve, counted
    assert s["tables"] == 3 and s["lobs"] == 3
    assert s["metrics"] > 0 and s["joins"] > 0
    assert s["joins_declared"] >= 1
    assert s["cards_over_budget"] == 0
    # what it does NOT serve reads as ABSENT, never as a zero that
    # would look like a measured emptiness
    assert s["coverage_unaccounted"] is None
    assert s["coverage_rendered"] is None
    assert s["facts_family_density_pct"] is None
    assert s["tables_with_facts_row"] == 0
    # the per-table fact matrix is read off the CARDS, so it works
    # whatever the build carries
    gms = snap["per_table"]["dw.gms_transaction"]
    assert {"purpose", "business_unit", "line_of_business"} <= set(
        gms["facts_present"])
    assert set(CARD_FACTS) >= set(snap["card_facts"])


def test_compare_never_calls_a_regression_an_improvement(tmp_path: Path):
    _compiled(tmp_path)
    snap = snapshot(resolve_build(tmp_path / "builds"))
    assert compare(snap, snap) == 0          # nothing changed
    # a build that lost a fact on one table is a regression
    worse = json.loads(json.dumps(snap))
    dropped = worse["per_table"]["dw.gms_transaction"]["facts_present"].pop()
    worse["card_facts"][dropped] -= 1
    assert compare(snap, worse) == 2
    assert compare(worse, snap) == 0         # regaining it is better
    # and a regression is NOT offset by an improvement elsewhere: more
    # metrics does not buy back a fact a table stopped carrying
    mixed = json.loads(json.dumps(worse))
    mixed["scalars"]["metrics"] = snap["scalars"]["metrics"] + 99
    assert compare(snap, mixed) == 2


def test_cli_round_trip(tmp_path: Path):
    _compiled(tmp_path)
    out = tmp_path / "snap.json"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "build_snapshot.py"),
         "snapshot", "--builds", str(tmp_path / "builds"),
         "--out", str(out), "--quiet"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-400:]
    assert json.loads(out.read_text())["schema"] == \
        "meridian.build_snapshot/1"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "build_snapshot.py"),
         "compare", str(out), str(out)],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0
    assert "0 worse" in result.stdout
