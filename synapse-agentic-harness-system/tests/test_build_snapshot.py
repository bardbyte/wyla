"""build_snapshot.py — the before/after instrument over a real build.

The snapshot reads only the build directory and must work on ANY build
shape: a build that predates the facts row / coverage ledger reports
them absent, never zero. The compare must never call a regression an
improvement — a lost fact on any table exits non-zero."""

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
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
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


def test_snapshot_reads_the_real_build_and_compare_is_honest(tmp_path):
    build_dir = _compiled(tmp_path)
    snap = snapshot(resolve_build(tmp_path / "builds"))
    assert snap["build_id"] == build_dir.name
    s = snap["scalars"]
    assert s["tables"] == 3 and s["lob_cards"] == 3
    assert s["coverage_unaccounted"] == 0
    assert s["tables_with_facts_row"] == 3
    assert s["card_fact_coverage_pct"] > 50
    gms = snap["per_table"]["dw.gms_transaction"]
    assert {"primary_key", "declared_fk", "answerability",
            "owner_chain", "vocabulary"} <= set(gms["facts_present"])
    assert gms["column_facts"]["column_domains"] >= 1

    # an OLD-shaped build (no facts row, no coverage) must report
    # absence, never zero — strip the new indexes and re-snapshot
    old = tmp_path / "old"
    old.mkdir()
    for name in ("manifest.json", "census.json"):
        (old / name).write_text((build_dir / name).read_text())
    (old / "indexes").mkdir()
    (old / "indexes" / "metrics.jsonl").write_text(
        (build_dir / "indexes" / "metrics.jsonl").read_text())
    (old / "cards" / "tables").mkdir(parents=True)
    # the pre-G3 card: header + columns + conflicts only
    (old / "cards" / "tables" / "dw__gms_transaction.md").write_text(
        "# table dw.gms_transaction\n- owner: own_a@corp · business "
        "unit: GMNS · layer: SOR [prov:x]\n- purpose: spine [prov:atlas]\n"
        "## columns\n- cm13 string [prov:bq·agree=1]\n## conflicts\n"
        "- none\n")
    before = snapshot(old)
    assert before["scalars"]["coverage_unaccounted"] is None
    assert before["scalars"]["facts_family_density_pct"] is None
    assert before["scalars"]["tables_with_facts_row"] == 0
    assert "primary_key" not in before["per_table"][
        "dw.gms_transaction"]["facts_present"]

    # before → after is BETTER, and every headline fact moved up
    assert compare(before, snap) == 0
    assert snap["card_facts"]["primary_key"] > \
        before["card_facts"]["primary_key"]
    # a regression is never called an improvement: after → before
    # loses facts on gms and exits non-zero
    assert compare(snap, before) == 2
    assert set(CARD_FACTS) >= set(snap["card_facts"])


def test_cli_round_trip(tmp_path):
    _compiled(tmp_path)
    out = tmp_path / "snap.json"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "build_snapshot.py"),
         "snapshot", "--builds", str(tmp_path / "builds"),
         "--out", str(out), "--quiet"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-400:]
    payload = json.loads(out.read_text())
    assert payload["schema"] == "meridian.build_snapshot/1"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "build_snapshot.py"),
         "compare", str(out), str(out)],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0
    assert "0 worse" in result.stdout
