"""P2 gate (compiler half): reconcile D1–D5, acl (E3), cards, indexes,
determinism, DIFF, CURRENT (E4)."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.compiler.compile import compile_build          # noqa: E402
from sahs.graph.clerk import set_status                  # noqa: E402
from sahs.graph.quads import GraphDir                    # noqa: E402

FX = SILO / "tests" / "fixtures"


def _build_graph(graph_dir: Path, out_dir: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "build-graph",
         "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(out_dir), "--plain", "--run-id", "test_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]


def _compiled(tmp_path: Path) -> tuple[Path, Path, dict]:
    graph_dir = tmp_path / "graph"
    _build_graph(graph_dir, tmp_path / "run")
    builds = tmp_path / "builds"
    build_dir, manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return graph_dir, build_dir, manifest


def test_reconcile_d1_to_d5_counts_and_handlers(tmp_path):
    _, build_dir, manifest = _compiled(tmp_path)
    census = json.loads((build_dir / "census.json").read_text())
    totals = census["structural"]["totals"]
    assert totals == {"D1": 1, "D2": 1, "D3": 1, "D4": 2, "D5": 1}
    tickets = [json.loads(x) for x in
               (build_dir / "tickets.jsonl").read_text().splitlines()]
    kinds = {t["ticket"] for t in tickets}
    assert kinds == {"catalog_stale", "coverage_gap", "catalog_mismatch",
                     "sensitivity_conflict"}
    gms_card = (build_dir / "cards" / "tables"
                / "dw__gms_transaction.md").read_text()
    assert "mdm_only_col" not in gms_card.split("## conflicts")[0].replace(
        "omitted catalog-only", "")  # D1 never renders as a column row
    assert "omitted catalog-only columns (D1): mdm_only_col" in gms_card
    assert "ungoverned — no business meaning on record" in gms_card  # D2
    assert "| lumi: Signed transaction amount" in gms_card           # D4


def test_acl_fails_closed_on_unknown_policy(tmp_path):
    _, build_dir, _ = _compiled(tmp_path)
    acl = json.loads((build_dir / "acl.json").read_text())
    assert acl["dw.wwcas_authorization"]["restricted"] == "unknown_policy"
    assert acl["dw.gms_transaction"]["restricted"] is None
    assert "cm13" in acl["dw.gms_transaction"]["pii_columns"]
    assert "card_no" in acl["dw.wwcas_authorization"]["pii_columns"]  # D5
    wwcas_card = (build_dir / "cards" / "tables"
                  / "dw__wwcas_authorization.md").read_text()
    assert "row-access policy UNKNOWN" in wwcas_card
    assert "live execution DENIED" in wwcas_card


def test_indexes_fts_and_rank_columns(tmp_path):
    _, build_dir, manifest = _compiled(tmp_path)
    db = sqlite3.connect(build_dir / "indexes" / "index.sqlite")
    if manifest["index"]["fts5"]:
        hits = db.execute(
            "SELECT text, ref FROM vocab WHERE vocab MATCH 'merchant'"
        ).fetchall()
        assert hits
    rows = db.execute(
        "SELECT label, authority, support FROM bindings "
        "WHERE label = 'consumer' ORDER BY authority DESC").fetchall()
    assert len(rows) == 2                 # the census conflict, indexed
    certified = db.execute(
        "SELECT label FROM metrics WHERE status = 'certified'").fetchall()
    assert certified
    # JSONL twins exist beside the sqlite
    for twin in ("vocab.jsonl", "bindings.jsonl", "metrics.jsonl"):
        assert (build_dir / "indexes" / twin).exists()


def test_metric_fusion_and_variant_lineage(tmp_path):
    _, build_dir, _ = _compiled(tmp_path)
    metrics = [json.loads(x) for x in
               (build_dir / "indexes" / "metrics.jsonl"
                ).read_text().splitlines()]
    spend = [m for m in metrics if m["label"] == "GMNS Merchant Spend"]
    assert len(spend) == 1                # dmp + mined fused into one row
    assert len(spend[0]["mgroups"]) >= 2
    assert spend[0]["status"] == "certified"
    variant_cards = [
        p for p in (build_dir / "cards" / "metrics").glob("*.md")
        if "variant of metric" in p.read_text()]
    assert variant_cards                  # off-meridian lineage rendered


def test_compile_deterministic_and_current_atomic(tmp_path):
    graph_dir, build_dir, manifest = _compiled(tmp_path)
    build_again, manifest_again, failures = compile_build(
        graph_dir, tmp_path / "builds2")
    assert not failures
    assert manifest_again["build_id"] == manifest["build_id"]
    assert (build_again / "manifest.json").read_bytes() == \
        (build_dir / "manifest.json").read_bytes()
    current = (tmp_path / "builds" / "CURRENT").read_text().strip()
    assert current == manifest["build_id"]


def test_diff_shows_semantic_change_after_clerk_promotion(tmp_path):
    graph_dir, build_dir, manifest = _compiled(tmp_path)
    graph = GraphDir(graph_dir)
    mined = next(s for (s, r, o, _w), q in graph.fold_edges().items()
                 if r == "certified_as" and o == "status:mined")
    ok, _ = set_status(graph_dir, mined, "team_candidate", "jane")
    assert ok
    build2, manifest2, failures = compile_build(graph_dir,
                                                tmp_path / "builds")
    assert not failures
    assert manifest2["build_id"] != manifest["build_id"]
    diff = (build2 / "DIFF_vs_prev.md").read_text()
    assert manifest["build_id"] in diff
    assert "status mined → team_candidate" in diff
    # promotion moved CURRENT to the new build
    assert (tmp_path / "builds" / "CURRENT").read_text().strip() \
        == manifest2["build_id"]


def test_first_build_diff_is_honest(tmp_path):
    _, build_dir, _ = _compiled(tmp_path)
    assert "no previous build" in (build_dir / "DIFF_vs_prev.md").read_text()
