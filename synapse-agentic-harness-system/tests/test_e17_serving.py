"""E17 serving surfaces: the source display registry, the tier
bridge, the Sources shelf (indexes/sources.json), and the cosmos map
(indexes/graph_map.json). Reality law enforced here: every rendered
name and tier traces to a pinned registry, every count to a prov
sweep, and the map is deterministic — same build, same sky."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.compiler.compile import compile_build            # noqa: E402
from sahs.compiler.display import (                        # noqa: E402
    SOURCE_DISPLAY,
    display_for,
    source_of_path,
    tier_of_join,
    tier_of_metric,
    tier_of_table,
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
         "--run-id", "e17_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    builds = tmp_path / "builds"
    build_dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return Path(build_dir)


def test_tier_bridge_pins():
    """The trust symbols map from real fields, pinned: crimson is not
    a tier — conflict renders only on conflict surfaces."""
    assert tier_of_metric({"status": "certified"}) == "ha"
    assert tier_of_metric({"status": "pending"}) == "gr"
    assert tier_of_metric({"status": "mined",
                           "witness_agreement": 2}) == "gr"
    assert tier_of_metric({"status": "mined", "witness_agreement": 1,
                           "status_served": "unreviewed"}) == "in"
    assert tier_of_join({"source": "constraints"}) == "ha"
    assert tier_of_join({"source": "studio",
                         "scope": "scoped_only"}) == "in"
    assert tier_of_join({"source": "studio",
                         "confidence": "pattern_only"}) == "gu"
    assert tier_of_join({"source": "co_query"}) == "gu"
    assert tier_of_join({"source": "jobs_30d",
                         "on": "a.x = b.x"}) == "gr"
    assert tier_of_table("Transaction data", 3, True) == "ha"
    assert tier_of_table("", 3, False) == "gr"
    assert tier_of_table("Some purpose", 0, False) == "in"
    assert tier_of_table("", 0, False) == "gu"


def test_display_registry_and_ledger_grouping():
    """Steward-approved names serve verbatim; unknown sources render
    as THEMSELVES (never silently prettified); ledger paths group to
    the right shelf card."""
    marketplace = {k for k, v in SOURCE_DISPLAY.items()
                   if v["family"] == "marketplace"}
    assert marketplace == {"metrics_dmp", "extended_gmns",
                           "studio_queries"}
    assert display_for("glossary")["display"] == "Acropedia"
    assert display_for("measures_catalog")["display"] \
        == "Metric Mining — BQ Query History"
    assert display_for("bq")["display"] \
        == "Lumi Warehouse — BigQuery Catalog"
    unknown = display_for("mystery_feed")
    assert unknown["family"] == "unregistered"
    assert unknown["display"] == "mystery_feed"
    assert source_of_path("17_queries_30d/jobs_30d.jsonl.gz") \
        == "jobs_30d"
    assert source_of_path("sources/studio_results_x.csv") \
        == "studio_queries"
    assert source_of_path("sources/data_cleaned.csv") == "glossary"


def test_sources_shelf_is_swept_not_estimated(tmp_path):
    """indexes/sources.json: every fixture-observed source carries a
    registered display identity, real contribution counts, a ledger
    line from the run manifest, and the readiness block serves its
    formula next to its numbers."""
    build_dir = _compiled(tmp_path)
    shelf = json.loads(
        (build_dir / "indexes" / "sources.json").read_text())
    rows = {r["source"]: r for r in shelf["sources"]}
    assert rows, "empty sources shelf"
    unregistered = [s for s, r in rows.items()
                    if r["family"] == "unregistered"]
    assert not unregistered, \
        f"sources missing a display name: {unregistered}"
    assert rows["metrics_dmp"]["display"] \
        == "Data Marketplace Steward Definitions"
    dmp_nodes = rows["metrics_dmp"]["contributes"]["nodes"]
    assert dmp_nodes.get("metric", 0) > 0        # swept, not estimated
    assert any(r["ledger"].get("consumed", 0) > 0
               for r in shelf["sources"]), \
        "no source shows a consumed ledger line"
    assert shelf["readiness"], "readiness block empty"
    for entry in shelf["readiness"].values():
        assert set(entry) == {"tables", "witnessed", "pct"}
    assert "witnessed metric" in shelf["meta"]["readiness"]


def test_graph_map_renders_the_real_sky(tmp_path):
    """indexes/graph_map.json: tables in LOB wells with seeded
    positions, certified/pending metrics as named stars, mined bulk
    counted-not-drawn, scoped joins labeled, every edge end present,
    and the whole projection deterministic."""
    build_dir = _compiled(tmp_path)
    payload = json.loads(
        (build_dir / "indexes" / "graph_map.json").read_text())
    nodes = {n["id"]: n for n in payload["nodes"]}
    gms = nodes.get("table:dw.gms_transaction")
    assert gms is not None
    assert len(gms["pos"]) == 3 and gms["well"]
    assert gms["tier"] in ("ha", "gr", "in", "gu")
    metric_nodes = [n for n in payload["nodes"]
                    if n["kind"] == "metric"]
    assert metric_nodes
    assert all(n.get("status") in ("certified",
                                   "pending_certification", "pending")
               for n in metric_nodes)
    assert payload["meta"]["truncated"]["mined_metrics"] > 0
    joins = [e for e in payload["edges"] if e["kind"] == "joins"]
    assert any(e.get("scope") == "scoped_only" for e in joins), \
        "the studio CTE-scoped join must stay labeled in the sky"
    assert any(e["kind"] == "computed-from" for e in payload["edges"])
    for edge in payload["edges"]:
        assert edge["a"] in nodes and edge["b"] in nodes
    wells = {w["id"] for w in payload["wells"]}
    assert wells and all(n["well"] in wells
                         for n in payload["nodes"])
    # determinism: recompile into a fresh root → byte-identical map
    builds2 = tmp_path / "builds2"
    build_dir2, _m, failures = compile_build(
        tmp_path / "graph", builds2)
    assert not failures
    assert (Path(build_dir2) / "indexes" / "graph_map.json"
            ).read_text() \
        == (build_dir / "indexes" / "graph_map.json").read_text()
