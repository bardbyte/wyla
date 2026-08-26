"""P2 gate (graph half): quads, validator catalog, E7 clerk, archives."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.graph.clerk import set_status                      # noqa: E402
from sahs.graph.crosswalk import Crosswalk                   # noqa: E402
from sahs.graph.ids import kind_of                           # noqa: E402
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad  # noqa: E402
from sahs.graph.validate import validate_graph               # noqa: E402

FX = SILO / "tests" / "fixtures"
CROSSWALK = FX / "identity" / "crosswalk.jsonl"


def _build(graph_dir: Path, out_dir: Path,
           crosswalk: Path = CROSSWALK) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "build-graph",
         "--graph", str(graph_dir), "--crosswalk", str(crosswalk),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(out_dir), "--plain", "--run-id", "test_r1"],
        capture_output=True, text=True, cwd=SILO)


def test_id_grammar():
    assert kind_of("table:dw.gms_transaction") == "table"
    assert kind_of("col:dw.gms_transaction.cm13") == "col"
    assert kind_of("pred:ab12cd34ef56") == "pred"
    assert kind_of("concept:consumer@table:dw.wwcas_authorization") \
        == "concept"
    assert kind_of("acr:aa@risk@us") == "acr"
    assert kind_of("table:no_dataset") is None
    assert kind_of("pred:SHOUTING") is None


def test_build_graph_end_to_end_fuses_three_witnesses(tmp_path):
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir)
    assert result.returncode == 0, result.stderr[-800:]
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()

    col = nodes["col:dw.gms_transaction.trans_usd_am"]
    assert col.props["data_type"] == "FLOAT64"          # bq (D3 one side)
    assert col.props["data_type_mdm"] == "STRING"       # lumi (D3 other)
    assert col.props["description_mdm"] != col.props["description_atlas"]

    # D1/D2 raw material present for the reconciler
    assert "col:dw.gms_transaction.mdm_only_col" in nodes      # D1
    assert "col:dw.gms_transaction.bq_only_col" in nodes       # D2
    atlas_types = nodes["col:dw.gms_transaction.bq_only_col"].props
    assert "data_type_atlas" not in atlas_types

    # E3 raw material: DENIED policy listing → unknown, never absence
    assert ("table:dw.wwcas_authorization", "has_policy",
            "policy:unknown_denied", "bq") in edges
    # confirmed-empty policies (gms 16 file = []) emit nothing
    assert not any(s == "table:dw.gms_transaction" and "unknown" in o
                   for (s, r, o, _w) in edges if r == "has_policy")

    # governance seeds: all four initial states present
    seeds = {o for (s, r, o, _w), q in edges.items()
             if r == "certified_as"}
    assert {"status:certified", "status:pending", "status:team_candidate",
            "status:mined"} <= seeds

    # co-query support + domains + templates + lineage
    co = edges[("table:dw.gms_transaction", "co_queried_with",
                "table:dw.wwcas_authorization", "bq")]
    assert co.prov.support == 12
    assert "domain:dw.gms_transaction.country_cd" in nodes
    # template headers drift across extractor versions: gms uses the
    # canonical normalized_sql, wwcas the drifted query_template/count —
    # both adapt into the same canonical node shape
    tmpls = {k: v for k, v in nodes.items() if k.startswith("tmpl:")}
    assert len(tmpls) == 3
    assert any("wwcas_authorization" in v.props["normalized_sql"]
               and v.props["occurrences"] == 30 for v in tmpls.values())
    assert ("table:data.raw_gms_feed", "upstream_of",
            "table:dw.gms_transaction", "lumi") in edges
    derived = [(s, o) for (s, r, o, _w) in edges
               if r == "derived_from"]
    assert derived and derived[0][1].startswith("col:data.raw_gms_feed")

    # mdm 503 → lifecycle UNKNOWN, stated not omitted
    wwcas = nodes["table:dw.wwcas_authorization"]
    assert wwcas.props["lifecycle_status"] == "unknown_unavailable"


def test_crosswalk_blocking_is_a_build_error(tmp_path):
    broken = tmp_path / "broken.jsonl"
    broken.write_text(json.dumps({
        "physical": "dw.gms_transaction", "lumi_asset_id": "lumi-ds-001",
        "atlas_entity_id": "gms_transaction", "verified_by": "x",
        "verified_on": "2026-08-25"}) + "\n")
    result = _build(tmp_path / "g", tmp_path / "run", crosswalk=broken)
    assert result.returncode == 2
    assert "crosswalk" in result.stderr


def test_validator_error_catalog(tmp_path):
    graph = GraphDir(tmp_path)
    prov = Prov(source="test", run="r1")
    graph.append_node(NodeRecord(id="table:dw.t1", props={}, prov=prov))
    # [3] bad grammar (raw write bypasses append_node's check)
    (tmp_path / "nodes" / "table.jsonl").open("a").write(
        json.dumps({"id": "table:UPPER", "props": {},
                    "prov": prov.model_dump()}) + "\n")
    # [4] unresolved endpoint
    graph.append_edge(Quad(s="table:dw.t1", r="co_queried_with",
                           o="table:dw.ghost", prov=prov))
    # [8] duplicate same-source
    graph.append_edge(Quad(s="table:dw.t1", r="co_queried_with",
                           o="table:dw.ghost", prov=prov))
    # [9] illegal transition + [12] certified without home
    m = "metric:aaaaaaaaaaaa"
    graph.append_node(NodeRecord(id=m, props={}, prov=prov))
    graph.append_edge(Quad(s=m, r="certified_as", o="status:certified",
                           prov=prov))
    graph.append_edge(Quad(s=m, r="certified_as", o="status:mined",
                           prov=prov))
    # [E7] clerk without actor
    graph.append_edge(Quad(s=m, r="certified_as", o="status:retracted",
                           prov=Prov(source="clerk", run="r2")))
    report = validate_graph(tmp_path)
    tags = {e.split()[0] for e in report.errors}
    assert {"[3]", "[4]", "[8]", "[9]", "[12]", "[E7]"} <= tags


def test_clerk_transitions_and_signature(tmp_path):
    graph = GraphDir(tmp_path)
    prov = Prov(source="measures_catalog", run="r1")
    m = "metric:bbbbbbbbbbbb"
    graph.append_node(NodeRecord(id=m, props={}, prov=prov))
    graph.append_node(NodeRecord(id="table:dw.t1", props={}, prov=prov))
    graph.append_edge(Quad(s=m, r="measured_on", o="table:dw.t1",
                           prov=prov))
    graph.append_edge(Quad(s=m, r="certified_as", o="status:mined",
                           prov=prov))
    ok, msg = set_status(tmp_path, m, "certified", "jane")
    assert not ok and "illegal transition" in msg
    ok, _ = set_status(tmp_path, m, "team_candidate", "jane")
    assert ok
    ok, _ = set_status(tmp_path, m, "certified", "jane")
    assert ok
    history = GraphDir(tmp_path).governance_history()[m]
    assert history == ["mined", "team_candidate", "certified"]


def test_fold_last_wins_and_retraction(tmp_path):
    graph = GraphDir(tmp_path)
    p1 = Prov(source="bq", run="r1")
    graph.append_node(NodeRecord(id="table:dw.t1", props={"a": 1},
                                 prov=p1))
    graph.append_node(NodeRecord(id="table:dw.t1", props={"b": 2},
                                 prov=Prov(source="lumi", run="r2")))
    nodes = graph.fold_nodes()
    assert nodes["table:dw.t1"].props == {"a": 1, "b": 2}
    graph.append_edge(Quad(s="table:dw.t1", r="co_queried_with",
                           o="table:dw.t1", prov=p1))
    graph.append_edge(Quad(
        s="table:dw.t1", r="co_queried_with", o="table:dw.t1",
        prov=Prov(source="bq", run="r3", status="retracted")))
    assert ("table:dw.t1", "co_queried_with", "table:dw.t1", "bq") \
        not in graph.fold_edges()


def test_crosswalk_lookup_paths():
    crosswalk = Crosswalk.load(CROSSWALK)
    assert crosswalk.physical_for_bq("dw", "gms_transaction") \
        == "dw.gms_transaction"
    assert crosswalk.physical_for_lumi("wwcas_authorization") \
        == "dw.wwcas_authorization"
    assert crosswalk.physical_for_lumi("?", "lumi-ds-001") \
        == "dw.gms_transaction"
    assert crosswalk.physical_for_atlas("gms_transaction") \
        == "dw.gms_transaction"
    assert crosswalk.physical_for_atlas("nope") is None


def test_utilization_ledger_accounts_for_every_file(tmp_path):
    """E12/A2: no archive artifact absent from the ledger; the
    inventoried set contains ONLY files we knowingly do nothing with."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir)
    assert result.returncode == 0, result.stderr[-800:]
    manifest = json.loads(next(
        (graph_dir / "runs").glob("*/manifest.json")).read_text())
    rows = manifest["utilization"]
    on_disk = sum(1 for root in ("real_extractions_production",
                                 "mdm_46_patched_v2", "sources")
                  for p in (FX / root).rglob("*") if p.is_file())
    assert len(rows) == on_disk          # nothing unledgered, ever
    assert all(r["sha256_12"] for r in rows)
    by_path = {f"{r['root']}/{r['path']}": r for r in rows}
    assert by_path["real_extractions_production/gms_transaction/"
                   "02_logical_columns.csv"]["status"] == "consumed"
    assert by_path["mdm_46_patched_v2/coverage.json"][
        "status"] == "consumed"
    assert by_path["sources/blue_business_insights.csv"][
        "status"] == "consumed"
    tls = by_path["sources/tls_reference.md"]
    assert tls["status"] == "deferred" and "doc evidence" in tls["reason"]
    assert all(r.get("reason") for r in rows
               if r["status"] == "deferred")
    allowed_inventoried = {
        "real_extractions_production/_batch_summary.csv",
        "real_extractions_production/_run_report.json",
        "mdm_46_patched_v2/run_manifest.json",
    }
    inventoried = {p for p, r in by_path.items()
                   if r["status"] == "inventoried"}
    unexpected = {p for p in inventoried
                  if p not in allowed_inventoried
                  and not p.endswith(("_summary.json", "summary.json",
                                      "qa_checks.yaml",
                                      "chart_contract.yaml",
                                      "data_specs.md", "discovery.json"))}
    assert not unexpected, unexpected
