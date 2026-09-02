"""state_report.py — the offline sections and the capability verdicts,
pinned without a network. The laptop fills in the real numbers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n",
                    encoding="utf-8")


def _synthetic(tmp_path: Path) -> tuple[Path, Path]:
    graph, builds = tmp_path / "graph", tmp_path / "builds"
    _write_jsonl(graph / "nodes" / "metric.jsonl",
                 [{"id": "metric:a"}, {"id": "metric:b"}])
    _write_jsonl(graph / "edges" / "bound_to.jsonl", [
        {"s": "metric:a", "r": "bound_to", "o": "table:t",
         "prov": {"witness": "catalog_mined"}},
        {"s": "metric:b", "r": "bound_to", "o": "table:t",
         "prov": {"witness": "gmns"}}])
    (graph / "runs" / "r1").mkdir(parents=True)
    (graph / "runs" / "r1" / "manifest.json").write_text(json.dumps({
        "run_id": "r1", "roots": {"bq": "/x"},
        "reports": {"utilization": {"consumed": 12, "deferred": 3}}}))
    (graph / "runs" / "r1" / "enrich_report.json").write_text(json.dumps({
        "prompt_version": "b1.2",
        "blind": {"recovered": 4, "n": 5, "rate": 0.8, "tier": "batch"},
        "metrics_enriched": 7, "concepts_enriched": 2}))
    bdir = builds / "b_test"
    (bdir / "indexes").mkdir(parents=True)
    (builds / "CURRENT").write_text("b_test")
    (bdir / "manifest.json").write_text(json.dumps({
        "counts": {"tables": 1, "metrics": 2}}))
    _write_jsonl(bdir / "indexes" / "metrics.jsonl", [
        {"id": "metric:a", "status_served": "certified",
         "line_of_business": "GMNS", "domain": "Merchant",
         "support_by_witness": {"catalog_mined": 3, "gmns": 1},
         "question_source": "enriched", "grain_source": "mined",
         "evidence_origin": "archive", "description": "spend"},
        {"id": "metric:b", "status_served": "pending_certification",
         "line_of_business": "", "domain": "",
         "support_by_witness": {"gmns": 2},
         "question_source": "", "grain_source": "",
         "evidence_origin": "", "description": ""}])
    _write_jsonl(bdir / "indexes" / "joins.jsonl",
                 [{"a": "t", "b": "u", "tier": "witnessed"}])
    _write_jsonl(bdir / "indexes" / "lob.jsonl", [
        {"code": "GMNS", "name": "Global Merchant & Network Services",
         "domains": ["merchant"], "tables": ["t"]}])
    (bdir / "indexes" / "cost_priors.json").write_text("{}")
    (bdir / "DIFF_vs_prev.md").write_text("# DIFF\nfirst build\n")
    return graph, builds


def test_offline_sections_read_the_synthetic_state(tmp_path):
    import state_report
    graph, builds = _synthetic(tmp_path)
    g = state_report.graph_section(graph)
    assert g["nodes"] == {"metric": 2} and g["edges"] == {"bound_to": 2}
    assert g["quads_by_witness"] == {"catalog_mined": 1, "gmns": 1}
    assert g["runs_total"] == 1
    assert g["runs"][0]["utilization"] == {"consumed": 12, "deferred": 3}
    b = state_report.build_section(builds, graph)
    assert b["build_id"] == "b_test"
    assert b["metrics_by_status"] == {"certified": 1,
                                      "pending_certification": 1}
    assert b["metrics_by_lob"] == {"GMNS": 1, "(none)": 1}
    assert b["witness_families_on_metrics"] == {"gmns": 2,
                                                "catalog_mined": 1}
    assert b["joins_by_tier"] == {"witnessed": 1}
    assert b["enrichment_coverage"]["question_source"] == {
        "enriched": 1, "(none)": 1}
    assert b["business_areas"][0]["code"] == "GMNS"
    assert b["diff_headline"][0] == "# DIFF"
    e = state_report.enrichment_section(graph)
    assert e["count"] == 1 and e["runs"][0]["blind"]["tier"] == "batch"
    markdown = state_report.render_markdown({
        "generated_at": "now", "graph": g, "build": b, "enrichment": e})
    assert "quads by witness: catalog_mined 1 · gmns 1" in markdown
    assert "blind 4/5 → batch" in markdown
    assert "GMNS (Global Merchant & Network Services; t)" in markdown


def test_missing_state_is_reported_not_crashed(tmp_path):
    import state_report
    assert "no graph at" in state_report.graph_section(
        tmp_path / "nope")["error"]
    assert "no promoted build" in state_report.build_section(
        tmp_path / "b", tmp_path / "g")["error"]
    rc = state_report.main(["--graph", str(tmp_path / "g"),
                            "--builds", str(tmp_path / "b"),
                            "--no-vertex", "--out", str(tmp_path / "o")])
    assert rc == 0
    assert (tmp_path / "o" / "state_report.md").exists()


def test_capability_probes_read_real_shapes():
    import state_report

    class Client:
        class connection:
            @staticmethod
            def ssl_context():
                return None
        def __init__(self, raw):
            self.raw = raw
        def _post(self, body):
            return self.raw

    fc = state_report.probe_function_calling(Client({
        "candidates": [{"content": {"parts": [
            {"functionCall": {"name": "search",
                              "args": {"query": "spend"}},
             "thoughtSignature": "abc"}]}}]}))
    assert fc["verdict"] == "ok" and fc["thought_signature"] is True
    none = state_report.probe_function_calling(Client({
        "candidates": [{"content": {"parts": [{"text": "hi"}]}}]}))
    assert none["verdict"] == "no_call"
    think = state_report.probe_thinking(Client({
        "candidates": [{"content": {"parts": [
            {"thought": True, "text": "sky…"}, {"text": "Blue"}]}}],
        "usageMetadata": {"thoughtsTokenCount": 9}}))
    assert think["low"] == "ok" and think["thought_summaries"] is True
    assert think["thought_tokens"] == 9
    js = state_report.probe_json_mode(Client({
        "candidates": [{"content": {"parts": [{"text": '{"ok": true}'}]}}]}))
    assert js["verdict"] == "ok"
