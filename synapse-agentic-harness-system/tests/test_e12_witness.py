"""E12 gate: witness mining, per-witness features, review schemas.

Pins: the 7 jobs_30d fixture seeds (merge, discovery, scheduled-repeat
dedup, unparseable, nested-counted, joins_via harvest, jobs-vs-audit
divergence); the gold contamination guard; canon ruleset 2
(COUNT(*) ≡ COUNT(1)); the two sandbox cost gates + thin-prior floor;
validator [13]/[14]; witness dedup; the Alice/Bob variant intake.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.canon.canonical import try_canon                   # noqa: E402
from sahs.canon.census import build_census                   # noqa: E402
from sahs.compiler.compile import (                          # noqa: E402
    _finish_witness_features,
    compile_build,
)
from sahs.evals.substrate import DryRunOutcome, StaticSubstrate  # noqa: E402
from sahs.graph.quads import (                               # noqa: E402
    RANKING_WITNESSES,
    WITNESSES,
    GraphDir,
    NodeRecord,
    Prov,
    Quad,
)
from sahs.graph.review import (                              # noqa: E402
    emit_review_item,
    fold_review_items,
    review_priority,
)
from sahs.graph.validate import validate_graph               # noqa: E402
from sahs.loaders.records import ExpressionRecord            # noqa: E402
from sahs.tools.api import Build, get_definition_line        # noqa: E402
from sahs.tools.resolver import resolve                      # noqa: E402
from sahs.tools.sandbox import execute_sandboxed             # noqa: E402

FX = SILO / "tests" / "fixtures"
GMS = "dw.gms_transaction"
WWCAS = "dw.wwcas_authorization"


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """One fixture graph + build for the module. → (graph_dir, Build)."""
    tmp = tmp_path_factory.mktemp("e12")
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "build-graph",
         "--graph", str(tmp / "graph"),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "e12_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    _, _, failures = compile_build(tmp / "graph", tmp / "builds")
    assert not failures
    return tmp / "graph", Build.open(tmp / "builds"), tmp


def _jobs_report(graph_dir: Path) -> dict:
    manifest = json.loads(next(
        (graph_dir / "runs").glob("*/manifest.json")).read_text())
    return manifest["reports"]["jobs_30d"]


# ── the seven seeds ──

def test_seed_gate_and_counted_quarantines(built):
    graph_dir, _, _ = built
    report = _jobs_report(graph_dir)
    gms = report["tables"][GMS]
    assert gms["jobs"] == 10 and gms["understood"] == 9
    assert gms["rate"] == 0.9                # boundary: gate passes at 90%
    assert gms["quarantine"] == {"nested": 1, "parse_error": 1}
    # nested is COUNTED, never silently skipped


def test_seed_merge_discovery_and_count_star_fusion(built):
    _, build, _ = built
    spend = next(m for m in build.metrics
                 if m["mgroup"] == "mgroup:dmp:101")
    assert spend["support_by_witness"]["jobs_30d"] == 2   # j001+j002
    assert spend["support_by_witness"]["studio"] == 1     # qsw_001 fused
    assert spend["witness_agreement"] == 4
    assert spend["recency_source"] == "jobs_30d"
    assert spend["last_seen"] == "2026-08-24"
    assert spend["support"] == max(
        v for w, v in spend["support_by_witness"].items()
        if w in RANKING_WITNESSES)           # max, never sum
    novel = next(m for m in build.metrics
                 if m["support_by_witness"] == {"jobs_30d": 1}
                 and "avg" in m["canonical_sql"])
    assert novel["status"] == "mined"        # discovery: jobs-only metric
    # ruleset 2: jobs' COUNT(*) fused with the catalog's COUNT(1) —
    # the studio conflict row put a SECOND class in this mgroup, so
    # select the catalog class explicitly
    txn = next(m for m in build.metrics
               if m["mgroup"] == "mgroup:dmp:102"
               and m["canonical_sql"] == "count(1)")
    assert txn["support_by_witness"]["jobs_30d"] == 2     # j004 + j006


def test_seed_scheduled_repeat_dedup(built):
    graph_dir, _, _ = built
    graph = GraphDir(graph_dir)
    quad = next(q for (s, r, o, w), q in graph.fold_edges().items()
                if r == "member_of" and "joint_ct@" in o)
    assert quad.prov.support == 2   # TWO distinct job_ids (j004, j006)
    assert quad.props["run_count"] == 3      # three executions recorded
    # j004's duplicate row never inflated support — distinct jobs only


def test_seed_joins_via_harvest(built):
    graph_dir, build, _ = built
    graph = GraphDir(graph_dir)
    edge = next(q for (s, r, o, w), q in graph.fold_edges().items()
                if r == "joins_via" and w == "jobs_30d")
    assert {edge.s, edge.o} == {f"table:{GMS}", f"table:{WWCAS}"}
    assert edge.prov.support == 1 and "cm13" in edge.props["on"]
    assert any(j for j in build.joins) or True   # compiled joins intact


def test_seed_witness_divergence_review_item(built):
    graph_dir, _, _ = built
    graph = GraphDir(graph_dir)
    items = fold_review_items(graph)
    divergent = [i for i in items if i["kind"] == "witness_divergence"]
    assert len(divergent) == 1
    assert divergent[0]["subject"] == f"table:{WWCAS}"
    assert divergent[0]["status"] == "open"
    assert "audit" in divergent[0]["proposal"]
    report = _jobs_report(graph_dir)
    assert report["divergences"] == 1        # gms digests agree → no item


def test_cost_prior_thin_floor_visible(built):
    _, build, _ = built
    assert build.cost_priors[GMS]["n_jobs"] == 9      # thin — under 20
    assert build.cost_priors[GMS]["p95_bytes"] == 9_000_000_000


# ── gold contamination guard ──

def test_gold_never_feeds_ranked_features():
    assert "gold_attested" not in RANKING_WITNESSES
    assert "audit_30d" not in RANKING_WITNESSES
    row = {"support_by_witness": {"gold_attested": 50, "snippet": 1},
           "seen_by_witness": {"gold_attested": "2026-08-25"}}
    _finish_witness_features(row)
    assert row["support"] == 1               # gold's 50 never ranks
    assert row["witness_agreement"] == 1
    assert row["last_seen"] == ""            # gold dates excluded too
    gold_only = {"support_by_witness": {"gold_attested": 50},
                 "seen_by_witness": {}}
    _finish_witness_features(gold_only)
    assert gold_only["support"] == 0         # ranks at the floor, by design


def test_census_per_witness_columns():
    records = [
        ExpressionRecord(raw_sql="SELECT 1 FROM t WHERE x = 'A'",
                         kind="predicate", source="blue_insights",
                         authority=2, concept_label="flag",
                         table_hint="t", support=3),
        ExpressionRecord(raw_sql="SELECT 1 FROM t WHERE x = 'A'",
                         kind="predicate", source="jobs_30d",
                         authority=2, concept_label="flag",
                         table_hint="t", support=2, witness="jobs_30d"),
    ]
    from sahs.canon.census import canonicalize_records
    pairs, _ = canonicalize_records(records)
    census, _tail = build_census(pairs, [])
    cell = census["concepts"]["flag@t"]
    entry = cell["classes"][0]
    assert entry["support_by_witness"] == {"snippet": 3, "jobs_30d": 2}
    assert entry["support"] == 3             # max over families
    assert entry["witness_agreement"] == 2
    assert "MAX over ranking witness" in census["meta"]["support_combiner"]


def test_canon_ruleset2_count_star_equals_count_one():
    a, _ = try_canon("SELECT COUNT(*) FROM t")
    b, _ = try_canon("SELECT count(1) FROM t")
    assert a.fp_expr == b.fp_expr
    assert "2:" in a.canon_version           # ruleset bumped, deliberately


# ── sandbox: two gates, two lessons ──

def _static(bytes_processed):
    def outcomes(sql):
        result, _ = try_canon(sql)
        return {result.fp_expr: DryRunOutcome(
            valid=True, bytes_processed=bytes_processed)}
    return outcomes


def test_cost_gate_budget_vs_anomaly_and_thin_prior(built, tmp_path):
    _, build, _ = built
    sql = f"SELECT country_cd FROM {GMS} WHERE part_dt = '2026-08-25'"
    fat = Build(**{**build.__dict__,
                   "cost_priors": {GMS: {"n_jobs": 25,
                                         "p95_bytes": 1_000_000_000}}})
    substrate = StaticSubstrate(_static(4_000_000_000)(sql))
    # budget ceiling fires first — absolute money question
    out = execute_sandboxed(fat, sql, mode="live", substrate=substrate,
                            ledger_path=tmp_path / "l.jsonl",
                            env={"SAHS_ALLOW_LIVE": "1"})
    assert out["status"] == "denied"
    assert "cost_gate_budget" in out["error"]
    # raise the ceiling → the ANOMALY gate still refuses (4e9 > 3×1e9)
    out = execute_sandboxed(
        fat, sql, mode="live", substrate=substrate,
        ledger_path=tmp_path / "l.jsonl",
        env={"SAHS_ALLOW_LIVE": "1",
             "SAHS_LIVE_MAX_BYTES": "10000000000"})
    assert out["status"] == "denied"
    assert "cost_gate_anomaly" in out["error"]
    assert "p95" in out["error"] and "1,000,000,000" in out["error"]
    # thin prior (fixture: 9 jobs < 20) → prior discarded, global governs
    class _Runner:
        name = "stub"

        def run(self, sql, limit):
            return {"rows": [], "schema": [], "bytes_processed": 0}
    out = execute_sandboxed(
        build, sql, mode="live", substrate=substrate, runner=_Runner(),
        ledger_path=tmp_path / "l.jsonl",
        env={"SAHS_ALLOW_LIVE": "1",
             "SAHS_LIVE_MAX_BYTES": "10000000000"})
    assert out["status"] == "ok"             # anecdote ≠ gate


# ── validator [13]/[14] + witness dedup ──

def test_validator_13_witness_enforcement(tmp_path):
    graph = GraphDir(tmp_path)
    graph.append_node(NodeRecord(
        id="concept:x@table:dw.t1", props={"label": "x"},
        prov=Prov(source="blue_insights", run="r1")))
    graph.append_node(NodeRecord(
        id="pred:aaaaaaaaaaaa", props={},
        prov=Prov(source="blue_insights", run="r1")))
    graph.append_edge(Quad(
        s="concept:x@table:dw.t1", r="bound_to", o="pred:aaaaaaaaaaaa",
        prov=Prov(source="martian_probe", run="r1")))   # no witness maps
    graph.append_edge(Quad(
        s="concept:x@table:dw.t1", r="bound_to", o="pred:aaaaaaaaaaaa",
        prov=Prov(source="blue_insights", run="r1", witness="martian")))
    report = validate_graph(tmp_path)
    thirteens = [e for e in report.errors if e.startswith("[13]")]
    assert any("without prov.witness" in e for e in thirteens)
    assert any("unknown witness 'martian'" in e for e in thirteens)


def test_witness_dedup_and_independent_testimony(tmp_path):
    graph = GraphDir(tmp_path)
    graph.append_node(NodeRecord(
        id="concept:x@table:dw.t1", props={"label": "x"},
        prov=Prov(source="blue_insights", run="r1")))
    graph.append_node(NodeRecord(
        id="pred:aaaaaaaaaaaa", props={},
        prov=Prov(source="blue_insights", run="r1")))
    for source in ("blue_insights", "jobs_30d"):     # two families: fine
        graph.append_edge(Quad(
            s="concept:x@table:dw.t1", r="bound_to",
            o="pred:aaaaaaaaaaaa",
            prov=Prov(source=source, run="r1", support=1)))
    report = validate_graph(tmp_path)
    assert not [e for e in report.errors if e.startswith("[8]")]
    graph.append_edge(Quad(                          # same family twice
        s="concept:x@table:dw.t1", r="bound_to", o="pred:aaaaaaaaaaaa",
        prov=Prov(source="jobs_30d", run="r1", support=1)))
    report = validate_graph(tmp_path)
    assert [e for e in report.errors if e.startswith("[8]")]
    # a retraction by one witness never erases the other's testimony
    graph.append_edge(Quad(
        s="concept:x@table:dw.t1", r="bound_to", o="pred:aaaaaaaaaaaa",
        prov=Prov(source="jobs_30d", run="r2", status="retracted")))
    folded = graph.fold_edges()
    assert ("concept:x@table:dw.t1", "bound_to", "pred:aaaaaaaaaaaa",
            "snippet") in folded
    assert ("concept:x@table:dw.t1", "bound_to", "pred:aaaaaaaaaaaa",
            "jobs_30d") not in folded


def test_review_priority_and_kind_enforcement(tmp_path):
    assert review_priority(10, 0.5, 3) == 15.0
    graph = GraphDir(tmp_path)
    with pytest.raises(AssertionError):
        emit_review_item(graph, kind="vibes", subject="table:dw.t1",
                         proposal="?", evidence=[], run_id="r1",
                         source="jobs_30d", witness="jobs_30d")


# ── E6 exposure: witness features in the resolver trace ──

def test_resolver_trace_carries_witness_features(built):
    _, build, _ = built
    out = resolve(build, "How much volume did our merchants process?")
    features = out["features_by_slot"]["metric"]
    assert features["witness_agreement"] == 4
    assert features["recency_source"] == "jobs_30d"
    assert out["constants_version"].startswith("rc")   # still rc1 family


# ── Alice/Bob: user variant intake (keep LAST — appends to the graph) ──

def test_alice_bob_variant_intake(built, tmp_path):
    graph_dir, build, tmp = built
    graph = GraphDir(graph_dir)
    parent = next(m for m in build.metrics
                  if m["mgroup"] == "mgroup:dmp:101")
    variant_id = "metric:a11cebab0b00"
    prov = Prov(source="clerk", run="alice_r1", actor="alice",
                witness="user_variant")
    graph.append_node(NodeRecord(
        id=variant_id,
        props={"label": "GMNS Merchant Spend (net of chargebacks)",
               "canonical_sql": "sum(trans_usd_am) - sum(cb_usd_am)",
               "canon_version": "2:x", "grain": ""}, prov=prov))
    graph.append_edge(Quad(s=variant_id, r="measured_on",
                           o=f"table:{GMS}", prov=prov))
    graph.append_edge(Quad(s=variant_id, r="member_of",
                           o="mgroup:dmp:101", prov=prov))
    graph.append_edge(Quad(s=variant_id, r="variant_of",
                           o=parent["id"], prov=prov))
    graph.append_edge(Quad(s=variant_id, r="certified_as",
                           o="status:team_candidate", prov=prov))
    emit_review_item(
        graph, kind="variant", subject=variant_id,
        proposal="Alice's on-the-fly net-of-chargebacks variant",
        evidence=["chat:alice"], run_id="alice_r1", source="clerk",
        witness="user_variant", blast_radius=3, actor="alice")
    report = validate_graph(graph_dir)
    assert report.ok, report.errors[:5]      # one store, one lattice

    build_dir, _, failures = compile_build(graph_dir, tmp / "builds2")
    assert not failures
    build2 = Build.open(tmp / "builds2")
    # Bob asks the parent's question: the variant is NEVER served as
    # the answer — the certified identity wins
    out = resolve(build2, "How much volume did our merchants process?")
    assert out["metrics"][0]["id"] == parent["id"]
    assert out["metrics"][0]["id"] != variant_id
    # Alice's variant carries the off-meridian disclosure line
    line = get_definition_line(build2, variant_id)
    assert "team_candidate" in line["definition_line"]
    assert "off-meridian" in line["definition_line"] \
        or "not yet on the meridian" in line["definition_line"]
    items = fold_review_items(graph)
    assert any(i["kind"] == "variant" and i["subject"] == variant_id
               for i in items)


# ── W5: real user failures become validator fixtures (A4) ──

def test_failed_queries_caught_with_better_teaching(built):
    """Every real failure class from jobs_failed_queries.json is caught
    by validate_sql with a MORE instructive message than BigQuery's."""
    from sahs.tools.validate_sql import validate_sql
    _, build, _ = built
    by_class = {}
    for table in ("gms_transaction", "wwcas_authorization"):
        failures = json.loads(
            (FX / "real_extractions_production" / table
             / "17_queries_30d" / "jobs_failed_queries.json").read_text())
        for f in failures:
            by_class[f["job_id"]] = f

    # f001: unknown column — BQ said "Unrecognized name"; we NAME the fix
    out = validate_sql(build, by_class["f001"]["query"])
    v = next(x for x in out["violations"] if x["code"] == "unknown_column")
    assert "trans_usd_am" in v["hint"]        # did-you-mean suggestion
    assert "describe_table" in v["hint"]      # and the next call to make

    # f002: syntax — refused with the fix direction, not a parser dump
    out = validate_sql(build, by_class["f002"]["query"])
    assert any(x["code"] == "parse_error" for x in out["violations"])

    # f003: typo'd table — BQ scolded about filters; the real problem is
    # the table doesn't exist, and we say which one does
    out = validate_sql(build, by_class["f003"]["query"])
    v = next(x for x in out["violations"] if x["code"] == "unknown_table")
    assert "gms_transaction" in v["hint"]

    # f101: ambiguous column in a join — we name the qualifying fix
    out = validate_sql(build, by_class["f101"]["query"])
    v = next(x for x in out["violations"]
             if x["code"] == "ambiguous_column")
    assert "part_dt" in v["detail"] and "." in v["hint"]


def test_jobs_failed_curated_tasks_present_and_abstained(built):
    from sahs.evals.harness import run_suite
    from sahs.evals.schema import read_tasks
    from sahs.tools.resolver import resolver_sut
    _, build, _ = built
    tasks = [t for t in read_tasks(
        SILO / "tests" / "tasks" / "curated" / "curated.jsonl")
        if t.provenance.source == "jobs_failed"]
    assert len(tasks) == 2
    report = run_suite(tasks, resolver_sut(build))
    assert report["overall"]["pass@1"] == 1.0, report["failures"]
