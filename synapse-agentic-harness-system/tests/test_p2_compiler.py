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
    # D2 = 4: gms bq_only_col + the two sbs_new_accounts columns (a
    # bq-only table — no 00 resource, no atlas/mdm plane — is honestly
    # all coverage gap) + the 03-minted nested field path (typed by BQ,
    # undocumented by any catalog plane)
    assert totals == {"D1": 1, "D2": 4, "D3": 1, "D4": 2, "D5": 1}
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
    assert "ungoverned, no business meaning on record" in gms_card  # D2
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


def test_lob_index_joins_sources_and_pedigree_serving(tmp_path):
    """Compiled LOB view: indexes/lob.jsonl for the tools, one witness-
    named line per table card, metric rows carrying their dmp pedigree,
    and joins.jsonl naming its three knowledge families (`source`)."""
    _, build_dir, manifest = _compiled(tmp_path)
    lob_rows = [json.loads(x) for x in
                (build_dir / "indexes" / "lob.jsonl"
                 ).read_text().splitlines()]
    by_code = {r["lob"]: r for r in lob_rows}
    assert by_code["gmns"]["tables"] == ["dw.gms_transaction",
                                         "dw.wwcas_authorization"]
    assert by_code["gmns"]["domains"] == ["merchant"]
    assert by_code["sbs"]["tables"] == ["dw.sbs_new_accounts"]
    # the usage plane compiled: the CRO org unit (child of SBS) with
    # its used tables; the LOB's own usage rides on the gmns row
    assert by_code["cro"]["kind"] == "org_unit"
    assert by_code["cro"]["parent"] == "SBS"
    assert by_code["cro"]["used_tables"] == ["dw.wwcas_authorization"]
    assert by_code["gmns"]["usage_support"] >= 27
    assert manifest["counts"]["lobs"] == 3

    joins = [json.loads(x) for x in
             (build_dir / "indexes" / "joins.jsonl"
              ).read_text().splitlines()]
    assert {"co_query", "jobs_30d", "constraints"} <= \
        {j["source"] for j in joins}
    fk_row = next(j for j in joins if j["source"] == "constraints")
    assert fk_row["on"] == ("dw.gms_transaction.cm13 = "
                            "dw.wwcas_authorization.card_no")

    gms_card = (build_dir / "cards" / "tables"
                / "dw__gms_transaction.md").read_text()
    assert ("- line of business: GMNS: Global Merchant & Network "
            "Services (steward; corroborated by") in gms_card
    assert "payment_detail.card.network" in gms_card   # nested, full path
    assert "- used by: " in gms_card
    wwcas_card = (build_dir / "cards" / "tables"
                  / "dw__wwcas_authorization.md").read_text()
    assert "CRO: Credit Risk Ops (SBS)" in wwcas_card  # usage ≠ ownership

    metrics = [json.loads(x) for x in
               (build_dir / "indexes" / "metrics.jsonl"
                ).read_text().splitlines()]
    approval = next(m for m in metrics if m["label"] == "SBS Approval Rate")
    assert approval["author"] == "steward_b"
    assert approval["domain"] == "Acquisition"
    assert approval["line_of_business"] == "SBS"
    card = (build_dir / "cards" / "metrics"
            / f"{approval['fp']}.md").read_text()
    assert "- domain: Acquisition · lob: SBS · author: steward_b" in card

    # the catalog's hand-written guidance rides row → card → search
    spend = next(m for m in metrics if m["label"] == "GMNS Merchant Spend")
    assert "Do not use for" in spend["description"]
    spend_card = (build_dir / "cards" / "metrics"
                  / f"{spend['fp']}.md").read_text()
    assert "- guidance: " in spend_card
    assert "Do not use for authorization counts" in spend_card

    # serving surface: Build loads the lob index; search exposes the
    # pedigree + guidance as hints (E6 — readable, never ranked)
    from sahs.tools.api import Build, search_metrics
    build = Build.open(tmp_path / "builds")
    assert build.lob and build.lob[0]["lob"] == "cro"
    top = search_metrics(build, "small business approval rate"
                         )["candidates"][0]
    assert top["line_of_business"] == "SBS"
    hit = search_metrics(build, "merchant spend volume")["candidates"]
    assert any("Do not use" in c["guidance"] for c in hit)


def test_table_reconciliation_explained_or_blocked(tmp_path):
    """The 46→45 rule: every crosswalk row either compiles or is
    EXPLAINED. The fixture's lineage endpoint (a crosswalk row with no
    columns anywhere) is intentionally excluded via
    identity/exclusions.jsonl and the manifest says so; strip the
    exclusion and the SAME graph refuses to promote — an unexplained
    gap may not survive a build."""
    graph_dir, build_dir, manifest = _compiled(tmp_path)
    recon = manifest["table_reconciliation"]
    assert recon["crosswalk_rows"] == 4
    assert recon["built"] == 3
    (gap,) = recon["missing"]
    assert gap["physical"] == "data.raw_gms_feed"
    # the node EXISTS (minted as an fk lineage endpoint) — what's
    # missing is columns, and the derived reason says exactly that
    assert "ZERO columns on record" in gap["reason"]
    assert "lineage endpoint" in gap["intentionally_excluded"]
    # now the same graph WITHOUT the explanation → gate blocks
    (graph_dir / "identity" / "exclusions.jsonl").write_text(
        "", encoding="utf-8")
    _, _, failures = compile_build(graph_dir, tmp_path / "builds2")
    assert any("missing from build, unexplained: data.raw_gms_feed"
               in f for f in failures)
    assert not (tmp_path / "builds2" / "CURRENT").exists()
    # strict sidecar: an exclusion naming a non-crosswalk table refuses
    (graph_dir / "identity" / "exclusions.jsonl").write_text(
        json.dumps({"physical": "dw.not_a_table", "reason": "x"}) + "\n",
        encoding="utf-8")
    _, _, failures = compile_build(graph_dir, tmp_path / "builds3")
    assert any("non-crosswalk table" in f for f in failures)


def test_census_duplicate_ids_and_conflict_scope_meta(tmp_path):
    """metric_conflicts counts same-IDENTITY drift only — the census
    says so in meta, so a zero is never read as 'no metric
    disagreements exist'. The inverse finding gets its own counter:
    the fixture's 101-dup row (same SQL, second catalog id) is a
    duplicate catalog entry, not a conflict."""
    _, build_dir, _ = _compiled(tmp_path)
    census = json.loads((build_dir / "census.json").read_text())
    assert census["summary"]["metric_duplicate_ids"] >= 1
    assert "same-IDENTITY drift only" in census["meta"]["metric_conflicts"]
    assert "multiple catalog ids" in \
        census["meta"]["metric_duplicate_ids"]


def test_served_status_splits_governance_from_evidence(tmp_path):
    """The agent-facing vocabulary separates the two axes: a mined
    metric serves as **unreviewed** with its evidence origin named —
    'mined' alone let origin read as endorsement. Store states and the
    E7 clerk lattice are untouched."""
    _, build_dir, _ = _compiled(tmp_path)
    metrics = [json.loads(x) for x in
               (build_dir / "indexes" / "metrics.jsonl"
                ).read_text().splitlines()]
    mined = next(m for m in metrics if m["status"] == "mined"
                 and m["source"] == "measures_catalog")
    assert mined["status_served"] == "unreviewed"
    assert mined["evidence_origin"] == "usage_mining"
    card = (build_dir / "cards" / "metrics"
            / f"{mined['fp']}.md").read_text()
    assert "status: **unreviewed** (evidence: usage_mining)" in card
    certified = next(m for m in metrics if m["status"] == "certified")
    assert certified["status_served"] == "certified"
    assert certified["evidence_origin"] == "certified_catalog"
    gms_card = (build_dir / "cards" / "tables"
                / "dw__gms_transaction.md").read_text()
    assert "· unreviewed [prov:measures_catalog]" in gms_card
    assert "· mined [prov:" not in gms_card
    from sahs.tools.api import Build, get_definition_line, search_metrics
    build = Build.open(tmp_path / "builds")
    top = search_metrics(build, "merchant spend volume")["candidates"][0]
    assert top["status_served"] and top["evidence_origin"]
    out = get_definition_line(build, mined["id"])
    assert "[unreviewed, usage_mining]" in out["definition_line"]


def test_studio_texture_and_mined_joins_serve(tmp_path):
    """The studio evidence survives compile intact: joins.jsonl carries
    the mined scoped edge (4th named family), BOTH table cards warn the
    join is CTE-scoped, and the metric cards render observed grain, the
    lineage-mismatch warning, data owners, and dmp's declared join
    condition. The same-id-different-SQL row is a VISIBLE conflict."""
    _, build_dir, _ = _compiled(tmp_path)
    joins = [json.loads(x) for x in
             (build_dir / "indexes" / "joins.jsonl"
              ).read_text().splitlines()]
    assert {"co_query", "jobs_30d", "constraints", "studio"} <= \
        {j["source"] for j in joins}
    studio = next(j for j in joins if j["source"] == "studio")
    assert studio["scope"] == "scoped_only"
    assert studio["on"] == ["cm13 = card_no", "offr_id = offr_id"]
    assert studio["join_type"] == "LEFT"
    assert studio["witness_metrics"] == ["102"]
    for name in ("dw__gms_transaction.md", "dw__wwcas_authorization.md"):
        card = (build_dir / "cards" / "tables" / name).read_text()
        assert "CTE-scoped, NOT raw-safe" in card, name
        assert "[prov:studio]" in card, name

    metrics = [json.loads(x) for x in
               (build_dir / "indexes" / "metrics.jsonl"
                ).read_text().splitlines()]
    conflict = next(m for m in metrics
                    if "mgroup:dmp:102" in m["mgroups"]
                    and "distinct" in m["canonical_sql"].lower())
    card = (build_dir / "cards" / "metrics"
            / f"{conflict['fp']}.md").read_text()
    assert "- grain: card member x day (observed) [prov:studio]" in card
    assert "associated but NOT referenced by the SQL: " \
           "wwcas_authorization" in card
    assert "- data owners: ops@example.com [prov:studio]" in card
    assert "- query shape: CTE/SUBQUERY" in card
    certified = next(m for m in metrics
                     if "mgroup:dmp:102" in m["mgroups"]
                     and m["canonical_sql"] == "count(1)")
    card = (build_dir / "cards" / "metrics"
            / f"{certified['fp']}.md").read_text()
    assert ("- declared join condition: `gms_transaction.cm13 = "
            "wwcas_authorization.card_no` [prov:dmp]") in card
    # the same-id-different-SQL witness is a VISIBLE census conflict
    census = json.loads((build_dir / "census.json").read_text())
    assert census["summary"]["metric_conflicts"] >= 1
