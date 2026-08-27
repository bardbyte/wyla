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
           crosswalk: Path = CROSSWALK,
           *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), "build-graph",
         "--graph", str(graph_dir), "--crosswalk", str(crosswalk),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(out_dir), "--plain", "--run-id", "test_r1",
         *extra],
        capture_output=True, text=True, cwd=SILO)


def test_no_jobs_30d_excludes_the_witness_and_ledgers_deferred(tmp_path):
    """A8: an incorrect 30-day history witnesses NOTHING — no jobs
    quads, no digests — while every 17_queries_30d file stays
    accounted for as deferred, never silently unread."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir, CROSSWALK, "--no-jobs-30d")
    assert result.returncode == 0, result.stderr[-800:]
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    assert not any(k.startswith("tmpl:") for k in nodes)
    assert not any(r == "co_queried_with" for (_s, r, _o, _w) in edges)
    assert not any(w == "jobs_30d" for (_s, _r, _o, w) in edges)
    assert nodes["table:dw.gms_transaction"].props["top_users"] == []
    manifest = json.loads((graph_dir / "runs" / "test_r1" /
                           "manifest.json").read_text())
    assert "jobs_30d" not in manifest["reports"]
    jobs_rows = [r for r in manifest["utilization"]
                 if "17_queries_30d" in r["path"]]
    assert jobs_rows
    assert all(r["status"] == "deferred" and "A8" in r.get("reason", "")
               for r in jobs_rows)


def test_torn_tail_repairs_on_next_append(tmp_path):
    """A run killed mid-append leaves a torn final line; the next
    appender truncates it to the .torn sidecar instead of gluing new
    records onto the fragment — the crash residue self-heals."""
    g = GraphDir(tmp_path)
    g.append_node(NodeRecord(id="table:dw.t_one", props={},
                             prov=Prov(source="bq", run="r1")))
    p = tmp_path / "nodes" / "table.jsonl"
    with p.open("a", encoding="utf-8") as f:
        f.write('{"id": "table:dw.t_two", "props": {"x": "trunca')
    g2 = GraphDir(tmp_path)
    g2.append_node(NodeRecord(id="table:dw.t_three", props={},
                              prov=Prov(source="bq", run="r2")))
    folded = g2.fold_nodes()
    assert "table:dw.t_one" in folded and "table:dw.t_three" in folded
    assert "table:dw.t_two" not in folded
    torn = (tmp_path / "nodes" / "table.jsonl.torn").read_text()
    assert "t_two" in torn                   # evidence preserved


def test_line_separator_chars_never_tear_records(tmp_path):
    """Real Atlas/BQ descriptions carry U+2028/U+2029/NEL. json.dumps
    writes them RAW and splitlines() treats them as line breaks — a
    valid record read back as two fragments ('Unterminated string').
    The writer escapes them; the reader splits on real newlines only,
    so even a LEGACY raw line parses whole."""
    hostile = "spend across markets\x85daily"
    g = GraphDir(tmp_path)
    g.append_node(NodeRecord(id="table:dw.t_one",
                             props={"description_bq": hostile},
                             prov=Prov(source="bq", run="r1")))
    p = tmp_path / "nodes" / "table.jsonl"
    raw = p.read_text(encoding="utf-8")
    assert raw.count("\n") == 1 and " " not in raw   # one clean line
    # legacy line written by the OLD writer: separator raw inside JSON
    with p.open("a", encoding="utf-8") as f:
        f.write('{"id": "table:dw.t_two", '
                '"props": {"d": "a b"}, '
                '"prov": {"source": "bq", "run": "r0"}}\n')
    folded = GraphDir(tmp_path).fold_nodes()
    assert folded["table:dw.t_one"].props["description_bq"] == hostile
    assert folded["table:dw.t_two"].props["d"] == "a b"


def test_mid_file_corruption_raises_named_error(tmp_path):
    g = GraphDir(tmp_path)
    g.append_node(NodeRecord(id="table:dw.t_one", props={},
                             prov=Prov(source="bq", run="r1")))
    p = tmp_path / "nodes" / "table.jsonl"
    with p.open("a", encoding="utf-8") as f:
        f.write('{"id": "table:dw.t_two", "broken\n')   # complete line
    g.append_node(NodeRecord(id="table:dw.t_three", props={},
                             prov=Prov(source="bq", run="r1")))
    try:
        list(g.iter_nodes())
        raise AssertionError("corrupt line must not pass silently")
    except ValueError as e:
        assert "nodes/table.jsonl:2" in str(e)
        assert "build-graph" in str(e)       # recovery named, not vague


def test_id_slugging_for_source_embedded_strings():
    """Ids embedding source-provided text slug hostile characters to
    `_` at the mint — a real mined catalog id carries `||` in itself."""
    from sahs.graph.ids import mgroup_id, owner_id, term_node_id
    m = mgroup_id("mined:count_distinct_a_hi||a_low")
    assert m == "mgroup:mined:count_distinct_a_hi__a_low"
    assert kind_of(m) == "mgroup"
    assert mgroup_id("mined:m_gms_spend_usd") == \
        "mgroup:mined:m_gms_spend_usd"          # clean keys untouched
    assert kind_of(term_node_id("BT-0017/X")) == "term"
    assert kind_of(owner_id("William Lentz II")) == "owner"


def manifest_reports(tmp_path: Path) -> dict:
    manifest = json.loads((tmp_path / "g" / "runs" / "test_r1" /
                           "manifest.json").read_text())
    return manifest["reports"]


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

    # the ||-bearing mined id lands as a slugged mgroup with the raw
    # key preserved in props
    slugged = "mgroup:mined:count_distinct_post_visitor_id_hi" \
              "__post_visitor_id_low"
    assert slugged in nodes
    assert nodes[slugged].props["group_key"] == \
        "mined:count_distinct_post_visitor_id_hi||post_visitor_id_low"

    # a column the 02 schema listing missed but the profiler saw:
    # observed values attest existence — minted from profile evidence
    zz = nodes["col:dw.gms_transaction.zz_profile_only"]
    assert zz.props["observed_via"] == "low_cardinality_profile"
    assert ("table:dw.gms_transaction", "has_column",
            "col:dw.gms_transaction.zz_profile_only", "bq") in edges
    assert "domain:dw.gms_transaction.zz_profile_only" in nodes
    assert manifest_reports(tmp_path)["bq"][
        "columns_from_profile_only"] == 1

    # catalogs attribute by data-product names: the alias sidecar maps
    # them onto crosswalked identities, and a metric whose hint fails
    # falls back to the tables its OWN SQL references — so the
    # certified plane lands instead of being scope-skipped
    expr = manifest_reports(tmp_path)["expressions"]
    assert expr["resolved_via_alias"] >= 1        # dmp product hint
    assert expr["resolved_via_sql_table"] >= 1    # gmns FROM fallback
    assert "mgroup:dmp:prodhint-0001" in nodes    # certified plane lands

    # gms's DOUBLE registration folds at emit: entries counted, facts
    # emitted once — [8] dedup stays meaningful (rc==0 proves no dupes)
    manifest = json.loads((tmp_path / "g" / "runs" / "test_r1" /
                           "manifest.json").read_text())
    std = manifest["reports"]["std_tech"]
    assert std["entries"] == 3 and std["tables"] == 2
    assert std["repeat_registrations"] == 1

    # a dir missing its 00 resource resolves by UNIQUE short name —
    # views and denied resource calls ship without the file, and the
    # crosswalk row is identity enough; its schema still lands
    assert "table:dw.sbs_new_accounts" in nodes
    assert ("table:dw.sbs_new_accounts", "has_schema",
            "schema:dw.sbs_new_accounts@v1", "bq") in edges


def test_alias_to_unknown_physical_dies_at_load(tmp_path):
    """An alias resolves an identity, it never mints one — a typo'd
    physical corrupts attribution, so the load refuses it loudly."""
    cw = tmp_path / "crosswalk.jsonl"
    cw.write_text(json.dumps({
        "physical": "dw.gms_transaction", "verified_by": "t",
        "verified_on": "2026-08-26"}) + "\n", encoding="utf-8")
    (tmp_path / "aliases.jsonl").write_text(json.dumps({
        "alias": "Some Product", "physical": "dw.not_a_table"}) + "\n",
        encoding="utf-8")
    try:
        Crosswalk.load(cw)
        raise AssertionError("unknown alias target must not load")
    except ValueError as e:
        assert "not a crosswalk row" in str(e)


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
    # full-utilization end-state: ZERO inventoried files in the fixture
    # tree — every artifact is consumed or carries a pinned reason
    inventoried = {p for p, r in by_path.items()
                   if r["status"] == "inventoried"}
    assert not inventoried, inventoried
    # the new deferral families, each teaching its lesson
    twin = by_path["real_extractions_production/gms_transaction/"
                   "13_table_metrics.json"]
    assert twin["status"] == "deferred" and "format twin" in twin["reason"]
    phys = by_path["real_extractions_production/gms_transaction/"
                   "12_physical_constraints.json"]
    assert phys["status"] == "deferred" \
        and "physical-layer twin" in phys["reason"]
    bak = by_path["real_extractions_production/gms_transaction/"
                  "02_logical_columns.csv.bak"]
    assert bak["status"] == "deferred" and "backup" in bak["reason"]
    specs = by_path["sources/skills/NewAccountsSkills/"
                    "SBS_NewAccountsApprovalRate/data_specs.md"]
    assert specs["status"] == "deferred" and "prose" in specs["reason"]
    for wired in ("01_logical_table_meta.csv",
                  "03_logical_column_field_paths.csv",
                  "11_logical_constraints.json"):
        assert by_path[f"real_extractions_production/gms_transaction/"
                       f"{wired}"]["status"] == "consumed", wired


def test_lob_layer_steward_declares_catalogs_corroborate(tmp_path):
    """The LOB layer is witnessed classification: the steward map and
    the dmp catalog testify per family (one in_lob quad each), mined
    business_unit values only corroborate an EXISTING lob node, and an
    unknown one is counted `lob_unmatched` — never guessed into the
    graph. Multi-membership is edges, not a winner-take-all field."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir)
    assert result.returncode == 0, result.stderr[-800:]
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    assert nodes["lob:gmns"].props["name"] == \
        "Global Merchant & Network Services"
    assert nodes["mdom:merchant"].props["name"] == "Merchant"
    gms_lob = {w: q for (s, r, o, w), q in edges.items()
               if r == "in_lob" and s == "table:dw.gms_transaction"
               and o == "lob:gmns"}
    assert gms_lob["steward"].prov.source == "lob_map"   # human declares
    assert "dmp" in gms_lob                # certified catalog testifies
    assert "catalog_mined" in gms_lob      # mined corroborates
    # metric → domain → lob chain (cross-domain joins route through it)
    assert any(r == "in_domain" and o == "mdom:merchant"
               for (s, r, o, _w) in edges)
    assert ("mdom:merchant", "in_lob", "lob:gmns", "dmp") in edges
    # mined never mints: CRO measures land in the counter, not the graph
    assert "lob:cro" not in nodes
    manifest = json.loads((graph_dir / "runs" / "test_r1" /
                           "manifest.json").read_text())
    assert manifest["reports"]["lob_map"] == {
        "lobs": 2, "memberships": 3, "duplicate_rows": 0}
    ex = manifest["reports"]["expressions"]
    assert ex["lob_corroborated_mined"] >= 1
    assert ex["lob_unmatched"] >= 1


def test_constraints_meta_and_field_paths_wired(tmp_path):
    """11 → PK props + fk_references (referenced table resolves through
    the crosswalk or the edge is a counted skip); 01 and the full 13
    first rows ride on the table node; 03 nested STRUCT paths become
    col nodes of their own — top-level twins skipped, dupes deduped."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir)
    assert result.returncode == 0, result.stderr[-800:]
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    assert nodes["col:dw.gms_transaction.se_no"].props[
        "is_primary_key"] is True
    minted = nodes["col:dw.gms_transaction.txn_uid"].props
    assert minted["is_primary_key"] is True
    assert minted["observed_via"] == "constraint_declaration"
    fk = [(s, o) for (s, r, o, _w) in edges if r == "fk_references"]
    assert fk == [("col:dw.gms_transaction.cm13",
                   "col:dw.wwcas_authorization.card_no")]
    nested = nodes["col:dw.gms_transaction.payment_detail.card.network"]
    assert nested.props["nested_path"] is True
    assert nested.props["data_type"] == "STRING"
    assert ("table:dw.gms_transaction", "has_column",
            "col:dw.gms_transaction.payment_detail.card.network",
            "bq") in edges
    props = nodes["table:dw.gms_transaction"].props
    assert props["table_meta_logical"]["application"] == "lumi"
    assert props["table_metrics"]["table_size_bytes"] == "987654321"
    manifest = json.loads((graph_dir / "runs" / "test_r1" /
                           "manifest.json").read_text())
    bq = manifest["reports"]["bq"]
    assert bq["pk_columns"] == 2
    assert bq["fk_edges"] == 1
    assert bq["fk_out_of_scope"] == 1          # merchant_dim: not ours
    assert bq["cols_minted_from_constraints"] == 1
    assert bq["nested_columns"] == 1
    assert bq["constraints_unrecognized"] == 0


def test_csv_reader_tolerates_giant_fields(tmp_path):
    """Python's csv module refuses any field over 128KB by default —
    real 01_logical_table_meta exports carry multi-hundred-KB cells
    (labels/options blobs). The loader adapts to the file: the raised
    limit reads the row whole, and the props harvest truncates the
    giant cell with byte count + hash instead of shipping it verbatim
    into the append-only store."""
    from sahs.loaders.archives.bq_extraction import (
        _csv_rows,
        _first_row_props,
    )
    big = "x" * 300_000
    p = tmp_path / "01_logical_table_meta.csv"
    p.write_text(
        f'table_name,options\ngms_transaction,"{big}"\n',
        encoding="utf-8")
    rows = _csv_rows(p)                       # would raise _csv.Error
    assert rows[0]["options"] == big          # read whole, untouched
    props = _first_row_props(rows)
    assert props["table_name"] == "gms_transaction"
    assert len(props["options"]) < 3000
    assert "truncated: 300000 bytes" in props["options"]
    assert "sha256_12=" in props["options"]
