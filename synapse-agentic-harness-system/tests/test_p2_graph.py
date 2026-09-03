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


def test_std_tech_full_utilization_reaches_the_graph(tmp_path):
    """Every field the Atlas loader parses lands as a prop or an edge.
    The audit that motivated this (docs/audits/
    card_sourcing_audit_2026_09.md) found ~20 documented fields going
    dark between the file and the graph; this is the fence."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    assert _build(graph_dir, out_dir).returncode == 0
    graph = GraphDir(graph_dir)
    nodes, edges = graph.fold_nodes(), graph.fold_edges()

    table = nodes["table:dw.gms_transaction"].props
    # where the table LIVES (envelope + Layer 2) — the qualified-name
    # pieces the graph could not previously state
    assert table["appl_id"] == "600001868"
    assert table["project_atlas"] == "axp-lumi"
    assert table["dataset_group_atlas"] == "data"
    assert table["technology_atlas"] == "BigQuery"
    assert table["is_active_atlas"] is True
    assert table["is_lineage_exist_atlas"] is True
    # how it is BUILT (Layer 3)
    assert table["data_sub_category"] == "Payments"
    assert table["table_type_atlas"] == "DERIVED"
    assert table["load_type_atlas"] == "LOAD_APPEND"
    assert table["is_partitioned_atlas"] is True
    assert table["target_system_atlas"] == "Lumi BigQuery"
    # an unsent flag stays absent rather than becoming a false denial
    assert "is_active_atlas" not in nodes[
        "table:dw.wwcas_authorization"].props

    # ownership is EDGES now, so atlas and lumi corroborate on one
    # owner node instead of atlas ownership sitting inert in a dict
    assert ("table:dw.gms_transaction", "owned_by",
            "owner:own_a@corp", "atlas") in edges
    assert ("table:dw.gms_transaction", "owned_by",
            "owner:own_a@corp", "lumi") in edges
    assert ("table:dw.gms_transaction", "owned_by",
            "owner:vp_a@corp", "atlas") in edges
    # a CAR id is an identifier, not an owner — it stays a prop
    assert "owner:car-1" not in nodes
    assert nodes["table:dw.gms_transaction"].props[
        "ownership_atlas"]["car_id"] == "CAR-1"

    # table-level has_pii finally emits its policy edge (only
    # oncop/gdpr did before — the strongest flag was the silent one)
    assert ("table:dw.gms_transaction", "has_policy",
            "policy:pii", "atlas") in edges

    # pii_columns[] is a SECOND PII witness: it names a column the pde
    # listing never carried, and the declaration mints the endpoint
    hashed = nodes["col:dw.gms_transaction.cm15_hash"].props
    assert hashed["pii_role_id"] == "R4"
    assert hashed["observed_via"] == "table_pii_declaration"
    assert ("col:dw.gms_transaction.cm15_hash", "has_policy",
            "policy:pii", "atlas") in edges

    # every documented pdeAttribute field on the column node
    amount = nodes["col:dw.gms_transaction.trans_usd_am"].props
    assert amount["column_length"] == 18
    assert amount["nullable_atlas"] is True
    assert amount["is_primary_key_atlas"] is False
    assert amount["ordinal_atlas"] == 5           # bq says 1 — drift,
    assert amount["ordinal"] == 1                 # now visible
    assert nodes["col:dw.wwcas_authorization.approval_cd"].props[
        "is_primary_key_atlas"] is True

    # derived_logic rides WHOLE as doc evidence (the view-SQL pattern)
    doc = [o for (s, r, o, _w) in edges
           if r == "described_by"
           and s == "col:dw.gms_transaction.trans_usd_am"]
    assert len(doc) == 1
    assert nodes[doc[0]].props["kind"] == "derived_logic"
    assert nodes[doc[0]].props["sql"].startswith("CASE WHEN se_cr_dr_in")

    # the term plane: resolve on ID first, fall back to name, and carry
    # the DEFINITION — business_terms.csv is id+name+status only, so
    # businessTermDescription is the sole source of meaning we have
    assert nodes["term:atlas:8"].props["description"] == \
        "USD amount of a transaction"
    assert nodes["term:atlas:8"].props["status"] == "Approved"   # merged
    link = edges[("col:dw.gms_transaction.trans_usd_am", "mapped_term",
                  "term:atlas:8", "atlas")]
    assert link.props["matched_on"] == "id"
    assert link.props["confidence"] == 0.95
    # a term Atlas declares by id that the glossary export has not
    # shipped is minted rather than dropped
    assert nodes["term:atlas:901"].props["name"] == \
        "Card Member 13 Digit Number"
    assert edges[("col:dw.wwcas_authorization.approval_cd",
                  "mapped_term", "term:atlas:10",
                  "atlas")].props["matched_on"] == "name"

    report = json.loads((graph_dir / "runs" / "test_r1" /
                         "manifest.json").read_text())["reports"]
    std = report["std_tech"]
    assert std.get("term_links_unmatched", 0) == 0   # id-first matching
    assert std["ownership_edges"] == 3
    assert std["derived_logic_docs"] == 1
    assert std["columns_from_pii_declaration"] == 1
    assert std["terms_minted_from_link"] == 2


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
    # run-2 audit findings: the value-profile manifests defer with a
    # reason; a view shipping its SQL only as a csv twin is CONSUMED
    manifest_row = by_path["real_extractions_production/"
                           "gms_transaction/"
                           "15_low_cardinality_manifest.csv"]
    assert manifest_row["status"] == "deferred" \
        and "profiling coverage" in manifest_row["reason"]
    assert by_path["real_extractions_production/sbs_new_accounts/"
                   "05_view_definition.csv"]["status"] == "consumed"


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
    # ownership is steward+catalog testimony ONLY — the mined witness
    # moved to the usage plane (used_by), never in_lob
    assert "catalog_mined" not in gms_lob
    # declared equivalence: the DMP display name ("Global Merchant &
    # Network Svcs", fixture metric 102) resolves onto the steward's
    # canonical lob:gmns via the lob_map alias — NO parallel node forks
    assert "lob:global_merchant___network_svcs" not in nodes
    assert nodes["lob:gmns"].props["code"] == "GMNS"     # never clobbered
    # metric → domain → lob chain (cross-domain joins route through it)
    assert any(r == "in_domain" and o == "mdom:merchant"
               for (s, r, o, _w) in edges)
    assert ("mdom:merchant", "in_lob", "lob:gmns", "dmp") in edges
    # the usage plane: mined business_unit names WHO QUERIES — GMNS
    # measures → used_by the LOB's own org; CRO measures → used_by the
    # steward-declared org unit (child of SBS), never ownership
    assert ("table:dw.gms_transaction", "used_by", "lob:gmns",
            "catalog_mined") in edges
    assert ("table:dw.wwcas_authorization", "used_by", "lob:cro",
            "catalog_mined") in edges
    cro = nodes["lob:cro"]
    assert cro.props["kind"] == "org_unit"
    assert cro.props["parent"] == "SBS"
    assert ("lob:cro", "in_lob", "lob:sbs", "steward") in edges
    manifest = json.loads((graph_dir / "runs" / "test_r1" /
                           "manifest.json").read_text())
    assert manifest["reports"]["lob_map"] == {
        "lobs": 2, "memberships": 3, "duplicate_rows": 0}
    assert manifest["reports"]["org_map"] == {"org_units": 1}
    ex = manifest["reports"]["expressions"]
    assert ex["used_by_edges"] >= 2
    assert ex.get("usage_unmatched", 0) == 0   # every fixture value maps


def test_studio_export_fuses_docs_and_mined_scoped_joins(tmp_path):
    """The raw Studio CSV consumed whole: same id + same SQL FUSES onto
    the canonical metric (a new `studio` witness family, never a
    duplicate node); same id + different SQL lands as a flagged second
    class; the full referenced SQL — from the studio export AND from
    dmp's own referencedSqlQuery — rides whole on ONE shared doc node;
    grain/query-shape/lineage-delta/data-owners ride as texture props
    (grain NEVER enters identity); joins are mined IN-SILO from the SQL
    (CTE-aware) into joins_via edges that are scoped_only by design,
    with self-join patterns counted, never edges."""
    graph_dir, out_dir = tmp_path / "g", tmp_path / "run"
    result = _build(graph_dir, out_dir)
    assert result.returncode == 0, result.stderr[-800:]
    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()

    # fusion: ONE metric node carries dmp + studio witnesses
    spend_members = {(s, w) for (s, r, o, w) in edges
                     if r == "member_of" and o == "mgroup:dmp:101"}
    spend_ids = {s for s, _w in spend_members}
    assert len(spend_ids) == 1                 # no parallel node minted
    assert {w for _s, w in spend_members} >= {"dmp", "studio"}
    (spend,) = spend_ids

    # ONE doc node, TWO witnessing sources (dmp's referencedSqlQuery
    # and the studio row carry the same author SQL → same fp → merge)
    doc_ids = {o for (s, r, o, _w) in edges
               if r == "evidenced_by" and s == spend
               and o.startswith("doc:referenced_sql_")}
    assert len(doc_ids) == 1
    doc = nodes[next(iter(doc_ids))]
    assert doc.props["kind"] == "referenced_sql"
    assert doc.props["sql"].startswith("WITH txn AS")
    assert "se_cr_dr_in = 'C'" in doc.props["sql"]    # bytes intact
    doc_witnesses = {w for (s, r, o, w) in edges
                     if r == "evidenced_by" and s == spend}
    assert {"dmp", "studio"} <= doc_witnesses

    # conflict: different SQL under the same catalog id → SECOND class
    txn_members = {(s, w) for (s, r, o, w) in edges
                   if r == "member_of" and o == "mgroup:dmp:102"}
    txn_ids = {s for s, _w in txn_members}
    assert len(txn_ids) >= 2                   # retained, never merged
    studio_class = next(s for s, w in txn_members if w == "studio")
    assert "distinct" in \
        nodes[studio_class].props["canonical_sql"].lower()
    # texture props ride the node; grain stays OUT of identity
    sprops = nodes[studio_class].props
    assert sprops["grain_observed"] == "card member x day"
    assert sprops["query_shape"] == ["CTE", "SUBQUERY"]
    assert sprops["data_owners"] == ["ops@example.com"]
    assert sprops["tables_associated_not_referenced"] == \
        ["wwcas_authorization"]
    # dmp's own unread field lands on the certified class
    dmp_class = next(s for s, w in txn_members if w == "dmp")
    assert nodes[dmp_class].props["join_condition"] == \
        "gms_transaction.cm13 = wwcas_authorization.card_no"

    # novel id → MINED candidate on the resolved table
    novel = {s for (s, r, o, _w) in edges
             if r == "member_of" and o == "mgroup:dmp:stud-901"}
    assert len(novel) == 1

    # in-silo mined join: CTE-aware, scoped_only, both keys, support =
    # witnessing metrics; the self-join pattern is counted, never an edge
    jw = edges[("table:dw.gms_transaction", "joins_via",
                "table:dw.wwcas_authorization", "studio")]
    assert jw.props["scope"] == "scoped_only"
    assert jw.props["on"] == ["cm13 = card_no", "offr_id = offr_id"]
    assert jw.props["join_type"] == "LEFT"
    assert jw.props["witness_metrics"] == ["102"]
    assert jw.prov.support == 1
    manifest = json.loads((graph_dir / "runs" / "test_r1" /
                           "manifest.json").read_text())
    assert manifest["reports"]["studio_joins"] == {
        "join_edges": 1, "pattern_only": 1, "join_unresolved": 0,
        "join_out_of_scope": 0, "sql_parse_failures": 0}
    assert ("table:dw.gms_transaction", "joins_via",
            "table:dw.gms_transaction", "studio") not in edges
    # custody: the raw export is CONSUMED, not deferred or inventoried
    by_path = {r["path"]: r for r in manifest["utilization"]
               if r["root"] == "sources"}
    assert by_path["studio_results_20260827_fixture_"
                   "cte_or_subqueries.csv"]["status"] == "consumed"


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
    # a view whose SQL ships only as a csv twin still lands its doc
    assert bq["view_sql_from_twin"] == 1
    twin_doc = nodes["doc:view_sql_dw_sbs_new_accounts"]
    assert "FROM dw.sbs_new_accounts_raw" in twin_doc.props["sql"]
    assert ("table:dw.sbs_new_accounts", "described_by",
            "doc:view_sql_dw_sbs_new_accounts", "bq") in edges


def test_csv_reader_tolerates_giant_fields(tmp_path):
    """Python's csv module refuses any field over 128KB by default —
    real 01_logical_table_meta exports carry multi-hundred-KB cells
    (labels/options blobs). The loader adapts to the file AND loses
    nothing: the raised limit reads the row whole, and a giant cell
    moves VERBATIM into a doc node (the view-SQL pattern) while the
    inline prop keeps a preview + doc pointer + content hash."""
    from sahs.loaders.archives.bq_extraction import (
        _csv_rows,
        _first_row_props,
        _offload_giant_cells,
    )
    big = "x" * 300_000
    p = tmp_path / "01_logical_table_meta.csv"
    p.write_text(
        f'table_name,options\ngms_transaction,"{big}"\n',
        encoding="utf-8")
    rows = _csv_rows(p)                       # would raise _csv.Error
    assert rows[0]["options"] == big          # read whole, untouched

    graph = GraphDir(tmp_path / "g")
    props = _first_row_props(rows)
    report = {"giant_cells_offloaded": 0}
    _offload_giant_cells(
        graph, "table:dw.gms_transaction", "dw.gms_transaction",
        "table_meta_logical", props,
        lambda **kw: Prov(source="bq", run="r1", **kw),
        "gms_transaction/01_logical_table_meta.csv", report)
    assert report["giant_cells_offloaded"] == 1
    assert props["table_name"] == "gms_transaction"   # small cell as-is
    doc_id = "doc:table_meta_logical_dw_gms_transaction_options"
    assert doc_id in props["options"]                 # preview points
    assert "300000 bytes" in props["options"]
    nodes = graph.fold_nodes()
    assert nodes[doc_id].props["text"] == big         # VERBATIM, whole
    assert ("table:dw.gms_transaction", "described_by", doc_id,
            "bq") in graph.fold_edges()
