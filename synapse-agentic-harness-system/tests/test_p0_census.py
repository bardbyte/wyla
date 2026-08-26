"""P0 gate: adapters, census determinism, CLI end-to-end on fixtures."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures" / "sources"
REGISTRY = FX / "tables_registry.txt"

sys.path.insert(0, str(SILO))

from sahs.canon.census import build_census, canonicalize_records  # noqa: E402
from sahs.loaders.registry import TableRegistry                    # noqa: E402
from sahs.loaders.sources.blue_insights import load_blue_insights  # noqa: E402
from sahs.loaders.sources.catalogs import (                        # noqa: E402
    load_extended_gmns,
    load_measures_catalog,
    load_metrics_dmp,
)
from sahs.loaders.sources.gold_queries import load_gold_queries    # noqa: E402
from sahs.loaders.sources.skills import load_skill_contracts       # noqa: E402
from sahs.loaders.sources.vocab import (                           # noqa: E402
    load_business_terms,
    load_glossary,
    load_std_tech_metadata,
)


def _cli(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), *argv],
        capture_output=True, text=True, cwd=SILO)


def test_registry_suffix_and_ambiguity():
    reg = TableRegistry.from_list_file(REGISTRY)
    assert reg.resolve("gms_transaction") == ("gms_transaction", "exact")
    assert reg.resolve("transaction")[1] == "suffix"
    assert reg.resolve("authorization") == (None, "ambiguous")
    assert reg.resolve("nope_table") == (None, "unknown")
    # real queries reference tables FULLY QUALIFIED — they resolve by
    # their table component; ambiguity still never guesses
    assert reg.resolve("`axp-lumi`.dw.gms_transaction") == \
        ("gms_transaction", "qualified")
    assert reg.resolve("dw.authorization") == (None, "ambiguous")
    assert reg.resolve("dw.nope_table") == (None, "unknown")


def test_blue_insights_quarantine_categories_tell_the_truth():
    reg = TableRegistry.from_list_file(REGISTRY)
    records, quar = load_blue_insights(
        FX / "blue_business_insights.csv", reg)
    assert len(records) == 43      # 46 rows − ambiguous − oos − prose
    by_cat: dict[str, list] = {}
    for q in quar:
        by_cat.setdefault(q.category, []).append(q)
    assert set(by_cat) == {"ambiguous_table", "out_of_scope", "not_sql"}
    # out-of-registry is a scope statement, not a missing field
    assert "unknown" in by_cat["out_of_scope"][0].detail
    assert "Cheque Cashing" in by_cat["not_sql"][0].detail
    # one bare word may be a boolean column — the parser judges those —
    # and signal-bearing junk (broken_1..5) must reach canon untouched
    assert any("is_active_flag" in r.raw_sql for r in records)
    assert any("and or not" in r.raw_sql for r in records)
    assert any(">>>" in r.raw_sql for r in records)
    kinds = {r.kind for r in records}
    assert kinds == {"predicate", "case"}
    # swapped-column rows recover: SQL from insight_name, label from
    # sql_logic, provenance flagged; both-broken rows still reach canon
    swapped = [r for r in records if r.extra.get("column_swap")]
    assert len(swapped) == 2
    assert {r.kind for r in swapped} == {"predicate"}
    assert any(r.concept_label == "SH On Card/ CNTR CHEQ."
               and "se_typ = 'G'" in r.raw_sql for r in swapped)
    assert any("case_setup_type in ('4407')" in r.raw_sql
               for r in swapped)          # predicate, not CASE-wrapped
    assert any("BUS Unit :: prose/ thing" in r.raw_sql
               for r in records)          # sql_logic side goes to canon


def test_gold_split_and_backlog():
    records, quar, backlog = load_gold_queries(
        FX / "extracted_gold_queries.json")
    assert len(records) == 10 and len(backlog) == 3 and not quar
    assert all(r.prompt for r in records)


def test_catalog_adapters_counts():
    dmp, q1 = load_metrics_dmp(FX / "metrics_dmp.json")
    gmns, q2 = load_extended_gmns(FX / "extended_gmns_semantics.json")
    mined, q3 = load_measures_catalog(FX / "measures_catalog.json")
    assert (len(dmp), len(gmns), len(mined)) == (3, 2, 31)
    assert not (q1 or q2 or q3)
    assert dmp[0].extra["question_answered"]
    assert mined[0].support > 1          # user_count scaling


def test_skills_contracts_extracted_knowledge_only_pack_skipped():
    records, quar = load_skill_contracts(FX / "skills")
    ids = {r.metric_ref for r in records}
    assert "skill:SBS_NewAccountsApprovalRate:approval_rate" in ids
    assert len(records) == 2 and not quar   # CPS pack has no contracts


def test_vocab_adapters_and_degenerates():
    glossary, gq = load_glossary(FX / "data_cleaned.csv")
    assert len(glossary) == 24 and len(gq) == 1      # orphan row
    dups = [g for g in glossary if g.symbol == "AA"]
    assert len(dups) == 3                             # BU-scoped meanings
    assert any(g.region == "EMEA" for g in dups)      # Global_Region header
    terms, tq = load_business_terms(FX / "business_terms.csv")
    assert len(terms) == 20 and len(tq) == 1          # bad-status row
    assert {t.status for t in terms} == {
        "Approved", "Candidate", "Under Review", "Rejected"}
    entries, sq = load_std_tech_metadata(FX / "std_tech_metadata")
    # the real feed registers a table more than once: gms carries TWO
    # registrations in one envelope (46 files → 70 entries on the
    # first real run) — the loader reports the feed as it is
    assert len(entries) == 3 and not sq
    assert sum(1 for e in entries if e.table == "gms_transaction") == 2
    gms = next(e for e in entries if e.table == "gms_transaction")
    assert gms.layer_type == "SOR" and gms.has_pii
    assert gms.columns[0].linked_terms[0]["sourceName"] == "LumiMDM"


def test_std_tech_combined_export_parity(tmp_path: Path):
    """The real Atlas feed can ship as ONE std_tech_metadata_all.json —
    a file path must load identically to the per-table directory."""
    combined = []
    for p in sorted((FX / "std_tech_metadata").glob("*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        combined.extend(payload if isinstance(payload, list) else [payload])
    all_file = tmp_path / "std_tech_metadata_all.json"
    all_file.write_text(json.dumps(combined), encoding="utf-8")
    from_file, fq = load_std_tech_metadata(all_file)
    from_dir, _ = load_std_tech_metadata(FX / "std_tech_metadata")
    assert not fq
    assert [(e.table, len(e.columns)) for e in from_file] == \
        [(e.table, len(e.columns)) for e in from_dir]
    # discovery: the combined file WINS over a (possibly partial) dir
    from scripts.laptop import _std_tech_path
    sources = tmp_path / "sources"
    (sources / "std_tech_metadata").mkdir(parents=True)
    assert _std_tech_path(sources) == sources / "std_tech_metadata"
    (sources / "std_tech_metadata_all.json").write_text("[]")
    assert _std_tech_path(sources) == \
        sources / "std_tech_metadata_all.json"


def test_census_finds_seeded_conflicts_and_meta_is_honest():
    reg = TableRegistry.from_list_file(REGISTRY)
    records = []
    records += load_blue_insights(FX / "blue_business_insights.csv", reg)[0]
    records += load_measures_catalog(FX / "measures_catalog.json")[0]
    records += load_metrics_dmp(FX / "metrics_dmp.json")[0]
    done, quar = canonicalize_records(records)
    census, tail = build_census(done, quar)
    assert census["summary"]["concept_conflicts"] == 2
    consumer = census["concepts"]["consumer@wwcas_authorization"]
    assert consumer["n_classes"] == 2 and consumer["entropy"] == 1.0
    # certified beats mined in class ranking for the same intent
    spend = census["metrics"]["total spend@gms_transaction"]
    assert spend["conflict"]
    assert "NOT alias/acronym-dedup" in census["meta"][
        "label_normalization"]
    assert census["meta"]["quarantine_by_category"]["parse_error"] == 6


def test_cli_census_gates_fire_and_outputs_written(tmp_path: Path):
    r = _cli("census", "--sources-dir", str(FX), "--registry",
             str(REGISTRY), "--out", str(tmp_path), "--plain", "--json",
             "--fresh")
    assert r.returncode == 1              # seeded blue failures trip the gate
    summary = json.loads(r.stdout)
    gates = {g["gate"]: g["passed"] for g in summary["gates"]}
    assert gates["blue_canon_rate"] is False
    assert gates["blocker_sources_100pct"] is True
    for name in ("census.json", "census_tail.jsonl", "quarantine.jsonl",
                 "coverage_crosstab.json", "events.jsonl"):
        assert (tmp_path / name).exists(), name
    events = [json.loads(x) for x in
              (tmp_path / "events.jsonl").read_text().splitlines()]
    assert events[0]["ev"] == "run_start"
    assert events[-1]["ev"] == "run_end"
    assert all(e["schema"] == "meridian.event/1" for e in events)


def test_cli_census_deterministic(tmp_path: Path):
    a, b = tmp_path / "a", tmp_path / "b"
    for out in (a, b):
        _cli("census", "--sources-dir", str(FX), "--registry",
             str(REGISTRY), "--out", str(out), "--plain", "--fresh")
    assert (a / "census.json").read_bytes() == (b / "census.json").read_bytes()


def test_cli_make_tasks(tmp_path: Path):
    r = _cli("make-tasks", "--sources-dir", str(FX), "--registry",
             str(REGISTRY), "--out", str(tmp_path), "--plain")
    assert r.returncode == 0
    tasks = [json.loads(x) for x in
             (tmp_path / "tasks" / "gold.jsonl").read_text().splitlines()]
    assert len(tasks) == 10
    assert all(t["schema"] == "meridian.task/1" for t in tasks)
    # a gold query on a FULLY QUALIFIED registry table is in-coverage —
    # the real answer key writes project.dataset.table, never bare names
    qual = next(t for t in tasks if t["provenance"]["source_id"] == "13")
    assert "coverage=internal" in qual["tags"]
    assert all(t["gold"]["canonical_fp"] in t["grading"]["accepted_fps"]
               for t in tasks)
    assert all(t["grading"]["dry_run"] == "required" for t in tasks)
    backlog = (tmp_path / "triage" / "empty_sql_backlog.jsonl"
               ).read_text().splitlines()
    assert len(backlog) == 3
    assert all(json.loads(x)["triage"] == "pending" for x in backlog)


def test_std_tech_harvest_survives_any_wrapper(tmp_path: Path):
    """The real Atlas export may wrap entries in any envelope — the
    loader harvests by SIGNATURE (dataset + pde/datasetAttribute), so
    every wrapper shape parses identically to the per-table directory."""
    entries = []
    for p in sorted((FX / "std_tech_metadata").glob("*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        entries.extend(payload if isinstance(payload, list) else [payload])
    baseline, _ = load_std_tech_metadata(FX / "std_tech_metadata")
    shapes = {
        "plain_list.json": entries,
        "data_wrapper.json": {"data": entries, "total": len(entries)},
        "keyed_by_table.json": {e["dataset"]: e for e in entries},
        "deep_envelope.json": [{"searchResults": {"page": 1,
                                                  "items": entries}}],
    }
    for name, payload in shapes.items():
        f = tmp_path / name
        f.write_text(json.dumps(payload), encoding="utf-8")
        got, quarantined = load_std_tech_metadata(f)
        assert not quarantined, (name, quarantined)
        assert [(e.table, len(e.columns)) for e in got] == \
            [(e.table, len(e.columns)) for e in baseline], name
    # a shape with NO entry-signature objects fails LOUDLY, never silently
    bad = tmp_path / "opaque.json"
    bad.write_text(json.dumps({"summary": {"count": 46}}))
    got, quarantined = load_std_tech_metadata(bad)
    assert not got and len(quarantined) == 1
    assert "no entry-shaped objects" in quarantined[0].detail


def test_std_tech_real_envelope_shape(tmp_path: Path):
    """The REAL per-table file (contract: docs/contracts/
    std_tech_metadata_layout.md): table name at the envelope, payload
    under tech_metadata_list[i] — incl. multi-element lists and the
    oncop/gdpr sensitivity flags."""
    envelope = {
        "dataset": "acqdw_acquisition_us",
        "appl_id": "600001868",
        "page_info": {"total_pages": 1, "downloaded_elements": 2},
        "tech_metadata_list": [
            {"datasource": "axp-lumi", "technology": "BigQuery",
             "isActive": "Y",
             "datasetAttribute": {
                 "business_name": "Acquisitions data for US market",
                 "description": "Acquisition data of Consumer and Open.",
                 "data_category": "Cobrand & Partners",
                 "data_type_name": "ODL",
                 "has_pii": True, "has_oncop": True, "has_gdpr": False,
                 "ownership": {"business_owner": "own@corp"}},
             "pde": [
                 {"pdeRelPath": "acct_open_dt",
                  "pdeAttribute": {"data_type_name": "DATE",
                                   "description": "account open date",
                                   "business_name": "Account Open Date",
                                   "pii_role_id": None},
                  "businessMetadata": [
                      {"businessTermName": "Account Open Date",
                       "businessTermId": None,
                       "sourceName": "LumiMDM",
                       "sourceType": "Declared"}]},
                 {"pdeRelPath": "cm_dob",
                  "pdeAttribute": {"data_type_name": "STRING",
                                   "pii_role_id":
                                       "NGBD-SDE-Date-of-Birth"}}]},
            {"datasource": "axp-lumi",
             "datasetAttribute": {"data_type_name": "ODL",
                                  "has_pii": False},
             "pde": []},
        ]}
    f = tmp_path / "acqdw_acquisition_us.json"
    f.write_text(json.dumps(envelope), encoding="utf-8")
    entries, quarantined = load_std_tech_metadata(f)
    assert not quarantined
    assert len(entries) == 2                  # every tech entry harvested
    first = entries[0]
    assert first.table == "acqdw_acquisition_us"   # envelope name flows down
    assert first.layer_type == "ODL" and first.has_pii
    assert first.has_oncop is True and first.has_gdpr is False
    assert [c.name for c in first.columns] == ["acct_open_dt", "cm_dob"]
    assert first.columns[1].pii_role_id == "NGBD-SDE-Date-of-Birth"
    assert first.columns[0].linked_terms[0]["sourceName"] == "LumiMDM"


def test_std_tech_loose_types_never_crash(tmp_path: Path):
    """The real feed sends sde_group/pii_role_id as `false` for absent
    (crashed the first laptop census); loose values normalize and a
    genuinely malformed entry QUARANTINES instead of killing the run."""
    envelope = {
        "dataset": "t_loose", "tech_metadata_list": [
            {"datasetAttribute": {"data_type_name": "ODL",
                                  "has_pii": False},
             "pde": [{"pdeRelPath": "col_a",
                      "pdeAttribute": {"data_type_name": "STRING",
                                       "sde_group": False,
                                       "pii_role_id": False}},
                     {"pdeRelPath": "col_b",
                      "pdeAttribute": {"sde_group": True,
                                       "pii_role_id": "R9"}}]},
            {"datasetAttribute": {"ownership": "not-a-dict",
                                  "has_pii": False},
             "pde": [{"pdeRelPath": "x",
                      "pdeAttribute": {},
                      "businessMetadata": ["not-a-dict-term"]}]},
        ]}
    f = tmp_path / "t_loose.json"
    f.write_text(json.dumps(envelope), encoding="utf-8")
    entries, quarantined = load_std_tech_metadata(f)
    assert len(entries) == 2 and not quarantined
    a, b = entries[0].columns
    assert a.sde_group is None and a.pii_role_id is None   # false → absent
    assert b.sde_group == "true" and b.pii_role_id == "R9"
    assert entries[1].ownership == {}          # non-dict → empty, counted
    assert entries[1].columns[0].linked_terms == []
    # a truly unparsable entry quarantines, never raises
    poison = {"dataset": "t_bad",
              "tech_metadata_list": [
                  {"datasetAttribute": {"has_pii": False}, "pde": "nope"}]}
    f2 = tmp_path / "t_bad.json"
    f2.write_text(json.dumps(poison), encoding="utf-8")
    entries2, quarantined2 = load_std_tech_metadata(f2)
    assert not entries2 and len(quarantined2) == 1
    assert quarantined2[0].category == "schema_mismatch"
