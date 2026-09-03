"""Vocabulary and values (docs/specs/vocabulary_and_values.md): the
common-word guard, the glossary view's drift, the value-meaning index
— through the loaders, the graph, the compiled build, the tools, the
literal hook, and the digest, against the real fixture sources."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
SRC = FX / "sources"
sys.path.insert(0, str(SILO))


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("vocabvalues")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(SRC),
         "--registry", str(SRC / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "vv"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    manifest = json.loads(next(
        (graph_dir / "runs").glob("*/manifest.json")).read_text())
    return Build.open(tmp / "builds"), graph_dir, manifest


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ─── the loaders: a guard list, a view's drift, a meaning index ──


def test_loaders_read_the_three_new_sources():
    from sahs.loaders.sources.vocab import (glossary_drift,
                                            load_common_words,
                                            load_glossary,
                                            load_value_lookup)
    symbols, quarantined = load_common_words(
        SRC / "potential_common_word_acronyms.csv")
    assert symbols == {"CASE", "POT", "YES"} and quarantined == []

    meanings, quarantined = load_value_lookup(
        SRC / "low_cardinality_synonyms_index.json")
    assert len(meanings) == 8
    assert [q.category for q in quarantined] == ["missing_field"]
    us = next(m for m in meanings if m.value == "US")
    assert (us.table, us.column, us.synonym) == (
        "dw.gms_transaction", "country_cd", "United States")

    corpus, _ = load_glossary(SRC / "data_cleaned.csv")
    drift = glossary_drift(SRC / "glossary_terms.csv", corpus)
    assert drift["view_rows"] == 4
    assert drift["matched_exact"] == 2      # Roll Rate, Write-Off
    assert drift["drifted"] == 1            # Net Sales: NBSP + blank BU
    assert drift["missing"] == 1            # Charge Volume
    assert any("Net Sales" in e for e in drift["examples"])


# ─── the graph: flags on nodes, meanings on domains, never twice ─


def test_graph_carries_the_guard_and_the_meanings(built):
    _build, graph_dir, manifest = built
    reports = manifest.get("reports") or {}
    vocab = reports["vocab"]
    assert vocab["acronyms"] == 28
    assert vocab["common_word_acronyms"] == 2       # CASE, POT
    assert vocab["common_words_unknown"] == 1       # YES is not in the corpus
    assert vocab["glossary_view_drift"]["drifted"] == 1
    values = reports["value_meanings"]
    assert values["domains_annotated"] == 2         # country_cd, approval_cd
    assert values["domains_minted"] == 1            # bq_only_col
    assert values["skipped_unknown_table"] == 1
    assert values["skipped_unknown_column"] == 1
    assert values["meanings"] == 6
    from sahs.graph.ids import acr_id
    acrs = {n["id"]: n for n in _jsonl(graph_dir / "nodes" / "acr.jsonl")}
    assert acrs[acr_id("CASE", "Technology", "All")]["props"][
        "common_word"] is True
    assert acrs[acr_id("ALIF", "GMNS", "All")]["props"][
        "common_word"] is False
    domains = [n for n in _jsonl(graph_dir / "nodes" / "domain.jsonl")
               if n["id"] == "domain:dw.gms_transaction.country_cd"]
    assert any(n["props"].get("meanings") for n in domains)
    lumi = [n for n in domains if n["prov"]["source"] == "value_lookup"]
    assert lumi and lumi[0]["prov"]["witness"] == "lumi"
    # the ledger: consumed, consumed, deferred with its reason
    by_path = {f"{r['root']}/{r['path']}": r
               for r in manifest["utilization"]}
    assert by_path["sources/potential_common_word_acronyms.csv"][
        "status"] == "consumed"
    assert by_path["sources/low_cardinality_synonyms_index.json"][
        "status"] == "consumed"
    view = by_path["sources/glossary_terms.csv"]
    assert view["status"] == "deferred" and "never loaded twice" \
        in view["reason"]


# ─── the build: indexes and sample_values ────────────────────


def test_build_serves_meanings_and_the_flag(built):
    from sahs.tools.api import sample_values
    build, _graph, _m = built
    assert len(build.value_meanings) == 6
    assert {"table": "dw.gms_transaction", "column": "country_cd",
            "value": "US", "synonym": "United States"} in build.value_meanings
    case = next(v for v in build.vocab if v.get("text") == "CASE")
    assert case["common_word"] is True
    alif = next(v for v in build.vocab if v.get("text") == "ALIF")
    assert alif["common_word"] is False
    got = sample_values(build, "gms_transaction", "country_cd")
    assert [v["value"] for v in got["values"]][:1] == ["US"]
    assert {"value": "US", "synonym": "United States"} in got["meanings"]
    minted = sample_values(build, "gms_transaction", "bq_only_col")
    assert minted["values"] == [] and minted["meanings"] == [
        {"value": "1", "synonym": "Flag set"}]
    assert "meanings on record" in minted["coverage_note"]


# ─── the tools: the guard, the values lane, the hook, the digest ─


def test_search_guards_common_words_and_resolves_phrases(built):
    from sahs.loop.tools import LoopState, toolkit
    build, _graph, _m = built
    tools = toolkit(build, LoopState())
    search = tools["search_semantics"].fn
    # "case" as a word: no expansion; CASE as an acronym: expanded and
    # marked; asked for vocabulary explicitly: shown with the guard
    plain = search("a case study in technology")
    assert not [r for r in plain["results"]
                if r["kind"] == "vocab" and r["text"] == "CASE"]
    upper = search("CASE tools in technology")
    hit = next(r for r in upper["results"]
               if r["kind"] == "vocab" and r["text"] == "CASE")
    assert hit["common_word"] is True and "ordinary word" in hit["guard"]
    explicit = search("case", kind="vocab")
    assert any(r["text"] == "CASE" for r in explicit["results"])
    # an acronym that is not a common word expands as before
    alif = search("ALIF by month")
    assert any(r["kind"] == "vocab" and r["text"] == "ALIF"
               for r in alif["results"])
    # a phrase is a stored code somewhere
    got = search("customers in the United States")
    value = next(r for r in got["results"] if r["kind"] == "value")
    assert value["predicate"] == "country_cd = 'US'"
    assert value["table"] == "dw.gms_transaction"
    approved = search("approved", kind="values")
    assert [r["predicate"] for r in approved["results"]] == [
        "approval_cd = 'A'"]
    assert "unknown kind" in search("x", kind="nope")["error"]
    assert "values" in search("x", kind="nope")["hint"]


def test_kit_search_passes_the_values_lane(built, tmp_path):
    from sahs.assistant.kit import build_kit
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _graph, _m = built
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    prepare_workspace(tmp_path / "ws", build.root)
    kit = build_kit(build, AssistantState(), store=store,
                    session_id=session["id"], turn_id="t",
                    workspace=tmp_path / "ws")
    got = kit["search"].fn("declined", kind="values")
    assert got["results"][0]["predicate"] == "approval_cd = 'D'"
    assert "kind=values" in kit["search"].description


def test_literal_hook_names_the_code_behind_a_meaning(built):
    from sahs.assistant.hooks import literal_warnings
    build, _graph, _m = built
    meant = literal_warnings(
        build, "SELECT 1 FROM dw.wwcas_authorization "
               "WHERE approval_cd = 'Approved'")
    assert len(meant) == 1
    assert "approval_cd = 'A'" in meant[0] and "meaning" in meant[0]
    assert literal_warnings(
        build, "SELECT 1 FROM dw.wwcas_authorization "
               "WHERE approval_cd = 'A'") == []
    unknown = literal_warnings(
        build, "SELECT 1 FROM dw.gms_transaction WHERE country_cd = 'ZZ'")
    assert len(unknown) == 1
    assert "meanings on record" in unknown[0] and "United States" \
        in unknown[0]
    # a minted domain: no observed values, meanings still teach
    flag = literal_warnings(
        build, "SELECT 1 FROM dw.gms_transaction WHERE bq_only_col = 'Y'")
    assert len(flag) == 1 and "known values" in flag[0]


def test_digest_tells_the_three_rules(built):
    from sahs.loop.digest import synapse_digest
    build, _graph, _m = built
    digest = synapse_digest(build, search_hint="search")
    assert "## words" in digest
    assert "same symbol can mean different things" in digest
    assert "also ordinary words (CASE, POT)" in digest
    assert 'search(kind="values")' in digest
    assert "Filter on the code, say the meaning" in digest
    v1 = synapse_digest(build)
    assert 'search_semantics(kind="vocab")' in v1


def test_summaries_show_why_the_validator_refused():
    from sahs.assistant.loop import summarize
    refused = {"error": "sql_invalid",
               "hint": "each violation names its correction",
               "violations": [{"code": "unknown_column",
                               "detail": "column 'trans_usd_amt' not in "
                                         "dw.gms_transaction",
                               "hint": "did you mean trans_usd_am?"}]}
    line = summarize("run_sql", refused)
    assert line.startswith("ERROR: sql_invalid — unknown_column: column "
                           "'trans_usd_amt'")
