"""Agent Loop v1 §9.1 — the tool layer, held to the new-hire test.

Every test calls a tool exactly the way someone who has read only its
description would, and asserts the result either answers or TEACHES
the correct next call. The descriptions themselves are pinned to the
spec's text (docs/specs/agent_loop_v1.md §3): descriptions are the
product, so a drive-by rewording is a test failure.

The fixture is the real compiled build — no mocked data anywhere; the
only stand-ins are seams (the dry-run substrate, a rows-empty snapshot
runner) that carry no invented numbers.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("loop_tools")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "looptools"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


@pytest.fixture()
def kit(compiled):
    from sahs.evals.substrate import StaticSubstrate
    from sahs.loop.tools import LoopState, toolkit
    build, tmp = compiled
    state = LoopState()
    return toolkit(build, state, substrate=StaticSubstrate({}),
                   ledger_path=tmp / "loop_ledger.jsonl"), state, build


# ─── descriptions are the product (§3, pinned verbatim) ──────


def test_descriptions_match_the_spec_verbatim(kit):
    tools, _state, _build = kit
    spec_text = {
        "list_tables":
            "Lists governed tables with one-line purpose, row count, "
            "readiness, owner.\nUse first when you don't know which "
            "table holds the concept.",
        "grep_cards":
            "Literal/regex search across every compiled card line. "
            "Returns card id, line, [prov].\nFast and exact. Use to "
            "find where a word, column, or code appears before "
            "reading.",
        "read_card":
            "Returns a card (or one section). Every line carries its "
            "witness tag.\nRead a table card before touching its "
            "columns; read a metric card before using it.",
        "search_semantics":
            "Ranked metrics/concepts/joins/vocab/values with "
            "status, support, agreement, aliases — and business "
            "areas: a query naming a line of business comes back as "
            "the area itself, not its furniture.\nUse for meaning "
            "(\"spend\", \"SMB\"); use grep_cards for exact tokens.",
        "resolve":
            "Binds words to governed metrics/concepts with "
            "confidence + candidates. Never guesses;\nreturns "
            "ambiguities you may settle by evidence or by asking.",
        "run_sql":
            "Validates, then dry-runs or runs on the frozen snapshot "
            "under cost gates and ACL.\nReturns schema, rows, bytes. "
            "Errors teach: unknown column → the 3 closest real ones.",
        "plan_set":
            "Update the session's semantic plan (metric, table, "
            "filters, grain, dims, time, checks).\nThe plan is what "
            "gets verified and disclosed; keep it current as you "
            "learn.",
    }
    for name, expected in spec_text.items():
        assert tools[name].description == expected, name


def test_the_write_surface_is_exactly_three_tools(kit):
    tools, _state, _build = kit
    writers = {n for n, t in tools.items() if t.writes}
    assert writers == {"plan_set", "note", "ask_user"}
    enders = {n for n, t in tools.items() if t.ends_turn}
    assert enders == {"ask_user"}
    # not tools, by design: the harness verifies, the clerk writes
    # truth, live execution does not exist in the loop; the scout
    # (§9.5) appears only when a runner is wired in — a bare kit,
    # like the scout's own kit, has none
    for absent in ("verify", "write_truth", "execute_live",
                   "delegate_scout"):
        assert absent not in tools


def test_render_tool_block_is_identical_across_calls(kit):
    from sahs.loop.tools import render_tool_block
    tools, _state, _build = kit
    once, twice = render_tool_block(tools), render_tool_block(tools)
    assert once == twice          # cacheable by construction
    assert "run_sql(sql, mode=dry_run|snapshot, limit=200)" in once
    assert "plan_set(patch)" in once
    assert "ask_user(question, options[])" in once


# ─── list_tables ─────────────────────────────────────────────


def test_list_tables_reads_like_a_shelf(kit):
    tools, _state, _build = kit
    out = tools["list_tables"].fn()
    assert out["count"] >= 2
    spine = next(t for t in out["tables"]
                 if t["table"] == "dw.gms_transaction")
    assert spine["purpose"].startswith("Global merchant")
    assert spine["owner"] == "own_a@corp"
    assert "certified" in spine["readiness"]
    assert "read_card" in out["hint"]
    # pinned: an absent row count is never a zero
    for row in out["tables"]:
        assert row["rows"] == "unknown" or int(row["rows"]) > 0


def test_list_tables_bad_lob_names_what_exists(kit):
    tools, _state, _build = kit
    out = tools["list_tables"].fn(lob="submarine_lending")
    assert out["count"] == 0
    assert "no table matches" in out["hint"]
    assert "no filter" in out["hint"]


# ─── grep_cards ──────────────────────────────────────────────


def test_grep_cards_finds_a_column_before_reading(kit):
    tools, _state, _build = kit
    out = tools["grep_cards"].fn("trans_usd_am")
    assert out["count"] >= 1
    hit = out["hits"][0]
    assert set(hit) == {"card", "line", "text"}
    assert any(h["card"].startswith("tables/") for h in out["hits"])
    assert any("[prov:" in h["text"] for h in out["hits"])
    assert "read_card" in out["hint"]


def test_grep_cards_invalid_regex_searches_literally(kit):
    tools, _state, _build = kit
    out = tools["grep_cards"].fn("sum(")
    assert out["hint"].startswith("pattern was not valid regex")
    assert out["count"] >= 1        # metric expressions contain sum(


def test_grep_cards_unknown_scope_names_the_scopes(kit):
    tools, _state, _build = kit
    out = tools["grep_cards"].fn("spend", scope="cards")
    assert "error" in out
    for scope in ("all", "tables", "metrics", "concepts"):
        assert scope in out["hint"]


def test_grep_cards_no_match_points_at_meaning_search(kit):
    tools, _state, _build = kit
    out = tools["grep_cards"].fn("zorbulator_qx")
    assert out["count"] == 0 and "search_semantics" in out["hint"]


# ─── read_card ───────────────────────────────────────────────


def test_read_card_answers_every_address_form(kit):
    tools, state, _build = kit
    by_id = tools["read_card"].fn("table:gms_transaction")
    assert by_id["card"] == "tables/dw__gms_transaction"
    assert "columns" in by_id["sections"]
    by_grep_address = tools["read_card"].fn(by_id["card"])
    assert by_grep_address["text"] == by_id["text"]
    # the trace is the sub-graph: reads are recorded automatically
    assert "tables/dw__gms_transaction" in state.subgraph["cards_read"]
    assert state.subgraph["cards_read"].count(
        "tables/dw__gms_transaction") == 1


def test_read_card_section_isolates_and_teaches_on_miss(kit):
    tools, _state, _build = kit
    cols = tools["read_card"].fn("table:gms_transaction",
                                 section="columns")
    assert "trans_usd_am" in cols["text"]
    assert "## " not in cols["text"]
    miss = tools["read_card"].fn("table:gms_transaction",
                                 section="tentacles")
    assert "error" in miss
    assert "columns" in miss["hint"]      # names the real sections


def test_read_card_unknown_suggests_and_names_the_finders(kit):
    tools, _state, _build = kit
    out = tools["read_card"].fn("table:gms_transactionz")
    assert "error" in out
    assert out["suggestions"]
    assert "grep_cards" in out["hint"]


# ─── search_semantics / resolve ──────────────────────────────


def test_search_semantics_ranks_meaning_with_receipts(kit):
    tools, _state, _build = kit
    out = tools["search_semantics"].fn("spend")
    metric_hits = [r for r in out["results"] if r["kind"] == "metric"]
    assert metric_hits
    top = metric_hits[0]
    assert {"status", "support", "agreement",
            "table"} <= set(top)
    # the catalog's group-by patterns ride the hit as `dimensions`
    assert any(r.get("dimensions") == ["part_dt"] for r in metric_hits)


def test_search_semantics_vocab_kind_serves_the_acronyms(kit):
    tools, _state, build = kit
    symbol = build.vocab[0]["text"] if build.vocab else None
    if symbol is None:
        pytest.skip("fixture build carries no vocab")
    out = tools["search_semantics"].fn(symbol, kind="vocab")
    hits = [r for r in out["results"] if r["kind"] == "vocab"]
    assert hits and hits[0]["definition"]


def test_search_semantics_unknown_kind_teaches(kit):
    tools, _state, _build = kit
    out = tools["search_semantics"].fn("spend", kind="meaning")
    assert "error" in out and "vocab" in out["hint"]


def test_resolve_is_the_same_binder_and_is_recorded(kit):
    tools, state, _build = kit
    out = tools["resolve"].fn("acquirer net spend by day")
    assert "metrics" in out and "confidence" in out
    assert state.subgraph["resolves"]
    assert state.subgraph["resolves"][-1]["text"] == \
        "acquirer net spend by day"


def test_resolve_unknown_table_scope_teaches(kit):
    tools, _state, _build = kit
    out = tools["resolve"].fn("spend", table="gms_transactionz")
    assert "error" in out and "list_tables" in out["hint"]
    assert out["suggestions"]


# ─── sample_values / joins / definition line ─────────────────


def test_sample_values_slices_and_never_lies_about_liveness(kit):
    tools, _state, build = kit
    from sahs.tools.api import _jsonl
    domains = _jsonl(build.root / "indexes" / "domains.jsonl")
    if not domains:
        pytest.skip("fixture build carries no compiled domains")
    table, column = domains[0]["key"].rsplit(".", 1)
    out = tools["sample_values"].fn(table, column, n=2)
    assert len(out["values"]) <= 2
    assert "not a live query" in out["coverage_note"]


def test_sample_values_unknown_column_names_the_closest(kit):
    tools, _state, _build = kit
    out = tools["sample_values"].fn("gms_transaction", "trans_usd_amm")
    assert "error" in out
    assert "trans_usd_am" in out["hint"]
    assert "read_card" in out["hint"]


def test_get_join_paths_tiers_the_evidence(kit):
    tools, _state, build = kit
    out = tools["get_join_paths"].fn(
        ["gms_transaction", "wwcas_authorization"])
    path = out["paths"][0]
    assert path["tables"] == sorted(
        ["dw.gms_transaction", "dw.wwcas_authorization"])
    assert path["tier"] in ("certified", "witnessed", "candidate")
    assert path["evidence"]
    for entry in path["evidence"]:
        assert entry["tier"] in ("certified", "witnessed", "candidate")


def test_get_join_paths_none_prefers_one_table(kit):
    tools, _state, build = kit
    lonely = [t for t in build.schema
              if not any(t in (j["a"], j["b"]) for j in build.joins)]
    if not lonely:
        pytest.skip("every fixture table has a join witness")
    out = tools["get_join_paths"].fn(
        ["gms_transaction", build.short_table(lonely[0])])
    assert out["paths"][0]["tier"] == "none"
    assert "prefer answering from one table" in out["hint"]


def test_get_join_paths_needs_two_tables(kit):
    tools, _state, _build = kit
    out = tools["get_join_paths"].fn(["gms_transaction"])
    assert "error" in out and "two or more" in out["error"]


def test_get_definition_line_still_speaks_disclosure(kit):
    tools, _state, build = kit
    metric = build.metrics[0]["id"]
    out = tools["get_definition_line"].fn(metric)
    assert out["definition_line"].startswith("Using ")


# ─── run_sql ─────────────────────────────────────────────────


def test_run_sql_dry_run_returns_shape_never_rows(kit):
    tools, _state, _build = kit
    out = tools["run_sql"].fn(GOOD_SQL)
    assert out["mode"] == "dry_run"
    assert out["valid"] is True
    assert out["rows"] is None


def test_run_sql_unknown_column_teaches_the_closest(kit):
    tools, _state, _build = kit
    bad = GOOD_SQL.replace("trans_usd_am", "trans_usd_amm")
    out = tools["run_sql"].fn(bad)
    assert out["error"] == "sql_invalid"
    unknown = [v for v in out["violations"]
               if v["code"] == "unknown_column"]
    assert unknown and "did you mean" in unknown[0]["hint"]


def test_run_sql_bad_mode_names_both_and_bans_live(kit):
    tools, _state, _build = kit
    out = tools["run_sql"].fn(GOOD_SQL, mode="live")
    assert "error" in out
    assert "dry_run" in out["hint"] and "snapshot" in out["hint"]
    assert "not a loop tool" in out["hint"]


def test_run_sql_snapshot_without_engine_is_honest(kit):
    tools, _state, _build = kit
    out = tools["run_sql"].fn(GOOD_SQL, mode="snapshot")
    assert out["error"] == "no_snapshot"
    assert "dry_run still checks" in out["hint"]


def test_run_sql_snapshot_engine_seam_returns_rows_shape(compiled,
                                                         tmp_path):
    from sahs.evals.substrate import StaticSubstrate
    from sahs.loop.tools import LoopState, toolkit
    build, _tmp = compiled

    class EmptyExtract:
        """A seam, not data: proves the plumbing without inventing
        a single row."""
        name = "test_extract"

        def run(self, sql, limit):
            return {"rows": [], "schema": [
                {"name": "part_dt", "type": "date"}]}

    tools = toolkit(build, LoopState(), substrate=StaticSubstrate({}),
                    snapshot_runner=EmptyExtract(),
                    ledger_path=tmp_path / "ledger.jsonl")
    out = tools["run_sql"].fn(GOOD_SQL, mode="snapshot")
    assert out["mode"] == "snapshot"
    assert out["rows"] == [] and out["row_count"] == 0
    assert out["source"] == "test_extract"


# ─── plan_set: the typechecked write ─────────────────────────


def _unique_certified(build, *, grainless=False):
    """A certified metric whose label no other metric shares — the
    label path only binds what a person could name unambiguously."""
    from collections import Counter
    labels = Counter((m.get("label") or "").lower()
                     for m in build.metrics)
    return next(m for m in build.metrics
                if m.get("status") == "certified"
                and labels[(m.get("label") or "").lower()] == 1
                and (not grainless
                     or not (m.get("grain") or "").strip()))


def test_plan_set_binds_a_real_metric_and_versions(kit):
    tools, state, build = kit
    certified = _unique_certified(build)
    out = tools["plan_set"].fn({"metric": certified["label"]})
    assert out["ok"] is True
    assert out["plan"]["metric_id"] == certified["id"]
    # the metric's home table rides along when the patch has none
    assert out["plan"]["table"] == certified["table"]
    assert out["plan"]["version"] == 2
    assert out["plan"]["parent"] == 1
    assert out["changes"]
    assert state.plan.metric_id == certified["id"]
    assert state.subgraph["bindings_used"][-1]["metric"] == \
        certified["id"]


def test_plan_set_unknown_metric_changes_nothing(kit):
    tools, state, _build = kit
    before = state.plan.version
    out = tools["plan_set"].fn({"metric": "flurble rate"})
    assert out["ok"] is False
    assert out["problems"][0]["code"] == "unknown_metric"
    assert "resolve()" in out["problems"][0]["hint"]
    assert state.plan.version == before


def test_plan_set_unknown_slot_names_the_seven(kit):
    tools, _state, _build = kit
    out = tools["plan_set"].fn({"metricc": "spend"})
    assert out["ok"] is False
    problem = out["problems"][0]
    assert problem["code"] == "unknown_slot"
    for slot in ("metric", "table", "filters", "grain", "dims",
                 "time", "checks"):
        assert slot in problem["hint"]


def test_plan_set_shared_label_refuses_to_argmax(kit):
    tools, state, build = kit
    from collections import Counter
    labels = Counter((m.get("label") or "").lower()
                     for m in build.metrics)
    shared = next((m for m in build.metrics
                   if m.get("label")
                   and labels[m["label"].lower()] > 1), None)
    if shared is None:
        pytest.skip("no shared label in this fixture build")
    out = tools["plan_set"].fn({"metric": shared["label"]})
    assert out["ok"] is False
    problem = out["problems"][0]
    assert problem["code"] == "ambiguous_metric"
    assert "metric:" in problem["hint"]   # the ids to name instead
    # naming the id settles it, same call, no model judgement needed
    by_id = tools["plan_set"].fn({"metric": shared["id"]})
    assert by_id["ok"] is True
    assert state.plan.metric_id == shared["id"]


def test_plan_set_missing_grain_warns_in_contract_language(kit):
    tools, _state, build = kit
    try:
        grainless = _unique_certified(build, grainless=True)
    except StopIteration:
        pytest.skip("every certified fixture metric declares a grain")
    out = tools["plan_set"].fn({"metric": grainless["label"]})
    warning = next(w for w in out["warnings"]
                   if w["code"] == "grain_missing")
    assert "no grain, no answer" in warning["detail"]
    assert "ask_user" in warning["hint"]


def test_plan_set_literal_off_domain_cites_sample_values(kit):
    tools, _state, build = kit
    from sahs.tools.api import _jsonl
    domains = _jsonl(build.root / "indexes" / "domains.jsonl")
    usable = next((d for d in domains if d.get("values")), None)
    if usable is None:
        pytest.skip("fixture build carries no compiled domains")
    table, column = usable["key"].rsplit(".", 1)
    out = tools["plan_set"].fn({
        "table": table.split(".")[-1],
        "filters": {column: "definitely_not_observed_x9"}})
    assert out["ok"] is True          # warnings travel, never block
    warning = next(w for w in out["warnings"]
                   if w["code"] == "literal_off_domain")
    assert "sample_values disagrees" in warning["hint"]


def test_plan_set_speaks_the_model_facing_slot_names(kit):
    tools, state, _build = kit
    out = tools["plan_set"].fn({
        "time": "last_quarter vs prior", "dims": ["month"],
        "checks": ["ratio reconciles to certified quarterly",
                   "coverage > 0"]})
    assert out["ok"] is True
    assert out["plan"]["time_window"] == "last_quarter vs prior"
    assert out["plan"]["dimensions"] == ["month"]
    assert len(out["plan"]["checks"]) == 2
    assert state.plan.checks[0].startswith("ratio reconciles")


# ─── note / ask_user ─────────────────────────────────────────


def test_note_keeps_the_ruled_out_and_refuses_the_empty(kit):
    tools, state, _build = kit
    empty = tools["note"].fn("")
    assert "error" in empty and "ruled out" in empty["hint"]
    ok = tools["note"].fn("drop is CNP-driven, month 2; "
                          "denominators stable")
    assert ok["ok"] is True and ok["notes"] == len(state.notes)


def test_ask_user_carries_evidence_and_caps_the_chips(kit):
    tools, state, _build = kit
    lineup = tools["ask_user"].fn(
        "Which country?", ["a", "b", "c", "d", "e"])
    assert "error" in lineup and "lineup" in lineup["error"]
    out = tools["ask_user"].fn(
        "Canada by merchant country or cardmember country?",
        [{"value": "merchant_ctry", "label": "merchant country",
          "evidence": "approval_rate is a merchant metric"},
         {"value": "cm_ctry", "label": "cardmember country"}])
    assert out["ok"] is True and out["ends_turn"] is True
    assert state.pending_question["question"].startswith("Canada")
    chips = state.pending_question["options"]
    assert chips[0]["evidence"].startswith("approval_rate")
    assert tools["ask_user"].ends_turn is True


def test_ask_user_without_options_teaches_navigation_duty(kit):
    tools, _state, _build = kit
    out = tools["ask_user"].fn("What do you want?", [])
    assert "error" in out
    assert "navigation" in out["hint"]
