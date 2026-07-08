"""Skills loader + graph ingestion — the L3 semantic witness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import canonical_uri
from synapse.loaders.skills_loader import load_skills_library

FIXTURE_LIBRARY = Path(__file__).parent / "fixtures" / "skills_library"


@pytest.fixture
def loaded(tmp_path: Path):
    result = load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    return result, tmp_path


def test_loads_every_package(loaded):
    result, _ = loaded
    assert result.status == "ok"
    assert result.records_count == 2
    assert result.source == "skills"
    names = {p.name for p in result.artifacts_written}
    assert names == {"DEMO_NewAccountsApprovalRate.json", "DEMO_RollRates.json"}


def test_canonical_blob_shape(loaded):
    _, out = loaded
    blob = json.loads(
        (out / "skills" / "DEMO_NewAccountsApprovalRate.json").read_text())
    assert blob["skill_id"] == "DEMO_NewAccountsApprovalRate"
    assert blob["domain"] == "new_accounts"
    assert "sbs_new_accounts" in blob["tables_used"]
    metric_names = {m["name"] for m in blob["metrics"]}
    assert {"gross_approval_rate", "credit_approval_rate"} <= metric_names
    # numerator/denominator folded into a formula
    gross = next(m for m in blob["metrics"] if m["name"] == "gross_approval_rate")
    assert "COUNT(DISTINCT" in gross["formula"] and "/" in gross["formula"]
    assert blob["qa_checks"], "qa_checks.yaml should be parsed"
    assert blob["knowledge_excerpt"].startswith("# New Accounts")


def test_authored_guardrails_win_over_mined_duplicates(loaded):
    _, out = loaded
    blob = json.loads(
        (out / "skills" / "DEMO_NewAccountsApprovalRate.json").read_text())
    rails = blob["guardrails"]
    authored = [g for g in rails if not g["mined_from_knowledge"]]
    mined = [g for g in rails if g["mined_from_knowledge"]]
    assert len(authored) == 3          # exactly the skill.yaml list
    assert mined                        # knowledge.md prose was mined too
    # the authored cm11 rule survives dedupe exactly once, tagged privacy;
    # differently-phrased knowledge.md variants may coexist as mined rules
    cm11_authored = [g for g in authored if "cm11_encrypted" in g["rule"]]
    assert len(cm11_authored) == 1
    assert cm11_authored[0]["category"] == "privacy"
    assert cm11_authored[0]["machine_checkable"] is True


def test_graph_ingestion_mints_first_class_nodes(loaded, tmp_path: Path):
    _, out = loaded
    store = build_graph_from_sources(out)
    stats = store.stats()
    assert stats["nodes_by_type"]["Skill"] == 2
    assert stats["nodes_by_type"]["Guardrail"] >= 6
    assert stats["nodes_by_type"]["Metric"] == 4
    assert stats["edges_by_type"]["APPLIES_TO"] == 2
    assert stats["edges_by_type"]["DEFINED_BY"] == 4
    assert stats["edges_by_type"]["CONSTRAINS"] >= 6
    # skills do NOT mint Table nodes — the data graph is sourced by the data
    assert "Table" not in stats["nodes_by_type"]
    # provenance: a skill is knowledge/guardrails, not a data authority, so an
    # uncorroborated skill fact sits at guessed until data agrees. (get_skill
    # and guardrail enforcement are unaffected — they don't grade on tier.)
    skill = store.get(canonical_uri("skill", "DEMO_RollRates"))
    assert skill is not None
    assert skill.provenance.sources == ["skills"]
    assert skill.provenance.confidence_tier == "guessed"


def test_missing_dir_is_error_not_crash(tmp_path: Path):
    result = load_skills_library(tmp_path / "nope", out_dir=tmp_path)
    assert result.status == "error"
    assert "not found" in (result.error or "")


def test_empty_dir_is_skipped(tmp_path: Path):
    empty = tmp_path / "lib"
    empty.mkdir()
    result = load_skills_library(empty, out_dir=tmp_path)
    assert result.status == "skipped"


# ─── nested library layout (real structure, screenshot 2026-07-06) ──


def test_loads_nested_domain_group_layout(tmp_path):
    """Real library nests skills/<DomainGroup>/<SkillName>/skill.yaml —
    the loader must recurse and derive domain from the group folder."""
    from synapse.loaders.skills_loader import load_skills_library

    nested = Path(__file__).parent / "fixtures" / "skills_nested"
    result = load_skills_library(nested, out_dir=tmp_path)
    assert result.status == "ok", result.warnings
    assert result.records_count == 2                 # found at depth 2

    approval = json.loads(
        (tmp_path / "skills" / "SBS_NewAccountsApprovalRate.json").read_text())
    # domain inferred from the NewAccountsSkills group folder, not the prefix
    assert approval["domain"] == "new_accounts"
    contrib = json.loads(
        (tmp_path / "skills" / "CPS_ContributionAnalysis.json").read_text())
    assert contrib["domain"] == "portfolio_analytics"


def test_flat_fixture_still_works_after_recursion(tmp_path):
    """The DEMO_* fixtures are flat (depth 1) — must still load."""
    from synapse.loaders.skills_loader import load_skills_library

    result = load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    assert result.status == "ok"
    assert result.records_count == 2


# ─── full package utilization: data_specs.md + chart_contract.yaml ──


def test_data_specs_valid_values_and_bands_extracted(tmp_path):
    from synapse.loaders.skills_loader import load_skills_library

    load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    blob = json.loads(
        (tmp_path / "skills" / "DEMO_NewAccountsApprovalRate.json").read_text())
    vv = {v["column"]: v["values"] for v in blob["valid_values"]}
    assert vv["decision_cd"] == ["A", "D", "P"]
    assert vv["fraud_decline_in"] == ["Y", "N"]
    # bands: column / raw / label roles assigned correctly (not swapped)
    bands = {b["raw"]: b["label"] for b in blob["bands"]}
    assert bands["800+"] == "Exceptional"
    assert all(b["column"] == "fico_band" for b in blob["bands"])
    # chart_contract + full knowledge captured, nothing discarded
    assert "approval_rate_by_month" in blob["chart_contracts"].get("charts", {})
    assert len(blob["knowledge_full"]) >= len(blob["knowledge_excerpt"])


def test_data_specs_become_graph_facts(tmp_path):
    from synapse.graph.builder import build_graph_from_sources
    from synapse.mcp.service import GraphService

    load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    # A skill's reference data (valid values / bands) attaches to a real
    # table — skills no longer mint the table themselves, so stage its MDM
    # row (the data authority) the way a real build would.
    mdm = tmp_path / "mdm_cache"
    mdm.mkdir(parents=True, exist_ok=True)
    (mdm / "sbs_new_accounts.json").write_text(json.dumps({
        "table_name": "sbs_new_accounts",
        "columns": [{"name": "decision_cd", "type": "STRING"},
                    {"name": "fico_band", "type": "STRING"}],
    }))
    svc = GraphService(build_graph_from_sources(tmp_path))

    # valid values → curated FilterValue nodes (highest-trust source)
    fv = svc.get_filter_values("sbs_new_accounts", "decision_cd")
    assert {v["raw_value"] for v in fv["data"]["values"]} == {"A", "D", "P"}
    assert fv["data"]["values"][0]["sources"] == ["skills"]

    # bands → CodeMapping resolvable BOTH directions
    assert svc.resolve_code("fico_band", "Exceptional"
                            )["data"]["resolved"]["raw_value"] == "800+"
    assert svc.resolve_code("fico_band", "800+"
                            )["data"]["resolved"]["human_meaning"] == "Exceptional"

    # chart contract rides on the Skill for the viz layer
    skill = svc.get_skill("approval rate")
    assert skill["data"]["skill"]["chart_contracts"]["charts"]
