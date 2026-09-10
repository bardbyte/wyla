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


def test_skills_load_into_registry_not_the_graph(loaded):
    _, out = loaded
    # Skills are NOT a graph source — building from a skills-only dir mints
    # ZERO nodes. Business logic + guardrails live in the file registry.
    store = build_graph_from_sources(out)
    by_type = store.stats()["nodes_by_type"]
    for absent in ("Skill", "Guardrail", "Metric", "Table"):
        assert absent not in by_type, f"{absent} must not enter the data graph"

    from synapse.mcp.skills_registry import SkillsRegistry
    reg = SkillsRegistry.from_dir(out / "skills")
    assert len(reg.skills) == 2
    assert len(reg.guardrails) >= 6
    # the security-critical guardrail is present and machine-checkable
    cm11 = [g for g in reg.guardrails if "cm11_encrypted" in g["rule"]]
    assert cm11 and cm11[0]["machine_checkable"] is True


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


def test_data_specs_available_to_agent_via_registry(tmp_path):
    """A skill's business logic — valid values, code bands, chart contracts,
    and the FULL knowledge — is read by the agent from the registry bundle,
    not the graph. get_skill returns all of it."""
    from synapse.graph.store import GraphStore
    from synapse.mcp.service import GraphService
    from synapse.mcp.skills_registry import SkillsRegistry

    load_skills_library(FIXTURE_LIBRARY, out_dir=tmp_path)
    # pure agent side: an empty graph + the registry — proves the skill
    # content needs no graph nodes at all
    svc = GraphService(GraphStore(),
                       skills=SkillsRegistry.from_dir(tmp_path / "skills"))

    res = svc.get_skill("approval rate")
    assert res["status"] == "ok"
    bundle = res["data"]["skill"]
    assert bundle["skill_id"] == "DEMO_NewAccountsApprovalRate"
    # valid values + code bands ride in the bundle (business reference data)
    vv = {v["column"]: v["values"] for v in bundle["valid_values"]}
    assert vv["decision_cd"] == ["A", "D", "P"]
    bands = {b["raw"]: b["label"] for b in bundle["bands"]}
    assert bands["800+"] == "Exceptional"
    # chart contract + the COMPLETE knowledge (not just an excerpt) — the agent
    # uses all of it to answer well
    assert bundle["chart_contracts"]["charts"]
    assert len(bundle["knowledge_full"]) >= len(bundle["knowledge_excerpt"])
