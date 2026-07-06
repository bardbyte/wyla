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
    # provenance: skills facts start life at inferred (single curated source)
    skill = store.get(canonical_uri("skill", "DEMO_RollRates"))
    assert skill is not None
    assert skill.provenance.sources == ["skills"]
    assert skill.provenance.confidence_tier == "inferred"


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
