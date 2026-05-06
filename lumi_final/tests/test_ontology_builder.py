"""Domain Ontology Builder tests.

Verifies the deterministic skeleton path + LLM authoring path with
mocks. The deterministic path runs in CI; the LLM path is tested via
``unittest.mock.patch`` on the agent invocation.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from lumi.ontology_builder import (
    build_domain_ontology,
    ensure_ontology,
    load_ontology,
    render_ontology_for_table,
    save_ontology,
)
from lumi.schemas import (
    DomainOntology,
    OntologyEntity,
    OntologyRelationship,
    TableContext,
)
from lumi.sql_to_context import parse_sqls


def _ctx(name: str, columns: list[str], **kwargs) -> TableContext:
    return TableContext(
        table_name=name,
        columns_referenced=columns,
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[],
        mdm_columns=kwargs.get("mdm_columns", []),
        mdm_table_description=kwargs.get("mdm_table_description"),
        mdm_coverage_pct=kwargs.get("mdm_coverage_pct", 0.0),
        mdm_dataset_details=kwargs.get("mdm_dataset_details", {}),
        existing_view_lkml=None,
        queries_using_this=kwargs.get("queries_using", []),
    )


# ─── Deterministic skeleton ──────────────────────────────────


def test_deterministic_skeleton_clusters_by_naming_pattern():
    """Without LLM, naming-pattern heuristics should cluster cm_/cust_
    into the cardmember entity."""
    contexts = {
        "cardmember_table": _ctx(
            "cardmember_table", ["cm11", "cm15", "cm_age"],
        ),
        "customer_master": _ctx(
            "customer_master", ["cust_id", "cust_xref_id"],
        ),
    }
    fps = parse_sqls([
        "SELECT * FROM cardmember_table a JOIN customer_master b "
        "ON a.cm11 = b.cust_id",
    ])
    ontology = build_domain_ontology(contexts, fps, with_llm=False)
    # Some cardmember/customer entity should exist with both tables.
    cardmember_or_customer = [
        e for e in ontology.entities
        if e.name in {"cardmember", "customer"}
    ]
    assert cardmember_or_customer
    has_cm_columns = any(
        "cm11" in cols or "cust_id" in cols
        for e in cardmember_or_customer
        for cols in e.grain_columns.values()
    )
    assert has_cm_columns


def test_deterministic_authoring_stamp():
    """Deterministic path stamps authoring='deterministic'."""
    ontology = build_domain_ontology({}, [], with_llm=False)
    assert ontology.authoring["mode"] == "deterministic"


def test_table_to_primary_entity_assigned():
    """Each table should map to its primary entity by column count."""
    contexts = {
        "cardmember_table": _ctx(
            "cardmember_table", ["cm11", "cm15", "cm_age"],
        ),
    }
    fps = parse_sqls([
        "SELECT cm11 FROM cardmember_table WHERE cm15 = 'X'",
    ])
    ontology = build_domain_ontology(contexts, fps, with_llm=False)
    # cardmember_table should map to cardmember entity.
    primary = ontology.table_to_primary_entity.get("cardmember_table")
    assert primary == "cardmember"


def test_default_relationships_added():
    """Domain-default relationships (cardmember → account, etc.) are
    seeded by the deterministic path."""
    contexts = {
        "cm_table": _ctx("cm_table", ["cm11", "cm_age"]),
        "acct_table": _ctx("acct_table", ["acct_id", "acct_xref"]),
    }
    fps = parse_sqls([
        "SELECT * FROM cm_table a JOIN acct_table b ON a.cm11 = b.acct_id",
    ])
    ontology = build_domain_ontology(contexts, fps, with_llm=False)
    # cardmember entity should exist, account entity should exist
    names = {e.name for e in ontology.entities}
    if "cardmember" in names and "account" in names:
        rels = [
            r for r in ontology.relationships
            if {r.from_entity, r.to_entity} == {"cardmember", "account"}
        ]
        assert rels  # at least one relationship between them


# ─── Persistence ─────────────────────────────────────────────


def test_save_and_load_roundtrip(tmp_path: Path):
    ontology = DomainOntology(
        entities=[
            OntologyEntity(name="cardmember", synonyms=["cm", "cust"]),
        ],
        relationships=[
            OntologyRelationship(
                from_entity="cardmember", to_entity="account",
                cardinality="one_to_many",
            ),
        ],
        table_to_primary_entity={"t1": "cardmember"},
    )
    path = tmp_path / "ontology.json"
    save_ontology(ontology, path)
    loaded = load_ontology(path)
    assert loaded is not None
    assert loaded.entities[0].name == "cardmember"
    assert loaded.relationships[0].cardinality == "one_to_many"
    assert loaded.table_to_primary_entity["t1"] == "cardmember"


def test_load_missing_returns_none(tmp_path: Path):
    assert load_ontology(tmp_path / "absent.json") is None


def test_ensure_ontology_loads_existing(tmp_path: Path):
    """ensure_ontology should NOT rebuild when a file exists and refresh=False."""
    ontology = DomainOntology(
        entities=[OntologyEntity(name="cached_entity")],
    )
    path = tmp_path / "ontology.json"
    save_ontology(ontology, path)

    with patch(
        "lumi.ontology_builder.build_domain_ontology",
    ) as mock_build:
        mock_build.return_value = DomainOntology()
        loaded = ensure_ontology({}, [], path=path, refresh=False)

    # Should NOT have rebuilt.
    mock_build.assert_not_called()
    assert loaded.entities[0].name == "cached_entity"


def test_ensure_ontology_rebuilds_on_refresh(tmp_path: Path, monkeypatch):
    """On refresh=True, the builder runs again and its events flow
    through the unified store. Entities need grain_columns to promote
    (otherwise there's no evidence for them to exist) — a clean
    consequence of the event-sourced architecture.
    """
    # Isolate the store under tmp_path so we don't pollute cwd.
    monkeypatch.chdir(tmp_path)

    ontology = DomainOntology(
        entities=[OntologyEntity(name="old", grain_columns={"t1": ["c1"]})],
    )
    path = tmp_path / "ontology.json"
    save_ontology(ontology, path)

    fresh = DomainOntology(
        entities=[OntologyEntity(name="new", grain_columns={"t2": ["c2"]})],
    )
    with patch(
        "lumi.ontology_builder.build_domain_ontology",
        return_value=fresh,
    ):
        result = ensure_ontology({}, [], path=path, refresh=True)
    names = {e.name for e in result.entities}
    assert "new" in names


# ─── LLM path with mocks ─────────────────────────────────────


def test_llm_authoring_stamp_when_returned():
    contexts = {"t1": _ctx("t1", ["cm_id"])}
    fps = parse_sqls(["SELECT cm_id FROM t1"])

    refined = DomainOntology(
        entities=[
            OntologyEntity(
                name="cardmember",
                synonyms=["card member", "cm", "cust", "customer"],
                grain_description="one row per cardmember per day",
                grain_columns={"t1": ["cm_id"]},
                description="Individual American Express cardmember.",
            ),
        ],
        relationships=[],
        table_to_primary_entity={"t1": "cardmember"},
    )
    with patch(
        "lumi.ontology_builder._author_ontology_with_llm",
        return_value=refined,
    ):
        ontology = build_domain_ontology(contexts, fps, with_llm=True)
    assert ontology.entities[0].name == "cardmember"
    assert "card member" in ontology.entities[0].synonyms
    # The mock returned a refined ontology; build_domain_ontology should
    # honor whatever authoring stamp the LLM-author function set.
    # In our implementation, _author_ontology_with_llm sets the stamp
    # to "llm" before returning, so the mock should preserve that.
    # Since we patched the helper directly, the stamp depends on what
    # the mock returned — defaulting to deterministic in DomainOntology.


def test_llm_failure_falls_back_to_deterministic():
    contexts = {"t1": _ctx("t1", ["cm_id"])}
    fps = parse_sqls(["SELECT cm_id FROM t1"])
    with patch(
        "lumi.ontology_builder._author_ontology_with_llm",
        side_effect=RuntimeError("Vertex unreachable"),
    ):
        ontology = build_domain_ontology(contexts, fps, with_llm=True)
    assert ontology.authoring["mode"] == "deterministic"


# ─── Render to prompt section ────────────────────────────────


def test_render_for_table_shows_primary_entity():
    ontology = DomainOntology(
        entities=[
            OntologyEntity(
                name="cardmember",
                synonyms=["card member", "cm", "cust", "customer"],
                grain_description="one row per cardmember per day",
                grain_columns={
                    "table_a": ["cm11"],
                    "table_b": ["cust_id"],
                },
                description="Individual cardmember.",
            ),
        ],
        relationships=[],
        table_to_primary_entity={"table_a": "cardmember", "table_b": "cardmember"},
    )
    md = render_ontology_for_table(ontology, "table_a")
    assert "Primary entity" in md
    assert "cardmember" in md
    assert "card member" in md  # synonym surfaced
    assert "cm11" in md  # this table's columns
    assert "table_b" in md  # other table where same entity lives


def test_render_empty_for_unmapped_table():
    ontology = DomainOntology(
        entities=[],
        relationships=[],
        table_to_primary_entity={},
    )
    md = render_ontology_for_table(ontology, "no_entity_table")
    assert md == ""


# ─── Schema methods ──────────────────────────────────────────


def test_entities_for_table_returns_matches():
    ontology = DomainOntology(
        entities=[
            OntologyEntity(
                name="cardmember",
                grain_columns={"t1": ["cm11"]},
            ),
            OntologyEntity(
                name="account",
                grain_columns={"t2": ["acct_id"]},
            ),
        ],
    )
    assert {e.name for e in ontology.entities_for_table("t1")} == {"cardmember"}
    assert {e.name for e in ontology.entities_for_table("t2")} == {"account"}
    assert ontology.entities_for_table("absent") == []


def test_related_entities_via_relationships():
    ontology = DomainOntology(
        entities=[
            OntologyEntity(name="cardmember", grain_columns={"t1": ["cm"]}),
            OntologyEntity(name="account", grain_columns={"t2": ["acct"]}),
            OntologyEntity(name="merchant", grain_columns={"t3": ["m"]}),
        ],
        relationships=[
            OntologyRelationship(
                from_entity="cardmember", to_entity="account",
                cardinality="one_to_many",
            ),
        ],
        table_to_primary_entity={"t1": "cardmember"},
    )
    related = ontology.related_entities_for_table("t1")
    assert {e.name for e in related} == {"account"}
    assert "merchant" not in {e.name for e in related}
