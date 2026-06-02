"""Typed schemas — every contract between layers is a pydantic model.

These are deliberately small and immutable. Mutating state lives in
the event store; everything here is "data the loaders return."
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ─── Source registry inputs ──────────────────────────────────


class GlossaryEntry(BaseModel):
    """One row of the acronym/synonym glossary CSV.

    Multiple rows can share the same `symbol` with different
    `(business_unit, region)` — that's the disambiguation context the
    graph encodes as edge qualifiers."""

    symbol: str = Field(..., description="The short form (e.g. 'AA', 'CM')")
    definition: str = Field(..., description="What the symbol expands to")
    business_unit: str | None = Field(
        default=None,
        description="BU scope for disambiguation (Finance, Marketing, etc.)",
    )
    region: str | None = Field(default=None, description="Region scope")
    entry_type: str | None = Field(
        default=None,
        description="e.g. 'Acronym', 'Abbreviation', 'Code'",
    )
    raw_row: dict = Field(
        default_factory=dict,
        description="Original CSV row for forward-compat",
    )


class MetricCatalogEntry(BaseModel):
    """One row of the business-metric catalog — the curated 'Freebase'
    prior. Highest source weight short of human_approval."""

    technical_name: str = Field(..., description="machine identifier (snake_case)")
    business_name: str | None = Field(default=None, description="human label")
    business_definition: str | None = Field(
        default=None, description="plain-English meaning",
    )
    calculation_logic: str | None = Field(
        default=None, description="SQL or formula",
    )
    primary_data_product: str | None = Field(
        default=None, description="source table / data product",
    )
    supporting_data_products: list[str] = Field(default_factory=list)
    associated_domain: str | None = Field(
        default=None, description="business area (Customer, Finance, ...)",
    )
    metric_grain: str | None = Field(
        default=None,
        description="row-level / account-level / aggregated / partition-level",
    )
    business_synonyms: list[str] = Field(default_factory=list)
    technical_references: list[str] = Field(default_factory=list)
    raw_row: dict = Field(default_factory=dict)


class TableCatalogEntry(BaseModel):
    """One row of the table catalog — scope + domain mapping."""

    table_name: str
    is_in_dmp: bool = Field(
        default=False, description="True = in DMP scope (the 'Yes' rows)",
    )
    company_domain: str | None = Field(
        default=None, description="Finance / Marketing / Loyalty / ..."
    )
    data_domain: str | None = Field(
        default=None, description="FODL / MERCHANT / Cardmember / ..."
    )
    raw_row: dict = Field(default_factory=dict)


# ─── Derived / aggregated inputs ─────────────────────────────


class CorpusNounStat(BaseModel):
    """A frequency-aggregated 'noun candidate' from the SQL corpus.

    Produced by walking every parsed fingerprint and counting
    occurrences of identifiers that look like entity names: GROUP BY
    columns, JOIN keys, CASE WHEN aliases, primary table names,
    select-alias outputs."""

    token: str = Field(..., description="lowered identifier")
    occurrence_count: int = 0
    appears_in_tables: list[str] = Field(default_factory=list)
    role_counts: dict[str, int] = Field(
        default_factory=dict,
        description="{group_by: 12, join_key: 5, case_alias: 2, ...}",
    )


class MDMTableDigest(BaseModel):
    """Minimal MDM digest projection used in the curation bundle.

    The full digest has 30+ fields per column; this is the
    distillation that the LLM actually needs to propose entities."""

    table_name: str
    bq_fqn: str | None = None
    table_business_name: str | None = None
    table_description: str | None = None
    data_category: str | None = None
    data_sub_category: str | None = None
    n_columns: int = 0
    key_columns: list[dict] = Field(
        default_factory=list,
        description="columns flagged is_primary/is_dedupe_key — "
                    "with name + business_name + description",
    )
    pii_columns: list[str] = Field(
        default_factory=list,
        description="columns flagged is_pii (or pii_role_id set)",
    )
    sample_columns: list[dict] = Field(
        default_factory=list,
        description="up to 20 columns with name + business_name + description",
    )


# ─── Bundle the curator sees ─────────────────────────────────


class EvidenceBundle(BaseModel):
    """The full slice handed to the LLM for entity curation.

    The contract is intentionally narrow: every field below is the
    grounded prior the model draws from. The model is not allowed to
    invent entities outside what the evidence supports."""

    scope_description: str = Field(
        ..., description="one-line description of the scoped slice",
    )
    table_catalog: list[TableCatalogEntry]
    glossary: list[GlossaryEntry]
    metric_catalog: list[MetricCatalogEntry]
    mdm_digests: list[MDMTableDigest]
    corpus_signals: list[CorpusNounStat]
    n_queries_analyzed: int = 0


# ─── LLM output schema ───────────────────────────────────────


class ProposedRelationship(BaseModel):
    type: Literal[
        "has_many", "has_one", "many_to_many",
        "is_a", "part_of", "describes",
    ]
    target_entity: str
    via_column: str | None = None
    cardinality_evidence: str | None = None


class ProposedEntity(BaseModel):
    """One LLM-proposed entity. Every field is required to carry
    evidence — no entity without provenance."""

    canonical_name: str = Field(
        ..., description="PascalCase identifier (Cardmember, CardProduct, ...)",
    )
    description: str = Field(..., max_length=400)
    source_evidence: dict[str, list[str]] = Field(
        ...,
        description="{tables: [...], columns: [...], metrics: [...], "
                    "acronyms: [...], data_categories: [...]} — "
                    "every key is a list of identifiers that point at this entity",
    )
    properties: dict = Field(
        default_factory=dict,
        description="{id_columns: [...], common_attributes: [...], "
                    "coded_values: [...]}",
    )
    parent_entity: str | None = None
    relationships: list[ProposedRelationship] = Field(default_factory=list)
    llm_confidence: float = Field(..., ge=0.0, le=1.0)
    human_review_notes: str = Field(default="")


class CurationProposal(BaseModel):
    """Top-level LLM output. Carries entities + LLM-flagged ambiguities
    + scope observations the human reviewer should consider."""

    proposed_entities: list[ProposedEntity]
    ambiguities_flagged: list[str] = Field(default_factory=list)
    scope_observations: list[str] = Field(default_factory=list)
    generated_at: str = Field(default="", description="ISO timestamp")
    model_used: str = Field(default="")
    prompt_sha256: str = Field(
        default="",
        description="hash of the prompt — reproducibility anchor",
    )
