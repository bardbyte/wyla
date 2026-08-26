"""Typed records every source adapter emits — the L0→L1 currency.

ExpressionRecord is anything SQL-shaped headed for c(sql) and the census;
VocabRecord / TermRecord / StdTechEntry are the vocabulary/catalog shapes
that skip canon and land in the graph at P2. Every record keeps an
``evidence_ref`` back to its exact source location — the bottom of the
provenance chain starts here.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from sahs.canon.authority import Authority


class ExpressionRecord(BaseModel):
    raw_sql: str
    kind: Literal["query", "predicate", "case", "metric_expr"]
    source: str                     # a SOURCE_AUTHORITY key, verbatim
    authority: Authority
    concept_label: str | None = None
    metric_ref: str | None = None   # stable metric identity where declared
    table_hint: str | None = None
    prompt: str | None = None       # gold pairs carry the NL question
    support: int = 1
    first_seen: str = ""
    last_seen: str = ""
    evidence_ref: str = ""
    witness: str = ""               # E12/A1: evidence family; derived
                                    # from source when a loader leaves it
                                    # empty (jobs_30d always sets it)
    extra: dict = Field(default_factory=dict)


class Quarantined(BaseModel):
    source: str
    category: str                   # parse_error|fragment|dialect|transform|
    #                                 ambiguous_table|missing_field|
    #                                 out_of_scope|not_sql|schema_mismatch|
    #                                 nested|ambiguous_attribution
    detail: str
    evidence_ref: str = ""


class VocabRecord(BaseModel):
    """Acronyms + glossary terms (data_cleaned.csv shape)."""

    symbol: str
    definition: str
    business_unit: str = "All"
    region: str = "All"
    entry_type: str = "Acronym"     # Acronym | Glossary Term
    evidence_ref: str = ""


class TermRecord(BaseModel):
    """Atlas Federated Data Catalog business terms."""

    term_id: str
    name: str
    status: str                     # Approved|Candidate|Under Review|Rejected
    evidence_ref: str = ""


class StdTechColumn(BaseModel):
    name: str
    description: str = ""
    business_name: str = ""
    data_type: str = ""
    pii_role_id: str | None = None
    sde_group: str | None = None
    linked_terms: list[dict] = Field(default_factory=list)


class StdTechEntry(BaseModel):
    """One Atlas getStdTechMetadata catalog entry."""

    table: str
    description: str = ""
    business_name: str = ""
    data_category: str = ""
    data_sub_category: str = ""
    layer_type: str = ""            # ODL | SOR | DERIVED
    has_pii: bool
    has_oncop: bool = False        # ONCOP compliance flag (real feed)
    has_gdpr: bool = False
    ownership: dict = Field(default_factory=dict)
    columns: list[StdTechColumn] = Field(default_factory=list)
    evidence_ref: str = ""
