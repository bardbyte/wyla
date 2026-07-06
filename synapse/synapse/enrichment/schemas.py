"""Pydantic schemas — the strict contract the LLM must match.

These are enforced server-side via Gemini's `response_schema` (when
running for real) and via Pydantic validation (always). The LLM does not
emit free text — only this shape.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ColumnRole = Literal[
    "identifier", "attribute", "category",
    "measure", "timestamp", "filter", "code",
]


class RelationProposal(BaseModel):
    target_table: str
    target_column: str
    verb: str                                # "joins to" | "rolls up to" | "decodes via"
    evidence_count: int = Field(ge=0)


class ColumnObservation(BaseModel):
    column_name: str
    proposed_description: str | None = None  # ≤2 sentences
    candidate_role: ColumnRole
    candidate_entity_name: str | None = None
    candidate_entity_rationale: str | None = None
    relates_to: list[RelationProposal] = Field(default_factory=list)
    evidence_used: list[str] = Field(default_factory=list)  # source names
    self_confidence: float = Field(ge=0.0, le=1.0)
    ambiguity_flag: str | None = None


class CandidateSynonym(BaseModel):
    surface_form: str                         # "TBB"
    canonical_form: str                       # "Total Billed Business"
    scope_business_unit: str | None = None
    scope_region: str | None = None
    evidence_source: Literal["corpus", "baseline_lookml", "table_name"]
    rationale: str


class CodeResolution(BaseModel):
    column: str
    raw_value: str
    proposed_meaning: str
    evidence: str
    confidence: float = Field(ge=0.0, le=1.0)


class FilterRationale(BaseModel):
    column: str
    value: str
    observed_in_pct_of_queries: float = Field(ge=0.0, le=1.0)
    proposed_rationale: str
    safety_note: str | None = None


DemoCapability = Literal[
    "column_semantics",     # column meanings/roles/descriptions
    "metrics",              # skill-defined metrics on this table
    "code_resolutions",     # raw code → human meaning ("005" → ...)
    "related_tables",       # join paths the corpus/LLM proved
    "lineage",              # upstream/downstream provenance
    "governance",           # ownership, BU, lifecycle, PII posture
    "guardrails",           # rules the agent enforces (the "save" demo)
    "usage",                # who queries it, how often
    "warehouse_sql",        # needs a live gated BQ run at demo time
]


class DemoQuestion(BaseModel):
    """A question worth asking live in the demo — ONLY if the graph can
    provably answer it. The gate verifies every claimed capability
    against the built graph before it reaches the demo script."""

    question: str                             # exec-friendly phrasing
    audience: Literal["analyst", "vp", "c_suite"]
    answered_by: list[DemoCapability]         # capabilities the answer uses
    grounding: list[str]                      # real table/column/metric/skill names
    expected_answer_sketch: str               # 1-2 sentences: what the graph says
    wow_factor: str                           # 1 sentence: why this lands


class SelfAssessment(BaseModel):
    tables_skipped_for_lack_of_signal: list[str] = Field(default_factory=list)
    columns_marked_ambiguous: int = Field(ge=0)
    proposed_entities_with_low_evidence: list[str] = Field(default_factory=list)
    requires_steward_attention: list[str] = Field(default_factory=list)


class EnrichmentBundle(BaseModel):
    """One bundle per table. The LLM emits exactly this shape."""

    table_name: str
    table_description_proposal: str | None = None
    column_observations: list[ColumnObservation] = Field(default_factory=list)
    candidate_synonyms: list[CandidateSynonym] = Field(default_factory=list)
    candidate_code_resolutions: list[CodeResolution] = Field(default_factory=list)
    candidate_filter_rationale: list[FilterRationale] = Field(default_factory=list)
    candidate_demo_questions: list[DemoQuestion] = Field(default_factory=list)
    self_assessment: SelfAssessment


class EntityProposal(BaseModel):
    """Reduced from many ColumnObservations across the memory.

    Goes into review_queue/ENTITIES.md for steward approval. Stewards say
    yes → entity gets minted as a real graph node. No → reason captured
    as negative training memory for next run."""

    proposed_name: str
    identified_by_columns: list[str]
    materialized_in_tables: list[str]
    relationships: list[RelationProposal] = Field(default_factory=list)
    conflict_signals: list[str] = Field(default_factory=list)
    evidence_packet_refs: list[str] = Field(default_factory=list)  # observation IDs
    aggregate_self_confidence: float = Field(ge=0.0, le=1.0)
    n_supporting_observations: int = Field(ge=0)
    requires_steward_review: bool = True
