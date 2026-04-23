"""View schemas — both the parsed-from-disk form and the LLM-emitted enriched form."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

FieldKind = Literal["dimension", "measure", "dimension_group", "filter", "parameter"]
EnrichmentOrigin = Literal["gold_query", "mdm", "inferred", "existing_preserved"]


class ParsedField(BaseModel):
    """A single field as read from a .view.lkml via the lkml library."""

    name: str
    kind: FieldKind
    type: str | None = Field(None, description='LookML type, e.g. "string", "number", "date".')
    sql: str | None = Field(None, description="Raw sql: expression.")
    label: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    existing_attributes: dict[str, str] = Field(
        default_factory=dict,
        description="Any other attributes we preserve verbatim (hidden, group_label, etc.).",
    )


class ParsedView(BaseModel):
    """A parsed .view.lkml file. The substrate we enrich."""

    view_name: str
    source_path: str = Field(..., description="Path within cloned repo, for provenance.")
    sql_table_name: str | None = None
    derived_table_sql: str | None = None
    fields: list[ParsedField] = Field(default_factory=list)

    @property
    def field_count(self) -> int:
        return len(self.fields)

    def field_by_name(self, name: str) -> ParsedField | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None


class EnrichedField(BaseModel):
    """The LLM's structured output per field.

    Preserves existing good content (via origin='existing_preserved') — never deletes.
    """

    name: str
    kind: FieldKind
    type: str | None = None
    sql: str | None = Field(
        None,
        description="SQL expression. For newly created measures, the LLM may produce this.",
    )
    label: str = Field(..., description="Human-readable display name (MDM canonical preferred).")
    description: str = Field(
        ..., min_length=1, description="Rich description using user vocabulary + MDM definitions."
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Synonyms, business glossary terms. Include 'inferred' for origin='inferred'.",
    )
    origin: EnrichmentOrigin = Field(
        ...,
        description=(
            "gold_query = seen in 137 queries (highest quality); "
            "mdm = MDM-informed; inferred = LLM-only (tag as 'inferred'); "
            "existing_preserved = original description retained verbatim."
        ),
    )


class EnrichedView(BaseModel):
    """LLM output schema for one view. This is what ViewEnricher emits."""

    view_name: str
    view_label: str = Field(..., description="Human display name for the view.")
    view_description: str = Field(
        ...,
        min_length=1,
        description="What this view represents. Mentions data_source='cornerstone' when applicable.",
    )
    fields: list[EnrichedField] = Field(..., min_length=1)
    derived_dimensions_added: list[str] = Field(
        default_factory=list,
        description="Names of dimensions created from CASE WHEN patterns in gold SQL.",
    )
    measures_added: list[str] = Field(
        default_factory=list,
        description="Names of measures created to cover missing aggregations.",
    )
