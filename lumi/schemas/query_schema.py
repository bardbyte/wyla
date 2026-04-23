"""Parsed-query schema. Every gold query becomes one ParsedQuery."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Measure(BaseModel):
    """A SQL aggregation extracted from a gold query (SUM, COUNT, AVG, ...)."""

    function: str = Field(..., description='Uppercase aggregation name, e.g. "SUM".')
    column: str | None = Field(
        None, description="Source column, if any. None for SUM(1), COUNT(*), etc."
    )
    distinct: bool = False
    expression: str = Field(..., description="Original SQL text of the aggregation.")


class Filter(BaseModel):
    """A WHERE clause predicate."""

    column: str
    operator: str = Field(..., description='e.g. "=", "IN", "BETWEEN", ">=".')
    value: str = Field(..., description="Right-hand side as SQL text.")


class JoinCondition(BaseModel):
    """One ON-clause equality."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = Field("inner", description='"inner", "left", "right", "full", "cross".')


class ParsedQuery(BaseModel):
    """One row from the gold queries Excel, fully extracted."""

    query_id: str = Field(..., description='Stable id, e.g. "q_000".')
    user_prompt: str
    expected_sql: str
    difficulty: str | None = None

    tables: list[str] = Field(default_factory=list)
    primary_table: str | None = Field(
        None, description="First FROM table; how we group queries by view."
    )
    measures: list[Measure] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list, description="GROUP BY + non-agg SELECT.")
    filters: list[Filter] = Field(default_factory=list)
    joins: list[JoinCondition] = Field(default_factory=list)

    parse_error: str | None = Field(
        None, description="If sqlglot choked, the error is recorded here and the row is still kept."
    )


class JoinPattern(BaseModel):
    """Distinct join signature across the corpus of gold queries."""

    tables: list[str] = Field(..., description="Sorted set of tables in the pattern.")
    joins: list[JoinCondition]
    query_ids: list[str] = Field(
        default_factory=list, description="Which gold queries use this pattern."
    )

    @property
    def signature(self) -> str:
        """Stable hash-like string used for dedup."""
        tbls = "|".join(sorted(self.tables))
        jns = "|".join(
            sorted(
                f"{j.left_table}.{j.left_column}={j.right_table}.{j.right_column}"
                for j in self.joins
            )
        )
        return f"{tbls}::{jns}"
