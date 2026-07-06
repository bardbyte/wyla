"""Typed chart specs — what the agent is allowed to ask for.

Constraints from the visualization discipline are encoded as validators,
so violations come back as instructive errors the model can self-correct
on (poka-yoke), not as bad charts a VP has to squint at:

  * one value axis, always (dual-axis is unrepresentable here)
  * ≤ 4 series per chart — more must fold into "Other" or split into
    small multiples (the error says so)
  * tables cap at 50 rows (auto-truncated, flagged)
  * categorical colors are assigned by fixed position, never chosen
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, field_validator, model_validator

NumberFormat = Literal["number", "percent", "currency", "bps"]

_MAX_SERIES = 4
_MAX_TABLE_ROWS = 50
_MAX_CATEGORIES = 24


class StatTile(BaseModel):
    """A single headline number — often the right 'chart' for C-suite."""

    kind: Literal["stat"] = "stat"
    title: str
    value: float
    format: NumberFormat = "number"
    delta: float | None = None            # change vs comparison period
    delta_is_good: bool | None = None     # None → neutral styling
    delta_label: str = ""                 # e.g. "MoM", "vs plan"
    subtitle: str = ""
    footnote: str = ""


class Series(BaseModel):
    name: str
    points: list[tuple[str, float | None]]  # (x label, value); None = gap


class LineChart(BaseModel):
    """Change over time. Single series needs no legend; ≤4 direct-labeled."""

    kind: Literal["line"] = "line"
    title: str
    series: list[Series] = Field(min_length=1)
    format: NumberFormat = "number"
    caption: str = ""

    @field_validator("series")
    @classmethod
    def _max_series(cls, v: list[Series]) -> list[Series]:
        if len(v) > _MAX_SERIES:
            raise ValueError(
                f"{len(v)} series — max is {_MAX_SERIES}. Fold the tail into "
                "an 'Other' series or render small multiples (one chart per "
                "group) instead."
            )
        return v


class BarChart(BaseModel):
    """Magnitude by category. horizontal=True is the top-N ranking form."""

    kind: Literal["bar"] = "bar"
    title: str
    categories: list[str] = Field(min_length=1, max_length=_MAX_CATEGORIES)
    series: list[Series] = Field(min_length=1)
    stacked: bool = False
    horizontal: bool = False
    format: NumberFormat = "number"
    caption: str = ""

    @field_validator("series")
    @classmethod
    def _max_series(cls, v: list[Series]) -> list[Series]:
        if len(v) > _MAX_SERIES:
            raise ValueError(
                f"{len(v)} series — max is {_MAX_SERIES}. Fold into 'Other' "
                "or use small multiples."
            )
        return v

    @model_validator(mode="after")
    def _series_match_categories(self) -> "BarChart":
        for s in self.series:
            if len(s.points) != len(self.categories):
                raise ValueError(
                    f"series {s.name!r} has {len(s.points)} points but there "
                    f"are {len(self.categories)} categories — every series "
                    "must supply one value per category (use null for gaps)."
                )
        return self


class DataTable(BaseModel):
    """The analyst's form — exact values. Auto-truncates past 50 rows."""

    kind: Literal["table"] = "table"
    title: str
    columns: list[str] = Field(min_length=1)
    rows: list[list[Any]]
    caption: str = ""
    truncated: bool = False

    @model_validator(mode="after")
    def _cap_rows(self) -> "DataTable":
        for i, row in enumerate(self.rows):
            if len(row) != len(self.columns):
                raise ValueError(
                    f"row {i} has {len(row)} cells for {len(self.columns)} "
                    "columns"
                )
        if len(self.rows) > _MAX_TABLE_ROWS:
            self.rows = self.rows[:_MAX_TABLE_ROWS]
            self.truncated = True
        return self


ChartSpec = Annotated[
    Union[StatTile, LineChart, BarChart, DataTable],
    Field(discriminator="kind"),
]


class DashboardItem(BaseModel):
    spec: ChartSpec
    span: int = Field(default=6, ge=3, le=12)  # 12-column grid


class Dashboard(BaseModel):
    """A composed page: stat row on top, evidence charts below."""

    kind: Literal["dashboard"] = "dashboard"
    title: str
    subtitle: str = ""
    items: list[DashboardItem] = Field(min_length=1, max_length=12)
    footer: str = ""                       # provenance line goes here


class _SpecEnvelope(BaseModel):
    spec: ChartSpec


def parse_spec(payload: dict[str, Any]) -> StatTile | LineChart | BarChart | DataTable | Dashboard:
    """Validate a raw dict into a spec; raises pydantic.ValidationError with
    self-correction guidance baked into the messages."""
    if payload.get("kind") == "dashboard":
        return Dashboard.model_validate(payload)
    return _SpecEnvelope.model_validate({"spec": payload}).spec
