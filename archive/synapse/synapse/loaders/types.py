"""Typed loader contracts — uniform shape across every source."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


class LoadResult(BaseModel):
    """Uniform return type for every loader.

    Shape matches the ADK FunctionTool result convention {status, error?}
    so the same function works as a plain Python call and as an agent tool.
    """

    status: Literal["ok", "error", "skipped", "partial"]
    source: str                                          # e.g. "bq", "mdm", "ghe_lookml"
    table_id: str
    artifacts_written: list[Path] = Field(default_factory=list)
    records_count: int = 0
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)
    cost_estimate_usd: float = 0.0
    latency_ms: int = 0
    cache_hit: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
