"""Uniform response envelope — every tool answer carries provenance.

Non-negotiable per MCP_SERVER_SPEC §3: no tool ever returns bare facts.
`status` + `data` + structured `error` + `meta` (tool version, snapshot
version, latency, cache flag, tenant). Facts inside `data` carry their own
`confidence_tier` + `sources`.
"""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class ErrorDetail(BaseModel):
    code: Literal[
        "not_found", "ambiguous", "low_confidence", "invalid_input",
        "rate_limited", "internal_error", "stale_snapshot",
    ]
    message: str
    suggestions: list[str] = Field(default_factory=list)


class ResponseMeta(BaseModel):
    tool_name: str
    tool_version: str = "1.0.0"
    snapshot_version: str = "unversioned"
    latency_ms: int = 0
    cached: bool = False
    tenant_id: str = "default"
    warnings: list[str] = Field(default_factory=list)


class SynapseResponse(BaseModel, Generic[T]):
    status: Literal["ok", "error", "partial"]
    data: T | None = None
    error: ErrorDetail | None = None
    meta: ResponseMeta
