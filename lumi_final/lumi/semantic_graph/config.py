"""Semantic graph configuration — env-gated, with sane defaults.

The single source of truth for: AGE connection, source weights,
promotion thresholds, decay windows. All other modules read from here.

Production-grade discipline: every threshold has a comment explaining
WHY this value, so when someone needs to tune they have context.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# ─── Feature flag ────────────────────────────────────────────


def is_age_enabled() -> bool:
    """When False, writer.record() is a no-op for AGE — JSONL still writes.

    Default off so dev environments without Postgres+AGE keep working.
    Set ``LUMI_AGE_ENABLED=1`` to enable AGE writes.
    """
    return os.environ.get("LUMI_AGE_ENABLED", "").lower() in {"1", "true", "yes"}


# ─── PostgreSQL + AGE connection ─────────────────────────────


@dataclass(frozen=True)
class AGEConnection:
    """psycopg connection parameters. Defaults match a stock dev Postgres."""

    host: str = field(default_factory=lambda: os.environ.get("LUMI_PG_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.environ.get("LUMI_PG_PORT", "5432")))
    database: str = field(default_factory=lambda: os.environ.get("LUMI_PG_DATABASE", "lumi"))
    user: str = field(default_factory=lambda: os.environ.get("LUMI_PG_USER", "lumi"))
    password: str = field(default_factory=lambda: os.environ.get("LUMI_PG_PASSWORD", "lumi"))
    graph_name: str = field(default_factory=lambda: os.environ.get("LUMI_AGE_GRAPH", "lumi_semantic"))

    def conninfo(self) -> str:
        """libpq connection string for psycopg.connect()."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


# ─── Node + edge label registry ──────────────────────────────


# The 15 semantic node types. Operational nodes (Event, Approval) listed too.
NODE_LABELS: tuple[str, ...] = (
    # Semantic nodes (the actual knowledge)
    "Table",
    "Column",
    "Entity",
    "Metric",
    "Filter",
    "FilterValue",
    "TimeGrain",
    "QuestionPattern",
    "Explore",
    "View",
    "Synonym",
    "Threshold",
    "Cohort",  # NEW Phase 0d — graph-side node for cohort_scope_signals
    # Operational nodes (provenance + audit)
    "Source",
    "Event",
    "Approval",
)

# The 15 edge types. JOIN_PATH replaces the prior n-ary USES_PATH+STEP
# per the 4-lens-review consolidation (binary edge with step_ordinal).
EDGE_LABELS: tuple[str, ...] = (
    "CONTAINS",
    "IDENTIFIES",
    "EQUIVALENT_TO",
    "RELATES_TO",
    "COMPUTED_FROM",
    "OBSERVED_AT_GRAIN",
    "COMPANION_OF",
    "HAS_SYNONYM",
    "REQUIRES_FILTER",
    "RENDERED_AS_VIEW",
    "RENDERED_AS_EXPLORE",
    "ANSWERS",
    "JOIN_PATH",
    "ASSERTS",
    "LOCKS",
    "DEPRECATES",
)


# ─── Source weights (Lens-1 finding: parametric thresholds) ──


# Multiplier applied to evidence counts based on source quality.
# Tuned for low-evidence environment (122 SQLs, 29 tables). Increase
# corpus weight once we have N>1000 queries.
SOURCE_WEIGHTS: dict[str, float] = {
    "human_approval": 10.0,
    "mdm": 3.0,
    "baseline_lookml": 2.0,
    "corpus_sql": 1.0,
    "llm_inferred": 0.5,
    "bq_probe_confirm": 2.0,
    "bq_probe_contradict": -3.0,
    # New for Phase 5 validation (offline conformance harness):
    "compilation_conformance_confirm": 2.0,
    "compilation_conformance_contradict": -3.0,
}


# ─── Promotion thresholds (Lens-1 finding: parametric) ───────


# Minimum weighted evidence required to promote candidate → promoted
# for each semantic node type. Edge thresholds are also here for the
# few edges that need explicit gates.
#
# Tuning note: these are first-pass for the 122-query bootstrap. As the
# corpus grows, raise Entity to 8 and Metric to 5 to reduce noise.
PROMOTION_THRESHOLDS: dict[str, float] = {
    "Table": 1.0,
    "Column": 1.0,
    "Entity": 5.0,
    "Metric": 3.0,
    "Filter": 1.0,
    "FilterValue": 2.0,
    "TimeGrain": 1.0,
    "QuestionPattern": 1.0,
    "Synonym": 3.0,
    "Threshold": 2.0,
    "Cohort": 2.0,
    # Edge gates
    "EQUIVALENT_TO": 1.0,
    "RELATES_TO": 5.0,
    "COMPUTED_FROM": 1.0,
    "REQUIRES_FILTER": 1.0,  # used with frequency ratio (≥50% of cluster members)
}


# ─── Decay windows (days) ────────────────────────────────────


# How long without a confirming event before a promoted claim drops one
# confidence tier. Locks override decay; decay is reversible (a new event
# bumps the timestamp).
DECAY_WINDOWS_DAYS: dict[str, int] = {
    "Table": 90,
    "Column": 90,
    "Entity": 60,
    "Metric": 90,
    "Filter": 60,
    "FilterValue": 90,
    "TimeGrain": 90,
    "QuestionPattern": 60,
    "Synonym": 365,  # synonyms accumulate slowly; long window
    "Threshold": 180,
    "Cohort": 60,
}


# ─── Confidence labels (the 5-tier lattice) ──────────────────


CONFIDENCE_LEVELS: tuple[str, ...] = (
    "deprecated",
    "guessed",
    "inferred",
    "grounded",
    "human_asserted",
)


def confidence_rank(label: str) -> int:
    """Higher number = higher confidence. Used by conflict resolution."""
    try:
        return CONFIDENCE_LEVELS.index(label)
    except ValueError:
        return -1


# ─── Schema version (for migrations) ─────────────────────────


SCHEMA_VERSION: str = "v1.0"


# ─── Tenancy (multi-tenant ready) ────────────────────────────


def default_tenant_id() -> str:
    return os.environ.get("LUMI_TENANT_ID", "amex_us_consumer")
