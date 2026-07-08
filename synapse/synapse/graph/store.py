"""In-memory typed graph store.

Every node carries the provenance envelope:
    - which of the 7 sources contributed
    - confidence tier (deprecated < guessed < inferred < grounded < human_asserted)
    - calibrated confidence score (0..1)
    - evidence event IDs (back-references for audit)

Plus node-type-specific properties: Column profiling, Table usage, etc.

No AGE / no networkx dependency — pure pydantic + dict so the demo
runs anywhere. The shape is intentionally JSON-serializable so the
inspector output is directly consumable by a Streamlit UI or an MCP
tool surface.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Literal

from pydantic import BaseModel, Field


SourceName = Literal[
    "mdm", "corpus", "bq", "glossary", "metric_catalog",
    "table_catalog", "baseline_lookml", "usage", "human_approval",
    # Dataplex-style additions:
    #   llm_generated → AI-suggested descriptions (Knowledge Catalog parallel)
    #   dq_engine     → rule-based data-quality checks (Auto DQ parallel)
    "llm_generated", "dq_engine",
    # Skills library — human-curated analytics skill packages (skill.yaml +
    # knowledge.md + metric_contracts.yaml). Expert testimony: narrow
    # coverage, highest per-fact trust short of an explicit human approval.
    "skills",
]

ConfidenceTier = Literal[
    "deprecated", "guessed", "inferred", "grounded", "human_asserted",
]


# ─── Source weights (calibration prior) ──────────────────────


SOURCE_WEIGHTS: dict[SourceName, int] = {
    "human_approval":  10,  # steward sign-off — the human ceiling
    # MDM is the metadata spine: it carries the business names, ownership,
    # sensitivity, and taxonomy for every column, so it is the highest DATA
    # authority. BQ is the physical truth (schema + profile) — second. With
    # both agreeing a fact reaches grounded; MDM alone is inferred.
    "mdm":              8,
    "bq":               6,
    "metric_catalog":   5,  # curated metric registry
    "glossary":         5,  # curated term registry
    "dq_engine":        4,  # rule-based DQ checks — system-attested
    "baseline_lookml":  3,
    "table_catalog":    3,
    # Skills are guardrails + agent knowledge, NOT a data-grounding witness:
    # a skill "applying to" a table says nothing about the table's data
    # quality, so skills no longer witness Table/Column tiers (see builder).
    "skills":           2,
    "corpus":           1,  # per observation
    "usage":            1,  # per observation
    "llm_generated":    1,  # AI-suggested; trust low until corroborated
}


# Per-source evidence-count cap. One chatty source (e.g. 200 corpus
# observations of the same JOIN) shouldn't outweigh 4 independent sources
# agreeing once each. Distinct-source breadth > single-source depth.
_PER_SOURCE_COUNT_CAP = 5
_SCORE_DENOMINATOR = 15.0


def confidence_from_sources(
    sources: list[SourceName], evidence_counts: dict[SourceName, int] | None = None,
) -> tuple[float, ConfidenceTier]:
    """Map a set of contributing sources + their counts to (score, tier).

    Calibration prior (until Phase 3 ships proper Platt calibration):

        weighted = Σ SOURCE_WEIGHTS[s] × min(count, 5)
        score    = min(0.99, weighted / 15)
        distinct = #unique sources contributing

        human_asserted  if human_approval in sources
        grounded        if distinct ≥ 4
                        OR (distinct ≥ 3 AND score ≥ 0.70)
                        OR score ≥ 0.90
        inferred        if distinct ≥ 2
                        OR score ≥ 0.45
        guessed         otherwise

    The distinct-source breadth gate matters more than the weighted sum:
    five independent sources concurring is stronger evidence than one
    catalog claim, even if the catalog has a high prior weight.
    """
    counts = evidence_counts or {s: 1 for s in sources}
    distinct_sources = set(sources)
    weighted = 0.0
    for s in distinct_sources:
        w = SOURCE_WEIGHTS.get(s, 0)
        n = min(max(counts.get(s, 1), 1), _PER_SOURCE_COUNT_CAP)
        weighted += w * n
    score = min(0.99, weighted / _SCORE_DENOMINATOR)
    distinct = len(distinct_sources)
    if "human_approval" in distinct_sources:
        return 0.99, "human_asserted"
    if distinct >= 4 or (distinct >= 3 and score >= 0.70) or score >= 0.90:
        return score, "grounded"
    if distinct >= 2 or score >= 0.45:
        return score, "inferred"
    return score, "guessed"


# ─── Node / edge schemas ─────────────────────────────────────


class Provenance(BaseModel):
    """The 7-source attribution envelope on every node and edge."""

    sources: list[SourceName] = Field(default_factory=list)
    evidence_count_by_source: dict[SourceName, int] = Field(default_factory=dict)
    evidence_event_ids: list[str] = Field(default_factory=list)
    first_observed_at: str = ""
    last_observed_at: str = ""
    confidence_score: float = 0.0
    confidence_tier: ConfidenceTier = "guessed"
    conflicts: list[str] = Field(default_factory=list)

    def record_source(
        self, source: SourceName, *,
        event_id: str | None = None, count_delta: int = 1,
    ) -> None:
        if source not in self.sources:
            self.sources.append(source)
        self.evidence_count_by_source[source] = (
            self.evidence_count_by_source.get(source, 0) + count_delta
        )
        if event_id:
            self.evidence_event_ids.append(event_id)
        ts = _dt.datetime.now(_dt.timezone.utc).isoformat()
        if not self.first_observed_at:
            self.first_observed_at = ts
        self.last_observed_at = ts
        score, tier = confidence_from_sources(
            self.sources, self.evidence_count_by_source,
        )
        self.confidence_score = score
        self.confidence_tier = tier


# Node-type-specific property containers
class TableProperties(BaseModel):
    business_name: str = ""
    description: str = ""
    fqn: str = ""
    company_domain: str = ""
    data_domain: str = ""
    is_in_dmp: bool = False
    row_count: int | None = None
    last_modified: str | None = None
    partition_field: str | None = None
    clustering_fields: list[str] = Field(default_factory=list)
    owner_team: str = ""
    # Usage signal (populated by profiler)
    total_queries_observed: int = 0
    top_users: list[dict[str, Any]] = Field(default_factory=list)
    peak_query_hours: list[int] = Field(default_factory=list)
    # Dataplex Catalog-style asset abstraction
    asset_kind: str = "Table"  # Table | View | MaterializedView | ExternalTable | BIDashboard
    tags: list[str] = Field(default_factory=list)
    lineage_upstream: list[str] = Field(default_factory=list)


class ColumnProperties(BaseModel):
    table_name: str
    data_type: str = ""
    is_nullable: bool = True
    description: str = ""
    business_name: str = ""
    is_primary: bool = False
    is_dedupe_key: bool = False
    is_partitioning: bool = False
    cluster_position: int | None = None
    # Profiling
    cardinality_bucket: str = "unknown"  # low | medium | high | very_high
    approx_distinct: int | None = None
    null_fraction: float | None = None
    min_value: Any = None
    max_value: Any = None
    distinct_sample: list[dict[str, Any]] = Field(default_factory=list)
    # PII / governance — MDM is the authority for all of these
    pii_taxonomy: str = "Internal"        # pii_role_id, e.g. Sensitive>Identifier>MemberID
    is_pii: bool = False
    is_sensitive: bool = False
    is_gdpr: bool = False
    is_critical_data_element: bool = False
    # MDM attribute-derivation logic (present → column is computed, not raw;
    # the enrichment skill uses this to abstain from entity-tagging it)
    derived_logic: str = ""
    external_references: list[Any] = Field(default_factory=list)
    # Usage
    reference_count: int = 0
    is_filter: bool = False
    is_group_by: bool = False
    is_join_key: bool = False
    # Code-resolution
    is_coded: bool = False
    resolved_by_table: str | None = None  # lookup table that decodes it
    # Knowledge Catalog-style AI suggestion (Dataplex AI parallel)
    ai_generated_description: str = ""


class EntityProperties(BaseModel):
    description: str = ""
    materialized_in_tables: list[str] = Field(default_factory=list)
    id_columns: list[str] = Field(default_factory=list)


class MetricProperties(BaseModel):
    business_name: str = ""
    formula: str = ""
    grain: str = ""
    domain: str = ""
    sourced_from_table: str = ""
    synonyms: list[str] = Field(default_factory=list)


class SynonymProperties(BaseModel):
    canonical_entity: str = ""
    surface_form: str = ""
    business_unit: str = ""
    region: str = ""
    entry_type: str = ""  # Acronym | Code | Abbreviation


class UserProperties(BaseModel):
    email: str = ""
    team: str = ""


class CodeMappingProperties(BaseModel):
    column: str = ""
    raw_value: str = ""
    human_meaning: str = ""
    source: str = ""  # "lookup_table" | "case_when" | "llm_inferred"


class FilterValueProperties(BaseModel):
    table_name: str = ""
    column_name: str = ""
    value: str = ""
    count_obs: int = 0
    is_structural: bool = False


class SkillProperties(BaseModel):
    """A curated analytics skill package (skill.yaml + knowledge.md + SQL).

    Skills are the L3 semantic witness: they carry business definitions,
    metric contracts, and guardrails a generated query must respect.
    """

    skill_id: str = ""
    domain: str = ""            # e.g. "new_accounts" | "portfolio_analytics"
    description: str = ""
    tables_used: list[str] = Field(default_factory=list)
    metrics_defined: list[str] = Field(default_factory=list)
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    knowledge_excerpt: str = ""  # first ~2k chars of knowledge.md
    files: list[str] = Field(default_factory=list)


class GuardrailProperties(BaseModel):
    """A first-class 'never do this' rule mined from a skill package.

    Guardrails are graph nodes (not doc strings) so an agent can query
    'what constraints apply before I emit SQL touching X'.
    """

    rule: str = ""               # imperative sentence, e.g. "never expose cm11_encrypted"
    category: str = "other"      # sql_generation | privacy | metric_math | grain | other
    applies_to: list[str] = Field(default_factory=list)  # table/column/metric names
    skill_id: str = ""
    severity: str = "error"      # error | warning
    machine_checkable: bool = False  # can validate_sql_plan enforce it statically?


class DataQualityRuleProperties(BaseModel):
    """Dataplex Auto-DQ-style rule attached to a column or table."""

    target_table: str = ""
    target_column: str | None = None  # None → table-level rule
    rule_kind: str = ""  # not_null | unique | range | enum | freshness | row_count | custom_sql
    threshold: str = ""  # e.g. "null_pct < 0.01", "row_count > 1_000_000"
    last_run_status: str = "unknown"  # pass | fail | warning | unknown
    last_run_at: str = ""
    last_run_value: str = ""
    severity: str = "warning"  # error | warning | info
    auto_suggested: bool = False  # AI-suggested vs human-authored


# Type-erased property union
NodeProperties = (
    TableProperties | ColumnProperties | EntityProperties | MetricProperties
    | SynonymProperties | UserProperties | CodeMappingProperties
    | FilterValueProperties | DataQualityRuleProperties
    | SkillProperties | GuardrailProperties
)


class Node(BaseModel):
    canonical_uri: str
    node_type: Literal[
        "Table", "Column", "Entity", "Metric", "Synonym", "User",
        "CodeMapping", "FilterValue", "DataQualityRule",
        "Skill", "Guardrail",
    ]
    properties: dict[str, Any] = Field(default_factory=dict)
    provenance: Provenance = Field(default_factory=Provenance)


class Edge(BaseModel):
    canonical_uri: str  # source URI + ":" + type + ":" + target URI
    from_uri: str
    to_uri: str
    edge_type: Literal[
        "CONTAINS",
        "IDENTIFIES",
        "RELATES_TO",
        "EQUIVALENT_TO",
        "COMPUTED_FROM",
        "SLICEABLE_BY",
        "HAS_SYNONYM",
        "ALWAYS_FILTER",
        "QUERIED_BY",
        "RESOLVED_BY",
        "LOOKUP_TO",
        # Dataplex-style additions:
        "UPSTREAM_OF",     # Table → Table; data lineage
        "VALIDATED_BY",    # Column|Table → DataQualityRule
        # Skills-library additions:
        "APPLIES_TO",      # Skill → Table|Metric; where the skill is authoritative
        "CONSTRAINS",      # Guardrail → Table|Column|Metric; must-respect rule
        "DEFINED_BY",      # Metric → Skill; contract that defines the metric
        # MDM attribute lineage:
        "DERIVES_FROM",    # Column → Column; value derivation with logic
    ]
    properties: dict[str, Any] = Field(default_factory=dict)
    provenance: Provenance = Field(default_factory=Provenance)


# ─── The store ───────────────────────────────────────────────


# Safety-critical governance flags. Once ANY witness asserts one True, a
# later witness's default False must not silently downgrade it — over-
# flagging sensitivity is safe, under-flagging is a governance breach. This
# closes the whole clobber class at the store (the BQ is_pii clobber was one
# instance): sensitivity set by MDM survives a profile pass that omits it.
_STICKY_TRUE = frozenset({"is_pii", "is_sensitive", "is_gdpr"})


class GraphStore(BaseModel):
    """In-memory typed graph. Nodes keyed by canonical_uri."""

    nodes: dict[str, Node] = Field(default_factory=dict)
    edges: dict[str, Edge] = Field(default_factory=dict)
    # Stamped at save time — every MCP/tool response echoes this so a
    # consumer can tell which compiled snapshot answered it.
    snapshot_version: str = "unversioned"

    # Adjacency caches — rebuilt on access
    model_config = {"arbitrary_types_allowed": True}

    # ── node mutations ──

    def upsert_node(
        self,
        node_type: str,
        canonical_uri: str,
        properties: dict[str, Any],
        source: SourceName,
        evidence_event_id: str | None = None,
    ) -> Node:
        if canonical_uri in self.nodes:
            node = self.nodes[canonical_uri]
            # Merge properties (new keys win; existing keys keep first value
            # unless the new value is non-empty). Safety-critical sensitivity
            # flags are monotonic — a later False never downgrades a True.
            for k, v in properties.items():
                if v is None or v == "" or v == []:
                    continue
                if k in _STICKY_TRUE and not v and node.properties.get(k):
                    continue
                node.properties[k] = v
        else:
            node = Node(
                canonical_uri=canonical_uri,
                node_type=node_type,  # type: ignore[arg-type]
                properties=dict(properties),
            )
            self.nodes[canonical_uri] = node
        node.provenance.record_source(source, event_id=evidence_event_id)
        return node

    def upsert_edge(
        self,
        edge_type: str,
        from_uri: str,
        to_uri: str,
        properties: dict[str, Any],
        source: SourceName,
        evidence_event_id: str | None = None,
    ) -> Edge:
        edge_uri = f"{from_uri}::{edge_type}::{to_uri}"
        if edge_uri in self.edges:
            edge = self.edges[edge_uri]
            for k, v in properties.items():
                if v is None or v == "":
                    continue
                edge.properties[k] = v
        else:
            edge = Edge(
                canonical_uri=edge_uri,
                from_uri=from_uri,
                to_uri=to_uri,
                edge_type=edge_type,  # type: ignore[arg-type]
                properties=dict(properties),
            )
            self.edges[edge_uri] = edge
        edge.provenance.record_source(source, event_id=evidence_event_id)
        return edge

    # ── query helpers (no fancy graph algos, just iteration) ──

    def get(self, uri: str) -> Node | None:
        return self.nodes.get(uri)

    def nodes_by_type(self, node_type: str) -> list[Node]:
        return [n for n in self.nodes.values() if n.node_type == node_type]

    def outgoing(self, from_uri: str, edge_type: str | None = None) -> list[Edge]:
        return [
            e for e in self.edges.values()
            if e.from_uri == from_uri
            and (edge_type is None or e.edge_type == edge_type)
        ]

    def incoming(self, to_uri: str, edge_type: str | None = None) -> list[Edge]:
        return [
            e for e in self.edges.values()
            if e.to_uri == to_uri
            and (edge_type is None or e.edge_type == edge_type)
        ]

    # ── serialization ──

    def stats(self) -> dict[str, Any]:
        by_node_type: dict[str, int] = {}
        for n in self.nodes.values():
            by_node_type[n.node_type] = by_node_type.get(n.node_type, 0) + 1
        by_edge_type: dict[str, int] = {}
        for e in self.edges.values():
            by_edge_type[e.edge_type] = by_edge_type.get(e.edge_type, 0) + 1
        by_tier: dict[str, int] = {}
        for n in self.nodes.values():
            by_tier[n.provenance.confidence_tier] = (
                by_tier.get(n.provenance.confidence_tier, 0) + 1
            )
        return {
            "n_nodes": len(self.nodes),
            "n_edges": len(self.edges),
            "nodes_by_type": by_node_type,
            "edges_by_type": by_edge_type,
            "nodes_by_confidence_tier": by_tier,
        }

    def save_json(self, path: "Path | str") -> "Path":
        """Persist the compiled snapshot; stamps `snapshot_version`.

        The version is content-addressed (sha256 of the serialized graph,
        first 12 hex chars) so identical builds share a version and any
        change — however small — produces a new one.
        """
        import hashlib
        from pathlib import Path as _Path

        p = _Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        body = self.model_dump_json(exclude={"snapshot_version"})
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
        ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d")
        self.snapshot_version = f"{ts}-{digest}"
        p.write_text(self.model_dump_json(), encoding="utf-8")
        return p

    @classmethod
    def load_json(cls, path: "Path | str") -> "GraphStore":
        from pathlib import Path as _Path

        return cls.model_validate_json(_Path(path).read_text(encoding="utf-8"))


def normalize_table_name(name: str) -> str:
    """One table, one identity — across every witness.

    Skills say ``common.roll_rate_calc``, gold SQL says
    ``axp-lumi.dw.roll_rate_calc``, MDM says ``roll_rate_calc``; without
    normalization each mints its own node and the witnesses never fuse.
    Canonical identity = the bare table name (last dot segment,
    lowercased, backticks stripped). Qualified forms live on in node
    PROPERTIES (`fqn`, `bq_dataset`) — only the URI collapses.

    Known tradeoff (documented, acceptable for the PoC): two tables with
    the same bare name in different datasets would collide; none exist
    in the current scope.
    """
    cleaned = name.strip().strip("`").lower().replace(" ", "_")
    return cleaned.rsplit(".", 1)[-1] if "." in cleaned else cleaned


def canonical_uri(node_type: str, *parts: str) -> str:
    """Stable URI scheme. Lowercases identifiers, joins with /.

    For Table and Column URIs the table part is normalized (see
    normalize_table_name) so all witnesses land on the same node.
    """
    kind = node_type.lower()
    items = [str(p) for p in parts if p]
    if items and kind in ("table", "column"):
        items[0] = normalize_table_name(items[0])
    body = "/".join(p.lower().replace(" ", "_") for p in items)
    return f"synapse://{kind}/{body}"
