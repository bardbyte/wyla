"""Table narrative — the holistic-understanding context block.

Where ``lumi.grounding`` produces per-column evidence chains, this
module produces the **table-level narrative** Gemini reads BEFORE
diving into per-column reasoning. Aggregates every description and
naming signal we have on disk into a single dense Markdown section
designed for top-tier context engineering.

Inputs:
  - TableContext (with the expanded MDM digest + deep baseline extraction
    + temp_tables_referencing_this from earlier work)
  - All SQLFingerprints across the corpus (for SQL alias intelligence
    + filter-value frequencies + CTE/temp-table semantic naming)

Output:
  - TableNarrative dataclass (structured, JSON-serializable)
  - render_table_narrative() → Markdown for the prompt

Order in the enrichment prompt:
  1. base template (table_name, fingerprint summary, ecosystem brief)
  2. approved enrichment plan (scope contract)
  3. baseline gap analysis
  4. ── this module's output: ## Table narrative ──
  5. grounding signals (per-column evidence)
  6. confidence-labeling rules
  7. SKILL.md sections 1-7

Narrative-before-grounding is intentional: Gemini reads "this is a
cardmember-grain customer-insights table" first, then reads the per-
column evidence with that frame already loaded. Inverting the order
empirically produces worse output.

No LLM calls in this module. Pure deterministic aggregation.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from lumi.schemas import TableContext
from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.narrative")


# ─── Quality filter for SQL aliases ──────────────────────────


# Aliases shorter than this OR matching these patterns are treated as
# noise (a, t1, x, tmp, result, col_3) and don't make it into the
# narrative shown to Gemini. They're still captured per-fingerprint for
# traceability — see fp.select_aliases.
_ALIAS_NOISE_PATTERNS = (
    re.compile(r"^[a-z]\d*$"),         # a, b, c, x12
    re.compile(r"^t\d+$"),             # t1, t2
    re.compile(r"^col_?\d+$"),         # col_3, col1
    re.compile(r"^tmp.*$"),
    re.compile(r"^result.*$"),
    re.compile(r"^x\d*$"),
    re.compile(r"^[a-z]_\d+$"),        # a_1
)
_ALIAS_TRIVIAL_NOUNS = {
    "result", "total", "count", "n", "sum", "avg", "min", "max",
    "value", "data", "row", "id",
}
_ALIAS_MIN_LEN = 4


def is_meaningful_alias(alias: str) -> bool:
    """Filter for aliases that actually carry domain semantics.

    Keeps: total_billed_business, unique_customers, consumer_segment,
           q4_2024_revenue, customer_lifecycle_stage
    Drops: a, t1, x, tmp, col_3, count, total
    """
    if not alias or len(alias) < _ALIAS_MIN_LEN:
        return False
    a = alias.lower()
    if a in _ALIAS_TRIVIAL_NOUNS:
        return False
    for pat in _ALIAS_NOISE_PATTERNS:
        if pat.match(a):
            return False
    # Must look like a phrase: contains underscore OR has 2+ word
    # boundaries via camelCase.
    has_underscore = "_" in a
    has_camel = any(c.isupper() for c in alias[1:])
    return has_underscore or has_camel


# ─── Structured narrative ────────────────────────────────────


@dataclass
class ColumnDescription:
    """One column's best available description, sourced + scored."""

    column: str
    description: str | None
    business_name: str | None
    source: str  # "mdm" | "baseline" | "alias_inference" | "missing"
    length: int = 0
    coverage_score: float = 0.0  # 0..1; how many sources support this column


@dataclass
class TableNarrative:
    """Holistic-understanding payload assembled from every metadata source."""

    table_name: str

    # Identity (from MDM)
    table_business_name: str | None = None
    table_description: str | None = None    # MDM data_desc
    table_type: str | None = None            # MDM dataset_details.table_type
    feed_type: str | None = None
    load_type: str | None = None
    data_category: str | None = None
    data_sub_category: str | None = None
    retention_period: int | None = None      # days
    is_internal: bool | None = None
    is_sor_certified: bool | None = None
    is_history_required: bool | None = None
    is_decommissioned: bool = False

    # BQ source
    bq_project: str | None = None
    bq_dataset: str | None = None
    bq_table: str | None = None
    sql_table_name_baseline: str | None = None

    # Ownership for view header comment
    business_contacts: list[dict[str, str]] = field(default_factory=list)
    tech_contacts: list[dict[str, str]] = field(default_factory=list)
    imr_queue: str | None = None
    aim_id: str | None = None

    # View-level baseline
    baseline_view_description: str | None = None
    baseline_view_label: str | None = None
    baseline_extends_chain: list[str] = field(default_factory=list)

    # Description corpus (per-column, every available source)
    column_descriptions: list[ColumnDescription] = field(default_factory=list)
    description_coverage_pct: float = 0.0

    # SQL alias intelligence — analysts' own glossary, quality-filtered
    alias_to_column: dict[str, str] = field(default_factory=dict)
    column_to_aliases: dict[str, list[str]] = field(default_factory=dict)
    alias_observations: int = 0      # how many filtered aliases found

    # Baseline alias map (rename pairs from baseline LookML)
    baseline_aliases: dict[str, str] = field(default_factory=dict)

    # Topical clusters from MDM data_category × name patterns
    semantic_clusters: dict[str, list[str]] = field(default_factory=dict)

    # CTE / temp table semantic naming (named intermediate concepts)
    cte_concepts: list[dict[str, Any]] = field(default_factory=list)
    temp_table_concepts: list[dict[str, Any]] = field(default_factory=list)

    # Filter-value frequencies (which values analysts actually use)
    filter_value_frequencies: dict[str, list[tuple[str, int]]] = field(default_factory=dict)

    # Columns with MDM-declared computation logic
    columns_with_formulas: list[dict[str, str]] = field(default_factory=list)

    # PII / sensitivity tags surfaced from MDM (cm11 → NGBD-SDE-CM11 etc.)
    pii_role_assignments: list[dict[str, str]] = field(default_factory=list)


# ─── Builder ─────────────────────────────────────────────────


def build_table_narrative(
    ctx: TableContext,
    all_fingerprints: list[SQLFingerprint] | None = None,
) -> TableNarrative:
    """Aggregate every metadata source into the structured narrative.

    Args:
        ctx: TableContext with expanded MDM digest + deep baseline extract.
        all_fingerprints: every parsed SQL — used for alias aggregation
            + filter-value frequency across the corpus. None falls back
            to per-table-level signals only.
    """
    n = TableNarrative(table_name=ctx.table_name)
    fps = all_fingerprints or []

    # ── Identity from MDM dataset_details ──
    dataset = ctx.mdm_dataset_details or {}
    n.table_business_name = (
        # Prefer business_name from the MDM digest's top-level (also lifted
        # from dataset_details).
        ctx.mdm_table_description and ctx.mdm_table_description  # type narrow
    )
    # Fields are lifted to top-level by _digest, but mdm_dataset_details
    # also carries the same dataset-level fields. Use whichever is present.
    n.table_description = ctx.mdm_table_description
    n.table_type = dataset.get("table_type")
    n.feed_type = dataset.get("feed_type")
    n.load_type = dataset.get("load_type")
    n.data_category = dataset.get("data_category")
    n.data_sub_category = dataset.get("data_sub_category")
    n.retention_period = dataset.get("retention_period")
    n.is_internal = dataset.get("is_internal")
    n.is_sor_certified = dataset.get("is_sor_certified")
    n.is_history_required = dataset.get("is_history_required")
    n.is_decommissioned = bool(dataset.get("is_decommissioned"))
    n.bq_project = dataset.get("bq_project") or dataset.get("project_id")
    n.bq_dataset = dataset.get("bq_dataset") or dataset.get("dataset_name")
    n.bq_table = dataset.get("bq_table") or dataset.get("table_name")
    # Pull MDM business_name (table-level) from mdm_columns hint or dataset.
    # mdm_dataset_details doesn't have it explicitly because we hoisted to
    # ctx.mdm_table_description; pull from the digest's top-level via
    # mdm_dataset_details.get("business_name") if present.
    if dataset.get("business_name"):
        n.table_business_name = dataset["business_name"]
    else:
        # Otherwise check the MDM digest hint we know is on TableContext.
        n.table_business_name = ctx.mdm_table_description and None  # placeholder

    # ── Ownership ──
    own = ctx.mdm_ownership or {}
    n.business_contacts = list(own.get("business_contacts") or [])
    n.tech_contacts = list(own.get("tech_contacts") or [])
    n.imr_queue = own.get("imr_queue")
    n.aim_id = own.get("aim_id")

    # ── Baseline view-level ──
    n.baseline_view_description = ctx.baseline_view_description
    n.baseline_view_label = ctx.baseline_view_label
    n.sql_table_name_baseline = ctx.baseline_sql_table_name
    n.baseline_extends_chain = list(ctx.baseline_extends_chain or [])
    n.baseline_aliases = dict(ctx.baseline_sql_aliases or {})

    # ── Description corpus ──
    n.column_descriptions = _build_description_corpus(ctx)
    if n.column_descriptions:
        described = sum(1 for c in n.column_descriptions if c.description)
        n.description_coverage_pct = described / len(n.column_descriptions)

    # ── SQL alias intelligence (cross-corpus, quality-filtered) ──
    relevant_fps = _filter_relevant_fingerprints(ctx.table_name, fps)
    n.alias_to_column, n.column_to_aliases, n.alias_observations = (
        _aggregate_aliases(relevant_fps)
    )

    # ── Topical clusters ──
    n.semantic_clusters = _build_semantic_clusters(ctx)

    # ── CTE + temp-table semantic concepts ──
    n.cte_concepts = _build_cte_concepts(ctx)
    n.temp_table_concepts = _build_temp_concepts(ctx)

    # ── Filter-value frequencies ──
    n.filter_value_frequencies = _aggregate_filter_values(relevant_fps)

    # ── Columns with MDM-declared formulas ──
    n.columns_with_formulas = [
        {"column": c.get("name"), "derived_logic": c.get("derived_logic")}
        for c in (ctx.mdm_columns or [])
        if c.get("derived_logic") and str(c.get("derived_logic")).strip()
    ]

    # ── PII role assignments (the cm11-style grounding signals) ──
    n.pii_role_assignments = [
        {
            "column": c.get("name"),
            "pii_role_id": c.get("pii_role_id"),
            "is_critical_data_element": bool(c.get("is_critical_data_element")),
        }
        for c in (ctx.mdm_columns or [])
        if c.get("pii_role_id")
    ]

    return n


# ─── Helpers ─────────────────────────────────────────────────


def _build_description_corpus(ctx: TableContext) -> list[ColumnDescription]:
    """For every column referenced anywhere, build the best available
    description from MDM > baseline > naming-pattern inference.
    """
    out: list[ColumnDescription] = []

    mdm_by_col = {
        (c.get("name") or "").lower(): c for c in (ctx.mdm_columns or [])
    }
    baseline_dims_by_col = {
        (d.get("name") or "").lower(): d
        for d in (ctx.baseline_dimensions or [])
    }
    baseline_msrs_by_col = {
        (m.get("name") or "").lower(): m
        for m in (ctx.baseline_measures or [])
    }

    # Union of every column we know about — referenced + baseline + MDM.
    all_columns: set[str] = set()
    for c in ctx.columns_referenced or []:
        if c:
            all_columns.add(c)
    for c in mdm_by_col:
        all_columns.add(c)
    for c in baseline_dims_by_col:
        all_columns.add(c)
    for c in baseline_msrs_by_col:
        all_columns.add(c)

    for col in sorted(all_columns):
        cl = col.lower()
        mdm = mdm_by_col.get(cl) or {}
        bdim = baseline_dims_by_col.get(cl) or {}
        bmsr = baseline_msrs_by_col.get(cl) or {}

        mdm_desc = (mdm.get("description") or "").strip()
        baseline_desc = (
            (bdim.get("description") or bmsr.get("description") or "").strip()
        )
        business_name = mdm.get("business_name")

        # Score: 1 source = 0.33, 2 sources = 0.67, 3 sources = 1.0.
        score = (
            (1 if mdm_desc else 0)
            + (1 if baseline_desc else 0)
            + (1 if business_name else 0)
        ) / 3.0

        if mdm_desc:
            description = mdm_desc
            source = "mdm"
            length = len(mdm_desc)
        elif baseline_desc and len(baseline_desc) >= 30:
            description = baseline_desc
            source = "baseline"
            length = len(baseline_desc)
        else:
            description = None
            source = "missing"
            length = 0

        out.append(ColumnDescription(
            column=col,
            description=description,
            business_name=business_name,
            source=source,
            length=length,
            coverage_score=round(score, 2),
        ))
    return out


def _filter_relevant_fingerprints(
    table_name: str, fps: list[SQLFingerprint]
) -> list[SQLFingerprint]:
    """Keep only fingerprints that touch this table (directly or via CTE)."""
    out: list[SQLFingerprint] = []
    for fp in fps:
        if fp.parse_error:
            continue
        if table_name in (fp.tables or []):
            out.append(fp)
            continue
        for cte in fp.ctes or []:
            if table_name in (cte.get("source_tables") or []):
                out.append(fp)
                break
        else:
            for tt in fp.temp_tables or []:
                if table_name in (tt.get("source_tables") or []):
                    out.append(fp)
                    break
    return out


def _aggregate_aliases(
    fps: list[SQLFingerprint],
) -> tuple[dict[str, str], dict[str, list[str]], int]:
    """Aggregate quality-filtered SELECT aliases across the corpus.

    Returns:
        (alias→column, column→[aliases], observations_count)
    """
    alias_to_column: dict[str, str] = {}
    column_to_aliases: dict[str, list[str]] = {}
    observations = 0

    for fp in fps:
        for entry in fp.select_aliases or []:
            alias = entry.get("alias") or ""
            col = entry.get("column")
            if not alias or not is_meaningful_alias(alias):
                continue
            if not col:
                continue
            observations += 1
            # Store first observation per alias.
            if alias not in alias_to_column:
                alias_to_column[alias] = col
            # Append to column's alias list (deduped).
            existing = column_to_aliases.setdefault(col, [])
            if alias not in existing:
                existing.append(alias)
    return alias_to_column, column_to_aliases, observations


def _build_semantic_clusters(ctx: TableContext) -> dict[str, list[str]]:
    """Group columns into topical clusters using MDM data_category +
    naming-pattern heuristics.

    Output keys are cluster labels (e.g. "Customer / Identity",
    "Time / Reporting", "Financial"); values are column lists.
    """
    clusters: dict[str, list[str]] = {}

    for col_dict in ctx.mdm_columns or []:
        name = col_dict.get("name")
        if not name:
            continue
        label = _cluster_for_column(name, col_dict, ctx)
        if label:
            clusters.setdefault(label, []).append(name)

    # If MDM is sparse, fall back to columns_referenced with name patterns.
    if not clusters and (ctx.columns_referenced or []):
        for name in ctx.columns_referenced:
            label = _cluster_for_column(name, {}, ctx)
            if label:
                clusters.setdefault(label, []).append(name)

    # Drop tiny clusters (single-col → not a group), but keep the label
    # in metadata so the prompt can still mention it.
    return {k: v for k, v in clusters.items() if len(v) >= 2}


def _cluster_for_column(
    name: str, col_dict: dict[str, Any], ctx: TableContext
) -> str | None:
    """Pick the best topical cluster for one column using:
      1. MDM data_category / data_sub_category (most authoritative)
      2. Naming-pattern heuristics (cm_*, _amt, _dt, _flag, _cd)
    """
    # MDM category takes precedence when present.
    cat = col_dict.get("data_category") or (ctx.mdm_dataset_details or {}).get(
        "data_category"
    )
    if cat:
        return cat.replace("_", " ").title()

    cl = name.lower()
    # Time / date
    if any(t in cl.split("_") for t in ("dt", "date", "ts", "tstmp", "timestamp")):
        return "Time / Reporting"
    # Identifiers (customer-grain when cm_* / cust_*)
    if re.match(r"^(cm|cust|cardmember)\d*$", cl) or cl.startswith(("cm_", "cust_")):
        return "Customer / Identity"
    if cl.endswith(("_id", "_xref_id", "_uuid", "_key")):
        return "Identifiers"
    # Financial — substring match for common money-token suffixes/parts,
    # plus AmEx-domain words ("billed", "business" volume).
    financial_substrings = (
        "amt", "amount", "balance", "revenue", "spend",
        "fee", "cost", "price", "charge", "billed",
    )
    if any(t in cl for t in financial_substrings):
        return "Financial"
    # "_business" suffix (e.g. "billed_business") without bus_ prefix
    if cl.endswith("_business") or cl == "billed_business":
        return "Financial"
    # Status / flags
    if cl.endswith(("_flag", "_ind", "_status")) or "_status_" in cl:
        return "Status / Flags"
    # Codes / categoricals
    if cl.endswith(("_cd", "_code", "_typ", "_type", "_seg", "_band", "_bkt")):
        return "Codes / Categories"
    return None


def _build_cte_concepts(ctx: TableContext) -> list[dict[str, Any]]:
    """CTE alias names are domain glossary the SQL author chose."""
    out: list[dict[str, Any]] = []
    for cte in ctx.ctes_referencing_this or []:
        alias = cte.get("alias")
        if not alias:
            continue
        out.append({
            "name": alias,
            "kind": "cte",
            "source_tables": cte.get("source_tables") or [],
            "structural_filters": cte.get("structural_filters") or [],
        })
    return out


def _build_temp_concepts(ctx: TableContext) -> list[dict[str, Any]]:
    """Temp-table names — named intermediate results, often domain terms.
    Reused temp tables are PDT (persistent derived table) candidates.
    """
    out: list[dict[str, Any]] = []
    for tt in ctx.temp_tables_referencing_this or []:
        alias = tt.get("alias")
        if not alias:
            continue
        out.append({
            "name": alias,
            "kind": "temp_table",
            "is_temp": tt.get("is_temp"),
            "is_replace": tt.get("is_replace"),
            "source_tables": tt.get("source_tables") or [],
        })
    return out


def _aggregate_filter_values(
    fps: list[SQLFingerprint],
) -> dict[str, list[tuple[str, int]]]:
    """Top-N most-frequent filter values per column, across all queries.

    Reveals canonical filter conventions ("90% of queries filter
    data_source = 'cornerstone'"). Combined with observed values from
    grounding signals this becomes allowed_values without BQ access.
    """
    counters: dict[str, Counter] = {}
    for fp in fps:
        for f in fp.filters or []:
            col = f.get("column")
            if not col:
                continue
            value = (f.get("value") or "").strip().strip("'\"`")
            if not value or "(" in value or "select" in value.lower():
                # skip subqueries / IN-list raw forms
                continue
            counters.setdefault(col, Counter())[value] += 1
        # Also count CTE-internal structural filter values.
        for cte in fp.ctes or []:
            for sf in cte.get("structural_filters") or []:
                col = sf.get("column")
                if not col:
                    continue
                value = (sf.get("value") or "").strip().strip("'\"`")
                if value:
                    counters.setdefault(col, Counter())[value] += 1

    # Top-5 per column.
    out: dict[str, list[tuple[str, int]]] = {}
    for col, ctr in counters.items():
        out[col] = ctr.most_common(5)
    return out


# ─── Render to prompt-ready Markdown ─────────────────────────


def render_table_narrative(n: TableNarrative) -> str:
    """Dense Markdown section for the enrichment prompt.

    Format optimized for LLM reading: explicit headers, structured
    bullets, evidence chains. Keeps the holistic "what is this table"
    framing front-loaded so per-column reasoning later is grounded.
    """
    lines: list[str] = [
        f"# Table narrative — `{n.table_name}` (read first for holistic understanding)",
        "",
    ]

    # ── Identity ──
    if n.table_business_name:
        lines.append(f"**Identity**: {n.table_business_name}")
    role = n.table_type or "(no MDM table_type)"
    lines.append(f"**Type**: {role}")
    if n.feed_type or n.load_type:
        feed = n.feed_type or "?"
        load = n.load_type or "?"
        lines.append(f"**Feed**: {feed}  |  **Load type**: {load}")
    if n.data_category:
        cat = n.data_category
        if n.data_sub_category:
            cat += f" / {n.data_sub_category}"
        lines.append(f"**Domain**: {cat}")
    if n.retention_period:
        lines.append(f"**Retention**: {n.retention_period} days")
    if n.is_decommissioned:
        lines.append("**⚠ Decommissioned**: yes — verify before enrichment")
    if n.is_internal is not None:
        lines.append(
            f"**Visibility**: {'internal-only' if n.is_internal else 'shared'}"
        )

    # ── BQ identity ──
    if n.sql_table_name_baseline:
        lines.append(f"**BQ table (baseline)**: `{n.sql_table_name_baseline}`")
    elif n.bq_project and n.bq_dataset and n.bq_table:
        lines.append(
            f"**BQ table (MDM)**: `{n.bq_project}.{n.bq_dataset}.{n.bq_table}`"
        )

    # ── Description (the core narrative paragraph) ──
    lines.append("")
    if n.table_description:
        lines.append("**Description (MDM)**:")
        lines.append(n.table_description)
        lines.append("")
    if (n.baseline_view_description
            and n.baseline_view_description != n.table_description):
        lines.append("**Description (baseline view-level)**:")
        lines.append(n.baseline_view_description)
        lines.append("")

    # ── Ownership header for the published .view.lkml ──
    if n.business_contacts or n.tech_contacts or n.imr_queue:
        lines.append("**Ownership** (use in the published view header comment):")
        if n.business_contacts:
            owners = ", ".join(
                f"{c.get('email', c.get('type', '?'))}"
                for c in n.business_contacts[:3]
            )
            lines.append(f"  - business: {owners}")
        if n.tech_contacts:
            techs = ", ".join(
                f"{c.get('email', c.get('type', '?'))}" for c in n.tech_contacts[:3]
            )
            lines.append(f"  - tech: {techs}")
        if n.imr_queue:
            lines.append(f"  - oncall imr_queue: `{n.imr_queue}`")
        if n.aim_id:
            lines.append(f"  - aim_id: `{n.aim_id}`")
        lines.append("")

    if n.baseline_extends_chain:
        lines.append(
            f"**Refinement chain** (Looker `extends:`): "
            f"{', '.join(n.baseline_extends_chain)}"
        )
        lines.append(
            "  → new fields belong on this view, not the parent unless told otherwise"
        )
        lines.append("")

    # ── Description corpus ──
    if n.column_descriptions:
        cov_pct = n.description_coverage_pct * 100
        lines.append(
            f"### Description corpus — {len(n.column_descriptions)} columns, "
            f"{cov_pct:.0f}% described"
        )
        # Group by semantic cluster when we have one.
        if n.semantic_clusters:
            for cluster_label, cols in sorted(n.semantic_clusters.items()):
                col_set = {c.lower() for c in cols}
                bucket = [
                    cd for cd in n.column_descriptions
                    if cd.column.lower() in col_set
                ]
                if not bucket:
                    continue
                lines.append(f"\n**{cluster_label}**:")
                for cd in bucket[:15]:
                    _render_column_description(cd, lines)
            # Also list "ungrouped" cols.
            grouped = {
                c.lower() for cols in n.semantic_clusters.values() for c in cols
            }
            ungrouped = [
                cd for cd in n.column_descriptions
                if cd.column.lower() not in grouped
            ]
            if ungrouped:
                lines.append("\n**Other**:")
                for cd in ungrouped[:15]:
                    _render_column_description(cd, lines)
        else:
            for cd in n.column_descriptions[:30]:
                _render_column_description(cd, lines)
        lines.append("")

    # ── PII role assignments (the cm11-style grounding signal) ──
    if n.pii_role_assignments:
        lines.append("### MDM-declared PII / sensitivity roles per column")
        lines.append(
            "These tell us specific access roles even when the column "
            "lacks a description. E.g. `cm11 → NGBD-SDE-CM11` means "
            "cm11 is a Sensitive Data Element of the cardmember-11 type — "
            "treat as a customer-grain identifier with PII access controls."
        )
        for assn in n.pii_role_assignments[:15]:
            cde = " [CDE]" if assn.get("is_critical_data_element") else ""
            lines.append(
                f"  - `{assn['column']}` → `{assn['pii_role_id']}`{cde}"
            )
        lines.append("")

    # ── SQL alias intelligence ──
    if n.alias_to_column:
        lines.append(
            f"### Domain aliases analysts gave to columns "
            f"({n.alias_observations} meaningful aliases observed)"
        )
        lines.append(
            "These are the analysts' own glossary — when query authors "
            "rename a column, the chosen name carries domain semantics."
        )
        # Show columns with most aliases first.
        cols_by_alias_count = sorted(
            n.column_to_aliases.items(),
            key=lambda kv: -len(kv[1]),
        )
        for col, aliases in cols_by_alias_count[:10]:
            sample = ", ".join(f'"{a}"' for a in aliases[:5])
            lines.append(f"  - `{col}` ← {sample}")
        lines.append("")

    # ── Baseline rename aliases (human-curated synonyms in baseline view) ──
    if n.baseline_aliases:
        lines.append("### Baseline rename pairs (human-curated synonyms)")
        lines.append(
            "Where the baseline dim NAME differs from its source column, "
            "preserve both — the dim name is a curated synonym."
        )
        for dim_name, source_col in list(n.baseline_aliases.items())[:10]:
            lines.append(f"  - `{dim_name}` ← `{source_col}` (preserve both as tags)")
        lines.append("")

    # ── CTE / temp-table semantic concepts ──
    if n.cte_concepts or n.temp_table_concepts:
        lines.append("### Named intermediate results (domain concepts)")
        for concept in n.cte_concepts:
            lines.append(
                f"  - CTE `{concept['name']}` over "
                f"{concept.get('source_tables') or ['?']}"
            )
        for concept in n.temp_table_concepts:
            kind = "TEMP" if concept.get("is_temp") else "CREATE"
            lines.append(
                f"  - {kind} table `{concept['name']}` "
                f"(PDT candidate) over {concept.get('source_tables') or ['?']}"
            )
        lines.append(
            "  → these names are domain terms the team uses; "
            "consider as derived dim/measure names, NL question variants, or tags."
        )
        lines.append("")

    # ── Filter-value frequencies (canonical slicing) ──
    if n.filter_value_frequencies:
        lines.append("### Canonical filter values (top by frequency across queries)")
        for col, vals in list(n.filter_value_frequencies.items())[:10]:
            top = ", ".join(f"{v}×{cnt}" for v, cnt in vals[:5])
            lines.append(f"  - `{col}`: {top}")
        lines.append(
            "  → these ARE the allowed_values for these columns "
            "(observed in WHERE = / IN literals across the corpus)."
        )
        lines.append("")

    # ── MDM-declared formulas ──
    if n.columns_with_formulas:
        lines.append("### Columns with MDM-declared computation logic")
        lines.append(
            "Use these formulas verbatim in the column description — "
            "they're the authoritative computation specification."
        )
        for col_formula in n.columns_with_formulas[:10]:
            lines.append(
                f"  - `{col_formula['column']}`: {col_formula['derived_logic']}"
            )
        lines.append("")

    # ── Inferred grain (the synthesis the LLM should use to anchor) ──
    grain_evidence = _infer_grain_evidence(n)
    if grain_evidence:
        lines.append("### Inferred grain")
        lines.append(grain_evidence)
        lines.append("")

    return "\n".join(lines)


def _render_column_description(cd: ColumnDescription, lines: list[str]) -> None:
    if cd.description:
        bus = f' ("{cd.business_name}")' if cd.business_name else ""
        lines.append(
            f"  - `{cd.column}`{bus} — {cd.description[:140]}"
            f"{'…' if len(cd.description) > 140 else ''}  "
            f"_[{cd.source}]_"
        )
    else:
        bus = f' ("{cd.business_name}")' if cd.business_name else ""
        lines.append(
            f"  - `{cd.column}`{bus} — ⚠ no description from any source "
            "(must mark confidence: guessed in enrichment)"
        )


def _infer_grain_evidence(n: TableNarrative) -> str:
    """One short paragraph synthesizing what the table's grain probably is.

    This is the punchline of the whole narrative — Gemini reads the
    description corpus + clusters + concepts, then sees this synthesis,
    and uses it as the frame for every per-column decision below.
    """
    bits: list[str] = []
    has_customer = "Customer / Identity" in n.semantic_clusters
    has_time = "Time / Reporting" in n.semantic_clusters
    has_financial = "Financial" in n.semantic_clusters

    if has_customer and has_time:
        bits.append(
            "Customer × time grain (likely cardmember × day given the "
            "cm-prefix columns + reporting-date partition)."
        )
    elif has_time:
        bits.append("Time-series grain (rows are point-in-time snapshots).")
    elif has_customer:
        bits.append("Customer-level grain (one row per customer-key).")

    if n.table_type:
        bits.append(f"MDM table_type={n.table_type}.")
    if n.feed_type:
        bits.append(f"Feed type {n.feed_type}.")
    if n.is_history_required:
        bits.append("History required → snapshots accumulate over time.")
    if has_financial:
        bits.append("Financial measures present → likely a fact table.")

    return " ".join(bits) if bits else ""
