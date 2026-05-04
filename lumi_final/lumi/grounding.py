"""Grounding signals — top-tier context engineering for the enrichment LLM.

The Gemini prompt for ``enrich_table`` was previously fed:
  - TableContext (deterministic)
  - MDM column digests
  - Existing baseline LookML as raw text
  - SKILL.md sections 1-7

That's adequate for tables Gemini "knows" by training-data resemblance.
It fails for AmEx-internal columns like ``cm11`` whose name is opaque
and which have NO description in any source — Gemini guesses, often
wrong.

This module derives a dense grounding-signals payload from the data we
already have on disk + the structured fingerprints, surfacing the
deterministic intelligence that lets Gemini reason about column
semantics instead of inventing them. It runs once per table during
enrichment, no additional LLM calls.

Public API:

    build_grounding_signals(
        ctx: TableContext,
        all_fingerprints: list[SQLFingerprint],
        contexts_by_table: dict[str, TableContext],
    ) -> GroundingSignals

    render_grounding_signals(g: GroundingSignals) -> str
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from lumi.schemas import TableContext
from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.grounding")


# ─── Confidence labels ───────────────────────────────────────


GROUNDED = "grounded"
INFERRED = "inferred"
GUESSED = "guessed"


# ─── Per-column usage profile ────────────────────────────────


@dataclass
class ColumnUsageProfile:
    """How one column is used across the entire gold-query corpus."""

    column: str

    # SELECT / SELECT DISTINCT membership
    select_count: int = 0
    select_distinct_count: int = 0  # Was the column part of a SELECT DISTINCT
    in_group_by: int = 0

    # WHERE-clause patterns (count of queries)
    where_eq: int = 0  # = 'literal'
    where_in: int = 0  # IN (...)
    where_between: int = 0
    where_is_null: int = 0
    where_is_not_null: int = 0
    where_other: int = 0

    # JOIN — appearing as a key on either side
    join_left_count: int = 0
    join_right_count: int = 0
    join_partners: list[tuple[str, str]] = field(default_factory=list)
    # ^ list of (other_table, other_column) this column joins to

    # Aggregation usage
    in_sum: int = 0
    in_count: int = 0
    in_count_distinct: int = 0
    in_avg: int = 0
    in_min_max: int = 0

    # Window-function membership
    in_partition_by: int = 0
    in_order_by_window: int = 0

    # CASE-WHEN appearance (as condition source)
    in_case_when: int = 0

    # Date function usage
    extract_units: list[str] = field(default_factory=list)  # YEAR, MONTH, ...
    in_date_trunc: int = 0
    in_date_cast: int = 0

    # Distinct values seen across all WHERE = / IN literals
    observed_values: list[str] = field(default_factory=list)

    # MDM hints (carried alongside; lookups are noisy)
    mdm_business_name: str | None = None
    mdm_description: str | None = None
    mdm_type: str | None = None
    mdm_is_partitioned: bool = False
    mdm_is_pii: bool = False
    mdm_is_critical: bool = False
    mdm_data_category: str | None = None

    # Baseline hints
    baseline_has_dimension: bool = False
    baseline_has_dimension_group: bool = False
    baseline_has_measure: bool = False
    baseline_alias_for: str | None = None
    baseline_is_primary_key: bool = False
    baseline_is_hidden: bool = False
    baseline_has_description: bool = False

    # Naming-pattern signal (deterministic)
    name_signal: str | None = None  # "id_like" | "amount" | "date" | "code" | None

    @property
    def queries_referencing(self) -> int:
        """Total queries that touch this column in any way."""
        return (
            self.select_count
            + self.where_eq + self.where_in + self.where_between
            + self.where_is_null + self.where_is_not_null + self.where_other
            + self.join_left_count + self.join_right_count
            + self.in_sum + self.in_count + self.in_count_distinct
            + self.in_avg + self.in_min_max + self.in_case_when
        )


# ─── Cross-source candidates ─────────────────────────────────


@dataclass
class PrimaryKeyCandidate:
    """One ranked PK candidate with attribution."""

    column: str
    score: int
    confidence: str  # grounded | inferred | guessed
    reasons: list[str] = field(default_factory=list)
    is_synthetic_composite: bool = False
    composite_columns: list[str] = field(default_factory=list)


@dataclass
class JoinHint:
    """A discovered or inferred join relationship."""

    this_table: str
    this_column: str
    other_table: str
    other_column: str
    relationship: str | None  # many_to_one | one_to_many | many_to_many | None
    source: str  # "fingerprint" | "mdm_external_ref" | "transitive"
    occurrences: int = 1
    confidence: str = INFERRED


@dataclass
class AlwaysFilterCandidate:
    """Column that should appear in explore.always_filter."""

    column: str
    reason: str  # "mdm_partitioned" | "high_freq_filter" | "date_partition"
    where_frequency_pct: float
    suggested_default: str | None = None  # e.g. "last 90 days"


@dataclass
class HiddenCandidate:
    """Column that should be hidden in the published view."""

    column: str
    reason: str
    confidence: str


@dataclass
class FilteredMeasureCandidate:
    """SUM(CASE WHEN x THEN amt ELSE 0) → filtered measure."""

    measure_name: str
    source_column: str
    filter_column: str
    filter_value: str
    aggregation_type: str  # sum | count | etc.


# ─── The full payload ────────────────────────────────────────


@dataclass
class GroundingSignals:
    """Densely-structured deterministic context the enrich prompt consumes
    to reason about column semantics, structure, and explore design.
    """

    table_name: str

    # Per-column intelligence
    column_usage: dict[str, ColumnUsageProfile] = field(default_factory=dict)
    column_confidence: dict[str, str] = field(default_factory=dict)

    # Cross-source candidates
    primary_key_candidates: list[PrimaryKeyCandidate] = field(default_factory=list)
    join_hints: list[JoinHint] = field(default_factory=list)
    always_filter_candidates: list[AlwaysFilterCandidate] = field(default_factory=list)
    hidden_candidates: list[HiddenCandidate] = field(default_factory=list)
    drill_fields_ordered: list[str] = field(default_factory=list)
    filtered_measure_candidates: list[FilteredMeasureCandidate] = field(default_factory=list)

    # Aggregated values seen in the corpus (cheap allowed_values without BQ)
    observed_values_by_column: dict[str, list[str]] = field(default_factory=dict)

    # Categorical / role hints
    table_role: str | None = None  # FACT | DIMENSION | UNKNOWN
    table_partition_columns: list[str] = field(default_factory=list)
    group_label_clusters: dict[str, list[str]] = field(default_factory=dict)

    # Co-occurrence map: which columns always appear together in WHERE
    where_cooccurrence: dict[str, list[str]] = field(default_factory=dict)


# ─── Public API ──────────────────────────────────────────────


def build_grounding_signals(
    ctx: TableContext,
    all_fingerprints: list[SQLFingerprint],
    contexts_by_table: dict[str, TableContext] | None = None,
) -> GroundingSignals:
    """Derive every grounding signal we can compute deterministically.

    Args:
        ctx:               TableContext for the table being enriched.
        all_fingerprints:  Every parsed SQL — used for cross-table
                           inference (column X joins to table Y in
                           query 7 even if the current table is Z).
        contexts_by_table: All table contexts keyed by name — used for
                           transitive join inference and MDM lookups
                           on neighboring tables.
    """
    contexts_by_table = contexts_by_table or {}
    g = GroundingSignals(table_name=ctx.table_name)

    # Filter to fingerprints that mention this table — these define our
    # "queries that use this table" lens for column usage.
    relevant_fps = [
        fp for fp in all_fingerprints
        if not fp.parse_error
        and (
            ctx.table_name in fp.tables
            or _table_in_ctes(ctx.table_name, fp)
            or _table_in_temp_tables(ctx.table_name, fp)
        )
    ]

    # Step 1: per-column usage from fingerprints + MDM/baseline hints.
    g.column_usage = _build_column_usage(ctx, relevant_fps)

    # Step 2: cross-table join graph (uses ALL fingerprints so we can
    # transitively figure out e.g. cm11 always joins to customer.cust_id).
    g.join_hints = _derive_join_hints(ctx, all_fingerprints, contexts_by_table)

    # Step 3: PK candidates with deterministic scoring + reasons.
    g.primary_key_candidates = _rank_pk_candidates(ctx, g.column_usage, g.join_hints)

    # Step 4: always_filter candidates.
    g.always_filter_candidates = _identify_always_filters(
        ctx, g.column_usage, len(relevant_fps)
    )

    # Step 5: hidden candidates.
    g.hidden_candidates = _identify_hidden_candidates(ctx, g.column_usage)

    # Step 6: drill_fields ordering (top SELECT/usage cols).
    g.drill_fields_ordered = _rank_drill_fields(g.column_usage, g.primary_key_candidates)

    # Step 7: filtered-measure candidates from CASE-WHEN-in-SUM patterns.
    g.filtered_measure_candidates = _identify_filtered_measures(relevant_fps)

    # Step 8: observed values per column (allowed_values without BQ).
    g.observed_values_by_column = {
        c: usage.observed_values
        for c, usage in g.column_usage.items()
        if usage.observed_values
    }

    # Step 9: table role from MDM hints + structural shape.
    g.table_role = _classify_table_role(ctx, g.column_usage)

    # Step 10: partition columns surfaced separately for explore design.
    g.table_partition_columns = [
        c for c, u in g.column_usage.items() if u.mdm_is_partitioned
    ]

    # Step 11: group_label clusters from MDM data_category × WHERE-cooccurrence.
    g.group_label_clusters = _build_group_labels(
        ctx, g.column_usage, relevant_fps
    )

    # Step 12: WHERE-cooccurrence map (used by clusters but also exposed
    # so the prompt can mention "these columns travel together").
    g.where_cooccurrence = _build_cooccurrence(relevant_fps)

    # Step 13: per-column confidence label aggregating all of the above.
    g.column_confidence = _label_column_confidence(g)

    return g


# ─── Per-column usage builder ────────────────────────────────


_BINARY_OP_PATTERNS = {"=", "!=", ">", ">=", "<", "<="}


def _build_column_usage(
    ctx: TableContext,
    fps: list[SQLFingerprint],
) -> dict[str, ColumnUsageProfile]:
    """Walk every relevant fingerprint and tally usage per column."""
    usage: dict[str, ColumnUsageProfile] = {}

    def _get(col: str) -> ColumnUsageProfile:
        if col not in usage:
            usage[col] = ColumnUsageProfile(column=col)
        return usage[col]

    # Seed with every column referenced anywhere on this table.
    for col in ctx.columns_referenced or []:
        if col:
            _get(col)

    for fp in fps:
        # WHERE filters
        for f in fp.filters or []:
            col = f.get("column")
            if not col:
                continue
            u = _get(col)
            op = f.get("operator")
            value = (f.get("value") or "").strip()
            if op == "=":
                u.where_eq += 1
                cleaned = _clean_literal(value)
                if cleaned and cleaned not in u.observed_values:
                    u.observed_values.append(cleaned)
            elif op == "IN":
                u.where_in += 1
                for v in _split_in_values(value):
                    if v not in u.observed_values:
                        u.observed_values.append(v)
            elif op == "BETWEEN":
                u.where_between += 1
            elif op == "IS":
                if "NULL" in value.upper():
                    if "NOT" in value.upper():
                        u.where_is_not_null += 1
                    else:
                        u.where_is_null += 1
            elif op in _BINARY_OP_PATTERNS:
                u.where_other += 1
            else:
                u.where_other += 1

        # CTE / temp-table internal filters get attributed too.
        for cte in fp.ctes or []:
            for sf in cte.get("structural_filters") or []:
                col = sf.get("column")
                if col:
                    u = _get(col)
                    u.where_eq += 1
                    cleaned = _clean_literal((sf.get("value") or "").strip())
                    if cleaned and cleaned not in u.observed_values:
                        u.observed_values.append(cleaned)

        # Aggregations
        for agg in fp.aggregations or []:
            col = agg.get("column")
            if not col:
                continue
            u = _get(col)
            fn = (agg.get("function") or "").upper()
            if fn == "SUM":
                u.in_sum += 1
            elif fn == "COUNT":
                if agg.get("distinct"):
                    u.in_count_distinct += 1
                else:
                    u.in_count += 1
            elif fn == "AVG":
                u.in_avg += 1
            elif fn in {"MIN", "MAX"}:
                u.in_min_max += 1

        # CASE WHEN sources
        for cw in fp.case_whens or []:
            src = cw.get("source_column")
            if src:
                _get(src).in_case_when += 1

        # Joins — both sides
        for j in fp.joins or []:
            left = j.get("left_key")
            right = j.get("right_key")
            left_table = j.get("left_table")
            right_table = j.get("right_table") or j.get("other_table")
            if left:
                u = _get(left)
                u.join_left_count += 1
                if right and right_table:
                    pair = (right_table, right)
                    if pair not in u.join_partners:
                        u.join_partners.append(pair)
            if right:
                u = _get(right)
                u.join_right_count += 1
                if left and left_table:
                    pair = (left_table, left)
                    if pair not in u.join_partners:
                        u.join_partners.append(pair)

        # Date functions
        for df in fp.date_functions or []:
            col = df.get("column")
            if not col:
                continue
            u = _get(col)
            fn = (df.get("function") or "").upper()
            if fn.startswith("DATE_TRUNC"):
                u.in_date_trunc += 1
            elif fn == "DATE_CAST":
                u.in_date_cast += 1
            else:  # YEAR / MONTH / DAY / etc.
                if fn and fn not in u.extract_units:
                    u.extract_units.append(fn)

    # Hydrate MDM hints per column.
    mdm_by_col = {
        (c.get("name") or "").lower(): c for c in (ctx.mdm_columns or [])
    }
    for col_name, u in usage.items():
        m = mdm_by_col.get(col_name.lower()) or {}
        u.mdm_business_name = m.get("business_name")
        u.mdm_description = m.get("description")
        u.mdm_type = m.get("type") or m.get("data_type")
        u.mdm_is_partitioned = bool(m.get("is_partitioned"))
        u.mdm_is_pii = bool(m.get("is_pii"))
        u.mdm_is_critical = bool(m.get("is_critical_data_element"))
        u.mdm_data_category = m.get("data_category")

    # Hydrate baseline hints per column.
    base_dims = {
        (d.get("name") or "").lower(): d for d in (ctx.baseline_dimensions or [])
    }
    base_dgs = {
        (d.get("name") or "").lower(): d for d in (ctx.baseline_dimension_groups or [])
    }
    base_msrs = {
        (m.get("name") or "").lower(): m for m in (ctx.baseline_measures or [])
    }
    for col_name, u in usage.items():
        cl = col_name.lower()
        d = base_dims.get(cl)
        if d:
            u.baseline_has_dimension = True
            u.baseline_has_description = bool((d.get("description") or "").strip())
            u.baseline_is_primary_key = (
                str(d.get("primary_key") or "").lower() in {"yes", "true"}
            )
            u.baseline_is_hidden = (
                str(d.get("hidden") or "").lower() in {"yes", "true"}
            )
            # Baseline `sql:` aliasing — if the dim's name differs from the
            # column it sources, that's a synonym we should preserve.
            sql = d.get("sql", "")
            m_match = re.search(r"\$\{TABLE\}\.(\w+)", sql, re.IGNORECASE)
            if m_match and m_match.group(1).lower() != cl:
                u.baseline_alias_for = m_match.group(1)
        if cl in base_dgs:
            u.baseline_has_dimension_group = True
        if cl in base_msrs:
            u.baseline_has_measure = True

    # Naming-pattern signal — purely deterministic, no glossary file.
    for col_name, u in usage.items():
        u.name_signal = _name_pattern_signal(col_name)

    return usage


# ─── Naming pattern detection ────────────────────────────────


_ID_SUFFIXES = ("_id", "_uuid", "_xref_id", "_key", "_no", "_num", "_nbr")
_ID_PREFIXES = ("id_", "uuid_", "key_")
_AMOUNT_TOKENS = ("amt", "amount", "balance", "bal", "revenue", "spend", "fee")
_DATE_TOKENS = ("dt", "date", "ts", "tstmp", "timestamp")
_CODE_SUFFIXES = ("_cd", "_code", "_typ", "_type", "_ind", "_flag")


def _name_pattern_signal(col: str) -> str | None:
    cl = col.lower()
    if any(cl.endswith(s) for s in _ID_SUFFIXES) or any(
        cl.startswith(p) for p in _ID_PREFIXES
    ):
        return "id_like"
    # cm11, acct1, etc. — short prefix + digits → likely cardmember/account field
    if re.match(r"^(cm|acct|cust|pmdl|drm)\d+$", cl):
        return "id_like"
    if any(t in cl for t in _AMOUNT_TOKENS):
        return "amount"
    if any(cl.endswith(s) for s in _CODE_SUFFIXES):
        return "code"
    if any(t in cl.split("_") for t in _DATE_TOKENS):
        return "date"
    return None


# ─── Join hint derivation ────────────────────────────────────


def _derive_join_hints(
    ctx: TableContext,
    all_fps: list[SQLFingerprint],
    contexts_by_table: dict[str, TableContext],
) -> list[JoinHint]:
    """Build join hints from fingerprint joins + transitive inference.

    Direct hints come from any query that joins this table to another.
    Transitive hints come from queries where THIS table's column joins
    to a known column in another table — even if the current query
    doesn't show this table directly.
    """
    hints: dict[tuple[str, str, str, str], JoinHint] = {}

    for fp in all_fps:
        if fp.parse_error:
            continue
        # In fp.joins, left_table is often the SQL alias (e.g. "c"), not
        # the real table name. Fingerprint reliably gives us:
        #   - fp.primary_table (the FROM target)
        #   - right_table (the JOINed-in target)
        # If our ctx is the primary_table OR appears in fp.tables, treat
        # joined-in tables as our partners.
        from_table = fp.primary_table
        for j in fp.joins or []:
            right_t = j.get("right_table") or j.get("other_table")
            left_k = j.get("left_key")
            right_k = j.get("right_key")
            if not (right_t and left_k and right_k):
                continue

            # CASE A: ctx is the FROM table (primary_table) → left_key
            # is OUR column, right_key is the partner's column.
            if from_table == ctx.table_name and right_t != ctx.table_name:
                key = (ctx.table_name, left_k, right_t, right_k)
                if key not in hints:
                    hints[key] = JoinHint(
                        this_table=ctx.table_name,
                        this_column=left_k,
                        other_table=right_t,
                        other_column=right_k,
                        relationship=None,
                        source="fingerprint",
                        occurrences=0,
                        confidence=GROUNDED,
                    )
                hints[key].occurrences += 1
            # CASE B: ctx is the JOINed-in table → right_key is OUR
            # column, left_key is the partner's column.
            elif right_t == ctx.table_name and from_table and from_table != ctx.table_name:
                key = (ctx.table_name, right_k, from_table, left_k)
                if key not in hints:
                    hints[key] = JoinHint(
                        this_table=ctx.table_name,
                        this_column=right_k,
                        other_table=from_table,
                        other_column=left_k,
                        relationship=None,
                        source="fingerprint",
                        occurrences=0,
                        confidence=GROUNDED,
                    )
                hints[key].occurrences += 1

    # Cardinality inference: if our column is COUNT(DISTINCT)'d in the
    # ecosystem, we're the "one" side → relationship=many_to_one from
    # the perspective of the other table. From OUR side it's many_to_many
    # unless we have unique-key evidence.
    for hint in hints.values():
        partner_ctx = contexts_by_table.get(hint.other_table)
        if partner_ctx is None:
            continue
        # If the partner column is COUNT-DISTINCT'd in queries, it's a
        # natural-key on that side → many_to_one from our table.
        partner_distinct = any(
            (a.get("column") or "").lower() == hint.other_column.lower()
            and a.get("distinct")
            for fp in all_fps
            for a in (fp.aggregations or [])
        )
        if partner_distinct:
            hint.relationship = "many_to_one"
        else:
            hint.relationship = "many_to_many"

    # MDM external_reference_details — if MDM tells us about a join we
    # didn't see in queries, add it (lower confidence; not yet observed).
    for c in ctx.mdm_columns or []:
        for ref in c.get("external_reference_details") or []:
            other_t = ref.get("table_name") or ref.get("dataset_table")
            other_c = ref.get("column_name") or ref.get("attribute_name")
            this_c = c.get("name")
            if not (other_t and other_c and this_c):
                continue
            key = (ctx.table_name, this_c, other_t, other_c)
            if key not in hints:
                hints[key] = JoinHint(
                    this_table=ctx.table_name,
                    this_column=this_c,
                    other_table=other_t,
                    other_column=other_c,
                    relationship=ref.get("relationship") or None,
                    source="mdm_external_ref",
                    occurrences=0,
                    confidence=INFERRED,
                )

    return list(hints.values())


# ─── Primary-key ranking ─────────────────────────────────────


def _rank_pk_candidates(
    ctx: TableContext,
    usage: dict[str, ColumnUsageProfile],
    joins: list[JoinHint],
) -> list[PrimaryKeyCandidate]:
    """Score every column on PK-likelihood; return ranked candidates.

    Scoring rubric:
      +5  name signal == id_like
      +5  MDM business_name contains identifier-keyword
      +5  baseline already has primary_key: yes on this column
      +4  appears as JOIN key in ≥2 queries (this table side)
      +3  appears in COUNT(DISTINCT) at least once (anywhere)
      +3  MDM is_partitioned = true
      +2  is non-nullable per MDM
      +1  per query that filters on this column
      -3  observed_values count > 5 (high cardinality WHERE values =
          probably not a unique key but a categorical)
    """
    scored: list[PrimaryKeyCandidate] = []
    join_keys = {j.this_column.lower() for j in joins if j.this_table == ctx.table_name}

    for col, u in usage.items():
        score = 0
        reasons: list[str] = []

        if u.name_signal == "id_like":
            score += 5
            reasons.append("name pattern matches *_id / *_xref_id / cm{N} / etc.")
        if u.baseline_is_primary_key:
            score += 5
            reasons.append("baseline already declares primary_key: yes")
        if u.mdm_business_name and any(
            tok in u.mdm_business_name.lower()
            for tok in ("identifier", " id ", " key ", " number")
        ):
            score += 5
            reasons.append(f"MDM business_name suggests identifier: '{u.mdm_business_name}'")

        join_occurrences = u.join_left_count + u.join_right_count
        if col.lower() in join_keys and join_occurrences >= 2:
            score += 4
            reasons.append(f"used as JOIN key in {join_occurrences} queries")

        if u.in_count_distinct >= 1:
            score += 3
            reasons.append(f"COUNT(DISTINCT {col}) seen — natural-key signal")

        if u.mdm_is_partitioned:
            score += 3
            reasons.append("MDM marks as partitioned column")

        if u.where_is_not_null >= 1:
            score += 1
            reasons.append("queries filter IS NOT NULL — column always populated")

        # Filter-frequency boost
        filter_count = u.where_eq + u.where_in + u.where_between
        if filter_count >= 3:
            score += 1
            reasons.append(f"appears in WHERE of {filter_count} queries")

        # Cardinality penalty
        if len(u.observed_values) > 5:
            score -= 3
            reasons.append(
                f"-{3}: high cardinality of WHERE values seen "
                f"({len(u.observed_values)}) — looks categorical, not unique"
            )

        if score == 0:
            continue

        if score >= 8:
            confidence = GROUNDED
        elif score >= 4:
            confidence = INFERRED
        else:
            confidence = GUESSED

        scored.append(PrimaryKeyCandidate(
            column=col,
            score=score,
            confidence=confidence,
            reasons=reasons,
        ))

    scored.sort(key=lambda p: -p.score)

    # Synthetic composite-key suggestion: if no candidate scores ≥ 8,
    # propose the smallest set of columns that uniquely identify rows
    # — heuristic: cols that appear together as JOIN keys in the same
    # queries. Cap at 3 cols.
    if not any(p.score >= 8 for p in scored):
        composite = _suggest_composite_pk(usage, joins)
        if composite and len(composite) >= 2:
            scored.insert(0, PrimaryKeyCandidate(
                column=f"composite({'+'.join(composite)})",
                score=6,
                confidence=INFERRED,
                reasons=[
                    "no single-column candidate scored ≥ 8",
                    f"these {len(composite)} columns co-occur as JOIN keys "
                    "and likely form a natural composite key",
                ],
                is_synthetic_composite=True,
                composite_columns=composite,
            ))

    return scored[:5]


def _suggest_composite_pk(
    usage: dict[str, ColumnUsageProfile],
    joins: list[JoinHint],
) -> list[str]:
    """Heuristic composite-key from JOIN-key co-occurrence."""
    candidates = [
        c for c, u in usage.items()
        if u.name_signal == "id_like" or (u.join_left_count + u.join_right_count) >= 1
    ]
    candidates.sort(
        key=lambda c: -(usage[c].join_left_count + usage[c].join_right_count)
    )
    return candidates[:3]


# ─── Always-filter detection ─────────────────────────────────


def _identify_always_filters(
    ctx: TableContext,
    usage: dict[str, ColumnUsageProfile],
    total_relevant: int,
) -> list[AlwaysFilterCandidate]:
    out: list[AlwaysFilterCandidate] = []
    if total_relevant == 0:
        return out
    for col, u in usage.items():
        in_where = u.where_eq + u.where_in + u.where_between
        pct = in_where / total_relevant if total_relevant else 0.0

        if u.mdm_is_partitioned:
            out.append(AlwaysFilterCandidate(
                column=col,
                reason="mdm_partitioned",
                where_frequency_pct=pct,
                suggested_default=(
                    "last 90 days" if u.name_signal == "date" else None
                ),
            ))
        elif pct >= 0.7 and u.name_signal == "date":
            out.append(AlwaysFilterCandidate(
                column=col,
                reason="date_partition",
                where_frequency_pct=pct,
                suggested_default="last 90 days",
            ))
        elif pct >= 0.85:
            out.append(AlwaysFilterCandidate(
                column=col,
                reason="high_freq_filter",
                where_frequency_pct=pct,
            ))
    return out


# ─── Hidden candidate detection ──────────────────────────────


_TECHNICAL_SUFFIXES = ("_load_dt", "_load_ts", "_audit_user", "_etl_run_id",
                      "_batch_id", "_src_sys", "_load_id")


def _identify_hidden_candidates(
    ctx: TableContext,
    usage: dict[str, ColumnUsageProfile],
) -> list[HiddenCandidate]:
    out: list[HiddenCandidate] = []
    queried_cols = {c.lower() for c, u in usage.items() if u.queries_referencing}

    # Technical-suffix columns that are never queried.
    for col_dict in ctx.baseline_dimensions or []:
        col = (col_dict.get("name") or "").lower()
        if not col:
            continue
        if any(col.endswith(s) for s in _TECHNICAL_SUFFIXES) and col not in queried_cols:
            out.append(HiddenCandidate(
                column=col,
                reason="technical/audit suffix; not used by any gold query",
                confidence=GROUNDED,
            ))

    # MDM-flagged metadata columns.
    for col_dict in ctx.mdm_columns or []:
        col = (col_dict.get("name") or "").lower()
        if not col:
            continue
        if col_dict.get("data_category") in {"metadata", "audit", "lineage"}:
            if col not in queried_cols:
                out.append(HiddenCandidate(
                    column=col,
                    reason=f"MDM data_category={col_dict.get('data_category')}; never queried",
                    confidence=GROUNDED,
                ))

    return out


# ─── Drill fields ────────────────────────────────────────────


def _rank_drill_fields(
    usage: dict[str, ColumnUsageProfile],
    pk_candidates: list[PrimaryKeyCandidate],
) -> list[str]:
    pk_set = {p.column.lower() for p in pk_candidates if not p.is_synthetic_composite}
    ranked = sorted(
        [c for c in usage if c.lower() not in pk_set],
        key=lambda c: -usage[c].queries_referencing,
    )
    return ranked[:8]


# ─── Filtered-measure detection ──────────────────────────────


def _identify_filtered_measures(
    fps: list[SQLFingerprint],
) -> list[FilteredMeasureCandidate]:
    """Detect SUM(CASE WHEN x = 'Y' THEN amt ELSE 0) patterns.

    These are the LookML idiom for filtered measures — much cleaner
    than synthesising a derived dimension. Surfacing them lets Gemini
    use ``measure { filters: [...] }`` instead of inventing a CASE WHEN
    derived dim that does the same thing less idiomatically.
    """
    out: list[FilteredMeasureCandidate] = []
    seen: set[str] = set()

    for fp in fps:
        if fp.parse_error:
            continue
        for cw in fp.case_whens or []:
            # Heuristic: parent is a SUM/COUNT/AVG and the WHEN sets a
            # column to a literal.
            mapped = cw.get("mapped_values") or []
            if not mapped:
                continue
            first = mapped[0]
            when = first.get("when", "")
            then_val = first.get("then", "")
            # WHEN looks like "col = 'value'", THEN looks like a column ref
            m_when = re.match(r"\s*(\w+)\s*=\s*['\"]([^'\"]+)['\"]", when)
            if not m_when:
                continue
            filter_col = m_when.group(1)
            filter_val = m_when.group(2)
            # The aliased name (if any) tells us the measure's intent.
            alias = cw.get("alias")
            if not alias:
                continue
            key = f"{alias}|{filter_col}|{filter_val}"
            if key in seen:
                continue
            seen.add(key)
            out.append(FilteredMeasureCandidate(
                measure_name=alias,
                source_column=then_val.split(".")[-1] if then_val else "?",
                filter_column=filter_col,
                filter_value=filter_val,
                aggregation_type="sum",
            ))
    return out


# ─── Table-role classification ───────────────────────────────


def _classify_table_role(
    ctx: TableContext,
    usage: dict[str, ColumnUsageProfile],
) -> str | None:
    """Use MDM + structural signals to classify FACT vs DIMENSION."""
    # Direct MDM signal.
    for col in ctx.mdm_columns or []:
        if col.get("table_type"):
            return str(col["table_type"]).upper()

    # Structural fallback:
    # - many measures (SUM/COUNT/AVG) → FACT
    # - mostly identifiers + lookups → DIMENSION
    measure_cols = sum(
        1 for u in usage.values() if u.in_sum + u.in_count + u.in_avg + u.in_min_max
    )
    id_like_cols = sum(1 for u in usage.values() if u.name_signal == "id_like")

    if measure_cols >= 3 and measure_cols > id_like_cols:
        return "FACT"
    if id_like_cols >= 2 and id_like_cols >= measure_cols:
        return "DIMENSION"
    return "UNKNOWN"


# ─── Group-label clustering ──────────────────────────────────


def _build_group_labels(
    ctx: TableContext,
    usage: dict[str, ColumnUsageProfile],
    fps: list[SQLFingerprint],
) -> dict[str, list[str]]:
    """Cluster columns into group_labels using MDM data_category +
    WHERE-cooccurrence patterns.
    """
    out: dict[str, list[str]] = {}

    # MDM data_category clusters first (high signal).
    for col, u in usage.items():
        cat = u.mdm_data_category
        if cat:
            label = cat.replace("_", " ").title()
            out.setdefault(label, []).append(col)

    # Drop tiny clusters (single-column → not a group).
    return {k: v for k, v in out.items() if len(v) >= 2}


# ─── Cooccurrence map ────────────────────────────────────────


def _build_cooccurrence(fps: list[SQLFingerprint]) -> dict[str, list[str]]:
    """For each column, which OTHER columns appear in the same WHERE clause."""
    out: dict[str, set[str]] = {}
    for fp in fps:
        if fp.parse_error:
            continue
        cols_in_where = {
            f.get("column") for f in (fp.filters or [])
            if f.get("column")
        }
        cols_in_where.discard(None)
        for c in cols_in_where:
            others = cols_in_where - {c}
            out.setdefault(c, set()).update(others)
    return {c: sorted(others) for c, others in out.items() if others}


# ─── Confidence labelling ────────────────────────────────────


def _label_column_confidence(g: GroundingSignals) -> dict[str, str]:
    """For each column, compute confidence based on how many sources
    actually describe it.
    """
    out: dict[str, str] = {}
    for col, u in g.column_usage.items():
        score = 0
        if (u.mdm_description or "").strip():
            score += 3
        if u.mdm_business_name:
            score += 2
        if u.baseline_has_description:
            score += 2
        if u.queries_referencing:
            score += 1
        if u.name_signal:
            score += 1
        if u.observed_values:
            score += 1

        if score >= 5:
            out[col] = GROUNDED
        elif score >= 2:
            out[col] = INFERRED
        else:
            out[col] = GUESSED
    return out


# ─── Helpers ─────────────────────────────────────────────────


def _table_in_ctes(table_name: str, fp: SQLFingerprint) -> bool:
    for cte in fp.ctes or []:
        if table_name in (cte.get("source_tables") or []):
            return True
    return False


def _table_in_temp_tables(table_name: str, fp: SQLFingerprint) -> bool:
    for tt in fp.temp_tables or []:
        if table_name in (tt.get("source_tables") or []):
            return True
    return False


def _clean_literal(value: str) -> str:
    return value.strip().strip("'").strip('"').strip("`")


def _split_in_values(in_clause: str) -> list[str]:
    """Pull values out of an `(a, b, c)` IN clause."""
    inner = in_clause.strip().strip("()")
    parts = [p.strip() for p in inner.split(",")]
    return [_clean_literal(p) for p in parts if p]


# ─── Render to prompt-ready markdown ────────────────────────


def render_grounding_signals(g: GroundingSignals) -> str:
    """Render the GroundingSignals payload as a dense Markdown block
    that goes into the enrich prompt.

    The format is optimized for LLM reading: bullets, named sections,
    explicit confidence labels, evidence chains. No prose padding.
    """
    lines: list[str] = [
        f"# Grounding signals for `{g.table_name}` (deterministic — anchor every claim here)",
        "",
        "**Use these signals as primary evidence. For any field whose "
        "description / type / role is not anchored to a signal here, "
        "set its confidence to `guessed` and add it to "
        "EnrichedOutput.uncertain_fields with the column name and reason.**",
        "",
    ]

    # Table role + partition
    if g.table_role:
        lines.append(f"## Table role: **{g.table_role}**")
    if g.table_partition_columns:
        lines.append(
            f"- Partition columns (must appear in explore.always_filter): "
            f"`{'`, `'.join(g.table_partition_columns)}`"
        )
    if g.table_role or g.table_partition_columns:
        lines.append("")

    # Primary key
    lines.append("## Primary-key candidates (ranked by deterministic score)")
    if not g.primary_key_candidates:
        lines.append(
            "- ⚠ NO candidates scored above zero. Propose a synthetic "
            "primary_key by concatenating the columns most likely to "
            "uniquely identify rows. Mark confidence: guessed."
        )
    else:
        for pk in g.primary_key_candidates:
            tag = (
                "**[GROUNDED]**" if pk.confidence == GROUNDED
                else "[INFERRED]" if pk.confidence == INFERRED
                else "[GUESSED]"
            )
            if pk.is_synthetic_composite:
                lines.append(
                    f"- {tag} composite of {pk.composite_columns} "
                    f"— score {pk.score}"
                )
            else:
                lines.append(
                    f"- {tag} `{pk.column}` — score {pk.score}"
                )
            for r in pk.reasons[:3]:
                lines.append(f"    - {r}")
    lines.append("")

    # Joins
    if g.join_hints:
        lines.append(f"## Join relationships ({len(g.join_hints)} discovered)")
        for h in g.join_hints[:15]:
            tag = (
                "[OBSERVED]" if h.source == "fingerprint"
                else "[MDM-DECLARED]" if h.source == "mdm_external_ref"
                else "[INFERRED]"
            )
            rel = h.relationship or "many_to_many (no cardinality evidence)"
            lines.append(
                f"- {tag} `{h.this_column}` → `{h.other_table}.{h.other_column}` "
                f"({rel}) — seen in {h.occurrences} quer(y/ies)"
            )
        lines.append("")

    # Always-filter
    if g.always_filter_candidates:
        lines.append("## explore.always_filter candidates")
        for af in g.always_filter_candidates:
            d = f", suggested default: `{af.suggested_default}`" if af.suggested_default else ""
            lines.append(
                f"- `{af.column}` — {af.reason}, in WHERE of "
                f"{af.where_frequency_pct * 100:.0f}% of queries{d}"
            )
        lines.append("")

    # Hidden
    if g.hidden_candidates:
        lines.append("## hidden: yes candidates (technical / audit / unused fields)")
        for h in g.hidden_candidates[:20]:
            lines.append(f"- `{h.column}` — {h.reason}")
        lines.append("")

    # Drill fields
    if g.drill_fields_ordered:
        lines.append("## drill_fields ordering (top SELECT-frequency, excluding PK)")
        lines.append(
            f"- {', '.join(f'`{c}`' for c in g.drill_fields_ordered)}"
        )
        lines.append("")

    # Filtered-measure candidates
    if g.filtered_measure_candidates:
        lines.append("## Filtered-measure candidates (CASE-WHEN-in-SUM patterns)")
        lines.append(
            "These should be expressed as `measure { filters: [...] }` — "
            "NOT as derived dimensions:"
        )
        for fm in g.filtered_measure_candidates[:10]:
            lines.append(
                f"- `{fm.measure_name}`: {fm.aggregation_type}({fm.source_column}) "
                f"WHERE {fm.filter_column} = '{fm.filter_value}'"
            )
        lines.append("")

    # Group labels
    if g.group_label_clusters:
        lines.append("## Suggested group_label clusters (from MDM data_category)")
        for label, cols in g.group_label_clusters.items():
            lines.append(
                f"- **{label}**: {', '.join(f'`{c}`' for c in cols)}"
            )
        lines.append("")

    # Per-column intelligence
    lines.append("## Per-column intelligence")
    lines.append(
        "Format: column | confidence | usage signals | observed values "
        "| MDM hints | baseline status"
    )
    lines.append("")
    # Sort columns by usage frequency desc.
    cols_sorted = sorted(
        g.column_usage.items(),
        key=lambda kv: -kv[1].queries_referencing,
    )
    for col, u in cols_sorted:
        conf = g.column_confidence.get(col, GUESSED)
        usage_bits: list[str] = []
        if u.in_sum or u.in_avg or u.in_count or u.in_count_distinct or u.in_min_max:
            agg_parts = []
            if u.in_sum:
                agg_parts.append(f"SUM×{u.in_sum}")
            if u.in_count:
                agg_parts.append(f"COUNT×{u.in_count}")
            if u.in_count_distinct:
                agg_parts.append(f"COUNT-DISTINCT×{u.in_count_distinct}")
            if u.in_avg:
                agg_parts.append(f"AVG×{u.in_avg}")
            if u.in_min_max:
                agg_parts.append(f"MIN/MAX×{u.in_min_max}")
            usage_bits.append("agg:" + ",".join(agg_parts))
        if u.where_eq or u.where_in or u.where_between or u.where_is_not_null:
            wparts = []
            if u.where_eq:
                wparts.append(f"={u.where_eq}")
            if u.where_in:
                wparts.append(f"IN×{u.where_in}")
            if u.where_between:
                wparts.append(f"BETWEEN×{u.where_between}")
            if u.where_is_not_null:
                wparts.append(f"NOT-NULL×{u.where_is_not_null}")
            usage_bits.append("where:" + ",".join(wparts))
        if u.join_left_count or u.join_right_count:
            usage_bits.append(
                f"join:{u.join_left_count + u.join_right_count}"
            )
        if u.extract_units:
            usage_bits.append(f"date:{','.join(u.extract_units)}")
        if u.in_case_when:
            usage_bits.append(f"case×{u.in_case_when}")
        if u.name_signal:
            usage_bits.append(f"name:{u.name_signal}")

        observed = ""
        if u.observed_values:
            sample = u.observed_values[:5]
            extra = f" (+{len(u.observed_values) - 5} more)" if len(u.observed_values) > 5 else ""
            observed = f"values=[{', '.join(repr(v) for v in sample)}]{extra}"

        mdm_bits: list[str] = []
        if u.mdm_business_name:
            mdm_bits.append(f"name='{u.mdm_business_name}'")
        if u.mdm_type:
            mdm_bits.append(f"type={u.mdm_type}")
        if u.mdm_is_partitioned:
            mdm_bits.append("partition")
        if u.mdm_is_pii:
            mdm_bits.append("pii")
        if u.mdm_is_critical:
            mdm_bits.append("CDE")

        baseline_bits: list[str] = []
        if u.baseline_is_primary_key:
            baseline_bits.append("PK")
        if u.baseline_is_hidden:
            baseline_bits.append("hidden")
        if u.baseline_has_description:
            baseline_bits.append("has-desc")
        if u.baseline_has_dimension_group:
            baseline_bits.append("dim_group")
        if u.baseline_alias_for:
            baseline_bits.append(f"sql={u.baseline_alias_for}")

        parts = [f"`{col}`", f"[{conf}]"]
        if usage_bits:
            parts.append(" ".join(usage_bits))
        if observed:
            parts.append(observed)
        if mdm_bits:
            parts.append("mdm:" + ",".join(mdm_bits))
        if baseline_bits:
            parts.append("base:" + ",".join(baseline_bits))
        lines.append(f"- {' | '.join(parts)}")

    return "\n".join(lines)
