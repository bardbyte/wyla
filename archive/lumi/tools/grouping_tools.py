"""Group queries by view, extract distinct join patterns, and compute per-view
statistics (field frequency, default filters, user-vocabulary map).

Pure Python, no LLM.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from typing import Any

from lumi.schemas import JoinPattern, ParsedQuery

logger = logging.getLogger(__name__)

# Tokenizer for user prompts — used to build the user→column vocabulary map.
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")


def group_queries_by_view(
    queries: list[ParsedQuery],
    view_name_to_table: dict[str, str] | None = None,
    default_filter_threshold: float = 0.8,
) -> dict[str, Any]:
    """Group queries by their primary table (with optional view_name → table map).

    Args:
        queries: Parsed gold queries.
        view_name_to_table: Optional reverse map (view_name → sql_table_name).
            When provided, grouping is keyed by view_name instead of table.
        default_filter_threshold: Fraction of queries (per view) a filter value
            must appear in to be considered a "default".

    Returns:
        dict with:
          status
          queries_by_view: dict[view_name, list[ParsedQuery]]
          field_frequency: dict[view_name, dict[column, count]]
          filter_defaults: dict[view_name, dict[column, value]]
          user_vocabulary: dict[view_name, dict[user_term, column]]
    """
    table_to_view: dict[str, str] | None = None
    if view_name_to_table:
        table_to_view = {t: v for v, t in view_name_to_table.items()}

    grouped: dict[str, list[ParsedQuery]] = defaultdict(list)
    for q in queries:
        key = q.primary_table
        if not key:
            continue
        if table_to_view:
            key = table_to_view.get(key, key)
        grouped[key].append(q)

    field_frequency: dict[str, dict[str, int]] = {
        view: _count_fields(qs) for view, qs in grouped.items()
    }
    filter_defaults: dict[str, dict[str, str]] = {
        view: _filter_defaults(qs, default_filter_threshold) for view, qs in grouped.items()
    }
    user_vocabulary: dict[str, dict[str, list[str]]] = {
        view: _user_vocabulary(qs) for view, qs in grouped.items()
    }

    logger.info(
        "Grouped %d queries across %d views",
        len(queries),
        len(grouped),
    )
    return {
        "status": "success",
        "queries_by_view": dict(grouped),
        "field_frequency": field_frequency,
        "filter_defaults": filter_defaults,
        "user_vocabulary": user_vocabulary,
        "error": None,
    }


def _count_fields(queries: list[ParsedQuery]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for q in queries:
        for d in q.dimensions:
            c[d] += 1
        for m in q.measures:
            if m.column:
                c[m.column] += 1
        for f in q.filters:
            c[f.column] += 1
    return dict(c.most_common())


def _filter_defaults(queries: list[ParsedQuery], threshold: float) -> dict[str, str]:
    n = len(queries)
    if n == 0:
        return {}
    # Map column -> Counter(value)
    value_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for q in queries:
        for f in q.filters:
            if f.operator == "=":
                value_counts[f.column][f.value] += 1

    defaults: dict[str, str] = {}
    for col, vc in value_counts.items():
        top_val, top_count = vc.most_common(1)[0]
        if top_count / n >= threshold:
            defaults[col] = top_val
    return defaults


def _user_vocabulary(queries: list[ParsedQuery]) -> dict[str, list[str]]:
    """For each user-prompt token, the columns that co-occurred with it.

    We do NOT filter to only unambiguous mappings — ambiguity is the norm
    (a term like "accounts" co-occurs with account_id, account_balance, etc.)
    and the LLM is the right place to disambiguate given full field context.

    Returns a mapping of lowercased token → sorted list of candidate columns,
    trimmed to co-occurrence count >= 2 to keep the noise floor down.
    """
    cooccur: dict[str, Counter[str]] = defaultdict(Counter)
    for q in queries:
        tokens = {t.lower() for t in _WORD_RE.findall(q.user_prompt) if len(t) > 2}
        columns: set[str] = set()
        columns.update(q.dimensions)
        columns.update(f.column for f in q.filters)
        columns.update(m.column for m in q.measures if m.column)
        for t in tokens:
            for c in columns:
                cooccur[t][c] += 1

    out: dict[str, list[str]] = {}
    for term, counter in cooccur.items():
        candidates = [c for c, n in counter.most_common() if n >= 2 or len(counter) == 1]
        if candidates:
            out[term] = candidates
    return out


def extract_join_graphs(queries: list[ParsedQuery]) -> dict[str, Any]:
    """Deduplicate multi-table join patterns across all queries.

    Returns:
        dict with:
          status
          patterns: list[JoinPattern] sorted by query_count desc
    """
    by_sig: dict[str, JoinPattern] = {}
    for q in queries:
        if not q.joins:
            continue
        tables = sorted({q.primary_table or "", *(j.left_table for j in q.joins), *(j.right_table for j in q.joins)})
        tables = [t for t in tables if t]
        if len(tables) < 2:
            continue
        pattern = JoinPattern(
            tables=tables,
            joins=sorted(
                q.joins,
                key=lambda j: (j.left_table, j.left_column, j.right_table, j.right_column),
            ),
            query_ids=[q.query_id],
        )
        sig = pattern.signature
        if sig in by_sig:
            by_sig[sig].query_ids.append(q.query_id)
        else:
            by_sig[sig] = pattern

    patterns = sorted(by_sig.values(), key=lambda p: len(p.query_ids), reverse=True)
    logger.info("Extracted %d distinct join patterns", len(patterns))
    return {"status": "success", "patterns": patterns, "error": None}
