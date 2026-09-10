"""Corpus signals — frequency-aggregated 'noun candidates' from the SQL.

We reuse `lumi.sql_to_context.parse_sqls` from lumi_final (battle-tested
on 122 production queries; same sqlglot extraction layered Layer-1/3/4).
The output is a per-token frequency table over identifiers that look
like entity candidates: GROUP BY columns, JOIN keys, CASE WHEN aliases,
primary tables, select aliases.

This is the corpus's 'vote' on what entities exist — to be cross-
referenced against MDM business_names + glossary symbols when the LLM
proposes entities.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

from synapse.registry.schemas import CorpusNounStat

# Make lumi_final importable without installing it.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_LUMI_FINAL = _REPO_ROOT / "lumi_final"
if _LUMI_FINAL.exists() and str(_LUMI_FINAL) not in sys.path:
    sys.path.insert(0, str(_LUMI_FINAL))


def aggregate_corpus_signals(
    sql_files: list[Path], *, top_n: int = 200,
) -> tuple[list[CorpusNounStat], int]:
    """Parse every SQL, count noun candidates, return top-N.

    Returns (stats, n_queries_analyzed). n_queries_analyzed counts
    only fingerprints with no parse_error.

    Each role role-counts:
        group_by:        column was GROUP BY'd
        join_key_left:   column was a JOIN ON left key
        join_key_right:  column was a JOIN ON right key
        case_alias:      column was a CASE WHEN output alias
        agg_source:      column was the SUM/COUNT/... argument
        select_alias:    column was a SELECT alias
        primary_table:   identifier was a primary FROM table
    """
    try:
        from lumi.sql_to_context import parse_sqls  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            f"lumi.sql_to_context unavailable: {e}. "
            "Ensure lumi_final is on the import path."
        ) from e

    sqls = [p.read_text(encoding="utf-8") for p in sql_files]
    fps = parse_sqls(sqls)

    counters: dict[str, Counter[str]] = {}     # token → Counter(role → count)
    table_membership: dict[str, set[str]] = {}  # token → {tables it co-occurs with}

    n_ok = 0
    for fp in fps:
        if getattr(fp, "parse_error", None):
            continue
        n_ok += 1
        tables = [t for t in (fp.tables or []) if t]
        primary = (
            fp.primary_table or (tables[0] if tables else None)
        )

        def _bump(tok: str | None, role: str) -> None:
            if not tok:
                return
            t = tok.lower().strip()
            if not t or len(t) > 64 or t.startswith("_"):
                return
            counters.setdefault(t, Counter())[role] += 1
            if primary:
                table_membership.setdefault(t, set()).add(primary)

        for gb in (fp.group_by or []):
            _bump(gb.get("column"), "group_by")
        for j in (fp.joins or []):
            _bump(j.get("left_key"), "join_key_left")
            _bump(j.get("right_key"), "join_key_right")
        for cw in (fp.case_whens or []):
            _bump(cw.get("source_column"), "case_source")
            _bump(cw.get("alias"), "case_alias")
        for agg in (fp.aggregations or []):
            _bump(agg.get("column"), "agg_source")
            _bump(agg.get("alias"), "agg_alias")
        for sa in (fp.select_aliases or []):
            _bump(sa.get("column"), "select_source")
            _bump(sa.get("alias"), "select_alias")
        for tbl in tables:
            _bump(tbl, "primary_table" if tbl == primary else "join_table")

    stats = [
        CorpusNounStat(
            token=tok,
            occurrence_count=sum(roles.values()),
            appears_in_tables=sorted(table_membership.get(tok, set())),
            role_counts=dict(roles),
        )
        for tok, roles in counters.items()
    ]
    stats.sort(key=lambda s: -s.occurrence_count)
    return stats[:top_n], n_ok
