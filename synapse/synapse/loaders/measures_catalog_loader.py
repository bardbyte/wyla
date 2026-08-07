"""Mined measures catalog → ``usage_mined`` witness facts on the graph.

The input is the output of a SQL-mining pass over historical BigQuery
execution logs: thousands of candidate metrics, each distilled from real
queries and scored (confidence high|medium|low, execution/user/query
counts). That makes it a BEHAVIORAL witness — stronger than one raw
corpus observation because each row already aggregates many queries,
far below any curated catalog because no human vouched for a single
row. Weight 2, and it can never overwrite what the graph already holds
(gap-fill merge, same policy as the Collibra connector).

Input JSON — ``{"measures": [...]}`` (a ``summary`` block is tolerated
and ignored) or a bare list. Per measure the loader understands:

    id               "table__metric_token" → the metric URI token
    name             human-ish name (URI fallback when id is malformed)
    table            base table; joined_tables [t1, t2, ...]
    business_unit    e.g. "GCS" — gap-filled onto the table
    data_category    domain; data_sub_category → sub_domain
    expression       SQL expression → formula
    agg_function     SUM | COUNT | ratio | ...
    column           base column (edge only if the column node EXISTS)
    confidence       high | medium | low  (threshold + scaling fallback)
    execution_count / user_count / query_count / first_seen / last_seen
    group_by_patterns / common_filters / complexity_tier / score

Evidence scaling — the Knowledge-Vault move: a metric derived
independently by 30 analysts is better-attested than one derived once.
``user_count`` maps to the witness count (capped at the store's
per-source cap of 5), falling back to confidence high→3 / medium→2 /
low→1 when absent. Same idea for tables (capped count of measures
mined from the table) and JOINS_WITH edges (capped observed count).

Join co-occurrence: measures' ``table → joined_tables[]`` pairs are
aggregated across ALL rows — including below-threshold ones, because
the join evidence is real even when the metric candidate is weak — and
land as directed table-level JOINS_WITH edges carrying
``observed_count`` (query-weighted) and ``last_seen``.

Anything unparseable is skipped WITH a reason — nothing is invented.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synapse.graph.store import (
    GraphStore, canonical_uri, normalize_table_name,
)

SOURCE = "usage_mined"

_CONF_ORDER = {"low": 0, "medium": 1, "high": 2}
# Witness-count fallback when user_count is absent: how many independent
# derivations a confidence label is worth.
_CONF_WITNESSES = {"high": 3, "medium": 2, "low": 1}


def load_table_aliases(path: "Path | str") -> dict[str, str]:
    """Read an alias map JSON: {"alias_table_name": "canonical_name"}.

    Real mined exports carry spelling drift (e.g. the offer/offr pair
    ``loyalty_rc_cm_offer_enroll`` vs ``loyalty_rc_cm_offr_enroll``);
    without a mapping each spelling mints its own node and the witnesses
    never fuse. Keys and values are normalized like table names.
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("table aliases must be a JSON object "
                         "{alias: canonical}")
    return {normalize_table_name(str(k)): normalize_table_name(str(v))
            for k, v in raw.items() if str(k).strip() and str(v).strip()}


def _resolve_table(name: str, aliases: dict[str, str] | None) -> str:
    norm = normalize_table_name(str(name))
    if aliases:
        return aliases.get(norm, norm)
    return norm


def _fill_only(store: GraphStore, uri: str,
               props: dict[str, Any]) -> dict[str, Any]:
    """Gap-filling merge: mined facts fill blanks, never replace.

    Full builds encode authority by ingest ORDER (last-non-empty wins in
    the store), and this loader runs late / in appends — so without the
    filter a mined name would overwrite MDM or DMP wording. Keys the
    node already holds a non-empty value for are dropped; the witness
    lands either way and the tier still recomputes.
    """
    node = store.get(uri)
    if node is None:
        return props
    return {k: v for k, v in props.items() if not node.properties.get(k)}


def _extra_witnesses(measure: dict[str, Any]) -> int:
    """How many witnesses BEYOND the first this measure is worth (0..4)."""
    user_count = measure.get("user_count")
    if isinstance(user_count, (int, float)) and user_count >= 1:
        return min(5, int(user_count)) - 1
    conf = str(measure.get("confidence") or "low").lower()
    return _CONF_WITNESSES.get(conf, 1) - 1


def load_measures_catalog(
    store: GraphStore,
    catalog_path: "Path | str",
    *,
    min_confidence: str = "medium",
    aliases: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fuse a mined measures catalog into the store as ``usage_mined``.

    Measures at or above ``min_confidence`` become Metric nodes (same
    URI scheme as the curated metric-catalog pass, so a mined metric
    and a curated one with the same table+token FUSE into one node with
    two witnesses). Below-threshold measures are counted, not silently
    dropped — and their join evidence still lands. Returns
    ``{"metrics", "tables_minted", "tables_witnessed", "join_edges",
    "below_threshold", "skipped"}``.
    """
    if min_confidence not in _CONF_ORDER:
        raise ValueError(f"min_confidence must be one of "
                         f"{sorted(_CONF_ORDER)}, got {min_confidence!r}")
    path = Path(catalog_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    measures = raw.get("measures") if isinstance(raw, dict) else raw
    if not isinstance(measures, list):
        return {"metrics": 0, "tables_minted": 0, "tables_witnessed": 0,
                "join_edges": 0, "below_threshold": {},
                "skipped": ["catalog is neither {'measures': [...]} nor "
                            "a list of measures"]}

    floor = _CONF_ORDER[min_confidence]
    skipped: list[str] = []
    below_threshold: dict[str, int] = {}
    # Group first so each touched table gets ONE aggregated testimony —
    # 300 measures on one table must not inflate it to 300 witnesses.
    by_table: dict[str, list[dict[str, Any]]] = {}
    # (from_table, to_table) → [query-weighted count, last_seen]
    join_obs: dict[tuple[str, str], list[Any]] = {}

    for i, m in enumerate(measures):
        if not isinstance(m, dict):
            skipped.append(f"measure[{i}]: not an object")
            continue
        table = _resolve_table(str(m.get("table") or ""), aliases)
        if not table:
            mid = str(m.get("id") or m.get("name") or f"[{i}]")
            skipped.append(f"measure {mid}: no table")
            continue

        # Join evidence first — it counts regardless of the threshold.
        weight = m.get("query_count")
        weight = int(weight) if isinstance(weight, (int, float)) \
            and weight >= 1 else 1
        for j in (m.get("joined_tables") or []):
            joined = _resolve_table(str(j), aliases)
            if not joined or joined == table:
                continue
            obs = join_obs.setdefault((table, joined), [0, ""])
            obs[0] += weight
            obs[1] = max(obs[1], str(m.get("last_seen") or ""))

        conf = str(m.get("confidence") or "low").lower()
        if _CONF_ORDER.get(conf, 0) < floor:
            below_threshold[conf] = below_threshold.get(conf, 0) + 1
            continue
        by_table.setdefault(table, []).append(m)

    n_metrics = n_tables_minted = 0
    for table, table_measures in by_table.items():
        t_uri = canonical_uri("table", table)
        if store.get(t_uri) is None:
            n_tables_minted += 1
        t_props: dict[str, Any] = {"table_name": table}
        for m in table_measures:
            if m.get("business_unit") and "business_unit" not in t_props:
                t_props["business_unit"] = str(m["business_unit"])
            if m.get("data_category") and "data_domain" not in t_props:
                t_props["data_domain"] = str(m["data_category"])
        node = store.upsert_node("Table", t_uri,
                                 _fill_only(store, t_uri, t_props),
                                 source=SOURCE)
        # One aggregated testimony per table per run: worth as many
        # witnesses as measures mined from it, capped by the store.
        extra = min(5, len(table_measures)) - 1
        if extra > 0:
            node.provenance.record_source(SOURCE, count_delta=extra)

        for m in table_measures:
            mid = str(m.get("id") or "")
            token = ""
            if "__" in mid:
                token = mid.split("__", 1)[1]
            if not token:
                token = str(m.get("name") or "").strip().replace(" ", "_")
            if not token:
                skipped.append(f"measure on {table}: no id and no name")
                continue
            m_uri = canonical_uri("metric", table, token)
            props: dict[str, Any] = {
                "business_name": str(m.get("name") or ""),
                "formula": str(m.get("expression") or ""),
                "sourced_from_table": table,
                "agg_function": str(m.get("agg_function") or ""),
                "base_column": str(m.get("column") or ""),
                "domain": str(m.get("data_category") or ""),
                "sub_domain": str(m.get("data_sub_category") or ""),
                "business_unit": str(m.get("business_unit") or ""),
                "mined_confidence": str(m.get("confidence") or ""),
                "mined_score": m.get("score"),
                "execution_count": m.get("execution_count"),
                "user_count": m.get("user_count"),
                "query_count": m.get("query_count"),
                "first_seen": str(m.get("first_seen") or ""),
                "last_seen": str(m.get("last_seen") or ""),
                "group_by_patterns": m.get("group_by_patterns") or [],
                "common_filters": m.get("common_filters") or [],
                "complexity_tier": str(m.get("complexity_tier") or ""),
            }
            props = {k: v for k, v in props.items()
                     if v is not None and v != "" and v != []}
            metric = store.upsert_node("Metric", m_uri,
                                       _fill_only(store, m_uri, props),
                                       source=SOURCE)
            extra = _extra_witnesses(m)
            if extra > 0:
                metric.provenance.record_source(SOURCE, count_delta=extra)
            n_metrics += 1

            store.upsert_edge(
                "COMPUTED_FROM", m_uri, t_uri,
                {"formula": str(m.get("expression") or "")}, source=SOURCE)
            # Column edge ONLY when the column node already exists —
            # a usage pass never mints columns (physical witnesses do).
            col = str(m.get("column") or "").strip()
            if col:
                c_uri = canonical_uri("column", table, col)
                if store.get(c_uri) is not None:
                    store.upsert_edge("COMPUTED_FROM", m_uri, c_uri, {},
                                      source=SOURCE)

    n_join_edges = 0
    for (from_t, to_t), (count, last_seen) in sorted(join_obs.items()):
        f_uri = canonical_uri("table", from_t)
        t_uri = canonical_uri("table", to_t)
        for name, uri in ((from_t, f_uri), (to_t, t_uri)):
            if store.get(uri) is None:
                store.upsert_node("Table", uri, {"table_name": name},
                                  source=SOURCE)
                n_tables_minted += 1
        edge = store.upsert_edge(
            "JOINS_WITH", f_uri, t_uri,
            {"observed_count": count, "last_seen": last_seen},
            source=SOURCE)
        extra = min(5, count) - 1
        if extra > 0:
            edge.provenance.record_source(SOURCE, count_delta=extra)
        n_join_edges += 1

    return {
        "metrics": n_metrics,
        "tables_minted": n_tables_minted,
        "tables_witnessed": len(by_table),
        "join_edges": n_join_edges,
        "below_threshold": below_threshold,
        "skipped": skipped,
    }
