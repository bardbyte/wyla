"""Coverage ledger — utilization at PROP grain (G2/G3 of the card
sourcing audit), the compile-time twin of the loaders' file ledger.

The file ledger (``loaders/ledger.py``) proves every archive file was
read. It cannot see a field parsed and then dropped, or a graph prop
that never reaches a card. This ledger closes that gap: at compile
time it sweeps every prop key on ``table:`` and ``col:`` nodes and
every edge predicate in the folded graph, and marks each

    rendered(where)     projected into the table facts row (and so
                        onto the card and the console) — the path
                        names the exact place
    deferred(reason)    deliberately not served, with the reason
    unaccounted         present in the graph, served nowhere, and no
                        one has said why — the honest "we have this
                        and show nothing"

``indexes/coverage.json`` lands in every build; the CI test holds
``unaccounted`` at empty, so a loader that starts emitting a new prop
fails the build until someone either renders it or names the reason
it stays dark. That is the whole point: "are we using everything?"
becomes a test, not an audit.
"""

from __future__ import annotations

from typing import Any

# ── table props → where the facts row carries them ────────────
TABLE_RENDERED: dict[str, str] = {
    "description_atlas": "identity.description",
    "description_bq": "identity.description / identity.description_bq",
    "business_name_atlas": "identity.business_name",
    "project": "identity.project",
    "project_atlas": "identity.project",
    "dataset_group_atlas": "identity.dataset",
    "table_name_atlas": "identity.table_name_atlas",
    "object_type": "identity.object_type",
    "table_type_atlas": "identity.table_type",
    "layer_type": "identity.layer_type",
    "load_type_atlas": "identity.load_type",
    "is_partitioned_atlas": "identity.is_partitioned_atlas",
    "target_system_atlas": "identity.target_system",
    "technology_atlas": "identity.technology",
    "data_server_atlas": "identity.data_server",
    "data_system_atlas": "identity.data_system",
    "appl_id": "identity.appl_id",
    "data_category": "identity.data_category",
    "data_sub_category": "identity.data_sub_category",
    "schema_fingerprint": "identity.schema_fingerprint",
    "business_unit": "business.business_unit",
    "ownership_atlas": "business.owners / business.ownership_ids",
    "top_users": "business.top_users",
    "lifecycle_status": "operations.lifecycle",
    "environment": "operations.environment",
    "feed_type": "operations.feed_type",
    "pipeline_name": "operations.pipeline_name",
    "source_system": "operations.source_system",
    "table_meta_logical": "operations.created / operations.last_modified",
    "total_rows": "operations.total_rows",
    "table_metrics": "operations.size_bytes",
    "n_partitions": "operations.n_partitions",
    "partition_latest": "operations.partition_latest",
    "usage_rhythm": "operations.usage_rhythm",
    "cost_prior": "operations.cost_prior",
    "answerability": "trust.answerability",
    "is_active_atlas": "trust.is_active_atlas",
    "is_latest_atlas": "trust.is_latest_atlas",
    "is_lineage_exist_atlas": "trust.is_lineage_exist_atlas",
    "has_pii_atlas": "access.has_pii_atlas",
    "has_gdpr_atlas": "access.has_gdpr_atlas",
    "has_oncop_atlas": "access.has_oncop_atlas",
}
TABLE_DEFERRED: dict[str, str] = {
    "stub": "internal endpoint marker (a lineage stub), not a fact",
}

# ── column props → where column_facts[] carries them ──────────
COLUMN_RENDERED: dict[str, str] = {
    "data_type": "column_facts[].type (bq wins, D3)",
    "data_type_mdm": "column_facts[].agreement / type_source",
    "data_type_atlas": "column_facts[].agreement / type_source",
    "description_atlas": "column_facts[].description",
    "description_mdm": "column_facts[].description_supplementary (D4)",
    "description_bq": "column_facts[].description (fallback)",
    "business_name": "column_facts[].business_name",
    "business_name_atlas": "column_facts[].business_name",
    "column_name_atlas": "column_facts[].column_name_atlas (when it "
                         "diverges from the identity)",
    "is_pii_mdm": "column_facts[].sensitive / sensitivity_sources",
    "pii_role_id": "column_facts[].pii_role",
    "pii_role_id_table_declared": "column_facts[].pii_role_table_declared",
    "sde_group": "column_facts[].sde_group",
    "is_primary_key": "column_facts[].primary_key / facts.primary_key",
    "is_primary_key_atlas": "column_facts[].primary_key_atlas",
    "is_partitioning": "column_facts[].partitioning",
    "is_partitioning_atlas": "column_facts[].partitioning_atlas",
    "nullable_atlas": "column_facts[].nullable_atlas",
    "ordinal": "column_facts[].ordinal (card order)",
    "ordinal_atlas": "column_facts[].ordinal_atlas",
    "column_length": "column_facts[].column_length",
    "approx_distinct": "column_facts[].approx_distinct",
    "null_count": "column_facts[].null_count",
    "profile_coverage": "column_facts[].profile_coverage",
    "derived_logic": "column_facts[].derived_logic",
    "declared_terms": "column_facts[].declared_terms",
    "observed_via": "column_facts[].observed_via",
}
COLUMN_DEFERRED: dict[str, str] = {
    "stub": "internal endpoint marker (a lineage stub), not a fact",
    "nested_path": "internal marker: the dotted name on the card "
                   "already says the column is nested",
}

# ── edge predicates → the consumer that serves them ───────────
EDGE_RENDERED: dict[str, str] = {
    "has_column": "consensus → column_facts[]",
    "fk_references": "joins.declared + column_facts[].fk_references",
    "co_queried_with": "joins.observed",
    "joins_via": "joins.scoped / indexes/joins.jsonl",
    "derived_from": "column_facts[].derived_from + lineage.derived_columns",
    "upstream_of": "lineage.upstream / lineage.downstream",
    "owned_by": "business.owners (with witnesses)",
    "has_policy": "access.policies + acl.json",
    "has_domain": "column_facts[].domain + indexes/domains.jsonl",
    "mapped_term": "column_facts[].terms (with definitions)",
    "described_by": "lineage.docs + column_facts[].derived_logic",
    "evidenced_by": "lineage.docs / metric cards (referenced SQL)",
    "in_lob": "business.lobs + lob cards",
    "used_by": "business.used_by + lob cards",
    "bound_to": "common filters (bindings.jsonl)",
    "measured_on": "metrics available (metrics.jsonl)",
    "member_of": "metric cards (mgroups)",
    "certified_as": "metric status_served",
    "variant_of": "metric cards (off-meridian variants)",
    "in_domain": "metric rows (domain) / lob.jsonl domains",
    "defines_metric": "metric cards (mgroups)",
}
EDGE_DEFERRED: dict[str, str] = {
    "has_schema": "schema versioning: the fingerprint prop already "
                  "rides on identity.schema_fingerprint",
    "valid_in": "schema-version validity: served when versioned "
                "builds land",
    "alias_of": "acronym→term aliasing: served by search_semantics "
                "(vocab), not a table fact",
    "concerns": "ReviewItem subjects: the steward queue surface "
                "(Operate), not a table fact",
}


def build_coverage(nodes: dict[str, Any],
                   edges: dict[tuple[str, str, str, str], Any]
                   ) -> dict[str, Any]:
    """Sweep the folded graph and account for every table/column
    prop key and every edge predicate. Deterministic (sorted)."""
    table_props: set[str] = set()
    column_props: set[str] = set()
    for node_id, record in nodes.items():
        kind = node_id.split(":", 1)[0]
        if kind == "table":
            table_props.update(record.props)
        elif kind == "col":
            column_props.update(record.props)
    predicates = {r for (_s, r, _o, _w) in edges}

    def account(seen: set[str], rendered: dict[str, str],
                deferred: dict[str, str]) -> dict[str, Any]:
        rows = []
        unaccounted = []
        for key in sorted(seen):
            if key in rendered:
                rows.append({"key": key, "status": "rendered",
                             "where": rendered[key]})
            elif key in deferred:
                rows.append({"key": key, "status": "deferred",
                             "reason": deferred[key]})
            else:
                rows.append({"key": key, "status": "unaccounted"})
                unaccounted.append(key)
        return {"rows": rows, "unaccounted": unaccounted,
                "summary": {
                    "rendered": sum(1 for r in rows
                                    if r["status"] == "rendered"),
                    "deferred": sum(1 for r in rows
                                    if r["status"] == "deferred"),
                    "unaccounted": len(unaccounted)}}

    table = account(table_props, TABLE_RENDERED, TABLE_DEFERRED)
    column = account(column_props, COLUMN_RENDERED, COLUMN_DEFERRED)
    edge = account(predicates, EDGE_RENDERED, EDGE_DEFERRED)
    return {
        "schema": "meridian.coverage/1",
        "table_props": table,
        "column_props": column,
        "edge_predicates": edge,
        "unaccounted": (
            [f"table.{k}" for k in table["unaccounted"]]
            + [f"col.{k}" for k in column["unaccounted"]]
            + [f"edge.{k}" for k in edge["unaccounted"]]),
        "meta": {
            "rendered": "projected into indexes/tables.jsonl (the "
                        "table facts row) — the card and the console "
                        "both render from it",
            "deferred": "deliberately not served, reason pinned in "
                        "sahs/compiler/coverage.py",
            "unaccounted": "in the graph, served nowhere, no reason "
                           "given — CI holds this list at empty",
        },
    }
