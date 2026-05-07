"""Radix-shaped filter catalog generation.

Radix's 5-pass filter cascade (exact → synonym → fuzzy → semantic →
passthrough) reads from ``config/filter_catalog.json``. The cascade can
only do passes 1, 3, and 5 deterministically; pass 2 (synonym) and pass
4 (semantic) require us to populate the right structure.

Schema Radix expects (per its doc):

    {
      "view_name.column_name": {
        "synonyms": {"alt": "canonical", ...},
        "values": ["GCS", "GMNS", ...],          # for low-cardinality
        "type": "string" | "number" | "date" | "yesno",
        "namespace": "view_name",
        "partition": true | false,
        "mandatory": true | false
      }
    }

Sources we mine:

  - **MDM column metadata** → type, partition flag, mandatory hint,
    namespace (the table name)
  - **Baseline LookML sql_aliases** → synonyms (the dim name analysts see
    is a synonym for the source column)
  - **Domain ontology** → entity-level synonyms when the column belongs
    to an entity (cm11 → ["cardmember", "card_member", "cm"])
  - **Query SELECT aliases** (from fp.select_aliases) → analysts'
    glossary
  - **Query WHERE values** → low-cardinality enum candidates (we capture
    every literal compared against this column across the corpus)

Public API:
    build_filter_catalog(contexts, fingerprints, ontology=None) -> dict
    write_filter_catalog(catalog, path) -> Path
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from lumi.schemas import DomainOntology, TableContext
from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.filter_catalog")


def build_filter_catalog(
    contexts: dict[str, TableContext],
    fingerprints: list[SQLFingerprint],
    ontology: DomainOntology | None = None,
) -> dict[str, dict[str, Any]]:
    """Build a Radix-shaped filter catalog from the corpus.

    Returns ``{namespaced_field: entry}`` where ``namespaced_field`` is
    ``view_name.column_name`` (Radix's convention) and ``entry`` carries
    synonyms, values, type, namespace, partition flag, mandatory flag.

    Generation is deterministic: same inputs → same output. Designed to
    be safe to run on every plan phase even before BigQuery DISTINCT
    probes (Tier 3) populate the values lists.
    """
    catalog: dict[str, dict[str, Any]] = {}

    # Mine query WHERE-clause literals for value candidates.
    observed_values = _collect_observed_filter_values(fingerprints)

    for table_name, ctx in contexts.items():
        for col_dict in ctx.mdm_columns or []:
            col_name = col_dict.get("name")
            if not col_name:
                continue
            key = f"{table_name}.{col_name}"
            entry = catalog.setdefault(key, {
                "synonyms": {},
                "values": [],
                "type": _infer_type(col_dict),
                "namespace": table_name,
                "partition": False,
                "mandatory": False,
            })
            # Partition + mandatory flags from MDM.
            if col_dict.get("is_partitioned") or col_dict.get(
                "partition_position"
            ):
                entry["partition"] = True
                entry["mandatory"] = True
            # Business name → synonym.
            bn = (col_dict.get("business_name") or "").strip()
            if bn and bn.lower() != col_name.lower():
                entry["synonyms"][bn] = col_name

        # Baseline sql_aliases → synonyms ({dim_name: source_column}).
        for dim_name, source_col in (ctx.baseline_sql_aliases or {}).items():
            if not (dim_name and source_col):
                continue
            key = f"{table_name}.{source_col}"
            entry = catalog.setdefault(key, {
                "synonyms": {},
                "values": [],
                "type": "string",
                "namespace": table_name,
                "partition": False,
                "mandatory": False,
            })
            entry["synonyms"][dim_name] = source_col

        # Filters observed on this table: extract values per column.
        for f in ctx.filters_on_this or []:
            col = f.get("column")
            if not col:
                continue
            key = f"{table_name}.{col}"
            entry = catalog.setdefault(key, {
                "synonyms": {},
                "values": [],
                "type": "string",
                "namespace": table_name,
                "partition": False,
                "mandatory": False,
            })
            value = (f.get("value") or "").strip().strip("'\"")
            if value and value not in entry["values"] and len(entry["values"]) < 50:
                entry["values"].append(value)
            # Structural filters → mandatory (sql_always_where invariants).
            if f.get("is_structural"):
                entry["mandatory"] = True

    # Merge corpus-wide observed values.
    for (table_name, col), values in observed_values.items():
        key = f"{table_name}.{col}"
        if key not in catalog:
            continue
        existing = set(catalog[key]["values"])
        for v in values:
            if v not in existing and len(catalog[key]["values"]) < 50:
                catalog[key]["values"].append(v)
                existing.add(v)

    # Ontology synonyms for entity columns.
    if ontology is not None:
        _attach_ontology_synonyms(catalog, ontology)

    # Query SELECT aliases as a synonym source — covers cases like
    # `SELECT cm11 AS card_member_id` revealing analyst vocabulary.
    _attach_select_alias_synonyms(catalog, contexts, fingerprints)

    return catalog


def write_filter_catalog(
    catalog: dict[str, dict[str, Any]], path: Path,
) -> Path:
    """Write catalog as JSON, sorted by key for stable diffs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_catalog = dict(sorted(catalog.items()))
    path.write_text(
        json.dumps(sorted_catalog, indent=2, default=str),
        encoding="utf-8",
    )
    return path


# ─── Helpers ─────────────────────────────────────────────────


def _infer_type(col_dict: dict[str, Any]) -> str:
    """Map MDM type to Radix filter type vocabulary."""
    raw = (col_dict.get("type") or col_dict.get("data_type") or "").upper()
    if raw in {"DATE", "DATETIME", "TIMESTAMP"}:
        return "date"
    if raw in {"NUMERIC", "FLOAT64", "INT64", "INTEGER", "NUMBER", "BIGNUMERIC"}:
        return "number"
    if raw in {"BOOL", "BOOLEAN"}:
        return "yesno"
    return "string"


def _collect_observed_filter_values(
    fingerprints: list[SQLFingerprint],
) -> dict[tuple[str, str], list[str]]:
    """Extract WHERE-clause literals per (table, column) across the corpus.

    Returns a dict keyed by (table, column) → list of distinct literal
    values seen. Strips surrounding quotes; capped at 50 values per col.
    Useful for low-cardinality enum-like dims.
    """
    out: dict[tuple[str, str], list[str]] = defaultdict(list)
    seen_per_key: dict[tuple[str, str], set[str]] = defaultdict(set)

    for fp in fingerprints:
        if fp.parse_error:
            continue
        from_t = fp.primary_table
        if not from_t:
            continue
        for f in fp.filters or []:
            col = f.get("column")
            value = (f.get("value") or "").strip().strip("'\"")
            if not (col and value):
                continue
            # Skip ranges / multi-value placeholders.
            if value.startswith("(") or "AND" in value:
                continue
            key = (from_t, col)
            if value in seen_per_key[key]:
                continue
            if len(out[key]) >= 50:
                continue
            seen_per_key[key].add(value)
            out[key].append(value)
    return out


def _attach_ontology_synonyms(
    catalog: dict[str, dict[str, Any]], ontology: DomainOntology,
) -> None:
    """For each catalog column belonging to an ontology entity, add the
    entity's synonyms to the synonym dict."""
    for entity in ontology.entities:
        for table, cols in (entity.grain_columns or {}).items():
            for col in cols:
                key = f"{table}.{col}"
                if key not in catalog:
                    continue
                for syn in entity.synonyms or []:
                    if syn and syn != col:
                        catalog[key]["synonyms"].setdefault(syn, col)


def _attach_select_alias_synonyms(
    catalog: dict[str, dict[str, Any]],
    contexts: dict[str, TableContext],
    fingerprints: list[SQLFingerprint],
) -> None:
    """Mine `SELECT col AS alias` patterns from query fingerprints and
    surface aliases as synonyms for the underlying column."""
    for fp in fingerprints:
        if fp.parse_error:
            continue
        from_t = fp.primary_table
        if not from_t or from_t not in contexts:
            continue
        for entry in fp.select_aliases or []:
            col = (entry.get("column") or "").strip()
            alias = (entry.get("alias") or "").strip()
            if not (col and alias) or col == alias:
                continue
            key = f"{from_t}.{col}"
            if key in catalog:
                catalog[key]["synonyms"].setdefault(alias, col)
