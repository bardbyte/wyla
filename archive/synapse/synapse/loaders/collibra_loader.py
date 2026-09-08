"""Collibra export → ``collibra`` witness facts on the graph.

The connector story made real: a governed external catalog fuses in as
ONE MORE WEIGHTED WITNESS (weight 5 — curated testimony), never as an
authority. On a table the graph already knows, Collibra's testimony adds
a distinct source, so the confidence tier recomputes upward; values the
graph already holds are never overwritten (monotonic merge). Tables the
graph has never seen are minted at the honest floor for a single
witness.

Input is a Collibra asset export as JSON — either ``{"assets": [...]}``
or a bare list. The parser is deliberately lenient about Collibra's
shape-shifting export formats; per asset it understands:

    type            "Table" | "Column"       (or {"name": ...})
    name/fullName   "sbs_merchants" or "sbs_merchants.merch_id"
                    (a Column may also carry a separate "table" field)
    description     plain string, or attributes {"Description": [
                    {"value": ...}]}
    domain          plain string or {"name": ...}
    status          plain string or {"name": ...}
    steward         plain string, or responsibilities [{"role":
                    "Steward"/"Owner", "user"/"name": ...}]
    classifications ["PII", "Sensitive", ...] (strings or {"name"})

Anything unparseable is skipped WITH a reason — nothing is invented.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synapse.graph.store import GraphStore, canonical_uri

SOURCE = "collibra"

_PII_MARKERS = ("pii", "personal", "gdpr")
_SENSITIVE_MARKERS = ("sensitive", "confidential", "restricted")


def _name_of(v: Any) -> str:
    """A Collibra value that may be a string or a {"name": ...} object."""
    if isinstance(v, dict):
        return str(v.get("name") or v.get("displayName") or "")
    return str(v or "")


def _description_of(asset: dict[str, Any]) -> str:
    if asset.get("description"):
        return str(asset["description"]).strip()
    attrs = asset.get("attributes")
    if isinstance(attrs, dict):
        for key in ("Description", "description"):
            vals = attrs.get(key)
            if isinstance(vals, list) and vals:
                first = vals[0]
                if isinstance(first, dict):
                    return str(first.get("value") or "").strip()
                return str(first).strip()
    return ""


def _steward_of(asset: dict[str, Any]) -> str:
    if asset.get("steward"):
        return _name_of(asset["steward"])
    resp = asset.get("responsibilities")
    if isinstance(resp, list):
        for r in resp:
            if not isinstance(r, dict):
                continue
            role = _name_of(r.get("role")).lower()
            if role in ("steward", "owner", "data steward", "data owner"):
                return _name_of(r.get("user") or r.get("name"))
    return ""


def _classifications_of(asset: dict[str, Any]) -> list[str]:
    vals = asset.get("classifications") or asset.get("tags") or []
    if not isinstance(vals, list):
        return []
    return [_name_of(v).lower() for v in vals if _name_of(v)]


def _fill_only(store: GraphStore, uri: str,
               props: dict[str, Any]) -> dict[str, Any]:
    """Gap-filling merge policy for an EXTERNAL catalog witness.

    The store's own merge is last-non-empty-wins (full builds encode
    authority by ingest ORDER); an append arrives after everything, so a
    weight-5 catalog would silently overwrite MDM's weight-8 wording.
    Filter to keys the node doesn't already carry a non-empty value for
    — Collibra fills gaps and adds its witness; it never replaces.
    Sticky flags (is_pii, is_sensitive) pass through: True is monotonic.
    """
    node = store.get(uri)
    if node is None:
        return props
    return {k: v for k, v in props.items()
            if k in ("is_pii", "is_sensitive", "is_gdpr")
            or not node.properties.get(k)}


def load_collibra_export(store: GraphStore,
                         export_path: "Path | str") -> dict[str, Any]:
    """Fuse a Collibra asset export into the store as ``collibra`` facts.

    Gap-filling by design: fields the graph lacks are filled, fields it
    holds are kept, the ``collibra`` witness lands either way and tiers
    recompute. Returns ``{"tables": n, "columns": n, "skipped": [...]}``.
    """
    path = Path(export_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    assets = raw.get("assets") if isinstance(raw, dict) else raw
    if not isinstance(assets, list):
        return {"tables": 0, "columns": 0,
                "skipped": ["export is neither {'assets': [...]} nor a "
                            "list of assets"]}

    n_tables = n_columns = 0
    skipped: list[str] = []
    for i, asset in enumerate(assets):
        if not isinstance(asset, dict):
            skipped.append(f"asset[{i}]: not an object")
            continue
        kind_raw = _name_of(asset.get("type"))
        kind = kind_raw.lower()
        name = str(asset.get("fullName") or asset.get("name") or "").strip()
        if not name:
            skipped.append(f"asset[{i}]: no name")
            continue
        classifications = _classifications_of(asset)
        flags: dict[str, Any] = {}
        if any(m in c for c in classifications for m in _PII_MARKERS):
            flags["is_pii"] = True
        if any(m in c for c in classifications
               for m in _SENSITIVE_MARKERS) or flags.get("is_pii"):
            flags["is_sensitive"] = True

        if kind == "table":
            props: dict[str, Any] = {"table_name": name}
            if _description_of(asset):
                props["description"] = _description_of(asset)
            if _name_of(asset.get("domain")):
                props["data_domain"] = _name_of(asset.get("domain"))
            if _steward_of(asset):
                props["steward"] = _steward_of(asset)
            if _name_of(asset.get("status")):
                props["catalog_status"] = _name_of(asset.get("status"))
            props.update(flags)
            t_uri = canonical_uri("table", name)
            store.upsert_node("Table", t_uri,
                              _fill_only(store, t_uri, props),
                              source=SOURCE)
            n_tables += 1
        elif kind == "column":
            table = _name_of(asset.get("table"))
            column = name
            if not table and "." in name:
                table, column = name.rsplit(".", 1)
            if not table:
                skipped.append(f"asset[{i}] '{name}': a Column needs "
                               "'table' or a dotted fullName")
                continue
            t_uri = canonical_uri("table", table)
            c_uri = canonical_uri("column", table, column)
            # upsert_edge never creates nodes, so the table must exist —
            # but mint it ONLY when missing: one export is one testimony,
            # and N column assets must not inflate the table's evidence
            if store.get(t_uri) is None:
                store.upsert_node("Table", t_uri, {"table_name": table},
                                  source=SOURCE)
            cprops: dict[str, Any] = {"table_name": table, "name": column}
            if _description_of(asset):
                cprops["description"] = _description_of(asset)
            cprops.update(flags)
            store.upsert_node("Column", c_uri,
                              _fill_only(store, c_uri, cprops),
                              source=SOURCE)
            store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source=SOURCE)
            n_columns += 1
        else:
            skipped.append(f"asset[{i}] '{name}': type "
                           f"'{kind_raw or '?'}' is not Table or Column")
    return {"tables": n_tables, "columns": n_columns, "skipped": skipped}
