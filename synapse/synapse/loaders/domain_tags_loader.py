"""Steward domain tags — the human-asserted Company Domain → tables map.

The rollup can derive business units from MDM ownership and catalog
gap-fill, but the org chart's OWNER is a person. This loader ingests the
steward's explicit map and stamps it as ``human_approval`` testimony —
the ceiling witness — so the curated segmentation outranks every mined
or crawled label and the rollup's BusinessUnit nodes inherit
human-asserted trust.

Accepted shapes (both JSON):

    {"Credit Risk": ["risk_pers_acct", "loyalty_rc_cm_offer_enroll"],
     "Merchant Services": ["gms_transaction", ...]}

    [{"company_domain": "Credit Risk",
      "description": "Underwriting, exposure and delinquency analytics",
      "tables": ["risk_pers_acct", ...]}, ...]

The list form may carry a ``description`` per domain — steward-authored
prose that OVERRIDES the rollup's derived description on the unit node
(recorded at human_approval, which is exactly what "the steward says what
this domain is" means).

Behavior per table:
  - business_unit AND company_domain are set to the domain label —
    override, not gap-fill: a human assignment wins over any machine one.
  - names run through the shared alias map first (--table-aliases), so
    variant spellings land on the canonical node.
  - a tagged table not yet in the graph is minted as a stub (name + the
    two domain props) and reported — the steward tagging it is evidence
    it exists; the next crawl fills it in.

Pure function, no network. Same report contract as the other loaders.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synapse.graph.store import GraphStore, canonical_uri

SOURCE = "human_approval"


def _normalize(payload: Any) -> list[dict[str, Any]]:
    """Both accepted shapes → [{company_domain, tables, description}]."""
    if isinstance(payload, dict):
        return [{"company_domain": str(domain), "tables": tables or [],
                 "description": ""}
                for domain, tables in payload.items()]
    if isinstance(payload, list):
        out = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            domain = str(row.get("company_domain")
                         or row.get("business_unit")
                         or row.get("domain") or "").strip()
            if domain:
                out.append({
                    "company_domain": domain,
                    "tables": row.get("tables") or [],
                    "description": str(row.get("description") or ""),
                })
        return out
    return []


def load_domain_tags(store: GraphStore, path: str | Path,
                     aliases: dict[str, str] | None = None) -> dict[str, Any]:
    """Apply the steward's domain map to the graph. Returns a report:
    domains applied, tables stamped, stubs minted, rows skipped (with
    reasons) — nothing silent."""
    p = Path(path).expanduser()
    payload = json.loads(p.read_text(encoding="utf-8"))
    rows = _normalize(payload)
    aliases = {str(k).strip().lower(): str(v).strip()
               for k, v in (aliases or {}).items()}

    skipped: list[str] = []
    stamped = 0
    stubs: list[str] = []
    descriptions: dict[str, str] = {}
    for row in rows:
        domain = row["company_domain"]
        if row["description"]:
            descriptions[domain] = row["description"]
        if not row["tables"]:
            skipped.append(f"{domain}: no tables listed")
            continue
        for raw in row["tables"]:
            name = str(raw).strip()
            if not name:
                continue
            name = aliases.get(name.lower(), name)
            uri = canonical_uri("table", name)
            minted = store.get(uri) is None
            store.upsert_node(
                "Table", uri,
                {"table_name": name, "business_unit": domain,
                 "company_domain": domain},
                source=SOURCE)
            stamped += 1
            if minted:
                stubs.append(name)

    return {
        "domains": len({r["company_domain"] for r in rows}),
        "tables_stamped": stamped,
        "stubs_minted": stubs,
        "descriptions": descriptions,
        "skipped": skipped,
    }


def apply_steward_descriptions(store: GraphStore,
                               descriptions: dict[str, str]) -> int:
    """After the rollup ran, overwrite derived unit descriptions with the
    steward's prose (human_approval). Call order matters: rollup first
    (it recomputes nodes), then this. Returns units updated."""
    updated = 0
    for domain, text in descriptions.items():
        node = store.get(canonical_uri("business_unit", domain))
        if node is None or not text:
            continue
        node.properties["description"] = text
        node.properties["description_by"] = "steward"
        node.provenance.record_source(SOURCE)
        updated += 1
    return updated
