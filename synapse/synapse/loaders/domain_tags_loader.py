"""Steward domain tags — the human-asserted Company Domain → tables map.

The rollup derives the domain layer from the labels tables carry (MDM
ownership, catalog gap-fill). This loader ingests the steward's explicit
map as the COEXISTING human witness: it mints ``Domain`` nodes and
membership edges at ``human_approval`` — it does **not** touch the
``business_unit``/``company_domain`` properties on tables. What MDM said
stays exactly as MDM said it; the steward's map lives beside it in the
layer, and the rollup fuses both (a membership asserted by steward AND
machine becomes one edge with two witnesses).

Overlap is first-class: list the same table under two domains and it
gets two memberships — that is the point of the layer being edges.

Accepted shapes (both JSON):

    {"Credit Risk": ["risk_pers_acct", "loyalty_rc_cm_offr_enroll"],
     "Merchant Services": ["gms_transaction", ...]}

    [{"company_domain": "Credit Risk",
      "description": "Underwriting, exposure and delinquency analytics",
      "tables": ["risk_pers_acct", ...]}, ...]

The list form may carry a ``description`` per domain — steward-authored
prose that outranks the rollup's derived description on the domain node
(``description_by: "steward"``, preserved across recomputes).

Behavior per table:
  - names run through the shared alias map first (--table-aliases);
  - a tagged table not yet in the graph is minted as a stub (name only —
    no domain props stamped, membership is the edge) and reported.

Pure function, no network. Same report contract as the other loaders.
Call ``rollup_domains`` after this loader — it recomputes every domain
profile with the steward memberships fused in.
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
    """Apply the steward's domain map as the coexisting human witness.
    Returns a report: domains, memberships, overlapping tables, stubs
    minted, rows skipped (with reasons) — nothing silent."""
    p = Path(path).expanduser()
    payload = json.loads(p.read_text(encoding="utf-8"))
    rows = _normalize(payload)
    aliases = {str(k).strip().lower(): str(v).strip()
               for k, v in (aliases or {}).items()}

    skipped: list[str] = []
    memberships = 0
    stubs: list[str] = []
    seen_domains: set[str] = set()
    table_domains: dict[str, set[str]] = {}
    for row in rows:
        domain = row["company_domain"]
        seen_domains.add(domain)
        d_uri = canonical_uri("domain", domain)
        d_props: dict[str, Any] = {"name": domain}
        if row["description"]:
            d_props["description"] = row["description"]
            d_props["description_by"] = "steward"
        store.upsert_node("Domain", d_uri, d_props, source=SOURCE)
        if not row["tables"]:
            skipped.append(f"{domain}: no tables listed "
                           "(domain node minted, no memberships)")
            continue
        for raw in row["tables"]:
            name = str(raw).strip()
            if not name:
                continue
            name = aliases.get(name.lower(), name)
            t_uri = canonical_uri("table", name)
            if store.get(t_uri) is None:
                store.upsert_node("Table", t_uri, {"table_name": name},
                                  source=SOURCE)
                stubs.append(name)
            store.upsert_edge("CONTAINS", d_uri, t_uri,
                              {"membership": "steward"}, source=SOURCE)
            memberships += 1
            table_domains.setdefault(name, set()).add(domain)

    return {
        "domains": len(seen_domains),
        "memberships": memberships,
        "overlapping_tables": {t: sorted(ds)
                               for t, ds in sorted(table_domains.items())
                               if len(ds) > 1},
        "stubs_minted": stubs,
        "skipped": skipped,
    }
