"""check_data_trust — the protective verdict (P4).

Bundles the governance / lifecycle / DQ / PII facts we already ingest into
one "should the user be warned?" answer, so the agent can flag a risk
BEFORE a number is relied on. Pure graph read — no LLM, no network.

Per the MVP call this is *agent-invoked when relevant*, not an always-on
badge: it surfaces a warning only when there is a real red flag.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from synapse.graph.store import GraphStore, canonical_uri

_FALSY = frozenset({"", "n", "no", "false", "0", "none", "null"})


def _flag(value: Any) -> bool:
    """Truthy that reads MDM's "Y"/"N" strings correctly."""
    if isinstance(value, str):
        return value.strip().lower() not in _FALSY
    return bool(value)


def assess_trust(
    store: GraphStore, table: str, *, as_of: str | None = None,
) -> dict[str, Any]:
    """A trust verdict for one table: ``{ok, warnings[], facts}``.

    Warnings are the unambiguous red flags — a recent breaking change, a
    decommission/purge flag, a passed recertification date, deprecated
    columns in the table, or failing data-quality rules. PII and lifecycle
    status ride as *facts* (context), not alarms.
    """
    t_uri = canonical_uri("table", table)
    node = store.get(t_uri)
    if node is None:
        return {"status": "error", "reason": f"{table} is not in the graph"}
    p = node.properties
    warnings: list[dict[str, str]] = []

    def warn(kind: str, severity: str, detail: str) -> None:
        warnings.append({"kind": kind, "severity": severity, "detail": detail})

    if _flag(p.get("is_breaking_change")):
        ver = p.get("lifecycle_version")
        warn("breaking_change", "high",
             f"{table} had a breaking change"
             + (f" (lifecycle v{ver})" if ver else "")
             + " — verify the schema before relying on this.")
    if _flag(p.get("is_decommissioned")):
        warn("decommissioned", "high",
             f"{table} is flagged decommissioned.")
    if _flag(p.get("is_purge")):
        warn("purge", "high", f"{table} is flagged for purge.")

    rd = str(p.get("recertification_date") or "")
    today = as_of or date.today().isoformat()
    if rd and rd < today:
        warn("recert_review", "medium",
             f"recertification date {rd} has passed — confirm the "
             "certification is current before filing.")

    pii_cols: list[dict[str, Any]] = []
    deprecated_cols: list[str] = []
    col_uris: list[str] = []
    for edge in store.outgoing(t_uri, "CONTAINS"):
        c = store.get(edge.to_uri)
        if c is None:
            continue
        col_uris.append(c.canonical_uri)
        cn = c.canonical_uri.rsplit("/", 1)[-1]
        if (c.provenance.confidence_tier == "deprecated"
                or _flag(c.properties.get("is_decommissioned"))):
            deprecated_cols.append(cn)
        if _flag(c.properties.get("is_pii")):
            pii_cols.append({"name": cn,
                             "pii_taxonomy": c.properties.get("pii_taxonomy")})
    if deprecated_cols:
        warn("deprecated_column", "medium",
             f"{len(deprecated_cols)} deprecated column(s): "
             f"{', '.join(sorted(deprecated_cols)[:5])}.")

    failing: set[str] = set()
    for src_uri in [t_uri, *col_uris]:
        for e in store.outgoing(src_uri, "VALIDATED_BY"):
            rule = store.get(e.to_uri)
            if rule and str(
                    rule.properties.get("last_run_status", "")).lower() == "fail":
                failing.add(str(rule.properties.get("rule_id")
                                or rule.canonical_uri.rsplit("/", 1)[-1]))
    if failing:
        warn("failing_dq", "high",
             f"{len(failing)} data-quality rule(s) failing: "
             f"{', '.join(sorted(failing)[:5])}.")

    return {
        "status": "ok",
        "table": table,
        "ok": not warnings,
        "warnings": warnings,
        "facts": {
            "lifecycle_status": p.get("lifecycle_status") or "",
            "recertification_date": rd,
            "has_pii": bool(pii_cols),
            "pii_columns": pii_cols,
        },
    }
