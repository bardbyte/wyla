"""Serving-plane display vocabulary — how sources and trust render.

Two pinned registries, both DISPLAY-ONLY (the store, the witness enum,
and every prov field are untouched — same discipline as
``_STATUS_SERVED``):

* ``SOURCE_DISPLAY`` — the product name for every ingestion source.
  Chips are short (≤14 chars), display names are full, blurbs carry
  one sentence of meaning. Steward-chosen names; changing one is a
  naming decision (escalate), not a refactor.
* The tier bridge — Meridian speaks witnesses/status_served; the UI
  speaks the four-symbol trust language (● ha · ◆ gr · ◐ in · ○ gu,
  crimson reserved for CONFLICT only). The mapping is pinned here
  with tests, never decided per-page.
"""

from __future__ import annotations

from typing import Any

# ── source naming (steward-approved, 2026-08-30) ─────────────
# family groups sources that share one shelf card; sub distinguishes
# members inside a family without flattening their AUTHORITY — status
# is always its own axis, never implied by the source name.

SOURCE_DISPLAY: dict[str, dict[str, str]] = {
    "bq": {
        "family": "warehouse",
        "display": "Lumi Warehouse: BigQuery Catalog",
        "chip": "BQ Catalog",
        "blurb": "Tables, columns, and constraints as BigQuery itself "
                 "declares them."},
    "jobs_30d": {
        "family": "activity",
        "display": "Query Activity: 30-Day Warehouse Logs",
        "chip": "Activity 30d",
        "blurb": "Real queries run against the warehouse in the last "
                 "30 days: true recency and usage."},
    "std_tech_metadata": {
        "family": "atlas_mdm",
        "display": "Atlas MDM",
        "chip": "Atlas MDM", "sub": "table registry",
        "blurb": "The enterprise master-data registry for tables and "
                 "their stewardship."},
    "lumi": {
        "family": "atlas_mdm",
        "display": "Atlas MDM",
        "chip": "Atlas MDM", "sub": "profile archive",
        "blurb": "Table profiling and field-level metadata from the "
                 "MDM extraction."},
    "business_terms": {
        "family": "atlas_catalog",
        "display": "Atlas Data Federated Catalog",
        "chip": "Atlas Catalog",
        "blurb": "The federated business-term catalog, the official "
                 "names for business concepts."},
    "glossary": {
        "family": "acropedia",
        "display": "Acropedia",
        "chip": "Acropedia",
        "blurb": "The enterprise glossary: acronyms and terms with "
                 "their meanings."},
    "value_lookup": {
        "family": "atlas_mdm",
        "display": "Lumi Value Meanings",
        "chip": "Value Meanings", "sub": "stored code → meaning",
        "blurb": "What a low-cardinality stored value means in one "
                 "column of one table — the index a business phrase "
                 "resolves against."},
    "atlas": {
        "family": "atlas_catalog",
        "display": "Atlas Data Federated Catalog",
        "chip": "Atlas Catalog",
        "blurb": "The federated business-term catalog."},
    "metrics_dmp": {
        "family": "marketplace",
        "display": "Data Marketplace Steward Definitions",
        "chip": "Marketplace", "sub": "certified",
        "blurb": "Steward-certified metric definitions from the Data "
                 "Marketplace."},
    "extended_gmns": {
        "family": "marketplace",
        "display": "Data Marketplace Steward Definitions",
        "chip": "Marketplace", "sub": "GMNS spec",
        "blurb": "GMNS metric specifications submitted to the Data "
                 "Marketplace, awaiting certification."},
    "studio_queries": {
        "family": "marketplace",
        "display": "Data Marketplace Steward Definitions",
        "chip": "Marketplace", "sub": "certified SQL",
        "blurb": "The full certified SQL behind Marketplace "
                 "metrics."},
    "measures_catalog": {
        "family": "query_mining",
        "display": "Metric Mining: BQ Query History",
        "chip": "Query Mining",
        "blurb": "Metric expressions mined from real analyst queries "
                 "in the warehouse logs."},
    "skill_contract": {
        "family": "knowledge",
        "display": "Knowledge Files",
        "chip": "Knowledge",
        "blurb": "Curated domain knowledge files (per business unit), "
                 "the Artifacts shelf."},
    "blue_insights": {
        "family": "snippets",
        "display": "Analyst Snippet Library",
        "chip": "Snippets",
        "blurb": "SQL fragments captured from analyst notebooks and "
                 "insight tooling."},
    "lob_map": {
        "family": "domain_map",
        "display": "Steward Domain Map",
        "chip": "Domain Map",
        "blurb": "Line-of-business and org-unit ownership, asserted "
                 "by a human steward."},
    "org_map": {
        "family": "domain_map",
        "display": "Steward Domain Map",
        "chip": "Domain Map", "sub": "org units",
        "blurb": "Org-unit usage mapping, asserted by a human "
                 "steward."},
    "gold_queries": {
        "family": "gold",
        "display": "Gold Answer Key (evaluation only)",
        "chip": "Gold",
        "blurb": "The certified question/SQL answer key. Never feeds "
                 "ranking: it is the exam, not the student."},
    "llm_enricher": {
        "family": "enrichment",
        "display": "Synapse Enrichment (draft, pending steward "
                   "review)",
        "chip": "Draft",
        "blurb": "Questions and grains drafted by the enrichment run "
                 "from real SQL evidence. Unreviewed until a steward "
                 "acts."},
    "clerk": {
        "family": "steward",
        "display": "Steward Decision",
        "chip": "Steward",
        "blurb": "A recorded human decision, the strongest evidence "
                 "in the graph."},
}

# witness-level fallback for sources the registry has not met — the
# honest render is the raw name, never a guess at a pretty one
WITNESS_DISPLAY: dict[str, str] = {
    "audit_30d": "Access Audit Corroboration",
    "steward": "Steward Decision",
    "user_variant": "Team Candidate",
    "studio": "Data Marketplace Steward Definitions",
}


def display_for(source: str, witness: str = "") -> dict[str, str]:
    """→ display entry for a source; unknown sources render as
    themselves (family "unregistered") so nothing is silently
    prettified — and the completeness test keeps the registry
    honest."""
    entry = SOURCE_DISPLAY.get(source)
    if entry is not None:
        return entry
    if witness in WITNESS_DISPLAY:
        return {"family": witness, "display": WITNESS_DISPLAY[witness],
                "chip": witness, "blurb": ""}
    return {"family": "unregistered", "display": source or witness,
            "chip": (source or witness)[:14], "blurb": ""}


# ── utilization-ledger grouping (Sources rail) ───────────────
# path-substring → source key; first match wins (ordered specific →
# general). The ledger speaks file paths; the shelf speaks sources.
UTILIZATION_PATTERNS: tuple[tuple[str, str], ...] = (
    ("17_queries", "jobs_30d"),
    ("audit_", "jobs_30d"),
    ("metrics_dmp", "metrics_dmp"),
    ("extended_gmns", "extended_gmns"),
    ("studio_results", "studio_queries"),
    ("measures_catalog", "measures_catalog"),
    ("business_terms", "business_terms"),
    ("low_cardinality_synonyms_index", "value_lookup"),
    ("value_lookup", "value_lookup"),
    ("potential_common_word", "glossary"),
    ("glossary_terms", "glossary"),
    ("data_cleaned", "glossary"),
    ("std_tech", "std_tech_metadata"),
    ("skill", "skill_contract"),
    ("gold", "gold_queries"),
    ("blue", "blue_insights"),
    ("lob_map", "lob_map"),
    ("org_map", "org_map"),
    ("mdm", "lumi"),
)


def source_of_path(path: str) -> str:
    lowered = (path or "").lower()
    for needle, source in UTILIZATION_PATTERNS:
        if needle in lowered:
            return source
    return "bq"        # both archive roots default to the warehouse


# ── the tier bridge (trust symbols) ──────────────────────────
# ● ha human_asserted · ◆ gr grounded · ◐ in inferred · ○ gu guessed.
# Crimson is NOT a tier: conflict renders only on conflict surfaces
# (ReviewItems, competing definitions) — "crimson = definition
# conflict, only, ever."

def tier_of_metric(row: dict[str, Any]) -> str:
    """Metric rows: certified → ha; a steward-authored spec awaiting
    certification → gr; corroborated mined (≥2 ranking witness
    families) → gr; single-witness mined and enrichment drafts →
    in; anything else → gu."""
    status = str(row.get("status") or "")
    if status == "certified":
        return "ha"
    if status == "pending":
        return "gr"
    if int(row.get("witness_agreement") or 0) >= 2:
        return "gr"
    if status in ("mined", "team_candidate") \
            or row.get("status_served") == "unreviewed":
        return "in"
    return "gu"


def tier_of_join(row: dict[str, Any]) -> str:
    """Join rows: declared constraints → ha; measured ON equalities →
    gr; CTE-scoped studio joins → in (real evidence, NOT raw-safe);
    pattern-only and bare co-query → gu."""
    source = str(row.get("source") or "")
    if source == "constraints":
        return "ha"
    if row.get("scope") == "scoped_only" \
            or row.get("confidence") == "pattern_only":
        return "in" if row.get("scope") == "scoped_only" else "gu"
    if source == "co_query":
        return "gu"
    return "gr"


def tier_of_table(purpose: str, metrics_here: int,
                  lob_mapped: bool) -> str:
    """Tables: documented purpose + steward LOB mapping → ha;
    ≥1 metric bound → gr; purpose only → in; bare structure → gu."""
    if purpose and lob_mapped:
        return "ha"
    if metrics_here > 0:
        return "gr"
    if purpose:
        return "in"
    return "gu"


# ── the Sources shelf (indexes/sources.json) ─────────────────

def build_sources_index(nodes: dict[str, Any],
                        edges: dict[tuple, Any],
                        metric_rows: list[dict[str, Any]],
                        lob_rows: list[dict[str, Any]],
                        graph_root) -> dict[str, Any]:
    """→ the Sources rail: every ingestion source with its display
    identity, what it contributed (swept from prov — never estimated),
    and its utilization-ledger line from the latest graph run. The
    ledger built for CI honesty becomes the trust centerpiece: here is
    everything the company handed Meridian, and proof we read it."""
    import json as _json
    from collections import Counter, defaultdict
    from pathlib import Path

    node_kinds: dict[str, Counter] = defaultdict(Counter)
    for node_id, record in nodes.items():
        source = record.prov.source or record.prov.witness or "?"
        node_kinds[source][node_id.split(":", 1)[0]] += 1
    edge_rels: dict[str, Counter] = defaultdict(Counter)
    for (_s, r, _o, _w), quad in edges.items():
        source = quad.prov.source or quad.prov.witness or "?"
        edge_rels[source][r] += 1

    ledger: dict[str, Counter] = defaultdict(Counter)
    runs = sorted(Path(graph_root).glob("runs/*/manifest.json"))
    for manifest_path in reversed(runs):
        try:
            payload = _json.loads(
                manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        utilization = payload.get("utilization") or []
        if not utilization:
            continue
        for entry in utilization:
            status = str(entry.get("status") or "")
            key = source_of_path(str(entry.get("path") or ""))
            ledger[key][status.split("(")[0]] += 1
        break                     # latest run with a ledger wins

    seen = sorted(set(node_kinds) | set(edge_rels) | set(ledger))
    rows = []
    for source in seen:
        entry = display_for(source)
        rows.append({
            "source": source, **entry,
            "contributes": {"nodes": dict(node_kinds.get(
                source, Counter())),
                "edges": dict(edge_rels.get(source, Counter()))},
            "ledger": dict(ledger.get(source, Counter()))})

    # readiness by LOB — the formula is served next to the number so
    # nobody has to trust a bare percentage
    metrics_by_table: dict[str, int] = Counter()
    for row in metric_rows:
        if row.get("table"):
            metrics_by_table[row["table"]] += 1
    readiness = {}
    for lob in lob_rows:
        code = str(lob.get("code") or lob.get("lob") or "?")
        tables = lob.get("tables", [])
        if not tables:
            continue
        ready = sum(1 for t in tables if metrics_by_table.get(t))
        readiness[code] = {"tables": len(tables), "witnessed": ready,
                           "pct": round(100 * ready / len(tables))}
    return {
        "schema": "meridian.sources/1",
        "sources": rows,
        "readiness": readiness,
        "meta": {
            "contributes": "swept from prov.source over the folded "
                           "graph: counts, never estimates",
            "ledger": "latest graph run's utilization ledger, grouped "
                      "by source (consumed / inventoried / deferred)",
            "readiness": "per LOB: share of its steward-mapped tables "
                         "with at least one witnessed metric bound",
        },
    }
