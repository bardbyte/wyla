"""Table facts — the compiled, serving-facing projection of EVERYTHING
the graph holds about a table (G3 of docs/audits/
card_sourcing_audit_2026_09.md).

One row per table, written to ``indexes/tables.jsonl`` and read by
three consumers that must never disagree: the table card the agent
reads (``cards.table_card``), the console's Table Profile
(``/api/meridian/table``), and the cosmos map. The card is a
RENDERING of this row, the profile is a RENDERING of this row —
neither re-derives a fact from the graph, so a number on the screen
and the number the agent reasoned from are the same number by
construction (S: compile once, serve twice).

Shape (``meridian.table_facts/1``) — every group is a fact family the
audit found extracted-and-never-shown:

  identity     what it IS: business name, description (+source),
               project/dataset, object/table/layer/load type,
               category → sub-category, the Atlas registry id
  business     who it is FOR: MDM business unit, steward LOBs with
               witnesses, org units that RUN queries on it, the
               ownership chain per role with witnesses, top users
  operations   how it BEHAVES: lifecycle, environment, feed, pipeline,
               source system, created/last modified, rows, bytes,
               partitions, usage rhythm, cost prior
  trust        whether to BELIEVE it: MDM answerability by axis,
               active/latest/lineage-declared flags, display tier
  access       whether you MAY: row policy, table-level PII/GDPR/
               ONCOP, sensitive columns with their roles
  column_facts every column with type + agreement, business name,
               description(s), sensitivity role/group, key/partition/
               nullable, ordinals from both planes, profile numbers,
               value-domain marker, linked business terms with their
               definitions, computed-column logic, lineage, D-flags
  joins        declared FKs (fiat), observed co-queries, scoped joins
  lineage      upstream/downstream tables, view SQL presence
  vocabulary   the acronyms whose symbol appears in a column or
               business name, scoped to THIS table's business units —
               the context card, embedded

Absent stays absent: a missing fact is a missing key or ``None``,
never a default that could be read as a claim.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from sahs.compiler.reconcile import TableConsensus

SCHEMA = "meridian.table_facts/1"

_TOKEN = re.compile(r"[^a-z0-9]+")


def _norm_symbol(text: str) -> str:
    """Acronym symbols and column tokens meet on one spelling:
    lower-case alphanumerics only (``A/R`` → ``ar``, ``CM13`` →
    ``cm13``). Exact token equality after that — never a prefix or a
    substring, which is how ``SE`` would falsely decode ``se_no``."""
    return _TOKEN.sub("", (text or "").lower())


def _tokens(*texts: str) -> set[str]:
    out: set[str] = set()
    for text in texts:
        if not text:
            continue
        out.add(_norm_symbol(text))
        out.update(t for t in _TOKEN.split(text.lower()) if t)
    out.discard("")
    return out


def _kept(mapping: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in mapping.items()
            if v not in (None, "", [], {})}


def _int(value: Any) -> int | None:
    if value in (None, "", False):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def match_vocabulary(vocab_rows: list[dict[str, Any]],
                     column_facts: list[dict[str, Any]],
                     business_units: set[str],
                     limit: int | None = None) -> list[dict[str, Any]]:
    """The context card, computed: every acronym/glossary entry whose
    normalized symbol equals a token of a column name or a column's
    business name on this table — restricted to entries scoped to
    one of THIS table's business units (or to ``All``). The BU scope
    is what resolves ``ABP`` to "Automatic Bill Pay" on a GMNS table
    and to "Abandoned Property" elsewhere; an entry from a foreign BU
    is not offered, because offering it would be a guess."""
    scopes = {b.lower() for b in business_units if b} | {"all", ""}
    tokens_by_col: dict[str, set[str]] = {
        c["name"]: _tokens(c["name"], c.get("business_name", ""))
        for c in column_facts}
    hits: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in vocab_rows:
        if row.get("kind") not in ("acronym", "term"):
            continue
        bu = str(row.get("bu") or "all").lower()
        if bu not in scopes:
            continue
        symbol = _norm_symbol(str(row.get("text") or ""))
        if len(symbol) < 2:
            continue
        matched = sorted(name for name, toks in tokens_by_col.items()
                         if symbol in toks)
        if not matched:
            continue
        key = (str(row.get("text")), bu, str(row.get("region") or "all"))
        entry = hits.setdefault(key, {
            "symbol": row.get("text", ""),
            "definition": row.get("definition", ""),
            "kind": row.get("kind", ""),
            "status": row.get("status", ""),
            "bu": row.get("bu", "All"),
            "region": row.get("region", "All"),
            "ref": row.get("ref", ""),
            "columns": []})
        entry["columns"] = sorted(set(entry["columns"]) | set(matched))
    ordered = sorted(hits.values(),
                     key=lambda e: (-len(e["columns"]),
                                    str(e["symbol"]).lower(),
                                    str(e["bu"]).lower()))
    return ordered[:limit] if limit else ordered


def build_table_facts(
        consensus: dict[str, TableConsensus],
        nodes: dict[str, Any],
        edges: dict[tuple[str, str, str, str], Any],
        acl: dict[str, Any],
        lob_by_table: dict[str, list[dict[str, Any]]],
        usage_by_table: dict[str, list[dict[str, Any]]],
        co_by_table: dict[str, list[tuple[str, int]]],
        scoped_by_table: dict[str, list[dict[str, Any]]],
        metrics_by_table: dict[str, int],
        filters_by_table: dict[str, int],
        vocab_rows: list[dict[str, Any]],
        tier_of_table) -> dict[str, dict[str, Any]]:
    """→ {physical: facts}. Deterministic: sorted iteration over the
    folded graph, no wall clock, no RNG."""
    # ── edge sweeps, once ─────────────────────────────────────
    fks_by_col: dict[str, list[str]] = defaultdict(list)
    derived_from: dict[str, list[dict[str, Any]]] = defaultdict(list)
    upstream: dict[str, list[str]] = defaultdict(list)
    downstream: dict[str, list[str]] = defaultdict(list)
    docs_of: dict[str, list[str]] = defaultdict(list)
    terms_of: dict[str, list[dict[str, Any]]] = defaultdict(list)
    owners_of: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    policies_of: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set))
    domain_of: dict[str, list[dict[str, Any]]] = {}
    for (s, r, o, w), quad in sorted(edges.items()):
        if quad.prov.status != "active":
            continue
        if r == "fk_references":
            fks_by_col[s].append(o)
        elif r == "derived_from":
            derived_from[s].append(_kept({
                "source": o.split(":", 1)[1],
                "logic": quad.props.get("derivation_logic", "")}))
        elif r == "upstream_of":
            upstream[o.split(":", 1)[1]].append(s.split(":", 1)[1])
            downstream[s.split(":", 1)[1]].append(o.split(":", 1)[1])
        elif r == "described_by" or r == "evidenced_by":
            docs_of[s].append(o)
        elif r == "mapped_term":
            term = nodes.get(o)
            props = term.props if term is not None else {}
            terms_of[s].append(_kept({
                "id": o.split(":", 2)[-1],
                "name": props.get("name", ""),
                "status": props.get("status", ""),
                "description": props.get("description", ""),
                "matched_on": quad.props.get("matched_on", ""),
                "confidence": quad.props.get("confidence")}))
        elif r == "owned_by":
            owner = o.split(":", 1)[1]
            record = nodes.get(o)
            role = (str(quad.props.get("role") or "")
                    or (record.props.get("role", "")
                        if record is not None else ""))
            cell = owners_of[s].setdefault(owner, {
                "owner": owner, "roles": set(), "witnesses": set()})
            if role:
                cell["roles"].add(role)
            cell["witnesses"].add(w or quad.prov.source)
        elif r == "has_policy":
            policies_of[s][o.split(":", 1)[1]].add(w or quad.prov.source)
        elif r == "has_domain":
            record = nodes.get(o)
            if record is not None:
                domain_of[s] = list(record.props.get("values") or [])
    # domains are also reachable by node id alone (the loader mints
    # the node and the edge together; be robust to either)
    for node_id, record in nodes.items():
        if node_id.startswith("domain:"):
            cid = "col:" + node_id.split(":", 1)[1]
            domain_of.setdefault(cid, list(record.props.get("values")
                                           or []))

    out: dict[str, dict[str, Any]] = {}
    for tid, table in sorted(consensus.items()):
        physical = table.physical
        record = nodes.get(tid)
        props = record.props if record is not None else {}
        meta = props.get("table_meta_logical") or {}
        metrics_row = props.get("table_metrics") or {}
        ownership_atlas = props.get("ownership_atlas") or {}
        business_units = {str(props.get("business_unit") or "")}
        business_units |= {str(e.get("code") or "")
                           for e in lob_by_table.get(physical, [])}
        business_units.discard("")

        # ── columns ──────────────────────────────────────────
        column_facts: list[dict[str, Any]] = []
        for name in sorted(table.columns):
            column = table.columns[name]
            cid = f"col:{physical}.{name}"
            crec = nodes.get(cid)
            cprops = crec.props if crec is not None else {}
            domain = domain_of.get(cid)
            fk_targets = fks_by_col.get(cid, [])
            # a nested field path carries its description from BQ
            # alone — reconcile's Atlas-first/Lumi-second order has
            # nowhere to fall to, so the fallback lives here
            description = column.description
            description_source = column.description_source
            if not description and cprops.get("description_bq"):
                description = str(cprops["description_bq"])
                description_source = "bq"
            atlas_name = str(cprops.get("column_name_atlas") or "")
            column_facts.append(_kept({
                "name": name,
                "type": column.data_type,
                "type_source": column.type_source,
                "agreement": column.agreement_count,
                "business_name": column.business_name,
                "column_name_atlas": (atlas_name if atlas_name
                                      and atlas_name.lower() != name
                                      else None),
                "description": description,
                "description_source": description_source,
                "description_supplementary":
                    column.description_supplementary,
                "sensitive": column.sensitive or None,
                "sensitivity_sources": column.sensitivity_sources,
                "pii_role": cprops.get("pii_role_id"),
                "pii_role_table_declared":
                    cprops.get("pii_role_id_table_declared"),
                "sde_group": cprops.get("sde_group"),
                "primary_key": (True if cprops.get("is_primary_key")
                                else None),
                "primary_key_atlas": cprops.get("is_primary_key_atlas"),
                "partitioning": (True if cprops.get("is_partitioning")
                                 else None),
                "partitioning_atlas": cprops.get("is_partitioning_atlas"),
                "nullable_atlas": cprops.get("nullable_atlas"),
                "ordinal": _int(cprops.get("ordinal")),
                "ordinal_atlas": _int(cprops.get("ordinal_atlas")),
                "column_length": _int(cprops.get("column_length")),
                "approx_distinct": _int(cprops.get("approx_distinct")),
                "null_count": _int(cprops.get("null_count")),
                "profile_coverage": cprops.get("profile_coverage"),
                "domain": ({"n_values": len(domain),
                            "top": [{"value": v.get("value"),
                                     "pct": v.get("pct")}
                                    for v in domain[:3]]}
                           if domain else None),
                "terms": terms_of.get(cid, []),
                "declared_terms": cprops.get("declared_terms") or [],
                "derived_logic": cprops.get("derived_logic", ""),
                "derived_from": derived_from.get(cid, []),
                "fk_references": [
                    {"table": ".".join(t.split(":", 1)[1]
                                       .split(".")[:2]),
                     "column": t.split(":", 1)[1].split(".", 2)[-1]}
                    for t in fk_targets],
                "flags": column.flags,
                "ungoverned": column.ungoverned or None,
                "observed_via": cprops.get("observed_via"),
            }))

        primary_key = [c["name"] for c in column_facts
                       if c.get("primary_key")]
        primary_key_atlas = [c["name"] for c in column_facts
                             if c.get("primary_key_atlas")]
        partition_columns = [c["name"] for c in column_facts
                             if c.get("partitioning")]
        partition_columns_atlas = [c["name"] for c in column_facts
                                   if c.get("partitioning_atlas")]
        sensitive = [c for c in column_facts if c.get("sensitive")]

        # ── owners: Atlas roles + MDM owned_by edges, merged ──
        owners: dict[str, dict[str, Any]] = {}
        for owner, cell in owners_of.get(tid, {}).items():
            owners[owner] = {"owner": owner,
                             "roles": sorted(cell["roles"]),
                             "witnesses": sorted(cell["witnesses"])}
        for role, value in sorted(ownership_atlas.items()):
            name = str(value or "").strip().lower()
            if not name or not ("owner" in role.lower()
                                or "vp" in role.lower()):
                continue
            cell = owners.setdefault(name, {"owner": name, "roles": [],
                                            "witnesses": []})
            if role not in cell["roles"]:
                cell["roles"] = sorted(set(cell["roles"]) | {role})
            if "atlas" not in cell["witnesses"]:
                cell["witnesses"] = sorted(
                    set(cell["witnesses"]) | {"atlas"})
        owner_rows = sorted(owners.values(),
                            key=lambda o: (o["roles"][:1], o["owner"]))
        ownership_ids = {k: v for k, v in ownership_atlas.items()
                         if not ("owner" in k.lower()
                                 or "vp" in k.lower())}

        # ── access ───────────────────────────────────────────
        acl_entry = acl.get(physical, {"restricted": None,
                                       "pii_columns": []})
        policies = {name: sorted(ws)
                    for name, ws in sorted(policies_of.get(tid, {})
                                           .items())}
        table_pii = bool(props.get("has_pii_atlas")) \
            or "pii" in policies or bool(sensitive)

        vocabulary = match_vocabulary(vocab_rows, column_facts,
                                      business_units)
        lobs = lob_by_table.get(physical, [])
        purpose = str(props.get("description_atlas")
                      or props.get("description_bq") or "")
        facts = {
            "schema": SCHEMA,
            "physical": physical,
            "short": physical.split(".")[-1],
            "columns": len(column_facts),
            # E18/C compatibility: fan-out judgement reads these flat
            "primary_key": primary_key,
            "total_rows": _int(props.get("total_rows")),
            "object_type": props.get("object_type") or None,
            "partition_latest": props.get("partition_latest") or None,
            "lifecycle": props.get("lifecycle_status") or None,
            "identity": _kept({
                "business_name": props.get("business_name_atlas", ""),
                "description": purpose,
                "description_source": (
                    "atlas" if props.get("description_atlas")
                    else "bq" if props.get("description_bq") else ""),
                "description_bq": (props.get("description_bq", "")
                                   if props.get("description_atlas")
                                   else ""),
                "project": props.get("project")
                or props.get("project_atlas", ""),
                "dataset": props.get("dataset_group_atlas")
                or physical.split(".")[0],
                "table_name_atlas": props.get("table_name_atlas", ""),
                "object_type": props.get("object_type", ""),
                "table_type": props.get("table_type_atlas", ""),
                "layer_type": props.get("layer_type", ""),
                "load_type": props.get("load_type_atlas", ""),
                "is_partitioned_atlas": props.get("is_partitioned_atlas"),
                "target_system": props.get("target_system_atlas", ""),
                "technology": props.get("technology_atlas", ""),
                "data_server": props.get("data_server_atlas", ""),
                "data_system": props.get("data_system_atlas", ""),
                "appl_id": props.get("appl_id", ""),
                "data_category": props.get("data_category", ""),
                "data_sub_category": props.get("data_sub_category", ""),
                "schema_fingerprint": props.get("schema_fingerprint", ""),
            }),
            "business": _kept({
                "business_unit": props.get("business_unit", ""),
                "business_units": sorted(business_units),
                "lobs": lobs,
                "used_by": usage_by_table.get(physical, []),
                "owners": owner_rows,
                "ownership_ids": ownership_ids,
                "top_users": props.get("top_users") or [],
            }),
            "operations": _kept({
                "lifecycle": props.get("lifecycle_status", ""),
                "environment": props.get("environment", ""),
                "feed_type": props.get("feed_type", ""),
                "pipeline_name": props.get("pipeline_name", ""),
                "source_system": props.get("source_system", ""),
                "created": meta.get("created", ""),
                "last_modified": meta.get("last_modified", ""),
                "total_rows": _int(props.get("total_rows")),
                "size_bytes": _int(metrics_row.get("table_size_bytes")),
                "n_partitions": _int(props.get("n_partitions")),
                "partition_latest": props.get("partition_latest", ""),
                "partition_columns": partition_columns,
                "partition_columns_atlas": partition_columns_atlas,
                "primary_key_atlas": primary_key_atlas,
                "usage_rhythm": props.get("usage_rhythm") or [],
                "cost_prior": props.get("cost_prior") or {},
            }),
            "trust": _kept({
                "answerability": props.get("answerability") or {},
                "is_active_atlas": props.get("is_active_atlas"),
                "is_latest_atlas": props.get("is_latest_atlas"),
                "is_lineage_exist_atlas":
                    props.get("is_lineage_exist_atlas"),
                "tier": tier_of_table(purpose,
                                      metrics_by_table.get(physical, 0),
                                      bool(lobs)),
                "metrics_here": metrics_by_table.get(physical, 0),
                "filters_here": filters_by_table.get(physical, 0),
                "structural": dict(table.structural),
            }),
            "access": _kept({
                "restricted": acl_entry.get("restricted"),
                "pii_table": table_pii or None,
                "has_pii_atlas": props.get("has_pii_atlas"),
                "has_gdpr_atlas": props.get("has_gdpr_atlas"),
                "has_oncop_atlas": props.get("has_oncop_atlas"),
                "policies": policies,
                "sensitive_columns": [
                    _kept({"name": c["name"],
                           "pii_role": c.get("pii_role"),
                           "sde_group": c.get("sde_group"),
                           "sources": c.get("sensitivity_sources")})
                    for c in sensitive],
            }),
            "column_facts": column_facts,
            "joins": _kept({
                "declared": [
                    {"column": c["name"], "ref_table": ref["table"],
                     "ref_column": ref["column"]}
                    for c in column_facts
                    for ref in c.get("fk_references", [])],
                "observed": [{"other": other, "support": support}
                             for other, support in
                             sorted(co_by_table.get(physical, []),
                                    key=lambda x: (-x[1], x[0]))],
                "scoped": scoped_by_table.get(physical, []),
            }),
            "lineage": _kept({
                "upstream": sorted(set(upstream.get(physical, []))),
                "downstream": sorted(set(downstream.get(physical, []))),
                "derived_columns": [c["name"] for c in column_facts
                                    if c.get("derived_from")
                                    or c.get("derived_logic")],
                "view_sql": any(d.startswith("doc:view_sql")
                                for d in docs_of.get(tid, [])),
                "docs": sorted(docs_of.get(tid, [])),
            }),
            "vocabulary": vocabulary,
            "omitted_catalog_only": table.omitted_catalog_only,
        }
        out[physical] = facts
    return out


def build_lob_facts(lob_rows: list[dict[str, Any]],
                    table_facts: dict[str, dict[str, Any]],
                    vocab_rows: list[dict[str, Any]]
                    ) -> list[dict[str, Any]]:
    """One row per LOB / org unit — the business-unit card's data.
    Tables come with their business name, description, tier and
    metric count so the card can be read as a shelf; vocabulary is
    the count of acronyms scoped to this unit (Acropedia's own
    Business_Unit column)."""
    vocab_by_bu: dict[str, int] = defaultdict(int)
    for row in vocab_rows:
        if row.get("kind") in ("acronym", "term"):
            vocab_by_bu[str(row.get("bu") or "all").lower()] += 1
    out = []
    for lob in sorted(lob_rows, key=lambda r: str(r.get("code")
                                                  or r.get("lob"))):
        code = str(lob.get("code") or lob.get("lob") or "?")
        tables = []
        for physical in lob.get("tables", []):
            facts = table_facts.get(physical, {})
            tables.append(_kept({
                "physical": physical,
                "business_name": facts.get("identity", {})
                .get("business_name", ""),
                "description": facts.get("identity", {})
                .get("description", ""),
                "tier": facts.get("trust", {}).get("tier", ""),
                "metrics_here": facts.get("trust", {})
                .get("metrics_here", 0),
                "lifecycle": facts.get("lifecycle"),
                "pii": facts.get("access", {}).get("pii_table"),
                "business_unit": facts.get("business", {})
                .get("business_unit", ""),
            }))
        witnessed = sum(1 for t in tables if t.get("metrics_here"))
        owners: dict[str, set[str]] = defaultdict(set)
        for t in tables:
            for o in table_facts.get(t["physical"], {}).get(
                    "business", {}).get("owners", []):
                owners[o["owner"]].update(o.get("roles", []))
        out.append(_kept({
            "code": code,
            "name": lob.get("name", ""),
            "kind": lob.get("kind", "lob"),
            "parent": lob.get("parent", ""),
            "domains": lob.get("domains", []),
            "tables": tables,
            "used_tables": lob.get("used_tables", []),
            "usage_support": lob.get("usage_support", 0),
            "readiness": ({"tables": len(tables), "witnessed": witnessed,
                           "pct": round(100 * witnessed / len(tables))}
                          if tables else {}),
            "owners": [{"owner": o, "roles": sorted(r)}
                       for o, r in sorted(owners.items())],
            "vocabulary_entries": vocab_by_bu.get(code.lower(), 0),
        }))
    return out
