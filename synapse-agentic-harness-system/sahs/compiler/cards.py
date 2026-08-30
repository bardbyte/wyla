"""Cards — the agent's entire world, one token-budgeted page at a time.

Three templates (pinned sections; every line carries a ``[prov:…]`` ref;
conflicts are ALWAYS printed — a card that hides ambiguity is lying to
the agent). Table cards budget to ≤2K tokens with a fixed drop order —
column long-tail → history/usage → join detail — and NEVER drop grain,
conflicts, or access.
"""

from __future__ import annotations

from typing import Any

from sahs.compiler.reconcile import TableConsensus

TOKEN_BUDGET_TABLE = 2000
_PROTECTED = ("grain", "conflicts", "access")


def _tokens_of(text: str) -> int:
    return max(1, len(text) // 4)          # the classic 4-chars/token bound


def _lob_line(lob_info: list[dict[str, Any]]) -> str:
    """One line per card: every LOB membership with its witnesses named
    — `GMNS — Global Merchant & Network Services (steward; corroborated
    by 12 dmp metrics)`. Multi-membership renders every entry; the card
    never picks a winner."""
    rendered = []
    for entry in lob_info:
        witnesses = entry.get("witnesses", {})
        notes = []
        if "steward" in witnesses:
            notes.append("steward")
        if witnesses.get("dmp"):
            notes.append(f"corroborated by {witnesses['dmp']} "
                         "dmp metric(s)")
        if witnesses.get("gmns"):
            notes.append(f"{witnesses['gmns']} gmns spec(s)")
        if witnesses.get("catalog_mined"):
            notes.append(f"mined support {witnesses['catalog_mined']}")
        label = entry["code"] + (f" — {entry['name']}"
                                 if entry.get("name") else "")
        rendered.append(f"{label} ({'; '.join(notes) or 'declared'})")
    return ("- line of business: " + " · ".join(rendered)
            + " [prov:in_lob]")


def _usage_line(usage_info: list[dict[str, Any]]) -> str:
    """Who RUNS the queries (mined witness) — distinct from ownership.
    `used by: CFR — Credit & Fraud Risk (Finance) · 1062` tells the
    agent a cross-LOB join with risk data is normal here, not a
    mistake."""
    rendered = []
    for entry in usage_info[:5]:
        label = entry["code"]
        if entry.get("name"):
            label += f" — {entry['name']}"
        if entry.get("parent"):
            label += f" ({entry['parent']})"
        rendered.append(f"{label} · {entry['support']}")
    more = len(usage_info) - 5
    return ("- used by: " + " · ".join(rendered)
            + (f" · +{more} more" if more > 0 else "")
            + " [prov:used_by·catalog_mined]")


def table_card(consensus: TableConsensus, node_props: dict[str, Any],
               metrics_here: list[dict[str, Any]],
               filters_here: list[dict[str, Any]],
               co_queried: list[tuple[str, int]],
               acl_entry: dict[str, Any],
               lob_info: list[dict[str, Any]] | None = None,
               usage_info: list[dict[str, Any]] | None = None,
               scoped_joins: list[dict[str, Any]] | None = None
               ) -> tuple[str, dict[str, Any]]:
    """→ (markdown, budget_report)."""
    physical = consensus.physical
    prov = f"[prov:table:{physical}]"
    lines: dict[str, list[str]] = {k: [] for k in (
        "header", "purpose", "grain", "columns", "joins", "filters",
        "metrics", "access", "conflicts", "footer")}

    lifecycle = node_props.get("lifecycle_status") or "unknown"
    lines["header"] = [
        f"# table {physical}",
        f"- object: {node_props.get('object_type', 'TABLE')} · "
        f"rows ≈ {node_props.get('total_rows', '?')} · "
        f"lifecycle: {lifecycle} {prov}",
        f"- owner: {node_props.get('ownership_atlas', {}).get('business_owner') or '?'} · "
        f"business unit: {node_props.get('business_unit', '?')} · "
        f"layer: {node_props.get('layer_type', '?')} {prov}",
    ]
    if lob_info:
        lines["header"].append(_lob_line(lob_info))
    if usage_info:
        lines["header"].append(_usage_line(usage_info))
    purpose = (node_props.get("description_atlas")
               or node_props.get("description_bq") or "")
    if purpose:
        lines["purpose"] = [f"- purpose: {purpose} [prov:atlas]"]
    partition = node_props.get("partition_latest")
    lines["grain"] = [
        f"- partitioned: latest {partition or 'n/a'} · "
        f"schema {node_props.get('schema_fingerprint', '?')} {prov}"]
    if node_props.get("usage_rhythm"):
        lines["grain"].append(
            "- usage rhythm: " + " · ".join(node_props["usage_rhythm"])
            + " [prov:jobs_30d]")

    column_rows = []
    for column in consensus.columns.values():
        flags = []
        if column.sensitive:
            flags.append("SENSITIVE")
        if column.ungoverned:
            flags.append("ungoverned — no business meaning on record")
        note = f" ({'; '.join(flags)})" if flags else ""
        desc = column.description
        supplementary = (f" | lumi: {column.description_supplementary}"
                         if column.description_supplementary else "")
        column_rows.append(
            f"- {column.name} {column.data_type.lower()}"
            f"{note} — {desc}{supplementary} "
            f"[prov:{column.type_source or 'bq'}"
            f"·agree={column.agreement_count}]")
    lines["columns"] = ["## columns"] + column_rows

    lines["joins"] = ["## joined with (observed)"] + [
        f"- {other} · {support} co-queries [prov:bq:history]"
        for other, support in co_queried[:8]]
    for j in (scoped_joins or [])[:4]:
        # a scoped_only witness means the equality was observed between
        # TRANSFORMED CTEs — the relationship exists; the raw tables do
        # NOT join safely without the stated preparation
        caveat = ("" if j.get("scope") == "raw_safe"
                  else " — CTE-scoped, NOT raw-safe"
                  + (f"; requires: {'; '.join(j['preconditions'][:2])}"
                     if j.get("preconditions") else ""))
        lines["joins"].append(
            f"- {j['other']} ON {' AND '.join(j.get('on') or ['?'])} · "
            f"{j.get('join_type') or 'JOIN'} · {j.get('scope')}{caveat} "
            f"[prov:{j.get('witness') or 'studio'}]")
    lines["filters"] = ["## common filters"] + [
        f"- {f['label']}: `{f['sql']}` · support {f['support']} "
        f"[prov:{f['source']}]" for f in filters_here[:8]]
    lines["metrics"] = ["## metrics available"] + [
        f"- {m['label'] or m['id']} · "
        f"{m.get('status_served') or m['status']} [prov:{m['source']}]"
        for m in metrics_here[:10]]

    access = []
    restricted = acl_entry.get("restricted")
    if restricted == "unknown_policy":
        access.append("- ⚠ row-access policy UNKNOWN (listing denied) — "
                      "live execution DENIED until resolved [prov:bq]")
    elif restricted:
        access.append(f"- row-access policy: {restricted} [prov:bq]")
    if acl_entry.get("pii_columns"):
        access.append("- sensitive columns: "
                      + ", ".join(acl_entry["pii_columns"])
                      + " [prov:union_most_restrictive]")
    lines["access"] = ["## access"] + (access or ["- no known restrictions"])

    conflicts = []
    for name, count in consensus.structural.items():
        if count:
            conflicts.append(f"- {name}: {count} — see structural census")
    for column in consensus.columns.values():
        for flag in column.flags:
            conflicts.append(f"- {column.name}: {flag}")
    if consensus.omitted_catalog_only:
        conflicts.append(
            "- omitted catalog-only columns (D1): "
            + ", ".join(consensus.omitted_catalog_only))
    lines["conflicts"] = ["## conflicts"] + (conflicts or ["- none"])
    lines["footer"] = [f"[prov: consensus of bq+lumi+atlas via "
                       f"merge_policy — every line traceable] {prov}"]

    # ── budgeter: fixed drop order, protected sections never dropped ──
    drop_order = [("columns", 12), ("joins", 3), ("filters", 3)]
    dropped: dict[str, int] = {}

    def render() -> str:
        order = ("header", "purpose", "grain", "columns", "joins",
                 "filters", "metrics", "access", "conflicts", "footer")
        return "\n".join(x for key in order for x in lines[key])

    text = render()
    for section, keep in drop_order:
        if _tokens_of(text) <= TOKEN_BUDGET_TABLE:
            break
        header, rows = lines[section][0], lines[section][1:]
        if len(rows) > keep:
            dropped[section] = len(rows) - keep
            lines[section] = [header] + rows[:keep] + [
                f"- … {len(rows) - keep} more (budget-dropped; "
                f"full detail in the truth graph)"]
        text = render()
    assert all(lines[p] for p in _PROTECTED)
    return text, {"tokens": _tokens_of(text),
                  "over_budget": _tokens_of(text) > TOKEN_BUDGET_TABLE,
                  "dropped": dropped}


def metric_card(metric: dict[str, Any],
                variants: list[dict[str, Any]]) -> str:
    served = metric.get("status_served") or metric["status"]
    origin = metric.get("evidence_origin") or metric["source"]
    lines = [
        f"# metric {metric['label'] or metric['id']}",
        # governance status and evidence origin are DIFFERENT axes —
        # "unreviewed (evidence: usage_mining)" tells the agent what it
        # may claim; "mined" alone let it read origin as endorsement
        f"- id: {metric['id']} · status: **{served}** "
        f"(evidence: {origin}) [prov:{metric['source']}]",
    ]
    if metric.get("support_by_witness"):
        witnesses = " · ".join(
            f"{family}:{n}" for family, n in sorted(
                metric["support_by_witness"].items()))
        lines.append(
            f"- witnesses: {witnesses} · agreement "
            f"{metric.get('witness_agreement', 0)}"
            + (f" · recency from {metric['recency_source']}"
               if metric.get("recency_source") else "")
            + " [prov:witness]")
    pedigree = [f"{key}: {metric[field]}" for key, field in
                (("domain", "domain"), ("lob", "line_of_business"),
                 ("author", "author"), ("scope", "scope"))
                if metric.get(field)]
    if pedigree:
        lines.append(f"- {' · '.join(pedigree)} "
                     f"[prov:{metric['source']}]")
    if metric.get("question"):
        q_src = metric.get("question_source") or "dmp"
        lines.append(f"- answers: “{metric['question']}” "
                     + ("[prov:llm_enriched·unreviewed]"
                        if q_src == "llm_enriched" else "[prov:dmp]"))
    if metric.get("description"):
        # the catalog's own hand-written guidance — what it measures,
        # the "do not use for…" list, disambiguation instructions.
        # Verbatim: this text IS the serving payload
        lines.append(f"- guidance: {metric['description']} "
                     f"[prov:{metric['source']}]")
    grain_prov = ("llm_enriched·unreviewed"
                  if metric.get("grain_source") == "llm_enriched"
                  else metric["source"])
    grain_line = (f"- grain: {metric.get('grain') or 'unspecified'} "
                  f"[prov:{grain_prov}]")
    if not metric.get("grain") and metric.get("grain_observed"):
        # observed in the studio export — texture, never identity
        grain_line = (f"- grain: {metric['grain_observed']} (observed) "
                      "[prov:studio]")
    lines += [
        grain_line,
        f"- expression: `{metric['canonical_sql']}` "
        f"[prov:{metric['source']}·fp={metric['fp']}]",
        f"- table: {metric['table']} [prov:{metric['source']}]",
    ]
    if metric.get("query_shape"):
        lines.append("- query shape: "
                     + "/".join(metric["query_shape"])
                     + " — full SQL retained as evidence [prov:studio]")
    if metric.get("tables_associated_not_referenced"):
        lines.append(
            "- ⚠ associated but NOT referenced by the SQL: "
            + ", ".join(metric["tables_associated_not_referenced"])
            + " — declared lineage the query never reads [prov:studio]")
    if metric.get("data_owners"):
        lines.append("- data owners: "
                     + ", ".join(metric["data_owners"])
                     + " [prov:studio]")
    if metric.get("join_condition"):
        lines.append(f"- declared join condition: "
                     f"`{metric['join_condition']}` [prov:dmp]")
    if metric.get("approved_dimensions"):
        lines.append("- approved dimensions: "
                     + ", ".join(metric["approved_dimensions"])
                     + " [prov:gmns]")
    if metric.get("sign_convention"):
        lines.append(f"- calculation notes: {metric['sign_convention']} "
                     "[prov:gmns]")
    if metric.get("parent_fp"):
        lines.append(f"- **off-meridian**: variant of metric "
                     f"{metric['parent_fp']} (the meridian line for this "
                     "intent) [prov:variant_of]")
    lines.append("## variants")
    if variants:
        for variant in variants:
            lines.append(
                f"- off-meridian by expression · fp={variant['fp']} · "
                f"{variant['status']} · support {variant['support']} "
                f"[prov:{variant['source']}] — "
                f"`{variant['canonical_sql'][:100]}`")
    else:
        lines.append("- none recorded")
    lines.append("## conflicts")
    lines.append("- see mgroup census cell" if len(variants) else "- none")
    return "\n".join(lines)


def concept_card(label: str, bindings: list[dict[str, Any]],
                 enriched: dict[str, str] | None = None) -> str:
    lines = [f"# concept {label}"]
    tables = sorted({b["table"] for b in bindings})
    lines.append(f"- bound on: {', '.join(tables)}")
    if enriched and enriched.get("description"):
        lines.append(f"- meaning: {enriched['description']} "
                     "[prov:llm_enriched·unreviewed]")
        if enriched.get("disambiguation"):
            lines.append(f"- disambiguation: "
                         f"{enriched['disambiguation']} "
                         "[prov:llm_enriched·unreviewed]")
    lines.append("## bindings (ranked: authority ≻ support ≻ recency)")
    for b in bindings:
        lines.append(
            f"- {b['table']}: `{b['canonical_sql']}` · "
            f"authority={b['authority']} · support={b['support']} "
            f"[prov:{b['source']}·fp={b['fp']}]")
    n_classes = len({b["fp"] for b in bindings})
    lines.append("## conflicts")
    lines.append(
        f"- {n_classes} distinct classes — resolver will ask below margin"
        if n_classes > 1 else "- none")
    return "\n".join(lines)
