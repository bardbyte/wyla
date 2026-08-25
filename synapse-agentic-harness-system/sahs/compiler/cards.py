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


def table_card(consensus: TableConsensus, node_props: dict[str, Any],
               metrics_here: list[dict[str, Any]],
               filters_here: list[dict[str, Any]],
               co_queried: list[tuple[str, int]],
               acl_entry: dict[str, Any]) -> tuple[str, dict[str, Any]]:
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
    purpose = (node_props.get("description_atlas")
               or node_props.get("description_bq") or "")
    if purpose:
        lines["purpose"] = [f"- purpose: {purpose} [prov:atlas]"]
    partition = node_props.get("partition_latest")
    lines["grain"] = [
        f"- partitioned: latest {partition or 'n/a'} · "
        f"schema {node_props.get('schema_fingerprint', '?')} {prov}"]

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
    lines["filters"] = ["## common filters"] + [
        f"- {f['label']}: `{f['sql']}` · support {f['support']} "
        f"[prov:{f['source']}]" for f in filters_here[:8]]
    lines["metrics"] = ["## metrics available"] + [
        f"- {m['label'] or m['id']} · {m['status']} [prov:{m['source']}]"
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
    lines = [
        f"# metric {metric['label'] or metric['id']}",
        f"- id: {metric['id']} · status: **{metric['status']}** "
        f"[prov:{metric['source']}]",
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
    if metric.get("question"):
        lines.append(f"- answers: “{metric['question']}” [prov:dmp]")
    lines += [
        f"- grain: {metric.get('grain') or 'unspecified'} "
        f"[prov:{metric['source']}]",
        f"- expression: `{metric['canonical_sql']}` "
        f"[prov:{metric['source']}·fp={metric['fp']}]",
        f"- table: {metric['table']} [prov:{metric['source']}]",
    ]
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


def concept_card(label: str, bindings: list[dict[str, Any]]) -> str:
    lines = [f"# concept {label}"]
    tables = sorted({b["table"] for b in bindings})
    lines.append(f"- bound on: {', '.join(tables)}")
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
