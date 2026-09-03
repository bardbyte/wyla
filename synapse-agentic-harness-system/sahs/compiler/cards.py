"""Cards — the agent's entire world, one token-budgeted page at a time.

Four templates (pinned sections; every line carries a ``[prov:…]`` ref;
conflicts are ALWAYS printed — a card that hides ambiguity is lying to
the agent). A table card is a RENDERING of the compiled facts row
(``compiler/facts.py``) — the same row the console's Table Profile
renders — so what the agent reasons from and what the steward sees on
screen are one number by construction. Table cards budget to ≤3K
tokens with a fixed drop order — vocabulary → column long-tail →
joins → filters → metrics — and NEVER drop grain, conflicts, or
access.
"""

from __future__ import annotations

from typing import Any

TOKEN_BUDGET_TABLE = 3000
_PROTECTED = ("grain", "conflicts", "access")


def _tokens_of(text: str) -> int:
    return max(1, len(text) // 4)          # the classic 4-chars/token bound


def _bytes(n: int | None) -> str:
    if n is None:
        return "?"
    for unit, size in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= size:
            return f"{n / size:.1f} {unit}"
    return f"{n} B"


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
        label = entry["code"] + (f": {entry['name']}"
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
            label += f": {entry['name']}"
        if entry.get("parent"):
            label += f" ({entry['parent']})"
        rendered.append(f"{label} · {entry['support']}")
    more = len(usage_info) - 5
    return ("- used by: " + " · ".join(rendered)
            + (f" · +{more} more" if more > 0 else "")
            + " [prov:used_by·catalog_mined]")


def _owner_line(owners: list[dict[str, Any]]) -> str:
    """The whole ownership chain with witnesses — `own_a@corp
    (business_owner; atlas+lumi)`. The FIRST entry stays the business
    owner so `- owner: <name> · …` keeps its shape for shelf readers."""
    if not owners:
        return "- owner: ? [prov:owned_by]"
    ranked = sorted(owners, key=lambda o: (
        0 if "business_owner" in o.get("roles", []) else
        1 if any("owner" in r for r in o.get("roles", [])) else 2,
        o["owner"]))
    parts = []
    for o in ranked[:4]:
        roles = "/".join(o.get("roles") or ["owner"])
        witnesses = "+".join(o.get("witnesses") or [])
        parts.append(f"{o['owner']} ({roles}; {witnesses})"
                     if witnesses else f"{o['owner']} ({roles})")
    return "- owner: " + " · ".join(parts) + " [prov:owned_by]"


def _column_line(c: dict[str, Any]) -> str:
    """Everything the graph knows about one column, on one line, in a
    fixed order: name, type, structural markers, business name,
    meaning, texture, links. Nothing here is prose the agent must
    parse twice — markers are tokens."""
    markers = []
    if c.get("primary_key"):
        markers.append("PK")
    elif c.get("primary_key_atlas"):
        markers.append("PK(atlas)")
    if c.get("partitioning"):
        markers.append("PARTITION")
    elif c.get("partitioning_atlas"):
        markers.append("PARTITION(atlas)")
    if c.get("sensitive"):
        role = "/".join(x for x in (c.get("pii_role"),
                                    c.get("sde_group")) if x)
        markers.append(f"SENSITIVE{' ' + role if role else ''}")
        if c.get("pii_role_table_declared"):
            markers.append(
                f"table-declared role {c['pii_role_table_declared']} "
                "disagrees")
    if c.get("ungoverned"):
        markers.append("ungoverned, no business meaning on record")
    if c.get("nullable_atlas") is False:
        markers.append("NOT NULL(atlas)")
    head = f"- {c['name']} {str(c.get('type') or '').lower()}".rstrip()
    if markers:
        head += " (" + "; ".join(markers) + ")"
    parts = [head]
    if c.get("business_name"):
        parts.append(f"“{c['business_name']}”")
    meaning = c.get("description", "")
    if c.get("description_supplementary"):
        meaning = (f"{meaning} | lumi: {c['description_supplementary']}"
                   if meaning else f"lumi: {c['description_supplementary']}")
    if meaning:
        parts.append(meaning)
    texture = []
    if c.get("approx_distinct") is not None:
        texture.append(f"~{c['approx_distinct']} distinct")
    if c.get("null_count") is not None:
        texture.append(f"{c['null_count']} null")
    if c.get("column_length") is not None:
        texture.append(f"len {c['column_length']}")
    if texture:
        parts.append(", ".join(texture))
    domain = c.get("domain")
    if domain:
        top = ", ".join(
            f"{v.get('value')} {v.get('pct')}%" if v.get("pct") is not None
            else str(v.get("value")) for v in domain.get("top", []))
        parts.append(f"{domain['n_values']} known values"
                     + (f" ({top})" if top else "")
                     + " → sample_values")
    for term in c.get("terms", [])[:2]:
        line = f"term: {term.get('name', '?')}"
        if term.get("status"):
            line += f" [{term['status']}]"
        if term.get("description"):
            line += f" — {term['description']}"
        parts.append(line)
    for ref in c.get("fk_references", []):
        parts.append(f"FK → {ref['table']}.{ref['column']}")
    if c.get("derived_logic"):
        parts.append(f"computed: `{c['derived_logic'][:80]}`")
    for src in c.get("derived_from", [])[:2]:
        parts.append(f"derived from {src.get('source')}")
    if c.get("column_name_atlas"):
        parts.append(f"atlas spells it {c['column_name_atlas']}")
    if (c.get("ordinal") is not None and c.get("ordinal_atlas") is not None
            and c["ordinal"] != c["ordinal_atlas"]):
        parts.append(f"ordinal bq {c['ordinal']} vs atlas "
                     f"{c['ordinal_atlas']}")
    prov = (f"[prov:{c.get('type_source') or 'bq'}"
            f"·agree={c.get('agreement', 1)}]")
    return " · ".join(parts) + " " + prov


def table_card(facts: dict[str, Any],
               metrics_here: list[dict[str, Any]],
               filters_here: list[dict[str, Any]]
               ) -> tuple[str, dict[str, Any]]:
    """→ (markdown, budget_report). ``facts`` is the compiled row from
    ``compiler/facts.py``; nothing here reaches back to the graph."""
    physical = facts["physical"]
    identity = facts.get("identity", {})
    business = facts.get("business", {})
    ops = facts.get("operations", {})
    trust = facts.get("trust", {})
    access = facts.get("access", {})
    joins = facts.get("joins", {})
    lineage = facts.get("lineage", {})
    prov = f"[prov:table:{physical}]"
    lines: dict[str, list[str]] = {k: [] for k in (
        "header", "purpose", "grain", "trust", "columns", "joins",
        "filters", "metrics", "lineage", "access", "vocabulary",
        "conflicts", "footer")}

    title = f"# table {physical}"
    if identity.get("business_name"):
        title += f" — {identity['business_name']}"
    lifecycle = ops.get("lifecycle") or "unknown"
    rows = ops.get("total_rows")
    lines["header"] = [
        title,
        f"- object: {identity.get('object_type', 'TABLE')} · "
        f"rows ≈ {rows if rows is not None else '?'} · "
        f"lifecycle: {lifecycle} {prov}",
        _owner_line(business.get("owners", [])),
    ]
    category = " › ".join(x for x in (identity.get("data_category"),
                                      identity.get("data_sub_category"))
                          if x)
    bu_bits = [
        f"business unit: {business.get('business_unit') or '?'}"
        + (" (MDM pipeline)" if business.get("business_unit") else ""),
        f"category: {category}" if category else "",
        f"layer: {identity.get('layer_type') or '?'}",
        f"type: {identity['table_type']}" if identity.get("table_type")
        else "",
    ]
    lines["header"].append(
        "- " + " · ".join(b for b in bu_bits if b) + " [prov:lumi+atlas]")
    if business.get("lobs"):
        lines["header"].append(_lob_line(business["lobs"]))
    if business.get("used_by"):
        lines["header"].append(_usage_line(business["used_by"]))
    if identity.get("description"):
        lines["purpose"] = [
            f"- purpose: {identity['description']} "
            f"[prov:{identity.get('description_source') or 'atlas'}]"]
        if identity.get("description_bq"):
            lines["purpose"].append(
                f"- bq describes it: {identity['description_bq']} "
                "[prov:bq]")
    where = ".".join(x for x in (identity.get("project"),
                                 identity.get("dataset"),
                                 physical.split(".")[-1]) if x)
    where_bits = [f"lives at: {where}"]
    if identity.get("technology") or identity.get("data_server"):
        where_bits.append(
            " via ".join(x for x in (identity.get("technology"),
                                     identity.get("data_server")) if x))
    if identity.get("appl_id"):
        where_bits.append(f"registry appl_id {identity['appl_id']}")
    if identity.get("target_system"):
        where_bits.append(f"target {identity['target_system']}")
    lines["purpose"].append("- " + " · ".join(where_bits)
                            + " [prov:bq+atlas]")

    # ── grain: protected, never dropped ──
    pk = facts.get("primary_key") or []
    pk_atlas = ops.get("primary_key_atlas") or []
    grain_pk = ("- primary key: " + ", ".join(pk)
                + " [prov:bq:constraints]") if pk else \
        ("- primary key: none declared in BigQuery"
         + (f" · atlas marks {', '.join(pk_atlas)}" if pk_atlas else "")
         + " [prov:bq+atlas]")
    if pk and pk_atlas and set(pk) != set(pk_atlas):
        grain_pk += f" · atlas marks {', '.join(pk_atlas)} [prov:atlas]"
    part_cols = ops.get("partition_columns") or \
        ops.get("partition_columns_atlas") or []
    part_bits = [
        ("partitioned by " + ", ".join(part_cols)) if part_cols else
        ("partitioned" if identity.get("is_partitioned_atlas")
         else "not partitioned" if identity.get("is_partitioned_atlas")
         is False else "partitioning unknown"),
        f"latest {ops.get('partition_latest') or 'n/a'}",
    ]
    if ops.get("n_partitions") is not None:
        part_bits.append(f"{ops['n_partitions']} partitions")
    if identity.get("load_type"):
        part_bits.append(f"load: {identity['load_type']}")
    lines["grain"] = [
        "## grain", grain_pk,
        "- " + " · ".join(part_bits) + " [prov:bq+atlas]",
        f"- rows ≈ {rows if rows is not None else '?'} · "
        f"{_bytes(ops.get('size_bytes'))} · schema "
        f"{identity.get('schema_fingerprint') or '?'} {prov}",
    ]

    # ── trust & operations ──
    trust_lines = ["## trust & operations"]
    if trust.get("answerability"):
        trust_lines.append(
            "- answerability: " + " · ".join(
                f"{k} {v}" for k, v in sorted(
                    trust["answerability"].items()))
            + " [prov:lumi]")
    when = [f"last modified {ops['last_modified']}"
            if ops.get("last_modified") else "",
            f"created {ops['created']}" if ops.get("created") else "",
            (f"feed {ops.get('feed_type')}"
             + (f" ({ops['pipeline_name']}" if ops.get("pipeline_name")
                else "")
             + (f" from {ops['source_system']}"
                if ops.get("source_system") else "")
             + (")" if ops.get("pipeline_name") else ""))
            if ops.get("feed_type") or ops.get("pipeline_name") else "",
            f"env {ops['environment']}" if ops.get("environment") else ""]
    if any(when):
        trust_lines.append("- " + " · ".join(w for w in when if w)
                           + " [prov:lumi+bq]")
    cost = ops.get("cost_prior") or {}
    if cost:
        trust_lines.append(
            f"- cost prior: p50 {_bytes(cost.get('p50_bytes'))} · "
            f"p95 {_bytes(cost.get('p95_bytes'))} per query over "
            f"{cost.get('n_jobs', '?')} jobs [prov:jobs_30d]")
    if ops.get("usage_rhythm"):
        trust_lines.append("- usage rhythm: "
                           + " · ".join(ops["usage_rhythm"])
                           + " [prov:jobs_30d]")
    if business.get("top_users"):
        trust_lines.append(
            "- top users: " + " · ".join(
                f"{u.get('user')} ×{u.get('queries')}"
                for u in business["top_users"][:4])
            + " [prov:bq:history]")
    flags = [name for key, name in (("is_active_atlas", "active"),
                                    ("is_latest_atlas", "latest"),
                                    ("is_lineage_exist_atlas",
                                     "lineage declared"))
             if trust.get(key) is True]
    denied = [name for key, name in (("is_active_atlas", "INACTIVE"),
                                     ("is_latest_atlas", "not latest"))
              if trust.get(key) is False]
    if flags or denied:
        trust_lines.append("- atlas flags: "
                           + " · ".join(denied + flags) + " [prov:atlas]")
    lines["trust"] = trust_lines if len(trust_lines) > 1 else []

    # ── columns, in schema order ──
    ordered = sorted(facts.get("column_facts", []),
                     key=lambda c: (c.get("ordinal")
                                    if c.get("ordinal") is not None
                                    else 10 ** 6, c["name"]))
    lines["columns"] = ["## columns"] + [_column_line(c) for c in ordered]

    # ── joins: declared first (fiat beats every mined witness) ──
    join_lines = ["## joins"]
    for fk in joins.get("declared", []):
        join_lines.append(
            f"- declared: {fk['column']} → {fk['ref_table']}."
            f"{fk['ref_column']} [prov:bq:constraints]")
    for other, support in ((o["other"], o["support"])
                           for o in joins.get("observed", [])[:8]):
        join_lines.append(f"- observed: {other} · {support} co-queries "
                          "[prov:bq:history]")
    for j in joins.get("scoped", [])[:4]:
        # a scoped_only witness means the equality was observed between
        # TRANSFORMED CTEs — the relationship exists; the raw tables do
        # NOT join safely without the stated preparation
        caveat = ("" if j.get("scope") == "raw_safe"
                  else " (CTE-scoped, NOT raw-safe)"
                  + (f"; requires: {'; '.join(j['preconditions'][:2])}"
                     if j.get("preconditions") else ""))
        join_lines.append(
            f"- {j['other']} ON {' AND '.join(j.get('on') or ['?'])} · "
            f"{j.get('join_type') or 'JOIN'} · {j.get('scope')}{caveat} "
            f"[prov:{j.get('witness') or 'studio'}]")
    lines["joins"] = join_lines
    lines["filters"] = ["## common filters"] + [
        f"- {f['label']}: `{f['sql']}` · support {f['support']} "
        f"[prov:{f['source']}]" for f in filters_here[:8]]
    lines["metrics"] = ["## metrics available"] + [
        f"- {m['label'] or m['id']} · "
        f"{m.get('status_served') or m['status']} [prov:{m['source']}]"
        for m in metrics_here[:10]]

    lineage_bits = []
    if lineage.get("upstream"):
        lineage_bits.append("upstream: " + ", ".join(lineage["upstream"]))
    if lineage.get("downstream"):
        lineage_bits.append("downstream: "
                            + ", ".join(lineage["downstream"]))
    if lineage.get("derived_columns"):
        lineage_bits.append(
            f"{len(lineage['derived_columns'])} computed column(s): "
            + ", ".join(lineage["derived_columns"][:4]))
    if lineage.get("view_sql"):
        lineage_bits.append("view definition retained (doc)")
    if lineage_bits:
        lines["lineage"] = ["## lineage",
                            "- " + " · ".join(lineage_bits)
                            + " [prov:lumi+bq]"]

    # ── access: protected ──
    access_lines = []
    restricted = access.get("restricted")
    if restricted == "unknown_policy":
        access_lines.append(
            "- ⚠ row-access policy UNKNOWN (listing denied): "
            "live execution DENIED until resolved [prov:bq]")
    elif restricted:
        access_lines.append(f"- row-access policy: {restricted} [prov:bq]")
    flag_bits = []
    for key, label in (("has_pii_atlas", "PII"),
                       ("has_gdpr_atlas", "GDPR"),
                       ("has_oncop_atlas", "ONCOP")):
        if access.get(key) is True:
            flag_bits.append(f"{label} yes")
        elif access.get(key) is False:
            flag_bits.append(f"{label} no")
    policies = access.get("policies") or {}
    if flag_bits or policies:
        pol = ", ".join(f"{p} ({'+'.join(w)})"
                        for p, w in sorted(policies.items()))
        access_lines.append(
            "- table flags: " + " · ".join(flag_bits)
            + (f" · policies: {pol}" if pol else "") + " [prov:atlas]")
    if access.get("sensitive_columns"):
        access_lines.append(
            "- sensitive columns: " + ", ".join(
                c["name"] + (" (" + "/".join(
                    x for x in (c.get("pii_role"), c.get("sde_group"))
                    if x) + ")" if (c.get("pii_role") or c.get("sde_group"))
                    else "")
                for c in access["sensitive_columns"])
            + " [prov:union_most_restrictive]")
    lines["access"] = ["## access"] + (access_lines
                                       or ["- no known restrictions"])

    vocab = facts.get("vocabulary") or []
    if vocab:
        scope = ", ".join(business.get("business_units") or []) or "All"
        rows_v = []
        for v in vocab:
            meaning = v.get("definition") or "(no definition on record)"
            if v.get("kind") == "term":
                meaning += (f" [{v['status']}]" if v.get("status")
                            else "") + " — Atlas business term"
            scope_v = str(v.get("bu") or "All")
            if v.get("region") and str(v["region"]).lower() != "all":
                scope_v += f"/{v['region']}"
            rows_v.append(
                f"- {v['symbol']} = {meaning} ({scope_v}) → "
                + ", ".join(v.get("columns", []))
                + (" [prov:atlas]" if v.get("kind") == "term"
                   else " [prov:glossary]"))
        lines["vocabulary"] = [
            f"## vocabulary (scoped to {scope}, All)"] + rows_v

    conflicts = []
    for name, count in sorted((trust.get("structural") or {}).items()):
        if count:
            conflicts.append(f"- {name}: {count}, see structural census")
    for c in ordered:
        for flag in c.get("flags", []):
            conflicts.append(f"- {c['name']}: {flag}")
    if facts.get("omitted_catalog_only"):
        conflicts.append(
            "- omitted catalog-only columns (D1): "
            + ", ".join(facts["omitted_catalog_only"]))
    lines["conflicts"] = ["## conflicts"] + (conflicts or ["- none"])
    lines["footer"] = [f"[prov: consensus of bq+lumi+atlas via "
                       f"merge_policy, every line traceable] {prov}"]

    # ── budgeter: fixed drop order, protected sections never dropped ──
    drop_order = [("vocabulary", 6), ("columns", 12), ("joins", 4),
                  ("filters", 3), ("metrics", 6)]
    dropped: dict[str, int] = {}

    def render() -> str:
        order = ("header", "purpose", "grain", "trust", "columns",
                 "joins", "filters", "metrics", "lineage", "access",
                 "vocabulary", "conflicts", "footer")
        return "\n".join(x for key in order for x in lines[key])

    text = render()
    for section, keep in drop_order:
        if _tokens_of(text) <= TOKEN_BUDGET_TABLE:
            break
        if not lines[section]:
            continue
        header, rows_ = lines[section][0], lines[section][1:]
        if len(rows_) > keep:
            dropped[section] = len(rows_) - keep
            lines[section] = [header] + rows_[:keep] + [
                f"- … {len(rows_) - keep} more (budget-dropped; "
                "full detail in the truth graph)"]
        text = render()
    assert all(lines[p] for p in _PROTECTED)
    return text, {"tokens": _tokens_of(text),
                  "over_budget": _tokens_of(text) > TOKEN_BUDGET_TABLE,
                  "dropped": dropped}


def lob_card(lob: dict[str, Any]) -> str:
    """The business-unit card — what an agent reads FIRST when a
    question names a unit ("GMNS spend", "SBS approvals") so it can
    orient on the unit as a thing: its tables as a shelf, who runs
    queries on them, the ownership chain, and how much of the unit's
    world is witnessed. Every line is a compiled fact."""
    code = lob.get("code", "?")
    kind = lob.get("kind", "lob")
    title = f"# {'org unit' if kind == 'org_unit' else 'business unit'} "
    title += code + (f" — {lob['name']}" if lob.get("name") else "")
    lines = [title]
    head = [f"kind: {kind}"]
    if lob.get("parent"):
        head.append(f"parent LOB: {lob['parent']}")
    if lob.get("domains"):
        head.append("metric domains: " + ", ".join(lob["domains"]))
    lines.append("- " + " · ".join(head) + " [prov:lob_map]")
    ready = lob.get("readiness") or {}
    if ready:
        lines.append(
            f"- readiness: {ready['witnessed']} of {ready['tables']} "
            f"tables carry a witnessed metric ({ready['pct']}%) "
            "[prov:compiled]")
    else:
        lines.append("- readiness: no steward-mapped tables "
                     "(usage-only unit) [prov:lob_map]")
    if lob.get("usage_support"):
        lines.append(
            f"- usage: {lob['usage_support']} mined patterns run by this "
            f"unit across {len(lob.get('used_tables', []))} table(s) "
            "[prov:used_by·catalog_mined]")
    if lob.get("vocabulary_entries"):
        lines.append(
            f"- vocabulary: {lob['vocabulary_entries']} Acropedia "
            f"entries scoped to {code} → search_semantics(kind=vocab) "
            "[prov:glossary]")
    lines.append("## tables")
    if not lob.get("tables"):
        lines.append("- none steward-mapped: read the used tables below")
    for t in lob.get("tables", []):
        bits = [t["physical"]]
        if t.get("business_name"):
            bits[0] += f" — {t['business_name']}"
        if t.get("description"):
            bits.append(t["description"])
        bits.append(f"{t.get('metrics_here', 0)} metrics")
        if t.get("lifecycle"):
            bits.append(t["lifecycle"])
        if t.get("pii"):
            bits.append("PII")
        if t.get("business_unit") and t["business_unit"] != code:
            bits.append(f"MDM unit {t['business_unit']}")
        bits.append(f'read_card("table:{t["physical"]}")')
        lines.append("- " + " · ".join(bits) + " [prov:in_lob]")
    if lob.get("used_tables"):
        lines.append("## queries these tables")
        for physical in lob["used_tables"]:
            lines.append(f"- {physical} [prov:used_by·catalog_mined]")
    lines.append("## owners")
    if lob.get("owners"):
        for o in lob["owners"]:
            lines.append(f"- {o['owner']} ({'/'.join(o['roles'])}) "
                         "[prov:owned_by]")
    else:
        lines.append("- none on record")
    return "\n".join(lines)


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
                     + " (full SQL retained as evidence) [prov:studio]")
    if metric.get("tables_associated_not_referenced"):
        lines.append(
            "- ⚠ associated but NOT referenced by the SQL: "
            + ", ".join(metric["tables_associated_not_referenced"])
            + " (declared lineage the query never reads) [prov:studio]")
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
                f"[prov:{variant['source']}] "
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
        f"- {n_classes} distinct classes: resolver will ask below margin"
        if n_classes > 1 else "- none")
    return "\n".join(lines)
