"""SYNAPSE.md — the world digest (Agent Loop v1 §4.2, ≤2K tokens).

Generated from the compiled build's indexes and NOTHING else: build
id, the top metrics with their grains, the concept bindings that
carry the most evidence, the join topology by tier, the conventions
that are actually observable in the build, and the known gaps stated
as counts. Hand-written prose has no entry point here — if a fact is
not derivable from the build, it does not appear (the reality law
applies to the model's briefing exactly as it applies to answers).

Deterministic per build by construction, so the assembled system
prompt is byte-identical across turns — which is what makes it
prompt-cacheable (§1's latency pin).
"""

from __future__ import annotations

from typing import Any

from sahs.tools.api import Build

MAX_CHARS = 7000          # ≈ 2K tokens, enforced by test


def _tier(row: dict[str, Any]) -> str:
    if row.get("source") == "constraints":
        return "certified"
    if row.get("on") and row.get("scope") != "scoped_only":
        return "witnessed"
    return "candidate"


def synapse_digest(build: Build,
                   list_hint: str = 'list_metrics("GMNS")',
                   search_hint: str = "search_semantics") -> str:
    lines: list[str] = [f"# SYNAPSE.md · build {build.version}", ""]

    # ── the shelf ────────────────────────────────────────────
    certified = [m for m in build.metrics
                 if (m.get("status_served") or m.get("status"))
                 == "certified"]
    lines.append(
        f"{len(build.schema)} tables · {len(build.metrics)} metrics "
        f"({len(certified)} certified) · {len(build.bindings)} concept "
        f"bindings · {len(build.joins)} join edges · "
        f"{len(build.vocab)} vocabulary entries.")
    lines.append("")

    # ── the business map: intent resolves here first ─────────
    lob_rows = list(getattr(build, "lob", []) or [])
    if lob_rows:
        from sahs.loop.tools import _metric_in_lob
        lines.append("## the business map")
        lines.append("Business words name these areas, never "
                     "tables. \"All GMNS metrics\" means "
                     + list_hint + ".")
        for row in lob_rows:
            members = sum(1 for m in build.metrics
                          if _metric_in_lob(m, row))
            domains = ", ".join(row.get("domains") or [])
            lines.append(
                f"- {row.get('code')} — {row.get('name')}"
                + (f" ({domains})" if domains else "")
                + f" · {members} metrics"
                + (f" · tables {', '.join(row.get('tables') or [])}"
                   if row.get("tables") else ""))
        lines.append("")

    # ── words: scoped acronyms, the common-word guard, value
    #    meanings — intent understanding before any table ──────
    acronyms = [v for v in build.vocab if v.get("kind") == "acronym"]
    observed = [d for d in (getattr(build, "domains", []) or [])
                if d.get("values")]
    if acronyms or getattr(build, "value_meanings", None) or observed:
        by_symbol: dict[str, int] = {}
        for v in acronyms:
            key = (v.get("text") or "").lower()
            by_symbol[key] = by_symbol.get(key, 0) + 1
        several = sum(1 for n in by_symbol.values() if n > 1)
        common = sorted({v.get("text", "") for v in acronyms
                         if v.get("common_word")})
        meanings = list(getattr(build, "value_meanings", []) or [])
        columns = {(m.get("table"), m.get("column")) for m in meanings}
        lines.append("## words")
        lines.append(
            f"Acronyms are scoped: the same symbol can mean different "
            f"things per business unit and region ({len(acronyms)} "
            f"acronym entries, {several} symbols with several "
            f"meanings); when the ask names a business area, prefer "
            f"that area's meaning, and say which one you used.")
        if common:
            shown = ", ".join(common[:8]) + (" …" if len(common) > 8
                                              else "")
            lines.append(
                f"{len(common)} acronyms are also ordinary words "
                f"({shown}): expand them only when the ask writes them "
                f"as acronyms — {search_hint}(kind=\"vocab\") shows "
                "the scope and the guard.")
        if meanings:
            lines.append(
                f"Stored codes have meanings on record for "
                f"{len(columns)} columns: {search_hint}(kind=\"values\") "
                "turns a phrase (\"KYC done\") into the code and the "
                "predicate to filter with; sample_values shows a "
                "column's codes with their meanings. Filter on the "
                "code, say the meaning.")
        if observed:
            estimated = sum(1 for d in observed
                            if d.get("distinct_estimate"))
            partial = sum(1 for d in observed
                          if (d.get("distinct_estimate") or 0)
                          > len(d.get("values") or []))
            lines.append(
                f"The profiler's observed values are on record for "
                f"{len(observed)} columns ({estimated} with an "
                f"estimated distinct count, {partial} of those lists "
                f"partial): {search_hint}(kind=\"values\") also "
                "matches a value written as stored (\"GB\", "
                "\"ACTIVE\") with its share of rows; sample_values "
                "says when a list is partial, so a literal outside it "
                "is a question, not an error.")
        lines.append("")

    # ── top metrics, certified first, by support ─────────────
    lines.append("## top metrics")
    ranked = sorted(build.metrics,
                    key=lambda m: (-(m.get("authority") or 0),
                                   -(m.get("support") or 0), m["id"]))
    for row in ranked[:12]:
        grain = (row.get("grain") or "").strip() or "grain unrecorded"
        lines.append(
            f"- {row.get('label') or row['id']} "
            f"[{row.get('status_served') or row.get('status')}] on "
            f"{row['table']} · {grain} · "
            f"`{(row.get('canonical_sql') or '')[:60]}`")
    lines.append("")

    # ── the concepts with the most evidence ──────────────────
    lines.append("## concepts")
    top_bindings = sorted(build.bindings,
                          key=lambda b: (-(b.get("support") or 0),
                                         b.get("fp", "")))[:8]
    for row in top_bindings:
        lines.append(f"- {row.get('label')} on {row['table']} · "
                     f"support {row.get('support', 0)} "
                     f"({row.get('source', '?')})")
    if len(build.vocab):
        lines.append(f"- plus {len(build.vocab)} vocabulary entries "
                     "(acronyms are scoped by business unit: "
                     "search_semantics serves them)")
    lines.append("")

    # ── join topology ────────────────────────────────────────
    lines.append("## join topology")
    seen: set[frozenset[str]] = set()
    rank = {"certified": 0, "witnessed": 1, "candidate": 2}
    edges = sorted(build.joins,
                   key=lambda j: (rank[_tier(j)],
                                  -(j.get("support") or 0)))
    for row in edges:
        pair = frozenset((row["a"], row["b"]))
        if pair in seen:
            continue
        seen.add(pair)
        lines.append(f"- {row['a']} ↔ {row['b']}: {_tier(row)}"
                     + (f" on `{row['on'][:50]}`" if row.get("on")
                        else "")
                     + f" · support {row.get('support', 0)}")
        if len(seen) >= 10:
            break
    if not seen:
        lines.append("- no join edges on record: answer from one "
                     "table")
    lines.append("")

    # ── conventions, all observable ──────────────────────────
    lines.append("## conventions")
    lines.append("- only certified metrics speak unqualified: "
                 "anything else carries its status and evidence "
                 "origin in the disclosure line")
    lines.append("- support counts distinct witnessed uses, never "
                 "executions; agreement counts witness families")
    lines.append("- sample_values serves compiled observations, "
                 "never live queries; the sandbox is the only door "
                 "to the warehouse")
    lines.append("")

    # ── known gaps, as counts ────────────────────────────────
    lines.append("## known gaps")
    grainless = sum(1 for m in build.metrics
                    if not (m.get("grain") or "").strip())
    bare_tables = [t for t in sorted(build.schema)
                   if not any(m.get("table") == t
                              for m in build.metrics)]
    unknown_policy = [t for t, entry in sorted(build.acl.items())
                      if entry.get("restricted") == "unknown_policy"]
    lines.append(f"- {grainless} metrics carry no recorded grain "
                 "(the contract will ask)")
    if bare_tables:
        lines.append(f"- {len(bare_tables)} tables have no metric on "
                     "record: " + ", ".join(bare_tables[:4]))
    if unknown_policy:
        lines.append(f"- {len(unknown_policy)} tables have UNKNOWN "
                     "row-access policy (live execution refuses "
                     "them): " + ", ".join(unknown_policy[:4]))
    if not bare_tables and not unknown_policy:
        lines.append("- none recorded beyond the grain gaps above")

    text = "\n".join(lines)
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS - 22] + "\n… digest truncated."
    return text
