"""Delta resolution (E18): the deterministic step that must never sit
behind a spinner.

Two pins live here:
  * **delta, not full re-plan.** A mutation re-resolves only the slots
    the edit touched. Changing a country filter does not re-resolve
    the metric — that is why "same for Canada" is instant.
  * **one clarifying question per turn.** Blockers are ranked (metric
    → grain → filter binding) and the FIRST one stops the turn with
    chips carrying evidence. Below-margin is a success state, not a
    failure: the resolver never argmaxes (E5/E6).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Any

from sahs.tools.api import Build, search_concepts
from sahs.tools.resolver import resolve as resolve_question

from .plan import Plan

MAX_CHIPS = 4


@dataclass
class ResolveOutcome:
    plan: Plan
    clarify: dict[str, Any] | None
    result: dict[str, Any]


def _metric_row(build: Build, metric_id: str) -> dict[str, Any]:
    for row in build.metrics:
        if row.get("id") == metric_id:
            return row
    return {}


def _grain_evidence(build: Build, table: str) -> list[str]:
    """Grains OTHER metrics on this table actually declare — real
    evidence, never a guessed vocabulary."""
    seen: list[str] = []
    for row in build.metrics:
        if row.get("table") != table:
            continue
        grain = (row.get("grain") or row.get("grain_observed") or "").strip()
        if grain and grain not in seen:
            seen.append(grain)
    return seen[:3]


def _chips(slot: str, question: str,
           options: list[dict[str, Any]]) -> dict[str, Any]:
    return {"slot": slot, "question": question,
            "options": options[:MAX_CHIPS]}


def resolve_plan(build: Build, plan: Plan, *, touched: list[str],
                 context: dict[str, Any] | None = None) -> ResolveOutcome:
    started = time.perf_counter()
    result: dict[str, Any] = {"slots_resolved": [], "candidate_node_ids": [],
                              "confidence": None, "features": {}}
    clarify: dict[str, Any] | None = None
    needs_metric = "metric" in touched or not plan.metric_id

    # ── 1. the metric (the only slot the resolver ranks) ─────
    if needs_metric:
        raw = resolve_question(build, plan.question, context or {})
        result["confidence"] = raw.get("confidence")
        result["features"] = raw.get("features_by_slot", {})
        result["acronyms_expanded"] = raw.get("acronyms_expanded", [])
        candidates = raw.get("metrics", [])
        result["candidates"] = candidates[:MAX_CHIPS]
        result["candidate_node_ids"] = [c["id"] for c in candidates[:12]]
        result["tables"] = raw.get("tables", [])
        ambiguity = next((a for a in raw.get("ambiguities", [])
                          if a.get("slot") == "metric"), None)
        if ambiguity:
            options = [{
                "value": opt.get("id"), "label": opt.get("label") or opt.get("id"),
                "why": opt.get("why", ""), "evidence": opt.get("prov", ""),
                "guidance": opt.get("guidance", ""),
            } for opt in ambiguity.get("options", [])]
            clarify = _chips("metric",
                             ambiguity.get("question")
                             or "Which metric do you mean?", options)
            return ResolveOutcome(plan, clarify, _stamp(result, started))
        if not candidates:
            clarify = _chips(
                "metric",
                "Nothing in the promoted build matches that yet. "
                "Which of these did you mean?",
                [{"value": row["id"], "label": row.get("label") or row["id"],
                  "why": f"on {row.get('table', '?')}",
                  "evidence": row.get("status_served", "")}
                 for row in build.metrics[:MAX_CHIPS]])
            return ResolveOutcome(plan, clarify, _stamp(result, started))
        top = candidates[0]
        row = _metric_row(build, top["id"])
        plan = replace(
            plan, metric_id=top["id"], metric_label=top.get("label", ""),
            metric_fp=row.get("fp", ""),
            metric_sql=row.get("canonical_sql", ""),
            table=top.get("table") or row.get("table", ""),
            lob=top.get("line_of_business") or plan.lob,
            provenance={**plan.provenance, "metric": "resolver"})
        result["slots_resolved"].append("metric")
        result["bound"] = {"id": plan.metric_id, "label": plan.metric_label,
                           "table": plan.table,
                           "confidence": top.get("confidence")}

    # ── 2. grain: required, so an unknown grain is a question ─
    if not plan.grain:
        row = _metric_row(build, plan.metric_id)
        grain = (row.get("grain") or row.get("grain_observed") or "").strip()
        if grain:
            plan = replace(plan, grain=grain,
                           provenance={**plan.provenance, "grain": "resolver"})
            result["slots_resolved"].append("grain")
        elif plan.dimensions:
            plan = replace(plan, grain=" × ".join(plan.dimensions),
                           provenance={**plan.provenance,
                                       "grain": "inherited"})
            result["slots_resolved"].append("grain")
        else:
            options = [{"value": g, "label": g,
                        "why": "declared by another metric on this table",
                        "evidence": plan.table}
                       for g in _grain_evidence(build, plan.table)]
            options.append({
                "value": "total", "label": "one row for the whole window",
                "why": "no grain on record for this metric",
                "evidence": "the answer will be a single total"})
            clarify = _chips("grain", "What should one row mean?", options)
            return ResolveOutcome(plan, clarify, _stamp(result, started))

    # ── 3. filters: each name binds to a real column/expression ─
    bindings = dict(plan.filter_bindings)
    for name in plan.filters:
        slot = f"filters.{name}"
        if name in bindings and slot not in touched:
            continue                      # remembered at scope
        found = search_concepts(build, name, table=plan.table, top_k=MAX_CHIPS)
        rows = found.get("bindings", [])
        if not rows:
            clarify = _chips(
                slot, f"Nothing in the build binds {name!r} on "
                      f"{build.short_table(plan.table)}. Drop it?",
                [{"value": "", "label": f"drop the {name} filter",
                  "why": "no binding on record", "evidence": found.get(
                      "hint", "")}])
            return ResolveOutcome(plan, clarify, _stamp(result, started))
        if len(rows) > 1 and rows[0].get("authority") == \
                rows[1].get("authority"):
            clarify = _chips(
                slot, f"Which {name} do you mean?",
                [{"value": r["sql"], "label": r.get("concept", name),
                  "why": r.get("sql", ""),
                  "evidence": f"{r.get('source', '?')} · support "
                              f"{r.get('support', 0)}"} for r in rows])
            return ResolveOutcome(plan, clarify, _stamp(result, started))
        bindings[name] = rows[0]["sql"]
        result["slots_resolved"].append(slot)
    plan = replace(plan, filter_bindings=bindings)

    return ResolveOutcome(plan, None, _stamp(result, started))


def _stamp(result: dict[str, Any], started: float) -> dict[str, Any]:
    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
    return result
