"""resolve() — the compiled ranking function, exposed as a tool (E5/E6).

The agent doesn't decide what "consumer" means; this does — or it
refuses to. Pipeline (pinned): tokenize → vocab tiers (exact > scoped
acronym > prefix > fuzzy) → candidates per slot (metrics, concepts,
tables) → **lexicographic sort (authority_tier, score_rest)** where

    score_rest = 0.4·log1p(support)/log1p(max_support)
               + 0.3·recency_decay(last_seen, half-life 90d)
               + 0.3·context_fit

Support can never outvote certification — that is the lattice made
literal. Margin: computed on score_rest when the top two tie on tier
(m = 1.0 on a strict tier win); m < margin_threshold OR a top candidate
reached only via fuzzy ⇒ NO bind — a structured ambiguity with named
options instead (never argmax; disambiguation is a success state).

Confidence per slot = tier_ceiling × (0.5 + 0.5·min(m/0.3, 1));
overall = min over bound slots. Every slot carries its ``features``
{tier, support_score, recency, context_fit, margin} + the build's
``constants_version`` (E6) — a wrong bind is a readable trace.
"""

from __future__ import annotations

import datetime as _dt
import difflib
import math
from typing import Any

from sahs.evals.grading import SutAnswer
from sahs.evals.schema import Task
from sahs.tools.api import Build, _tokens
from sahs.tools.constants import RESOLVER_CONSTANTS


def _parse_date(value: str) -> "_dt.date | None":
    try:
        return _dt.date.fromisoformat((value or "")[:10])
    except ValueError:
        return None


def _recency(last_seen: str, half_life_days: int,
             reference: "_dt.date | None") -> float:
    """DATA-relative decay: age is measured against the build's own
    newest last_seen, not the wall clock — deterministic forever, and a
    build never 'goes stale' by merely being re-scored later."""
    seen = _parse_date(last_seen)
    if seen is None or reference is None:
        return 0.0
    age = max((reference - seen).days, 0)
    return 0.5 ** (age / half_life_days)


def _score_rest(support: int, max_support: int, last_seen: str,
                context_fit: float, constants: dict,
                reference: "_dt.date | None" = None) -> dict[str, float]:
    weights = constants["weights"]
    support_score = (math.log1p(max(support, 1))
                     / math.log1p(max(max_support, 2)))
    recency = _recency(last_seen, constants["recency_half_life_days"],
                       reference)
    return {
        "support_score": round(support_score, 4),
        "recency": round(recency, 4),
        "context_fit": round(context_fit, 4),
        "rest": round(weights["support"] * support_score
                      + weights["recency"] * recency
                      + weights["context_fit"] * context_fit, 4),
    }


def _slot_result(candidates: list[dict[str, Any]], constants: dict
                 ) -> dict[str, Any]:
    """candidates: [{id, label, table, tier, rest{...}, via, prov}] →
    bind | ambiguity | empty."""
    if not candidates:
        return {"bound": None, "ambiguity": None, "confidence": 0.0,
                "features": None}
    ranked = sorted(candidates,
                    key=lambda c: (-c["tier"], -c["rest"]["rest"],
                                   c["id"]))
    top = ranked[0]
    if len(ranked) == 1:
        margin = 1.0
    elif top["tier"] > ranked[1]["tier"]:
        margin = 1.0
    else:
        margin = round(top["rest"]["rest"] - ranked[1]["rest"]["rest"], 4)
    fuzzy_only = top["via"] == "fuzzy" and constants[
        "fuzzy_reach_forces_ask"]
    features = {"tier": top["tier"], **top["rest"], "margin": margin,
                "via": top["via"]}
    # E12/E6 exposure ONLY — witness features are readable in every
    # trace but NOT in the score. Never change the ranker and the
    # measurement in the same week: constants move on floor-triage
    # evidence (A2), not on the excitement of new features.
    for extra in ("witness_agreement", "recency_source"):
        if extra in top:
            features[extra] = top[extra]
    if margin < constants["margin_threshold"] or fuzzy_only:
        # when the catalog wrote its own disambiguation guidance
        # ("clarify whether they want the submitted-country-code
        # method or…"), the ask carries it — the steward's words, not
        # a generated question
        options = [{
            "id": c["id"], "label": c["option_label"],
            "why": f"tier={c['tier']} rest={c['rest']['rest']}",
            "prov": c["prov"],
            **({"guidance": c["description"][:240]}
               if c.get("description") else {}),
        } for c in ranked[:4]]
        return {"bound": None, "confidence": 0.0, "features": features,
                "ambiguity": {"options": options,
                              "question": "Which do you mean?"}}
    ceiling = constants["tier_ceiling"][str(top["tier"])]
    confidence = round(ceiling * (0.5 + 0.5 * min(margin / 0.3, 1.0)), 4)
    return {"bound": top, "ambiguity": None, "confidence": confidence,
            "features": features}


def resolve(build: Build, question: str,
            context: dict[str, Any] | None = None) -> dict[str, Any]:
    """STRUCTURED resolution: {metrics[], concepts[], tables[], joins[],
    ambiguities[], confidence, features_by_slot, constants_version}.
    Deterministic core — zero LLM."""
    constants = build.manifest.get("resolver_constants",
                                   RESOLVER_CONSTANTS)
    context = context or {}
    tokens = _tokens(question)
    question_lower = " ".join((question or "").lower().split())

    # vocab expansion: scoped acronyms whose symbol appears as a token
    expanded = set(tokens)
    acronym_hits = []
    for row in build.vocab:
        if row["kind"] != "acronym":
            continue
        symbol = row["text"].lower()
        if symbol in (question or "").lower().split() \
                or symbol in tokens:
            bu = (context.get("bu") or "").lower()
            # real acropedia rows carry comma-joined multi-BU scopes
            # ("GMNS,Technology") — an acronym matches when ANY of its
            # scopes matches the question's bu context
            scopes = {b.strip() for b in
                      str(row.get("bu", "all")).lower().split(",")}
            if "all" in scopes or not bu or bu in scopes:
                expanded |= _tokens(row.get("definition", ""))
                acronym_hits.append(row)

    max_metric_support = max((m["support"] for m in build.metrics),
                             default=1)
    max_binding_support = max((b["support"] for b in build.bindings),
                              default=1)
    reference = max(
        (d for row in build.metrics + build.bindings
         for d in [_parse_date(row.get("last_seen", ""))]
         if d is not None), default=None)
    allowed = {t.lower() for t in context.get("tables_allowed", [])}

    # ── metric slot ──
    metric_candidates = []
    for row in build.metrics:
        label_tokens = _tokens(row["label"])
        question_tokens = _tokens(row.get("question", ""))
        overlap = len(expanded & label_tokens)
        q_overlap = len(expanded & question_tokens)
        exact_question = (row.get("question", "").lower().strip("?! .")
                          in question_lower) and bool(row.get("question"))
        full_label = bool(label_tokens) and label_tokens <= expanded
        # candidacy gate: one generic token ("spend") is noise, not a
        # candidate — relevance gates entry; trust ranks entrants
        if not (exact_question or full_label or overlap >= 2
                or q_overlap >= 2):
            continue
        via = "exact" if exact_question or full_label else "token"
        fit = 0.0
        if allowed and row["table"].lower() in allowed:
            fit += 0.6
        if exact_question:
            fit += 0.4
        fit = min(fit + min(q_overlap, 4) * 0.1, 1.0)
        metric_candidates.append({
            "id": row["id"], "label": row["label"], "table": row["table"],
            "tier": row["authority"],
            "rest": _score_rest(row["support"], max_metric_support,
                                row.get("last_seen", ""), fit, constants,
                                reference),
            "via": via,
            "prov": row["source"],
            "option_label": f"{row['label']}@{row['table']}"
                            f"::{row['canonical_sql'][:60]}",
            "mgroup": row.get("mgroup", ""),
            "mgroups": row.get("mgroups", [row.get("mgroup", "")]),
            "witness_agreement": row.get("witness_agreement", 1),
            "recency_source": row.get("recency_source", ""),
            "line_of_business": row.get("line_of_business", ""),
            "description": row.get("description", ""),
            "exact_q": exact_question,
        })
    # a curated DMP question matching VERBATIM is definitional — prune
    # token-overlap rivals rather than letting them force a fake tie
    if any(c["exact_q"] for c in metric_candidates):
        metric_candidates = [c for c in metric_candidates if c["exact_q"]]
    metric_slot = _slot_result(metric_candidates, constants)

    # ── concept slot(s): each label with token overlap ──
    concept_slots: dict[str, dict[str, Any]] = {}
    labels = {}
    for row in build.bindings:
        label_tokens = _tokens(row["label"])
        if label_tokens and label_tokens <= expanded:
            labels.setdefault(row["label"], []).append(row)
    for label, rows in sorted(labels.items()):
        candidates = [{
            "id": f"pred:{r['fp']}", "label": label, "table": r["table"],
            "tier": r["authority"],
            "rest": _score_rest(r["support"], max_binding_support,
                                r.get("last_seen", ""),
                                0.6 if allowed and r["table"].lower()
                                in allowed else 0.0, constants,
                                reference),
            "via": "exact",
            "prov": r["source"],
            "option_label": f"{label}::{r['canonical_sql']}",
            "witness_agreement": r.get("witness_agreement", 1),
            "recency_source": r.get("recency_source", ""),
        } for r in rows]
        concept_slots[label] = _slot_result(candidates, constants)

    # ── table slot: explicit mentions + implied by bound slots ──
    table_candidates = []
    table_universe = set(build.schema)
    if metric_slot["bound"] is not None:
        table_universe.add(metric_slot["bound"]["table"])
    for slot in concept_slots.values():
        if slot["bound"] is not None:
            table_universe.add(slot["bound"]["table"])
    for physical in sorted(table_universe):
        short = build.short_table(physical)
        via = None
        if short in expanded or short in question_lower:
            via = "exact"
        elif difflib.get_close_matches(
                short, list(expanded), 1, cutoff=0.85):
            via = "fuzzy"
        implied = (metric_slot["bound"] is not None
                   and metric_slot["bound"]["table"] == physical) or any(
            s["bound"] is not None and s["bound"]["table"] == physical
            for s in concept_slots.values())
        if via is None and not implied:
            continue
        fit = 0.6 if implied else 0.0
        if allowed and physical.lower() in allowed:
            fit += 0.4
        table_candidates.append({
            "id": f"table:{physical}", "label": short, "table": physical,
            "tier": 5 if implied else 3,
            "rest": _score_rest(1, 2, "", min(fit, 1.0), constants),
            "via": via or "implied", "prov": "build",
            "option_label": physical,
        })
    bound_tables = sorted({c["table"] for c in table_candidates})

    ambiguities = []
    if metric_slot["ambiguity"]:
        ambiguities.append({"slot": "metric", **metric_slot["ambiguity"]})
    for label, slot in concept_slots.items():
        if slot["ambiguity"]:
            ambiguities.append({"slot": f"concept:{label}",
                                **slot["ambiguity"]})

    joins = [j for j in build.joins
             if j["a"] in bound_tables and j["b"] in bound_tables]

    confidences = [metric_slot["confidence"]] if metric_slot["bound"] \
        else []
    confidences += [s["confidence"] for s in concept_slots.values()
                    if s["bound"]]
    overall = (0.0 if ambiguities else
               round(min(confidences), 4) if confidences else 0.0)

    return {
        "metrics": ([{"id": metric_slot["bound"]["id"],
                      "mgroup": metric_slot["bound"]["mgroup"],
                      "mgroups": metric_slot["bound"]["mgroups"],
                      "label": metric_slot["bound"]["label"],
                      "table": metric_slot["bound"]["table"],
                      # exposure only (E6) — a hint the agent reads,
                      # never a feature the ranker scores
                      "line_of_business":
                          metric_slot["bound"]["line_of_business"],
                      "confidence": metric_slot["confidence"]}]
                    if metric_slot["bound"] else []),
        "concepts": [{"label": label,
                      "binding": slot["bound"]["option_label"]
                      .split("::", 1)[1],
                      "table": slot["bound"]["table"],
                      "confidence": slot["confidence"]}
                     for label, slot in concept_slots.items()
                     if slot["bound"]],
        "tables": bound_tables,
        "joins": joins,
        "ambiguities": ambiguities,
        "confidence": overall,
        "features_by_slot": {
            "metric": metric_slot["features"],
            **{f"concept:{label}": slot["features"]
               for label, slot in concept_slots.items()},
        },
        "acronyms_expanded": [r["text"] for r in acronym_hits],
        "constants_version": constants["version"],
        "build": build.version,
    }


def resolver_sut(build: Build):
    """The resolver as a harness SUT (kind=resolve_bind /
    disambiguate) — the deterministic floor gets measured, not claimed."""

    def _sut(task: Task) -> SutAnswer:
        result = resolve(build, task.prompt, {
            "tables_allowed": task.context.tables_allowed,
            "bu": task.context.bu, "region": task.context.region})
        if result["ambiguities"]:
            options = [o["label"]
                       for a in result["ambiguities"]
                       for o in a["options"]]
            return SutAnswer(kind="disambiguate", options=options)
        bindings: dict[str, list[str]] = {}
        if result["metrics"]:
            top = result["metrics"][0]
            # bind the PRIMARY identity: a fused metric answers as its
            # certified name, not as every catalog that corroborates it
            primary = top.get("mgroup") or sorted(top["mgroups"])[0]
            bindings["metrics"] = [primary.split(":", 1)[1]]
            bindings["tables"] = [top["table"]]
        elif result["tables"]:
            bindings["tables"] = result["tables"]
        if not bindings:
            return SutAnswer(kind="abstain", reason="nothing_resolved")
        return SutAnswer(kind="bindings", bindings=bindings)

    # the deterministic resolver BINDS — it never generates SQL. The
    # harness measures it on binding kinds only; nl2sql gold stays the
    # ground for generation-capable SUTs.
    _sut.answerable_kinds = {"resolve_bind", "disambiguate", "abstain"}
    return _sut
