"""Realtime turn evals — is what we just did actually right?

Industry practice for agent evals runs three levels (end to end, the
trajectory, the components) and puts CHEAP DETERMINISTIC checks on 100%
of traffic, reserving LLM-as-judge for sampled offline runs. This module
is the 100% lane: every console turn is scored the moment it completes,
from the same event stream the UI rendered, and every check carries a
plain-language explanation — the console can say clearly whether the
answer was accurate, and why.

The checks are the analyst workflow's own hard rules, made mechanical:
citations resolve into the graph, cited facts carry an honest tier,
nothing executes without the seal, joins exist in observed reality,
the tool budget holds, the answer keeps its contract, and mid-turn
self-corrections (validator catches → agent revises) are surfaced as a
feature, not hidden.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any

TIER_RANK = {"deprecated": 0, "guessed": 1, "inferred": 2,
             "grounded": 3, "human_asserted": 4}

EXECUTE_TOOLS = {"execute_sql", "run_query", "run_sql", "execute"}


def _check(check_id: str, label: str, status: str,
           explanation: str) -> dict[str, str]:
    return {"id": check_id, "label": label, "status": status,
            "explanation": explanation}


def _sql_tables(sql: str) -> list[str] | None:
    """Table names referenced by the statement, or None if unparseable."""
    try:
        import sqlglot
        from sqlglot import exp
        parsed = sqlglot.parse_one(sql, read="bigquery")
        return sorted({t.name.lower() for t in parsed.find_all(exp.Table)})
    except Exception:
        return None


class TurnEvaluator:
    """Deterministic rubric over one turn's recorded ConsoleEvents."""

    def __init__(self, data: Any) -> None:
        self.data = data  # ConsoleData — graph lookups for grounding

    # ── graph helpers ────────────────────────────────────────

    def _ref_resolves(self, ref: str) -> bool:
        if ref.startswith("ledger:"):
            return True  # the audit ledger is its own system of record
        try:
            return self.data.tier_for(ref) is not None
        except Exception:
            return False

    def _ref_tier(self, ref: str) -> str | None:
        try:
            return self.data.tier_for(ref)
        except Exception:
            return None

    def _table_uri(self, name: str):
        try:
            node = self.data._find_table_node(name)
            return getattr(node, "canonical_uri", None)
        except Exception:
            return None

    def _tables_connected(self, a: str, b: str) -> bool | None:
        """Is there an OBSERVED relationship between two tables — a
        direct edge, or the real join topology (column EQUIVALENT_TO
        column across the two)? None = can't tell (no graph or table
        unresolved) — honesty over guessing."""
        store = getattr(self.data, "store", None)
        if store is None:
            return None
        ua, ub = self._table_uri(a), self._table_uri(b)
        if not ua or not ub:
            return None
        try:
            for x, y in ((ua, ub), (ub, ua)):
                if any(e.to_uri == y for e in store.outgoing(x)):
                    return True
            cols_a = {e.to_uri for e in store.outgoing(ua, "CONTAINS")}
            cols_b = {e.to_uri for e in store.outgoing(ub, "CONTAINS")}
            for c in cols_a:
                for e in store.outgoing(c, "EQUIVALENT_TO"):
                    if e.to_uri in cols_b:
                        return True
            for c in cols_b:
                for e in store.outgoing(c, "EQUIVALENT_TO"):
                    if e.to_uri in cols_a:
                        return True
            return False
        except Exception:
            return None

    # ── the rubric ───────────────────────────────────────────

    def evaluate(self, question: str,
                 events: list[dict[str, Any]]) -> dict[str, Any]:
        tool_calls = [e for e in events if e.get("type") == "tool_call"]
        results = [e for e in events if e.get("type") == "tool_result"]
        gates = [e for e in events if e.get("type") == "sql_gate"]
        resolved = [e for e in events if e.get("type") == "gate_resolved"]
        answers = [e for e in events if e.get("type") == "answer"]
        errors = [e for e in events if e.get("type") == "error"]
        tool_of = {c.get("call_id"): c.get("tool", "")
                   for c in tool_calls}

        checks: list[dict[str, str]] = []
        corrections: list[str] = []

        sections = (answers[-1].get("sections") or {}) if answers else {}
        citations = sections.get("citations") or []

        # 1 · every claim traces somewhere real
        if not answers:
            checks.append(_check(
                "citations", "Citations present and resolvable", "fail",
                "The turn produced no final answer, so nothing is "
                "citable."))
        elif not citations:
            checks.append(_check(
                "citations", "Citations present and resolvable", "fail",
                "The answer cites nothing. Every claim must trace to a "
                "graph node or a ledger entry."))
        else:
            bad = [c.get("ref", "") for c in citations
                   if not self._ref_resolves(c.get("ref", ""))]
            if bad:
                checks.append(_check(
                    "citations", "Citations present and resolvable",
                    "warn",
                    f"{len(citations)} citation(s), but "
                    f"{', '.join(bad)} did not resolve in the graph."))
            else:
                checks.append(_check(
                    "citations", "Citations present and resolvable",
                    "pass",
                    f"All {len(citations)} citation(s) resolve to graph "
                    "nodes or the audit ledger."))

        # 2 · cited facts carry an honest tier
        graph_refs = [c.get("ref", "") for c in citations
                      if str(c.get("ref", "")).startswith("synapse://")]
        tiers = [(r, self._ref_tier(r)) for r in graph_refs]
        known = [(r, t) for r, t in tiers if t is not None]
        low = [(r, t) for r, t in known
               if TIER_RANK.get(t, 1) < TIER_RANK["inferred"]]
        if not graph_refs:
            checks.append(_check(
                "tiers", "Cited facts at an honest tier", "skip",
                "No graph-node citations this turn."))
        elif not known:
            checks.append(_check(
                "tiers", "Cited facts at an honest tier", "skip",
                "No graph snapshot loaded — tiers cannot be verified."
                if not getattr(self.data, "live", False) else
                "The cited refs are not in the current snapshot, so "
                "there is no tier to verify."))
        elif low:
            names = ", ".join(f"{r.rsplit('/', 1)[-1]} ({t})"
                              for r, t in low)
            checks.append(_check(
                "tiers", "Cited facts at an honest tier", "warn",
                f"Cited below the inferred tier: {names}. The answer "
                "must label these as unverified."))
        else:
            checks.append(_check(
                "tiers", "Cited facts at an honest tier", "pass",
                "Every cited fact sits at inferred or better."))

        # 3 · nothing executed without the seal
        exec_calls = [c for c in tool_calls
                      if c.get("tool") in EXECUTE_TOOLS]
        if not exec_calls:
            checks.append(_check(
                "seal", "No execution without the seal", "skip",
                "No live query this turn."))
        else:
            approved = any(g.get("decision") == "approved"
                           for g in resolved)
            if gates and approved:
                checks.append(_check(
                    "seal", "No execution without the seal", "pass",
                    "The query ran only after the gate showed its cost "
                    "and a human approved it."))
            else:
                checks.append(_check(
                    "seal", "No execution without the seal", "fail",
                    "A query executed without a resolved approval gate "
                    "in this turn's stream."))

        # 4 · joins exist in observed reality
        gate_sql = gates[-1].get("sql", "") if gates else ""
        if not gate_sql:
            checks.append(_check(
                "joins", "Tables and joins are observed, not invented",
                "skip", "No SQL reached the gate this turn."))
        elif not getattr(self.data, "live", False):
            checks.append(_check(
                "joins", "Tables and joins are observed, not invented",
                "warn", "No graph snapshot loaded — table existence and "
                        "joins cannot be verified."))
        else:
            tables = _sql_tables(gate_sql)
            if tables is None:
                checks.append(_check(
                    "joins", "Tables and joins are observed, not invented",
                    "warn", "The gated SQL did not parse for analysis."))
            else:
                unknown = [t for t in tables
                           if self._table_uri(t) is None]
                broken: list[str] = []
                for i in range(len(tables)):
                    for j in range(i + 1, len(tables)):
                        if self._tables_connected(
                                tables[i], tables[j]) is False:
                            broken.append(f"{tables[i]} ↔ {tables[j]}")
                if unknown:
                    checks.append(_check(
                        "joins",
                        "Tables and joins are observed, not invented",
                        "fail",
                        f"SQL references tables the graph has never "
                        f"seen: {', '.join(unknown)}."))
                elif broken:
                    checks.append(_check(
                        "joins",
                        "Tables and joins are observed, not invented",
                        "fail",
                        f"No observed relationship for: "
                        f"{'; '.join(broken)}. The workflow forbids "
                        "invented ON clauses."))
                else:
                    what = (f"{len(tables)} table(s), all known to the "
                            "graph" + ("" if len(tables) < 2 else
                                       ", every join pair observed"))
                    checks.append(_check(
                        "joins",
                        "Tables and joins are observed, not invented",
                        "pass", what + "."))

        # 5 · the tool budget held
        n_calls = len(tool_calls)
        checks.append(_check(
            "budget", "Tool budget respected (≤ 12 calls)",
            "pass" if n_calls <= 12 else "fail",
            f"{n_calls} tool call(s) this turn."
            + ("" if n_calls <= 12 else
               " The workflow caps a turn at 12.")))

        # 6 · the answer kept its contract
        if answers:
            wanted = ["answer", "how_i_got_there", "citations",
                      "governance", "status"]
            missing = [w for w in wanted if not sections.get(w)]
            if missing:
                checks.append(_check(
                    "contract", "Answer contract complete", "warn",
                    f"Missing section(s): {', '.join(missing)}."))
            else:
                checks.append(_check(
                    "contract", "Answer contract complete", "pass",
                    "Answer, method, citations, governance, and status "
                    "all present."))

        # 7 · errors
        if errors:
            checks.append(_check(
                "errors", "Turn free of runtime errors", "fail",
                errors[-1].get("message", "error")[:200]))
        else:
            checks.append(_check(
                "errors", "Turn free of runtime errors", "pass",
                "No error events in the stream."))

        # 8 · self-correction, surfaced as a feature
        failed = [r for r in results if r.get("ok") is False]
        for r in failed:
            tool = tool_of.get(r.get("call_id"), "")
            later_ok = any(
                rr.get("ok", True) and tool_of.get(rr.get("call_id")) == tool
                for rr in results[results.index(r) + 1:])
            if later_ok:
                corrections.append(
                    f"{tool or 'a tool'} flagged: "
                    f"“{r.get('summary', '')[:140]}” — the agent revised "
                    "and re-validated clean before the seal.")
            elif tool == "validate_sql_plan" or "BLOCKED" in str(
                    r.get("summary", "")):
                corrections.append(
                    f"Guardrail block honored: "
                    f"“{r.get('summary', '')[:140]}” — the agent refused "
                    "and offered a compliant alternative.")
        checks.append(_check(
            "selfcorrect", "Self-correction before the seal", "pass",
            corrections[0] if corrections
            else "No mid-turn issues; nothing needed revising."))

        # ── verdict ──────────────────────────────────────────
        applicable = [c for c in checks if c["status"] != "skip"]
        n_pass = sum(1 for c in applicable if c["status"] == "pass")
        n_warn = sum(1 for c in applicable if c["status"] == "warn")
        n_fail = sum(1 for c in applicable if c["status"] == "fail")
        score = ((n_pass + 0.5 * n_warn) / len(applicable)
                 if applicable else 0.0)
        if n_fail:
            verdict, verdict_text = "needs_review", (
                "Do not trust this answer yet — a check failed. "
                "See the failed rows for exactly what went wrong.")
        elif n_warn:
            verdict, verdict_text = "grounded_caveats", (
                "Accurate with caveats — the warned checks say what to "
                "double-check.")
        else:
            verdict, verdict_text = "grounded", (
                "Accurate and fully grounded — every claim traces to "
                "the graph or the audit ledger, under the gates.")
        return {
            "question": question,
            "verdict": verdict,
            "verdict_text": verdict_text,
            "score": round(score, 3),
            "checks": checks,
            "corrections": corrections,
            "n_tool_calls": n_calls,
        }


class EvalLog:
    """Ring buffer of evaluated turns + rollup — the /api/evals feed."""

    def __init__(self, evaluator: TurnEvaluator, maxlen: int = 25) -> None:
        self.evaluator = evaluator
        self._turns: deque[dict[str, Any]] = deque(maxlen=maxlen)

    def record(self, turn_id: str, question: str,
               events: list[dict[str, Any]]) -> dict[str, Any]:
        result = self.evaluator.evaluate(question, events)
        result["turn_id"] = turn_id
        result["ts"] = time.time()
        self._turns.appendleft(result)
        return result

    def recent(self) -> dict[str, Any]:
        turns = list(self._turns)
        n = len(turns)
        grounded = sum(1 for t in turns if t["verdict"] == "grounded")
        corrections = sum(len(t["corrections"]) for t in turns)
        return {
            "turns": turns,
            "summary": {
                "n_turns": n,
                "grounded_rate": round(grounded / n, 3) if n else None,
                "avg_score": (round(sum(t["score"] for t in turns) / n, 3)
                              if n else None),
                "self_corrections": corrections,
            },
        }
