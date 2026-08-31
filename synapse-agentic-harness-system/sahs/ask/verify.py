"""The verifier worker (E18): fresh context, read-only tools,
default-FAIL contract.

It sees the artifacts (plan, SQL, result) and NOTHING of the
generator's reasoning — no prompt, no draft, no self-report. Criteria
flip only on evidence it gathers itself; anything it cannot evaluate
stays false. UNKNOWN is a failure here, deliberately: a cost gate that
cannot read the byte estimate must not wave the query through.

The only model call is one groundedness judge, and its prompt carries
the artifacts alone.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from sahs.tools.api import Build
from sahs.tools.sandbox import DEFAULT_MAX_BYTES, MIN_PRIOR_JOBS
from sahs.tools.validate_sql import validate_sql

from .contract import Contract
from .generate import Generation, run_query
from .plan import Plan

JUDGE_SYSTEM = """You are a skeptical reviewer. You are shown a \
question, a query, its result, and a written answer. You did not \
write any of them.

Decide ONE thing: does every factual claim in the written answer \
follow from the query and the result shown? A claim about a number \
that is not in the result is NOT grounded. Saying "not executed" when \
there are no rows IS grounded.

Return STRICT JSON: {"grounded": true|false, "why": "<one sentence>"}"""


def _group_keys(sql: str) -> tuple[list[str], str]:
    """The GROUP BY keys, read from the AST rather than the string."""
    try:
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(sql, read="bigquery")
        group = tree.find(exp.Group)
        if group is None:
            return [], ""
        return [k.sql(dialect="bigquery") for k in group.expressions], ""
    except Exception as e:                       # unparseable → UNKNOWN
        return [], f"{type(e).__name__}: {e}"


def _cost_cap(build: Build, tables: list[str]) -> tuple[int, str]:
    """min(global budget ceiling, 3× this table's p95) — the same two
    questions the sandbox asks, so the verdict cannot disagree with
    the gate that actually runs."""
    try:
        ceiling = int(os.environ.get("SAHS_LIVE_MAX_BYTES")
                      or DEFAULT_MAX_BYTES)
    except ValueError:
        ceiling = DEFAULT_MAX_BYTES
    why = "global budget ceiling"
    for table in tables:
        prior = build.cost_priors.get(table) or {}
        p95 = prior.get("p95_bytes") or prior.get("p95")
        jobs = prior.get("n_jobs") or prior.get("jobs") or 0
        if p95 and jobs >= MIN_PRIOR_JOBS:
            anomaly = int(p95) * 3
            if anomaly < ceiling:
                ceiling, why = anomaly, f"3x p95 for {table}"
    return ceiling, why


def verify(build: Build, plan: Plan, contract: Contract, gen: Generation,
           model: Any, *,
           on_progress: Callable[[dict[str, Any]], None] | None = None,
           abort_check: Callable[[], None] | None = None) -> Contract:
    def flip(criterion_id: str, passed: bool, evidence: str) -> None:
        if contract.get(criterion_id) is None:
            return
        criterion = contract.flip(criterion_id, passed, evidence)
        if on_progress:
            on_progress(criterion.to_dict())

    # ── 1. it runs (the verifier executes it itself) ─────────
    if abort_check:
        abort_check()
    envelope = run_query(build, gen.sql)
    data = envelope.get("data") or {}
    ran = envelope.get("status") == "ok" and (
        data.get("valid") is True or bool(data.get("rows") is not None))
    flip("executes", ran,
         f"sandbox {envelope.get('status')}"
         + (f": {envelope.get('error')}" if envelope.get("error") else ""))

    # ── 2. every name in it is real ──────────────────────────
    if abort_check:
        abort_check()
    report = validate_sql(build, gen.sql, plan.metric_id)
    codes = [v.get("code") for v in report["violations"]]
    flip("contract_ast", report["ok"],
         "no violations" if report["ok"] else ", ".join(codes))

    # ── 3. the grain it declares is the grain it computes ────
    keys, parse_error = _group_keys(gen.sql)
    if parse_error:
        flip("grain_declared", False, f"could not parse: {parse_error}")
    elif not plan.grain:
        flip("grain_declared", False, "no grain declared")
    elif plan.grain.strip().lower() in ("total", "one row"):
        flip("grain_declared", not keys,
             "single total, no GROUP BY" if not keys
             else f"declared total but groups by {', '.join(keys)}")
    else:
        flip("grain_declared", bool(keys),
             f"GROUP BY {', '.join(keys)}" if keys
             else f"declares {plan.grain!r} but groups by nothing")

    # ── 4. the join cannot double-count ──────────────────────
    if contract.get("fan_out_guard") is not None:
        tables = sorted({t for t in (envelope.get("meta") or {}).get(
            "tables", []) if t})
        safe = [j for j in build.joins
                if j.get("a") in tables and j.get("b") in tables
                and j.get("scope") != "scoped_only"]
        flip("fan_out_guard", bool(safe),
             f"raw-safe join on record: {safe[0].get('on')}" if safe
             else f"no raw-safe join between {', '.join(tables)}: a "
                  "CTE-scoped witness is evidence the relationship "
                  "exists, not that the raw tables join safely")

    # ── 5. cost: UNKNOWN fails closed ────────────────────────
    meta = envelope.get("meta") or {}
    scanned = data.get("bytes_processed", meta.get("bytes_scanned"))
    cap, why = _cost_cap(build, list(meta.get("tables") or [plan.table]))
    if scanned is None:
        flip("cost_gate", False,
             "no byte estimate available: fail-closed on UNKNOWN")
    else:
        flip("cost_gate", int(scanned) <= cap,
             f"{int(scanned):,} bytes against {cap:,} ({why})")

    # ── 6. groundedness: one judge, artifacts only ───────────
    if abort_check:
        abort_check()
    rows_block = ("(no rows: dry run only)" if not gen.rows
                  else "\n".join(str(r) for r in gen.rows[:20]))
    judgment = model.json(
        f"QUESTION: {plan.question}\nGRAIN: {plan.grain}\n"
        f"SQL:\n{gen.sql}\n\nRESULT:\n{rows_block}\n\n"
        f"WRITTEN ANSWER:\n{gen.prose}",
        system=JUDGE_SYSTEM, temperature=0.0, max_tokens=400)
    if isinstance(judgment, dict) and isinstance(
            judgment.get("grounded"), bool):
        flip("grounded", judgment["grounded"],
             str(judgment.get("why", ""))[:300])
    else:
        flip("grounded", False,
             "the judge returned no usable verdict: fail-closed")
    return contract
