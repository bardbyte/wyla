"""The generator worker (E18): retrieval BEFORE generation, then one
composed query and a streamed answer.

Retrieval first, always: the table card, the metric's definition line
and its certified expression are read from the promoted build and put
in front of the model, so composition is assembly from real parts
rather than recall. The certified expression rides in VERBATIM and
``validate_sql`` refuses a query that dropped it
(``metric_expression_missing``) — that is what "never invents a
metric" means in code.

The generator never sees the verdict: verification happens after this
module has already streamed its prose, so the answer cannot be
written to flatter its own grade.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable

from sahs.tools.api import Build, describe_table, get_definition_line
from sahs.tools.sandbox import execute_sandboxed
from sahs.tools.validate_sql import validate_sql

from .budget import truncate_rows
from .plan import Plan

CARD_BUDGET = 6000          # characters of table card put in context

SQL_SYSTEM = """You compose ONE BigQuery SELECT for a governed \
semantic layer. You are assembling certified parts, not inventing SQL.

Hard rules:
- Use the CERTIFIED EXPRESSION exactly as given, character for \
character, as the measure. Never rewrite or "improve" it.
- Only reference tables and columns listed in the card.
- Honour the grain: the GROUP BY must produce exactly the row meaning \
stated.
- Apply every filter binding given, verbatim.
- No SELECT *. No DDL/DML. One statement.

Return STRICT JSON: {"sql": "<the query>", "why": "<one sentence>"}"""

PROSE_SYSTEM = """You are Synapse, answering an analyst inside a \
governed semantic layer.

Write 2-5 sentences of plain, specific prose. Say what the query \
measures, at what grain, over what filter. If rows are provided, \
state the actual figures. If no rows were executed, say plainly that \
the query was validated but not run, and do not imply a number.

Never invent a value, a trend, or a comparison you were not given. \
No preamble, no bullet lists, no headings."""


@dataclass
class Generation:
    sql: str = ""
    prose: str = ""
    definition_line: str = ""
    why: str = ""
    repaired: bool = False
    violations: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    rows: list[list[Any]] | None = None
    result_schema: list[dict[str, str]] = field(default_factory=list)
    bytes_processed: int | None = None
    execution: dict[str, Any] = field(default_factory=dict)
    retrieval: dict[str, Any] = field(default_factory=dict)


def _execute_mode() -> str:
    """Live only when BOTH the sandbox is unlocked and Ask is asked to
    use it; otherwise the dry run, which returns zero rows by design."""
    from sahs.tools.sandbox import live_enabled
    if (live_enabled()
            and os.environ.get("ASK_EXECUTE", "").lower() == "live"):
        return "live"
    return "snapshot"


def run_query(build: Build, sql: str) -> dict[str, Any]:
    """Execute through the sandbox: ACL, then cost gates, then run.

    A denial is a result, not an exception — and neither is an
    unreachable warehouse: the envelope carries the failure so the
    ``executes`` and ``cost_gate`` criteria fail on evidence instead
    of the turn dying. A machine with no BigQuery still gets a
    composed, validated, honestly-unverified answer."""
    try:
        envelope = execute_sandboxed(build, sql, mode=_execute_mode())
    except Exception as e:                       # transport, creds, ACL
        return {"status": "error", "data": {},
                "error": f"{type(e).__name__}: {e}", "meta": {}}
    if (envelope.get("status") == "denied"
            and _execute_mode() == "live"):
        # the gates said no: fall back to the dry run and keep the
        # refusal, so the answer can say why there is no number
        try:
            fallback = execute_sandboxed(build, sql, mode="snapshot")
        except Exception as e:
            return {"status": "error", "data": {},
                    "error": f"{type(e).__name__}: {e}", "meta": {}}
        fallback.setdefault("meta", {})["live_denied"] = \
            envelope.get("error") or "denied"
        return fallback
    return envelope


def _retrieve(build: Build, plan: Plan) -> dict[str, Any]:
    card = describe_table(build, plan.table)
    definition = get_definition_line(build, plan.metric_id)
    return {
        "card": (card.get("card") or "")[:CARD_BUDGET],
        "card_error": card.get("error", ""),
        "definition_line": definition.get("definition_line", ""),
        "columns": sorted(build.schema.get(plan.table, {})),
    }


def _sql_prompt(plan: Plan, retrieval: dict[str, Any],
                violations: list[dict[str, Any]] | None = None) -> str:
    filters = "\n".join(
        f"  {name} = {value!r}  →  binding: {plan.filter_bindings.get(name, '?')}"
        for name, value in plan.filters.items()) or "  (none)"
    parts = [
        f"TABLE CARD for {plan.table}:\n{retrieval['card']}",
        f"\nCERTIFIED EXPRESSION (use verbatim):\n{plan.metric_sql}",
        f"\nMETRIC: {plan.metric_label or plan.metric_id}",
        f"GRAIN (one row means): {plan.grain}",
        f"FILTERS:\n{filters}",
        f"TIME WINDOW: {plan.time_window or '(none stated)'}",
        f"DIMENSIONS: {', '.join(plan.dimensions) or '(none)'}",
        f"\nQUESTION: {plan.question}",
    ]
    if violations:
        listed = "\n".join(f"  - {v.get('code')}: {v.get('detail')} "
                           f"({v.get('hint', '')})" for v in violations)
        parts.append("\nYour previous attempt was REFUSED by the "
                     f"validator:\n{listed}\nFix exactly these.")
    return "\n".join(parts)


def generate(model: Any, build: Build, plan: Plan, *,
             on_token: Callable[[str], None] | None = None,
             abort_check: Callable[[], None] | None = None) -> Generation:
    """Compose, validate (one repair attempt), execute, then stream the
    prose through ``on_token``."""
    retrieval = _retrieve(build, plan)
    gen = Generation(definition_line=retrieval["definition_line"],
                     retrieval=retrieval)

    # ── compose ──────────────────────────────────────────────
    answer = model.json(_sql_prompt(plan, retrieval), system=SQL_SYSTEM,
                        temperature=0.0, max_tokens=1200) or {}
    gen.sql = str(answer.get("sql") or "").strip()
    gen.why = str(answer.get("why") or "")
    if not gen.sql:
        raise RuntimeError("the generator returned no SQL")

    # ── validate, with exactly one repair attempt ────────────
    if abort_check:
        abort_check()
    report = validate_sql(build, gen.sql, plan.metric_id)
    if not report["ok"]:
        repair = model.json(
            _sql_prompt(plan, retrieval, report["violations"]),
            system=SQL_SYSTEM, temperature=0.0, max_tokens=1200) or {}
        candidate = str(repair.get("sql") or "").strip()
        if candidate:
            second = validate_sql(build, candidate, plan.metric_id)
            gen.repaired = True
            if second["ok"] or len(second["violations"]) < len(
                    report["violations"]):
                gen.sql, report = candidate, second
    gen.violations = report["violations"]
    gen.warnings = report["warnings"]

    # ── execute (dry run by default: zero rows, real gates) ──
    if abort_check:
        abort_check()
    envelope = run_query(build, gen.sql)
    gen.execution = {"status": envelope.get("status"),
                     "error": envelope.get("error"),
                     "meta": envelope.get("meta", {})}
    data = envelope.get("data") or {}
    gen.result_schema = data.get("result_schema") or data.get("schema") or []
    gen.bytes_processed = data.get("bytes_processed")
    if data.get("rows"):
        rows, withheld = truncate_rows(list(data["rows"]))
        gen.rows = rows
        if withheld:
            gen.execution["withheld_rows"] = withheld

    # ── stream the prose ─────────────────────────────────────
    if abort_check:
        abort_check()
    rows_block = ("no rows: the query was validated by dry run, not "
                  "executed" if not gen.rows else
                  "\n".join(str(r) for r in gen.rows[:20]))
    prose_prompt = (
        f"QUESTION: {plan.question}\n"
        f"METRIC: {plan.metric_label or plan.metric_id}\n"
        f"GRAIN: {plan.grain}\n"
        f"FILTERS: {plan.filters or '(none)'}\n"
        f"DEFINITION LINE: {gen.definition_line}\n"
        f"SQL:\n{gen.sql}\n\nRESULT:\n{rows_block}\n\n"
        "Write the answer.")
    chunks: list[str] = []
    for chunk in model.stream(prose_prompt, system=PROSE_SYSTEM,
                              temperature=0.3, max_tokens=700):
        if abort_check:
            abort_check()
        chunks.append(chunk)
        if on_token:
            on_token(chunk)
    gen.prose = "".join(chunks).strip()
    return gen
