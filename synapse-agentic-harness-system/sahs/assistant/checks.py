"""The verification toolkit (Synapse v2 §4/§6): checks as callable
tools whose results are FACTS the model can cite.

Each check returns ``{fact_id, kind, passed, method, detail}`` and is
recorded on the turn's state; a PASSED fact id cited in an artifact's
``provenance.facts`` is what lets a composed number shed the
EXPLORATORY watermark (rule 2). The checks are deterministic — they
read saved query results, the compiled build, and the SQL itself.
``verify_answer`` is the one exception: its groundedness half is the
fresh-context judge, called as its own model invocation.

Honesty of method is part of the fact: a structural reconcile says
``structural`` (the certified expression is contained in the composed
SQL), a numeric one says ``numeric`` (totals compared over snapshot
rows). A fact never claims more than its method delivered.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from sahs.tools.api import Build
from sahs.tools.validate_sql import validate_sql

TOLERANCE = 0.005          # ±0.5%: reconciliation, not replication


def _fact(state: Any, kind: str, passed: bool, method: str,
          detail: str) -> dict[str, Any]:
    fact_id = f"f{len(state.facts_log) + 1}"
    fact = {"fact_id": fact_id, "kind": kind, "passed": bool(passed),
            "method": method, "detail": detail[:600]}
    state.facts_log.append(fact)
    if passed:
        state.facts.add(fact_id)
    return fact


def _load_rows(workspace: Path, name: str
               ) -> tuple[list[dict[str, Any]] | None, str]:
    path = workspace / (name if str(name).endswith(".json")
                        else f"{name}.json")
    if not path.exists():
        saved = sorted(p.stem for p in workspace.glob("q*.json"))
        return None, ("no saved result " + repr(name) + "; saved: "
                      + (", ".join(saved) or "none — run_sql snapshot "
                         "saves rows as q<N>"))
    return json.loads(path.read_text(encoding="utf-8")), ""


def _numeric_key(rows: list[dict[str, Any]],
                 column: str = "") -> tuple[str, str]:
    if not rows:
        return "", "the result has no rows"
    if column:
        value = rows[0].get(column)
        if isinstance(value, (int, float)) \
                and not isinstance(value, bool):
            return column, ""
        return "", (f"column {column!r} is not numeric here; "
                    "columns: " + ", ".join(rows[0]))
    for key, value in rows[0].items():
        if isinstance(value, (int, float)) \
                and not isinstance(value, bool):
            return key, ""
    return "", ("no numeric column found; columns: "
                + ", ".join(rows[0]))


def _total(rows: list[dict[str, Any]], key: str) -> float:
    return float(sum(r.get(key) or 0 for r in rows))


def _within(a: float, b: float, tolerance: float) -> bool:
    return abs(a - b) <= tolerance * max(1.0, abs(b))


# ─── the check primitives ────────────────────────────────────


def check_part_whole(state: Any, workspace: Path, *, breakdown: str,
                     total: str, column: str = "",
                     tolerance: float = TOLERANCE) -> dict[str, Any]:
    rows_b, err = _load_rows(workspace, breakdown)
    if err:
        return {"error": err, "hint": "run the breakdown first"}
    rows_t, err = _load_rows(workspace, total)
    if err:
        return {"error": err, "hint": "run the total first"}
    key, err = _numeric_key(rows_t, column)
    if err:
        return {"error": err, "hint": "name the column to compare"}
    sum_b, sum_t = _total(rows_b, key), _total(rows_t, key)
    passed = _within(sum_b, sum_t, tolerance)
    return _fact(
        state, "part_whole", passed, "numeric",
        f"sum({key}) over {breakdown} = {sum_b:,.4g} vs {total} = "
        f"{sum_t:,.4g}"
        + ("" if passed else f" — outside ±{tolerance:.1%}: the "
           "breakdown does not add up to the total (double count, "
           "missing slice, or different filters)"))


def check_crosscheck(state: Any, workspace: Path, *, a: str, b: str,
                     column: str = "",
                     tolerance: float = TOLERANCE) -> dict[str, Any]:
    rows_a, err = _load_rows(workspace, a)
    if err:
        return {"error": err, "hint": "both results must exist"}
    rows_b, err = _load_rows(workspace, b)
    if err:
        return {"error": err, "hint": "both results must exist"}
    key, err = _numeric_key(rows_a, column)
    if err:
        return {"error": err, "hint": "name the column to compare"}
    ta, tb = _total(rows_a, key), _total(rows_b, key)
    passed = _within(ta, tb, tolerance)
    return _fact(
        state, "crosscheck", passed, "numeric",
        f"{a}.{key} = {ta:,.4g} vs {b}.{key} = {tb:,.4g}"
        + ("" if passed else " — the two routes disagree: say so, "
           "or find which assumption differs"))


def check_coverage(state: Any, workspace: Path, *,
                   result: str) -> dict[str, Any]:
    rows, err = _load_rows(workspace, result)
    if err:
        return {"error": err, "hint": "run the query first"}
    if not rows:
        return _fact(state, "coverage", False, "numeric",
                     f"{result} returned zero rows: an empty answer "
                     "needs saying, not charting")
    first_key = next(iter(rows[0]), "")
    nulls = sum(1 for r in rows if r.get(first_key) in (None, ""))
    passed = nulls == 0
    return _fact(
        state, "coverage", passed, "numeric",
        f"{result}: {len(rows)} rows"
        + ("" if passed else f", {nulls} null {first_key!r} keys — "
           "the grain has holes"))


def check_fanout(state: Any, build: Build, *,
                 tables: list[str]) -> dict[str, Any]:
    from sahs.loop.tools import _tier
    if not isinstance(tables, (list, tuple)) or len(tables) < 2:
        return {"error": "check_fanout takes two or more tables",
                "hint": "the tables the join touches"}
    physicals = []
    for name in tables:
        physical = build.physical_of(str(name))
        if physical is None:
            return {"error": f"unknown table {name!r}",
                    "hint": "list_tables shows what exists"}
        physicals.append(physical)
    pair = set(physicals[:2])
    rows = [r for r in build.joins if {r["a"], r["b"]} == pair]
    safe = [r for r in rows if _tier(r) in ("certified", "witnessed")]
    passed = bool(safe)
    return _fact(
        state, "fanout", passed, "topology",
        (f"raw-safe join on record: {safe[0].get('on', '(declared)')}"
         if passed else
         f"no raw-safe join between {' and '.join(sorted(pair))}: "
         "a co-query or CTE-scoped witness is evidence the "
         "relationship exists, not that raw rows join without "
         "double-counting"))


def check_reconcile(state: Any, build: Build, *, sql: str,
                    metric: str) -> dict[str, Any]:
    from sahs.loop.tools import _metric_row
    row, others = _metric_row(build, metric)
    if row is None:
        return {"error": f"no metric matches {metric!r}",
                "hint": "search_semantics or resolve first; pass the "
                        "metric id"}
    if others:
        return {"error": f"{metric!r} is ambiguous",
                "hint": "pass one id: " + ", ".join(
                    m["id"] for m in [row] + others[:2])}
    report = validate_sql(build, sql, metric_id=row["id"])
    blocking = [v for v in report["violations"]
                if v["code"] in ("metric_expression_missing",
                                 "parse_error", "unknown_table",
                                 "unknown_column", "unknown_metric")]
    passed = not blocking
    detail = (f"the certified expression of {row['label']} is "
              "contained verbatim in the composed SQL" if passed else
              "; ".join(f"{v['code']}: {v['detail']}"
                        for v in blocking))
    fact = _fact(state, "reconcile", passed, "structural", detail)
    fact["hint"] = ("structural containment only: for a NUMERIC "
                    "reconcile, run both on the snapshot and "
                    "check_crosscheck the saved results")
    return fact


def verify_answer(state: Any, build: Build, model: Any, *, sql: str,
                  claim: str = "",
                  substrate: Any = None) -> dict[str, Any]:
    from sahs.ask.verify import JUDGE_SYSTEM
    from sahs.tools.sandbox import execute_sandboxed
    report = validate_sql(build, sql)
    parts = ["names_real" if report["ok"] else
             "names_unreal(" + ",".join(
                 v["code"] for v in report["violations"][:3]) + ")"]
    ran = execute_sandboxed(build, sql, mode="snapshot",
                            substrate=substrate)
    executed = ran.get("status") == "ok"
    parts.append("validates_on_build" if executed
                 else f"refused({ran.get('error', '?')[:80]})")
    grounded = True
    if claim and model is not None:
        judgment = model.json(
            f"SQL:\n{sql}\n\nCLAIM:\n{claim}",
            system=JUDGE_SYSTEM, temperature=0.0, max_tokens=300)
        grounded = bool(isinstance(judgment, dict)
                        and judgment.get("grounded") is True)
        parts.append("claim_grounded" if grounded
                     else "claim_unsupported(fresh-context judge)")
    passed = report["ok"] and executed and grounded
    return _fact(state, "verify_answer", passed,
                 "structural+judge" if claim else "structural",
                 " · ".join(parts))


# ─── subgraph: the receipts as data ──────────────────────────


def build_subgraph(build: Build, state: Any,
                   ids: list[str] | None = None) -> dict[str, Any]:
    """The nodes and edges behind a set of facts — or, with no ids,
    behind everything this turn touched (the recorded trace)."""
    wanted = list(ids or [])
    if not wanted:
        wanted = [c.replace("tables/", "table:").replace("__", ".")
                  if c.startswith("tables/") else c
                  for c in state.subgraph.get("cards_read", [])]
        wanted += [f"metric:{b['metric'].split(':', 1)[-1]}"
                   if not str(b.get("metric", "")).startswith("metric:")
                   else b["metric"]
                   for b in state.subgraph.get("bindings_used", [])]
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    tables: set[str] = set()

    def add_table(physical: str) -> None:
        if physical and physical in build.schema:
            tables.add(physical)
            nodes.setdefault(f"table:{physical}", {
                "id": f"table:{physical}", "kind": "table",
                "label": build.short_table(physical)})

    for ref in wanted:
        ref = str(ref)
        if ref.startswith("metric"):
            from sahs.loop.tools import _metric_row
            row, _ = _metric_row(build, ref.split(":", 1)[-1]
                                 if ":" in ref else ref)
            if row is None:
                row, _ = _metric_row(build, ref)
            if row:
                nodes[row["id"]] = {
                    "id": row["id"], "kind": "metric",
                    "label": row.get("label") or row["id"],
                    "status": row.get("status_served")
                    or row.get("status")}
                add_table(row.get("table", ""))
                edges.append({"a": row["id"],
                              "b": f"table:{row['table']}",
                              "rel": "bound_to"})
        elif ref.startswith(("table:", "tables/")):
            name = ref.split(":", 1)[-1].split("/")[-1]
            add_table(build.physical_of(name) or name)
        elif ref.startswith(("concept:", "concepts/")):
            label = ref.split(":", 1)[-1].split("/")[-1]
            for binding in build.bindings:
                if binding.get("label") == label:
                    node_id = f"concept:{label}@{binding['table']}"
                    nodes[node_id] = {"id": node_id, "kind": "concept",
                                      "label": label}
                    add_table(binding["table"])
                    edges.append({"a": node_id,
                                  "b": f"table:{binding['table']}",
                                  "rel": "binds"})
    from sahs.loop.tools import _tier
    seen_pairs = set()
    for join in build.joins:
        pair = frozenset((join["a"], join["b"]))
        if pair <= tables and pair not in seen_pairs:
            seen_pairs.add(pair)
            edges.append({"a": f"table:{join['a']}",
                          "b": f"table:{join['b']}",
                          "rel": "joins", "tier": _tier(join)})
    return {"nodes": sorted(nodes.values(), key=lambda n: n["id"]),
            "edges": edges,
            "hint": "" if nodes else
            "nothing referenced yet: pass metric:/table:/concept: "
            "ids, or read some cards first — the trace fills this in"}


# ─── template bindings: shared with the loop kit ─────────────

from sahs.loop.tools import binding_template  # noqa: E402,F401
