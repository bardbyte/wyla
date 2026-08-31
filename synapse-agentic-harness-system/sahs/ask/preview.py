"""The join and grain preview (E18 Stage C): fan-out in row counts,
judgeable by anyone, BEFORE the query runs.

The card the analyst reads is "1,020 rows stay 1,020 after the join"
or "this join may inflate totals". Both are decided from two facts the
build already carries and one it carries about the join:

  * the row count of each table (``indexes/tables.jsonl``);
  * whether the join key on the far side is a DECLARED primary key;
  * which witness family attests the join, and at what scope.

If the far-side key is a declared primary key, at most one row can
match, so the base row count survives: that is provable, not guessed.
If it is not, we do NOT invent a multiplier — the verdict is
``unproven`` and it says exactly which fact is missing. A witness that
only ever appeared inside a CTE (``scoped_only``) is evidence the
relationship exists, never evidence the raw tables join safely; that
distinction is the same one the verifier's fan_out_guard enforces
after the fact, brought forward to where it can still change the plan.
"""

from __future__ import annotations

from typing import Any

from sahs.tools.api import Build

from .plan import Plan

SCHEMA = "meridian.join_preview/1"

# a join is only as trustworthy as the family that saw it. co_query
# digests say two tables appear together, never how they relate.
STRUCTURAL = ("constraints", "studio", "jobs_30d")


def _side_columns(on: str) -> list[tuple[str, str]]:
    """The equalities in an ON clause as (left, right) column pairs.
    Text, deliberately: the ON strings in the build were themselves
    harvested from real SQL and are stored as written."""
    pairs = []
    for conjunct in on.replace(" AND ", "\n").replace(" and ", "\n").split("\n"):
        if "=" not in conjunct:
            continue
        left, _, right = conjunct.partition("=")
        pairs.append((left.strip(), right.strip()))
    return pairs


def _column_of(qualified: str, physical: str) -> str:
    """`dw.t.col` or `t.col` or `col` → `col`, when it belongs to
    ``physical``; otherwise ''."""
    bare = qualified.split(".")[-1]
    if qualified == bare:
        return bare                       # unqualified: assume this side
    prefix = ".".join(qualified.split(".")[:-1])
    if physical.endswith(prefix) or prefix.endswith(
            physical.split(".")[-1]):
        return bare
    return ""


def _joins_between(build: Build, a: str, b: str) -> list[dict[str, Any]]:
    out = []
    for join in build.joins:
        ends = {join.get("a"), join.get("b")}
        if ends == {a, b}:
            out.append(join)
    # structural witnesses first, then by support: the ON clause that
    # tells you HOW outranks the digest that says THAT
    return sorted(out, key=lambda j: (
        j.get("source") not in STRUCTURAL, -int(j.get("support") or 0)))


def _judge(build: Build, base: str, other: str,
           join: dict[str, Any]) -> dict[str, Any]:
    """One join → a verdict a steward could defend."""
    on = str(join.get("on") or "")
    scope = str(join.get("scope") or "")
    source = str(join.get("source") or "?")
    support = int(join.get("support") or 0)
    far = build.table_facts(other)
    keys = [str(k) for k in (far.get("primary_key") or [])]

    if scope == "scoped_only":
        return {"verdict": "unsafe", "on": on, "source": source,
                "support": support, "scope": scope,
                "why": f"this relationship was only ever witnessed inside "
                       f"a CTE on {build.short_table(other)}: that is "
                       f"evidence the relationship exists, not that the "
                       f"raw tables join safely"}
    if not on:
        return {"verdict": "unproven", "on": "", "source": source,
                "support": support, "scope": scope,
                "why": f"{source} says these tables are queried together "
                       f"{support} times but never says on what: a "
                       f"co-query digest cannot rule out fan-out"}

    matched = [_column_of(right, other) or _column_of(left, other)
               for left, right in _side_columns(on)]
    matched = [c for c in matched if c]
    if keys and matched and all(c in keys for c in matched):
        return {"verdict": "safe", "on": on, "source": source,
                "support": support, "scope": scope,
                "why": f"the join lands on {', '.join(sorted(set(matched)))}"
                       f", the declared primary key of "
                       f"{build.short_table(other)}: at most one row can "
                       f"match, so no row is counted twice"}
    return {"verdict": "unproven", "on": on, "source": source,
            "support": support, "scope": scope,
            "why": (f"{', '.join(sorted(set(matched))) or 'the join key'} "
                    f"is not a declared primary key of "
                    f"{build.short_table(other)}"
                    + (f" (declared: {', '.join(keys)})" if keys else
                       ", and this build carries no key for it")
                    + ": whether a row can match twice is not on record")}


def join_grain_preview(build: Build, plan: Plan,
                       tables: list[str] | None = None) -> dict[str, Any]:
    """The preview payload for one plan. Single-table plans get the
    grain half alone: there is no join to judge, and saying so is the
    honest card rather than an omitted one."""
    base = plan.table
    facts = build.table_facts(base)
    rows = facts.get("total_rows")
    others = [t for t in (tables or []) if t and t != base]

    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "base": {"table": base, "short": build.short_table(base),
                 "rows": rows, "primary_key": facts.get("primary_key") or []},
        "grain": plan.grain,
        "grain_source": plan.provenance.get("grain", ""),
        "joins": [],
        "verdict": "safe",
        "headline": "",
    }
    if not others:
        payload["headline"] = (
            f"one table, so nothing can fan out: every row of "
            f"{build.short_table(base)} is counted once"
            + (f" ({rows:,} rows on record)" if isinstance(rows, int) else ""))
        return payload

    verdicts = []
    for other in others:
        candidates = _joins_between(build, base, other)
        if not candidates:
            entry = {"table": other, "short": build.short_table(other),
                     "rows": build.table_facts(other).get("total_rows"),
                     "verdict": "unwitnessed", "on": "", "source": "",
                     "support": 0, "scope": "",
                     "why": f"no join between {build.short_table(base)} and "
                            f"{build.short_table(other)} on record in this "
                            f"build: the relationship is not attested at all"}
        else:
            entry = {"table": other, "short": build.short_table(other),
                     "rows": build.table_facts(other).get("total_rows"),
                     "alternates": len(candidates) - 1,
                     **_judge(build, base, other, candidates[0])}
        verdicts.append(entry["verdict"])
        payload["joins"].append(entry)

    if "unsafe" in verdicts or "unwitnessed" in verdicts:
        payload["verdict"] = "unsafe"
    elif "unproven" in verdicts:
        payload["verdict"] = "unproven"

    short = build.short_table(base)
    count = f"{rows:,} " if isinstance(rows, int) else ""
    if payload["verdict"] == "safe":
        payload["headline"] = (
            f"{count}{short} rows stay {count.strip() or 'the same'} after "
            f"the join" if count else
            f"every {short} row is still counted once after the join")
    elif payload["verdict"] == "unproven":
        payload["headline"] = (
            f"this join may inflate totals: nothing on record says a "
            f"{short} row can match only once")
    else:
        payload["headline"] = (
            f"this join is not safe to run raw against {short}")
    return payload
