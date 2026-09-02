#!/usr/bin/env python3
"""build_snapshot.py — freeze what a promoted build gives the agent, then
compare two freezes: better or worse, fact by fact.

    # BEFORE touching anything (the build dir is keyed by GRAPH hash,
    # so a recompile with new compiler code overwrites it in place —
    # freeze first or the "before" is gone):
    python scripts/build_snapshot.py snapshot --out before.json

    # after build-graph + compile on the new code:
    python scripts/build_snapshot.py snapshot --out after.json

    python scripts/build_snapshot.py compare before.json after.json

Reads ONLY the build directory (builds/CURRENT by default, or --builds
pointing at a builds root or a single build dir). Works on any build
shape — a build that predates the facts row / coverage ledger reports
those as absent, never as zero. The agent-visibility matrix is read
off the CARDS THEMSELVES (the text the agent is served), so it is the
same instrument on every build, old or new.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]

# ── what the agent can see on a table card — one regex per fact ──
# (matched line by line against the served card; presence, not prose)
CARD_FACTS: dict[str, str] = {
    "business_name":      r"^# table \S+ — .+",
    "purpose":            r"^- purpose: ",
    "business_unit":      r"^- .*business unit: (?!\?)",
    "line_of_business":   r"^- line of business: ",
    "used_by":            r"^- used by: ",
    "owner_chain":        r"^- owner: .*\(.*;.*\)",
    "lives_at":           r"^- lives at: ",
    "primary_key":        r"^- primary key: (?!none)",
    "partition_column":   r"^- partitioned by ",
    "load_type":          r"load: \S+",
    "size_bytes":         r"^- rows ≈ .* · \d+(\.\d+)? [KMG]?B ·",
    "answerability":      r"^- answerability: ",
    "freshness":          r"last modified ",
    "feed":               r"feed \S+",
    "cost_prior":         r"^- cost prior: ",
    "usage_rhythm":       r"^- usage rhythm: ",
    "top_users":          r"^- top users: ",
    "atlas_flags":        r"^- atlas flags: ",
    "declared_fk":        r"^- declared: .* → ",
    "observed_join":      r"co-queries",
    "lineage":            r"^## lineage",
    "access_flags":       r"^- table flags: ",
    "sensitive_roles":    r"^- sensitive columns: .*\(",
    "vocabulary":         r"^## vocabulary",
    "conflicts_listed":   r"^## conflicts",
}
# per-column facts: counted, not just present
COLUMN_FACTS: dict[str, str] = {
    "column_business_names": r"^- \S+ .*“.+”",
    "column_domains":        r"→ sample_values",
    "column_terms":          r"term: .+ — ",
    "column_pk_markers":     r"^- \S+ .*\(.*PK",
    "column_partition":      r"^- \S+ .*\(.*PARTITION",
    "column_sensitive_role": r"SENSITIVE \S",
    "column_computed":       r"computed: `",
    "column_lineage":        r"derived from ",
    "column_profile":        r"~\d+ distinct",
}
HIGHER_IS_BETTER = {
    "tables", "metrics", "metrics_certified", "joins", "joins_declared",
    "lobs", "lob_cards", "vocab", "coverage_rendered",
    "card_fact_coverage_pct", "column_fact_hits", "tables_with_facts_row",
    "facts_family_density_pct",
}
LOWER_IS_BETTER = {"coverage_unaccounted", "cards_over_budget",
                   "cards_budget_dropped"}


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in
            path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) \
        if path.exists() else None


def resolve_build(builds: Path) -> Path:
    builds = Path(builds)
    if (builds / "manifest.json").exists():
        return builds
    current = builds / "CURRENT"
    if current.exists():
        return builds / current.read_text(encoding="utf-8").strip()
    raise SystemExit(f"no build at {builds}: expected a build dir "
                     "(manifest.json) or a builds root with CURRENT")


def snapshot(build_dir: Path) -> dict[str, Any]:
    manifest = _json(build_dir / "manifest.json") or {}
    counts = manifest.get("counts", {})
    metrics = _jsonl(build_dir / "indexes" / "metrics.jsonl")
    joins = _jsonl(build_dir / "indexes" / "joins.jsonl")
    tables = _jsonl(build_dir / "indexes" / "tables.jsonl")
    lobs = _jsonl(build_dir / "indexes" / "lob.jsonl")
    coverage = _json(build_dir / "indexes" / "coverage.json")
    census = _json(build_dir / "census.json") or {}
    tickets = _jsonl(build_dir / "tickets.jsonl")

    by_status: dict[str, int] = {}
    for row in metrics:
        key = row.get("status_served") or row.get("status") or "?"
        by_status[key] = by_status.get(key, 0) + 1
    joins_by_source: dict[str, int] = {}
    for j in joins:
        joins_by_source[j.get("source", "?")] = \
            joins_by_source.get(j.get("source", "?"), 0) + 1

    # ── cards: what the agent is actually served ──
    cards_dir = build_dir / "cards"
    card_counts = {kind: len(list((cards_dir / kind).glob("*.md")))
                   if (cards_dir / kind).exists() else 0
                   for kind in ("tables", "metrics", "concepts", "lob")}
    per_table: dict[str, dict[str, Any]] = {}
    fact_hits: dict[str, int] = {k: 0 for k in CARD_FACTS}
    column_hits: dict[str, int] = {k: 0 for k in COLUMN_FACTS}
    token_total = 0
    for card in sorted((cards_dir / "tables").glob("*.md")) \
            if (cards_dir / "tables").exists() else []:
        text = card.read_text(encoding="utf-8")
        lines = text.splitlines()
        tokens = max(1, len(text) // 4)
        token_total += tokens
        present = {
            name: any(re.search(rx, ln) for ln in lines)
            for name, rx in CARD_FACTS.items()}
        for name, hit in present.items():
            fact_hits[name] += int(hit)
        col_counts = {
            name: sum(1 for ln in lines if re.search(rx, ln))
            for name, rx in COLUMN_FACTS.items()}
        for name, n in col_counts.items():
            column_hits[name] += n
        sections = [ln[3:].strip() for ln in lines
                    if ln.startswith("## ")]
        per_table[card.stem.replace("__", ".")] = {
            "tokens": tokens, "sections": sections,
            "facts_present": sorted(k for k, v in present.items() if v),
            "column_facts": col_counts}
    n_cards = len(per_table)
    fact_pct = (round(100 * sum(fact_hits.values())
                      / (n_cards * len(CARD_FACTS)))
                if n_cards else 0)

    # ── the facts row (new builds) — family density ──
    families = ("identity", "business", "operations", "trust", "access",
                "column_facts", "joins", "lineage", "vocabulary")
    with_facts = [t for t in tables if t.get("schema", "").startswith(
        "meridian.table_facts")]
    density = None
    if with_facts:
        filled = sum(
            1 for t in with_facts for fam in families if t.get(fam))
        density = round(100 * filled / (len(with_facts) * len(families)))

    budget = manifest.get("budget", {})
    return {
        "schema": "meridian.build_snapshot/1",
        "build_id": manifest.get("build_id", build_dir.name),
        "build_dir": str(build_dir),
        "scalars": {
            "tables": counts.get("tables", len(per_table)),
            "metrics": counts.get("metrics", len(metrics)),
            "metrics_certified": by_status.get("certified", 0),
            "joins": len(joins),
            "joins_declared": joins_by_source.get("constraints", 0),
            "lobs": counts.get("lobs", len(lobs)),
            "lob_cards": card_counts["lob"],
            "vocab": counts.get("vocab"),
            "tickets": len(tickets),
            "cards_over_budget": budget.get("over_budget", 0),
            "cards_budget_dropped": len(budget.get("dropped", {})),
            "card_tokens_avg": (round(token_total / n_cards)
                                if n_cards else 0),
            "card_fact_coverage_pct": fact_pct,
            "column_fact_hits": sum(column_hits.values()),
            "tables_with_facts_row": len(with_facts),
            "facts_family_density_pct": density,
            "coverage_rendered": (
                sum(coverage[k]["summary"]["rendered"] for k in (
                    "table_props", "column_props", "edge_predicates"))
                if coverage else None),
            "coverage_unaccounted": (len(coverage["unaccounted"])
                                     if coverage else None),
        },
        "metrics_by_status": by_status,
        "joins_by_source": joins_by_source,
        "structural_totals": (census.get("structural") or {}).get(
            "totals", manifest.get("structural_totals", {})),
        "readiness": (_json(build_dir / "indexes" / "sources.json")
                      or {}).get("readiness", {}),
        "card_counts": card_counts,
        "card_facts": fact_hits,
        "column_facts": column_hits,
        "per_table": per_table,
        "coverage_present": coverage is not None,
        "facts_row_present": bool(with_facts),
    }


def _fmt(v: Any) -> str:
    return "absent" if v is None else str(v)


def print_snapshot(snap: dict[str, Any]) -> None:
    s = snap["scalars"]
    print(f"═══ BUILD SNAPSHOT · {snap['build_id']} ═══")
    print(f"dir              {snap['build_dir']}")
    print("counts           " + " · ".join(
        f"{k} {_fmt(s[k])}" for k in (
            "tables", "metrics", "metrics_certified", "joins",
            "joins_declared", "lobs", "lob_cards", "vocab", "tickets")))
    print("metrics          " + " · ".join(
        f"{k} {v}" for k, v in sorted(snap["metrics_by_status"].items(),
                                      key=lambda kv: -kv[1])))
    print("joins by source  " + (" · ".join(
        f"{k} {v}" for k, v in sorted(snap["joins_by_source"].items()))
        or "none"))
    if snap["structural_totals"]:
        print("structural       " + " · ".join(
            f"{k} {v}" for k, v in sorted(
                snap["structural_totals"].items())))
    for lob, r in sorted(snap["readiness"].items()):
        print(f"readiness        {lob}: {r['witnessed']}/{r['tables']} "
              f"({r['pct']}%)")
    print()
    print("─── what the agent is served (read off the cards) ───")
    print(f"cards            " + " · ".join(
        f"{k} {v}" for k, v in snap["card_counts"].items())
        + f" · avg {s['card_tokens_avg']} tokens/table card · "
          f"over budget {s['cards_over_budget']} · "
          f"budget-dropped {s['cards_budget_dropped']}")
    n = max(1, snap["card_counts"]["tables"])
    print(f"table-fact coverage {s['card_fact_coverage_pct']}% "
          f"({len(CARD_FACTS)} facts × {n} cards)")
    for name, hit in snap["card_facts"].items():
        bar = "█" * hit + "·" * (n - hit)
        print(f"  {name:<20} {bar} {hit}/{n}")
    print(f"column facts     " + " · ".join(
        f"{k.replace('column_', '')} {v}"
        for k, v in snap["column_facts"].items()))
    print()
    print("─── compiled instruments ───")
    print(f"facts row        "
          + (f"{s['tables_with_facts_row']} tables · family density "
             f"{s['facts_family_density_pct']}%"
             if snap["facts_row_present"] else
             "absent (build predates indexes/tables.jsonl facts)"))
    print(f"coverage ledger  "
          + (f"{s['coverage_rendered']} rendered · "
             f"{s['coverage_unaccounted']} unaccounted"
             if snap["coverage_present"] else
             "absent (build predates indexes/coverage.json)"))


def compare(before: dict[str, Any], after: dict[str, Any]) -> int:
    print(f"═══ COMPARE · {before['build_id']} → {after['build_id']} ═══")
    print(f"{'metric':<28}{'before':>10}{'after':>10}{'delta':>9}  verdict")
    better = worse = 0
    for key in before["scalars"]:
        b, a = before["scalars"].get(key), after["scalars"].get(key)
        if b is None and a is None:
            continue
        if b is None or a is None:
            delta = "new" if b is None else "gone"
            verdict = ("▲ better" if (b is None and key in
                                      HIGHER_IS_BETTER) or
                       (b is None and key in LOWER_IS_BETTER
                        and a == 0) else "")
        else:
            d = a - b
            delta = f"{d:+d}" if isinstance(d, int) else f"{d:+.1f}"
            if d == 0:
                verdict = "="
            elif key in HIGHER_IS_BETTER:
                verdict = "▲ better" if d > 0 else "▼ worse"
            elif key in LOWER_IS_BETTER:
                verdict = "▲ better" if d < 0 else "▼ worse"
            else:
                verdict = "~ changed"
        better += verdict.startswith("▲")
        worse += verdict.startswith("▼")
        print(f"{key:<28}{_fmt(b):>10}{_fmt(a):>10}{delta:>9}  {verdict}")

    print()
    print("─── per-fact: how many table cards carry it ───")
    n_b = max(1, before["card_counts"]["tables"])
    n_a = max(1, after["card_counts"]["tables"])
    for name in CARD_FACTS:
        b = before["card_facts"].get(name, 0)
        a = after["card_facts"].get(name, 0)
        mark = "▲" if a / n_a > b / n_b else "▼" if a / n_a < b / n_b \
            else " "
        print(f"  {mark} {name:<20} {b}/{n_b} → {a}/{n_a}")
    print("─── per-column facts (total mentions) ───")
    for name in COLUMN_FACTS:
        b = before["column_facts"].get(name, 0)
        a = after["column_facts"].get(name, 0)
        mark = "▲" if a > b else "▼" if a < b else " "
        print(f"  {mark} {name.replace('column_', ''):<20} {b} → {a}")

    # tables that lost anything: the honest tail
    lost = []
    for physical, row in before["per_table"].items():
        now = after["per_table"].get(physical)
        if now is None:
            lost.append(f"{physical}: card GONE")
            continue
        missing = set(row["facts_present"]) - set(now["facts_present"])
        if missing:
            lost.append(f"{physical}: lost {', '.join(sorted(missing))}")
    print()
    if lost:
        print("⚠ regressions on specific tables:")
        for line in lost:
            print("  " + line)
    else:
        print("no table lost a fact it previously carried")
    print()
    print(f"verdict: {better} better · {worse} worse · "
          + ("BETTER" if better and not worse and not lost else
             "MIXED — read the rows above" if better else
             "NO IMPROVEMENT"))
    return 0 if not worse and not lost else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_snap = sub.add_parser("snapshot", help="freeze one build")
    p_snap.add_argument("--builds", default=str(SILO / "builds"),
                        help="builds root (uses CURRENT) or a build dir")
    p_snap.add_argument("--out", default="",
                        help="write the snapshot JSON here")
    p_snap.add_argument("--quiet", action="store_true")
    p_cmp = sub.add_parser("compare", help="before vs after")
    p_cmp.add_argument("before")
    p_cmp.add_argument("after")
    args = parser.parse_args(argv)

    if args.command == "snapshot":
        snap = snapshot(resolve_build(Path(args.builds)))
        if not args.quiet:
            print_snapshot(snap)
        if args.out:
            Path(args.out).write_text(
                json.dumps(snap, indent=1, sort_keys=True) + "\n",
                encoding="utf-8")
            print(f"\nwrote {args.out}")
        return 0
    before = json.loads(Path(args.before).read_text(encoding="utf-8"))
    after = json.loads(Path(args.after).read_text(encoding="utf-8"))
    return compare(before, after)


if __name__ == "__main__":
    sys.exit(main())
