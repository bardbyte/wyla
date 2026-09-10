"""End-to-end demo — synthetic sources → graph → inspect one table.

Run on this laptop with no real warehouse or LLM access:

    python synapse/scripts/build_demo_graph.py

What it does:
    1. Generates all 7 source artifacts under synapse/data/demo/
    2. Builds the in-memory typed graph
    3. Prints a structured inspection of the most-queried table —
       exactly the dict shape the UI / MCP tool would consume.

Optional flags:
    --inspect <table_name>   inspect a specific table
    --json                   emit JSON only (no terminal pretty-print)
    --save-graph <path>      dump the full graph as JSON for downstream tools
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.graph import build_graph_from_sources, inspect_table
from synapse.synthetic import generate_all_sources


DEFAULT_DEMO_DIR = SYNAPSE_ROOT / "data" / "demo"
DEFAULT_INSPECT_TABLE = "custins_customer_insights_cardmember"


def _hdr(msg: str) -> None: print(f"\n\033[1;36m═══ {msg} ═══\033[0m")
def _pass(msg: str) -> None: print(f"  \033[1;32m✓\033[0m {msg}")
def _info(msg: str) -> None: print(f"    \033[2m{msg}\033[0m")
def _sub(msg: str) -> None: print(f"\033[1;34m── {msg} ──\033[0m")


def _print_inspection(inspection: dict) -> None:
    if "error" in inspection:
        print(f"\033[1;31m✗\033[0m {inspection['error']}: {inspection.get('table')}")
        if inspection.get("available"):
            print(f"    available tables: {inspection['available'][:10]}")
        return

    ident = inspection["identity"]
    fused = inspection["fused_view"]
    _sub("Identity")
    print(f"  table:          {ident['table']}")
    print(f"  fqn:            {ident.get('fqn') or '—'}")
    print(f"  business_name:  {ident.get('business_name') or '—'}")
    print(f"  domain:         {ident.get('company_domain')} / {ident.get('data_domain')}")
    print(f"  in DMP:         {ident.get('is_in_dmp')}")

    _sub("Fused confidence")
    print(f"  tier:           {fused['confidence_tier']}")
    print(f"  score:          {fused['confidence_score']}")
    print(f"  n_sources:      {fused['n_sources_agree']}  → {fused['sources_contributed']}")
    print(f"  evidence_count: {fused['evidence_count']}")

    _sub("Per-source view (the '7-source breakdown' panel)")
    for src, view in inspection["per_source_view"].items():
        contrib = view.get("contributed")
        marker = "\033[1;32m✓\033[0m" if contrib else "\033[2m·\033[0m"
        n = view.get("evidence_count", 0)
        print(f"  {marker} {src.ljust(18)} ({n} event{'s' if n != 1 else ''})")
        # Show 1-2 key facts per source if contributed
        if contrib:
            for k, v in view.items():
                if k in {"contributed", "evidence_count", "note"}:
                    continue
                if v is None or v == "" or v == [] or v == 0:
                    continue
                disp = str(v) if len(str(v)) < 80 else str(v)[:77] + "..."
                print(f"        {k}: {disp}")

    _sub(f"Columns ({len(inspection['columns'])})")
    for c in inspection["columns"][:12]:
        flags = []
        if c["is_primary"]: flags.append("PK")
        if c["is_partitioning"]: flags.append("PART")
        if c["is_pii"]: flags.append(f"PII({c.get('pii_taxonomy')})")
        if c["is_coded"]: flags.append("CODED")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        print(f"  • {c['name']} ({c['data_type']}){flag_str}")
        print(f"      confidence: {c['confidence_tier']} ({c['confidence_score']}) "
              f"from {c['sources_contributed']}")
        if c.get("approx_distinct"):
            print(f"      cardinality: {c['cardinality_bucket']} (~{c['approx_distinct']:,})")
        if c.get("description"):
            desc = c["description"][:90]
            print(f"      desc: {desc}")
    if len(inspection["columns"]) > 12:
        print(f"  … and {len(inspection['columns']) - 12} more")

    _sub(f"Metrics ({len(inspection['metrics'])})")
    for m in inspection["metrics"]:
        print(f"  • {m['technical_name']} = {m['formula']}")
        print(f"      domain: {m.get('domain')}  grain: {m.get('grain')}")
        if m.get("synonyms"):
            print(f"      synonyms: {m['synonyms']}")
        print(f"      confidence: {m['confidence_tier']} from {m['sources_contributed']}")

    _sub(f"Related tables ({len(inspection['related_tables'])})")
    for r in inspection["related_tables"][:8]:
        print(f"  • {r['table']}  ({r['n_join_observations']} JOIN observation(s))")
        for lc in r["linking_columns"][:3]:
            print(f"      via {lc['from']} ↔ {lc['to']}")

    _sub("Usage")
    u = inspection["usage"]
    print(f"  total_queries_observed: {u['total_queries_observed']}")
    for top in u["top_users"][:5]:
        print(f"  • {top.get('team', '?')}: {top.get('email')}  "
              f"({top.get('query_count', 0)} queries)")

    _sub("Governance")
    g = inspection["governance"]
    print(f"  has_pii: {g['has_pii']}")
    if g["pii_columns"]:
        print("  pii_columns:")
        for p in g["pii_columns"][:6]:
            print(f"    • {p['name']}  ({p['pii_taxonomy']})")
    print(f"  owner: {g.get('owner_team') or '—'}")

    _sub("Data quality")
    dq = inspection["data_quality"]
    print(f"  completeness:    {dq['completeness_score']}")
    print(f"  consistency:     {dq['consistency_score']}")
    print(f"  freshness (hrs): {dq.get('freshness_hours')}")
    print(f"  cols described:  {dq.get('n_columns_described')}/{len(inspection['columns'])}")
    print(f"  cols multi-srcd: {dq.get('n_columns_multi_sourced')}/{len(inspection['columns'])}")

    _sub(f"Code resolutions ({len(inspection['code_resolutions'])})")
    for cm in inspection["code_resolutions"][:8]:
        print(f"  • {cm['column']} = '{cm['raw_value']}'  →  {cm['human_meaning']}  "
              f"({cm['source']}, {cm['confidence_tier']})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--demo-dir", type=Path, default=DEFAULT_DEMO_DIR,
        help=f"Where to write/read synthetic sources (default: {DEFAULT_DEMO_DIR})",
    )
    parser.add_argument(
        "--inspect", type=str, default=DEFAULT_INSPECT_TABLE,
        help=f"Table to inspect (default: {DEFAULT_INSPECT_TABLE})",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Emit inspection as raw JSON only (no pretty-print).",
    )
    parser.add_argument(
        "--save-graph", type=Path, default=None,
        help="Dump full graph as JSON to this path.",
    )
    parser.add_argument(
        "--skip-generate", action="store_true",
        help="Skip synthetic generation; assume --demo-dir already populated.",
    )
    args = parser.parse_args()

    if not args.skip_generate:
        _hdr("1. Generate synthetic sources")
        counts = generate_all_sources(args.demo_dir)
        for src, n in counts.items():
            _pass(f"{src}: {n}")
        _info(f"written to {args.demo_dir}")
    else:
        _hdr("1. Synthetic generation SKIPPED (--skip-generate)")

    _hdr("2. Build graph")
    store = build_graph_from_sources(args.demo_dir)
    stats = store.stats()
    _pass(f"{stats['n_nodes']} nodes, {stats['n_edges']} edges")
    for nt, n in sorted(stats["nodes_by_type"].items(), key=lambda kv: -kv[1]):
        _info(f"  {nt}: {n}")
    _sub("Confidence-tier distribution")
    for tier, n in sorted(stats["nodes_by_confidence_tier"].items(), key=lambda kv: -kv[1]):
        _info(f"  {tier}: {n}")

    if args.save_graph:
        args.save_graph.parent.mkdir(parents=True, exist_ok=True)
        args.save_graph.write_text(store.model_dump_json(indent=2), encoding="utf-8")
        _pass(f"graph dumped → {args.save_graph}")

    _hdr(f"3. Inspect: {args.inspect}")
    inspection = inspect_table(store, args.inspect)
    if args.json:
        print(json.dumps(inspection, indent=2, default=str))
    else:
        _print_inspection(inspection)
    return 0


if __name__ == "__main__":
    sys.exit(main())
