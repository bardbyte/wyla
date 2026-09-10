#!/usr/bin/env python3
"""Grade the graph's retrieval accuracy against its own gold set.

    python synapse/scripts/eval_retrieval.py --graph out/graph_snapshot.json
    python synapse/scripts/eval_retrieval.py --graph snap.json \
        --report out/eval_retrieval.json --gold-out out/gold_set.json \
        --fail-under-mrr 0.8

The gold set is extracted from the snapshot itself (DMP curated
questions, mined measure names, business names, business units) — see
synapse.evals.retrieval for what each kind proves. Prints the scoreboard;
--report persists the full JSON (including diagnosable failures) so runs
can be diffed. --fail-under-mrr gates CI: exit 1 when overall MRR drops
below the bar, exit 0 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SYNAPSE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.evals.retrieval import (  # noqa: E402
    evaluate_retrieval,
    extract_gold_set,
    format_report,
    gold_to_json,
)
from synapse.graph.store import GraphStore  # noqa: E402
from synapse.mcp.service import GraphService  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retrieval eval over a compiled graph snapshot")
    parser.add_argument("--graph", required=True,
                        help="compiled graph snapshot JSON")
    parser.add_argument("--top-k", type=int, default=10,
                        help="search depth per query (default 10)")
    parser.add_argument("--kinds", default="",
                        help="comma-separated gold kinds to run "
                             "(default: all)")
    parser.add_argument("--report", default="",
                        help="write the full JSON report here")
    parser.add_argument("--gold-out", default="",
                        help="write the extracted gold set here "
                             "(inspect/extend it by hand)")
    parser.add_argument("--fail-under-mrr", type=float, default=None,
                        help="exit 1 if overall MRR falls below this bar")
    args = parser.parse_args()

    snap = Path(args.graph).expanduser()
    if not snap.exists():
        raise SystemExit(f"--graph: no snapshot at {snap}")
    store = GraphStore.load_json(snap)
    service = GraphService(store)

    gold = extract_gold_set(store)
    if args.kinds:
        wanted = {k.strip() for k in args.kinds.split(",") if k.strip()}
        gold = [g for g in gold if g.kind in wanted]
    if not gold:
        raise SystemExit(
            "no gold examples extractable from this snapshot — ingest the "
            "DMP/measures catalogs (or business names) first")

    if args.gold_out:
        out = Path(args.gold_out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(gold_to_json(gold), indent=2),
                       encoding="utf-8")
        print(f"gold set → {out}  ({len(gold)} examples)")

    report = evaluate_retrieval(service, gold, top_k=args.top_k)
    print(format_report(report))

    if args.report:
        out = Path(args.report).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"report → {out}")

    bar = args.fail_under_mrr
    if bar is not None and report["overall"]["mrr"] < bar:
        print(f"FAIL: overall MRR {report['overall']['mrr']} < {bar}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
