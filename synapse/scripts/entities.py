#!/usr/bin/env python3
"""Entity-layer bootstrap — propose from enrichment memory, steward
review, apply into the compiled snapshot. Zero LLM calls, no pipeline
re-run.

Laptop flow:

    # 1. reduce the memory your --enrich run already wrote (seconds)
    python synapse/scripts/entities.py propose \
        --memory synapse/data/cache/enrichment_memory.json \
        --out    synapse/data/cache/entity_review.yaml

    # 2. open entity_review.yaml, flip `approve: true` on the real
    #    business entities (~10 minutes)

    # 3. stamp them into the existing snapshot + persist the approvals
    #    so every future compile re-ingests them automatically
    python synapse/scripts/entities.py apply \
        --review   synapse/data/cache/entity_review.yaml \
        --snapshot synapse/data/cache/graph_snapshot.json \
        --save-approvals semantic-graph/config/entities.yaml

Thresholds default to the 5-table scope (an identifier seen in 2+
tables); the single-table-era behavior was min-tables 1 and the
50-table default is 3 — tune with --min-tables.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "synapse"))

from synapse.enrichment.enricher import propose_entities          # noqa: E402
from synapse.graph.entities import (                              # noqa: E402
    apply_entities, load_bundles_from_memory, read_approved,
    write_review_yaml,
)
from synapse.graph.store import GraphStore                        # noqa: E402


def cmd_propose(args: argparse.Namespace) -> int:
    bundles = load_bundles_from_memory(Path(args.memory))
    if not bundles:
        print(f"no bundles readable from {args.memory}", file=sys.stderr)
        return 1
    n_obs = sum(len(b.column_observations) for b in bundles.values())
    proposals = propose_entities(
        bundles,
        min_supporting_tables=args.min_tables,
        min_aggregate_confidence=args.min_confidence,
    )
    out = write_review_yaml(
        proposals, Path(args.out),
        meta={
            "memory": str(args.memory),
            "tables_in_memory": sorted(bundles),
            "observations_reduced": n_obs,
            "thresholds": {"min_tables": args.min_tables,
                           "min_confidence": args.min_confidence},
        })
    print(f"{len(proposals)} entity proposal(s) from {n_obs} observations "
          f"across {len(bundles)} table(s) → {out}")
    if not proposals:
        print("  (none cleared the bar — try --min-tables 1 to inspect "
              "single-table candidates, or re-run after the booster "
              "enrichment covers all 5 tables)")
    else:
        print("  next: flip `approve: true` on the real ones, then run "
              "the apply subcommand")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    approved = read_approved(Path(args.review))
    if not approved:
        print("nothing approved in the review file — flip `approve: true` "
              "on the entities you want, then re-run", file=sys.stderr)
        return 1
    store = GraphStore.load_json(Path(args.snapshot))
    report = apply_entities(store, approved)
    store.save_json(Path(args.snapshot))
    print(f"{report['entities_added']} entit(ies) → {args.snapshot}")
    for name, r in report["per_entity"].items():
        note = (f" · {r['skipped']} column ref(s) not in graph (skipped)"
                if r["skipped"] else "")
        print(f"  {name}: {r['edges']} IDENTIFIES edge(s){note}")
    print(f"  tier: human_asserted (source=human_approval, weight 10)")

    if args.save_approvals:
        # persist ONLY the approved entries — this file is witness #6 on
        # every future compile (pipeline --entities / auto-detected)
        approvals_path = Path(args.save_approvals)
        approvals_path.parent.mkdir(parents=True, exist_ok=True)
        import yaml
        approvals_path.write_text(
            yaml.safe_dump(
                {"meta": {"what": "steward-approved entities — ingested "
                                  "as witness #6 on every compile"},
                 "entities": [{k: v for k, v in e.items() if k != "approve"}
                              for e in approved]},
                sort_keys=False, allow_unicode=True, width=88),
            encoding="utf-8")
        print(f"  approvals persisted → {approvals_path} "
              "(future compiles re-ingest automatically)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="entities", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("propose", help="memory → review YAML (no LLM calls)")
    p.add_argument("--memory", required=True,
                   help="enrichment_memory.json from a --enrich run")
    p.add_argument("--out", required=True, help="review YAML to write")
    p.add_argument("--min-tables", type=int, default=2,
                   help="identifier must appear in N+ tables (default 2)")
    p.add_argument("--min-confidence", type=float, default=0.6,
                   help="aggregate self-confidence floor (default 0.6)")
    p.set_defaults(fn=cmd_propose)

    a = sub.add_parser("apply", help="approved review → snapshot + approvals")
    a.add_argument("--review", required=True, help="edited review YAML")
    a.add_argument("--snapshot", required=True,
                   help="graph_snapshot.json to stamp entities into")
    a.add_argument("--save-approvals",
                   default=str(REPO_ROOT / "semantic-graph" / "config"
                               / "entities.yaml"),
                   help="where to persist approved entities for future "
                        "compiles (default semantic-graph/config/"
                        "entities.yaml; pass '' to skip)")
    a.set_defaults(fn=cmd_apply)

    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
