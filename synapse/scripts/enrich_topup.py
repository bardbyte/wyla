#!/usr/bin/env python3
"""Top-up enrichment — finish what the budget cut, without touching
what the last run already established.

Loads the EXISTING snapshot + enrichment memory, computes which columns
were never observed (tables the budget skipped entirely, plus the
unreached remainder of partially-enriched wide tables), enriches ONLY
those through the same grounding gate, and reports the delta. Existing
facts are never re-written; the merged memory keeps every prior
observation.

Laptop flow:

    # see what's missing — ZERO Gemini calls
    python synapse/scripts/enrich_topup.py --plan

    # top it up (same Vertex env + TLS as the pipeline)
    python synapse/scripts/enrich_topup.py --max-calls 130

Progress prints live (per table, per call, rolling ETA), and every
finished table is checkpointed to _topup_memory_partial.json next to
the snapshot — if a run is interrupted, just rerun the same command:
the checkpoint is folded into memory + snapshot first and only the
true remainder spends calls.

Defaults point at the pipeline's artifact paths under
synapse/data/cache/; override any of them for non-default layouts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.enrichment.enricher import (          # noqa: E402
    _render_demo_script, collect_enrichment_failures, compute_topup_plan,
    enrich_graph, merge_memories, propose_entities,
)
from synapse.graph.entities import load_bundles_from_memory  # noqa: E402
from synapse.graph.inspector import context_readiness  # noqa: E402
from synapse.graph.store import GraphStore         # noqa: E402

CACHE = SYNAPSE_ROOT / "data" / "cache"


def _norm_q(text: str) -> str:
    return "".join(ch for ch in str(text).lower() if ch.isalnum())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="enrich_topup", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot", default=str(CACHE / "graph_snapshot.json"))
    parser.add_argument("--memory", default=str(CACHE / "enrichment_memory.json"))
    parser.add_argument("--evidence-dir",
                        default=str(CACHE / "sources" / "mdm_cache"),
                        help="staged session signals (real analyst SQL)")
    parser.add_argument("--demo", default=str(CACHE / "demo_questions.json"))
    parser.add_argument("--tables", default="",
                        help="comma list to restrict the top-up (applied "
                             "after the manifest scope)")
    parser.add_argument("--manifest",
                        default=str(REPO_ROOT / "semantic-graph" / "config"
                                    / "tables.yaml"),
                        help="tables.yaml scoping the top-up (default: the "
                             "repo manifest when it exists) — the graph can "
                             "contain out-of-scope tables staged by earlier "
                             "runs; only manifest tables get calls")
    parser.add_argument("--all-tables", action="store_true",
                        help="bypass the manifest scope and top up every "
                             "table the plan finds (spends calls on "
                             "out-of-scope tables — be sure)")
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--max-calls", type=int, default=200)
    parser.add_argument("--plan", action="store_true",
                        help="print the work plan and exit — no calls")
    args = parser.parse_args(argv)

    snapshot_path = Path(args.snapshot).expanduser()
    memory_path = Path(args.memory).expanduser()
    store = GraphStore.load_json(snapshot_path)
    old_bundles = (load_bundles_from_memory(memory_path)
                   if memory_path.exists() else {})

    # Resume: enrich_graph checkpoints each finished table to a partial
    # file. If a prior run died mid-flight, fold that checkpoint in —
    # facts re-applied to the snapshot, observations into memory — so an
    # interrupted run never re-buys the calls it already made.
    partial_path = snapshot_path.with_name("_topup_memory_partial.json")
    resume_note = None
    partial = (load_bundles_from_memory(partial_path)
               if partial_path.exists() else {})
    if partial and args.plan:
        resume_note = (f"interrupted-run checkpoint present "
                       f"({len(partial)} table(s)) — folded in when the "
                       "top-up runs")
    elif partial:
        from synapse.enrichment.enricher import _apply_bundle
        for bundle in partial.values():
            _apply_bundle(store, bundle)
        old_bundles = merge_memories(old_bundles, partial)
        memory_path.parent.mkdir(parents=True, exist_ok=True)
        memory_path.write_text(
            json.dumps({t: b.model_dump() for t, b in old_bundles.items()},
                       indent=2, default=str), encoding="utf-8")
        store.save_json(snapshot_path)
        partial_path.unlink()
        resume_note = (f"resumed interrupted run: {len(partial)} table(s) "
                       "folded into memory + snapshot — those calls are "
                       "not re-spent")

    plan = compute_topup_plan(store, old_bundles)
    scope_note = None

    # Manifest scope FIRST — the graph legitimately contains tables the
    # user never selected (join partners, artifacts staged by earlier
    # runs); calls are spent on manifest tables only unless --all-tables
    manifest_path = Path(args.manifest).expanduser() if args.manifest else None
    if not args.all_tables and manifest_path and manifest_path.exists():
        from synapse.graph.store import normalize_table_name
        from synapse.utils.manifest import read_tables_manifest
        manifest_names = {
            normalize_table_name(t["name"])
            for t in read_tables_manifest(manifest_path)
        }
        before = len(plan)
        plan = {t: cols for t, cols in plan.items()
                if normalize_table_name(t) in manifest_names}
        skipped = before - len(plan)
        if skipped:
            scope_note = (f"manifest scope ({manifest_path.name}): kept "
                          f"{len(plan)} of {before} planned table(s) — "
                          f"{skipped} out-of-scope skipped "
                          "(--all-tables to include)")

    if args.tables:
        wanted = {t.strip().lower() for t in args.tables.split(",") if t.strip()}
        plan = {t: cols for t, cols in plan.items() if t.lower() in wanted}

    print(f"\n═══ Top-up plan (snapshot {snapshot_path.name}) ═══")
    if resume_note:
        print(f"  {resume_note}")
    if scope_note:
        print(f"  {scope_note}")
    if not plan:
        print("  nothing to do — every table's columns are already observed")
        return 0
    total_cols = 0
    for table, remaining in sorted(plan.items()):
        est = -(-len(remaining) // max(1, args.batch_size))  # ceil
        covered = "never enriched" if table not in old_bundles else "partial"
        print(f"  {table:38} {len(remaining):>5} column(s) remaining "
              f"→ ~{est} call(s)  [{covered}]")
        total_cols += len(remaining)
    est_total = -(-total_cols // max(1, args.batch_size))
    print(f"  total: {total_cols} column(s) → ~{est_total} call(s) "
          f"(budget {args.max_calls})")
    if args.plan:
        return 0

    # ── run it — same client, same TLS, same gate as the pipeline ──
    from synapse.enrichment.vertex_client import VertexLLMClient
    try:
        client = VertexLLMClient()
    except RuntimeError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        return 1

    tiers_before = store.stats()["nodes_by_confidence_tier"]
    grounding: dict[str, dict] = {}
    demo_tmp = snapshot_path.with_name("_topup_demo.json")
    skip = {
        t.lower(): {
            obs.column_name
            for obs in old_bundles[t].column_observations
        }
        for t in old_bundles
    }
    new_bundles = enrich_graph(
        store, client,
        only_tables=sorted(plan),
        column_batch_size=args.batch_size,
        max_calls=args.max_calls,
        memory_out=partial_path,              # per-table crash checkpoint;
        grounding_reports=grounding,          # merged + removed at the end
        evidence_dir=Path(args.evidence_dir).expanduser(),
        demo_out=demo_tmp,
        skip_columns=skip,
        verbose=True,
        planned_calls=min(est_total, args.max_calls),
    )

    # ── gate summary + failure digest (never silent) ─────────
    totals: dict[str, int] = {}
    for report in grounding.values():
        for key, value in report.items():
            if isinstance(value, (int, float)):
                totals[key] = totals.get(key, 0) + int(value)
    n_obs = sum(len(b.column_observations) for b in new_bundles.values())
    print(f"\n  {len(new_bundles)} table(s) topped up · {n_obs} new column "
          "observations")
    print(f"  grounding gate: {totals.get('applied_descriptions', 0)} "
          f"descriptions applied · dropped "
          f"{totals.get('dropped_imagined_columns', 0)} imagined columns, "
          f"{totals.get('dropped_ungrounded_synonyms', 0)} ungrounded "
          f"synonyms · {totals.get('applied_relations', 0)} new relation(s)")
    print(f"  gemini: {client.stats.get('calls', 0)} call(s) · "
          f"{client.stats.get('corrective_retries', 0)} corrective · "
          f"{client.stats.get('call_retries', 0)} call retr(ies) · "
          f"{client.stats.get('context_truncations', 0)} truncation(s)")
    failures = collect_enrichment_failures(new_bundles)
    if failures["empty_bundles"]:
        print(f"  ⚠ {failures['empty_bundles']}/{failures['n_bundles']} "
              "bundle(s) empty — reasons:")
        for note, count in failures["notes"][:5]:
            print(f"      {count}× {note}")

    # ── merge artifacts: memory, demo pack ───────────────────
    merged = merge_memories(old_bundles, new_bundles)
    memory_path.write_text(
        json.dumps({t: b.model_dump() for t, b in merged.items()},
                   indent=2, default=str), encoding="utf-8")
    print(f"  memory merged → {memory_path} ({len(merged)} table(s), "
          "prior observations preserved)")

    demo_path = Path(args.demo).expanduser()
    old_demo = (json.loads(demo_path.read_text(encoding="utf-8"))
                if demo_path.exists() else {"verified": [], "held": []})
    new_demo = (json.loads(demo_tmp.read_text(encoding="utf-8"))
                if demo_tmp.exists() else {"verified": [], "held": []})
    if demo_tmp.exists():
        demo_tmp.unlink()
        demo_tmp.with_suffix(".md").unlink(missing_ok=True)
    seen = {_norm_q(q.get("question", "")) for q in old_demo["verified"]}
    added_q = [q for q in new_demo.get("verified", [])
               if _norm_q(q.get("question", "")) not in seen]
    old_demo["verified"].extend(added_q)
    old_demo["held"].extend(new_demo.get("held", []))
    demo_path.write_text(json.dumps(old_demo, indent=2, default=str),
                         encoding="utf-8")
    demo_path.with_suffix(".md").write_text(
        _render_demo_script(old_demo["verified"],
                            n_held=len(old_demo["held"])),
        encoding="utf-8")
    print(f"  demo pack: +{len(added_q)} verified question(s) → "
          f"{demo_path.with_suffix('.md')}")

    # ── entity proposals refresh (pure reduction over merged memory) ──
    proposals = propose_entities(merged)
    proposals_path = snapshot_path.with_name("entity_proposals.json")
    proposals_path.write_text(
        json.dumps([p.model_dump() for p in proposals], indent=2),
        encoding="utf-8")
    print(f"  entity proposals refreshed: {len(proposals)} → "
          f"{proposals_path} (re-run scripts/entities.py propose for the "
          "review file)")

    # ── save + the delta that makes the update legible ───────
    store.save_json(snapshot_path)
    partial_path.unlink(missing_ok=True)   # snapshot + memory now durable
    tiers_after = store.stats()["nodes_by_confidence_tier"]
    print(f"\n═══ Delta ═══")
    for tier in ("human_asserted", "grounded", "inferred", "guessed",
                 "deprecated"):
        b, a = tiers_before.get(tier, 0), tiers_after.get(tier, 0)
        if b or a:
            sign = f"+{a - b}" if a >= b else str(a - b)
            print(f"  {tier:16} {b:>6} → {a:<6} ({sign})")
    print(f"\n  readiness (topped-up tables):")
    print(f"  {'table':38} {'cols':>5} {'mean%':>5} {'rel':>4} tier")
    for row in context_readiness(store, sorted(plan)):
        if not row.get("in_graph"):
            continue
        print(f"  {row['table'][:38]:38} {row['n_columns']:>5} "
              f"{row['pct_columns_with_meaning']:>4}% "
              f"{row['n_related_tables']:>4} {row['confidence_tier']}")
    print(f"\n  snapshot updated in place → {snapshot_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
