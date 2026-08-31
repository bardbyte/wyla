#!/usr/bin/env python3
"""graph_state.py — one read-only look at where the graph stands.

    python scripts/graph_state.py            # from the silo root
    python scripts/graph_state.py --graph graph --builds builds

Prints: the promoted build and its counts, metrics by served status,
readiness, reviews, every enrichment run with its blind-gate line
(the A5 decision input), and where the Knowledge Files shelf resolves
from on THIS machine. Writes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.util.auth import load_dotenv  # noqa: E402

load_dotenv()          # .env paths ride along; shell exports win


def _aux(build_dir: Path, rel: str):
    p = build_dir / rel
    if not p.exists():
        return None
    if rel.endswith(".jsonl"):
        return [json.loads(ln) for ln in
                p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", default=os.environ.get(
        "MERIDIAN_GRAPH_DIR") or str(SILO / "graph"))
    ap.add_argument("--builds", default=os.environ.get(
        "MERIDIAN_BUILDS_DIR") or str(SILO / "builds"))
    args = ap.parse_args()
    graph = Path(args.graph)
    builds = Path(args.builds)

    print("═══ MERIDIAN GRAPH STATE ═══")

    # ── the promoted build ──
    current = builds / "CURRENT"
    if not current.exists():
        print(f"no promoted build: {current} missing — "
              "run `python scripts/laptop.py compile` first")
        return 1
    build_id = current.read_text(encoding="utf-8").strip()
    bdir = builds / build_id
    manifest = _aux(bdir, "manifest.json") or {}
    counts = manifest.get("counts", {})
    print(f"promoted build   {build_id}")
    print("counts           " + " · ".join(
        f"{k} {v}" for k, v in sorted(counts.items())))

    metrics = _aux(bdir, "indexes/metrics.jsonl") or []
    by_status: dict[str, int] = {}
    for row in metrics:
        s = row.get("status_served") or row.get("status") or "?"
        by_status[s] = by_status.get(s, 0) + 1
    print("metrics          " + " · ".join(
        f"{k} {v}" for k, v in sorted(
            by_status.items(), key=lambda kv: -kv[1])))
    certified = by_status.get("certified", 0)

    recon = manifest.get("table_reconciliation", {})
    if recon:
        print(f"tables           {recon.get('built', '?')} built of "
              f"{recon.get('crosswalk_rows', '?')} crosswalk rows"
              + (f" · {len([m for m in recon.get('missing', []) if m.get('intentionally_excluded')])}"
                 " excluded with reasons" if recon.get("missing") else ""))

    shelf = _aux(bdir, "indexes/sources.json") or {}
    for lob, r in (shelf.get("readiness") or {}).items():
        print(f"readiness        {lob}: {r['witnessed']}/{r['tables']} "
              f"tables with a witnessed metric ({r['pct']}%)")

    review_path = graph / "review.jsonl"
    if review_path.exists():
        rows = [json.loads(ln) for ln in
                review_path.read_text(encoding="utf-8").splitlines()
                if ln.strip()]
        open_reviews = sum(1 for r in rows
                           if r.get("props", {}).get("status", "open")
                           == "open")
        print(f"reviews          {open_reviews} open of {len(rows)}")

    # ── enrichment history: every run's blind-gate line ──
    print("\n─── enrichment runs (A5 blind gate: the decision input) ───")
    runs = sorted(graph.glob("runs/*/enrich_report.json"))
    if not runs:
        print("none recorded — the blind smoke has not been run on "
              "this graph yet")
    for rp in runs:
        rep = json.loads(rp.read_text(encoding="utf-8"))
        blind = rep.get("blind") or {}
        tier = blind.get("tier", "?")
        line = (f"{rp.parent.name:<14} prompt {rep.get('prompt_version', '?'):<6}"
                f" blind {blind.get('recovered', '?')}/{blind.get('n', '?')}"
                f" ({round(100 * blind.get('rate', 0))}%) → {tier}")
        if blind.get("leaky_contexts") is not None:
            line += f" · leaky {blind['leaky_contexts']}"
        line += (f" · wrote {rep.get('metrics_enriched', 0)} metrics"
                 f" + {rep.get('concepts_enriched', 0)} concepts")
        print(line)
    print(f"(gate: ≥80% batch · 60–79% item · <60% halt; blind set = "
          f"the {certified} certified metrics)")

    # ── where the Knowledge Files shelf resolves on this machine ──
    print("\n─── knowledge files shelf ───")
    src = os.environ.get("MERIDIAN_SOURCES_DIR")
    how = "MERIDIAN_SOURCES_DIR (env or .env)"
    if not src:
        if (SILO / "sources").exists():
            src, how = str(SILO / "sources"), "silo sources/"
        else:
            for mp in reversed(sorted(graph.glob("runs/*/manifest.json"))):
                try:
                    recorded = (json.loads(mp.read_text(encoding="utf-8"))
                                .get("roots") or {}).get("sources")
                except Exception:
                    continue
                if recorded:
                    src, how = recorded, f"recorded by {mp.parent.name}"
                    break
    if src and Path(src).exists():
        n = sum(1 for p in Path(src).rglob("*") if p.is_file())
        print(f"resolves to      {src}")
        print(f"via              {how} · {n} files under it")
    else:
        print("UNRESOLVED — paste MERIDIAN_SOURCES_DIR=/path/to/"
              "$DATA/sources into the silo .env (or re-run "
              "build-graph so the manifest records it)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
