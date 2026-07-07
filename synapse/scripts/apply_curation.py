#!/usr/bin/env python3
"""Apply steward curation to an EXISTING snapshot — no re-compile.

Folds two curation surfaces into the graph in place:
  · briefings   semantic-graph/config/briefings/<table>.md
  · corrections semantic-graph/config/corrections.json

Both are also ingested automatically on every full compile; this script
exists so a briefing written today lands in today's snapshot without
re-running the pipeline (same philosophy as entities apply).

    python synapse/scripts/apply_curation.py
    python synapse/scripts/apply_curation.py --snapshot path/to/snap.json
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

from synapse.graph.briefings import ingest_briefings_dir  # noqa: E402
from synapse.graph.corrections import ingest_corrections_file  # noqa: E402
from synapse.graph.store import GraphStore  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="apply_curation")
    parser.add_argument(
        "--snapshot",
        default=str(SYNAPSE_ROOT / "data" / "cache" / "graph_snapshot.json"))
    parser.add_argument(
        "--briefings",
        default=str(REPO_ROOT / "semantic-graph" / "config" / "briefings"))
    parser.add_argument(
        "--corrections",
        default=str(REPO_ROOT / "semantic-graph" / "config"
                    / "corrections.json"))
    args = parser.parse_args(argv)

    snapshot = Path(args.snapshot).expanduser()
    if not snapshot.exists():
        print(f"✗ no snapshot at {snapshot}", file=sys.stderr)
        return 1
    store = GraphStore.load_json(snapshot)

    b = ingest_briefings_dir(store, Path(args.briefings).expanduser())
    print(f"briefings: {len(b['applied'])} applied"
          + (f" · skipped (table not in graph): "
             f"{', '.join(b['skipped_missing_table'])}"
             if b["skipped_missing_table"] else ""))
    for name in b["applied"]:
        print(f"  ✓ {name}")

    c = ingest_corrections_file(store, Path(args.corrections).expanduser())
    print(f"corrections: {c['applied']} applied"
          + (f" · skipped (column not in graph): "
             f"{len(c['skipped_missing_column'])}"
             if c["skipped_missing_column"] else ""))
    for miss in c["skipped_missing_column"][:5]:
        print(f"  ⏭ {miss}")

    if not b["applied"] and not c["applied"]:
        print("nothing applied — snapshot unchanged")
        return 0
    store.save_json(snapshot)
    print(f"snapshot updated in place → {snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
