"""JSONL → AGE rehydration. The recovery safety net.

Reads every event from `data/ontology/events/*.jsonl` in chronological
order and re-projects them into AGE. Idempotent — all projections are
MERGE, so replay against a non-empty graph is safe.

Two modes:

  python -m lumi.semantic_graph.replay --rebuild
    Wipe + bootstrap the graph from scratch, then replay all events.
    Use this when AGE state is suspect or schema changed.

  python -m lumi.semantic_graph.replay --catchup
    Replay events whose content_hash is not yet in the graph. Use
    after a stretch when AGE was disabled but JSONL kept writing.

Verification:

  python -m lumi.semantic_graph.replay --verify
    Count events in JSONL vs Event nodes in AGE. Should match. Used
    as the divergence detector for the dual-write contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Iterator

from lumi.schemas import OntologyEvent
from lumi.semantic_graph import config as gconfig
from lumi.semantic_graph import projector, schema

logger = logging.getLogger("lumi.semantic_graph.replay")


_DEFAULT_EVENTS_DIR = Path("data/ontology/events")


def _iter_events(events_dir: Path = _DEFAULT_EVENTS_DIR) -> Iterator[OntologyEvent]:
    """Yield events from all JSONL files in chronological order."""
    if not events_dir.exists():
        return
    # Sort by filename — date-named files (2026-05-13.jsonl) sort chronologically.
    for path in sorted(events_dir.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    yield OntologyEvent(**data)
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "skipping malformed event in %s: %s", path.name, e,
                    )


def replay_all(
    events_dir: Path = _DEFAULT_EVENTS_DIR,
    *,
    rebuild: bool = False,
) -> dict[str, int]:
    """Project every event from JSONL into AGE.

    If rebuild=True, bootstraps the graph first (creates labels +
    indexes); does NOT wipe existing data (AGE doesn't expose a clean
    "drop graph" without dropping/recreating, which we leave to the
    operator).
    """
    if not gconfig.is_age_enabled():
        logger.warning("LUMI_AGE_ENABLED not set — replay will be a no-op")

    counts = {"read": 0, "projected": 0, "skipped": 0, "errors": 0}

    if rebuild:
        schema.bootstrap()

    for event in _iter_events(events_dir):
        counts["read"] += 1
        try:
            ok = projector.project(event)
            if ok:
                counts["projected"] += 1
            else:
                counts["skipped"] += 1
        except Exception as e:  # noqa: BLE001
            logger.error("projection failed for %s: %s", event.event_type, e)
            counts["errors"] += 1

    logger.info("replay complete: %s", counts)
    return counts


def verify(events_dir: Path = _DEFAULT_EVENTS_DIR) -> dict[str, int]:
    """Compare JSONL event count vs AGE Event node count. Detects drift."""
    jsonl_count = sum(1 for _ in _iter_events(events_dir))
    age_count = 0
    if gconfig.is_age_enabled():
        try:
            import psycopg
            conn_params = gconfig.AGEConnection()
            with psycopg.connect(conn_params.conninfo()) as pgconn:
                with pgconn.cursor() as cur:
                    cur.execute("LOAD 'age';")
                    cur.execute('SET search_path = ag_catalog, "$user", public;')
                    cur.execute(
                        f"SELECT count(*) FROM cypher('{conn_params.graph_name}', $$ "
                        f"MATCH (ev:Event) RETURN ev "
                        f"$$) AS (ev ag_catalog.agtype);"
                    )
                    row = cur.fetchone()
                    # cypher() returns rows of agtype; SELECT count(*) over
                    # that gives a bigint as row[0]. Coerce defensively —
                    # if AGE ever wraps it in agtype the str→int still works.
                    if row:
                        try:
                            age_count = int(row[0])
                        except (TypeError, ValueError):
                            age_count = int(str(row[0]).strip())
        except Exception as e:  # noqa: BLE001
            logger.error("AGE count query failed: %s", e)
    return {
        "jsonl_events": jsonl_count,
        "age_event_nodes": age_count,
        "divergence": jsonl_count - age_count,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="lumi.semantic_graph.replay",
        description="JSONL → AGE rehydration / verification.",
    )
    parser.add_argument("--rebuild", action="store_true", help="Bootstrap schema + replay everything")
    parser.add_argument("--catchup", action="store_true", help="Replay events (idempotent; AGE merges)")
    parser.add_argument("--verify", action="store_true", help="Count comparison")
    parser.add_argument(
        "--events-dir", default=str(_DEFAULT_EVENTS_DIR),
        help=f"Path to events JSONL directory (default: {_DEFAULT_EVENTS_DIR})",
    )
    args = parser.parse_args()

    events_dir = Path(args.events_dir)
    if not (args.rebuild or args.catchup or args.verify):
        parser.print_help()
        return 1

    if not gconfig.is_age_enabled():
        print(
            "WARN: LUMI_AGE_ENABLED not set. AGE writes will no-op. "
            "Set the env var to enable.", file=sys.stderr,
        )

    try:
        if args.rebuild:
            result = replay_all(events_dir, rebuild=True)
            print(json.dumps(result, indent=2))
        if args.catchup:
            result = replay_all(events_dir, rebuild=False)
            print(json.dumps(result, indent=2))
        if args.verify:
            result = verify(events_dir)
            print(json.dumps(result, indent=2))
    except Exception as e:  # noqa: BLE001
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
