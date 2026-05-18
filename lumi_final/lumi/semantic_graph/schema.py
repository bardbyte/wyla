"""Apache AGE schema bootstrap — idempotent DDL.

Run via:

    python -m lumi.semantic_graph.schema --bootstrap

Creates (idempotently):
  1. The AGE graph (``lumi_semantic`` by default)
  2. All vertex labels (15 node types from config.NODE_LABELS)
  3. All edge labels (15 edge types from config.EDGE_LABELS)
  4. GIN indexes on hot ``properties->>`` paths for fast lookup

Safe to re-run: each statement uses CREATE IF NOT EXISTS semantics or
catches the "already exists" error. Production-grade: a fresh database
+ this script + replay.py → fully rebuilt graph in seconds.
"""

from __future__ import annotations

import argparse
import logging
import sys
from contextlib import contextmanager
from typing import Any, Iterator

from lumi.semantic_graph import config as gconfig

logger = logging.getLogger("lumi.semantic_graph.schema")


# ─── Connection helper ──────────────────────────────────────


@contextmanager
def _connect(conn: gconfig.AGEConnection | None = None) -> Iterator[Any]:
    """Yield a psycopg connection with AGE loaded and search_path set.

    psycopg is imported lazily so this module imports fine even when
    AGE isn't installed (CI without Postgres).
    """
    try:
        import psycopg
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required for AGE operations. "
            "Install via `pip install 'psycopg[binary]'`."
        ) from e

    conn_params = conn or gconfig.AGEConnection()
    with psycopg.connect(conn_params.conninfo()) as pgconn:
        pgconn.autocommit = False
        with pgconn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
        try:
            yield pgconn
        finally:
            pgconn.commit()


# ─── DDL operations ─────────────────────────────────────────


def create_graph(conn_params: gconfig.AGEConnection | None = None) -> bool:
    """Create the AGE graph if it doesn't exist. Returns True on creation,
    False if already exists."""
    conn_params = conn_params or gconfig.AGEConnection()
    with _connect(conn_params) as pgconn:
        with pgconn.cursor() as cur:
            # ag_catalog.ag_graph holds known graphs.
            cur.execute(
                "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s;",
                (conn_params.graph_name,),
            )
            if cur.fetchone():
                logger.info("AGE graph '%s' already exists", conn_params.graph_name)
                return False
            cur.execute(
                "SELECT ag_catalog.create_graph(%s);",
                (conn_params.graph_name,),
            )
            logger.info("AGE graph '%s' created", conn_params.graph_name)
            return True


def create_labels(conn_params: gconfig.AGEConnection | None = None) -> dict[str, int]:
    """Create every vlabel and elabel. Idempotent — catches duplicates.

    Returns counts: {"vlabels_created": N, "elabels_created": M}.
    """
    conn_params = conn_params or gconfig.AGEConnection()
    counts = {"vlabels_created": 0, "elabels_created": 0, "vlabels_existing": 0, "elabels_existing": 0}
    with _connect(conn_params) as pgconn:
        for label in gconfig.NODE_LABELS:
            created = _create_vlabel(pgconn, conn_params.graph_name, label)
            counts["vlabels_created" if created else "vlabels_existing"] += 1
        for label in gconfig.EDGE_LABELS:
            created = _create_elabel(pgconn, conn_params.graph_name, label)
            counts["elabels_created" if created else "elabels_existing"] += 1
    logger.info("labels: %s", counts)
    return counts


def _create_vlabel(pgconn: Any, graph: str, label: str) -> bool:
    """Create a vertex label; return True on creation, False if exists."""
    with pgconn.cursor() as cur:
        try:
            cur.execute(
                "SELECT ag_catalog.create_vlabel(%s, %s);",
                (graph, label),
            )
            return True
        except Exception as e:  # noqa: BLE001
            # AGE raises a specific error if label exists. Roll back the
            # failed statement so the transaction stays usable.
            pgconn.rollback()
            if "already exists" in str(e).lower():
                return False
            raise


def _create_elabel(pgconn: Any, graph: str, label: str) -> bool:
    """Create an edge label; return True on creation, False if exists."""
    with pgconn.cursor() as cur:
        try:
            cur.execute(
                "SELECT ag_catalog.create_elabel(%s, %s);",
                (graph, label),
            )
            return True
        except Exception as e:  # noqa: BLE001
            pgconn.rollback()
            if "already exists" in str(e).lower():
                return False
            raise


def create_indexes(conn_params: gconfig.AGEConnection | None = None) -> dict[str, int]:
    """Create GIN indexes on hot lookup paths.

    AGE stores node properties as JSONB inside the vertex table; GIN
    indexes on ``properties`` accelerate ``WHERE properties->>'key' = ?``
    lookups. Btree indexes on specific keys are faster but less flexible;
    GIN is the default for evolving schemas.
    """
    conn_params = conn_params or gconfig.AGEConnection()
    counts = {"indexes_created": 0, "indexes_existing": 0, "indexes_failed": 0}
    with _connect(conn_params) as pgconn:
        graph = conn_params.graph_name
        for label in gconfig.NODE_LABELS:
            idx_name = f"ix_{graph}_{label.lower()}_props"
            sql = (
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                f'ON {graph}."{label}" '
                f'USING gin (properties);'
            )
            with pgconn.cursor() as cur:
                try:
                    cur.execute(sql)
                    counts["indexes_created"] += 1
                except Exception as e:  # noqa: BLE001
                    pgconn.rollback()
                    if "already exists" in str(e).lower():
                        counts["indexes_existing"] += 1
                    else:
                        counts["indexes_failed"] += 1
                        logger.warning("index %s failed: %s", idx_name, e)
    logger.info("indexes: %s", counts)
    return counts


def bootstrap(conn_params: gconfig.AGEConnection | None = None) -> dict[str, Any]:
    """Full bootstrap: graph + labels + indexes. Idempotent.

    Returns a structured summary suitable for printing in the probe script.
    """
    conn_params = conn_params or gconfig.AGEConnection()
    summary: dict[str, Any] = {
        "graph_name": conn_params.graph_name,
        "schema_version": gconfig.SCHEMA_VERSION,
        "tenant_id": gconfig.default_tenant_id(),
    }
    summary["graph_created"] = create_graph(conn_params)
    summary["labels"] = create_labels(conn_params)
    summary["indexes"] = create_indexes(conn_params)
    return summary


def verify(conn_params: gconfig.AGEConnection | None = None) -> dict[str, Any]:
    """Read-only verification that the schema is in place.

    Returns: {graph_exists, vlabels_present[], elabels_present[],
              vlabels_missing[], elabels_missing[]}
    """
    conn_params = conn_params or gconfig.AGEConnection()
    result: dict[str, Any] = {
        "graph_name": conn_params.graph_name,
        "graph_exists": False,
        "vlabels_present": [],
        "vlabels_missing": [],
        "elabels_present": [],
        "elabels_missing": [],
    }
    with _connect(conn_params) as pgconn:
        with pgconn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s;",
                (conn_params.graph_name,),
            )
            result["graph_exists"] = cur.fetchone() is not None
            if not result["graph_exists"]:
                result["vlabels_missing"] = list(gconfig.NODE_LABELS)
                result["elabels_missing"] = list(gconfig.EDGE_LABELS)
                return result

            # AGE keeps labels in ag_catalog.ag_label, scoped by graph oid.
            cur.execute(
                "SELECT graphid FROM ag_catalog.ag_graph WHERE name = %s;",
                (conn_params.graph_name,),
            )
            graph_oid = cur.fetchone()[0]
            cur.execute(
                "SELECT name, kind FROM ag_catalog.ag_label "
                "WHERE graph = %s;",
                (graph_oid,),
            )
            rows = cur.fetchall()
            present_v = {n for n, k in rows if k == "v"}
            present_e = {n for n, k in rows if k == "e"}
            result["vlabels_present"] = sorted(present_v & set(gconfig.NODE_LABELS))
            result["vlabels_missing"] = sorted(set(gconfig.NODE_LABELS) - present_v)
            result["elabels_present"] = sorted(present_e & set(gconfig.EDGE_LABELS))
            result["elabels_missing"] = sorted(set(gconfig.EDGE_LABELS) - present_e)
    return result


# ─── CLI ─────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="lumi.semantic_graph.schema",
        description="Bootstrap or verify the LUMI semantic graph in Apache AGE.",
    )
    parser.add_argument("--bootstrap", action="store_true", help="Create graph + labels + indexes (idempotent)")
    parser.add_argument("--verify", action="store_true", help="Read-only schema verification")
    args = parser.parse_args()

    if not (args.bootstrap or args.verify):
        parser.print_help()
        return 1

    if not gconfig.is_age_enabled():
        print(
            "WARN: LUMI_AGE_ENABLED is not set. Proceeding anyway since this "
            "is a direct schema command, but downstream writers will no-op.",
            file=sys.stderr,
        )

    import json as _json
    try:
        if args.bootstrap:
            print(_json.dumps(bootstrap(), indent=2, default=str))
        if args.verify:
            print(_json.dumps(verify(), indent=2, default=str))
    except Exception as e:  # noqa: BLE001
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
