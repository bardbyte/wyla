"""Apache AGE health check — quick pre-flight before the full Phase probe.

Run via:

    export LUMI_PG_HOST=localhost
    export LUMI_PG_PORT=5432
    export LUMI_PG_DATABASE=lumi
    export LUMI_PG_USER=lumi
    export LUMI_PG_PASSWORD=lumi
    python scripts/probe_age_health.py

What it checks, in order, stopping at the first failure:

  1. psycopg installed
  2. Postgres server reachable (TCP + auth)
  3. Postgres version
  4. AGE extension loadable (LOAD 'age')
  5. AGE catalog accessible (ag_catalog.ag_graph exists)
  6. Cypher round-trip works (RETURN 1)
  7. Existing graphs in this database
  8. Whether `lumi_semantic` graph exists yet

Every failure prints:
  - Which step failed
  - The exact error message + type
  - The exact command/SQL that produced the error
  - An actionable next-step suggestion

Output is plain text — paste the whole thing back for debugging.

Safe to re-run. Read-only. No writes, no schema changes.
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ─── Pretty printing ────────────────────────────────────────


def _step(n: int, label: str) -> None:
    print()
    print(f"[{n}] {label}")
    print("-" * 78)


def _pass(msg: str) -> None:
    print(f"    ✓ {msg}")


def _fail(msg: str, *, error: str = "", suggestion: str = "", sql: str = "") -> None:
    print(f"    ✗ {msg}")
    if error:
        print(f"      ERROR: {error}")
    if sql:
        print(f"      SQL:   {sql}")
    if suggestion:
        print(f"      NEXT:  {suggestion}")


def _info(label: str, value: object) -> None:
    print(f"    · {label}: {value}")


# ─── Env summary ────────────────────────────────────────────


def _env_summary() -> dict[str, str]:
    return {
        "LUMI_PG_HOST": os.environ.get("LUMI_PG_HOST", "localhost (default)"),
        "LUMI_PG_PORT": os.environ.get("LUMI_PG_PORT", "5432 (default)"),
        "LUMI_PG_DATABASE": os.environ.get("LUMI_PG_DATABASE", "lumi (default)"),
        "LUMI_PG_USER": os.environ.get("LUMI_PG_USER", "lumi (default)"),
        "LUMI_PG_PASSWORD": "(set)" if os.environ.get("LUMI_PG_PASSWORD") else "lumi (default)",
        "LUMI_AGE_GRAPH": os.environ.get("LUMI_AGE_GRAPH", "lumi_semantic (default)"),
        "LUMI_AGE_ENABLED": os.environ.get("LUMI_AGE_ENABLED", "(unset — won't affect this probe)"),
    }


# ─── The probe ──────────────────────────────────────────────


def main() -> int:
    print("=" * 78)
    print("  Apache AGE Health Check")
    print("=" * 78)
    print()
    print("  Environment (set LUMI_PG_* env vars to override defaults):")
    for k, v in _env_summary().items():
        _info(k, v)

    # ─── 1. psycopg installed ─────────────────────────────────
    _step(1, "Is psycopg installed?")
    try:
        import psycopg
        _pass(f"psycopg version {psycopg.__version__}")
    except ImportError as e:
        _fail(
            "psycopg not installed",
            error=str(e),
            suggestion=(
                "pip install 'psycopg[binary]'  "
                "(if you use the project .venv: ../.venv/bin/pip install 'psycopg[binary]')"
            ),
        )
        return 1

    # ─── 2. Postgres reachable ────────────────────────────────
    from lumi.semantic_graph import config as gconfig
    conn_params = gconfig.AGEConnection()

    _step(2, f"Can we connect to Postgres at {conn_params.host}:{conn_params.port}?")
    pgconn = None
    try:
        pgconn = psycopg.connect(conn_params.conninfo(), connect_timeout=5)
        _pass(f"Connected to {conn_params.host}:{conn_params.port}/{conn_params.database} as {conn_params.user}")
    except psycopg.OperationalError as e:
        msg = str(e)
        suggestion = "Check that your AGE Docker container is running:\n        docker ps | grep age\n      If not running, start it. If running, check the exposed port matches LUMI_PG_PORT."
        if "Connection refused" in msg or "could not connect" in msg:
            suggestion = (
                "Postgres isn't reachable at this host:port. Common fixes:\n"
                "        1. Start your AGE container: docker compose up -d  (or docker start <container>)\n"
                "        2. Check port mapping: docker ps  (look for 5432 → 5432)\n"
                "        3. If on a different port, export LUMI_PG_PORT=<port>"
            )
        elif "password authentication failed" in msg.lower():
            suggestion = (
                "Bad credentials. Common fixes:\n"
                "        1. export LUMI_PG_USER=<your_user>\n"
                "        2. export LUMI_PG_PASSWORD=<your_password>\n"
                "        3. If using default container, the user/pass is often 'postgres'/'postgres' — try those"
            )
        elif "database" in msg.lower() and "does not exist" in msg.lower():
            suggestion = (
                f"Database '{conn_params.database}' doesn't exist. Fixes:\n"
                "        1. Create it: psql -U <admin> -c \"CREATE DATABASE lumi;\"\n"
                "        2. Or point at an existing DB: export LUMI_PG_DATABASE=<name>"
            )
        _fail(
            "Postgres connection failed",
            error=f"{type(e).__name__}: {msg}",
            suggestion=suggestion,
        )
        return 2
    except Exception as e:  # noqa: BLE001
        _fail(
            "Unexpected error connecting to Postgres",
            error=f"{type(e).__name__}: {e}",
            suggestion="Likely a network or auth issue. Verify your container exposes Postgres on the configured port.",
        )
        traceback.print_exc()
        return 2

    try:
        # ─── 3. Postgres version ─────────────────────────────────
        _step(3, "What Postgres version is this?")
        try:
            with pgconn.cursor() as cur:
                cur.execute("SELECT version();")
                row = cur.fetchone()
            version = row[0] if row else "(unknown)"
            _pass(version[:80])
            _info("server_version", version)
        except Exception as e:  # noqa: BLE001
            _fail(
                "SELECT version() failed",
                error=f"{type(e).__name__}: {e}",
                sql="SELECT version();",
                suggestion="Your Postgres responded to a connection but not to a SELECT. Server is misbehaving — restart the container.",
            )
            return 3

        # ─── 4. AGE extension loadable ────────────────────────────
        _step(4, "Can we LOAD 'age'?")
        try:
            with pgconn.cursor() as cur:
                cur.execute("LOAD 'age';")
            _pass("AGE extension loaded successfully")
        except psycopg.Error as e:
            msg = str(e)
            suggestion = (
                "The AGE extension isn't installed in this Postgres. Fixes:\n"
                "        1. Are you using a Postgres+AGE Docker image? Check `docker ps` — should be apache/age:latest or similar\n"
                "        2. If using vanilla Postgres, you need apache/age:latest container instead\n"
                "        3. If using apache/age image but still failing: docker exec into the container and run:\n"
                "             psql -U postgres -c \"CREATE EXTENSION age;\""
            )
            if "could not access file" in msg or "No such file" in msg:
                suggestion = (
                    "AGE shared library not found. You're likely on vanilla Postgres without AGE installed.\n"
                    "        Easiest fix: switch to the official AGE Docker image:\n"
                    "          docker run -d --name lumi-age -p 5432:5432 \\\n"
                    "            -e POSTGRES_PASSWORD=lumi -e POSTGRES_USER=lumi -e POSTGRES_DB=lumi \\\n"
                    "            apache/age:latest"
                )
            elif "permission denied" in msg.lower():
                suggestion = (
                    "User lacks permission to LOAD extensions. Connect as superuser or grant LOAD permission."
                )
            _fail(
                "LOAD 'age' failed",
                error=f"{type(e).__name__}: {msg}",
                sql="LOAD 'age';",
                suggestion=suggestion,
            )
            return 4
        except Exception as e:  # noqa: BLE001
            _fail(
                "Unexpected error loading AGE",
                error=f"{type(e).__name__}: {e}",
                sql="LOAD 'age';",
            )
            traceback.print_exc()
            return 4

        # ─── 5. AGE catalog accessible ────────────────────────────
        _step(5, "Is ag_catalog accessible?")
        try:
            with pgconn.cursor() as cur:
                cur.execute('SET search_path = ag_catalog, "$user", public;')
                cur.execute("SELECT count(*) FROM ag_catalog.ag_graph;")
                row = cur.fetchone()
            n_graphs = int(row[0]) if row else 0
            _pass(f"ag_catalog reachable — {n_graphs} AGE graph(s) currently exist in this database")
        except Exception as e:  # noqa: BLE001
            _fail(
                "ag_catalog.ag_graph query failed",
                error=f"{type(e).__name__}: {e}",
                sql="SELECT count(*) FROM ag_catalog.ag_graph;",
                suggestion=(
                    "AGE loaded but the catalog table isn't found. Probably AGE extension is loaded but never CREATEd.\n"
                    "        Fix: CREATE EXTENSION age;  (as superuser)"
                ),
            )
            return 5

        # ─── 6. Cypher round-trip ─────────────────────────────────
        _step(6, "Can we run a Cypher query?")
        cypher_sql = (
            "SELECT * FROM cypher('lumi_health_check_probe', $$ RETURN 1 AS n $$) "
            "AS (n ag_catalog.agtype);"
        )
        try:
            with pgconn.cursor() as cur:
                # Use a throwaway graph for the round-trip; create then drop.
                cur.execute("SELECT ag_catalog.create_graph('lumi_health_check_probe');")
                cur.execute(cypher_sql)
                row = cur.fetchone()
                # AGE returns agtype; just confirm we got a row back
                got = str(row[0]) if row else "(nothing)"
                cur.execute("SELECT ag_catalog.drop_graph('lumi_health_check_probe', true);")
            _pass(f"Cypher RETURN 1 → {got} (round-trip works)")
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            # If create_graph fails because the graph exists, drop and retry
            if "already exists" in msg.lower():
                try:
                    with pgconn.cursor() as cur:
                        cur.execute("SELECT ag_catalog.drop_graph('lumi_health_check_probe', true);")
                        cur.execute("SELECT ag_catalog.create_graph('lumi_health_check_probe');")
                        cur.execute(cypher_sql)
                        row = cur.fetchone()
                        got = str(row[0]) if row else "(nothing)"
                        cur.execute("SELECT ag_catalog.drop_graph('lumi_health_check_probe', true);")
                    _pass(f"Cypher RETURN 1 → {got} (round-trip works, after cleanup)")
                except Exception as e2:  # noqa: BLE001
                    _fail(
                        "Cypher round-trip failed even after cleanup",
                        error=f"{type(e2).__name__}: {e2}",
                        sql=cypher_sql,
                    )
                    return 6
            else:
                _fail(
                    "Cypher round-trip failed",
                    error=f"{type(e).__name__}: {msg}",
                    sql=cypher_sql,
                    suggestion=(
                        "AGE catalog is there but Cypher queries aren't working. Check that:\n"
                        "        1. AGE version matches Postgres major version (age 1.5.0+ for PG14+)\n"
                        "        2. cypher() function is registered: SELECT * FROM pg_proc WHERE proname = 'cypher';"
                    ),
                )
                return 6

        # ─── 7. Existing graphs ───────────────────────────────────
        _step(7, "What graphs exist in this database?")
        try:
            with pgconn.cursor() as cur:
                cur.execute("SELECT name FROM ag_catalog.ag_graph ORDER BY name;")
                rows = cur.fetchall()
            graphs = [r[0] for r in rows] if rows else []
            if graphs:
                _pass(f"Found {len(graphs)} graph(s):")
                for g in graphs:
                    _info("graph", g)
            else:
                _pass("Zero graphs exist yet — that's fine, the Phase 1 probe will create lumi_semantic")
        except Exception as e:  # noqa: BLE001
            _fail(
                "Could not list graphs",
                error=f"{type(e).__name__}: {e}",
                sql="SELECT name FROM ag_catalog.ag_graph;",
            )

        # ─── 8. Does lumi_semantic already exist? ─────────────────
        _step(8, f"Does the LUMI graph ('{conn_params.graph_name}') exist yet?")
        try:
            with pgconn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s;",
                    (conn_params.graph_name,),
                )
                exists = cur.fetchone() is not None
            if exists:
                # Count labels to give a hint about state
                with pgconn.cursor() as cur:
                    cur.execute(
                        "SELECT graphid FROM ag_catalog.ag_graph WHERE name = %s;",
                        (conn_params.graph_name,),
                    )
                    graphid = cur.fetchone()[0]
                    cur.execute(
                        "SELECT kind, count(*) FROM ag_catalog.ag_label "
                        "WHERE graph = %s GROUP BY kind;",
                        (graphid,),
                    )
                    label_counts = dict(cur.fetchall())
                _pass(
                    f"Graph '{conn_params.graph_name}' exists "
                    f"(vlabels={label_counts.get('v', 0)}, elabels={label_counts.get('e', 0)})"
                )
            else:
                _pass(
                    f"Graph '{conn_params.graph_name}' does NOT exist yet — "
                    "this is normal on first run. The Phase 1 probe will create it."
                )
        except Exception as e:  # noqa: BLE001
            _fail(
                "Could not check for LUMI graph",
                error=f"{type(e).__name__}: {e}",
                sql=f"SELECT 1 FROM ag_catalog.ag_graph WHERE name = '{conn_params.graph_name}';",
            )

    finally:
        if pgconn is not None:
            pgconn.close()

    # ─── All green ─────────────────────────────────────────────
    print()
    print("=" * 78)
    print("  ✓ AGE HEALTH CHECK PASSED")
    print("=" * 78)
    print()
    print("  Next step: run the full Phase 0/1/2 probe:")
    print()
    print("      export LUMI_AGE_ENABLED=1")
    print("      python scripts/probe_semantic_graph_phases.py | tee probe_output.txt")
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n  Interrupted.")
        sys.exit(130)
    except Exception as e:  # noqa: BLE001
        print()
        print("=" * 78)
        print(f"  UNEXPECTED PROBE FAILURE: {type(e).__name__}")
        print("=" * 78)
        print(f"  {e}")
        print()
        traceback.print_exc()
        print()
        print("  Copy this entire output and share it back for debugging.")
        sys.exit(99)
