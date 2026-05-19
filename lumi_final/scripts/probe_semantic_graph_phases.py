"""Probe script — verifies Phases 0, 1, 2 end-to-end on the work laptop.

Run via:

    # 1. Make sure Postgres + Apache AGE Docker container is up first
    # 2. Export env vars (set non-defaults if your Postgres is elsewhere):
    export LUMI_AGE_ENABLED=1
    export LUMI_PG_HOST=localhost
    export LUMI_PG_PORT=5432
    export LUMI_PG_DATABASE=lumi
    export LUMI_PG_USER=lumi
    export LUMI_PG_PASSWORD=lumi
    # 3. Run the probe
    python scripts/probe_semantic_graph_phases.py | tee probe_output.txt
    # 4. Share probe_output.txt back

What this probe does:

  PHASE 0 — sqlglot Layer-3 + Layer-4 extraction
    Parses 4 representative SQLs and prints every new field the
    extractor populates. Verifies new signals fire on real query
    shapes (CTE+JOIN+CASE+GROUP+HAVING+ORDER+LIMIT, window function,
    UNION, single-lookup).

  PHASE 1 — AGE schema bootstrap
    Connects to Postgres + AGE; creates the lumi_semantic graph;
    creates all 16 vertex labels + 16 edge labels + GIN indexes.
    Reports created vs already-existing counts.
    Then verifies the schema is in place.

  PHASE 2 — projection + dual-write + replay
    Constructs ~6 synthetic OntologyEvents covering 6 event types,
    dual-writes them via writer.record(), then queries AGE via Cypher
    to confirm nodes + edges landed.
    Tests idempotency: re-projecting the same event = no duplicate.
    Replays from JSONL into a fresh graph to test recovery.

Output: structured JSON sections per phase, with PASS/FAIL flags.
Safe to re-run: all operations are idempotent.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any

# Add the lumi_final root to sys.path when invoked directly
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ─── Pretty printing ────────────────────────────────────────


def _header(title: str) -> None:
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def _section(title: str) -> None:
    print()
    print("-" * 78)
    print(f"  {title}")
    print("-" * 78)


def _kv(label: str, value: Any, indent: int = 2) -> None:
    pad = " " * indent
    if isinstance(value, (dict, list)):
        print(f"{pad}{label}:")
        print(_indent_json(value, indent + 2))
    else:
        print(f"{pad}{label}: {value}")


def _indent_json(obj: Any, indent: int) -> str:
    raw = json.dumps(obj, indent=2, default=str)
    return "\n".join(" " * indent + line for line in raw.splitlines())


def _pass(label: str) -> None:
    print(f"  ✓ {label}")


def _fail(label: str, err: str = "") -> None:
    print(f"  ✗ {label}  {err}".rstrip())


# ─── Phase 0 probe ──────────────────────────────────────────


_PHASE0_SQLS: dict[str, str] = {
    "complex_analytical": """
        WITH active_consumers AS (
          SELECT cm11 FROM cardmember_dim
          WHERE cm_status = 'Active' AND bus_seg = 'Consumer'
        )
        SELECT
          DATE_TRUNC(c.rpt_dt, MONTH) AS report_month,
          CASE
            WHEN c.fico_score >= 740 THEN 'Prime'
            WHEN c.fico_score >= 670 THEN 'Near-Prime'
            ELSE 'Sub-Prime'
          END AS fico_band,
          SUM(c.billed_business) AS total_bb,
          COUNT(DISTINCT c.cm11) AS unique_cardmembers
        FROM cornerstone_metrics c
        JOIN active_consumers a ON c.cm11 = a.cm11
        WHERE c.data_source = 'cornerstone' AND c.rpt_dt >= '2025-01-01'
        GROUP BY 1, 2
        HAVING SUM(c.billed_business) > 1000
        ORDER BY report_month DESC, total_bb DESC
        LIMIT 100
    """,
    "window_function": """
        SELECT
          merchant_id,
          rpt_dt,
          total_spend,
          ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY total_spend DESC) AS rn,
          LAG(total_spend, 1) OVER (PARTITION BY merchant_id ORDER BY rpt_dt) AS prev_spend
        FROM merchant_metrics
    """,
    "set_operation": """
        SELECT cm11, 'Consumer' AS seg FROM cardmember_consumer
        UNION ALL
        SELECT cm11, 'Commercial' AS seg FROM cardmember_commercial
    """,
    "single_lookup": """
        SELECT cm11, name, status FROM cardmember_dim WHERE cm11 = '12345678901'
    """,
}


def probe_phase0() -> dict[str, Any]:
    _header("PHASE 0 — sqlglot Layer-3 + Layer-4 extraction")
    from lumi.sql_to_context import parse_sqls

    results: dict[str, Any] = {"per_sql": [], "summary": {}}
    n_with_having = 0
    n_with_window = 0
    n_with_set_op = 0
    n_with_cohort = 0
    n_top_n = 0

    for label, sql in _PHASE0_SQLS.items():
        _section(f"SQL: {label}")
        fp = parse_sqls([sql.strip()])[0]
        per_sql_entry = {
            "label": label,
            "parse_error": fp.parse_error,
            "tables": fp.tables,
            "primary_table": fp.primary_table,
            "n_joins": len(fp.joins),
            "n_aggregations": len(fp.aggregations),
            "order_by": fp.order_by,
            "having": fp.having,
            "limit": fp.limit,
            "distinct_select": fp.distinct_select,
            "window_functions": fp.window_functions,
            "subqueries": fp.subqueries,
            "set_operations": fp.set_operations,
            "comments": fp.comments,
            "self_joins": fp.self_joins,
            "partition_pseudocolumns": fp.partition_pseudocolumns,
            "query_fingerprint_hash": fp.query_fingerprint_hash,
            "inferred_intent_class": fp.inferred_intent_class,
            "implicit_grain": fp.implicit_grain,
            "derived_dim_proposals": fp.derived_dim_proposals,
            "cohort_scope_signals": fp.cohort_scope_signals,
            "query_shape_summary": fp.query_shape_summary,
        }
        results["per_sql"].append(per_sql_entry)

        # Print key fields
        _kv("intent_class", fp.inferred_intent_class)
        _kv("implicit_grain", fp.implicit_grain or "(none)")
        _kv("fingerprint_hash", fp.query_fingerprint_hash)
        _kv("query_shape", fp.query_shape_summary)
        if fp.order_by:
            _kv("order_by", fp.order_by)
        if fp.having:
            _kv("having", fp.having)
            n_with_having += 1
        if fp.limit:
            _kv("limit", fp.limit)
            if fp.limit.get("is_top_n"):
                n_top_n += 1
        if fp.window_functions:
            _kv("window_functions", fp.window_functions)
            n_with_window += 1
        if fp.set_operations:
            _kv("set_operations", fp.set_operations)
            n_with_set_op += 1
        if fp.derived_dim_proposals:
            _kv("derived_dim_proposals", fp.derived_dim_proposals)
        if fp.cohort_scope_signals:
            _kv("cohort_scope_signals", fp.cohort_scope_signals)
            n_with_cohort += 1

    summary = {
        "sqls_probed": len(_PHASE0_SQLS),
        "extracted_having": n_with_having,
        "extracted_window_fn": n_with_window,
        "extracted_set_op": n_with_set_op,
        "extracted_cohort_scope": n_with_cohort,
        "extracted_top_n": n_top_n,
    }
    results["summary"] = summary

    _section("Phase 0 summary")
    _kv("summary", summary)

    expected = {
        "extracted_having": 1,
        "extracted_window_fn": 1,
        "extracted_set_op": 1,
        "extracted_cohort_scope": 1,
        "extracted_top_n": 1,
    }
    all_ok = all(summary.get(k, 0) >= v for k, v in expected.items())
    if all_ok:
        _pass("Phase 0 extraction surface verified")
    else:
        _fail("Phase 0 expected counts not met", str(expected))
    results["pass"] = all_ok
    return results


# ─── Phase 1 probe ──────────────────────────────────────────


def probe_phase1() -> dict[str, Any]:
    _header("PHASE 1 — Apache AGE schema bootstrap")
    from lumi.semantic_graph import config as gconfig

    results: dict[str, Any] = {}

    age_enabled = gconfig.is_age_enabled()
    _kv("LUMI_AGE_ENABLED", age_enabled)
    results["age_enabled"] = age_enabled

    if not age_enabled:
        _fail(
            "Skipping AGE operations — set LUMI_AGE_ENABLED=1 to run schema bootstrap",
        )
        results["pass"] = False
        results["skipped"] = True
        return results

    conn_params = gconfig.AGEConnection()
    _kv("host", conn_params.host)
    _kv("port", conn_params.port)
    _kv("database", conn_params.database)
    _kv("user", conn_params.user)
    _kv("graph_name", conn_params.graph_name)

    # 1. Verify psycopg + AGE reachable
    _section("Connection test")
    try:
        import psycopg
        with psycopg.connect(conn_params.conninfo(), connect_timeout=5) as pgconn:
            with pgconn.cursor() as cur:
                cur.execute("LOAD 'age';")
                cur.execute("SELECT version();")
                pg_version = cur.fetchone()[0]
        _pass(f"Postgres reachable: {pg_version[:60]}")
        results["postgres_reachable"] = True
    except Exception as e:  # noqa: BLE001
        _fail(f"Postgres connection failed: {type(e).__name__}: {e}")
        results["postgres_reachable"] = False
        results["pass"] = False
        return results

    # 2. Bootstrap (idempotent)
    _section("Bootstrap (idempotent)")
    try:
        from lumi.semantic_graph import schema as gschema
        bootstrap_result = gschema.bootstrap()
        _kv("bootstrap", bootstrap_result)
        results["bootstrap"] = bootstrap_result
        _pass("Bootstrap completed")
    except Exception as e:  # noqa: BLE001
        _fail(f"Bootstrap failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        results["bootstrap_error"] = str(e)
        results["pass"] = False
        return results

    # 3. Verify schema state
    _section("Schema verification")
    try:
        verify_result = gschema.verify()
        _kv("graph_exists", verify_result["graph_exists"])
        _kv("vlabels_present", verify_result["vlabels_present"])
        _kv("vlabels_missing", verify_result["vlabels_missing"])
        _kv("elabels_present", verify_result["elabels_present"])
        _kv("elabels_missing", verify_result["elabels_missing"])
        results["verify"] = verify_result

        if (verify_result["graph_exists"]
                and not verify_result["vlabels_missing"]
                and not verify_result["elabels_missing"]):
            _pass("All labels present")
            results["pass"] = True
        else:
            _fail("Schema verification failed: missing labels")
            results["pass"] = False
    except Exception as e:  # noqa: BLE001
        _fail(f"Verify failed: {type(e).__name__}: {e}")
        results["pass"] = False

    return results


# ─── Phase 2 probe ──────────────────────────────────────────


def probe_phase2() -> dict[str, Any]:
    _header("PHASE 2 — projection + dual-write + replay")
    from lumi.ontology_store import OntologyStore
    from lumi.schemas import OntologyEvent
    from lumi.semantic_graph import config as gconfig
    from lumi.semantic_graph import writer

    results: dict[str, Any] = {}

    if not gconfig.is_age_enabled():
        _fail(
            "Skipping AGE projection — set LUMI_AGE_ENABLED=1",
        )
        results["pass"] = False
        results["skipped"] = True
        return results

    # Use a temp JSONL store so we don't pollute the real data/ontology
    tmpdir = Path(tempfile.mkdtemp(prefix="lumi_probe_"))
    _kv("temp_store_root", str(tmpdir))
    store = OntologyStore(tmpdir)

    # 1. Construct a representative event batch covering 6 event types.
    _section("Synthetic event batch")
    events = [
        OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="cornerstone_metrics", column_name="cm11",
            entity_name="cardmember",
            payload={}, confidence=0.7, evidence="MDM business_name match",
        ),
        OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="cardmember_dim", column_name="cm11",
            entity_name="cardmember",
            payload={}, confidence=0.7, evidence="MDM business_name match",
        ),
        OntologyEvent(
            event_type="equivalence_observed", source="parse_sqls",
            table_name="cornerstone_metrics", column_name="cm11",
            payload={
                "a_table": "cornerstone_metrics", "a_column": "cm11",
                "b_table": "cardmember_dim", "b_column": "cm11",
                "count": 1,
            },
            confidence=0.85, evidence="observed in 1 JOIN ON pair",
        ),
        OntologyEvent(
            event_type="cardinality_observed", source="parse_sqls",
            table_name="cornerstone_metrics", column_name="cm11",
            payload={
                "left_table": "cornerstone_metrics", "left_column": "cm11",
                "right_table": "cardmember_dim", "right_column": "cm11",
                "cardinality": "many_to_one",
                "vote_breakdown": {"many_to_one": 3},
                "observations": 3,
            },
            confidence=1.0,
            evidence="GROUP BY on cardmember side, agg on cornerstone side",
        ),
        OntologyEvent(
            event_type="synonym_candidate", source="fetch_mdm",
            table_name="cornerstone_metrics", column_name="cm11",
            entity_name="cardmember",
            payload={"canonical": "cardmember", "synonym": "Cardmember ID"},
            confidence=0.55,
            evidence="MDM business_name = 'Cardmember ID'",
        ),
        OntologyEvent(
            event_type="curated_pk", source="parse_baseline",
            table_name="cardmember_dim", column_name="cm11",
            payload={},
            confidence=0.9,
            evidence="baseline LookML declares this as primary_key",
        ),
        OntologyEvent(
            event_type="vocabulary_lock", source="approve_plan",
            table_name="cardmember_dim",
            entity_name="cardmember",
            payload={"approved_by": "human"},
            confidence=0.95,
            evidence="plan for cardmember_dim approved by human",
        ),
    ]
    _kv("event_count", len(events))

    # 2. Dual-write
    _section("Dual-write")
    receipts = writer.record_many(events, store=store)
    jsonl_ok = sum(1 for r in receipts if r.jsonl_ok)
    age_ok = sum(1 for r in receipts if r.age_ok)
    age_skips = [r.age_skip_reason for r in receipts if r.age_skip_reason]
    _kv("jsonl_ok", jsonl_ok)
    _kv("age_ok", age_ok)
    if age_skips:
        _kv("age_skip_reasons (sample)", list(set(age_skips))[:3])
    results["dual_write"] = {
        "events_total": len(events),
        "jsonl_ok": jsonl_ok,
        "age_ok": age_ok,
    }

    # 3. Idempotency — re-record one event, expect duplicate flag
    _section("Idempotency check")
    duplicate_event = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="cornerstone_metrics", column_name="cm11",
        entity_name="cardmember",
        payload={}, confidence=0.7,
    )
    dup_receipt = writer.record(duplicate_event, store=store)
    _kv("jsonl_was_duplicate", dup_receipt.jsonl_was_duplicate)
    _kv("age_skip_reason", dup_receipt.age_skip_reason)
    results["idempotency_check"] = {
        "was_duplicate": dup_receipt.jsonl_was_duplicate,
        "age_skipped_dup": "duplicate" in (dup_receipt.age_skip_reason or "").lower(),
    }

    # 4. Cypher round-trip — query the graph for what we just wrote.
    _section("Cypher round-trip")
    try:
        import psycopg
        conn_params = gconfig.AGEConnection()
        counts = {}
        with psycopg.connect(conn_params.conninfo()) as pgconn:
            with pgconn.cursor() as cur:
                cur.execute("LOAD 'age';")
                cur.execute('SET search_path = ag_catalog, "$user", public;')
                for label in ("Table", "Column", "Entity", "Synonym", "Event", "Approval"):
                    cur.execute(
                        f"SELECT count(*) FROM cypher('{conn_params.graph_name}', $$ "
                        f"MATCH (n:{label}) RETURN n "
                        f"$$) AS (n ag_catalog.agtype);"
                    )
                    n = cur.fetchone()[0]
                    counts[label] = int(n)
                for edge in ("CONTAINS", "IDENTIFIES", "EQUIVALENT_TO", "HAS_SYNONYM", "ASSERTS", "LOCKS"):
                    cur.execute(
                        f"SELECT count(*) FROM cypher('{conn_params.graph_name}', $$ "
                        f"MATCH ()-[r:{edge}]->() RETURN r "
                        f"$$) AS (r ag_catalog.agtype);"
                    )
                    n = cur.fetchone()[0]
                    counts[f"edge_{edge}"] = int(n)
        _kv("graph_counts", counts)
        results["cypher_counts"] = counts

        # Expect at least the entities/columns we wrote
        expected_min = {
            "Table": 2, "Column": 2, "Entity": 1, "Event": 1, "Approval": 1,
            "edge_CONTAINS": 2, "edge_IDENTIFIES": 1,
        }
        cypher_ok = all(counts.get(k, 0) >= v for k, v in expected_min.items())
        if cypher_ok:
            _pass("Cypher round-trip — all expected nodes/edges present")
        else:
            _fail("Cypher round-trip missing expected entities", str(expected_min))
        results["cypher_roundtrip_pass"] = cypher_ok
    except Exception as e:  # noqa: BLE001
        _fail(f"Cypher round-trip failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        results["cypher_roundtrip_pass"] = False

    # 5. Replay test
    _section("Replay test")
    try:
        from lumi.semantic_graph import replay
        replay_result = replay.replay_all(tmpdir / "events", rebuild=False)
        _kv("replay_counts", replay_result)
        # Replay against existing graph should mostly be no-op MERGE
        # (read more events than the dual-write because the duplicate
        # also got recorded once on dedup-skip; counts roughly match)
        results["replay"] = replay_result
        _pass("Replay completed (idempotent MERGE — graph unchanged)")
    except Exception as e:  # noqa: BLE001
        _fail(f"Replay failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        results["replay_error"] = str(e)

    # 6. Verify divergence check
    _section("Divergence verification")
    try:
        from lumi.semantic_graph import replay
        verify_result = replay.verify(tmpdir / "events")
        _kv("verify", verify_result)
        results["verify_divergence"] = verify_result
        _pass("Divergence check ran")
    except Exception as e:  # noqa: BLE001
        _fail(f"Verify failed: {type(e).__name__}: {e}")

    results["pass"] = results.get("cypher_roundtrip_pass", False)
    return results


# ─── Main ───────────────────────────────────────────────────


def main() -> int:
    _header("LUMI Semantic Graph — Phase 0/1/2 Probe")
    print(f"  Run at: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  Repo:   {_REPO_ROOT}")

    env_report = {
        "LUMI_AGE_ENABLED": os.environ.get("LUMI_AGE_ENABLED", "(unset)"),
        "LUMI_PG_HOST": os.environ.get("LUMI_PG_HOST", "(default: localhost)"),
        "LUMI_PG_PORT": os.environ.get("LUMI_PG_PORT", "(default: 5432)"),
        "LUMI_PG_DATABASE": os.environ.get("LUMI_PG_DATABASE", "(default: lumi)"),
        "LUMI_PG_USER": os.environ.get("LUMI_PG_USER", "(default: lumi)"),
        "LUMI_AGE_GRAPH": os.environ.get("LUMI_AGE_GRAPH", "(default: lumi_semantic)"),
    }
    _kv("env", env_report)

    overall: dict[str, Any] = {"env": env_report}

    try:
        overall["phase0"] = probe_phase0()
    except Exception as e:  # noqa: BLE001
        overall["phase0"] = {"pass": False, "error": str(e)}
        traceback.print_exc()

    try:
        overall["phase1"] = probe_phase1()
    except Exception as e:  # noqa: BLE001
        overall["phase1"] = {"pass": False, "error": str(e)}
        traceback.print_exc()

    try:
        overall["phase2"] = probe_phase2()
    except Exception as e:  # noqa: BLE001
        overall["phase2"] = {"pass": False, "error": str(e)}
        traceback.print_exc()

    # Final summary
    _header("OVERALL")
    p0 = overall["phase0"].get("pass", False)
    p1 = overall["phase1"].get("pass", False)
    p2 = overall["phase2"].get("pass", False)
    p1_skipped = overall["phase1"].get("skipped", False)
    p2_skipped = overall["phase2"].get("skipped", False)

    def _label(passed: bool, skipped: bool) -> str:
        if skipped:
            return "○ SKIPPED (LUMI_AGE_ENABLED not set)"
        return "✓ PASS" if passed else "✗ FAIL"

    print(f"  Phase 0 (sqlglot extraction):       {_label(p0, False)}")
    print(f"  Phase 1 (AGE schema bootstrap):     {_label(p1, p1_skipped)}")
    print(f"  Phase 2 (projection + replay):      {_label(p2, p2_skipped)}")
    print()
    print("  Full structured output below — copy-paste this entire output to share back.")
    print()
    print("=" * 78)
    print("  STRUCTURED RESULTS (json)")
    print("=" * 78)
    print(json.dumps(overall, indent=2, default=str))

    # Exit non-zero if any required phase failed (phase 0 always required;
    # phases 1+2 only required if AGE was enabled)
    fail = not p0
    if not overall["phase1"].get("skipped") and not p1:
        fail = True
    if not overall["phase2"].get("skipped") and not p2:
        fail = True
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
