"""Corpus-scale Phase 0 + 1 + 2 probe — runs the full event pipeline on
every .sql in a directory (defaults to data/gold_queries/) and emits an
inspection report so we can eyeball whether the JSONL audit log and the
AGE graph match the design standards.

This is NOT a unit test. It exercises the same code paths the real
pipeline does:

    Phase 0 (extraction):
        - parse_sqls() on every SQL
        - per-field coverage report (which Layer-3/4 signals actually fire
          on this corpus; which never do → may indicate dead code)
        - parse-error rate
        - intent class distribution
        - implicit_grain / cohort signals surfaced

    Phase 1 (event emission — same hooks as lumi.pipeline.run_plan_phase):
        - record_equivalences_from_fingerprints
        - record_cardinalities_from_fingerprints
        - record_entity_hints_from_mdm     (skipped if --no-mdm)
        - record_curated_synonyms_from_baseline   (skipped if no baseline)
        - all write to data/ontology/events/*.jsonl

    Phase 2 (projection + replay):
        - if LUMI_AGE_ENABLED=1, runs replay.replay_all(rebuild=True)
          to project every JSONL event into AGE
        - then runs verification: node counts per label, edge counts per
          label, sample nodes with full provenance, schema invariants
          (orphan columns, missing canonical_uri, promoted-without-asserts),
          source distribution, JSONL-vs-AGE event count parity

Usage:
    # Phase 0 audit only (no Postgres needed):
    python scripts/probe_corpus_phase012.py --no-age

    # Full Phase 0 + 1 + 2, fresh JSONL + fresh AGE:
    rm -rf data/ontology/events/
    LUMI_AGE_ENABLED=1 python scripts/probe_corpus_phase012.py --fresh

    # Run against a different corpus location:
    python scripts/probe_corpus_phase012.py --input /path/to/sqls/

Writes:
    review_queue/CORPUS_PROBE_REPORT.md   — human-readable inspection
    data/probe/corpus_phase012_stats.json — machine-readable stats
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent  # lumi_final/
sys.path.insert(0, str(REPO_ROOT))


# ─── pretty print ─────────────────────────────────────────────

def _hdr(msg: str) -> None:
    print(f"\n\033[1;36m═══ {msg} ═══\033[0m")

def _sub(msg: str) -> None:
    print(f"\033[1;34m── {msg} ──\033[0m")

def _pass(msg: str) -> None:
    print(f"  \033[1;32m✓\033[0m {msg}")

def _fail(msg: str) -> None:
    print(f"  \033[1;31m✗\033[0m {msg}")

def _info(msg: str) -> None:
    print(f"    \033[2m{msg}\033[0m")

def _warn(msg: str) -> None:
    print(f"  \033[1;33m!\033[0m {msg}")


# ─── Phase 0 audit ────────────────────────────────────────────

# Every Layer-3/4 field we expect; report which ones fired and on how many.
_LAYER3_FIELDS = (
    "order_by", "having", "limit", "distinct_select",
    "window_functions", "subqueries", "set_operations",
    "null_handlers", "type_casts", "string_functions", "math_functions",
    "comments", "parameters", "qualify_clauses",
    "array_operations", "struct_access", "json_operations",
    "self_joins", "partition_pseudocolumns", "sql_hints",
    "query_shape_summary",
)
_LAYER4_FIELDS = (
    "query_fingerprint_hash", "inferred_intent_class", "business_domain_tags",
    "implicit_grain", "derived_dim_proposals", "cohort_scope_signals",
)


def _is_nonempty(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, (list, dict, str)):
        return len(v) > 0
    if isinstance(v, bool):
        return v
    return True


@dataclass
class Phase0Stats:
    n_queries: int = 0
    n_parse_errors: int = 0
    parse_error_files: list[str] = field(default_factory=list)
    field_coverage: dict[str, int] = field(default_factory=dict)  # field → count nonzero
    intent_distribution: Counter = field(default_factory=Counter)
    domain_distribution: Counter = field(default_factory=Counter)
    grain_distribution: Counter = field(default_factory=Counter)
    top_tables: Counter = field(default_factory=Counter)
    top_columns: Counter = field(default_factory=Counter)
    fingerprint_collisions: int = 0


def run_phase0(input_dir: Path) -> tuple[list[Any], Phase0Stats]:
    """Parse every .sql in input_dir; return fingerprints + audit stats."""
    from lumi.sql_to_context import parse_sqls

    files = sorted(input_dir.glob("*.sql"))
    if not files:
        _fail(f"No .sql files in {input_dir}")
        return [], Phase0Stats()

    sqls = []
    name_by_index = []
    for f in files:
        sqls.append(f.read_text())
        name_by_index.append(f.name)

    _info(f"parsing {len(sqls)} SQL file(s) …")
    fps = parse_sqls(sqls)

    stats = Phase0Stats(n_queries=len(fps))
    stats.field_coverage = {f: 0 for f in (*_LAYER3_FIELDS, *_LAYER4_FIELDS)}

    seen_hashes = set()
    for i, fp in enumerate(fps):
        if getattr(fp, "parse_error", None):
            stats.n_parse_errors += 1
            stats.parse_error_files.append(name_by_index[i])
            continue

        # field-presence counters
        for f in (*_LAYER3_FIELDS, *_LAYER4_FIELDS):
            v = getattr(fp, f, None)
            if _is_nonempty(v):
                stats.field_coverage[f] += 1

        # intent
        ic = getattr(fp, "inferred_intent_class", "unknown") or "unknown"
        stats.intent_distribution[ic] += 1

        # domain tags
        for d in getattr(fp, "business_domain_tags", []) or []:
            stats.domain_distribution[d] += 1

        # grain
        g = getattr(fp, "implicit_grain", "") or ""
        if g:
            stats.grain_distribution[g] += 1

        # tables + columns
        for t in getattr(fp, "tables", []) or []:
            stats.top_tables[t] += 1
        for agg in getattr(fp, "aggregations", []) or []:
            src = (agg.get("source_column") if isinstance(agg, dict)
                   else None)
            if src:
                stats.top_columns[src] += 1
        for sa in getattr(fp, "select_aliases", []) or []:
            if isinstance(sa, dict) and sa.get("source_column"):
                stats.top_columns[sa["source_column"]] += 1

        # fingerprint dedup
        h = getattr(fp, "query_fingerprint_hash", "")
        if h:
            if h in seen_hashes:
                stats.fingerprint_collisions += 1
            seen_hashes.add(h)

    return fps, stats


# ─── Phase 1 — event emission ─────────────────────────────────

@dataclass
class MDMTableAudit:
    table_name: str
    sql_columns_referenced: int
    mdm_columns: int
    mdm_coverage_pct: float
    cache_hit: bool
    # table-level signals
    table_description_present: bool
    dataset_details_present: bool
    ownership_present: bool
    table_type: str
    feed_type: str
    data_category: str
    is_decommissioned: bool
    bq_fqn: str
    # per-column counts — description leg
    n_cols_with_business_name: int
    n_cols_with_attribute_desc: int
    n_cols_with_attribute_type: int
    n_cols_with_format: int
    # per-column counts — grounding leg (highest-signal MDM facts)
    n_cols_is_primary: int
    n_cols_is_dedupe_key: int
    n_cols_pii: int
    n_cols_critical_data_element: int
    n_cols_is_mandatory: int
    # per-column counts — BQ physical leg
    n_cols_partition: int
    n_cols_clustered: int
    # per-column counts — derived / cross-ref leg
    n_cols_with_derived_logic: int
    n_cols_with_external_references: int
    has_external_references: bool
    # gap signal — every missing SQL ref classified
    sql_cols_not_in_mdm: list[str]
    # breakdown of WHY each missing name isn't in MDM:
    #   metric    — alias of an aggregation (SUM/COUNT/...) — captured by metric_observed
    #   threshold — alias of a CASE WHEN expression — captured by threshold_observed
    #   synonym?  — fuzzy match to an MDM technical name (e.g., cm_id ≈ cm11)
    #   real_gap  — none of the above; investigate / refresh MDM
    missing_metric: list[str]
    missing_threshold: list[str]
    missing_synonym: list[tuple[str, str]]  # (sql_name, suggested_mdm_name)
    missing_real_gap: list[str]


@dataclass
class Phase1Stats:
    n_equivalence: int = 0
    n_cardinality: int = 0
    n_corpus_facts: int = 0
    n_mdm: int = 0
    n_baseline: int = 0
    n_contexts: int = 0
    mdm_attempted: bool = False
    mdm_error: str = ""
    mdm_cache_dir: str = ""
    mdm_cache_misses: list[str] = field(default_factory=list)
    mdm_audits: list[MDMTableAudit] = field(default_factory=list)
    jsonl_total_lines: int = 0
    jsonl_files: list[str] = field(default_factory=list)


def _audit_mdm_context(ctx: Any, cache_misses: set[str]) -> MDMTableAudit:
    """Profile MDM richness for one TableContext. Defensive against
    real-cache shape drift — any non-dict entry is silently skipped."""
    import difflib

    raw_cols = ctx.mdm_columns or []
    cols = [c for c in raw_cols if isinstance(c, dict)]
    sql_refs = [c.lower() for c in (ctx.columns_referenced or [])]
    # Match SQL refs against ALL technical names — name / attribute_name
    # / current_col_name. business_name is the human label ("Card Member
    # ID"), not what SQL refers to ("cm11"). Including it here would
    # always miss.
    mdm_col_names: set[str] = set()
    for c in cols:
        for k in ("name", "attribute_name", "current_col_name"):
            v = c.get(k)
            if v:
                mdm_col_names.add(str(v).lower())
    missing_in_mdm = [c for c in sql_refs if c not in mdm_col_names]

    # ── Classify each missing column ──
    # Build alias-source maps from the TableContext (aggregations and
    # case_whens come accumulated across every fp that touched this table).
    metric_aliases = {
        (a.get("alias") or "").lower()
        for a in (ctx.aggregations or [])
        if a.get("alias")
    }
    threshold_aliases = {
        (cw.get("alias") or "").lower()
        for cw in (ctx.case_whens or [])
        if cw.get("alias")
    }
    mdm_names_list = sorted(mdm_col_names)

    missing_metric: list[str] = []
    missing_threshold: list[str] = []
    missing_synonym: list[tuple[str, str]] = []
    missing_real_gap: list[str] = []
    for m in missing_in_mdm:
        if m in metric_aliases:
            missing_metric.append(m)
        elif m in threshold_aliases:
            missing_threshold.append(m)
        else:
            # Fuzzy match — 0.75 cutoff catches cm_id↔cm11, acct_id↔acct,
            # as_of_dt↔rpt_dt at the edge; below that returns no match.
            cand = difflib.get_close_matches(m, mdm_names_list, n=1, cutoff=0.75)
            if cand:
                missing_synonym.append((m, cand[0]))
            else:
                missing_real_gap.append(m)

    ds = ctx.mdm_dataset_details if isinstance(
        ctx.mdm_dataset_details, dict,
    ) else {}
    bq_fqn = ".".join(filter(None, [
        ds.get("bq_project"), ds.get("bq_dataset"), ds.get("bq_table"),
    ]))

    return MDMTableAudit(
        table_name=ctx.table_name,
        sql_columns_referenced=len(sql_refs),
        mdm_columns=len(cols),
        mdm_coverage_pct=ctx.mdm_coverage_pct,
        cache_hit=ctx.table_name not in cache_misses,
        # table-level
        table_description_present=bool(ctx.mdm_table_description),
        dataset_details_present=bool(ds),
        ownership_present=bool(
            isinstance(ctx.mdm_ownership, dict) and ctx.mdm_ownership,
        ),
        table_type=str(ds.get("table_type") or ""),
        feed_type=str(ds.get("feed_type") or ""),
        data_category=str(ds.get("data_category") or ""),
        is_decommissioned=bool(ds.get("is_decommissioned")),
        bq_fqn=bq_fqn,
        # description leg
        n_cols_with_business_name=sum(1 for c in cols if c.get("business_name")),
        n_cols_with_attribute_desc=sum(
            1 for c in cols if c.get("attribute_desc") or c.get("description")
        ),
        n_cols_with_attribute_type=sum(
            1 for c in cols if c.get("attribute_type") or c.get("type")
        ),
        n_cols_with_format=sum(
            1 for c in cols if c.get("attribute_format") or c.get("format")
        ),
        # grounding leg
        n_cols_is_primary=sum(1 for c in cols if c.get("is_primary")),
        n_cols_is_dedupe_key=sum(1 for c in cols if c.get("is_dedupe_key")),
        n_cols_pii=sum(1 for c in cols if c.get("is_pii")),
        n_cols_critical_data_element=sum(
            1 for c in cols if c.get("is_critical_data_element")
        ),
        n_cols_is_mandatory=sum(1 for c in cols if c.get("is_mandatory")),
        # BQ physical leg
        n_cols_partition=sum(
            1 for c in cols
            if c.get("is_partitioned") or c.get("partition_column")
        ),
        n_cols_clustered=sum(1 for c in cols if c.get("is_clustered")),
        # derived / cross-ref leg — external_references is PLURAL in the digest
        n_cols_with_derived_logic=sum(
            1 for c in cols if c.get("derived_logic")
        ),
        n_cols_with_external_references=sum(
            1 for c in cols if c.get("external_references")
        ),
        has_external_references=any(
            c.get("external_references") for c in cols
        ),
        sql_cols_not_in_mdm=missing_in_mdm[:20],
        missing_metric=missing_metric,
        missing_threshold=missing_threshold,
        missing_synonym=missing_synonym,
        missing_real_gap=missing_real_gap,
    )


def run_phase1(fps: list[Any], *, with_mdm: bool) -> Phase1Stats:
    """Emit all corpus events through OntologyStore hooks (same as real
    pipeline). Returns counts."""
    from lumi.ontology_store import (
        OntologyStore,
        record_cardinalities_from_fingerprints,
        record_corpus_facts,
        record_curated_synonyms_from_baseline,
        record_entity_hints_from_mdm,
        record_equivalences_from_fingerprints,
    )

    store = OntologyStore()
    stats = Phase1Stats()

    _info("emitting equivalences …")
    stats.n_equivalence = record_equivalences_from_fingerprints(store, fps)
    _info(f"  → {stats.n_equivalence} events")

    _info("emitting cardinalities …")
    stats.n_cardinality = record_cardinalities_from_fingerprints(store, fps)
    _info(f"  → {stats.n_cardinality} events")

    _info("emitting corpus semantic facts (verb layer) …")
    stats.n_corpus_facts = record_corpus_facts(store, fps)
    _info(f"  → {stats.n_corpus_facts} events "
          f"(metric / threshold / filter / time_grain / cohort / "
          f"question_pattern)")

    if with_mdm:
        try:
            from lumi.mdm import CachedMDMClient
            from lumi.sql_to_context import discover_tables

            mdm_cache_dir = REPO_ROOT / "data" / "mdm_cache"
            baseline_dir = REPO_ROOT / "data" / "baseline_views"
            if not mdm_cache_dir.exists():
                raise FileNotFoundError(
                    f"MDM cache dir not found: {mdm_cache_dir}. "
                    "Populate it with: python scripts/probe_mdm.py --save "
                    f"{mdm_cache_dir}"
                )

            # ── MDM cache health check (before we use it) ──
            _info("  validating MDM cache files …")
            cache_files = sorted(mdm_cache_dir.glob("*.json"))
            malformed: list[str] = []
            empty_cols: list[str] = []
            for f in cache_files:
                try:
                    blob = json.loads(f.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as e:
                    malformed.append(f"{f.name}: {type(e).__name__}")
                    continue
                if not isinstance(blob, dict):
                    malformed.append(f"{f.name}: top-level not a dict")
                    continue
                cols = blob.get("columns")
                if not isinstance(cols, list):
                    malformed.append(f"{f.name}: columns not a list")
                    continue
                non_dict_cols = sum(1 for c in cols if not isinstance(c, dict))
                if non_dict_cols:
                    malformed.append(
                        f"{f.name}: {non_dict_cols} non-dict column entry(ies)"
                    )
                if not cols:
                    empty_cols.append(f.name)
            _info(f"  {len(cache_files)} cache files, "
                  f"{len(malformed)} malformed, {len(empty_cols)} empty")
            if malformed:
                for m in malformed[:10]:
                    _warn(f"    {m}")
                if len(malformed) > 10:
                    _info(f"    …and {len(malformed) - 10} more")

            mdm = CachedMDMClient(mdm_cache_dir)
            stats.mdm_cache_dir = str(mdm_cache_dir)
            contexts = discover_tables(
                fps, mdm, str(baseline_dir) if baseline_dir.exists() else "",
            )
            stats.n_contexts = len(contexts)
            stats.mdm_attempted = True
            _info(f"  discovered {len(contexts)} table context(s)")
            # Audit MDM richness BEFORE emitting events so we can correlate
            # event counts to MDM coverage.
            cache_misses_set = set(mdm.cache_misses)
            for ctx in contexts.values():
                stats.mdm_audits.append(
                    _audit_mdm_context(ctx, cache_misses_set),
                )
            for ctx in contexts.values():
                stats.n_mdm += record_entity_hints_from_mdm(store, ctx)
                stats.n_baseline += record_curated_synonyms_from_baseline(
                    store, ctx,
                )
            _info(f"  → {stats.n_mdm} MDM + {stats.n_baseline} baseline events")
            stats.mdm_cache_misses = list(mdm.cache_misses)
            if stats.mdm_cache_misses:
                _warn(f"  {len(stats.mdm_cache_misses)} MDM cache miss(es): "
                      f"{stats.mdm_cache_misses[:5]}"
                      f"{' …' if len(stats.mdm_cache_misses) > 5 else ''}")
            # Diagnostic: any table with 0 MDM coverage is a red flag
            zero_cov = [a for a in stats.mdm_audits if a.mdm_columns == 0]
            if zero_cov:
                _warn(f"  {len(zero_cov)} table(s) have ZERO MDM coverage: "
                      f"{[a.table_name for a in zero_cov[:5]]}")
        except Exception as e:
            import traceback
            stats.mdm_error = f"{type(e).__name__}: {e}"
            _warn(f"MDM step skipped: {stats.mdm_error}")
            # Print the traceback so we never have to guess which line.
            for line in traceback.format_exc().splitlines()[-8:]:
                _info(line)
    else:
        _info("MDM step skipped (--no-mdm)")

    events_dir = REPO_ROOT / "data" / "ontology" / "events"
    if events_dir.exists():
        files = sorted(events_dir.glob("*.jsonl"))
        stats.jsonl_files = [f.name for f in files]
        for f in files:
            with f.open() as fh:
                stats.jsonl_total_lines += sum(1 for _ in fh)

    return stats


# ─── Phase 2 — AGE replay + verify ────────────────────────────

@dataclass
class Phase2Stats:
    age_enabled: bool = False
    replay_ok: bool = False
    replay_error: str = ""
    n_events_replayed: int = 0
    node_counts: dict[str, int] = field(default_factory=dict)
    edge_counts: dict[str, int] = field(default_factory=dict)
    source_distribution: dict[str, int] = field(default_factory=dict)
    confidence_distribution: dict[str, int] = field(default_factory=dict)
    samples_by_label: dict[str, list[dict]] = field(default_factory=dict)
    invariants: dict[str, dict] = field(default_factory=dict)
    jsonl_vs_age_event_delta: int = 0


def _cypher_one(cur, cypher: str, return_cols: str = "v agtype") -> Any:
    cur.execute(
        f"SELECT * FROM cypher('lumi_semantic', $${cypher}$$) AS ({return_cols});"
    )
    return cur.fetchall()


def run_phase2(jsonl_total: int) -> Phase2Stats:
    """Replay JSONL into AGE, then run verification Cypher queries."""
    from lumi.semantic_graph import config as gconfig

    stats = Phase2Stats(age_enabled=gconfig.is_age_enabled())
    if not stats.age_enabled:
        _warn("LUMI_AGE_ENABLED not set — Phase 2 skipped.")
        return stats

    try:
        from lumi.semantic_graph import replay, schema
    except ImportError as e:
        stats.replay_error = f"import: {e}"
        return stats

    # Ensure schema present
    try:
        schema.bootstrap()
    except Exception as e:
        stats.replay_error = f"bootstrap: {type(e).__name__}: {e}"
        _fail(stats.replay_error)
        return stats

    events_dir = REPO_ROOT / "data" / "ontology" / "events"
    try:
        # replay_all returns a dict of counts; we want the projected count.
        counts = replay.replay_all(events_dir, rebuild=True)
        if isinstance(counts, dict):
            stats.n_events_replayed = int(counts.get("projected", 0))
            n_read = int(counts.get("read", 0))
            n_err = int(counts.get("errors", 0))
            _pass(
                f"replayed {stats.n_events_replayed}/{n_read} events"
                f" ({n_err} errors)"
            )
        else:
            stats.n_events_replayed = int(counts)
            _pass(f"replayed {stats.n_events_replayed} events into AGE")
        stats.replay_ok = True
    except Exception as e:
        stats.replay_error = f"replay: {type(e).__name__}: {e}"
        _fail(stats.replay_error)
        return stats

    # ── verification queries ──
    try:
        import psycopg

        conn_cfg = gconfig.AGEConnection()
        with psycopg.connect(
            host=conn_cfg.host, port=conn_cfg.port, dbname=conn_cfg.database,
            user=conn_cfg.user, password=conn_cfg.password, connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute("SET search_path = ag_catalog, \"$user\", public;")

            # node counts per label
            for label in gconfig.NODE_LABELS:
                rows = _cypher_one(
                    cur, f"MATCH (n:{label}) RETURN count(n) AS c",
                    return_cols="c agtype",
                )
                count = int(str(rows[0][0]).strip()) if rows else 0
                stats.node_counts[label] = count

            # edge counts per label
            for label in gconfig.EDGE_LABELS:
                rows = _cypher_one(
                    cur, f"MATCH ()-[r:{label}]->() RETURN count(r) AS c",
                    return_cols="c agtype",
                )
                count = int(str(rows[0][0]).strip()) if rows else 0
                stats.edge_counts[label] = count

            # source distribution across Events
            rows = _cypher_one(
                cur,
                "MATCH (e:Event) RETURN e.source AS s, count(e) AS c",
                return_cols="s agtype, c agtype",
            )
            for s, c in rows:
                key = str(s).strip().strip('"')
                stats.source_distribution[key] = int(str(c).strip())

            # confidence distribution across all promoted-or-candidate nodes
            for label in ("Entity", "Column", "Synonym", "Metric"):
                rows = _cypher_one(
                    cur,
                    f"MATCH (n:{label}) RETURN n.confidence AS c, count(n) AS k",
                    return_cols="c agtype, k agtype",
                )
                for c, k in rows:
                    key = f"{label}::{str(c).strip().strip(chr(34))}"
                    stats.confidence_distribution[key] = int(str(k).strip())

            # samples — 3 per label with all properties
            for label in gconfig.NODE_LABELS:
                if stats.node_counts.get(label, 0) == 0:
                    continue
                rows = _cypher_one(
                    cur,
                    f"MATCH (n:{label}) RETURN n LIMIT 3",
                    return_cols="n agtype",
                )
                samples = []
                for (r,) in rows:
                    s = str(r)
                    if "::vertex" in s:
                        s = s.rsplit("::vertex", 1)[0]
                    try:
                        samples.append(json.loads(s))
                    except json.JSONDecodeError:
                        samples.append({"raw": s[:300]})
                stats.samples_by_label[label] = samples

            # ── invariants ──
            inv = {}

            # I1: orphan Columns
            rows = _cypher_one(
                cur,
                "MATCH (c:Column) "
                "WHERE NOT EXISTS { (:Table)-[:CONTAINS]->(c) } "
                "RETURN count(c) AS k",
                return_cols="k agtype",
            )
            inv["orphan_columns"] = {
                "count": int(str(rows[0][0]).strip()) if rows else 0,
                "ok": (int(str(rows[0][0]).strip()) if rows else 0) == 0,
            }

            # I2: nodes missing canonical_uri
            missing_uri = 0
            for label in gconfig.NODE_LABELS:
                if label in ("Event", "Source", "Approval"):
                    continue  # these may legitimately omit canonical_uri
                rows = _cypher_one(
                    cur,
                    f"MATCH (n:{label}) WHERE n.canonical_uri IS NULL "
                    "RETURN count(n) AS k",
                    return_cols="k agtype",
                )
                missing_uri += int(str(rows[0][0]).strip()) if rows else 0
            inv["missing_canonical_uri"] = {
                "count": missing_uri,
                "ok": missing_uri == 0,
            }

            # I3: promoted node without ASSERTS or LOCKS
            rows = _cypher_one(
                cur,
                "MATCH (n) WHERE n.status = 'promoted' "
                "AND NOT EXISTS { (:Event)-[:ASSERTS]->(n) } "
                "AND NOT EXISTS { (:Approval)-[:LOCKS]->(n) } "
                "RETURN count(n) AS k",
                return_cols="k agtype",
            )
            inv["promoted_without_provenance"] = {
                "count": int(str(rows[0][0]).strip()) if rows else 0,
                "ok": (int(str(rows[0][0]).strip()) if rows else 0) == 0,
            }

            # I4: MDM-sourced Entity nodes (proxy for "did MDM events
            # actually project into the graph?")
            rows = _cypher_one(
                cur,
                "MATCH (e:Event) WHERE e.source = 'fetch_mdm' "
                "RETURN count(e) AS k",
                return_cols="k agtype",
            )
            mdm_events = int(str(rows[0][0]).strip()) if rows else 0
            inv["mdm_events_projected"] = {
                "count": mdm_events,
                "ok": mdm_events > 0,  # zero MDM events = MDM is dead
            }

            # I5: Columns carrying MDM-derived properties
            rows = _cypher_one(
                cur,
                "MATCH (c:Column) "
                "WHERE c.mdm_business_name IS NOT NULL "
                "   OR c.attribute_desc IS NOT NULL "
                "   OR c.is_pii IS NOT NULL "
                "RETURN count(c) AS k",
                return_cols="k agtype",
            )
            mdm_columns = int(str(rows[0][0]).strip()) if rows else 0
            inv["columns_with_mdm_properties"] = {
                "count": mdm_columns,
                "ok": True,  # informational, not gating
            }

            stats.invariants = inv

            # I6: JSONL line count vs AGE Event node count
            n_event_nodes = stats.node_counts.get("Event", 0)
            stats.jsonl_vs_age_event_delta = jsonl_total - n_event_nodes

    except Exception as e:
        stats.replay_error = (
            stats.replay_error + " ;; "
            f"verify: {type(e).__name__}: {e}"
        ).strip(" ;")
        _fail(f"verification failed: {e}")

    return stats


# ─── report writer ────────────────────────────────────────────

def write_report(
    *, input_dir: Path, p0: Phase0Stats, p1: Phase1Stats, p2: Phase2Stats,
    report_path: Path, stats_path: Path,
) -> None:
    """Emit human + machine reports."""
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    md = []
    md.append("# Corpus Phase 0 + 1 + 2 probe report\n")
    md.append(f"_input dir:_ `{input_dir}`\n")

    md.append("\n## Phase 0 — extraction\n")
    md.append(f"- queries parsed: **{p0.n_queries}**")
    md.append(f"- parse errors: **{p0.n_parse_errors}** "
              f"({100 * p0.n_parse_errors / max(p0.n_queries, 1):.1f}%)")
    if p0.parse_error_files:
        md.append(f"  - files: {', '.join(p0.parse_error_files[:10])}"
                  f"{' …' if len(p0.parse_error_files) > 10 else ''}")
    md.append(f"- fingerprint collisions (duplicate SQL): "
              f"**{p0.fingerprint_collisions}**")

    md.append("\n### Layer-3 / Layer-4 field coverage")
    md.append("\n| field | queries with non-empty value | % |")
    md.append("|---|---:|---:|")
    for f, n in p0.field_coverage.items():
        pct = 100 * n / max(p0.n_queries, 1)
        md.append(f"| `{f}` | {n} | {pct:.0f}% |")

    md.append("\n### Intent class distribution")
    for ic, n in p0.intent_distribution.most_common():
        md.append(f"- `{ic}`: {n}")

    if p0.domain_distribution:
        md.append("\n### Business domain tags (top 10)")
        for d, n in p0.domain_distribution.most_common(10):
            md.append(f"- `{d}`: {n}")

    if p0.grain_distribution:
        md.append("\n### Implicit grains (top 10)")
        for g, n in p0.grain_distribution.most_common(10):
            md.append(f"- `{g}`: {n}")

    md.append("\n### Top 10 tables seen")
    for t, n in p0.top_tables.most_common(10):
        md.append(f"- `{t}`: {n}")

    md.append("\n## Phase 1 — event emission\n")
    md.append(f"- equivalence events: **{p1.n_equivalence}**")
    md.append(f"- cardinality events: **{p1.n_cardinality}**")
    md.append(f"- corpus semantic facts (verb layer): **{p1.n_corpus_facts}**")
    md.append("  - = metric / threshold / filter / time_grain / cohort / "
              "question_pattern events")
    md.append(f"- MDM entity-hint events: **{p1.n_mdm}** "
              f"({'attempted' if p1.mdm_attempted else 'skipped'})")
    md.append(f"- baseline-synonym events: **{p1.n_baseline}**")
    md.append(f"- table contexts discovered: **{p1.n_contexts}**")
    if p1.mdm_error:
        md.append(f"- MDM error: `{p1.mdm_error}`")
    md.append(f"\n- JSONL files: **{len(p1.jsonl_files)}**")
    md.append(f"- JSONL total event lines: **{p1.jsonl_total_lines}**")

    # ── MDM richness audit ──
    if p1.mdm_audits:
        md.append("\n## Phase 1.5 — MDM richness audit\n")
        md.append(f"_cache dir:_ `{p1.mdm_cache_dir}`")
        md.append(f"- tables profiled: **{len(p1.mdm_audits)}**")
        md.append(f"- cache misses: **{len(p1.mdm_cache_misses)}** "
                  f"({', '.join(p1.mdm_cache_misses[:10])}"
                  f"{' …' if len(p1.mdm_cache_misses) > 10 else ''})")

        # overall coverage stats
        total_sql_refs = sum(a.sql_columns_referenced for a in p1.mdm_audits)
        total_unknown = sum(len(a.sql_cols_not_in_mdm) for a in p1.mdm_audits)
        md.append(f"- SQL columns referenced across corpus: **{total_sql_refs}**")
        md.append(f"- SQL columns NOT in MDM: **{total_unknown}** "
                  f"({100 * total_unknown / max(total_sql_refs, 1):.1f}%)")

        md.append("\n### Per-table MDM profile — table-level metadata")
        md.append("\n| table | bq_fqn | table_type | feed_type | data_category | "
                  "decom | desc | dataset | own |")
        md.append("|---|---|---|---|---|:-:|:-:|:-:|:-:|")
        for a in sorted(p1.mdm_audits, key=lambda x: x.table_name):
            def y(b: bool) -> str:
                return "✓" if b else "—"
            md.append(
                f"| `{a.table_name}` | `{a.bq_fqn or '—'}` | "
                f"{a.table_type or '—'} | {a.feed_type or '—'} | "
                f"{a.data_category or '—'} | "
                f"{y(a.is_decommissioned)} | "
                f"{y(a.table_description_present)} | "
                f"{y(a.dataset_details_present)} | {y(a.ownership_present)} |"
            )

        md.append("\n### Per-table MDM profile — column richness")
        md.append("\n| table | SQL refs | MDM cols | cov % | cache | "
                  "biz | desc | type | fmt | "
                  "PK | dedupe | PII | CDE | mand | "
                  "part | clust | derived | ext-ref |")
        md.append("|---|---:|---:|---:|:-:|"
                  "---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for a in sorted(p1.mdm_audits, key=lambda x: -x.mdm_coverage_pct):
            def y(b: bool) -> str:
                return "✓" if b else "—"
            md.append(
                f"| `{a.table_name}` | {a.sql_columns_referenced} | "
                f"{a.mdm_columns} | {a.mdm_coverage_pct * 100:.0f}% | "
                f"{y(a.cache_hit)} | "
                f"{a.n_cols_with_business_name} | "
                f"{a.n_cols_with_attribute_desc} | "
                f"{a.n_cols_with_attribute_type} | "
                f"{a.n_cols_with_format} | "
                f"{a.n_cols_is_primary} | "
                f"{a.n_cols_is_dedupe_key} | "
                f"{a.n_cols_pii} | "
                f"{a.n_cols_critical_data_element} | "
                f"{a.n_cols_is_mandatory} | "
                f"{a.n_cols_partition} | "
                f"{a.n_cols_clustered} | "
                f"{a.n_cols_with_derived_logic} | "
                f"{a.n_cols_with_external_references} |"
            )

        md.append("\n### Per-table MDM gaps — classified")
        md.append(
            "\n_Each 'missing' SQL ref classified by WHY MDM doesn't have it._\n"
            "- **metric** = aggregation alias → already captured as "
            "`metric_observed` event (Metric node)\n"
            "- **threshold** = CASE WHEN alias → captured as "
            "`threshold_observed` event (Threshold node)\n"
            "- **synonym?** = fuzzy match to an MDM technical name "
            "(synonym candidate — should mint HAS_SYNONYM)\n"
            "- **real_gap** = actual MDM gap — refresh cache or "
            "escalate to data governance"
        )
        md.append("\n| table | total | metric | threshold | synonym? | real_gap |")
        md.append("|---|---:|---:|---:|---:|---:|")
        # corpus-wide totals
        tot_m = sum(len(a.missing_metric) for a in p1.mdm_audits)
        tot_t = sum(len(a.missing_threshold) for a in p1.mdm_audits)
        tot_s = sum(len(a.missing_synonym) for a in p1.mdm_audits)
        tot_g = sum(len(a.missing_real_gap) for a in p1.mdm_audits)
        tot_tot = tot_m + tot_t + tot_s + tot_g
        md.append(
            f"| **CORPUS TOTAL** | **{tot_tot}** | **{tot_m}** | "
            f"**{tot_t}** | **{tot_s}** | **{tot_g}** |"
        )
        for a in sorted(p1.mdm_audits,
                        key=lambda x: -(len(x.missing_metric)
                                        + len(x.missing_threshold)
                                        + len(x.missing_synonym)
                                        + len(x.missing_real_gap))):
            n_total = (len(a.missing_metric) + len(a.missing_threshold)
                       + len(a.missing_synonym) + len(a.missing_real_gap))
            if n_total == 0:
                continue
            md.append(
                f"| `{a.table_name}` | {n_total} | "
                f"{len(a.missing_metric)} | {len(a.missing_threshold)} | "
                f"{len(a.missing_synonym)} | {len(a.missing_real_gap)} |"
            )

        # Detailed breakdown for the *actionable* categories: synonyms
        # (mint HAS_SYNONYM) and real_gaps (refresh MDM). Skip metric +
        # threshold detail — we already capture them as Metric/Threshold
        # nodes, no human action needed.
        synonym_lines = []
        gap_lines = []
        for a in p1.mdm_audits:
            for sql_name, mdm_match in a.missing_synonym:
                synonym_lines.append(
                    f"  - `{a.table_name}.{sql_name}` ≈ `{mdm_match}`"
                )
            for sql_name in a.missing_real_gap:
                gap_lines.append(f"  - `{a.table_name}.{sql_name}`")
        if synonym_lines:
            md.append(f"\n#### Synonym candidates ({len(synonym_lines)})")
            md.append("_Each is a candidate `HAS_SYNONYM` edge from "
                      "the corpus-name to the MDM technical name._\n")
            md.extend(synonym_lines[:50])
            if len(synonym_lines) > 50:
                md.append(f"  - … and {len(synonym_lines) - 50} more")
        if gap_lines:
            md.append(f"\n#### Real MDM gaps ({len(gap_lines)})")
            md.append("_Columns the SQL uses that MDM doesn't have — "
                      "refresh cache or escalate to data governance._\n")
            md.extend(gap_lines[:50])
            if len(gap_lines) > 50:
                md.append(f"  - … and {len(gap_lines) - 50} more")

        md.append("\n_Column legend (per-col counts within MDM):_")
        md.append("- **biz** = has `business_name`")
        md.append("- **desc** = has `attribute_desc`")
        md.append("- **type** = has `attribute_type`")
        md.append("- **fmt** = has `attribute_format` (value_format hint)")
        md.append("- **PK** = `is_primary=true` (GROUNDED PK signal)")
        md.append("- **dedupe** = `is_dedupe_key=true` (natural-key signal)")
        md.append("- **PII** = `is_pii=true`")
        md.append("- **CDE** = `is_critical_data_element=true` (governance)")
        md.append("- **mand** = `is_mandatory=true` (NOT NULL inference)")
        md.append("- **part** = partition column")
        md.append("- **clust** = `is_clustered=true` (BQ filter ordering)")
        md.append("- **derived** = has `derived_logic`")
        md.append("- **ext-ref** = has `external_references` (declared FK)")

        md.append("\n### MDM coverage health check")
        zero_cov = [a for a in p1.mdm_audits if a.mdm_columns == 0]
        no_desc = [a for a in p1.mdm_audits if a.mdm_columns > 0
                   and a.n_cols_with_attribute_desc == 0]
        no_dataset = [a for a in p1.mdm_audits if not a.dataset_details_present]
        no_owner = [a for a in p1.mdm_audits if not a.ownership_present]
        md.append(f"- ❌ tables with ZERO MDM columns: **{len(zero_cov)}** "
                  f"{[a.table_name for a in zero_cov[:5]]}")
        md.append(f"- ⚠ tables with MDM cols but NO attribute_desc anywhere: "
                  f"**{len(no_desc)}** {[a.table_name for a in no_desc[:5]]}")
        md.append(f"- ⚠ tables missing dataset_details: **{len(no_dataset)}**")
        md.append(f"- ⚠ tables missing ownership: **{len(no_owner)}**")

    md.append("\n## Phase 2 — AGE projection + verification\n")
    md.append(f"- AGE enabled: **{p2.age_enabled}**")
    if not p2.age_enabled:
        md.append("- (set `LUMI_AGE_ENABLED=1` and re-run to populate AGE)")
    else:
        md.append(f"- replay ok: **{p2.replay_ok}**")
        if p2.replay_error:
            md.append(f"- error: `{p2.replay_error}`")
        md.append(f"- events replayed: **{p2.n_events_replayed}**")
        md.append(f"- JSONL ↔ AGE Event-node delta: "
                  f"**{p2.jsonl_vs_age_event_delta}** "
                  f"({'OK' if p2.jsonl_vs_age_event_delta == 0 else 'INVESTIGATE'})")

        md.append("\n### Node counts by label")
        md.append("\n| label | count |")
        md.append("|---|---:|")
        for label, n in sorted(p2.node_counts.items(), key=lambda x: -x[1]):
            md.append(f"| `{label}` | {n} |")

        md.append("\n### Edge counts by label")
        md.append("\n| label | count |")
        md.append("|---|---:|")
        for label, n in sorted(p2.edge_counts.items(), key=lambda x: -x[1]):
            md.append(f"| `{label}` | {n} |")

        if p2.source_distribution:
            md.append("\n### Event source distribution")
            for s, n in sorted(p2.source_distribution.items(), key=lambda x: -x[1]):
                md.append(f"- `{s}`: {n}")

        if p2.confidence_distribution:
            md.append("\n### Confidence distribution (selected node types)")
            for k, n in sorted(p2.confidence_distribution.items()):
                md.append(f"- `{k}`: {n}")

        md.append("\n### Schema invariants")
        for name, r in p2.invariants.items():
            tag = "✅" if r["ok"] else "❌"
            md.append(f"- {tag} `{name}`: count={r['count']}")

        if p2.samples_by_label:
            md.append("\n### Sample nodes (3 per label) — eyeball for canonical_uri, confidence, evidence")
            for label, samples in p2.samples_by_label.items():
                md.append(f"\n#### {label}")
                for s in samples:
                    md.append("```json")
                    md.append(json.dumps(s, indent=2, default=str))
                    md.append("```")

    report_path.write_text("\n".join(md))

    machine = {
        "input_dir": str(input_dir),
        "phase0": {
            "n_queries": p0.n_queries,
            "n_parse_errors": p0.n_parse_errors,
            "parse_error_files": p0.parse_error_files,
            "field_coverage": p0.field_coverage,
            "intent_distribution": dict(p0.intent_distribution),
            "domain_distribution": dict(p0.domain_distribution),
            "grain_distribution": dict(p0.grain_distribution),
            "top_tables": dict(p0.top_tables.most_common(50)),
            "top_columns": dict(p0.top_columns.most_common(50)),
            "fingerprint_collisions": p0.fingerprint_collisions,
        },
        "phase1": {
            "n_equivalence": p1.n_equivalence,
            "n_cardinality": p1.n_cardinality,
            "n_corpus_facts": p1.n_corpus_facts,
            "n_mdm": p1.n_mdm,
            "n_baseline": p1.n_baseline,
            "n_contexts": p1.n_contexts,
            "mdm_attempted": p1.mdm_attempted,
            "mdm_error": p1.mdm_error,
            "mdm_cache_dir": p1.mdm_cache_dir,
            "mdm_cache_misses": p1.mdm_cache_misses,
            "mdm_audits": [a.__dict__ for a in p1.mdm_audits],
            "jsonl_total_lines": p1.jsonl_total_lines,
            "jsonl_files": p1.jsonl_files,
        },
        "phase2": {
            "age_enabled": p2.age_enabled,
            "replay_ok": p2.replay_ok,
            "replay_error": p2.replay_error,
            "n_events_replayed": p2.n_events_replayed,
            "node_counts": p2.node_counts,
            "edge_counts": p2.edge_counts,
            "source_distribution": p2.source_distribution,
            "confidence_distribution": p2.confidence_distribution,
            "invariants": p2.invariants,
            "jsonl_vs_age_event_delta": p2.jsonl_vs_age_event_delta,
        },
    }
    stats_path.write_text(json.dumps(machine, indent=2, default=str))


# ─── main ─────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path,
        default=REPO_ROOT / "data" / "gold_queries",
        help="Directory of .sql files to probe (default: data/gold_queries/).",
    )
    parser.add_argument(
        "--no-mdm", action="store_true",
        help="Skip MDM-dependent event emission (entity_hint, baseline_synonym).",
    )
    parser.add_argument(
        "--no-age", action="store_true",
        help="Skip Phase 2 (replay + verify). Phase 0/1 still run.",
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Wipe data/ontology/events/ before running (clean slate).",
    )
    parser.add_argument(
        "--report-path", type=Path,
        default=REPO_ROOT / "review_queue" / "CORPUS_PROBE_REPORT.md",
    )
    parser.add_argument(
        "--stats-path", type=Path,
        default=REPO_ROOT / "data" / "probe" / "corpus_phase012_stats.json",
    )
    args = parser.parse_args()

    if not args.input.exists():
        _fail(f"Input dir not found: {args.input}")
        return 1

    if args.fresh:
        events_dir = REPO_ROOT / "data" / "ontology" / "events"
        if events_dir.exists():
            _warn(f"--fresh: wiping {events_dir}")
            shutil.rmtree(events_dir)

    _hdr("Phase 0 — extraction audit")
    fps, p0 = run_phase0(args.input)
    if not fps:
        return 1
    _pass(f"{p0.n_queries} queries parsed, {p0.n_parse_errors} parse errors")
    _info(f"intents: {dict(p0.intent_distribution.most_common(5))}")
    nonzero_fields = sum(1 for v in p0.field_coverage.values() if v > 0)
    _info(f"{nonzero_fields}/{len(p0.field_coverage)} Layer-3/4 fields fire on this corpus")

    _hdr("Phase 1 — event emission via real pipeline hooks")
    p1 = run_phase1(fps, with_mdm=not args.no_mdm)
    _pass(f"{p1.jsonl_total_lines} JSONL lines across {len(p1.jsonl_files)} files")

    if args.no_age:
        _warn("--no-age: skipping Phase 2")
        p2 = Phase2Stats()
    else:
        _hdr("Phase 2 — AGE replay + verify")
        p2 = run_phase2(p1.jsonl_total_lines)
        if p2.age_enabled and p2.replay_ok:
            failed_inv = [n for n, r in p2.invariants.items() if not r["ok"]]
            if failed_inv:
                _fail(f"invariants failed: {failed_inv}")
            else:
                _pass("all schema invariants OK")

    _hdr("Writing reports")
    write_report(
        input_dir=args.input, p0=p0, p1=p1, p2=p2,
        report_path=args.report_path, stats_path=args.stats_path,
    )
    _pass(f"human report → {args.report_path}")
    _pass(f"stats JSON  → {args.stats_path}")

    # exit non-zero if anything looks broken
    if p0.n_parse_errors > 0:
        return 2
    if p2.age_enabled and not p2.replay_ok:
        return 3
    if p2.age_enabled:
        if any(not r["ok"] for r in p2.invariants.values()):
            return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
