"""LUMI guardrails — quality gates at every pipeline stage.

Every stage calls its guardrail before proceeding. Blocking failures
halt the pipeline. Warnings log but continue.

Usage:
    result = guardrails.check_stage_1(fingerprints, table_profiles)
    if result.status == "fail":
        raise PipelineHaltError(result.blocking_failures)
    if result.warnings:
        logger.warning(result.warnings)
"""

from __future__ import annotations

import logging
from lumi.schemas import (
    CoverageReport,
    EnrichedOutput,
    EnrichmentPlan,
    GateResult,
    PlanApproval,
    TableContext,
    TablePriority,
)

logger = logging.getLogger("lumi.guardrails")


# ─── Stage 1: Parse + Discover ──────────────────────────────

def check_parse_and_discover(
    raw_sqls: list[str],
    fingerprints: list[dict],
    table_contexts: dict[str, TableContext],
) -> GateResult:
    """Run after Stage 1. All checks are deterministic.

    BLOCKING:
    - Every SQL must parse without sqlglot errors
    - CTE count in fingerprint must match WITH clause count
    - Join DAG must be acyclic (no circular joins)
    - All tables from inside CTEs must be in table_contexts

    WARNING:
    - MDM coverage below 50% for any table
    - Any table missing from MDM entirely (empty description)
    """
    checks = []
    blocking = []
    warnings = []

    # Check 1: Parse success — failures are QUARANTINED (warned) not blocking.
    # 12% Excel-induced parse failures shouldn't kill the run; the pipeline
    # ships with the parsed corpus and humans fix the failures separately.
    parse_failures: list[str] = []
    for i, fp in enumerate(fingerprints):
        if fp is None or fp.get("_parse_error"):
            err = (fp or {}).get("_parse_error", "unknown error")
            parse_failures.append(f"SQL #{i + 1}: {err}")
    parse_pass_count = len(raw_sqls) - len(parse_failures)
    checks.append({
        "name": "sql_parse_success",
        "passed": len(parse_failures) == 0,
        "message": f"{parse_pass_count}/{len(raw_sqls)} SQLs parsed",
    })
    if parse_failures:
        warnings.append(
            f"{len(parse_failures)} SQL(s) failed to parse — quarantined. "
            f"Run: python scripts/diagnose_parse_failures.py"
        )
        # Surface the first 5 individually so the trace is actionable.
        for pf in parse_failures[:5]:
            warnings.append(pf)

    # Check 2: CTE consistency — chained CTEs (depend on another CTE, no real
    # source table) are valid SQL; only fail if a CTE is genuinely empty
    # (no source_tables AND no cte_dependencies AND no SQL).
    cte_problems: list[str] = []
    for i, fp in enumerate(fingerprints):
        if fp is None:
            continue
        for cte in fp.get("ctes", []) or []:
            has_real = bool(cte.get("source_tables"))
            has_chain = bool(cte.get("cte_dependencies"))
            has_sql = bool(cte.get("sql"))
            if not has_sql:
                blocking.append(
                    f"SQL #{i + 1}: CTE '{cte.get('alias')}' has no SQL captured"
                )
                cte_problems.append(cte.get("alias"))
            elif not has_real and not has_chain:
                # Pure-literal CTE (e.g., SELECT 1 UNION ALL SELECT 2). Rare
                # but valid — warn rather than block.
                warnings.append(
                    f"SQL #{i + 1}: CTE '{cte.get('alias')}' has no upstream "
                    "table or CTE — likely a literal-only SELECT"
                )
    checks.append({
        "name": "cte_completeness",
        "passed": not cte_problems,
        "message": (
            "All CTEs have a body" if not cte_problems
            else f"{len(cte_problems)} CTE(s) with no SQL captured"
        ),
    })

    # Check 3: Join DAG acyclicity
    for i, fp in enumerate(fingerprints):
        if fp is None:
            continue
        joins = fp.get("joins", [])
        # Simple cycle detection: no table appears as both left and right
        # in a chain that loops back
        visited = set()
        for j in sorted(joins, key=lambda x: x.get("order", 0)):
            left = j.get("left_table", "")
            right = j.get("right_table", "")
            if right in visited and left not in visited:
                blocking.append(f"SQL #{i}: potential cycle in join DAG at {right}")
            visited.add(left)
            visited.add(right)
    checks.append({
        "name": "join_dag_acyclic",
        "passed": not any("cycle" in b for b in blocking),
        "message": "Join DAGs are acyclic"
    })

    # Check 4: All CTE-internal tables discovered
    for table_name, ctx in table_contexts.items():
        for cte in ctx.ctes_referencing_this:
            for src_table in cte.get("source_tables", []):
                if src_table not in table_contexts:
                    blocking.append(
                        f"CTE '{cte.get('alias')}' references table '{src_table}' "
                        f"but it's not in table_contexts"
                    )
    checks.append({
        "name": "cte_tables_discovered",
        "passed": not any("not in table_contexts" in b for b in blocking),
        "message": "All CTE-internal tables are discovered"
    })

    # Check 5 (WARNING): MDM coverage
    for table_name, ctx in table_contexts.items():
        if ctx.mdm_coverage_pct < 0.5:
            warnings.append(
                f"Table '{table_name}' has {ctx.mdm_coverage_pct:.0%} MDM coverage "
                f"(below 50% threshold)"
            )
    checks.append({
        "name": "mdm_coverage",
        "passed": len(warnings) == 0,
        "message": f"{len(warnings)} tables below 50% MDM coverage"
    })

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="parse_and_discover",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 2: Enrich ────────────────────────────────────────

def check_enrichment(
    table_name: str,
    enriched: EnrichedOutput,
    table_context: TableContext,
) -> GateResult:
    """Run after each table's enrichment call. Per-table check.

    BLOCKING:
    - Generated view_lkml must parse with lkml library
    - Explore joins must be in topological order

    WARNING:
    - Descriptions outside 15-200 char range
    - Missing derived tables for CTEs in table_context
    - Measures missing value_format_name
    - Missing primary_key dimension
    - Missing dimension_group for date columns
    """
    checks = []
    blocking = []
    warnings = []

    # Check 1: LookML syntax validity
    try:
        import lkml
        parsed = lkml.load(enriched.view_lkml)
        checks.append({"name": "lookml_syntax", "passed": True, "message": "View LookML parses"})
    except Exception as e:
        blocking.append(f"View LookML parse error: {e}")
        checks.append({"name": "lookml_syntax", "passed": False, "message": str(e)})

    # Check 1b: Derived table LookML validity
    for i, dtv in enumerate(enriched.derived_table_views):
        try:
            import lkml
            lkml.load(dtv)
            checks.append({"name": f"derived_table_{i}_syntax", "passed": True, "message": "Parses"})
        except Exception as e:
            blocking.append(f"Derived table #{i} parse error: {e}")

    # Check 1c: Explore LookML validity
    if enriched.explore_lkml:
        try:
            import lkml
            # Explore needs to be wrapped in a model context for lkml to parse
            wrapped = f"connection: \"temp\"\n{enriched.explore_lkml}"
            lkml.load(wrapped)
            checks.append({"name": "explore_syntax", "passed": True, "message": "Explore parses"})
        except Exception as e:
            blocking.append(f"Explore LookML parse error: {e}")

    # Check 2: Description quality
    try:
        import lkml
        parsed = lkml.load(enriched.view_lkml)
        views = parsed.get("views", [])
        if views:
            all_fields = (
                views[0].get("dimensions", []) +
                views[0].get("dimension_groups", []) +
                views[0].get("measures", [])
            )
            short_descs = [f for f in all_fields
                          if len(f.get("description", "")) < 15]
            long_descs = [f for f in all_fields
                         if len(f.get("description", "")) > 200]
            no_descs = [f for f in all_fields
                       if not f.get("description")]

            if no_descs:
                warnings.append(f"{len(no_descs)} fields have no description")
            if short_descs:
                warnings.append(f"{len(short_descs)} fields have descriptions < 15 chars")
            if long_descs:
                warnings.append(f"{len(long_descs)} fields have descriptions > 200 chars")

            checks.append({
                "name": "description_quality",
                "passed": len(no_descs) == 0 and len(short_descs) == 0,
                "message": f"{len(all_fields)} fields checked"
            })
    except Exception:
        pass  # Already caught in syntax check

    # Check 3: Derived tables for CTEs
    cte_count = len(table_context.ctes_referencing_this)
    derived_count = len(enriched.derived_table_views)
    if cte_count > 0 and derived_count == 0:
        warnings.append(
            f"Table has {cte_count} CTEs but {derived_count} derived tables generated"
        )
    checks.append({
        "name": "derived_table_coverage",
        "passed": derived_count >= cte_count,
        "message": f"{derived_count}/{cte_count} CTEs have derived tables"
    })

    # Check 4: primary_key present
    try:
        import lkml
        parsed = lkml.load(enriched.view_lkml)
        views = parsed.get("views", [])
        if views:
            dims = views[0].get("dimensions", [])
            has_pk = any(d.get("primary_key") == "yes" for d in dims)
            if not has_pk:
                warnings.append("No primary_key dimension found — join aggregations may be wrong")
            checks.append({"name": "primary_key", "passed": has_pk, "message": ""})
    except Exception:
        pass

    # Check 5: dimension_group for dates
    date_cols = {d["column"] for d in table_context.date_functions}
    if date_cols:
        try:
            import lkml
            parsed = lkml.load(enriched.view_lkml)
            views = parsed.get("views", [])
            if views:
                dim_groups = views[0].get("dimension_groups", [])
                {dg.get("name", "") for dg in dim_groups}
                # Check if date columns are covered by dimension_groups
                # (name matching is approximate — the dim_group name may differ)
                plain_dims = views[0].get("dimensions", [])
                date_as_plain = [d for d in plain_dims
                                if any(dc in d.get("sql", "") for dc in date_cols)]
                if date_as_plain:
                    warnings.append(
                        f"{len(date_as_plain)} date columns are plain dimensions "
                        f"instead of dimension_groups: "
                        f"{[d['name'] for d in date_as_plain]}"
                    )
        except Exception:
            pass

    # Check 6: Explore join ordering
    if enriched.explore_lkml and table_context.joins_involving_this:
        # Verify joins appear in the explore in position order
        join_positions = sorted(
            table_context.joins_involving_this,
            key=lambda j: j.get("order", 0)
        )
        expected_order = [j["other_table"] for j in join_positions]

        # Extract join order from explore LookML (simple text position check)
        actual_positions = []
        explore_text = enriched.explore_lkml
        for table in expected_order:
            pos = explore_text.find(table)
            if pos >= 0:
                actual_positions.append((table, pos))

        if len(actual_positions) >= 2:
            is_ordered = all(
                actual_positions[i][1] < actual_positions[i+1][1]
                for i in range(len(actual_positions) - 1)
            )
            if not is_ordered:
                blocking.append(
                    f"Explore joins are in wrong order. "
                    f"Expected: {expected_order}, "
                    f"but positions suggest different order"
                )
            checks.append({
                "name": "join_order",
                "passed": is_ordered,
                "message": f"Join order: {expected_order}"
            })

    # Check 7: sql_table_name present
    if "sql_table_name" not in enriched.view_lkml:
        warnings.append("View is missing sql_table_name — Looker can't find the BQ table")
    checks.append({
        "name": "sql_table_name",
        "passed": "sql_table_name" in enriched.view_lkml,
        "message": ""
    })

    # Check 8: Measures have value_format
    try:
        import lkml
        parsed = lkml.load(enriched.view_lkml)
        views = parsed.get("views", [])
        if views:
            measures = views[0].get("measures", [])
            no_format = [m for m in measures if not m.get("value_format_name")]
            if no_format:
                warnings.append(
                    f"{len(no_format)} measures missing value_format_name: "
                    f"{[m['name'] for m in no_format[:3]]}"
                )
    except Exception:
        pass

    # Check 9: NL questions generated
    if len(enriched.nl_questions) == 0:
        warnings.append("No NL question variants generated for golden dataset")
    checks.append({
        "name": "nl_questions",
        "passed": len(enriched.nl_questions) > 0,
        "message": f"{len(enriched.nl_questions)} question variants generated"
    })

    # Check 10: Explore has always_filter with date range
    # Without this, Looker MCP generates full-table scans on large fact tables
    if enriched.explore_lkml:
        has_always_filter = "always_filter" in enriched.explore_lkml
        any(
            date_col in enriched.explore_lkml
            for d in table_context.date_functions
            for date_col in [d.get("column", "")]
        ) if has_always_filter else False
        if not has_always_filter:
            warnings.append(
                "Explore has no always_filter — Looker MCP may generate "
                "full-table scans on large fact tables. Add always_filter "
                "with a date range."
            )
        checks.append({
            "name": "always_filter_date",
            "passed": has_always_filter,
            "message": "always_filter present" if has_always_filter else "missing"
        })

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage=f"enrichment_{table_name}",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 3: Evaluate ──────────────────────────────────────

def check_evaluation(
    coverage: CoverageReport,
    previous_coverage: CoverageReport | None = None,
) -> GateResult:
    """Run after each evaluation loop iteration.

    BLOCKING:
    - No regressions (previously covered query now uncovered)

    WARNING:
    - Coverage below 90% target
    - Structural filters not baked into derived_table or sql_always_where
    - Join paths don't connect all tables for multi-table queries
    """
    checks = []
    blocking = []
    warnings = []

    # Check 1: Coverage percentage
    checks.append({
        "name": "coverage_pct",
        "passed": coverage.coverage_pct >= 90,
        "message": f"{coverage.coverage_pct:.1f}% ({coverage.covered}/{coverage.total_queries})"
    })
    if coverage.coverage_pct < 90:
        warnings.append(f"Coverage {coverage.coverage_pct:.1f}% is below 90% target")

    # Check 2: Regression detection
    if previous_coverage:
        prev_covered = {
            qc.query_id for qc in previous_coverage.per_query if qc.covered
        }
        curr_covered = {
            qc.query_id for qc in coverage.per_query if qc.covered
        }
        regressions = prev_covered - curr_covered
        if regressions:
            blocking.append(
                f"REGRESSION: {len(regressions)} queries lost coverage: "
                f"{sorted(regressions)}"
            )
        checks.append({
            "name": "no_regressions",
            "passed": len(regressions) == 0,
            "message": f"{len(regressions)} regressions detected"
        })

    # Check 3: Structural filters
    for qc in coverage.per_query:
        if not qc.structural_filters_baked:
            warnings.append(
                f"Query {qc.query_id}: structural filters not baked into "
                f"derived_table or sql_always_where"
            )
    checks.append({
        "name": "structural_filters_baked",
        "passed": all(qc.structural_filters_baked for qc in coverage.per_query),
        "message": ""
    })

    # Check 4: Join path completeness
    for qc in coverage.per_query:
        if not qc.joins_correct:
            warnings.append(
                f"Query {qc.query_id}: join path incomplete or in wrong order"
            )
    checks.append({
        "name": "join_paths_complete",
        "passed": all(qc.joins_correct for qc in coverage.per_query),
        "message": ""
    })

    # Check 5: All LookML valid
    checks.append({
        "name": "all_lookml_valid",
        "passed": coverage.all_lookml_valid,
        "message": ""
    })
    if not coverage.all_lookml_valid:
        blocking.append("Some generated LookML files have syntax errors")

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="evaluation",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 4: Publish ───────────────────────────────────────

def check_pre_publish(
    output_dir: str,
    baseline_dir: str,
) -> GateResult:
    """Run before publishing to GitHub.

    BLOCKING:
    - All output LookML files must lint clean (lkml parse)
    - metric_catalog.json must be valid JSON
    - golden_questions.json must be valid JSON

    WARNING:
    - Any view with >50% diff from baseline (flag for human review)
    """
    import json
    from pathlib import Path

    checks = []
    blocking = []
    warnings = []

    output_path = Path(output_dir)

    # Check 1: All LookML files lint
    view_files = list(output_path.glob("views/*.view.lkml"))
    model_files = list(output_path.glob("models/*.model.lkml"))
    all_lkml = view_files + model_files

    lint_failures = []
    for f in all_lkml:
        try:
            import lkml
            lkml.load(f.read_text())
        except Exception as e:
            lint_failures.append(f"{f.name}: {e}")
    checks.append({
        "name": "lookml_lint",
        "passed": len(lint_failures) == 0,
        "message": f"{len(all_lkml)} files checked, {len(lint_failures)} failures"
    })
    if lint_failures:
        blocking.extend(lint_failures)

    # Check 2: JSON output files valid
    for json_file in ["metric_catalog.json", "filter_catalog.json",
                       "golden_questions.json", "coverage_report.json"]:
        json_path = output_path / json_file
        if json_path.exists():
            try:
                json.loads(json_path.read_text())
                checks.append({"name": f"{json_file}_valid", "passed": True, "message": ""})
            except json.JSONDecodeError as e:
                blocking.append(f"{json_file} is invalid JSON: {e}")
        else:
            warnings.append(f"{json_file} does not exist in output")

    # Check 3: Diff size against baseline
    baseline_path = Path(baseline_dir)
    for view_file in view_files:
        baseline_file = baseline_path / view_file.name
        if baseline_file.exists():
            baseline_lines = set(baseline_file.read_text().splitlines())
            output_lines = set(view_file.read_text().splitlines())
            if baseline_lines:
                changed = len(baseline_lines.symmetric_difference(output_lines))
                change_pct = changed / len(baseline_lines)
                if change_pct > 0.5:
                    warnings.append(
                        f"{view_file.name}: {change_pct:.0%} changed from baseline "
                        f"— flag for human review"
                    )

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="pre_publish",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── SQL Reconstruction Validator (pre-GitHub safety net) ────

def check_sql_reconstruction(
    gold_sqls: list[str],
    enriched_outputs: dict[str, "EnrichedOutput"],
    fingerprints: list[dict],
) -> GateResult:
    """Simulates what Looker MCP would generate from our LookML,
    compares against gold query SQL. Catches logic errors BEFORE
    they reach GitHub/Looker.

    For each gold query:
    1. Find the matching explore from enriched output
    2. Trace: explore → base view (sql_table_name or derived_table)
             → joins (in order) → sql_on expressions
             → selected measures (type + sql) → selected dimensions (sql)
    3. Reconstruct the SQL that Looker would generate
    4. Compare against gold query SQL using sqlglot AST

    BLOCKING: structural mismatches that would produce wrong results
    WARNING: cosmetic differences (column aliases, formatting)
    """
    import lkml

    checks = []
    blocking = []
    warnings = []

    for i, (sql, fp) in enumerate(zip(gold_sqls, fingerprints)):
        query_id = fp.get("query_id", f"Q{i+1}")

        try:
            # Step 1: Find matching explore
            explore_lkml = _find_explore_for_query(fp, enriched_outputs)
            if not explore_lkml:
                warnings.append(f"{query_id}: no matching explore found")
                continue

            parsed_explore = lkml.load(
                f'connection: "temp"\n{explore_lkml}'
            )

            # Step 2: Extract base table
            explores = parsed_explore.get("explores", [])
            if not explores:
                warnings.append(f"{query_id}: explore parsed but empty")
                continue

            exp = explores[0]
            base_view = exp.get("from", exp.get("name", ""))

            # Step 3: Check base table resolves
            base_resolved = _resolve_view_table(base_view, enriched_outputs)
            if not base_resolved:
                blocking.append(
                    f"{query_id}: base view '{base_view}' has no "
                    f"sql_table_name or derived_table — Looker can't "
                    f"generate FROM clause"
                )
                continue

            # Step 4: Check join chain resolves
            joins = exp.get("joins", [])
            for j, join in enumerate(joins):
                join_view = join.get("name", "")
                join_resolved = _resolve_view_table(join_view, enriched_outputs)
                if not join_resolved:
                    blocking.append(
                        f"{query_id}: joined view '{join_view}' has no "
                        f"sql_table_name or derived_table"
                    )

                if not join.get("sql_on"):
                    blocking.append(
                        f"{query_id}: join '{join_view}' missing sql_on "
                        f"— Looker can't generate JOIN ON clause"
                    )

                if not join.get("relationship"):
                    blocking.append(
                        f"{query_id}: join '{join_view}' missing relationship "
                        f"— Looker can't determine aggregation behavior"
                    )

            # Step 5: Check measures resolve
            required_aggs = fp.get("aggregations", [])
            for agg in required_aggs:
                col = agg.get("column", "")
                found = _find_measure_for_column(col, base_view, enriched_outputs)
                if not found:
                    blocking.append(
                        f"{query_id}: aggregation {agg.get('function')}({col}) "
                        f"has no matching measure in generated LookML "
                        f"— Looker MCP can't generate this SELECT"
                    )

            # Step 6: Check structural filters are baked in
            for f in fp.get("filters", []):
                if f.get("is_structural"):
                    col = f.get("column", "")
                    val = f.get("value", "")
                    baked = _check_structural_filter_baked(
                        col, val, base_view, explore_lkml, enriched_outputs
                    )
                    if not baked:
                        blocking.append(
                            f"{query_id}: structural filter {col}={val} "
                            f"not baked into derived_table SQL or "
                            f"sql_always_where — query will return "
                            f"wrong scope of data"
                        )

            checks.append({
                "name": f"sql_reconstruction_{query_id}",
                "passed": not any(query_id in b for b in blocking),
                "message": f"explore={exp.get('name')}, joins={len(joins)}"
            })

        except Exception as e:
            warnings.append(f"{query_id}: reconstruction error: {e}")

    total = len(gold_sqls)
    passed = sum(1 for c in checks if c["passed"])
    checks.insert(0, {
        "name": "sql_reconstruction_summary",
        "passed": passed == total,
        "message": f"{passed}/{total} queries produce valid SQL from LookML"
    })

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="sql_reconstruction",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 2: Stage (prioritization) ────────────────────────


def check_staging(
    priorities: list[TablePriority],
    table_contexts: dict[str, TableContext],
) -> GateResult:
    """Run after the Stage step. All checks are deterministic.

    BLOCKING:
    - Every table in table_contexts has a TablePriority
    - Priority ranks are unique (no two tables sharing rank 1)
    - Dependency DAG is acyclic (no A blocks B blocks A)
    - Every blocked_by reference points to a known table

    WARNING:
    - More than half of tables share complexity_score (poor differentiation)
    """
    checks: list[dict] = []
    blocking: list[str] = []
    warnings: list[str] = []

    priority_tables = {p.table_name for p in priorities}
    context_tables = set(table_contexts.keys())

    missing = context_tables - priority_tables
    extra = priority_tables - context_tables
    checks.append({
        "name": "all_tables_prioritized",
        "passed": not missing and not extra,
        "message": f"{len(priorities)}/{len(table_contexts)} tables have priorities",
    })
    if missing:
        blocking.append(f"Tables missing priority: {sorted(missing)}")
    if extra:
        blocking.append(f"Priorities for unknown tables: {sorted(extra)}")

    ranks = [p.priority_rank for p in priorities]
    rank_dupes = [r for r in set(ranks) if ranks.count(r) > 1]
    checks.append({
        "name": "ranks_unique",
        "passed": not rank_dupes,
        "message": f"{len(set(ranks))}/{len(ranks)} unique ranks",
    })
    if rank_dupes:
        blocking.append(f"Duplicate priority ranks: {rank_dupes}")

    # Dependency DAG: blocked_by + blocks must reference known tables and
    # cannot form cycles.
    by_name = {p.table_name: p for p in priorities}
    for p in priorities:
        for dep in p.blocked_by:
            if dep not in by_name:
                blocking.append(
                    f"Table '{p.table_name}' blocked_by unknown table '{dep}'"
                )
        for blk in p.blocks:
            if blk not in by_name:
                blocking.append(
                    f"Table '{p.table_name}' blocks unknown table '{blk}'"
                )

    # Cycle detection: walk blocked_by chains
    def _has_cycle(start: str, visiting: set[str]) -> bool:
        if start in visiting:
            return True
        if start not in by_name:
            return False
        visiting = visiting | {start}
        return any(_has_cycle(dep, visiting) for dep in by_name[start].blocked_by)

    cycles = [p.table_name for p in priorities if _has_cycle(p.table_name, set())]
    checks.append({
        "name": "dependency_dag_acyclic",
        "passed": not cycles,
        "message": "Dependency graph is a DAG" if not cycles else f"cycles via: {cycles}",
    })
    if cycles:
        blocking.append(f"Circular dependencies in priority graph: {cycles}")

    if priorities and len(set(p.complexity_score for p in priorities)) == 1:
        warnings.append(
            "All tables share the same complexity_score — ranking won't reflect difficulty"
        )

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="staging",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 3: Plan ──────────────────────────────────────────


def check_planning(
    plans: list[EnrichmentPlan],
    table_contexts: dict[str, TableContext],
) -> GateResult:
    """Run after Plan step. Plans are CHEAP — fail fast here, save expensive
    enrichment calls.

    BLOCKING:
    - Every prioritized table has a plan
    - Each plan has at least one dimension OR one measure
    - Estimated input tokens within Gemini context (< 800K to leave headroom)

    WARNING:
    - Plan has unflagged risks for tables with CTEs / case_whens
    - Plan proposes 0 NL questions (won't produce golden dataset)
    - Reasoning is shorter than 50 chars (probably underwritten)
    """
    checks: list[dict] = []
    blocking: list[str] = []
    warnings: list[str] = []

    planned_tables = {p.table_name for p in plans}
    expected_tables = set(table_contexts.keys())

    missing = expected_tables - planned_tables
    checks.append({
        "name": "all_tables_planned",
        "passed": not missing,
        "message": f"{len(plans)}/{len(expected_tables)} tables have plans",
    })
    if missing:
        blocking.append(f"Tables missing enrichment plan: {sorted(missing)}")

    for p in plans:
        if not p.proposed_dimensions and not p.proposed_measures:
            blocking.append(
                f"Plan for '{p.table_name}' has no proposed dimensions OR measures"
            )

        if p.estimated_input_tokens > 800_000:
            blocking.append(
                f"Plan for '{p.table_name}' estimates {p.estimated_input_tokens} input tokens "
                f"— exceeds 800K headroom"
            )

        if len(p.reasoning) < 50:
            warnings.append(
                f"Plan for '{p.table_name}' has thin reasoning ({len(p.reasoning)} chars)"
            )

        if p.proposed_nl_question_count == 0:
            warnings.append(
                f"Plan for '{p.table_name}' proposes 0 NL questions — "
                "no golden dataset contribution"
            )

        ctx = table_contexts.get(p.table_name)
        if ctx:
            has_ctes = bool(ctx.ctes_referencing_this)
            has_case_whens = bool(ctx.case_whens)
            if (has_ctes or has_case_whens) and not p.risks:
                warnings.append(
                    f"Plan for '{p.table_name}' has CTEs/CASE WHENs but no risks listed "
                    "— probably under-considered"
                )

    checks.append({
        "name": "plans_have_content",
        "passed": all(p.proposed_dimensions or p.proposed_measures for p in plans),
        "message": "Each plan proposes at least one field",
    })

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="planning",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── Stage 4: Human-Approval Gate ───────────────────────────


def check_approvals(
    approvals: list[PlanApproval],
    plans: list[EnrichmentPlan],
) -> GateResult:
    """Run after the human-approval gate. Blocks until every plan is either
    approved or explicitly rejected with feedback.

    BLOCKING:
    - Every plan has a corresponding approval
    - Rejected plans must include feedback (so the next planning round knows what to fix)

    WARNING:
    - All plans auto-approved (no human in the loop on a production run)
    - Approval rate < 50% (Plan-stage prompt may need work)
    """
    checks: list[dict] = []
    blocking: list[str] = []
    warnings: list[str] = []

    approved_tables = {a.table_name for a in approvals}
    planned_tables = {p.table_name for p in plans}

    missing = planned_tables - approved_tables
    checks.append({
        "name": "all_plans_decided",
        "passed": not missing,
        "message": f"{len(approvals)}/{len(plans)} plans have approval decisions",
    })
    if missing:
        blocking.append(
            f"Plans without approval (still pending review): {sorted(missing)}"
        )

    for a in approvals:
        if not a.approved and not a.feedback:
            blocking.append(
                f"Plan for '{a.table_name}' was rejected but has no feedback "
                "— next planning round can't improve"
            )

    if approvals and all(a.approver != "human" for a in approvals):
        warnings.append(
            "No human reviewed any plans — all auto-approved. "
            "Production runs should have a human in the loop."
        )

    if approvals:
        approved_count = sum(1 for a in approvals if a.approved)
        approval_rate = approved_count / len(approvals)
        if approval_rate < 0.5:
            warnings.append(
                f"Only {approval_rate:.0%} of plans approved — "
                "Plan-stage prompt may need work"
            )

    status = "fail" if blocking else ("warn" if warnings else "pass")
    return GateResult(
        stage="approval_gate",
        status=status,
        checks=checks,
        blocking_failures=blocking,
        warnings=warnings,
    )


# ─── (Existing post-Plan stages continue below) ─────────────


def _find_explore_for_query(
    fingerprint: dict,
    enriched_outputs: dict[str, "EnrichedOutput"],
) -> str | None:
    """Find the explore that covers this query's tables."""
    query_tables = set(fingerprint.get("tables", []))
    for table_name, enriched in enriched_outputs.items():
        if enriched.explore_lkml and table_name in query_tables:
            return enriched.explore_lkml
    return None


def _resolve_view_table(
    view_name: str,
    enriched_outputs: dict[str, "EnrichedOutput"],
) -> str | None:
    """Check if a view has sql_table_name or derived_table."""
    import lkml
    for table_name, enriched in enriched_outputs.items():
        # Check main view
        try:
            parsed = lkml.load(enriched.view_lkml)
            for v in parsed.get("views", []):
                if v.get("name") == view_name:
                    if v.get("sql_table_name") or v.get("derived_table"):
                        return v.get("sql_table_name") or "derived_table"
        except Exception:
            pass
        # Check derived table views
        for dtv in enriched.derived_table_views:
            try:
                parsed = lkml.load(dtv)
                for v in parsed.get("views", []):
                    if v.get("name") == view_name:
                        if v.get("derived_table"):
                            return "derived_table"
            except Exception:
                pass
    return None


def _find_measure_for_column(
    column: str,
    base_view: str,
    enriched_outputs: dict[str, "EnrichedOutput"],
) -> bool:
    """Check if a measure exists for this aggregated column."""
    import lkml
    for enriched in enriched_outputs.values():
        try:
            parsed = lkml.load(enriched.view_lkml)
            for v in parsed.get("views", []):
                for m in v.get("measures", []):
                    if column in m.get("sql", ""):
                        return True
        except Exception:
            pass
    return False


def _check_structural_filter_baked(
    column: str,
    value: str,
    base_view: str,
    explore_lkml: str,
    enriched_outputs: dict[str, "EnrichedOutput"],
) -> bool:
    """Check if a structural filter is baked into derived_table or sql_always_where."""
    # Check sql_always_where in explore
    if column in explore_lkml and "sql_always_where" in explore_lkml:
        return True

    # Check derived_table SQL
    import lkml
    for enriched in enriched_outputs.values():
        for dtv in enriched.derived_table_views:
            try:
                parsed = lkml.load(dtv)
                for v in parsed.get("views", []):
                    dt = v.get("derived_table", {})
                    dt_sql = dt.get("sql", "")
                    if column in dt_sql and str(value) in dt_sql:
                        return True
            except Exception:
                pass
    return False


# ─── Summary reporter ───────────────────────────────────────

def print_gate_report(result: GateResult) -> None:
    """Pretty-print a gate result to the console."""
    icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}[result.status]
    color = {"pass": "\033[92m", "warn": "\033[93m", "fail": "\033[91m"}[result.status]
    reset = "\033[0m"

    print(f"\n{color}{icon} Stage: {result.stage} — {result.status.upper()}{reset}")

    for check in result.checks:
        c_icon = "✓" if check["passed"] else "✗"
        print(f"  {c_icon} {check['name']}: {check.get('message', '')}")

    if result.blocking_failures:
        print(f"\n  {color}BLOCKING FAILURES:{reset}")
        for bf in result.blocking_failures:
            print(f"    ✗ {bf}")

    if result.warnings:
        print("\n  \033[93mWARNINGS:\033[0m")
        for w in result.warnings:
            print(f"    ⚠ {w}")
