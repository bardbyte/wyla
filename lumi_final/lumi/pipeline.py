"""LUMI orchestration — Phase 1 (plan) + Phase 2 (execute).

Two phases bracketed by a human-approval gate:

  PHASE 1 (cheap, automated, deterministic + sqlglot + lkml):
    1. parse SQLs                      (lumi.sql_to_context.parse_sqls)
    2. discover tables (MDM+baseline)  (lumi.sql_to_context.discover_tables)
    3. build EnrichmentPlan per table  (lumi.plan_builder.build_enrichment_plan)
    4. write review_queue/<table>.plan.md + data/plans/<table>.plan.json
       Stops here. Human reviews & ticks ✅/❌.

  PHASE 2 (expensive, automated, Gemini-driven):
    5. load approvals from review_queue/      (lumi.approval.collect_approvals)
    6. for each approved table (parallel, capped at config.max_concurrent_enrichments):
         enrich_table(ctx, plan)              (lumi.enrich.enrich_table)
         → save data/enriched/<table>.json    (resumable checkpoint)
    7. coverage_check + reconstruct_sql_check (lumi.validate)
    8. publish_to_disk                        (lumi.publish.publish_to_disk)

Resumability:
  - data/session1_output.json     written by Phase 1 step 2; loaded by Phase 2
  - data/plans/<table>.plan.json  written by Phase 1 step 3; loaded by Phase 2
  - data/enriched/<table>.json    written per-table by Phase 2 step 6
  - On Phase 2 re-run, tables with an existing data/enriched/<X>.json
    are skipped unless ``force=True``.

Error policy:
  - Auth / permission errors  → halt the pipeline, surface clearly.
  - Per-table enrich failures → log + continue; failures listed in summary.
  - Validation regressions    → run completes but Phase 2 exit code = 1.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lumi.approval import collect_approvals
from lumi.config import LumiConfig
from lumi.enrich import enrich_table
from lumi.guardrails import (
    check_approvals,
    check_parse_and_discover,
)
from lumi.mdm import CachedMDMClient
from lumi.ontology_store import (
    OntologyStore,
    record_cardinalities_from_fingerprints,
    record_curated_synonyms_from_baseline,
    record_entity_hints_from_mdm,
    record_equivalences_from_fingerprints,
)
from lumi.plan_builder import (
    build_enrichment_plan,
    format_enrichment_plan_markdown,
    load_plan_json,
    save_plan_json,
)
from lumi.publish import publish_to_disk
from lumi.schemas import (
    EnrichedOutput,
    EnrichmentPlan,
    PlanApproval,
    TableContext,
)
from lumi.sql_to_context import (
    discover_tables,
    parse_sqls,
)
from lumi.validate import coverage_check, reconstruct_sql_check

logger = logging.getLogger("lumi.pipeline")


class PipelineHaltError(RuntimeError):
    """Raised on conditions that should stop the pipeline immediately —
    auth failures, missing inputs, blocking guardrail fails before any
    Gemini tokens are spent.
    """


@dataclass
class PipelineResult:
    """End-of-run summary. JSON-serializable for status / observability."""

    phase: str  # "plan" | "execute"
    started_at: float
    finished_at: float = 0.0
    tables_total: int = 0
    tables_succeeded: int = 0
    tables_failed: int = 0
    tables_skipped_resume: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)
    coverage_pct: float | None = None
    files_written: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def elapsed_s(self) -> float:
        end = self.finished_at or time.time()
        return round(end - self.started_at, 1)


# ─── Phase 1 ─────────────────────────────────────────────────


def run_plan_phase(
    config: LumiConfig | None = None,
    *,
    only_tables: list[str] | None = None,
    with_llm: bool = False,
) -> PipelineResult:
    """Stages 1-4 — read SQLs, build TableContexts, write plan files.

    On completion, ``review_queue/<table>.plan.md`` exists for every
    discovered table (or just the ``only_tables`` subset).

    Args:
        with_llm: when True, each plan is authored by Gemini using the
            full TableContext + grounding signals + narrative. When
            False (default), plans are deterministic skeletons. Either
            way, on Vertex error individual plans gracefully degrade
            to the skeleton — never crashes the pipeline.
    """
    cfg = config or LumiConfig()
    started = time.time()
    result = PipelineResult(phase="plan", started_at=started)

    queries_dir = Path(cfg.gold_queries_dir)
    baseline_dir = Path(cfg.baseline_views_dir)
    mdm_cache_dir = Path(cfg.mdm_cache_dir)

    if not queries_dir.exists() or not list(queries_dir.glob("*.sql")):
        raise PipelineHaltError(
            f"no .sql files at {queries_dir} — run scripts/excel_to_queries.py first"
        )
    if not baseline_dir.exists():
        logger.warning(
            "baseline dir %s missing — enrichment will generate from scratch",
            baseline_dir,
        )
    if not mdm_cache_dir.exists() or not list(mdm_cache_dir.glob("*.json")):
        logger.warning(
            "MDM cache %s empty — plans will lack rich descriptions. "
            "Run scripts/probe_mdm.py --save %s to hydrate.",
            mdm_cache_dir, mdm_cache_dir,
        )

    sqls = [
        f.read_text(encoding="utf-8")
        for f in sorted(queries_dir.glob("*.sql"))
    ]
    mdm = CachedMDMClient(mdm_cache_dir)

    fps = parse_sqls(sqls)
    contexts = discover_tables(fps, mdm, str(baseline_dir))

    # Unified ontology pipeline — single store, single read path.
    # 1. Hooks emit deterministic events from every signal source.
    # 2. On cold start (no current.json), the seed_fn fires the LLM
    #    builder and converts its output into events.
    # 3. promote_candidates() materializes the canonical DomainOntology.
    # 4. save_current() writes data/ontology/current.json — the only
    #    ontology consumed by plan, critic, and review.
    store = OntologyStore()
    n_eq = record_equivalences_from_fingerprints(store, fps)
    n_card = record_cardinalities_from_fingerprints(store, fps)
    n_mdm = 0
    n_baseline = 0
    for ctx in contexts.values():
        n_mdm += record_entity_hints_from_mdm(store, ctx)
        n_baseline += record_curated_synonyms_from_baseline(store, ctx)
    logger.info(
        "ontology events recorded: %d eq + %d card + %d MDM + %d baseline",
        n_eq, n_card, n_mdm, n_baseline,
    )
    result.extra["ontology_events"] = {
        "equivalence": n_eq, "cardinality": n_card,
        "mdm": n_mdm, "baseline": n_baseline,
    }

    fp_dicts = [
        {
            "tables": fp.tables,
            "ctes": fp.ctes,
            "joins": fp.joins,
            "_parse_error": fp.parse_error,
        }
        for fp in fps
    ]
    gate = check_parse_and_discover(sqls, fp_dicts, contexts)
    result.extra["parse_discover_gate"] = gate.status
    if gate.status == "fail":
        raise PipelineHaltError(
            f"parse_and_discover gate FAIL: {gate.blocking_failures}"
        )

    session1_path = Path("data/session1_output.json")
    session1_path.parent.mkdir(parents=True, exist_ok=True)
    session1_path.write_text(
        json.dumps(
            {n: c.model_dump() for n, c in contexts.items()},
            indent=2, default=str,
        ),
        encoding="utf-8",
    )
    result.files_written.append(str(session1_path))

    if only_tables:
        wanted = set(only_tables)
        contexts = {n: c for n, c in contexts.items() if n in wanted}

    queue_dir = Path("review_queue")
    plans_dir = Path("data/plans")
    queue_dir.mkdir(parents=True, exist_ok=True)
    plans_dir.mkdir(parents=True, exist_ok=True)

    ranked = sorted(
        contexts.values(),
        key=lambda c: -len(c.queries_using_this or []),
    )
    result.tables_total = len(ranked)

    # Compute fingerprints once if LLM authoring is enabled — it'll be
    # passed to every per-table plan call to compute grounding+narrative.
    fps_for_plan = fps if with_llm else None

    # Materialize the unified ontology from the store. On cold start,
    # the seed_fn fires the LLM builder and feeds its output into
    # events. On warm restart, we just promote the existing candidates.
    ontology = None
    if with_llm:
        from lumi.ontology_builder import _emit_seed_events, build_domain_ontology

        def _seed(s: OntologyStore) -> int:
            seed_ontology = build_domain_ontology(
                contexts, fps, with_llm=True, config=cfg,
            )
            return _emit_seed_events(s, seed_ontology)

        ontology = store.refresh(
            seed_fn=_seed,
            evidence_threshold=1,
            snapshot_reason="plan_phase_start",
        )
        result.extra["ontology_entities"] = len(ontology.entities)
        result.extra["ontology_relationships"] = len(ontology.relationships)
        result.extra["ontology_authoring"] = ontology.authoring.get("mode", "?")
        logger.info(
            "domain ontology ready (%d entities, %d relationships, mode=%s)",
            len(ontology.entities), len(ontology.relationships),
            ontology.authoring.get("mode", "?"),
        )

    llm_authored = 0
    skeleton_fallback = 0
    fallback_reasons: dict[str, int] = {}

    # Parallelize the plan stage — each per-table call is one Vertex
    # invocation (with self-repair retries). Bound concurrency at
    # cfg.max_concurrent_plans so we don't smash QPS.
    if with_llm and len(ranked) > 1:
        plan_outcomes = asyncio.run(
            _plan_many(
                ranked, cfg,
                all_fingerprints=fps_for_plan,
                contexts_by_table=contexts if with_llm else None,
                with_llm=with_llm,
                ontology=ontology,
            )
        )
    else:
        # Sequential path — used for skeleton-only runs and tests that
        # need deterministic ordering. Same logic, no event loop overhead.
        plan_outcomes = {}
        for ctx in ranked:
            try:
                plan_outcomes[ctx.table_name] = build_enrichment_plan(
                    ctx,
                    all_fingerprints=fps_for_plan,
                    contexts_by_table=contexts if with_llm else None,
                    with_llm=with_llm,
                    config=cfg,
                    ontology=ontology,
                )
            except Exception as e:  # noqa: BLE001
                plan_outcomes[ctx.table_name] = e

    for rank, ctx in enumerate(ranked, start=1):
        outcome = plan_outcomes.get(ctx.table_name)
        if isinstance(outcome, Exception):
            result.tables_failed += 1
            result.failures.append({
                "table": ctx.table_name,
                "stage": "plan",
                "error": f"{type(outcome).__name__}: {outcome}",
            })
            logger.exception(
                "plan failed for %s: %s", ctx.table_name, outcome,
            )
            continue
        if outcome is None:
            result.tables_failed += 1
            result.failures.append({
                "table": ctx.table_name,
                "stage": "plan",
                "error": "no plan produced",
            })
            continue

        plan = outcome
        try:
            save_plan_json(plan, plans_dir)
            md_path = queue_dir / f"{ctx.table_name}.plan.md"
            md_path.write_text(
                format_enrichment_plan_markdown(plan, ctx, rank=rank),
                encoding="utf-8",
            )
            result.files_written.append(str(md_path))
            result.tables_succeeded += 1
            mode = (plan.authoring or {}).get("mode", "skeleton")
            if mode == "llm":
                llm_authored += 1
            else:
                skeleton_fallback += 1
                reason = (plan.authoring or {}).get("reason") or "(unspecified)"
                bucket = reason[:80]
                fallback_reasons[bucket] = fallback_reasons.get(bucket, 0) + 1
            logger.info(
                "plan written for %s (rank #%d, mode=%s)",
                ctx.table_name, rank, mode,
            )
        except Exception as e:  # noqa: BLE001
            result.tables_failed += 1
            result.failures.append({
                "table": ctx.table_name,
                "stage": "plan_write",
                "error": f"{type(e).__name__}: {e}",
            })
            logger.exception("plan write failed for %s", ctx.table_name)

    # Surface authoring stats in the run summary.
    if with_llm or skeleton_fallback:
        result.extra["plans_llm_authored"] = llm_authored
        result.extra["plans_skeleton_fallback"] = skeleton_fallback
        if fallback_reasons:
            # Top-3 fallback reasons so the user sees what to fix.
            top = sorted(
                fallback_reasons.items(), key=lambda kv: -kv[1],
            )[:3]
            result.extra["fallback_reasons_top3"] = [
                f"{count}× {reason}" for reason, count in top
            ]

    summary_path = queue_dir / "REVIEW.md"
    summary_path.write_text(
        _render_review_summary(ranked, result),
        encoding="utf-8",
    )
    result.files_written.append(str(summary_path))

    # End-of-phase ontology refresh — critic findings during the plan
    # stage emitted entity_refinement events; promote them now and
    # snapshot so the next phase reads improved synonyms/grain hints.
    final_ontology = store.refresh(
        evidence_threshold=1,
        snapshot_reason="plan_phase_complete",
    )
    result.extra["ontology_final"] = {
        "entities": len(final_ontology.entities),
        "relationships": len(final_ontology.relationships),
    }

    # T2: cluster gold queries → ExplorePlans. Persisted to data/explore_plans.json
    # so execute phase + publish can render clustered explores.
    try:
        from lumi.explore_clusters import build_explore_plans
        from lumi.joins import infer_join_cardinalities
        cardinalities = infer_join_cardinalities(fps)
        explore_plans = build_explore_plans(
            fps, contexts, cardinalities=cardinalities, min_cluster_size=1,
        )
        explore_plans_path = Path("data/explore_plans.json")
        explore_plans_path.parent.mkdir(parents=True, exist_ok=True)
        explore_plans_path.write_text(
            json.dumps(
                [ep.model_dump() for ep in explore_plans],
                indent=2, default=str,
            ),
            encoding="utf-8",
        )
        result.files_written.append(str(explore_plans_path))
        result.extra["explore_clusters"] = len(explore_plans)
        logger.info(
            "T2 explore clusters: %d explores designed across %d gold queries",
            len(explore_plans), len(fps),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("explore clustering failed: %s", e)

    result.finished_at = time.time()
    return result


# ─── Phase 2 ─────────────────────────────────────────────────


def run_execute_phase(
    config: LumiConfig | None = None,
    *,
    only_tables: list[str] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> PipelineResult:
    """Stages 5-8 — collect approvals, enrich (parallel), validate, publish."""
    cfg = config or LumiConfig()
    started = time.time()
    result = PipelineResult(phase="execute", started_at=started)

    session1_path = Path("data/session1_output.json")
    if not session1_path.exists():
        raise PipelineHaltError(
            "data/session1_output.json missing — run Phase 1 first "
            "(`python -m lumi plan`)"
        )

    contexts = _load_session1_output(session1_path)
    plans_dir = Path("data/plans")
    queue_dir = Path("review_queue")

    approvals = collect_approvals(str(queue_dir))
    plans_for_gate: list[EnrichmentPlan] = []
    for a in approvals:
        plan = load_plan_json(plans_dir, a.table_name)
        if plan is not None:
            plans_for_gate.append(plan)
    gate = check_approvals(approvals, plans_for_gate)
    result.extra["approval_gate"] = gate.status
    if gate.status == "fail":
        raise PipelineHaltError(
            f"approval gate FAIL — open review_queue/<table>.plan.md and "
            f"tick a checkbox: {gate.blocking_failures}"
        )

    approved = [a for a in approvals if a.approved]
    if only_tables:
        wanted = set(only_tables)
        approved = [a for a in approved if a.table_name in wanted]

    # Hook 4: vocabulary lock + plan-content mining for every approved
    # plan. Boosts the entity, locks renames as curated synonyms, and
    # mines explicit primary_key dims as curated PKs.
    if approved:
        store = OntologyStore()
        from lumi.ontology_store import record_approval_lock
        n_locks = 0
        for a in approved:
            ctx = contexts.get(a.table_name)
            plan = load_plan_json(plans_dir, a.table_name)
            n_locks += record_approval_lock(store, a, ctx, plan=plan)
        if n_locks:
            logger.info(
                "ontology approval events recorded: %d", n_locks,
            )
            result.extra["vocabulary_locks"] = n_locks
    if not approved:
        result.finished_at = time.time()
        result.extra["note"] = (
            "no approved plans — open review_queue/*.plan.md, tick "
            "[x] APPROVED, then re-run `python -m lumi execute`"
        )
        return result

    result.tables_total = len(approved)

    enriched_dir = Path("data/enriched")
    enriched_dir.mkdir(parents=True, exist_ok=True)
    enriched: dict[str, EnrichedOutput] = {}

    work: list[tuple[TableContext, EnrichmentPlan]] = []
    rejected_skipped = 0
    for a in approved:
        ctx = contexts.get(a.table_name)
        if ctx is None:
            result.failures.append({
                "table": a.table_name,
                "stage": "load_context",
                "error": "table missing from session1_output.json",
            })
            result.tables_failed += 1
            continue
        plan = load_plan_json(plans_dir, a.table_name)
        if plan is None:
            result.failures.append({
                "table": a.table_name,
                "stage": "load_plan",
                "error": f"no plan json at {plans_dir}/{a.table_name}.plan.json",
            })
            result.tables_failed += 1
            continue
        # Plans the critic rejected (or flip-flop-escalated) must NOT
        # spend enrichment tokens — even if the human ticked APPROVED,
        # that approval was on a known-bad plan. We log + skip.
        reason = (plan.authoring or {}).get("reason") or ""
        if reason in {"rejected_by_critic", "flip_flop_detected"}:
            logger.warning(
                "skipping enrichment for %s — plan flagged %s by critic",
                a.table_name, reason,
            )
            rejected_skipped += 1
            result.failures.append({
                "table": a.table_name,
                "stage": "critic_gate",
                "error": f"plan {reason} — fix the plan before enriching",
            })
            continue
        cached = enriched_dir / f"{a.table_name}.json"
        if cached.exists() and not force:
            try:
                enriched[a.table_name] = EnrichedOutput(
                    **json.loads(cached.read_text(encoding="utf-8"))
                )
                result.tables_skipped_resume += 1
                logger.info("resume: skipping %s (already enriched)", a.table_name)
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "cached enrichment for %s unparseable, redoing: %s",
                    a.table_name, e,
                )
        work.append((ctx, plan))

    if rejected_skipped:
        result.extra["rejected_by_critic"] = rejected_skipped

    # Grounding signals need the full corpus of fingerprints + all
    # discovered contexts, NOT just the current table's. Compute once
    # outside the per-table loop so we don't re-parse 138 SQLs 29 times.
    all_fps = parse_sqls(_load_gold_sqls(cfg))

    if work:
        enriched_results = asyncio.run(
            _enrich_many(
                work, cfg, dry_run=dry_run,
                all_fingerprints=all_fps,
                contexts_by_table=contexts,
            )
        )
        for table_name, result_or_err in enriched_results.items():
            if isinstance(result_or_err, EnrichedOutput):
                enriched[table_name] = result_or_err
                cached = enriched_dir / f"{table_name}.json"
                cached.write_text(
                    json.dumps(result_or_err.model_dump(), indent=2, default=str),
                    encoding="utf-8",
                )
                result.tables_succeeded += 1
                result.files_written.append(str(cached))
            else:
                result.tables_failed += 1
                result.failures.append({
                    "table": table_name,
                    "stage": "enrich",
                    "error": str(result_or_err),
                })

    if not enriched:
        result.finished_at = time.time()
        return result

    fps = parse_sqls(_load_gold_sqls(cfg))
    coverage = coverage_check(fps, enriched)
    sql_gate = reconstruct_sql_check(
        gold_sqls=_load_gold_sqls(cfg),
        fingerprints=[
            {
                "tables": fp.tables, "ctes": fp.ctes, "joins": fp.joins,
                "aggregations": fp.aggregations, "filters": fp.filters,
                "query_id": f"Q{i + 1:02d}",
            }
            for i, fp in enumerate(fps)
        ],
        enriched=enriched,
    )
    result.coverage_pct = coverage.coverage_pct
    result.extra["coverage_status"] = (
        "pass" if coverage.coverage_pct >= cfg.coverage_target_pct else "warn"
    )
    result.extra["sql_reconstruction_status"] = sql_gate.status

    # Load the unified ontology so the Radix-shaped filter catalog can
    # attach entity-level synonyms.
    radix_ontology = OntologyStore().current()
    # Load T2 explore plans if plan phase wrote them.
    explore_plans_data: list = []
    explore_plans_path = Path("data/explore_plans.json")
    if explore_plans_path.exists():
        try:
            from lumi.schemas import ExplorePlan
            raw = json.loads(explore_plans_path.read_text(encoding="utf-8"))
            explore_plans_data = [ExplorePlan(**ep) for ep in raw]
            logger.info(
                "loaded %d clustered explore plans from %s",
                len(explore_plans_data), explore_plans_path,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("could not load explore plans: %s", e)
    publish_result = publish_to_disk(
        enriched,
        baseline_dir=Path(cfg.baseline_views_dir),
        output_dir=Path(cfg.output_dir),
        coverage=coverage,
        contexts=contexts,
        fingerprints=all_fps,
        ontology=radix_ontology,
        explore_plans=explore_plans_data,
    )
    if publish_result.get("status") == "ok":
        result.files_written.extend(publish_result.get("files_written", []))
    else:
        result.failures.append({
            "table": "<all>",
            "stage": "publish",
            "error": publish_result.get("error") or "publish failed",
        })

    # End-of-execute ontology refresh — vocabulary_lock + plan-mining
    # events accumulated during this phase. Promote + snapshot.
    store = OntologyStore()
    final_ontology = store.refresh(
        evidence_threshold=1,
        snapshot_reason="execute_phase_complete",
    )
    result.extra["ontology_final"] = {
        "entities": len(final_ontology.entities),
        "relationships": len(final_ontology.relationships),
    }

    result.finished_at = time.time()
    return result


# ─── Concurrent planning ─────────────────────────────────────


async def _plan_many(
    ranked: list[TableContext],
    cfg: LumiConfig,
    *,
    all_fingerprints: list | None,
    contexts_by_table: dict | None,
    with_llm: bool,
    ontology=None,  # type: ignore[no-untyped-def]
) -> dict[str, EnrichmentPlan | Exception]:
    """Run build_enrichment_plan concurrently with a Semaphore cap.

    Each per-table call may itself fire 1-3 Gemini calls (planner + critic
    + repair retries), so we bound at cfg.max_concurrent_plans (default 5)
    to keep total QPS sane.
    """
    sem = asyncio.Semaphore(cfg.max_concurrent_plans)
    out: dict[str, EnrichmentPlan | Exception] = {}

    async def _one(ctx: TableContext) -> None:
        async with sem:
            try:
                plan = await asyncio.to_thread(
                    build_enrichment_plan, ctx,
                    all_fingerprints=all_fingerprints,
                    contexts_by_table=contexts_by_table,
                    with_llm=with_llm,
                    config=cfg,
                    ontology=ontology,
                )
                out[ctx.table_name] = plan
            except Exception as e:  # noqa: BLE001
                logger.exception("plan failed for %s", ctx.table_name)
                out[ctx.table_name] = e

    await asyncio.gather(*[_one(c) for c in ranked])
    return out


# ─── Concurrent enrichment ───────────────────────────────────


async def _enrich_many(
    work: list[tuple[TableContext, EnrichmentPlan]],
    cfg: LumiConfig,
    *,
    dry_run: bool,
    all_fingerprints: list | None = None,
    contexts_by_table: dict | None = None,
) -> dict[str, EnrichedOutput | Exception]:
    """Run enrich_table concurrently with a Semaphore cap.

    Each call is a single LlmAgent invocation under the hood (with its own
    self-repair retry loop). We just bound how many fire in parallel so we
    don't smash Vertex's per-project QPS limit. ``cfg.max_concurrent_enrichments``
    defaults to 5.

    Args:
        all_fingerprints / contexts_by_table: passed to enrich_table so it
            can compute grounding signals against the full corpus.
    """
    sem = asyncio.Semaphore(cfg.max_concurrent_enrichments)
    out: dict[str, EnrichedOutput | Exception] = {}

    async def _one(ctx: TableContext, plan: EnrichmentPlan) -> None:
        async with sem:
            try:
                if dry_run:
                    eo = _load_dry_run_fixture(ctx.table_name)
                else:
                    eo = await asyncio.to_thread(
                        enrich_table, ctx, plan, None, cfg, 2,
                        all_fingerprints=all_fingerprints,
                        contexts_by_table=contexts_by_table,
                    )
                out[ctx.table_name] = eo
                logger.info("enriched %s", ctx.table_name)
            except Exception as e:  # noqa: BLE001
                logger.exception("enrich failed for %s", ctx.table_name)
                out[ctx.table_name] = e

    await asyncio.gather(*[_one(c, p) for c, p in work])
    return out


def _load_dry_run_fixture(table_name: str) -> EnrichedOutput:
    """Load a fixture EnrichedOutput for ``--dry-run`` mode.

    Falls back to a minimal-but-valid synthetic EnrichedOutput so dry-run
    works for every table even without a per-table fixture.
    """
    fixture = (
        Path("tests/fixtures/llm_responses") / f"enrich_{table_name}.json"
    )
    if fixture.exists():
        return EnrichedOutput(
            **json.loads(fixture.read_text(encoding="utf-8"))
        )
    return EnrichedOutput(
        view_lkml=(
            f"view: {table_name} {{\n"
            f"  sql_table_name: `dryrun.{table_name}` ;;\n"
            "  dimension: stub_pk { primary_key: yes ;; "
            "sql: ${TABLE}.id ;; hidden: yes }\n"
            "}\n"
        ),
        derived_table_views=[],
        explore_lkml=f"explore: {table_name} {{}}\n",
        filter_catalog=[],
        metric_catalog=[],
        nl_questions=[],
    )


# ─── Helpers ─────────────────────────────────────────────────


def _load_session1_output(path: Path) -> dict[str, TableContext]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {n: TableContext(**d) for n, d in raw.items()}


def _load_gold_sqls(cfg: LumiConfig) -> list[str]:
    queries_dir = Path(cfg.gold_queries_dir)
    return [
        f.read_text(encoding="utf-8")
        for f in sorted(queries_dir.glob("*.sql"))
    ]


def _render_review_summary(
    ranked: list[TableContext],
    result: PipelineResult,
) -> str:
    lines = [
        "# Phase 1 review queue",
        "",
        f"- {result.tables_succeeded}/{result.tables_total} plan files written",
        f"- {result.tables_failed} plan failures",
        "",
        "Open each `<table>.plan.md`, tick `[x] ✅ APPROVED` or "
        "`[x] ❌ REJECTED` (with feedback), then run "
        "`python -m lumi execute`.",
        "",
        "## Tables (priority order — highest impact first)",
        "",
    ]
    for rank, ctx in enumerate(ranked, start=1):
        n_q = len(ctx.queries_using_this or [])
        n_cte = len(ctx.ctes_referencing_this or [])
        n_temp = len(ctx.temp_tables_referencing_this or [])
        sig = ctx.baseline_quality_signals or {}
        pk = "✓" if sig.get("has_primary_key") else "—"
        lines.append(
            f"{rank}. `{ctx.table_name}` — {n_q} quer(y/ies), "
            f"{n_cte} CTE / {n_temp} temp_table, "
            f"PK={pk}, MDM={ctx.mdm_coverage_pct * 100:.0f}%"
        )
    return "\n".join(lines) + "\n"


# ─── Legacy class shim — keeps __main__.py and tests stable ──


class LumiPipeline:
    """Backward-compatible class wrapper over the functional API.

    Old callers used ``LumiPipeline().run_plan_phase(sqls)`` etc. The
    module-level ``run_plan_phase`` / ``run_execute_phase`` functions are
    the real implementation; this class just delegates and stashes results.
    """

    STAGES = (
        "Parse", "Discover", "Stage", "Plan",
        "Enrich", "Validate", "Publish",
    )

    def __init__(self, config: LumiConfig | None = None) -> None:
        self.config = config or LumiConfig()
        self.last_plan_result: PipelineResult | None = None
        self.last_execute_result: PipelineResult | None = None

    def run_plan_phase(
        self, sql_inputs: list[str] | None = None,
        *, only_tables: list[str] | None = None,
    ) -> PipelineResult:
        # sql_inputs is accepted for API compatibility but ignored — the
        # functional version reads from disk per LumiConfig.gold_queries_dir.
        _ = sql_inputs
        result = run_plan_phase(self.config, only_tables=only_tables)
        self.last_plan_result = result
        return result

    def collect_approvals(self) -> list[PlanApproval]:
        return collect_approvals("review_queue")

    def run_execute_phase(
        self,
        *,
        only_tables: list[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> PipelineResult:
        result = run_execute_phase(
            self.config,
            only_tables=only_tables,
            force=force,
            dry_run=dry_run,
        )
        self.last_execute_result = result
        return result

    def print_status(self) -> None:
        for stage in self.STAGES:
            marker, detail = self._stage_state(stage)
            print(f"{marker} {stage:<10} {detail}")

    def _stage_state(self, stage: str) -> tuple[str, str]:
        if stage in {"Parse", "Discover"}:
            p = Path("data/session1_output.json")
            if p.exists():
                return "✓", f"({p})"
            return "○", "(not started)"
        if stage in {"Stage", "Plan"}:
            queue = Path("review_queue")
            if queue.exists():
                n = len(list(queue.glob("*.plan.md")))
                return ("✓" if n else "○"), f"({n} plan files)"
            return "○", "(not started)"
        if stage == "Enrich":
            ed = Path("data/enriched")
            if ed.exists():
                n = len(list(ed.glob("*.json")))
                return ("✓" if n else "○"), f"({n} tables enriched)"
            return "○", "(not started)"
        if stage == "Validate":
            cr = Path(self.config.output_dir) / "coverage_report.json"
            if cr.exists():
                try:
                    pct = json.loads(cr.read_text())["coverage_pct"]
                    return "✓", f"(coverage {pct:.1f}%)"
                except Exception:  # noqa: BLE001
                    return "✓", "(coverage report present)"
            return "○", "(not started)"
        if stage == "Publish":
            views = Path(self.config.output_dir) / "views"
            if views.exists():
                n = len(list(views.glob("*.view.lkml")))
                return ("✓" if n else "○"), f"({n} views in output/)"
            return "○", "(not started)"
        return "○", ""
