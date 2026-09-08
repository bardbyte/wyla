#!/usr/bin/env python3
"""Phase 1 driver — Stages 1-4 end to end.

The CLI wrapper at ``python -m lumi plan`` raises NotImplementedError
because the SequentialAgent wiring was deferred. This script does the
same job by calling the deterministic + LLM components directly:

  Stage 1+2  → already done; reads data/session1_output.json
  Stage 3    → planner.compute_priority + rank tables
  Stage 4    → planner.compute_deterministic_diff + classify_risk
               + (optional) Gemini call for llm_understanding /
               llm_existing_assessment flavor text
  Output     → review_queue/<table>.plan.md per table

Usage:
    python scripts/run_phase1.py                  # all 29 tables, deterministic only
    python scripts/run_phase1.py --with-gemini    # also fill LLM understanding/assessment
    python scripts/run_phase1.py --table cornerstone_metrics  # single table
    python scripts/run_phase1.py --insecure       # bypass corp TLS during Gemini calls
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make `lumi` importable when run from the repo root or scripts/.
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.config import LumiConfig  # noqa: E402
from lumi.planner import (  # noqa: E402
    ReviewQueue,
    TablePlan,
    classify_risk,
    compute_deterministic_diff,
    compute_priority,
    format_plan_markdown,
)
from lumi.schemas import TableContext  # noqa: E402


def _load_session1_output(path: Path) -> dict[str, TableContext]:
    """Round-trip session1_output.json back into TableContext models."""
    if not path.exists():
        raise SystemExit(
            f"ERROR: {path} not found. Run scripts/run_session1.py first."
        )
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {name: TableContext(**ctx_dict) for name, ctx_dict in raw.items()}


def _llm_understanding(
    ctx: TableContext,
    *,
    model: str,
    insecure: bool,
) -> tuple[str, str]:
    """Call Gemini for two short paragraphs:
      - what this table IS (1-2 sentences from MDM + columns)
      - assessment of the current LookML quality vs what queries need.

    Returns (understanding, assessment). On any failure, returns ("","").
    """
    try:
        # Deferred import — only needed if --with-gemini.
        from google import genai
        from google.genai import types as gt
    except ImportError:
        return "", ""

    if insecure:
        # Same comprehensive bypass we use elsewhere.
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
        try:
            import google.auth.transport.requests as gat
            _orig_init = gat.AuthorizedSession.__init__

            def _patched(self, *a, **kw):  # type: ignore[no-untyped-def]
                _orig_init(self, *a, **kw)
                self.verify = False
            gat.AuthorizedSession.__init__ = _patched  # type: ignore[method-assign]
        except ImportError:
            pass

    cfg = LumiConfig()
    client = genai.Client(
        vertexai=True,
        project=cfg.vertex_project,
        location=cfg.vertex_location,
    )

    cols = ", ".join(c.get("name") for c in (ctx.mdm_columns or [])[:25] if c.get("name"))
    aggs = ", ".join(
        f"{a.get('function')}({a.get('column')})" for a in (ctx.aggregations or [])[:10]
    )
    prompt = (
        f"You are a LookML expert. Two SHORT paragraphs only, 60 words each, "
        f"plain prose (no markdown).\n\n"
        f"Table: {ctx.table_name}\n"
        f"MDM description: {ctx.mdm_table_description or '(none)'}\n"
        f"Sampled columns: {cols}\n"
        f"Query aggregations seen: {aggs}\n"
        f"Queries using this table: {len(ctx.queries_using_this)}\n"
        f"Existing dims/measures: "
        f"{len(ctx.baseline_dimensions)}/{len(ctx.baseline_measures)}\n"
        f"Quality signals: {ctx.baseline_quality_signals}\n\n"
        f"Reply with EXACTLY two paragraphs separated by `---`.\n"
        f"Paragraph 1 (UNDERSTANDING): what this table represents in business terms.\n"
        f"Paragraph 2 (ASSESSMENT): how good is the existing LookML view for "
        f"answering the queries seen, and what's the most impactful upgrade."
    )
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=gt.GenerateContentConfig(temperature=0.0, max_output_tokens=512),
        )
        text = (response.text or "").strip()
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠  Gemini call for {ctx.table_name} failed: {e}", file=sys.stderr)
        return "", ""

    if "---" in text:
        understanding, _, assessment = text.partition("---")
        return understanding.strip(), assessment.strip()
    return text, ""


def _build_plan(
    ctx: TableContext,
    rank: int,
    *,
    with_gemini: bool,
    model: str,
    insecure: bool,
) -> TablePlan:
    diff = compute_deterministic_diff(ctx)
    has_structural, changes = classify_risk(diff, ctx)

    if with_gemini:
        understanding, assessment = _llm_understanding(
            ctx, model=model, insecure=insecure
        )
    else:
        understanding = (
            "(deterministic-only mode — re-run with --with-gemini to fill this in)"
        )
        assessment = (
            f"Existing view has {diff['existing']['dimensions']} dims, "
            f"{diff['existing']['measures']} measures, "
            f"{diff['existing']['dim_groups']} dim_groups, "
            f"primary_key={'yes' if diff['existing']['has_pk'] else 'no'}. "
            f"MDM coverage {ctx.mdm_coverage_pct * 100:.0f}%. "
            f"{len(changes)} changes proposed."
        )

    auto_approved = not has_structural and len(changes) > 0
    return TablePlan(
        table_name=ctx.table_name,
        priority_score=compute_priority(ctx),
        priority_rank=rank,
        query_count=len(ctx.queries_using_this),
        llm_understanding=understanding or "(no LLM input)",
        llm_existing_assessment=assessment or "(no LLM input)",
        existing_dimensions=diff["existing"]["dimensions"],
        existing_measures=diff["existing"]["measures"],
        existing_dim_groups=diff["existing"]["dim_groups"],
        has_primary_key=diff["existing"]["has_pk"],
        has_sql_table_name=diff["existing"]["has_sql_table_name"],
        mdm_coverage_pct=ctx.mdm_coverage_pct,
        new_measures_needed=diff.get("new_measures") or [],
        new_derived_tables=diff.get("new_derived_tables") or [],
        new_derived_dimensions=diff.get("new_derived_dimensions") or [],
        description_upgrades_needed=diff.get("description_upgrades_needed", 0),
        dimension_group_conversions=diff.get("dim_group_conversions") or [],
        filtered_measures_needed=diff.get("filtered_measures_needed") or [],
        changes=changes,
        has_structural_changes=has_structural,
        auto_approved=auto_approved,
        human_approved=None,
    )


_REVIEW_FOOTER = (
    "\n---\n"
    "## Reviewer decision\n\n"
    "Mark ONE:\n"
    "- [ ] ✅ APPROVED\n"
    "- [ ] ❌ REJECTED\n\n"
    "Feedback (required if rejected):\n\n"
)


def main() -> int:
    p = argparse.ArgumentParser(prog="run_phase1")
    p.add_argument(
        "--session1",
        default="data/session1_output.json",
        help="Output of run_session1.py",
    )
    p.add_argument(
        "--queue",
        default="review_queue",
        help="Where to write <table>.plan.md files",
    )
    p.add_argument("--table", action="append", help="Specific table(s); repeat to add more")
    p.add_argument(
        "--with-gemini",
        action="store_true",
        help="Also call Gemini per-table for the LLM understanding/assessment paragraphs",
    )
    p.add_argument(
        "--model",
        default=None,
        help="Override Gemini model. Default: from LumiConfig",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL verification (for corporate-MITM networks)",
    )
    args = p.parse_args()

    cfg = LumiConfig()
    model = args.model or cfg.model_name

    contexts = _load_session1_output(Path(args.session1))
    if args.table:
        wanted = set(args.table)
        contexts = {n: c for n, c in contexts.items() if n in wanted}
        if not contexts:
            print(f"ERROR: none of {sorted(wanted)} found in {args.session1}",
                  file=sys.stderr)
            return 2

    # Rank by priority desc.
    ranked = sorted(
        contexts.values(),
        key=lambda c: -compute_priority(c),
    )

    queue_dir = Path(args.queue)
    queue_dir.mkdir(parents=True, exist_ok=True)

    print(f"Processing {len(ranked)} table(s)"
          f"{' with Gemini understanding+assessment' if args.with_gemini else ' (deterministic only)'}")
    print()

    plans: list[TablePlan] = []
    for rank, ctx in enumerate(ranked, start=1):
        print(f"[{rank}/{len(ranked)}] {ctx.table_name}", end=" ", flush=True)
        plan = _build_plan(
            ctx, rank,
            with_gemini=args.with_gemini,
            model=model,
            insecure=args.insecure,
        )
        plans.append(plan)
        target = queue_dir / f"{ctx.table_name}.plan.md"
        body = format_plan_markdown(plan) + _REVIEW_FOOTER
        target.write_text(body, encoding="utf-8")
        flag = "auto" if plan.auto_approved else "REVIEW" if plan.has_structural_changes else "ok"
        print(f"→ {target.name}  [{flag}]")

    # Summary review document covering the whole queue.
    queue = ReviewQueue(
        total_tables=len(plans),
        auto_approved_count=sum(1 for p in plans if p.auto_approved),
        needs_review_count=sum(1 for p in plans if p.has_structural_changes),
        plans=plans,
    )
    summary_path = queue_dir / "REVIEW.md"
    summary_path.write_text(
        "# Phase 1 review queue\n\n"
        "Open each `<table>.plan.md` to tick approval. The queue is "
        "topologically sorted by priority (highest impact first).\n\n"
        f"- Total tables: {queue.total_tables}\n"
        f"- Auto-approvable (description-only): {queue.auto_approved_count}\n"
        f"- Needs human review (structural): {queue.needs_review_count}\n",
        encoding="utf-8",
    )

    print()
    print("=" * 78)
    print(f"Phase 1 done — {queue.total_tables} plan files in {queue_dir}/")
    print(
        f"  auto-approvable: {queue.auto_approved_count}, "
        f"needs review: {queue.needs_review_count}"
    )
    print()
    print("Next:")
    print("  python scripts/probe_review_queue.py")
    print(f"  # then open each {queue_dir}/<table>.plan.md and tick ✅ or ❌")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
