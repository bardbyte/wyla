#!/usr/bin/env python3
"""End-to-end enrichment probe for one table.

Walks the same path the production pipeline does:

  1. Stage 1 (`prepare_enrichment_context`): parse the gold queries that
     reference the table and assemble its :class:`TableContext` from
     fingerprint + MDM cache + baseline LookML.
  2. Build a minimal valid :class:`EnrichmentPlan` (or load one from
     ``review_queue/<table>.plan.md`` if ``--plan-from`` is passed).
  3. Render the prompt and either:
        - call Gemini 3.1 Pro via Vertex (default), or
        - return a fixture EnrichedOutput from
          ``tests/fixtures/llm_responses/enrich_<table>.json``
          when ``--dry-run`` is set.
  4. Print the interpolated prompt (truncated), the raw EnrichedOutput,
     the :func:`check_enrichment` gate result, and the first 100 lines of
     ``view_lkml`` so the user can eyeball quality.

Usage (offline / no LLM call — for prompt debugging):

    python scripts/probe_enrich.py --table cornerstone_metrics --dry-run

Usage (real Gemini call on Saheb's work laptop, on VPN):

    source agent_test/setup_vertex_env.sh ~/Downloads/key.json
    python scripts/probe_enrich.py --table cornerstone_metrics

Usage (persist enriched output for downstream stages):

    python scripts/probe_enrich.py --table cornerstone_metrics \\
        --save data/enriched/

Refuses any service-account JSON path inside this repo (defensive — keys
must live outside the worktree so they cannot be committed).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

# --- Corporate-network TLS handling (same pattern as check_bq_access.py) - #
try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
    _TRUSTSTORE_LOADED = True
except ImportError:
    _TRUSTSTORE_LOADED = False
# ------------------------------------------------------------------------- #

logger = logging.getLogger("probe_enrich")

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent  # lumi_final/
_FIXTURE_DIR = _REPO_ROOT / "tests" / "fixtures" / "llm_responses"
_DEFAULT_MDM_CACHE = _REPO_ROOT / "data" / "mdm_cache"
_DEFAULT_BASELINE = _REPO_ROOT / "data" / "looker_master"
_DEFAULT_GOLD = _REPO_ROOT / "data" / "gold_queries"
_DEFAULT_REVIEW = _REPO_ROOT / "review_queue"


# ─── Helpers ────────────────────────────────────────────────────


def _refuse_repo_local_keys() -> None:
    """SA JSONs MUST live outside the worktree so they cannot be committed.

    Mirrors the safety check in ``scripts/check_bq_access.py``: if the active
    GOOGLE_APPLICATION_CREDENTIALS / LUMI_BQ_KEY_FILE points anywhere inside
    the repo, refuse to run.
    """
    suspect_keys = ("GOOGLE_APPLICATION_CREDENTIALS", "LUMI_BQ_KEY_FILE")
    for var in suspect_keys:
        val = os.environ.get(var)
        if not val:
            continue
        try:
            resolved = Path(val).expanduser().resolve()
        except OSError:
            continue
        try:
            resolved.relative_to(_REPO_ROOT.parent)
        except ValueError:
            continue
        # Inside the repo (or a parent of it) — refuse.
        if str(_REPO_ROOT) in str(resolved) or str(resolved).startswith(
            str(_REPO_ROOT.parent)
        ):
            # Allow keys that are clearly outside (e.g. ~/Downloads/) — the
            # check above is conservative, so confirm with a stricter test.
            if str(_REPO_ROOT) in str(resolved):
                raise SystemExit(
                    f"REFUSING TO RUN: {var}={val} resolves inside the "
                    f"repo ({_REPO_ROOT}). Move the SA JSON outside the "
                    "worktree (e.g. ~/Downloads/key.json) so it cannot be "
                    "committed."
                )


def _disable_ssl_verification() -> None:
    """Mirror of ``check_bq_access.py:_disable_ssl_verification`` for the
    Vertex AI / google-genai HTTP path.

    Patches stdlib ssl, requests, httpx, and google-auth's
    ``AuthorizedSession`` to skip cert verification. Only safe on networks
    you already trust — last resort behind a TLS-intercepting proxy when
    truststore can't see the corporate root CA in macOS Keychain.
    """
    import ssl
    import warnings

    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
    os.environ["PYTHONHTTPSVERIFY"] = "0"

    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass

    try:
        import google.auth.transport.requests as gat

        _orig_init = gat.AuthorizedSession.__init__

        def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
            _orig_init(self, *args, **kwargs)
            self.verify = False

        gat.AuthorizedSession.__init__ = _patched_init  # type: ignore[method-assign]
    except ImportError:
        pass

    try:
        import requests

        _orig_req_init = requests.Session.__init__

        def _patched_req_init(self: Any, *args: Any, **kwargs: Any) -> None:
            _orig_req_init(self, *args, **kwargs)
            self.verify = False

        requests.Session.__init__ = _patched_req_init  # type: ignore[method-assign]
    except ImportError:
        pass

    try:
        import httpx

        _orig_client = httpx.Client
        _orig_async = httpx.AsyncClient

        def _client_no_verify(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("verify", False)
            return _orig_client(*args, **kwargs)

        def _async_no_verify(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("verify", False)
            return _orig_async(*args, **kwargs)

        httpx.Client = _client_no_verify  # type: ignore[misc]
        httpx.AsyncClient = _async_no_verify  # type: ignore[misc]
    except ImportError:
        pass

    warnings.warn(
        "SSL verification disabled — only safe on trusted networks.",
        stacklevel=2,
    )


def _load_gold_queries(gold_dir: Path) -> list[str]:
    """Read every ``*.sql`` file under ``gold_dir`` (sorted)."""
    if not gold_dir.exists():
        raise SystemExit(f"ERROR: gold queries dir not found: {gold_dir}")
    files = sorted(gold_dir.glob("*.sql"))
    if not files:
        raise SystemExit(f"ERROR: no *.sql files in {gold_dir}")
    return [f.read_text(encoding="utf-8") for f in files]


def _build_minimal_plan(table_name: str, ctx: Any) -> Any:
    """Synthesise a minimal-but-valid :class:`EnrichmentPlan` from context.

    Used when no plan file is provided. We pull dimensions from
    ``columns_referenced``, dimension_groups from ``date_functions``,
    and measures from ``aggregations``. The plan's reasoning explains
    that this is a synthetic minimum — real production runs will use
    plans approved through ``review_queue/``.
    """
    from lumi.schemas import EnrichmentPlan

    # Lift columns_referenced into proposed_dimensions (skip dates — they
    # become dimension_groups).
    date_cols = {d.get("column") for d in ctx.date_functions if d.get("column")}
    agg_cols = {a.get("column") for a in ctx.aggregations if a.get("column")}

    dimensions: list[dict] = []
    for col in ctx.columns_referenced:
        if col in date_cols or col in agg_cols:
            continue
        dimensions.append(
            {
                "name": col,
                "type": "string",
                "source_column": col,
                "description_summary": f"Dimension over column {col}",
            }
        )

    dim_groups: list[dict] = []
    for d in ctx.date_functions:
        col = d.get("column")
        if not col:
            continue
        # Strip _dt / _date / _ts suffixes for the dim_group name.
        base = col
        for suffix in ("_dt", "_date", "_ts", "_timestamp"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        dim_groups.append({"name": base, "source_column": col})

    measures: list[dict] = []
    for a in ctx.aggregations:
        col = a.get("column") or "?"
        func = (a.get("function") or "SUM").lower()
        type_map = {
            "sum": "sum",
            "count": "count",
            "count_distinct": "count_distinct",
            "avg": "average",
            "average": "average",
            "min": "min",
            "max": "max",
        }
        measures.append(
            {
                "name": f"{func}_{col}".replace("(", "").replace(")", ""),
                "type": type_map.get(func, "sum"),
                "source_column": col,
                "description_summary": f"{func.upper()} of {col}",
            }
        )

    derived_tables: list[dict] = []
    for cte in ctx.ctes_referencing_this:
        derived_tables.append(
            {
                "name": cte.get("alias", "derived"),
                "source_cte": cte.get("alias"),
                "structural_filters": cte.get("structural_filters", []),
                "primary_key": (ctx.columns_referenced[0] if ctx.columns_referenced else "id"),
            }
        )

    has_complexity = bool(ctx.ctes_referencing_this or ctx.case_whens)
    complexity = "complex" if has_complexity else "simple"

    return EnrichmentPlan(
        table_name=table_name,
        proposed_dimensions=dimensions,
        proposed_dimension_groups=dim_groups,
        proposed_measures=measures,
        proposed_derived_tables=derived_tables,
        proposed_filter_catalog_count=max(len(dimensions), 1),
        proposed_metric_catalog_count=max(len(measures), 1),
        proposed_nl_question_count=6,
        complexity=complexity,
        reasoning=(
            f"Synthetic minimal plan for {table_name}: {len(dimensions)} "
            f"dimension(s), {len(dim_groups)} dimension_group(s), "
            f"{len(measures)} measure(s), {len(derived_tables)} derived "
            "table(s) from CTE scope. Generated by probe_enrich.py — "
            "real runs use human-approved plans from review_queue/."
        ),
        risks=[],
    )


def _load_plan_file(path: Path, table_name: str) -> Any:
    """Load an :class:`EnrichmentPlan` from a JSON or markdown file.

    Markdown plans live in ``review_queue/<table>.plan.md`` with a
    fenced ```json``` block carrying the EnrichmentPlan payload. JSON
    files are parsed as the model directly.
    """
    from lumi.schemas import EnrichmentPlan

    if not path.exists():
        raise SystemExit(f"ERROR: plan file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return EnrichmentPlan.model_validate_json(text)

    # Markdown: extract first ```json block.
    fence_open = text.find("```json")
    if fence_open == -1:
        raise SystemExit(
            f"ERROR: {path} has no ```json block; pass a .json plan instead"
        )
    body_start = text.find("\n", fence_open) + 1
    fence_close = text.find("```", body_start)
    if fence_close == -1:
        raise SystemExit(f"ERROR: {path} has unterminated ```json block")
    payload = text[body_start:fence_close].strip()
    plan = EnrichmentPlan.model_validate_json(payload)
    if plan.table_name != table_name:
        logger.warning(
            "Plan file table_name=%s does not match --table %s — using as-is",
            plan.table_name,
            table_name,
        )
    return plan


def _load_fixture_response(table_name: str) -> Any:
    """Read the canonical fixture EnrichedOutput for ``table_name``."""
    from lumi.schemas import EnrichedOutput

    path = _FIXTURE_DIR / f"enrich_{table_name}.json"
    if not path.exists():
        raise SystemExit(
            f"ERROR: no dry-run fixture at {path}. Available: "
            f"{sorted(p.stem for p in _FIXTURE_DIR.glob('enrich_*.json'))}"
        )
    return EnrichedOutput.model_validate_json(path.read_text(encoding="utf-8"))


# ─── Driver ────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(prog="probe_enrich")
    p.add_argument("--table", required=True, help="Table name to enrich")
    p.add_argument(
        "--mdm-cache",
        default=str(_DEFAULT_MDM_CACHE),
        help=f"MDM cache dir (default: {_DEFAULT_MDM_CACHE})",
    )
    p.add_argument(
        "--baseline",
        default=str(_DEFAULT_BASELINE),
        help=f"Baseline LookML dir (default: {_DEFAULT_BASELINE})",
    )
    p.add_argument(
        "--gold-queries",
        default=str(_DEFAULT_GOLD),
        help=f"Gold queries dir (default: {_DEFAULT_GOLD})",
    )
    p.add_argument(
        "--plan-from",
        default=None,
        help=(
            "Path to an EnrichmentPlan JSON or review_queue/*.plan.md file. "
            "If omitted, a minimal synthetic plan is built from TableContext."
        ),
    )
    p.add_argument(
        "--save",
        default=None,
        help="Directory to write enriched JSON (e.g. data/enriched/)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Skip the Gemini call. Returns the fixture from "
            "tests/fixtures/llm_responses/enrich_<table>.json instead. "
            "Useful for prompt-debugging without burning Vertex quota."
        ),
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification (last-resort behind corp MITM proxy)",
    )
    p.add_argument(
        "--max-attempts",
        type=int,
        default=2,
        help="Max LLM invocations (1 = no self-repair, 2 = one repair attempt)",
    )
    p.add_argument(
        "--prompt-preview-chars",
        type=int,
        default=6000,
        help="How many chars of the rendered prompt to print (default: 6000)",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    _refuse_repo_local_keys()

    if args.insecure:
        _disable_ssl_verification()
        print("WARN: TLS verification disabled (--insecure)", file=sys.stderr)
    elif not args.dry_run:
        print(
            f"truststore: {'loaded' if _TRUSTSTORE_LOADED else 'NOT loaded'}",
            file=sys.stderr,
        )

    # Make `lumi` importable when run as a script from the repo root.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    from lumi import enrich as enrich_mod
    from lumi.config import LumiConfig
    from lumi.guardrails import check_enrichment, print_gate_report
    from lumi.mdm import CachedMDMClient
    from lumi.sql_to_context import prepare_enrichment_context

    # Stage 1: build TableContext.
    print(f"\n{'=' * 78}\n  Stage 1 — assembling TableContext for {args.table}\n{'=' * 78}")
    sqls = _load_gold_queries(Path(args.gold_queries))
    mdm = CachedMDMClient(args.mdm_cache)
    contexts = prepare_enrichment_context(sqls, mdm, args.baseline)
    if args.table not in contexts:
        print(
            f"ERROR: table {args.table!r} not discovered from gold queries. "
            f"Discovered: {sorted(contexts.keys())}",
            file=sys.stderr,
        )
        return 1
    ctx = contexts[args.table]
    print(
        f"  columns_referenced={len(ctx.columns_referenced)}  "
        f"aggregations={len(ctx.aggregations)}  "
        f"case_whens={len(ctx.case_whens)}  "
        f"ctes={len(ctx.ctes_referencing_this)}  "
        f"joins={len(ctx.joins_involving_this)}  "
        f"date_functions={len(ctx.date_functions)}  "
        f"mdm_columns={len(ctx.mdm_columns)}  "
        f"mdm_coverage={ctx.mdm_coverage_pct:.0%}"
    )
    if mdm.cache_misses:
        print(f"  MDM cache misses: {mdm.cache_misses}")

    # Stage 2: build / load plan.
    print(f"\n{'=' * 78}\n  Stage 2 — assembling EnrichmentPlan\n{'=' * 78}")
    if args.plan_from:
        plan = _load_plan_file(Path(args.plan_from), args.table)
        print(f"  Loaded plan from {args.plan_from} (complexity={plan.complexity})")
    else:
        plan = _build_minimal_plan(args.table, ctx)
        print(
            f"  Synthesised minimal plan: "
            f"{len(plan.proposed_dimensions)} dim(s), "
            f"{len(plan.proposed_dimension_groups)} dim_group(s), "
            f"{len(plan.proposed_measures)} measure(s), "
            f"{len(plan.proposed_derived_tables)} derived table(s)"
        )

    # Stage 3: render prompt and (optionally) invoke LLM.
    cfg = LumiConfig()
    prompt = enrich_mod.build_enrichment_prompt(ctx, plan, config=cfg)

    print(f"\n{'=' * 78}\n  Rendered prompt ({len(prompt)} chars total)\n{'=' * 78}")
    preview = prompt if len(prompt) <= args.prompt_preview_chars else (
        prompt[: args.prompt_preview_chars]
        + f"\n\n... [truncated, {len(prompt) - args.prompt_preview_chars} more chars] ...\n"
    )
    print(preview)

    print(f"\n{'=' * 78}\n  Stage 3 — invoking enrichment\n{'=' * 78}")
    if args.dry_run:
        print("  --dry-run: returning fixture EnrichedOutput")
        # Patch the LLM seam to return the fixture.
        fixture = _load_fixture_response(args.table)
        enrich_mod._invoke_enrichment_agent = (  # type: ignore[assignment]
            lambda agent, prompt, table_name: fixture
        )

    enriched = enrich_mod.enrich_table(
        ctx, plan, config=cfg, max_attempts=args.max_attempts
    )

    # Stage 4: report.
    print(f"\n{'=' * 78}\n  EnrichedOutput summary\n{'=' * 78}")
    summary = {
        "view_lkml_chars": len(enriched.view_lkml),
        "derived_table_view_count": len(enriched.derived_table_views),
        "explore_present": enriched.explore_lkml is not None,
        "filter_catalog_entries": len(enriched.filter_catalog),
        "metric_catalog_entries": len(enriched.metric_catalog),
        "nl_question_count": len(enriched.nl_questions),
    }
    for k, v in summary.items():
        print(f"  {k}: {v}")

    raw_json = enriched.model_dump_json(indent=2)
    raw_preview = raw_json if len(raw_json) <= 3000 else (
        raw_json[:3000]
        + f"\n... [truncated, {len(raw_json) - 3000} more chars] ...\n"
    )
    print(f"\n{'-' * 78}\n  Raw EnrichedOutput (first 3KB)\n{'-' * 78}")
    print(raw_preview)

    print(f"\n{'-' * 78}\n  view_lkml — first 100 lines\n{'-' * 78}")
    for line in enriched.view_lkml.splitlines()[:100]:
        print(line)

    print(f"\n{'-' * 78}\n  Guardrail check_enrichment\n{'-' * 78}")
    gate = check_enrichment(args.table, enriched, ctx)
    print_gate_report(gate)

    if args.save:
        save_dir = Path(args.save)
        save_dir.mkdir(parents=True, exist_ok=True)
        out_path = save_dir / f"{args.table}.json"
        out_path.write_text(raw_json, encoding="utf-8")
        print(f"\n  → wrote {out_path}")

    print(f"\n{'=' * 78}")
    if gate.status == "fail":
        print(f"  RESULT: FAIL ({len(gate.blocking_failures)} blocking)")
        return 1
    if gate.status == "warn":
        print(f"  RESULT: PASS WITH WARNINGS ({len(gate.warnings)} warnings)")
        return 0
    print("  RESULT: PASS (clean)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
