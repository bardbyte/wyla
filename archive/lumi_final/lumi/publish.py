"""Stage 7: Publish — additive merge + on-disk emission of enriched LookML.

All deterministic — no LLM calls. The merge is strictly ADDITIVE: anything
that exists in the baseline view is preserved verbatim (sql, type, joins,
etc.), and the enriched output may only ADD new dimensions / measures /
dimension_groups or augment metadata fields (description, label, tags).

Public API:

    additive_merge_view(baseline_lkml, enriched_lkml) -> str
        Merge one view body. Used per table.

    build_metric_catalog(enriched_outputs) -> list[dict]
        Union of every measure across every enriched view, with the fields
        Radix / NL2SQL needs (field_key, type, value_format, description).

    build_filter_catalog(enriched_outputs) -> list[dict]
        Union of every entry in EnrichedOutput.filter_catalog.

    build_golden_questions(enriched_outputs) -> list[dict]
        Union of every NLQuestionVariant across all outputs.

    publish_to_disk(enriched_outputs, baseline_dir, output_dir, *, coverage=...)
        Writes:
            output_dir/views/<table>.view.lkml          (merged)
            output_dir/models/lumi_enriched.model.lkml  (one model per run)
            output_dir/metric_catalog.json
            output_dir/filter_catalog.json
            output_dir/golden_questions.json
            output_dir/coverage_report.json (only if `coverage` provided)
        Returns a dict with status + the list of files written.

Sessions 4-7 will wire ``publish_to_disk`` into the ADK pipeline and add the
GitHub PR opener; this module exists so the deterministic pieces are testable
and the dry-run probe has something concrete to call.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import lkml

from lumi.schemas import (
    CoverageReport,
    EnrichedOutput,
    ExploreDescription,
    ViewDescription,
)

logger = logging.getLogger("lumi.publish")


# ─── Additive merge ─────────────────────────────────────────────────


_PRESERVE_FIELDS = ("sql", "type", "primary_key", "hidden", "datatype", "convert_tz")
_ADDITIVE_FIELDS = ("label", "description", "group_label", "tags", "value_format",
                    "value_format_name", "filters", "drill_fields")
# Baseline values shorter than this are treated as auto-generated stubs that
# can be replaced by enrichment without violating "additive only". Above the
# threshold we assume human curation and preserve.
_DESCRIPTION_QUALITY_THRESHOLD = 30
# Track every override we did due to the quality threshold so we can emit
# proposed_overwrites.md at publish time. Reset per merge call.
_OVERWRITE_LEDGER_KEY = "_overwrite_ledger"


def _index_by_name(items: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    if not items:
        return {}
    return {it.get("name", ""): it for it in items if it.get("name")}


def _merge_field(
    baseline: dict[str, Any],
    enriched: dict[str, Any],
    ledger: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Merge one dimension/measure/dim_group dict.

    Policy (per the design discussion in the docs):
      - Tags: unioned (preserving order, baseline first). Always cumulative.
      - Description: keep baseline if it's ≥ 30 chars (assumed human-curated).
        Replace with enriched if baseline is missing OR < 30 chars
        (assumed Looker-auto-generated stub like "Customer ID"). Every
        such replacement is recorded in `ledger` for the proposed_overwrites
        side file.
      - Label / group_label / value_format / value_format_name / filters /
        drill_fields: keep baseline if present, else add enriched.
      - sql / type / primary_key / hidden / datatype / convert_tz: NEVER
        overwrite baseline. Schema decisions are sacred.
    """
    merged = dict(baseline)
    field_name = baseline.get("name") or enriched.get("name") or "<unnamed>"

    for field in _ADDITIVE_FIELDS:
        if field == "tags":
            base_tags = baseline.get("tags") or []
            enr_tags = enriched.get("tags") or []
            seen: set[str] = set()
            unioned: list[str] = []
            for t in [*base_tags, *enr_tags]:
                if t not in seen:
                    seen.add(t)
                    unioned.append(t)
            if unioned:
                merged["tags"] = unioned
            continue

        if field == "description":
            base_desc = (baseline.get("description") or "").strip()
            enr_desc = (enriched.get("description") or "").strip()
            if not enr_desc:
                # Nothing to add; preserve baseline (which may also be empty).
                continue
            if not base_desc:
                # Pure additive — baseline lacks description.
                merged["description"] = enr_desc
                continue
            if len(base_desc) < _DESCRIPTION_QUALITY_THRESHOLD:
                # Auto-generated stub — replace, but log so a human can
                # double-check next iteration via proposed_overwrites.md.
                if base_desc != enr_desc:
                    merged["description"] = enr_desc
                    if ledger is not None:
                        ledger.append({
                            "field_kind": _kind_from_dict(baseline),
                            "field_name": field_name,
                            "attribute": "description",
                            "baseline_value": base_desc,
                            "proposed_value": enr_desc,
                            "reason": (
                                f"baseline description was {len(base_desc)} chars "
                                f"(< {_DESCRIPTION_QUALITY_THRESHOLD} threshold) "
                                "— treated as auto-generated stub"
                            ),
                        })
                continue
            # Baseline description ≥ threshold → preserve. If LLM strongly
            # disagrees, it can put the alternative on EnrichedOutput.
            # proposed_overwrites and we'll surface that separately at
            # publish time (handled in publish_to_disk, not here).
            continue

        # Default additive: only fill if baseline lacks the field.
        if field in baseline and baseline[field]:
            continue
        if field in enriched and enriched[field]:
            merged[field] = enriched[field]

    # NEVER overwrite preserve fields from baseline.
    for field in _PRESERVE_FIELDS:
        if field in baseline:
            merged[field] = baseline[field]
    return merged


def _kind_from_dict(field_dict: dict[str, Any]) -> str:
    """Infer dimension / dimension_group / measure from the dict shape."""
    if "type" in field_dict and field_dict.get("type") == "time":
        return "dimension_group"
    # Heuristic: measures usually have a sum/count/avg type.
    measure_types = {"count", "sum", "average", "min", "max",
                     "count_distinct", "median", "number"}
    if (field_dict.get("type") or "").lower() in measure_types:
        return "measure"
    return "dimension"


def additive_merge_view(
    baseline_lkml: str,
    enriched_lkml: str,
    ledger: list[dict[str, Any]] | None = None,
) -> str:
    """Merge enriched view INTO baseline. Returns serialised LookML string.

    If baseline is empty/unparseable, the enriched view is returned as-is
    (this happens for brand-new tables that had no Looker-generated baseline).

    Args:
        ledger: optional list — appended to with one entry per "we replaced
            a stub baseline value with enriched content" event. Used by
            publish_to_disk to emit output/proposed_overwrites.md.
    """
    if not (baseline_lkml or "").strip():
        return enriched_lkml

    try:
        base_tree = lkml.load(baseline_lkml)
    except Exception as e:  # noqa: BLE001
        logger.warning("Baseline unparseable, falling back to enriched: %s", e)
        return enriched_lkml
    try:
        enr_tree = lkml.load(enriched_lkml)
    except Exception as e:  # noqa: BLE001
        logger.warning("Enriched unparseable, falling back to baseline: %s", e)
        return baseline_lkml

    base_views = base_tree.get("views") or []
    enr_views = enr_tree.get("views") or []
    enr_by_name = {v.get("name", ""): v for v in enr_views}

    merged_views: list[dict[str, Any]] = []
    seen_view_names: set[str] = set()
    for bv in base_views:
        name = bv.get("name", "")
        seen_view_names.add(name)
        ev = enr_by_name.get(name)
        if ev is None:
            merged_views.append(bv)
            continue
        merged_views.append(_merge_one_view(bv, ev, ledger=ledger))
    # Add any enriched-only views (e.g. derived_table views from CTEs).
    for ev in enr_views:
        if ev.get("name", "") not in seen_view_names:
            merged_views.append(ev)

    out_tree: dict[str, Any] = {"views": merged_views}
    return lkml.dump(out_tree)


def _merge_one_view(
    baseline: dict[str, Any],
    enriched: dict[str, Any],
    ledger: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Merge a single view dict — preserves baseline structure, adds enriched fields."""
    merged = dict(baseline)
    base_dims = _index_by_name(baseline.get("dimensions"))
    base_dgs = _index_by_name(baseline.get("dimension_groups"))
    base_msrs = _index_by_name(baseline.get("measures"))
    enr_dims = _index_by_name(enriched.get("dimensions"))
    enr_dgs = _index_by_name(enriched.get("dimension_groups"))
    enr_msrs = _index_by_name(enriched.get("measures"))

    out_dims: list[dict[str, Any]] = []
    for name, bd in base_dims.items():
        ed = enr_dims.get(name)
        out_dims.append(_merge_field(bd, ed, ledger=ledger) if ed else bd)
    for name, ed in enr_dims.items():
        if name not in base_dims:
            out_dims.append(ed)

    out_dgs: list[dict[str, Any]] = []
    for name, bd in base_dgs.items():
        ed = enr_dgs.get(name)
        out_dgs.append(_merge_field(bd, ed, ledger=ledger) if ed else bd)
    for name, ed in enr_dgs.items():
        if name not in base_dgs:
            out_dgs.append(ed)

    out_msrs: list[dict[str, Any]] = []
    for name, bm in base_msrs.items():
        em = enr_msrs.get(name)
        out_msrs.append(_merge_field(bm, em, ledger=ledger) if em else bm)
    for name, em in enr_msrs.items():
        if name not in base_msrs:
            out_msrs.append(em)

    if out_dims:
        merged["dimensions"] = out_dims
    if out_dgs:
        merged["dimension_groups"] = out_dgs
    if out_msrs:
        merged["measures"] = out_msrs
    # Carry over sql_table_name / derived_table from baseline if present.
    for k in ("sql_table_name", "derived_table", "label", "description"):
        if k in baseline and baseline[k]:
            merged[k] = baseline[k]
        elif k in enriched and enriched[k]:
            merged[k] = enriched[k]
    return merged


# ─── Catalog builders ───────────────────────────────────────────────


def _measures_from_lkml(lkml_text: str) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    if not (lkml_text or "").strip():
        return out
    try:
        tree = lkml.load(lkml_text)
    except Exception:  # noqa: BLE001
        return out
    for v in tree.get("views") or []:
        vname = v.get("name", "")
        for m in v.get("measures") or []:
            out.append((vname, m))
    return out


def build_metric_catalog(
    enriched_outputs: dict[str, EnrichedOutput],
) -> list[dict[str, Any]]:
    """Union every measure across every enriched view into a flat catalog."""
    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table_name, eo in enriched_outputs.items():
        # Merge measures from the main view + every derived_table view.
        sources = [eo.view_lkml, *(eo.derived_table_views or [])]
        for src in sources:
            for vname, m in _measures_from_lkml(src):
                key = f"{vname}.{m.get('name', '')}"
                if not m.get("name") or key in seen:
                    continue
                seen.add(key)
                catalog.append(
                    {
                        "field_key": key,
                        "table": table_name,
                        "type": m.get("type"),
                        "label": m.get("label"),
                        "description": m.get("description"),
                        "value_format": m.get("value_format")
                        or m.get("value_format_name"),
                        "sql": m.get("sql"),
                    }
                )
    return catalog


def build_filter_catalog(
    enriched_outputs: dict[str, EnrichedOutput],
) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for table_name, eo in enriched_outputs.items():
        for entry in eo.filter_catalog or []:
            row = {**entry, "table": table_name}
            catalog.append(row)
    return catalog


def build_golden_questions(
    enriched_outputs: dict[str, EnrichedOutput],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for eo in enriched_outputs.values():
        for q in eo.nl_questions or []:
            out.append(q.model_dump() if hasattr(q, "model_dump") else dict(q))
    return out


# ─── Disk emitter ───────────────────────────────────────────────────


def _read_baseline(baseline_dir: Path, table_name: str) -> str:
    target = f"{table_name}.view.lkml"
    direct = baseline_dir / target
    if direct.is_file():
        return direct.read_text(encoding="utf-8")
    if baseline_dir.exists():
        for path in baseline_dir.rglob(target):
            return path.read_text(encoding="utf-8")
    return ""


def publish_to_disk(
    enriched_outputs: dict[str, EnrichedOutput],
    baseline_dir: str | Path,
    output_dir: str | Path,
    *,
    coverage: CoverageReport | None = None,
    contexts: dict | None = None,
    fingerprints: list | None = None,
    ontology: Any = None,
    explore_plans: list | None = None,
    aggregate_tables: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Materialise enriched outputs to ``output_dir``.

    Layout:
      ``output_dir/views/<table>.view.lkml``     additively merged view
      ``output_dir/models/lumi_enriched.model.lkml`` one explore include per view
      ``output_dir/metric_catalog.json``
      ``output_dir/filter_catalog.json``       Radix-shaped (T1.3)
      ``output_dir/filter_catalog_legacy.json`` per-table aggregates
      ``output_dir/golden_questions.json``
      ``output_dir/coverage_report.json`` (only if ``coverage`` given)

    Optional kwargs ``contexts`` / ``fingerprints`` / ``ontology`` enable
    the Radix-shaped catalog. When omitted, only the legacy catalog is
    emitted (back-compat).

    Returns a dict with ``status``, ``error``, and the list of files written.
    """
    out = Path(output_dir)
    baseline = Path(baseline_dir)
    views_dir = out / "views"
    models_dir = out / "models"
    views_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    explore_includes: list[str] = []
    # Aggregate ledger across every table's merge — entries here become
    # output/proposed_overwrites.md so a human can sanity-check the
    # quality-threshold decisions before the next iteration.
    overwrite_ledger: list[dict[str, Any]] = []

    for table_name, eo in enriched_outputs.items():
        baseline_lkml = _read_baseline(baseline, table_name)
        per_table_ledger: list[dict[str, Any]] = []
        merged = additive_merge_view(
            baseline_lkml, eo.view_lkml, ledger=per_table_ledger
        )
        # Tag every ledger entry with the table it came from.
        for entry in per_table_ledger:
            entry["table"] = table_name
        overwrite_ledger.extend(per_table_ledger)
        # LLM-flagged "this baseline value is wrong, not just terse" entries
        # bypass the merge entirely — they live on EnrichedOutput.proposed_
        # overwrites and we just append them.
        for entry in eo.proposed_overwrites or []:
            row = {**entry, "table": table_name, "source": "llm_flagged"}
            overwrite_ledger.append(row)
        # Render disambiguating description (B): comment block + inject
        # description: parameter into the view body.
        merged = _apply_view_description(merged, eo.view_description)
        view_path = views_dir / f"{table_name}.view.lkml"
        view_path.write_text(merged, encoding="utf-8")
        written.append(str(view_path))
        explore_includes.append(table_name)
        # Derived-table views: write under views/<table>__<idx>.view.lkml
        for idx, dtv in enumerate(eo.derived_table_views or [], start=1):
            dtv_path = views_dir / f"{table_name}__derived_{idx}.view.lkml"
            dtv_path.write_text(dtv, encoding="utf-8")
            written.append(str(dtv_path))

    # Single combined model file referencing every view + each EnrichedOutput's
    # explore_lkml verbatim.
    model_lines: list[str] = ["# Auto-generated by lumi.publish — do not edit by hand"]
    for name in explore_includes:
        model_lines.append(f'include: "../views/{name}.view.lkml"')
    model_lines.append("")
    for table_name, eo in enriched_outputs.items():
        if eo.explore_lkml and eo.explore_lkml.strip():
            explore_text = _apply_explore_description(
                eo.explore_lkml, eo.explore_description,
            )
            model_lines.append(explore_text.rstrip())
            model_lines.append("")

    # T2: clustered explores authored at corpus level. These are the
    # explores Radix retrieves against — designed per question pattern,
    # with corpus-validated relationships and partition always_filter.
    if explore_plans:
        model_lines.append(
            "# === T2 CLUSTERED EXPLORES — authored at corpus level ==="
        )
        # T4.2: detect dim views used by multiple explores → propose
        # view_label aliases so each explore can label the dim with its
        # role in this explore's question pattern.
        explore_plans = _annotate_with_aliasing(explore_plans)
        for ep in explore_plans:
            model_lines.append(_render_clustered_explore(
                ep, aggregate_tables=aggregate_tables,
            ))
            model_lines.append("")

    model_path = models_dir / "lumi_enriched.model.lkml"
    model_path.write_text("\n".join(model_lines), encoding="utf-8")
    written.append(str(model_path))

    metric_catalog = build_metric_catalog(enriched_outputs)
    legacy_filter_catalog = build_filter_catalog(enriched_outputs)
    golden = build_golden_questions(enriched_outputs)

    metric_path = out / "metric_catalog.json"
    metric_path.write_text(json.dumps(metric_catalog, indent=2), encoding="utf-8")
    written.append(str(metric_path))

    # Radix-shaped filter catalog (T1.3) — primary artifact when contexts
    # are available. Legacy aggregate kept as filter_catalog_legacy.json.
    if contexts is not None and fingerprints is not None:
        from lumi.filter_catalog import (
            build_filter_catalog as build_radix_catalog,
        )
        radix_catalog = build_radix_catalog(
            contexts, fingerprints, ontology=ontology,
        )
        # T3.3: optionally enrich values via BigQuery DISTINCT probe.
        # Env-gated (LUMI_BQ_ENABLE=1) so it never fires in tests/CI.
        try:
            from lumi.bq_probe import is_enabled as _bq_enabled
            from lumi.bq_probe import probe_distinct_values
            if _bq_enabled():
                radix_catalog = probe_distinct_values(
                    radix_catalog, contexts=contexts,
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("BQ probe failed: %s", e)
        filter_path = out / "filter_catalog.json"
        filter_path.write_text(
            json.dumps(radix_catalog, indent=2, default=str),
            encoding="utf-8",
        )
        written.append(str(filter_path))
        legacy_path = out / "filter_catalog_legacy.json"
        legacy_path.write_text(
            json.dumps(legacy_filter_catalog, indent=2),
            encoding="utf-8",
        )
        written.append(str(legacy_path))
    else:
        filter_path = out / "filter_catalog.json"
        filter_path.write_text(
            json.dumps(legacy_filter_catalog, indent=2),
            encoding="utf-8",
        )
        written.append(str(filter_path))

    golden_path = out / "golden_questions.json"
    golden_path.write_text(json.dumps(golden, indent=2), encoding="utf-8")
    written.append(str(golden_path))

    if coverage is not None:
        cov_path = out / "coverage_report.json"
        cov_path.write_text(
            json.dumps(coverage.model_dump(), indent=2, default=str),
            encoding="utf-8",
        )
        written.append(str(cov_path))

    # proposed_overwrites.md — every place we replaced a stub baseline value
    # with enriched content. Empty file is fine (means baseline was already
    # in good shape OR the LLM proposed nothing).
    overwrites_path = out / "proposed_overwrites.md"
    overwrites_path.write_text(
        _render_overwrites_md(overwrite_ledger), encoding="utf-8"
    )
    written.append(str(overwrites_path))

    # uncertain_fields.md — every field the LLM admitted it couldn't ground.
    # This is the trust signal: you know which descriptions are best-guesses
    # vs evidence-backed before the LookML lands in production.
    uncertain_ledger: list[dict[str, Any]] = []
    confidence_summary: dict[str, dict[str, int]] = {}
    for table_name, eo in enriched_outputs.items():
        for entry in eo.uncertain_fields or []:
            row = {**entry, "table": table_name}
            uncertain_ledger.append(row)
        if eo.field_confidences:
            counts = {"grounded": 0, "inferred": 0, "guessed": 0}
            for c in eo.field_confidences.values():
                if c in counts:
                    counts[c] += 1
            confidence_summary[table_name] = counts
    uncertain_path = out / "uncertain_fields.md"
    uncertain_path.write_text(
        _render_uncertain_md(uncertain_ledger, confidence_summary),
        encoding="utf-8",
    )
    written.append(str(uncertain_path))

    return {"status": "ok", "error": None, "files_written": written}


def _render_uncertain_md(
    ledger: list[dict[str, Any]],
    confidence_summary: dict[str, dict[str, int]],
) -> str:
    """Per-table summary of confidence labels + the full list of fields
    the LLM flagged as guessed/inferred without anchoring evidence.
    """
    lines: list[str] = ["# Uncertain fields", ""]
    if confidence_summary:
        lines.append("## Confidence summary per table")
        lines.append("")
        lines.append("| Table | Grounded | Inferred | Guessed |")
        lines.append("|---|---:|---:|---:|")
        for table_name in sorted(confidence_summary.keys()):
            c = confidence_summary[table_name]
            lines.append(
                f"| `{table_name}` | {c.get('grounded', 0)} "
                f"| {c.get('inferred', 0)} | {c.get('guessed', 0)} |"
            )
        lines.append("")

    if not ledger:
        lines.append(
            "_No fields flagged as uncertain — every description / type / "
            "role the LLM produced is anchored to MDM, baseline, or "
            "query-usage evidence._"
        )
        return "\n".join(lines) + "\n"

    lines.append("## Fields needing human review")
    lines.append("")
    lines.append(
        "Each entry below is a field the LLM admitted it couldn't ground. "
        "Verify the description / type / role is correct before the next "
        "iteration; if a field is consistently guessed, the underlying "
        "data source (MDM, BQ DISTINCT, glossary) probably needs a fix."
    )
    lines.append("")

    by_table: dict[str, list[dict[str, Any]]] = {}
    for e in ledger:
        by_table.setdefault(e.get("table") or "<unknown>", []).append(e)

    for table_name in sorted(by_table.keys()):
        lines.append(f"### `{table_name}`")
        lines.append("")
        for e in by_table[table_name]:
            kind = e.get("field_kind") or "field"
            name = e.get("field_name") or "?"
            attr = e.get("attribute") or "value"
            conf = e.get("confidence") or "guessed"
            value = e.get("value") or ""
            reason = e.get("reason") or "no anchor in any source"
            lines.append(
                f"- **{kind} `{name}` — {attr}** [{conf}]"
            )
            if value:
                lines.append(f"    - proposed: `{value}`")
            lines.append(f"    - reason: _{reason}_")
        lines.append("")
    return "\n".join(lines)


def _render_overwrites_md(ledger: list[dict[str, Any]]) -> str:
    """Render the merge ledger to a scannable markdown report."""
    if not ledger:
        return (
            "# Proposed overwrites\n\n"
            "_No baseline values were replaced this run — every existing "
            "description / label / etc. was either ≥ 30 chars (assumed "
            "human-curated, preserved) or had no enriched alternative._\n"
        )

    lines: list[str] = ["# Proposed overwrites", ""]
    lines.append(
        "Each entry below is a baseline value that was either replaced "
        "(because it was a < 30-char stub) or that the LLM flagged as "
        "actually wrong. Review before the next iteration.\n"
    )
    by_table: dict[str, list[dict[str, Any]]] = {}
    for e in ledger:
        by_table.setdefault(e.get("table") or "<unknown>", []).append(e)

    for table_name in sorted(by_table.keys()):
        lines.append(f"## `{table_name}`\n")
        for e in by_table[table_name]:
            kind = e.get("field_kind") or "field"
            name = e.get("field_name") or "<unnamed>"
            attr = e.get("attribute") or "value"
            lines.append(f"### {kind} `{name}` — `{attr}`")
            source = e.get("source")
            if source == "llm_flagged":
                lines.append("LLM-flagged as inaccurate (not auto-replaced).")
            else:
                lines.append(f"_{e.get('reason', '')}_")
            lines.append("")
            lines.append("**baseline:**")
            lines.append(f"```\n{e.get('baseline_value', '')}\n```")
            lines.append("**proposed:**")
            lines.append(f"```\n{e.get('proposed_value', '')}\n```")
            lines.append("")
    return "\n".join(lines)


# ─── Disambiguating description rendering (B) ────────────────


def _apply_view_description(
    view_lkml: str, vd: ViewDescription | None,
) -> str:
    """Inject a disambiguation comment block + description: parameter.

    Two output surfaces:
      1. ``# === DISAMBIGUATION ===`` comment block above ``view:`` —
         indexable by Radix, readable for humans editing the file.
      2. LookML ``description:`` parameter inside the view body — this
         is the field Looker shows in the UI. Synthesized from one_liner +
         grain + scope.

    No-op when vd is None or has no content. Idempotent: re-applying
    on a previously-decorated view replaces the existing block so we
    don't accumulate stale disambiguation as the plan iterates.
    """
    if vd is None:
        return view_lkml
    has_content = any([
        vd.one_liner, vd.grain, vd.scope,
        vd.when_to_use, vd.when_not_to_use, vd.distinguishes_from,
    ])
    if not has_content:
        return view_lkml

    # Build comment block.
    block_lines = ["# === DISAMBIGUATION ==="]
    if vd.one_liner:
        block_lines.append(f"# summary: {vd.one_liner}")
    if vd.grain:
        block_lines.append(f"# grain: {vd.grain}")
    if vd.scope:
        block_lines.append(f"# scope: {vd.scope}")
    if vd.when_to_use:
        block_lines.append(f"# use_when: {vd.when_to_use}")
    if vd.when_not_to_use:
        block_lines.append(f"# do_not_use_when: {vd.when_not_to_use}")
    if vd.distinguishes_from:
        block_lines.append("# distinguishes_from:")
        for entry in vd.distinguishes_from:
            vn = entry.get("view_name", "?")
            diff = entry.get("how_it_differs", "")
            block_lines.append(f"#   - {vn}: {diff}")
    block_lines.append("# === END DISAMBIGUATION ===")
    block = "\n".join(block_lines)

    # Strip any prior disambiguation block (idempotency).
    text = re.sub(
        r"# === DISAMBIGUATION ===.*?# === END DISAMBIGUATION ===\n?",
        "", view_lkml, flags=re.DOTALL,
    )

    # IMPORTANT: inject `description:` FIRST. ``lkml.dump`` re-emits the
    # entire file, which strips comments — so the comment block must be
    # prepended AFTER the lkml round-trip.
    synthesized = _synthesize_view_description(vd)
    if synthesized:
        text = _inject_view_description_param(text, synthesized)

    # Now find the view: keyword and prepend block above it.
    view_match = re.search(r"^(\s*)view:\s*", text, flags=re.MULTILINE)
    if view_match:
        idx = view_match.start()
        text = text[:idx] + block + "\n\n" + text[idx:]
    else:
        # No view: keyword found — prepend at top.
        text = block + "\n\n" + text
    return text


def _synthesize_view_description(vd: ViewDescription) -> str:
    """Compose a single LookML description: value from structured fields."""
    parts: list[str] = []
    if vd.one_liner:
        parts.append(vd.one_liner.strip().rstrip("."))
    if vd.grain:
        parts.append(f"Grain: {vd.grain.strip().rstrip('.')}")
    if vd.scope:
        parts.append(f"Scope: {vd.scope.strip().rstrip('.')}")
    text = ". ".join(p for p in parts if p)
    if len(text) > 240:
        text = text[:237] + "..."
    return text


def _inject_view_description_param(view_lkml: str, desc: str) -> str:
    """Add or replace the ``description:`` parameter inside ``view: x { ... }``.

    Uses lkml.parse to be safe; falls back to no-op if the parser can't
    find a single view block.
    """
    try:
        parsed = lkml.load(view_lkml)
    except Exception:  # noqa: BLE001
        return view_lkml
    views = parsed.get("views") or []
    if not views or len(views) != 1:
        return view_lkml
    view = views[0]
    # Quote-escape the description for LookML — wrap in double quotes,
    # escape internal double quotes.
    safe = desc.replace('"', '\\"')
    view["description"] = safe
    try:
        dumped = lkml.dump(parsed)
        return dumped if dumped is not None else view_lkml
    except Exception:  # noqa: BLE001
        return view_lkml


def _apply_explore_description(
    explore_lkml: str, ed: ExploreDescription | None,
) -> str:
    """Inject a disambiguation comment block + description: parameter for an explore.

    Same idempotency as _apply_view_description. Renders primary_questions
    and anti_questions as structured comments so Radix indexing can pick
    them up; canonical_filters and join_paths surface in both block and
    LookML description.
    """
    if ed is None:
        return explore_lkml
    has_content = any([
        ed.one_liner, ed.primary_questions, ed.anti_questions,
        ed.canonical_filters, ed.join_paths,
    ])
    if not has_content:
        return explore_lkml

    block_lines = ["# === EXPLORE DISAMBIGUATION ==="]
    if ed.one_liner:
        block_lines.append(f"# summary: {ed.one_liner}")
    if ed.join_paths:
        block_lines.append("# join_paths:")
        for jp in ed.join_paths:
            block_lines.append(f"#   - {jp}")
    if ed.canonical_filters:
        block_lines.append("# canonical_filters:")
        for k, v in ed.canonical_filters.items():
            block_lines.append(f"#   - {k}: {v}")
    if ed.primary_questions:
        block_lines.append("# primary_questions (use this explore):")
        for q in ed.primary_questions:
            block_lines.append(f"#   - {q}")
    if ed.anti_questions:
        block_lines.append("# anti_questions (use a sibling explore):")
        for q in ed.anti_questions:
            block_lines.append(f"#   - {q}")
    block_lines.append("# === END EXPLORE DISAMBIGUATION ===")
    block = "\n".join(block_lines)

    text = re.sub(
        r"# === EXPLORE DISAMBIGUATION ===.*?# === END EXPLORE DISAMBIGUATION ===\n?",
        "", explore_lkml, flags=re.DOTALL,
    )

    # Inject description param BEFORE prepending the comment block —
    # lkml.dump strips comments on re-emit.
    if ed.one_liner:
        text = _inject_explore_description_param(text, ed.one_liner)

    explore_match = re.search(r"^(\s*)explore:\s*", text, flags=re.MULTILINE)
    if explore_match:
        idx = explore_match.start()
        text = text[:idx] + block + "\n\n" + text[idx:]
    else:
        text = block + "\n\n" + text
    return text


def _inject_explore_description_param(explore_lkml: str, desc: str) -> str:
    """Add or replace `description:` inside `explore: x { ... }`."""
    try:
        parsed = lkml.load(explore_lkml)
    except Exception:  # noqa: BLE001
        return explore_lkml
    explores = parsed.get("explores") or []
    if not explores or len(explores) != 1:
        return explore_lkml
    safe = desc[:240].replace('"', '\\"')
    explores[0]["description"] = safe
    try:
        dumped = lkml.dump(parsed)
        return dumped if dumped is not None else explore_lkml
    except Exception:  # noqa: BLE001
        return explore_lkml


# ─── T2: clustered explore rendering ────────────────────────


def _render_clustered_explore(
    ep: Any, *, aggregate_tables: list[dict[str, Any]] | None = None,
) -> str:
    """Render an ExplorePlan as a Looker explore block.

    Includes the disambiguation comment block (description.one_liner +
    primary_questions + anti_questions if present) above the explore,
    then the LookML body with joins (each carrying corpus-inferred
    relationship), always_filter, sql_always_where, and any
    aggregate_table proposals matching this explore's base_view.
    """
    lines: list[str] = []
    desc = getattr(ep, "description", None)

    # Disambiguation comment block above the explore.
    block: list[str] = [f"# === EXPLORE: {ep.explore_name} ==="]
    block.append(
        f"# cluster_id: {ep.cluster_id} "
        f"(serves {ep.member_query_count} gold queries; "
        f"base_view_bonus≈{ep.base_view_bonus_estimate})"
    )
    if desc is not None:
        if getattr(desc, "one_liner", ""):
            block.append(f"# summary: {desc.one_liner}")
        if getattr(desc, "join_paths", None):
            block.append("# join_paths:")
            for jp in desc.join_paths:
                block.append(f"#   - {jp}")
        if getattr(desc, "primary_questions", None):
            block.append("# primary_questions:")
            for q in desc.primary_questions:
                block.append(f"#   - {q}")
        if getattr(desc, "anti_questions", None):
            block.append("# anti_questions:")
            for q in desc.anti_questions:
                block.append(f"#   - {q}")
    block.append(f"# === END EXPLORE: {ep.explore_name} ===")
    lines.extend(block)
    lines.append("")

    # Explore body.
    lines.append(f"explore: {ep.explore_name} {{")
    if ep.base_view and ep.base_view != ep.explore_name:
        lines.append(f"  view_name: {ep.base_view}")
    if desc is not None and getattr(desc, "one_liner", ""):
        safe_desc = desc.one_liner[:240].replace('"', '\\"')
        lines.append(f'  description: "{safe_desc}"')
    if ep.sql_always_where:
        lines.append(f"  sql_always_where: {ep.sql_always_where} ;;")
    if ep.always_filter:
        af_lines = ", ".join(
            f"{k}: \"{v}\"" for k, v in ep.always_filter.items()
        )
        lines.append(f"  always_filter: {{ filters: [{af_lines}] }}")
    for j in ep.joins:
        right = j.get("right_table", "?")
        rel = j.get("relationship", "many_to_one")
        lk = j.get("left_key", "?")
        rk = j.get("right_key", "?")
        # T4.2: when the dim view is used by multiple explores in the
        # corpus, propose an aliased join name via `from:` so the
        # explore can have a role-specific label without colliding
        # with sibling explores' use of the same view.
        from_alias = j.get("from")
        view_label = j.get("view_label")
        join_name = j.get("alias") or right
        lines.append(f"  join: {join_name} {{")
        if from_alias and from_alias != join_name:
            lines.append(f"    from: {from_alias}")
        if view_label:
            safe = view_label.replace('"', '\\"')
            lines.append(f'    view_label: "{safe}"')
        lines.append(f"    relationship: {rel}")
        lines.append(
            f"    sql_on: ${{{ep.base_view}.{lk}}} = ${{{join_name}.{rk}}} ;;"
        )
        lines.append("  }")

    # T4.1: aggregate_table proposals matching this explore's base_view.
    matching_aggs = [
        a for a in (aggregate_tables or [])
        if (a.get("base_view") or "").lower() == (ep.base_view or "").lower()
    ]
    for agg in matching_aggs:
        lines.append("")
        lines.append(_render_aggregate_table(agg))

    lines.append("}")
    return "\n".join(lines)


def _render_aggregate_table(agg: dict[str, Any]) -> str:
    """Render one aggregate_table block for inclusion inside an explore.

    Aggregate tables are Looker's materialized rollup mechanism: queries
    fitting the (dimensions, measures, filters) shape get answered from
    the rollup in 5% of the underlying-query time. Generated proposals
    come from corpus hot-GROUP-BYs (T3.2 propose_aggregate_tables).

    Refresh strategy: ``sql_trigger_value: SELECT CURRENT_DATE() ;;``
    rebuilds daily — safe default for analytical workloads. Operators
    can tune per their freshness SLO.
    """
    name = agg.get("name", "agg_default")
    dims = agg.get("group_by") or []
    measures = agg.get("measures") or []
    base = agg.get("base_view", "?")
    # Measures in aggregate_table reference the explore's measure names —
    # we use a heuristic of "total_<col>" since that's our skeleton naming.
    # Real measure names get mapped via the metric catalog post-publish.
    measure_refs = [f"{base}.total_{m}" for m in measures]
    dim_refs = [f"{base}.{d}" for d in dims]

    lines = [
        f"  # Materialized rollup — answers {agg.get('frequency', '?')} "
        f"corpus queries in 5% of base time.",
        f"  aggregate_table: {name} {{",
        "    materialization: {",
        "      sql_trigger_value: SELECT CURRENT_DATE() ;;",
        "    }",
        "    query: {",
    ]
    if dim_refs:
        lines.append(f"      dimensions: [{', '.join(dim_refs)}]")
    if measure_refs:
        lines.append(f"      measures: [{', '.join(measure_refs)}]")
    if agg.get("filters"):
        filt_pairs = ", ".join(
            f'{k}: "{v}"' for k, v in (agg["filters"] or {}).items()
        )
        lines.append(f"      filters: [{filt_pairs}]")
    lines.append("      timezone: \"UTC\"")
    lines.append("    }")
    lines.append("  }")
    return "\n".join(lines)


# ─── T4.2: view_label / from for re-used dims ───────────────


def _annotate_with_aliasing(
    explore_plans: list[Any],
) -> list[Any]:
    """Tag joins with `view_label` when the dim view is shared across explores.

    Heuristic: if a dim view appears as a join target in ≥ 2 distinct
    explores, each of those explore-level joins gets a view_label tied
    to the explore's role for that dim. This prevents Looker UI showing
    the same view name three times when the analyst is browsing.

    The annotation lands on the join dict (j["view_label"]). Renderer
    picks it up via the `view_label:` LookML parameter.
    """
    # Tally dim usage across explores.
    dim_usage: dict[str, int] = {}
    for ep in explore_plans:
        for j in (ep.joins or []):
            right = (j.get("right_table") or "").lower()
            if right:
                dim_usage[right] = dim_usage.get(right, 0) + 1

    for ep in explore_plans:
        explore_role_word = _role_word_for_explore(ep)
        for j in (ep.joins or []):
            right = (j.get("right_table") or "").lower()
            if dim_usage.get(right, 0) >= 2:
                # Annotate with explore-scoped label so the same dim
                # gets different labels in different explores. Include
                # the base view (e.g. "Transaction") so labels never
                # collide when role words match across explores.
                base_pretty = _humanize_view_name(ep.base_view)
                role = explore_role_word or "Joined"
                pretty = _humanize_view_name(j.get("right_table"))
                j["view_label"] = (
                    f"{base_pretty} {pretty}".strip()
                    if base_pretty
                    else f"{role} {pretty}".strip()
                )
    return explore_plans


def _role_word_for_explore(ep: Any) -> str:
    """Extract a role word from the explore_name suffix.

    For ``transaction_by_cm``, returns "By Cm".
    For ``cardmember_dim``, returns "" (no role suffix).
    """
    name = (ep.explore_name or "").lower()
    base = (ep.base_view or "").lower()
    if name.startswith(base + "_by_"):
        suffix = name[len(base) + 4:]
        return " ".join(w.capitalize() for w in suffix.split("_"))
    if name.startswith(base + "_for_"):
        suffix = name[len(base) + 5:]
        return " ".join(w.capitalize() for w in suffix.split("_"))
    return ""


def _humanize_view_name(view: str | None) -> str:
    if not view:
        return ""
    return " ".join(w.capitalize() for w in view.split("_"))
