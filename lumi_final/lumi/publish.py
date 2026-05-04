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
from pathlib import Path
from typing import Any

import lkml

from lumi.schemas import CoverageReport, EnrichedOutput

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
) -> dict[str, Any]:
    """Materialise enriched outputs to ``output_dir``.

    Layout:
      ``output_dir/views/<table>.view.lkml``     additively merged view
      ``output_dir/models/lumi_enriched.model.lkml`` one explore include per view
      ``output_dir/metric_catalog.json``
      ``output_dir/filter_catalog.json``
      ``output_dir/golden_questions.json``
      ``output_dir/coverage_report.json`` (only if ``coverage`` given)

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
            model_lines.append(eo.explore_lkml.rstrip())
            model_lines.append("")
    model_path = models_dir / "lumi_enriched.model.lkml"
    model_path.write_text("\n".join(model_lines), encoding="utf-8")
    written.append(str(model_path))

    metric_catalog = build_metric_catalog(enriched_outputs)
    filter_catalog = build_filter_catalog(enriched_outputs)
    golden = build_golden_questions(enriched_outputs)

    metric_path = out / "metric_catalog.json"
    metric_path.write_text(json.dumps(metric_catalog, indent=2), encoding="utf-8")
    written.append(str(metric_path))

    filter_path = out / "filter_catalog.json"
    filter_path.write_text(json.dumps(filter_catalog, indent=2), encoding="utf-8")
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

    return {"status": "ok", "error": None, "files_written": written}


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
