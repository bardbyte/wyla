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


def _index_by_name(items: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    if not items:
        return {}
    return {it.get("name", ""): it for it in items if it.get("name")}


def _merge_field(baseline: dict[str, Any], enriched: dict[str, Any]) -> dict[str, Any]:
    """Merge one dimension/measure/dim_group dict.

    Baseline is authoritative for ``sql``/``type``/``primary_key``/etc.
    Enriched may CONTRIBUTE description/label/tags/value_format if absent in
    baseline. Tags are unioned (preserving order, baseline first).
    """
    merged = dict(baseline)
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
        if field in baseline and baseline[field]:
            continue
        if field in enriched and enriched[field]:
            merged[field] = enriched[field]
    # NEVER overwrite preserve fields from baseline.
    for field in _PRESERVE_FIELDS:
        if field in baseline:
            merged[field] = baseline[field]
    return merged


def additive_merge_view(baseline_lkml: str, enriched_lkml: str) -> str:
    """Merge enriched view INTO baseline. Returns serialised LookML string.

    If baseline is empty/unparseable, the enriched view is returned as-is
    (this happens for brand-new tables that had no Looker-generated baseline).
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
        merged_views.append(_merge_one_view(bv, ev))
    # Add any enriched-only views (e.g. derived_table views from CTEs).
    for ev in enr_views:
        if ev.get("name", "") not in seen_view_names:
            merged_views.append(ev)

    out_tree: dict[str, Any] = {"views": merged_views}
    return lkml.dump(out_tree)


def _merge_one_view(baseline: dict[str, Any], enriched: dict[str, Any]) -> dict[str, Any]:
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
        out_dims.append(_merge_field(bd, ed) if ed else bd)
    for name, ed in enr_dims.items():
        if name not in base_dims:
            out_dims.append(ed)

    out_dgs: list[dict[str, Any]] = []
    for name, bd in base_dgs.items():
        ed = enr_dgs.get(name)
        out_dgs.append(_merge_field(bd, ed) if ed else bd)
    for name, ed in enr_dgs.items():
        if name not in base_dgs:
            out_dgs.append(ed)

    out_msrs: list[dict[str, Any]] = []
    for name, bm in base_msrs.items():
        em = enr_msrs.get(name)
        out_msrs.append(_merge_field(bm, em) if em else bm)
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

    for table_name, eo in enriched_outputs.items():
        baseline_lkml = _read_baseline(baseline, table_name)
        merged = additive_merge_view(baseline_lkml, eo.view_lkml)
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

    return {"status": "ok", "error": None, "files_written": written}
