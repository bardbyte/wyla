"""LookML parsing via the lkml library. Also handles dependency-ordered batching
for views with more than N fields and writes enriched views back to disk.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import lkml

from lumi.schemas import EnrichedView, ParsedField, ParsedView

logger = logging.getLogger(__name__)

_FIELD_KINDS = {
    "dimensions": "dimension",
    "measures": "measure",
    "dimension_groups": "dimension_group",
    "filters": "filter",
    "parameters": "parameter",
}

# Extract ${field_name} references (never regex-for-SQL, just for LookML ref expansion).
_REF_RE = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*)\}")

# Which attribute names we map onto ParsedField directly.
_RESERVED_ATTRS = {"name", "type", "sql", "label", "description", "tags"}


def parse_lookml_file(path: str | Path) -> dict[str, Any]:
    """Parse a single .view.lkml into a ParsedView."""
    file_path = Path(path)
    if not file_path.exists():
        return _err(f"File not found: {file_path}")

    try:
        text = file_path.read_text(encoding="utf-8")
        tree = lkml.load(text)
    except Exception as e:
        return _err(f"lkml.load failed for {file_path.name}: {e}")

    views = tree.get("views") or []
    if not views:
        return _err(f"No view blocks found in {file_path.name}")
    if len(views) > 1:
        logger.warning("%d views in %s — only the first is used.", len(views), file_path.name)

    raw_view = views[0]
    view_name = raw_view.get("name") or file_path.stem.removesuffix(".view")
    sql_table_name = raw_view.get("sql_table_name")
    derived_sql = None
    if "derived_table" in raw_view:
        derived_sql = raw_view["derived_table"].get("sql")

    fields = _collect_fields(raw_view)

    parsed = ParsedView(
        view_name=view_name,
        source_path=str(file_path),
        sql_table_name=sql_table_name,
        derived_table_sql=derived_sql,
        fields=fields,
    )
    logger.info("Parsed view %s (%d fields)", view_name, parsed.field_count)
    return {"status": "success", "parsed_view": parsed, "error": None}


def _collect_fields(raw_view: dict[str, Any]) -> list[ParsedField]:
    fields: list[ParsedField] = []
    for section, kind in _FIELD_KINDS.items():
        for raw_field in raw_view.get(section, []) or []:
            name = raw_field.get("name")
            if not name:
                continue
            existing = {
                k: _stringify(v)
                for k, v in raw_field.items()
                if k not in _RESERVED_ATTRS and v is not None
            }
            tags_raw = raw_field.get("tags")
            tags = list(tags_raw) if isinstance(tags_raw, list) else []
            fields.append(
                ParsedField(
                    name=name,
                    kind=kind,
                    type=_stringify(raw_field.get("type")) or None,
                    sql=_stringify(raw_field.get("sql")) or None,
                    label=_stringify(raw_field.get("label")) or None,
                    description=_stringify(raw_field.get("description")) or None,
                    tags=tags,
                    existing_attributes=existing,
                )
            )
    return fields


def _stringify(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, list | dict):
        return json.dumps(v, separators=(",", ":"))
    return str(v)


def batch_fields(
    view: ParsedView, field_threshold: int = 150, batch_size: int = 30
) -> list[list[str]]:
    """Return field-name batches for LLM enrichment, in dependency order.

    If field_count <= field_threshold, returns [[all field names]] (single batch).
    Otherwise splits into dep-ordered batches of <= batch_size, co-locating
    semantic siblings (same prefix) even if a batch briefly exceeds the size.
    """
    if view.field_count <= field_threshold:
        return [[f.name for f in view.fields]]

    deps = _build_dependency_graph(view)
    order = _topo_sort(list(deps.keys()), deps)

    batches: list[list[str]] = []
    current: list[str] = []
    current_prefix: str | None = None

    for name in order:
        prefix = name.split("_")[0]
        would_break_sibling = current_prefix is not None and prefix == current_prefix
        if len(current) >= batch_size and not would_break_sibling:
            batches.append(current)
            current = []
            current_prefix = None
        current.append(name)
        current_prefix = prefix

    if current:
        batches.append(current)
    return batches


def _build_dependency_graph(view: ParsedView) -> dict[str, set[str]]:
    names = {f.name for f in view.fields}
    graph: dict[str, set[str]] = {}
    for f in view.fields:
        refs = set()
        if f.sql:
            for match in _REF_RE.finditer(f.sql):
                token = match.group(1).split(".")[0]
                if token in names and token != f.name:
                    refs.add(token)
        graph[f.name] = refs
    return graph


def _topo_sort(names: list[str], deps: dict[str, set[str]]) -> list[str]:
    """Topological sort by dependency order. Cycles are broken arbitrarily."""
    visited: set[str] = set()
    ordered: list[str] = []

    def visit(n: str, stack: set[str]) -> None:
        if n in visited or n in stack:
            return
        stack.add(n)
        for dep in deps.get(n, set()):
            visit(dep, stack)
        stack.discard(n)
        visited.add(n)
        ordered.append(n)

    for n in names:
        visit(n, set())
    return ordered


def write_lookml_files(
    enriched_views: dict[str, EnrichedView],
    out_dir: str | Path,
) -> dict[str, Any]:
    """Render each EnrichedView as a .view.lkml file under `out_dir`.

    We produce clean LookML via lkml.dump() — this overwrites the original
    structure rather than surgically patching. The parsed view is already a
    complete representation; enrichment fills in label/description/tags/sql.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for view_name, ev in enriched_views.items():
        payload = _enriched_to_lkml(ev)
        target = out_path / f"{view_name}.view.lkml"
        try:
            target.write_text(lkml.dump(payload) or "", encoding="utf-8")
        except Exception as e:
            return _err(f"Failed to write {target}: {e}")
        written.append(str(target))

    logger.info("Wrote %d enriched view files to %s", len(written), out_path)
    return {"status": "success", "files": written, "error": None}


def _enriched_to_lkml(ev: EnrichedView) -> dict[str, Any]:
    dims: list[dict[str, Any]] = []
    measures: list[dict[str, Any]] = []
    dim_groups: list[dict[str, Any]] = []
    filters: list[dict[str, Any]] = []
    parameters: list[dict[str, Any]] = []

    for field in ev.fields:
        block: dict[str, Any] = {
            "name": field.name,
            "label": field.label,
            "description": field.description,
        }
        if field.type:
            block["type"] = field.type
        if field.sql:
            block["sql"] = field.sql
        if field.tags:
            block["tags"] = list(field.tags)

        if field.kind == "dimension":
            dims.append(block)
        elif field.kind == "measure":
            measures.append(block)
        elif field.kind == "dimension_group":
            dim_groups.append(block)
        elif field.kind == "filter":
            filters.append(block)
        elif field.kind == "parameter":
            parameters.append(block)

    view_block: dict[str, Any] = {
        "name": ev.view_name,
        "label": ev.view_label,
        "description": ev.view_description,
    }
    if dims:
        view_block["dimensions"] = dims
    if dim_groups:
        view_block["dimension_groups"] = dim_groups
    if measures:
        view_block["measures"] = measures
    if filters:
        view_block["filters"] = filters
    if parameters:
        view_block["parameters"] = parameters

    return {"views": [view_block]}


def _err(msg: str) -> dict[str, Any]:
    logger.error(msg)
    return {"status": "error", "parsed_view": None, "files": [], "error": msg}
