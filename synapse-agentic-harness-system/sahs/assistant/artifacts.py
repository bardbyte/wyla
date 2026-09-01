"""Artifact types and the rendering rules (Synapse v2 §5/§6).

Artifacts are the outputs the user keeps: standalone, versioned,
exportable. Governance lives HERE now, as schema, not as a gate in
front of the conversation:

  Rule 1 — any artifact that shows a number carries its definition
  status and meridian line in-schema. The validator refuses one that
  does not, with a teaching message; the model fixes its own spec.

  Rule 2 — a status of ``composed`` keeps the EXPLORATORY watermark
  until a passing reconcile/crosscheck fact from THIS trajectory is
  cited. The watermark is forced by the validator, never negotiated
  by prose. (Check facts arrive with the §13.2 toolkit; until a fact
  is cited, composed numbers simply stay watermarked.)

  Rule 3 lives elsewhere: nothing here writes truth; the clerk is
  still the only writer.

Every stored spec is normalized: build id stamped, watermark decided,
unknown fields dropped. What the panel renders is exactly what the
validator passed — the renderer never patches an artifact up.
"""

from __future__ import annotations

from typing import Any

TYPES: tuple[str, ...] = ("chart", "table", "document", "kpi",
                          "dashboard", "diagram")
CHART_KINDS = ("line", "bar", "scatter", "area")
DIAGRAM_KINDS = ("graph", "mermaid")
# a dashboard nests tiles, never another dashboard (or a diagram)
PANEL_TYPES = ("kpi", "chart", "table", "document")
STATUSES = ("certified", "pending", "composed", "exploratory")
# statuses that may shed the watermark without a cited check fact
SELF_STANDING = ("certified", "pending")


def _problem(code: str, detail: str, hint: str) -> dict[str, str]:
    return {"code": code, "detail": detail, "hint": hint}


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_provenance(spec: dict[str, Any],
                      problems: list[dict[str, str]]) -> dict[str, Any]:
    prov = spec.get("provenance")
    if not isinstance(prov, dict):
        problems.append(_problem(
            "provenance_missing",
            "this artifact shows numbers but carries no provenance",
            "add \"provenance\": {\"status\": certified|pending|"
            "composed|exploratory, \"meridian_line\": \"<the "
            "one-sentence disclosure>\"} — get_definition_line "
            "writes it for a governed metric"))
        return {}
    status = str(prov.get("status", "")).lower()
    if status not in STATUSES:
        problems.append(_problem(
            "status_unknown",
            f"provenance.status {prov.get('status')!r} is not one of "
            + ", ".join(STATUSES),
            "say what the number IS: certified (on the meridian), "
            "pending (governed but unreviewed), composed (you built "
            "it from parts), exploratory (a look, not a claim)"))
    if not str(prov.get("meridian_line", "")).strip():
        problems.append(_problem(
            "meridian_line_missing",
            "no meridian line: the reader cannot see whose "
            "definition this is",
            "one sentence: which definition, whose authority, on the "
            "meridian or off it — get_definition_line returns it"))
    return prov


def validate_artifact(type: str, spec: Any, *,
                      build_id: str = "",
                      facts: frozenset[str] = frozenset(),
                      build: Any = None
                      ) -> tuple[dict[str, Any] | None,
                                 list[dict[str, str]]]:
    """→ (normalized spec, []) or (None, teaching problems)."""
    problems: list[dict[str, str]] = []
    if type not in TYPES:
        return None, [_problem(
            "unknown_type", f"unknown artifact type {type!r}",
            "types: " + " | ".join(TYPES))]
    if not isinstance(spec, dict):
        return None, [_problem(
            "bad_spec", "the spec must be an object",
            "see the artifact tool description for each type's shape")]

    out: dict[str, Any] = {"build_id": build_id}
    shows_numbers = False

    if type == "chart":
        kind = str(spec.get("kind", "")).lower()
        if kind not in CHART_KINDS:
            problems.append(_problem(
                "chart_kind", f"chart kind {spec.get('kind')!r} is "
                "not one of " + ", ".join(CHART_KINDS),
                "pick the shape that answers the question: line for "
                "change over time, bar for comparison"))
        series = spec.get("series")
        clean_series = []
        if not isinstance(series, list) or not series:
            problems.append(_problem(
                "chart_series", "a chart needs a non-empty series "
                "list", "series: [{\"name\": …, \"points\": "
                "[[x, y], …]}] with numeric y"))
        else:
            for i, entry in enumerate(series):
                points = (entry or {}).get("points") \
                    if isinstance(entry, dict) else None
                if (not isinstance(points, list) or not points
                        or not all(isinstance(p, (list, tuple))
                                   and len(p) == 2 and _numeric(p[1])
                                   for p in points)):
                    problems.append(_problem(
                        "chart_points",
                        f"series[{i}] has no usable points",
                        "each point is [x, y] with numeric y; x is a "
                        "label or a date string"))
                    continue
                clean_series.append({
                    "name": str(entry.get("name", f"series {i + 1}")),
                    "points": [[p[0], p[1]] for p in points]})
        shows_numbers = True
        out.update(kind=kind, series=clean_series,
                   x_label=str(spec.get("x_label", "")),
                   y_label=str(spec.get("y_label", "")),
                   unit=str(spec.get("unit", "")))

    elif type == "table":
        columns = spec.get("columns")
        rows = spec.get("rows")
        if not isinstance(columns, list) or not columns or not all(
                isinstance(c, dict) and c.get("key") for c in columns):
            problems.append(_problem(
                "table_columns", "columns must be a non-empty list "
                "of {key, label}", "e.g. [{\"key\": \"day\", "
                "\"label\": \"Day\"}, …]"))
            columns = []
        if not isinstance(rows, list):
            problems.append(_problem(
                "table_rows", "rows must be a list of objects keyed "
                "by column key", "e.g. [{\"day\": \"2026-08-01\", "
                "\"spend\": 1200}]"))
            rows = []
        keys = [c["key"] for c in columns]
        clean_rows = [{k: (r or {}).get(k) for k in keys}
                      for r in rows if isinstance(r, dict)]
        shows_numbers = any(_numeric(v) for r in clean_rows
                            for v in r.values())
        out.update(columns=[{"key": c["key"],
                             "label": str(c.get("label", c["key"])),
                             **({"status": c["status"]}
                                if c.get("status") else {})}
                            for c in columns],
                   rows=clean_rows)

    elif type == "document":
        markdown = spec.get("markdown")
        if not isinstance(markdown, str) or not markdown.strip():
            problems.append(_problem(
                "document_markdown", "a document needs markdown text",
                "spec: {\"markdown\": \"…\"} — headings, lists, and "
                "tables render; numbers in prose still need the "
                "provenance footer"))
        out["markdown"] = markdown if isinstance(markdown, str) else ""
        # prose numbers cannot be schema-detected; a document is
        # exploratory unless provenance is declared
        shows_numbers = bool(spec.get("provenance"))

    elif type == "kpi":
        value = spec.get("value")
        if not _numeric(value):
            problems.append(_problem(
                "kpi_value", "a kpi needs a numeric value",
                "spec: {\"value\": <number>, \"unit\"?, \"label\"?, "
                "\"delta\"?} plus provenance {status, meridian_line}"))
        shows_numbers = True
        out.update(value=value if _numeric(value) else None,
                   unit=str(spec.get("unit", "")),
                   label=str(spec.get("label", "")),
                   **({"delta": spec["delta"]}
                      if _numeric(spec.get("delta")) else {}))

    elif type == "dashboard":
        panels = spec.get("panels")
        clean_panels: list[dict[str, Any]] = []
        marked = False
        if not isinstance(panels, list) or not panels:
            problems.append(_problem(
                "dashboard_panels",
                "a dashboard needs a non-empty panels list",
                "panels: [{\"type\": kpi|chart|table|document, "
                "\"title\"?, \"spec\": {…}}] — each numeric panel "
                "carries its OWN provenance"))
        else:
            for i, panel in enumerate(panels):
                ptype = str(panel.get("type", "")) \
                    if isinstance(panel, dict) else ""
                if ptype not in PANEL_TYPES:
                    problems.append(_problem(
                        "panel_type",
                        f"panels[{i}] type {ptype!r} is not one of "
                        + ", ".join(PANEL_TYPES),
                        "a dashboard nests tiles, never another "
                        "dashboard"))
                    continue
                sub_spec = panel.get("spec")
                sub, sub_problems = validate_artifact(
                    ptype,
                    sub_spec if isinstance(sub_spec, dict) else {},
                    build_id=build_id, facts=facts, build=build)
                if sub_problems:
                    problems.extend(_problem(
                        p["code"],
                        f"panels[{i}] ({ptype}): {p['detail']}",
                        p["hint"]) for p in sub_problems)
                    continue
                clean_panels.append({
                    "type": ptype,
                    "title": str(panel.get("title", ""))[:120],
                    "spec": sub})
                marked = marked or bool(sub.get("watermark"))
        filters: list[dict[str, Any]] = []
        for j, item in enumerate(spec.get("filters") or []):
            slot = str((item or {}).get("slot", "")).strip() \
                if isinstance(item, dict) else ""
            options = (item or {}).get("options") \
                if isinstance(item, dict) else None
            if not slot or not isinstance(options, list) \
                    or not options:
                problems.append(_problem(
                    "dashboard_filter",
                    f"filters[{j}] needs a slot and options",
                    "filters: [{\"slot\": \"country\", \"options\": "
                    "[\"US\", \"CA\"], \"active\"?}] — picking one "
                    "sends a whatif request through the conversation, "
                    "never a hidden query"))
                continue
            options = [str(o) for o in options][:12]
            active = str(item.get("active", options[0]))
            filters.append({
                "slot": slot, "label": str(item.get("label", slot)),
                "options": options,
                "active": active if active in options
                else options[0]})
        out.update(panels=clean_panels, filters=filters,
                   notes=str(spec.get("notes", ""))[:2000])
        if marked:
            out["watermark"] = "EXPLORATORY"
        shows_numbers = False        # the tiles carry the disclosure

    elif type == "diagram":
        kind = str(spec.get("kind", "")).lower()
        if kind not in DIAGRAM_KINDS:
            problems.append(_problem(
                "diagram_kind", f"diagram kind {spec.get('kind')!r} "
                "is not one of " + ", ".join(DIAGRAM_KINDS),
                "graph: {nodes, edges} as subgraph()/constellation "
                "return them; mermaid: {source} for lineage, "
                "funnels, decision trees"))
        if kind == "mermaid":
            source = spec.get("source")
            if not isinstance(source, str) or not source.strip():
                problems.append(_problem(
                    "diagram_source",
                    "a mermaid diagram needs source text",
                    "spec: {\"kind\": \"mermaid\", \"source\": "
                    "\"flowchart TD; …\"}"))
            out.update(kind="mermaid",
                       source=(source if isinstance(source, str)
                               else "")[:6000])
        elif kind == "graph":
            nodes = spec.get("nodes")
            clean_nodes: list[dict[str, Any]] = []
            if not isinstance(nodes, list) or not nodes:
                problems.append(_problem(
                    "diagram_nodes",
                    "a graph diagram needs a non-empty nodes list",
                    "nodes: [{\"id\", \"kind\"?, \"label\"?, "
                    "\"status\"?}] — subgraph() returns exactly this "
                    "shape"))
            else:
                for n in nodes:
                    if isinstance(n, dict) and n.get("id"):
                        clean_nodes.append({
                            "id": str(n["id"]),
                            "kind": str(n.get("kind", "")),
                            "label": str(n.get("label")
                                         or n["id"])[:80],
                            **({"status": str(n["status"])}
                               if n.get("status") else {})})
            ids = {n["id"] for n in clean_nodes}
            clean_edges: list[dict[str, Any]] = []
            for j, e in enumerate(spec.get("edges") or []):
                a, b = str((e or {}).get("a", "")), \
                    str((e or {}).get("b", ""))
                if a not in ids or b not in ids:
                    problems.append(_problem(
                        "diagram_edges",
                        f"edges[{j}] joins {a!r}–{b!r} but both ends "
                        "must be node ids",
                        "every edge endpoint must appear in nodes"))
                    continue
                clean_edges.append({
                    "a": a, "b": b, "rel": str(e.get("rel", "")),
                    **({"tier": str(e["tier"])}
                       if e.get("tier") else {})})
            out.update(kind="graph", nodes=clean_nodes,
                       edges=clean_edges)
        shows_numbers = bool(spec.get("provenance"))

    # ── the rendering rules ──────────────────────────────────
    if shows_numbers or spec.get("provenance") is not None:
        prov = _check_provenance(spec, problems)
        # rule 1 with teeth: "certified" is a claim about the BUILD,
        # not a vibe — it must name the metric, and the metric must
        # actually be certified there
        if str((prov or {}).get("status", "")).lower() == "certified":
            metric_id = (prov or {}).get("metric_id", "")
            if not metric_id:
                problems.append(_problem(
                    "certified_needs_metric",
                    "status certified without a metric_id",
                    "name the governed metric in provenance."
                    "metric_id (from search_semantics or resolve), "
                    "or use status composed/exploratory"))
            elif build is not None:
                row = next((m for m in getattr(build, "metrics", [])
                            if m.get("id") == metric_id), None)
                served = (row or {}).get("status_served")                     or (row or {}).get("status")
                if row is None or served != "certified":
                    problems.append(_problem(
                        "status_overclaimed",
                        f"{metric_id} is "
                        + ("not in this build" if row is None
                           else f"{served}, not certified"),
                        "say what it IS — the reader decides what "
                        "certified means, not the chart"))
        if prov:
            out["provenance"] = {
                "status": str(prov.get("status", "")).lower(),
                "meridian_line": str(prov.get("meridian_line", "")),
                "facts": [str(f) for f in (prov.get("facts") or [])],
                **({"metric_id": prov["metric_id"]}
                   if prov.get("metric_id") else {}),
                **({"source_sql": str(prov["source_sql"])[:2000]}
                   if prov.get("source_sql") else {}),
            }
    elif type == "document":
        out["watermark"] = "EXPLORATORY"

    if problems:
        return None, problems

    # Rule 2: composed/exploratory keep the watermark unless a cited
    # check fact from this trajectory stands behind the number
    prov = out.get("provenance")
    if prov:
        cited = set(prov.get("facts", [])) & set(facts)
        if prov["status"] in SELF_STANDING:
            pass
        elif prov["status"] == "composed" and cited:
            prov["facts_verified"] = sorted(cited)
        else:
            out["watermark"] = "EXPLORATORY"
    if str(spec.get("caption", "")).strip():
        out["caption"] = str(spec["caption"])[:400]
    return out, []
