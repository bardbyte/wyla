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

TYPES: tuple[str, ...] = ("chart", "table", "document")
CHART_KINDS = ("line", "bar", "scatter", "area")
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
                      facts: frozenset[str] = frozenset()
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

    # ── the rendering rules ──────────────────────────────────
    if shows_numbers or spec.get("provenance") is not None:
        prov = _check_provenance(spec, problems)
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
