"""The conflict census — the platform's honest difficulty meter.

Week-one deliverable, recomputed every build: for every
(concept_label_norm, table) cell and every metric intent, how many
distinct canonical classes exist, with what support and what maximum
authority. One number per cell answers "how contested is this meaning?"
— which is simultaneously the governance work queue and the resolver's
ambiguity map.

Honesty constraints (pinned):
- label normalization is lower/trim/collapse-whitespace ONLY (E9): the
  census meta says so out loud — `ALIF` and "active locations in force"
  count separately; totals must not be read as deduplicated.
- census.json content is run-independent (no timestamps, no run ids) so
  byte-identical re-runs are provable — run metadata lives in the event
  stream, not the census.
- tail control: the main file keeps classes with support ≥ 2 or authority
  above snippet; the long tail spills to census_tail.jsonl; at most 10
  classes render per cell with an overflow count.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from sahs.canon.authority import Authority
from sahs.canon.canonical import CANON_VERSION, CanonResult, try_canon
from sahs.graph.quads import RANKING_WITNESSES, SOURCE_WITNESS
from sahs.loaders.records import ExpressionRecord, Quarantined

_MAX_CLASSES_PER_CELL = 10

E9_NOTE = ("labels are whitespace/case-normalized only, NOT alias/acronym-"
           "deduplicated; concept totals must not be read as deduplicated. "
           "An alias-aware derived view is deferred to post-P3.")


def norm_label(label: str) -> str:
    return " ".join((label or "").lower().split())


def canonicalize_records(
    records: list[ExpressionRecord],
    *,
    schema: dict[str, dict[str, str]] | None = None,
    on_ok: Callable[[], None] | None = None,
    on_quarantined: Callable[[str, str], None] | None = None,
) -> tuple[list[tuple[ExpressionRecord, CanonResult]], list[Quarantined]]:
    """Run every record through c(sql); quarantine failures loudly."""
    done: list[tuple[ExpressionRecord, CanonResult]] = []
    quarantined: list[Quarantined] = []
    for record in records:
        result, err = try_canon(record.raw_sql, schema=schema)
        if err is not None:
            quarantined.append(Quarantined(
                source=record.source, category=err.category,
                detail=str(err)[:200], evidence_ref=record.evidence_ref))
            if on_quarantined:
                on_quarantined(err.category, record.evidence_ref)
            continue
        done.append((record, result))
        if on_ok:
            on_ok()
    return done, quarantined


def _cell_report(members: list[tuple[ExpressionRecord, CanonResult]]
                 ) -> dict[str, Any]:
    classes: dict[str, dict[str, Any]] = {}
    for record, canon in members:
        entry = classes.setdefault(canon.fp_expr, {
            "fp": canon.fp_expr,
            "canonical_sql": canon.canonical_sql,
            "support": 0,
            "support_by_witness": {},
            "authority_max": 0,
            "sources": set(),
            "last_seen": "",
        })
        family = (record.witness
                  or SOURCE_WITNESS.get(record.source, "unknown"))
        entry["support_by_witness"][family] = (
            entry["support_by_witness"].get(family, 0)
            + max(record.support, 1))
        entry["authority_max"] = max(entry["authority_max"],
                                     int(record.authority))
        entry["sources"].add(record.source)
        entry["last_seen"] = max(entry["last_seen"], record.last_seen or "")
    for entry in classes.values():
        # E12/A1 combiner: effective = MAX over ranking families (the
        # catalog was mined from a superset of the same history — a sum
        # double-counts); gold_attested/audit stay visible per-witness
        # but never in the effective number
        ranking = {f: n for f, n in entry["support_by_witness"].items()
                   if f in RANKING_WITNESSES}
        entry["support"] = max(ranking.values(), default=0)
        entry["witness_agreement"] = len(ranking)
    ranked = sorted(
        classes.values(),
        key=lambda e: (-e["authority_max"], -e["support"], e["fp"]))
    for entry in ranked:
        entry["sources"] = sorted(entry["sources"])
    supports = [e["support"] for e in ranked]
    total = sum(supports)
    entropy = 0.0
    if len(supports) > 1 and total:
        entropy = -sum((s / total) * math.log2(s / total)
                       for s in supports if s)
    return {
        "n_expr": len(members),
        "n_classes": len(ranked),
        "conflict": len(ranked) > 1,
        "entropy": round(entropy, 3),
        "classes": ranked,
    }


def _split_tail(cell: dict[str, Any]) -> tuple[dict[str, Any],
                                               list[dict[str, Any]]]:
    keep, tail = [], []
    for entry in cell["classes"]:
        if entry["support"] >= 2 or entry["authority_max"] > int(
                Authority.SNIPPET):
            keep.append(entry)
        else:
            tail.append(entry)
    overflow = 0
    if len(keep) > _MAX_CLASSES_PER_CELL:
        overflow = len(keep) - _MAX_CLASSES_PER_CELL
        tail.extend(keep[_MAX_CLASSES_PER_CELL:])
        keep = keep[:_MAX_CLASSES_PER_CELL]
    out = dict(cell)
    out["classes"] = keep
    out["classes_in_tail"] = len(tail)
    out["classes_overflow"] = overflow
    return out, tail


def build_census(
    canonical: list[tuple[ExpressionRecord, CanonResult]],
    quarantined: list[Quarantined],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """→ (census, tail_rows). Deterministic: same inputs, same bytes."""
    concept_cells: dict[tuple[str, str], list] = defaultdict(list)
    metric_cells: dict[tuple[str, str], list] = defaultdict(list)
    per_source: dict[str, int] = defaultdict(int)

    for record, canon in canonical:
        per_source[record.source] += 1
        table = (record.table_hint
                 or (canon.tables[0] if canon.tables else "?"))
        if record.kind in ("predicate", "case") and record.concept_label:
            concept_cells[(norm_label(record.concept_label), table)].append(
                (record, canon))
        elif record.kind == "metric_expr":
            label = norm_label(record.concept_label or record.metric_ref
                               or "?")
            metric_cells[(label, table)].append((record, canon))

    tail_rows: list[dict[str, Any]] = []

    def _section(cells: dict) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for (label, table), members in sorted(cells.items()):
            cell = _cell_report(members)
            trimmed, tail = _split_tail(cell)
            for t in tail:
                tail_rows.append({"label": label, "table": table, **t})
            out[f"{label}@{table}"] = trimmed
        return out

    concepts = _section(concept_cells)
    metrics = _section(metric_cells)

    quarantine_by_category: dict[str, int] = defaultdict(int)
    for q in quarantined:
        quarantine_by_category[q.category] += 1

    census = {
        "schema": "meridian.census/1",
        "canon_version": CANON_VERSION,
        "meta": {
            "label_normalization": E9_NOTE,
            "support_combiner": (
                "per-class support = MAX over ranking witness families "
                "(never summed across families — the upstream catalog "
                "was mined from a superset of the same query history); "
                "gold_attested and audit_30d are shown per-witness but "
                "excluded from the effective number and from "
                "witness_agreement"),
            "records_canonicalized": len(canonical),
            "records_quarantined": len(quarantined),
            "quarantine_by_category": dict(sorted(
                quarantine_by_category.items())),
            "records_by_source": dict(sorted(per_source.items())),
        },
        "summary": {
            "concept_cells": len(concepts),
            "concept_conflicts": sum(
                1 for c in concepts.values() if c["conflict"]),
            "metric_cells": len(metrics),
            "metric_conflicts": sum(
                1 for c in metrics.values() if c["conflict"]),
        },
        "concepts": concepts,
        "metrics": metrics,
    }
    return census, tail_rows


def write_census(out_dir: Path, census: dict[str, Any],
                 tail_rows: list[dict[str, Any]],
                 quarantined: list[Quarantined]) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    p = out_dir / "census.json"
    p.write_text(json.dumps(census, indent=1, sort_keys=False,
                            ensure_ascii=False) + "\n", encoding="utf-8")
    paths.append(p)
    p = out_dir / "census_tail.jsonl"
    with p.open("w", encoding="utf-8") as f:
        for row in tail_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    paths.append(p)
    p = out_dir / "quarantine.jsonl"
    with p.open("w", encoding="utf-8") as f:
        for q in quarantined:
            f.write(q.model_dump_json() + "\n")
    paths.append(p)
    return paths
