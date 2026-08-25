"""DIFF_vs_prev.md — the promotion gate's reading material.

Semantic deltas first (metric expression/status changes, binding flips),
then census deltas (including E1 structural drift — "catalogs and
warehouse drifted closer/further"), budgeter-drop deltas (E10/risk 9),
then changed-card inventory. Capped; a human reads this before CURRENT
moves."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _jsonl(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                out[str(row.get("id") or row.get("fp") or "")] = row
    return out


def _json(path: Path) -> dict[str, Any]:
    return (json.loads(path.read_text(encoding="utf-8"))
            if path.exists() else {})


def build_diff(new_dir: Path, prev_dir: Path | None) -> str:
    lines = ["# DIFF vs previous build", ""]
    if prev_dir is None or not Path(prev_dir).exists():
        lines.append("no previous build — this is the first promotion "
                     "candidate")
        return "\n".join(lines)
    new_manifest = _json(new_dir / "manifest.json")
    old_manifest = _json(prev_dir / "manifest.json")
    lines.append(f"- previous: {old_manifest.get('build_id', '?')} → "
                 f"new: {new_manifest.get('build_id', '?')}")

    # semantic first: metric status/expression changes
    old_metrics = _jsonl(prev_dir / "indexes" / "metrics.jsonl")
    new_metrics = _jsonl(new_dir / "indexes" / "metrics.jsonl")
    changed = []
    for mid, row in new_metrics.items():
        old = old_metrics.get(mid)
        if old is None:
            changed.append(f"+ metric {mid} ({row.get('label')}) "
                           f"[{row.get('status')}]")
        elif (old.get("status"), old.get("canonical_sql")) != (
                row.get("status"), row.get("canonical_sql")):
            what = []
            if old.get("status") != row.get("status"):
                what.append(f"status {old.get('status')} → "
                            f"{row.get('status')}")
            if old.get("canonical_sql") != row.get("canonical_sql"):
                what.append("EXPRESSION CHANGED")
            changed.append(f"~ metric {mid}: {'; '.join(what)}")
    for mid in old_metrics.keys() - new_metrics.keys():
        changed.append(f"- metric {mid} removed")
    lines.append("")
    lines.append("## semantic changes (read these first)")
    lines += [f"  {c}" for c in changed[:40]] or ["  none"]

    # census deltas incl. structural drift
    old_census = _json(prev_dir / "census.json")
    new_census = _json(new_dir / "census.json")
    lines.append("")
    lines.append("## census deltas")
    for key in ("concept_conflicts", "metric_conflicts"):
        old_value = (old_census.get("summary") or {}).get(key, 0)
        new_value = (new_census.get("summary") or {}).get(key, 0)
        if old_value != new_value:
            lines.append(f"  {key}: {old_value} → {new_value}")
    old_structural = ((old_census.get("structural") or {})
                      .get("totals") or {})
    new_structural = ((new_census.get("structural") or {})
                      .get("totals") or {})
    drift = {k: (old_structural.get(k, 0), new_structural.get(k, 0))
             for k in sorted(set(old_structural) | set(new_structural))
             if old_structural.get(k, 0) != new_structural.get(k, 0)}
    if drift:
        direction = ("closer" if sum(b - a for a, b in drift.values()) < 0
                     else "further apart")
        lines.append(f"  structural (E1): catalogs and warehouse drifted "
                     f"{direction}: "
                     + ", ".join(f"{k} {a}→{b}"
                                 for k, (a, b) in drift.items()))
    else:
        lines.append("  structural (E1): no drift")

    # budgeter drops
    old_drops = old_manifest.get("budget", {})
    new_drops = new_manifest.get("budget", {})
    if old_drops != new_drops:
        lines.append("")
        lines.append(f"## budgeter deltas: {old_drops} → {new_drops}")

    # changed cards inventory
    lines.append("")
    lines.append("## changed cards")
    old_cards = {p.relative_to(prev_dir).as_posix():
                 p.read_bytes() for p in (prev_dir / "cards").rglob("*.md")}
    changed_cards = []
    for p in sorted((new_dir / "cards").rglob("*.md")):
        rel = p.relative_to(new_dir).as_posix()
        if old_cards.get(rel) != p.read_bytes():
            changed_cards.append(rel)
    lines += [f"  {c}" for c in changed_cards[:60]] or ["  none"]
    return "\n".join(lines)
