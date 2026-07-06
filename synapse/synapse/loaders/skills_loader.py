"""Skills-library loader — the L3 semantic witness.

Reads a directory of curated analytics skill packages (the layout the
credit-risk skills library uses) and writes one canonical JSON per skill
that the graph builder ingests:

    <skills_dir>/
      SBS_RollRates/
        skill.yaml              # id, description, parameters, guardrails?
        knowledge.md            # business definitions + "never do this" rules
        sample_codes.sql        # reference SQL for the underlying tables
        data_specs.md           # column definitions, valid values (optional)
        metric_contracts.yaml   # executable numerator/denominator rules (optional)
        qa_checks.yaml          # validation rules generated SQL must pass (optional)
        chart_contract.yaml     # visualization rules (optional, stored as file ref)

    out_dir/skills/<skill_id>.json

Two things make this source special and the loader enforces both:

  1. Guardrails become DATA, not doc strings. An explicit `guardrails:`
     list in skill.yaml is taken verbatim (human-authored). Additionally,
     imperative-negative lines in knowledge.md ("never …", "do not …",
     "must not …") are mined as guardrail candidates and marked
     `mined_from_knowledge: true` so the graph can distinguish authored
     rules from mined ones.

  2. Nothing that looks like row-level data is ingested. The loader reads
     only the curated text/YAML files; sample SQL is scanned for table
     names, never executed, and its literals are not extracted.

Pure function, no network. ADK FunctionTool-wrappable, same LoadResult
contract as the other loaders.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from synapse.loaders.types import LoadResult

try:  # optional — used only to harvest table names from sample SQL
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


# Imperative-negative sentence openers that signal a guardrail in prose.
_GUARDRAIL_LINE = re.compile(
    r"^\s*(?:[-*•]\s*)?(?:\*\*)?"
    r"(never|do not|don't|must not|always)\b",
    re.IGNORECASE,
)

# Table-name harvest from reference SQL: FROM/JOIN <ident chain>.
_SQL_TABLE = re.compile(
    r"\b(?:from|join)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_][\w$]*){0,2})",
    re.IGNORECASE,
)

_KNOWLEDGE_EXCERPT_CHARS = 2_000


def load_skills_library(
    skills_dir: Path,
    *,
    out_dir: Path,
    dry_run: bool = False,
) -> LoadResult:
    """Scan every package under ``skills_dir`` and write canonical JSON.

    Returns one LoadResult for the whole library (table_id is the library
    path); per-skill outcomes are in ``metadata["skills"]``.
    """
    started = time.monotonic()
    skills_dir = Path(skills_dir).expanduser()
    if not skills_dir.is_dir():
        return LoadResult(
            status="error", source="skills", table_id=str(skills_dir),
            error=f"skills dir not found: {skills_dir}",
        )

    packages = sorted(
        p.parent for p in skills_dir.glob("*/skill.yaml")
    ) or sorted(p.parent for p in skills_dir.glob("*/skill.yml"))
    if not packages:
        return LoadResult(
            status="skipped", source="skills", table_id=str(skills_dir),
            warnings=[f"no */skill.yaml packages under {skills_dir}"],
        )

    written: list[Path] = []
    outcomes: dict[str, str] = {}
    warnings: list[str] = []
    target_dir = Path(out_dir) / "skills"

    for pkg in packages:
        try:
            blob = _parse_skill_package(pkg)
        except Exception as exc:  # tolerate one bad package, keep loading
            outcomes[pkg.name] = f"error: {exc}"
            warnings.append(f"{pkg.name}: {exc}")
            continue
        outcomes[pkg.name] = "ok"
        if dry_run:
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / f"{blob['skill_id']}.json"
        out_path.write_text(
            json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8",
        )
        written.append(out_path)

    n_ok = sum(1 for v in outcomes.values() if v == "ok")
    status = "ok" if n_ok == len(packages) else ("partial" if n_ok else "error")
    return LoadResult(
        status=status,
        source="skills",
        table_id=str(skills_dir),
        artifacts_written=written,
        records_count=n_ok,
        warnings=warnings,
        latency_ms=int((time.monotonic() - started) * 1000),
        metadata={"skills": outcomes, "dry_run": dry_run},
    )


def _parse_skill_package(pkg_dir: Path) -> dict[str, Any]:
    """One package directory → one canonical skill blob."""
    manifest_path = next(
        p for p in (pkg_dir / "skill.yaml", pkg_dir / "skill.yml") if p.exists()
    )
    manifest = _read_yaml(manifest_path) or {}
    if not isinstance(manifest, dict):
        raise ValueError(f"{manifest_path.name} is not a mapping")

    skill_id = str(
        manifest.get("id") or manifest.get("skill_id") or pkg_dir.name
    ).strip()
    domain = str(manifest.get("domain") or _infer_domain(skill_id)).strip()

    knowledge_path = pkg_dir / "knowledge.md"
    knowledge_text = (
        knowledge_path.read_text(encoding="utf-8", errors="replace")
        if knowledge_path.exists() else ""
    )

    guardrails: list[dict[str, Any]] = []
    for raw in manifest.get("guardrails") or []:
        guardrails.append(_normalize_guardrail(raw, skill_id, mined=False))
    for line in _mine_guardrail_lines(knowledge_text):
        guardrails.append(_normalize_guardrail(line, skill_id, mined=True))
    guardrails = _dedupe_guardrails(guardrails)

    tables: list[str] = [str(t) for t in (manifest.get("tables") or [])]
    sql_path = pkg_dir / "sample_codes.sql"
    if sql_path.exists():
        sql_text = sql_path.read_text(encoding="utf-8", errors="replace")
        for match in _SQL_TABLE.finditer(sql_text):
            name = match.group(1)
            if name.lower() not in {t.lower() for t in tables}:
                tables.append(name)

    metrics = _parse_metric_contracts(pkg_dir / "metric_contracts.yaml")
    qa_checks = _parse_qa_checks(pkg_dir / "qa_checks.yaml")

    return {
        "skill_id": skill_id,
        "domain": domain,
        "description": str(manifest.get("description") or "").strip(),
        "parameters": manifest.get("parameters") or [],
        "outputs": manifest.get("outputs") or [],
        "tables_used": tables,
        "metrics": metrics,
        "guardrails": guardrails,
        "qa_checks": qa_checks,
        "knowledge_excerpt": knowledge_text[:_KNOWLEDGE_EXCERPT_CHARS],
        "files": sorted(p.name for p in pkg_dir.iterdir() if p.is_file()),
        "package_dir": str(pkg_dir),
    }


def _read_yaml(path: Path) -> Any:
    if yaml is None:
        raise RuntimeError("pyyaml is required for the skills loader")
    return yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))


def _infer_domain(skill_id: str) -> str:
    lowered = skill_id.lower()
    if "newaccount" in lowered.replace("_", ""):
        return "new_accounts"
    if lowered.startswith(("cps_", "sbs_")):
        return "portfolio_analytics"
    return "general"


def _mine_guardrail_lines(knowledge_text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in knowledge_text.splitlines():
        if _GUARDRAIL_LINE.match(raw_line):
            cleaned = re.sub(r"^[\s\-*•]+", "", raw_line).strip().strip("*").strip()
            if len(cleaned) > 12:  # skip bare "Never." fragments
                lines.append(cleaned)
    return lines


def _normalize_guardrail(
    raw: Any, skill_id: str, *, mined: bool,
) -> dict[str, Any]:
    if isinstance(raw, str):
        entry: dict[str, Any] = {"rule": raw}
    elif isinstance(raw, dict):
        entry = dict(raw)
    else:
        entry = {"rule": str(raw)}
    rule = str(entry.get("rule") or "").strip()
    return {
        "rule": rule,
        "category": str(entry.get("category") or _infer_category(rule)),
        "applies_to": [str(a) for a in (entry.get("applies_to") or [])],
        "severity": str(entry.get("severity") or "error"),
        "machine_checkable": bool(entry.get("machine_checkable", False)),
        "skill_id": skill_id,
        "mined_from_knowledge": mined,
    }


def _infer_category(rule: str) -> str:
    lowered = rule.lower()
    if any(k in lowered for k in ("encrypt", "pii", "expose", "mask")):
        return "privacy"
    if any(k in lowered for k in ("lag(", "lag ", "grain", "month", "cohort")):
        return "grain"
    if any(k in lowered for k in ("rate", "numerator", "denominator", "average")):
        return "metric_math"
    if any(k in lowered for k in ("select", "count", "distinct", "join", "sql")):
        return "sql_generation"
    return "other"


def _dedupe_guardrails(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Authored rules win over mined duplicates of the same sentence."""
    seen: dict[str, dict[str, Any]] = {}
    for entry in entries:
        key = re.sub(r"\W+", " ", entry["rule"].lower()).strip()
        if not key:
            continue
        if key not in seen or (
            seen[key]["mined_from_knowledge"] and not entry["mined_from_knowledge"]
        ):
            seen[key] = entry
    return list(seen.values())


def _parse_metric_contracts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    blob = _read_yaml(path) or {}
    raw_metrics = blob.get("metrics") if isinstance(blob, dict) else blob
    metrics: list[dict[str, Any]] = []
    for entry in raw_metrics or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or entry.get("id") or "").strip()
        if not name:
            continue
        formula = str(entry.get("formula") or "").strip()
        if not formula and entry.get("numerator") and entry.get("denominator"):
            formula = f"({entry['numerator']}) / ({entry['denominator']})"
        metrics.append({
            "name": name,
            "business_name": str(entry.get("business_name") or name),
            "formula": formula,
            "grain": str(entry.get("grain") or ""),
            "table": str(entry.get("table") or ""),
            "synonyms": [str(s) for s in (entry.get("synonyms") or [])],
        })
    return metrics


def _parse_qa_checks(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    blob = _read_yaml(path) or {}
    raw_checks = blob.get("checks") if isinstance(blob, dict) else blob
    checks: list[dict[str, Any]] = []
    for entry in raw_checks or []:
        if isinstance(entry, str):
            checks.append({"rule_kind": "custom_sql", "threshold": entry,
                           "severity": "warning", "target_column": None})
        elif isinstance(entry, dict):
            checks.append({
                "rule_kind": str(entry.get("kind") or entry.get("rule_kind")
                                 or "custom_sql"),
                "threshold": str(entry.get("threshold") or entry.get("rule")
                                 or ""),
                "severity": str(entry.get("severity") or "warning"),
                "target_column": entry.get("column"),
            })
    return checks
