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

    # Find skill.yaml at ANY depth — real libraries group packages under a
    # domain folder (skills/<DomainGroup>/<SkillName>/skill.yaml), while the
    # test fixtures are flat (skills/<SkillName>/skill.yaml). Both work.
    manifests = sorted({
        p for pat in ("**/skill.yaml", "**/skill.yml")
        for p in skills_dir.glob(pat)
    })
    packages: list[Path] = []
    seen: set[Path] = set()
    for manifest_path in manifests:
        pkg = manifest_path.parent
        if pkg not in seen:
            seen.add(pkg)
            packages.append(pkg)
    if not packages:
        return LoadResult(
            status="skipped", source="skills", table_id=str(skills_dir),
            warnings=[f"no skill.yaml found at any depth under {skills_dir}"],
        )

    written: list[Path] = []
    outcomes: dict[str, str] = {}
    warnings: list[str] = []
    target_dir = Path(out_dir) / "skills"

    for pkg in packages:
        # the folder BETWEEN skills_dir and the package is the domain group
        # (NewAccountsSkills / PortfolioAnalyticsSkills); "" when flat
        domain_group = pkg.parent.name if pkg.parent != skills_dir else ""
        try:
            blob = _parse_skill_package(pkg, domain_group=domain_group)
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


def _parse_skill_package(pkg_dir: Path, domain_group: str = "") -> dict[str, Any]:
    """One package directory → one canonical skill blob.

    ``domain_group`` is the enclosing folder name (e.g. NewAccountsSkills)
    — the authoritative domain when skill.yaml doesn't declare one.
    """
    manifest_path = next(
        p for p in (pkg_dir / "skill.yaml", pkg_dir / "skill.yml") if p.exists()
    )
    manifest = _read_yaml(manifest_path) or {}
    if not isinstance(manifest, dict):
        raise ValueError(f"{manifest_path.name} is not a mapping")

    skill_id = str(
        manifest.get("id") or manifest.get("skill_id") or pkg_dir.name
    ).strip()
    domain = str(
        manifest.get("domain")
        or _domain_from_group(domain_group)
        or _infer_domain(skill_id)
    ).strip()

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
    data_specs = _parse_data_specs(pkg_dir / "data_specs.md", tables)
    chart_contracts = _parse_chart_contract(pkg_dir / "chart_contract.yaml")

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
        # data_specs.md — valid values + segmentation bands → the graph's
        # highest-trust FilterValue / CodeMapping facts
        "valid_values": data_specs["valid_values"],
        "bands": data_specs["bands"],
        "data_specs_text": data_specs["text"],
        # chart_contract.yaml — per-KPI visualization rules for the viz layer
        "chart_contracts": chart_contracts,
        "knowledge_excerpt": knowledge_text[:_KNOWLEDGE_EXCERPT_CHARS],
        "knowledge_full": knowledge_text,
        "files": sorted(p.name for p in pkg_dir.iterdir() if p.is_file()),
        "package_dir": str(pkg_dir),
    }


def _read_yaml(path: Path) -> Any:
    if yaml is None:
        raise RuntimeError("pyyaml is required for the skills loader")
    return yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))


# ─── data_specs.md — valid values + segmentation bands ───────


def _parse_markdown_tables(text: str) -> list[dict[str, Any]]:
    """Every GitHub-flavored markdown table → {headers, rows[]}."""
    tables: list[dict[str, Any]] = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.count("|") >= 2 and i + 1 < len(lines) \
                and set(lines[i + 1].replace("|", "").strip()) <= set("-: "):
            headers = [c.strip() for c in line.strip().strip("|").split("|")]
            rows = []
            j = i + 2
            while j < len(lines) and lines[j].count("|") >= 2:
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))
                j += 1
            tables.append({"headers": headers, "rows": rows})
            i = j
        else:
            i += 1
    return tables


def _split_values(cell: str) -> list[str]:
    """A cell like `A, D, P` or `'A' | 'D'` → discrete values."""
    parts = re.split(r"[;,/|]", cell)
    out = []
    for p in parts:
        v = p.strip().strip("`'\"").strip()
        if v and v.lower() not in ("", "n/a", "null", "…", "..."):
            out.append(v)
    return out


def _parse_data_specs(path: Path, tables: list[str]) -> dict[str, Any]:
    """Extract valid-value sets and segmentation bands from data_specs.md.

    Format-tolerant: reads every markdown table and classifies columns by
    header keywords. Nothing forced — a table that doesn't match a known
    shape is skipped, but the full text is always preserved so no signal
    is lost.

        valid_values: [{column, values[], table?}]
        bands:        [{column, raw, label, table?}]   (code-mapping shaped)
    """
    if not path.exists():
        return {"valid_values": [], "bands": [], "text": ""}
    text = path.read_text(encoding="utf-8", errors="replace")
    default_table = tables[0] if tables else ""
    valid_values: list[dict[str, Any]] = []
    bands: list[dict[str, Any]] = []

    for table in _parse_markdown_tables(text):
        headers = table["headers"]
        headers_lc = [h.lower() for h in headers]

        def pick(keys: tuple[str, ...], exclude: set[str]) -> str | None:
            for idx, h in enumerate(headers_lc):
                if headers[idx] in exclude:
                    continue
                if any(k in h for k in keys):
                    return headers[idx]
            return None

        # roles assigned by priority with mutual exclusion, so a header is
        # never claimed twice (label vs raw vs column-name)
        values_key = pick(("valid value", "allowed value", "values",
                           "domain", "enum"), set())
        label_key = pick(("label", "meaning", "description", "name"),
                         {values_key} if values_key else set())
        raw_key = pick(("range", "raw", "code", "value", "definition",
                       "condition"), {label_key, values_key})
        col_key = pick(("column", "field", "attribute"),
                       {label_key, raw_key, values_key})
        if not col_key:  # band/segment header names the column when unlabeled
            col_key = pick(("band", "segment", "bucket", "tier", "dimension"),
                           {label_key, raw_key, values_key})

        for row in table["rows"]:
            column = ((row.get(col_key) if col_key else "") or "").strip().strip("`")
            if values_key and row.get(values_key) and column:
                vals = _split_values(row[values_key])
                if vals:
                    valid_values.append({
                        "column": column, "values": vals,
                        "table": default_table})
            if label_key and raw_key and column:
                label = (row.get(label_key) or "").strip()
                raw = (row.get(raw_key) or "").strip()
                if label and raw:
                    bands.append({
                        "column": column, "raw": raw, "label": label,
                        "table": default_table})
    return {"valid_values": valid_values, "bands": bands, "text": text}


def _parse_chart_contract(path: Path) -> dict[str, Any]:
    """chart_contract.yaml → per-KPI viz rules (kept as-is for the viz layer)."""
    if not path.exists():
        return {}
    blob = _read_yaml(path) or {}
    return blob if isinstance(blob, dict) else {"raw": blob}


def _domain_from_group(group_name: str) -> str:
    """Map a domain-group folder (NewAccountsSkills / PortfolioAnalyticsSkills)
    to a clean domain slug. Empty when there's no group or no match."""
    slug = group_name.lower().replace("skills", "").replace("_", "").strip()
    if not slug:
        return ""
    if "newaccount" in slug:
        return "new_accounts"
    if "portfolio" in slug:
        return "portfolio_analytics"
    if "acquisition" in slug:
        return "new_accounts"
    return slug  # unknown group → use it verbatim (better than a guess)


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
