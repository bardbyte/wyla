"""Skill packs — 10 analytical instruction sets, contract expressions out.

Layout per pack (the documented shape): skill.yaml (identity, routing,
tables), metric_contracts.yaml (exact SQL expressions — what we canon),
knowledge.md, sample_codes.sql, qa_checks.yaml, chart_contract.yaml,
data_specs.md. P0 extracts the machine-readable metric contracts as
skill-contract-tier ExpressionRecords; the prose knowledge stays a P2
doc-evidence concern. sample_codes.sql is SQLite-dialect demo material —
deliberately NOT canonicalized as BigQuery (dialect quarantine trap)."""

from __future__ import annotations

from pathlib import Path

import yaml

from sahs.canon.authority import Authority
from sahs.loaders.records import ExpressionRecord, Quarantined

SOURCE = "skill_contract"


def _packs(root: Path) -> list[Path]:
    """Packs are dirs containing skill.yaml — flat or one level nested."""
    return sorted({p.parent for p in Path(root).glob("**/skill.yaml")})


def load_skill_contracts(root: Path) -> tuple[list[ExpressionRecord],
                                              list[Quarantined]]:
    records, quarantined = [], []
    for pack in _packs(root):
        pack_id = pack.name
        manifest = {}
        try:
            manifest = yaml.safe_load(
                (pack / "skill.yaml").read_text(encoding="utf-8")) or {}
        except Exception as e:
            quarantined.append(Quarantined(
                source=SOURCE, category="missing_field",
                detail=f"{pack_id}: unreadable skill.yaml ({e})",
                evidence_ref=str(pack / "skill.yaml")))
            continue
        tables = [str(t).lower() for t in (manifest.get("tables") or [])]
        contracts_path = pack / "metric_contracts.yaml"
        if not contracts_path.exists():
            continue                     # knowledge-only pack: nothing to canon
        try:
            contracts = yaml.safe_load(
                contracts_path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            quarantined.append(Quarantined(
                source=SOURCE, category="missing_field",
                detail=f"{pack_id}: unreadable metric_contracts.yaml ({e})",
                evidence_ref=str(contracts_path)))
            continue
        metrics = contracts.get("metrics") or contracts.get("contracts") or []
        if isinstance(metrics, dict):
            metrics = [{"name": k, **(v or {})} for k, v in metrics.items()]
        for m in metrics:
            name = str(m.get("name") or "?")
            ref = f"{contracts_path.as_posix()}#metric={name}"
            sql = str(m.get("expression") or m.get("sql")
                      or m.get("formula") or "").strip()
            if not sql:
                quarantined.append(Quarantined(
                    source=SOURCE, category="missing_field",
                    detail=f"{pack_id}/{name}: contract without expression",
                    evidence_ref=ref))
                continue
            records.append(ExpressionRecord(
                raw_sql=sql, kind="metric_expr", source=SOURCE,
                authority=Authority.SKILL_CONTRACT,
                metric_ref=f"skill:{pack_id}:{name}",
                concept_label=str(m.get("business_name") or name),
                table_hint=(str(m.get("table") or "").lower()
                            or (tables[0] if tables else None)),
                evidence_ref=ref,
                extra={"pack": pack_id, "grain": m.get("grain"),
                       "numerator": m.get("numerator"),
                       "denominator": m.get("denominator"),
                       "do_not_average_ratios":
                           m.get("do_not_average_ratios")}))
    return records, quarantined
