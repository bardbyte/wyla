"""Metric catalog loader — the curated business-metric registry.

Columns from the Dataset Metric Summary screenshot:
    Metric Identification (technical + business names)
    Data Lineage           (primary + supporting data products)
    Business Definition    (plain-language)
    Calculation Logic      (SQL / aggregation / window / business rule)
    Associated Domain      (Customer / Finance / Risk / Loyalty / ...)
    Metric Grain           (row / account / aggregated / partition)
    Business Synonyms      (comma-separated alt names)
    Technical References   (SQL files / query IDs)

This is the highest-trust metric source short of human_approval. The
loader is lenient about column naming; required minimum is a
technical_name OR business_name.
"""

from __future__ import annotations

import csv
from pathlib import Path

from synapse.registry.schemas import MetricCatalogEntry


_TECH_NAME_KEYS = (
    "technical_name", "metric_name", "metric", "metric_id",
    "technical metric name", "name",
)
_BIZ_NAME_KEYS = (
    "business_name", "business-friendly metric name",
    "business friendly metric name", "business friendly name",
    "display_name", "label",
)
_BIZ_DEF_KEYS = (
    "business_definition", "definition", "description",
    "plain-language description", "purpose",
)
_CALC_LOGIC_KEYS = (
    "calculation_logic", "calculation", "logic",
    "sql expression", "formula", "calculation_sql",
)
_PRIMARY_DP_KEYS = (
    "primary_data_product", "data_product", "data product",
    "primary data product", "source", "primary_source",
)
_SUPPORTING_DP_KEYS = (
    "supporting_data_products", "supporting data products",
    "supporting_sources", "secondary_sources",
)
_DOMAIN_KEYS = (
    "associated_domain", "domain", "business_domain",
    "associated domain", "business area",
)
_GRAIN_KEYS = (
    "metric_grain", "grain", "level", "granularity",
)
_SYNONYM_KEYS = (
    "business_synonyms", "synonyms", "aliases", "alternative names",
    "alt_names", "alternative business terms",
)
_TECH_REF_KEYS = (
    "technical_references", "references", "implementation_references",
    "associated sql queries", "sql_references", "code_references",
)


def _pick(row: dict, candidates: tuple[str, ...]) -> str | None:
    lower = {(k or "").strip().lower(): v for k, v in row.items()}
    for c in candidates:
        v = lower.get(c)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _split_csv(s: str | None) -> list[str]:
    """Split a comma/semicolon/pipe-separated string into a clean list."""
    if not s:
        return []
    for sep in (";", "|", ","):
        if sep in s:
            return [x.strip() for x in s.split(sep) if x.strip()]
    return [s.strip()] if s.strip() else []


def load_metric_catalog(path: Path) -> list[MetricCatalogEntry]:
    """Load the metric catalog CSV. Skips rows with no name."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Metric catalog CSV not found: {p}")

    out: list[MetricCatalogEntry] = []
    with p.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tech = _pick(row, _TECH_NAME_KEYS)
            biz = _pick(row, _BIZ_NAME_KEYS)
            if not (tech or biz):
                continue
            # Prefer technical name; fall back to business
            technical = tech or biz
            assert technical is not None
            out.append(MetricCatalogEntry(
                technical_name=technical,
                business_name=biz,
                business_definition=_pick(row, _BIZ_DEF_KEYS),
                calculation_logic=_pick(row, _CALC_LOGIC_KEYS),
                primary_data_product=_pick(row, _PRIMARY_DP_KEYS),
                supporting_data_products=_split_csv(
                    _pick(row, _SUPPORTING_DP_KEYS),
                ),
                associated_domain=_pick(row, _DOMAIN_KEYS),
                metric_grain=_pick(row, _GRAIN_KEYS),
                business_synonyms=_split_csv(_pick(row, _SYNONYM_KEYS)),
                technical_references=_split_csv(_pick(row, _TECH_REF_KEYS)),
                raw_row=dict(row),
            ))
    return out


def index_by_domain(
    entries: list[MetricCatalogEntry],
) -> dict[str, list[MetricCatalogEntry]]:
    """Group metrics by associated_domain (case-insensitive)."""
    out: dict[str, list[MetricCatalogEntry]] = {}
    for e in entries:
        key = (e.associated_domain or "_unknown").strip().lower()
        out.setdefault(key, []).append(e)
    return out
