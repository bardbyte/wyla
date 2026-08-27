"""The three metric catalogs, in authority order.

metrics_dmp.json           35 certified KPIs — the meridian line
extended_gmns_semantics    14 pending specs (calculation, approved dims,
                           grain, scope; status Submitted)
measures_catalog.json      6,223 mined patterns with usage support

All three emit metric_expr ExpressionRecords keyed by a stable metric_ref
so P2 can group variants under mgroup identities. Support scaling: mined
records carry their user_count as support (capped later by census math —
depth never outshouts breadth)."""

from __future__ import annotations

import json
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.loaders.records import ExpressionRecord, Quarantined


def _read(path: Path) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_metrics_dmp(path: Path) -> tuple[list[ExpressionRecord],
                                          list[Quarantined]]:
    payload = _read(path)
    rows = (payload.get("metric_catalog", [])
            if isinstance(payload, dict) else payload)
    records, quarantined = [], []
    for row in rows:
        mid = str(row.get("metricCatalogId") or row.get("metricName") or "?")
        ref = f"{Path(path).name}#metric={mid}"
        sql = str(row.get("sqlExpression")
                  or row.get("referencedSqlQuery") or "").strip()
        if not sql:
            quarantined.append(Quarantined(
                source="metrics_dmp", category="missing_field",
                detail=f"certified metric {mid} without SQL",
                evidence_ref=ref))
            continue
        products = row.get("associatedDataProductNames") or []
        records.append(ExpressionRecord(
            raw_sql=sql, kind="metric_expr", source="metrics_dmp",
            authority=Authority.CERTIFIED,
            metric_ref=f"dmp:{mid}",
            concept_label=str(row.get("businessFriendlyMetricName")
                              or row.get("metricName") or ""),
            table_hint=(str(products[0]).lower() if products else None),
            first_seen=str(row.get("createdAt") or ""),
            last_seen=str(row.get("updatedAt") or ""),
            evidence_ref=ref,
            extra={"question_answered": row.get("questionAnswered"),
                   "status": row.get("status"),
                   "author": row.get("author"),
                   "domain": row.get("metricDomain"),
                   "line_of_business": row.get("lineOfBusiness"),
                   "products": [str(p) for p in products]}))
    return records, quarantined


def _looks_like_metric_specs(value) -> bool:
    if not (isinstance(value, list) and value
            and isinstance(value[0], dict)):
        return False
    keys = " ".join(k.lower() for k in value[0])
    return (("metric" in keys or "name" in keys)
            and ("sql" in keys or "calculation" in keys
                 or "expression" in keys))


def load_extended_gmns(path: Path) -> tuple[list[ExpressionRecord],
                                            list[Quarantined]]:
    """The real export wraps its list under a key that is NOT
    "metrics" (run-2 finding: the loader yielded ZERO records and the
    14 pending specs silently never existed). The loader adapts to the
    file: known wrapper keys first, then the first list-of-dicts value
    that LOOKS like metric specs; an unrecognizable shape quarantines
    loudly instead of returning nothing."""
    payload = _read(path)
    if isinstance(payload, list):
        rows = payload
    else:
        rows = []
        for key in ("metrics", "metric_catalog", "semantics",
                    "extended_metrics", "extendedMetrics",
                    "definitions", "metricDefinitions"):
            if _looks_like_metric_specs(payload.get(key)):
                rows = payload[key]
                break
        else:
            rows = next((v for v in payload.values()
                         if _looks_like_metric_specs(v)), [])
    records, quarantined = [], []
    if not rows and payload:
        top = (sorted(payload)[:8] if isinstance(payload, dict)
               else type(payload).__name__)
        quarantined.append(Quarantined(
            source="extended_gmns", category="missing_field",
            detail=f"no metric list found in the file — top-level "
                   f"shape: {top}",
            evidence_ref=Path(path).name))
    for row in rows:
        name = str(row.get("metricName") or row.get("metric_name")
                   or row.get("name") or "?")
        ref = f"{Path(path).name}#metric={name}"
        sql = str(row.get("sqlExpression") or row.get("sql_expression")
                  or row.get("sql") or row.get("expression")
                  or "").strip()
        if not sql:
            quarantined.append(Quarantined(
                source="extended_gmns", category="missing_field",
                detail=f"pending metric {name} without SQL",
                evidence_ref=ref))
            continue
        records.append(ExpressionRecord(
            raw_sql=sql, kind="metric_expr", source="extended_gmns",
            authority=Authority.PENDING,
            metric_ref=f"gmns:{name}", concept_label=name,
            table_hint=str(row.get("table") or row.get("tableName")
                           or row.get("associatedTable")
                           or "gms_transaction").lower(),
            evidence_ref=ref,
            extra={"calculation": row.get("calculation"),
                   "approved_dimensions": row.get("approvedDimensions"),
                   "metric_grain": row.get("metricGrain"),
                   "metric_scope": row.get("metricScope"),
                   "requestor": row.get("requestor"),
                   "status": row.get("status", "Submitted"),
                   # the catalog's own name declares its scope: every
                   # pending spec in this file is a GMNS metric
                   "line_of_business":
                       row.get("lineOfBusiness") or "GMNS"}))
    return records, quarantined


def load_measures_catalog(path: Path) -> tuple[list[ExpressionRecord],
                                               list[Quarantined]]:
    payload = _read(path)
    rows = (payload.get("measures", [])
            if isinstance(payload, dict) else payload)
    records, quarantined = [], []
    for row in rows:
        mid = str(row.get("id") or row.get("name") or "?")
        ref = f"{Path(path).name}#measure={mid}"
        sql = str(row.get("expression") or "").strip()
        table = str(row.get("table") or "").strip().lower()
        if not sql or not table:
            quarantined.append(Quarantined(
                source="measures_catalog", category="missing_field",
                detail=f"measure {mid}: missing "
                       f"{'expression' if not sql else 'table'}",
                evidence_ref=ref))
            continue
        records.append(ExpressionRecord(
            raw_sql=sql, kind="metric_expr", source="measures_catalog",
            authority=Authority.MINED,
            metric_ref=f"mined:{mid}",
            concept_label=str(row.get("name") or ""),
            table_hint=table,
            support=max(int(row.get("user_count") or 1), 1),
            first_seen=str(row.get("first_seen") or ""),
            last_seen=str(row.get("last_seen") or ""),
            evidence_ref=ref,
            extra={"confidence": row.get("confidence"),
                   "execution_count": row.get("execution_count"),
                   "group_by_patterns": row.get("group_by_patterns"),
                   "common_filters": row.get("common_filters"),
                   "joined_tables": row.get("joined_tables"),
                   "business_unit": row.get("business_unit"),
                   "data_category": row.get("data_category")}))
    return records, quarantined
