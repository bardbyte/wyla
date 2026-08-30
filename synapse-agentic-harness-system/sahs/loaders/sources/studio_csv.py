"""Studio catalog export — the raw CSV, consumed whole and in-silo.

``studio_results_*_cte_or_subqueries.csv`` is a filtered re-export of
the certified metric catalog (same ``metric_catalog_id`` UUIDs as
``metrics_dmp.json``) restricted to metrics whose SQL carries CTEs or
subqueries. Custody rule: Meridian reads the RAW export and derives
everything itself — never an upstream tool's normalization pass. What
each column becomes:

CONSUMED — metric evidence (fuses via ``metric_ref = dmp:<id>``: same
id + same SQL corroborates the canonical metric as a new ``studio``
witness family; same id + different SQL lands as a flagged second
class; a novel id becomes a MINED candidate):
- ``sql_expression``            the canonical-identity SQL (our canon)
- ``referenced_sql_query``      rides WHOLE as a doc node + the join
                                miner's input (``metric_sql_query``
                                only as fallback when it is empty)
- ``base_tables``/``associated_tables``/``associated_data_product_names``
                                the attribution chain (base wins — the
                                SQL's own tables — then associated,
                                then product names via the alias
                                sidecar); ``associated − base`` is OUR
                                computed lineage-mismatch warning
- ``grain``                     → ``grain_observed`` prop. DELIBERATELY
                                outside the identity fingerprint: the
                                certified catalog has NO grain field,
                                so identity-bearing grain would fork
                                every fusion into duplicate nodes
- ``filter_reason``             → ``query_shape`` (CTE/SUBQUERY tags)
- ``dataowners``                → ``data_owners`` (stewardship contacts)
- ``author``                    is the catalog's authorId → author_id
- names/description/question/domain/lob/status/timestamps as props

DELIBERATELY IGNORED (named so the choice is visible):
- ``metric_sql_query``          upstream normalization — Meridian
                                canonicalizes SQL itself (kept only as
                                the referenced-query fallback)
- ``metric_fingerprint``        upstream sha — we mint our own
- ``query_semantics_id``        upstream key, no consumer
- ``metric_raw_id``/``metric_intent``/``calculation_logic``/
  ``data_product_relationship``/``confidence_*``/``reviewers``
                                empty in every observed export

Join knowledge is MINED IN-SILO from the kept full SQL (sqlglot,
CTE-aware): every ON equality between two resolvable tables becomes a
``joins_via`` edge that is **scoped_only by design** — the equality was
observed between TRANSFORMED CTEs, evidence a relationship EXISTS,
never that the raw tables join safely. Same-table (period-over-period)
patterns are counted, never edges; unresolvable pieces are counted out,
never guessed.
"""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import table_id
from sahs.graph.quads import GraphDir, Prov, Quad
from sahs.loaders.records import ExpressionRecord, Quarantined

SOURCE = "studio_queries"              # → witness "studio"

# real exports carry multi-KB SQL cells — the csv module's 128KB default
# field cap is a crash, not a policy (same raise as the archive reader)
_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_limit)
        break
    except OverflowError:               # pragma: no cover — platform dep
        _limit //= 10


def _pg_array(cell) -> list[str]:
    """``{a,b}`` / ``{"quoted, item"}`` / ``{}`` Postgres-style array
    cells (JSON arrays tolerated) → list of clean strings."""
    s = str(cell or "").strip()
    if not s or s in ("{}", "[]"):
        return []
    if s.startswith("["):
        try:
            return [str(x).strip() for x in json.loads(s) if str(x).strip()]
        except (ValueError, TypeError):
            pass
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    s = s.replace('\\"', '"')
    row = next(csv.reader(io.StringIO(s), skipinitialspace=True), [])
    return [item.strip().strip('"').strip() for item in row
            if item.strip().strip('"').strip()]


def _short(name: str) -> str:
    return str(name or "").strip().split(".")[-1].lower()


def _shape(filter_reason: str) -> list[str]:
    """``referenced_sql_query:CTE;metric_sql_query:SUBQUERY;…`` →
    the distinct shape tags, sorted."""
    tags = set()
    for part in str(filter_reason or "").split(";"):
        part = part.strip()
        if part:
            tags.add(part.split(":")[-1].strip().upper())
    return sorted(t for t in tags if t)


def load_studio_csv(path: Path) -> tuple[list[ExpressionRecord],
                                         list[Quarantined]]:
    records: list[ExpressionRecord] = []
    quarantined: list[Quarantined] = []
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        for n, row in enumerate(csv.DictReader(f), 2):   # header = line 1
            ref = f"{Path(path).name}:{n}"
            mid = str(row.get("metric_catalog_id") or "").strip()
            expr = str(row.get("sql_expression") or "").strip()
            referenced = str(row.get("referenced_sql_query") or "").strip()
            normalized = str(row.get("metric_sql_query") or "").strip()
            sql = expr or referenced or normalized
            if not mid or not sql:
                quarantined.append(Quarantined(
                    source=SOURCE, category="missing_field",
                    detail="row without "
                           + ("metric_catalog_id" if not mid else "SQL"),
                    evidence_ref=ref))
                continue
            base = [_short(t) for t in _pg_array(row.get("base_tables"))]
            associated = [_short(t)
                          for t in _pg_array(row.get("associated_tables"))]
            products = _pg_array(row.get("associated_data_product_names"))
            hint = (base[0] if base
                    else associated[0] if associated
                    else products[0] if products else "")
            records.append(ExpressionRecord(
                raw_sql=sql, kind="metric_expr", source=SOURCE,
                authority=Authority.MINED,
                # the SAME ref namespace the dmp loader mints — a
                # matching catalog id FUSES onto the canonical metric
                # ("enrich, not duplicate"); a novel id keys a candidate
                # group that fuses the day the catalog adopts it
                metric_ref=f"dmp:{mid}",
                concept_label=str(
                    row.get("business_friendly_metric_name")
                    or row.get("metric_name") or "").strip(),
                table_hint=hint or None,
                first_seen=str(row.get("created_at") or "").strip(),
                last_seen=str(row.get("updated_at") or "").strip(),
                evidence_ref=ref,
                extra={
                    "question_answered":
                        str(row.get("question_answered") or "").strip(),
                    "description":
                        str(row.get("metric_description") or "").strip(),
                    "domain": str(row.get("metric_domain") or "").strip(),
                    "line_of_business":
                        str(row.get("line_of_business") or "").strip(),
                    "author_id": str(row.get("author") or "").strip(),
                    "status": str(row.get("status") or "").strip(),
                    "grain_observed": str(row.get("grain") or "").strip(),
                    "query_shape": _shape(row.get("filter_reason")),
                    "data_owners": _pg_array(row.get("dataowners")),
                    "products": products,
                    "referenced_query": referenced or normalized,
                    # OUR derivation from the raw columns — computable
                    # only when the export states what the SQL reads
                    "tables_associated_not_referenced":
                        sorted(t for t in associated if t not in base)
                        if base else [],
                }))
    return records, quarantined


# ── in-silo join mining (sqlglot, CTE-aware, all depths) ──

def _cte_bases(tree) -> dict[str, str]:
    """CTE alias → the ONE real short table it selects from. A CTE
    wrapping several tables (or an unresolvable chain) maps to nothing —
    joins through it are counted out, never guessed."""
    import sqlglot.expressions as exp
    defs = {}
    for cte in tree.find_all(exp.CTE):
        defs[cte.alias_or_name.lower()] = {
            t.name.lower() for t in cte.this.find_all(exp.Table)}
    resolved: dict[str, str] = {}
    for _ in range(len(defs) + 1):       # transitive, cycles terminate
        progressed = False
        for alias, names in defs.items():
            if alias in resolved:
                continue
            real = {resolved.get(n, n) for n in names
                    if n not in defs or n in resolved}
            if len(real) == 1 and len(names) >= 1 \
                    and all(n not in defs or n in resolved for n in names):
                resolved[alias] = next(iter(real))
                progressed = True
        if not progressed:
            break
    return resolved


def mine_join_witnesses(records: list[ExpressionRecord], graph: GraphDir,
                        crosswalk: Crosswalk, run_id: str,
                        evidence_name: str = "studio") -> dict:
    """→ report. The scope discipline, derived by US from the raw SQL:
    an equality observed between TRANSFORMED CTEs is evidence a
    relationship EXISTS, never that the raw tables join safely — every
    mined edge ships ``scope: scoped_only``."""
    import sqlglot
    import sqlglot.expressions as exp
    report = {"join_edges": 0, "pattern_only": 0, "join_unresolved": 0,
              "join_out_of_scope": 0, "sql_parse_failures": 0}
    pairs: dict[tuple[str, str], dict] = {}
    for record in records:
        sql = str(record.extra.get("referenced_query") or "").strip()
        mid = (record.metric_ref or "").split(":", 1)[-1]
        if not sql:
            continue
        try:
            tree = sqlglot.parse_one(sql, read="bigquery")
        except Exception:
            report["sql_parse_failures"] += 1
            continue
        cte_base = _cte_bases(tree)
        # every table reference in the tree: alias-or-name → real short
        # table (CTE aliases route through their single base)
        alias_map: dict[str, str] = {}
        for t in tree.find_all(exp.Table):
            real = cte_base.get(t.name.lower(), t.name.lower())
            real = cte_base.get(real, real)
            alias_map[(t.alias or t.name).lower()] = real
        for alias, real in list(cte_base.items()):
            alias_map.setdefault(alias, real)
        seen_here: set[tuple[str, str, str]] = set()
        for join in tree.find_all(exp.Join):
            on = join.args.get("on")
            if on is None:
                report["join_unresolved"] += 1
                continue
            resolved_any = False
            for eq in on.find_all(exp.EQ):
                lcol, rcol = eq.this, eq.expression
                if not (isinstance(lcol, exp.Column)
                        and isinstance(rcol, exp.Column)
                        and lcol.table and rcol.table):
                    continue
                lt = alias_map.get(lcol.table.lower())
                rt = alias_map.get(rcol.table.lower())
                if not lt or not rt:
                    continue
                resolved_any = True
                lphys = (crosswalk.physical_for_short(lt)
                         or crosswalk.physical_for_alias(lt))
                rphys = (crosswalk.physical_for_short(rt)
                         or crosswalk.physical_for_alias(rt))
                if lphys is None or rphys is None:
                    report["join_out_of_scope"] += 1
                    continue
                if lphys == rphys:
                    # period-over-period self-join: a query pattern,
                    # not a physical relationship — counted, never an edge
                    key = (lphys, rphys, "self")
                    if key not in seen_here:
                        seen_here.add(key)
                        report["pattern_only"] += 1
                    continue
                a, b = sorted((lphys, rphys))
                cond = (f"{lcol.name} = {rcol.name}" if a == lphys
                        else f"{rcol.name} = {lcol.name}")
                side = " ".join(x for x in (join.side, join.kind) if x) \
                    or "JOIN"
                entry = pairs.setdefault((a, b), {
                    "on": [], "join_type": side, "metrics": set(),
                    "evidence": record.evidence_ref})
                if cond not in entry["on"]:
                    entry["on"].append(cond)
                entry["metrics"].add(mid)
            if not resolved_any:
                report["join_unresolved"] += 1
    for (a, b), entry in sorted(pairs.items()):
        graph.append_edge(Quad(
            s=table_id(a), r="joins_via", o=table_id(b),
            props={"on": sorted(entry["on"]),
                   "join_type": entry["join_type"],
                   "scope": "scoped_only",
                   "witness_metrics": sorted(entry["metrics"]),
                   "confidence": "mined"},
            prov=Prov(source=SOURCE, run=run_id, witness="studio",
                      support=len(entry["metrics"]),
                      evidence=entry["evidence"])))
        report["join_edges"] += 1
    return report
