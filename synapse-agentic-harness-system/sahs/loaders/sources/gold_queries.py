"""extracted_gold_queries.json — 158 human-verified prompt→SQL pairs.

Non-empty SQL becomes an ExpressionRecord carrying its prompt (the eval
task materializer reads the same records). Empty SQL is NOT quarantine —
it is the triage backlog (genuine-abstention gold vs broken extraction),
returned separately so P0's exit criterion can count it."""

from __future__ import annotations

import json
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.loaders.records import ExpressionRecord, Quarantined

SOURCE = "gold_queries"


def load_gold_queries(path: Path) -> tuple[list[ExpressionRecord],
                                           list[Quarantined],
                                           list[dict]]:
    """→ (records, quarantined, empty_sql_backlog)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else payload.get("queries", [])
    records: list[ExpressionRecord] = []
    quarantined: list[Quarantined] = []
    backlog: list[dict] = []
    for row in rows:
        rid = str(row.get("id", "?"))
        ref = f"{Path(path).name}#id={rid}"
        prompt = str(row.get("prompt") or "").strip()
        sql = str(row.get("sql") or "").strip()
        if not prompt:
            quarantined.append(Quarantined(
                source=SOURCE, category="missing_field",
                detail="row without prompt", evidence_ref=ref))
            continue
        if not sql:
            backlog.append({"id": rid, "prompt": prompt,
                            "source_row": row.get("source_row"),
                            "evidence_ref": ref})
            continue
        records.append(ExpressionRecord(
            raw_sql=sql, kind="query", source=SOURCE,
            authority=Authority.SKILL_CONTRACT, prompt=prompt,
            evidence_ref=ref,
            extra={"gold_id": rid, "difficulty": row.get("difficulty")}))
    return records, quarantined, backlog
