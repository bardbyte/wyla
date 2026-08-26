"""repair_sources — the mechanical parts are pinned; the human-in-the-
loop DMP write is deliberately NOT automated (semantics stay human)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from scripts.repair_sources import (  # noqa: E402
    _defence,
    _dmp_candidates,
    _parses,
    _repair_gold,
)


def test_defence_strips_fences_only_at_edges():
    assert _defence("```sql\nSELECT 1 FROM t\n```") == "SELECT 1 FROM t"
    assert _defence("SELECT a FROM `p`.d.t\n``") == "SELECT a FROM `p`.d.t"
    kept = "SELECT a FROM `p.d.t`"        # lone identifier backtick stays
    assert _defence(kept) == kept
    inner = "SELECT 1 FROM t WHERE x = 'a``b' AND y = 1"
    assert _defence(inner) == inner       # fence-ish INSIDE is content


def test_dmp_candidates_verified_and_alias_preserving_first():
    stored = "COUNT(DISTINCT enrolling_card)\n) AS avg_enrollments"
    cands = _dmp_candidates(stored)
    assert cands and all(_parses(c) for c in cands)
    assert "AS avg_enrollments" in cands[0]   # most content survives
    assert _dmp_candidates("SAFE_DIVIDE(a, b)") == []   # healthy → none


def test_repair_gold_defences_backs_up_and_sweeps(tmp_path: Path):
    rows = [{"id": 1, "sql": "SELECT 1 FROM t"},
            {"id": 63, "sql": "SELECT a FROM `p`.d.t\n```"},
            {"id": 99, "sql": ""}]        # backlog row stays untouched
    path = tmp_path / "extracted_gold_queries.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    assert _repair_gold(tmp_path) is True
    fixed = json.loads(path.read_text(encoding="utf-8"))
    assert fixed[1]["sql"] == "SELECT a FROM `p`.d.t"
    assert fixed[2]["sql"] == ""
    assert (tmp_path / "extracted_gold_queries.json.bak").exists()
    assert _repair_gold(tmp_path) is True     # idempotent second pass
