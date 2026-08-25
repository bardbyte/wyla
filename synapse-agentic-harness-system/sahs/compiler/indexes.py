"""Compiled indexes — sqlite (FTS5 where available) + JSONL twins.

JSONL is the diffable source of truth; index.sqlite is a derived
artifact rebuilt from the same rows. Vocab gets FTS5 (with a plain-table
LIKE/difflib fallback behind the same API when the build asserts FTS5 is
absent); bindings and metrics are plain tables with precomputed rank
columns so the resolver's sort is a lookup, not a computation.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def fts5_available() -> bool:
    try:
        probe = sqlite3.connect(":memory:")
        probe.execute("CREATE VIRTUAL TABLE _p USING fts5(x)")
        return True
    except sqlite3.OperationalError:
        return False


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False,
                               sort_keys=True) + "\n")
    return path


def build_indexes(out_dir: Path, vocab_rows: list[dict[str, Any]],
                  binding_rows: list[dict[str, Any]],
                  metric_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_dir / "vocab.jsonl", vocab_rows)
    write_jsonl(out_dir / "bindings.jsonl", binding_rows)
    write_jsonl(out_dir / "metrics.jsonl", metric_rows)

    db_path = out_dir / "index.sqlite"
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    fts = fts5_available()
    if fts:
        connection.execute(
            "CREATE VIRTUAL TABLE vocab USING fts5("
            "text, kind UNINDEXED, ref UNINDEXED, bu UNINDEXED, "
            "region UNINDEXED, definition UNINDEXED)")
    else:
        connection.execute(
            "CREATE TABLE vocab (text TEXT, kind TEXT, ref TEXT, "
            "bu TEXT, region TEXT, definition TEXT)")
    connection.executemany(
        "INSERT INTO vocab (text, kind, ref, bu, region, definition) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [(r["text"], r["kind"], r["ref"], r.get("bu", "all"),
          r.get("region", "all"), r.get("definition", ""))
         for r in vocab_rows])

    connection.execute(
        "CREATE TABLE bindings (label TEXT, tbl TEXT, fp TEXT, "
        "canonical_sql TEXT, authority INTEGER, support INTEGER, "
        "last_seen TEXT, source TEXT, agreement INTEGER, "
        "ungoverned INTEGER)")
    connection.executemany(
        "INSERT INTO bindings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(r["label"], r["table"], r["fp"], r["canonical_sql"],
          r["authority"], r["support"], r.get("last_seen", ""),
          r["source"], r.get("agreement", 1),
          int(bool(r.get("ungoverned"))))
         for r in binding_rows])
    connection.execute("CREATE INDEX ix_bind ON bindings(label, tbl)")

    connection.execute(
        "CREATE TABLE metrics (id TEXT, mgroup TEXT, label TEXT, "
        "tbl TEXT, grain TEXT, status TEXT, question TEXT, "
        "canonical_sql TEXT, fp TEXT, authority INTEGER, "
        "support INTEGER, source TEXT, dims TEXT)")
    connection.executemany(
        "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(r["id"], r["mgroup"], r["label"], r["table"], r.get("grain", ""),
          r["status"], r.get("question", ""), r["canonical_sql"], r["fp"],
          r["authority"], r["support"], r["source"],
          json.dumps(r.get("approved_dimensions", [])))
         for r in metric_rows])
    connection.execute("CREATE INDEX ix_metric ON metrics(label)")
    connection.commit()
    connection.close()
    return {"fts5": fts, "vocab_rows": len(vocab_rows),
            "binding_rows": len(binding_rows),
            "metric_rows": len(metric_rows)}
