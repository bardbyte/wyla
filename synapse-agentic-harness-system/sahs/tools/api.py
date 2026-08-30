"""L4 — the eight serving tools over a compiled build.

The agent's world ends here: every function reads ONLY the immutable
build directory (cards + indexes + acl + schema) — never the truth
graph. All eight are ADK-wrappable plain functions with docstring
schemas; ``Build.open`` resolves through ``builds/CURRENT`` by default
(E4), explicit paths are for tests. Search-over-list throughout; every
response returns meaning, not bare IDs; error messages teach the
correct call.
"""

from __future__ import annotations

import difflib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").split("\n") if line.strip()]


@dataclass
class Build:
    root: Path
    manifest: dict[str, Any]
    metrics: list[dict[str, Any]]
    bindings: list[dict[str, Any]]
    vocab: list[dict[str, Any]]
    joins: list[dict[str, Any]]
    acl: dict[str, Any]
    schema: dict[str, dict[str, str]]
    cost_priors: dict[str, dict[str, Any]] = field(default_factory=dict)
    lob: list[dict[str, Any]] = field(default_factory=list)
    _db: sqlite3.Connection | None = field(default=None, repr=False)

    @classmethod
    def open(cls, builds_root_or_dir: Path) -> "Build":
        path = Path(builds_root_or_dir)
        current = path / "CURRENT"
        if current.exists():
            path = path / current.read_text(encoding="utf-8").strip()
        if not (path / "manifest.json").exists():
            raise FileNotFoundError(
                f"no build at {path} — run `laptop.py compile` first "
                "(or point at a builds/ root containing CURRENT)")
        return cls(
            root=path,
            manifest=json.loads((path / "manifest.json").read_text()),
            metrics=_jsonl(path / "indexes" / "metrics.jsonl"),
            bindings=_jsonl(path / "indexes" / "bindings.jsonl"),
            vocab=_jsonl(path / "indexes" / "vocab.jsonl"),
            joins=_jsonl(path / "indexes" / "joins.jsonl"),
            acl=json.loads((path / "acl.json").read_text())
            if (path / "acl.json").exists() else {},
            schema=json.loads((path / "schema.json").read_text())
            if (path / "schema.json").exists() else {},
            cost_priors=json.loads(
                (path / "indexes" / "cost_priors.json").read_text())
            if (path / "indexes" / "cost_priors.json").exists() else {},
            lob=_jsonl(path / "indexes" / "lob.jsonl"),
        )

    @property
    def version(self) -> str:
        return str(self.manifest.get("build_id", "?"))

    def short_table(self, physical: str) -> str:
        return physical.split(".")[-1]

    def physical_of(self, name: str) -> str | None:
        name = (name or "").strip().lower()
        if name in self.schema:
            return name
        hits = [t for t in self.schema if t.split(".")[-1] == name
                or t.endswith(name)]
        return hits[0] if len(hits) == 1 else None


# ─── the eight tools ─────────────────────────────────────────


def search_metrics(build: Build, intent: str, top_k: int = 8) -> dict:
    """Ranked metric candidates for an intent phrase — status, grain,
    table, and conflict flags included. Certified first, always."""
    tokens = _tokens(intent)
    scored = []
    for row in build.metrics:
        overlap = (len(tokens & _tokens(row["label"]))
                   + len(tokens & _tokens(row.get("question", ""))))
        if overlap:
            scored.append((row["authority"], overlap, row["support"], row))
    scored.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]["id"]))
    groups: dict[str, int] = {}
    for row in build.metrics:
        for g in row.get("mgroups", [row.get("mgroup", "")]):
            groups[g] = groups.get(g, 0) + 1
    return {"candidates": [{
        "id": r["id"], "mgroup": r["mgroup"], "label": r["label"],
        "status": r["status"],
        "status_served": r.get("status_served", r["status"]),
        "evidence_origin": r.get("evidence_origin", r["source"]),
        "grain": r.get("grain", ""),
        "table": r["table"], "question": r.get("question", ""),
        "question_source": r.get("question_source", ""),
        "source": r["source"],
        "line_of_business": r.get("line_of_business", ""),
        "domain": r.get("domain", ""),
        "guidance": (r.get("description") or "")[:280],
        "conflict": any(groups.get(g, 0) > 1
                        for g in r.get("mgroups", [r.get("mgroup", "")])),
    } for _, _, _, r in scored[:top_k]],
        "hint": "" if scored else
        "no metric matches — try search_concepts, or describe_table "
        "to browse what a table offers"}


def search_concepts(build: Build, phrase: str, table: str = "",
                    top_k: int = 8) -> dict:
    """Ranked concept→predicate bindings with support, status, and the
    actual SQL. Pass table to scope."""
    tokens = _tokens(phrase)
    physical = build.physical_of(table) if table else None
    scored = []
    for row in build.bindings:
        if physical and row["table"] != physical:
            continue
        overlap = len(tokens & _tokens(row["label"]))
        if overlap:
            scored.append((row["authority"], overlap, row["support"], row))
    scored.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]["fp"]))
    return {"bindings": [{
        "concept": r["label"], "table": r["table"], "sql":
            r["canonical_sql"], "support": r["support"],
        "authority": r["authority"], "source": r["source"], "fp": r["fp"],
    } for _, _, _, r in scored[:top_k]],
        "hint": "" if scored else
        f"no binding for {phrase!r} — resolve() can route the whole "
        "question, or the concept may need a steward"}


def describe_table(build: Build, name: str) -> dict:
    """The compiled table card — the token-budgeted, provenance-lined
    view the compiler promised. The heavy call; resolve names first."""
    physical = build.physical_of(name)
    if physical is None:
        close = difflib.get_close_matches(
            name.lower(), [build.short_table(t) for t in build.schema], 3)
        return {"error": f"unknown table {name!r}",
                "suggestions": close,
                "hint": "tables in this build: "
                        + ", ".join(sorted(build.schema)[:10])}
    card = (build.root / "cards" / "tables"
            / f"{physical.replace('.', '__')}.md")
    return {"table": physical, "card": card.read_text(encoding="utf-8")
            if card.exists() else "(card missing — compiler bug)",
            "build": build.version}


def sample_values(build: Build, table: str, column: str) -> dict:
    """Observed value domain for a low-cardinality column — from the
    COMPILED extraction, never live. Call before writing WHERE literals."""
    physical = build.physical_of(table)
    if physical is None:
        return {"error": f"unknown table {table!r}"}
    domains = _jsonl(build.root / "indexes" / "domains.jsonl")
    key = f"{physical}.{column.lower()}"
    for row in domains:
        if row.get("key") == key:
            return {"table": physical, "column": column,
                    "values": row["values"],
                    "coverage_note": "compiled snapshot domain — "
                                     "not a live query"}
    return {"error": f"no compiled domain for {physical}.{column}",
            "hint": "only confirmed low-cardinality columns carry "
                    "domains; check the table card's columns section"}


def get_definition_line(build: Build, metric_id: str,
                        variant_id: str = "") -> dict:
    """The one-sentence disclosure every answer must carry — which
    definition, whose authority, on the meridian or off it."""
    target = variant_id or metric_id
    row = next((m for m in build.metrics
                if m["id"] == target or m["fp"] == target
                or target in m.get("mgroups", [])), None)
    if row is None:
        return {"error": f"unknown metric {target!r}",
                "hint": "ids come from search_metrics / resolve"}
    if row["status"] == "certified" and not row.get("parent_fp"):
        line = (f"Using certified '{row['label']}' "
                f"({row['source']}) on {row['table']} — the meridian "
                f"line for this metric.")
    else:
        # served vocabulary: governance status, then evidence origin —
        # "unreviewed, usage_mining" cannot be read as an endorsement
        line = (f"Using '{row['label']}' "
                f"[{row.get('status_served') or row['status']}, "
                f"{row.get('evidence_origin') or row['source']}] "
                f"on {row['table']}"
                + (f" — off-meridian variant of {row['parent_fp']}"
                   if row.get("parent_fp") else
                   " — not yet on the meridian line")
                + ".")
    return {"definition_line": line, "metric": row["id"],
            "status": row["status"],
            "status_served": row.get("status_served", row["status"])}


def _tokens(text: str) -> set[str]:
    stop = {"the", "a", "an", "of", "and", "or", "in", "for", "to", "on",
            "by", "with", "our", "we", "did", "do", "how", "what",
            "which", "much", "many", "is", "are", "was", "last", "per"}
    out = set()
    for token in "".join(c if c.isalnum() else " "
                         for c in (text or "").lower()).split():
        if len(token) > 1 and token not in stop:
            out.add(token[:-1] if len(token) > 3 and token.endswith("s")
                    and not token.endswith("ss") else token)
    return out
