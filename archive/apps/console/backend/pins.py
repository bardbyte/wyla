"""The pin store — verified-query escrow.

A pin is an answer the organization decided to KEEP: the question, the
exact SQL that produced it, the result snapshot, the citations with
their evidence tiers, and the audit-ledger link. Pins are what the
Morning Briefing renders, and re-running one replays the STORED SQL
deterministically through the full warehouse gate chain — no model in
the loop, every attempt on the audit ledger, joinable to it by
`sql_sha256` (computed byte-identically to WarehouseRunner._begin).

Storage is a JSON file next to the graph snapshot (SYNAPSE_PINS_PATH,
default synapse/data/cache/pins.json — the cache dir is gitignored).
Atomic writes, in-process lock. No seeds, no samples: before the first
real pin the Briefing is honestly empty — everything shown is something
someone actually asked and kept.

`verified` is a flag in this file, not a graph fact — the console never
writes the graph. Promoting a verified pin to a weight-10
human_approval fact is a later steward-CLI increment.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from apps.console.backend.data import REPO_ROOT

_DEFAULT_PATH = REPO_ROOT / "synapse" / "data" / "cache" / "pins.json"

_ROWS_CAP = 50
_HISTORY_CAP = 50

# columns that LOOK numeric but are dates/ids/years — never headlines
_NON_MEASURE = re.compile(
    r"(^|_)(id|key|yr|year|mo|month|mth|dt|date|ts|time)(_|$)", re.I)

_TIER_ORDER = ["deprecated", "guessed", "inferred", "grounded",
               "human_asserted"]


class NoSqlError(Exception):
    """Fact pins carry no SQL — nothing to re-run."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sql_hash(sql: str) -> str:
    """Byte-identical to WarehouseRunner._begin — the ledger join key."""
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def choose_locator(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the headline cell ONCE, at pin time: first numeric column of
    the last row, skipping date/id-named columns when a real measure
    exists. Stored so the choice never drifts per render."""
    if not rows:
        return None
    last = rows[-1]
    numeric_cols = [c for c, v in last.items() if _numeric(v)]
    if not numeric_cols:
        return None
    measures = [c for c in numeric_cols if not _NON_MEASURE.search(c)]
    return {"row": "last", "column": (measures or numeric_cols)[0]}


def compute_headline(rows: list[dict[str, Any]],
                     locator: dict[str, Any] | None) -> dict[str, Any]:
    if not rows:
        return {"kind": "none"}
    if locator is None:
        return {"kind": "rows", "n_rows": len(rows)}
    column = locator["column"]
    if column not in rows[-1] or not _numeric(rows[-1][column]):
        return {"kind": "rows", "n_rows": len(rows),
                "locator_missed": True}
    value = rows[-1][column]
    kind = "scalar" if len(rows) == 1 and len(rows[0]) == 1 \
        else "series_last"
    return {"kind": kind, "column": column, "value": value,
            "n_rows": len(rows)}


def worst_tier(citations: list[dict[str, Any]]) -> str:
    """Worst chip governs. Ledger refs carry no tier; zero resolvable
    tiers → the honest guessed floor."""
    tiers = [c.get("tier") for c in citations
             if c.get("tier") in _TIER_ORDER]
    if not tiers:
        return "guessed"
    return min(tiers, key=_TIER_ORDER.index)


class PinStore:
    def __init__(self, path: str | Path | None = None, *,
                 tier_resolver: Callable[[str], str | None] | None = None,
                 ) -> None:
        self.path = Path(path or os.environ.get("SYNAPSE_PINS_PATH",
                                                _DEFAULT_PATH)).expanduser()
        self._lock = threading.Lock()
        self._tier_resolver = tier_resolver

    # ── persistence ──────────────────────────────────────────

    def _load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            doc = json.loads(self.path.read_text(encoding="utf-8"))
            return list(doc.get("pins", []))
        except Exception:
            return []

    def _save(self, pins: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"version": 1, "pins": pins}, indent=2),
                       encoding="utf-8")
        os.replace(tmp, self.path)

    # ── reads ────────────────────────────────────────────────

    def list(self) -> list[dict[str, Any]]:
        """Newest-first. Citation tiers (and the rollup) refresh through
        the resolver on every read, so a steward signature elsewhere
        shows up here without a re-pin."""
        pins = self._load()
        if self._tier_resolver is not None:
            for pin in pins:
                changed = False
                for cite in pin.get("citations", []):
                    ref = str(cite.get("ref", ""))
                    if ref.startswith("ledger:"):
                        continue
                    tier = self._tier_resolver(ref)
                    if tier and tier != cite.get("tier"):
                        cite["tier"] = tier
                        changed = True
                if changed:
                    pin["tier"] = worst_tier(pin.get("citations", []))
        return sorted(pins, key=lambda p: p.get("created_at", ""),
                      reverse=True)

    def get(self, pin_id: str) -> dict[str, Any] | None:
        return next((p for p in self.list() if p.get("id") == pin_id),
                    None)

    # ── writes ───────────────────────────────────────────────

    def create(self, *, question: str, answer: str = "",
               citations: list[dict[str, Any]] | None = None,
               sql: str | None = None,
               rows: list[dict[str, Any]] | None = None,
               ledger_id: str | None = None,
               actor: str = "user",
               source: str = "live") -> dict[str, Any]:
        citations = [dict(c) for c in (citations or [])]
        if self._tier_resolver is not None:
            for cite in citations:
                ref = str(cite.get("ref", ""))
                if not ref.startswith("ledger:") and \
                        cite.get("tier") is None:
                    cite["tier"] = self._tier_resolver(ref)
        rows = list(rows or [])[:_ROWS_CAP]
        locator = choose_locator(rows) if sql else None
        headline = compute_headline(rows, locator)
        now = _now()
        pin = {
            "id": f"pin_{uuid.uuid4().hex[:12]}",
            "question": question.strip(),
            "answer": answer,
            "sql": sql,
            "sql_sha256": sql_hash(sql) if sql else None,
            "citations": citations,
            "tier": worst_tier(citations),
            "locator": locator,
            "headline": headline,
            "rows": rows,
            "created_at": now,
            "actor": actor,
            "source": source,
            "verified": None,
            "history": [{
                "kind": "capture", "ts": now, "actor": actor,
                "status": "ok", "code": None,
                "value": headline.get("value"),
                "n_rows": len(rows),
                "ledger_id": ledger_id,
            }],
        }
        with self._lock:
            pins = self._load()
            pins.append(pin)
            self._save(pins)
        return pin

    def _mutate(self, pin_id: str,
                fn: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        with self._lock:
            pins = self._load()
            pin = next((p for p in pins if p.get("id") == pin_id), None)
            if pin is None:
                raise KeyError(pin_id)
            fn(pin)
            self._save(pins)
            return pin

    def verify(self, pin_id: str, *, verified: bool = True,
               actor: str = "steward") -> dict[str, Any]:
        def apply(pin: dict[str, Any]) -> None:
            pin["verified"] = ({"by": actor, "at": _now()}
                               if verified else None)
        return self._mutate(pin_id, apply)

    def delete(self, pin_id: str) -> None:
        with self._lock:
            pins = self._load()
            kept = [p for p in pins if p.get("id") != pin_id]
            if len(kept) == len(pins):
                raise KeyError(pin_id)
            self._save(kept)          # [] persists → true empty state

    # ── the escrow replay ────────────────────────────────────

    def rerun(self, pin_id: str, warehouse: Any,
              actor: str = "user") -> dict[str, Any]:
        """Replay the STORED SQL through the full gate chain. The
        warehouse runner enforces shape/guardrails/dry-run/budget and
        writes the audit-ledger line itself; this method only records
        the outcome on the pin — ok refreshes rows/headline via the
        STORED locator, a refusal leaves the pin's value untouched."""
        with self._lock:
            pins = self._load()
            pin = next((p for p in pins if p.get("id") == pin_id), None)
            if pin is None:
                raise KeyError(pin_id)
            if not pin.get("sql"):
                raise NoSqlError(pin_id)

            run: dict[str, Any] = {"kind": "rerun", "ts": _now(),
                                   "actor": actor}
            if warehouse is None:
                run.update({
                    "status": "refused", "code": "no_graph",
                    "reason": "no graph snapshot loaded — guardrails "
                              "unavailable, so nothing runs",
                })
            else:
                outcome = warehouse.execute(pin["sql"], max_rows=100)
                if outcome.get("status") == "ok":
                    data = outcome.get("data") or {}
                    rows = list(data.get("rows") or [])[:_ROWS_CAP]
                    headline = compute_headline(rows, pin.get("locator"))
                    pin["rows"] = rows
                    pin["headline"] = headline
                    run.update({
                        "status": "ok", "code": None,
                        "value": headline.get("value"),
                        "n_rows": len(rows),
                        "ledger_id": data.get("job_id")
                        or pin.get("sql_sha256"),
                        "bytes_billed": data.get("bytes_billed"),
                    })
                    if headline.get("locator_missed"):
                        run["locator_missed"] = True
                else:
                    run.update({
                        "status": "refused",
                        "code": outcome.get("code", "refused"),
                        "reason": str(
                            outcome.get("reason")
                            or outcome.get("message") or "")[:300],
                    })
            pin.setdefault("history", []).append(run)
            pin["history"] = pin["history"][-_HISTORY_CAP:]
            self._save(pins)
            return {"pin": pin, "run": run}


