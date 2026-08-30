"""Meridian read plane — the Synapse by Lumi admin endpoints (E17).

Read-only projections of the compiled Meridian build (the
``synapse-agentic-harness-system`` silo) for the admin surfaces:
Home, Sources, Explorer, Profiles, Cosmos, Builds, Enrichment runs,
Artifacts. One reader implementation — ``sahs.tools.api.Build`` —
imported from the silo, never re-parsed here.

Reality law: when no compiled build exists every endpoint answers
``{"available": false, "reason": …}`` with HTTP 200 and the UI
renders its designed empty state. Nothing is mocked, ever.

The ONLY write here is the feedback affordance (skill §5): append-only
JSONL under ``graph/runs/feedback/`` — quads-adjacent records a
steward can review, never graph writes (those stay with the clerk).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

_REPO = Path(__file__).resolve().parents[3]
_SILO = Path(os.environ.get("MERIDIAN_SILO_DIR",
                            _REPO / "synapse-agentic-harness-system"))


def _silo_import():
    if str(_SILO) not in sys.path:
        sys.path.insert(0, str(_SILO))
    from sahs.compiler.display import tier_of_join, tier_of_metric
    from sahs.tools.api import Build
    return Build, tier_of_metric, tier_of_join


def _builds_root() -> Path:
    return Path(os.environ.get("MERIDIAN_BUILDS_DIR", _SILO / "builds"))


def _graph_root() -> Path:
    return Path(os.environ.get("MERIDIAN_GRAPH_DIR", _SILO / "graph"))


class MeridianData:
    """Lazy, mtime-cached view over builds/CURRENT + graph sidecars."""

    def __init__(self) -> None:
        self._build = None
        self._stamp: tuple | None = None
        self._bridge = None

    # ── loading ──────────────────────────────────────────────

    def _load(self):
        current = _builds_root() / "CURRENT"
        if not current.exists():
            return None, (f"no compiled build — {current} missing; "
                          "run `laptop.py compile` on this machine")
        stamp = (current.stat().st_mtime_ns,
                 current.read_text(encoding="utf-8").strip())
        if self._build is not None and stamp == self._stamp:
            return self._build, ""
        try:
            Build, tier_of_metric, tier_of_join = _silo_import()
            self._build = Build.open(_builds_root())
            self._bridge = (tier_of_metric, tier_of_join)
            self._stamp = stamp
            return self._build, ""
        except Exception as exc:
            return None, f"build unreadable: {exc}"

    def _aux(self, name: str) -> Any:
        build, reason = self._load()
        if build is None:
            return None
        path = build.root / name
        if not path.exists():
            return None
        if name.endswith(".jsonl"):
            return [json.loads(line) for line in
                    path.read_text(encoding="utf-8").splitlines()
                    if line.strip()]
        if name.endswith(".md"):
            return path.read_text(encoding="utf-8")
        return json.loads(path.read_text(encoding="utf-8"))

    def _reviews(self) -> list[dict]:
        path = _graph_root() / "nodes" / "review.jsonl"
        if not path.exists():
            return []
        folded: dict[str, dict] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            folded[record["id"]] = record
        return list(folded.values())

    @staticmethod
    def _unavailable(reason: str) -> dict:
        return {"available": False, "reason": reason}

    # ── surfaces ─────────────────────────────────────────────

    def home(self) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        tier_of_metric, _ = self._bridge
        by_served: dict[str, int] = {}
        for row in build.metrics:
            key = row.get("status_served") or row.get("status") or "?"
            by_served[key] = by_served.get(key, 0) + 1
        joins_scoped = sum(1 for j in build.joins
                           if j.get("scope") == "scoped_only")
        sources = self._aux("indexes/sources.json") or {}
        reviews = self._reviews()
        recon = build.manifest.get("table_reconciliation", {})
        excluded = [m for m in recon.get("missing", [])
                    if m.get("intentionally_excluded")]
        return {
            "available": True,
            "build_id": build.version,
            "counts": build.manifest.get("counts", {}),
            "metrics_by_status": by_served,
            "joins": {"total": len(build.joins),
                      "scoped_only": joins_scoped},
            "readiness": sources.get("readiness", {}),
            "sources_count": len(sources.get("sources", [])),
            "open_reviews": sum(
                1 for r in reviews
                if r.get("props", {}).get("status", "open") == "open"),
            "excluded_tables": excluded,
            "census": self._aux("census.json") or {},
            "diff": (self._aux("DIFF_vs_prev.md") or "")[:4000],
        }

    def sources(self) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        shelf = self._aux("indexes/sources.json")
        if shelf is None:
            return self._unavailable(
                "build predates the Sources shelf — recompile")
        return {"available": True, "build_id": build.version, **shelf}

    def explorer_metrics(self, q: str = "", status: str = "",
                         lob: str = "", limit: int = 200) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        tier_of_metric, _ = self._bridge
        needle = (q or "").lower().strip()
        rows = []
        for row in sorted(build.metrics,
                          key=lambda r: (-int(r.get("support") or 0),
                                         r["id"])):
            served = row.get("status_served") or row.get("status")
            if status and served != status:
                continue
            if lob and (row.get("line_of_business") or "") != lob:
                continue
            if needle and needle not in (
                    (row.get("label") or "") + " "
                    + (row.get("canonical_sql") or "")
                    + " " + (row.get("question") or "")).lower():
                continue
            rows.append({
                "id": row["id"], "fp": row.get("fp", ""),
                "label": row.get("label") or "",
                "expr": (row.get("canonical_sql") or "")[:160],
                "status_served": served,
                "evidence_origin": row.get("evidence_origin", ""),
                "tier": tier_of_metric(row),
                "agreement": row.get("witness_agreement", 0),
                "support": row.get("support", 0),
                "witnesses": row.get("support_by_witness", {}),
                "used_by": row.get("used_by", {}),
                "table": row.get("table", ""),
                "lob": row.get("line_of_business", "")})
            if len(rows) >= limit:
                break
        total = len(build.metrics)
        return {"available": True, "build_id": build.version,
                "total": total, "shown": len(rows), "rows": rows}

    def explorer_tables(self) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        metrics_by_table: dict[str, int] = {}
        for row in build.metrics:
            if row.get("table"):
                metrics_by_table[row["table"]] = \
                    metrics_by_table.get(row["table"], 0) + 1
        joins_by_table: dict[str, int] = {}
        for j in build.joins:
            for end in (j.get("a"), j.get("b")):
                if end:
                    joins_by_table[end] = joins_by_table.get(end, 0) + 1
        lob_of: dict[str, str] = {}
        for lrow in build.lob:
            code = str(lrow.get("code") or lrow.get("lob") or "")
            for physical in lrow.get("tables", []):
                lob_of.setdefault(physical, code)
        tickets = self._aux("tickets.jsonl") or []
        tickets_by_table: dict[str, int] = {}
        for t in tickets:
            physical = t.get("table") or ""
            if physical:
                tickets_by_table[physical] = \
                    tickets_by_table.get(physical, 0) + 1
        rows = [{"physical": physical,
                 "short": physical.split(".")[-1],
                 "columns": len(columns),
                 "lob": lob_of.get(physical, ""),
                 "metrics_here": metrics_by_table.get(physical, 0),
                 "joins": joins_by_table.get(physical, 0),
                 "tickets": tickets_by_table.get(physical, 0),
                 "cost_prior": build.cost_priors.get(physical)}
                for physical, columns in sorted(build.schema.items())]
        return {"available": True, "build_id": build.version,
                "rows": rows}

    def metric(self, metric_id: str) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        tier_of_metric, _ = self._bridge
        row = next((r for r in build.metrics
                    if r["id"] == metric_id
                    or r.get("fp") == metric_id), None)
        if row is None:
            return {"available": True, "found": False,
                    "id": metric_id}
        family = [r for r in build.metrics
                  if r is not row and r.get("fp") == row.get("fp")]
        reviews = [r for r in self._reviews()
                   if r.get("props", {}).get("subject") == row["id"]]
        return {"available": True, "found": True, "metric": row,
                "tier": tier_of_metric(row),
                "family": family[:12],
                "reviews": [r.get("props", {}) for r in reviews]}

    def table(self, physical: str) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        if physical not in build.schema:
            return {"available": True, "found": False,
                    "physical": physical}
        card = (build.root / "cards" / "tables"
                / f"{physical.replace('.', '__')}.md")
        joins = [j for j in build.joins
                 if physical in (j.get("a"), j.get("b"))]
        metrics_here = [
            {"id": r["id"], "label": r.get("label") or "",
             "status_served": r.get("status_served"),
             "support": r.get("support", 0)}
            for r in build.metrics if r.get("table") == physical]
        return {"available": True, "found": True,
                "physical": physical,
                "columns": build.schema.get(physical, {}),
                "card": card.read_text(encoding="utf-8")
                if card.exists() else "",
                "joins": joins,
                "metrics_here": sorted(
                    metrics_here,
                    key=lambda m: -m["support"])[:50],
                "cost_prior": build.cost_priors.get(physical)}

    def graph_map(self) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        payload = self._aux("indexes/graph_map.json")
        if payload is None:
            return self._unavailable(
                "build predates the cosmos map — recompile")
        return {"available": True, "build_id": build.version,
                **payload}

    def builds(self) -> dict:
        build, reason = self._load()
        if build is None:
            return self._unavailable(reason)
        siblings = sorted(
            p.name for p in _builds_root().iterdir()
            if p.is_dir() and (p / "manifest.json").exists())
        return {"available": True, "current": build.version,
                "builds": siblings,
                "manifest": build.manifest,
                "diff": self._aux("DIFF_vs_prev.md") or ""}

    def enrich_runs(self) -> dict:
        runs = []
        for report_path in sorted(
                _graph_root().glob("runs/*/enrich_report.json")):
            try:
                report = json.loads(
                    report_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            runs.append({"run": report_path.parent.name, **report})
        # out-dirs live under graph/runs/<name>/ for laptop runs, but
        # the enrich --out flag is free-form; sweep one level deeper
        # is deliberately NOT done — honest scope: what the graph
        # runs/ tree holds
        return {"available": True, "runs": runs[-20:]}

    def artifacts(self) -> dict:
        build, reason = self._load()
        shelf = (self._aux("indexes/sources.json") or {}) \
            if build is not None else {}
        known = [s for s in shelf.get("sources", [])
                 if s.get("family") == "knowledge"]
        staged_dir = _SILO / "sources" / "artifacts"
        staged = sorted(p.name for p in staged_dir.glob("*")
                        if p.is_file()) if staged_dir.exists() else []
        return {"available": build is not None,
                **({} if build is not None else {"reason": reason}),
                "known": known,
                "staged": staged,
                "staging_dir": str(staged_dir)}


class FeedbackEvent(BaseModel):
    screen: str
    object_id: str = ""
    vote: str = Field(pattern="^(up|down)$")
    note: str = ""
    session_kind: str = "steward"       # steward | analyst (two hats)
    actor: str = "admin"


router = APIRouter(prefix="/api/meridian")
_DATA = MeridianData()


@router.get("/home")
def home() -> dict:
    return _DATA.home()


@router.get("/sources")
def sources() -> dict:
    return _DATA.sources()


@router.get("/explorer/metrics")
def explorer_metrics(q: str = "", status: str = "", lob: str = "",
                     limit: int = 200) -> dict:
    return _DATA.explorer_metrics(q, status, lob, min(limit, 1000))


@router.get("/explorer/tables")
def explorer_tables() -> dict:
    return _DATA.explorer_tables()


@router.get("/metric/{metric_id:path}")
def metric(metric_id: str) -> dict:
    return _DATA.metric(metric_id)


@router.get("/table/{physical}")
def table(physical: str) -> dict:
    return _DATA.table(physical)


@router.get("/graph_map")
def graph_map() -> dict:
    return _DATA.graph_map()


@router.get("/builds")
def builds() -> dict:
    return _DATA.builds()


@router.get("/enrich_runs")
def enrich_runs() -> dict:
    return _DATA.enrich_runs()


@router.get("/artifacts")
def artifacts() -> dict:
    return _DATA.artifacts()


@router.post("/feedback", status_code=201)
def feedback(event: FeedbackEvent) -> dict:
    """The talk-back affordance on every surface — append-only JSONL
    the steward loop reviews later. Auto-attaches build + timestamp."""
    build, _ = _DATA._load()
    record = {
        "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "build": build.version if build is not None else "",
        **event.model_dump()}
    out_dir = _graph_root() / "runs" / "feedback"
    out_dir.mkdir(parents=True, exist_ok=True)
    day = _dt.date.today().isoformat()
    with (out_dir / f"feedback_{day}.jsonl").open(
            "a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")
    return {"recorded": True, "build": record["build"]}
