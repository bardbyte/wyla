"""KC Enrichment mounted on the Synapse app: the catalog-enabled tables,
one table's bundle (deterministic at once, model sections streamed),
its ledger and exports, the coverage dictionary, the push record, and
the read-back loader.

Paths:
    GET  /api/kc/tables                       the tables, readiness, last push
    GET  /api/kc/coverage                     the coverage registry + dictionary
    GET  /api/kc/glossary                     every term across every table, merged
    GET  /api/kc/glossary/export?format=jsonl|links|sheet
    GET  /api/kc/export-all                   one zip: a folder per table + the merged glossary
    GET  /api/kc/bundle/{table}?llm=1|0       the bundle (cached model sections)
    GET  /api/kc/bundle/{table}/stream        SSE: model sections as they land
    GET  /api/kc/bundle/{table}/ledger        the translation ledger alone
    GET  /api/kc/bundle/{table}/export?format=md|json|csv|sheet|zip
    POST /api/kc/push-record/{table}          {sections, actor, note}
    POST /api/kc/witness-import               {kind, payload, actor}

Every read answers ``available: false`` with the reason when no build
is compiled. The frontend never learns a model endpoint or a key: the
model is called here, through the same Vertex contract as enrichment.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from apps.synapse_admin.backend.meridian import (_builds_root, _graph_root,
                                                 _silo_import)

router = APIRouter(prefix="/api/kc")
POLL_SECONDS = 0.05
HEARTBEAT_SECONDS = 15.0
_STREAMS: dict[str, dict[str, Any]] = {}
_LOCK = threading.Lock()


def _kc():
    _silo_import()
    from sahs.kc import bundle as kc_bundle
    from sahs.kc.config import load_config
    return kc_bundle, load_config()


def _unavailable(reason: str) -> dict:
    return {"available": False, "reason": reason}


def _no_build() -> str:
    current = _builds_root() / "CURRENT"
    if not current.exists():
        return (f"no compiled build: {current} missing; run `pipeline.py "
                "compile` on this machine")
    return ""


class PushRecord(BaseModel):
    sections: list[str] = Field(min_length=1, max_length=16)
    actor: str = Field(min_length=1, max_length=80)
    note: str = Field(default="", max_length=2000)


class WitnessImport(BaseModel):
    kind: str = Field(pattern="^(glossary|descriptions|dq)$")
    payload: dict | list
    actor: str = Field(default="admin", max_length=80)


@router.get("/tables")
def tables(light: int = 0) -> dict:
    """The tables in scope. ``light=1`` answers names and LOBs only
    (the picker on a table page), without assembling anything."""
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    try:
        if light:
            return kc.table_names(builds_root=_builds_root(), graph_root=_graph_root(),
                                  config=cfg)
        return kc.list_tables(builds_root=_builds_root(), graph_root=_graph_root(),
                              config=cfg)
    except Exception as exc:
        return _unavailable(f"build unreadable: {exc}")


@router.get("/coverage")
def coverage() -> dict:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, _cfg = _kc()
    return kc.coverage_payload(builds_root=_builds_root(), graph_root=_graph_root())


@router.get("/glossary")
def glossary() -> dict:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    return kc.glossary_across(builds_root=_builds_root(), graph_root=_graph_root(),
                              config=cfg)


@router.get("/glossary/export")
def glossary_export(format: str = "jsonl") -> Any:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    try:
        data, media, filename = kc.glossary_export(
            format, builds_root=_builds_root(), graph_root=_graph_root(), config=cfg)
    except ValueError as exc:
        return {"available": True, "error": str(exc)}
    return Response(content=data, media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/export-all")
def export_all() -> Any:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    data, media, filename = kc.export_all(builds_root=_builds_root(),
                                          graph_root=_graph_root(), config=cfg)
    return Response(content=data, media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def _bundle(table: str, llm: bool, regenerate: bool = False,
            bus: Any = None) -> dict:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    try:
        bundle = kc.build_bundle(table, use_llm=llm, regenerate=regenerate,
                                 builds_root=_builds_root(), graph_root=_graph_root(),
                                 config=cfg, bus=bus)
    except KeyError:
        return {"available": True, "found": False, "table": table}
    except FileNotFoundError as exc:
        return _unavailable(str(exc))
    return {"available": True, "found": True, **bundle.to_dict()}


@router.get("/bundle/{table}")
def bundle(table: str, llm: int = 1) -> dict:
    # the model sections come from the cache here; a first generation
    # is the stream's job so the page can watch it land
    kc, _cfg = _kc()
    return _bundle(table, llm=bool(llm) and _cached(table), regenerate=False)


def _cached(table: str) -> bool:
    kc, cfg = _kc()
    from sahs.kc.fold import open_build
    from sahs.kc.write import cache_path, load_json
    try:
        build = open_build(_builds_root())
    except FileNotFoundError:
        return False
    return load_json(cache_path(_graph_root(), cfg, table, build.version)) is not None


@router.get("/bundle/{table}/ledger")
def ledger(table: str) -> dict:
    payload = _bundle(table, llm=False)
    if not payload.get("found"):
        return payload
    return {"available": True, "found": True, "table": table,
            "build_id": payload["build_id"], "ledger": payload["ledger"],
            "counts": payload["counts"]}


@router.get("/bundle/{table}/export")
def export(table: str, format: str = "md", llm: int = 1) -> Any:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    from sahs.kc.export import export as kc_export
    try:
        bundle = kc.build_bundle(table, use_llm=bool(llm) and _cached(table),
                                 builds_root=_builds_root(), graph_root=_graph_root(),
                                 config=cfg)
        data, media, filename = kc_export(bundle, format, cfg)
    except KeyError:
        return {"available": True, "found": False, "table": table}
    except ValueError as exc:
        return {"available": True, "found": True, "error": str(exc)}
    return Response(content=data, media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/bundle/{table}/stream")
async def stream(table: str, request: Request, regenerate: int = 0,
                 after: int = 0) -> Any:
    """Generate the model sections (or re-generate) in a worker thread
    and stream the kc_* events; the final event carries the bundle."""
    reason = _no_build()
    if reason:
        return StreamingResponse(
            iter([f'event: error\ndata: {{"reason": {reason!r}}}\n\n'.replace("'", '"')]),
            media_type="text/event-stream")
    _silo_import()
    from sahs.ask.events import EventBus, sse_frame
    from sahs.kc.write import KC_EVENTS
    import json as _json

    with _LOCK:
        job = _STREAMS.get(table)
        if job is None or not job["thread"].is_alive():
            bus = EventBus(f"kc:{table}", events=KC_EVENTS + ("bundle_ready",))
            job = {"bus": bus, "thread": None, "result": None}

            def worker() -> None:
                try:
                    payload = _bundle(table, llm=True, regenerate=bool(regenerate),
                                      bus=bus)
                except Exception as exc:          # surfaced, never swallowed
                    bus.emit("error", reason=str(exc)[:400])
                    payload = _unavailable(str(exc))
                job["result"] = payload
                bus.emit("bundle_ready", available=payload.get("available"),
                         found=payload.get("found"),
                         llm=payload.get("llm"), gate=payload.get("gate"))

            job["thread"] = threading.Thread(target=worker, daemon=True)
            job["thread"].start()
            _STREAMS[table] = job
    bus = job["bus"]

    async def pump():
        seq = after
        idle = 0.0
        while True:
            if await request.is_disconnected():
                return
            batch = bus.since(seq)
            if batch:
                idle = 0.0
                for record in batch:
                    seq = record["seq"]
                    yield sse_frame(record)
                    if record["ev"] == "bundle_ready":
                        # the bundle itself rides one final frame
                        yield ("event: bundle\ndata: "
                               + _json.dumps(job["result"], ensure_ascii=False)
                               + "\n\n")
                        return
            else:
                await asyncio.sleep(POLL_SECONDS)
                idle += POLL_SECONDS
                if idle >= HEARTBEAT_SECONDS:
                    idle = 0.0
                    yield ": keep-alive\n\n"

    return StreamingResponse(pump(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "Connection": "keep-alive",
                                      "X-Accel-Buffering": "no"})


@router.post("/push-record/{table}", status_code=201)
def push_record(table: str, req: PushRecord) -> dict:
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    kc, cfg = _kc()
    try:
        result = kc.record_push(table, sections=req.sections, actor=req.actor,
                                note=req.note, builds_root=_builds_root(),
                                graph_root=_graph_root(), config=cfg)
    except KeyError:
        return {"available": True, "found": False, "table": table}
    return {"available": True, "found": True, **result}


@router.post("/witness-import", status_code=201)
def witness_import(req: WitnessImport) -> dict:
    """The read-back loader: a catalog export becomes pending quads
    under ``witness: kc``. Loader only; the review surface comes later."""
    reason = _no_build()
    if reason:
        return _unavailable(reason)
    _silo_import()
    import datetime as _dt
    from sahs.kc.fold import open_build
    from sahs.kc.witness import import_kc_export
    build = open_build(_builds_root())
    run_id = f"kc_import_{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    payload = req.payload if isinstance(req.payload, dict) else {"items": req.payload}
    report = import_kc_export(_graph_root(), req.kind, payload, run_id=run_id,
                              actor=req.actor, known_tables=set(build.schema))
    return {"available": True, "run_id": run_id, **report}
