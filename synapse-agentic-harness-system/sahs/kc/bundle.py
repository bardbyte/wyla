"""The bundle: one table's enrichment, assembled, rendered, and (when
asked) written by the model — the public API of the package.

    bundle = build_bundle("dw.gms_transaction", use_llm=False)

Deterministic sections re-assemble on every load; the model's sections
come from a cache keyed (table, build id, prompt version), so a second
load makes zero model calls and ``regenerate`` re-runs only the model.
The blind gate runs once per build and prompt version before the first
real generation; its tier badges every model-written block."""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.ask.events import EventBus
from sahs.kc import coverage
from sahs.kc.assemble import FactSet, assemble
from sahs.kc.config import KcConfig, load_config
from sahs.kc.render import (Section, aspect_type_templates, aspects_payload,
                            guide, ledger, render_sections, suggestions)
from sahs.kc.write import (KC_EVENTS, Writer, bundle_dir, cache_path,
                           default_client, gate_path, load_json, run_gate,
                           save_json)
from sahs.tools.api import Build

SILO = Path(__file__).resolve().parents[2]


def builds_root_default() -> Path:
    return Path(os.environ.get("MERIDIAN_BUILDS_DIR", SILO / "builds"))


def graph_root_default() -> Path:
    return Path(os.environ.get("MERIDIAN_GRAPH_DIR", SILO / "graph"))


@dataclass
class Bundle:
    table: str
    build_id: str
    graph_run: str
    prompt_version: str
    digest: str
    facts: list[dict[str, Any]]
    counts: dict[str, int]
    sections: dict[str, Section]
    ledger: list[dict[str, Any]]
    aspects: dict[str, Any]
    aspect_types: list[dict[str, Any]]
    suggestions: list[dict[str, Any]]
    guide: list[dict[str, str]]
    llm: dict[str, Any]
    gate: dict[str, Any]
    push_records: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sections"] = {k: v.to_dict() for k, v in self.sections.items()}
        payload["section_order"] = list(self.sections)
        return payload


def push_records(graph_root: Path, cfg: KcConfig, table: str) -> list[dict[str, Any]]:
    """Every push record for the table, oldest first, across builds."""
    root = Path(graph_root) / cfg.push_record_dir / table.replace(".", "__")
    records = []
    if not root.exists():
        return records
    for path in sorted(root.glob("*/push_*.json")):
        payload = load_json(path)
        if payload:
            records.append(payload)
    return sorted(records, key=lambda r: r.get("ts", ""))


def gate_for(graph_root: Path, cfg: KcConfig, build_id: str) -> dict[str, Any]:
    return load_json(gate_path(graph_root, cfg, build_id)) or {
        "tier": "not run", "rate": None, "n": 0, "recovered": 0,
        "reason": "the blind gate has not run for this build and prompt version"}


def _llm_status(cfg: KcConfig, use_llm: bool, cached: dict | None,
                fresh: dict | None, reason: str) -> dict[str, Any]:
    record = fresh or cached
    return {
        "enabled": bool(use_llm),
        "cached": fresh is None and cached is not None,
        "generated": fresh is not None,
        "calls": (fresh or {}).get("calls", 0),
        "invalid_json": (record or {}).get("invalid_json", 0),
        "dropped": len((record or {}).get("dropped", [])),
        "stopped": (record or {}).get("stopped", ""),
        "usage": (record or {}).get("usage", {}),
        "budget": (record or {}).get("budget", {}),
        "model": (record or {}).get("model", ""),
        "generated_at": (record or {}).get("generated_at", ""),
        "prompt_version": cfg.prompt_version,
        "reason": reason,
        "dropped_sentences": (record or {}).get("dropped", []),
    }


def build_bundle(table: str, *, use_llm: bool = True, regenerate: bool = False,
                 client: Any = None, builds_root: Path | None = None,
                 graph_root: Path | None = None, config: KcConfig | None = None,
                 bus: EventBus | None = None, run_gate_first: bool = True,
                 log: Callable[[str], None] | None = None) -> Bundle:
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    log = log or (lambda _m: None)
    build = Build.open(builds_root)
    fs = assemble(build, graph_root, table)
    out_dir = bundle_dir(graph_root, cfg, table, build.version)
    cached = load_json(cache_path(graph_root, cfg, table, build.version))
    if cached and cached.get("facts_digest") != fs.digest():
        cached = None            # the knowledge moved under the prose
    fresh: dict | None = None
    reason = ""
    gate = gate_for(graph_root, cfg, build.version)
    if use_llm and (regenerate or cached is None):
        try:
            model = client or default_client(log)
        except Exception as exc:          # no Vertex contract on this machine
            model = None
            reason = f"model unavailable: {exc}"
        if model is not None:
            if run_gate_first and (gate.get("tier") == "not run" or regenerate):
                gate = run_gate(fs, model, cfg, log)
                save_json(gate_path(graph_root, cfg, build.version), gate)
            if gate.get("tier") == "halt":
                reason = (f"blind gate halted ({gate.get('line', gate.get('rate'))}): "
                          "no model-written block is offered for copying")
            else:
                events_path = out_dir / "events.jsonl"
                writer_bus = bus or EventBus(f"kc:{table}", path=events_path,
                                             events=KC_EVENTS)
                writer = Writer(model, cfg, bus=writer_bus, log=log)
                try:
                    fresh = writer.write(fs)
                except Exception as exc:
                    writer_bus.emit("error", reason=str(exc)[:400])
                    reason = f"model call failed: {exc}"
                else:
                    save_json(cache_path(graph_root, cfg, table, build.version), fresh)
                    save_json(out_dir / "usage.json",
                              {"usage": fresh.get("usage"), "budget": fresh.get("budget"),
                               "calls": fresh.get("calls"),
                               "generated_at": fresh.get("generated_at")})
    elif not use_llm:
        reason = "model sections off for this load (deterministic sections only)"
    llm_record = fresh or cached
    tier = gate.get("tier", "not run")
    llm_for_render = llm_record if (llm_record and tier != "halt") else None
    records = push_records(graph_root, cfg, table)
    sections = render_sections(fs, cfg, llm_for_render, gate_tier=tier,
                               push_records=records)
    return Bundle(
        table=table, build_id=build.version, graph_run=fs.graph_run,
        prompt_version=cfg.prompt_version, digest=fs.digest(),
        facts=[f.to_dict() for f in fs.facts], counts=fs.counts,
        sections=sections, ledger=ledger(fs), aspects=aspects_payload(fs, cfg),
        aspect_types=aspect_type_templates(cfg), suggestions=suggestions(fs),
        guide=guide(cfg), llm=_llm_status(cfg, use_llm, cached, fresh, reason),
        gate=gate, push_records=records)


def table_summary(table: str, *, builds_root: Path, graph_root: Path,
                  cfg: KcConfig, build: Build | None = None) -> dict[str, Any]:
    """The list row: coverage numbers, gate tier, cache state, last push."""
    build = build or Build.open(builds_root)
    fs = assemble(build, graph_root, table)
    cached = load_json(cache_path(graph_root, cfg, table, build.version))
    records = push_records(graph_root, cfg, table)
    total = max(1, len(fs.facts))
    copy = fs.counts.get("copy", 0)
    review = fs.counts.get("review", 0)
    never = fs.counts.get("never", 0)
    lob = ""
    for row in build.lob:
        if table in (row.get("tables") or []):
            lob = str(row.get("code") or row.get("lob") or "")
            break
    return {
        "physical": table, "lob": lob, "facts": len(fs.facts),
        "by_kind": {k[5:]: v for k, v in fs.counts.items() if k.startswith("kind:")},
        "copy": copy, "review": review, "never": never,
        "pct_copy": round(100 * copy / total, 1),
        "pct_review": round(100 * review / total, 1),
        "pct_never": round(100 * never / total, 1),
        "gate": gate_for(graph_root, cfg, build.version).get("tier", "not run"),
        "cached": bool(cached and cached.get("facts_digest") == fs.digest()),
        "last_push": records[-1] if records else None,
        "build_id": build.version,
    }


def list_tables(*, builds_root: Path | None = None, graph_root: Path | None = None,
                config: KcConfig | None = None) -> dict[str, Any]:
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    build = Build.open(builds_root)
    configured = [t for t in cfg.tables if t in build.schema]
    unknown = [t for t in cfg.tables if t not in build.schema]
    fallback = not cfg.tables
    tables = configured if configured else sorted(build.schema)
    note = ""
    if fallback:
        shown = cfg.path or "config/kc.yaml"
        try:
            shown = str(Path(shown).resolve().relative_to(SILO))
        except ValueError:
            shown = Path(shown).name
        note = (f"{shown} names no tables yet: showing "
                f"every table in build {build.version}")
    elif unknown:
        note = f"{len(unknown)} configured table(s) not in this build: {', '.join(unknown)}"
    return {"available": True, "build_id": build.version, "fallback": fallback,
            "note": note, "unknown": unknown, "config_path": cfg.path,
            "rows": [table_summary(t, builds_root=builds_root, graph_root=graph_root,
                                   cfg=cfg, build=build) for t in tables]}


def record_push(table: str, *, sections: list[str], actor: str, note: str = "",
                builds_root: Path | None = None, graph_root: Path | None = None,
                config: KcConfig | None = None) -> dict[str, Any]:
    """A human says these sections were entered into the catalog: write
    the clerk quad (actor-signed) and the JSON record next to the bundle."""
    from sahs.graph.clerk import record_kc_push
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    bundle = build_bundle(table, use_llm=False, builds_root=builds_root,
                          graph_root=graph_root, config=cfg)
    known = set(bundle.sections)
    chosen = sorted(s for s in sections if s in known and s != "push")
    if not chosen:
        return {"recorded": False, "reason": "no known section named"}
    hashes = {s: hashlib.sha256((bundle.sections[s].text or "").encode("utf-8")).hexdigest()[:12]
              for s in chosen}
    now = _dt.datetime.now(_dt.timezone.utc)
    ts = now.isoformat(timespec="seconds")
    stamp = now.strftime("%Y%m%dT%H%M%SZ")          # id grammar: [A-Za-z0-9_-]
    run_id = f"kc_{bundle.build_id}_{stamp}"
    ok, message = record_kc_push(graph_root, table, build_id=bundle.build_id,
                                 sections=chosen, hashes=hashes, actor=actor,
                                 run_id=run_id, note=note)
    if not ok:
        return {"recorded": False, "reason": message}
    record = {"ts": ts, "actor": actor, "build": bundle.build_id, "table": table,
              "sections": chosen, "hashes": hashes, "run_id": run_id,
              "prompt_version": cfg.prompt_version, "note": note,
              "facts_digest": bundle.digest}
    out = bundle_dir(graph_root, cfg, table, bundle.build_id)
    save_json(out / f"push_{stamp}.json", record)
    return {"recorded": True, "record": record, "message": message}


def coverage_payload(*, builds_root: Path | None = None,
                     graph_root: Path | None = None) -> dict[str, Any]:
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    build = Build.open(builds_root)
    report = coverage.coverage_report(graph_root, build.root)
    return {"available": True, "build_id": build.version, **report,
            "dictionary": coverage.dictionary(report)}
