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
import io
import json
import os
import zipfile
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.ask.events import EventBus
from sahs.kc import coverage
from sahs.kc.assemble import FactSet, _graph_stamp, assemble
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


# the assemble-derived part of a list row, per (graph state, build,
# config): 46 tables re-assembling on every list load is the difference
# between a page and a wait. Push records and the model cache are read
# fresh each time (they change without the graph changing).
_SUMMARIES: dict[tuple, dict[str, dict[str, Any]]] = {}


def _summary_key(graph_root: Path, build: Build, cfg: KcConfig) -> tuple:
    return (str(graph_root), _graph_stamp(graph_root), build.version,
            cfg.prompt_version, tuple(sorted(cfg.aspect_types.items())))


def _assembled_summary(table: str, *, graph_root: Path, cfg: KcConfig,
                       build: Build) -> dict[str, Any]:
    key = _summary_key(graph_root, build, cfg)
    if key not in _SUMMARIES:
        _SUMMARIES.clear()
        _SUMMARIES[key] = {}
    held = _SUMMARIES[key].get(table)
    if held is not None:
        return held
    fs = assemble(build, graph_root, table)
    sections = render_sections(fs, cfg)
    total = max(1, len(fs.facts))
    copy = fs.counts.get("copy", 0)
    review = fs.counts.get("review", 0)
    never = fs.counts.get("never", 0)
    lob = ""
    for row in build.lob:
        if table in (row.get("tables") or []):
            lob = str(row.get("code") or row.get("lob") or "")
            break
    summary = {
        "physical": table, "lob": lob, "facts": len(fs.facts),
        "digest": fs.digest(),
        "by_kind": {k[5:]: v for k, v in fs.counts.items() if k.startswith("kind:")},
        "copy": copy, "review": review, "never": never,
        "pct_copy": round(100 * copy / total, 1),
        "pct_review": round(100 * review / total, 1),
        "pct_never": round(100 * never / total, 1),
        # what each section would yield: ready (has copy text), how
        # many items wait for review, or the reason it is empty
        "sections": {key: {"ready": bool(sec.text), "review": len(sec.review),
                           "facts": len(sec.facts_used),
                           "empty_reason": sec.empty_reason}
                     for key, sec in sections.items() if key != "push"},
        "columns": len(build.schema.get(table) or {}),
        "metrics": sum(1 for m in build.metrics if m.get("table") == table),
    }
    _SUMMARIES[key][table] = summary
    return summary


def table_summary(table: str, *, builds_root: Path, graph_root: Path,
                  cfg: KcConfig, build: Build | None = None) -> dict[str, Any]:
    """The list row: coverage numbers, section readiness, gate tier,
    cache state, last push."""
    build = build or Build.open(builds_root)
    summary = dict(_assembled_summary(table, graph_root=graph_root, cfg=cfg,
                                      build=build))
    cached = load_json(cache_path(graph_root, cfg, table, build.version))
    records = push_records(graph_root, cfg, table)
    summary.update({
        "gate": gate_for(graph_root, cfg, build.version).get("tier", "not run"),
        "cached": bool(cached and cached.get("facts_digest") == summary["digest"]),
        "last_push": records[-1] if records else None,
        "build_id": build.version,
    })
    return summary


def list_tables(*, builds_root: Path | None = None, graph_root: Path | None = None,
                config: KcConfig | None = None) -> dict[str, Any]:
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    build = Build.open(builds_root)
    configured = [t for t in cfg.tables if t in build.schema]
    unknown = [t for t in cfg.tables if t not in build.schema]
    # scope: every table in the build unless the config names a subset;
    # both are legitimate modes, and the page says which one it is in
    scope = "configured" if configured else "all"
    tables = configured if configured else sorted(build.schema)
    shown = cfg.path or "config/kc.yaml"
    try:
        shown = str(Path(shown).resolve().relative_to(SILO))
    except ValueError:
        shown = Path(shown).name
    if scope == "all":
        note = (f"scope: every table in build {build.version} ({len(tables)}); "
                f"name a subset under tables: in {shown} to narrow")
    else:
        note = f"scope: {len(tables)} table(s) named in {shown}"
    if unknown:
        note += f" · {len(unknown)} named table(s) not in this build: {', '.join(unknown)}"
    rows = [table_summary(t, builds_root=builds_root, graph_root=graph_root,
                          cfg=cfg, build=build) for t in tables]
    lobs = sorted({r["lob"] for r in rows if r["lob"]})
    return {"available": True, "build_id": build.version, "scope": scope,
            "fallback": scope == "all", "note": note, "unknown": unknown,
            "config_path": cfg.path, "lobs": lobs,
            "totals": {"tables": len(rows),
                       "facts": sum(r["facts"] for r in rows),
                       "copy": sum(r["copy"] for r in rows),
                       "review": sum(r["review"] for r in rows),
                       "never": sum(r["never"] for r in rows)},
            "rows": rows}


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


def _scope_tables(build: Build, cfg: KcConfig) -> list[str]:
    configured = [t for t in cfg.tables if t in build.schema]
    return configured if configured else sorted(build.schema)


def glossary_across(*, builds_root: Path | None = None, graph_root: Path | None = None,
                    config: KcConfig | None = None, use_llm: bool = False
                    ) -> dict[str, Any]:
    """Every glossary term across every table in scope, merged the way
    the catalog holds them: a glossary is one per project, so a term a
    metric family measures on three tables is one term with three
    related entries, and a LOB category appears once. Each merged term
    names the tables it came from and their fact ids, so the ledger
    trail survives the merge."""
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    build = Build.open(builds_root)
    terms: dict[tuple[str, str], dict[str, Any]] = {}
    categories: dict[tuple[str, str], dict[str, Any]] = {}
    review: list[dict[str, Any]] = []
    tables = _scope_tables(build, cfg)
    for table in tables:
        bundle = build_bundle(table, use_llm=use_llm, builds_root=builds_root,
                              graph_root=graph_root, config=cfg)
        sec = bundle.sections["glossary"]
        if sec.items:
            for c in sec.items[0].get("categories", []):
                key = (str(c.get("name") or ""), str(c.get("parent") or ""))
                held = categories.setdefault(key, {**{k: v for k, v in c.items()
                                                      if k != "fact_ids"},
                                                   "tables": [], "fact_ids": {}})
                held["tables"].append(table)
                held["fact_ids"][table] = c.get("fact_ids", [])
            for t in sec.items[0].get("terms", []):
                key = (t["category"], t["term"])
                held = terms.get(key)
                if held is None:
                    held = terms[key] = {
                        "term": t["term"], "category": t["category"],
                        "definition": t.get("definition", ""),
                        "definition_llm": t.get("definition_llm", ""),
                        "status_note": t.get("status_note", ""),
                        "status": t.get("status", ""), "witness": set(),
                        "synonyms": [], "related_terms": [], "related_entries": [],
                        "contacts": [], "tables": [], "fact_ids": {}}
                held["witness"].update(w for w in str(t.get("witness", "")).split(",") if w)
                for field_name in ("synonyms", "related_terms", "related_entries", "contacts"):
                    for value in t.get(field_name, []):
                        if value not in held[field_name]:
                            held[field_name].append(value)
                if not held["definition"] and t.get("definition"):
                    held["definition"] = t["definition"]
                if not held["definition_llm"] and t.get("definition_llm"):
                    held["definition_llm"] = t["definition_llm"]
                held["tables"].append(table)
                held["fact_ids"][table] = t.get("fact_ids", [])
        for item in sec.review:
            review.append({**item, "table": table})
    merged_terms = sorted(({**t, "witness": ",".join(sorted(t["witness"]))}
                           for t in terms.values()),
                          key=lambda t: (t["category"], t["term"]))
    merged_categories = sorted(categories.values(),
                               key=lambda c: (c.get("parent") or "", c.get("name") or ""))
    by_category: dict[str, int] = defaultdict(int)
    for t in merged_terms:
        by_category[t["category"]] += 1
    return {"available": True, "build_id": build.version, "tables": tables,
            "categories": merged_categories, "terms": merged_terms,
            "by_category": dict(sorted(by_category.items())),
            "review": review, "glossary_path": cfg.glossary_path()}


def glossary_export(fmt: str, *, builds_root: Path | None = None,
                    graph_root: Path | None = None, config: KcConfig | None = None
                    ) -> tuple[bytes, str, str]:
    """The merged glossary as an import file: ``jsonl`` (glossary
    entries), ``links`` (entry links), ``sheet`` (the sheet columns)."""
    from sahs.kc.export import (entry_link_lines, glossary_lines,
                                glossary_sheet_lines)
    cfg = config or load_config()
    merged = glossary_across(builds_root=builds_root, graph_root=graph_root, config=cfg)
    meta = {"build": merged["build_id"], "tables": len(merged["tables"]),
            "scope": "merged glossary across every table in scope"}
    if fmt == "jsonl":
        return (glossary_lines(merged["categories"], merged["terms"], cfg, meta)
                .encode("utf-8"), "application/x-ndjson", "glossary_all.jsonl")
    if fmt == "links":
        related = [{"term": t["term"], "target": e}
                   for t in merged["terms"] for e in t.get("related_entries", [])]
        return (entry_link_lines(related, merged["terms"], cfg, meta).encode("utf-8"),
                "application/x-ndjson", "entry_links_all.jsonl")
    if fmt == "sheet":
        return (glossary_sheet_lines(merged["terms"]).encode("utf-8"), "text/csv",
                "glossary_all.csv")
    raise ValueError(f"unknown glossary export {fmt!r}: jsonl, links, sheet")


def export_all(*, builds_root: Path | None = None, graph_root: Path | None = None,
               config: KcConfig | None = None, use_llm: bool = False,
               log: Callable[[str], None] | None = None) -> tuple[bytes, str, str]:
    """One zip for every table in scope: a folder per table with the
    same members as the single-table zip, plus the merged glossary,
    its entry links and sheet at the root, and a manifest."""
    from sahs.kc.export import zip_members
    cfg = config or load_config()
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    log = log or (lambda _m: None)
    build = Build.open(builds_root)
    tables = _scope_tables(build, cfg)
    buffer = io.BytesIO()
    manifest: dict[str, Any] = {"build_id": build.version, "tables": {},
                                "generated_at": _dt.datetime.now(
                                    _dt.timezone.utc).isoformat(timespec="seconds")}
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for table in tables:
            bundle = build_bundle(table, use_llm=use_llm, builds_root=builds_root,
                                  graph_root=graph_root, config=cfg)
            for path, text in zip_members(bundle, cfg).items():
                zf.writestr(path, text)
            manifest["tables"][table] = {"facts": len(bundle.facts),
                                         "counts": bundle.counts, "digest": bundle.digest}
            log(f"  {table}: {len(bundle.facts)} facts")
        for fmt, name in (("jsonl", "glossary_all.jsonl"),
                          ("links", "entry_links_all.jsonl"),
                          ("sheet", "glossary_all.csv")):
            data, _media, _n = glossary_export(fmt, builds_root=builds_root,
                                               graph_root=graph_root, config=cfg)
            zf.writestr(name, data.decode("utf-8"))
        zf.writestr("manifest.json", json.dumps(manifest, indent=1, sort_keys=True))
    return buffer.getvalue(), "application/zip", f"kc_all_{build.version}.zip"


def coverage_payload(*, builds_root: Path | None = None,
                     graph_root: Path | None = None) -> dict[str, Any]:
    builds_root = Path(builds_root or builds_root_default())
    graph_root = Path(graph_root or graph_root_default())
    build = Build.open(builds_root)
    report = coverage.coverage_report(graph_root, build.root)
    return {"available": True, "build_id": build.version, **report,
            "dictionary": coverage.dictionary(report)}
