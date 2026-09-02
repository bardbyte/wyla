#!/usr/bin/env python3
"""state_report.py — everything the session needs to know about THIS
laptop in one paste: the graph, the promoted build, the enrichment
history, and what the Vertex SVC-ID can actually do.

    python scripts/state_report.py                # full report
    python scripts/state_report.py --no-vertex    # offline sections only
    python scripts/state_report.py --graph graph --builds builds
    cat docs/evals/state_report.md                # then PASTE it

Sections (each best-effort: a failing section prints why and the
report continues):

  graph        nodes by kind · edges by relation · quads by witness
               family · build-graph runs and the utilization ledger ·
               open reviews
  build        scripts/graph_state.py verbatim, then the detail it
               omits: metrics by business area, joins by tier, witness
               families on metrics, enrichment coverage (where each
               metric's question / grain / evidence came from), cost
               priors, the DIFF headline
  enrichment   every enrich run's blind-gate line and the last report
  vertex       one control call on the configured Pro model, then the
               capability probes Synapse v3 depends on — native
               function calling (and whether thought signatures come
               back), thinking level, streaming, JSON mode, context
               caching, model listing — then the Flash sweep from
               flash_check.py

Writes docs/evals/state_report.{md,json}. Exit 0 when the offline
sections load; 3 when Vertex was requested and cannot even bootstrap.
The laptop cannot push — the report travels by PASTE.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))

from sahs.util.auth import load_dotenv        # noqa: E402

load_dotenv()


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in
            path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _dist(values: Any) -> dict[str, int]:
    counter = collections.Counter(str(v or "(none)") for v in values)
    return dict(sorted(counter.items(), key=lambda kv: -kv[1]))


# ─── graph ───────────────────────────────────────────────────


def graph_section(graph: Path) -> dict[str, Any]:
    if not graph.exists():
        return {"error": f"no graph at {graph}"}
    nodes = {p.stem: len(_jsonl(p))
             for p in sorted((graph / "nodes").glob("*.jsonl"))}
    edges: dict[str, int] = {}
    witnesses: collections.Counter = collections.Counter()
    for p in sorted((graph / "edges").glob("*.jsonl")):
        rows = _jsonl(p)
        edges[p.stem] = len(rows)
        for row in rows:
            prov = row.get("prov") or {}
            witnesses[str(prov.get("witness") or row.get("witness")
                          or "(none)")] += 1
    runs = []
    for mp in sorted(graph.glob("runs/*/manifest.json")):
        m = _json(mp)
        util = (m.get("reports") or {}).get("utilization") or {}
        runs.append({"run": mp.parent.name, "run_id": m.get("run_id"),
                     "roots": m.get("roots"),
                     "utilization": util if isinstance(util, dict)
                     else str(util)[:200]})
    reviews = _jsonl(graph / "review.jsonl")
    return {
        "nodes": nodes, "edges": edges,
        "quads_by_witness": dict(witnesses.most_common()),
        "runs": runs[-5:], "runs_total": len(runs),
        "reviews_open": sum(1 for r in reviews if (r.get("props") or {})
                            .get("status", "open") == "open"),
        "reviews_total": len(reviews),
        "skills": sorted(p.name for p in (graph / "skills").glob("*.md"))
        if (graph / "skills").exists() else [],
    }


# ─── build ───────────────────────────────────────────────────


def build_section(builds: Path, graph: Path) -> dict[str, Any]:
    current = builds / "CURRENT"
    if not current.exists():
        return {"error": f"no promoted build: {current} missing"}
    build_id = current.read_text(encoding="utf-8").strip()
    bdir = builds / build_id
    manifest = _json(bdir / "manifest.json")
    metrics = _jsonl(bdir / "indexes" / "metrics.jsonl")
    joins = _jsonl(bdir / "indexes" / "joins.jsonl")
    lobs = _jsonl(bdir / "indexes" / "lob.jsonl")
    try:
        state = subprocess.run(
            [sys.executable, str(SILO / "scripts" / "graph_state.py"),
             "--graph", str(graph), "--builds", str(builds)],
            capture_output=True, text=True, timeout=120).stdout
    except Exception as exc:                        # noqa: BLE001
        state = f"(graph_state.py failed: {exc})"
    fams: collections.Counter = collections.Counter()
    for m in metrics:
        for f in (m.get("support_by_witness") or {}):
            fams[f] += 1
    diff = (bdir / "DIFF_vs_prev.md")
    return {
        "build_id": build_id,
        "counts": manifest.get("counts", {}),
        "graph_state": state.strip(),
        "metrics_by_status": _dist(m.get("status_served") or m.get("status")
                                   for m in metrics),
        "metrics_by_lob": _dist(m.get("line_of_business") for m in metrics),
        "metrics_by_domain": _dist(m.get("domain") for m in metrics),
        "witness_families_on_metrics": dict(fams.most_common()),
        "joins_by_tier": _dist(
            (j.get("tier") or ("certified" if j.get("certified")
                               else "witnessed" if j.get("witnessed")
                               else "candidate")) for j in joins),
        "enrichment_coverage": {
            "question_source": _dist(m.get("question_source")
                                     for m in metrics),
            "grain_source": _dist(m.get("grain_source") for m in metrics),
            "evidence_origin": _dist(m.get("evidence_origin")
                                     for m in metrics),
            "with_description": sum(1 for m in metrics
                                    if str(m.get("description") or "")
                                    .strip()),
        },
        "business_areas": [{"code": r.get("code"), "name": r.get("name"),
                            "domains": r.get("domains"),
                            "tables": r.get("tables")} for r in lobs],
        "cost_priors": len(_json(bdir / "indexes" / "cost_priors.json")),
        "tickets": len(_jsonl(bdir / "tickets.jsonl")),
        "diff_headline": (diff.read_text(encoding="utf-8").strip()
                          .splitlines()[:6] if diff.exists() else []),
    }


def enrichment_section(graph: Path) -> dict[str, Any]:
    runs = []
    for rp in sorted(graph.glob("runs/*/enrich_report.json")):
        rep = _json(rp)
        runs.append({"run": rp.parent.name,
                     "prompt_version": rep.get("prompt_version"),
                     "blind": rep.get("blind"),
                     "metrics_enriched": rep.get("metrics_enriched", 0),
                     "concepts_enriched": rep.get("concepts_enriched", 0),
                     "planned": {"metrics": rep.get("planned_metrics", 0),
                                 "concepts": rep.get("planned_concepts",
                                                     0)}})
    return {"runs": runs, "count": len(runs)}


# ─── vertex ──────────────────────────────────────────────────


def _classify(exc: Exception) -> tuple[str, str]:
    import flash_check
    return flash_check.classify(exc)


def _get(client: Any, url: str) -> tuple[str, Any]:
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {client._token()}"})
    try:
        with urllib.request.urlopen(
                request, timeout=30,
                context=client.connection.ssl_context()) as response:
            return "ok", json.loads(response.read().decode("utf-8"))
    except Exception as exc:                        # noqa: BLE001
        return _classify(exc)


def _parts(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return (((raw.get("candidates") or [{}])[0].get("content") or {})
            .get("parts") or [])


def probe_function_calling(client: Any) -> dict[str, Any]:
    body = {
        "contents": [{"role": "user", "parts": [{
            "text": "Find the certified spend metric. Use the tool."}]}],
        "tools": [{"functionDeclarations": [{
            "name": "search",
            "description": "Search the governed graph by meaning.",
            "parameters": {"type": "OBJECT", "properties": {
                "query": {"type": "STRING"}}, "required": ["query"]}}]}],
        "toolConfig": {"functionCallingConfig": {"mode": "ANY"}},
        "generationConfig": {"maxOutputTokens": 512}}
    started = time.perf_counter()
    try:
        raw = client._post(body)
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict, "detail": detail}
    parts = _parts(raw)
    calls = [p["functionCall"] for p in parts if "functionCall" in p]
    return {"verdict": "ok" if calls else "no_call",
            "detail": json.dumps(calls[0])[:120] if calls
            else json.dumps(parts)[:120],
            "thought_signature": any("thoughtSignature" in p
                                     for p in parts),
            "latency_ms": round((time.perf_counter() - started) * 1000)}


def probe_thinking(client: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for level in ("low", "LOW"):
        body = {"contents": [{"role": "user", "parts": [{
                    "text": "In one word, what colour is the sky?"}]}],
                "generationConfig": {
                    "maxOutputTokens": 256,
                    "thinkingConfig": {"thinkingLevel": level,
                                       "includeThoughts": True}}}
        try:
            raw = client._post(body)
        except Exception as exc:                    # noqa: BLE001
            out[level] = _classify(exc)[0]
            continue
        parts = _parts(raw)
        out[level] = "ok"
        out["thought_summaries"] = any(p.get("thought") for p in parts)
        out["thought_tokens"] = int((raw.get("usageMetadata") or {})
                                    .get("thoughtsTokenCount") or 0)
        break
    return out


def probe_streaming(client: Any) -> dict[str, Any]:
    url = client._url("streamGenerateContent") + "?alt=sse"
    body = {"contents": [{"role": "user", "parts": [{
                "text": "Count from one to five, one word per line."}]}],
            "generationConfig": {"maxOutputTokens": 256}}
    request = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {client._token()}",
                 "Content-Type": "application/json"}, method="POST")
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(
                request, timeout=60,
                context=client.connection.ssl_context()) as response:
            first = None
            chunks = 0
            for line in response:
                if line.startswith(b"data:"):
                    chunks += 1
                    if first is None:
                        first = round((time.perf_counter() - started)
                                      * 1000)
            return {"verdict": "ok" if chunks else "no_chunks",
                    "chunks": chunks, "first_chunk_ms": first}
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict, "detail": detail}


def probe_json_mode(client: Any) -> dict[str, Any]:
    body = {"contents": [{"role": "user", "parts": [{
                "text": 'Return exactly this JSON: {"ok": true}'}]}],
            "generationConfig": {"maxOutputTokens": 512,
                                 "responseMimeType": "application/json"}}
    try:
        raw = client._post(body)
        text = " ".join(str(p.get("text", "")) for p in _parts(raw)
                        if not p.get("thought")).strip()
        json.loads(text)
        return {"verdict": "ok"}
    except Exception as exc:                        # noqa: BLE001
        verdict, detail = _classify(exc)
        return {"verdict": verdict if verdict != "error"
                else "unparseable", "detail": detail[:120]}


def vertex_section(models: list[str] | None = None) -> dict[str, Any]:
    from sahs.enrich.client import VertexClient
    from sahs.util.auth import AuthError, VertexConnection
    import flash_check
    try:
        connection = VertexConnection.from_env()
    except AuthError as exc:
        return {"error": str(exc)}
    out: dict[str, Any] = {
        "project": connection.project, "location": connection.location,
        "endpoint": connection.endpoint, "model": connection.model,
        "truststore": connection.truststore_active,
        "proxy": bool(os.environ.get("HTTPS_PROXY")
                      or os.environ.get("https_proxy"))}
    control = VertexClient(connection)
    try:
        token = control._token()
    except Exception as exc:                        # noqa: BLE001
        out["error"] = f"token refresh failed: {exc}"
        return out
    client = VertexClient(connection, token_provider=lambda: token)
    print(f"  control call on {connection.model}…")
    out["control"] = flash_check.probe_answers(client)
    print(f"  function calling…")
    out["function_calling"] = probe_function_calling(client)
    print(f"  thinking level…")
    out["thinking"] = probe_thinking(client)
    print(f"  streaming…")
    out["streaming"] = probe_streaming(client)
    print(f"  json mode…")
    out["json_mode"] = probe_json_mode(client)
    c = connection
    base = f"{c.endpoint}/v1/projects/{c.project}/locations/{c.location}"
    out["cached_contents"] = _get(client, f"{base}/cachedContents")[0]
    verdict, listing = _get(client,
                            f"{base}/publishers/google/models")
    out["model_listing"] = {"verdict": verdict,
                            "count": len((listing or {}).get(
                                "publisherModels", []))
                            if isinstance(listing, dict) else 0}
    print("  flash sweep…")
    out["flash"] = flash_check.probe_all(
        connection, models or list(flash_check.DEFAULT_CANDIDATES),
        token=token)
    return out


# ─── render ──────────────────────────────────────────────────


def _kv(d: dict[str, Any]) -> str:
    return " · ".join(f"{k} {v}" for k, v in d.items()) or "(none)"


def render_markdown(report: dict[str, Any]) -> str:
    g, b, e, v = (report.get("graph", {}), report.get("build", {}),
                  report.get("enrichment", {}), report.get("vertex", {}))
    lines = [f"# Laptop state report — {report['generated_at']}", ""]
    lines += ["## graph", ""]
    if g.get("error"):
        lines.append(g["error"])
    else:
        lines += [f"- nodes: {_kv(g['nodes'])}",
                  f"- edges: {_kv(g['edges'])}",
                  f"- quads by witness: {_kv(g['quads_by_witness'])}",
                  f"- build-graph runs: {g['runs_total']}"
                  + (f" · latest {g['runs'][-1]['run']}"
                     if g["runs"] else ""),
                  f"- reviews: {g['reviews_open']} open of "
                  f"{g['reviews_total']}",
                  f"- user skills: {', '.join(g['skills']) or 'none'}"]
        if g["runs"]:
            lines.append("- utilization (latest run): "
                         + json.dumps(g["runs"][-1]["utilization"])[:400])
    lines += ["", "## build", ""]
    if b.get("error"):
        lines.append(b["error"])
    else:
        lines += ["```", b["graph_state"], "```", "",
                  f"- metrics by status: {_kv(b['metrics_by_status'])}",
                  f"- metrics by business area: {_kv(b['metrics_by_lob'])}",
                  f"- metrics by domain: {_kv(b['metrics_by_domain'])}",
                  f"- witness families on metrics: "
                  f"{_kv(b['witness_families_on_metrics'])}",
                  f"- joins by tier: {_kv(b['joins_by_tier'])}",
                  f"- enrichment coverage — question_source: "
                  f"{_kv(b['enrichment_coverage']['question_source'])}",
                  f"- enrichment coverage — grain_source: "
                  f"{_kv(b['enrichment_coverage']['grain_source'])}",
                  f"- enrichment coverage — evidence_origin: "
                  f"{_kv(b['enrichment_coverage']['evidence_origin'])}",
                  f"- metrics with a description: "
                  f"{b['enrichment_coverage']['with_description']}",
                  f"- business areas: " + "; ".join(
                      f"{a['code']} ({a['name']}; "
                      f"{', '.join(a.get('tables') or [])})"
                      for a in b["business_areas"]),
                  f"- cost priors: {b['cost_priors']} tables · tickets: "
                  f"{b['tickets']}"]
        if b["diff_headline"]:
            lines += ["- DIFF vs previous:"] + [
                f"  {ln}" for ln in b["diff_headline"]]
    lines += ["", "## enrichment", ""]
    if not e.get("runs"):
        lines.append("no enrich runs recorded")
    for r in e.get("runs", []):
        blind = r.get("blind") or {}
        lines.append(f"- {r['run']}: prompt {r.get('prompt_version')} · "
                     + (f"blind {blind.get('recovered')}/{blind.get('n')} "
                        f"→ {blind.get('tier')} · " if blind
                        else "plan only · ")
                     + f"wrote {r['metrics_enriched']} metrics + "
                       f"{r['concepts_enriched']} concepts")
    lines += ["", "## vertex", ""]
    if v.get("error"):
        lines.append(v["error"])
    elif v:
        lines += [f"- project {v['project']} · location {v['location']} · "
                  f"model {v['model']} · truststore {v['truststore']} · "
                  f"proxy {v['proxy']}",
                  f"- control ({v['model']}): {v['control'].get('verdict')} "
                  f"· {v['control'].get('latency_ms')} ms · "
                  f"thought tokens {v['control'].get('thought_tokens', '?')}",
                  f"- function calling: "
                  f"{v['function_calling'].get('verdict')} · thought "
                  f"signature returned: "
                  f"{v['function_calling'].get('thought_signature')} · "
                  f"{v['function_calling'].get('detail', '')[:100]}",
                  f"- thinking level: {json.dumps(v['thinking'])}",
                  f"- streaming: {json.dumps(v['streaming'])}",
                  f"- json mode: {v['json_mode'].get('verdict')}",
                  f"- context caching endpoint: {v['cached_contents']}",
                  f"- model listing: {json.dumps(v['model_listing'])}",
                  "", "### flash sweep", ""]
        import flash_check
        lines.append(flash_check.render_markdown(
            v["flash"], label=f"{v['project']} · {v['location']}")
            .split("\n", 2)[2])
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="state_report.py")
    parser.add_argument("--graph", default=os.environ.get(
        "MERIDIAN_GRAPH_DIR") or str(SILO / "graph"))
    parser.add_argument("--builds", default=os.environ.get(
        "MERIDIAN_BUILDS_DIR") or str(SILO / "builds"))
    parser.add_argument("--no-vertex", action="store_true",
                        help="skip every network probe")
    parser.add_argument("--models", default="",
                        help="Flash ids for the sweep (comma-separated)")
    parser.add_argument("--out", default=str(SILO / "docs" / "evals"))
    args = parser.parse_args(argv)
    graph, builds = Path(args.graph), Path(args.builds)

    report: dict[str, Any] = {
        "generated_at": dt.datetime.now(dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "graph_dir": str(graph), "builds_dir": str(builds)}
    for name, fn in (("graph", lambda: graph_section(graph)),
                     ("build", lambda: build_section(builds, graph)),
                     ("enrichment", lambda: enrichment_section(graph))):
        print(f"[{name}]")
        try:
            report[name] = fn()
        except Exception as exc:                    # noqa: BLE001
            report[name] = {"error": f"{type(exc).__name__}: {exc}"}
    if not args.no_vertex:
        print("[vertex]")
        models = [m.strip() for m in args.models.split(",") if m.strip()]
        try:
            report["vertex"] = vertex_section(models or None)
        except Exception as exc:                    # noqa: BLE001
            report["vertex"] = {"error": f"{type(exc).__name__}: {exc}"}

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "state_report.json").write_text(
        json.dumps(report, indent=1, default=str), encoding="utf-8")
    markdown = render_markdown(report)
    (out / "state_report.md").write_text(markdown, encoding="utf-8")
    print("\n" + markdown)
    print("→ PASTE docs/evals/state_report.md back into the session.")
    vertex_failed = (not args.no_vertex
                     and bool(report.get("vertex", {}).get("error")))
    return 3 if vertex_failed and "token" in str(
        report["vertex"].get("error", "")).lower() + "x" else 0


if __name__ == "__main__":
    raise SystemExit(main())
