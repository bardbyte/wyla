#!/usr/bin/env python3
"""Vertex/Gemini readiness probe — is the SVC ID fully set up, which
models can it call, and which properties does each model support?

Run on the work laptop (VPN + SA key), paste the SUMMARY back:

    python scripts/probe_vertex_readiness.py                    # full matrix
    python scripts/probe_vertex_readiness.py --env-only         # no network
    python scripts/probe_vertex_readiness.py --models gemini-3.1-pro-preview
    python scripts/probe_vertex_readiness.py --ca-bundle ~/corp-root.pem
    python scripts/probe_vertex_readiness.py --auth key         # explicit SA key

What it checks, in order:

  1. ENV + KEY (offline): the exact env the pipeline's VertexLLMClient
     reads (GOOGLE_GENAI_USE_VERTEXAI, GOOGLE_CLOUD_PROJECT/LOCATION,
     GOOGLE_APPLICATION_CREDENTIALS, GEMINI_*); SA key shape (type,
     project match, client_email) — never the private key.
  2. MODEL INVENTORY (best effort): what `models.list` says this
     project/SVC can see.
  3. CAPABILITY MATRIX: for each candidate model, tiny probes for
     plain generation, JSON mime mode, dynamic thinking (with thought
     tokens as proof it engaged), thinking-off support, simple
     response_schema enforcement, and context-window metadata.
     404 = model not enabled · 403 = IAM role missing · 429 = quota.
  4. BUNDLE SMOKE: the REAL production path — synapse's VertexLLMClient
     + the real enrichment skill.md against a 2-column toy table, on the
     best pro and best flash found. Proves the whole contract (prompt →
     thinking → JSON → parse → corrective retry) end to end.
  5. RECOMMENDATION: exact `export GEMINI_MODEL_PRO/FLASH=...` lines for
     the pipeline's tiered enrichment strategy.

Total cost ≈ 30 tiny calls (≤1K tokens each) + 2 bundle smokes (~4K
tokens each) — negligible. Exit code 0 when at least one model is fully
usable (plain + JSON mode).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "synapse"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

GREEN, YELLOW, RED, DIM, END = (
    "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m")

# env the pipeline's VertexLLMClient actually reads (value shown as-is;
# GOOGLE_APPLICATION_CREDENTIALS shows the PATH only, never contents)
ENV_VARS = [
    "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GEMINI_MODEL",
    "GEMINI_MODEL_PRO",
    "GEMINI_MODEL_FLASH",
    "GEMINI_THINKING_BUDGET",
    "GEMINI_MAX_CONTEXT_CHARS",
]

# preference ladders — first usable wins the recommendation
PRO_LADDER = [
    "gemini-3.1-pro-preview",
    "gemini-3-pro-preview",
    "gemini-2.5-pro",
]
FLASH_LADDER = [
    "gemini-3.1-flash-preview",
    "gemini-3.1-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
]

SIMPLE_SCHEMA = {                       # flat: no $ref/anyOf (Vertex-safe)
    "type": "OBJECT",
    "properties": {"ok": {"type": "BOOLEAN"}},
    "required": ["ok"],
}

# 2-column toy table for the end-to-end bundle smoke — the REAL skill.md
# + VertexLLMClient path, so a pass here means `--enrich` will work
TOY_COLUMNS = {"acct_id", "status_code"}
TOY_CONTEXT: dict[str, Any] = {
    "inspection": {
        "identity": {"table": "probe_txn_summary"},
        "columns": [
            {"name": "acct_id", "data_type": "STRING", "description": ""},
            {"name": "status_code", "data_type": "STRING",
             "sample_values": ["A", "C", "V"]},
        ],
    },
    "corpus_sql_evidence": {
        "queries": ["SELECT status_code, COUNT(*) FROM probe_txn_summary "
                    "WHERE status_code = 'A' GROUP BY 1"],
        "n_queries_total": 1,
    },
    "skills_evidence": [],
    "tables_in_scope": [
        {"table": "probe_txn_summary", "columns": sorted(TOY_COLUMNS)}],
    "steward_glossary": [],
    "batch": {"chunk": 1, "of": 1, "columns_in_chunk": 2, "total_columns": 2},
}


def _classify(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    up = text.upper()
    if "404" in up or "NOT_FOUND" in up:
        return "not-enabled"
    if "403" in up or "PERMISSION_DENIED" in up:
        return "forbidden"
    if "429" in up or "RESOURCE_EXHAUSTED" in up:
        return "quota"
    if "400" in up or "INVALID_ARGUMENT" in up:
        return "unsupported-config"
    return type(exc).__name__


# ─── 1. env + key (offline, stdlib only) ─────────────────────


def env_and_key_report() -> dict[str, Any]:
    report: dict[str, Any] = {"env": {}, "key": {}, "warnings": []}
    for var in ENV_VARS:
        report["env"][var] = os.environ.get(var) or None

    if (report["env"]["GOOGLE_GENAI_USE_VERTEXAI"] or "").lower() not in (
            "true", "1", "yes"):
        report["warnings"].append(
            "GOOGLE_GENAI_USE_VERTEXAI is not 'true' — genai.Client() will "
            "target the consumer API (needs GOOGLE_API_KEY), NOT Vertex; "
            "the SVC ID would never be used. export "
            "GOOGLE_GENAI_USE_VERTEXAI=true")

    key_path_raw = report["env"]["GOOGLE_APPLICATION_CREDENTIALS"]
    if not key_path_raw:
        report["key"]["present"] = False
        report["warnings"].append(
            "GOOGLE_APPLICATION_CREDENTIALS unset — auth falls back to "
            "gcloud ADC if you've run `gcloud auth application-default "
            "login`; otherwise calls will fail")
        return report

    key_path = Path(key_path_raw).expanduser()
    report["key"]["path"] = str(key_path)
    report["key"]["present"] = key_path.exists()
    if not key_path.exists():
        report["warnings"].append(f"key file not found: {key_path}")
        return report
    try:
        key_path.resolve().relative_to(REPO_ROOT)
        report["warnings"].append(
            "SA key lives INSIDE the repo — move it out (e.g. ~/.gcp/) "
            "before you commit anything")
    except ValueError:
        pass  # outside the repo — good
    try:
        key = json.loads(key_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report["warnings"].append(f"key file unparseable: {exc}")
        return report
    report["key"]["type"] = key.get("type")
    report["key"]["project_id"] = key.get("project_id")
    report["key"]["client_email"] = key.get("client_email")
    if key.get("type") != "service_account":
        report["warnings"].append(
            f"key type is {key.get('type')!r}, expected 'service_account'")
    env_project = report["env"]["GOOGLE_CLOUD_PROJECT"]
    if env_project and key.get("project_id") and \
            env_project != key.get("project_id"):
        report["warnings"].append(
            f"GOOGLE_CLOUD_PROJECT ({env_project}) != key project_id "
            f"({key.get('project_id')}) — quota/billing may hit the wrong "
            "project")
    return report


# ─── 2+3. live probes ────────────────────────────────────────


def make_client(args: argparse.Namespace):
    """env mode = EXACTLY what the pipeline's VertexLLMClient does."""
    from google import genai  # lazy — --env-only works without it
    if args.project:
        os.environ["GOOGLE_CLOUD_PROJECT"] = args.project
    if args.location:
        os.environ["GOOGLE_CLOUD_LOCATION"] = args.location
    if args.auth == "key":
        from google.oauth2 import service_account
        key_file = args.key_file or os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS")
        if not key_file:
            raise RuntimeError("--auth key needs --key-file or "
                               "GOOGLE_APPLICATION_CREDENTIALS")
        creds = service_account.Credentials.from_service_account_file(
            str(Path(key_file).expanduser()),
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return genai.Client(
            vertexai=True,
            project=os.environ.get("GOOGLE_CLOUD_PROJECT"),
            location=os.environ.get("GOOGLE_CLOUD_LOCATION") or "global",
            credentials=creds)
    return genai.Client()      # env-driven — the pipeline's exact path


def model_inventory(client) -> list[str]:
    """Best-effort `models.list` — the capability matrix is ground truth."""
    names: list[str] = []
    for kwargs in ({"config": {"query_base": True}}, {}):
        try:
            for m in client.models.list(**kwargs):
                name = getattr(m, "name", "") or ""
                if "gemini" in name.lower():
                    names.append(name.rsplit("/", 1)[-1])
            if names:
                break
        except Exception:
            continue
    return sorted(set(names))[:25]


def probe_model(client, model: str) -> dict[str, Any]:
    from google.genai import types

    report: dict[str, Any] = {"model": model, "probes": {}}
    try:
        info = client.models.get(model=model)
        report["input_token_limit"] = getattr(info, "input_token_limit", None)
        report["output_token_limit"] = getattr(
            info, "output_token_limit", None)
    except Exception as exc:
        report["metadata_error"] = _classify(exc)

    def attempt(name: str, config, prompt: str) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            resp = client.models.generate_content(
                model=model, contents=prompt, config=config)
            text = (resp.text or "").strip()
            usage = resp.usage_metadata
            out = {
                "ok": bool(text),
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "prompt_tokens": getattr(usage, "prompt_token_count", 0) or 0,
                "response_tokens": getattr(
                    usage, "candidates_token_count", 0) or 0,
                "thought_tokens": getattr(
                    usage, "thoughts_token_count", 0) or 0,
                "text_head": text[:60],
            }
        except Exception as exc:
            out = {
                "ok": False,
                "error": _classify(exc),
                "error_detail": str(exc)[:160],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            }
        report["probes"][name] = out
        return out

    plain = attempt(
        "plain",
        types.GenerateContentConfig(temperature=0.0),
        "Reply with exactly one word: ok")
    if not plain["ok"] and plain.get("error") in ("not-enabled", "forbidden"):
        return report      # unusable — don't burn 4 more calls on it

    jm = attempt(
        "json_mode",
        types.GenerateContentConfig(
            temperature=0.0, response_mime_type="application/json"),
        'Return exactly this JSON object: {"ok": true}')
    if jm["ok"]:
        try:
            jm["json_valid"] = isinstance(
                json.loads(jm.get("text_head", "")), dict)
        except json.JSONDecodeError:
            jm["json_valid"] = False

    td = attempt(
        "thinking_dynamic",
        types.GenerateContentConfig(
            temperature=0.0,
            thinking_config=types.ThinkingConfig(thinking_budget=-1)),
        "Reply with exactly one word: ok")
    td["thinking_engaged"] = bool(td.get("ok")) and \
        (td.get("thought_tokens") or 0) > 0

    attempt(
        "thinking_off",       # pro models typically REJECT budget=0
        types.GenerateContentConfig(
            temperature=0.0,
            thinking_config=types.ThinkingConfig(thinking_budget=0)),
        "Reply with exactly one word: ok")

    rs = attempt(
        "response_schema",
        types.GenerateContentConfig(
            temperature=0.0, response_mime_type="application/json",
            response_schema=SIMPLE_SCHEMA),
        "Is the sky blue on a clear day? Answer via the schema.")
    if rs["ok"]:
        try:
            rs["schema_respected"] = "ok" in json.loads(
                rs.get("text_head", ""))
        except json.JSONDecodeError:
            rs["schema_respected"] = False
    return report


def model_is_usable(report: dict[str, Any]) -> bool:
    probes = report.get("probes", {})
    return bool(probes.get("plain", {}).get("ok")) and \
        bool(probes.get("json_mode", {}).get("json_valid"))


# ─── 4. bundle smoke — the REAL production path ──────────────


def bundle_smoke(model: str) -> dict[str, Any]:
    from synapse.enrichment.vertex_client import (
        VertexLLMClient, _failure_note)
    skill_md = (REPO_ROOT / "synapse" / "synapse" / "enrichment"
                / "skill.md").read_text(encoding="utf-8")
    t0 = time.perf_counter()
    try:
        client = VertexLLMClient(model=model)
        bundle = client.enrich(
            skill_md=skill_md, context=TOY_CONTEXT,
            table_name="probe_txn_summary")
    except Exception as exc:
        return {"model": model, "ok": False,
                "error": f"{_classify(exc)}: {str(exc)[:160]}"}
    note = _failure_note(bundle)
    observed = [o.column_name for o in bundle.column_observations]
    imagined = [c for c in observed if c not in TOY_COLUMNS]
    return {
        "model": model,
        "ok": note is None and bool(observed),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "n_observations": len(observed),
        "imagined_columns": imagined,      # grounding-gate preview
        "n_code_resolutions": len(bundle.candidate_code_resolutions),
        "corrective_retries": client.stats["corrective_retries"],
        "thinking_fallbacks": client.stats["thinking_fallbacks"],
        "failure": note,
    }


# ─── 5. recommendation ───────────────────────────────────────


def recommend(model_reports: list[dict[str, Any]]) -> dict[str, Any]:
    usable = {r["model"] for r in model_reports if model_is_usable(r)}
    pro = next((m for m in PRO_LADDER if m in usable), None)
    flash = next((m for m in FLASH_LADDER if m in usable), None)
    if pro and flash:
        strategy = "tiered"
    elif pro or flash:
        strategy = "pro-only" if pro else "flash-only"
    else:
        strategy = "blocked"
    return {"pro": pro, "flash": flash, "strategy": strategy,
            "usable": sorted(usable)}


# ─── CLI ─────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="probe_vertex_readiness",
        description="Vertex/Gemini SVC-ID + model-capability probe")
    parser.add_argument("--models", help="comma list; default = pro+flash "
                        "ladders + $GEMINI_MODEL")
    parser.add_argument("--auth", choices=["env", "key"], default="env",
                        help="env (pipeline's exact path, default) or an "
                        "explicit SA key file")
    parser.add_argument("--key-file")
    parser.add_argument("--project")
    parser.add_argument("--location")
    parser.add_argument("--ca-bundle", help="corporate root CA .pem")
    parser.add_argument("--insecure", action="store_true",
                        help="disable TLS verification (trusted intranet "
                        "behind a known MITM proxy only)")
    parser.add_argument("--env-only", action="store_true",
                        help="offline: env + key shape only, no calls")
    parser.add_argument("--no-bundle-smoke", action="store_true")
    parser.add_argument("--report", default="data/probes/vertex_readiness.json")
    args = parser.parse_args(argv)

    print(f"\n{'═' * 24} VERTEX READINESS PROBE {'═' * 24}")

    # 1 — env + key, always
    ek = env_and_key_report()
    print("\nenv (what VertexLLMClient reads):")
    for var, value in ek["env"].items():
        mark = f"{GREEN}set{END}" if value else f"{DIM}·{END}"
        shown = value if value else ""
        print(f"  {var:32} {mark:12} {shown}")
    if ek["key"].get("present"):
        print(f"  SA key: type={ek['key'].get('type')} "
              f"project={ek['key'].get('project_id')} "
              f"email={ek['key'].get('client_email')}")
    for w in ek["warnings"]:
        print(f"  {YELLOW}⚠ {w}{END}")

    full_report: dict[str, Any] = {"env_and_key": ek}
    if args.env_only:
        vertexai_on = (ek["env"]["GOOGLE_GENAI_USE_VERTEXAI"] or "").lower() \
            in ("true", "1", "yes")
        key_ok = (ek["env"]["GOOGLE_APPLICATION_CREDENTIALS"] is None
                  or ek["key"].get("type") == "service_account")
        verdict_ok = vertexai_on and key_ok
        print(f"\nverdict (offline): "
              f"{'shape OK' if verdict_ok else 'fix warnings above'}")
        _save(args.report, full_report)
        return 0 if verdict_ok else 1

    # TLS bootstrap (reuses the proven corporate-proxy handling)
    if args.ca_bundle or args.insecure:
        from check_vertex_gemini import (
            _disable_ssl_verification, _set_ca_bundle)
        if args.ca_bundle:
            _set_ca_bundle(args.ca_bundle)
        if args.insecure:
            _disable_ssl_verification()

    try:
        client = make_client(args)
    except Exception as exc:
        print(f"\n{RED}✗ client init failed: {str(exc)[:200]}{END}")
        print("  (fix env/key per warnings above, or try --auth key)")
        _save(args.report, full_report)
        return 1
    print(f"\nauth-mode: {args.auth} "
          f"{'(same code path as pipeline --enrich)' if args.auth == 'env' else ''}")

    # 2 — inventory (decoration; matrix below is ground truth)
    inventory = model_inventory(client)
    full_report["inventory"] = inventory
    if inventory:
        print(f"models.list sees {len(inventory)} gemini model(s): "
              f"{', '.join(inventory[:8])}"
              + (" …" if len(inventory) > 8 else ""))
    else:
        print(f"{DIM}models.list unavailable/empty — probing candidates "
              f"directly{END}")

    # 3 — capability matrix
    if args.models:
        candidates = [m.strip() for m in args.models.split(",") if m.strip()]
    else:
        env_model = os.environ.get("GEMINI_MODEL")
        candidates = list(dict.fromkeys(
            ([env_model] if env_model else []) + PRO_LADDER + FLASH_LADDER))
    print(f"\nprobing {len(candidates)} model(s) × ≤5 properties "
          f"(tiny calls):\n")
    header = (f"  {'model':30} {'plain':6} {'json':6} {'think':10} "
              f"{'think0':7} {'schema':7} {'ctx-in':>9}")
    print(header)
    model_reports = []
    for model in candidates:
        r = probe_model(client, model)
        model_reports.append(r)
        p = r["probes"]

        def cell(name: str, extra_key: str | None = None) -> str:
            pr = p.get(name)
            if pr is None:
                return f"{DIM}–{END}"
            if pr.get("ok") and (extra_key is None or pr.get(extra_key)):
                if name == "thinking_dynamic":
                    return f"{GREEN}✓({pr.get('thought_tokens', 0)}t){END}"
                return f"{GREEN}✓{END}"
            if pr.get("ok"):
                return f"{YELLOW}~{END}"          # call ok, property didn't hold
            return f"{RED}✗{END}"

        limit = r.get("input_token_limit")
        ctx = (f"{limit // 1000}K" if isinstance(limit, int) and limit
               else "?")
        err = p.get("plain", {}).get("error")
        if err in ("not-enabled", "forbidden") and len(p) == 1:
            print(f"  {model:30} {RED}✗ {err}{END} "
                  f"{DIM}{p['plain'].get('error_detail', '')[:70]}{END}")
            continue
        print(f"  {model:30} {cell('plain'):15} "
              f"{cell('json_mode', 'json_valid'):15} "
              f"{cell('thinking_dynamic', 'thinking_engaged'):19} "
              f"{cell('thinking_off'):16} "
              f"{cell('response_schema', 'schema_respected'):16} {ctx:>9}")
    full_report["models"] = model_reports

    # 5 — recommendation (before smoke: smoke targets the winners)
    rec = recommend(model_reports)
    full_report["recommendation"] = rec

    # 4 — bundle smoke on the winners
    smokes = []
    if not args.no_bundle_smoke:
        targets = [m for m in (rec["pro"], rec["flash"]) if m]
        if targets:
            print(f"\nbundle smoke (REAL VertexLLMClient + skill.md, "
                  f"2-col toy table):")
        for model in targets:
            s = bundle_smoke(model)
            smokes.append(s)
            if s["ok"]:
                extra = (f"{RED} · {len(s['imagined_columns'])} IMAGINED "
                         f"column(s)!{END}" if s["imagined_columns"] else "")
                print(f"  {s['model']:30} {GREEN}✓{END} "
                      f"{s['n_observations']} obs · "
                      f"retries {s['corrective_retries']} · "
                      f"{s['latency_ms']} ms{extra}")
            else:
                print(f"  {s['model']:30} {RED}✗ "
                      f"{s.get('failure') or s.get('error')}{END}")
    full_report["bundle_smoke"] = smokes

    # ── SUMMARY (paste this back) ────────────────────────────
    print(f"\n{'═' * 26} SUMMARY (paste this back) {'═' * 26}")
    env = ek["env"]
    print(f"env: vertexai={env['GOOGLE_GENAI_USE_VERTEXAI']} "
          f"project={env['GOOGLE_CLOUD_PROJECT']} "
          f"location={env['GOOGLE_CLOUD_LOCATION']} "
          f"sa={ek['key'].get('client_email', 'ADC')}")
    for w in ek["warnings"]:
        print(f"warn: {w}")
    for r in model_reports:
        p = r["probes"]
        if not p:
            continue
        bits = []
        for name, key in (("plain", None), ("json_mode", "json_valid"),
                          ("thinking_dynamic", "thinking_engaged"),
                          ("thinking_off", None),
                          ("response_schema", "schema_respected")):
            pr = p.get(name)
            if pr is None:
                bits.append(f"{name}=–")
            elif pr.get("ok") and (key is None or pr.get(key)):
                tok = (f":{pr.get('thought_tokens')}t"
                       if name == "thinking_dynamic" else "")
                bits.append(f"{name}=ok{tok}")
            else:
                bits.append(f"{name}={pr.get('error', 'no')}")
        print(f"model {r['model']}: {' '.join(bits)} "
              f"ctx={r.get('input_token_limit')}")
    for s in smokes:
        print(f"smoke {s['model']}: ok={s['ok']} "
              f"obs={s.get('n_observations')} "
              f"imagined={s.get('imagined_columns')} "
              f"retries={s.get('corrective_retries')} "
              f"failure={s.get('failure') or s.get('error')}")
    print(f"strategy: {rec['strategy']}")
    if rec["pro"]:
        print(f"  export GEMINI_MODEL_PRO={rec['pro']}")
    if rec["flash"]:
        print(f"  export GEMINI_MODEL_FLASH={rec['flash']}")
    if rec["strategy"] == "tiered":
        print("  pipeline: --enrich --enrich-strategy tiered   "
              "(chunk 1 + narrow tables → PRO; chunks 2..N of wide "
              "tables → FLASH)")
    elif rec["strategy"] == "blocked":
        print(f"  {RED}no model passed plain+json — check 403/404 "
              f"details above{END}")

    _save(args.report, full_report)
    return 0 if rec["strategy"] != "blocked" else 1


def _save(path_str: str, report: dict[str, Any]) -> None:
    path = Path(path_str).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str),
                    encoding="utf-8")
    print(f"{DIM}full report → {path}{END}")


if __name__ == "__main__":
    raise SystemExit(main())
