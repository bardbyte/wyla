#!/usr/bin/env python3
"""Full MDM read-side probe for ONE table — run on the work laptop (VPN).

Exercises every endpoint the semantic-layer crawler uses (plus the spec's
documented alternatives and a few high-value extras), prints a compact
paste-back report, and saves every raw response in the crawler's own
cache layout — so a successful probe pre-populates the crawl:

    python scripts/probe_mdm_full.py --table roll_rate_calc \
        --base https://<mdm-host>/api/v1/ngbd/mdm-api \
        --out ./data/mdm_raw
    # then the pipeline replays from that cache, zero re-fetch:
    python synapse/scripts/pipeline.py --mdm-crawl roll_rate_calc \
        --mdm-raw-dir ./data/mdm_raw ...

Paste the SUMMARY block back to Claude to drive the crawler refactor.

Stdlib only (urllib). GETs only; same allowlist + hard deny of the
credential-bearing surfaces (api-polling-info / keys / key-schema-
mappings) as the crawler. Printed report shows SHAPES and presence
booleans, never contact addresses or token-like values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DENY = re.compile(r"api-polling-info|/keys\b|key-schema-mappings", re.I)
ALLOWED = ("/datasets", "/app-flows", "/portal-v2", "/portal/",
           "/table-lineages", "/attribute-lineage", "/lifecycle",
           "/schemas", "/health", "/ready", "/info", "/region")

GREEN, YELLOW, RED, DIM, END = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m"


def resolve_base(cli_base: str) -> str:
    if cli_base:
        return cli_base.rstrip("/")
    env = os.environ.get("SYNAPSE_MDM_BASE", "").strip()
    if env:
        return env.rstrip("/")
    legacy = os.environ.get("SYNAPSE_MDM_ENDPOINT", "").strip()
    if "/mdm-api" in legacy:
        return legacy[: legacy.index("/mdm-api") + len("/mdm-api")]
    return ""


class Probe:
    def __init__(self, base: str, out_dir: Path, table: str,
                 timeout: float, ca_bundle: str | None) -> None:
        self.base = base
        self.table_dir = out_dir / table
        self.timeout = timeout
        self.ctx = (ssl.create_default_context(cafile=ca_bundle)
                    if ca_bundle else ssl.create_default_context())
        self.rows: list[dict[str, Any]] = []

    def get(self, step: str, path_query: str, *, save_as: str | None = None,
            optional: bool = False) -> Any:
        """One guarded GET. Returns parsed JSON or None; records a row."""
        row: dict[str, Any] = {"step": step, "url": path_query}
        self.rows.append(row)
        if DENY.search(path_query) or not path_query.startswith(ALLOWED):
            row.update(status="DENIED", note="blocked by allowlist policy")
            return None
        url = f"{self.base}{path_query}"
        started = time.monotonic()
        try:
            req = urllib.request.Request(
                url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=self.timeout,
                                        context=self.ctx) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                code = resp.status
        except urllib.error.HTTPError as exc:
            row.update(status=f"HTTP_{exc.code}",
                       ms=int((time.monotonic() - started) * 1000),
                       note="optional endpoint" if optional else "")
            return None
        except Exception as exc:
            row.update(status="ERROR", note=str(exc)[:140])
            return None
        row["ms"] = int((time.monotonic() - started) * 1000)
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            row.update(status=f"HTTP_{code}", note="non-JSON body",
                       shape=f"text[{len(body)}]")
            return None
        row["status"] = f"HTTP_{code}"
        row["shape"] = describe_shape(data)
        if save_as:
            self.table_dir.mkdir(parents=True, exist_ok=True)
            (self.table_dir / save_as).write_text(
                json.dumps(data, indent=2, default=str), encoding="utf-8")
            row["saved"] = save_as
        return data


def describe_shape(data: Any) -> str:
    if isinstance(data, list):
        inner = (f" of {describe_shape(data[0])}" if data else "")
        return f"array[{len(data)}]{inner}"
    if isinstance(data, dict):
        keys = sorted(data.keys())
        head = ",".join(keys[:12]) + ("…" if len(keys) > 12 else "")
        return f"dict{{{head}}}"
    return type(data).__name__


def peel(data: Any) -> Any:
    if isinstance(data, list):
        return data[0] if data else {}
    if isinstance(data, dict) and isinstance(data.get("content"), list):
        return data["content"][0] if data["content"] else {}
    return data


def find_key(obj: Any, *names: str) -> Any:
    """First matching key, searching top level then one level down."""
    if not isinstance(obj, dict):
        return None
    for name in names:
        if obj.get(name) not in (None, "", []):
            return obj[name]
    for value in obj.values():
        if isinstance(value, dict):
            for name in names:
                if value.get(name) not in (None, "", []):
                    return value[name]
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--table", required=True)
    ap.add_argument("--base", default="", help="…/api/v1/ngbd/mdm-api "
                    "(or SYNAPSE_MDM_BASE / derived from SYNAPSE_MDM_ENDPOINT)")
    ap.add_argument("--out", default="./data/mdm_raw",
                    help="crawler-compatible raw cache root")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--ca-bundle", default=os.environ.get("REQUESTS_CA_BUNDLE")
                    or None, help="corporate root CA pem if system store lacks it")
    args = ap.parse_args()

    base = resolve_base(args.base)
    if not base:
        print(f"{RED}no base URL — pass --base or set SYNAPSE_MDM_BASE{END}")
        return 2
    table = args.table
    quoted = urllib.parse.quote(table)
    probe = Probe(base, Path(args.out).expanduser(), table,
                  args.timeout, args.ca_bundle)
    extracted: dict[str, Any] = {}

    print(f"\n{DIM}base={base}  table={table}{END}\n")

    # ── 0. connectivity ──────────────────────────────────────
    probe.get("health", "/health", optional=True)

    # ── 1. exists ────────────────────────────────────────────
    data = probe.get("exists",
                     f"/datasets/table-name/exists?tableName={quoted}",
                     save_as="exists.json")
    extracted["exists"] = find_key(peel(data), "exists")

    # ── 2. schema — BOTH variants; spec anchor first ─────────
    schema = probe.get(
        "schema_filter",
        f"/datasets/schemas/filter?tableName={quoted}&storageType=BigQuery",
        save_as="schema.json")
    if schema is None:
        schema = probe.get(
            "schema_plain",
            f"/datasets/schemas?tableName={quoted}",
            save_as="schema.json")
        extracted["schema_variant"] = "plain (/datasets/schemas)"
    else:
        # also try plain, report-only, to compare shapes
        probe.get("schema_plain",
                  f"/datasets/schemas?tableName={quoted}", optional=True)
        extracted["schema_variant"] = "filter (spec anchor)"
    peeled = peel(schema)
    extracted["dataset_id"] = find_key(peeled, "dataset_id", "datasetId")
    extracted["dataset_parent_id"] = find_key(
        peeled, "dataset_parent_id", "datasetParentId")
    attrs = (find_key(peeled, "schema") or {})
    attrs = attrs.get("schema_attributes") if isinstance(attrs, dict) else None
    extracted["n_columns"] = len(attrs) if isinstance(attrs, list) else 0
    if isinstance(attrs, list) and attrs:
        nested_sens = any(isinstance(a, dict) and a.get("sensitivity_details")
                          for a in attrs)
        top_sens = isinstance(peeled, dict) and bool(
            peeled.get("sensitivity_details"))
        extracted["sensitivity_shape"] = (
            "both" if nested_sens and top_sens
            else "nested-per-attribute" if nested_sens
            else "top-level-array" if top_sens else "NONE FOUND")
        extracted["first_attr_keys"] = sorted(attrs[0].keys())[:10]

    # ── 3. ownership (needs dataset_parent_id) ───────────────
    dpid = extracted.get("dataset_parent_id")
    if dpid:
        own = probe.get("ownership",
                        f"/datasets/{urllib.parse.quote(str(dpid))}/ownership",
                        save_as="ownership.json")
        own_p = peel(own)
        extracted["business_unit"] = find_key(own_p, "business_unit",
                                              "businessUnit")
        for k in ("business_contacts", "tech_contacts"):
            v = find_key(own_p, k)
            extracted[f"n_{k}"] = len(v) if isinstance(v, list) else 0
    else:
        probe.rows.append({"step": "ownership", "status": "SKIPPED",
                           "note": "no dataset_parent_id from schema"})

    # ── 4. appflow — by table AND by dataset parent ──────────
    appflow = probe.get("appflow",
                        f"/app-flows/cdm-storage?tableName={quoted}",
                        save_as="appflow.json")
    afp = find_key(peel(appflow), "parent_app_flow_id", "app_flow_parent_id",
                   "appflow_parent_id", "parentAppFlowId", "appFlowParentId")
    if dpid:
        by_parent = probe.get(
            "appflow_by_parent",
            f"/datasets/{urllib.parse.quote(str(dpid))}/appflow",
            save_as="appflow_by_parent.json", optional=True)
        if not afp:  # cdm-storage 500s on the real deployment — fall back
            first = peel(by_parent)
            afp = (find_key(first, "parent_app_flow_id", "app_flow_parent_id",
                            "appflow_parent_id", "parentAppFlowId",
                            "appFlowParentId", "app_flow_id", "appFlowId")
                   or (first.strip() if isinstance(first, str) else None))
            if afp:
                extracted["appflow_route"] = "by_parent (cdm-storage failed)"
    extracted["appflow_parent_id"] = afp

    # ── 5. pipeline (needs appflow parent) ───────────────────
    if afp:
        pipe = probe.get(
            "pipeline",
            "/portal-v2/pipeline/appflow-parent-id/"
            f"{urllib.parse.quote(str(afp))}/details",
            save_as="pipeline.json")
        pipe_p = peel(pipe)
        extracted["pipeline_id"] = find_key(pipe_p, "id", "pipeline_id")
        extracted["pipeline_business_unit"] = find_key(
            pipe_p, "business_unit", "businessUnit")
        gov = find_key(pipe_p, "governance")
        extracted["governance_keys"] = (sorted(gov.keys())[:12]
                                        if isinstance(gov, dict) else None)
    else:
        probe.rows.append({"step": "pipeline", "status": "SKIPPED",
                           "note": "no appflow parent id"})

    # ── 6. lineage, both directions ──────────────────────────
    up = probe.get("lineage_up",
                   f"/table-lineages/?tableName={quoted}&isSourceTableName=false",
                   save_as="lineage_up.json")
    down = probe.get("lineage_down",
                     f"/table-lineages/?tableName={quoted}&isSourceTableName=true",
                     save_as="lineage_down.json")
    for label, payload in (("up", up), ("down", down)):
        rows = payload if isinstance(payload, list) else \
            (payload or {}).get("content") if isinstance(payload, dict) else []
        rows = rows or []
        extracted[f"n_lineage_{label}"] = len(rows)
        if rows and isinstance(rows[0], dict):
            extracted[f"lineage_{label}_keys"] = sorted(rows[0].keys())[:12]

    # ── 7. attribute lineage ─────────────────────────────────
    attr = probe.get("attr_lineage",
                     f"/attribute-lineage?tableName={quoted}",
                     save_as="attr_lineage.json")
    rows = attr if isinstance(attr, list) else \
        (attr or {}).get("content") if isinstance(attr, dict) else []
    rows = rows or []
    extracted["n_attr_lineage"] = len(rows)
    if rows and isinstance(rows[0], dict):
        extracted["attr_lineage_keys"] = sorted(rows[0].keys())[:14]

    # ── 8. lifecycle — spec-correct variants (NOT /lifecycle/latest) ──
    life = probe.get("lifecycle_by_table",
                     f"/lifecycle?tableName={quoted}",
                     save_as="lifecycle.json")
    lrows = life if isinstance(life, list) else \
        (life or {}).get("content") if isinstance(life, dict) else []
    lrows = lrows or []
    extracted["n_lifecycle_records"] = len(lrows)
    if lrows and isinstance(lrows[0], dict):
        extracted["lifecycle_keys"] = sorted(lrows[0].keys())[:12]
    if extracted.get("pipeline_id"):
        probe.get(
            "lifecycle_latest",
            f"/lifecycle/{urllib.parse.quote(str(extracted['pipeline_id']))}"
            f"/latest?tableName={quoted}",
            save_as="lifecycle_latest.json", optional=True)
    # the deployment-validated URL (spec brief said it doesn't exist; the
    # real MDM 200s it) — if by-table failed, THIS becomes the cache file
    # the crawler replays:
    probe.get("lifecycle_LEGACY_probe",
              f"/lifecycle/latest?tableName={quoted}",
              save_as=None if lrows else "lifecycle.json", optional=True)

    # ── 9. high-value extras (refactor candidates, report-only) ──
    if extracted.get("dataset_id"):
        probe.get("ddl_views",
                  f"/datasets/views?datasetId="
                  f"{urllib.parse.quote(str(extracted['dataset_id']))}",
                  save_as="ddl_views.json", optional=True)
    probe.get("sensitivity_bulk",
              "/schemas/sensitivity-details?pageNo=0&pageSize=25"
              "&includeMetadata=true&tableType=all",
              save_as="sensitivity_bulk_sample.json", optional=True)
    probe.get("table_inventory_v2",
              "/datasets/v2/table-list?tableType=all", optional=True)

    # ── report ───────────────────────────────────────────────
    print(f"{'step':22} {'status':10} {'ms':>5}  shape / note")
    print("─" * 88)
    for row in probe.rows:
        status = row.get("status", "?")
        color = (GREEN if status.startswith("HTTP_2")
                 else YELLOW if status in ("SKIPPED", "DENIED")
                 or "optional" in str(row.get("note", ""))
                 else RED)
        detail = row.get("shape") or row.get("note") or ""
        saved = f"  → {row['saved']}" if row.get("saved") else ""
        print(f"{row['step']:22} {color}{status:10}{END} "
              f"{row.get('ms', ''):>5}  {detail[:70]}{saved}")

    print(f"\n{'═'*30} SUMMARY (paste this back) {'═'*30}")
    summary = {
        "table": table,
        "steps": {r["step"]: r.get("status") for r in probe.rows},
        "extracted": extracted,
    }
    print(json.dumps(summary, indent=2, default=str))
    probe.table_dir.mkdir(parents=True, exist_ok=True)
    (probe.table_dir / "_probe_report.json").write_text(
        json.dumps({"rows": probe.rows, "extracted": extracted},
                   indent=2, default=str), encoding="utf-8")
    print(f"\n{DIM}raw responses + report saved under "
          f"{probe.table_dir} (crawler cache layout — the pipeline "
          f"--mdm-raw-dir replays from here){END}")
    ok = sum(1 for r in probe.rows
             if str(r.get("status", "")).startswith("HTTP_2"))
    print(f"{ok}/{len(probe.rows)} endpoints returned 2xx")
    return 0 if extracted.get("n_columns") else 1


if __name__ == "__main__":
    sys.exit(main())
