"""Preflight — does this environment have everything synapse needs?

Run this BEFORE curate_entities.py or any extractor. It exercises every
external dependency (Python packages, env vars, SA keys, TLS through
corporate MITM, real Vertex + BQ auth) and tells you exactly what's
missing or broken before you waste a session debugging it later.

Nine independent sections:

    1. TLS bootstrap         truststore installed + injected
    2. Python dependencies   every import the pipeline needs
    3. Environment variables required + recommended env vars
    4. Service-account keys  exist? valid JSON? right shape? not in repo?
    5. Network connectivity  HTTPS reach to Google endpoints
    6. Vertex AI auth        build client + optional generate_content smoke
    7. BigQuery auth         build client + optional SELECT 1 smoke
    8. Inputs                3 CSVs + SQL corpus + MDM cache discovery
    9. Summary               green / red per section + next-step

Each section reports PASS / WARN / FAIL independently and continues.
Final exit code: 0 if no FAIL; non-zero otherwise (WARNs don't gate).

Flags:
    --skip-smoke      skip the actual API calls (env-only check, free)
    --vertex-only     skip the BQ section
    --bq-only         skip the Vertex section
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from pathlib import Path

# Repo paths
REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"

# Make `import synapse...` work when running this script directly.
if str(SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNAPSE_ROOT))

# Load .env BEFORE anything reads os.environ.
try:
    from synapse.utils.dotenv import load_dotenv_chain  # noqa: E402
    _DOTENV_APPLIED = load_dotenv_chain()
except ImportError:
    _DOTENV_APPLIED = {}
LUMI_FINAL = REPO_ROOT / "lumi_final"

# ── Pretty print ──────────────────────────────────────────────


def _hdr(msg: str) -> None:
    print(f"\n\033[1;36m═══ {msg} ═══\033[0m")


def _sec(name: str) -> None:
    print(f"\n\033[1;34m── {name} ──\033[0m")


def _pass(msg: str) -> None:
    print(f"  \033[1;32m✓\033[0m {msg}")


def _fail(msg: str) -> None:
    print(f"  \033[1;31m✗\033[0m {msg}")


def _warn(msg: str) -> None:
    print(f"  \033[1;33m!\033[0m {msg}")


def _info(msg: str) -> None:
    print(f"    \033[2m{msg}\033[0m")


def _fixhint(msg: str) -> None:
    print(f"    \033[2m→ fix: {msg}\033[0m")


# ── Section 1: TLS bootstrap ─────────────────────────────────


def section_tls() -> tuple[int, int]:
    """Returns (failures, warnings)."""
    _sec("1. TLS bootstrap (corporate-MITM safe)")
    f = w = 0
    try:
        import truststore  # type: ignore[import-not-found]
        try:
            truststore.inject_into_ssl()
            _pass("truststore installed AND injected — corporate TLS handled")
        except Exception as e:  # noqa: BLE001
            _fail(f"truststore inject failed: {e}")
            f += 1
    except ImportError:
        _warn("truststore not installed")
        _info("On corporate networks (TLS MITM), all HTTPS calls will fail")
        _info("without it. If you're not on a corporate proxy, ignore.")
        _fixhint("pip install truststore")
        w += 1
    return f, w


# ── Section 2: Python dependencies ───────────────────────────


_DEPS_REQUIRED = [
    ("pydantic", "pip install pydantic"),
    ("yaml", "pip install pyyaml"),
]
_DEPS_RECOMMENDED = [
    ("sqlglot", "pip install sqlglot",
     "needed for SQL corpus analysis (lumi_final reuse)"),
    ("openpyxl", "pip install openpyxl",
     "needed if reading .xlsx table catalog (CSV doesn't need it)"),
]
_DEPS_VERTEX = [
    ("google.genai", "pip install google-genai",
     "Vertex Gemini SDK"),
    ("google.oauth2.service_account", "pip install google-auth",
     "explicit SA key loading"),
]
_DEPS_BQ = [
    ("google.cloud.bigquery", "pip install google-cloud-bigquery",
     "BigQuery client"),
    ("google.cloud.datacatalog", "pip install google-cloud-datacatalog",
     "Data Catalog policy tags (optional but recommended)"),
]


def _try_import(modpath: str) -> tuple[bool, str]:
    try:
        __import__(modpath)
        return True, ""
    except ImportError as e:
        return False, str(e)


def section_python_deps() -> tuple[int, int]:
    _sec("2. Python dependencies")
    f = w = 0

    print("    \033[1mrequired\033[0m")
    for mod, fix in _DEPS_REQUIRED:
        ok, err = _try_import(mod)
        if ok:
            _pass(f"{mod}")
        else:
            _fail(f"{mod}: {err}")
            _fixhint(fix)
            f += 1

    print("    \033[1mrecommended\033[0m")
    for mod, fix, why in _DEPS_RECOMMENDED:
        ok, _ = _try_import(mod)
        if ok:
            _pass(f"{mod}")
        else:
            _warn(f"{mod} — {why}")
            _fixhint(fix)
            w += 1

    print("    \033[1mvertex\033[0m")
    for mod, fix, why in _DEPS_VERTEX:
        ok, _ = _try_import(mod)
        if ok:
            _pass(f"{mod}")
        else:
            _warn(f"{mod} — {why}")
            _fixhint(fix)
            w += 1

    print("    \033[1mbigquery\033[0m")
    for mod, fix, why in _DEPS_BQ:
        ok, _ = _try_import(mod)
        if ok:
            _pass(f"{mod}")
        else:
            _warn(f"{mod} — {why}")
            _fixhint(fix)
            w += 1

    return f, w


# ── Section 3: Environment variables ─────────────────────────


_ENV_VARS = [
    # name, required, description, default
    # Vertex
    ("LUMI_VERTEX_PROJECT", True,
     "GCP project that owns the Gemini grant", "your-vertex-project"),
    ("LUMI_VERTEX_LOCATION", False,
     "Vertex region; 'global' is the right answer for prj-d-ea-poc", "global"),
    ("LUMI_VERTEX_SA_KEY", False,
     "explicit Vertex SA key (else falls back to GOOGLE_APPLICATION_CREDENTIALS)",
     "(unset)"),
    # BigQuery — required: project + dataset
    ("BQ_PROJECT_ID", True,
     "BQ execution project (preferred name; fallback: LUMI_BQ_PROJECT)",
     "your-bq-project"),
    ("LUMI_BQ_DATASET", True,
     "default BQ dataset for unqualified tables", "dw"),
    ("LUMI_BQ_SA_KEY", False,
     "explicit BQ SA key (else falls back to GOOGLE_APPLICATION_CREDENTIALS)",
     "(unset)"),
    # BigQuery — optional enterprise plumbing
    ("BIGQUERY_API_BASE_URL", False,
     "REST endpoint override (e.g. https://bigquery-prod.p.googleapis.com)",
     "(default: public endpoint)"),
    ("BIGQUERY_URL", False,
     "fallback endpoint override if BIGQUERY_API_BASE_URL is unset",
     "(default: public endpoint)"),
    ("BQ_LOCATION", False,
     "BQ region for jobReference.location", "US"),
    ("BQ_FORCE_PROXY", False,
     "1 = keep proxy; skip NO_PROXY injection", "0"),
    ("BQ_DISABLE_PROXY", False,
     "1 = ignore proxy env for auth token refresh (use when 407 hits)", "0"),
    ("NO_PROXY", False,
     "comma-separated bypass list; synapse merges Google hosts in",
     "(unset; will be created)"),
    ("REQUESTS_CA_BUNDLE", False,
     "custom CA bundle for corporate TLS interception", "(unset)"),
    ("SSL_CERT_FILE", False,
     "CA bundle fallback; mirrored into REQUESTS_CA_BUNDLE when set",
     "(unset)"),
    # Legacy
    ("GOOGLE_APPLICATION_CREDENTIALS", False,
     "legacy single-key fallback for both services", "(unset)"),
]


def section_env_vars() -> tuple[int, int]:
    _sec("3. Environment variables")
    f = w = 0

    # Aliases the requirement can be satisfied by EITHER var
    _aliases = {
        "BQ_PROJECT_ID": "LUMI_BQ_PROJECT",
    }

    for name, required, why, default in _ENV_VARS:
        v = os.environ.get(name)
        marker = "REQUIRED" if required else "optional"
        if v:
            disp = v if len(v) <= 60 else v[:30] + "..." + v[-15:]
            _pass(f"{name}={disp}  ({marker})")
        else:
            alias = _aliases.get(name)
            if alias and os.environ.get(alias):
                _pass(f"{name} satisfied via {alias}={os.environ[alias]}  "
                      f"({marker})")
                continue
            if required:
                _fail(f"{name} NOT SET  ({marker})")
                _info(f"meaning: {why}")
                _fixhint(f"export {name}={default}")
                if alias:
                    _info(f"or satisfy via alias: export {alias}=...")
                f += 1
            else:
                _info(f"{name} (optional): {why}")

    # Special: at least one of {vertex_sa, bq_sa, GAC} must be set
    if not any(os.environ.get(k) for k in (
        "LUMI_VERTEX_SA_KEY", "LUMI_BQ_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS",
    )):
        _fail("no service-account key configured — "
              "set at least one of LUMI_VERTEX_SA_KEY / LUMI_BQ_SA_KEY / "
              "GOOGLE_APPLICATION_CREDENTIALS")
        f += 1

    # Surface what .env contributed (helps when "I set it but preflight
    # says unset" — turns out the wrong file got loaded).
    if _DOTENV_APPLIED:
        _info(f".env applied {len(_DOTENV_APPLIED)} value(s): "
              f"{sorted(_DOTENV_APPLIED.keys())[:8]}"
              f"{' …' if len(_DOTENV_APPLIED) > 8 else ''}")

    return f, w


def section_bq_network() -> tuple[int, int]:
    """Resolved BQ network config — endpoint, location, proxy, CA."""
    _sec("4b. BigQuery network resolution")
    f = w = 0
    try:
        from synapse.utils.auth import (
            DEFAULT_BQ_ENDPOINT,
            resolve_bq_endpoint,
            resolve_bq_location,
            resolve_bq_project,
            setup_bq_network_env,
        )
    except ImportError as e:
        _fail(f"synapse.utils.auth import failed: {e}")
        return 1, 0

    state = setup_bq_network_env()
    _pass(f"endpoint: {state['endpoint']}"
          + ("  (custom override)"
             if state["endpoint"] != DEFAULT_BQ_ENDPOINT else "  (default public)"))
    _pass(f"location: {state['location']}")
    _pass(f"project:  {state['project'] or '(unset — will fail BQ calls)'}")

    if state["force_proxy"]:
        _info("BQ_FORCE_PROXY=1 — NO_PROXY injection SKIPPED")
    else:
        if state["no_proxy_hosts"]:
            _pass(f"NO_PROXY merged: {len(state['no_proxy_hosts'])} hosts "
                  f"(incl. {state['endpoint_host']})")
        else:
            _info("NO_PROXY: no hosts injected (BQ_FORCE_PROXY active or no need)")

    if state["disable_proxy_for_auth"]:
        _info("BQ_DISABLE_PROXY=1 — auth Session will set trust_env=False")

    if state["ca_bundle"]:
        if Path(state["ca_bundle"]).exists():
            _pass(f"CA bundle: {state['ca_bundle']}")
        else:
            _fail(f"CA bundle path does not exist: {state['ca_bundle']}")
            f += 1
    else:
        _info("no custom CA bundle (system default trust will be used)")

    if not resolve_bq_project():
        _warn("BQ project unresolved — set BQ_PROJECT_ID or LUMI_BQ_PROJECT")
        w += 1

    return f, w


# ── Section 4: SA key files ──────────────────────────────────


def _validate_sa_key(path: Path) -> tuple[bool, str]:
    """Return (ok, message). Checks file shape, not network access."""
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return False, f"unreadable JSON: {e}"
    if blob.get("type") != "service_account":
        return False, f"not a service-account key (type={blob.get('type')!r})"
    for required in ("client_email", "private_key", "project_id"):
        if not blob.get(required):
            return False, f"missing required field: {required}"
    return True, (
        f"sa={blob['client_email']}  project={blob['project_id']}"
    )


def section_sa_keys() -> tuple[int, int]:
    _sec("4. Service-account key files")
    f = w = 0
    from synapse.utils.auth import (
        resolve_bq_key_path,
        resolve_vertex_key_path,
    )

    vertex_p = resolve_vertex_key_path()
    bq_p = resolve_bq_key_path()

    if vertex_p:
        ok, msg = _validate_sa_key(vertex_p)
        if ok:
            _pass(f"Vertex key: {vertex_p}")
            _info(msg)
            # Safety: warn if key lives inside the repo
            try:
                vertex_p.relative_to(REPO_ROOT)
                _warn("Vertex key is INSIDE the repo — move outside before committing")
                w += 1
            except ValueError:
                pass
        else:
            _fail(f"Vertex key at {vertex_p}: {msg}")
            f += 1
    else:
        _warn("no Vertex key configured "
              "(LUMI_VERTEX_SA_KEY or GOOGLE_APPLICATION_CREDENTIALS)")
        w += 1

    if bq_p:
        ok, msg = _validate_sa_key(bq_p)
        if ok:
            _pass(f"BQ key: {bq_p}")
            _info(msg)
            try:
                bq_p.relative_to(REPO_ROOT)
                _warn("BQ key is INSIDE the repo — move outside before committing")
                w += 1
            except ValueError:
                pass
        else:
            _fail(f"BQ key at {bq_p}: {msg}")
            f += 1
    else:
        _warn("no BQ key configured "
              "(LUMI_BQ_SA_KEY or GOOGLE_APPLICATION_CREDENTIALS)")
        w += 1

    if vertex_p and bq_p and vertex_p == bq_p:
        _info("Vertex and BQ point at the same key — "
              "fine if that SA has both permissions, else split them.")

    return f, w


# ── Section 5: Network connectivity ──────────────────────────


_ENDPOINTS = [
    ("google.com", 443),
    ("aiplatform.googleapis.com", 443),
    ("bigquery.googleapis.com", 443),
    ("oauth2.googleapis.com", 443),
]


def section_network() -> tuple[int, int]:
    _sec("5. Network connectivity")
    f = w = 0
    for host, port in _ENDPOINTS:
        try:
            with socket.create_connection((host, port), timeout=5):
                _pass(f"TCP reach {host}:{port}")
        except (socket.gaierror, socket.timeout, OSError) as e:
            _fail(f"{host}:{port} — {type(e).__name__}: {e}")
            _info("VPN? Corporate firewall? Check your network.")
            f += 1
    return f, w


# ── Section 6: Vertex auth + smoke ───────────────────────────


def section_vertex(skip_smoke: bool) -> tuple[int, int]:
    _sec("6. Vertex AI auth + smoke")
    f = w = 0
    try:
        from synapse.utils.auth import (
            build_vertex_genai_client,
            resolve_vertex_key_path,
        )
    except ImportError as e:
        _fail(f"synapse.utils.auth import failed: {e}")
        return 1, 0

    if resolve_vertex_key_path() is None:
        _warn("no Vertex SA key — skipping auth + smoke")
        return 0, 1

    try:
        client = build_vertex_genai_client()
        _pass("Vertex genai client built")
    except Exception as e:  # noqa: BLE001
        _fail(f"client build failed: {type(e).__name__}: {e}")
        return 1, 0

    if skip_smoke:
        _warn("--skip-smoke: not making a real generate_content call")
        return 0, 1

    try:
        model = os.environ.get("LUMI_VERTEX_MODEL", "gemini-3.1-pro-preview")
        result = client.models.generate_content(
            model=model,
            contents="Reply with exactly the word: OK",
        )
        text = (getattr(result, "text", "") or "").strip()
        if not text:
            _fail(f"empty response from {model}")
            f += 1
        else:
            _pass(f"{model} responded: {text[:50]!r}")
    except Exception as e:  # noqa: BLE001
        _fail(f"generate_content failed: {type(e).__name__}: {e}")
        _info("Common causes: SA lacks roles/aiplatform.user, "
              "wrong --location, model name typo, billing not enabled.")
        f += 1
    return f, w


# ── Section 7: BigQuery auth + smoke ─────────────────────────


def section_bq(skip_smoke: bool) -> tuple[int, int]:
    _sec("7. BigQuery auth + smoke")
    f = w = 0
    try:
        from synapse.utils.auth import build_bq_client, resolve_bq_key_path
    except ImportError as e:
        _fail(f"synapse.utils.auth import failed: {e}")
        return 1, 0

    if resolve_bq_key_path() is None:
        _warn("no BQ SA key — skipping auth + smoke")
        return 0, 1

    try:
        client = build_bq_client()
        _pass(f"BQ client built  (project={client.project})")
    except Exception as e:  # noqa: BLE001
        _fail(f"client build failed: {type(e).__name__}: {e}")
        return 1, 0

    if skip_smoke:
        _warn("--skip-smoke: not running a real query")
        return 0, 1

    try:
        rows = list(client.query("SELECT 1 AS one, CURRENT_TIMESTAMP() AS ts")
                    .result(timeout=30))
        if rows and rows[0]["one"] == 1:
            _pass(f"SELECT 1 returned ok at {rows[0]['ts']}")
        else:
            _fail(f"unexpected SELECT 1 result: {rows}")
            f += 1
    except Exception as e:  # noqa: BLE001
        _fail(f"query failed: {type(e).__name__}: {e}")
        _info("Common causes: SA lacks roles/bigquery.jobUser + "
              "roles/bigquery.dataViewer, wrong --project, billing not enabled.")
        f += 1

    # Probe access to the configured default dataset
    dataset = os.environ.get("LUMI_BQ_DATASET", "dw")
    project = os.environ.get("LUMI_BQ_PROJECT")
    if project:
        try:
            ds_rows = list(client.query(
                f"SELECT count(*) AS n_tables "
                f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`"
            ).result(timeout=30))
            n = int(ds_rows[0]["n_tables"]) if ds_rows else 0
            _pass(f"INFORMATION_SCHEMA reachable: "
                  f"{project}.{dataset} has {n} tables")
        except Exception as e:  # noqa: BLE001
            _warn(f"INFORMATION_SCHEMA on {project}.{dataset} failed: "
                  f"{type(e).__name__}: {e}")
            _info("Could be wrong project/dataset, or SA lacks "
                  "roles/bigquery.metadataViewer on this dataset.")
            w += 1
    return f, w


# ── Section 8: Input files ───────────────────────────────────


def section_inputs() -> tuple[int, int]:
    _sec("8. Input files")
    f = w = 0

    # The 3 CSVs (required)
    raw_dir = SYNAPSE_ROOT / "data" / "registries" / "raw"
    required_csvs = [
        ("glossary.csv", "Acronym glossary with disambiguation context"),
        ("metric_catalog.csv", "Business-metric catalog"),
        ("table_catalog.csv", "Table-scope + domain catalog (csv or xlsx)"),
    ]
    print(f"    \033[1m3 CSVs at {raw_dir}\033[0m")
    for name, why in required_csvs:
        p = raw_dir / name
        p_xlsx = raw_dir / name.replace(".csv", ".xlsx")
        if p.exists():
            _pass(f"{name}  ({p.stat().st_size:,} bytes)")
        elif p_xlsx.exists():
            _pass(f"{p_xlsx.name}  ({p_xlsx.stat().st_size:,} bytes)")
        else:
            _fail(f"{name} missing — {why}")
            _fixhint(f"place at {p}  (or {p_xlsx.name})")
            f += 1

    # SQL corpus (required)
    sql_dir = LUMI_FINAL / "data" / "gold_queries"
    n_sql = len(list(sql_dir.glob("*.sql"))) if sql_dir.exists() else 0
    print(f"    \033[1mSQL corpus at {sql_dir}\033[0m")
    if n_sql > 0:
        _pass(f"{n_sql} .sql file(s)")
    else:
        _warn("no .sql files found")
        _info("if your SQLs live in an Excel, run the Excel→SQL step from "
              "lumi_final/scripts/probe_corpus_phase012.py --from-excel "
              "before running curate_entities.py")
        w += 1

    # MDM cache (optional)
    mdm_dir = LUMI_FINAL / "data" / "mdm_cache"
    n_mdm = len(list(mdm_dir.glob("*.json"))) if mdm_dir.exists() else 0
    print(f"    \033[1mMDM cache at {mdm_dir}\033[0m")
    if n_mdm > 0:
        _pass(f"{n_mdm} MDM table digest(s)")
    else:
        _warn("no MDM cache — entity curation proceeds without MDM signal")
        _info("to populate: python lumi_final/scripts/probe_mdm.py "
              "--from-sqls lumi_final/data/gold_queries/ "
              "--save lumi_final/data/mdm_cache/")
        w += 1

    return f, w


# ── Main ──────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-smoke", action="store_true",
        help="Skip actual API calls (still checks env + auth file shape).",
    )
    parser.add_argument(
        "--vertex-only", action="store_true",
        help="Run only Vertex auth + smoke (skip BQ).",
    )
    parser.add_argument(
        "--bq-only", action="store_true",
        help="Run only BQ auth + smoke (skip Vertex).",
    )
    args = parser.parse_args()

    _hdr("Synapse preflight")
    _info(f"repo root: {REPO_ROOT}")
    _info(f"synapse root: {SYNAPSE_ROOT}")
    _info(f"lumi_final root: {LUMI_FINAL}")
    _info(f"mode: skip_smoke={args.skip_smoke}, "
          f"vertex_only={args.vertex_only}, bq_only={args.bq_only}")

    total_f = total_w = 0
    results: list[tuple[str, int, int]] = []

    # 1
    f, w = section_tls()
    results.append(("1. TLS bootstrap", f, w))
    total_f += f
    total_w += w

    # 2
    f, w = section_python_deps()
    results.append(("2. Python deps", f, w))
    total_f += f
    total_w += w

    # 3
    f, w = section_env_vars()
    results.append(("3. Environment vars", f, w))
    total_f += f
    total_w += w

    # 4
    try:
        f, w = section_sa_keys()
    except ImportError as e:
        _fail(f"synapse not importable (run `pip install -e ./synapse` first): {e}")
        f, w = 1, 0
    results.append(("4. SA key files", f, w))
    total_f += f
    total_w += w

    # 4b — BQ network resolution (only meaningful when SA + bq libs present)
    if not args.vertex_only:
        f, w = section_bq_network()
        results.append(("4b. BQ network", f, w))
        total_f += f
        total_w += w

    # 5
    f, w = section_network()
    results.append(("5. Network", f, w))
    total_f += f
    total_w += w

    # 6 — Vertex (gated)
    if args.bq_only:
        _sec("6. Vertex AI auth + smoke")
        _warn("--bq-only: skipped")
        results.append(("6. Vertex", 0, 1))
        total_w += 1
    else:
        f, w = section_vertex(args.skip_smoke)
        results.append(("6. Vertex", f, w))
        total_f += f
        total_w += w

    # 7 — BQ (gated)
    if args.vertex_only:
        _sec("7. BigQuery auth + smoke")
        _warn("--vertex-only: skipped")
        results.append(("7. BigQuery", 0, 1))
        total_w += 1
    else:
        f, w = section_bq(args.skip_smoke)
        results.append(("7. BigQuery", f, w))
        total_f += f
        total_w += w

    # 8
    f, w = section_inputs()
    results.append(("8. Input files", f, w))
    total_f += f
    total_w += w

    # 9 — Summary
    _hdr("9. Summary")
    width = max(len(s[0]) for s in results)
    for name, f, w in results:
        mark = ("\033[1;32m✓\033[0m" if f == 0
                else "\033[1;31m✗\033[0m")
        warn_str = f" ({w} warn)" if w else ""
        print(f"  {mark} {name.ljust(width)}  {f} fail{warn_str}")

    print()
    if total_f == 0:
        print(f"\033[1;32m✓ PREFLIGHT GREEN\033[0m — "
              f"0 fails, {total_w} warnings.")
        if total_w:
            print("  Warnings are non-blocking but worth a look "
                  "(missing optional deps, weak inputs, etc.).")
        print("\nNext steps:")
        print("  1. python synapse/scripts/probe_curation.py")
        print("  2. python synapse/scripts/curate_entities.py --dry-run")
        print("  3. python synapse/scripts/curate_entities.py")
        return 0
    print(f"\033[1;31m✗ PREFLIGHT RED\033[0m — "
          f"{total_f} fails, {total_w} warnings.")
    print("  Fix the failures above before running curate_entities.py.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
