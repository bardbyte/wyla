#!/usr/bin/env python3
"""Explore the raw MDM API payload structure exhaustively.

We currently extract ~5 fields per column out of ~22 + ~3 fields per
table out of ~15. Before expanding the digest we need to see the
ACTUAL keys MDM returns for AmEx tables — different deployments expose
different keys, and CLAUDE.md only documents what we've encountered so
far. Run this on the work laptop, paste the structured output back so
the digest expansion is field-accurate, not guessed.

Usage:
    # Default: probe one table, dump full structure to stdout
    python scripts/explore_mdm_payload.py --table custins_customer_insights_cardmember

    # Probe multiple tables to see field-presence variation
    python scripts/explore_mdm_payload.py \\
        --table custins_customer_insights_cardmember \\
        --table cornerstone_metrics \\
        --table risk_pers_acct_history

    # Save raw JSON for offline inspection later
    python scripts/explore_mdm_payload.py \\
        --table foo --save data/mdm_raw/

    # Spot-check ALL discovered tables (from session1_output.json)
    python scripts/explore_mdm_payload.py --from-session1

Two output modes:

  Default (structured view):
    Recursively walks every nested dict + lists-of-dicts up to --max-depth
    (default 4) so nothing is masked behind "dict(N keys)" placeholders.
    Sample values truncated to ~100 chars to keep output scannable.

  --raw-dump:
    Prints `json.dumps(payload, indent=2)` verbatim — bytes-level truth,
    no transformation, no truncation, no key filtering. Use this when
    you want zero risk of the structured view masking anything.

Both modes can be paired with --save to also persist raw JSON to disk
under data/mdm_raw/<table>.raw.json.

Designed to be pasted back into the conversation — output is verbose
but well-structured.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import warnings
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# --- Corporate-network TLS handling ---------------------------------------
try:
    import truststore  # type: ignore[import-not-found]

    truststore.inject_into_ssl()
    _TRUSTSTORE_LOADED = True
except ImportError:
    _TRUSTSTORE_LOADED = False
# ---------------------------------------------------------------------------

DEFAULT_ENDPOINT = (
    "https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas"
)
DEFAULT_TIMEOUT_SECS = 30
SAMPLE_VALUE_MAXLEN = 100  # truncate long string values when displaying


def _disable_ssl_verification() -> None:
    """Last-resort TLS bypass — same pattern as our other probes."""
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
    os.environ["PYTHONHTTPSVERIFY"] = "0"
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    warnings.warn(
        "SSL verification disabled — only safe on networks you already trust.",
        stacklevel=2,
    )


def _fetch_raw(table_name: str, endpoint: str) -> Any:
    """Hit the MDM API and return the parsed JSON body verbatim.

    Returns whatever shape the API gives us (CLAUDE.md says it's an
    array of length 1; this probe verifies that without assuming).
    """
    url = f"{endpoint}?{urllib.parse.urlencode({'tableName': table_name})}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ─── Structural exploration helpers ──────────────────────────


def _typeof(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return f"str({len(v)} chars)"
    if isinstance(v, list):
        return f"list({len(v)} items)"
    if isinstance(v, dict):
        return f"dict({len(v)} keys)"
    return type(v).__name__


def _is_populated(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, (str, list, dict)) and len(v) == 0:
        return False
    return True


def _truncate(s: str, n: int = SAMPLE_VALUE_MAXLEN) -> str:
    s = s.replace("\n", " ").replace("\r", "")
    return s if len(s) <= n else s[:n] + "..."


def _sample_value(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return f'"{_truncate(v)}"'
    if isinstance(v, list):
        if not v:
            return "[]"
        first = v[0]
        if isinstance(first, dict):
            return f"[{{...{len(first)} keys...}}, ×{len(v)}]"
        return f"[{_sample_value(first)}, ×{len(v)}]"
    if isinstance(v, dict):
        return f"{{...{len(v)} keys...}}"
    return str(v)


# ─── Output rendering ────────────────────────────────────────


def _print_section(title: str) -> None:
    print()
    print("═" * 78)
    print(f"  {title}")
    print("═" * 78)


def _print_dict_keys(
    d: dict[str, Any],
    indent: int = 2,
    *,
    recurse: bool = False,
    max_depth: int = 4,
    _depth: int = 0,
) -> None:
    """Print every key of `d` with type + populated marker + sample value.

    When ``recurse=True``, also drills into nested dicts up to ``max_depth``
    so unexpected sections (e.g. a `lineage_details` block we didn't
    anticipate) get their keys shown, not just `dict(N keys)`.
    """
    pad = " " * indent
    if not d:
        print(f"{pad}(empty dict)")
        return
    # Sort populated first, then null/empty.
    items = sorted(
        d.items(),
        key=lambda kv: (not _is_populated(kv[1]), kv[0]),
    )
    for k, v in items:
        marker = "✓" if _is_populated(v) else "✗"
        print(f"{pad}{marker} {k} ({_typeof(v)}): {_sample_value(v)}")
        # Recursive drill — keys that are themselves dicts get expanded.
        if recurse and _depth < max_depth and isinstance(v, dict) and v:
            _print_dict_keys(
                v,
                indent=indent + 4,
                recurse=True,
                max_depth=max_depth,
                _depth=_depth + 1,
            )
        # If a list-of-dicts, drill into the FIRST item's keys.
        elif (
            recurse and _depth < max_depth
            and isinstance(v, list) and v
            and isinstance(v[0], dict)
        ):
            print(f"{pad}    [first item shape:]")
            _print_dict_keys(
                v[0],
                indent=indent + 8,
                recurse=True,
                max_depth=max_depth,
                _depth=_depth + 1,
            )


def _print_list_item_shape(label: str, lst: list, indent: int = 2) -> None:
    pad = " " * indent
    if not lst:
        print(f"{pad}(empty list — no shape to inspect)")
        return
    first = lst[0]
    print(f"{pad}list of {len(lst)} items, first item shape:")
    if isinstance(first, dict):
        _print_dict_keys(first, indent=indent + 4)
    else:
        print(f"{pad}    {_typeof(first)}: {_sample_value(first)}")


def explore_payload(table_name: str, payload: Any, *, max_depth: int = 4) -> None:
    """Pretty-print the structure of one MDM payload."""
    _print_section(f"Table: {table_name}")

    print(f"\nTop-level shape: {_typeof(payload)}")
    if not isinstance(payload, list) or not payload:
        print("\n⚠  Expected a list with at least one element. Got:")
        print(f"   {_sample_value(payload)}")
        return

    print(f"  → length {len(payload)}, peeling [0]")
    data = payload[0]

    if not isinstance(data, dict):
        print(f"\n⚠  Expected dict at [0]. Got {_typeof(data)}: {_sample_value(data)}")
        return

    # -- Top-level keys (recursive — every nested dict gets fully expanded) --
    print(f"\n📦 Top-level keys ({len(data)} total) — RECURSIVE walk:")
    _print_dict_keys(data, indent=4, recurse=True, max_depth=max_depth)

    # -- dataset_details --
    if "dataset_details" in data and isinstance(data["dataset_details"], dict):
        _print_section("dataset_details (table-level metadata)")
        _print_dict_keys(data["dataset_details"], indent=4,
                         recurse=True, max_depth=max_depth)

    # -- dataset_source_details --
    if "dataset_source_details" in data and isinstance(data["dataset_source_details"], dict):
        _print_section("dataset_source_details (BQ location)")
        _print_dict_keys(data["dataset_source_details"], indent=4,
                         recurse=True, max_depth=max_depth)

    # -- ownership_details --
    if "ownership_details" in data and isinstance(data["ownership_details"], dict):
        _print_section("ownership_details")
        _print_dict_keys(data["ownership_details"], indent=4,
                         recurse=True, max_depth=max_depth)
        # Drill into business_contacts / tech_contacts shape if list-of-dicts.
        for k in ("business_contacts", "tech_contacts"):
            v = data["ownership_details"].get(k)
            if isinstance(v, list) and v:
                print(f"\n  {k} item shape:")
                _print_list_item_shape(k, v, indent=4)

    # -- schema.schema_attributes (per-column) --
    schema = data.get("schema") or {}
    cols = schema.get("schema_attributes") or []
    _print_section(f"schema.schema_attributes — {len(cols)} columns")
    if cols:
        first_col = cols[0]
        col_name = (
            (first_col.get("attribute_details") or {}).get("attribute_name")
            or first_col.get("attribute_name")
            or "(?)"
        )
        print(f"\nFirst column: {col_name}")
        print(f"  Top-level keys ({len(first_col)} total):")
        _print_dict_keys(first_col, indent=6)

        # attribute_details (the meaty per-column section)
        ad = first_col.get("attribute_details") or {}
        if ad:
            print(f"\n  attribute_details ({len(ad)} keys):")
            _print_dict_keys(ad, indent=6)

        # sensitivity_details
        sd = first_col.get("sensitivity_details") or {}
        if sd:
            print(f"\n  sensitivity_details ({len(sd)} keys):")
            _print_dict_keys(sd, indent=6)

        # external_reference_details — THE JOIN GOLDMINE
        erd = first_col.get("external_reference_details") or []
        print(f"\n  external_reference_details — {_typeof(erd)}")
        if isinstance(erd, list) and erd:
            _print_list_item_shape("external_reference_details", erd, indent=6)
        elif isinstance(erd, list):
            print("    (empty — first column has no MDM-declared joins)")

        # -- Hunt for the FIRST column with non-empty external_reference_details --
        # because the first column might be metadata; the join-key columns later
        # are where the gold lives.
        _print_section("Cross-reference scan (first 3 cols with populated external_references)")
        found = 0
        for col in cols:
            erd = col.get("external_reference_details") or []
            if isinstance(erd, list) and erd:
                col_name = (
                    (col.get("attribute_details") or {}).get("attribute_name")
                    or col.get("attribute_name") or "(?)"
                )
                print(f"\n  Column: {col_name}")
                _print_list_item_shape("external_reference_details", erd, indent=8)
                found += 1
                if found >= 3:
                    break
        if found == 0:
            print("\n  (no columns in this table have populated external_reference_details)")

        # -- Survey field presence across ALL columns --
        _print_section("Field-presence histogram across ALL columns")
        _print_field_presence_histogram(cols)

    # -- Any remaining top-level keys we haven't shown explicitly --
    # FULL recursive walk so anything unexpected gets its keys exposed,
    # not just a "dict(N keys)" placeholder.
    handled = {
        "dataset_details", "dataset_source_details",
        "ownership_details", "schema",
    }
    remaining = {k: v for k, v in data.items() if k not in handled}
    if remaining:
        _print_section("Other top-level keys (not in our digest yet) — recursive")
        _print_dict_keys(remaining, indent=4, recurse=True, max_depth=4)


def _print_field_presence_histogram(cols: list[dict[str, Any]]) -> None:
    """For every key seen in attribute_details / sensitivity_details across
    all columns, count how many columns actually have a populated value.
    Tells us which keys are real signal vs MDM-defined-but-always-null.
    """
    sections = {
        "attribute_details": {},
        "sensitivity_details": {},
    }
    section_external_ref_pop = 0

    for col in cols:
        for sect_name, agg in sections.items():
            sect = col.get(sect_name) or {}
            for k, v in sect.items():
                if k not in agg:
                    agg[k] = {"populated": 0, "total": 0}
                agg[k]["total"] += 1
                if _is_populated(v):
                    agg[k]["populated"] += 1
        erd = col.get("external_reference_details")
        if isinstance(erd, list) and erd:
            section_external_ref_pop += 1

    for sect_name, agg in sections.items():
        if not agg:
            continue
        print(f"\n  {sect_name} keys (column-populated / total):")
        for k in sorted(agg.keys()):
            a = agg[k]
            pct = (a["populated"] / a["total"]) * 100 if a["total"] else 0
            print(f"    {k:40s}  {a['populated']:>4} / {a['total']}  ({pct:>5.1f}%)")

    print(
        f"\n  external_reference_details populated on "
        f"{section_external_ref_pop} / {len(cols)} columns"
    )


# ─── Driver ──────────────────────────────────────────────────


def _save_raw(payload: Any, table_name: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{table_name}.raw.json"
    target.write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    return target


def _tables_from_session1(path: Path) -> list[str]:
    if not path.exists():
        raise SystemExit(f"ERROR: {path} not found.")
    data = json.loads(path.read_text(encoding="utf-8"))
    return sorted(data.keys())


def main() -> int:
    p = argparse.ArgumentParser(prog="explore_mdm_payload")
    p.add_argument(
        "--table", action="append",
        help="Table name to probe; repeat for multiple",
    )
    p.add_argument(
        "--from-session1",
        action="store_true",
        help="Probe every table in data/session1_output.json",
    )
    p.add_argument(
        "--endpoint", default=DEFAULT_ENDPOINT,
        help=f"MDM endpoint (default: {DEFAULT_ENDPOINT})",
    )
    p.add_argument(
        "--save",
        help="Directory to dump raw JSON for each table",
    )
    p.add_argument(
        "--insecure", action="store_true",
        help="Disable SSL verification (corporate-MITM networks)",
    )
    p.add_argument(
        "--limit", type=int, default=3,
        help="Cap how many tables to probe (default 3, since output is verbose)",
    )
    p.add_argument(
        "--raw-dump", action="store_true",
        help=(
            "Print json.dumps(payload, indent=2) for each table — bytes-level "
            "truth, no transformation, no truncation, no key filtering. "
            "Use this when you want zero risk of the structured view masking "
            "anything."
        ),
    )
    p.add_argument(
        "--max-depth", type=int, default=4,
        help=(
            "Max recursion depth when walking nested dicts in the structured "
            "view. Default 4 keeps output bounded; bump to 10 for deepest dive."
        ),
    )
    args = p.parse_args()

    if args.insecure:
        _disable_ssl_verification()
        print("WARN: TLS verification disabled (--insecure)", file=sys.stderr)
    else:
        print(
            f"truststore: "
            f"{'loaded' if _TRUSTSTORE_LOADED else 'NOT loaded — pip install truststore'}",
            file=sys.stderr,
        )

    targets: list[str] = []
    if args.table:
        targets.extend(args.table)
    if args.from_session1:
        targets.extend(_tables_from_session1(Path("data/session1_output.json")))
    if not targets:
        print(
            "ERROR: pass --table NAME (repeat for multi) "
            "or --from-session1 to discover from disk.",
            file=sys.stderr,
        )
        return 2

    # De-dup, preserve order, cap at --limit.
    seen: set[str] = set()
    targets = [t for t in targets if not (t in seen or seen.add(t))]
    if args.limit and len(targets) > args.limit:
        print(
            f"\nNOTE: capping to first {args.limit} table(s) "
            f"(out of {len(targets)}). Override with --limit 0.",
            file=sys.stderr,
        )
        targets = targets[: args.limit]

    save_dir = Path(args.save) if args.save else None
    failures: list[tuple[str, str]] = []
    for table in targets:
        try:
            payload = _fetch_raw(table, args.endpoint)
        except urllib.error.HTTPError as e:
            failures.append((table, f"HTTP {e.code}: {e.reason}"))
            print(f"\n✗ {table}: HTTP {e.code} — {e.reason}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            failures.append((table, f"URLError: {e.reason}"))
            print(f"\n✗ {table}: {e.reason}", file=sys.stderr)
            continue
        except Exception as e:  # noqa: BLE001
            failures.append((table, f"{type(e).__name__}: {e}"))
            print(f"\n✗ {table}: {type(e).__name__}: {e}", file=sys.stderr)
            continue

        if args.raw_dump:
            # Bytes-level truth — no transformation, no truncation, no
            # filtering. This is the "I trust nothing in the structured
            # view" mode. Pair with --save for a file copy too.
            print(f"\n# === RAW JSON for {table} ({len(json.dumps(payload))} bytes) ===")
            print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        else:
            explore_payload(table, payload, max_depth=args.max_depth)

        if save_dir is not None:
            target = _save_raw(payload, table, save_dir)
            print(f"\n  → saved raw to {target}")

    print()
    print("═" * 78)
    print(f"  Summary: probed {len(targets) - len(failures)}/{len(targets)} tables")
    print("═" * 78)
    if failures:
        for t, e in failures:
            print(f"  ✗ {t}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
