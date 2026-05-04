#!/usr/bin/env python3
"""Explore the raw MDM API payload by inferring its schema automatically.

We currently extract ~5 fields per column out of ~22 + ~3 fields per
table out of ~15. Before expanding the digest we need to see the
ACTUAL keys MDM returns for AmEx tables.

This probe walks the payload tree and infers a deduplicated schema:
  - lists of dicts (e.g. 193 columns) collapse into ONE schema entry
    with populate-rate stats across all items
  - every nested key is shown once with type, populated/total count,
    and up to 5 sample values
  - depth is auto-determined; the program walks until it hits primitives

This means the output for a 193-column table is ~50 lines instead of
~5000, while still telling you EVERY key MDM uses + how often it's
populated + what kinds of values it carries.

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

  Default (schema-inferred view):
    Auto-walks the payload, collapses lists of dicts into a single
    schema entry, and reports populate-rate per key. Output for a
    193-column table is ~50 lines, not ~5000. Lossless on the SHAPE
    of the data — only repetition is collapsed.

  --raw-dump:
    Prints `json.dumps(payload, indent=2)` verbatim — bytes-level truth,
    no transformation, no truncation, no key filtering. Use this when
    you want zero risk of the structured view masking anything.

Both modes can be paired with --save to persist raw JSON under
data/mdm_raw/<table>.raw.json.
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
from dataclasses import dataclass, field
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


# ─── Schema inference engine ─────────────────────────────────


@dataclass
class FieldStats:
    """Inferred schema for one path through the payload tree.

    Lists of dicts get collapsed into a single FieldStats whose
    ``populated`` / ``total`` count across every list item. So 193
    columns become ONE entry per nested key, with populate-rate
    showing how reliably MDM fills that key on this table.
    """
    path: str                                       # "schema.schema_attributes[].attribute_details.business_name"
    types_seen: set[str] = field(default_factory=set)
    populated: int = 0                              # count of non-empty observations
    total: int = 0                                  # count of times the key appeared (incl. null)
    sample_values: list[Any] = field(default_factory=list)
    container_kind: str | None = None               # "dict" | "list[dict]" | "list[primitive]" | None
    list_lengths: list[int] = field(default_factory=list)

    def add_sample(self, v: Any, max_samples: int = 5) -> None:
        if v is None or isinstance(v, (dict, list)):
            return
        if v in self.sample_values:
            return
        if len(self.sample_values) >= max_samples:
            return
        self.sample_values.append(v)


def _short_type(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def infer_schema(
    value: Any,
    path: str = "$",
    stats: dict[str, FieldStats] | None = None,
) -> dict[str, FieldStats]:
    """Walk the payload and accumulate per-path FieldStats.

    Behavior:
      - dict: recurse into each key with path "<path>.<key>"
      - list of dicts: collapse — recurse each item with path "<path>[]"
        so all items contribute to the same FieldStats node
      - list of primitives: record samples + types at the list path
      - primitive: caller's responsibility to record (we just touch)

    No max depth — JSON is acyclic, so we recurse fully. Repetition is
    handled by path-deduping, which is what makes this efficient on
    wide tables.
    """
    if stats is None:
        stats = {}

    s = stats.setdefault(path, FieldStats(path=path))
    s.types_seen.add(_short_type(value))
    s.total += 1
    if _is_populated(value):
        s.populated += 1

    if isinstance(value, dict):
        s.container_kind = "dict"
        for k, v in value.items():
            child = f"{path}.{k}"
            infer_schema(v, child, stats)
    elif isinstance(value, list):
        s.list_lengths.append(len(value))
        if value and isinstance(value[0], dict):
            s.container_kind = "list[dict]"
            list_path = f"{path}[]"
            for item in value:
                infer_schema(item, list_path, stats)
        elif value:
            s.container_kind = "list[primitive]"
            for item in value[:10]:  # sample first 10 primitives
                if item not in s.sample_values and len(s.sample_values) < 10:
                    s.sample_values.append(item)
        else:
            s.container_kind = "list[empty]"
    else:
        # Primitive — record sample.
        s.add_sample(value)

    return stats


def render_schema(stats: dict[str, FieldStats], out: list[str] | None = None) -> str:
    """Render the inferred schema as a tree, sorted by path so siblings
    stay together. Each line shows: path, types, populated/total, samples.
    """
    if out is None:
        out = []

    paths_sorted = sorted(stats.keys())
    for p in paths_sorted:
        s = stats[p]
        # Indent based on path depth (count of '.' + '[' tokens beyond root).
        depth = _depth_of(p)
        pad = "  " * depth

        # Type label
        types = sorted(s.types_seen - {"dict", "list"})
        if s.container_kind == "list[dict]":
            type_label = "list[dict]"
            len_info = f", lengths={s.list_lengths[:5]}" if s.list_lengths else ""
        elif s.container_kind == "list[primitive]":
            type_label = f"list[{','.join(types) or '?'}]"
            len_info = f", lengths={s.list_lengths[:5]}" if s.list_lengths else ""
        elif s.container_kind == "list[empty]":
            type_label = "list[empty]"
            len_info = ""
        elif s.container_kind == "dict":
            type_label = "dict"
            len_info = ""
        else:
            type_label = "/".join(types) if types else "null"
            len_info = ""

        pop_info = ""
        if s.total > 1:
            pop_pct = (s.populated / s.total) * 100 if s.total else 0
            pop_info = f"  populated={s.populated}/{s.total} ({pop_pct:.0f}%)"
        elif not _populated_marker(s):
            pop_info = "  populated=NO"

        # Sample values (truncated)
        sample = ""
        if s.sample_values:
            shown = [
                _truncate_sample(v) for v in s.sample_values[:3]
            ]
            sample = f"  e.g. {shown}"
            if len(s.sample_values) > 3:
                sample += f" (+{len(s.sample_values) - 3} more samples)"

        marker = "✓" if _populated_marker(s) else "✗"
        # Last segment of the path is the "name" we display indented.
        leaf = _leaf_name(p)
        out.append(
            f"{pad}{marker} {leaf}  ({type_label}{len_info}){pop_info}{sample}"
        )

    return "\n".join(out)


def _depth_of(path: str) -> int:
    """How nested this path is from root. $ → 0, $.a → 1, $.a.b → 2,
    $.a[].b → 3 (list-of-dicts adds one level for the list, one for the item).
    """
    if path == "$":
        return 0
    # Count separators after stripping the root.
    body = path[2:] if path.startswith("$.") else path
    depth = 0
    for ch in body:
        if ch == ".":
            depth += 1
        elif ch == "[":
            depth += 1
    return depth + 1  # +1 because the leaf itself counts as a level


def _leaf_name(path: str) -> str:
    """Just the last segment of a path, with [] preserved on lists."""
    if path == "$":
        return "$ (root)"
    # Split on the LAST separator, preserving "[]" markers
    last_dot = path.rfind(".")
    last_bracket = path.rfind("[]")
    cut = max(last_dot, last_bracket if last_bracket > 0 else -1)
    if cut < 0:
        return path
    return path[cut + 1:] if path[cut] == "." else path[cut:]


def _populated_marker(s: FieldStats) -> bool:
    """A FieldStats is 'populated' if at least one observation was non-null."""
    return s.populated > 0


def _truncate_sample(v: Any, n: int = SAMPLE_VALUE_MAXLEN) -> str:
    if isinstance(v, str):
        return f'"{v[:n]}{"..." if len(v) > n else ""}"'
    if isinstance(v, bool):
        return str(v).lower()
    return repr(v)


# ─── Output rendering ────────────────────────────────────────


def _print_section(title: str) -> None:
    print()
    print("═" * 78)
    print(f"  {title}")
    print("═" * 78)


# (Legacy hand-coded walkers were removed — the schema-inference engine
# above produces the same output more compactly and without hardcoded
# section names or max_depth bounds.)


def explore_payload(table_name: str, payload: Any, **_: Any) -> None:
    """Infer + render the schema of one MDM payload.

    Output is the deduplicated schema tree: every key seen at every
    nested level, with populate-rate (e.g. business_name populated on
    192/193 cols), types, and up to 3 sample values per key.
    No max_depth — auto-determined by walking until primitives.
    """
    _print_section(f"Table: {table_name}")

    print(f"\nTop-level shape: {_typeof(payload)}")
    if not isinstance(payload, list) or not payload:
        print("\n⚠  Expected a list with at least one element. Got:")
        print(f"   {_sample_value(payload)}")
        return

    print(f"  → length {len(payload)}, peeling [0]\n")

    data = payload[0]
    if not isinstance(data, dict):
        print(f"\n⚠  Expected dict at [0]. Got {_typeof(data)}: {_sample_value(data)}")
        return

    # Infer schema for the WHOLE payload (starting from [0] data).
    stats = infer_schema(data, path="$")

    # Render the schema tree.
    rendered = render_schema(stats)
    print(rendered)

    # Auto-collapse summary.
    list_dict_paths = [
        s for s in stats.values() if s.container_kind == "list[dict]"
    ]
    collapsed_items = sum(
        (s.list_lengths[0] if s.list_lengths else 0) for s in list_dict_paths
    )
    print(
        f"\n  → {len(stats)} unique schema paths "
        f"(collapsed {collapsed_items} repeated list-items into "
        f"{len(list_dict_paths)} list[dict] schemas)"
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
            explore_payload(table, payload)

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
