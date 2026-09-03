#!/usr/bin/env python
"""std_tech_keys — what does the Atlas feed SEND, and what do we KEEP?

The loader picks ``std_tech_metadata`` fields by name (a whitelist),
so a key the real feed carries that the fixture never had is dropped
at the record boundary with no trace. This census walks every entry
the loader would harvest, counts every key at every layer, and marks
each one against ``STD_TECH_CONSUMED_KEYS``:

    consumed     → reaches a record (and, per the contract, the graph)
    deferred     → read and deliberately not carried; reason pinned
    edge+prop    → an ``ownership`` key that ALSO became an owned_by edge
    prop only    → an ``ownership`` key kept whole in ownership_atlas but
                   not recognised as a person — a steward, a custodian
    UNCONSUMED   → the feed sends it, nothing keeps it. Decide.

Run it against the real archive BEFORE deciding what to add to the
loader — one cannot classify what one has not enumerated::

    python scripts/std_tech_keys.py $SRC/std_tech_metadata
    python scripts/std_tech_keys.py $SRC/std_tech_metadata_all.json \
        --json /tmp/keys.json
    python scripts/std_tech_keys.py \
        tests/fixtures/sources/std_tech_metadata --strict   # CI: none

Read-only. Never touches the graph.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.loaders.sources.vocab import (          # noqa: E402
    STD_TECH_CONSUMED_KEYS,
    STD_TECH_DEFERRED_KEYS,
    ownership_key_is_person,
)

SCHEMA = "meridian.std_tech_key_census/1"
LAYERS = ["envelope", "entry", "datasetAttribute", "ownership",
          "pii_columns[]", "pde", "pdeAttribute", "businessMetadata[]"]
_SAMPLE_LEN = 48


def _shape(value: Any) -> str:
    if isinstance(value, dict):
        return "object{" + ",".join(sorted(value)[:6]) + \
            ("…" if len(value) > 6 else "") + "}"
    if isinstance(value, list):
        return f"list[{len(value)}]"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    return "string"


def _sample(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return _shape(value)
    text = json.dumps(value) if not isinstance(value, str) else value
    text = " ".join(text.split())
    return text if len(text) <= _SAMPLE_LEN else text[:_SAMPLE_LEN - 1] + "…"


def _empty(value: Any) -> bool:
    return value in (None, "", [], {})


class Census:
    def __init__(self, samples: int = 3) -> None:
        self.samples = samples
        # (layer, key) → counters
        self.keys: dict[tuple[str, str], dict] = defaultdict(
            lambda: {"carriers": 0, "nonempty": 0, "shapes": set(),
                     "samples": []})
        self.carriers: dict[str, int] = defaultdict(int)   # per layer
        self.files = 0
        self.unparsed: list[str] = []

    def see(self, layer: str, obj: dict) -> None:
        self.carriers[layer] += 1
        for key, value in obj.items():
            cell = self.keys[(layer, key)]
            cell["carriers"] += 1
            cell["shapes"].add(_shape(value))
            if _empty(value):
                continue
            cell["nonempty"] += 1
            s = _sample(value)
            if s not in cell["samples"] and len(cell["samples"]) < \
                    self.samples:
                cell["samples"].append(s)

    # ── the walk mirrors the loader's harvest signature exactly ──
    def walk(self, node: Any) -> None:
        if isinstance(node, dict):
            if "dataset" in node and ("pde" in node
                                      or "datasetAttribute" in node):
                self._entry(node)
                return
            if "dataset" in node and isinstance(
                    node.get("tech_metadata_list"), list):
                self.see("envelope", node)
                for item in node["tech_metadata_list"]:
                    if isinstance(item, dict) and (
                            "pde" in item or "datasetAttribute" in item):
                        self._entry(item)
                return
            for value in node.values():
                self.walk(value)
        elif isinstance(node, list):
            for item in node:
                self.walk(item)

    def _entry(self, entry: dict) -> None:
        self.see("entry", entry)
        attr = entry.get("datasetAttribute")
        if isinstance(attr, dict):
            self.see("datasetAttribute", attr)
            if isinstance(attr.get("ownership"), dict):
                self.see("ownership", attr["ownership"])
            for cell in attr.get("pii_columns") or []:
                if isinstance(cell, dict):
                    self.see("pii_columns[]", cell)
        for pde in entry.get("pde") or []:
            if not isinstance(pde, dict):
                continue
            self.see("pde", pde)
            if isinstance(pde.get("pdeAttribute"), dict):
                self.see("pdeAttribute", pde["pdeAttribute"])
            for link in pde.get("businessMetadata") or []:
                if isinstance(link, dict):
                    self.see("businessMetadata[]", link)

    def load(self, root: Path) -> None:
        paths = [root] if root.is_file() else sorted(root.glob("*.json"))
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as e:
                self.unparsed.append(f"{path.name}: {e}")
                continue
            self.files += 1
            self.walk(payload)

    # ── classification ──
    @staticmethod
    def status(layer: str, key: str) -> tuple[str, str]:
        if layer == "ownership":
            return (("edge+prop", "owned_by edge + ownership_atlas prop")
                    if ownership_key_is_person(key)
                    else ("prop only", "kept in ownership_atlas; not "
                                       "recognised as a person"))
        reason = STD_TECH_DEFERRED_KEYS.get(layer, {}).get(key)
        if reason:
            return "deferred", reason
        if key in STD_TECH_CONSUMED_KEYS.get(layer, frozenset()):
            return "consumed", ""
        return "UNCONSUMED", "the feed sends it; nothing keeps it"

    def report(self) -> dict:
        rows = []
        for (layer, key), cell in self.keys.items():
            status, note = self.status(layer, key)
            rows.append({
                "layer": layer, "key": key, "status": status,
                "note": note, "carriers": cell["carriers"],
                "layer_carriers": self.carriers[layer],
                "nonempty": cell["nonempty"],
                "shapes": sorted(cell["shapes"]),
                "samples": cell["samples"]})
        rows.sort(key=lambda r: (LAYERS.index(r["layer"])
                                 if r["layer"] in LAYERS else 99,
                                 r["key"]))
        by_status: dict[str, int] = defaultdict(int)
        for r in rows:
            by_status[r["status"]] += 1
        return {"schema": SCHEMA, "files": self.files,
                "unparsed": self.unparsed,
                "layer_carriers": dict(self.carriers),
                "counts": dict(by_status),
                "unconsumed": [f"{r['layer']}.{r['key']}" for r in rows
                               if r["status"] == "UNCONSUMED"],
                "rows": rows}


def render(rep: dict) -> str:
    out = [f"std_tech key census — {rep['files']} file(s), "
           + ", ".join(f"{k}={v}" for k, v in sorted(
               rep["layer_carriers"].items(),
               key=lambda kv: LAYERS.index(kv[0])
               if kv[0] in LAYERS else 99))]
    if rep["unparsed"]:
        out.append("  unparsed: " + "; ".join(rep["unparsed"]))
    out.append("")
    current = None
    for r in rep["rows"]:
        if r["layer"] != current:
            current = r["layer"]
            out.append(f"[{current}]  ({r['layer_carriers']} carrier(s))")
        flag = "  " if r["status"] in ("consumed", "edge+prop") else "! "
        sample = " · ".join(r["samples"]) if r["samples"] else "—"
        out.append(f"{flag}{r['key']:<28} {r['status']:<11} "
                   f"{r['nonempty']:>4}/{r['carriers']:<4} "
                   f"{'/'.join(r['shapes']):<10} {sample}")
        if r["note"] and r["status"] != "edge+prop":
            out.append(f"{'':32}↳ {r['note']}")
    out.append("")
    counts = rep["counts"]
    out.append("keys: " + ", ".join(f"{k} {v}" for k, v in sorted(
        counts.items())))
    if rep["unconsumed"]:
        out.append("UNCONSUMED (decide each — a prop, an edge, or a pinned "
                   "deferral): " + ", ".join(rep["unconsumed"]))
    else:
        out.append("every key the feed sends is consumed or deferred "
                   "with a reason.")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("path", type=Path,
                    help="std_tech_metadata/ dir or the combined "
                         "std_tech_metadata_all.json")
    ap.add_argument("--json", type=Path, help="write the census here")
    ap.add_argument("--samples", type=int, default=3)
    ap.add_argument("--strict", action="store_true",
                    help="exit 2 if any key is UNCONSUMED")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    if not args.path.exists():
        print(f"no such path: {args.path}", file=sys.stderr)
        return 1
    census = Census(samples=args.samples)
    census.load(args.path)
    rep = census.report()
    if args.json:
        args.json.write_text(json.dumps(rep, indent=1, sort_keys=True))
    if not args.quiet:
        print(render(rep))
    return 2 if (args.strict and rep["unconsumed"]) else 0


if __name__ == "__main__":
    sys.exit(main())
