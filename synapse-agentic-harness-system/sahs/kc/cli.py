"""``pipeline.py kc`` and ``pipeline.py kc-coverage`` — the module from
the terminal, with the same console the other stages use."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def add_parsers(sub: Any, silo: Path) -> None:
    p = sub.add_parser("kc", help="one table's Knowledge Catalog bundle")
    p.add_argument("--table", required=True,
                   help="physical name, dataset.table; or 'all' for every "
                        "table in scope (one folder each plus the merged glossary)")
    p.add_argument("--graph", default=None)
    p.add_argument("--builds", default=None)
    p.add_argument("--no-llm", action="store_true",
                   help="deterministic sections only: no model call")
    p.add_argument("--regenerate", action="store_true",
                   help="re-run the model sections even when cached")
    p.add_argument("--export", default="", help="md|json|csv|sheet|zip → --out")
    p.add_argument("--out", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--plain", action="store_true")
    p.add_argument("--json", action="store_true", dest="json_out")
    p.add_argument("--fresh", action="store_true")
    p.set_defaults(fn=cmd_kc)

    p = sub.add_parser("kc-coverage",
                       help="row every graph item against its catalog home")
    p.add_argument("--graph", default=None)
    p.add_argument("--builds", default=None)
    p.add_argument("--docs", default=str(silo / "docs"),
                   help="where kc_coverage.md / kc_coverage.json land")
    p.add_argument("--out", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--plain", action="store_true")
    p.add_argument("--json", action="store_true", dest="json_out")
    p.add_argument("--fresh", action="store_true")
    p.set_defaults(fn=cmd_kc_coverage)


def cmd_kc(args: argparse.Namespace, console: Any) -> int:
    from sahs.kc.bundle import build_bundle
    from sahs.kc.config import load_config
    from sahs.kc.export import export
    console.phase("kc bundle")
    cfg = load_config()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.table == "all":
        from sahs.kc.bundle import export_all
        data, _media, filename = export_all(
            builds_root=Path(args.builds), graph_root=Path(args.graph), config=cfg,
            use_llm=not args.no_llm, log=console.note)
        (out / filename).write_bytes(data)
        console.output(out / filename)
        console.gate("kc_export_all", True, f"{len(data)} bytes, one folder per "
                     "table plus the merged glossary")
        return 0
    bundle = build_bundle(args.table, use_llm=not args.no_llm,
                          regenerate=args.regenerate,
                          builds_root=Path(args.builds), graph_root=Path(args.graph),
                          config=cfg, log=console.note)
    fmt = args.export or "md"
    data, _media, filename = export(bundle, fmt, cfg)
    (out / filename).write_bytes(data)
    console.output(out / filename)
    for key, sec in bundle.sections.items():
        console.note(f"{key:16s} {'copy-ready' if sec.text else 'empty: ' + sec.empty_reason}"
                     + (f" · {len(sec.review)} to review" if sec.review else "")
                     + (" · model" if sec.llm else ""))
    gate = bundle.gate
    if gate.get("line"):
        console.note(gate["line"])
    console.gate("kc_facts", bool(bundle.facts),
                 f"{len(bundle.facts)} facts: {bundle.counts.get('copy', 0)} copy · "
                 f"{bundle.counts.get('review', 0)} review · "
                 f"{bundle.counts.get('never', 0)} never")
    llm = bundle.llm
    console.gate("kc_model", not (llm["enabled"] and llm.get("reason", "").startswith("model call failed")),
                 (f"{llm['calls']} call(s), {llm['invalid_json']} malformed, "
                  f"{llm['dropped']} sentence(s) dropped by the verifier"
                  if llm["enabled"] else llm["reason"])
                 + (f" · {llm['reason']}" if llm["enabled"] and llm["reason"] else ""))
    return 0


def cmd_kc_coverage(args: argparse.Namespace, console: Any) -> int:
    from sahs.kc.coverage import build_root_of, write_coverage_docs
    console.phase("kc coverage")
    build_root = build_root_of(Path(args.builds))
    report = write_coverage_docs(Path(args.graph), build_root, Path(args.docs))
    console.output(Path(args.docs) / "kc_coverage.md")
    console.output(Path(args.docs) / "kc_coverage.json")
    counts = report["counts"]
    console.note(f"rows {counts['rows']} · observed {counts['observed']} · "
                 f"targeted {counts['pct_targeted']}% · suggestion-only "
                 f"{counts['pct_suggestion_only']}% · excluded "
                 f"{counts['pct_excluded_with_reason']}%")
    ok = console.gate("kc_coverage_complete", not report["missing"],
                      "every item rowed" if not report["missing"]
                      else f"{len(report['missing'])} unrowed: "
                           + ", ".join(report["missing"][:8]))
    if report["missing"]:
        from sahs.kc.coverage import stub_rows
        console.note("row stubs for sahs/kc/coverage.py ROWS (fill the "
                     "<placeholders>; a row may be EXCLUDED with a reason):")
        for line in stub_rows(report["missing"]).splitlines():
            console.note(line)
    return 0 if ok else 3
