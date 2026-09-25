#!/usr/bin/env python3
"""What the chat's skill picker would list from the roots this machine
resolves, and what it would not: every pack with its size, whether it
loads whole at the default whole-load limit, its frontmatter policy —
and every file skipped or refusing to load, with the reason.

    python scripts/skills_check.py                          # the .env's roots
    python scripts/skills_check.py --skills-dir /path/to/skills
    python scripts/skills_check.py --graph graph --owner "Ana Lyst"
    python scripts/skills_check.py --model gemini-3.7-flash --json

The roots, in the picker's precedence order (the first pack under a
name wins): the built-in packs, the owner's own folder
(``<graph>/skills/users/<owner>/``), the skills tree
(``MERIDIAN_SKILLS_DIR``, nested folders allowed, the name from the
path as a slug), then ``<graph>/skills``. ``--skills-dir`` stands in
for ``MERIDIAN_SKILLS_DIR``; ``--graph`` for ``MERIDIAN_GRAPH_DIR``.
The store-backed own shelf (``SAHS_STORE=spanner|sqlite``) is not
read here: the folder is, as on a laptop.

Exit 0 when every file the roots hold is listed and loads; 1 when a
file was skipped (a name taken by an earlier pack, a path with no
name) or lists but refuses to load (not UTF-8, a frontmatter with no
end). Nothing here calls a model or writes a file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.assistant.skills_loader import (  # noqa: E402
    BUILTIN, SKILLS_DIR_VAR, Pack, builtin_root, builtin_skills,
    collect_skills, own_skills, owner_slug, shelf_roots, tree_packs,
    user_root, whole_load_limit, _packs)
from sahs.loop.skills import (  # noqa: E402
    CHARS_VAR, frontmatter_warnings, max_skill_chars, policy_of)
from sahs.util.profiles import whole_load_chars_for  # noqa: E402


def _row(pack: Pack, limit: int) -> dict[str, Any]:
    policy = policy_of(pack.text) if not pack.error else policy_of("")
    fits = pack.chars <= limit
    return {"name": pack.name, "origin": pack.origin, "owner": pack.owner,
            "title": pack.title, "chars": pack.chars, "fits": fits,
            "mode": "whole" if fits else "library",
            "runtime_loading": policy.runtime_loading,
            "truncation_allowed": policy.truncation_allowed,
            "preferred": "whole" if policy.requires_whole else "sectioned",
            "description": pack.description, "aliases": list(policy.aliases),
            "path": pack.path, "error": pack.error,
            "warnings": frontmatter_warnings(pack.text) if not pack.error
            else []}


def report(graph_root: Path | None, owner: str = "",
           env: dict[str, str] | None = None, model: str = ""
           ) -> dict[str, Any]:
    """The picker's view as data: the roots with what each holds, the
    whole-load limit, every listed pack, the skipped files, the packs
    that refuse to load, and the frontmatter warnings."""
    env = dict(os.environ if env is None else env)
    limit = whole_load_limit(model, env)
    roots: list[dict[str, Any]] = [{
        "label": "built-in", "path": str(builtin_root()), "exists": True,
        "packs": len(builtin_skills()), "skipped": 0}]
    if graph_root is not None and owner_slug(owner):
        own = user_root(Path(graph_root), owner)
        roots.append({"label": f"own ({owner_slug(owner)})", "path": str(own),
                      "exists": own.is_dir(),
                      "packs": len(own_skills(Path(graph_root), owner)),
                      "skipped": 0})
    for label, root, nested in shelf_roots(graph_root, env):
        if nested:
            packs, dropped = tree_packs(root)
        else:
            packs, dropped = _packs(root, "unreviewed"), []
        roots.append({"label": label, "path": str(root),
                      "exists": root.is_dir(), "packs": len(packs),
                      "skipped": len(dropped)})
    listed, skipped = collect_skills(graph_root, owner, env)
    rows = [_row(p, limit) for p in listed]
    refusing = [{"name": r["name"], "path": r["path"], "reason": r["error"]}
                for r in rows if r["error"]]
    warnings = [{"name": r["name"], "path": r["path"], "warning": w}
                for r in rows for w in r["warnings"]]
    return {"limit": limit,
            "limit_note": (f"min({CHARS_VAR}={max_skill_chars(env):,}, the "
                           f"{model or 'unnamed'} engine's whole-load window "
                           f"{whole_load_chars_for(model, env):,})"),
            "roots": roots, "packs": rows, "skipped": skipped,
            "refusing": refusing, "warnings": warnings,
            "ok": not skipped and not refusing}


def render(data: dict[str, Any]) -> str:
    out = ["Skill roots, in the picker's order (the first pack under a "
           "name wins):"]
    for root in data["roots"]:
        state = ("" if root["exists"] else "  (no such folder)")
        extra = f", {root['skipped']} skipped" if root["skipped"] else ""
        out.append(f"  {root['label']:<22} {root['path']}{state}  "
                   f"{root['packs']} pack{'s' if root['packs'] != 1 else ''}"
                   f"{extra}")
    out.append(f"Whole-load limit: {data['limit']:,} characters = "
               f"{data['limit_note']}")
    out.append("")
    out.append(f"Listed ({len(data['packs'])}):")
    width = max((len(r["name"]) for r in data["packs"]), default=4)
    for r in data["packs"]:
        tag = r["origin"] + (f":{r['owner']}" if r["owner"] else "")
        policy = (f"runtime_loading={r['runtime_loading']} "
                  f"truncation_allowed={str(r['truncation_allowed']).lower()}"
                  f" → {r['preferred']}")
        load = ("REFUSES: " + r["error"] if r["error"]
                else f"{r['mode']} ({r['chars']:,} chars)")
        out.append(f"  {r['name']:<{width}}  {tag:<20} {load}; {policy}"
                   + (f"; aliases: {', '.join(r['aliases'])}"
                      if r["aliases"] else ""))
        if r["path"]:
            out.append(f"  {'':<{width}}  {r['path']}")
    if data["refusing"]:
        out.append("")
        out.append(f"Listed but refusing to load ({len(data['refusing'])}): "
                   "fix the file and load it again")
        for r in data["refusing"]:
            out.append(f"  {r['name']}: {r['reason']}  ({r['path']})")
    if data["skipped"]:
        out.append("")
        out.append(f"Skipped, not listed ({len(data['skipped'])}):")
        for s in data["skipped"]:
            out.append(f"  {s['path']} [{s.get('shelf', '')}]"
                       + (f" as {s['name']!r}" if s.get("name") else "")
                       + f": {s['reason']}")
    if data["warnings"]:
        out.append("")
        out.append(f"Frontmatter read differently from how it was written "
                   f"({len(data['warnings'])}):")
        for w in data["warnings"]:
            out.append(f"  {w['name']}: {w['warning']}")
    out.append("")
    out.append("ok: every file listed loads" if data["ok"] else
               "not ok: a file is skipped or refuses to load (above)")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--graph", default="",
                    help="the graph dir (default MERIDIAN_GRAPH_DIR, else "
                         "the silo's graph/)")
    ap.add_argument("--skills-dir", default="",
                    help=f"stands in for {SKILLS_DIR_VAR}")
    ap.add_argument("--owner", default="",
                    help="whose own folder to read (default "
                         "SYNAPSE_USER_NAME)")
    ap.add_argument("--model", default="",
                    help="the engine whose whole-load window bounds the "
                         "limit (default: the unnamed engine)")
    ap.add_argument("--no-dotenv", action="store_true",
                    help="do not read the silo .env first")
    ap.add_argument("--json", action="store_true", help="print the data")
    args = ap.parse_args(argv)
    if not args.no_dotenv:
        from sahs.util.auth import load_dotenv
        load_dotenv()
    env = dict(os.environ)
    if args.skills_dir:
        env[SKILLS_DIR_VAR] = args.skills_dir
    graph = Path(args.graph or env.get("MERIDIAN_GRAPH_DIR")
                 or SILO / "graph")
    owner = args.owner or env.get("SYNAPSE_USER_NAME", "")
    data = report(graph, owner, env, args.model)
    print(json.dumps(data, indent=2) if args.json else render(data))
    return 0 if data["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
