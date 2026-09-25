#!/usr/bin/env python3
"""Check skill retrieval on your own packs: the table of contents
sizes and the top hits for a query, the way the chat would see them.

    python scripts/skill_index_check.py <skills dir> "<query>"
    python scripts/skill_index_check.py graph/skills "how is spend reconciled" --k 5
    python scripts/skill_index_check.py graph/skills "fiscal quarter" --skill fiscal-notes --toc-depth 3

Every ``*.md`` under the directory (nested folders included) is a
pack; the index lives at ``<skills dir>/../runs/skill_index.sqlite3``
unless ``--index`` names another file or ``--memory`` keeps it in
memory. Unchanged packs cost a hash; changed ones re-index. Nothing
here calls a model.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.loop.skill_index import INDEX_FILE, SkillIndex  # noqa: E402
from sahs.loop.skills import _parse  # noqa: E402


@dataclass(frozen=True)
class Source:
    name: str
    title: str
    text: str
    updated: str = ""


def sources(skills_dir: Path) -> list[Source]:
    out = []
    for path in sorted(skills_dir.rglob("*.md")):
        skill = _parse(path)
        name = path.relative_to(skills_dir).with_suffix("").as_posix()
        out.append(Source(name, skill.title, skill.text,
                          str(path.stat().st_mtime)))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("skills_dir", help="a folder of *.md packs")
    ap.add_argument("query", help="the words the passage would use")
    ap.add_argument("--k", type=int, default=8, help="hits to show")
    ap.add_argument("--skill", default="", help="one pack only")
    ap.add_argument("--toc-depth", type=int, default=2,
                    help="heading levels of the contents to print (0: all)")
    ap.add_argument("--index", default="",
                    help="the sqlite file (default <dir>/../runs/"
                         + INDEX_FILE + ")")
    ap.add_argument("--memory", action="store_true",
                    help="index in memory, write nothing")
    args = ap.parse_args(argv)

    skills_dir = Path(args.skills_dir)
    if not skills_dir.is_dir():
        print(f"not a directory: {skills_dir}", file=sys.stderr)
        return 2
    packs = sources(skills_dir)
    if not packs:
        print(f"no *.md packs under {skills_dir}", file=sys.stderr)
        return 2
    if args.skill:
        packs = [p for p in packs if p.name == args.skill]
        if not packs:
            print(f"no pack named {args.skill!r} under {skills_dir}",
                  file=sys.stderr)
            return 2
    where = None if args.memory else (
        Path(args.index) if args.index
        else skills_dir.resolve().parent / "runs" / INDEX_FILE)
    print(f"Index: {where or 'memory'} (derived data, safe to delete)")
    index = SkillIndex(where)
    t0 = time.perf_counter()
    status = index.ensure(packs)
    print(f"Skills ({len(packs)}, {time.perf_counter() - t0:.2f}s):")
    for pack in packs:
        info = index.overview(pack.name) or {}
        print(f"  {pack.name}  {len(pack.text):,} chars  "
              f"{info.get('sections', 0):,} sections  "
              f"{info.get('chunks', 0):,} chunks  {status[pack.name]}")
    for pack in packs:
        toc = index.toc(pack.name, max_level=args.toc_depth)
        total = len(index.toc(pack.name))
        print(f"Contents of {pack.name} "
              f"({'all' if not args.toc_depth else f'levels 1-{args.toc_depth}'}; "
              f"{len(toc)} of {total} sections):")
        for s in toc:
            print(f"  {s['section_id']} · {s['heading_path']} "
                  f"({s['chunks']} chunk{'s' if s['chunks'] != 1 else ''}, "
                  f"{s['chars']:,} chars)")
    print(f"Query: {args.query!r}")
    # the routing hint the chat uses to order its shelf: every pack's
    # frontmatter description, aliases and headings
    index.ensure_routing(packs)
    likely = index.rank_skills(args.query, k=3)
    print("Likely skills (by description, aliases, headings): "
          + (", ".join(f"{r['skill']} ({r['score']:.1f})" for r in likely)
             or "none"))
    hits = index.search(args.query, [p.name for p in packs], k=args.k)
    if not hits:
        print("  no passage matched: try the pack's own words")
        return 1
    for n, hit in enumerate(hits, 1):
        print(f"  {n}. {hit.score:6.2f}  {hit.skill}  {hit.chunk_id}  "
              f"{hit.heading_path}  (chars {hit.start:,}–{hit.end:,})")
        print(f"     {hit.snippet}")
    index.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
