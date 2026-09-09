#!/usr/bin/env python3
"""Scan a repository for references that should not ship in a published codebase.

Usage:
    python scan.py <repo_root> --terms terms.txt [--out scan.md] [--json]

Reports every hit of the user-supplied terms plus a set of built-in patterns
(emails, internal hostnames, ticket IDs, IPs, home-dir paths, note-style
comments, coding-agent names and config files). Each hit is tagged with where it lives — comment, string,
identifier, filename, doc, or config — because the fix differs by location.
"""
import argparse
import json
import os
import re
import sys

SKIP_DIRS = {".git", ".mystify", "node_modules", ".venv", "venv", "__pycache__", "dist",
             "build", ".mypy_cache", ".pytest_cache", ".tox", "target", ".idea"}
BINARY_EXT = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip", ".gz", ".whl",
              ".pyc", ".so", ".dylib", ".dll", ".parquet", ".pkl", ".bin", ".ico"}
DOC_EXT = {".md", ".rst", ".txt", ".adoc"}
CONFIG_EXT = {".toml", ".yaml", ".yml", ".json", ".ini", ".cfg", ".env",
              ".conf", ".properties", ".lock"}
CONFIG_NAMES = {"Dockerfile", "Makefile", "Jenkinsfile", ".npmrc", ".pypirc",
                "pip.conf", ".env", ".env.example", "CODEOWNERS"}

# Files and directories that reveal a coding agent was used. Reported as
# filename hits regardless of content; the skill deletes them in Phase 4.
AGENT_FILES = {"CLAUDE.md", "AGENTS.md", "GEMINI.md", ".cursorrules",
               ".windsurfrules", ".clinerules", ".aider.conf.yml",
               ".aiderignore", "copilot-instructions.md", "CLAUDE.local.md"}
AGENT_DIRS = {".claude", ".cursor", ".codex", ".aider", ".windsurf",
              ".cline", ".gemini", ".devin"}

COMMENT_RE = re.compile(r"(#|//|/\*|\*|<!--|--|;)\s")
STRING_RE = re.compile(r"""(['"`])(?:(?!\1).)*\1""")

BUILTIN_PATTERNS = {
    "email": re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"),
    "internal_host": re.compile(
        r"\b[\w.-]+\.(corp|internal|local|intranet|lan|aexp|amex)\b", re.I),
    "ticket_id": re.compile(r"\b[A-Z]{2,10}-\d{2,6}\b"),
    "ipv4": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "home_path": re.compile(r"(/Users/|/home/|C:\\\\Users\\\\)[\w.-]+"),
    # bare "cursor" is excluded on purpose: it collides with DB cursors.
    "agent": re.compile(
        r"\b(claude|anthropic|copilot|codex|aider|windsurf|cline|devin|"
        r"cursor\.sh|cursor ai|cursorrules|co-authored-by|"
        r"generated (with|by) [\w ]{0,20}(claude|copilot|cursor|codex|ai)\b)",
        re.I),
    "note_comment": re.compile(
        r"\b(paste|laptop|ask \w+|ping \w+|vpn|on-prem|onprem|prod only|"
        r"internal only|confidential|do not share)\b", re.I),
}


def classify(line, path, in_docstring):
    ext = os.path.splitext(path)[1].lower()
    name = os.path.basename(path)
    if ext in DOC_EXT:
        return "doc"
    if ext in CONFIG_EXT or name in CONFIG_NAMES:
        return "config"
    stripped = line.lstrip()
    if in_docstring or COMMENT_RE.match(stripped):
        return "comment"
    return None  # decide per-hit below


def locate(line, start, in_docstring, base):
    if base:
        return base
    if in_docstring:
        return "comment"
    # comment after code?
    for m in re.finditer(r"(#|//)", line):
        if m.start() < start and not _inside_string(line, m.start()):
            return "comment"
    if _inside_string(line, start):
        return "string"
    return "identifier"


def _inside_string(line, pos):
    return any(m.start() < pos < m.end() for m in STRING_RE.finditer(line))


def scan_file(path, term_res, hits):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except (OSError, UnicodeDecodeError):
        return
    in_doc = False
    for ln, line in enumerate(lines, 1):
        stripped = line.lstrip()
        if line.count('"""') % 2 == 1 or line.count("'''") % 2 == 1:
            in_doc = not in_doc
            in_doc_line = True
        elif stripped.startswith(('"""', "'''")):
            in_doc_line = True  # single-line docstring
        else:
            in_doc_line = in_doc
        base = classify(line, path, in_doc_line)
        for label, rx in list(term_res.items()) + list(BUILTIN_PATTERNS.items()):
            for m in rx.finditer(line):
                where = locate(line, m.start(), in_doc_line, base)
                kind = "term" if label in term_res else "pattern"
                hits.append({"kind": kind, "label": label, "file": path,
                             "line": ln, "where": where,
                             "match": m.group(0), "text": line.rstrip()[:160]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--terms", required=True)
    ap.add_argument("--out")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    with open(args.terms) as f:
        terms = [t.strip() for t in f if t.strip() and not t.startswith("#")]
    term_res = {t: re.compile(r"(?<![A-Za-z0-9])" + re.escape(t) +
                              r"(?![A-Za-z0-9])", re.I) for t in terms}

    hits = []
    for dirpath, dirnames, filenames in os.walk(args.root):
        for d in dirnames:
            if d in AGENT_DIRS:
                hits.append({"kind": "pattern", "label": "agent", "where": "filename",
                             "file": os.path.join(dirpath, d), "line": 0,
                             "match": d, "text": "(agent config directory)"})
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and d not in AGENT_DIRS]
        for d in dirnames:
            for t, rx in term_res.items():
                if rx.search(d):
                    hits.append({"kind": "term", "label": t, "where": "filename",
                                 "file": os.path.join(dirpath, d), "line": 0,
                                 "match": t, "text": "(directory name)"})
        for fn in filenames:
            path = os.path.join(dirpath, fn)
            if fn in AGENT_FILES:
                hits.append({"kind": "pattern", "label": "agent", "where": "filename",
                             "file": path, "line": 0, "match": fn,
                             "text": "(agent instruction file)"})
            for t, rx in term_res.items():
                if rx.search(fn):
                    hits.append({"kind": "term", "label": t, "where": "filename",
                                 "file": path, "line": 0, "match": t,
                                 "text": "(file name)"})
            if os.path.splitext(fn)[1].lower() in BINARY_EXT:
                continue
            scan_file(path, term_res, hits)

    if args.json:
        out = json.dumps(hits, indent=2)
    else:
        out = render_md(hits, terms)
    if args.out:
        with open(args.out, "w") as f:
            f.write(out)
        print(f"{len(hits)} hits -> {args.out}")
    else:
        print(out)
    sys.exit(1 if any(h["kind"] == "term" for h in hits) else 0)


def render_md(hits, terms):
    lines = ["# Mystify scan", "", f"Terms: {', '.join(terms)}", "",
             f"Total hits: {len(hits)} "
             f"(terms: {sum(h['kind']=='term' for h in hits)}, "
             f"patterns: {sum(h['kind']=='pattern' for h in hits)})", ""]
    by_where = {}
    for h in hits:
        by_where.setdefault(h["where"], []).append(h)
    for where in ["filename", "identifier", "string", "comment", "config", "doc"]:
        group = by_where.get(where)
        if not group:
            continue
        lines += [f"## {where} ({len(group)})", "",
                  "| label | file:line | match | text |", "|---|---|---|---|"]
        for h in group:
            text = h['text'].replace('|', '\\|')
            lines.append(f"| {h['label']} | {h['file']}:{h['line']} | "
                         f"`{h['match']}` | `{text}` |")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
