"""Skill retrieval: a library with a card catalogue.

A skill pack can be a book — a doctrine pack or a runtime knowledge
bundle runs to megabytes, far past what any engine reads whole, and
past what belongs in a prompt even where it would fit. So a pack over
the whole-load ceiling (``SAHS_MAX_SKILL_CHARS``) is not refused any
more: it is CHUNKED by its Markdown structure and INDEXED, and the
model reads the catalogue (the table of contents) and then only the
pages it needs (the chunks a query ranks first).

The chunker (``chunk_markdown``): headings first, then paragraph
boundaries; ~1,200 tokens a chunk with ~150 tokens of overlap taken
as whole blocks from the previous chunk; a table row and a fenced
code block are never split; every chunk carries its heading path
("H1 > H2 > H3") as the breadcrumb the model cites. Offsets are into
the original text, so a chunk is always a slice of the pack.

The index (``SkillIndex``): one sqlite file with an FTS5 table ranked
by BM25, at ``<graph>/runs/skill_index.sqlite3`` — derived data, safe
to delete. It is keyed by (skill name, content hash, chunker version):
an unchanged skill is never re-chunked, a changed one re-indexes on
its next load, and only itself. Built lazily on first use.

It never fails a turn. A file that cannot be opened or written (a
read-only disk, a corrupt file) falls back to one in-memory index for
the process, logged once per path; a pack the chunker cannot read
falls back to a single chunk (the whole text as one section), logged
once per pack, so it is still searchable and readable — and it is
re-tried on the next load, since the fallback row carries no chunker
version.

Query expansion is lexical and deterministic: the query's terms, a
small stemming of plural and verb endings, FTS5 prefix matching on
the stem, the terms ANDed first and ORed to fill. ``Embedder`` is the
seam for an embedding-based reranker later; it is None by default and
nothing here calls an embedding service.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import logging
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Protocol

_log = logging.getLogger("sahs.loop.skill_index")

CHUNKER_VERSION = 1
FALLBACK_CHUNKER_VERSION = 0    # a single-chunk row: re-tried next load
CHARS_PER_TOKEN = 4                     # the estimate every budget uses
TARGET_TOKENS = 1200
OVERLAP_TOKENS = 150
TARGET_CHARS = TARGET_TOKENS * CHARS_PER_TOKEN      # 4,800
OVERLAP_CHARS = OVERLAP_TOKENS * CHARS_PER_TOKEN    # 600
INDEX_FILE = "skill_index.sqlite3"
PREAMBLE = "(preamble)"                 # text before the first heading
SNIPPET_TOKENS = 24

# words that carry no lookup value: dropped from every query
STOPWORDS = frozenset((
    "a an and are as at be by can could do does did for from has have "
    "how i if in is it its me my of on or our should that the their "
    "them then there these they this those to us was we were what when "
    "where which who whom why will with would you your about into over "
    "any all some").split())


def estimate_tokens(text: str) -> int:
    return (len(text) + CHARS_PER_TOKEN - 1) // CHARS_PER_TOKEN


# ─── the chunker ─────────────────────────────────────────────


@dataclass(frozen=True)
class Block:
    kind: str           # heading | para | fence | table
    start: int
    end: int
    level: int = 0      # headings only
    title: str = ""     # headings only
    rows: tuple[tuple[int, int], ...] = ()    # tables: each row's span


@dataclass(frozen=True)
class Chunk:
    skill: str
    chunk_id: str       # c1, c2, … in document order
    section_id: str     # s1, s2, … the section it belongs to
    heading_path: str   # "H1 > H2 > H3"
    start: int
    end: int
    text: str
    title: str = ""     # the section's own heading (the path's last)

    @property
    def chars(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class Section:
    skill: str
    section_id: str
    heading_path: str
    level: int
    title: str
    start: int
    end: int
    chunk_ids: tuple[str, ...]

    @property
    def chars(self) -> int:
        return self.end - self.start

    @property
    def chunks(self) -> int:
        return len(self.chunk_ids)


_HEADING = re.compile(r"^(#{1,6})[ \t]+(.*?)[ \t#]*$")
_FENCE = re.compile(r"^[ \t]{0,3}(`{3,}|~{3,})")
_TABLE_ROW = re.compile(r"^[ \t]{0,3}\|")


def _blocks(text: str) -> list[Block]:
    """The Markdown as blocks with exact spans: headings, paragraphs,
    fenced code (atomic), tables (rows never split)."""
    blocks: list[Block] = []
    pos = 0
    lines: list[tuple[int, int, str]] = []       # (start, end, line)
    for line in text.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        lines.append((pos, pos + len(stripped), stripped))
        pos += len(line)
    i, n = 0, len(lines)
    while i < n:
        start, end, line = lines[i]
        if not line.strip():
            i += 1
            continue
        fence = _FENCE.match(line)
        if fence:
            marker = fence.group(1)
            j = i + 1
            while j < n:
                closing = _FENCE.match(lines[j][2])
                if closing and closing.group(1)[0] == marker[0] \
                        and len(closing.group(1)) >= len(marker) \
                        and not lines[j][2].strip()[len(closing.group(1)):].strip():
                    break
                j += 1
            last = min(j, n - 1)
            blocks.append(Block("fence", start, lines[last][1]))
            i = last + 1
            continue
        heading = _HEADING.match(line)
        if heading:
            blocks.append(Block("heading", start, end,
                                level=len(heading.group(1)),
                                title=heading.group(2).strip()))
            i += 1
            continue
        if _TABLE_ROW.match(line):
            j = i
            rows: list[tuple[int, int]] = []
            while j < n and _TABLE_ROW.match(lines[j][2]):
                rows.append((lines[j][0], lines[j][1]))
                j += 1
            blocks.append(Block("table", rows[0][0], rows[-1][1],
                                rows=tuple(rows)))
            i = j
            continue
        j = i
        while j < n and lines[j][2].strip() \
                and not _HEADING.match(lines[j][2]) \
                and not _FENCE.match(lines[j][2]) \
                and not _TABLE_ROW.match(lines[j][2]):
            j += 1
        blocks.append(Block("para", start, lines[j - 1][1]))
        i = j
    return blocks


def _units(text: str, block: Block, target: int) -> list[tuple[int, int]]:
    """The spans the packer may place separately: a fence whole; a
    table whole when it fits, else its rows (a row never splits); a
    paragraph whole when it fits, else its lines, else hard cuts."""
    size = block.end - block.start
    if block.kind == "fence" or size <= target:
        return [(block.start, block.end)]
    if block.kind == "table":
        return list(block.rows)
    out: list[tuple[int, int]] = []
    pos = block.start
    for line in text[block.start:block.end].splitlines(keepends=True):
        body = line.rstrip("\r\n")
        if len(body) <= target:
            out.append((pos, pos + len(body)))
        else:
            for cut in range(0, len(body), target):
                out.append((pos + cut, pos + min(cut + target, len(body))))
        pos += len(line)
    return out


def chunk_markdown(text: str, skill: str = "", *,
                   target: int = TARGET_CHARS,
                   overlap: int = OVERLAP_CHARS
                   ) -> tuple[list[Section], list[Chunk]]:
    """Sections (the table of contents) and chunks (the pages) of one
    skill. Deterministic: the same text always chunks the same way."""
    blocks = _blocks(text)
    sections: list[Section] = []
    chunks: list[Chunk] = []
    path: list[str] = []
    # group blocks into sections: a heading opens one
    groups: list[tuple[Block | None, list[Block]]] = []
    for block in blocks:
        if block.kind == "heading":
            groups.append((block, []))
        elif groups:
            groups[-1][1].append(block)
        else:
            groups.append((None, [block]))

    def _pack(units: list[tuple[int, int]], section_id: str,
              breadcrumb: str, first_start: int, title: str) -> list[str]:
        """Greedy packing of spans into chunks under the target, with
        whole trailing spans as the overlap into the next chunk."""
        ids: list[str] = []
        current: list[tuple[int, int]] = []
        chunk_start = first_start

        def close() -> None:
            nonlocal current, chunk_start
            if not current:
                return
            start, end = chunk_start, current[-1][1]
            cid = f"c{len(chunks) + 1}"
            chunks.append(Chunk(skill, cid, section_id, breadcrumb,
                                start, end, text[start:end], title))
            ids.append(cid)
            # the overlap: whole spans from the tail, within the budget
            tail: list[tuple[int, int]] = []
            used = 0
            for span in reversed(current):
                size = span[1] - span[0]
                if used + size > overlap:
                    break
                tail.insert(0, span)
                used += size
            current = tail
            chunk_start = tail[0][0] if tail else end

        for span in units:
            if current and span[1] - chunk_start > target:
                close()
                if current and span[1] - chunk_start > target:
                    # even with the overlap it overflows: drop it
                    current, chunk_start = [], span[0]
            if not current:
                chunk_start = span[0]
            current.append(span)
        # the trailing chunk: only when it holds something beyond the
        # overlap it inherited
        if current and (len(ids) == 0 or current[-1][1] > chunks[-1].end):
            close()
        return ids

    for heading, body in groups:
        if heading is not None:
            level = heading.level
            del path[level - 1:]
            while len(path) < level - 1:
                path.append("")
            path.append(heading.title)
            breadcrumb = " > ".join(p for p in path if p)
            start = heading.start
            title = heading.title
        else:
            level, breadcrumb, start, title = 0, PREAMBLE, body[0].start, PREAMBLE
        section_id = f"s{len(sections) + 1}"
        units: list[tuple[int, int]] = []
        if heading is not None:
            units.append((heading.start, heading.end))
        for block in body:
            units.extend(_units(text, block, target))
        end = units[-1][1] if units else start
        ids = _pack(units, section_id, breadcrumb, start, title)
        sections.append(Section(skill, section_id, breadcrumb, level,
                                title, start, end, tuple(ids)))
    return sections, chunks


# ─── the query: lexical, stemmed, deterministic ──────────────


def stem_variants(term: str) -> list[str]:
    """The term and a few inflections folded (plural, -ing, -ed):
    'settlements' → settlements, settlement; 'reconciling' →
    reconciling, reconcil, reconcile. Small on purpose."""
    out = [term]
    t = term
    if t.endswith("ies") and len(t) > 4:
        out.append(t[:-3] + "y")
    elif t.endswith("sses") and len(t) > 5:
        out.append(t[:-2])
    elif t.endswith("es") and len(t) > 4:
        out += [t[:-1], t[:-2]]
    elif t.endswith("s") and not t.endswith("ss") and len(t) > 3:
        out.append(t[:-1])
    if t.endswith("ing") and len(t) > 5:
        out += [t[:-3], t[:-3] + "e"]
    elif t.endswith("ed") and len(t) > 4:
        out += [t[:-2], t[:-1]]
    seen: list[str] = []
    for v in out:
        if v and v not in seen:
            seen.append(v)
    return seen


def query_terms(query: str, limit: int = 12) -> list[str]:
    words = re.findall(r"[a-z0-9][a-z0-9_.\-/]*", (query or "").lower())
    terms: list[str] = []
    for w in words:
        w = w.strip(".-_/")
        if len(w) < 2 or w in STOPWORDS or w in terms:
            continue
        terms.append(w)
    return terms[:limit]


def _fts_group(term: str) -> str:
    variants = stem_variants(term)
    parts = [f'"{v}"' for v in variants]
    stem = min(variants, key=len)
    if len(stem) >= 4:
        parts.append(f'"{stem}"*')
    return "(" + " OR ".join(parts) + ")"


def fts_queries(query: str, terms: list[str] | None = None) -> list[str]:
    """The match strings, strict then loose: every term-group
    required, then any. The loose one only fills what the strict one
    left. ``terms`` overrides the query's own (the index passes the
    terms that occur somewhere in the searched packs: a word absent
    from the whole library cannot pick a page, so it must not veto
    the strict match)."""
    groups = [_fts_group(t) for t in (query_terms(query)
                                      if terms is None else terms)]
    if not groups:
        return []
    out = [" AND ".join(groups)]
    if len(groups) > 1:
        out.append(" OR ".join(groups))
    return out


# ─── the reranker seam ───────────────────────────────────────


class Embedder(Protocol):
    """SEAM: an embedding-based reranker, plugged in later. Given
    texts, their vectors. The index never constructs one; pass it to
    ``SkillIndex(embedder=…)`` and search reranks its lexical
    candidates by cosine similarity to the query. Nothing here calls
    an embedding service."""

    def embed(self, texts: list[str]) -> list[list[float]]: ...


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


# ─── the index ───────────────────────────────────────────────


@dataclass(frozen=True)
class Hit:
    skill: str
    chunk_id: str
    section_id: str
    heading_path: str
    score: float
    snippet: str
    start: int
    end: int

    def as_row(self) -> dict[str, Any]:
        return {"skill": self.skill, "chunk_id": self.chunk_id,
                "section_id": self.section_id,
                "heading_path": self.heading_path,
                "score": round(self.score, 3), "snippet": self.snippet,
                "start": self.start, "end": self.end}


class SkillSource(Protocol):
    """What the index takes: any object with a name, a title and the
    text — a file-backed Skill, a Pack, or a store-backed row. An
    optional ``updated`` / ``version`` / ``mtime`` is recorded, but
    the key is the content hash: text decides, not the clock."""

    name: str
    title: str
    text: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS skills(
    name TEXT PRIMARY KEY, content_hash TEXT NOT NULL,
    chunker_version INTEGER NOT NULL, title TEXT, version TEXT,
    chars INTEGER, sections INTEGER, chunks INTEGER, indexed_at TEXT,
    text TEXT);
CREATE TABLE IF NOT EXISTS sections(
    skill TEXT, section_id TEXT, heading_path TEXT, level INTEGER,
    title TEXT, start INTEGER, end INTEGER, chunks INTEGER,
    chars INTEGER, chunk_ids TEXT, ord INTEGER,
    PRIMARY KEY(skill, section_id));
CREATE TABLE IF NOT EXISTS chunks(
    skill TEXT, chunk_id TEXT, section_id TEXT, heading_path TEXT,
    start INTEGER, end INTEGER, text TEXT, ord INTEGER, title TEXT,
    PRIMARY KEY(skill, chunk_id));
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    skill UNINDEXED, chunk_id UNINDEXED, title, heading_path, text,
    tokenize='unicode61 remove_diacritics 2');
CREATE TABLE IF NOT EXISTS routing(
    name TEXT PRIMARY KEY, content_hash TEXT NOT NULL, title TEXT,
    description TEXT, aliases TEXT, headings TEXT, indexed_at TEXT);
CREATE VIRTUAL TABLE IF NOT EXISTS routing_fts USING fts5(
    skill UNINDEXED, title, description, aliases, headings,
    tokenize='unicode61 remove_diacritics 2');
"""
# the routing hint's weights, in routing_fts column order: an alias
# is the strongest signal (someone wrote it for this), then the
# description and the title, then the headings
_ROUTING_WEIGHTS = "0.0, 2.0, 2.0, 3.0, 1.0"
# BM25 column weights, in the FTS table's column order: the section's
# own title counts double, its inherited breadcrumb half (else every
# subsection outranks its parent on the parent's words), the text once
_WEIGHTS = "0.0, 0.0, 2.0, 0.5, 1.0"
_TEXT_COLUMN = 4


def index_path(graph_root: Path | None) -> Path | None:
    """``<graph>/runs/skill_index.sqlite3``; None (memory) without a
    graph root."""
    if graph_root is None:
        return None
    return Path(graph_root) / "runs" / INDEX_FILE


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# the process's in-memory fallbacks, one per file path that could not
# be used, so a turn after the first does not re-chunk what the last
# one indexed; plus the paths already warned about (logged once)
_MEMORY_FALLBACKS: dict[str, sqlite3.Connection] = {}
_WARNED: set[str] = set()
_FALLBACK_LOCK = threading.Lock()


def _warn_once(key: str, message: str) -> None:
    with _FALLBACK_LOCK:
        if key in _WARNED:
            return
        _WARNED.add(key)
    _log.warning(message)


def _memory_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:", timeout=30.0, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(_SCHEMA)
    return db


def _shared_memory_db(path: str) -> sqlite3.Connection:
    """The process's fallback index for a file that cannot be used:
    created once per path, shared by every SkillIndex that falls
    back on it."""
    with _FALLBACK_LOCK:
        db = _MEMORY_FALLBACKS.get(path)
        if db is None:
            db = _memory_db()
            _MEMORY_FALLBACKS[path] = db
        return db


class SkillIndex:
    """The card catalogue and the pages, over sqlite FTS5. ``fallback``
    says why the index is in memory instead of at ``path`` ('' when
    the file is in use); ``single_chunk`` names the skills the
    chunker could not read, indexed as one chunk each."""

    def __init__(self, path: Path | str | None = None,
                 embedder: Embedder | None = None) -> None:
        self.path = Path(path) if path not in (None, ":memory:") else None
        self.embedder = embedder
        self.chunked: list[str] = []        # skills chunked this process
        self.fallback = ""                  # why memory, when it is
        self.single_chunk: list[str] = []   # packs the chunker gave up on
        self._shared = False                # a process-wide memory db
        if self.path is None:
            self._db = _memory_db()
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(str(self.path), timeout=30.0,
                                       check_same_thread=False)
            self._db.row_factory = sqlite3.Row
            self._db.executescript(_SCHEMA)
        except (sqlite3.Error, OSError) as e:
            self._go_memory(f"cannot open {self.path}: {e}")

    def _go_memory(self, reason: str) -> None:
        """Fall back to the process's in-memory index for this path:
        the turn goes on, the file is left alone, and the reason is
        logged once per path."""
        key = str(self.path) if self.path else ":memory:"
        _warn_once(key, f"skill index: {reason}; using an in-memory "
                        "index for this process (the file is derived "
                        "data: delete it, or fix the disk, and it "
                        "rebuilds)")
        try:
            if getattr(self, "_db", None) is not None and not self._shared:
                self._db.close()
        except sqlite3.Error:
            pass
        self.fallback = reason
        self._db = _shared_memory_db(key)
        self._shared = True

    def close(self) -> None:
        if not self._shared:
            self._db.close()

    def __enter__(self) -> "SkillIndex":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ── building ─────────────────────────────────────────────
    def status(self, name: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT name, content_hash, chunker_version, title, version, "
            "chars, sections, chunks, indexed_at FROM skills WHERE name=?",
            (name,)).fetchone()
        return dict(row) if row else None

    def ensure(self, sources: Iterable[SkillSource]) -> dict[str, str]:
        """Index what changed, skip what did not. Returns name →
        indexed | reindexed | unchanged. A write the file refuses
        (read-only, full, corrupt) moves the index to memory and
        indexes every source there — the turn never fails."""
        sources = list(sources)
        try:
            return self._ensure(sources)
        except sqlite3.Error as e:
            if self._shared:
                raise
            self._go_memory(f"cannot write {self.path}: {e}")
            return self._ensure(sources)

    def _ensure(self, sources: list[SkillSource]) -> dict[str, str]:
        out: dict[str, str] = {}
        for source in sources:
            name = str(source.name)
            digest = content_hash(source.text)
            current = self.status(name)
            if current and current["content_hash"] == digest \
                    and current["chunker_version"] == CHUNKER_VERSION:
                out[name] = "unchanged"
                continue
            self._index(source, digest)
            self.chunked.append(name)
            out[name] = "reindexed" if current else "indexed"
        return out

    def _chunk(self, source: SkillSource
               ) -> tuple[list[Section], list[Chunk], int]:
        """The chunker's sections and chunks — or, when it throws on
        this pack, one section and one chunk holding the whole text
        (still searchable, still readable, offsets exact), logged
        once per pack and re-tried on the next load."""
        name = str(source.name)
        try:
            sections, chunks = chunk_markdown(source.text, name)
            return sections, chunks, CHUNKER_VERSION
        except Exception as e:    # noqa: BLE001 — any chunker fault
            _warn_once(f"chunk:{name}",
                       f"skill index: chunking {name!r} failed ({e!r}); "
                       "indexed as a single chunk (the whole text as one "
                       "section) until the next load")
            if name not in self.single_chunk:
                self.single_chunk.append(name)
            title = str(source.title or name)
            text = source.text
            chunk = Chunk(name, "c1", "s1", title, 0, len(text), text, title)
            section = Section(name, "s1", title, 1, title, 0, len(text),
                              ("c1",))
            return [section], [chunk], FALLBACK_CHUNKER_VERSION

    def drop(self, name: str) -> None:
        """Forget a skill's pages (its routing row stays until its
        text changes or ``drop_routing``)."""
        with self._db:
            self._db.execute("DELETE FROM chunks_fts WHERE skill=?", (name,))
            self._db.execute("DELETE FROM chunks WHERE skill=?", (name,))
            self._db.execute("DELETE FROM sections WHERE skill=?", (name,))
            self._db.execute("DELETE FROM skills WHERE name=?", (name,))

    def _index(self, source: SkillSource, digest: str) -> None:
        name = str(source.name)
        sections, chunks, chunker_version = self._chunk(source)
        version = str(getattr(source, "updated", "")
                      or getattr(source, "version", "")
                      or getattr(source, "mtime", "") or "")
        stamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat(
            timespec="seconds")
        with self._db:
            self.drop(name)
            self._db.execute(
                "INSERT INTO skills VALUES (?,?,?,?,?,?,?,?,?,?)",
                (name, digest, chunker_version, str(source.title or name),
                 version, len(source.text), len(sections), len(chunks),
                 stamp, source.text))
            self._db.executemany(
                "INSERT INTO sections VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [(name, s.section_id, s.heading_path, s.level, s.title,
                  s.start, s.end, s.chunks, s.chars, " ".join(s.chunk_ids),
                  i) for i, s in enumerate(sections)])
            self._db.executemany(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?,?,?,?)",
                [(name, c.chunk_id, c.section_id, c.heading_path,
                  c.start, c.end, c.text, i, c.title)
                 for i, c in enumerate(chunks)])
            self._db.executemany(
                "INSERT INTO chunks_fts(skill, chunk_id, title, heading_path, "
                "text) VALUES (?,?,?,?,?)",
                [(name, c.chunk_id, c.title, c.heading_path, c.text)
                 for c in chunks])

    # ── the routing hint: which skill is likely, before any load ─
    def ensure_routing(self, sources: Iterable[SkillSource]) -> dict[str, str]:
        """Index what routes to each skill — its frontmatter
        description and aliases, its title, its section headings —
        for every pack on the shelf, whatever its size. Keyed by the
        content hash like the pages: unchanged skills cost a hash.
        A write the file refuses moves the index to memory, like
        ``ensure``."""
        sources = list(sources)
        try:
            return self._ensure_routing(sources)
        except sqlite3.Error as e:
            if self._shared:
                raise
            self._go_memory(f"cannot write {self.path}: {e}")
            return self._ensure_routing(sources)

    def _ensure_routing(self, sources: list[SkillSource]) -> dict[str, str]:
        from .skills import policy_of
        out: dict[str, str] = {}
        stamp = _dt.datetime.now(tz=_dt.timezone.utc).isoformat(
            timespec="seconds")
        for source in sources:
            name = str(source.name)
            digest = content_hash(source.text)
            row = self._db.execute(
                "SELECT content_hash FROM routing WHERE name=?",
                (name,)).fetchone()
            if row and row["content_hash"] == digest:
                out[name] = "unchanged"
                continue
            policy = policy_of(source.text)
            headings = " | ".join(
                b.title for b in _blocks(source.text) if b.kind == "heading")
            title = str(source.title or name)
            with self._db:
                self._db.execute("DELETE FROM routing_fts WHERE skill=?",
                                 (name,))
                self._db.execute(
                    "INSERT OR REPLACE INTO routing VALUES (?,?,?,?,?,?,?)",
                    (name, digest, title, policy.description,
                     " | ".join(policy.aliases), headings, stamp))
                self._db.execute(
                    "INSERT INTO routing_fts(skill, title, description, "
                    "aliases, headings) VALUES (?,?,?,?,?)",
                    (name, title, policy.description,
                     " ".join(policy.aliases), headings))
            out[name] = "reindexed" if row else "indexed"
        return out

    def rank_skills(self, question: str, k: int = 5) -> list[dict[str, Any]]:
        """The skills a question likely wants, best first: BM25 over
        the routing text (aliases 3, description and title 2,
        headings 1), any term matching, absent terms dropped. A hint
        for the catalogue's order — the model still decides."""
        k = max(1, int(k or 5))
        terms = [t for t in query_terms(question)
                 if self._present_in("routing_fts", _fts_group(t))]
        groups = [_fts_group(t) for t in terms]
        if not groups:
            return []
        sql = (f"SELECT skill, bm25(routing_fts, {_ROUTING_WEIGHTS}) AS rank, "
               f"snippet(routing_fts, -1, '[', ']', '…', 12) AS why "
               f"FROM routing_fts WHERE routing_fts MATCH ? "
               f"ORDER BY rank, skill LIMIT ?")
        try:
            rows = self._db.execute(sql, (" OR ".join(groups), k)).fetchall()
        except sqlite3.OperationalError:
            return []
        return [{"skill": r["skill"], "score": round(-float(r["rank"]), 3),
                 "why": " ".join(str(r["why"]).split())} for r in rows]

    def _present_in(self, table: str, match: str) -> bool:
        try:
            row = self._db.execute(
                f"SELECT 1 FROM {table} WHERE {table} MATCH ? LIMIT 1",
                (match,)).fetchone()
        except sqlite3.OperationalError:
            return False
        return row is not None

    # ── reading ──────────────────────────────────────────────
    def skills(self) -> list[dict[str, Any]]:
        rows = self._db.execute(
            "SELECT name, title, chars, sections, chunks, indexed_at, "
            "content_hash FROM skills ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def overview(self, skill: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT name, title, chars, sections, chunks FROM skills "
            "WHERE name=?", (skill,)).fetchone()
        return dict(row) if row else None

    def toc(self, skill: str, under: str = "",
            max_level: int = 0) -> list[dict[str, Any]]:
        """The table of contents: heading paths with chunk counts and
        character sizes, in document order. ``under`` keeps the
        sections whose path starts with that heading; ``max_level``
        keeps the shallow ones (0 = all)."""
        rows = self._db.execute(
            "SELECT section_id, heading_path, level, title, start, end, "
            "chunks, chars, chunk_ids FROM sections WHERE skill=? "
            "ORDER BY ord", (skill,)).fetchall()
        out = []
        want = (under or "").strip().lower()
        for r in rows:
            d = dict(r)
            d["chunk_ids"] = d["chunk_ids"].split() if d["chunk_ids"] else []
            if max_level and d["level"] > max_level:
                continue
            if want and not d["heading_path"].lower().startswith(want) \
                    and want not in d["heading_path"].lower():
                continue
            out.append(d)
        return out

    def search(self, query: str, skills: list[str] | None = None,
               k: int = 8) -> list[Hit]:
        """Ranked chunks: BM25 over the heading path (weight 2) and
        the text (weight 1); every term required first, any term to
        fill; the embedder, when one is plugged in, reranks."""
        k = max(1, int(k or 8))
        names = [str(s) for s in (skills or []) if str(s).strip()]
        # a term no searched pack contains cannot pick a page: it is
        # dropped before the strict match rather than vetoing it
        terms = [t for t in query_terms(query)
                 if self._present(_fts_group(t), names)]
        matches = fts_queries(query, terms)
        if not matches:
            return []
        hits: list[Hit] = []
        seen: set[tuple[str, str]] = set()
        pool = k * 4 if self.embedder is not None else k
        for match in matches:
            if len(hits) >= pool:
                break
            for hit in self._match(match, names, pool):
                key = (hit.skill, hit.chunk_id)
                if key in seen:
                    continue
                seen.add(key)
                hits.append(hit)
                if len(hits) >= pool:
                    break
        if self.embedder is not None and hits:
            hits = self._rerank(query, hits)
        return hits[:k]

    def _scope(self, match: str, names: list[str]) -> tuple[str, list[Any]]:
        where = "chunks_fts MATCH ?"
        params: list[Any] = [match]
        if names:
            where += " AND skill IN (%s)" % ",".join("?" * len(names))
            params += names
        return where, params

    def _present(self, match: str, names: list[str]) -> bool:
        where, params = self._scope(match, names)
        try:
            row = self._db.execute(
                f"SELECT 1 FROM chunks_fts WHERE {where} LIMIT 1",
                params).fetchone()
        except sqlite3.OperationalError:
            return False
        return row is not None

    def _match(self, match: str, names: list[str], limit: int) -> list[Hit]:
        where, params = self._scope(match, names)
        sql = (f"SELECT skill, chunk_id, heading_path, "
               f"bm25(chunks_fts, {_WEIGHTS}) AS rank, "
               f"snippet(chunks_fts, {_TEXT_COLUMN}, '[', ']', '…', "
               f"{SNIPPET_TOKENS}) AS snip FROM chunks_fts WHERE {where} "
               f"ORDER BY rank, skill, chunk_id LIMIT ?")
        params.append(limit)
        try:
            rows = self._db.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []
        out = []
        for r in rows:
            meta = self._db.execute(
                "SELECT section_id, start, end FROM chunks WHERE skill=? "
                "AND chunk_id=?", (r["skill"], r["chunk_id"])).fetchone()
            if meta is None:
                continue
            out.append(Hit(r["skill"], r["chunk_id"], meta["section_id"],
                           r["heading_path"], -float(r["rank"]),
                           " ".join(str(r["snip"]).split()),
                           meta["start"], meta["end"]))
        return out

    def _rerank(self, query: str, hits: list[Hit]) -> list[Hit]:
        # SEAM: the embedder scores the lexical candidates; the blend
        # keeps BM25's say so a lexical exact hit is never buried
        assert self.embedder is not None
        texts = [self.chunk(h.skill, h.chunk_id)["text"] for h in hits]
        vectors = self.embedder.embed([query] + texts)
        top = max((h.score for h in hits), default=1.0) or 1.0
        scored = []
        for h, v in zip(hits, vectors[1:]):
            blended = 0.5 * (h.score / top) + 0.5 * _cosine(vectors[0], v)
            scored.append(Hit(h.skill, h.chunk_id, h.section_id,
                              h.heading_path, blended, h.snippet,
                              h.start, h.end))
        return sorted(scored, key=lambda h: (-h.score, h.skill, h.chunk_id))

    def chunk(self, skill: str, chunk_id: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT skill, chunk_id, section_id, heading_path, start, end, "
            "text FROM chunks WHERE skill=? AND chunk_id=?",
            (skill, chunk_id)).fetchone()
        return dict(row) if row else None

    def chunks_of(self, skill: str, section_id: str) -> list[dict[str, Any]]:
        rows = self._db.execute(
            "SELECT skill, chunk_id, section_id, heading_path, start, end, "
            "text FROM chunks WHERE skill=? AND section_id=? ORDER BY ord",
            (skill, section_id)).fetchall()
        return [dict(r) for r in rows]

    def resolve(self, skill: str, ref: str) -> dict[str, Any]:
        """A chunk id (c12), a section id (s3), a heading path, a
        heading, or a unique fragment of one → {kind, section_id,
        chunk_id?, heading_path, start, end}; or {error, candidates}."""
        ref = (ref or "").strip()
        if not ref:
            return {"error": "name a section: a heading, a heading path, "
                             "an s<N> from the contents, or a c<N> chunk"}
        if re.fullmatch(r"c\d+", ref):
            got = self.chunk(skill, ref)
            if got:
                return {"kind": "chunk", "section_id": got["section_id"],
                        "chunk_id": ref, "heading_path": got["heading_path"],
                        "start": got["start"], "end": got["end"]}
            return {"error": f"no chunk {ref!r} in {skill!r}"}
        toc = self.toc(skill)
        if not toc:
            return {"error": f"{skill!r} is not indexed"}
        if re.fullmatch(r"s\d+", ref):
            for s in toc:
                if s["section_id"] == ref:
                    return {"kind": "section", **_span(s)}
            return {"error": f"no section {ref!r} in {skill!r}"}
        want = ref.lower()
        exact = [s for s in toc if s["heading_path"].lower() == want]
        if not exact:
            exact = [s for s in toc if s["title"].lower() == want]
        if not exact:
            exact = [s for s in toc if want in s["heading_path"].lower()]
        if len(exact) == 1:
            return {"kind": "section", **_span(exact[0])}
        if not exact:
            return {"error": f"no section of {skill!r} matches {ref!r}",
                    "candidates": [s["heading_path"] for s in toc[:12]]}
        return {"error": f"{ref!r} matches {len(exact)} sections of "
                         f"{skill!r}: name one",
                "candidates": [f"{s['section_id']} · {s['heading_path']}"
                               for s in exact[:12]]}

    def read(self, skill: str, ref: str, max_chars: int = 6000,
             offset: int = 0) -> dict[str, Any]:
        """A section or a chunk, as a slice of the pack: the text,
        its breadcrumb and offsets, and where to continue when it is
        longer than ``max_chars``."""
        where = self.resolve(skill, ref)
        if "error" in where:
            return where
        row = self._db.execute("SELECT text FROM skills WHERE name=?",
                               (skill,)).fetchone()
        if row is None:
            return {"error": f"{skill!r} is not indexed"}
        max_chars = max(200, int(max_chars or 6000))
        offset = max(0, int(offset or 0))
        start, end = where["start"], where["end"]
        total = end - start
        head = start + min(offset, total)
        tail = min(head + max_chars, end)
        text = row["text"][head:tail]
        out = {"skill": skill, "ref": ref, "kind": where["kind"],
               "section_id": where["section_id"],
               "heading_path": where["heading_path"],
               "start": head, "end": tail, "chars": total,
               "text": text, "truncated": tail < end}
        if where.get("chunk_id"):
            out["chunk_id"] = where["chunk_id"]
        if tail < end:
            out["next_offset"] = tail - start
            out["note"] = (f"{end - tail:,} more characters: call again "
                           f"with offset={tail - start}")
        return out


def _chunk_order(chunk_id: str) -> int:
    digits = chunk_id.lstrip("c")
    return int(digits) if digits.isdigit() else 0


def _span(section: dict[str, Any]) -> dict[str, Any]:
    return {"section_id": section["section_id"],
            "heading_path": section["heading_path"],
            "start": section["start"], "end": section["end"]}


def open_index(graph_root: Path | None,
               embedder: Embedder | None = None) -> SkillIndex:
    """The graph's index (``<graph>/runs/skill_index.sqlite3``), or an
    in-memory one when there is no graph root — or when the file
    cannot be used (then the process's fallback for that path, see
    ``SkillIndex``)."""
    return SkillIndex(index_path(graph_root), embedder=embedder)


__all__ = ["CHUNKER_VERSION", "FALLBACK_CHUNKER_VERSION", "TARGET_CHARS",
           "OVERLAP_CHARS", "INDEX_FILE",
           "Chunk", "Section", "Hit", "Embedder", "SkillSource",
           "SkillIndex", "chunk_markdown", "estimate_tokens",
           "stem_variants", "query_terms", "fts_queries", "index_path",
           "content_hash", "open_index"]
