"""Skill retrieval — the chunker and the index, against a synthetic
pack the size of a real runtime knowledge bundle (~2.5 MB, hundreds
of sections, near-duplicate twins, tables, fenced code).

Accuracy is asserted, not described: twenty asks phrased unlike the
headings must rank their section first; no chunk may split a table
row or a fence; an unchanged pack must cost no chunking; a changed
pack must re-index only itself."""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from synthetic_skill import build_pack, queries  # noqa: E402


@dataclass
class Source:
    name: str
    title: str
    text: str
    updated: str = ""


@pytest.fixture(scope="module")
def pack():
    text, truths = build_pack()
    assert len(text) >= 2_400_000, len(text)
    return text, truths


@pytest.fixture(scope="module")
def indexed(pack, tmp_path_factory):
    from sahs.loop.skill_index import SkillIndex
    text, truths = pack
    path = tmp_path_factory.mktemp("idx") / "skill_index.sqlite3"
    index = SkillIndex(path)
    status = index.ensure([Source("bundle", "Runtime knowledge bundle", text)])
    assert status == {"bundle": "indexed"}
    return index, text, truths


# ─── the chunker: structure first, nothing split ─────────────


def test_chunks_follow_headings_and_never_split_a_row_or_a_fence(pack):
    from sahs.loop.skill_index import (OVERLAP_CHARS, TARGET_CHARS,
                                       chunk_markdown)
    text, truths = pack
    sections, chunks = chunk_markdown(text, "bundle")
    assert len(sections) >= 500 and len(chunks) >= len(sections)
    by_id = {s.section_id: s for s in sections}
    # the table of contents is the ground truth's, path for path
    for t in truths:
        assert by_id[t.section_id].heading_path == t.path
    # every chunk is a slice of the pack, on line boundaries, with
    # its breadcrumb, under the target
    for c in chunks:
        assert text[c.start:c.end] == c.text
        assert c.start == 0 or text[c.start - 1] == "\n"
        assert c.end == len(text) or text[c.end] == "\n"
        assert c.heading_path and c.chunk_id.startswith("c")
        assert c.chars <= TARGET_CHARS
        # a fence never opens in one chunk and closes in another
        assert len(re.findall(r"^```", c.text, re.M)) % 2 == 0, c.chunk_id
    # a table row never straddles a boundary
    boundaries = sorted({c.start for c in chunks} | {c.end for c in chunks})
    for m in re.finditer(r"^\|[^\n]*$", text, re.M):
        assert not any(m.start() < b < m.end() for b in boundaries)
    # consecutive chunks of one section overlap by whole blocks, at
    # most the overlap budget; chunks of different sections never do
    for a, b in zip(chunks, chunks[1:]):
        if a.section_id == b.section_id:
            assert a.end - b.start <= OVERLAP_CHARS and b.start > a.start
        else:
            assert b.start >= a.end
    # the sections list every chunk exactly once, in order
    listed = [cid for s in sections for cid in s.chunk_ids]
    assert listed == [c.chunk_id for c in chunks]
    # determinism: the same text chunks the same way
    assert chunk_markdown(text, "bundle") == (sections, chunks)


def test_a_fence_or_a_table_larger_than_the_target_stays_intact():
    from sahs.loop.skill_index import TARGET_CHARS, chunk_markdown
    fence = "```\n" + "\n".join(f"line {i} " + "x" * 80
                                for i in range(120)) + "\n```"
    rows = "\n".join(f"| r{i} | " + "v" * 100 + " |" for i in range(90))
    text = ("# T\n\n## Big fence\n\n" + fence + "\n\n## Big table\n\n"
            "| a | b |\n|---|---|\n" + rows + "\n")
    assert len(fence) > TARGET_CHARS and len(rows) > TARGET_CHARS
    sections, chunks = chunk_markdown(text, "t")
    fenced = [c for c in chunks if "```" in c.text]
    assert len(fenced) == 1 and fenced[0].text.count("```") == 2
    for c in chunks:
        for line in c.text.splitlines():
            if line.startswith("|"):
                assert line.endswith("|"), "a row was cut"


# ─── the query: lexical, stemmed, deterministic ──────────────


def test_query_expansion_is_lexical_and_deterministic():
    from sahs.loop.skill_index import (fts_queries, query_terms,
                                       stem_variants)
    assert stem_variants("settlements") == ["settlements", "settlement"]
    assert stem_variants("reconciling") == ["reconciling", "reconcil",
                                            "reconcile"]
    assert stem_variants("studies") == ["studies", "study"]
    assert stem_variants("passed") == ["passed", "pass", "passe"]
    assert query_terms("How do the settlements reconcile?") \
        == ["settlements", "reconcile"]
    strict, loose = fts_queries("why do vorlexes pile up")
    assert strict == ('("vorlexes" OR "vorlexe" OR "vorlex" OR "vorlex"*) '
                      'AND ("pile" OR "pile"*) AND ("up")')
    assert loose == strict.replace(" AND ", " OR ")
    assert fts_queries("the of a") == []
    assert fts_queries("ledger") == ['("ledger" OR "ledger"*)']


# ─── accuracy: the right section first, twenty ways ──────────


def test_search_ranks_the_right_section_first(indexed, capsys):
    index, text, truths = indexed
    asks = queries(truths, 20)
    assert len(asks) == 20
    right = 0
    for ask, truth in asks:
        # the ask never repeats the heading's wording
        assert ask.lower() != truth.heading.lower()
        hits = index.search(ask, ["bundle"], k=8)
        assert hits, ask
        top = hits[0]
        assert top.section_id == truth.section_id, \
            f"{ask!r}: wanted {truth.path}, got {top.heading_path}"
        assert top.heading_path == truth.path
        assert top.snippet and top.start < top.end
        assert top.score >= hits[-1].score
        right += 1
    assert right == 20
    # the twins never win: the same prose around other terms
    twins = [t for t in truths if t.twin_of]
    assert len(twins) >= 30
    for t in twins[:20]:
        hits = index.search(f"what happens when {t.a}s pile up", ["bundle"])
        assert hits[0].section_id == t.section_id
    # and across every original section, one ask each, the number
    # the report quotes
    templates = ["what happens when {a}s pile up",
                 "{b}ing failure left the {a} half done",
                 "nightly reconcile of {a} versus {b}",
                 "does the {a} step write before the {b} pass",
                 "{a} ledger before {b} rollup",
                 "why is the {b} pass waiting on {a} backlog",
                 "set the {a} quota and the {b} window together",
                 "checkpoint rerun for {b}ing failures on {a}"]
    originals = [t for t in truths if not t.twin_of
                 and t.path.count(" > ") == 2]
    first = 0
    for i, t in enumerate(originals):
        ask = templates[i % len(templates)].format(a=t.a, b=t.b)
        hits = index.search(ask, ["bundle"], k=3)
        first += bool(hits) and hits[0].section_id == t.section_id
    with capsys.disabled():
        print(f"\n[skill retrieval] hits@1 {right}/20 on the rephrased "
              f"asks; {first}/{len(originals)} across every section "
              f"({100 * first / len(originals):.1f}%)")
    assert first / len(originals) >= 0.97


def test_table_cells_and_code_identifiers_are_found(indexed):
    index, text, truths = indexed
    with_table = next(t for t in truths if t.table and not t.twin_of)
    hits = index.search(f"{with_table.a}-retries before paging", ["bundle"])
    assert hits[0].section_id == with_table.section_id
    with_fence = next(t for t in truths if t.fence and not t.twin_of)
    hits = index.search(f"dw.{with_fence.a}_{with_fence.b}_ledger", ["bundle"])
    assert hits[0].section_id == with_fence.section_id
    assert "```" in index.chunk("bundle", hits[0].chunk_id)["text"]


def test_a_word_no_pack_contains_does_not_veto_the_match(indexed):
    index, text, truths = indexed
    t = next(t for t in truths if not t.twin_of and t.path.count(" > ") == 2)
    plain = index.search(f"reconciling {t.a} against {t.b}", ["bundle"])
    noisy = index.search(f"reconciling {t.a} against {t.b} zzqx", ["bundle"])
    assert plain[0].section_id == t.section_id
    assert noisy[0].section_id == t.section_id
    assert index.search("zzqx qxzz", ["bundle"]) == []


# ─── the index: keyed by content, lazy, incremental ──────────


def test_reindexing_an_unchanged_skill_does_zero_work(indexed, monkeypatch):
    import sahs.loop.skill_index as si
    index, text, truths = indexed
    before = index.status("bundle")
    calls = []
    monkeypatch.setattr(si, "chunk_markdown",
                        lambda *a, **k: calls.append(1) or (_ for _ in ()).throw(
                            AssertionError("re-chunked an unchanged skill")))
    src = Source("bundle", "Runtime knowledge bundle", text, updated="later")
    assert index.ensure([src]) == {"bundle": "unchanged"}
    assert calls == []
    assert index.status("bundle") == before


def test_a_changed_skill_reindexes_only_itself(tmp_path):
    from sahs.loop.skill_index import CHUNKER_VERSION, SkillIndex, index_path
    graph = tmp_path / "graph"
    path = index_path(graph)
    assert path == graph / "runs" / "skill_index.sqlite3"
    index = SkillIndex(path)
    a = Source("a", "A", "# A\n\n## Alpha\n\nThe quorvex step.\n")
    b = Source("b", "B", "# B\n\n## Beta\n\nThe tandrel pass.\n")
    assert index.ensure([a, b]) == {"a": "indexed", "b": "indexed"}
    assert index.chunked == ["a", "b"]
    b_before = index.status("b")
    assert b_before["chunker_version"] == CHUNKER_VERSION
    a2 = Source("a", "A", "# A\n\n## Alpha\n\nThe quorvex step.\n\n"
                "## Gamma\n\nThe new plimsol rule.\n")
    assert index.ensure([a2, b]) == {"a": "reindexed", "b": "unchanged"}
    assert index.chunked == ["a", "b", "a"]
    assert index.status("b") == b_before
    assert [s["heading_path"] for s in index.toc("a")] \
        == ["A", "A > Alpha", "A > Gamma"]
    assert index.search("plimsol rule")[0].skill == "a"
    assert index.search("tandrel", ["b"])[0].heading_path == "B > Beta"
    # the file is derived data: delete it and the next ensure rebuilds
    index.close()
    path.unlink()
    fresh = SkillIndex(path)
    assert fresh.ensure([a2, b]) == {"a": "indexed", "b": "indexed"}
    fresh.close()


# ─── the index never fails a turn ────────────────────────────


def test_an_index_file_that_cannot_be_used_falls_back_to_memory_once(
        tmp_path, caplog):
    """A corrupt file, a path that cannot be created, and a file that
    stops taking writes: each falls back to one in-memory index for
    the process, logged once per path, and the pack stays searchable.
    The file is left as derived data to delete."""
    import logging
    import sqlite3

    from sahs.loop import skill_index as si
    from sahs.loop.skill_index import SkillIndex, open_index
    pack = Source("notes", "Notes", "# Notes\n\n## Quorvex\n\nThe quorvex "
                                    "rule holds.\n\n## Other\n\nUnrelated.\n")
    caplog.set_level(logging.WARNING, logger="sahs.loop.skill_index")

    # a corrupt file: not a database
    graph = tmp_path / "corrupt"
    (graph / "runs").mkdir(parents=True)
    (graph / "runs" / "skill_index.sqlite3").write_bytes(b"not a database\n" * 40)
    index = open_index(graph)
    assert index.fallback.startswith("cannot open") and index.path is not None
    assert index.ensure([pack]) == {"notes": "indexed"}
    assert index.search("quorvex rule", ["notes"])[0].heading_path \
        == "Notes > Quorvex"
    assert index.toc("notes")[1]["heading_path"] == "Notes > Quorvex"
    assert (graph / "runs" / "skill_index.sqlite3").read_bytes().startswith(
        b"not a database")                      # left alone
    # the process reuses that memory index: the pack is unchanged there
    again = open_index(graph)
    assert again.fallback and again.ensure([pack]) == {"notes": "unchanged"}
    again.close()                               # never closes the shared db
    assert again.search("quorvex", ["notes"])
    warned = [r for r in caplog.records if "in-memory index" in r.getMessage()]
    assert len(warned) == 1 and str(graph) in warned[0].getMessage()

    # a path that cannot be created: runs/ is a file
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    (blocked / "runs").write_text("a file where the folder should be")
    index = open_index(blocked)
    assert index.fallback.startswith("cannot open")
    assert index.ensure([pack]) == {"notes": "indexed"}
    assert index.search("quorvex", ["notes"])

    # a file that stops taking writes after it opened
    ro = tmp_path / "ro"
    index = open_index(ro)
    assert not index.fallback
    index._db.close()
    index._db = sqlite3.connect(f"file:{si.index_path(ro)}?mode=ro", uri=True,
                                check_same_thread=False)
    index._db.row_factory = sqlite3.Row
    assert index.ensure([pack]) == {"notes": "indexed"}
    assert index.fallback.startswith("cannot write")
    assert index.search("quorvex", ["notes"])[0].chunk_id == "c2"
    assert index.ensure_routing([pack]) == {"notes": "indexed"}
    assert index.rank_skills("quorvex")[0]["skill"] == "notes"
    # three paths, three warnings — and none repeated
    warned = [r for r in caplog.records if "in-memory index" in r.getMessage()]
    assert len(warned) == 3
    # a real file elsewhere still works as before
    good = SkillIndex(tmp_path / "good" / "skill_index.sqlite3")
    assert not good.fallback and good.ensure([pack]) == {"notes": "indexed"}


def test_a_pack_the_chunker_cannot_read_is_indexed_as_one_chunk(
        tmp_path, monkeypatch, caplog):
    """When chunking a pack throws, the pack is still indexed — the
    whole text as one section and one chunk, offsets exact — so it is
    searchable and readable; the fault is logged once, and the pack is
    re-chunked properly on the next load once the chunker can."""
    import logging

    from sahs.loop import skill_index as si
    from sahs.loop.skill_index import (CHUNKER_VERSION,
                                       FALLBACK_CHUNKER_VERSION, SkillIndex)
    caplog.set_level(logging.WARNING, logger="sahs.loop.skill_index")
    text = "# Notes\n\n## Quorvex\n\nThe quorvex rule holds.\n"
    good = Source("fine", "Fine", "# Fine\n\nA fine pack.\n")
    bad = Source("notes", "Notes", text)
    real = si.chunk_markdown

    def flaky(text, skill="", **kw):
        if skill == "notes":
            raise RuntimeError("boom")
        return real(text, skill, **kw)

    monkeypatch.setattr(si, "chunk_markdown", flaky)
    index = SkillIndex(tmp_path / "skill_index.sqlite3")
    assert index.ensure([good, bad]) == {"fine": "indexed", "notes": "indexed"}
    assert index.single_chunk == ["notes"]
    assert index.status("notes")["chunker_version"] == FALLBACK_CHUNKER_VERSION
    assert index.status("fine")["chunker_version"] == CHUNKER_VERSION
    assert index.overview("notes")["sections"] == 1 \
        and index.overview("notes")["chunks"] == 1
    (row,) = index.toc("notes")
    assert row["heading_path"] == "Notes" and row["chunk_ids"] == ["c1"]
    assert (row["start"], row["end"]) == (0, len(text))
    hit = index.search("quorvex rule", ["notes"])[0]
    assert hit.chunk_id == "c1" and (hit.start, hit.end) == (0, len(text))
    page = index.read("notes", "c1")
    assert page["text"] == text and text[page["start"]:page["end"]] == text
    # logged once, even across a second ensure (which re-tries)
    assert index.ensure([bad]) == {"notes": "reindexed"}
    warned = [r for r in caplog.records if "single chunk" in r.getMessage()]
    assert len(warned) == 1 and "'notes'" in warned[0].getMessage()
    # the chunker recovers: the next load re-chunks properly
    monkeypatch.setattr(si, "chunk_markdown", real)
    assert index.ensure([bad, good]) == {"notes": "reindexed", "fine": "unchanged"}
    assert index.status("notes")["chunker_version"] == CHUNKER_VERSION
    assert [s["heading_path"] for s in index.toc("notes")] \
        == ["Notes", "Notes > Quorvex"]


def test_toc_and_read_carry_breadcrumbs_and_offsets(indexed):
    index, text, truths = indexed
    toc = index.toc("bundle")
    assert toc[0]["heading_path"] == "Runtime knowledge bundle"
    assert sum(s["chunks"] for s in toc) == index.overview("bundle")["chunks"]
    assert all(s["chars"] == s["end"] - s["start"] for s in toc)
    parts = index.toc("bundle", max_level=2)
    assert all(s["level"] <= 2 for s in parts) and len(parts) < len(toc)
    t = next(t for t in truths if not t.twin_of and t.path.count(" > ") == 2)
    under = index.toc("bundle", under=t.path)
    assert [s["heading_path"] for s in under][0] == t.path
    # read by heading, by id, by chunk: each a slice with offsets
    got = index.read("bundle", t.heading, max_chars=1000)
    assert got["heading_path"] == t.path and got["section_id"] == t.section_id
    assert text[got["start"]:got["end"]] == got["text"]
    assert got["text"].startswith(f"### {t.heading}")
    assert got["truncated"] and got["next_offset"] == 1000
    assert "offset=1000" in got["note"]
    rest = index.read("bundle", t.section_id, max_chars=1000,
                      offset=got["next_offset"])
    assert rest["start"] == got["end"] and rest["text"]
    whole = index.read("bundle", t.section_id, max_chars=100_000)
    assert not whole["truncated"] and whole["chars"] == len(whole["text"])
    chunk = index.read("bundle", toc[5]["chunk_ids"][0])
    assert chunk["kind"] == "chunk" and chunk["section_id"] == toc[5]["section_id"]
    # an ambiguous heading names its candidates; a miss says so
    ambiguous = index.read("bundle", "Edge cases in")
    assert "candidates" in ambiguous and "matches" in ambiguous["error"]
    assert "no section" in index.read("bundle", "nothing like this")["error"]
    assert "not indexed" in index.read("ghost", "x")["error"]


def test_the_embedder_seam_is_off_by_default_and_reranks_when_given(tmp_path):
    from sahs.loop.skill_index import SkillIndex
    text = ("# P\n\n## One\n\nquorvex quorvex quorvex tandrel.\n\n"
            "## Two\n\nquorvex once, and much else besides tandrel.\n")
    plain = SkillIndex()
    plain.ensure([Source("p", "P", text)])
    assert plain.embedder is None
    lexical = [h.heading_path for h in plain.search("quorvex tandrel", ["p"])]
    assert lexical == ["P > One", "P > Two"]

    class Flip:
        """A fake: prefers the text that says 'much else'."""
        calls = 0

        def embed(self, texts):
            Flip.calls += 1
            return [[1.0, 0.0]] + [[1.0, 0.0] if "much else" in t
                                   else [0.0, 1.0] for t in texts[1:]]

    with SkillIndex(embedder=Flip()) as reranked:
        reranked.ensure([Source("p", "P", text)])
        got = [h.heading_path for h in reranked.search("quorvex tandrel", ["p"])]
    assert got == ["P > Two", "P > One"] and Flip.calls == 1


# ─── the check script a person runs on their own packs ───────


def test_the_check_script_prints_toc_sizes_and_top_hits(pack, tmp_path):
    text, truths = pack
    skills = tmp_path / "skills"
    (skills / "cfr").mkdir(parents=True)
    (skills / "bundle.md").write_text(text, encoding="utf-8")
    (skills / "cfr" / "notes.md").write_text(
        "# Notes\n\n## Fiscal\n\nQuarters end in March.\n", encoding="utf-8")
    t = next(t for t in truths if not t.twin_of and t.path.count(" > ") == 2)
    ask = f"why is the {t.b} pass waiting on {t.a} backlog"
    run = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "skill_index_check.py"),
         str(skills), ask, "--k", "3"],
        capture_output=True, text=True, cwd=SILO, timeout=300)
    assert run.returncode == 0, run.stderr[-800:]
    out = run.stdout
    assert f"Index: {tmp_path / 'runs' / 'skill_index.sqlite3'}" in out
    assert re.search(r"bundle\s+2,\d{3},\d{3} chars\s+\d{3} sections\s+"
                     r"1,\d{3} chunks\s+indexed", out)
    assert re.search(r"cfr/notes\s+\d+ chars\s+2 sections\s+2 chunks\s+indexed",
                     out)
    assert "Contents of bundle (levels 1-2;" in out
    assert "s1 · Runtime knowledge bundle (1 chunk," in out
    assert "Contents of cfr/notes" in out and "s2 · Notes > Fiscal" in out
    assert f"Query: {ask!r}" in out
    first = out.split("\n  1. ", 1)[1].splitlines()[0]
    assert "bundle" in first and t.path in first and "(chars " in first
    assert (tmp_path / "runs" / "skill_index.sqlite3").exists()
    # the second run finds everything unchanged and writes nothing new
    again = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "skill_index_check.py"),
         str(skills), ask, "--skill", "cfr/notes", "--toc-depth", "0"],
        capture_output=True, text=True, cwd=SILO, timeout=300)
    assert again.returncode == 1                # no hit: says so, exit 1
    assert "cfr/notes" in again.stdout and "unchanged" in again.stdout
    assert "no passage matched" in again.stdout


# ─── the routing hint: which skill, before any load ──────────

SHELF = {
    "fiscal-calendar": (
        "---\ndescription: How the fiscal year and its quarters are counted\n"
        "aliases: [FY, fiscal quarter, quarter close]\n---\n"
        "# Fiscal calendar\n\n## Quarter boundaries\n\nQ1 opens in "
        "February.\n\n## Year end\n\nThe year ends in January.\n"),
    "gmns-metrics": (
        "---\ndescription: Reading the GMNS spend and volume metrics\n"
        "aliases:\n  - gross merchant net spend\n  - GMNS\n---\n"
        "# GMNS metrics\n\n## Net spend definition\n\nGross minus refunds."
        "\n\n## Volume versus count\n\nUnits, not transactions.\n"),
    "settlement-ops": (
        "---\ndescription: Settlement windows and the reconciliation runs\n"
        "aliases: [settle, recon]\nruntime_loading: sectioned\n---\n"
        "# Settlement operations\n\n## Nightly reconciliation\n\nRuns at "
        "close.\n\n## Late settlement close\n\nWhen the window slips.\n"),
    "dashboard-grammar": (
        "---\ndescription: How to lay out tiles and charts on a dashboard\n"
        "aliases: [tiles, KPI row]\n---\n"
        "# Dashboard grammar\n\n## Tile order\n\nBiggest number first."
        "\n\n## Chart kinds\n\nA trend is a line.\n"),
    "kyc-vocab": (
        "---\ndescription: KYC statuses and the codes they are stored as\n"
        "aliases: [know your customer, onboarding status]\n---\n"
        "# KYC vocabulary\n\n## Status codes\n\nAPPROVED is A.\n\n"
        "## Approved versus verified\n\nNot the same thing.\n"),
}
QUESTIONS = [
    ("when does the FY end", "fiscal-calendar"),
    ("what does GMNS stand for", "gmns-metrics"),
    ("why did the nightly recon fail", "settlement-ops"),
    ("which chart kind shows a trend", "dashboard-grammar"),
    ("how is a KYC approved status stored", "kyc-vocab"),
    ("quarter close dates this year", "fiscal-calendar"),
    ("tile order on the KPI row", "dashboard-grammar"),
    ("onboarding status codes", "kyc-vocab"),
    ("the late settlement close window", "settlement-ops"),
    ("net spend definition, gross merchant", "gmns-metrics"),
]


def test_rank_skills_lists_the_likely_pack_first_for_ten_questions(pack):
    from sahs.loop.skill_index import SkillIndex
    text, truths = pack
    index = SkillIndex()
    shelf = [Source(name, name, body) for name, body in SHELF.items()]
    shelf.append(Source("bundle", "Runtime knowledge bundle", text))
    status = index.ensure_routing(shelf)
    assert set(status.values()) == {"indexed"}
    # the routing rows hold the frontmatter and the headings, not the
    # pages: the bundle was not chunked for this
    assert index.chunked == [] and index.overview("bundle") is None
    for question, want in QUESTIONS:
        ranked = index.rank_skills(question, k=3)
        assert ranked, question
        assert ranked[0]["skill"] == want, (question, ranked)
        assert ranked[0]["score"] > 0 and ranked[0]["why"]
    # a heading of the bundle routes to the bundle, by its words alone
    t = next(t for t in truths if not t.twin_of)
    assert index.rank_skills(f"handling {t.a} {t.b} drift")[0]["skill"] \
        == "bundle"
    # nothing routes on words no pack carries; stopwords alone route nothing
    assert index.rank_skills("zzqx plover") == []
    assert index.rank_skills("the of a") == []
    # keyed by content: unchanged packs cost a hash, a changed pack
    # re-indexes only itself
    assert set(index.ensure_routing(shelf).values()) == {"unchanged"}
    changed = Source("kyc-vocab", "kyc-vocab",
                     SHELF["kyc-vocab"].replace("Status codes", "Code table"))
    again = index.ensure_routing(shelf[:4] + [changed])
    assert again == {**{n: "unchanged" for n in list(SHELF)[:4]},
                     "kyc-vocab": "reindexed"}
    assert index.rank_skills("code table")[0]["skill"] == "kyc-vocab"
