"""One directory for both shelves: the chat's skill picker reads the
skills tree the Knowledge Files shelf walks (``MERIDIAN_SKILLS_DIR``,
nested folders, the pack name from the path as a slug) after the
built-ins and the owner's own and before ``<graph>/skills``, first
wins on a name; ``all_skills`` stays cheap through the per-file parse
cache; and ``scripts/skills_check.py`` prints what the picker lists,
with sizes, the whole-load verdict, the frontmatter policy, and every
file skipped or refusing to load, with why."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.assistant import skills_loader as loader  # noqa: E402
from sahs.assistant.skills_loader import (  # noqa: E402
    SKILLS_DIR_VAR, all_skills, clear_cache, collect_skills, get_skill,
    load_packs, pack_name)
from sahs.loop.skills import (  # noqa: E402
    SkillUnreadable, frontmatter_error, frontmatter_warnings, parse_skill)

_spec = importlib.util.spec_from_file_location(
    "skills_check", SILO / "scripts" / "skills_check.py")
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)

SEMANTICS = ("---\n"
             "description: The TLS vocabulary and where each term binds\n"
             "aliases: [tls, vocab]\n"
             "runtime_loading: full_file_required\n"
             "truncation_allowed: false\n"
             "---\n"
             "# Semantics of TLS\n\nThe first prose line.\n")


@pytest.fixture()
def shelves(tmp_path: Path) -> dict[str, Path]:
    """A skills tree the way the owner keeps one (nested folders, odd
    names, a users/ folder, one broken file of each kind) and a graph
    shelf that shares a name with it."""
    tree = tmp_path / "skills"
    (tree / "CFR" / "TLS").mkdir(parents=True)
    (tree / "CFR" / "TLS" / "Semantics.md").write_text(SEMANTICS, encoding="utf-8")
    (tree / "fiscal notes.md").write_text(
        "# Fiscal notes\n\nQ1 starts in February.\n", encoding="utf-8")
    (tree / "big.md").write_text(
        "# Big\n\n" + ("A governed paragraph. " * 300), encoding="utf-8")
    (tree / "a").mkdir()
    (tree / "a" / "b.md").write_text("# a/b\n\nfirst\n", encoding="utf-8")
    (tree / "a-b.md").write_text("# a-b\n\nsecond, same name\n", encoding="utf-8")
    (tree / "broken.md").write_bytes(b"# Broken\n\n\xff\xfe not utf-8\n")
    (tree / "open.md").write_text("---\ndescription: never closed\n# Open\n",
                                  encoding="utf-8")
    (tree / "odd.md").write_text(
        "---\nruntime_loading: whole\ntruncation_allowed: maybe\n---\n"
        "# Odd\n\nfolded\n", encoding="utf-8")
    (tree / "analysis-playbooks.md").write_text(
        "# Not the built-in\n\nsmuggled\n", encoding="utf-8")
    (tree / "users" / "ana-lyst").mkdir(parents=True)
    (tree / "users" / "ana-lyst" / "mine.md").write_text(
        "# Mine\n\nowned\n", encoding="utf-8")
    (tree / ".drafts").mkdir()
    (tree / ".drafts" / "hidden.md").write_text("# Hidden\n\nno\n", encoding="utf-8")
    graph = tmp_path / "graph"
    (graph / "skills").mkdir(parents=True)
    (graph / "skills" / "fiscal-notes.md").write_text(
        "# Graph fiscal\n\nthe graph's copy\n", encoding="utf-8")
    (graph / "skills" / "graph-only.md").write_text(
        "# Graph only\n\nonly here\n", encoding="utf-8")
    clear_cache()
    return {"tree": tree, "graph": graph}


def test_the_pack_name_is_the_path_as_a_slug():
    assert pack_name("CFR/TLS/Semantics.md") == "cfr-tls-semantics"
    assert pack_name("fiscal notes.md") == "fiscal-notes"
    assert pack_name(Path("a") / "b.md") == "a-b" == pack_name("a-b.md")
    assert pack_name("Ünïcode & Co.md") == "n-code-co"
    assert len(pack_name("deep/" + "x" * 90 + ".md")) == 64
    assert pack_name("---.md") == ""


def test_the_picker_reads_the_tree_after_own_and_before_the_graph_shelf(
        shelves, monkeypatch):
    # the variable as the .env sets it: the process environment is the
    # default source, and every reader (get_skill, load_packs, the
    # runtime's shelf) goes through all_skills without naming it
    monkeypatch.setenv(SKILLS_DIR_VAR, str(shelves["tree"]))
    env = {SKILLS_DIR_VAR: str(shelves["tree"])}
    packs = all_skills(shelves["graph"], "Ana Lyst")
    assert packs == all_skills(shelves["graph"], "Ana Lyst", env)
    names = [p.name for p in packs]
    builtin = [p.name for p in loader.builtin_skills()]
    assert names[:len(builtin)] == builtin
    assert names[len(builtin):] == [
        "a-b", "big", "broken", "cfr-tls-semantics", "fiscal-notes",
        "odd", "open", "graph-only"]
    by_name = {p.name: p for p in packs}
    # the tree's copy wins the graph shelf's on the shared name
    assert by_name["fiscal-notes"].title == "Fiscal notes"
    assert by_name["fiscal-notes"].path == str(shelves["tree"] / "fiscal notes.md")
    # the built-in wins the tree's smuggled copy
    assert by_name["analysis-playbooks"].origin == "built-in"
    # first wins inside the tree too, in relative-path order regardless
    # of case: a-b.md sorts before a/b.md ('-' < '/')
    assert by_name["a-b"].path == str(shelves["tree"] / "a-b.md")
    # the name, the title, description and aliases from the frontmatter
    sem = by_name["cfr-tls-semantics"]
    assert sem.origin == "unreviewed" and sem.owner == ""
    assert sem.title == "Semantics of TLS"
    assert sem.description == "The TLS vocabulary and where each term binds"
    assert loader.preference_of(sem) == (
        "whole", "runtime_loading: full_file_required, truncation_allowed: false")
    assert get_skill(shelves["graph"], "cfr-tls-semantics", "Ana Lyst") == sem
    # users/ and hidden folders in the tree are never shared packs
    assert "users-ana-lyst-mine" not in names and "mine" not in names
    assert "drafts-hidden" not in names and "hidden" not in names
    # the two broken files list with the reason and refuse to load
    assert by_name["broken"].error.startswith("not UTF-8")
    assert by_name["open"].error == (
        "frontmatter: the opening --- on line 1 has no closing ---")
    assert by_name["open"].description.startswith("unreadable: frontmatter")
    for name in ("broken", "open"):
        with pytest.raises(SkillUnreadable, match=name):
            loader.check_readable(by_name[name])
    # the skipped files, each with its reason
    _listed, skipped = collect_skills(shelves["graph"], "Ana Lyst", env)
    rows = {Path(s["path"]).name: s for s in skipped}
    assert set(rows) == {"b.md", "analysis-playbooks.md", "fiscal-notes.md"}
    assert rows["b.md"]["shelf"] == SKILLS_DIR_VAR and rows["b.md"]["name"] == "a-b"
    assert "first wins" in rows["b.md"]["reason"]
    assert str(shelves["tree"] / "a-b.md") in rows["b.md"]["reason"]
    assert "built-in pack 'analysis-playbooks'" in rows["analysis-playbooks.md"]["reason"]
    assert rows["fiscal-notes.md"]["shelf"] == "<graph>/skills"
    assert "unreviewed pack 'fiscal-notes'" in rows["fiscal-notes.md"]["reason"]
    # without the variable the picker is what it was: built-ins and the graph
    assert [p.name for p in all_skills(shelves["graph"], "Ana Lyst", {})] == \
        builtin + ["fiscal-notes", "graph-only"]
    monkeypatch.delenv(SKILLS_DIR_VAR)
    assert "cfr-tls-semantics" not in [p.name for p in all_skills(shelves["graph"])]


def test_the_tree_packs_pin_and_load_like_any_other(shelves, monkeypatch):
    monkeypatch.setenv(SKILLS_DIR_VAR, str(shelves["tree"]))
    loaded, missing = load_packs(shelves["graph"], ["cfr-tls-semantics", "ghost"],
                                 "Ana Lyst")
    assert [p.name for p in loaded] == ["cfr-tls-semantics"] and missing == ["ghost"]
    assert loaded[0].text == SEMANTICS
    # a broken file pins nothing and says why, by name
    with pytest.raises(SkillUnreadable, match="'open' cannot be loaded: frontmatter"):
        load_packs(shelves["graph"], ["open"], "Ana Lyst")


def test_all_skills_parses_a_file_once_until_it_changes(shelves, monkeypatch):
    env = {SKILLS_DIR_VAR: str(shelves["tree"])}
    calls: list[Path] = []
    real = loader._parse

    def counting(path: Path):
        calls.append(path)
        return real(path)
    monkeypatch.setattr(loader, "_parse", counting)
    clear_cache()
    first = all_skills(shelves["graph"], "", env)
    parsed_once = len(calls)
    assert parsed_once >= 10                      # every file, once
    assert all_skills(shelves["graph"], "", env) == first
    assert all_skills(shelves["graph"], "", env) == first
    assert len(calls) == parsed_once              # a stat each, no parse
    # a changed file is parsed again, and only it
    target = shelves["tree"] / "fiscal notes.md"
    target.write_text("# Fiscal notes, revised\n\nQ1 starts in March.\n",
                      encoding="utf-8")
    stat = target.stat()
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))
    again = all_skills(shelves["graph"], "", env)
    assert calls[parsed_once:] == [target]
    assert next(p for p in again if p.name == "fiscal-notes").title == \
        "Fiscal notes, revised"


def test_frontmatter_errors_and_warnings_are_named():
    assert frontmatter_error("# Plain\n") == ""
    assert frontmatter_error("---\na: 1\n---\n# T\n") == ""
    assert frontmatter_error("---\na: 1\n...\n# T\n") == ""
    assert frontmatter_error("---\na: 1\n# T\n") == (
        "frontmatter: the opening --- on line 1 has no closing ---")
    broken = parse_skill("x", "---\na: 1\n# T\n")
    assert broken.error and broken.text == "---\na: 1\n# T\n"
    assert frontmatter_warnings("---\nruntime_loading: sectioned\n"
                                "truncation_allowed: false\n---\n") == []
    warnings = frontmatter_warnings(
        "---\nruntime_loading: whole\ntruncation_allowed: maybe\n"
        "aliases: 3\n---\n")
    assert warnings == [
        "runtime_loading: 'whole' is not one of sectioned | "
        "full_file_required; read as sectioned",
        "truncation_allowed: 'maybe' is not true/false; read as true",
    ]
    assert frontmatter_warnings("---\naliases: one\n---\n") == []


def test_the_check_script_prints_the_pickers_view_and_why(shelves, capsys):
    env = {SKILLS_DIR_VAR: str(shelves["tree"]), "SAHS_MAX_SKILL_CHARS": "4000"}
    data = check.report(shelves["graph"], "Ana Lyst", env, "")
    assert [r["label"] for r in data["roots"]] == [
        "built-in", "own (ana-lyst)", SKILLS_DIR_VAR, "<graph>/skills"]
    tree_root = data["roots"][2]
    assert tree_root["path"] == str(shelves["tree"]) and tree_root["exists"]
    assert tree_root["packs"] == 8 and tree_root["skipped"] == 1
    assert data["roots"][1]["exists"] is False and data["roots"][1]["packs"] == 0
    assert data["limit"] == 4000 and "SAHS_MAX_SKILL_CHARS=4,000" in data["limit_note"]
    rows = {r["name"]: r for r in data["packs"]}
    assert rows["big"]["fits"] is False and rows["big"]["mode"] == "library"
    assert rows["fiscal-notes"]["fits"] is True and rows["fiscal-notes"]["mode"] == "whole"
    sem = rows["cfr-tls-semantics"]
    assert sem["runtime_loading"] == "full_file_required"
    assert sem["truncation_allowed"] is False and sem["preferred"] == "whole"
    assert sem["aliases"] == ["tls", "vocab"] and sem["chars"] == len(SEMANTICS)
    assert rows["odd"]["preferred"] == "sectioned" and len(rows["odd"]["warnings"]) == 2
    assert {r["name"] for r in data["refusing"]} == {"broken", "open"}
    assert {Path(s["path"]).name for s in data["skipped"]} == {
        "b.md", "analysis-playbooks.md", "fiscal-notes.md"}
    assert {w["name"] for w in data["warnings"]} == {"odd"}
    assert data["ok"] is False
    text = check.render(data)
    for piece in ("Skill roots", "MERIDIAN_SKILLS_DIR", "Whole-load limit: 4,000",
                  "big", "library (", "cfr-tls-semantics",
                  "runtime_loading=full_file_required truncation_allowed=false → whole",
                  "aliases: tls, vocab", "Listed but refusing to load (2)",
                  "not UTF-8", "no closing ---", "Skipped, not listed (3)",
                  "first wins", "built-in pack 'analysis-playbooks'",
                  "Frontmatter read differently", "read as sectioned",
                  "not ok"):
        assert piece in text, piece
    # the CLI: the same view, exit 1 while a file is skipped or refuses
    code = check.main(["--graph", str(shelves["graph"]), "--skills-dir",
                       str(shelves["tree"]), "--owner", "Ana Lyst",
                       "--no-dotenv"])
    out = capsys.readouterr().out
    assert code == 1 and "not ok" in out and "cfr-tls-semantics" in out
    code = check.main(["--graph", str(shelves["graph"]), "--skills-dir",
                       str(shelves["tree"]), "--no-dotenv", "--json"])
    out = capsys.readouterr().out
    assert code == 1 and '"skipped"' in out
    # a clean tree exits 0
    for name in ("broken.md", "open.md", "a/b.md", "analysis-playbooks.md"):
        (shelves["tree"] / name).unlink()
    (shelves["graph"] / "skills" / "fiscal-notes.md").unlink()
    code = check.main(["--graph", str(shelves["graph"]), "--skills-dir",
                       str(shelves["tree"]), "--no-dotenv"])
    out = capsys.readouterr().out
    assert code == 0 and out.rstrip().endswith("ok: every file listed loads")
    # and as a subprocess, the way the owner runs it
    done = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "skills_check.py"),
         "--graph", str(shelves["graph"]), "--skills-dir", str(shelves["tree"]),
         "--no-dotenv", "--model", "gemini-3.7-flash"],
        capture_output=True, text=True, cwd=SILO,
        env={**os.environ, "SAHS_MAX_SKILL_CHARS": "650000"})
    assert done.returncode == 0, done.stderr[-800:]
    assert "Whole-load limit: 650,000" in done.stdout
    assert "big" in done.stdout and "library (" not in done.stdout
