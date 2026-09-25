"""spanner_check.py --emit-ddl: from the schema listing the check
fetches, CREATE TABLE text for every live table the repo's DDL lacks,
parents before children — on a canned listing, never a Spanner."""

from __future__ import annotations

import importlib.util
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "spanner_check", SILO / "scripts" / "spanner_check.py")
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


def _report():
    return {"tables": [
        {"name": "Users", "parent": None,
         "columns": [["UserId", "STRING(36)"], ["Email", "STRING(320)"]], "indexes": []},
        {"name": "BuildBundleChunks", "parent": "BuildBundles",
         "columns": [["BuildId", "STRING(64)"], ["Seq", "INT64"], ["Chunk", "BYTES(MAX)"]],
         "indexes": []},
        {"name": "BuildBundles", "parent": "Builds",
         "columns": [["BuildId", "STRING(64)"], ["Format", "STRING(32)"],
                     ["Complete", "BOOL"], ["PublishedAt", "TIMESTAMP"],
                     ["PublishedBy", "STRING(36)"]], "indexes": []},
        {"name": "Builds", "parent": None,
         "columns": [["BuildId", "STRING(64)"]], "indexes": []},
    ], "designed": {"present": ["Users", "Builds"], "missing": ["ChatSessions"],
                    "undesigned": ["BuildBundleChunks", "BuildBundles"]}}


def test_emits_the_undesigned_tables_parents_first_with_keys_and_interleaves():
    text = check.emit_ddl(
        _report(),
        keys={"BuildBundles": ["BuildId"], "BuildBundleChunks": ["BuildId", "Seq"]},
        nullable={("BuildBundles", "BuildId"): False, ("BuildBundles", "Format"): False,
                  ("BuildBundles", "Complete"): False, ("BuildBundles", "PublishedAt"): False,
                  ("BuildBundles", "PublishedBy"): True,
                  ("BuildBundleChunks", "BuildId"): False,
                  ("BuildBundleChunks", "Seq"): False, ("BuildBundleChunks", "Chunk"): False})
    assert "CREATE TABLE Users" not in text and "CREATE TABLE Builds (" not in text
    assert text.index("CREATE TABLE BuildBundles (") < text.index(
        "CREATE TABLE BuildBundleChunks (")
    bundles = text.split("CREATE TABLE BuildBundles (", 1)[1].split(";", 1)[0]
    assert "Format       STRING(32)  NOT NULL," in bundles
    assert "PublishedBy  STRING(36)," in bundles                # nullable: no NOT NULL
    assert bundles.rstrip().endswith(
        ") PRIMARY KEY (BuildId),\n  INTERLEAVE IN PARENT Builds ON DELETE CASCADE")
    chunks = text.split("CREATE TABLE BuildBundleChunks (", 1)[1].split(";", 1)[0]
    assert "Chunk    BYTES(MAX)  NOT NULL," in chunks
    assert "PRIMARY KEY (BuildId, Seq)" in chunks
    assert "INTERLEAVE IN PARENT BuildBundles ON DELETE CASCADE" in chunks
    assert text.endswith(";\n")
    # the emitted text is itself DDL the lint reads: two statements
    stmts = [s for s in text.split(";") if "CREATE TABLE" in s]
    assert len(stmts) == 2


def test_without_keys_the_primary_key_is_left_to_fill_in_and_designed_wins():
    report = _report()
    report["designed"] = {}                       # no diff in the report:
    text = check.emit_ddl(report, designed={"Users", "Builds", "BuildBundles"})
    assert "CREATE TABLE BuildBundles" not in text
    assert "CREATE TABLE BuildBundleChunks (" in text and "PRIMARY KEY (?)" in text
    assert check.emit_ddl(report, designed={"Users", "Builds", "BuildBundles",
                                            "BuildBundleChunks"}) == \
        "-- every live table is in db/spanner\n"


def test_the_repo_ddl_now_designs_the_bundle_tables():
    from sahs.util.spanner import designed_tables
    designed = designed_tables(SILO / "db" / "spanner")
    assert designed["BuildBundles"] == [
        "BuildId", "Format", "SizeBytes", "Sha256", "FileCount", "ChunkBytes",
        "ChunkCount", "Complete", "PublishedBy", "PublishedAt"]
    assert designed["BuildBundleChunks"] == ["BuildId", "Seq", "Chunk"]
    # the columns the writer names are exactly the designed ones
    src = (SILO / "sahs" / "builds" / "spanner_store.py").read_text(encoding="utf-8")
    insert = src.split('"BuildBundles",', 1)[1].split("[(", 1)[0]
    for column in designed["BuildBundles"]:
        assert f'"{column}"' in insert, column
