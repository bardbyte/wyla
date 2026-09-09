"""Files in the chat: what is accepted and how it rides, the office
converters without a dependency, the workspace manifest, the parts on
the user turn, and a whole turn carrying a PDF through the scripted
gateway — the request body pinned."""

from __future__ import annotations

import base64
import importlib.util
import json
import sys
from pathlib import Path

import pytest

from sahs.assistant import files as files_mod
from sahs.assistant.files import (FileRefused, docx_to_text, manifest,
                                  mark_sent, parts_for, pending, remove,
                                  store, support_table, xlsx_to_text)

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO / "tests"))
from test_eag_plane import (SECRET, FakeEag, _client,  # noqa: E402,F401
                            compiled)

_spec = importlib.util.spec_from_file_location(
    "files_check", SILO / "scripts" / "files_check.py")
files_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(files_check)


def test_the_support_table_and_the_refusals(tmp_path):
    rows = {r["suffix"]: r for r in support_table()}
    assert rows["pdf"]["rides"] == "as itself, inline"
    assert rows["csv"]["rides"] == "as text"
    assert rows["xlsx"]["rides"] == "converted to text here"
    assert {"png", "jpg", "webp", "txt", "md", "json", "docx", "pptx"} <= set(rows)
    for name, why in (("a.exe", "not a data file"),
                      ("a.xls", "save it as .xlsx"),
                      ("a.mp4", "video"), ("noext", "no file type"),
                      ("a.parquet", "not a file type the model reads")):
        with pytest.raises(FileRefused) as err:
            store(tmp_path, name, b"x")
        assert why in str(err.value)
    with pytest.raises(FileRefused) as err:
        store(tmp_path, "big.csv", b"x" * (files_mod.MAX_FILE_BYTES + 1))
    assert "over the 10 MB" in str(err.value)
    assert manifest(tmp_path) == []            # nothing left behind


def test_office_files_convert_here_and_text_rides_as_text(tmp_path):
    book = store(tmp_path, "Q2 spend.xlsx",
                 files_check.tiny_xlsx("XLSX-1"))
    assert book.rides == "convert" and book.family == "workbook"
    assert book.name == "Q2-spend.xlsx" and "converted to text" in book.note
    text = (tmp_path / "files" / f"{book.id}.txt").read_text()
    assert "## sheet: Summary" in text and "## sheet: Notes" in text
    assert "spend,12.5" in text and "XLSX-1" in text
    memo = store(tmp_path, "memo.docx", files_check.tiny_docx("DOCX-1"))
    assert memo.rides == "convert"
    assert "Token: DOCX-1" in (tmp_path / "files" / f"{memo.id}.txt").read_text()
    csv = store(tmp_path, "rows.csv", b"a,b\n1,2\n")
    assert csv.rides == "text" and csv.text_chars == 8
    assert xlsx_to_text(files_check.tiny_xlsx("T")).count("## sheet") == 2
    assert docx_to_text(files_check.tiny_docx("T")).startswith("Quarterly note.")
    assert [r["name"] for r in manifest(tmp_path)] == [
        "Q2-spend.xlsx", "memo.docx", "rows.csv"]
    assert remove(tmp_path, csv.id) and not remove(tmp_path, csv.id)
    assert not list((tmp_path / "files").glob(f"{csv.id}.*"))


def test_parts_ride_the_turn_in_order_and_the_budget_holds(tmp_path):
    pdf = store(tmp_path, "token.pdf", files_check.tiny_pdf("P"))
    png = store(tmp_path, "square.png", files_check.tiny_png(16))
    csv = store(tmp_path, "rows.csv", b"a,b\n1,2\n")
    book = store(tmp_path, "book.xlsx", files_check.tiny_xlsx("X"))
    parts, used = parts_for(tmp_path, [pdf.id, csv.id, png.id, book.id])
    assert [r["id"] for r in used] == [pdf.id, csv.id, png.id, book.id]
    assert parts[0] == {"text": "[attached file: token.pdf]"}
    assert parts[1]["inlineData"]["mimeType"] == "application/pdf"
    assert base64.b64decode(parts[1]["inlineData"]["data"]).startswith(b"%PDF")
    assert parts[2]["text"].startswith("[attached file: rows.csv]\na,b")
    assert parts[2]["text"].endswith("[end of file]")
    assert parts[4]["inlineData"]["mimeType"] == "image/png"
    assert parts[5]["text"].startswith(
        "[attached file: book.xlsx (converted to text)]\n## sheet: Summary")
    with pytest.raises(FileRefused) as err:
        parts_for(tmp_path, ["f_nope"])
    assert "no file f_nope" in str(err.value)
    # pending until sent, then remembered as sent
    assert {r["id"] for r in pending(tmp_path)} == {pdf.id, png.id, csv.id,
                                                    book.id}
    mark_sent(tmp_path, [pdf.id, csv.id], "t_1")
    assert {r["id"] for r in pending(tmp_path)} == {png.id, book.id}
    assert next(r for r in manifest(tmp_path)
                if r["id"] == pdf.id)["sent_turn"] == "t_1"
    # the inline budget: refused with the reason, never trimmed
    files_mod.MAX_INLINE_BYTES, saved = 300, files_mod.MAX_INLINE_BYTES
    try:
        with pytest.raises(FileRefused) as err:
            parts_for(tmp_path, [pdf.id, png.id])
        assert "add up to more than" in str(err.value)
    finally:
        files_mod.MAX_INLINE_BYTES = saved


def test_a_whole_turn_carries_a_pdf_to_the_model(compiled, tmp_path):
    """The chat with a file: the request body carries the inline PDF
    before the words on the user turn, the stored message names the
    file, the turn record names it, the file is no longer pending, and
    the next turn's history says a file was sent — by name, not bytes."""
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import EagAgent
    build, tmp = compiled
    seen: list[dict] = []

    def answer(body):
        seen.append(body)
        return {"parts": [{"text": "The token is PDF-7413-ORCHID."}]}

    fake = FakeEag([answer, answer])
    client = _client(fake)
    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp_path / "chat.sqlite3",
        model_factory=lambda budget: EagAgent(client, budget))
    session = runtime.create_session()
    sid = session["id"]
    row = runtime.add_file(sid, "token.pdf",
                           files_check.tiny_pdf("PDF-7413-ORCHID"))
    assert row["rides"] == "inline" and row["family"] == "document"
    assert [f["id"] for f in runtime.files(sid)] == [row["id"]]
    started = runtime.start_turn(sid, "what token is in the file?",
                                 files=[row["id"]])
    assert started["files"] == ["token.pdf"]
    assert runtime.wait(sid, 60)
    user_parts = seen[0]["contents"][-1]["parts"]
    assert user_parts[0] == {"text": "[attached file: token.pdf]"}
    assert user_parts[1]["inlineData"]["mimeType"] == "application/pdf"
    assert user_parts[2] == {"text": "what token is in the file?"}
    events = runtime.runtime(sid).bus.since(0)
    assert events[0]["ev"] == "turn_started" and events[0]["files"] == [
        "token.pdf"]
    prompt = next(e for e in events if e["ev"] == "model_prompt"
                  and e.get("kind") == "call")
    assert "inline application/pdf" in prompt["content"]
    assert "JVBER" not in prompt["content"]            # no base64 in the log
    stored = runtime.store.messages(sid)[0]
    assert stored["role"] == "user" and stored["payload"]["files"][0][
        "name"] == "token.pdf"
    assert runtime.files(sid)[0]["sent_turn"] == started["turn_id"]
    # the next turn: the history names the file, carries no bytes
    runtime.start_turn(sid, "and what else?")
    assert runtime.wait(sid, 60)
    history = seen[1]["contents"]
    assert history[0]["role"] == "user"
    assert history[0]["parts"][0]["text"].endswith(
        "(attached files on that message: token.pdf)")
    assert not any("inlineData" in p for c in history for p in c["parts"])
    # a message naming an unknown file is refused before anything lands
    with pytest.raises(FileRefused):
        runtime.start_turn(sid, "again", files=["f_nope"])
    assert len(runtime.store.messages(sid)) == 4
    assert json.dumps(runtime.file_support())            # serialisable
    assert runtime.file_support()["max_file_mb"] == 10
