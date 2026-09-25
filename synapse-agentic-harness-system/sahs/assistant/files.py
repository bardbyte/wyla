"""Files in the chat (Synapse v3): what a person attaches to a
message, how it reaches the model, and what is honestly supported.

Gemini reads three families natively, as ``inlineData`` parts on the
user turn: documents (PDF), images (PNG, JPEG, WEBP, HEIC/HEIF) and
text in its many suffixes (plain, markdown, CSV, JSON, XML, HTML,
source code). Office files are NOT native: a workbook, a Word file
or a deck is converted here — sheets to CSV text, paragraphs to
text — and rides as text, disclosed as such. Audio and video are
native to the model but are not offered: this is an analytical
chat, and a recording is not a data file. Anything else is refused
with the reason, never silently dropped.

Everything lands in the session's workspace under ``files/`` with a
manifest, so a turn can name exactly what it carried; a file rides
the message it is sent with (the history replays as text), and the
stored user message says which files it carried. Under
``SAHS_STORE=spanner|sqlite`` the same manifest is a ``ChatFiles``
row and the bytes are ``ChatFileChunks`` rows
(``sahs/assistant/content_store.py``): ``prepare`` classifies and
converts for both homes, ``build_parts`` builds the turn's parts for
both, and the functions below that take a workspace are the
filesystem home, unchanged.

No new dependency: the workbook and the Word file are zipped XML,
read with the standard library; a deck uses python-pptx when it is
installed (the assistant extra) and is refused otherwise.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import re
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree as ET

MAX_FILE_BYTES = 10 * 1024 * 1024      # one file
MAX_INLINE_BYTES = 18 * 1024 * 1024    # every inline part on one turn:
                                       # the request must stay under
                                       # the 20 MB the model accepts
MAX_TEXT_CHARS = 200_000               # a converted or text file, per file
MAX_FILES_PER_TURN = 10

# how each supported suffix reaches the model
INLINE = "inline"        # bytes, base64, as the model's own format
TEXT = "text"            # the bytes are text: they ride as a text part
CONVERT = "convert"      # office file: converted to text here

SUPPORTED: dict[str, tuple[str, str, str]] = {
    # suffix: (mime type, how it rides, the family the person sees)
    "pdf":  ("application/pdf", INLINE, "document"),
    "png":  ("image/png", INLINE, "image"),
    "jpg":  ("image/jpeg", INLINE, "image"),
    "jpeg": ("image/jpeg", INLINE, "image"),
    "webp": ("image/webp", INLINE, "image"),
    "heic": ("image/heic", INLINE, "image"),
    "heif": ("image/heif", INLINE, "image"),
    "txt":  ("text/plain", TEXT, "text"),
    "md":   ("text/markdown", TEXT, "text"),
    "csv":  ("text/csv", TEXT, "text"),
    "tsv":  ("text/tab-separated-values", TEXT, "text"),
    "json": ("application/json", TEXT, "text"),
    "xml":  ("text/xml", TEXT, "text"),
    "html": ("text/html", TEXT, "text"),
    "sql":  ("text/plain", TEXT, "text"),
    "py":   ("text/x-python", TEXT, "text"),
    "yaml": ("text/plain", TEXT, "text"),
    "yml":  ("text/plain", TEXT, "text"),
    "xlsx": ("application/vnd.openxmlformats-officedocument."
             "spreadsheetml.sheet", CONVERT, "workbook"),
    "docx": ("application/vnd.openxmlformats-officedocument."
             "wordprocessingml.document", CONVERT, "document"),
    "pptx": ("application/vnd.openxmlformats-officedocument."
             "presentationml.presentation", CONVERT, "deck"),
}

NOT_OFFERED = {
    "xls": "the old Excel format: save it as .xlsx or .csv",
    "doc": "the old Word format: save it as .docx or .txt",
    "ppt": "the old PowerPoint format: save it as .pptx",
    "zip": "an archive: attach the files inside it",
    "mp3": "audio is not a data file for this chat",
    "wav": "audio is not a data file for this chat",
    "mp4": "video is not a data file for this chat",
    "mov": "video is not a data file for this chat",
    "exe": "not a data file",
}


class FileRefused(ValueError):
    """The file cannot ride: the reason is the message."""


def support_table() -> list[dict[str, Any]]:
    """What the composer says it accepts, from the one table above."""
    rows = []
    for suffix, (mime, how, family) in SUPPORTED.items():
        rows.append({"suffix": suffix, "mime": mime, "family": family,
                     "rides": {INLINE: "as itself, inline",
                               TEXT: "as text",
                               CONVERT: "converted to text here"}[how]})
    return rows


def suffix_of(name: str) -> str:
    return Path(name or "").suffix.lstrip(".").lower()


def _slug(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(name).stem).strip("-.")
    return (stem or "file")[:80]


@dataclass
class Stored:
    id: str
    name: str
    suffix: str
    mime: str
    family: str
    rides: str
    size: int
    text_chars: int
    note: str

    def row(self) -> dict[str, Any]:
        return self.__dict__.copy()


# ── the office converters: zipped XML, standard library only ──
_NS_SHEET = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_NS_REL = ("{http://schemas.openxmlformats.org/officeDocument/2006/"
           "relationships}")
_NS_WORD = ("{http://schemas.openxmlformats.org/wordprocessingml/2006/"
            "main}")


def _col_index(ref: str) -> int:
    letters = re.match(r"[A-Z]+", ref or "")
    n = 0
    for ch in (letters.group(0) if letters else "A"):
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def xlsx_to_text(data: bytes, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Every sheet as CSV text under a heading, shared strings
    resolved, formulas by their cached value."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.iter(f"{_NS_SHEET}si"):
                shared.append("".join(t.text or "" for t in
                                      si.iter(f"{_NS_SHEET}t")))
        book = ET.fromstring(z.read("xl/workbook.xml"))
        rels = {}
        if "xl/_rels/workbook.xml.rels" in z.namelist():
            for rel in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")):
                rels[rel.get("Id")] = rel.get("Target")
        out: list[str] = []
        for sheet in book.iter(f"{_NS_SHEET}sheet"):
            title = sheet.get("name") or "sheet"
            target = rels.get(sheet.get(f"{_NS_REL}id"), "")
            path = target if target.startswith("xl/") else f"xl/{target}"
            if path not in z.namelist():
                continue
            buf = io.StringIO()
            writer = csv.writer(buf)
            root = ET.fromstring(z.read(path))
            for row in root.iter(f"{_NS_SHEET}row"):
                cells: list[str] = []
                for c in row.iter(f"{_NS_SHEET}c"):
                    at = _col_index(c.get("r", ""))
                    while len(cells) < at:
                        cells.append("")
                    v = c.find(f"{_NS_SHEET}v")
                    value = v.text if v is not None and v.text else ""
                    if c.get("t") == "s" and value.isdigit():
                        value = shared[int(value)] \
                            if int(value) < len(shared) else value
                    elif c.get("t") == "inlineStr":
                        value = "".join(t.text or "" for t in
                                        c.iter(f"{_NS_SHEET}t"))
                    cells.append(value)
                writer.writerow(cells)
            out.append(f"## sheet: {title}\n{buf.getvalue().rstrip()}")
            if sum(len(x) for x in out) > max_chars:
                break
    text = "\n\n".join(out)
    return text[:max_chars] + ("\n… truncated" if len(text) > max_chars
                               else "")


def docx_to_text(data: bytes, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Paragraphs in order, table cells tab-separated."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        root = ET.fromstring(z.read("word/document.xml"))
    lines: list[str] = []
    body = root.find(f"{_NS_WORD}body")
    for block in (body if body is not None else root):
        tag = block.tag.replace(_NS_WORD, "")
        if tag == "p":
            lines.append("".join(t.text or "" for t in
                                 block.iter(f"{_NS_WORD}t")))
        elif tag == "tbl":
            for tr in block.iter(f"{_NS_WORD}tr"):
                lines.append("\t".join(
                    "".join(t.text or "" for t in tc.iter(f"{_NS_WORD}t"))
                    for tc in tr.iter(f"{_NS_WORD}tc")))
    text = "\n".join(lines).strip()
    return text[:max_chars] + ("\n… truncated" if len(text) > max_chars
                               else "")


def pptx_to_text(data: bytes, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Slide by slide, every text frame; needs python-pptx (the
    assistant extra), refused with the reason when it is absent."""
    try:
        from pptx import Presentation
    except ImportError as e:
        raise FileRefused("a deck needs python-pptx installed: "
                          "pip install -e '.[assistant]'") from e
    deck = Presentation(io.BytesIO(data))
    out: list[str] = []
    for n, slide in enumerate(deck.slides, 1):
        texts = []
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False) and shape.text_frame:
                texts.append(shape.text_frame.text)
        out.append(f"## slide {n}\n" + "\n".join(t for t in texts if t))
    text = "\n\n".join(out)
    return text[:max_chars] + ("\n… truncated" if len(text) > max_chars
                               else "")


CONVERTERS = {"xlsx": xlsx_to_text, "docx": docx_to_text,
              "pptx": pptx_to_text}


# ── the workspace: files/<id>.<suffix> + files/<id>.txt + manifest ──
def _files_dir(workspace: Path) -> Path:
    d = Path(workspace) / "files"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _manifest_path(workspace: Path) -> Path:
    return _files_dir(workspace) / "manifest.json"


def manifest(workspace: Path) -> list[dict[str, Any]]:
    path = _manifest_path(workspace)
    if not path.exists():
        return []
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return []
    return rows if isinstance(rows, list) else []


def _write_manifest(workspace: Path, rows: list[dict[str, Any]]) -> None:
    _manifest_path(workspace).write_text(
        json.dumps(rows, indent=1), encoding="utf-8")


def check(name: str, size: int) -> tuple[str, str, str, str]:
    """(suffix, mime, how it rides, family) or FileRefused with why."""
    suffix = suffix_of(name)
    if not suffix:
        raise FileRefused(f"{name}: no file type to go on")
    if suffix in NOT_OFFERED:
        raise FileRefused(f"{name}: {NOT_OFFERED[suffix]}")
    if suffix not in SUPPORTED:
        raise FileRefused(f"{name}: .{suffix} is not a file type the "
                          "model reads; PDF, images, text, CSV, JSON, "
                          "workbooks, Word files and decks are")
    if size > MAX_FILE_BYTES:
        raise FileRefused(f"{name}: {size / 1048576:.1f} MB is over the "
                          f"{MAX_FILE_BYTES // 1048576} MB a file may be")
    mime, how, family = SUPPORTED[suffix]
    return suffix, mime, how, family


def prepare(name: str, data: bytes) -> tuple[Stored, str | None]:
    """The one classification for both homes of a file — the workspace
    and the store's ``ChatFiles`` row: check the type and the size,
    convert what needs converting, decode what is text, and describe
    it; ``(the manifest row, the text or None)``. Refused files raise
    ``FileRefused`` before anything is written anywhere."""
    suffix, mime, how, family = check(name, len(data))
    file_id = f"f_{uuid.uuid4().hex[:10]}"
    note = ""
    text_chars = 0
    text: str | None = None
    if how == CONVERT:
        text = CONVERTERS[suffix](data)         # raises FileRefused
        text_chars = len(text)
        note = (f"converted to text here ({text_chars:,} characters): "
                "the model reads the text, not the file")
    elif how == TEXT:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1")
        if len(text) > MAX_TEXT_CHARS:
            text = text[:MAX_TEXT_CHARS] + "\n… truncated"
            note = f"truncated to {MAX_TEXT_CHARS:,} characters"
        text_chars = len(text)
    stored = Stored(id=file_id, name=f"{_slug(name)}.{suffix}",
                    suffix=suffix, mime=mime, family=family, rides=how,
                    size=len(data), text_chars=text_chars, note=note)
    return stored, text


def store(workspace: Path, name: str, data: bytes) -> Stored:
    """Keep the file in the session's workspace, convert what needs
    converting, and record it; refused files leave nothing behind."""
    stored, text = prepare(name, data)
    files_dir = _files_dir(workspace)
    if text is not None:
        (files_dir / f"{stored.id}.txt").write_text(text, encoding="utf-8")
    (files_dir / f"{stored.id}.{stored.suffix}").write_bytes(data)
    rows = manifest(workspace)
    rows.append(stored.row())
    _write_manifest(workspace, rows)
    return stored


def remove(workspace: Path, file_id: str) -> bool:
    rows = manifest(workspace)
    keep = [r for r in rows if r.get("id") != file_id]
    if len(keep) == len(rows):
        return False
    for path in _files_dir(workspace).glob(f"{file_id}.*"):
        path.unlink(missing_ok=True)
    _write_manifest(workspace, keep)
    return True


def mark_sent(workspace: Path, file_ids: list[str], turn_id: str) -> None:
    """A file rides one message: once sent it is no longer pending in
    the composer (it stays in the workspace and in the manifest)."""
    rows = manifest(workspace)
    for row in rows:
        if row.get("id") in file_ids:
            row["sent_turn"] = turn_id
    _write_manifest(workspace, rows)


def pending(workspace: Path) -> list[dict[str, Any]]:
    return [r for r in manifest(workspace) if not r.get("sent_turn")]


def parts_for(workspace: Path,
              file_ids: list[str]) -> tuple[list[dict[str, Any]],
                                            list[dict[str, Any]]]:
    """The parts that ride the user turn for these files, in order,
    and the manifest rows they came from: inline data for a PDF or an
    image, a labelled text part for text and converted files. Over
    the inline budget the turn is refused, never trimmed in silence."""
    files_dir = _files_dir(workspace)
    return build_parts(
        manifest(workspace), file_ids,
        read_bytes=lambda row: (files_dir / f"{row['id']}.{row['suffix']}")
        .read_bytes(),
        read_text=lambda row: (files_dir / f"{row['id']}.txt")
        .read_text(encoding="utf-8"))


def build_parts(rows: list[dict[str, Any]], file_ids: list[str], *,
                read_bytes: Callable[[dict[str, Any]], bytes],
                read_text: Callable[[dict[str, Any]], str]
                ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``parts_for`` over any home for the bytes: the manifest rows and
    two readers (the workspace's files, or the store's chunks and text
    column). The same order, the same labels, the same budget."""
    by_id = {r["id"]: r for r in rows}
    parts: list[dict[str, Any]] = []
    used: list[dict[str, Any]] = []
    inline_total = 0
    for file_id in file_ids[:MAX_FILES_PER_TURN]:
        row = by_id.get(file_id)
        if row is None:
            raise FileRefused(f"no file {file_id} on this chat")
        if row["rides"] == INLINE:
            raw = read_bytes(row)
            inline_total += len(raw)
            if inline_total > MAX_INLINE_BYTES:
                raise FileRefused(
                    f"{row['name']}: the files on this message add up to "
                    f"more than {MAX_INLINE_BYTES // 1048576} MB; send "
                    "fewer at once")
            parts.append({"text": f"[attached file: {row['name']}]"})
            parts.append({"inlineData": {
                "mimeType": row["mime"],
                "data": base64.b64encode(raw).decode("ascii")}})
        else:
            text = read_text(row)
            label = row["name"] + (" (converted to text)"
                                   if row["rides"] == CONVERT else "")
            parts.append({"text": f"[attached file: {label}]\n{text}\n"
                                  "[end of file]"})
        used.append(row)
    return parts, used


__all__ = ["SUPPORTED", "NOT_OFFERED", "MAX_FILE_BYTES", "MAX_INLINE_BYTES",
           "MAX_TEXT_CHARS", "MAX_FILES_PER_TURN", "FileRefused", "Stored",
           "support_table", "suffix_of", "check", "prepare", "store", "remove",
           "manifest", "pending", "mark_sent", "parts_for", "build_parts",
           "xlsx_to_text", "docx_to_text", "pptx_to_text"]
