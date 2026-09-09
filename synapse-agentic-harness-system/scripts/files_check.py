"""Files through the model: what Gemini actually reads on this
machine's plane, proven before anyone attaches a file in the chat.

Builds small files here (no fixtures to carry): a one-page PDF with a
token in it, a PNG with a red square on white, a CSV with a token, a
workbook with a token on its second sheet (converted to text the way
the chat does), a Word file with a token (converted). Each rides one
model call as the chat would send it, and the answer is read for the
token. The report says, per file, whether the model read it, how long
the call took, and what the gateway said when it refused.

    python scripts/files_check.py            # the .env's plane
    SAHS_MODEL_PLANE=vertex python scripts/files_check.py
    python scripts/files_check.py --only pdf,png

Nothing is printed from the files but the tokens; no secret is read
beyond what the plane needs.
"""

from __future__ import annotations

import argparse
import base64
import io
import struct
import sys
import time
import zipfile
import zlib
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.assistant.files import (docx_to_text, parts_for,  # noqa: E402
                                  store, xlsx_to_text)

TOKENS = {"pdf": "PDF-7413-ORCHID", "png": "red square",
          "csv": "CSV-2290-JUNIPER", "xlsx": "XLSX-5581-MARIGOLD",
          "docx": "DOCX-3327-CEDAR", "txt": "TXT-9904-WILLOW"}


# ── the files, built here ─────────────────────────────────────
def tiny_pdf(text: str) -> bytes:
    """A valid one-page PDF with Helvetica text."""
    stream = f"BT /F1 24 Tf 72 700 Td ({text}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n"
        + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for n, obj in enumerate(objects, 1):
        offsets.append(out.tell())
        out.write(f"{n} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for off in offsets:
        out.write(f"{off:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
              f"startxref\n{xref}\n%%EOF\n".encode())
    return out.getvalue()


def tiny_png(size: int = 96) -> bytes:
    """White, with a red square in the middle."""
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (struct.pack(">I", len(body)) + kind + body
                + struct.pack(">I", zlib.crc32(kind + body) & 0xffffffff))
    rows = []
    lo, hi = size // 4, 3 * size // 4
    for y in range(size):
        row = bytearray(b"\x00")
        for x in range(size):
            row += b"\xff\x00\x00" if lo <= x < hi and lo <= y < hi \
                else b"\xff\xff\xff"
        rows.append(bytes(row))
    raw = zlib.compress(b"".join(rows), 9)
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2,
                                         0, 0, 0))
            + chunk(b"IDAT", raw) + chunk(b"IEND", b""))


def tiny_xlsx(token: str) -> bytes:
    """Two sheets; the token on the second, as a shared string."""
    def sheet(cells: list[list[str]]) -> str:
        rows = []
        for r, row in enumerate(cells, 1):
            cs = "".join(
                f'<c r="{chr(64 + c)}{r}" t="s"><v>{v}</v></c>'
                for c, v in enumerate(row, 1))
            rows.append(f'<row r="{r}">{cs}</row>')
        return ('<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns='
                '"http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f'<sheetData>{"".join(rows)}</sheetData></worksheet>')
    strings = ["metric", "value", "spend", "12.5", "note", token]
    shared = ('<?xml version="1.0" encoding="UTF-8"?><sst xmlns="http://'
              'schemas.openxmlformats.org/spreadsheetml/2006/main">'
              + "".join(f"<si><t>{s}</t></si>" for s in strings) + "</sst>")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns='
                   '"http://schemas.openxmlformats.org/package/2006/content-'
                   'types"><Default Extension="xml" ContentType="application/'
                   'xml"/></Types>')
        z.writestr("xl/workbook.xml", '<?xml version="1.0"?><workbook xmlns='
                   '"http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
                   ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/'
                   '2006/relationships"><sheets><sheet name="Summary" sheetId='
                   '"1" r:id="rId1"/><sheet name="Notes" sheetId="2" r:id='
                   '"rId2"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/'
                   'package/2006/relationships"><Relationship Id="rId1" Type='
                   '"x" Target="worksheets/sheet1.xml"/><Relationship Id="rId2"'
                   ' Type="x" Target="worksheets/sheet2.xml"/></Relationships>')
        z.writestr("xl/sharedStrings.xml", shared)
        z.writestr("xl/worksheets/sheet1.xml", sheet([["0", "1"], ["2", "3"]]))
        z.writestr("xl/worksheets/sheet2.xml", sheet([["4", "5"]]))
    return buf.getvalue()


def tiny_docx(token: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns='
                   '"http://schemas.openxmlformats.org/package/2006/content-'
                   'types"><Default Extension="xml" ContentType="application/'
                   'xml"/></Types>')
        z.writestr("word/document.xml", '<?xml version="1.0"?><w:document '
                   'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml'
                   '/2006/main"><w:body><w:p><w:r><w:t>Quarterly note.</w:t>'
                   f'</w:r></w:p><w:p><w:r><w:t>Token: {token}</w:t></w:r>'
                   '</w:p></w:body></w:document>')
    return buf.getvalue()


FILES = {
    "pdf": ("token.pdf", lambda: tiny_pdf(TOKENS["pdf"]),
            "What token is written in the attached PDF? Answer with the "
            "token only."),
    "png": ("square.png", tiny_png,
            "What shape and colour is drawn in the attached image? One "
            "short sentence."),
    "csv": ("rows.csv", lambda: f"metric,value,note\nspend,12.5,{TOKENS['csv']}\n".encode(),
            "What is in the note column of the attached CSV? Answer with "
            "the value only."),
    "txt": ("note.txt", lambda: f"Reminder token: {TOKENS['txt']}\n".encode(),
            "What is the reminder token in the attached text file? Answer "
            "with the token only."),
    "xlsx": ("book.xlsx", lambda: tiny_xlsx(TOKENS["xlsx"]),
             "What token is on the Notes sheet of the attached workbook? "
             "Answer with the token only."),
    "docx": ("memo.docx", lambda: tiny_docx(TOKENS["docx"]),
             "What token does the attached memo carry? Answer with the "
             "token only."),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", default="", help="pdf,png,csv,txt,xlsx,docx")
    ap.add_argument("--workspace", default="",
                    help="where the files land (default: a temp dir)")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(SILO / ".env")
    except ImportError:
        pass
    from sahs.assistant.agent import agent_from_env
    from sahs.ask.model import ModelUnavailable
    from sahs.util.gateway import model_plane, plane_note
    print(time.strftime("%H:%M:%S"))
    print(f"plane: {model_plane()} ({plane_note()})")
    # the converters first: they run here, not on the model
    print("converters: xlsx →", "ok" if TOKENS["xlsx"] in
          xlsx_to_text(tiny_xlsx(TOKENS["xlsx"])) else "FAILED",
          "· docx →", "ok" if TOKENS["docx"] in
          docx_to_text(tiny_docx(TOKENS["docx"])) else "FAILED")
    try:
        agent = agent_from_env()
    except ModelUnavailable as e:
        print(f"✗ no model: {e}", file=sys.stderr)
        return 3
    import tempfile
    workspace = Path(args.workspace) if args.workspace else Path(
        tempfile.mkdtemp(prefix="files_check_"))
    wanted = [k for k in FILES if not args.only or k in args.only.split(",")]
    results = {}
    for key in wanted:
        name, make, question = FILES[key]
        stored = store(workspace, name, make())
        parts, _rows = parts_for(workspace, [stored.id])
        contents = [{"role": "user", "parts": [*parts, {"text": question}]}]
        started = time.perf_counter()
        said, err = "", ""
        try:
            for event in agent.converse(contents, thinking_level="low",
                                        max_output_tokens=256):
                if event.get("kind") == "text":
                    said += event.get("delta", "")
        except Exception as e:                      # the gateway's word
            err = f"{type(e).__name__}: {e}"[:300]
        seconds = time.perf_counter() - started
        token = TOKENS[key]
        ok = (not err) and (token.lower() in said.lower())
        results[key] = ok
        mark = "✓" if ok else "✗"
        rides = "inline" if stored.rides == "inline" else "as text"
        print(f"  {mark} {key:<4} {rides:<8} {seconds:5.1f}s  "
              f"{'read it' if ok else (err or 'answer: ' + said.strip()[:120])}")
    print()
    good = [k for k, v in results.items() if v]
    bad = [k for k, v in results.items() if not v]
    print(f"read: {', '.join(good) or 'none'}"
          + (f" · not read: {', '.join(bad)}" if bad else ""))
    print("a file rides the message it is sent with; the chat's Add files "
          "offers exactly the kinds that read here")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
