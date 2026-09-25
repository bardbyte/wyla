"""The content store: what the assistant used to file on the
filesystem beside the chat, in the same database as the chat tables
(``db/spanner/002_chat.sql``, ``007_content.sql``) when ``SAHS_STORE``
is ``spanner`` or ``sqlite``.

Think of the hotel again (``docs/spanner-wiring.md``): the chat
tables are each guest's room, and this store moves the last of the
lobby's filing into them — or into the front desk's shared register
where the thing is shared by design. Four groups of verbs, each the
mirror of the filesystem module it replaces, so the callers barely
change:

  * **files on a chat** (``sahs/assistant/files.py``): the manifest
    row in ``ChatFiles``, the converted text in its ``Text`` column,
    the bytes in ``ChatFileChunks`` in slices of at most 8 MiB — the
    build bundle's pattern, since the deployment has no bucket. The
    classification and the conversion are ``files.prepare``; the parts
    that ride a turn are ``files.build_parts``. Nothing is duplicated.
  * **a person's own skills** (``sahs/assistant/authoring.py``,
    ``skills_loader.py``): ``UserSkills`` rows, one per name, for the
    owner alone.
  * **knowledge files** (``sources/artifacts/`` today): ``KnowledgeFiles``
    rows, shared — the shelf lists them, the staging door and an
    approved submission write them.
  * **the review board** (``sahs/assistant/reviews.py``): the same
    verbs as ``Reviews`` over ``ReviewSubmissions`` / ``ReviewVersions``
    / ``ReviewEvents`` / ``ReviewSeen``. The events are the ledger,
    folded by the very function the JSONL is folded by, so a
    submission reads the same from either book. The board is one per
    deployment: the review verbs ignore the owner except as the
    actor (whose notices, whose seen-mark).

The SQL is portable, as ``spanner_store.py`` writes it: ``@name``
parameters, ids made here, timestamps passed as values, mutations
for the writes; so one class runs on Cloud Spanner (``SpannerDatabase``)
and on the sqlite stand-in (``SqliteDatabase``), and ``ensure()``
teaches the stand-in the tables on first use.
"""

from __future__ import annotations

import json
import re
import threading
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable

from sahs.identity.database import Database, JsonValue, utcnow

from . import files as files_mod
from .reviews import (EXTS, Approver, board_row, check_decision,
                      check_submission, checks_review, fold_records,
                      notice_rows)

# Spanner caps a cell at 10 MiB; 8 MiB slices keep a 10 MB file to two
# rows and each commit well under the mutation ceiling
CHUNK_BYTES = 8 * 1024 * 1024
_SKILL_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")     # UserSkills' CHECK
_BU = re.compile(r"^[A-Za-z0-9_-]{1,40}$")

# the same tables as 002_chat.sql (the content ones) and 007_content.sql
# in sqlite's words: JSON as text, BOOL as integer, TIMESTAMP as text,
# BYTES as blob. ChatSessions (the parent of ChatFiles) and Users come
# from the chat and identity schemas on the same file.
CONTENT_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ChatFiles (
  SessionId TEXT NOT NULL, FileId TEXT NOT NULL, Name TEXT NOT NULL,
  Suffix TEXT NOT NULL, Mime TEXT NOT NULL, Family TEXT NOT NULL, Rides TEXT NOT NULL,
  SizeBytes INTEGER NOT NULL, ObjectPath TEXT, Text TEXT,
  TextChars INTEGER NOT NULL DEFAULT 0, Note TEXT, SentTurn TEXT, CreatedAt TEXT NOT NULL,
  PRIMARY KEY (SessionId, FileId));
CREATE TABLE IF NOT EXISTS ChatFileChunks (
  SessionId TEXT NOT NULL, FileId TEXT NOT NULL, Seq INTEGER NOT NULL,
  Chunk BLOB NOT NULL, PRIMARY KEY (SessionId, FileId, Seq));
CREATE TABLE IF NOT EXISTS UserSkills (
  UserId TEXT NOT NULL, Name TEXT NOT NULL, Title TEXT NOT NULL,
  Description TEXT NOT NULL DEFAULT '', Text TEXT NOT NULL,
  Origin TEXT NOT NULL DEFAULT 'unreviewed', Shared INTEGER NOT NULL DEFAULT 0,
  SharedBy TEXT, CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL,
  PRIMARY KEY (UserId, Name));
CREATE INDEX IF NOT EXISTS UserSkillsShared ON UserSkills (Shared, Name);
CREATE TABLE IF NOT EXISTS KnowledgeFiles (
  FileId TEXT PRIMARY KEY, BusinessUnit TEXT NOT NULL, Name TEXT NOT NULL,
  Ext TEXT NOT NULL DEFAULT 'md', Content TEXT NOT NULL, StagedBy TEXT NOT NULL,
  StagedAt TEXT NOT NULL, IngestedRun TEXT, RetiredAt TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS KnowledgeFilesByName ON KnowledgeFiles (BusinessUnit, Name, Ext);
CREATE TABLE IF NOT EXISTS ReviewSubmissions (
  SubmissionId TEXT PRIMARY KEY, Kind TEXT NOT NULL, Name TEXT NOT NULL,
  Title TEXT NOT NULL DEFAULT '', Description TEXT NOT NULL DEFAULT '',
  Purpose TEXT NOT NULL DEFAULT '', BusinessUnit TEXT NOT NULL DEFAULT '',
  Ext TEXT NOT NULL DEFAULT 'md', SubmitterUserId TEXT NOT NULL,
  SubmitterName TEXT NOT NULL, ApproverName TEXT NOT NULL, ApproverBand INTEGER NOT NULL,
  Status TEXT NOT NULL DEFAULT 'pending', Version INTEGER NOT NULL DEFAULT 1,
  CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ReviewSubmissionsByStatus ON ReviewSubmissions (Status, UpdatedAt);
CREATE INDEX IF NOT EXISTS ReviewSubmissionsBySubmitter ON ReviewSubmissions (SubmitterUserId, UpdatedAt);
CREATE TABLE IF NOT EXISTS ReviewVersions (
  SubmissionId TEXT NOT NULL, Version INTEGER NOT NULL, Text TEXT NOT NULL,
  Description TEXT NOT NULL DEFAULT '', Purpose TEXT NOT NULL DEFAULT '',
  Actor TEXT NOT NULL DEFAULT '', CreatedAt TEXT NOT NULL,
  PRIMARY KEY (SubmissionId, Version));
CREATE TABLE IF NOT EXISTS ReviewEvents (
  SubmissionId TEXT NOT NULL, Seq INTEGER NOT NULL, Event TEXT NOT NULL,
  Actor TEXT NOT NULL DEFAULT '', Comment TEXT NOT NULL DEFAULT '', Payload TEXT,
  OccurredAt TEXT NOT NULL, PRIMARY KEY (SubmissionId, Seq));
CREATE INDEX IF NOT EXISTS ReviewEventsByTime ON ReviewEvents (OccurredAt);
CREATE TABLE IF NOT EXISTS ReviewSeen (
  UserId TEXT PRIMARY KEY, SeenAt TEXT NOT NULL);
"""

_FILE_COLUMNS = ("SessionId, FileId, Name, Suffix, Mime, Family, Rides, SizeBytes, "
                 "TextChars, Note, SentTurn, CreatedAt")
_SKILL_COLUMNS = "UserId, Name, Title, Description, Text, Origin, CreatedAt, UpdatedAt"
_KNOWLEDGE_COLUMNS = ("FileId, BusinessUnit, Name, Ext, Content, StagedBy, StagedAt, "
                      "IngestedRun, RetiredAt")
_EVENT_COLUMNS = "SubmissionId, Seq, Event, Actor, Comment, Payload, OccurredAt"


# ── readers that accept Spanner's types and sqlite's text alike ──
def _stamp(value: Any) -> datetime | None:
    """A timestamp as the database returned it, as an aware datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def _iso(value: Any) -> str:
    """The ``now_iso`` shape the ledger writes (seconds, UTC offset)."""
    stamp = _stamp(value)
    if stamp is None:
        return "" if value is None else str(value)
    return stamp.astimezone(timezone.utc).isoformat(timespec="seconds")


def _loads(value: Any) -> Any:
    """A JSON column back to Python: a ``JsonObject`` (it serializes),
    sqlite's text, a fake's either; None stays None."""
    if value is None:
        return None
    if hasattr(value, "serialize"):
        value = value.serialize()
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _blob(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):                 # a base64 cell, should one appear
        import base64
        return base64.b64decode(value)
    return bytes(value)


def _new_id(prefix: str, width: int = 12) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:width]}"


class SpannerContentStore:
    """Files, own skills, knowledge files and the review board, in the
    identity database, for one signed-in person."""

    def __init__(self, database: Database, owner_user_id: str, *,
                 chunk_bytes: int = CHUNK_BYTES) -> None:
        owner = (owner_user_id or "").strip()
        if not owner:
            raise ValueError("the content tables need an owner: the signed-in "
                             "person's user id")
        self.db = database
        self.owner_user_id = owner
        self.chunk_bytes = max(1, int(chunk_bytes))
        self._threads: dict[str, threading.Thread] = {}
        ensure = getattr(database, "ensure", None)
        if callable(ensure):
            ensure(CONTENT_SQLITE_SCHEMA)

    # ══════════════════════════════════════════════════════════
    # (a) files on a chat: ChatFiles + ChatFileChunks
    # ══════════════════════════════════════════════════════════
    def _owns_session(self, session_id: str) -> bool:
        rows = self.db.query(
            "SELECT SessionId FROM ChatSessions WHERE SessionId = @id "
            "AND OwnerUserId = @owner",
            {"id": session_id, "owner": self.owner_user_id})
        return bool(rows)

    @staticmethod
    def _file_out(row: dict[str, Any]) -> dict[str, Any]:
        out = {"id": str(row["FileId"]), "name": str(row.get("Name") or ""),
               "suffix": str(row.get("Suffix") or ""), "mime": str(row.get("Mime") or ""),
               "family": str(row.get("Family") or ""), "rides": str(row.get("Rides") or ""),
               "size": int(row.get("SizeBytes") or 0),
               "text_chars": int(row.get("TextChars") or 0),
               "note": str(row.get("Note") or "")}
        if row.get("SentTurn"):
            out["sent_turn"] = str(row["SentTurn"])       # as the manifest marks it
        return out

    def add_file(self, session_id: str, name: str, data: bytes) -> dict[str, Any]:
        """Keep a file on the chat: the manifest row, the text, the
        bytes in chunks — one commit, so a refused file leaves nothing
        behind. ``KeyError`` for a chat that is not the owner's;
        ``FileRefused`` with the reason for a file that cannot ride."""
        if not self._owns_session(session_id):
            raise KeyError(session_id)
        stored, text = files_mod.prepare(name, data)     # raises FileRefused
        if text is not None:
            # as the workspace hands it back: a text-mode read folds
            # CRLF to LF, so the model sees the same text either home
            text = text.replace("\r\n", "\n").replace("\r", "\n")
        now = utcnow()
        chunks = [data[i:i + self.chunk_bytes]
                  for i in range(0, len(data), self.chunk_bytes)] or [b""]

        def work(tx: Any) -> None:
            tx.insert("ChatFiles",
                      ("SessionId", "FileId", "Name", "Suffix", "Mime", "Family", "Rides",
                       "SizeBytes", "ObjectPath", "Text", "TextChars", "Note", "SentTurn",
                       "CreatedAt"),
                      [(session_id, stored.id, stored.name, stored.suffix, stored.mime,
                        stored.family, stored.rides, stored.size, None, text,
                        stored.text_chars, stored.note or None, None, now)])
            tx.insert("ChatFileChunks", ("SessionId", "FileId", "Seq", "Chunk"),
                      [(session_id, stored.id, seq, chunk)
                       for seq, chunk in enumerate(chunks)])

        self.db.run(work)
        return stored.row()

    def files(self, session_id: str) -> list[dict[str, Any]]:
        """The manifest: every file on the chat, in the order added,
        pending and sent alike."""
        if not self._owns_session(session_id):
            raise KeyError(session_id)
        rows = self.db.query(
            f"SELECT {_FILE_COLUMNS} FROM ChatFiles WHERE SessionId = @id "
            "ORDER BY CreatedAt, FileId", {"id": session_id})
        return [self._file_out(r) for r in rows]

    def pending(self, session_id: str) -> list[dict[str, Any]]:
        return [r for r in self.files(session_id) if not r.get("sent_turn")]

    def _file_row(self, session_id: str, file_id: str,
                  columns: str = _FILE_COLUMNS) -> dict[str, Any] | None:
        if not self._owns_session(session_id):
            return None
        rows = self.db.query(
            f"SELECT {columns} FROM ChatFiles WHERE SessionId = @s AND FileId = @f",
            {"s": session_id, "f": file_id})
        return rows[0] if rows else None

    def remove_file(self, session_id: str, file_id: str) -> bool:
        if self._file_row(session_id, file_id) is None:
            return False

        def work(tx: Any) -> None:
            tx.execute_update(
                "DELETE FROM ChatFileChunks WHERE SessionId = @s AND FileId = @f",
                {"s": session_id, "f": file_id})
            tx.execute_update(
                "DELETE FROM ChatFiles WHERE SessionId = @s AND FileId = @f",
                {"s": session_id, "f": file_id})

        self.db.run(work)
        return True

    def file_bytes(self, session_id: str, file_id: str) -> bytes:
        """The bytes, the chunks concatenated in order; ``KeyError``
        for a file that is not on the owner's chat."""
        if self._file_row(session_id, file_id, "FileId") is None:
            raise KeyError(file_id)
        rows = self.db.query(
            "SELECT Chunk FROM ChatFileChunks WHERE SessionId = @s AND FileId = @f "
            "ORDER BY Seq", {"s": session_id, "f": file_id})
        return b"".join(_blob(r["Chunk"]) for r in rows)

    def file_text(self, session_id: str, file_id: str) -> str:
        """The text of a text or converted file ('' for an inline one)."""
        row = self._file_row(session_id, file_id, "FileId, Text")
        if row is None:
            raise KeyError(file_id)
        return str(row.get("Text") or "")

    def mark_sent(self, session_id: str, file_ids: list[str], turn_id: str) -> None:
        """A file rides one message: once sent it is no longer pending
        in the composer (the row stays)."""
        ids = [f for f in file_ids if f]
        if not ids or not self._owns_session(session_id):
            return
        self.db.run(lambda tx: tx.update(
            "ChatFiles", ("SessionId", "FileId", "SentTurn"),
            [(session_id, file_id, turn_id[:24]) for file_id in ids]))

    def parts_for(self, session_id: str, file_ids: list[str]
                  ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """``files.parts_for`` over the store: the parts that ride the
        user turn and the manifest rows they came from."""
        return files_mod.build_parts(
            self.files(session_id), list(file_ids),
            read_bytes=lambda row: self.file_bytes(session_id, row["id"]),
            read_text=lambda row: self.file_text(session_id, row["id"]))

    # ══════════════════════════════════════════════════════════
    # (b) a person's own skills: UserSkills
    # ══════════════════════════════════════════════════════════
    @staticmethod
    def _skill_out(row: dict[str, Any]) -> dict[str, Any]:
        return {"name": str(row["Name"]), "title": str(row.get("Title") or ""),
                "description": str(row.get("Description") or ""),
                "text": str(row.get("Text") or ""),
                "origin": str(row.get("Origin") or "unreviewed"),
                "user_id": str(row.get("UserId") or ""),
                "updated": _iso(row.get("UpdatedAt"))}

    def save_skill(self, name: str, title: str, description: str, text: str, *,
                   user_id: str = "") -> dict[str, Any]:
        """One row per name for the person: a second save replaces the
        text (``replaced`` says so). The name is held to the DDL's
        rule. ``user_id`` names another person's shelf — the approver
        publishing the submitter's pack — and is the owner otherwise."""
        name = (name or "").strip()
        if not _SKILL_NAME.match(name):
            raise ValueError(f"a skill name is lowercase letters, digits and "
                             f"dashes, at most 40 characters: {name!r}")
        who = (user_id or "").strip() or self.owner_user_id
        now = utcnow()

        def work(tx: Any) -> bool:
            rows = tx.query("SELECT Name FROM UserSkills WHERE UserId = @u AND Name = @n",
                            {"u": who, "n": name})
            if rows:
                tx.update("UserSkills",
                          ("UserId", "Name", "Title", "Description", "Text", "UpdatedAt"),
                          [(who, name, title[:200], description[:400], text, now)])
                return True
            tx.insert("UserSkills",
                      ("UserId", "Name", "Title", "Description", "Text", "Origin",
                       "Shared", "SharedBy", "CreatedAt", "UpdatedAt"),
                      [(who, name, title[:200], description[:400], text, "unreviewed",
                        False, None, now, now)])
            return False

        replaced = bool(self.db.run(work))
        return {"name": name, "title": title[:200], "description": description[:400],
                "text": text, "origin": "unreviewed", "user_id": who,
                "updated": _iso(now), "replaced": replaced}

    def delete_skill(self, name: str, *, user_id: str = "") -> bool:
        who = (user_id or "").strip() or self.owner_user_id
        hit = self.db.run(lambda tx: tx.execute_update(
            "DELETE FROM UserSkills WHERE UserId = @u AND Name = @n",
            {"u": who, "n": (name or "").strip()}))
        return int(hit or 0) > 0

    def my_skills(self, *, user_id: str = "") -> list[dict[str, Any]]:
        who = (user_id or "").strip() or self.owner_user_id
        rows = self.db.query(
            f"SELECT {_SKILL_COLUMNS} FROM UserSkills WHERE UserId = @u ORDER BY Name",
            {"u": who})
        return [self._skill_out(r) for r in rows]

    # ══════════════════════════════════════════════════════════
    # (c) knowledge files: KnowledgeFiles (shared)
    # ══════════════════════════════════════════════════════════
    @staticmethod
    def _knowledge_out(row: dict[str, Any]) -> dict[str, Any]:
        unit = str(row.get("BusinessUnit") or "")
        name = str(row.get("Name") or "")
        ext = str(row.get("Ext") or "md")
        return {"id": str(row["FileId"]), "business_unit": unit, "name": name,
                "ext": ext, "file": f"{unit.lower()}_{name}.{ext}",
                "content": str(row.get("Content") or ""),
                "staged_by": str(row.get("StagedBy") or ""),
                "staged_at": _iso(row.get("StagedAt")),
                "ingested_run": str(row.get("IngestedRun") or ""),
                "retired_at": _iso(row.get("RetiredAt"))}

    def stage_knowledge(self, business_unit: str, name: str, ext: str, content: str,
                        staged_by: str = "", *, replace: bool = False) -> dict[str, Any]:
        """A knowledge file for a business unit, where the shelf and the
        build read it. The same name again is refused unless ``replace``
        (an approved resubmission replaces its earlier version); a
        retired row under that name comes back with the new content."""
        unit = (business_unit or "").strip()
        name = (name or "").strip()
        ext = (ext or "md").strip().lower()
        if not _BU.match(unit):
            return {"ok": False, "reason": "a knowledge file names the business "
                                           "unit it is for"}
        if not name or len(name) > 120:
            return {"ok": False, "reason": "the file needs a name"}
        if ext not in EXTS:
            return {"ok": False, "reason": "ext is one of " + ", ".join(EXTS)}
        if not (content or "").strip():
            return {"ok": False, "reason": "the file is empty"}
        who = (staged_by or "").strip() or self.owner_user_id
        now = utcnow()

        def work(tx: Any) -> dict[str, Any]:
            rows = tx.query(
                "SELECT FileId, RetiredAt FROM KnowledgeFiles WHERE BusinessUnit = @b "
                "AND Name = @n AND Ext = @e", {"b": unit, "n": name, "e": ext})
            if rows:
                file_id = str(rows[0]["FileId"])
                if not rows[0].get("RetiredAt") and not replace:
                    return {"ok": False, "id": file_id,
                            "reason": f"{unit.lower()}_{name}.{ext} already staged; "
                                      "pick another name or edit the file directly"}
                tx.update("KnowledgeFiles",
                          ("FileId", "Content", "StagedBy", "StagedAt", "IngestedRun",
                           "RetiredAt"),
                          [(file_id, content, who, now, None, None)])
                return {"ok": True, "id": file_id, "replaced": True}
            file_id = _new_id("kf")
            tx.insert("KnowledgeFiles",
                      ("FileId", "BusinessUnit", "Name", "Ext", "Content", "StagedBy",
                       "StagedAt", "IngestedRun", "RetiredAt"),
                      [(file_id, unit, name, ext, content, who, now, None, None)])
            return {"ok": True, "id": file_id, "replaced": False}

        got = self.db.run(work)
        if not got.get("ok"):
            return got
        return {**got, "file": f"{unit.lower()}_{name}.{ext}",
                "business_unit": unit, "name": name, "ext": ext,
                "staged_by": who, "staged_at": _iso(now)}

    def knowledge_files(self, business_unit: str | None = None) -> list[dict[str, Any]]:
        """The active rows (not retired), the shelf's order: by unit and name."""
        sql = f"SELECT {_KNOWLEDGE_COLUMNS} FROM KnowledgeFiles WHERE RetiredAt IS NULL"
        params: dict[str, Any] = {}
        if business_unit:
            sql += " AND BusinessUnit = @b"
            params["b"] = business_unit.strip()
        rows = self.db.query(sql + " ORDER BY BusinessUnit, Name, Ext", params)
        return [self._knowledge_out(r) for r in rows]

    def knowledge_file(self, file_id: str) -> dict[str, Any] | None:
        rows = self.db.query(
            f"SELECT {_KNOWLEDGE_COLUMNS} FROM KnowledgeFiles WHERE FileId = @id",
            {"id": file_id})
        return self._knowledge_out(rows[0]) if rows else None

    def retire_knowledge(self, file_id: str) -> bool:
        hit = self.db.run(lambda tx: tx.execute_update(
            "UPDATE KnowledgeFiles SET RetiredAt = @now WHERE FileId = @id "
            "AND RetiredAt IS NULL", {"now": utcnow(), "id": file_id}))
        return int(hit or 0) > 0

    # ══════════════════════════════════════════════════════════
    # (d) the review board: the ledger as rows, the same fold
    # ══════════════════════════════════════════════════════════
    def _records(self, sid: str | None = None
                 ) -> tuple[list[dict[str, Any]], list[datetime | None]]:
        """The ledger's records from the event rows, oldest first, and
        beside each the instant it happened (for the seen-mark)."""
        sql = f"SELECT {_EVENT_COLUMNS} FROM ReviewEvents"
        params: dict[str, Any] = {}
        if sid:
            sql += " WHERE SubmissionId = @id"
            params["id"] = sid
        rows = self.db.query(sql + " ORDER BY OccurredAt, SubmissionId, Seq", params)
        records: list[dict[str, Any]] = []
        stamps: list[datetime | None] = []
        for r in rows:
            rec: dict[str, Any] = {"ev": str(r["Event"]), "id": str(r["SubmissionId"]),
                                   "at": _iso(r.get("OccurredAt"))}
            if r.get("Actor"):
                rec["by"] = str(r["Actor"])
            if r.get("Comment"):
                rec["comment"] = str(r["Comment"])
            payload = _loads(r.get("Payload"))
            if isinstance(payload, dict):
                rec.update(payload)
            records.append(rec)
            stamps.append(_stamp(r.get("OccurredAt")))
        return records, stamps

    def _fold(self, sid: str | None = None) -> dict[str, dict[str, Any]]:
        return fold_records(self._records(sid)[0])

    def _append(self, sid: str, event: str, *, by: str = "", comment: str = "",
                payload: dict[str, Any] | None = None,
                head: dict[str, Any] | None = None) -> None:
        """One event row (the next Seq of its submission) and, with
        ``head``, the head row's columns brought in step, in one commit."""
        now = utcnow()

        def work(tx: Any) -> None:
            rows = tx.query("SELECT MAX(Seq) AS S FROM ReviewEvents WHERE SubmissionId = @id",
                            {"id": sid})
            seq = int((rows[0]["S"] if rows else 0) or 0) + 1
            tx.insert("ReviewEvents",
                      ("SubmissionId", "Seq", "Event", "Actor", "Comment", "Payload",
                       "OccurredAt"),
                      [(sid, seq, event, (by or "")[:200], (comment or "")[:4000],
                        JsonValue(payload) if payload is not None else None, now)])
            if head:
                cols = ("SubmissionId", *head.keys(), "UpdatedAt")
                tx.update("ReviewSubmissions", cols, [(sid, *head.values(), now)])

        self.db.run(work)

    def text_of(self, sid: str, version: int | None = None) -> str:
        if version is None:
            rows = self.db.query(
                "SELECT Version FROM ReviewSubmissions WHERE SubmissionId = @id", {"id": sid})
            if not rows:
                return ""
            version = int(rows[0]["Version"] or 1)
        rows = self.db.query(
            "SELECT Text FROM ReviewVersions WHERE SubmissionId = @id AND Version = @v",
            {"id": sid, "v": int(version)})
        return str(rows[0]["Text"] or "") if rows else ""

    # ── reads ──
    def list(self, *, with_text: bool = False) -> list[dict[str, Any]]:
        rows = [board_row(sub, text=self.text_of(sub["id"]) if with_text else None)
                for sub in self._fold().values()]
        rows.sort(key=lambda r: r["updated_at"], reverse=True)
        return rows

    def get(self, sid: str) -> dict[str, Any] | None:
        sub = self._fold(sid).get(sid)
        return board_row(sub, text=self.text_of(sid)) if sub else None

    def find(self, kind: str, name: str, submitter_slug: str) -> dict[str, Any] | None:
        heads = self.db.query(
            "SELECT SubmissionId FROM ReviewSubmissions WHERE Kind = @k AND Name = @n "
            "AND Status != 'withdrawn' ORDER BY CreatedAt", {"k": kind, "n": name})
        for head in heads:
            sub = self._fold(str(head["SubmissionId"])).get(str(head["SubmissionId"]))
            if (sub is not None and sub.get("submitter_slug") == submitter_slug
                    and sub["status"] != "withdrawn"):
                return sub
        return None

    def published_names(self, kind: str = "skill") -> set[str]:
        rows = self.db.query(
            "SELECT Name FROM ReviewSubmissions WHERE Kind = @k AND Status = 'published'",
            {"k": kind})
        return {str(r["Name"]) for r in rows}

    # ── writes ──
    def submit(self, *, kind: str, name: str, text: str, title: str = "",
               description: str = "", purpose: str = "", business_unit: str = "",
               ext: str = "md", submitter: str, submitter_slug: str,
               approver: Approver, reserved: set[str] | frozenset[str] = frozenset()
               ) -> dict[str, Any]:
        """A new submission, or a new version of the one this person
        already has under that name: the ``Reviews`` door, on rows."""
        checked = check_submission(
            kind=kind, name=name, text=text, title=title, description=description,
            purpose=purpose, business_unit=business_unit, ext=ext,
            submitter_slug=submitter_slug, reserved=reserved)
        if not checked["ok"]:
            return checked
        existing = self.find(checked["kind"], checked["name"], submitter_slug)
        if existing is not None:
            return self.resubmit(existing["id"], text=checked["text"],
                                 description=checked["description"],
                                 purpose=checked["purpose"], by=submitter,
                                 title=checked["title"])
        sid = _new_id("sub")
        now = utcnow()
        record = {"kind": checked["kind"], "name": checked["name"],
                  "title": checked["title"], "description": checked["description"],
                  "purpose": checked["purpose"], "business_unit": checked["business_unit"],
                  "ext": checked["ext"], "submitter": submitter,
                  "submitter_slug": submitter_slug, "approver": asdict(approver),
                  "chars": len(checked["text"]),
                  # beyond the ledger's record: whose UserSkills row an
                  # approval writes (the approver may be another person)
                  "submitter_user_id": self.owner_user_id}

        def work(tx: Any) -> None:
            tx.insert("ReviewSubmissions",
                      ("SubmissionId", "Kind", "Name", "Title", "Description", "Purpose",
                       "BusinessUnit", "Ext", "SubmitterUserId", "SubmitterName",
                       "ApproverName", "ApproverBand", "Status", "Version", "CreatedAt",
                       "UpdatedAt"),
                      [(sid, record["kind"], record["name"], record["title"][:200],
                        record["description"], record["purpose"], record["business_unit"],
                        record["ext"], self.owner_user_id, submitter[:200],
                        approver.name[:200], int(approver.band), "pending", 1, now, now)])
            tx.insert("ReviewVersions",
                      ("SubmissionId", "Version", "Text", "Description", "Purpose", "Actor",
                       "CreatedAt"),
                      [(sid, 1, checked["text"], record["description"], record["purpose"],
                        submitter[:200], now)])
            tx.insert("ReviewEvents",
                      ("SubmissionId", "Seq", "Event", "Actor", "Comment", "Payload",
                       "OccurredAt"),
                      [(sid, 1, "submitted", "", "", JsonValue(record), now)])

        self.db.run(work)
        return {"ok": True, "submission": self.get(sid), "resubmitted": False}

    def resubmit(self, sid: str, *, text: str, description: str = "", purpose: str = "",
                 by: str = "", title: str = "", comment: str = "") -> dict[str, Any]:
        sub = self._fold(sid).get(sid)
        if sub is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        if sub["status"] == "withdrawn":
            return {"ok": False, "reason": "this submission was withdrawn"}
        text = (text or "").strip() + "\n"
        if len(text.strip()) < 20:
            return {"ok": False, "reason": "the file is empty"}
        version = sub["version"] + 1
        by = by or sub["submitter"]
        title, description = (title or "").strip()[:200], (description or "").strip()[:400]
        purpose, comment = (purpose or "").strip()[:600], (comment or "").strip()[:2000]
        now = utcnow()
        self.db.run(lambda tx: tx.insert(
            "ReviewVersions",
            ("SubmissionId", "Version", "Text", "Description", "Purpose", "Actor",
             "CreatedAt"),
            [(sid, version, text, description, purpose, by[:200], now)]))
        head: dict[str, Any] = {"Version": version, "Status": "pending"}
        for key, column in (("title", "Title"), ("description", "Description"),
                            ("purpose", "Purpose")):
            value = {"title": title, "description": description, "purpose": purpose}[key]
            if value:
                head[column] = value
        self._append(sid, "resubmitted", by=by, comment=comment,
                     payload={"version": version, "title": title,
                              "description": description, "purpose": purpose,
                              "chars": len(text)}, head=head)
        return {"ok": True, "submission": self.get(sid), "resubmitted": True}

    def record_ai(self, sid: str, version: int, review: dict[str, Any] | None, *,
                  reason: str = "") -> None:
        self._append(sid, "ai_review",
                     payload={"version": version, "review": review,
                              "status": "done" if review else "failed",
                              "reason": reason})

    def start_ai(self, sid: str, read: Callable[[dict[str, Any], str], dict[str, Any]]
                 ) -> None:
        """The model's read in the background, the checks first and at
        once — as ``Reviews.start_ai`` does, the record here a row."""
        sub = self._fold(sid).get(sid)
        if sub is None:
            return
        version = sub["version"]
        text = self.text_of(sid, version)
        checks = checks_review(sub["kind"], sub.get("title", ""), text,
                               purpose=sub.get("purpose", ""))
        self._append(sid, "ai_review",
                     payload={"version": version, "review": checks, "status": "running",
                              "reason": "the model's read is on its way"})

        def worker() -> None:
            try:
                got = read(sub, text)
            except Exception as e:                       # noqa: BLE001
                got = {"ok": False, "reason": str(e)[:300]}
            if got.get("ok"):
                self.record_ai(sid, version, got["review"])
            else:
                self._append(sid, "ai_review",
                             payload={"version": version, "review": checks,
                                      "status": "failed",
                                      "reason": got.get("reason", "no read")})
        t = threading.Thread(target=worker, name=f"review-{sid}", daemon=True)
        self._threads[sid] = t
        t.start()

    def wait(self, sid: str, timeout: float = 30.0) -> bool:
        t = self._threads.get(sid)
        if t is None:
            return True
        t.join(timeout)
        return not t.is_alive()

    def decide(self, sid: str, decision: str, *, comment: str = "", by: str = "",
               publish: Callable[[dict[str, Any], str], dict[str, Any]] | None = None
               ) -> dict[str, Any]:
        """Approve (publish through the caller's door) or reject, with
        the manager's comment; the band rule and the pending rule are
        ``check_decision``, shared with the ledger."""
        sub = self._fold(sid).get(sid)
        if sub is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        checked = check_decision(sub, decision, comment)
        if not checked["ok"]:
            return checked
        comment = checked["comment"]
        by = by or sub["approver"]["name"]
        if decision == "approve":
            published_path = ""
            if publish is not None:
                got = publish(sub, self.text_of(sid))
                if not got.get("ok"):
                    return {"ok": False, "reason": got.get("reason") or "not published"}
                published_path = str(got.get("path") or "")
            self._append(sid, "approved", by=by, comment=comment,
                         payload={"published_path": published_path,
                                  "version": sub["version"]},
                         head={"Status": "published"})
        else:
            self._append(sid, "rejected", by=by, comment=comment,
                         payload={"version": sub["version"]}, head={"Status": "rejected"})
        return {"ok": True, "submission": self.get(sid)}

    def withdraw(self, sid: str, *, by: str = "") -> dict[str, Any]:
        if self._fold(sid).get(sid) is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        self._append(sid, "withdrawn", by=by, head={"Status": "withdrawn"})
        return {"ok": True, "submission": self.get(sid)}

    # ── notices ──
    def _seen_at(self) -> datetime | None:
        rows = self.db.query("SELECT SeenAt FROM ReviewSeen WHERE UserId = @u",
                             {"u": self.owner_user_id})
        return _stamp(rows[0]["SeenAt"]) if rows else None

    def notices(self, me: str = "", *, limit: int = 30) -> dict[str, Any]:
        """The notices for the signed-in person (``me`` is accepted for
        the ``Reviews`` signature; the seen-mark is the owner's row):
        a notice is unread when its event is later than their mark."""
        records, stamps = self._records()
        subs = fold_records(records)
        seen = self._seen_at()

        def unread(idx: int, rec: dict[str, Any]) -> bool:
            stamp = stamps[idx]
            return seen is None or stamp is None or stamp > seen

        out = notice_rows(records, subs, unread)
        out.reverse()                                # newest first
        return {"notices": out[:limit],
                "unread": sum(1 for n in out if n["unread"]),
                "pending": sum(1 for s in subs.values() if s["status"] == "pending")}

    def mark_seen(self, me: str = "") -> None:
        """Everything on the board so far is read for the signed-in person."""
        self.db.run(lambda tx: tx.insert_or_update(
            "ReviewSeen", ("UserId", "SeenAt"), [(self.owner_user_id, utcnow())]))


def open_content_store(settings: Any, owner_user_id: str, *,
                       database: Database | None = None) -> SpannerContentStore:
    """The content store the settings describe: Spanner, or the sqlite
    stand-in, for one owner; pass the identity store's database to
    share one connection (the app does)."""
    from sahs.identity.database import open_database
    return SpannerContentStore(database if database is not None
                               else open_database(settings), owner_user_id)


__all__ = ["CHUNK_BYTES", "CONTENT_SQLITE_SCHEMA", "SpannerContentStore",
           "open_content_store"]
