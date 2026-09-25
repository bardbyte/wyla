"""The assistant's store on the chat tables of the identity database
(``db/spanner/002_chat.sql``): the same verbs as ``AssistantStore`` —
sessions, messages, plan versions, feedback, artifacts, projects,
memory — with the signed-in person as the owner of every row.

Think of ``AssistantStore`` as a notebook on one laptop and this as
the same pages in a shared ledger: the app asks the same questions in
the same words, only the book changed. Every read filters by the
owner, so two people on one deployment never see each other's chats.

The SQL is portable, the way the identity store writes it: ``@name``
parameters, ids generated here, timestamps passed as values. So the
one class runs on Cloud Spanner (``SpannerDatabase``) and on the
sqlite stand-in (``SqliteDatabase``, ``SAHS_STORE=sqlite``), and the
stand-in rehearses the deployment table for table. Columns that come
back typed on Spanner (TIMESTAMP, BOOL, ARRAY, JSON) come back as text
or integers from sqlite; the readers below accept both.

The event stream lands here too (``ChatEvents``): the runtime's bus
hands every record to ``add_event`` and a pod that restarts replays a
chat from ``events``. What sits beside it: ``sahs/assistant/content_store.py`` keeps the
files on a chat, a person's own skills, the knowledge files and the
review board in the same database; ``docs/spanner-wiring.md`` lists
every path and the few that are still on the filesystem.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from sahs.identity.database import Database, JsonValue, utcnow

# the same tables as 002_chat.sql, in sqlite's words, for the stand-in
# and the tests: JSON and ARRAY as text, BOOL as integer, TIMESTAMP as
# text; the identity file already holds Users, which these refer to
CHAT_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ChatProjects (
  ProjectId TEXT PRIMARY KEY, OwnerUserId TEXT NOT NULL, Name TEXT NOT NULL,
  Instructions TEXT NOT NULL DEFAULT '', Skills TEXT NOT NULL DEFAULT '[]',
  Archived INTEGER NOT NULL DEFAULT 0, CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ChatProjectsByOwner ON ChatProjects (OwnerUserId, Archived, UpdatedAt);
CREATE TABLE IF NOT EXISTS ChatSessions (
  SessionId TEXT PRIMARY KEY, OwnerUserId TEXT NOT NULL,
  Kind TEXT NOT NULL DEFAULT 'assistant', Title TEXT NOT NULL DEFAULT '',
  BuildId TEXT NOT NULL DEFAULT '', ProjectId TEXT,
  Model TEXT NOT NULL DEFAULT '' CHECK (length(Model) <= 64),
  Skills TEXT NOT NULL DEFAULT '[]', Starred INTEGER NOT NULL DEFAULT 0,
  Archived INTEGER NOT NULL DEFAULT 0, Handoff TEXT, Notes TEXT,
  MessageCount INTEGER NOT NULL DEFAULT 0, CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ChatSessionsByOwner ON ChatSessions (OwnerUserId, Archived, UpdatedAt);
CREATE TABLE IF NOT EXISTS ChatMessages (
  SessionId TEXT NOT NULL, MessageId TEXT NOT NULL, OwnerUserId TEXT NOT NULL,
  Seq INTEGER NOT NULL, TurnId TEXT NOT NULL DEFAULT '', Role TEXT NOT NULL,
  Text TEXT NOT NULL DEFAULT '', Payload TEXT, CreatedAt TEXT NOT NULL,
  PRIMARY KEY (SessionId, MessageId));
CREATE INDEX IF NOT EXISTS ChatMessagesBySeq ON ChatMessages (SessionId, Seq);
CREATE TABLE IF NOT EXISTS ChatArtifacts (
  SessionId TEXT NOT NULL, ArtifactId TEXT NOT NULL, Version INTEGER NOT NULL,
  TurnId TEXT NOT NULL DEFAULT '', Type TEXT NOT NULL, Title TEXT NOT NULL DEFAULT '',
  Spec TEXT NOT NULL, CreatedAt TEXT NOT NULL,
  PRIMARY KEY (SessionId, ArtifactId, Version));
CREATE TABLE IF NOT EXISTS ChatPlanVersions (
  SessionId TEXT NOT NULL, Version INTEGER NOT NULL, Parent INTEGER,
  TurnId TEXT NOT NULL DEFAULT '', Plan TEXT NOT NULL, Summary TEXT NOT NULL DEFAULT '',
  CreatedAt TEXT NOT NULL, PRIMARY KEY (SessionId, Version));
CREATE TABLE IF NOT EXISTS ChatFeedback (
  SessionId TEXT NOT NULL, FeedbackId TEXT NOT NULL, UserId TEXT NOT NULL,
  TurnId TEXT NOT NULL DEFAULT '', Subject TEXT NOT NULL, Vote TEXT NOT NULL,
  Note TEXT, CreatedAt TEXT NOT NULL, PRIMARY KEY (SessionId, FeedbackId));
CREATE TABLE IF NOT EXISTS ChatMemories (
  UserId TEXT NOT NULL, MemoryId TEXT NOT NULL, Text TEXT NOT NULL,
  Scope TEXT NOT NULL DEFAULT 'global', Status TEXT NOT NULL DEFAULT 'active',
  Source TEXT NOT NULL DEFAULT 'assistant', CreatedAt TEXT NOT NULL, RetiredAt TEXT,
  PRIMARY KEY (UserId, MemoryId));
CREATE INDEX IF NOT EXISTS ChatMemoriesActive ON ChatMemories (UserId, Status, Scope);
CREATE TABLE IF NOT EXISTS ChatEvents (
  SessionId TEXT NOT NULL, Seq INTEGER NOT NULL, TurnId TEXT NOT NULL DEFAULT '',
  Ev TEXT NOT NULL, Ts TEXT NOT NULL, Payload TEXT NOT NULL,
  PRIMARY KEY (SessionId, Seq));
"""

SESSION_KINDS = ("analyst", "steward", "assistant")
# ChatSessions.Model after 008_chat_model.sql: STRING(64), no plane CHECK
MODEL_CHOICE_CHARS = 64
_SESSION_COLUMNS = ("SessionId, OwnerUserId, Kind, Title, BuildId, ProjectId, Model, "
                    "Skills, Starred, Archived, Handoff, Notes, MessageCount, "
                    "CreatedAt, UpdatedAt")
_MESSAGE_COLUMNS = "MessageId, SessionId, TurnId, Role, Text, Payload, CreatedAt"
_ARTIFACT_COLUMNS = "SessionId, ArtifactId, Version, TurnId, Type, Title, Spec, CreatedAt"
_PROJECT_COLUMNS = ("ProjectId, Name, Instructions, Skills, Archived, "
                    "CreatedAt, UpdatedAt")


# ── readers that accept Spanner's types and sqlite's text alike ──
def _iso(value: Any) -> str:
    """A timestamp as the database returned it → the ``now_iso`` shape
    the sqlite store hands the page (seconds, UTC offset)."""
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        stamp = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return stamp.astimezone(timezone.utc).isoformat(timespec="seconds")
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc).isoformat(timespec="seconds")
    except ValueError:
        return text


def _loads(value: Any) -> Any:
    """A JSON column back to Python: Spanner hands a ``JsonObject`` (it
    serializes), sqlite the text, a fake either; None stays None."""
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


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    parsed = _loads(value)
    return [str(v) for v in parsed] if isinstance(parsed, list) else []


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class SpannerAssistantStore:
    """``AssistantStore``'s verbs over the chat tables, for one owner."""

    def __init__(self, database: Database, owner_user_id: str) -> None:
        owner = (owner_user_id or "").strip()
        if not owner:
            raise ValueError("the chat tables need an owner: the signed-in "
                             "person's user id (ChatSessions.OwnerUserId)")
        self.db = database
        self.owner_user_id = owner
        # the sqlite stand-in learns the chat tables on first use; a
        # Spanner database has them from the DDL
        ensure = getattr(database, "ensure", None)
        if callable(ensure):
            ensure(CHAT_SQLITE_SCHEMA)

    # ── shapes: what the page and the loop read ───────────────
    def _session_out(self, row: dict[str, Any]) -> dict[str, Any]:
        handoff = _loads(row.get("Handoff"))
        notes = _loads(row.get("Notes"))
        return {
            "id": str(row["SessionId"]),
            "kind": str(row.get("Kind") or "assistant"),
            "title": str(row.get("Title") or ""),
            "build_id": str(row.get("BuildId") or ""),
            "actor": str(row.get("OwnerUserId") or self.owner_user_id),
            "skills": _list(row.get("Skills")),
            "created_at": _iso(row.get("CreatedAt")),
            "updated_at": _iso(row.get("UpdatedAt")),
            "project_id": str(row.get("ProjectId") or ""),
            "starred": bool(row.get("Starred")),
            "archived": bool(row.get("Archived")),
            "handoff": handoff if isinstance(handoff, dict) else None,
            "notes": list(notes) if isinstance(notes, list) else [],
            "model": str(row.get("Model") or "").strip().lower(),
            "messages": int(row.get("MessageCount") or 0),
        }

    @staticmethod
    def _message_out(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(row["MessageId"]),
            "session_id": str(row["SessionId"]),
            "turn_id": str(row.get("TurnId") or ""),
            "role": str(row.get("Role") or ""),
            "text": str(row.get("Text") or ""),
            "payload": _loads(row.get("Payload")),
            "created_at": _iso(row.get("CreatedAt")),
        }

    @staticmethod
    def _artifact_out(row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "artifact_id": str(row["ArtifactId"]),
            "version": int(row["Version"]),
            "session_id": str(row["SessionId"]),
            "turn_id": str(row.get("TurnId") or ""),
            "type": str(row.get("Type") or ""),
            "title": str(row.get("Title") or ""),
            "created_at": _iso(row.get("CreatedAt")),
        }
        if "Spec" in row:
            spec = _loads(row.get("Spec"))
            out["spec"] = spec if isinstance(spec, dict) else {}
        return out

    @staticmethod
    def _project_out(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(row["ProjectId"]),
            "name": str(row.get("Name") or ""),
            "instructions": str(row.get("Instructions") or ""),
            "skills": _list(row.get("Skills")),
            "archived": bool(row.get("Archived")),
            "created_at": _iso(row.get("CreatedAt")),
            "updated_at": _iso(row.get("UpdatedAt")),
        }

    @staticmethod
    def _memory_out(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(row["MemoryId"]),
            "text": str(row.get("Text") or ""),
            "scope": str(row.get("Scope") or "global"),
            "status": str(row.get("Status") or "active"),
            "source": str(row.get("Source") or "assistant"),
            "created_at": _iso(row.get("CreatedAt")),
        }

    # ── sessions ─────────────────────────────────────────────
    def _session_row(self, session_id: str) -> dict[str, Any] | None:
        rows = self.db.query(
            f"SELECT {_SESSION_COLUMNS} FROM ChatSessions "
            "WHERE SessionId = @id AND OwnerUserId = @owner",
            {"id": session_id, "owner": self.owner_user_id})
        return rows[0] if rows else None

    def create_session(self, kind: str = "analyst", *, build_id: str = "",
                       actor: str = "admin", title: str = "") -> dict[str, Any]:
        if kind not in SESSION_KINDS:
            raise ValueError("kind is analyst, steward, or assistant "
                             "(the two hats, and the v2 chat)")
        session_id = _new_id("s")
        now = utcnow()
        self.db.run(lambda tx: tx.insert(
            "ChatSessions",
            ("SessionId", "OwnerUserId", "Kind", "Title", "BuildId", "ProjectId",
             "Model", "Skills", "Starred", "Archived", "Handoff", "Notes",
             "MessageCount", "CreatedAt", "UpdatedAt"),
            [(session_id, self.owner_user_id, kind, title[:120], build_id[:64], None,
              "", [], False, False, None, JsonValue([]), 0, now, now)]))
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        row = self._session_row(session_id)
        return self._session_out(row) if row else None

    def list_sessions(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.db.query(
            f"SELECT {_SESSION_COLUMNS} FROM ChatSessions WHERE OwnerUserId = @owner "
            "ORDER BY UpdatedAt DESC LIMIT @limit",
            {"owner": self.owner_user_id, "limit": int(limit)})
        return [self._session_out(r) for r in rows]

    def _update_session(self, session_id: str, columns: tuple[str, ...],
                        values: tuple[Any, ...], *, touch: bool = True) -> None:
        if self._session_row(session_id) is None:
            return
        cols = ("SessionId", *columns) + (("UpdatedAt",) if touch else ())
        vals = (session_id, *values) + ((utcnow(),) if touch else ())
        self.db.run(lambda tx: tx.update("ChatSessions", cols, [vals]))

    def set_skills(self, session_id: str, names: list[str]) -> None:
        self._update_session(session_id, ("Skills",), ([str(n) for n in names],))

    def set_title(self, session_id: str, title: str) -> None:
        self._update_session(session_id, ("Title",), (title[:120],))

    def touch(self, session_id: str) -> None:
        self._update_session(session_id, (), ())

    def set_flag(self, session_id: str, flag: str, on: bool) -> None:
        assert flag in ("starred", "archived")
        self._update_session(session_id, (flag.capitalize(),), (bool(on),))

    def set_model(self, session_id: str, plane: str) -> None:
        """The model choice the chat rides — a plane, or plane:model as
        the catalog spells it (the runtime validates it against the
        catalog before calling here). Stored whole, up to the column's
        64 characters (008_chat_model.sql); longer is refused by name,
        never cut to a different choice in silence."""
        choice = (plane or "").strip().lower()
        if len(choice) > MODEL_CHOICE_CHARS:
            raise ValueError(
                f"model choice {choice[:MODEL_CHOICE_CHARS]!r}… is "
                f"{len(choice)} characters; the column holds "
                f"{MODEL_CHOICE_CHARS}")
        self._update_session(session_id, ("Model",), (choice,))

    def set_project(self, session_id: str, project_id: str) -> None:
        self._update_session(session_id, ("ProjectId",), (project_id or None,))

    def set_handoff(self, session_id: str, handoff: dict[str, Any]) -> None:
        self._update_session(session_id, ("Handoff",), (JsonValue(dict(handoff)),),
                             touch=False)

    def set_notes(self, session_id: str, notes: list[str]) -> None:
        self._update_session(session_id, ("Notes",),
                             (JsonValue([str(n) for n in list(notes)[-8:]]),),
                             touch=False)

    # ── messages ─────────────────────────────────────────────
    def add_message(self, session_id: str, role: str, text: str = "", *,
                    turn_id: str = "", payload: Any = None) -> dict[str, Any]:
        message_id = _new_id("m")
        now = utcnow()

        def work(tx: Any) -> None:
            rows = tx.query(
                "SELECT MessageCount FROM ChatSessions WHERE SessionId = @id "
                "AND OwnerUserId = @owner",
                {"id": session_id, "owner": self.owner_user_id})
            if not rows:
                raise KeyError(session_id)
            seq = int(rows[0]["MessageCount"] or 0) + 1
            tx.insert("ChatMessages",
                      ("SessionId", "MessageId", "OwnerUserId", "Seq", "TurnId", "Role",
                       "Text", "Payload", "CreatedAt"),
                      [(session_id, message_id, self.owner_user_id, seq, turn_id[:24],
                        role, text, JsonValue(payload) if payload is not None else None,
                        now)])
            tx.update("ChatSessions", ("SessionId", "MessageCount", "UpdatedAt"),
                      [(session_id, seq, now)])

        self.db.run(work)
        return {"id": message_id, "session_id": session_id, "turn_id": turn_id,
                "role": role, "text": text,
                "payload": json.dumps(payload) if payload is not None else "",
                "created_at": _iso(now)}

    def messages(self, session_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            f"SELECT {_MESSAGE_COLUMNS} FROM ChatMessages WHERE SessionId = @id "
            "AND OwnerUserId = @owner ORDER BY Seq",
            {"id": session_id, "owner": self.owner_user_id})
        return [self._message_out(r) for r in rows]

    # ── plan versions (the stateful spine) ───────────────────
    def add_plan_version(self, session_id: str, plan: dict[str, Any], *,
                         parent: int | None = None, turn_id: str = "",
                         summary: str = "") -> dict[str, Any]:
        now = utcnow()

        def work(tx: Any) -> int:
            rows = tx.query(
                "SELECT MAX(Version) AS V FROM ChatPlanVersions WHERE SessionId = @id",
                {"id": session_id})
            version = int((rows[0]["V"] if rows else 0) or 0) + 1
            tx.insert("ChatPlanVersions",
                      ("SessionId", "Version", "Parent", "TurnId", "Plan", "Summary",
                       "CreatedAt"),
                      [(session_id, version, parent, turn_id[:24], JsonValue(dict(plan)),
                        summary, now)])
            return version

        version = self.db.run(work)
        return {"id": f"pv_{session_id}_{version}", "session_id": session_id,
                "version": version, "parent": parent, "turn_id": turn_id,
                "plan": plan, "summary": summary, "created_at": _iso(now)}

    def plan_versions(self, session_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT SessionId, Version, Parent, TurnId, Plan, Summary, CreatedAt "
            "FROM ChatPlanVersions WHERE SessionId = @id ORDER BY Version",
            {"id": session_id})
        out = []
        for r in rows:
            plan = _loads(r.get("Plan"))
            out.append({"id": f"pv_{session_id}_{int(r['Version'])}",
                        "session_id": session_id, "version": int(r["Version"]),
                        "parent": (int(r["Parent"]) if r.get("Parent") is not None else None),
                        "turn_id": str(r.get("TurnId") or ""),
                        "plan": plan if isinstance(plan, dict) else {},
                        "summary": str(r.get("Summary") or ""),
                        "created_at": _iso(r.get("CreatedAt"))})
        return out

    def latest_plan(self, session_id: str) -> dict[str, Any] | None:
        versions = self.plan_versions(session_id)
        return versions[-1] if versions else None

    # ── feedback ─────────────────────────────────────────────
    def add_feedback(self, session_id: str, subject: str, vote: str, *,
                     turn_id: str = "", note: str = "",
                     actor: str = "admin") -> dict[str, Any]:
        if vote not in ("up", "down"):
            raise ValueError("vote is up or down")
        feedback_id = _new_id("fb")
        now = utcnow()
        self.db.run(lambda tx: tx.insert(
            "ChatFeedback",
            ("SessionId", "FeedbackId", "UserId", "TurnId", "Subject", "Vote", "Note",
             "CreatedAt"),
            [(session_id, feedback_id, self.owner_user_id, turn_id[:24], subject[:24],
              vote, note[:2000] or None, now)]))
        return {"id": feedback_id, "session_id": session_id, "turn_id": turn_id,
                "subject": subject, "vote": vote, "note": note,
                "actor": self.owner_user_id, "created_at": _iso(now)}

    def feedback(self, session_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT SessionId, FeedbackId, UserId, TurnId, Subject, Vote, Note, CreatedAt "
            "FROM ChatFeedback WHERE SessionId = @id ORDER BY CreatedAt, FeedbackId",
            {"id": session_id})
        return [{"id": str(r["FeedbackId"]), "session_id": session_id,
                 "turn_id": str(r.get("TurnId") or ""), "subject": str(r.get("Subject") or ""),
                 "vote": str(r.get("Vote") or ""), "note": str(r.get("Note") or ""),
                 "actor": str(r.get("UserId") or ""), "created_at": _iso(r.get("CreatedAt"))}
                for r in rows]

    # ── the event stream (ChatEvents): what the page replays ─
    def add_event(self, session_id: str, record: dict[str, Any]) -> None:
        """One bus record into ChatEvents, keyed by the bus's own seq.
        The whole record is the Payload, so a replay hands the page
        exactly what the live stream did. Raises on a database error:
        the caller (the runtime's bus sink) decides that a turn never
        fails for it."""
        seq = int(record["seq"])
        self.db.run(lambda tx: tx.insert(
            "ChatEvents",
            ("SessionId", "Seq", "TurnId", "Ev", "Ts", "Payload"),
            [(session_id, seq, str(record.get("turn_id") or "")[:24],
              str(record.get("ev") or "")[:32], utcnow(),
              JsonValue(dict(record)))]))

    def events(self, session_id: str, after_seq: int = 0,
               limit: int = 4000) -> list[dict[str, Any]]:
        """The records after ``after_seq``, in order, for a chat this
        owner holds: the bus's ``since`` served from the table."""
        rows = self.db.query(
            "SELECT Seq, Payload FROM ChatEvents WHERE SessionId = @id "
            "AND Seq > @after AND SessionId IN (SELECT SessionId FROM ChatSessions "
            "WHERE OwnerUserId = @owner) ORDER BY Seq LIMIT @limit",
            {"id": session_id, "after": int(after_seq), "owner": self.owner_user_id,
             "limit": int(limit)})
        out = []
        for r in rows:
            record = _loads(r.get("Payload"))
            if not isinstance(record, dict):
                continue
            record["seq"] = int(r["Seq"])
            out.append(record)
        return out

    def last_event_seq(self, session_id: str) -> int:
        """The highest seq in the table for the chat, 0 when none: a
        fresh bus resumes from here so its numbering never collides."""
        rows = self.db.query(
            "SELECT MAX(Seq) AS S FROM ChatEvents WHERE SessionId = @id "
            "AND SessionId IN (SELECT SessionId FROM ChatSessions WHERE OwnerUserId = @owner)",
            {"id": session_id, "owner": self.owner_user_id})
        return int((rows[0]["S"] if rows else 0) or 0)

    def last_turn(self, session_id: str) -> dict[str, Any]:
        """The last turn the table knows: its id, its first seq, and
        whether it closed (a turn_done or error event). A pod that came
        back mid-turn reads this to tell the page where to replay from."""
        rows = self.db.query(
            "SELECT Seq, TurnId, Ev FROM ChatEvents WHERE SessionId = @id "
            "AND TurnId != '' AND SessionId IN (SELECT SessionId FROM ChatSessions "
            "WHERE OwnerUserId = @owner) ORDER BY Seq DESC LIMIT 1",
            {"id": session_id, "owner": self.owner_user_id})
        if not rows:
            return {"turn_id": "", "first_seq": None, "closed": True}
        turn_id = str(rows[0]["TurnId"])
        kinds = self.db.query(
            "SELECT MIN(Seq) AS First, "
            "SUM(CASE WHEN Ev IN ('turn_done', 'error') THEN 1 ELSE 0 END) AS Closed "
            "FROM ChatEvents WHERE SessionId = @id AND TurnId = @turn",
            {"id": session_id, "turn": turn_id})
        first = int((kinds[0]["First"] if kinds else 0) or 0)
        closed = int((kinds[0]["Closed"] if kinds else 0) or 0) > 0
        return {"turn_id": turn_id, "first_seq": first or None, "closed": closed}

    # ── projects ─────────────────────────────────────────────
    def create_project(self, name: str, *, instructions: str = "",
                       skills: list[str] | None = None) -> dict[str, Any]:
        project_id = f"p_{uuid.uuid4().hex[:10]}"
        now = utcnow()
        self.db.run(lambda tx: tx.insert(
            "ChatProjects",
            ("ProjectId", "OwnerUserId", "Name", "Instructions", "Skills", "Archived",
             "CreatedAt", "UpdatedAt"),
            [(project_id, self.owner_user_id, name.strip()[:80], instructions[:4000],
              [str(s) for s in list(skills or [])[:4]], False, now, now)]))
        return self.get_project(project_id)

    def update_project(self, project_id: str, **fields: Any) -> dict[str, Any] | None:
        if self.get_project(project_id) is None:
            return None
        columns: list[str] = []
        values: list[Any] = []
        for key, column in (("name", "Name"), ("instructions", "Instructions")):
            if fields.get(key) is not None:
                columns.append(column)
                values.append(str(fields[key])[:4000])
        if fields.get("skills") is not None:
            columns.append("Skills")
            values.append([str(s) for s in list(fields["skills"])[:4]])
        if fields.get("archived") is not None:
            columns.append("Archived")
            values.append(bool(fields["archived"]))
        if columns:
            cols = ("ProjectId", *columns, "UpdatedAt")
            vals = (project_id, *values, utcnow())
            self.db.run(lambda tx: tx.update("ChatProjects", cols, [vals]))
        return self.get_project(project_id)

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        rows = self.db.query(
            f"SELECT {_PROJECT_COLUMNS} FROM ChatProjects WHERE ProjectId = @id "
            "AND OwnerUserId = @owner", {"id": project_id, "owner": self.owner_user_id})
        return self._project_out(rows[0]) if rows else None

    def list_projects(self, include_archived: bool = False) -> list[dict[str, Any]]:
        rows = self.db.query(
            f"SELECT {_PROJECT_COLUMNS} FROM ChatProjects WHERE OwnerUserId = @owner"
            + ("" if include_archived else " AND Archived = @no")
            + " ORDER BY CreatedAt, ProjectId",
            {"owner": self.owner_user_id, **({} if include_archived else {"no": False})})
        return [self._project_out(r) for r in rows]

    # ── memory ───────────────────────────────────────────────
    def add_memory(self, text: str, *, scope: str = "global",
                   source: str = "assistant") -> dict[str, Any]:
        memory_id = f"m_{uuid.uuid4().hex[:10]}"
        now = utcnow()
        row = {"id": memory_id, "text": text.strip()[:400], "scope": scope[:48],
               "status": "active", "source": source[:24], "created_at": _iso(now)}
        self.db.run(lambda tx: tx.insert(
            "ChatMemories",
            ("UserId", "MemoryId", "Text", "Scope", "Status", "Source", "CreatedAt"),
            [(self.owner_user_id, memory_id, row["text"], row["scope"], "active",
              row["source"], now)]))
        return row

    def retire_memory(self, memory_id: str) -> bool:
        hit = self.db.run(lambda tx: tx.execute_update(
            "UPDATE ChatMemories SET Status = 'retired', RetiredAt = @now "
            "WHERE UserId = @owner AND MemoryId = @id AND Status = 'active'",
            {"now": utcnow(), "owner": self.owner_user_id, "id": memory_id}))
        return int(hit or 0) > 0

    def list_memories(self, *, project_id: str = "",
                      status: str = "active") -> list[dict[str, Any]]:
        scopes = ["global"] + ([f"project:{project_id}"] if project_id else [])
        params: dict[str, Any] = {"owner": self.owner_user_id, "status": status}
        marks = []
        for i, scope in enumerate(scopes):
            params[f"scope{i}"] = scope
            marks.append(f"@scope{i}")
        rows = self.db.query(
            "SELECT MemoryId, Text, Scope, Status, Source, CreatedAt FROM ChatMemories "
            "WHERE UserId = @owner AND Status = @status AND Scope IN ("
            + ", ".join(marks) + ") ORDER BY CreatedAt, MemoryId", params)
        return [self._memory_out(r) for r in rows]

    # ── artifacts ────────────────────────────────────────────
    def _insert_artifact(self, row: dict[str, Any]) -> None:
        now = utcnow()
        self.db.run(lambda tx: tx.insert(
            "ChatArtifacts",
            ("SessionId", "ArtifactId", "Version", "TurnId", "Type", "Title", "Spec",
             "CreatedAt"),
            [(row["session_id"], row["artifact_id"], row["version"], row["turn_id"][:24],
              row["type"], row["title"], JsonValue(dict(row["spec"])), now)]))
        row["created_at"] = _iso(now)

    def add_artifact(self, session_id: str, *, turn_id: str, type: str, title: str,
                     spec: dict[str, Any]) -> dict[str, Any]:
        row = {"artifact_id": _new_id("a"), "version": 1, "session_id": session_id,
               "turn_id": turn_id, "type": type, "title": title[:120], "spec": spec}
        self._insert_artifact(row)
        self.touch(session_id)
        return row

    def update_artifact(self, artifact_id: str, *, turn_id: str, spec: dict[str, Any],
                        title: str | None = None) -> dict[str, Any] | None:
        latest = self.get_artifact(artifact_id)
        if latest is None:
            return None
        row = {"artifact_id": artifact_id, "version": latest["version"] + 1,
               "session_id": latest["session_id"], "turn_id": turn_id,
               "type": latest["type"],
               "title": (title if title is not None else latest["title"])[:120],
               "spec": spec}
        self._insert_artifact(row)
        self.touch(latest["session_id"])
        return row

    def get_artifact(self, artifact_id: str,
                     version: int | None = None) -> dict[str, Any] | None:
        # an artifact is reached by its id alone (the panel, the export),
        # so the owner check rides on the chat it belongs to
        sql = (f"SELECT {_ARTIFACT_COLUMNS} FROM ChatArtifacts a "
               "WHERE a.ArtifactId = @a AND a.SessionId IN (SELECT SessionId FROM "
               "ChatSessions WHERE OwnerUserId = @owner)")
        params: dict[str, Any] = {"a": artifact_id, "owner": self.owner_user_id}
        if version is None:
            sql += " ORDER BY a.Version DESC LIMIT 1"
        else:
            sql += " AND a.Version = @v"
            params["v"] = int(version)
        rows = self.db.query(sql, params)
        return self._artifact_out(rows[0]) if rows else None

    def list_artifacts(self, session_id: str) -> list[dict[str, Any]]:
        """Latest version of each artifact in the session, in creation
        order, spec omitted (the panel fetches the one it shows)."""
        rows = self.db.query(
            "SELECT a.SessionId, a.ArtifactId, a.Version, a.TurnId, a.Type, a.Title, "
            "a.CreatedAt FROM ChatArtifacts a JOIN (SELECT ArtifactId, MAX(Version) AS V "
            "FROM ChatArtifacts WHERE SessionId = @id GROUP BY ArtifactId) m "
            "ON a.ArtifactId = m.ArtifactId AND a.Version = m.V "
            "WHERE a.SessionId = @id ORDER BY a.CreatedAt, a.ArtifactId",
            {"id": session_id})
        return [self._artifact_out(r) for r in rows]

    def artifact_versions(self, artifact_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT Version, Title, TurnId, CreatedAt FROM ChatArtifacts "
            "WHERE ArtifactId = @a AND SessionId IN (SELECT SessionId FROM ChatSessions "
            "WHERE OwnerUserId = @owner) ORDER BY Version",
            {"a": artifact_id, "owner": self.owner_user_id})
        return [{"version": int(r["Version"]), "title": str(r.get("Title") or ""),
                 "turn_id": str(r.get("TurnId") or ""), "created_at": _iso(r.get("CreatedAt"))}
                for r in rows]


def open_assistant_store(settings: Any, owner_user_id: str, *,
                         database: Database | None = None) -> SpannerAssistantStore:
    """The chat store the settings describe: Spanner, or the sqlite
    stand-in, for one owner. Pass the identity store's database to share
    one connection (the app does)."""
    from sahs.identity.database import open_database
    return SpannerAssistantStore(database if database is not None
                                 else open_database(settings), owner_user_id)


__all__ = ["CHAT_SQLITE_SCHEMA", "SESSION_KINDS", "SpannerAssistantStore",
           "open_assistant_store"]
