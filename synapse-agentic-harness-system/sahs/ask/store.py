"""Session store for Ask (E18): sessions, messages, plan versions,
feedback — SQLite, stdlib only, one connection per operation (WAL, so
the turn thread and the request handlers never fight).

The stateful spine of a session is the PLAN VERSION CHAIN, not the
transcript: every executed plan is stored with its parent so a
mutation ("same for Canada") is a diff, restore is a new version, and
the accrual counters in Stage E have something real to count.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from .events import now_iso

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,                 -- analyst | steward (two hats)
  title TEXT NOT NULL DEFAULT '',
  build_id TEXT NOT NULL DEFAULT '',
  actor TEXT NOT NULL DEFAULT 'admin',
  skills TEXT NOT NULL DEFAULT '[]',  -- JSON: loaded skill names
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  turn_id TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL,                 -- user | assistant | choice
  text TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL DEFAULT '',   -- JSON: answer_payload, chips…
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_session
  ON messages(session_id, created_at);
CREATE TABLE IF NOT EXISTS plan_versions (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  parent INTEGER,
  turn_id TEXT NOT NULL DEFAULT '',
  plan TEXT NOT NULL,                 -- JSON
  summary TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS plans_session
  ON plan_versions(session_id, version);
CREATE TABLE IF NOT EXISTS feedback (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  turn_id TEXT NOT NULL DEFAULT '',
  subject TEXT NOT NULL,              -- answer | verdict | chip
  vote TEXT NOT NULL,                 -- up | down
  note TEXT NOT NULL DEFAULT '',
  actor TEXT NOT NULL DEFAULT 'admin',
  created_at TEXT NOT NULL
);
"""


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class SessionStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)
            # forward migration for stores created before skills:
            # ALTER is idempotent-by-catch, the default keeps every
            # old session honest ("nothing loaded")
            try:
                conn.execute("ALTER TABLE sessions ADD COLUMN skills "
                             "TEXT NOT NULL DEFAULT '[]'")
            except sqlite3.OperationalError:
                pass                      # column already exists

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ── sessions ─────────────────────────────────────────────
    def create_session(self, kind: str = "analyst", *, build_id: str = "",
                       actor: str = "admin", title: str = "") -> dict:
        if kind not in ("analyst", "steward"):
            raise ValueError("kind is analyst or steward (the two hats)")
        row = {"id": _new_id("s"), "kind": kind, "title": title,
               "build_id": build_id, "actor": actor, "skills": "[]",
               "created_at": now_iso(), "updated_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO sessions (id,kind,title,build_id,actor,"
                "skills,created_at,updated_at) VALUES (:id,:kind,"
                ":title,:build_id,:actor,:skills,:created_at,"
                ":updated_at)", row)
        return self._session_out(row)

    @staticmethod
    def _session_out(row: dict) -> dict:
        out = dict(row)
        try:
            out["skills"] = list(json.loads(out.get("skills") or "[]"))
        except (TypeError, ValueError):
            out["skills"] = []
        return out

    def get_session(self, session_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE id=?",
                               (session_id,)).fetchone()
        return self._session_out(dict(row)) if row else None

    def list_sessions(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM sessions ORDER BY updated_at DESC LIMIT ?",
                (limit,)).fetchall()
        return [self._session_out(dict(r)) for r in rows]

    def set_skills(self, session_id: str, names: list[str]) -> None:
        """The session's loaded skills, replaced whole: selection is
        explicit and visible, never additive by accident."""
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET skills=?, updated_at=? "
                         "WHERE id=?",
                         (json.dumps(list(names)), now_iso(),
                          session_id))

    def set_title(self, session_id: str, title: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET title=?, updated_at=? "
                         "WHERE id=?", (title[:120], now_iso(), session_id))

    def touch(self, session_id: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET updated_at=? WHERE id=?",
                         (now_iso(), session_id))

    # ── messages ─────────────────────────────────────────────
    def add_message(self, session_id: str, role: str, text: str = "", *,
                    turn_id: str = "", payload: Any = None) -> dict:
        row = {"id": _new_id("m"), "session_id": session_id,
               "turn_id": turn_id, "role": role, "text": text,
               "payload": json.dumps(payload) if payload is not None else "",
               "created_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO messages (id,session_id,turn_id,role,text,"
                "payload,created_at) VALUES (:id,:session_id,:turn_id,:role,"
                ":text,:payload,:created_at)", row)
        self.touch(session_id)
        return row

    def messages(self, session_id: str) -> list[dict]:
        with self._conn() as conn:
            # rowid breaks same-second ties in INSERTION order; the
            # random message id would shuffle a fast turn's transcript
            rows = conn.execute(
                "SELECT * FROM messages WHERE session_id=? "
                "ORDER BY created_at, rowid", (session_id,)).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["payload"] = json.loads(item["payload"]) \
                if item["payload"] else None
            out.append(item)
        return out

    # ── plan versions (the stateful spine) ───────────────────
    def add_plan_version(self, session_id: str, plan: dict, *,
                         parent: int | None = None, turn_id: str = "",
                         summary: str = "") -> dict:
        with self._conn() as conn:
            top = conn.execute(
                "SELECT MAX(version) AS v FROM plan_versions "
                "WHERE session_id=?", (session_id,)).fetchone()["v"]
            version = (top or 0) + 1
            row = {"id": _new_id("pv"), "session_id": session_id,
                   "version": version, "parent": parent, "turn_id": turn_id,
                   "plan": json.dumps(plan), "summary": summary,
                   "created_at": now_iso()}
            conn.execute(
                "INSERT INTO plan_versions (id,session_id,version,parent,"
                "turn_id,plan,summary,created_at) VALUES (:id,:session_id,"
                ":version,:parent,:turn_id,:plan,:summary,:created_at)", row)
        row["plan"] = plan
        return row

    def plan_versions(self, session_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM plan_versions WHERE session_id=? "
                "ORDER BY version", (session_id,)).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["plan"] = json.loads(item["plan"])
            out.append(item)
        return out

    def latest_plan(self, session_id: str) -> dict | None:
        versions = self.plan_versions(session_id)
        return versions[-1] if versions else None

    # ── feedback (the flywheel's raw input) ──────────────────
    def add_feedback(self, session_id: str, subject: str, vote: str, *,
                     turn_id: str = "", note: str = "",
                     actor: str = "admin") -> dict:
        if vote not in ("up", "down"):
            raise ValueError("vote is up or down")
        row = {"id": _new_id("fb"), "session_id": session_id,
               "turn_id": turn_id, "subject": subject, "vote": vote,
               "note": note, "actor": actor, "created_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO feedback (id,session_id,turn_id,subject,vote,"
                "note,actor,created_at) VALUES (:id,:session_id,:turn_id,"
                ":subject,:vote,:note,:actor,:created_at)", row)
        return row

    def feedback(self, session_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM feedback WHERE session_id=? "
                "ORDER BY created_at", (session_id,)).fetchall()
        return [dict(r) for r in rows]
