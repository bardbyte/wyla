"""The assistant's store (Synapse v2 §5/§8): the ask store plus
artifacts — standalone, versioned outputs the user keeps — and the
§8 organization: projects (a folder with its own instructions and
pinned skills), stars, archive, user memory, and the handoff.

Versions are append-only rows keyed (artifact_id, version): an edit
is a new version, never an overwrite, so "what did the dashboard say
on Tuesday" stays answerable.

Memory is scoped, statused, and never silently gone: an entry is
``global`` or ``project:<id>``, retiring sets ``status=retired``
instead of deleting, and everything active is disclosed in the
system prompt. Memory holds preferences and disambiguation choices —
never metric definitions or numbers; those belong in the graph,
through the steward's door.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from sahs.ask.events import now_iso
from sahs.ask.store import SessionStore

ARTIFACTS_SQL = """
CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  turn_id TEXT NOT NULL DEFAULT '',
  type TEXT NOT NULL,                 -- chart | table | document | …
  title TEXT NOT NULL DEFAULT '',
  spec TEXT NOT NULL,                 -- JSON, validated before store
  created_at TEXT NOT NULL,
  PRIMARY KEY (artifact_id, version)
);
CREATE INDEX IF NOT EXISTS artifacts_session
  ON artifacts(session_id, created_at);
CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  instructions TEXT NOT NULL DEFAULT '',
  skills TEXT NOT NULL DEFAULT '[]',  -- pinned pack names, JSON
  archived INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  text TEXT NOT NULL,
  scope TEXT NOT NULL DEFAULT 'global',   -- global | project:<id>
  status TEXT NOT NULL DEFAULT 'active',  -- active | retired
  source TEXT NOT NULL DEFAULT 'assistant',
  created_at TEXT NOT NULL
);
"""

# sessions grew organization columns in §13.5; older stores migrate
# forward on open, one ALTER at a time
_SESSION_COLUMNS = (
    ("project_id", "TEXT NOT NULL DEFAULT ''"),
    ("starred", "INTEGER NOT NULL DEFAULT 0"),
    ("archived", "INTEGER NOT NULL DEFAULT 0"),
    ("handoff", "TEXT NOT NULL DEFAULT ''"),   # JSON or ''
    ("notes", "TEXT NOT NULL DEFAULT '[]'"),   # working notes, JSON
    ("model", "TEXT NOT NULL DEFAULT ''"),     # the plane this chat
                                               # rides: vertex | gateway |
                                               # '' (the .env default)
    # the chat's usage, added to by the runtime when a turn ends
    # (009_usage.sql on Spanner): tokens in and out, model calls, the
    # wall time, the turns — the sidebar's "8.2K tokens · 3 turns"
    ("tokens_in", "INTEGER NOT NULL DEFAULT 0"),
    ("tokens_out", "INTEGER NOT NULL DEFAULT 0"),
    ("model_calls", "INTEGER NOT NULL DEFAULT 0"),
    ("elapsed_ms", "INTEGER NOT NULL DEFAULT 0"),
    ("turns", "INTEGER NOT NULL DEFAULT 0"),
)

# what a session carries of its usage, in the shape both stores hand
# the page (the Spanner store's columns are the same names in
# CamelCase); ``tokens`` is the sum, computed on read
USAGE_FIELDS = ("tokens_in", "tokens_out", "model_calls", "elapsed_ms",
                "turns")


def usage_shape(row: dict[str, Any]) -> dict[str, int]:
    """The usage fields of a session row, as integers, ``tokens``
    summed; a row from before the columns reads as zero."""
    out = {}
    for name in USAGE_FIELDS:
        try:
            out[name] = int(row.get(name) or 0)
        except (TypeError, ValueError):
            out[name] = 0
    out["tokens"] = out["tokens_in"] + out["tokens_out"]
    return out


def usage_of(record: dict[str, Any]) -> dict[str, Any]:
    """A turn's usage from its ``turn_done`` record: the turn's own
    token split (``Budget.tick``), the loop's ``model_calls`` (the
    budget's ``turn_calls`` when the record has none), the wall time,
    and the cost only when a rate is configured. The same shape rides
    the final assistant message's payload as ``usage``."""
    def num(key: str, default: int = 0) -> int:
        try:
            return int(float(record.get(key) or default))
        except (TypeError, ValueError):
            return default
    calls = num("model_calls") if record.get("model_calls") is not None \
        else num("turn_calls")
    cost = record.get("turn_cost_usd")
    return {"tokens_in": num("turn_tokens_in"),
            "tokens_out": num("turn_tokens_out"),
            "tokens": num("turn_tokens_in") + num("turn_tokens_out"),
            "calls": calls, "elapsed_ms": num("elapsed_ms"),
            "cost_usd": (float(cost) if isinstance(cost, (int, float))
                         else None)}


class AssistantStore(SessionStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        with self._conn() as conn:
            conn.executescript(ARTIFACTS_SQL)
            have = {r[1] for r in conn.execute(
                "PRAGMA table_info(sessions)")}
            for name, decl in _SESSION_COLUMNS:
                if name not in have:
                    conn.execute("ALTER TABLE sessions ADD COLUMN "
                                 f"{name} {decl}")

    # ── §8 session shape ─────────────────────────────────────
    # the base store dispatches through self._session_out, so this
    # override shapes every read path (get, list, create) once
    @staticmethod
    def _session_out(row: dict[str, Any]) -> dict[str, Any]:
        out = SessionStore._session_out(row)
        handoff = out.get("handoff")
        if isinstance(handoff, str):
            try:
                handoff = json.loads(handoff) if handoff else None
            except ValueError:
                handoff = None
        out["handoff"] = handoff if isinstance(handoff, dict) else None
        notes = out.get("notes")
        if isinstance(notes, str):
            try:
                notes = json.loads(notes or "[]")
            except ValueError:
                notes = []
        out["notes"] = list(notes) if isinstance(notes, list) else []
        out["starred"] = bool(out.get("starred"))
        out["archived"] = bool(out.get("archived"))
        out["model"] = str(out.get("model") or "").strip().lower()
        out.update(usage_shape(out))
        return out

    # ── usage: what a turn cost, added to the chat's row ─────
    def add_usage(self, session_id: str, tokens_in: int = 0,
                  tokens_out: int = 0, calls: int = 0,
                  elapsed_ms: int | float = 0) -> None:
        """One finished turn onto the chat's totals: tokens in and
        out, model calls, wall time, and one more turn. Sub-turns of
        a multi-task turn never call this — the parent's turn_done
        carries their sum, so they count once."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE sessions SET tokens_in = tokens_in + ?, "
                "tokens_out = tokens_out + ?, model_calls = model_calls + ?, "
                "elapsed_ms = elapsed_ms + ?, turns = turns + 1 WHERE id=?",
                (max(0, int(tokens_in)), max(0, int(tokens_out)),
                 max(0, int(calls)), max(0, int(round(float(elapsed_ms)))),
                 session_id))

    def set_message_usage(self, session_id: str, turn_id: str,
                          usage: dict[str, Any]) -> bool:
        """The turn's usage onto the payload of its final assistant
        message (the last one of that turn), so the transcript replays
        the footer the stream drew. False when the turn stored no
        assistant message (an error before anything was said)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id, payload FROM messages WHERE session_id=? AND "
                "turn_id=? AND role='assistant' ORDER BY created_at DESC, "
                "rowid DESC LIMIT 1", (session_id, turn_id)).fetchone()
            if row is None:
                return False
            try:
                payload = json.loads(row["payload"]) if row["payload"] \
                    else {}
            except ValueError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {"payload": payload}
            payload["usage"] = dict(usage)
            conn.execute("UPDATE messages SET payload=? WHERE id=?",
                         (json.dumps(payload), row["id"]))
        return True

    def usage_totals(self) -> dict[str, int]:
        """Every chat's usage summed: the one person's totals under
        the single-developer store (the People page's row)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(tokens_in), 0) AS tokens_in, "
                "COALESCE(SUM(tokens_out), 0) AS tokens_out, "
                "COALESCE(SUM(model_calls), 0) AS model_calls, "
                "COALESCE(SUM(elapsed_ms), 0) AS elapsed_ms, "
                "COALESCE(SUM(turns), 0) AS turns, COUNT(*) AS chats "
                "FROM sessions").fetchone()
        out = usage_shape(dict(row))
        out["chats"] = int(row["chats"] or 0)
        return out

    def set_flag(self, session_id: str, flag: str, on: bool) -> None:
        assert flag in ("starred", "archived")
        with self._conn() as conn:
            conn.execute(f"UPDATE sessions SET {flag}=?, updated_at=?"
                         " WHERE id=?",
                         (1 if on else 0, now_iso(), session_id))

    def set_model(self, session_id: str, plane: str) -> None:
        """The plane a chat rides from its next message on (the
        composer's model switch); '' goes back to the .env default."""
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET model=?, updated_at=? "
                         "WHERE id=?",
                         ((plane or "").strip().lower(), now_iso(),
                          session_id))

    def set_project(self, session_id: str, project_id: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET project_id=?, "
                         "updated_at=? WHERE id=?",
                         (project_id, now_iso(), session_id))

    def set_handoff(self, session_id: str,
                    handoff: dict[str, Any]) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET handoff=? WHERE id=?",
                         (json.dumps(handoff), session_id))

    def set_notes(self, session_id: str, notes: list[str]) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE sessions SET notes=? WHERE id=?",
                         (json.dumps(list(notes)[-8:]), session_id))

    # ── projects: a folder with its own context ──────────────
    def create_project(self, name: str, *, instructions: str = "",
                       skills: list[str] | None = None
                       ) -> dict[str, Any]:
        row = {"id": f"p_{uuid.uuid4().hex[:10]}",
               "name": name.strip()[:80],
               "instructions": instructions[:4000],
               "skills": json.dumps(list(skills or [])[:4]),
               "archived": 0,
               "created_at": now_iso(), "updated_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO projects (id,name,instructions,skills,"
                "archived,created_at,updated_at) VALUES (:id,:name,"
                ":instructions,:skills,:archived,:created_at,"
                ":updated_at)", row)
        return self._project_out(row)

    def update_project(self, project_id: str,
                       **fields: Any) -> dict[str, Any] | None:
        current = self.get_project(project_id)
        if current is None:
            return None
        sets, values = [], []
        for key in ("name", "instructions"):
            if fields.get(key) is not None:
                sets.append(f"{key}=?")
                values.append(str(fields[key])[:4000])
        if fields.get("skills") is not None:
            sets.append("skills=?")
            values.append(json.dumps(list(fields["skills"])[:4]))
        if fields.get("archived") is not None:
            sets.append("archived=?")
            values.append(1 if fields["archived"] else 0)
        if sets:
            with self._conn() as conn:
                conn.execute("UPDATE projects SET "
                             + ", ".join(sets + ["updated_at=?"])
                             + " WHERE id=?",
                             (*values, now_iso(), project_id))
        return self.get_project(project_id)

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM projects WHERE id=?",
                               (project_id,)).fetchone()
        return self._project_out(dict(row)) if row else None

    def list_projects(self, include_archived: bool = False
                      ) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM projects"
                + ("" if include_archived else " WHERE archived=0")
                + " ORDER BY created_at").fetchall()
        return [self._project_out(dict(r)) for r in rows]

    @staticmethod
    def _project_out(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        try:
            out["skills"] = list(json.loads(out.get("skills") or "[]"))
        except (TypeError, ValueError):
            out["skills"] = []
        out["archived"] = bool(out.get("archived"))
        return out

    # ── memory: scoped, statused, never silently gone ────────
    def add_memory(self, text: str, *, scope: str = "global",
                   source: str = "assistant") -> dict[str, Any]:
        row = {"id": f"m_{uuid.uuid4().hex[:10]}",
               "text": text.strip()[:400], "scope": scope,
               "status": "active", "source": source,
               "created_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO memories (id,text,scope,status,source,"
                "created_at) VALUES (:id,:text,:scope,:status,"
                ":source,:created_at)", row)
        return dict(row)

    def retire_memory(self, memory_id: str) -> bool:
        with self._conn() as conn:
            hit = conn.execute(
                "UPDATE memories SET status='retired' WHERE id=? AND "
                "status='active'", (memory_id,))
        return hit.rowcount > 0

    def list_memories(self, *, project_id: str = "",
                      status: str = "active") -> list[dict[str, Any]]:
        scopes = ["global"]
        if project_id:
            scopes.append(f"project:{project_id}")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM memories WHERE status=? AND scope IN ("
                + ",".join("?" * len(scopes))
                + ") ORDER BY created_at", (status, *scopes)).fetchall()
        return [dict(r) for r in rows]

    # ── artifacts ────────────────────────────────────────────
    def add_artifact(self, session_id: str, *, turn_id: str,
                     type: str, title: str,
                     spec: dict[str, Any]) -> dict[str, Any]:
        artifact_id = f"a_{uuid.uuid4().hex[:12]}"
        row = {"artifact_id": artifact_id, "version": 1,
               "session_id": session_id, "turn_id": turn_id,
               "type": type, "title": title[:120],
               "spec": json.dumps(spec), "created_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO artifacts (artifact_id,version,session_id,"
                "turn_id,type,title,spec,created_at) VALUES "
                "(:artifact_id,:version,:session_id,:turn_id,:type,"
                ":title,:spec,:created_at)", row)
        self.touch(session_id)
        return self._artifact_out(row)

    def update_artifact(self, artifact_id: str, *, turn_id: str,
                        spec: dict[str, Any],
                        title: str | None = None
                        ) -> dict[str, Any] | None:
        latest = self.get_artifact(artifact_id)
        if latest is None:
            return None
        row = {"artifact_id": artifact_id,
               "version": latest["version"] + 1,
               "session_id": latest["session_id"], "turn_id": turn_id,
               "type": latest["type"],
               "title": (title if title is not None
                         else latest["title"])[:120],
               "spec": json.dumps(spec), "created_at": now_iso()}
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO artifacts (artifact_id,version,session_id,"
                "turn_id,type,title,spec,created_at) VALUES "
                "(:artifact_id,:version,:session_id,:turn_id,:type,"
                ":title,:spec,:created_at)", row)
        self.touch(latest["session_id"])
        return self._artifact_out(row)

    def get_artifact(self, artifact_id: str,
                     version: int | None = None) -> dict[str, Any] | None:
        with self._conn() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT * FROM artifacts WHERE artifact_id=? "
                    "ORDER BY version DESC LIMIT 1",
                    (artifact_id,)).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM artifacts WHERE artifact_id=? AND "
                    "version=?", (artifact_id, version)).fetchone()
        return self._artifact_out(dict(row)) if row else None

    def list_artifacts(self, session_id: str) -> list[dict[str, Any]]:
        """Latest version of each artifact in the session, in creation
        order, spec omitted (the panel fetches the one it shows)."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT a.* FROM artifacts a JOIN (SELECT artifact_id,"
                " MAX(version) AS v FROM artifacts WHERE session_id=?"
                " GROUP BY artifact_id) m ON a.artifact_id=m.artifact_id"
                " AND a.version=m.v ORDER BY a.created_at, a.rowid",
                (session_id,)).fetchall()
        out = []
        for r in rows:
            item = self._artifact_out(dict(r))
            item.pop("spec", None)
            out.append(item)
        return out

    def artifact_versions(self, artifact_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT version, title, turn_id, created_at FROM "
                "artifacts WHERE artifact_id=? ORDER BY version",
                (artifact_id,)).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _artifact_out(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        if isinstance(out.get("spec"), str):
            out["spec"] = json.loads(out["spec"])
        out.pop("rowid", None)
        return out
