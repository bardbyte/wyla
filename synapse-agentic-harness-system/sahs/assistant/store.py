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
)


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
        return out

    def set_flag(self, session_id: str, flag: str, on: bool) -> None:
        assert flag in ("starred", "archived")
        with self._conn() as conn:
            conn.execute(f"UPDATE sessions SET {flag}=?, updated_at=?"
                         " WHERE id=?",
                         (1 if on else 0, now_iso(), session_id))

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
