"""The assistant's store (Synapse v2 §5/§8): the ask store plus
artifacts — standalone, versioned outputs the user keeps.

Versions are append-only rows keyed (artifact_id, version): an edit
is a new version, never an overwrite, so "what did the dashboard say
on Tuesday" stays answerable. Projects, stars, and memory arrive in
§13.5 on the same store.
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
"""


class AssistantStore(SessionStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        with self._conn() as conn:
            conn.executescript(ARTIFACTS_SQL)

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
