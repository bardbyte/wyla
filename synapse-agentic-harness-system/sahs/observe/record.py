"""The record, read back: sessions, events, messages and feedback from
the chat tables (``db/spanner/002_chat.sql``) through the assistant
store, grouped into turns.

Langfuse is a mirror of the record, never a source. Everything the
mirror shows is rebuilt from here: ``backfill`` replays each turn's
records through the same tracer the live turn used, the dataset
builders (``datasets.py``) read the turns this module groups, and the
coverage check (``coverage.py``) counts what the rows can and cannot
supply. The reader speaks to one ``Database`` object (Spanner, or the
sqlite stand-in under ``SAHS_STORE=sqlite``) and goes through
``SpannerAssistantStore`` for every per-session read, so the owner
filter the store applies is the one the app applies.

Mind the window: ``ChatEvents`` rows are deleted 90 days after their
``Ts`` (the row deletion policy in the DDL). A backfill reaches only
what is still there; ``ChatSessions``, ``ChatMessages`` and
``ChatFeedback`` keep no such policy, so a session older than the
window still has its messages and votes but no trace to hang them on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from sahs.assistant.spanner_store import SpannerAssistantStore, _iso, _list
from sahs.identity.database import Database

from .tracer import TurnTracer, feedback_score, replay_records

EVENTS_WINDOW_DAYS = 90
# turn_done statuses that count as a finished answer (a task's own
# status is ``done``; the plain turn's are the loop's)
DONE_STATUSES = frozenset({"answered", "proposed", "clarify", "done"})
_SESSION_COLUMNS = ("SessionId, OwnerUserId, Kind, Title, BuildId, ProjectId, "
                    "Model, Skills, MessageCount, CreatedAt, UpdatedAt")


def parse_since(value: str | datetime | None) -> datetime | None:
    """An ISO instant (date or datetime, Z or offset) as an aware UTC
    datetime; None or '' → None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        stamp = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if len(text) == 10:
            text += "T00:00:00+00:00"
        stamp = datetime.fromisoformat(text)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


class RecordReader:
    """Reads the record for every owner (a backfill spans people), one
    store per owner for the per-session reads."""

    def __init__(self, database: Database) -> None:
        self.db = database
        self._stores: dict[str, SpannerAssistantStore] = {}

    def store_for(self, owner: str) -> SpannerAssistantStore:
        store = self._stores.get(owner)
        if store is None:
            store = self._stores[owner] = SpannerAssistantStore(self.db, owner)
        return store

    def sessions(self, *, since: str | datetime | None = None,
                 session_id: str = "", owner: str = "",
                 limit: int = 10000) -> list[dict[str, Any]]:
        """The sessions to read, oldest first: filtered by owner, by
        id, and by ``UpdatedAt >= since``."""
        conditions: list[str] = []
        params: dict[str, Any] = {"limit": int(limit)}
        if owner:
            conditions.append("OwnerUserId = @owner")
            params["owner"] = owner
        if session_id:
            conditions.append("SessionId = @sid")
            params["sid"] = session_id
        stamp = parse_since(since)
        if stamp is not None:
            conditions.append("UpdatedAt >= @since")
            params["since"] = stamp
        sql = f"SELECT {_SESSION_COLUMNS} FROM ChatSessions"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY UpdatedAt, SessionId LIMIT @limit"
        return [self._session(r) for r in self.db.query(sql, params)]

    @staticmethod
    def _session(row: dict[str, Any]) -> dict[str, Any]:
        return {"id": str(row["SessionId"]),
                "owner": str(row.get("OwnerUserId") or ""),
                "kind": str(row.get("Kind") or ""),
                "title": str(row.get("Title") or ""),
                "build_id": str(row.get("BuildId") or ""),
                "project_id": str(row.get("ProjectId") or ""),
                "model": str(row.get("Model") or ""),
                "skills": _list(row.get("Skills")),
                "messages": int(row.get("MessageCount") or 0),
                "created_at": _iso(row.get("CreatedAt")),
                "updated_at": _iso(row.get("UpdatedAt"))}

    def events(self, session: dict[str, Any],
               page: int = 4000) -> list[dict[str, Any]]:
        """Every record of the session, in seq order, paged through the
        store's ``events``."""
        store = self.store_for(session["owner"])
        out: list[dict[str, Any]] = []
        after = 0
        while True:
            batch = store.events(session["id"], after_seq=after, limit=page)
            out.extend(batch)
            if len(batch) < page:
                return out
            after = int(batch[-1]["seq"])

    def messages(self, session: dict[str, Any]) -> list[dict[str, Any]]:
        return self.store_for(session["owner"]).messages(session["id"])

    def feedback(self, session: dict[str, Any]) -> list[dict[str, Any]]:
        return self.store_for(session["owner"]).feedback(session["id"])

    def turns(self, session: dict[str, Any]) -> list[dict[str, Any]]:
        return turns_of(session, self.events(session), self.messages(session),
                        self.feedback(session))

    def counts(self) -> dict[str, int]:
        """Row counts the coverage table starts from."""
        out: dict[str, int] = {}
        for table in ("ChatSessions", "ChatEvents", "ChatMessages",
                      "ChatFeedback", "ChatArtifacts"):
            rows = self.db.query(f"SELECT COUNT(*) AS N FROM {table}")
            out[table] = int((rows[0]["N"] if rows else 0) or 0)
        rows = self.db.query(
            "SELECT COUNT(DISTINCT OwnerUserId) AS N FROM ChatSessions")
        out["owners"] = int((rows[0]["N"] if rows else 0) or 0)
        return out


# ── grouping the record into turns ───────────────────────────


def _blank_turn(session: dict[str, Any], turn_id: str) -> dict[str, Any]:
    return {"session_id": session["id"], "turn_id": turn_id,
            "owner": session.get("owner", ""),
            "build_id": session.get("build_id", ""),
            "plane": session.get("model", ""),
            "kind": session.get("kind", ""),
            "question": "", "status": "", "started_at": "", "done_at": "",
            "skills": [], "skills_loaded": [], "skill_modes": {},
            "prompt_version": "", "prompt_parts": {},
            "model_calls": 0, "steps": 0, "elapsed_ms": 0.0,
            "tokens": {}, "refused": [], "checks": 0,
            "answer": "", "sql": "", "proposal": None,
            "artifacts": [], "plan": None, "tasks": [],
            "feedback": [], "message_id": "", "records": 0}


def turns_of(session: dict[str, Any], events: Iterable[dict[str, Any]],
             messages: Iterable[dict[str, Any]] = (),
             feedback: Iterable[dict[str, Any]] = ()) -> list[dict[str, Any]]:
    """The session's top-level turns, in order, each read off its
    records: the question, the status, what loaded, what was refused,
    the prompt version, the answer, the proposal's SQL (from the
    message payload first, the proposal event second), the plan and
    its tasks, and the votes on it. A pure function of the rows."""
    turns: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for record in events:
        turn_id = str(record.get("turn_id") or "")
        if not turn_id:
            continue
        parent_id = turn_id.split(".", 1)[0]
        turn = turns.get(parent_id)
        if turn is None:
            turn = turns[parent_id] = _blank_turn(session, parent_id)
            order.append(parent_id)
        turn["records"] += 1
        ev = record.get("ev")
        sub = turn_id != parent_id
        if ev == "turn_started":
            if sub:
                continue
            turn["question"] = str(record.get("text") or "")
            turn["started_at"] = str(record.get("ts") or "")
            turn["build_id"] = str(record.get("build_id") or turn["build_id"])
            turn["plane"] = str(record.get("plane") or turn["plane"])
            turn["skills"] = list(record.get("skills") or [])
            turn["version"] = str(record.get("version") or "")
            turn["mode"] = str(record.get("mode") or "")
            turn["thinking_level"] = str(record.get("thinking_level") or "")
        elif ev == "skills_loaded":
            loaded = list(record.get("skills_loaded") or [])
            turn["skills_loaded"] = list(dict.fromkeys(
                turn["skills_loaded"] + loaded))
            for r in record.get("skills") or []:
                if isinstance(r, dict) and r.get("skill_name"):
                    turn["skill_modes"][str(r["skill_name"])] = r.get("mode")
        elif ev == "model_prompt":
            if record.get("kind") == "system" and not sub:
                turn["prompt_version"] = str(record.get("prompt_version") or "")
                turn["prompt_parts"] = dict(record.get("prompt_parts") or {})
            elif record.get("kind") == "call":
                turn["model_calls"] += 1
        elif ev == "tool_step":
            turn["steps"] += 1
            summary = str(record.get("summary") or "")
            tool = str(record.get("tool") or "")
            if tool == "check":
                turn["checks"] += 1
            if summary.startswith("ERROR"):
                turn["refused"].append(
                    f"{tool}: {summary[6:].strip(': ')}"[:240])
        elif ev == "say_token" and not sub:
            turn["answer"] += str(record.get("delta") or "")
        elif ev == "proposal":
            proposal = record.get("proposal")
            if isinstance(proposal, dict):
                turn["proposal"] = proposal
                turn["sql"] = str(proposal.get("sql") or turn["sql"])
                turn["message_id"] = str(record.get("message_id")
                                         or turn["message_id"])
        elif ev == "artifact":
            turn["artifacts"].append({
                k: record.get(k) for k in ("artifact_id", "version",
                                           "type", "title")})
        elif ev == "plan_made":
            turn["plan"] = {k: record.get(k) for k in
                            ("tasks", "synthesis", "repairs", "pool", "waves")}
        elif ev == "task_done":
            turn["tasks"].append({
                k: record.get(k) for k in ("task", "status", "reason",
                                           "cost", "checked", "refused",
                                           "artifacts", "saved")})
        elif ev == "turn_done" and not sub:
            turn["status"] = str(record.get("status") or "")
            turn["done_at"] = str(record.get("ts") or "")
            turn["elapsed_ms"] = float(record.get("elapsed_ms") or 0)
            turn["tokens"] = {k: int(record.get(k) or 0)
                              for k in ("tokens_in", "tokens_out", "tokens")}
            for key in ("model_calls", "steps"):
                if record.get(key) is not None:
                    turn[key] = int(record[key])
    # the message payload is the answer the person kept: the proposal
    # there is the one on the card, the text there is the whole answer
    for message in messages:
        turn = turns.get(str(message.get("turn_id") or ""))
        if turn is None or message.get("role") != "assistant":
            continue
        payload = message.get("payload")
        if not isinstance(payload, dict):
            continue
        proposal = payload.get("proposal")
        if isinstance(proposal, dict):
            turn["proposal"] = proposal
            turn["sql"] = str(proposal.get("sql") or turn["sql"])
            turn["message_id"] = str(message.get("id") or "")
        if message.get("text") and not turn["answer"]:
            turn["answer"] = str(message["text"])
        if isinstance(payload.get("plan"), dict) and turn["plan"] is None:
            turn["plan"] = payload["plan"]
    for vote in feedback:
        turn = turns.get(str(vote.get("turn_id") or ""))
        if turn is not None:
            turn["feedback"].append({k: vote.get(k) for k in
                                     ("vote", "subject", "note", "actor",
                                      "created_at", "id")})
    return [turns[t] for t in order]


# ── the backfill ─────────────────────────────────────────────


def backfill(reader: RecordReader, tracer: TurnTracer, *,
             since: str | datetime | None = None, session_id: str = "",
             owner: str = "", log: Callable[[str], None] | None = None
             ) -> dict[str, int]:
    """Replay every session's records through the tracer, oldest
    first, then score the votes on the replayed turns. The user of
    each trace is the session's owner (``ChatSessions.OwnerUserId``).
    ``since`` keeps the sessions touched since then and, within them,
    the turns started since then. Idempotent: the tracer's ids are a
    function of the record."""
    stamp = parse_since(since)
    stats = {"sessions": 0, "turns": 0, "records": 0, "feedback": 0,
             "skipped_turns": 0}
    for session in reader.sessions(since=stamp, session_id=session_id,
                                   owner=owner):
        records = reader.events(session)
        keep: set[str] = set()
        skipped = 0
        for record in records:
            if record.get("ev") != "turn_started" \
                    or "." in str(record.get("turn_id") or ""):
                continue
            started = parse_since(record.get("ts")) if record.get("ts") \
                else None
            if stamp is not None and started is not None and started < stamp:
                skipped += 1
                continue
            keep.add(str(record.get("turn_id") or ""))
        chosen = [r for r in records
                  if str(r.get("turn_id") or "").split(".", 1)[0] in keep]
        if not chosen:
            stats["skipped_turns"] += skipped
            continue
        tracer.user_id = session["owner"]
        n = replay_records(chosen, tracer)
        votes = 0
        for vote in reader.feedback(session):
            if vote.get("turn_id") in keep:
                feedback_score(tracer.emitter, session["id"],
                               str(vote["turn_id"]), str(vote.get("vote")),
                               subject=str(vote.get("subject") or ""),
                               note=str(vote.get("note") or ""))
                votes += 1
        stats["sessions"] += 1
        stats["turns"] += len(keep)
        stats["records"] += n
        stats["feedback"] += votes
        stats["skipped_turns"] += skipped
        if log is not None:
            log(f"{session['id']:40} {len(keep):>4} turns {n:>6} records "
                f"{votes:>3} votes")
    tracer.flush()
    return stats


__all__ = ["DONE_STATUSES", "EVENTS_WINDOW_DAYS", "RecordReader", "backfill",
           "parse_since", "turns_of"]
