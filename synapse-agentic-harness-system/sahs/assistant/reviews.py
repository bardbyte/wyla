"""Submissions and their approval: the single-level workflow the PRD
asks for. A skill or a knowledge file a person brings in goes to
their manager before the agent may read a word of it.

  * **a submission** is one file under review: kind (skill or
    knowledge), a name, the title, a description and the intended
    purpose the person gave, the submitter, the approver the system
    assigned (the submitter's manager, band 40 or above — nobody
    picks their own approver), the version, the status;
  * **the statuses**: ``pending`` (Pending Manager Approval),
    ``published`` (approved: the file is where the agent reads it),
    ``rejected`` (the manager's comments shown; update and resubmit),
    ``withdrawn``. Pending and rejected files never reach the loader:
    their text lives here, under runs/reviews/, not on the shelf;
  * **the AI-assisted read**: on every submission the model reads the
    file and returns a content summary (executive summary, key
    business topics, intended purpose, suggested audience), review
    insights (ambiguous statements, inconsistent terminology,
    contradictory information, missing context, duplicate content,
    outdated references — each with a confidence, an explanation
    and a reference to the section) and an advisory recommendation
    (ready for approval, needs minor updates, requires significant
    revision). Advisory only: nothing here approves or rejects. The
    read runs in the background; until it lands, and whenever the
    model is unavailable, a local set of checks stands in and says
    so;
  * **the ledger** is append-only JSONL, one record per event
    (submitted, resubmitted, ai_review, approved, rejected,
    withdrawn); the current state of a submission is a fold, the
    same discipline as the graph. The notices the PRD lists are read
    off the same events.

Layout under ``<graph>/runs/reviews/``:

    ledger.jsonl            one JSON record per event
    files/<id>/v<n>.md      the text of every version
    seen.json               when each person last read their notices
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from sahs.ask.events import now_iso

KINDS = ("skill", "knowledge")
STATUSES = ("pending", "published", "rejected", "withdrawn")
STATUS_LABEL = {"pending": "Pending manager approval",
                "published": "Published", "rejected": "Rejected",
                "withdrawn": "Withdrawn"}
EVENTS = ("submitted", "resubmitted", "ai_review", "approved", "rejected",
          "withdrawn")
MIN_BAND = 40
RECOMMENDATIONS = ("ready", "minor", "significant")
RECOMMENDATION_LABEL = {"ready": "Ready for approval",
                        "minor": "Needs minor updates",
                        "significant": "Requires significant revision"}
CATEGORIES = ("ambiguous", "inconsistent_terminology", "contradictory",
              "missing_context", "duplicate", "outdated")
CATEGORY_LABEL = {"ambiguous": "Ambiguous statement",
                  "inconsistent_terminology": "Inconsistent terminology",
                  "contradictory": "Contradictory information",
                  "missing_context": "Missing context",
                  "duplicate": "Duplicate or repetitive content",
                  "outdated": "Potentially outdated reference"}
CONFIDENCES = ("high", "medium", "low")
MAX_SKILL_CHARS = 12_000
MAX_KNOWLEDGE_CHARS = 200_000
EXTS = ("md", "txt", "csv", "json", "yaml", "yml", "sql")
_BU = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


# ── the manager seam ─────────────────────────────────────────

@dataclass(frozen=True)
class Approver:
    name: str
    band: int
    self_review: bool = False     # no manager configured: the laptop
    note: str = ""

    @property
    def may_approve(self) -> bool:
        return self.band >= MIN_BAND


def approver_for(submitter: str,
                 env: dict[str, str] | None = None) -> Approver:
    """The submitter's direct manager, from the directory this
    deployment has. On the laptop that is the .env: SYNAPSE_USER_MANAGER
    names the manager and SYNAPSE_USER_MANAGER_BAND their band (40 when
    unset); without a manager the configured person reviews their own
    submissions, and the record says so. The signed-in directory
    replaces this function when identity lands."""
    env = dict(os.environ if env is None else env)

    def band_of(var: str) -> int:
        try:
            return int((env.get(var) or "").strip() or MIN_BAND)
        except ValueError:
            return MIN_BAND
    manager = (env.get("SYNAPSE_USER_MANAGER") or "").strip()
    if manager:
        return Approver(manager, band_of("SYNAPSE_USER_MANAGER_BAND"))
    return Approver((submitter or "").strip() or "you",
                    band_of("SYNAPSE_USER_BAND"), self_review=True,
                    note="no manager configured (SYNAPSE_USER_MANAGER in the "
                         "silo .env): on this laptop you review your own "
                         "submissions")


# ── the AI-assisted read ─────────────────────────────────────

REVIEW_SYSTEM = """\
You are the reviewer's assistant for files people upload to an
enterprise data assistant: a SKILL file is doctrine (the moves and
checks for a kind of question; it steers, it never states facts); a
KNOWLEDGE file is reference (definitions, tables, metrics, caveats,
owners). The manager decides; you only help them read.

Return ONE JSON object and nothing else:
{
  "summary": {
    "executive": "<three sentences at most: what this file is and what it would change for the assistant>",
    "topics": ["<key business topic>", "..."],
    "purpose": "<the intended purpose, in one sentence>",
    "audience": "<who this serves: which analysts, which questions>"
  },
  "insights": [
    {"category": "ambiguous | inconsistent_terminology | contradictory | missing_context | duplicate | outdated",
     "confidence": "high | medium | low",
     "explanation": "<what you found and why it matters to a reader>",
     "reference": "<the section heading or the quoted phrase it is in>"}
  ],
  "recommendation": "ready | minor | significant",
  "reason": "<one sentence for the recommendation>"
}

Rules: every insight cites the section or phrase it is about; report
nothing you cannot point at; an empty insights list is a valid answer
for a clean file; a skill that states facts about the data (numbers,
table names asserted as truth) is missing_context or contradictory
territory — say so; never recommend on tone. JSON only."""


def _norm_review(raw: Any, *, by: str) -> dict[str, Any] | None:
    """The model's answer held to the shape the page shows; None when
    it is not a review."""
    if not isinstance(raw, dict):
        return None
    summary = raw.get("summary") if isinstance(raw.get("summary"), dict) \
        else {}
    insights = []
    for row in raw.get("insights") or []:
        if not isinstance(row, dict):
            continue
        raw_cat = str(row.get("category") or "").strip().lower() \
            .replace(" ", "_")
        # the model may say it the PRD's way ("Ambiguous statements",
        # "Potentially outdated references"): the key it starts with,
        # or contains, is the category
        category = next((k for k in CATEGORIES
                         if raw_cat.startswith(k) or k in raw_cat),
                        "missing_context")
        confidence = str(row.get("confidence") or "medium").strip().lower()
        if confidence not in CONFIDENCES:
            confidence = "medium"
        explanation = str(row.get("explanation") or "").strip()
        if not explanation:
            continue
        insights.append({"category": category, "confidence": confidence,
                         "explanation": explanation[:600],
                         "reference": str(row.get("reference") or "")
                         .strip()[:200]})
    rec = str(raw.get("recommendation") or "").strip().lower()
    rec = {"ready for approval": "ready", "needs minor updates": "minor",
           "requires significant revision": "significant"}.get(rec, rec)
    if rec not in RECOMMENDATIONS:
        rec = "significant" if any(i["confidence"] == "high"
                                   for i in insights) \
            else "minor" if insights else "ready"
    topics = summary.get("topics") if isinstance(summary.get("topics"),
                                                  list) else []
    executive = str(summary.get("executive") or "").strip()
    if not executive and not insights:
        return None
    return {"by": by, "at": now_iso(),
            "summary": {"executive": executive[:1200],
                        "topics": [str(t).strip()[:80] for t in topics
                                   if str(t).strip()][:8],
                        "purpose": str(summary.get("purpose") or "")
                        .strip()[:400],
                        "audience": str(summary.get("audience") or "")
                        .strip()[:400]},
            "insights": insights[:12], "recommendation": rec,
            "reason": str(raw.get("reason") or "").strip()[:400]}


def ai_review(agent: Any, kind: str, title: str, text: str, *,
              purpose: str = "", by: str = "the model") -> dict[str, Any]:
    """The model's read of a submission, checked into shape. A
    missing or malformed answer is a reason, never an empty review."""
    user = (f"Kind: {kind}\nTitle: {title or 'untitled'}\n"
            f"Intended purpose, in the submitter's words: "
            f"{purpose.strip() or 'not stated'}\n\nThe file:\n<<<\n"
            f"{text[:MAX_KNOWLEDGE_CHARS]}\n>>>")
    answer = agent.json(user, system=REVIEW_SYSTEM, temperature=0.1,
                        max_tokens=4096)
    review = _norm_review(answer, by=by)
    if review is None:
        return {"ok": False, "reason": "the model returned no review"}
    return {"ok": True, "review": review}


_YEAR = re.compile(r"\b(19[89]\d|20[0-4]\d)\b")
_VAGUE = re.compile(r"\b(TODO|TBD|TBC|FIXME|XXX|etc\.?|as needed|as "
                    r"appropriate|and so on|somehow|maybe|probably)\b|\?\?",
                    re.I)


def checks_review(kind: str, title: str, text: str, *,
                  purpose: str = "", year: int | None = None
                  ) -> dict[str, Any]:
    """What can be checked without a model: the house format, vague
    words, repeated lines, old years, one term spelt two ways. It
    stands in until the model's read lands, and stays when the model
    is unavailable — labelled as checks, never as a read."""
    import datetime as _dt
    year = year or _dt.date.today().year
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    insights: list[dict[str, Any]] = []

    def add(category: str, confidence: str, explanation: str,
            reference: str = "") -> None:
        insights.append({"category": category, "confidence": confidence,
                         "explanation": explanation,
                         "reference": reference[:200]})
    heads = [ln.lstrip("#").strip() for ln in lines if ln.startswith("#")]
    body = [ln for ln in lines if ln.strip() and not ln.startswith("#")]
    if not any(ln.startswith("# ") for ln in lines):
        add("missing_context", "high",
            "no title heading: the shelf and the agent name a file by "
            "its first '# ' line", "the top of the file")
    if not body:
        add("missing_context", "high", "the file has a title and "
            "nothing under it", "the whole file")
    elif len(" ".join(body)) < 120:
        add("missing_context", "medium",
            "very little content: a reader cannot tell what it changes",
            "the whole file")
    if kind == "skill":
        if not any(h.lower().startswith("never") for h in heads):
            add("missing_context", "medium",
                "no '## Never' section: a skill states the rules it "
                "implies as prohibitions", "the sections")
        if not any(re.match(r"^\s*\d+\.\s", ln) for ln in lines):
            add("missing_context", "low",
                "no numbered steps: a move is steps naming the tool "
                "each implies", "the sections")
        if re.search(r"\b\d{1,3}(,\d{3})+(\.\d+)?\b|\$\s?\d", text or ""):
            add("contradictory", "medium",
                "a skill carries numbers: doctrine steers, it never "
                "asserts a figure — a figure belongs in a knowledge file "
                "with its source", "the lines with numbers")
    for i, ln in enumerate(lines):
        m = _VAGUE.search(ln)
        if m and not ln.startswith("#"):
            add("ambiguous", "medium",
                f"'{m.group(0)}' leaves the reader to guess",
                f"line {i + 1}: {ln.strip()[:80]}")
            if len(insights) > 10:
                break
    seen: dict[str, int] = {}
    for i, ln in enumerate(lines):
        key = re.sub(r"\s+", " ", ln.strip().lower())
        if len(key) >= 40:
            if key in seen:
                add("duplicate", "high",
                    f"the same line appears twice (lines {seen[key] + 1} "
                    f"and {i + 1})", ln.strip()[:80])
                break
            seen[key] = i
    old = sorted({int(y) for y in _YEAR.findall(text or "")
                  if int(y) <= year - 2})
    if old:
        add("outdated", "low" if old[-1] >= year - 3 else "medium",
            f"references {', '.join(str(y) for y in old[-3:])}: check the "
            "figures and rules still hold", "the dated lines")
    # one term spelt two ways: "card-member", "card member" and
    # "cardmember" share a joined form; two surfaces for one is a finding
    forms: dict[str, set[str]] = {}

    def note(norm: str, surface: str) -> None:
        if len(norm) >= 6:
            forms.setdefault(norm, set()).add(surface)
    words = re.findall(r"[A-Za-z]+(?:-[A-Za-z]+)*", text or "")
    for i, word in enumerate(words):
        low = word.lower()
        note(low.replace("-", ""), low)
        if "-" not in low and i + 1 < len(words) and "-" not in words[i + 1]:
            pair = f"{low} {words[i + 1].lower()}"
            note(pair.replace(" ", ""), pair)
    two_ways = [sorted(v) for v in forms.values() if len(v) > 1]
    for pair in two_ways[:2]:
        add("inconsistent_terminology", "low",
            "one term spelt two ways: " + " / ".join(f"'{p}'" for p in pair),
            "throughout")
    rec = ("significant" if any(i["confidence"] == "high" for i in insights)
           else "minor" if insights else "ready")
    executive = ""
    for ln in body:
        executive = ln.strip()[:300]
        break
    return {"by": "checks", "at": now_iso(),
            "summary": {"executive": executive,
                        "topics": [h[:80] for h in heads[1:7]],
                        "purpose": (purpose or "").strip()[:400],
                        "audience": ("analysts asking about "
                                     + (title or "this subject"))[:200]},
            "insights": insights[:12], "recommendation": rec,
            "reason": ("the checks found nothing to point at; the model's "
                       "read may still" if not insights else
                       f"{len(insights)} thing(s) the checks could point "
                       "at; the manager reads the file")}


# ── the ledger and the fold ──────────────────────────────────

def _slug(name: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower())
    return out.strip("-")[:40]


def _title_of(text: str, fallback: str) -> tuple[str, str]:
    """(title, description) the way the shelf reads a file."""
    title, description = fallback, ""
    for line in (text or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") and title == fallback:
            title = stripped.lstrip("#").strip() or fallback
            continue
        if stripped.startswith("<!--"):
            continue
        description = stripped[:160]
        break
    return title, description


class Reviews:
    """The workflow over one folder. Every mutation appends a record;
    every read folds the ledger (a few hundred records at most)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.ledger = self.root / "ledger.jsonl"
        self.files = self.root / "files"
        self.seen_path = self.root / "seen.json"
        self._lock = threading.Lock()
        self._threads: dict[str, threading.Thread] = {}

    # ── records ──
    def _append(self, record: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self._lock, self.ledger.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _records(self) -> list[dict[str, Any]]:
        if not self.ledger.exists():
            return []
        out = []
        for line in self.ledger.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out

    def _fold(self) -> dict[str, dict[str, Any]]:
        subs: dict[str, dict[str, Any]] = {}
        for rec in self._records():
            ev, sid = rec.get("ev"), rec.get("id")
            if not sid:
                continue
            if ev == "submitted":
                subs[sid] = {**{k: v for k, v in rec.items() if k != "ev"},
                             "status": "pending", "version": 1,
                             "submitted_at": rec["at"], "updated_at": rec["at"],
                             "decided_at": "", "decided_by": "",
                             "comments": [], "ai_review": None,
                             "ai_status": "running", "published_path": ""}
                continue
            sub = subs.get(sid)
            if sub is None:
                continue
            sub["updated_at"] = rec.get("at", sub["updated_at"])
            if ev == "resubmitted":
                sub.update(version=int(rec.get("version") or sub["version"] + 1),
                           status="pending", ai_review=None,
                           ai_status="running", decided_at="", decided_by="")
                for key in ("description", "purpose", "title"):
                    if rec.get(key):
                        sub[key] = rec[key]
                if rec.get("comment"):
                    sub["comments"].append({"at": rec["at"], "by": rec.get("by", ""),
                                            "event": "resubmitted",
                                            "text": rec["comment"]})
            elif ev == "ai_review":
                if rec.get("version") and rec["version"] != sub["version"]:
                    continue                 # a read of an older version
                sub["ai_review"] = rec.get("review")
                sub["ai_status"] = rec.get("status") or (
                    "done" if rec.get("review") else "failed")
                sub["ai_reason"] = rec.get("reason", "")
            elif ev in ("approved", "rejected"):
                sub.update(status="published" if ev == "approved" else "rejected",
                           decided_at=rec["at"], decided_by=rec.get("by", ""))
                if ev == "approved":
                    sub["published_path"] = rec.get("published_path", "")
                sub["comments"].append({"at": rec["at"], "by": rec.get("by", ""),
                                        "event": ev,
                                        "text": rec.get("comment", "")})
            elif ev == "withdrawn":
                sub["status"] = "withdrawn"
        return subs

    def _file(self, sid: str, version: int) -> Path:
        return self.files / sid / f"v{version}.md"

    def text_of(self, sid: str, version: int | None = None) -> str:
        sub = self._fold().get(sid)
        if sub is None:
            return ""
        path = self._file(sid, version or sub["version"])
        return path.read_text(encoding="utf-8") if path.exists() else ""

    # ── reads ──
    def list(self, *, with_text: bool = False) -> list[dict[str, Any]]:
        rows = []
        for sub in self._fold().values():
            row = dict(sub)
            row["status_label"] = STATUS_LABEL.get(row["status"], row["status"])
            review = row.get("ai_review")
            row["ai_summary"] = ({"recommendation": review["recommendation"],
                                  "recommendation_label": RECOMMENDATION_LABEL[
                                      review["recommendation"]],
                                  "by": review["by"],
                                  "insights": len(review.get("insights") or [])}
                                 if review else None)
            if not with_text:
                row.pop("ai_review", None)
            else:
                row["text"] = self.text_of(sub["id"])
            rows.append(row)
        rows.sort(key=lambda r: r["updated_at"], reverse=True)
        return rows

    def get(self, sid: str) -> dict[str, Any] | None:
        for row in self.list(with_text=True):
            if row["id"] == sid:
                return row
        return None

    def find(self, kind: str, name: str, submitter_slug: str
             ) -> dict[str, Any] | None:
        for sub in self._fold().values():
            if (sub["kind"] == kind and sub["name"] == name
                    and sub.get("submitter_slug") == submitter_slug
                    and sub["status"] != "withdrawn"):
                return sub
        return None

    def published_names(self, kind: str = "skill") -> set[str]:
        return {s["name"] for s in self._fold().values()
                if s["kind"] == kind and s["status"] == "published"}

    # ── writes ──
    def submit(self, *, kind: str, name: str, text: str, title: str = "",
               description: str = "", purpose: str = "",
               business_unit: str = "", ext: str = "md",
               submitter: str, submitter_slug: str, approver: Approver,
               reserved: set[str] | frozenset[str] = frozenset()
               ) -> dict[str, Any]:
        """A new submission, or a new version of the one this person
        already has under that name (pending, rejected or published):
        the same door either way, status pending, the read started by
        the caller."""
        if kind not in KINDS:
            return {"ok": False, "reason": "kind is skill or knowledge"}
        text = (text or "").strip() + "\n"
        cap = MAX_SKILL_CHARS if kind == "skill" else MAX_KNOWLEDGE_CHARS
        if len(text.strip()) < 20:
            return {"ok": False, "reason": "the file is empty"}
        if len(text) > cap:
            return {"ok": False, "reason": f"over {cap:,} characters"}
        parsed_title, parsed_description = _title_of(text, name or "untitled")
        title = (title or "").strip() or parsed_title
        name = _slug(name) or _slug(title)
        if not name:
            return {"ok": False, "reason": "the file needs a name"}
        if kind == "skill" and name in reserved:
            return {"ok": False,
                    "reason": f"{name} is a built-in skill's name: pick another"}
        if kind == "knowledge":
            if not _BU.match(business_unit or ""):
                return {"ok": False, "reason": "a knowledge file names the "
                                               "business unit it is for"}
            if ext not in EXTS:
                return {"ok": False, "reason": "ext is one of " + ", ".join(EXTS)}
        if not (submitter_slug or "").strip():
            return {"ok": False, "reason": "no submitter to file this for"}
        description = (description or "").strip()[:400] or parsed_description
        purpose = (purpose or "").strip()[:600]
        existing = self.find(kind, name, submitter_slug)
        if existing is not None:
            return self.resubmit(existing["id"], text=text, description=description,
                                 purpose=purpose, by=submitter, title=title)
        sid = f"sub_{uuid.uuid4().hex[:12]}"
        self._write_version(sid, 1, text)
        self._append({"ev": "submitted", "id": sid, "at": now_iso(),
                      "kind": kind, "name": name, "title": title,
                      "description": description, "purpose": purpose,
                      "business_unit": business_unit if kind == "knowledge" else "",
                      "ext": ext if kind == "knowledge" else "md",
                      "submitter": submitter, "submitter_slug": submitter_slug,
                      "approver": asdict(approver), "chars": len(text)})
        return {"ok": True, "submission": self.get(sid), "resubmitted": False}

    def resubmit(self, sid: str, *, text: str, description: str = "",
                 purpose: str = "", by: str = "", title: str = "",
                 comment: str = "") -> dict[str, Any]:
        sub = self._fold().get(sid)
        if sub is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        if sub["status"] == "withdrawn":
            return {"ok": False, "reason": "this submission was withdrawn"}
        text = (text or "").strip() + "\n"
        if len(text.strip()) < 20:
            return {"ok": False, "reason": "the file is empty"}
        version = sub["version"] + 1
        self._write_version(sid, version, text)
        self._append({"ev": "resubmitted", "id": sid, "at": now_iso(),
                      "version": version, "by": by or sub["submitter"],
                      "title": (title or "").strip()[:200],
                      "description": (description or "").strip()[:400],
                      "purpose": (purpose or "").strip()[:600],
                      "comment": (comment or "").strip()[:2000],
                      "chars": len(text)})
        return {"ok": True, "submission": self.get(sid), "resubmitted": True}

    def _write_version(self, sid: str, version: int, text: str) -> None:
        path = self._file(sid, version)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def record_ai(self, sid: str, version: int,
                  review: dict[str, Any] | None, *, reason: str = "") -> None:
        self._append({"ev": "ai_review", "id": sid, "at": now_iso(),
                      "version": version, "review": review,
                      "status": "done" if review else "failed",
                      "reason": reason})

    def start_ai(self, sid: str, read: Callable[[dict[str, Any], str],
                                                dict[str, Any]]) -> None:
        """The model's read in the background: ``read(submission,
        text)`` returns {ok, review|reason}; the ledger gets one
        ai_review record for this version either way. The checks land
        first, synchronously, so the page always has something."""
        sub = self._fold().get(sid)
        if sub is None:
            return
        version = sub["version"]
        text = self.text_of(sid, version)
        checks = checks_review(sub["kind"], sub.get("title", ""), text,
                               purpose=sub.get("purpose", ""))
        # the checks stand in while the model reads: one record, status
        # running, the checks as the review the page shows meanwhile
        self._append({"ev": "ai_review", "id": sid, "at": now_iso(),
                      "version": version, "review": checks,
                      "status": "running",
                      "reason": "the model's read is on its way"})

        def worker() -> None:
            try:
                got = read(sub, text)
            except Exception as e:                       # noqa: BLE001
                got = {"ok": False, "reason": str(e)[:300]}
            if got.get("ok"):
                self.record_ai(sid, version, got["review"])
            else:
                # the checks stay, marked as what they are
                self._append({"ev": "ai_review", "id": sid, "at": now_iso(),
                              "version": version, "review": checks,
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

    def decide(self, sid: str, decision: str, *, comment: str = "",
               by: str = "", publish: Callable[[dict[str, Any], str],
                                               dict[str, Any]] | None = None
               ) -> dict[str, Any]:
        """Approve (publish through the caller's door) or reject, with
        the manager's comment. The approver on the record must hold
        band 40 or above; nothing else may approve."""
        sub = self._fold().get(sid)
        if sub is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        if sub["status"] != "pending":
            return {"ok": False, "reason": f"this submission is "
                                           f"{STATUS_LABEL[sub['status']].lower()}, "
                                           "not pending"}
        approver = Approver(**sub["approver"])
        if not approver.may_approve:
            return {"ok": False,
                    "reason": f"the approver must be band {MIN_BAND} or above; "
                              f"{approver.name} is band {approver.band}"}
        comment = (comment or "").strip()[:4000]
        by = by or approver.name
        if decision == "approve":
            published_path = ""
            if publish is not None:
                got = publish(sub, self.text_of(sid))
                if not got.get("ok"):
                    return {"ok": False, "reason": got.get("reason") or "not published"}
                published_path = str(got.get("path") or "")
            self._append({"ev": "approved", "id": sid, "at": now_iso(), "by": by,
                          "comment": comment, "published_path": published_path,
                          "version": sub["version"]})
        elif decision == "reject":
            if not comment:
                return {"ok": False, "reason": "a rejection carries the reason "
                                               "for the submitter"}
            self._append({"ev": "rejected", "id": sid, "at": now_iso(), "by": by,
                          "comment": comment, "version": sub["version"]})
        else:
            return {"ok": False, "reason": "decision is approve or reject"}
        return {"ok": True, "submission": self.get(sid)}

    def withdraw(self, sid: str, *, by: str = "") -> dict[str, Any]:
        sub = self._fold().get(sid)
        if sub is None:
            return {"ok": False, "reason": f"no submission {sid}"}
        self._append({"ev": "withdrawn", "id": sid, "at": now_iso(), "by": by})
        return {"ok": True, "submission": self.get(sid)}

    # ── notices ──
    def notices(self, me: str, *, limit: int = 30) -> dict[str, Any]:
        """What the PRD says people are told: a file submitted (to the
        submitter and the manager), approved, rejected, resubmitted.
        On the laptop one person wears both hats and reads them all;
        each notice names whose it is."""
        subs = self._fold()
        # "read through record N": the ledger is append-only, so the
        # count is exact where a clock would tie within a second
        seen = int(self._seen().get(me, 0) or 0)
        out = []
        for idx, rec in enumerate(self._records()):
            sub = subs.get(rec.get("id", ""))
            ev = rec.get("ev")
            if sub is None or ev not in ("submitted", "resubmitted", "approved",
                                         "rejected"):
                continue
            unread = idx >= seen
            title = sub.get("title") or sub.get("name")
            approver = (sub.get("approver") or {}).get("name", "")
            if ev in ("submitted", "resubmitted"):
                verb = "submitted" if ev == "submitted" else "resubmitted"
                out.append({"at": rec["at"], "id": sub["id"], "event": ev,
                            "to": "approver", "who": approver, "unread": unread,
                            "text": f"{sub.get('submitter', 'someone')} {verb} "
                                    f"'{title}' ({sub['kind']}) for your review"})
                out.append({"at": rec["at"], "id": sub["id"], "event": ev,
                            "to": "submitter", "who": sub.get("submitter", ""),
                            "unread": unread,
                            "text": f"'{title}' {verb} to {approver} for approval"})
            else:
                out.append({"at": rec["at"], "id": sub["id"], "event": ev,
                            "to": "submitter", "who": sub.get("submitter", ""),
                            "unread": unread,
                            "text": f"'{title}' was {ev} by {rec.get('by') or approver}"
                                    + (": " + rec["comment"][:120]
                                       if rec.get("comment") else "")})
        out.reverse()                                # newest first
        return {"notices": out[:limit],
                "unread": sum(1 for n in out if n["unread"]),
                "pending": sum(1 for s in subs.values()
                               if s["status"] == "pending")}

    def _seen(self) -> dict[str, int]:
        if not self.seen_path.exists():
            return {}
        try:
            got = json.loads(self.seen_path.read_text(encoding="utf-8"))
        except ValueError:
            return {}
        return {k: v for k, v in got.items() if isinstance(v, int)}

    def mark_seen(self, me: str) -> None:
        """Everything on the ledger so far is read for this person."""
        self.root.mkdir(parents=True, exist_ok=True)
        seen = self._seen()
        seen[me] = len(self._records())
        self.seen_path.write_text(json.dumps(seen), encoding="utf-8")


__all__ = ["KINDS", "STATUSES", "STATUS_LABEL", "EVENTS", "MIN_BAND",
           "RECOMMENDATIONS", "RECOMMENDATION_LABEL", "CATEGORIES",
           "CATEGORY_LABEL", "CONFIDENCES", "Approver", "approver_for",
           "REVIEW_SYSTEM", "ai_review", "checks_review", "Reviews"]
