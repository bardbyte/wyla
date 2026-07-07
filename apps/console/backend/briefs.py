"""The brief store — a finished inquiry, kept.

A brief is the unit of work the console produces: the question, the
signed-off answer card, the working thread, and the audit ledger rows
it generated. The store is in-memory with three seeded briefs (the
golden transcripts, in document form) so the workspace is furnished on
first open; `/chat` tees every Answer event in, so real turns become
briefs with zero extra calls.

Phase 1 keeps this deliberately small: list / get / append. Durable
storage and sharing are later phases — the seeded store is labeled
sample data through the same `live` flag as every other read.
"""

from __future__ import annotations

import itertools
from typing import Any


class BriefStore:
    def __init__(self, *, seed: bool = True) -> None:
        self._briefs: dict[str, dict[str, Any]] = {}
        self._ids = itertools.count(1)
        if seed:
            for brief in _SEEDS:
                self._briefs[brief["id"]] = dict(brief)

    def list(self) -> list[dict[str, Any]]:
        """Newest first; card-sized projection (no thread bodies)."""
        rows = sorted(self._briefs.values(),
                      key=lambda b: b["created_at"], reverse=True)
        return [{k: b[k] for k in
                 ("id", "title", "created_at", "status", "tier",
                  "live", "citation_count")} for b in rows]

    def get(self, brief_id: str) -> dict[str, Any] | None:
        return self._briefs.get(brief_id)

    def add_from_answer(self, *, turn_id: str, question: str,
                        sections: dict[str, Any],
                        ts: str | None = None) -> dict[str, Any]:
        """Called by the /chat tee when a turn emits its Answer."""
        brief_id = f"b{next(self._ids) + 100}"
        citations = sections.get("citations", []) or []
        brief = {
            "id": brief_id,
            "title": _title_from(question),
            "created_at": ts or "",
            "status": sections.get("status", ""),
            "tier": _tier_from_status(sections.get("status", "")),
            "live": True,
            "citation_count": len(citations),
            "question": question,
            "sections": sections,
            "thread": [{"role": "user", "text": question}],
            "ledger": [{"ref": c.get("ref", "")} for c in citations
                       if str(c.get("ref", "")).startswith("ledger:")],
            "turn_id": turn_id,
        }
        self._briefs[brief_id] = brief
        return brief


def _title_from(question: str) -> str:
    text = " ".join(question.strip().split())
    return (text[:64] + "…") if len(text) > 65 else text


def _tier_from_status(status: str) -> str:
    s = status.lower()
    if "guardrail" in s or "refused" in s:
        return "blocked"
    if "grounded" in s:
        return "grounded"
    if "await" in s or "held" in s:
        return "inferred"
    return "inferred"


_SEEDS: list[dict[str, Any]] = [
    {
        "id": "b3", "title": "New-account volume, month over month",
        "created_at": "2026-07-06T14:22:00Z",
        "status": "grounded · live query", "tier": "grounded",
        "live": False, "citation_count": 2,
        "question": "How are new accounts trending month over month?",
        "sections": {
            "answer": "New accounts are up **8.4% month over month**, "
                      "with the most recent full month leading.",
            "how_i_got_there": "Resolved the concept to acct_open_dt and "
                               "acct_id on sbs_new_accounts, validated and "
                               "dry-ran the SQL (1.24 GB), ran it with the "
                               "row cap after approval, computed the delta "
                               "in the sandbox, and charted the series.",
            "citations": [
                {"label": "bq:sbs_new_accounts",
                 "ref": "synapse://table/sbs_new_accounts"},
                {"label": "ledger#4821", "ref": "ledger:#4821"},
            ],
            "governance": "Read-only, row-capped, recorded on the audit "
                          "ledger.",
            "status": "grounded · live query",
        },
        "thread": [
            {"role": "user",
             "text": "How are new accounts trending month over month?"},
            {"role": "agent",
             "text": "Drafted and validated the query, presented the cost, "
                     "and ran it after your approval — the brief has the "
                     "chart and the delta."},
        ],
        "ledger": [{"ref": "ledger:#4821"}],
        "turn_id": "seed-3",
    },
    {
        "id": "b2", "title": "Ownership and lineage for sbs_new_accounts",
        "created_at": "2026-07-06T11:05:00Z",
        "status": "grounded · 2 sources", "tier": "grounded",
        "live": False, "citation_count": 2,
        "question": "Who owns sbs_new_accounts, and which pipeline feeds "
                    "it?",
        "sections": {
            "answer": "**New Accounts (SBS)** owns `sbs_new_accounts`; it "
                      "is fed by the new-accounts approval pipeline.",
            "how_i_got_there": "Read the metadata spine for ownership and "
                               "lifecycle, then traced lineage for the "
                               "feeding pipeline.",
            "citations": [
                {"label": "mdm:ownership",
                 "ref": "synapse://table/sbs_new_accounts"},
                {"label": "mdm:lineage",
                 "ref": "synapse://table/sbs_new_accounts"},
            ],
            "governance": "No restricted columns surfaced in this answer.",
            "status": "grounded · 2 sources",
        },
        "thread": [
            {"role": "user",
             "text": "Who owns sbs_new_accounts, and which pipeline feeds "
                     "it?"},
            {"role": "agent",
             "text": "Ownership and lineage both come from the metadata "
                     "spine — two independent reads, cited in the brief."},
        ],
        "ledger": [],
        "turn_id": "seed-2",
    },
    {
        "id": "b1", "title": "Cardmember-level detail request (declined)",
        "created_at": "2026-07-05T16:40:00Z",
        "status": "guardrail enforced", "tier": "blocked",
        "live": False, "citation_count": 1,
        "question": "Break down roll rates by cm11 at cardmember grain.",
        "sections": {
            "answer": "That column is protected by a governance guardrail "
                      "and cannot be exposed at cardmember grain. The same "
                      "analysis is available on the masked account key.",
            "how_i_got_there": "Statically validated the plan; the "
                               "RollRates playbook marks the column "
                               "never-expose, so the query was declined "
                               "before anything ran.",
            "citations": [
                {"label": "skill:SBS_RollRates/guardrail#cm11",
                 "ref": "synapse://guardrail/sbs_rollrates/cm11"},
            ],
            "governance": "Declined: restricted-column exposure. A "
                          "compliant alternative was offered.",
            "status": "guardrail enforced",
        },
        "thread": [
            {"role": "user",
             "text": "Break down roll rates by cm11 at cardmember grain."},
            {"role": "agent",
             "text": "Declined before execution — the guardrail is "
                     "enforced in code, and the brief records the refusal "
                     "with its citation."},
        ],
        "ledger": [],
        "turn_id": "seed-1",
    },
]
