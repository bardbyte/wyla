"""On-demand column enrichment — the graph's lazy-loading layer (S4).

When the agent hits a column with no grounded meaning, ``explain_column``
fills it: one LLM call over that column's evidence, through the SAME
grounding gate as batch enrichment (``_apply_bundle``), written at the SAME
capped provenance (``llm_generated`` → ``inferred``). Read-through: a column
that already has a description is returned from the graph, no call. No
evidence ⇒ the gate holds it ⇒ honest abstention, never invention.

Policy A: the fill auto-persists — into the live store *and* a labeled
``OverlayStore`` that survives rebuilds — and is recorded as a steward
proposal (``reviewed=False``). The canonical MDM+BQ build stays
reproducible; the agent layer is a distinct, reviewable, revertible
overlay. The proposal record is the bridge to policy B (human-gated): a
config change, not a rebuild.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synapse.enrichment.enricher import (
    _SKILL_MD_PATH,
    _apply_bundle,
    _build_context_for_table,
)
from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment
from synapse.graph.store import GraphStore, canonical_uri

# the grounded profile the agent always gets back, LLM or not
_GROUNDED_FACT_KEYS = (
    "data_type", "min_value", "max_value", "avg_value", "null_fraction",
    "approx_distinct", "cardinality_bucket", "is_primary", "is_foreign_key",
    "is_pii", "pii_taxonomy", "derived_logic",
)


class OverlayStore:
    """Durable, labeled side-store of agent-authored column fills. Each fill
    is also a steward proposal (``reviewed=False``). ``apply`` replays them
    onto a loaded snapshot at the capped provenance; the canonical build is
    untouched, so the agent layer is always separable and revertible."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._data: dict[str, Any] = {"version": 1, "fills": []}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self._data = loaded
            except (OSError, json.JSONDecodeError):
                pass
        self._data.setdefault("fills", [])

    def record(
        self, *, table: str, column: str, description: str, tier: str,
        evidence: list[str], actor: str = "agent",
        question: str | None = None, at: str | None = None,
    ) -> dict[str, Any]:
        entry = {
            "table": table, "column": column, "description": description,
            "tier": tier, "evidence": list(evidence or []), "actor": actor,
            "question": question, "reviewed": False, "at": at,
        }
        self._data["fills"].append(entry)
        self._save()
        return entry

    def record_assertion(
        self, *, subject_type: str, subject_ref: str, statement: str,
        actor: str, tier: str = "human_asserted", at: str | None = None,
    ) -> dict[str, Any]:
        """A trusted human's assertion — the top-tier twin of a fill. Also a
        steward-review record (the bridge to policy B)."""
        entry = {
            "kind": "assertion", "subject_type": subject_type,
            "subject_ref": subject_ref, "statement": statement,
            "actor": actor, "tier": tier, "reviewed": False, "at": at,
        }
        self._data.setdefault("assertions", []).append(entry)
        self._save()
        return entry

    def apply(self, store: GraphStore) -> int:
        """Replay the overlay onto a store: agent fills at the capped
        ``llm_generated`` provenance, human assertions at ``human_approval``
        (→ ``human_asserted``). Only touches subjects already present.
        Returns how many landed."""
        n = 0
        for f in self._data.get("fills", []):
            c_uri = canonical_uri("column", f["table"], f["column"])
            if c_uri in store.nodes and f.get("description"):
                store.upsert_node(
                    "Column", c_uri,
                    {"ai_generated_description": f["description"]},
                    source="llm_generated")
                n += 1
        for a in self._data.get("assertions", []):
            from synapse.graph.capture import resolve_subject_uri
            uri, _ = resolve_subject_uri(
                a.get("subject_type"), a.get("subject_ref"))
            if uri and uri in store.nodes and a.get("statement"):
                store.upsert_node(
                    store.get(uri).node_type, uri,
                    {"description": a["statement"],
                     "asserted_by": a.get("actor")},
                    source="human_approval")
                n += 1
        return n

    def proposals(self, *, pending_only: bool = False) -> list[dict[str, Any]]:
        return [f for f in self._data.get("fills", [])
                if not (pending_only and f.get("reviewed"))]

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self._data, indent=2, default=str), encoding="utf-8")


def _grounded_facts(props: dict[str, Any]) -> dict[str, Any]:
    return {k: props.get(k) for k in _GROUNDED_FACT_KEYS
            if props.get(k) not in (None, "", [])}


def explain_column(
    store: GraphStore, table: str, column: str, *,
    llm_client: Any = None, overlay: OverlayStore | None = None,
    evidence_dir: Path | None = None, question: str | None = None,
) -> dict[str, Any]:
    """Read-through fill for one column.

    Returns ``{status, cached, description, tier, grounded_facts, ...}``:
      * cache hit (already described) → return it, no LLM.
      * gap + no client → the grounded profile + an honest note.
      * gap + client → one gated LLM fill at capped tier, persisted to the
        overlay; or an abstention note if the gate held it.
    """
    c_uri = canonical_uri("column", table, column)
    node = store.get(c_uri)
    if node is None:
        return {"status": "error", "reason": f"{table}.{column} not in graph"}
    props = node.properties
    facts = _grounded_facts(props)

    existing = (str(props.get("description") or "").strip()
                or str(props.get("ai_generated_description") or "").strip())
    if existing:
        return {"status": "ok", "cached": True, "description": existing,
                "tier": node.provenance.confidence_tier,
                "grounded_facts": facts}

    if llm_client is None:
        return {"status": "partial", "cached": False, "description": "",
                "tier": node.provenance.confidence_tier,
                "grounded_facts": facts,
                "note": "no grounded meaning yet and no enrichment client; "
                        "returning the grounded profile only"}

    # one-column LLM fill — same context builder, same gate, same provenance
    context = _build_context_for_table(store, table, evidence_dir=evidence_dir)
    inspection = dict(context.get("inspection") or {})
    this = [c for c in (inspection.get("columns") or [])
            if str(c.get("name", "")).lower() == column.lower()]
    inspection["columns"] = this or [{"name": column}]
    context["inspection"] = inspection
    context["on_demand"] = {"column": column, "question": question}

    skill_md = _SKILL_MD_PATH.read_text(encoding="utf-8")
    bundle = llm_client.enrich(
        skill_md=skill_md, context=context, table_name=table)
    obs = next((o for o in bundle.column_observations
                if o.column_name.lower() == column.lower()), None)
    if obs is None:
        return {"status": "partial", "cached": False, "description": "",
                "grounded_facts": facts,
                "note": "model returned no observation for this column"}

    one = EnrichmentBundle(
        table_name=table, column_observations=[obs],
        self_assessment=SelfAssessment(
            tables_skipped_for_lack_of_signal=[], columns_marked_ambiguous=0,
            proposed_entities_with_low_evidence=[],
            requires_steward_attention=[]))
    _apply_bundle(store, one)      # gate-enforced; source=llm_generated

    node = store.get(c_uri)
    desc = str(node.properties.get("ai_generated_description") or "").strip()
    if not desc:
        return {"status": "partial", "cached": False, "description": "",
                "grounded_facts": facts,
                "note": "insufficient evidence to ground a description "
                        "(held by the grounding gate)"}
    tier = node.provenance.confidence_tier
    if overlay is not None:
        overlay.record(table=table, column=column, description=desc,
                       tier=tier, evidence=obs.evidence_used,
                       question=question)
    return {"status": "ok", "cached": False, "description": desc,
            "tier": tier, "grounded_facts": facts, "held": False,
            "written_to_overlay": overlay is not None}
