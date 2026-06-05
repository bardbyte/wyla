"""Real Vertex AI Gemini client for enrichment.

Uses google-genai (the same SDK ADK uses internally), which automatically
picks up the four GOOGLE_* env vars set by `config.load_config()`.
Returns parsed EnrichmentBundle objects via Pydantic.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Reach into sibling synapse package for the existing schemas
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment  # noqa: E402


class VertexEnrichmentClient:
    """Thin wrapper that calls Gemini for one batch of columns at a time."""

    def __init__(self, *, model: str, skill_md: str, dry_run: bool = False) -> None:
        self.model = model
        self.skill_md = skill_md
        self.dry_run = dry_run
        self._client: Any | None = None

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        # Lazy import — only pay the cost if we actually call out
        try:
            from google import genai  # type: ignore[import-not-found]
        except ImportError as e:
            raise RuntimeError(
                "google-genai SDK not installed. Run `pip install google-genai` "
                f"(came in via google-adk). Original error: {e}"
            )
        self._client = genai.Client()
        return self._client

    def enrich_batch(
        self, *, table_name: str, batch_context: dict[str, Any],
    ) -> EnrichmentBundle:
        """Call Gemini with the skill + batch context. Returns a validated
        EnrichmentBundle. If `dry_run`, returns a canned synthetic bundle
        (useful for dev when Vertex isn't reachable)."""
        if self.dry_run:
            return _canned_bundle(table_name, batch_context)

        client = self._ensure_client()

        prompt = _build_prompt(self.skill_md, table_name, batch_context)

        # Gemini 3.1 Pro with strict JSON response shape via response_schema
        # (avoids the markdown-fenced JSON parsing dance).
        response_schema_json = EnrichmentBundle.model_json_schema()

        try:
            response = client.models.generate_content(
                model=self.model,
                contents=prompt,
                config={
                    "temperature": 0.0,
                    "response_mime_type": "application/json",
                    "response_schema": response_schema_json,
                    "max_output_tokens": 8192,
                },
            )
        except Exception as e:  # noqa: BLE001
            # Network / quota / auth failures fall back to a stub so the
            # demo doesn't die — but we tag it loudly.
            return _error_bundle(table_name, batch_context, error=str(e))

        raw_text = response.text or ""
        return _parse_or_repair(raw_text, table_name, batch_context)


def _build_prompt(skill_md: str, table_name: str, batch_context: dict[str, Any]) -> str:
    """Compose the full prompt the LLM sees."""
    return (
        skill_md
        + "\n\n---\n\n## INPUT — context for this batch\n\n"
        + f"Target table: `{table_name}`\n\n"
        + "Batch of columns to enrich (with all available signal):\n\n"
        + "```json\n"
        + json.dumps(batch_context, indent=2, default=str)
        + "\n```\n\n"
        + "Return ONLY the EnrichmentBundle JSON. No prose."
    )


def _parse_or_repair(
    raw: str, table_name: str, batch_context: dict[str, Any],
) -> EnrichmentBundle:
    """Validate the response or recover."""
    try:
        data = json.loads(raw)
        # The model may have emitted ONLY the column_observations for the
        # batch — pad table_name and self_assessment if missing.
        data.setdefault("table_name", table_name)
        data.setdefault("column_observations", [])
        data.setdefault("self_assessment", SelfAssessment(
            columns_marked_ambiguous=0,
        ).model_dump())
        return EnrichmentBundle.model_validate(data)
    except Exception as e:  # noqa: BLE001
        # Recovery: emit an empty bundle with the parse error captured
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[],
            self_assessment=SelfAssessment(
                columns_marked_ambiguous=0,
                requires_steward_attention=[
                    f"LLM response failed parse: {str(e)[:200]}"
                ],
            ),
        )


def _canned_bundle(table_name: str, batch_context: dict[str, Any]) -> EnrichmentBundle:
    """Used when ENRICHMENT_DRY_RUN=1 — keeps the pipeline runnable offline."""
    from synapse.enrichment.schemas import ColumnObservation  # local import

    cols = batch_context.get("columns_in_batch", [])
    observations = []
    for c in cols:
        # Inspector uses `name`; LLM skill schema uses `column_name`.
        name = (
            c.get("name") or c.get("column_name") if isinstance(c, dict) else c
        )
        observations.append(ColumnObservation(
            column_name=name,
            proposed_description=f"[dry-run] {name} on {table_name}",
            candidate_role="attribute",
            evidence_used=["mdm", "bq"],
            self_confidence=0.5,
        ))
    return EnrichmentBundle(
        table_name=table_name,
        column_observations=observations,
        self_assessment=SelfAssessment(
            columns_marked_ambiguous=0,
            requires_steward_attention=["DRY_RUN mode — no real Gemini calls made"],
        ),
    )


def _error_bundle(
    table_name: str, batch_context: dict[str, Any], *, error: str,
) -> EnrichmentBundle:
    """Used when Gemini call itself fails — keeps demo running, flags loudly."""
    return EnrichmentBundle(
        table_name=table_name,
        column_observations=[],
        self_assessment=SelfAssessment(
            columns_marked_ambiguous=0,
            requires_steward_attention=[f"Vertex call failed: {error[:200]}"],
        ),
    )
