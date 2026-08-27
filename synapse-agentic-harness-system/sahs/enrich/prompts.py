"""Pinned prompt builders — versioned so a prompt change is a diff.

Every prompt demands ONE strict JSON object and nothing else; the loop
validates keys and counts anything malformed. Context is drawn from the
compiled build only (cards + indexes) — the enricher sees exactly what
the serving agent would see, never the raw archives."""

from __future__ import annotations

from typing import Any

PROMPT_VERSION = "b1.1"

SYSTEM = (
    "You are a careful analytics engineer documenting an enterprise "
    "metric catalog. You answer with ONE JSON object and nothing else. "
    "You never invent table or column names — you only describe what "
    "the SQL in front of you actually computes. When you are not "
    "confident, you say so via the confidence field instead of "
    "guessing confidently.")


def _table_context(item: dict[str, Any]) -> str:
    lines = [f"table: {item.get('table') or 'unknown'}"]
    if item.get("table_purpose"):
        lines.append(f"table purpose: {item['table_purpose']}")
    if item.get("line_of_business"):
        lines.append(f"line of business: {item['line_of_business']}")
    if item.get("domain"):
        lines.append(f"metric domain: {item['domain']}")
    if item.get("columns"):
        lines.append("columns referenced: " + ", ".join(item["columns"]))
    return "\n".join(lines)


def metric_semantics_prompt(item: dict[str, Any]) -> str:
    """question + grain for a mined metric that has neither."""
    return f"""A mined metric was observed in real analyst usage. Document it.

{_table_context(item)}
metric label (usage alias, may be weak): {item.get('label') or '(none)'}
canonical SQL expression:
{item['sql']}

Return ONE JSON object with exactly these keys:
{{"question": "<the business question this metric answers, one sentence, phrased as an analyst would ask it>",
 "grain": "<the level each value is computed at, e.g. 'per merchant per day', 'per card member per month', 'total over the filtered period'>",
 "confidence": <0.0-1.0>,
 "caveat": "<one sentence on any ambiguity, or empty string>"}}"""


def blind_name_prompt(item: dict[str, Any]) -> str:
    """A5 blind test: recover the certified name from expression +
    context alone (the name is withheld)."""
    return f"""An enterprise metric catalog entry lost its name. Reconstruct it.

{_table_context(item)}
canonical SQL expression:
{item['sql']}

Return ONE JSON object with exactly these keys:
{{"name": "<the business-friendly metric name, 2-6 words, as a certified catalog would title it>",
 "confidence": <0.0-1.0>}}"""


def concept_description_prompt(item: dict[str, Any]) -> str:
    """One-paragraph description for a thin concept card."""
    bindings = "\n".join(
        f"- on {b['table']}: `{b['sql']}` (support {b['support']})"
        for b in item.get("bindings", [])[:6])
    return f"""A business concept appears in real analyst SQL with these bindings:

concept: {item['label']}
{bindings}

Return ONE JSON object with exactly these keys:
{{"description": "<2-3 sentences: what the concept means in business terms and when an analyst would filter by it — grounded ONLY in the bindings above>",
 "disambiguation": "<one sentence on how the bindings differ, or empty string if they agree>",
 "confidence": <0.0-1.0>}}"""
