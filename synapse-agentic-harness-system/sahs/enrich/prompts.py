"""Pinned prompt builders — versioned so a prompt change is a diff.

Every prompt demands ONE strict JSON object and nothing else; the loop
validates keys and counts anything malformed. Context is drawn from the
compiled build only (cards + indexes) — the enricher sees exactly what
the serving agent would see, never the raw archives."""

from __future__ import annotations

from typing import Any

PROMPT_VERSION = "b1.3"

SYSTEM = (
    "You are a careful analytics engineer documenting an enterprise "
    "metric catalog. You answer with ONE JSON object and nothing else. "
    "You never invent table or column names — you only describe what "
    "the SQL in front of you actually computes. When you are not "
    "confident, you say so via the confidence field instead of "
    "guessing confidently.")

# House dialect, distilled from the catalog's own conventions (b1.2 —
# the b1.1 blind exam showed the model answering in literal SQL English
# where the catalog speaks the house language). The exam's specific
# names are never embedded here; note honestly that iterating a prompt
# against exam feedback adds mild optimistic bias to A5 — the true
# audit is steward review of real outputs (B2).
HOUSE_STYLE = """Naming and phrasing conventions in this catalog:
- Money movement is called Spend, not "net transaction amount": prefer
  short business names ("Spend", "Purchase Spend", "Cash Spend",
  "Inbound Spend") over SQL-literal phrases.
- When the SQL discriminates on a method/indicator/code column, ALWAYS
  append the method as a suffix, deriving its name from that column's
  meaning: "<Thing> - <Indicator/Code Meaning> Method". A metric that
  measures the same thing by a different column gets a different
  suffix — the suffix is what tells them apart.
- The metric's FILTERS are part of its identity: a filtered page,
  channel, program, or population from the filters MUST appear in the
  name, quoted for pages ("... on 'Page Name' Page", "... for
  Prospects", "Paid Search ...").
- Funnel stages are distinct and must not be conflated: eligible,
  enrolled, redeeming are different populations — name the stage the
  SQL actually filters to. Sessions and visits are different things.
- Prefer the catalog's short population nouns (Redeemers,
  Enrollments, Prospects); the formal terms — Card Member (CM),
  Service Establishment / Submitter Merchant (SE) — only when no
  short noun fits.
- Negations are opposite metrics: "Card Not Present" and
  "Card Present" must never be swapped — check filter polarity
  carefully before naming."""


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
    if item.get("filters"):
        lines.append("metric filters (part of its identity): "
                     + "; ".join(item["filters"]))
    if item.get("vocab"):
        lines.append("company vocabulary (Atlas business terms + the "
                     "enterprise glossary — authoritative here):")
        lines += [f"  - {entry}" for entry in item["vocab"]]
    return "\n".join(lines)


def metric_semantics_prompt(item: dict[str, Any]) -> str:
    """question + grain for a mined metric that has neither."""
    return f"""A mined metric was observed in real analyst usage. Document it.

{HOUSE_STYLE}

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

{HOUSE_STYLE}

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
    vocab = ""
    if item.get("vocab"):
        vocab = ("\ncompany vocabulary (Atlas business terms + the "
                 "enterprise glossary — authoritative here):\n"
                 + "\n".join(f"  - {entry}" for entry in item["vocab"]))
    return f"""A business concept appears in real analyst SQL with these bindings:

concept: {item['label']}
{bindings}{vocab}

Return ONE JSON object with exactly these keys:
{{"description": "<2-3 sentences: what the concept means in business terms and when an analyst would filter by it — grounded ONLY in the bindings above>",
 "disambiguation": "<one sentence on how the bindings differ, or empty string if they agree>",
 "confidence": <0.0-1.0>}}"""
