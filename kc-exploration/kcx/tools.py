"""The one tool the model may call, in the sample's contract.

``knowledge_catalog_search`` takes a query and returns a dict: the rows
under ``results``, or an ``error`` the model can read. It never raises;
a refused search is data the model reasons about, not a crash of the
turn. The declaration is what the model sees. The size discipline keeps
four searches of fifty rows inside what one turn can carry back.
"""

from __future__ import annotations

import functools
import json
from typing import Any, Callable

from .catalog import CatalogClient, CatalogError

TOOL_NAME = "knowledge_catalog_search"
DESCRIPTION_CHARS = 240        # a description is a hint, not a document
RESULT_CHARS = 24_000          # one tool answer, serialized
SAMPLE_KEYS = ("entry_name", "system", "resource_id", "display_name")

DECLARATIONS: list[dict[str, Any]] = [{
    "name": TOOL_NAME,
    "description": (
        "Searches Knowledge Catalog with semantic search. Takes one search "
        "query: free text, qualified predicates (type=table, "
        "system=bigquery, projectid=my-project, parent=dataset, name:foo, "
        "displayname:foo, -name:bar), or both, joined with uppercase AND / "
        "OR. Returns the matching entries with their full entry names, "
        "source system, resource id and display name, or an error to read."),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "query": {"type": "STRING",
                      "description": "The search query, exactly as the "
                                     "catalog should see it. No double "
                                     "quotes in free text."}},
        "required": ["query"]},
}]


def knowledge_catalog_search(client: CatalogClient, query: str,
                             page_token: str = "") -> dict[str, Any]:
    """The tool: a query in, a dict out, never an exception."""
    try:
        result = client.search(str(query), page_token=page_token or "")
    except CatalogError as e:
        if e.status == 403:
            return {"error": f"Permission denied: {e.message}",
                    "reason": e.reason}
        return {"error": f"An unexpected error occurred: {e.message}",
                "reason": e.reason}
    except Exception as e:                       # noqa: BLE001
        return {"error": f"An unexpected error occurred: {e}"}
    return fit(result)


def fit(result: dict[str, Any]) -> dict[str, Any]:
    """Keep a tool answer inside RESULT_CHARS: descriptions trimmed, then
    dropped, then rows cut from the end, with a note that says so. The
    sample's four keys always survive on every row kept."""
    rows = [dict(row) for row in result.get("results") or []]
    for row in rows:
        description = row.get("description") or ""
        if len(description) > DESCRIPTION_CHARS:
            row["description"] = description[:DESCRIPTION_CHARS - 1] + "…"
    out: dict[str, Any] = {"results": rows,
                           "total_size": int(result.get("total_size") or 0),
                           "next_page_token": result.get("next_page_token")
                           or ""}
    if result.get("unreachable"):
        out["unreachable"] = list(result["unreachable"])
    if _size(out) <= RESULT_CHARS:
        return out
    for row in rows:
        row.pop("description", None)
    if _size(out) <= RESULT_CHARS:
        out["note"] = "descriptions dropped to fit the reply"
        return out
    dropped = 0
    while rows and _size(out) > RESULT_CHARS:
        rows.pop()
        dropped += 1
    out["results"] = rows
    out["truncated"] = dropped
    out["note"] = (f"{dropped} rows dropped to fit the reply; narrow the "
                   "query for the rest")
    return out


def _size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False))


def make_kit(client: CatalogClient) -> dict[str, Callable[..., dict[str, Any]]]:
    """The tools by name, bound to a client: what the agent dispatches on."""
    return {TOOL_NAME: functools.partial(knowledge_catalog_search, client)}
