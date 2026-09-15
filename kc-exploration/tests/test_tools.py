"""The tool the model calls: the declaration, the never-raise contract,
the size discipline."""

from __future__ import annotations

import json

from kcx.catalog import CatalogError
from kcx.tools import (DECLARATIONS, DESCRIPTION_CHARS, RESULT_CHARS,
                       SAMPLE_KEYS, TOOL_NAME, fit, knowledge_catalog_search,
                       make_kit)


class _Stub:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls: list = []

    def search(self, query, *, page_token=""):
        self.calls.append((query, page_token))
        if self.error is not None:
            raise self.error
        return self.result


def _row(i: int, description: str = "") -> dict:
    return {"entry_name": f"projects/p/locations/us/entryGroups/g/entries/t{i}",
            "system": "BIGQUERY", "resource_id": f"//bq/t{i}",
            "display_name": f"t{i}", "description": description,
            "entry_type": "projects/1/locations/global/entryTypes/bigquery-table",
            "fully_qualified_name": f"bigquery:p.d.t{i}"}


def test_the_declaration_is_the_sample_s_one_string_parameter():
    assert len(DECLARATIONS) == 1
    d = DECLARATIONS[0]
    assert d["name"] == TOOL_NAME
    assert d["parameters"]["type"] == "OBJECT"
    assert list(d["parameters"]["properties"]) == ["query"]
    assert d["parameters"]["properties"]["query"]["type"] == "STRING"
    assert d["parameters"]["required"] == ["query"]
    assert "projectid" in d["description"]


def test_success_keeps_the_sample_keys_and_passes_the_token_through():
    stub = _Stub({"results": [_row(1, "short")], "total_size": 9,
                  "next_page_token": "next", "unreachable": ["projects/x"]})
    out = knowledge_catalog_search(stub, "q", page_token="p")
    assert stub.calls == [("q", "p")]
    assert all(key in out["results"][0] for key in SAMPLE_KEYS)
    assert out["total_size"] == 9 and out["next_page_token"] == "next"
    assert out["unreachable"] == ["projects/x"]


def test_refusals_become_dicts_and_nothing_raises():
    denied = knowledge_catalog_search(
        _Stub(error=CatalogError(403, "USER_PROJECT_DENIED", "HTTP 403: no")),
        "q")
    assert denied == {"error": "Permission denied: HTTP 403: no",
                      "reason": "USER_PROJECT_DENIED"}
    other = knowledge_catalog_search(
        _Stub(error=CatalogError(503, "UNAVAILABLE", "HTTP 503: later")), "q")
    assert other["error"] == "An unexpected error occurred: HTTP 503: later"
    crash = knowledge_catalog_search(_Stub(error=RuntimeError("boom")), "q")
    assert crash == {"error": "An unexpected error occurred: boom"}


def test_fit_trims_descriptions_then_drops_them_then_cuts_rows():
    long = "x" * 1000
    small = fit({"results": [_row(1, long)], "total_size": 1})
    assert len(small["results"][0]["description"]) == DESCRIPTION_CHARS
    assert small["results"][0]["description"].endswith("…")
    assert "note" not in small

    many = fit({"results": [_row(i, long) for i in range(60)],
                "total_size": 60})
    assert len(json.dumps(many)) <= RESULT_CHARS
    assert many["note"] == "descriptions dropped to fit the reply"
    assert "description" not in many["results"][0]
    assert len(many["results"]) == 60

    flood = fit({"results": [_row(i) for i in range(400)], "total_size": 400})
    assert len(json.dumps(flood)) <= RESULT_CHARS
    assert flood["truncated"] > 0 and "rows dropped" in flood["note"]
    assert len(flood["results"]) + flood["truncated"] == 400
    assert all(all(key in row for key in SAMPLE_KEYS)
               for row in flood["results"])


def test_make_kit_binds_the_client_under_the_tool_name():
    stub = _Stub({"results": [], "total_size": 0, "next_page_token": ""})
    kit = make_kit(stub)
    assert list(kit) == [TOOL_NAME]
    assert kit[TOOL_NAME](query="x") == {"results": [], "total_size": 0,
                                         "next_page_token": ""}
    assert stub.calls == [("x", "")]
