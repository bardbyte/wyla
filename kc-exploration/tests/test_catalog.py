"""The catalog plane: what from_env refuses, what a search sends, how a
refusal comes back."""

from __future__ import annotations

import json

import pytest

from kcx.catalog import (BACKOFFS, CatalogClient, CatalogConnection,
                         CatalogError, shape_response)
from kcx.env import ConfigError
from kcx.transport import TransportError
from tests.doubles import FakeCatalog, entry, google_error


def _client(fake: FakeCatalog, sleeps: list | None = None, **overrides):
    fields = dict(project="demo-project", key_path=None,
                  quota_project="demo-project")
    fields.update(overrides)
    return CatalogClient(CatalogConnection(**fields), http=fake.http,
                         token=fake.token,
                         sleep=(sleeps.append if sleeps is not None
                                else lambda s: None))


def test_from_env_refuses_by_name_and_never_borrows_another_key(
        monkeypatch, sa_key):
    with pytest.raises(ConfigError, match="KC_PROJECT_ID"):
        CatalogConnection.from_env()
    monkeypatch.setenv("KC_PROJECT_ID", "demo-project")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_key))
    monkeypatch.setenv("SYNAPSE_VERTEX_SA_KEY", str(sa_key))
    monkeypatch.setenv("VERTEX_SA_KEY", str(sa_key))
    with pytest.raises(ConfigError, match="KC_SA_KEY"):
        CatalogConnection.from_env()
    monkeypatch.setenv("KC_SA_KEY", str(sa_key.with_name("missing.json")))
    with pytest.raises(ConfigError, match="not found on disk"):
        CatalogConnection.from_env()


def test_from_env_defaults_and_knobs(monkeypatch, catalog_env):
    c = CatalogConnection.from_env()
    assert c.parent == "projects/demo-project/locations/global"
    assert c.url() == ("https://dataplex.googleapis.com/v1/projects/"
                       "demo-project/locations/global:searchEntries")
    assert c.quota_project == "demo-project" and c.page_size == 50
    assert c.semantic is True and c.search_scope == "" and c.proxies == {}
    assert c.ssl_verify is True and c.ca_bundle is None
    monkeypatch.setenv("HTTPS_PROXY", "http://u:p@proxy:8080")
    monkeypatch.setenv("KC_QUOTA_PROJECT", "none")
    monkeypatch.setenv("KC_SEARCH_SCOPE", "projects/other")
    monkeypatch.setenv("KC_PAGE_SIZE", "7")
    monkeypatch.setenv("KC_SEMANTIC_SEARCH", "0")
    monkeypatch.setenv("KC_API_BASE_URL", "https://private.example.com/")
    c = CatalogConnection.from_env()
    assert c.proxies == {"https": "http://u:p@proxy:8080"}
    assert c.route() == "via http://proxy:8080"
    assert c.quota_project == "" and c.search_scope == "projects/other"
    assert c.page_size == 7 and c.semantic is False
    assert c.url().startswith("https://private.example.com/v1/projects/")
    monkeypatch.setenv("KC_DISABLE_PROXY", "1")
    monkeypatch.setenv("KC_QUOTA_PROJECT", "billing-project")
    c = CatalogConnection.from_env()
    assert c.proxies == {} and c.quota_project == "billing-project"
    shown = json.dumps(c.describe())
    assert "demo-project" in shown and "u:p@" not in shown
    assert c.describe()["key_exists"] is True


def test_a_search_sends_the_sample_request(search_payload):
    fake = FakeCatalog(default=search_payload)
    client = _client(fake, search_scope="")
    result = client.search("card transactions")
    request = fake.requests[0]
    assert request["method"] == "POST"
    assert request["url"] == ("https://dataplex.googleapis.com/v1/projects/"
                              "demo-project/locations/global:searchEntries")
    assert request["body"] == {"query": "card transactions", "pageSize": 50,
                               "semanticSearch": True}
    assert request["headers"]["Authorization"] == "Bearer catalog-token"
    assert request["headers"]["X-Goog-User-Project"] == "demo-project"
    assert request["headers"]["Content-Type"] == "application/json"
    rows = result["results"]
    assert [r["display_name"] for r in rows] == ["gms_transaction", "",
                                                 "merchant"]
    assert set(rows[0]) == {"entry_name", "system", "resource_id",
                            "display_name", "description", "entry_type",
                            "fully_qualified_name"}
    assert rows[0]["system"] == "BIGQUERY"
    assert rows[0]["resource_id"].startswith("//bigquery.googleapis.com/")
    assert rows[1]["description"] == "" and rows[1]["entry_type"].endswith(
        "bigquery-dataset")
    assert result["total_size"] == 3 and result["next_page_token"] == ""
    assert result["unreachable"] == []
    assert client.usage == {"calls": 1, "rows": 3}


def test_optional_fields_ride_only_when_given():
    fake = FakeCatalog()
    client = _client(fake, quota_project="", search_scope="organizations/1",
                     page_size=5, semantic=False)
    client.search("q", page_token="tok", order_by="relevance")
    body = fake.requests[0]["body"]
    assert body == {"query": "q", "pageSize": 5, "semanticSearch": False,
                    "pageToken": "tok", "scope": "organizations/1",
                    "orderBy": "relevance"}
    assert "X-Goog-User-Project" not in fake.requests[0]["headers"]
    client.search("q", page_size=2, semantic=True, scope="")
    assert fake.requests[1]["body"] == {"query": "q", "pageSize": 2,
                                        "semanticSearch": True}


def test_a_403_is_a_typed_refusal_with_the_reason_and_no_sleep():
    fake = FakeCatalog()
    fake.fail_next = [(403, google_error(
        403, "IAM_PERMISSION_DENIED",
        "Permission 'dataplex.projects.search' denied on resource"))]
    sleeps: list = []
    with pytest.raises(CatalogError) as caught:
        _client(fake, sleeps).search("q")
    assert caught.value.status == 403
    assert caught.value.reason == "IAM_PERMISSION_DENIED"
    assert caught.value.message.startswith("HTTP 403: Permission")
    assert "refused (HTTP 403)" in str(caught.value)
    assert sleeps == [] and len(fake.requests) == 1


def test_transient_refusals_and_transport_failures_ride_the_ladder():
    fake = FakeCatalog(default=[entry("a")])
    fake.fail_next = [429, TransportError("reset")]
    sleeps: list = []
    result = _client(fake, sleeps).search("q")
    assert len(result["results"]) == 1
    assert sleeps == [2, 4] and len(fake.requests) == 3

    fake = FakeCatalog()
    fake.fail_next = [503] * 5
    sleeps = []
    with pytest.raises(CatalogError) as caught:
        _client(fake, sleeps).search("q")
    assert sleeps == list(BACKOFFS)
    assert caught.value.status == 503
    assert f"after {len(BACKOFFS) + 1} attempts" in str(caught.value)


def test_a_token_failure_is_immediate_and_a_non_json_200_is_a_refusal():
    fake = FakeCatalog()
    client = _client(fake)

    def dead():
        raise TransportError("token: dns down")
    client.token = dead
    with pytest.raises(TransportError, match="token: dns down"):
        client.search("q")
    assert fake.requests == []

    fake.fail_next = [(200, b"<html>login</html>")]
    with pytest.raises(CatalogError, match="not JSON"):
        _client(fake).search("q")


def test_shape_response_tolerates_missing_fields():
    assert shape_response({}) == {"results": [], "total_size": 0,
                                  "next_page_token": "", "unreachable": []}
    shaped = shape_response({"results": [{"dataplexEntry": {"name": "n"}},
                                         "junk"],
                             "totalSize": "2", "nextPageToken": "t",
                             "unreachable": ["projects/x"]})
    assert shaped["results"][0]["entry_name"] == "n"
    assert shaped["results"][0]["display_name"] == ""
    assert shaped["total_size"] == 2 and shaped["next_page_token"] == "t"
    assert shaped["unreachable"] == ["projects/x"]
