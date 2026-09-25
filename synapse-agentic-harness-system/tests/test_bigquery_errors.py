"""BigQuery's HTTP errors read for people, and authorization told from SQL."""

from __future__ import annotations

import io
import urllib.error

from sahs.util.bigquery_errors import bigquery_http_error_message, is_bigquery_auth_error


def _error(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://bq/jobs", code, "reason", {}, io.BytesIO(body))


def test_json_body_becomes_one_line():
    body = (b'{"error": {"code": 403, "message": "Access Denied: Table p:d.t", '
            b'"status": "PERMISSION_DENIED", "errors": [{"reason": "accessDenied"}]}}')
    text = bigquery_http_error_message(_error(403, body))
    assert text == "BigQuery HTTP 403 PERMISSION_DENIED (accessDenied): Access Denied: Table p:d.t"
    assert is_bigquery_auth_error(text)


def test_non_json_body_and_sql_errors():
    assert bigquery_http_error_message(_error(500, b"<html>gateway</html>")).startswith(
        "BigQuery HTTP 500: <html>")
    assert not is_bigquery_auth_error("Syntax error: Unexpected keyword FROM at [1:8]")
    assert is_bigquery_auth_error("Request had invalid authentication credentials")
