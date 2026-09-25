"""Read BigQuery's HTTP errors for people, and tell an authorization
failure from a bad query.

A failed ``jobs.query`` carries a JSON body with an ``error`` object:
a numeric code, a message, a status word, and a list of reasons.
``bigquery_http_error_message`` turns that into one line;
``is_bigquery_auth_error`` says whether a message means the caller may
not (a 401 or 403, a denial, an insufficient scope), so the sandbox can
label it ``bigquery_authorization`` instead of ``invalid_sql`` and the
person is told to connect Google rather than to fix their SQL.
"""

from __future__ import annotations

import json
import urllib.error

_AUTH_MARKERS = (
    "401", "403", "access denied", "accessdenied", "permission denied",
    "permission_denied", "unauthenticated", "unauthorized", "forbidden",
    "insufficient", "invalid authentication credentials", "login required",
    "invalid_grant", "does not have bigquery.",
    "request had invalid authentication", "user does not have permission",
)


def bigquery_http_error_message(error: urllib.error.HTTPError) -> str:
    """One line for a failed BigQuery HTTP call: code, status, message."""
    code = getattr(error, "code", None)
    body = ""
    try:
        body = error.read().decode("utf-8", "replace")
    except Exception:  # noqa: BLE001 - the body is a courtesy
        body = ""
    message, status, reasons = "", "", []
    if body:
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        detail = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(detail, dict):
            message = str(detail.get("message") or "")
            status = str(detail.get("status") or "")
            reasons = [str(e.get("reason") or "") for e in detail.get("errors") or []
                       if isinstance(e, dict) and e.get("reason")]
        elif isinstance(detail, str):
            message = detail
        if not message:
            message = body.strip()[:300]
    if not message:
        message = str(getattr(error, "reason", "") or error)
    head = f"BigQuery HTTP {code}" if code else "BigQuery HTTP error"
    if status:
        head += f" {status}"
    if reasons:
        head += f" ({', '.join(sorted(set(reasons)))})"
    return f"{head}: {message}"


def is_bigquery_auth_error(text: str) -> bool:
    """Whether an error message means the caller lacks authorization,
    as opposed to having written SQL BigQuery refuses."""
    value = (text or "").lower()
    return any(marker in value for marker in _AUTH_MARKERS)


__all__ = ["bigquery_http_error_message", "is_bigquery_auth_error"]
