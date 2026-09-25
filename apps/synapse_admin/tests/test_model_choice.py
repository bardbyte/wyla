"""The composer's model switch takes a real 3.x choice.

A catalog choice is a plane or plane:model — ``gateway:gemini-3.5-flash``
is 24 characters, which the chat API once capped at 12 (a 422 on every
3.x engine) and the store cut to 16. Both are gone: the choice
round-trips through ``POST /api/chat/sessions/{id}/model`` and
``GET /api/chat/sessions/{id}`` into the store's ``Model`` column whole,
on the sqlite stand-in and on Spanner (the fake SDK database), and the
message body takes it too. The one cap left is the column's 64
characters (``db/spanner/008_chat_model.sql``)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from apps.synapse_admin.backend import chat  # noqa: E402
from apps.synapse_admin.tests.test_local_login import (  # noqa: E402,F401
    ANA, _client, _csrf, laptop, spanner)

DEFAULT = "gemini-3.7-flash"
OTHER = "gemini-3.5-flash"
CHOICE = f"gateway:{OTHER}"                  # 24 characters: over the old 12


@pytest.fixture()
def gateway(monkeypatch):
    """The gateway plane configured (no call is made: the switch only
    validates against the catalog) with two 3.x engines, so a
    plane:model choice exists that is not the plane's default."""
    monkeypatch.setenv("GATEWAY_MODEL", DEFAULT)
    monkeypatch.setenv("GATEWAY_MODELS", f"{DEFAULT} {OTHER}")
    monkeypatch.setenv("APP_ID", "app-id")
    monkeypatch.setenv("APP_SECRET", "c2VjcmV0")
    monkeypatch.setenv("IDP_TOKEN_URL", "https://idp.example/token")
    monkeypatch.setenv("GATEWAY_BASE_URL", "https://gateway.example/genai/google/v1")
    monkeypatch.setenv("SAHS_MODEL_PLANE", "gateway")


@pytest.mark.parametrize("store", ["laptop", "spanner"])
def test_a_3x_choice_round_trips_through_the_api_and_the_store(
        request, gateway, store):
    fixture = request.getfixturevalue(store)
    client = _client()
    signed_up = client.post("/api/auth/signup", json=ANA)
    assert signed_up.status_code == 201, signed_up.text
    ana = signed_up.json()["user"]
    csrf = _csrf(client)
    assert len(CHOICE) == 24
    dials = client.get("/api/chat/dials").json()
    assert CHOICE in [m["id"] for m in dials["models"]]

    session = client.post("/api/chat/sessions", headers=csrf).json()["session"]
    switched = client.post(f"/api/chat/sessions/{session['id']}/model",
                           json={"model": CHOICE}, headers=csrf)
    assert switched.status_code == 200, switched.text      # not 422
    body = switched.json()
    assert body["available"] and body["plane"] == "gateway"
    assert body["choice"] == CHOICE and body["model"] == "Gemini 3.5 Flash"
    shown = client.get(f"/api/chat/sessions/{session['id']}").json()
    assert shown["session"]["model"] == CHOICE and shown["plane"] == "gateway"
    assert shown["model"] == "Gemini 3.5 Flash"

    # the store holds the choice whole, on this backend
    runtime = chat._RUNTIMES[ana["user_id"]]
    assert runtime.store.get_session(session["id"])["model"] == CHOICE
    if store == "spanner":
        rows = fixture.rows("ChatSessions")
        assert [r["Model"] for r in rows] == [CHOICE]

    # the message body takes the same choice (a queued turn validates
    # it the same way; here only the field's width is at stake)
    from apps.synapse_admin.backend.chat import DraftRequest, NewMessage, SessionModel
    for model in (NewMessage, SessionModel, DraftRequest):
        assert model.model_fields["model"].metadata[0].max_length == 64
    assert NewMessage(text="hi", model=CHOICE).model == CHOICE
    assert DraftRequest(kind="skill", material="m", model=CHOICE).model == CHOICE

    # the plane's default model folds to the plane, as before
    folded = client.post(f"/api/chat/sessions/{session['id']}/model",
                         json={"model": f"gateway:{DEFAULT}"}, headers=csrf).json()
    assert folded["choice"] == "gateway"
    # a model the plane does not serve is a reason, not a 422 and not a swap
    nope = client.post(f"/api/chat/sessions/{session['id']}/model",
                       json={"model": "gateway:gemini-9.9-ultra"}, headers=csrf)
    assert nope.status_code == 200 and nope.json()["available"] is False
    assert "does not serve" in nope.json()["reason"] or OTHER in nope.json()["reason"]
    assert client.get(f"/api/chat/sessions/{session['id']}").json()[
        "session"]["model"] == "gateway"
    # the only cap left is the column's: 64 passes validation, 65 is 422
    at_cap = client.post(f"/api/chat/sessions/{session['id']}/model",
                         json={"model": "gateway:" + "m" * 56}, headers=csrf)
    assert at_cap.status_code == 200 and at_cap.json()["available"] is False
    over = client.post(f"/api/chat/sessions/{session['id']}/model",
                       json={"model": "gateway:" + "m" * 57}, headers=csrf)
    assert over.status_code == 422
