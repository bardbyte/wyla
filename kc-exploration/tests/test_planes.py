"""Which door: the auto rule, the pin, the catalog rows, and the typed
refusals when a plane's contract is missing."""

from __future__ import annotations

import pytest

from kcx.gateway import GatewayModel
from kcx.planes import (ModelUnavailable, gateway_configured, model_for,
                        model_plane, plane_catalog, plane_note, pretty_model,
                        vertex_configured)
from kcx.vertex import VertexModel


def test_the_auto_rule_and_the_pin(sa_key):
    assert model_plane({}) == "vertex"
    gateway = {"GATEWAY_BASE_URL": "https://gw.example.com/genai/google/v1",
               "IDP_TOKEN_URL": "https://idp.example.com/token",
               "APP_ID": "a", "APP_SECRET": "cw=="}
    assert gateway_configured(gateway) and not vertex_configured(gateway)
    assert model_plane(gateway) == "gateway"
    both = {**gateway, "VERTEX_PROJECT_ID": "p", "VERTEX_SA_KEY": str(sa_key)}
    assert vertex_configured(both) and model_plane(both) == "vertex"
    assert model_plane({**both, "SAHS_MODEL_PLANE": "gateway"}) == "gateway"
    assert model_plane({**both, "SAHS_MODEL_PLANE": "gateway",
                        "KC_MODEL_PLANE": "vertex"}) == "vertex"
    assert model_plane({**both, "KC_MODEL_PLANE": "nonsense"}) == "vertex"
    assert "KC_MODEL_PLANE=gateway" in plane_note({"KC_MODEL_PLANE": "gateway"})
    assert "no Vertex key and no gateway" in plane_note({})
    assert not vertex_configured({"VERTEX_PROJECT_ID": "p",
                                  "VERTEX_SA_KEY": "/nope.json"})


def test_the_catalog_names_what_each_plane_needs(sa_key):
    rows = plane_catalog({})
    assert [r["id"] for r in rows] == ["vertex", "gateway"]
    assert rows[0]["label"] == "Gemini 3.1 Pro Preview"
    assert rows[1]["label"] == "Gemini 2.5 Pro"
    assert not rows[0]["available"] and "VERTEX_SA_KEY" in rows[0]["reason"]
    assert not rows[1]["available"] and "GATEWAY_BASE_URL" in rows[1]["reason"]
    assert [r["default"] for r in rows] == [True, False]
    rows = plane_catalog({"VERTEX_PROJECT_ID": "p", "VERTEX_SA_KEY": str(sa_key),
                          "VERTEX_MODEL": "gemini-3.1-pro-preview"})
    assert rows[0]["available"] and rows[0]["reason"] == ""
    assert pretty_model("gemini-2.5-pro") == "Gemini 2.5 Pro"


def test_model_for_builds_the_plane_or_refuses_by_name(monkeypatch, sa_key,
                                                       gateway_env):
    with pytest.raises(ModelUnavailable, match="vertex and gateway"):
        model_for("gpt")
    with pytest.raises(ModelUnavailable, match="VERTEX_SA_KEY"):
        model_for("vertex")
    assert isinstance(model_for("gateway"), GatewayModel)
    assert isinstance(model_for(""), GatewayModel)      # auto: gateway only
    monkeypatch.setenv("VERTEX_PROJECT_ID", "p")
    monkeypatch.setenv("VERTEX_SA_KEY", str(sa_key))
    assert isinstance(model_for(""), VertexModel)       # auto: Vertex now
    monkeypatch.delenv("GATEWAY_BASE_URL")
    with pytest.raises(ModelUnavailable) as caught:
        model_for("gateway")
    assert "GATEWAY_BASE_URL" in str(caught.value)
