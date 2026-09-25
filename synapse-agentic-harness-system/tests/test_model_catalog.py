"""One plane, several models: the gateway's model list from the
environment, each model's thinking style and cap, the composer's
catalog of choices (a plane, or plane:model), and the runtime's switch
between them. Nothing here touches a network."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_gateway_plane import SECRET, compiled  # noqa: E402,F401

from sahs.assistant.agent import join_choice, model_catalog, split_choice  # noqa: E402
from sahs.enrich.gateway_client import GatewayClient  # noqa: E402
from sahs.util.gateway import (DEFAULT_SCOPES, Config, GatewayError,  # noqa: E402
                               gateway_models, output_cap, scopes_for,
                               thinking_kind, thinking_levels)

HOSTS = {"IDP_TOKEN_URL": "https://identity.example/security/digital/v1/application/token",
         "GATEWAY_BASE_URL": "https://gateway.example/genai/google/v1"}
MODELS = "gemini-2.5-pro gemini-3.5-flash,gemini-3.7-flash gemini-3.1-flash-lite"


def test_the_model_list_comes_from_the_environment_default_first():
    assert gateway_models({}) == ["gemini-2.5-pro"]
    assert gateway_models({"GATEWAY_MODEL": "gemini-2.5-flash"}) == ["gemini-2.5-flash"]
    listed = gateway_models({"GATEWAY_MODELS": MODELS})
    assert listed == ["gemini-2.5-pro", "gemini-3.5-flash", "gemini-3.7-flash", "gemini-3.1-flash-lite"]
    # GATEWAY_MODEL names the default: it comes first whatever the list's order
    assert gateway_models({"GATEWAY_MODELS": MODELS, "GATEWAY_MODEL": "gemini-3.5-flash"})[0] == "gemini-3.5-flash"
    # a default the list does not name is added, first
    assert gateway_models({"GATEWAY_MODELS": "gemini-3.5-flash", "GATEWAY_MODEL": "gemini-2.5-pro"}) == [
        "gemini-2.5-pro", "gemini-3.5-flash"]


def test_scopes_follow_the_models_unless_given():
    assert scopes_for({}) == DEFAULT_SCOPES                          # the guide's four, as before
    derived = scopes_for({"GATEWAY_MODELS": "gemini-2.5-pro gemini-3.5-flash"})
    assert derived[:2] == ["/genai/google/v1/models/gemini-2.5-pro/**::post",
                           "/genai/google/v1/models/gemini-3.5-flash/**::post"]
    assert derived[2:] == DEFAULT_SCOPES[2:]                          # the embedding scopes ride along
    given = scopes_for({"GATEWAY_MODELS": MODELS, "GATEWAY_SCOPES": "/a/**::post, /b/**::post"})
    assert given == ["/a/**::post", "/b/**::post"]                    # the gateway team's list wins
    cfg = Config.from_env({"GATEWAY_MODELS": MODELS, **HOSTS})
    assert cfg.model == "gemini-2.5-pro" and cfg.models == gateway_models({"GATEWAY_MODELS": MODELS})
    assert cfg.display()["models"] == cfg.models


def test_thinking_style_and_cap_by_family_with_overrides():
    assert thinking_kind("gemini-2.5-pro") == "budget"
    assert thinking_kind("gemini-2.5-flash-lite") == "budget"
    assert thinking_kind("gemini-3.5-flash") == "level"
    assert thinking_kind("gemini-3.1-flash-lite") == "level"
    assert thinking_kind("gemini-3.1-flash-lite", {"GATEWAY_MODEL_THINKING": "gemini-3.1-flash-lite:none"}) == "none"
    assert thinking_kind("gemini-3.5-flash", {"GATEWAY_MODEL_THINKING": "gemini-3.5-flash:sideways"}) == "level"
    # five stops; the ends fold onto the nearest level every 3.x model knows
    assert thinking_levels({}) == {"minimal": "low", "low": "low", "medium": "medium",
                                   "high": "high", "max": "high"}
    assert thinking_levels({"GATEWAY_THINKING_LEVELS": "medium:high,minimal:minimal,bogus:x"}) == {
        "minimal": "minimal", "low": "low", "medium": "high", "high": "high", "max": "high"}
    assert output_cap("gemini-2.5-pro") == 65536
    assert output_cap("gemini-3.1-flash-lite", {"GATEWAY_MODEL_CAPS": "gemini-3.1-flash-lite:8192"}) == 8192
    assert output_cap("gemini-3.1-flash-lite", {"GATEWAY_MODEL_CAPS": "gemini-3.1-flash-lite:lots"}) == 65536


def _client(model: str = "gemini-2.5-pro") -> GatewayClient:
    cfg = Config(app_id="app", secret=SECRET, model=model,
                 models=["gemini-2.5-pro", "gemini-3.5-flash", "gemini-3.1-flash-lite"], **{
                     "token_url": HOSTS["IDP_TOKEN_URL"], "base_url": HOSTS["GATEWAY_BASE_URL"]})
    return GatewayClient(cfg=cfg, tokens=None, http=None)


def test_each_model_takes_its_depth_its_own_way(monkeypatch):
    monkeypatch.setenv("GATEWAY_MODEL_THINKING", "gemini-3.1-flash-lite:none")
    monkeypatch.setenv("GATEWAY_MODEL_CAPS", "gemini-3.1-flash-lite:8192")
    pro = _client()
    assert pro.thinking == "budget"
    assert pro._config("high", 8192) == {"maxOutputTokens": 8192 + 16384,
                                         "thinkingConfig": {"includeThoughts": True, "thinkingBudget": 16384}}
    flash = pro.for_model("gemini-3.5-flash")
    assert flash.thinking == "level" and flash.model == "gemini-3.5-flash"
    assert flash._url().endswith("/models/gemini-3.5-flash/generateContent")
    assert flash._config("low", 8192) == {"maxOutputTokens": 8192,
                                          "thinkingConfig": {"includeThoughts": True, "thinkingLevel": "low"}}
    assert flash._config("medium", 8192, False)["thinkingConfig"] == {"includeThoughts": False, "thinkingLevel": "medium"}
    lite = pro.for_model("gemini-3.1-flash-lite")
    assert lite.thinking == "none" and lite.cap == 8192
    assert lite._config("high", 65536) == {"maxOutputTokens": 8192}     # no thinkingConfig, the model's cap
    # the token manager and the route are shared; the counters are not
    assert flash.tokens is pro.tokens and flash.usage is not pro.usage
    # a rejected thinkingConfig turns any style off for the run
    flash.thinking_ok = False
    assert "thinkingConfig" not in flash._config("high", 100)
    with pytest.raises(GatewayError, match="no gateway model called 'gpt'"):
        pro.for_model("gpt")


def test_the_catalog_lists_every_model_on_every_plane(monkeypatch):
    for var in ("APP_ID", "APP_SECRET", "GEMINI_BEARER_TOKEN", "SAHS_MODEL_PLANE",
                "SYNAPSE_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "VERTEX_PROJECT_ID",
                "GATEWAY_MODEL", "GEMINI_MODEL", "GATEWAY_MODEL_THINKING"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("GATEWAY_MODELS", raising=False)
    rows = model_catalog()
    assert [r["id"] for r in rows] == ["vertex", "gateway"]              # one model each: the planes as before
    monkeypatch.setenv("GATEWAY_MODELS", MODELS)
    for key, value in HOSTS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    rows = model_catalog()
    assert [r["id"] for r in rows] == ["vertex", "gateway", "gateway:gemini-3.5-flash",
                                       "gateway:gemini-3.7-flash", "gateway:gemini-3.1-flash-lite"]
    by = {r["id"]: r for r in rows}
    assert by["gateway"]["model"] == "gemini-2.5-pro" and by["gateway"]["default"]
    assert by["gateway:gemini-3.5-flash"]["label"] == "Gemini 3.5 Flash"
    assert by["gateway:gemini-3.5-flash"]["thinking"] == "level"
    assert by["gateway"]["thinking"] == "budget"
    assert all(r["plane"] == "gateway" and r["available"] and not r["default"]
               for r in rows[2:])
    assert not by["vertex"]["available"] and "SYNAPSE_VERTEX_SA_KEY" in by["vertex"]["reason"]
    assert split_choice("gateway:gemini-3.5-flash") == ("gateway", "gemini-3.5-flash")
    assert split_choice("Vertex") == ("vertex", "") and split_choice("") == ("", "")
    assert join_choice("gateway", "") == "gateway" and join_choice("gateway", "x") == "gateway:x"


def test_the_runtime_switches_between_models_and_refuses_unknown_ones(compiled, tmp_path, monkeypatch):  # noqa: F811
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    build, tmp = compiled
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    for key, value in HOSTS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("SAHS_MODEL_PLANE", "gateway")
    monkeypatch.setenv("GATEWAY_MODELS", MODELS)
    heard: list[str] = []

    def factory(budget, plane):
        heard.append(plane)
        return ScriptedAgent(steps=[[{"text": f"answered on {plane}"}]])

    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp_path / "chat.sqlite3", model_factory=factory)
    session = runtime.create_session()
    assert runtime.choice_of(session) == "gateway" and runtime.plane_of(session) == "gateway"
    assert [m["id"] for m in runtime.dials()["models"]][:3] == ["vertex", "gateway", "gateway:gemini-3.5-flash"]
    # a model on the plane: remembered as plane:model
    got = runtime.set_session_model(session["id"], "gateway:gemini-3.5-flash")
    assert got == {"ok": True, "plane": "gateway", "choice": "gateway:gemini-3.5-flash", "model": "scripted"}
    assert runtime.store.get_session(session["id"])["model"] == "gateway:gemini-3.5-flash"
    # the plane's default model is the plane itself
    assert runtime.set_session_model(session["id"], "gateway:gemini-2.5-pro")["choice"] == "gateway"
    # a model the plane does not serve is refused, naming what it does
    with pytest.raises(ModelUnavailable, match="it serves gemini-2.5-pro, gemini-3.5-flash"):
        runtime.set_session_model(session["id"], "gateway:gemini-9")
    with pytest.raises(ModelUnavailable, match="no model plane called 'gpt'"):
        runtime.set_session_model(session["id"], "gpt:x")
    # a message names a model: it rides it and the chat remembers it
    started = runtime.start_turn(session["id"], "hello", model="gateway:gemini-3.7-flash")
    assert started["choice"] == "gateway:gemini-3.7-flash" and started["plane"] == "gateway"
    assert runtime.wait(session["id"], 30)
    assert runtime.store.get_session(session["id"])["model"] == "gateway:gemini-3.7-flash"
    events = runtime.runtime(session["id"]).bus.since(0)
    assert events[0]["ev"] == "turn_started" and events[0]["plane"] == "gateway"
    assert events[0]["model"] == "scripted"
    assert heard == ["gateway"]


def test_the_granted_scopes_are_exactly_what_the_models_derive():
    """The gateway team grants one path pattern per model; with the four
    models named, the derived scopes are those patterns (plus the
    embedding scopes), so GATEWAY_SCOPES need not repeat them."""
    env = {"GATEWAY_MODELS": "gemini-2.5-pro gemini-3.5-flash gemini-3.7-flash gemini-3.1-flash-lite"}
    derived = scopes_for(env)
    for granted in ("/genai/google/v1/models/gemini-3.7-flash/**::post",
                    "/genai/google/v1/models/gemini-3.5-flash/**::post",
                    "/genai/google/v1/models/gemini-3.1-flash-lite/**::post",
                    "/genai/google/v1/models/gemini-2.5-pro/**::post"):
        assert granted in derived, granted
    assert derived[-2:] == DEFAULT_SCOPES[-2:]
    assert [thinking_kind(m, env) for m in gateway_models(env)] == ["budget", "level", "level", "level"]
